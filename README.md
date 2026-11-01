# RabbitMQ Messages

Idempotent and resilient **domain events between bounded contexts** for Laravel, transported over RabbitMQ.

A bounded context (an application, a module, a service) publishes facts such as `billing.invoice.paid`;
every other bounded context interested in them consumes the event, exactly once from its point of view,
even when the network, the broker or a handler fails.

The library implements the patterns that make this safe:

| Pattern | Side | What it gives you |
|---|---|---|
| **Transactional outbox** (`dispatch_message`) | producer | The event is stored in your database (inside your transaction) before it is published, so it is never lost and never published for a rolled-back change. |
| **Publisher confirms** | producer | A publish only counts as done when RabbitMQ has confirmed it. |
| **Idempotent inbox** (`listen_message`) | consumer | Each message `uuid` is processed once; redeliveries and duplicates are acknowledged and discarded. |
| **Delayed retries** | consumer | A failing handler is retried after a delay, up to `max_tries`, without blocking the queue and without other consumers seeing the retry. |
| **Dead letter queue** | consumer | Messages that exhausted their retries, or are not retriable, are parked for inspection instead of being lost or looping forever. |
| **Message rotation** | both | Old outbox/inbox rows are deleted on a schedule to keep the tables small. |

> **v2** removes the dependency on `vladimir-yuldashev/laravel-queue-rabbitmq`. It talks to RabbitMQ directly
> through `php-amqplib` and ships its own Laravel queue driver. Upgrading from v1? Read
> [Migrating from v1](#migrating-from-v1).

---

## Contents

- [How it works](#how-it-works)
- [Requirements and support matrix](#requirements-and-support-matrix)
- [Installation](#installation)
- [Configuration](#configuration)
- [Dispatching events](#dispatching-events)
- [Consuming events](#consuming-events)
- [Retries, dead letters and idempotency](#retries-dead-letters-and-idempotency)
- [Running the workers](#running-the-workers)
- [Artisan commands](#artisan-commands)
- [Message rotation](#message-rotation)
- [Operational notes and constraints](#operational-notes-and-constraints)
- [Migrating from v1](#migrating-from-v1)
- [Developing the package](#developing-the-package)

---

## How it works

```text
 Bounded context A (producer)                         RabbitMQ                          Bounded context B (consumer)
┌───────────────────────────────┐                                                   ┌────────────────────────────────────┐
│ DB transaction                │                                                   │ php artisan queue:work rabbitmq    │
│  ├─ your domain changes       │                                                   │  ├─ validate message               │
│  └─ RabbitMQMessage::         │      DispatchMessagesJob            ┌─────────┐   │  ├─ find handler                    │
│       dispatchMessage()  ─────┼──▶ (regular Laravel queue) ──────▶ │exchange │──▶│  ├─ already processed? → ack & skip │
│       (row in dispatch_message│      publish + confirm              └─────────┘   │  ├─ store in listen_message         │
│        status "created")      │      status → "dispatched"               │        │  ├─ handler (DB transaction)        │
└───────────────────────────────┘                                          ▼        │  └─ status "processed" → ack        │
                                                                   [ B consumer   ] │                                    │
                                                                   [ queue        ]◀┼─── retry after delay ──┐           │
                                                                          │         │  on failure:           │           │
                                                                          ▼         │   tries < max ─▶ [retry queue]     │
                                                                                    │   otherwise    ─▶ [dead letter q]  │
                                                                                    └────────────────────────────────────┘
```

* **Dispatch side.** `RabbitMQMessage::dispatchMessage()` writes a `dispatch_message` row and queues a
  `DispatchMessagesJob` *after the database transaction commits*. That job runs on any regular Laravel queue
  connection (database, redis, sqs…), publishes the message with publisher confirms and marks the row
  `dispatched` (or `failed`, in which case Laravel's normal `--tries`/`failed_jobs`/`queue:retry` apply).
* **Consume side.** Each library connection is also a Laravel queue connection that uses the
  `rabbitmq-messages` driver, so the standard `php artisan queue:work rabbitmq` consumes events. Every
  delivery goes through the listener pipeline (validation → handler lookup → idempotency → retries
  exhausted? → store → handler → ack).

Messages are plain JSON with standard AMQP properties, so any producer or consumer (in any language)
can take part as long as it sets `app_id`, `type` (event name) and `message_id` (UUID).

---

## Requirements and support matrix

| | Supported |
|---|---|
| PHP | **8.2 – 8.5** (extensions: `sockets`, `mbstring`, `json`; `pcntl` recommended for `--timeout`) |
| Laravel | **11.x, 12.x, 13.x** |
| RabbitMQ | 3.12+ and 4.x (AMQP 0-9-1) |
| AMQP client | `php-amqplib/php-amqplib` ^3.7 (installed automatically) |

Laravel itself constrains which PHP versions can be combined:

| Laravel | PHP supported by Laravel | Upstream status (Laravel support policy, Nov 2026) | Verification for this release |
|---|---|---|---|
| 11.x | 8.2 – 8.4 | end of life (security fixes ended March 2026); supported here on a best-effort basis | full suite run on PHP 8.3 + RabbitMQ 3.12; other PHP versions via the CI matrix |
| 12.x | 8.2 – 8.5 | security fixes only | full suite run on PHP 8.3 + RabbitMQ 3.12; other PHP versions via the CI matrix |
| 13.x | 8.3 – 8.5 | active | full suite run on PHP 8.3 + RabbitMQ 3.12; other PHP versions via the CI matrix |

PHP 8.4/8.5 review: the code base has no implicitly nullable parameters (deprecated in 8.4), no
non-canonical casts, backtick operators, `__sleep`/`__wakeup` or other constructs deprecated in 8.5.
The GitHub Actions workflow (`.github/workflows/tests.yml`) runs the unit and broker-backed feature
tests for every PHP × Laravel combination above. If you need PHP 8.5, use Laravel 12 or 13: Laravel 11
does not officially support it.

Dropped in v2: PHP 8.1 and Laravel 10 (both end of life).

---

## Installation

```bash
composer require sloth-dev-guy/rabbitmq-messages
```

The service provider and the `RabbitMQMessage` facade are auto-discovered.

```bash
php artisan rabbitmq-messages:install   # publishes config, migrations and worker scripts
php artisan migrate                     # creates dispatch_message and listen_message
php artisan rabbitmq-messages:setup     # declares exchanges, queues, bindings, retry and dead letter queues
```

`rabbitmq-messages:install` asks which connections you need (`all`, `fanout` or `topic`), trims the
published `config/rabbitmq-messages.php` accordingly and prints the environment variables to add.
It never touches `config/queue.php`. Use `--connections=fanout --force` for non-interactive installs.

Minimal `.env` for a bounded context called *billing*:

```dotenv
RABBITMQ_APP_ID=billing
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=billing
RABBITMQ_PASSWORD=secret
RABBITMQ_VHOST=/

RABBITMQ_QUEUE=billing.messages                    # this context's consumer queue
RABBITMQ_RETRY_QUEUE=billing.messages.retry        # one retry queue per consumer queue
RABBITMQ_RETRY_QUEUE_DELAY=3000                    # milliseconds
RABBITMQ_DEAD_LETTER_QUEUE=billing.messages.dead-letter
RABBITMQ_MESSAGES_MAX_TRIES=3
```

---

## Configuration

Everything lives in `config/rabbitmq-messages.php`. The most relevant keys:

| Key | Default | Meaning |
|---|---|---|
| `app_id` | `RABBITMQ_APP_ID` → `APP_NAME` | Identifies this bounded context as producer (AMQP `app_id`). |
| `default` | `rabbitmq` | Connection used by `dispatchMessage()` when none is given. |
| `connections.*` | `rabbitmq` (fanout), `rabbitmq_topic` (topic) | Where to publish (`exchange`, `exchange_type`, `routing_key`) and where to consume (`queue`), plus `retry` and `dead_letter`. |
| `connections.*.hosts` | one host from `RABBITMQ_*` | List of hosts tried in order (fail-over). |
| `connections.*.options` | see file | Timeouts, heartbeat, keepalive, SSL, `publisher_confirms`, `publish_timeout`. |
| `declare_topology` | `true` | Declare exchanges/queues/bindings lazily on first publish/consume. Set to `false` if the topology is provisioned elsewhere. |
| `register_queue_connections` | `true` | Expose every connection as `queue.connections.<name>` with the `rabbitmq-messages` driver. |
| `outbox.queue_connection` | `null` (= `QUEUE_CONNECTION`) | Laravel queue connection running `DispatchMessagesJob`. |
| `outbox.queue` | `rabbitmq-messages.dispatch-queue` | Laravel queue name of `DispatchMessagesJob`. |
| `outbox.after_commit` | `true` | Publish only after the surrounding DB transaction commits. |
| `database_connection` | `DB_CONNECTION` | Database connection of the `dispatch_message`/`listen_message` tables. |
| `models.*` | package models | Swap the Eloquent models (they must extend the package ones). |
| `message_handlers` | `[]` | Event name (or `/regex/`) ⇒ handler class. |
| `max_tries` | `3` | Retries before dead-lettering. `0` disables retries. |
| `rotation.*` | see [Message rotation](#message-rotation) | Retention and schedule of the cleanup job. |

A connection is both a **publishing destination** and a **consumer definition**. Several connections may
share one consumer queue: the default config binds `RABBITMQ_QUEUE` to `amq.fanout` (connection `rabbitmq`)
and to `amq.topic` with `RABBITMQ_TOPIC_ROUTING_KEY` (connection `rabbitmq_topic`), so a single worker on
`rabbitmq` receives both. Incoming messages are attributed to the connection whose exchange/routing key they
arrived through, so retries and dead letters use that connection's settings.

Adding your own connection, e.g. a dedicated exchange for the *billing* context:

```php
// config/rabbitmq-messages.php
'connections' => [
    // ...
    'billing_events' => [
        'hosts' => $hosts,
        'options' => $options,
        'exchange' => 'billing.events',
        'exchange_type' => 'topic',
        'routing_key' => 'billing.invoice',
        'queue' => env('RABBITMQ_QUEUE'),
        'retry' => ['queue' => env('RABBITMQ_RETRY_QUEUE'), 'delay' => 5000],
        'dead_letter' => ['queue' => env('RABBITMQ_DEAD_LETTER_QUEUE')],
    ],
],
```

If you prefer to declare the Laravel queue connection yourself (for example to pin the consumer queue),
add it to `config/queue.php`; the library will not override it:

```php
'billing_events' => [
    'driver' => 'rabbitmq-messages',
    'connection' => 'billing_events',   // key in rabbitmq-messages.connections
    'queue' => 'billing.messages',      // used by queue:work/queue:clear when --queue is omitted
],
```

---

## Dispatching events

```php
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;

RabbitMQMessage::dispatchMessage('billing.invoice.paid', collect([
    'invoice_id' => $invoice->id,
    'amount' => $invoice->total,
    'currency' => 'EUR',
]));
```

Dispatch inside the same transaction as the change it describes. The outbox row is committed (or rolled
back) together with your data and the publish job only runs after commit:

```php
DB::transaction(function () use ($order) {
    $order->markAsShipped();

    RabbitMQMessage::dispatchMessage('shipping.order.shipped', collect([
        'order_id' => $order->id,
        'shipped_at' => now()->toIso8601String(),
    ]));
});
```

Choose a connection (exchange) explicitly:

```php
RabbitMQMessage::dispatchMessage('catalog.product.updated', collect($payload), 'rabbitmq_topic');
```

Encapsulate an event in its own class by extending `DispatchMessage`:

```php
use SlothDevGuy\RabbitMQMessages\DispatchMessage;

final class InvoicePaid extends DispatchMessage
{
    public function __construct(Invoice $invoice)
    {
        parent::__construct('billing.invoice.paid', collect([
            'invoice_id' => $invoice->id,
            'amount' => $invoice->total,
        ]));
    }
}

RabbitMQMessage::dispatchMessage(new InvoicePaid($invoice));
```

What is published:

```text
properties:  app_id=billing  type=billing.invoice.paid  message_id=<uuid v4>
             timestamp=<unix>  content_type=application/json  delivery_mode=2 (persistent)
body:        {"uuid":"<same uuid>","fired_at":"2026-11-01T10:00:00+00:00","invoice_id":42,"amount":"99.90"}
```

The returned `DispatchMessageModel` tells you the state of the outbox row (`created`, `dispatched`, `failed`).
Failed publishes are re-thrown so the outbox worker applies `--tries`/`--backoff`; once exhausted they are
in `failed_jobs` and `php artisan queue:retry` publishes them again.

You can also inject `MessageDispatcherInterface` instead of using the facade.

---

## Consuming events

1. Write a handler:

```php
namespace App\Messages;

use Illuminate\Support\Collection;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageHandlerInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

final class InvoicePaidHandler implements MessageHandlerInterface
{
    public function __construct(private SubscriptionService $subscriptions) {}

    public function handle(Collection $payload, ListenMessageModel $message): void
    {
        // runs inside a database transaction together with the inbox update
        $this->subscriptions->activate($payload->get('invoice_id'));

        // $message->uuid, $message->name, $message->properties->get('app_id') ...
    }
}
```

2. Register it (exact names or regular expressions starting with `/`):

```php
// config/rabbitmq-messages.php
'message_handlers' => [
    'billing.invoice.paid' => App\Messages\InvoicePaidHandler::class,
    '/^catalog\.product\./' => App\Messages\ProductProjector::class,
],
```

Messages without a handler are acknowledged and ignored, which lets many bounded contexts share a
fanout exchange and only react to what they care about.

3. Run a worker:

```bash
php artisan queue:work rabbitmq
```

Controlling the outcome from a handler:

| Throw… | Outcome |
|---|---|
| nothing | stored as `processed`, acknowledged |
| any exception | retried after `retry.delay` ms until `max_tries`, then dead-lettered |
| `NonRetriableMessageException` (or anything implementing `NonRetriableMessageThrowable`) | dead-lettered immediately, e.g. invalid business data |
| anything implementing `SkipListenMessageThrowable` | acknowledged and discarded, nothing stored |

```php
use SlothDevGuy\RabbitMQMessages\Exceptions\NonRetriableMessageException;

if (! Customer::whereKey($payload->get('customer_id'))->exists()) {
    throw new NonRetriableMessageException('Unknown customer');
}
```

---

## Retries, dead letters and idempotency

**Idempotency.** `listen_message.uuid` is unique. A message whose `message_id` was already stored is
acknowledged and skipped, whatever its previous outcome. Because the producer side is at-least-once
(a publish may be repeated after a timeout), this check is what turns delivery into
*effectively exactly once* processing. The handler and the `processed` status update run in the same
database transaction, so database side effects of a handler are applied at most once; side effects
outside the database (HTTP calls, e-mails) must be idempotent themselves.

**Retries.** On failure the message is republished, with an incremented `tries` header and the failure
reason, to the connection's **retry queue**. That queue has a TTL of `retry.delay` ms and dead-letters
expired messages back to the **consumer queue they came from only** (through the default exchange). Other
bounded contexts bound to the same exchange never see the retried copy. The original exchange and routing
key travel in the `origin_exchange`/`origin_routing_key` headers.

**Dead letters.** After `max_tries` retries, or for non-retriable errors, the message is published to the
`dead_letter.queue`. When no dead letter queue is configured, the message is kept as a `failed` row in
`listen_message` instead (with the exception history in `metadata.exceptions`).

**Infrastructure failures.** If the database or the broker fails *before* the delivery could be settled,
the job re-throws: the worker reports the exception and the message is requeued untouched.

---

## Running the workers

Two kinds of worker processes run per bounded context:

| Worker | Command | Purpose |
|---|---|---|
| outbox | `php artisan queue:work <QUEUE_CONNECTION> --queue=rabbitmq-messages.dispatch-queue --tries=3 --backoff=10` | publishes dispatched events |
| consumer | `php artisan queue:work rabbitmq` | consumes events from other bounded contexts |

`php artisan rabbitmq-messages:install` publishes ready-to-use loops to `etc/bin/dispatch-messages.sh` and
`etc/bin/listen-messages.sh` (the latter runs `rabbitmq-messages:setup` first). A Supervisor example:

```ini
[program:billing-outbox]
command=php /app/artisan queue:work database --queue=rabbitmq-messages.dispatch-queue --tries=3 --backoff=10 --max-time=3600
numprocs=1
autorestart=true
stopwaitsecs=60

[program:billing-consumer]
command=php /app/artisan queue:work rabbitmq --sleep=1 --max-time=3600 --timeout=300
process_name=%(program_name)s_%(process_num)02d
numprocs=2
autorestart=true
stopwaitsecs=330
```

Remember `php artisan queue:restart` after each deployment.

---

## Artisan commands

### Library commands

| Command | Description |
|---|---|
| `rabbitmq-messages:install [--connections=all\|fanout\|topic] [--force]` | Publish config, migrations and worker scripts; print suggested `.env` variables. |
| `rabbitmq-messages:setup [connection...] [--queue=]` | Declare exchanges, consumer queue, bindings, retry and dead letter queues (idempotent). Replaces `rabbitmq:exchange-declare`, `rabbitmq:queue-declare` and `rabbitmq:queue-bind`. |
| `rabbitmq-messages:rotate [--table=] [--status=] [--days=] [--chunk=] [--pretend] [--force]` | Delete old messages, see [Message rotation](#message-rotation). |

### `php artisan queue:*` compatibility

The `rabbitmq-messages` driver implements only what consuming domain events needs. A
`CommandStarting` listener explains the rest before the command runs.

| Command | On a `rabbitmq-messages` connection |
|---|---|
| `queue:work {connection}` | ✅ Supported. `--queue` (comma separated), `--once`, `--stop-when-empty`, `--max-jobs`, `--max-time`, `--memory`, `--sleep`, `--rest`, `--timeout`, `--force`, `--name`, `--json` behave as usual. ⚠️ `--tries`, `--backoff`, `--delay` are **not supported** and ignored with a warning: retries are governed by `max_tries` and the retry queue. |
| `queue:listen {connection}` | ✅ Supported (spawns `queue:work`), same option rules as `queue:work`. |
| `queue:clear {connection} [--queue=]` | ✅ Supported: purges the consumer queue. |
| `queue:monitor connection:queue,...` | ✅ Supported: *size/pending* = ready messages, *delayed* = messages waiting in the retry queue, *reserved* = 0 (not exposed by AMQP). |
| `queue:pause` / `queue:resume` / `queue:restart` | ✅ Supported (Laravel-level, cache based). |
| `queue:failed`, `queue:retry`, `queue:forget`, `queue:flush`, `queue:prune-failed` | Global commands over Laravel's `failed_jobs`, used for the **outbox** jobs. Consumed events never reach `failed_jobs` (see dead letters). Retrying a job onto a `rabbitmq-messages` connection fails with *"[pushRaw] is not supported by the rabbitmq-messages driver"*. |
| any other queue command targeting the connection | ❌ Aborts with *"[queue:…] is not supported by the rabbitmq-messages driver"*. |
| `dispatch(new SomeJob)->onConnection('rabbitmq')`, `Queue::connection('rabbitmq')->push()`/`later()`/`bulk()` | ❌ `UnsupportedOperationException`: a domain-events connection does not transport Laravel jobs. Use `RabbitMQMessage::dispatchMessage()`. |

---

## Message rotation

Both tables grow with every event. `rabbitmq-messages:rotate` deletes rows by **status** and **age**
(`updated_at`), in primary-key chunks so it never holds long locks:

```php
// config/rabbitmq-messages.php
'rotation' => [
    'enabled' => env('RABBITMQ_MESSAGES_ROTATION_ENABLED', true),
    'schedule' => env('RABBITMQ_MESSAGES_ROTATION_CRON', '30 3 * * *'),   // daily at 03:30
    'on_one_server' => env('RABBITMQ_MESSAGES_ROTATION_ONE_SERVER', false),
    'chunk_size' => 1000,
    'retention' => [   // days; null = keep forever
        'dispatch_message' => ['dispatched' => 30, 'failed' => 90, 'created' => null],
        'listen_message'   => ['processed' => 30, 'failed' => 90, 'queued' => null],
    ],
],
```

**Scheduled job.** The command is registered in Laravel's scheduler automatically (`withoutOverlapping()`,
plus `onOneServer()` when enabled; that needs a cache store with locks). Check it with
`php artisan schedule:list`. You only need Laravel's usual cron entry:

```cron
* * * * * cd /path/to/app && php artisan schedule:run >> /dev/null 2>&1
```

Without Laravel's scheduler, call the command from cron directly:

```cron
30 3 * * * cd /path/to/app && php artisan rabbitmq-messages:rotate >> /dev/null 2>&1
```

**Manual runs:**

```bash
php artisan rabbitmq-messages:rotate --pretend                                   # count only
php artisan rabbitmq-messages:rotate --table=listen_message --status=failed --days=14
php artisan rabbitmq-messages:rotate --force                                     # even if disabled
```

> ⚠️ **Rotation and idempotency.** `listen_message` rows *are* the idempotency records. A duplicate of a
> message that arrives after its row was rotated is processed again. Keep `processed` retention well above
> the longest window in which a producer could re-publish (outbox retries, `queue:retry` of old failed jobs)
> or messages could sit in a queue. `created`/`queued` rows (not yet published / in progress) are never
> rotated by default.

Run the index migration published by `rabbitmq-messages:install`
(`add_rotation_indexes_to_rabbitmq_messages_tables`) so the cleanup stays cheap on large tables.

---

## Operational notes and constraints

* **Topology permissions.** With `declare_topology=true` (default) the broker user needs the *configure*
  permission on the queues/exchanges it declares. Otherwise set it to `false` and provision the topology
  with `rabbitmq-messages:setup` from a privileged user, a definitions file or IaC.
* **One retry queue per consumer queue.** A retry queue dead-letters to one consumer queue (fixed when it
  is declared). Do not share a retry queue between bounded contexts. Dead letter queues can be shared.
* **Queue arguments are immutable.** Changing `retry.delay` (TTL) or the consumer queue of an existing retry
  queue makes RabbitMQ reply `PRECONDITION_FAILED`: drain and delete the retry queue, or use a new name.
* **Classic durable queues.** The library declares durable classic queues without extra arguments. For
  quorum queues or policies, provision them yourself and set `declare_topology=false`.
* **Polling consumer.** `queue:work` fetches with `basic.get` (manual ack), one message at a time per process:
  when the queue is empty, latency is bounded by `--sleep`. Scale throughput by running more worker processes.
* **Ordering is not guaranteed.** Retries re-enqueue messages behind newer ones and several workers run in parallel.
* **At-least-once publishing.** A publish timeout or a crash between publish and the status update re-publishes
  the message; consumers discard the duplicate by `uuid`.
* **Heartbeats.** php-amqplib is single threaded: no heartbeats are sent while a handler runs. Keep
  `RABBITMQ_HEARTBEAT=0` (default) or set it above your longest handler duration.
* **Publisher confirms** add one broker round trip per publish. Disable them (`RABBITMQ_PUBLISHER_CONFIRMS=false`)
  only if you accept losing messages when the broker fails.
* **Connections.** Each process opens at most two AMQP connections per broker: one for publishing (outbox,
  retries, dead letters) and one for consuming, so publisher flow control or a publish reconnect never breaks
  the delivery being processed. Library connections pointing to the same hosts share them.
* **Multiple hosts** are tried in order when connecting (fail-over, not load balancing).
* **Rotation deletes data,** including idempotency records. See [Message rotation](#message-rotation).

---

## Migrating from v1

v1 extended `vladimir-yuldashev/laravel-queue-rabbitmq`. v2 removes that dependency entirely.

### Integration points that depended on the removed package

| v1 integration point | v2 replacement |
|---|---|
| `composer.json` required `vladimir-yuldashev/laravel-queue-rabbitmq ^15` | requires `php-amqplib/php-amqplib ^3.7` directly |
| `SlothDevGuy\RabbitMQMessages\RabbitMQQueue` extended the vendor `RabbitMQQueue` (`declareDestination`, `declareQueue`, `bindQueue`, `publishBasic`, `getChannel`) | `Amqp\AmqpConnectionManager` (connections/channels, reconnects), `Amqp\AmqpTopology` (declarations), `Amqp\AmqpPublisher` (publish, retry, dead letter) |
| `SlothDevGuy\RabbitMQMessages\RabbitMQJob` extended the vendor `RabbitMQJob` | `Queue\Jobs\RabbitMQMessagesJob` |
| Vendor queue driver `rabbitmq` + `worker`/`options.queue.job` keys in `config/queue.php` | own driver `rabbitmq-messages` (`Queue\RabbitMQMessagesConnector`, `Queue\RabbitMQMessagesQueue`), connections auto-registered |
| Published `config/queue.php` **overwrote the application's file** | nothing is published to `config/queue.php`; all settings live in `config/rabbitmq-messages.php` |
| `ConsumeCommand` override in the service provider (workaround for vendor issue #668) and `php artisan rabbitmq:consume` | `php artisan queue:work rabbitmq` |
| `rabbitmq:exchange-declare`, `rabbitmq:queue-declare`, `rabbitmq:queue-bind` in `listen-messages.sh` | `php artisan rabbitmq-messages:setup` |
| `rabbitmq:queue-purge` | `php artisan queue:clear rabbitmq` |
| `DispatchMessagesJob` / `MessageResilient` used `Queue::connection('rabbitmq')` cast to `RabbitMQQueue` | inject `AmqpPublisher` |
| `DispatchMessage` / `FromRabbitMQMessage` read `queue.connections.*.options.queue.*` and `app_id` | read `rabbitmq-messages.connections.*` and `rabbitmq-messages.app_id` |

### Upgrade steps

1. `composer remove vladimir-yuldashev/laravel-queue-rabbitmq` (unless you use it for something else; both
   can coexist) and `composer require sloth-dev-guy/rabbitmq-messages:^2.0`.
2. **Remove the `rabbitmq` and `rabbitmq_topic` connections from `config/queue.php`** (v1 published them).
   If you keep them, v2 detects the v1 shape, ignores it and raises a deprecation notice.
3. Re-publish the config: `php artisan vendor:publish --tag=rabbitmq-messages-config --force`
   (or run `rabbitmq-messages:install --force`), then port your `message_handlers`, `models`, `max_tries`.
4. Publish and run the new migration (rotation indexes):
   `php artisan vendor:publish --tag=rabbitmq-messages-migrations && php artisan migrate`.
5. Environment variables keep their names: `RABBITMQ_HOST/PORT/USERNAME/PASSWORD/VHOST`, `RABBITMQ_QUEUE`,
   `RABBITMQ_EXCHANGE`, `RABBITMQ_TOPIC_EXCHANGE`, `RABBITMQ_TOPIC_ROUTING_KEY`, `RABBITMQ_RETRY_QUEUE`,
   `RABBITMQ_TOPIC_RETRY_QUEUE`, `RABBITMQ_RETRY_QUEUE_DELAY`, `RABBITMQ_DEAD_LETTER_QUEUE`,
   `RABBITMQ_MESSAGES_MAX_TRIES`, `RABBITMQ_MESSAGES_DB_CONNECTION`. **No longer used:**
   `RABBITMQ_RETRY_EXCHANGE`, `RABBITMQ_RETRY_EXCHANGE_TYPE`, `RABBITMQ_RETRY_ROUTING_KEY`,
   `RABBITMQ_TOPIC_RETRY_ROUTING_KEY`, `RABBITMQ_DEAD_LETTER_EXCHANGE`, `RABBITMQ_DEAD_LETTER_EXCHANGE_TYPE`,
   `RABBITMQ_DEAD_LETTER_ROUTING_KEY`.
6. **Recreate the retry queues.** In v1 a retry queue dead-lettered to the *exchange*, so retried messages were
   re-broadcast to every bounded context. In v2 it dead-letters to the consumer queue only, which changes the
   queue arguments. Stop the consumers, wait for the retry queues to drain, delete them (or choose new names,
   e.g. `<context>.messages.retry`), then run `php artisan rabbitmq-messages:setup`. `setup` reports
   `PRECONDITION_FAILED` if an old retry queue is still there.
7. Replace `php artisan rabbitmq:consume rabbitmq …` with `php artisan queue:work rabbitmq …` in your process
   manager (or re-publish the worker scripts with `--tag=rabbitmq-messages-bin --force`). Drop
   `--prefetch-count`, `--prefetch-size`, `--consumer-tag`, `--max-priority`. Drop `--tries`: it is ignored.
8. Deploy producers and consumers in any order: the message format did not change, so v1 and v2 bounded
   contexts interoperate.

### Behaviour changes

* Retried messages are routed to the failing consumer only (see step 6) and carry
  `origin_exchange`/`origin_routing_key` headers.
* Dead letters are published straight to `dead_letter.queue`. v1 bound the dead letter queue to `amq.direct`
  using the exchange **type** (`direct`) as routing key; that binding is no longer used and can be removed.
* Publisher confirms are on by default; a broker `nack` or confirm timeout marks the outbox row `failed`.
* `DispatchMessagesJob` is dispatched with `afterCommit()` and honours `outbox.queue_connection`/`outbox.queue`.
* Validation now checks the payload and metadata themselves (v1 validated the properties three times);
  a body that is not valid JSON is rejected (dropped) instead of being handled with an empty payload.
* A non-retriable message whose inbox row already existed is now marked `failed` when there is no dead letter
  queue (v1 left it `queued`).
* Message rotation is **enabled by default** and scheduled daily: rows older than 30 days (`processed`/`dispatched`)
  and 90 days (`failed`) are deleted. Set `RABBITMQ_MESSAGES_ROTATION_ENABLED=false` to opt out.
* PHP 8.1 and Laravel 10 are no longer supported.

### API changes (only relevant if you extended internals)

* Removed: `RabbitMQQueue`, `RabbitMQJob`, `src/helpers.php`.
* `Pipes\MessageListener::sendMessageThroughPipes(IncomingMessageInterface $message, string $connection)`
  (was `AMQPMessage $message, string $queue, string $connection`); `Builders\FromRabbitMQMessage` and the
  resiliency pipes take an `IncomingMessageInterface` too.
* `Services\MessageResilient` is constructed with an `AmqpPublisher`; `getRetryConfiguration()` returns
  `retry_queue`/`retry_queue_delay` and `getDeadLetterConfiguration()` returns `dead_letter_queue`.
* Nullable parameters are now explicit (`?Enumerable $payload = null`), which is signature-compatible.
* `MessageHandlerInterface`, the facade methods, the models and the exceptions are unchanged.

---

## Developing the package

```bash
docker compose up -d                       # PHP 8.5 dev container + RabbitMQ
docker compose exec php.dev composer install
docker compose exec php.dev vendor/bin/phpunit
```

* `tests/Unit` needs no broker.
* `tests/Feature` publishes and consumes through a real RabbitMQ (`RABBITMQ_HOST`, `RABBITMQ_USERNAME`…);
  the tests are skipped when the broker is unreachable.
* `vendor/bin/pint` formats the code; CI runs the PHP × Laravel matrix with a RabbitMQ service.

## License

MIT. See [LICENSE.md](LICENSE.md).
