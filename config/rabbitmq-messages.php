<?php

use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

/*
|--------------------------------------------------------------------------
| Broker hosts and AMQP options
|--------------------------------------------------------------------------
|
| Shared by every connection below. Hosts are tried in order (fail-over).
| Connections that point to the same hosts/options share one AMQP socket.
|
*/
$hosts = [
    [
        'host' => env('RABBITMQ_HOST', '127.0.0.1'),
        'port' => (int) env('RABBITMQ_PORT', 5672),
        'user' => env('RABBITMQ_USERNAME', 'guest'),
        'password' => env('RABBITMQ_PASSWORD', 'guest'),
        'vhost' => env('RABBITMQ_VHOST', '/'),
    ],
];

$options = [
    'connection_timeout' => (float) env('RABBITMQ_CONNECTION_TIMEOUT', 3.0),
    'read_write_timeout' => (float) env('RABBITMQ_READ_WRITE_TIMEOUT', 3.0),
    'heartbeat' => (int) env('RABBITMQ_HEARTBEAT', 0),
    'keepalive' => (bool) env('RABBITMQ_KEEPALIVE', false),
    // wait for the broker to confirm every publish (recommended for the outbox)
    'publisher_confirms' => (bool) env('RABBITMQ_PUBLISHER_CONFIRMS', true),
    'publish_timeout' => (float) env('RABBITMQ_PUBLISH_TIMEOUT', 5.0),
    'ssl' => [
        'enabled' => (bool) env('RABBITMQ_SSL', false),
        'cafile' => env('RABBITMQ_SSL_CAFILE'),
        'local_cert' => env('RABBITMQ_SSL_LOCALCERT'),
        'local_key' => env('RABBITMQ_SSL_LOCALKEY'),
        'verify_peer' => (bool) env('RABBITMQ_SSL_VERIFY_PEER', true),
        'passphrase' => env('RABBITMQ_SSL_PASSPHRASE'),
    ],
];

return [

    /*
    |--------------------------------------------------------------------------
    | Application id
    |--------------------------------------------------------------------------
    |
    | Identifies this bounded context as the producer of every message
    | (AMQP "app_id" property). Must be unique per bounded context.
    |
    */
    'app_id' => env('RABBITMQ_APP_ID', env('APP_NAME', 'laravel')),

    /*
    |--------------------------------------------------------------------------
    | Default connection
    |--------------------------------------------------------------------------
    |
    | Connection used by RabbitMQMessage::dispatchMessage() when none is given.
    |
    */
    'default' => env('RABBITMQ_MESSAGES_CONNECTION', 'rabbitmq'),

    /*
    |--------------------------------------------------------------------------
    | Connections
    |--------------------------------------------------------------------------
    |
    | Each connection describes where messages are published (exchange and
    | routing key) and where this application consumes them (queue). Several
    | connections may share the same consumer queue (e.g. fanout + topic).
    |
    | retry.queue        Delayed retry queue. Failed messages wait there for
    |                    retry.delay milliseconds and then go back ONLY to the
    |                    consumer queue they came from. Leave empty to disable.
    | dead_letter.queue  Messages that exhausted their retries or are not
    |                    retriable end up there. Leave empty to keep them as
    |                    "failed" rows in the listen_message table instead.
    |
    | Every connection is automatically exposed as a Laravel queue connection
    | with the same name (driver "rabbitmq-messages"), so it can be consumed
    | with: php artisan queue:work rabbitmq
    |
    */
    'connections' => [
        // <fanout>
        'rabbitmq' => [
            'hosts' => $hosts,
            'options' => $options,
            'exchange' => env('RABBITMQ_EXCHANGE', 'amq.fanout'),
            'exchange_type' => env('RABBITMQ_EXCHANGE_TYPE', 'fanout'),
            'routing_key' => env('RABBITMQ_ROUTING_KEY', ''),
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'retry' => [
                'queue' => env('RABBITMQ_RETRY_QUEUE'),
                'delay' => (int) env('RABBITMQ_RETRY_QUEUE_DELAY', 3000),
            ],
            'dead_letter' => [
                'queue' => env('RABBITMQ_DEAD_LETTER_QUEUE'),
            ],
        ],
        // </fanout>
        // <topic>
        'rabbitmq_topic' => [
            'hosts' => $hosts,
            'options' => $options,
            'exchange' => env('RABBITMQ_TOPIC_EXCHANGE', 'amq.topic'),
            'exchange_type' => env('RABBITMQ_TOPIC_EXCHANGE_TYPE', 'topic'),
            'routing_key' => env('RABBITMQ_TOPIC_ROUTING_KEY', ''),
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'retry' => [
                'queue' => env('RABBITMQ_TOPIC_RETRY_QUEUE', env('RABBITMQ_RETRY_QUEUE')),
                'delay' => (int) env('RABBITMQ_RETRY_QUEUE_DELAY', 3000),
            ],
            'dead_letter' => [
                'queue' => env('RABBITMQ_DEAD_LETTER_QUEUE'),
            ],
        ],
        // </topic>
    ],

    /*
    |--------------------------------------------------------------------------
    | Topology
    |--------------------------------------------------------------------------
    |
    | When enabled, exchanges, queues and bindings are declared lazily the
    | first time a process publishes or consumes. Disable it when the broker
    | topology is managed elsewhere (e.g. definitions.json / Terraform) or
    | the broker user has no "configure" permission; then run
    | `php artisan rabbitmq-messages:setup` (or provision it yourself).
    |
    */
    'declare_topology' => (bool) env('RABBITMQ_MESSAGES_DECLARE_TOPOLOGY', true),

    /*
    |--------------------------------------------------------------------------
    | Laravel queue integration
    |--------------------------------------------------------------------------
    |
    | Register every connection above as `queue.connections.<name>` using the
    | "rabbitmq-messages" driver, unless your config/queue.php already
    | defines a connection with that name.
    |
    */
    'register_queue_connections' => true,

    /*
    |--------------------------------------------------------------------------
    | Outbox (dispatch side)
    |--------------------------------------------------------------------------
    |
    | dispatchMessage() stores the message in the dispatch_message table and
    | queues a DispatchMessagesJob on a regular Laravel queue (database, redis,
    | sqs, ...) that publishes it to RabbitMQ.
    |
    */
    'outbox' => [
        // null = the application's default queue connection (QUEUE_CONNECTION)
        'queue_connection' => env('RABBITMQ_MESSAGES_OUTBOX_CONNECTION'),
        'queue' => env('RABBITMQ_MESSAGES_OUTBOX_QUEUE', 'rabbitmq-messages.dispatch-queue'),
        // only publish once the surrounding database transaction committed
        'after_commit' => true,
    ],

    /*
    |--------------------------------------------------------------------------
    | Storage
    |--------------------------------------------------------------------------
    */
    'database_connection' => env('RABBITMQ_MESSAGES_DB_CONNECTION', env('DB_CONNECTION', 'sqlite')),

    'models' => [
        'dispatch_message' => DispatchMessageModel::class,
        'listen_message' => ListenMessageModel::class,
    ],

    /*
    |--------------------------------------------------------------------------
    | Message handlers (consume side)
    |--------------------------------------------------------------------------
    |
    | Map message names (AMQP "type") to classes implementing
    | MessageHandlerInterface. Keys starting with "/" are regular expressions.
    |
    | 'billing.invoice.paid' => App\Messages\InvoicePaidHandler::class,
    | '/^catalog\.product\./' => App\Messages\ProductHandler::class,
    |
    */
    'message_handlers' => [

    ],

    /*
    |--------------------------------------------------------------------------
    | Retries
    |--------------------------------------------------------------------------
    |
    | Maximum number of retries before a message is dead-lettered (or stored
    | as failed). 0 disables retries.
    |
    */
    'max_tries' => (int) env('RABBITMQ_MESSAGES_MAX_TRIES', 3),

    /*
    |--------------------------------------------------------------------------
    | Message rotation
    |--------------------------------------------------------------------------
    |
    | One rotation for both the outbox (dispatch_message) and the inbox
    | (listen_message): finished messages (dispatched, processed or failed)
    | whose last update is older than "retention_days" are deleted. Messages
    | still in flight (created / queued) are never rotated.
    |
    | IMPORTANT: listen_message rows are the idempotency (inbox) records. A
    | duplicated delivery that arrives after its row was rotated WILL be
    | processed again, so keep the retention well above the longest window in
    | which a producer could re-publish or the broker could redeliver.
    |
    | The command is registered in Laravel's scheduler automatically; you
    | still need the usual `schedule:run` cron entry.
    |
    */
    'rotation' => [
        'enabled' => (bool) env('RABBITMQ_MESSAGES_ROTATION_ENABLED', true),
        'schedule' => env('RABBITMQ_MESSAGES_ROTATION_CRON', '30 3 * * *'),
        'on_one_server' => (bool) env('RABBITMQ_MESSAGES_ROTATION_ONE_SERVER', false),
        'chunk_size' => (int) env('RABBITMQ_MESSAGES_ROTATION_CHUNK', 1000),
        'retention_days' => (int) env('RABBITMQ_MESSAGES_RETENTION_DAYS', 30),
    ],
];
