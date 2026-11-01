<?php

namespace SlothDevGuy\RabbitMQMessages\Amqp;

use Illuminate\Support\Str;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * Declares the broker objects the library relies on.
 *
 *   producer ──▶ [exchange] ──(routing key)──▶ [consumer queue] ──▶ queue:work
 *                                                   ▲      │ failure
 *                         TTL expired (retry.delay) │      ▼
 *                                              [retry queue]   [dead letter queue]
 *
 * Retries are published straight to the retry queue (default exchange) and,
 * once the TTL expires, dead-lettered back to the consumer queue only, so other
 * bounded contexts bound to the same exchange never see a retried copy.
 *
 * Declarations are idempotent and cached per process.
 */
class AmqpTopology
{
    /**
     * @var array<string, true>
     */
    protected array $declared = [];

    /**
     * Declare everything a connection needs: exchange, consumer queue and
     * binding, retry queue and dead letter queue.
     *
     * @return array<int, string> human readable list of what was declared
     */
    public function setup(AMQPChannel $channel, array $connection, ?string $queue = null): array
    {
        $queue = $queue ?: $connection['queue'];
        $report = [];

        if ($this->declareExchange($channel, $connection['exchange'], $connection['exchange_type'])) {
            $report[] = "exchange [{$connection['exchange']}] ({$connection['exchange_type']})";
        }

        $this->declareQueue($channel, $queue);
        $report[] = "queue [$queue]";

        $this->bind($channel, $queue, $connection['exchange'], (string) $connection['routing_key']);
        $report[] = "binding [{$connection['exchange']}] -> [$queue] (routing key: '{$connection['routing_key']}')";

        if ($retryQueue = MessagesConfig::retryQueue($connection)) {
            $this->declareRetryQueue($channel, $retryQueue, $queue, MessagesConfig::retryDelay($connection));
            $report[] = "retry queue [$retryQueue] -> [$queue] after ".MessagesConfig::retryDelay($connection).'ms';
        }

        if ($deadLetterQueue = MessagesConfig::deadLetterQueue($connection)) {
            $this->declareQueue($channel, $deadLetterQueue);
            $report[] = "dead letter queue [$deadLetterQueue]";
        }

        return $report;
    }

    /**
     * Declare an exchange; pre-declared broker exchanges (amq.*, default) are skipped.
     */
    public function declareExchange(AMQPChannel $channel, string $exchange, string $type): bool
    {
        if ($exchange === '' || Str::startsWith($exchange, 'amq.') || $this->wasDeclared($channel, "exchange:$exchange")) {
            return false;
        }

        $channel->exchange_declare($exchange, $type, false, true, false);

        return $this->markDeclared($channel, "exchange:$exchange");
    }

    public function declareQueue(AMQPChannel $channel, string $queue, array $arguments = []): bool
    {
        if ($this->wasDeclared($channel, "queue:$queue")) {
            return false;
        }

        $channel->queue_declare($queue, false, true, false, false, false, $arguments ? new AMQPTable($arguments) : []);

        return $this->markDeclared($channel, "queue:$queue");
    }

    /**
     * Declare a delayed retry queue that dead-letters expired messages back to
     * the given consumer queue (through the default exchange).
     */
    public function declareRetryQueue(AMQPChannel $channel, string $retryQueue, string $targetQueue, int $delay): bool
    {
        return $this->declareQueue($channel, $retryQueue, [
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $targetQueue,
            'x-message-ttl' => $delay,
        ]);
    }

    public function bind(AMQPChannel $channel, string $queue, string $exchange, string $routingKey): bool
    {
        // every queue is implicitly bound to the default exchange
        if ($exchange === '' || $this->wasDeclared($channel, "bind:$exchange:$routingKey:$queue")) {
            return false;
        }

        $channel->queue_bind($queue, $exchange, $routingKey);

        return $this->markDeclared($channel, "bind:$exchange:$routingKey:$queue");
    }

    /**
     * Forget the cache (e.g. after a channel error or in tests).
     */
    public function flush(): void
    {
        $this->declared = [];
    }

    protected function wasDeclared(AMQPChannel $channel, string $key): bool
    {
        return isset($this->declared[$this->scope($channel).$key]);
    }

    protected function markDeclared(AMQPChannel $channel, string $key): bool
    {
        return $this->declared[$this->scope($channel).$key] = true;
    }

    /**
     * Declarations are scoped to the underlying socket: a reconnect (maybe to
     * another node or vhost) declares everything again.
     */
    protected function scope(AMQPChannel $channel): string
    {
        $connection = $channel->getConnection();

        return ($connection ? spl_object_id($connection) : 0).'|';
    }
}
