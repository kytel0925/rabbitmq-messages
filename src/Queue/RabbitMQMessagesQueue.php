<?php

namespace SlothDevGuy\RabbitMQMessages\Queue;

use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpIncomingMessage;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpTopology;
use SlothDevGuy\RabbitMQMessages\Exceptions\UnsupportedOperationException;
use SlothDevGuy\RabbitMQMessages\Queue\Jobs\RabbitMQMessagesJob;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * Laravel queue driver ("rabbitmq-messages") used to CONSUME domain events.
 *
 * Supported:   pop (queue:work / queue:listen), size & friends (queue:monitor),
 *              clear (queue:clear).
 * Unsupported: pushing arbitrary Laravel jobs (push, pushRaw, later, bulk and
 *              therefore queue:retry of jobs stored for this connection).
 *              Domain events are published with RabbitMQMessage::dispatchMessage().
 */
class RabbitMQMessagesQueue extends Queue implements ClearableQueue, QueueContract
{
    protected const PUSH_HINT = 'Laravel jobs cannot be pushed to a domain-events connection; publish domain events with RabbitMQMessage::dispatchMessage() and use a regular queue connection for jobs.';

    public function __construct(
        protected AmqpConnectionManager $connections,
        protected AmqpTopology $topology,
        protected string $messagesConnection,
        protected ?string $defaultQueue = null,
    ) {}

    public function size($queue = null): int
    {
        $queue = $this->getQueue($queue);

        try {
            return $this->connections->run($this->messagesConnection, AmqpConnectionManager::CONSUME,
                function (AMQPChannel $channel) use ($queue) {
                    [, $messages] = $channel->queue_declare($queue, true);

                    return (int) $messages;
                });
        } catch (AMQPProtocolChannelException $e) {
            if ($e->getCode() === 404) {
                return 0;
            }

            throw $e;
        }
    }

    public function pendingSize($queue = null): int
    {
        return $this->size($queue);
    }

    /**
     * Messages waiting in the delayed retry queue of this connection.
     */
    public function delayedSize($queue = null): int
    {
        $retryQueue = MessagesConfig::retryQueue($this->config());

        return $retryQueue ? $this->size($retryQueue) : 0;
    }

    /**
     * AMQP does not expose unacknowledged deliveries through a passive declare.
     */
    public function reservedSize($queue = null): int
    {
        return 0;
    }

    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        return null;
    }

    public function push($job, $data = '', $queue = null)
    {
        throw UnsupportedOperationException::queueOperation('push', $this->messagesConnection, static::PUSH_HINT);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        throw UnsupportedOperationException::queueOperation('pushRaw', $this->messagesConnection, static::PUSH_HINT);
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        throw UnsupportedOperationException::queueOperation('later', $this->messagesConnection, static::PUSH_HINT);
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
        throw UnsupportedOperationException::queueOperation('bulk', $this->messagesConnection, static::PUSH_HINT);
    }

    /**
     * Fetch the next message (basic.get, manual acknowledgement).
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        /** @var AMQPMessage|null $message */
        $message = $this->connections->run($this->messagesConnection, AmqpConnectionManager::CONSUME,
            function (AMQPChannel $channel, array $config) use ($queue) {
                if (MessagesConfig::declareTopology()) {
                    $this->declareConsumerTopology($channel, $config, $queue);
                }

                return $channel->basic_get($queue);
            });

        if (! $message) {
            return null;
        }

        return new RabbitMQMessagesJob(
            $this->container,
            new AmqpIncomingMessage($message, $queue),
            $this->connectionName,
            $this->messagesConnection,
            $queue,
        );
    }

    /**
     * Purge the consumer queue (queue:clear).
     */
    public function clear($queue): int
    {
        $queue = $this->getQueue($queue);

        try {
            return (int) $this->connections->run($this->messagesConnection, AmqpConnectionManager::CONSUME,
                fn (AMQPChannel $channel) => $channel->queue_purge($queue));
        } catch (AMQPProtocolChannelException $e) {
            if ($e->getCode() === 404) {
                return 0;
            }

            throw $e;
        }
    }

    public function getQueue(?string $queue = null): string
    {
        return $queue ?: ($this->defaultQueue ?: $this->config()['queue']);
    }

    public function getMessagesConnection(): string
    {
        return $this->messagesConnection;
    }

    /**
     * Declare the topology of every library connection feeding this queue
     * (e.g. fanout + topic connections sharing one consumer queue).
     */
    protected function declareConsumerTopology(AMQPChannel $channel, array $config, string $queue): void
    {
        $connections = [$config];
        foreach (array_keys(MessagesConfig::connections()) as $name) {
            if ($name === $config['name']) {
                continue;
            }

            $candidate = MessagesConfig::connection($name);
            if ($candidate['queue'] === $queue && $candidate['hosts'] == $config['hosts']) {
                $connections[] = $candidate;
            }
        }

        foreach ($connections as $connection) {
            $this->topology->setup($channel, $connection, $queue);
        }
    }

    protected function config(): array
    {
        return MessagesConfig::connection($this->messagesConnection);
    }
}
