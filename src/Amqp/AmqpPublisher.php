<?php

namespace SlothDevGuy\RabbitMQMessages\Amqp;

use Illuminate\Support\Arr;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\RabbitMQProperties;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * Publishes domain events, retries and dead letters to RabbitMQ.
 */
class AmqpPublisher
{
    public function __construct(
        protected AmqpConnectionManager $connections,
        protected AmqpTopology $topology,
    ) {}

    /**
     * Publish a stored (outbox) message to its exchange.
     *
     * @throws Throwable
     */
    public function publish(DispatchMessageModel $message): void
    {
        $connection = (string) $message->metadata->get('connection', MessagesConfig::defaultConnection());

        $this->connections->run($connection, AmqpConnectionManager::PUBLISH, function (AMQPChannel $channel, array $config) use ($message) {
            $exchange = (string) $message->metadata->get('exchange', $config['exchange']);
            $exchangeType = (string) $message->metadata->get('exchange_type', $config['exchange_type']);
            $routingKey = (string) $message->metadata->get('routing_key', $config['routing_key']);

            if (MessagesConfig::declareTopology()) {
                $this->topology->declareExchange($channel, $exchange, $exchangeType);
            }

            $this->basicPublish($channel, $config, static::makeAMQPMessage($message), $exchange, $routingKey);
        });
    }

    /**
     * Publish a failed message to the delayed retry queue of its connection.
     * Once the delay expires the broker routes it back to the consumer queue.
     *
     * @throws Throwable
     */
    public function retry(ListenMessageModel $message, string $connection): void
    {
        $this->connections->run($connection, AmqpConnectionManager::PUBLISH, function (AMQPChannel $channel, array $config) use ($message) {
            $retryQueue = MessagesConfig::retryQueue($config)
                ?? throw new InvalidConfigurationException("Connection [{$config['name']}] has no retry queue configured.");

            if (MessagesConfig::declareTopology()) {
                $consumerQueue = (string) ($message->metadata->get('queue') ?: $config['queue']);
                $this->topology->declareRetryQueue($channel, $retryQueue, $consumerQueue, MessagesConfig::retryDelay($config));
            }

            $this->basicPublish($channel, $config, static::makeAMQPMessageFromListenedMessage($message), '', $retryQueue);
        });
    }

    /**
     * Publish a message to the dead letter queue of its connection.
     *
     * @throws Throwable
     */
    public function deadLetter(ListenMessageModel $message, string $connection): void
    {
        $this->connections->run($connection, AmqpConnectionManager::PUBLISH, function (AMQPChannel $channel, array $config) use ($message) {
            $deadLetterQueue = MessagesConfig::deadLetterQueue($config)
                ?? throw new InvalidConfigurationException("Connection [{$config['name']}] has no dead letter queue configured.");

            if (MessagesConfig::declareTopology()) {
                $this->topology->declareQueue($channel, $deadLetterQueue);
            }

            $this->basicPublish($channel, $config, static::makeAMQPMessageFromListenedMessage($message), '', $deadLetterQueue);
        });
    }

    public static function makeAMQPMessage(DispatchMessageModel $message): AMQPMessage
    {
        return new AMQPMessage(
            $message->payload->toJson(JSON_THROW_ON_ERROR),
            RabbitMQProperties::toAMQPProperties($message->properties->toArray())
        );
    }

    public static function makeAMQPMessageFromListenedMessage(ListenMessageModel $message): AMQPMessage
    {
        return new AMQPMessage(
            $message->payload->toJson(JSON_THROW_ON_ERROR),
            RabbitMQProperties::fromListenedMessage($message)
        );
    }

    protected function basicPublish(AMQPChannel $channel, array $config, AMQPMessage $message, string $exchange, string $routingKey): void
    {
        $channel->basic_publish($message, $exchange, $routingKey);

        if (Arr::get($config, 'options.publisher_confirms', true)) {
            $channel->wait_for_pending_acks((float) Arr::get($config, 'options.publish_timeout', 5.0));
        }
    }
}
