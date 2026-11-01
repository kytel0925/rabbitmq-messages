<?php

namespace SlothDevGuy\RabbitMQMessages\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpTopology;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * Builds the "rabbitmq-messages" queue driver.
 *
 * config/queue.php entry (registered automatically, see register_queue_connections):
 *
 *   'rabbitmq' => [
 *       'driver' => 'rabbitmq-messages',
 *       'connection' => 'rabbitmq',   // key in config('rabbitmq-messages.connections')
 *       'queue' => null,              // optional: overrides the connection's consumer queue
 *   ],
 */
class RabbitMQMessagesConnector implements ConnectorInterface
{
    public function __construct(
        protected AmqpConnectionManager $connections,
        protected AmqpTopology $topology,
    ) {}

    public function connect(array $config)
    {
        $name = $config['connection'] ?? MessagesConfig::defaultConnection();

        // fail fast with a clear message when the connection is not configured
        MessagesConfig::connection($name);

        return new RabbitMQMessagesQueue($this->connections, $this->topology, $name, $config['queue'] ?? null);
    }
}
