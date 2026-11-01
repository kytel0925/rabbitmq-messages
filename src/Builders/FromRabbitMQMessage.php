<?php

namespace SlothDevGuy\RabbitMQMessages\Builders;

use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Services\MessageResilient;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * Builds the inbox model (ListenMessageModel) from a broker delivery.
 */
class FromRabbitMQMessage
{
    protected string $exchange;

    protected string $routingKey;

    public function __construct(
        protected IncomingMessageInterface $message,
        protected string $connection,
    ) {
        $headers = (array) ($this->message->getProperties()['application_headers'] ?? []);

        // retried messages come back through the default exchange; the
        // original destination travels in the message headers
        $this->exchange = (string) ($headers[MessageResilient::ORIGIN_EXCHANGE_HEADER] ?? $this->message->getExchange());
        $this->routingKey = (string) ($headers[MessageResilient::ORIGIN_ROUTING_KEY_HEADER] ?? $this->message->getRoutingKey());

        $this->setRealConnection();
    }

    /**
     * @throws Throwable
     */
    public function buildListenMessageModel(ListenMessageModel $listenMessageModel): ListenMessageModel
    {
        try {
            $listenedMessage = new $listenMessageModel;
            $listenedMessage->properties = collect($this->message->getProperties());
            $listenedMessage->payload = collect(json_decode($this->message->getBody(), true, 512, JSON_THROW_ON_ERROR));
            $listenedMessage->metadata = collect([
                'connection' => $this->connection,
                'queue' => $this->message->getQueue(),
                'exchange' => $this->exchange,
                'routing_key' => $this->routingKey,
                'size' => $this->message->getBodySize(),
            ]);
            $listenedMessage->uuid = $listenedMessage->properties->get('message_id');
            $listenedMessage->name = $listenedMessage->properties->get('type');

            return $listenedMessage;
        } catch (Throwable $ex) {
            Log::error("invalid message {$ex->getMessage()}", [
                'properties' => $this->message->getProperties(),
                'body' => $this->message->getBody(),
            ]);
            $this->message->reject(false);
            throw $ex;
        }
    }

    /**
     * The library connection resolved for this delivery.
     */
    public function getConnection(): string
    {
        return $this->connection;
    }

    /**
     * Several connections may feed the same consumer queue (e.g. fanout and
     * topic). Attribute the delivery to the connection whose exchange (and
     * routing key) it came through, so retries and dead letters use that
     * connection's configuration.
     */
    protected function setRealConnection(): void
    {
        $queue = $this->message->getQueue();
        $byExchange = null;

        foreach (MessagesConfig::connections() as $name => $config) {
            if (! is_array($config) || ($config['queue'] ?? null) !== $queue || ($config['exchange'] ?? null) !== $this->exchange) {
                continue;
            }

            if ((string) ($config['routing_key'] ?? '') === $this->routingKey) {
                $this->connection = (string) $name;

                return;
            }

            $byExchange ??= (string) $name;
        }

        $this->connection = $byExchange ?? $this->connection;
    }
}
