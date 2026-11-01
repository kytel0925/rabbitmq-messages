<?php

namespace SlothDevGuy\RabbitMQMessages;

use Illuminate\Support\Collection;
use Illuminate\Support\Enumerable;
use PhpAmqpLib\Message\AMQPMessage;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * Class DispatchMessage
 *
 * @package SlothDevGuy\RabbitMQMessages
 */
class DispatchMessage
{
    /**
     * The app_id that is sending this message
     */
    protected string $appID;

    /**
     * The uuid for this message
     */
    protected UuidInterface $uuid;

    /**
     * The message properties and headers
     */
    protected Collection $properties;

    /**
     * The message metadata to be stored
     */
    protected Collection $metadata;

    /**
     * The message configurations
     */
    protected array $configurations;

    /**
     * @param  Enumerable|Collection  $payload
     */
    public function __construct(
        protected string $name,
        protected Enumerable|Collection|null $payload = null,
        protected ?string $connection = null,
    ) {
        $this->connection = $this->connection ?: MessagesConfig::defaultConnection();
        $this->configurations = MessagesConfig::connection($this->connection);

        $this->uuid = static::buildUuid();
        $this->appID = (string) ($this->configurations['app_id'] ?? MessagesConfig::appId());

        $this->buildProperties();
        $this->buildPayload();
        $this->buildMetadata();
    }

    /**
     * Generic getter
     */
    public function getAppID(): string
    {
        return $this->appID;
    }

    /**
     * Generic getter
     */
    public function getUuid(): UuidInterface
    {
        return $this->uuid;
    }

    /**
     * Generic getter
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Get the properties of the RabbitMQ message
     */
    public function getProperties(): Collection
    {
        return $this->properties;
    }

    /**
     * Get the payload of the RabbitMQ message
     */
    public function getPayload(): Collection
    {
        return $this->payload;
    }

    /**
     * Get the metadata of the message
     */
    public function getMetadata(): Collection
    {
        return $this->metadata;
    }

    /**
     * Build properties for RabbitMQ message
     */
    protected function buildProperties(): void
    {
        $this->properties = collect([
            'app_id' => $this->getAppID(),
            'type' => $this->name,
            'timestamp' => null,
            'message_id' => $this->uuid->toString(),
            'content_type' => data_get($this->configurations, 'content_type', 'application/json'),
            'delivery_mode' => data_get($this->configurations, 'delivery_mode', AMQPMessage::DELIVERY_MODE_PERSISTENT),
            // 'priority' => null,
            // 'correlation_id' => null,
        ]);
    }

    /**
     * Build the payload for the RabbitMQ message
     */
    protected function buildPayload(): void
    {
        $payload = [
            'uuid' => $this->uuid->toString(),
            'fired_at' => null,
        ];

        $this->payload = collect($payload)
            ->merge($this->payload)
            ->merge($payload);
    }

    /**
     * Build metadata for RabbitMQ message
     */
    protected function buildMetadata(): void
    {
        $this->metadata = collect([
            'connection' => $this->connection,
            'exchange' => (string) $this->configurations['exchange'],
            'exchange_type' => (string) $this->configurations['exchange_type'],
            'routing_key' => (string) $this->configurations['routing_key'],
        ]);
    }

    /**
     * Build a UUID using version 4
     */
    public static function buildUuid(): UuidInterface
    {
        return Uuid::uuid4();
    }
}
