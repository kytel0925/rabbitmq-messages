<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Support;

use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;

/**
 * In-memory delivery used by unit tests.
 */
class FakeIncomingMessage implements IncomingMessageInterface
{
    /**
     * @var array<int, array{0: string, 1: bool}>
     */
    public array $settlements = [];

    public function __construct(
        public array $properties,
        public string $body,
        public string $queue = 'foo-queue',
        public string $exchange = 'amq.fanout',
        public string $routingKey = '',
        public bool $redelivered = false,
    ) {}

    public function getBody(): string
    {
        return $this->body;
    }

    public function getProperties(): array
    {
        return $this->properties;
    }

    public function getExchange(): string
    {
        return $this->exchange;
    }

    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function getBodySize(): int
    {
        return strlen($this->body);
    }

    public function isRedelivered(): bool
    {
        return $this->redelivered;
    }

    public function ack(): void
    {
        $this->settle('ack', false);
    }

    public function nack(bool $requeue = false): void
    {
        $this->settle('nack', $requeue);
    }

    public function reject(bool $requeue = false): void
    {
        $this->settle('reject', $requeue);
    }

    public function isSettled(): bool
    {
        return ! empty($this->settlements);
    }

    public function settlement(): ?string
    {
        return $this->settlements[0][0] ?? null;
    }

    protected function settle(string $type, bool $requeue): void
    {
        if (! $this->isSettled()) {
            $this->settlements[] = [$type, $requeue];
        }
    }
}
