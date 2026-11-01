<?php

namespace SlothDevGuy\RabbitMQMessages\Amqp;

use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\RabbitMQProperties;

/**
 * IncomingMessageInterface backed by a php-amqplib message.
 */
class AmqpIncomingMessage implements IncomingMessageInterface
{
    protected bool $settled = false;

    public function __construct(
        protected AMQPMessage $message,
        protected string $queue,
    ) {}

    public function getAMQPMessage(): AMQPMessage
    {
        return $this->message;
    }

    public function getBody(): string
    {
        return (string) $this->message->getBody();
    }

    public function getProperties(): array
    {
        return RabbitMQProperties::fromAMQPMessage($this->message);
    }

    public function getExchange(): string
    {
        return (string) $this->message->getExchange();
    }

    public function getRoutingKey(): string
    {
        return (string) $this->message->getRoutingKey();
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function getBodySize(): int
    {
        return (int) $this->message->getBodySize();
    }

    public function isRedelivered(): bool
    {
        return (bool) $this->message->isRedelivered();
    }

    public function getDeliveryTag(): ?int
    {
        return $this->message->getDeliveryTag();
    }

    public function ack(): void
    {
        $this->settle(fn () => $this->message->ack());
    }

    public function nack(bool $requeue = false): void
    {
        $this->settle(fn () => $this->message->nack($requeue));
    }

    public function reject(bool $requeue = false): void
    {
        $this->settle(fn () => $this->message->reject($requeue));
    }

    public function isSettled(): bool
    {
        return $this->settled;
    }

    protected function settle(callable $callback): void
    {
        if ($this->settled) {
            return;
        }

        $callback();
        $this->settled = true;
    }
}
