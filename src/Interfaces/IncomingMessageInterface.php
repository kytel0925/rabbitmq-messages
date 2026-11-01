<?php

namespace SlothDevGuy\RabbitMQMessages\Interfaces;

/**
 * A message delivered by the broker, decoupled from the AMQP client library.
 *
 * Every delivery must be settled exactly once (ack, nack or reject);
 * implementations must ignore subsequent settle calls.
 */
interface IncomingMessageInterface
{
    /**
     * Raw message body (JSON encoded payload).
     */
    public function getBody(): string;

    /**
     * Message properties (app_id, type, message_id, timestamp, application_headers ...).
     * Broker managed "x-" headers are not included.
     */
    public function getProperties(): array;

    /**
     * Exchange the message was delivered through ('' for the default exchange).
     */
    public function getExchange(): string;

    public function getRoutingKey(): string;

    /**
     * Queue the message was consumed from.
     */
    public function getQueue(): string;

    public function getBodySize(): int;

    public function isRedelivered(): bool;

    /**
     * Acknowledge: the message was handled and can be removed from the queue.
     */
    public function ack(): void;

    /**
     * Negative acknowledge (the message is dropped unless $requeue is true).
     */
    public function nack(bool $requeue = false): void;

    /**
     * Reject (the message is dropped unless $requeue is true).
     */
    public function reject(bool $requeue = false): void;

    /**
     * Whether ack/nack/reject was already called.
     */
    public function isSettled(): bool;
}
