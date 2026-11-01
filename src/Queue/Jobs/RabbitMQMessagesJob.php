<?php

namespace SlothDevGuy\RabbitMQMessages\Queue\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Pipes\MessageListener;
use SlothDevGuy\RabbitMQMessages\Services\MessageResilient;
use Throwable;

/**
 * A domain event consumed by `php artisan queue:work <connection>`.
 *
 * Retries, dead-lettering and idempotency are handled by the library's
 * listener pipeline, NOT by Laravel's worker: maxTries() is 0 so --tries,
 * --backoff and the failed_jobs table never apply to these deliveries.
 */
class RabbitMQMessagesJob extends Job implements JobContract
{
    public function __construct(
        Container $container,
        protected IncomingMessageInterface $message,
        string $connectionName,
        protected string $messagesConnection,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    /**
     * Send the delivery through the listener pipeline (validation,
     * idempotency, handler, retry / dead letter).
     *
     * @throws Throwable
     */
    public function fire(): void
    {
        try {
            /** @var MessageListener $listener */
            $listener = $this->container->make(MessageListener::class);
            $listener->sendMessageThroughPipes($this->message, $this->messagesConnection);
        } catch (Throwable $e) {
            if (! $this->message->isSettled()) {
                // infrastructure failure (database / broker down) before the
                // delivery could be settled: let the worker report it and
                // release (requeue) the message
                throw $e;
            }

            // already handled by the resiliency pipeline (skipped, retried or dead-lettered)
            Log::debug('rabbitmq-messages: message handled by the resiliency pipeline', [
                'message_id' => $this->getJobId(),
                'reason' => class_basename($e),
            ]);
        } finally {
            if ($this->message->isSettled()) {
                $this->deleted = true;
            }
        }
    }

    /**
     * Acknowledge the delivery (no-op when it was already settled).
     */
    public function delete(): void
    {
        parent::delete();

        $this->message->ack();
    }

    /**
     * Requeue the delivery. AMQP has no per-message delay, so $delay is ignored;
     * delayed retries are implemented by the retry queue instead.
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->message->reject(true);
    }

    /**
     * Only reachable through Laravel's worker internals (e.g. a timeout); the
     * delivery is acknowledged by fail() -> delete(), nothing else to run.
     */
    protected function failed($e)
    {
        Log::warning('rabbitmq-messages: delivery failed by the queue worker', [
            'message_id' => $this->getJobId(),
            'name' => $this->getName(),
            'reason' => $e ? $e->getMessage() : null,
        ]);
    }

    public function getJobId()
    {
        return $this->property('message_id');
    }

    public function uuid()
    {
        return $this->property('message_id');
    }

    public function getRawBody()
    {
        return $this->message->getBody();
    }

    public function payload()
    {
        return json_decode($this->getRawBody(), true) ?: [];
    }

    /**
     * Number of retries already performed by the resiliency pipeline + 1.
     */
    public function attempts()
    {
        return (int) data_get($this->message->getProperties(), MessageResilient::getTriesKey(), 0) + 1;
    }

    public function getName()
    {
        return (string) ($this->property('type') ?? 'unknown-message');
    }

    public function resolveName()
    {
        return $this->getName();
    }

    public function resolveQueuedJobClass()
    {
        return $this->getName();
    }

    /**
     * 0 = never let Laravel's worker fail the delivery because of --tries.
     */
    public function maxTries()
    {
        return 0;
    }

    public function maxExceptions()
    {
        return null;
    }

    public function backoff()
    {
        return null;
    }

    public function retryUntil()
    {
        return null;
    }

    public function timeout()
    {
        return null;
    }

    public function shouldFailOnTimeout()
    {
        return false;
    }

    public function getIncomingMessage(): IncomingMessageInterface
    {
        return $this->message;
    }

    public function getMessagesConnection(): string
    {
        return $this->messagesConnection;
    }

    protected function property(string $key): mixed
    {
        return $this->message->getProperties()[$key] ?? null;
    }
}
