<?php

namespace SlothDevGuy\RabbitMQMessages\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Container\Container;
use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpPublisher;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * Publishes one outbox message (dispatch_message row) to RabbitMQ.
 *
 * Runs on a regular Laravel queue connection (database, redis, sqs ...).
 * A failed publish marks the row as "failed" and re-throws so Laravel's
 * worker applies --tries/--backoff and, finally, records it in failed_jobs
 * (`php artisan queue:retry` re-publishes it).
 *
 * Delivery is at-least-once: if the row cannot be saved after a successful
 * publish the job is retried and consumers discard the duplicate by uuid.
 */
class DispatchMessagesJob implements ShouldQueue
{
    use InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(
        protected DispatchMessageModel $dispatchEvent,
    ) {
        if ($connection = MessagesConfig::outbox('queue_connection')) {
            $this->onConnection($connection);
        }

        $this->onQueue(MessagesConfig::outbox('queue', 'rabbitmq-messages.dispatch-queue'));

        if (MessagesConfig::outbox('after_commit', true)) {
            $this->afterCommit();
        }
    }

    /**
     * Dispatch the job with the given arguments (kept for backward compatibility).
     */
    public static function dispatch(DispatchMessageModel $dispatchEvent): mixed
    {
        return Container::getInstance()->make(Dispatcher::class)->dispatch(new static($dispatchEvent));
    }

    /**
     * @throws Throwable
     */
    public function handle(?AmqpPublisher $publisher = null): void
    {
        $publisher ??= Container::getInstance()->make(AmqpPublisher::class);

        try {
            $this->dispatchEvent->setDispatchedAt();
            $publisher->publish($this->dispatchEvent);
            $this->dispatchEvent->save();

            Log::info('event-published', $this->dispatchEvent->properties->toArray());
        } catch (Throwable $e) {
            Log::error("event-publish failed: {$e->getMessage()}", $this->dispatchEvent->properties->toArray());

            $this->dispatchEvent->setFailedAt($e);
            $this->dispatchEvent->save();

            throw $e;
        }
    }

    public function getDispatchMessage(): DispatchMessageModel
    {
        return $this->dispatchEvent;
    }
}
