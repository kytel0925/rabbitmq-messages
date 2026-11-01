<?php

namespace SlothDevGuy\RabbitMQMessages\Services;

use Illuminate\Container\Container;
use Illuminate\Support\Enumerable;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\DispatchMessage;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageDispatcherInterface;
use SlothDevGuy\RabbitMQMessages\Jobs\DispatchMessagesJob;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\Enums\DispatchMessageStatusEnum;

/**
 * Dispatch side entry point (the transactional outbox):
 *
 *   1. the message is stored in dispatch_message (status "created"), inside the
 *      caller's database transaction if there is one;
 *   2. a DispatchMessagesJob is queued (after commit) on a regular Laravel queue;
 *   3. the job publishes it to RabbitMQ and marks it "dispatched" (or "failed").
 */
class MessageDispatcher implements MessageDispatcherInterface
{
    protected DispatchMessageModel $dispatchMessage;

    public function __construct(
        protected ?string $connection = null,
    ) {
        $class = Config::get('rabbitmq-messages.models.dispatch_message', DispatchMessageModel::class);
        $this->dispatchMessage = Container::getInstance()->make($class);
    }

    /**
     * {@inheritdoc}
     */
    public function dispatchMessage(DispatchMessage|string $message, ?Enumerable $payload = null, ?string $connection = null): DispatchMessageModel
    {
        if (is_string($message)) {
            $message = new DispatchMessage($message, $payload, $connection ?: $this->connection);
        }

        /** @var DispatchMessageModel $dispatchMessage */
        $dispatchMessage = new $this->dispatchMessage;
        $dispatchMessage->uuid = $message->getUuid()->toString();
        $dispatchMessage->name = $message->getName();
        $dispatchMessage->status = DispatchMessageStatusEnum::CREATED;
        $dispatchMessage->properties = $message->getProperties();
        $dispatchMessage->payload = $message->getPayload();
        $dispatchMessage->metadata = $message->getMetadata();
        $dispatchMessage->save();

        DispatchMessagesJob::dispatch($dispatchMessage);

        return $dispatchMessage;
    }
}
