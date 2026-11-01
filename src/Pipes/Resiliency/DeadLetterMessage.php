<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes\Resiliency;

use Illuminate\Container\Container;
use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Pipes\StoreMessage;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;
use Throwable;

class DeadLetterMessage
{
    public function __construct(
        protected ListenMessageModel $listenedMessage,
        protected IncomingMessageInterface $message,
        protected string $connection,
        protected Throwable $ex
    ) {}

    /**
     * @throws Throwable
     */
    public function handle(): void
    {
        $this->listenedMessage->getConnection()->transaction(function () {
            Log::error('MessageListener::deadLetterMessage', [
                'reason' => class_basename($this->ex),
                'message' => $this->ex->getMessage(),
                'properties' => $this->listenedMessage->properties->toArray(),
            ]);

            if (! RabbitMQMessage::canDeadLetterMessages($this->connection)) {
                if (! $this->listenedMessage->exists) {
                    /** @var StoreMessage $storeMessage */
                    $storeMessage = Container::getInstance()->make(StoreMessage::class);
                    $storeMessage->storeMessage($this->listenedMessage);
                }

                $this->listenedMessage->setAsFailed($this->ex);
                $this->listenedMessage->save();
            } else {
                $this->listenedMessage->id && $this->listenedMessage->delete();
                RabbitMQMessage::deadLetterMessage($this->listenedMessage, $this->connection);
            }

            $this->message->reject(false);
        });
    }
}
