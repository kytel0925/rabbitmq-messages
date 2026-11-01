<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes\Resiliency;

use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;
use Throwable;

class RetryMessage
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
            Log::warning('MessageListener::retryMessage', [
                'reason' => class_basename($this->ex),
                'message' => $this->ex->getMessage(),
                'properties' => $this->listenedMessage->properties->toArray(),
            ]);

            $this->listenedMessage->setAsFailed($this->ex);
            $this->listenedMessage->save();

            if (RabbitMQMessage::canRetryMessages($this->connection)) {
                $this->listenedMessage->delete();
                RabbitMQMessage::retryMessage($this->listenedMessage, $this->ex, $this->connection);
            }

            $this->message->nack();
        });
    }
}
