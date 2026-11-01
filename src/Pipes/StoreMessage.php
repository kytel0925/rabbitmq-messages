<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes;

use Closure;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageAlreadyRegisterException;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

class StoreMessage
{
    /**
     * @throws MessageAlreadyRegisterException
     */
    public function handle(ListenMessageModel $message, Closure $next): mixed
    {
        $this->storeMessage($message);

        return $next($message);
    }

    /**
     * @throws MessageAlreadyRegisterException
     */
    public function storeMessage(ListenMessageModel $message): ListenMessageModel
    {
        VerifyMessageAlreadyRegister::assert($message);
        $message->setAsQueued();
        $message->save();

        return $message;
    }
}
