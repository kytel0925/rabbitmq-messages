<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes;

use Closure;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageAlreadyRegisterException;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

class VerifyMessageAlreadyRegister
{
    /**
     * @throws MessageAlreadyRegisterException
     */
    public function handle(ListenMessageModel $message, Closure $next): mixed
    {
        static::assert($message);

        return $next($message);
    }

    /**
     * @throws MessageAlreadyRegisterException
     */
    public static function assert(ListenMessageModel $message): void
    {
        if ($exists = $message::findByUuid($message->uuid)) {
            throw new MessageAlreadyRegisterException("Message already registered[$exists->id:$exists->uuid]");
        }
    }
}
