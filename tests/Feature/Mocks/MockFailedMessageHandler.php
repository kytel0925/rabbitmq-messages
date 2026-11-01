<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Feature\Mocks;

use Exception;
use Illuminate\Support\Collection;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageHandlerInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

class MockFailedMessageHandler implements MessageHandlerInterface
{
    /**
     * Number of calls that fail before the handler succeeds (-1 = always fail).
     */
    public static int $failures = -1;

    /**
     * @throws Exception
     */
    public function handle(Collection $payload, ListenMessageModel $message): void
    {
        if (static::$failures === 0) {
            return;
        }

        static::$failures > 0 && static::$failures--;
        $name = fake()->name;

        throw new Exception("failed message handler, guilty one: $name");
    }
}
