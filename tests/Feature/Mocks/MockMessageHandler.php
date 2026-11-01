<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Feature\Mocks;

use Illuminate\Support\Collection;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageHandlerInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

class MockMessageHandler implements MessageHandlerInterface
{
    public static int $handled = 0;

    public function handle(Collection $payload, ListenMessageModel $message): void
    {
        static::$handled++;

        logger()->info('message handle', [
            'name' => $message->name,
            'uuid' => $message->uuid,
        ]);
    }
}
