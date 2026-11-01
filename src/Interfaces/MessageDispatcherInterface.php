<?php

namespace SlothDevGuy\RabbitMQMessages\Interfaces;

use Illuminate\Support\Enumerable;
use SlothDevGuy\RabbitMQMessages\DispatchMessage;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;

/**
 * Interface MessageDispatcherInterface
 *
 * @package SlothDevGuy\RabbitMQMessages\Interfaces
 */
interface MessageDispatcherInterface
{
    /**
     * Dispatch a rabbitmq message
     */
    public function dispatchMessage(DispatchMessage|string $message, ?Enumerable $payload = null, ?string $connection = null): DispatchMessageModel;
}
