<?php

namespace SlothDevGuy\RabbitMQMessages\Exceptions;

use LogicException;

/**
 * Thrown when a Laravel queue operation (or artisan queue:* command) is used
 * against a "rabbitmq-messages" connection but has no meaning for it.
 */
class UnsupportedOperationException extends LogicException
{
    public static function queueOperation(string $operation, string $connection, string $hint = ''): static
    {
        $message = "[$operation] is not supported by the rabbitmq-messages driver (connection [$connection]).";

        return new static(trim("$message $hint"));
    }
}
