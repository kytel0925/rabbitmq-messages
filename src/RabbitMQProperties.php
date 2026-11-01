<?php

namespace SlothDevGuy\RabbitMQMessages;

use Illuminate\Support\Str;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

/**
 * Converts AMQP basic properties from/to plain arrays that can be stored as JSON.
 */
class RabbitMQProperties
{
    /**
     * Broker managed headers (x-death, x-first-death-*, ...) are dropped.
     */
    public static function castAMQPTable(AMQPTable $table): array
    {
        return array_filter($table->getNativeData(), fn ($key) => ! Str::startsWith((string) $key, 'x-'), ARRAY_FILTER_USE_KEY);
    }

    public static function fromAMQPMessage(AMQPMessage $message): array
    {
        $properties = $message->get_properties();
        if (isset($properties['application_headers']) && $properties['application_headers'] instanceof AMQPTable) {
            $properties['application_headers'] = static::castAMQPTable($properties['application_headers']);
        }

        return $properties;
    }

    public static function fromListenedMessage(ListenMessageModel $message): array
    {
        return static::toAMQPProperties($message->properties->toArray());
    }

    /**
     * Prepare a stored properties array to build an AMQPMessage.
     */
    public static function toAMQPProperties(array $properties): array
    {
        if (isset($properties['application_headers']) && is_array($properties['application_headers'])) {
            $properties['application_headers'] = static::castAsAMQPTable($properties['application_headers']);
        }

        return array_filter($properties, fn ($value) => ! is_null($value));
    }

    public static function castAsAMQPTable(array $headers): AMQPTable
    {
        $table = new AMQPTable;
        foreach ($headers as $key => $value) {
            $table->set((string) $key, is_array($value) ? static::castAsAMQPTable($value) : $value);
        }

        return $table;
    }
}
