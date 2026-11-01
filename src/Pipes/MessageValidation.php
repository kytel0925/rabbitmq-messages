<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes;

use Closure;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\ValidationException;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidRabbitMQMessageException;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use Throwable;

class MessageValidation
{
    /**
     * @throws Throwable
     */
    public function handle(ListenMessageModel $message, Closure $next): mixed
    {
        try {
            Validator::make([
                'properties' => $message->properties?->toArray(),
                'payload' => $message->payload?->toArray(),
                'metadata' => $message->metadata?->toArray(),
            ], static::rules())->validate();
        } catch (ValidationException $ex) {
            throw new InvalidRabbitMQMessageException($ex->getMessage(), $ex->getCode(), $ex);
        }

        return $next($message);
    }

    public static function rules(): array
    {
        return [
            'properties' => ['required', 'array'],
            'properties.app_id' => ['required', 'string'],
            'properties.message_id' => ['required', 'string', 'uuid'],
            'properties.type' => ['required', 'string', 'min:5', 'max:255'],
            'payload' => ['present', 'array'],
            'metadata' => ['present', 'array'],
        ];
    }
}
