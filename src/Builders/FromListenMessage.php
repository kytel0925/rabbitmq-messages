<?php

namespace SlothDevGuy\RabbitMQMessages\Builders;

use Closure;
use Illuminate\Container\Container;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Str;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageWithoutHandlerException;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageHandlerInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

class FromListenMessage
{
    protected MessageHandlerInterface $handler;

    /**
     * @throws MessageWithoutHandlerException
     * @throws BindingResolutionException
     */
    public function handle(ListenMessageModel $message, Closure $next): mixed
    {
        $this->handler = $this->buildMessageHandler($message);

        return $next($message);
    }

    /**
     * @throws BindingResolutionException
     * @throws MessageWithoutHandlerException
     */
    public function buildMessageHandler(ListenMessageModel $message): MessageHandlerInterface
    {
        $handlers = (array) Config::get('rabbitmq-messages.message_handlers', []);
        $name = (string) $message->name;
        $handler = $handlers[$name] ?? Arr::first(array_filter(
            $handlers,
            fn ($key) => Str::startsWith((string) $key, '/'),
            ARRAY_FILTER_USE_KEY
        ), fn ($handler, $pattern) => (bool) preg_match($pattern, $name));

        if (is_null($handler)) {
            throw new MessageWithoutHandlerException("No handler for message: $message->name");
        }

        return Container::getInstance()->make($handler);
    }

    public function getHandler(): MessageHandlerInterface
    {
        return $this->handler;
    }
}
