<?php

namespace SlothDevGuy\RabbitMQMessages\Support;

use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;

/**
 * Typed access to the `rabbitmq-messages` configuration.
 *
 * Every RabbitMQ setting of the library lives in config/rabbitmq-messages.php;
 * nothing is read from config/queue.php anymore.
 */
class MessagesConfig
{
    public const DRIVER = 'rabbitmq-messages';

    /**
     * Defaults merged into every connection definition.
     */
    protected const CONNECTION_DEFAULTS = [
        'hosts' => [],
        'options' => [
            'connection_timeout' => 3.0,
            'read_write_timeout' => 3.0,
            'heartbeat' => 0,
            'keepalive' => false,
            'publisher_confirms' => true,
            'publish_timeout' => 5.0,
            'ssl' => ['enabled' => false],
        ],
        'exchange' => 'amq.fanout',
        'exchange_type' => 'fanout',
        'routing_key' => '',
        'queue' => 'default',
        'retry' => [
            'queue' => null,
            'delay' => 3000,
        ],
        'dead_letter' => [
            'queue' => null,
        ],
    ];

    public static function defaultConnection(): string
    {
        return (string) Config::get('rabbitmq-messages.default', 'rabbitmq');
    }

    /**
     * @return array<string, array>
     */
    public static function connections(): array
    {
        return (array) Config::get('rabbitmq-messages.connections', []);
    }

    public static function hasConnection(string $name): bool
    {
        return is_array(Config::get("rabbitmq-messages.connections.$name"));
    }

    /**
     * Get a connection definition merged with the defaults.
     *
     * @throws InvalidConfigurationException
     */
    public static function connection(?string $name = null): array
    {
        $name = $name ?: static::defaultConnection();
        $config = Config::get("rabbitmq-messages.connections.$name");

        if (! is_array($config)) {
            throw new InvalidConfigurationException(
                "RabbitMQ messages connection [$name] is not defined in config/rabbitmq-messages.php (connections.$name)."
            );
        }

        $config = array_replace_recursive(static::CONNECTION_DEFAULTS, $config);
        $config['name'] = $name;
        $config['hosts'] = array_values(array_filter((array) $config['hosts'], 'is_array'));

        if (empty($config['hosts'])) {
            throw new InvalidConfigurationException("RabbitMQ messages connection [$name] has no hosts configured.");
        }

        return $config;
    }

    public static function appId(): string
    {
        return (string) Config::get('rabbitmq-messages.app_id', 'laravel');
    }

    public static function maxTries(): int
    {
        return (int) Config::get('rabbitmq-messages.max_tries', 0);
    }

    public static function declareTopology(): bool
    {
        return (bool) Config::get('rabbitmq-messages.declare_topology', true);
    }

    public static function retryQueue(array $connection): ?string
    {
        return static::filled(Arr::get($connection, 'retry.queue'));
    }

    public static function retryDelay(array $connection): int
    {
        return max(0, (int) Arr::get($connection, 'retry.delay', 3000));
    }

    public static function deadLetterQueue(array $connection): ?string
    {
        return static::filled(Arr::get($connection, 'dead_letter.queue'));
    }

    public static function outbox(string $key, mixed $default = null): mixed
    {
        return Config::get("rabbitmq-messages.outbox.$key", $default);
    }

    public static function rotation(string $key, mixed $default = null): mixed
    {
        return Config::get("rabbitmq-messages.rotation.$key", $default);
    }

    protected static function filled(mixed $value): ?string
    {
        return is_string($value) && trim($value) !== '' ? $value : null;
    }
}
