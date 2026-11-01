<?php

namespace SlothDevGuy\RabbitMQMessages\Amqp;

use Closure;
use Illuminate\Support\Arr;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessagePublishException;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * Owns the AMQP sockets and channels of the current process.
 *
 * - Connections are lazy and shared between library connections that point
 *   to the same hosts/options.
 * - Publishing and consuming use separate connections (as recommended by
 *   RabbitMQ): a publish failure, reconnect or flow-control block never
 *   invalidates the delivery being consumed (its ack/nack still works).
 * - Transient network failures are retried once on a fresh connection.
 */
class AmqpConnectionManager
{
    public const PUBLISH = 'publish';

    public const CONSUME = 'consume';

    /**
     * @var array<string, AbstractConnection>
     */
    protected array $connections = [];

    /**
     * @var array<string, AMQPChannel>
     */
    protected array $channels = [];

    /**
     * @param  Closure(array): AbstractConnection|null  $factory  custom connection factory (testing / advanced setups)
     */
    public function __construct(
        protected ?Closure $factory = null,
    ) {}

    /**
     * Run a callback with a channel, reconnecting once on transient failures.
     *
     * @template T
     *
     * @param  Closure(AMQPChannel, array): T  $callback
     * @return T
     *
     * @throws Throwable
     */
    public function run(string $connection, string $purpose, Closure $callback, int $retries = 1): mixed
    {
        $config = MessagesConfig::connection($connection);
        $attempt = 0;

        while (true) {
            try {
                return $callback($this->channel($config, $purpose), $config);
            } catch (AMQPProtocolChannelException $e) {
                // the broker closed the channel (404, 406 ...): not transient
                $this->forgetChannel($config, $purpose);
                throw $e;
            } catch (AMQPRuntimeException|AMQPIOException|AMQPTimeoutException $e) {
                $this->reset($connection, $purpose);

                if ($attempt++ >= $retries) {
                    throw $e;
                }
            }
        }
    }

    /**
     * Get (or open) the channel used for a given purpose.
     */
    public function channel(array|string $connection, string $purpose = self::PUBLISH): AMQPChannel
    {
        $config = is_array($connection) ? $connection : MessagesConfig::connection($connection);
        $key = $this->channelKey($config, $purpose);

        if (isset($this->channels[$key]) && $this->channels[$key]->is_open()) {
            return $this->channels[$key];
        }

        $channel = $this->connection($config, $purpose)->channel();

        if ($purpose === self::PUBLISH && Arr::get($config, 'options.publisher_confirms', true)) {
            $channel->confirm_select();
            $channel->set_nack_handler(function (AMQPMessage $message) {
                throw new MessagePublishException(
                    'The broker rejected (nack) the message ['.($message->has('message_id') ? $message->get('message_id') : '?').'].'
                );
            });
        }

        return $this->channels[$key] = $channel;
    }

    /**
     * Get (or open) the AMQP connection for a library connection.
     */
    public function connection(array|string $connection, string $purpose = self::PUBLISH): AbstractConnection
    {
        $config = is_array($connection) ? $connection : MessagesConfig::connection($connection);
        $key = $this->connectionKey($config, $purpose);

        if (isset($this->connections[$key]) && $this->connections[$key]->isConnected()) {
            return $this->connections[$key];
        }

        return $this->connections[$key] = $this->connect($config);
    }

    /**
     * Close and forget the connection (and its channels) used by a library
     * connection for one purpose, or for every purpose.
     */
    public function reset(string $connection, ?string $purpose = null): void
    {
        $config = MessagesConfig::connection($connection);
        $purposes = $purpose ? [$purpose] : array_unique([self::PUBLISH, self::CONSUME, ...$this->openPurposes($config)]);

        foreach ($purposes as $purpose) {
            $this->forgetChannel($config, $purpose);
            $key = $this->connectionKey($config, $purpose);

            if (isset($this->connections[$key])) {
                $this->quietly(fn () => $this->connections[$key]->close());
                unset($this->connections[$key]);
            }
        }
    }

    /**
     * Close every channel and connection opened by this process.
     */
    public function disconnect(): void
    {
        foreach ($this->channels as $channel) {
            $this->quietly(fn () => $channel->close());
        }

        foreach ($this->connections as $connection) {
            $this->quietly(fn () => $connection->close());
        }

        $this->channels = [];
        $this->connections = [];
    }

    /**
     * Open a new connection trying every configured host in order.
     *
     * @throws Throwable
     */
    protected function connect(array $config): AbstractConnection
    {
        if ($this->factory) {
            return ($this->factory)($config);
        }

        $last = null;
        foreach ($config['hosts'] as $host) {
            try {
                return AMQPConnectionFactory::create($this->makeConnectionConfig($host, $config['options'] ?? []));
            } catch (Throwable $e) {
                $last = $e;
            }
        }

        throw $last ?? new AMQPIOException("No hosts configured for connection [{$config['name']}].");
    }

    protected function makeConnectionConfig(array $host, array $options): AMQPConnectionConfig
    {
        $config = new AMQPConnectionConfig;
        $config->setHost((string) Arr::get($host, 'host', '127.0.0.1'));
        $config->setPort((int) Arr::get($host, 'port', 5672));
        $config->setUser((string) Arr::get($host, 'user', 'guest'));
        $config->setPassword((string) Arr::get($host, 'password', 'guest'));
        $config->setVhost((string) Arr::get($host, 'vhost', '/'));

        $config->setConnectionTimeout((float) Arr::get($options, 'connection_timeout', 3.0));
        $config->setReadTimeout((float) Arr::get($options, 'read_write_timeout', 3.0));
        $config->setWriteTimeout((float) Arr::get($options, 'read_write_timeout', 3.0));
        $config->setHeartbeat((int) Arr::get($options, 'heartbeat', 0));
        $config->setKeepalive((bool) Arr::get($options, 'keepalive', false));

        if (Arr::get($options, 'ssl.enabled', false)) {
            $config->setIsSecure(true);
            $config->setSslCaCert(Arr::get($options, 'ssl.cafile'));
            $config->setSslCert(Arr::get($options, 'ssl.local_cert'));
            $config->setSslKey(Arr::get($options, 'ssl.local_key'));
            $config->setSslVerify((bool) Arr::get($options, 'ssl.verify_peer', true));
            $config->setSslPassPhrase(Arr::get($options, 'ssl.passphrase'));
        }

        return $config;
    }

    protected function forgetChannel(array $config, string $purpose): void
    {
        $key = $this->channelKey($config, $purpose);

        if (isset($this->channels[$key])) {
            $this->quietly(fn () => $this->channels[$key]->close());
            unset($this->channels[$key]);
        }
    }

    protected function connectionKey(array $config, string $purpose): string
    {
        return md5(serialize([$config['hosts'], Arr::except($config['options'] ?? [], ['publisher_confirms', 'publish_timeout'])])).":$purpose";
    }

    protected function channelKey(array $config, string $purpose): string
    {
        $confirms = $purpose === self::PUBLISH && Arr::get($config, 'options.publisher_confirms', true) ? 'confirm' : 'plain';

        return $this->connectionKey($config, $purpose).":$confirms";
    }

    /**
     * Purposes with an open connection for this configuration (e.g. custom ones).
     *
     * @return array<int, string>
     */
    protected function openPurposes(array $config): array
    {
        $prefix = $this->connectionKey($config, '');
        $purposes = [];

        foreach (array_keys($this->connections) as $key) {
            if (str_starts_with($key, $prefix)) {
                $purposes[] = substr($key, strlen($prefix));
            }
        }

        return $purposes;
    }

    protected function quietly(Closure $callback): void
    {
        try {
            $callback();
        } catch (Throwable) {
            // the socket may already be gone
        }
    }
}
