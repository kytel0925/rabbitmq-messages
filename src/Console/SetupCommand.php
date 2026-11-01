<?php

namespace SlothDevGuy\RabbitMQMessages\Console;

use Illuminate\Console\Command;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpTopology;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * php artisan rabbitmq-messages:setup [connection ...]
 *
 * Replaces the rabbitmq:exchange-declare / queue-declare / queue-bind
 * commands of vladimir-yuldashev/laravel-queue-rabbitmq.
 */
class SetupCommand extends Command
{
    protected $signature = 'rabbitmq-messages:setup
                            {connection?* : Connections to set up (default: every configured connection)}
                            {--queue= : Override the consumer queue name}';

    protected $description = 'Declare the RabbitMQ exchanges, queues, bindings, retry and dead letter queues used by rabbitmq-messages';

    public function handle(AmqpConnectionManager $connections, AmqpTopology $topology): int
    {
        $names = $this->argument('connection') ?: array_keys(MessagesConfig::connections());
        $status = self::SUCCESS;

        if (empty($names)) {
            $this->components->error('No connections configured in config/rabbitmq-messages.php.');

            return self::FAILURE;
        }

        foreach ($names as $name) {
            try {
                $report = $connections->run($name, AmqpConnectionManager::CONSUME,
                    fn (AMQPChannel $channel, array $config) => $topology->setup($channel, $config, $this->option('queue') ?: null));

                $this->components->info("Connection [$name] is ready.");
                $this->components->bulletList($report);
            } catch (AMQPProtocolChannelException $e) {
                $status = self::FAILURE;
                $this->components->error("Connection [$name]: {$e->getMessage()}");

                if ($e->getCode() === 406) {
                    $this->components->warn(
                        'A queue/exchange already exists with different arguments (PRECONDITION_FAILED). '.
                        'Retry queues created by v1 dead-letter to an exchange; delete them (after draining) or use a new name. See README "Migrating from v1".'
                    );
                }
            } catch (Throwable $e) {
                $status = self::FAILURE;
                $this->components->error("Connection [$name]: ".class_basename($e).": {$e->getMessage()}");
            }
        }

        return $status;
    }
}
