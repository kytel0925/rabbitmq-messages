<?php

namespace SlothDevGuy\RabbitMQMessages;

use Illuminate\Console\Events\CommandStarting;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\ServiceProvider;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpPublisher;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpTopology;
use SlothDevGuy\RabbitMQMessages\Console\InstallCommand;
use SlothDevGuy\RabbitMQMessages\Console\QueueCommandGuard;
use SlothDevGuy\RabbitMQMessages\Console\RotateMessagesCommand;
use SlothDevGuy\RabbitMQMessages\Console\SetupCommand;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageDispatcherInterface;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageResilientInterface;
use SlothDevGuy\RabbitMQMessages\Queue\RabbitMQMessagesConnector;
use SlothDevGuy\RabbitMQMessages\Services\FacadeService;
use SlothDevGuy\RabbitMQMessages\Services\MessageDispatcher;
use SlothDevGuy\RabbitMQMessages\Services\MessageResilient;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

class RabbitMQMessagesServiceProvider extends ServiceProvider
{
    /**
     * Queue worker class used by v1 configs (config/queue.php "worker" key).
     */
    protected const LEGACY_WORKER = 'SlothDevGuy\\RabbitMQMessages\\RabbitMQQueue';

    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/rabbitmq-messages.php', 'rabbitmq-messages');

        $this->app->singleton(AmqpConnectionManager::class);
        $this->app->singleton(AmqpTopology::class);
        $this->app->singleton(AmqpPublisher::class);

        $this->app->bind(MessageDispatcherInterface::class, MessageDispatcher::class);
        $this->app->bind(MessageResilientInterface::class, MessageResilient::class);
        $this->app->singleton(RabbitMQMessage::ACCESSOR, FacadeService::class);

        $this->callAfterResolving('queue', function (QueueManager $manager) {
            $manager->addConnector(MessagesConfig::DRIVER, fn () => new RabbitMQMessagesConnector(
                $this->app->make(AmqpConnectionManager::class),
                $this->app->make(AmqpTopology::class),
            ));
        });
    }

    public function boot(): void
    {
        $this->registerQueueConnections();

        $this->app->make(Dispatcher::class)->listen(WorkerStopping::class, function () {
            $this->app->make(AmqpConnectionManager::class)->disconnect();
        });

        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->commands([
            InstallCommand::class,
            SetupCommand::class,
            RotateMessagesCommand::class,
        ]);

        $this->app->make(Dispatcher::class)->listen(CommandStarting::class, [QueueCommandGuard::class, 'handle']);

        $this->scheduleRotation();
        $this->configurePublishing();
    }

    /**
     * Expose every library connection as a Laravel queue connection so it can
     * be consumed with `php artisan queue:work <connection>`.
     */
    protected function registerQueueConnections(): void
    {
        if (! Config::get('rabbitmq-messages.register_queue_connections', true)) {
            return;
        }

        foreach (MessagesConfig::connections() as $name => $connection) {
            $existing = Config::get("queue.connections.$name");

            if (is_array($existing) && ($existing['worker'] ?? null) === static::LEGACY_WORKER) {
                trigger_error(
                    "config/queue.php connection [$name] uses the v1 rabbitmq-messages configuration; it is ignored. ".
                    'Remove it and configure config/rabbitmq-messages.php instead (see README "Migrating from v1").',
                    E_USER_DEPRECATED
                );
                $existing = null;
            }

            if (! is_array($existing)) {
                // "queue" is what queue:work / queue:clear use when --queue is omitted
                Config::set("queue.connections.$name", [
                    'driver' => MessagesConfig::DRIVER,
                    'connection' => $name,
                    'queue' => is_array($connection) ? ($connection['queue'] ?? 'default') : 'default',
                ]);
            }
        }
    }

    /**
     * Register `rabbitmq-messages:rotate` in Laravel's scheduler.
     */
    protected function scheduleRotation(): void
    {
        $this->callAfterResolving(Schedule::class, function (Schedule $schedule) {
            if (! MessagesConfig::rotation('enabled', true) || ! ($cron = MessagesConfig::rotation('schedule'))) {
                return;
            }

            $event = $schedule->command(RotateMessagesCommand::class)
                ->cron((string) $cron)
                ->withoutOverlapping()
                ->description('rabbitmq-messages: rotate old messages');

            if (MessagesConfig::rotation('on_one_server', false)) {
                $event->onOneServer();
            }
        });
    }

    protected function configurePublishing(): void
    {
        $this->publishes([
            __DIR__.'/../database/migrations/2024_05_20_152036_create_rabbitmq_messages_tables.php' => $this->app->databasePath('migrations/2024_05_20_152036_create_rabbitmq_messages_tables.php'),
            __DIR__.'/../database/migrations/2026_11_01_000000_add_rotation_indexes_to_rabbitmq_messages_tables.php' => $this->app->databasePath('migrations/2026_11_01_000000_add_rotation_indexes_to_rabbitmq_messages_tables.php'),
        ], 'rabbitmq-messages-migrations');

        $this->publishes([
            __DIR__.'/../config/rabbitmq-messages.php' => $this->app->configPath('rabbitmq-messages.php'),
        ], 'rabbitmq-messages-config');

        $this->publishes([
            __DIR__.'/../stubs/bin/dispatch-messages.stub' => $this->app->basePath('etc/bin/dispatch-messages.sh'),
            __DIR__.'/../stubs/bin/listen-messages.stub' => $this->app->basePath('etc/bin/listen-messages.sh'),
        ], 'rabbitmq-messages-bin');
    }
}
