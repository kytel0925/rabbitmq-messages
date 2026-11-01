<?php

namespace SlothDevGuy\RabbitMQMessagesTests;

use Illuminate\Foundation\Application;
use Orchestra\Testbench\TestCase as BaseTestCase;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessagesServiceProvider;

abstract class TestCase extends BaseTestCase
{
    protected function getPackageProviders($app): array
    {
        return [
            RabbitMQMessagesServiceProvider::class,
        ];
    }

    /**
     * @param  Application  $app
     */
    protected function defineEnvironment($app): void
    {
        $config = $app['config'];

        $config->set('database.default', 'testing');
        $config->set('rabbitmq-messages.database_connection', 'testing');
        $config->set('queue.default', 'sync');
        $config->set('rabbitmq-messages.app_id', 'rabbitmq-messages-tests');

        $host = [
            'host' => env('RABBITMQ_HOST', '127.0.0.1'),
            'port' => (int) env('RABBITMQ_PORT', 5672),
            'user' => env('RABBITMQ_USERNAME', 'username'),
            'password' => env('RABBITMQ_PASSWORD', 'password'),
            'vhost' => env('RABBITMQ_VHOST', '/'),
        ];

        $config->set('rabbitmq-messages.connections.rabbitmq', array_replace_recursive(
            $config->get('rabbitmq-messages.connections.rabbitmq'),
            [
                'hosts' => [$host],
                'queue' => 'rabbitmq-messages.tests',
                'retry' => ['queue' => 'rabbitmq-messages.tests.retry', 'delay' => 1000],
                'dead_letter' => ['queue' => 'rabbitmq-messages.tests.dead-letter'],
            ]
        ));

        $config->set('rabbitmq-messages.connections.rabbitmq_topic', array_replace_recursive(
            $config->get('rabbitmq-messages.connections.rabbitmq_topic'),
            [
                'hosts' => [$host],
                'routing_key' => 'rabbitmq-messages.tests',
                'queue' => 'rabbitmq-messages.tests',
                'retry' => ['queue' => 'rabbitmq-messages.tests.retry', 'delay' => 1000],
                'dead_letter' => ['queue' => 'rabbitmq-messages.tests.dead-letter'],
            ]
        ));
    }

    protected function defineDatabaseMigrations(): void
    {
        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');
    }
}
