<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Feature;

use Illuminate\Foundation\Testing\RefreshDatabase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;

/**
 * Tests that talk to a real RabbitMQ broker. They are skipped when the broker
 * configured by RABBITMQ_HOST/RABBITMQ_PORT is not reachable.
 */
abstract class FeatureTestCase extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        $host = config('rabbitmq-messages.connections.rabbitmq.hosts.0.host');
        $port = config('rabbitmq-messages.connections.rabbitmq.hosts.0.port');
        $socket = @fsockopen($host, (int) $port, $errno, $error, 1);

        if (! $socket) {
            $this->markTestSkipped("RabbitMQ is not reachable at $host:$port ($error)");
        }

        fclose($socket);

        $this->artisan('rabbitmq-messages:setup')->assertSuccessful();

        foreach ($this->testQueues() as $queue) {
            $this->channel()->queue_purge($queue);
        }
    }

    protected function tearDown(): void
    {
        if ($this->app) {
            $this->app->make(AmqpConnectionManager::class)->disconnect();
        }

        parent::tearDown();
    }

    protected function testQueues(): array
    {
        return [
            config('rabbitmq-messages.connections.rabbitmq.queue'),
            config('rabbitmq-messages.connections.rabbitmq.retry.queue'),
            config('rabbitmq-messages.connections.rabbitmq.dead_letter.queue'),
        ];
    }

    protected function channel(string $connection = 'rabbitmq'): AMQPChannel
    {
        return $this->app->make(AmqpConnectionManager::class)->channel($connection, 'tests');
    }

    protected function getMessage(string $queue): ?AMQPMessage
    {
        return $this->channel()->basic_get($queue, true);
    }

    protected function assertQueueIsEmpty(string $queue): void
    {
        $this->assertNull($this->getMessage($queue), "Queue [$queue] is not empty");
    }

    protected function assertQueueIsNotEmpty(string $queue): void
    {
        $this->assertNotNull($this->getMessage($queue), "Queue [$queue] is empty");
    }

    /**
     * Consume a single delivery exactly as production does.
     */
    protected function consumeOnce(string $connection = 'rabbitmq'): void
    {
        $this->artisan('queue:work', [
            'connection' => $connection,
            '--queue' => config("rabbitmq-messages.connections.$connection.queue"),
            '--once' => true,
            '--sleep' => 0,
        ])->assertSuccessful();
    }

    protected static function makeFakePayload(): array
    {
        return [
            'name' => fake()->name,
            'last_name' => fake()->lastName,
            'birth_date' => fake()->date,
            'notes' => fake()->paragraph,
        ];
    }
}
