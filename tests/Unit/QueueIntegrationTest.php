<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Unit;

use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Queue;
use PHPUnit\Framework\Attributes\DataProvider;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;
use SlothDevGuy\RabbitMQMessages\Exceptions\UnsupportedOperationException;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Pipes\MessageListener;
use SlothDevGuy\RabbitMQMessages\Queue\Jobs\RabbitMQMessagesJob;
use SlothDevGuy\RabbitMQMessages\Queue\RabbitMQMessagesQueue;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessagesServiceProvider;
use SlothDevGuy\RabbitMQMessagesTests\Support\FakeIncomingMessage;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;

class QueueIntegrationTest extends TestCase
{
    public function testEveryConnectionIsRegisteredAsAQueueConnection(): void
    {
        $this->assertEquals(
            ['driver' => 'rabbitmq-messages', 'connection' => 'rabbitmq', 'queue' => 'rabbitmq-messages.tests'],
            config('queue.connections.rabbitmq')
        );
        $this->assertEquals('rabbitmq-messages', config('queue.connections.rabbitmq_topic.driver'));

        $queue = Queue::connection('rabbitmq');
        $this->assertInstanceOf(RabbitMQMessagesQueue::class, $queue);
        $this->assertEquals('rabbitmq', $queue->getMessagesConnection());
        $this->assertEquals('rabbitmq-messages.tests', $queue->getQueue());
    }

    public function testUndefinedConnectionFailsWithAClearMessage(): void
    {
        Config::set('queue.connections.broken', ['driver' => 'rabbitmq-messages', 'connection' => 'missing']);

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('connection [missing] is not defined');

        Queue::connection('broken');
    }

    public static function unsupportedOperations(): array
    {
        return [
            'push' => [fn (RabbitMQMessagesQueue $queue) => $queue->push('SomeJob'), 'push'],
            'pushRaw' => [fn (RabbitMQMessagesQueue $queue) => $queue->pushRaw('{}'), 'pushRaw'],
            'later' => [fn (RabbitMQMessagesQueue $queue) => $queue->later(10, 'SomeJob'), 'later'],
            'bulk' => [fn (RabbitMQMessagesQueue $queue) => $queue->bulk(['SomeJob']), 'bulk'],
            'pushOn' => [fn (RabbitMQMessagesQueue $queue) => $queue->pushOn('foo', 'SomeJob'), 'push'],
        ];
    }

    #[DataProvider('unsupportedOperations')]
    public function testPushingLaravelJobsIsNotSupported(\Closure $operation, string $name): void
    {
        $this->expectException(UnsupportedOperationException::class);
        $this->expectExceptionMessage("[$name] is not supported by the rabbitmq-messages driver (connection [rabbitmq])");

        $operation(Queue::connection('rabbitmq'));
    }

    public function testJobExposesTheDomainEvent(): void
    {
        $message = new FakeIncomingMessage([
            'type' => 'billing.invoice.paid',
            'message_id' => 'c0a8e7d2-1f1b-4c3e-9a55-3f6e0d2b9a10',
            'application_headers' => ['tries' => 2],
        ], '{"uuid":"c0a8e7d2-1f1b-4c3e-9a55-3f6e0d2b9a10","amount":10}');

        $job = new RabbitMQMessagesJob($this->app, $message, 'rabbitmq', 'rabbitmq', 'billing');

        $this->assertEquals('billing.invoice.paid', $job->resolveName());
        $this->assertEquals('c0a8e7d2-1f1b-4c3e-9a55-3f6e0d2b9a10', $job->getJobId());
        $this->assertEquals('c0a8e7d2-1f1b-4c3e-9a55-3f6e0d2b9a10', $job->uuid());
        $this->assertEquals(3, $job->attempts());
        $this->assertSame(0, $job->maxTries(), 'Laravel must never fail deliveries because of --tries');
        $this->assertEquals(10, $job->payload()['amount']);
        $this->assertEquals('billing', $job->getQueue());
    }

    public function testJobDeleteAcknowledgesAndReleaseRequeues(): void
    {
        $acked = new FakeIncomingMessage(['type' => 'foo'], '{}');
        (new RabbitMQMessagesJob($this->app, $acked, 'rabbitmq', 'rabbitmq', 'q'))->delete();
        $this->assertEquals([['ack', false]], $acked->settlements);

        $released = new FakeIncomingMessage(['type' => 'foo'], '{}');
        $job = new RabbitMQMessagesJob($this->app, $released, 'rabbitmq', 'rabbitmq', 'q');
        $job->release(30);
        $job->delete(); // already settled: no second ack
        $this->assertEquals([['reject', true]], $released->settlements);
        $this->assertTrue($job->isReleased());
    }

    public function testJobRethrowsWhenTheDeliveryCouldNotBeSettled(): void
    {
        $this->app->bind(MessageListener::class, fn () => new class extends MessageListener
        {
            public function sendMessageThroughPipes(IncomingMessageInterface $message, string $connection): ListenMessageModel
            {
                throw new \RuntimeException('database is down');
            }
        });

        $message = new FakeIncomingMessage(['type' => 'foo'], '{}');
        $job = new RabbitMQMessagesJob($this->app, $message, 'rabbitmq', 'rabbitmq', 'q');

        try {
            $job->fire();
            $this->fail('An unsettled delivery must be reported to the worker');
        } catch (\RuntimeException $e) {
            $this->assertEquals('database is down', $e->getMessage());
            $this->assertFalse($job->isDeleted());
        }
    }

    public function testLegacyQueueConfigurationIsReplaced(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'worker' => 'SlothDevGuy\\RabbitMQMessages\\RabbitMQQueue',
        ]);

        $deprecations = [];
        set_error_handler(function (int $level, string $message) use (&$deprecations) {
            $deprecations[] = $message;

            return true;
        }, E_USER_DEPRECATED);

        try {
            (new RabbitMQMessagesServiceProvider($this->app))->boot();
        } finally {
            restore_error_handler();
        }

        $this->assertEquals('rabbitmq-messages', config('queue.connections.rabbitmq.driver'));
        $this->assertStringContainsString('v1 rabbitmq-messages configuration', $deprecations[0] ?? '');
    }
}
