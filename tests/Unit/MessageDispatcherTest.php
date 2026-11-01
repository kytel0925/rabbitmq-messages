<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Unit;

use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Queue;
use SlothDevGuy\RabbitMQMessages\DispatchMessage;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;
use SlothDevGuy\RabbitMQMessages\Jobs\DispatchMessagesJob;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\Enums\DispatchMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;
use SlothDevGuy\RabbitMQMessages\Services\MessageDispatcher;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;

/**
 * Class MessageDispatcherTest
 *
 * @package SlothDevGuy\RabbitMQMessagesTests\Unit
 */
class MessageDispatcherTest extends TestCase
{
    public function testDispatchMessage(): void
    {
        Queue::fake();
        Config::set('rabbitmq-messages.models.dispatch_message', get_class($this->mockDispatchMessageModel()));
        $dispatcher = new MessageDispatcher;
        $message = $dispatcher->dispatchMessage($name = 'foo-event', $payload = collect(['foo-key' => 'foo-value']));
        $this->assertNotNull($uuid = $message->uuid);
        $this->assertEquals($name, $message->name);
        $this->assertEquals(DispatchMessageStatusEnum::CREATED, $message->status);

        $this->assertNotNull($message->properties->get('app_id'));
        $this->assertEquals($name, $message->properties->get('type'));
        $this->assertNull($message->properties->get('timestamp'));
        $this->assertEquals($uuid, $message->properties->get('message_id'));

        $diff = $payload->diff($message->payload);
        $this->assertTrue($diff->isEmpty());
        $this->assertEquals($uuid, $message->payload->get('uuid'));
        $this->assertNull($message->payload->get('fired_at'));

        $this->assertEquals('rabbitmq', $message->metadata->get('connection'));
        Queue::assertPushed(DispatchMessagesJob::class, function (DispatchMessagesJob $job) {
            return $job->queue === 'rabbitmq-messages.dispatch-queue' && $job->afterCommit === true;
        });
    }

    public function testDispatchUsesTheConfiguredOutboxQueue(): void
    {
        Queue::fake();
        Config::set('rabbitmq-messages.models.dispatch_message', get_class($this->mockDispatchMessageModel()));
        Config::set('rabbitmq-messages.outbox.queue_connection', 'redis');
        Config::set('rabbitmq-messages.outbox.queue', 'outbox');

        RabbitMQMessage::dispatchMessage('foo-event');

        Queue::assertPushed(DispatchMessagesJob::class, function (DispatchMessagesJob $job) {
            return $job->connection === 'redis' && $job->queue === 'outbox';
        });
    }

    public function testUnknownConnectionFailsWithAClearMessage(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('connection [missing] is not defined');

        new DispatchMessage('foo-event', collect(), 'missing');
    }

    public function testDispatchMessageFacade(): void
    {
        Queue::fake();
        Config::set('rabbitmq-messages.models.dispatch_message', get_class($this->mockDispatchMessageModel()));

        $message = RabbitMQMessage::dispatchMessage($name = 'foo-event', collect(['foo-key' => 'foo-value']));
        $this->assertNotNull($message->uuid);
        $this->assertEquals($name, $message->name);

        Queue::assertPushed(DispatchMessagesJob::class);
    }

    public function testDispatchMockMessage(): void
    {
        Queue::fake();
        $connection = 'test-connection';
        Config::set('rabbitmq-messages.models.dispatch_message', get_class($this->mockDispatchMessageModel()));
        Config::set("rabbitmq-messages.connections.$connection", config('rabbitmq-messages.connections.rabbitmq'));

        $mockMessage = $this->mockDispatchMessage([], $connection);
        $message = RabbitMQMessage::dispatchMessage($mockMessage);
        $this->assertNotNull($message->uuid);
        $this->assertEquals($mockMessage->getName(), $message->name);
        $this->assertEquals($connection, $message->metadata->get('connection'));

        Queue::assertPushed(DispatchMessagesJob::class);
    }

    /**
     * Mocks an instance of DispatchMessageModel.
     *
     * @return DispatchMessageModel The mocked instance of DispatchMessageModel.
     */
    protected function mockDispatchMessageModel(): DispatchMessageModel
    {
        return new class extends DispatchMessageModel
        {
            public function save(array $options = []) {}
        };
    }

    protected function mockDispatchMessage(array $payload, string $connection): DispatchMessage
    {
        return new class($payload, $connection) extends DispatchMessage
        {
            public function __construct(array $payload, string $connection)
            {
                $name = 'mock-message';
                $payload = collect($payload);
                parent::__construct($name, $payload, $connection);
            }
        };
    }
}
