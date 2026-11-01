<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Feature;

use Illuminate\Support\Facades\Config;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPIOException;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpConnectionManager;
use SlothDevGuy\RabbitMQMessages\Models\Enums\DispatchMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;

class DispatchMessageTest extends FeatureTestCase
{
    public function testMessageDispatch(): void
    {
        $message = RabbitMQMessage::dispatchMessage('foo-event', collect(['foo-key' => 'foo-value']));

        $fresh = $message->fresh();
        $this->assertEquals(DispatchMessageStatusEnum::DISPATCHED, $fresh->status);
        $this->assertNotNull($fresh->payload->get('fired_at'));
        $this->assertNotNull($fresh->properties->get('timestamp'));

        $published = $this->getMessage(config('rabbitmq-messages.connections.rabbitmq.queue'));
        $this->assertNotNull($published);
        $this->assertEquals($message->uuid, $published->get('message_id'));
        $this->assertEquals('foo-event', $published->get('type'));
        $this->assertEquals('rabbitmq-messages-tests', $published->get('app_id'));
        $this->assertEquals('foo-value', json_decode($published->getBody(), true)['foo-key']);
    }

    public function testTopicMessageDispatch(): void
    {
        RabbitMQMessage::dispatchMessage('foo-event', collect(), 'rabbitmq_topic');

        $published = $this->getMessage(config('rabbitmq-messages.connections.rabbitmq_topic.queue'));
        $this->assertNotNull($published);
        $this->assertEquals('amq.topic', $published->getExchange());
        $this->assertEquals('rabbitmq-messages.tests', $published->getRoutingKey());
    }

    public function testMessageDispatchFailedInvalidHost(): void
    {
        $this->expectException(AMQPIOException::class);
        $this->setInvalidConnection(['host' => 'invalid-host.invalid']);

        try {
            RabbitMQMessage::dispatchMessage('foo-event', collect(['foo-key' => 'foo-value']), 'invalid');
        } finally {
            $this->assertFailedDispatch();
        }
    }

    public function testMessageDispatchFailedInvalidUsernameAndPassword(): void
    {
        $this->expectException(AMQPConnectionClosedException::class);
        $this->setInvalidConnection([
            'user' => 'invalid-username',
            'password' => 'invalid-password',
        ]);

        try {
            RabbitMQMessage::dispatchMessage('foo-event', collect(['foo-key' => 'foo-value']), 'invalid');
        } finally {
            $this->assertFailedDispatch();
        }
    }

    public function testPublishingAndConsumingUseSeparateConnections(): void
    {
        $manager = $this->app->make(AmqpConnectionManager::class);
        $consume = $manager->channel('rabbitmq', AmqpConnectionManager::CONSUME);
        $publish = $manager->channel('rabbitmq', AmqpConnectionManager::PUBLISH);

        $this->assertNotSame($consume->getConnection(), $publish->getConnection());
        // connections pointing to the same broker share the socket
        $this->assertSame($consume, $manager->channel('rabbitmq_topic', AmqpConnectionManager::CONSUME));

        $manager->reset('rabbitmq', AmqpConnectionManager::PUBLISH);
        $this->assertTrue($consume->is_open(), 'a publisher reconnect must not close the consumer channel');
        $this->assertFalse($publish->is_open());

        $custom = $this->channel();
        $manager->reset('rabbitmq');
        $this->assertFalse($consume->is_open());
        $this->assertFalse($custom->is_open(), 'custom purposes are reset too');
    }

    protected function setInvalidConnection(array $host): void
    {
        $connection = config('rabbitmq-messages.connections.rabbitmq');
        $connection['hosts'][0] = array_merge($connection['hosts'][0], $host);
        $connection['options']['connection_timeout'] = 1.0;

        Config::set('rabbitmq-messages.connections.invalid', $connection);
    }

    protected function assertFailedDispatch(): void
    {
        $this->assertDatabaseCount('dispatch_message', 1);
        $this->assertDatabaseHas('dispatch_message', [
            'status' => DispatchMessageStatusEnum::FAILED,
        ]);
    }
}
