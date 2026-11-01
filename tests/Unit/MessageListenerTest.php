<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Unit;

use Illuminate\Database\QueryException;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Config;
use PhpAmqpLib\Message\AMQPMessage;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidRabbitMQMessageException;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageAlreadyRegisterException;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageWithoutHandlerException;
use SlothDevGuy\RabbitMQMessages\Models\Enums\ListenMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Pipes\MessageListener;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;
use SlothDevGuy\RabbitMQMessages\Services\MessageResilient;
use SlothDevGuy\RabbitMQMessagesTests\Support\FakeIncomingMessage;
use SlothDevGuy\RabbitMQMessagesTests\Support\MockMessageHandler;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;
use Throwable;

class MessageListenerTest extends TestCase
{
    /**
     * @throws Throwable
     */
    public function testListenMessage(): void
    {
        $message = $this->fakeMessage($properties = $this->validProperties());

        Config::set('rabbitmq-messages.message_handlers', [
            $properties['type'] => MockMessageHandler::class,
        ]);

        $this->app->bind(ListenMessageModel::class, fn () => MockMessageHandler::mockListenMessageModel());
        $listened = (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');

        $this->assertEquals($properties['app_id'], $listened->properties->get('app_id'));
        $this->assertEquals($properties['message_id'], $listened->uuid);
        $this->assertEquals($properties['type'], $listened->name);
        $this->assertEquals(ListenMessageStatusEnum::PROCESSED, $listened->status);
        $this->assertEquals(MockMessageHandler::class, $listened->metadata->get('handler'));
        $this->assertNotNull($listened->properties->get('timestamp'));
        $this->assertEquals('ack', $message->settlement());
    }

    /**
     * @throws Throwable
     */
    public function testMessageAlreadyRegisterException(): void
    {
        $message = $this->fakeMessage($properties = $this->validProperties());

        Config::set('rabbitmq-messages.message_handlers', [
            $properties['type'] => MockMessageHandler::class,
        ]);

        $mock = MockMessageHandler::mockListenMessageModel();
        $mock::$findReturn = new $mock;
        $this->app->bind(ListenMessageModel::class, fn () => $mock);

        try {
            (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
            $this->fail('A duplicated message must be skipped');
        } catch (MessageAlreadyRegisterException) {
            // a duplicated delivery is acknowledged and discarded (idempotency)
            $this->assertEquals('ack', $message->settlement());
        } finally {
            $mock::$findReturn = null;
        }
    }

    public function testMessageWithoutHandlerIsSkipped(): void
    {
        $message = $this->fakeMessage($this->validProperties());
        Config::set('rabbitmq-messages.message_handlers', []);
        $this->app->bind(ListenMessageModel::class, fn () => MockMessageHandler::mockListenMessageModel());

        $this->expectException(MessageWithoutHandlerException::class);

        try {
            (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
        } finally {
            $this->assertEquals('ack', $message->settlement());
        }
    }

    public function testInvalidMessageIsSkipped(): void
    {
        $properties = $this->validProperties();
        unset($properties['message_id']);
        $message = $this->fakeMessage($properties);
        $this->app->bind(ListenMessageModel::class, fn () => MockMessageHandler::mockListenMessageModel());

        $this->expectException(InvalidRabbitMQMessageException::class);

        try {
            (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
        } finally {
            $this->assertEquals('ack', $message->settlement());
        }
    }

    public function testUndecodableBodyIsRejected(): void
    {
        $message = new FakeIncomingMessage($this->validProperties(), '{invalid json');

        try {
            (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
            $this->fail('An undecodable body must be rejected');
        } catch (\JsonException) {
            $this->assertEquals(['reject', false], $message->settlements[0]);
        }
    }

    /**
     * @throws Throwable
     */
    public function testRetryMessageForSaveException(): void
    {
        RabbitMQMessage::shouldReceive('canRetryMessages')
            ->once()
            ->andReturn(true);

        $message = $this->fakeMessage($properties = $this->validProperties());

        Config::set('rabbitmq-messages.message_handlers', [
            $properties['type'] => MockMessageHandler::class,
        ]);

        $listenMessage = MockMessageHandler::mockListenMessageModel();
        $listenMessage::$fails['save'] = true;
        $listenMessage::$findReturn = null;
        $this->app->bind(ListenMessageModel::class, fn () => $listenMessage);

        RabbitMQMessage::shouldReceive('retryMessage')
            ->once()
            ->withArgs(fn (ListenMessageModel $message, Throwable $reason, string $connection) => $connection === 'rabbitmq')
            ->andReturn($listenMessage);

        $this->expectException(QueryException::class);

        try {
            (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
        } finally {
            $this->assertEquals(['nack', false], $message->settlements[0]);
        }
    }

    /**
     * @throws Throwable
     */
    public function testRetryMessageForFindException(): void
    {
        RabbitMQMessage::shouldReceive('canRetryMessages')
            ->once()
            ->andReturn(true);

        $message = $this->fakeMessage($properties = $this->validProperties());

        Config::set('rabbitmq-messages.message_handlers', [
            $properties['type'] => MockMessageHandler::class,
        ]);

        $listenMessage = MockMessageHandler::mockListenMessageModel();
        $this->app->bind(ListenMessageModel::class, fn () => $listenMessage);
        $listenMessage::$fails['find'] = true;

        RabbitMQMessage::shouldReceive('retryMessage')
            ->once()
            ->andReturn($listenMessage);

        $this->expectException(QueryException::class);

        (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');
    }

    public function testRetriedMessageKeepsItsOriginalDestination(): void
    {
        $properties = $this->validProperties();
        $properties['application_headers'] = [
            'tries' => 1,
            MessageResilient::ORIGIN_EXCHANGE_HEADER => 'amq.topic',
            MessageResilient::ORIGIN_ROUTING_KEY_HEADER => 'rabbitmq-messages.tests',
        ];

        // retried copies are delivered through the default exchange
        $message = $this->fakeMessage($properties, 'rabbitmq-messages.tests', '', 'rabbitmq-messages.tests');
        Config::set('rabbitmq-messages.message_handlers', ['/.*/' => MockMessageHandler::class]);
        $this->app->bind(ListenMessageModel::class, fn () => MockMessageHandler::mockListenMessageModel());

        $listened = (new MessageListener)->sendMessageThroughPipes($message, 'rabbitmq');

        $this->assertEquals('rabbitmq_topic', $listened->metadata->get('connection'));
        $this->assertEquals('amq.topic', $listened->metadata->get('exchange'));
    }

    protected function fakeMessage(array $properties, string $queue = 'foo-queue', string $exchange = 'amq.fanout', string $routingKey = ''): FakeIncomingMessage
    {
        return new FakeIncomingMessage($properties, $this->validBody($properties), $queue, $exchange, $routingKey);
    }

    protected function validProperties(): array
    {
        return [
            'app_id' => fake()->domainName,
            'type' => 'foo-bar.test.event_dispatched',
            'timestamp' => fake()->dateTime->getTimestamp(),
            'message_id' => fake()->uuid,
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];
    }

    protected function validBody(array $properties): string
    {
        return json_encode([
            'app_id' => $properties['app_id'],
            'uuid' => $properties['message_id'] ?? null,
            'fired_at' => Carbon::createFromTimestamp($properties['timestamp'])->toIso8601String(),
        ]);
    }
}
