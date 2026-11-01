<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Feature;

use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Queue;
use PhpAmqpLib\Wire\AMQPTable;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpPublisher;
use SlothDevGuy\RabbitMQMessages\DispatchMessage;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\Enums\ListenMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\RabbitMQMessage;
use SlothDevGuy\RabbitMQMessages\Services\MessageResilient;
use SlothDevGuy\RabbitMQMessagesTests\Feature\Mocks\MockFailedMessageHandler;
use SlothDevGuy\RabbitMQMessagesTests\Feature\Mocks\MockInvalidMessageHandler;
use SlothDevGuy\RabbitMQMessagesTests\Feature\Mocks\MockMessageHandler;

class ListenMessageTest extends FeatureTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        MockFailedMessageHandler::$failures = -1;
        MockMessageHandler::$handled = 0;
    }

    public function testMessageListen(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockMessageHandler::class);

        $message = RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()))->fresh();
        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($message->uuid);
        $this->assertNotNull($actualMessage);
        $this->assertEquals(ListenMessageStatusEnum::PROCESSED, $actualMessage->status);
        $this->assertEquals($message->name, $actualMessage->name);
        $this->assertEquals($message->payload->toArray(), $actualMessage->payload->toArray());
        $this->assertEquals('rabbitmq', $actualMessage->metadata->get('connection'));
        $this->assertEquals(MockMessageHandler::class, $actualMessage->metadata->get('handler'));
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testMessageListenPattern(): void
    {
        Config::set('rabbitmq-messages.message_handlers', ['/.*/' => MockMessageHandler::class]);

        $message = RabbitMQMessage::dispatchMessage('test-message-listen', collect(static::makeFakePayload()))->fresh();
        $this->consumeOnce();

        $this->assertEquals(ListenMessageStatusEnum::PROCESSED, ListenMessageModel::findByUuid($message->uuid)->status);
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testDuplicatedMessageIsProcessedOnce(): void
    {
        Config::set('rabbitmq-messages.message_handlers', ['/.*/' => MockMessageHandler::class]);
        MockMessageHandler::$handled = 0;

        $message = RabbitMQMessage::dispatchMessage('test-message-listen', collect(static::makeFakePayload()));
        // the outbox is at-least-once: publish the same row again
        $this->app->make(AmqpPublisher::class)->publish($message->fresh());

        $this->consumeOnce();
        $this->consumeOnce();

        $this->assertEquals(1, MockMessageHandler::$handled);
        $this->assertDatabaseCount('listen_message', 1);
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testSkippedMessageListen(): void
    {
        RabbitMQMessage::dispatchMessage('test-message-listen', collect(static::makeFakePayload()));
        $this->consumeOnce();

        $this->assertDatabaseEmpty('listen_message');
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testFailedMessageListen(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
        Config::set('rabbitmq-messages.connections.rabbitmq.retry.queue', ''); // disable retries

        $message = RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()))->fresh();
        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($message->uuid);
        $this->assertEquals(ListenMessageStatusEnum::FAILED, $actualMessage->status);
        $this->assertNotEmpty($actualMessage->metadata->get('exceptions'));
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testNoRetries(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
        Config::set('rabbitmq-messages.max_tries', 0);

        $message = RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()))->fresh();
        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($message->uuid);
        $this->assertEquals(ListenMessageStatusEnum::FAILED, $actualMessage->status);
        $this->assertNotEmpty($actualMessage->metadata->get('exceptions'));
        $this->assertQueueIsEmpty($this->queue());
        $this->assertQueueIsEmpty($this->retryQueue());
    }

    public function testRetryMessage(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
        Config::set('rabbitmq-messages.connections.rabbitmq.dead_letter.queue', ''); // disable dead letter
        Config::set('rabbitmq-messages.max_tries', $maxRetries = 2);

        $message = RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()))->fresh();

        for ($i = 0; $i < $maxRetries; $i++) {
            $this->consumeOnce();
            $this->assertQueueIsEmpty($this->queue());
            $this->waitForRetry();
        }

        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($message->uuid);
        $this->assertEquals(ListenMessageStatusEnum::FAILED, $actualMessage->status);
        $this->assertEquals($message->payload->toArray(), $actualMessage->payload->toArray());
        $this->assertNotEmpty($actualMessage->metadata->get('exceptions'));
        $this->assertEquals($maxRetries, data_get($actualMessage->properties, MessageResilient::getTriesKey()));
        $this->assertEquals('amq.fanout', $actualMessage->metadata->get('exchange'));
        $this->assertCount($maxRetries, json_decode(data_get($actualMessage->properties, 'application_headers.exceptions'), true));
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testRetriedMessageSucceeds(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
        MockFailedMessageHandler::$failures = 1;

        $message = RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()))->fresh();
        $this->consumeOnce();
        $this->assertDatabaseEmpty('listen_message');

        $this->waitForRetry();
        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($message->uuid);
        $this->assertEquals(ListenMessageStatusEnum::PROCESSED, $actualMessage->status);
        $this->assertEquals(1, data_get($actualMessage->properties, MessageResilient::getTriesKey()));
    }

    public function testDeadLetterMessage(): void
    {
        $message = 'test-message-listen';
        Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
        Config::set('rabbitmq-messages.max_tries', $maxRetries = 1);

        RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()));

        for ($i = 0; $i < $maxRetries; $i++) {
            $this->consumeOnce();
            $this->waitForRetry();
        }

        $this->consumeOnce();

        $this->assertDatabaseEmpty('listen_message');
        $this->assertQueueIsEmpty($this->queue());
        $this->assertQueueIsNotEmpty($this->deadLetterQueue());
    }

    public function testDeadLetterInvalidMessages(): void
    {
        $message = 'some-invalid-message';
        Config::set("rabbitmq-messages.message_handlers.$message", MockInvalidMessageHandler::class);
        Config::set('rabbitmq-messages.max_tries', 3);

        RabbitMQMessage::dispatchMessage(new DispatchMessage($message, collect()));
        $this->consumeOnce();

        $this->assertDatabaseEmpty('listen_message');
        $this->assertQueueIsEmpty($this->queue());
        $this->assertQueueIsEmpty($this->retryQueue());
        $this->assertQueueIsNotEmpty($this->deadLetterQueue());
    }

    public function testNonRetriableMessageIsStoredAsFailedWithoutDeadLetterQueue(): void
    {
        $message = 'some-invalid-message';
        Config::set("rabbitmq-messages.message_handlers.$message", MockInvalidMessageHandler::class);
        Config::set('rabbitmq-messages.connections.rabbitmq.dead_letter.queue', '');

        $dispatched = RabbitMQMessage::dispatchMessage(new DispatchMessage($message, collect()));
        $this->consumeOnce();

        $actualMessage = ListenMessageModel::findByUuid($dispatched->uuid);
        $this->assertEquals(ListenMessageStatusEnum::FAILED, $actualMessage->status);
        $this->assertDatabaseCount('listen_message', 1);
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testDeadLetterMessageInAllConnections(): void
    {
        foreach (['rabbitmq', 'rabbitmq_topic'] as $connection) {
            $message = 'test-message-listen';
            Config::set("rabbitmq-messages.message_handlers.$message", MockFailedMessageHandler::class);
            Config::set('rabbitmq-messages.max_tries', 1);

            RabbitMQMessage::dispatchMessage($message, collect(static::makeFakePayload()), $connection);

            $this->consumeOnce($connection);
            $this->waitForRetry();
            $this->consumeOnce($connection);

            $this->assertDatabaseEmpty('listen_message');
            $this->assertQueueIsEmpty($this->queue());

            $deadLetter = $this->getMessage(config("rabbitmq-messages.connections.$connection.dead_letter.queue"));
            $this->assertNotNull($deadLetter, "connection [$connection]");
            $headers = $deadLetter->get('application_headers')->getNativeData();
            $this->assertEquals(config("rabbitmq-messages.connections.$connection.exchange"), $headers[MessageResilient::ORIGIN_EXCHANGE_HEADER]);
        }
    }

    public function testSkipInvalidMessage(): void
    {
        $empty = new class extends DispatchMessageModel
        {
            public function save(array $options = []): bool
            {
                return true;
            }
        };
        $empty->properties = collect();
        $empty->payload = collect();
        $empty->metadata = collect(['connection' => 'rabbitmq', 'exchange' => 'amq.fanout', 'exchange_type' => 'fanout', 'routing_key' => '']);

        $this->app->make(AmqpPublisher::class)->publish($empty);
        $this->consumeOnce();

        $this->assertDatabaseEmpty('listen_message');
        $this->assertQueueIsEmpty($this->queue());
        $this->assertQueueIsEmpty($this->deadLetterQueue());
    }

    public function testQueueCommandsSupportedByTheDriver(): void
    {
        RabbitMQMessage::dispatchMessage('test-message-listen', collect());
        RabbitMQMessage::dispatchMessage('test-message-listen', collect());

        $this->assertEquals(2, Queue::connection('rabbitmq')->size($this->queue()));

        $this->artisan('queue:monitor', ['queues' => 'rabbitmq:'.$this->queue(), '--max' => 1])
            ->expectsOutputToContain('ALERT')
            ->assertSuccessful();

        $this->artisan('queue:clear', ['connection' => 'rabbitmq', '--queue' => $this->queue(), '--force' => true])
            ->assertSuccessful();

        $this->assertEquals(0, Queue::connection('rabbitmq')->size($this->queue()));
        $this->assertEquals(0, Queue::connection('rabbitmq')->size('a-queue-that-does-not-exist'));
    }

    public function testQueueCommandsDefaultToTheConsumerQueue(): void
    {
        Config::set('rabbitmq-messages.message_handlers', ['/.*/' => MockMessageHandler::class]);
        $message = RabbitMQMessage::dispatchMessage('test-message-listen', collect());

        // no --queue: Laravel falls back to queue.connections.rabbitmq.queue
        $this->artisan('queue:work', ['connection' => 'rabbitmq', '--once' => true, '--sleep' => 0])->assertSuccessful();
        $this->assertDatabaseHas('listen_message', ['uuid' => $message->uuid]);

        RabbitMQMessage::dispatchMessage('test-message-listen', collect());
        $this->artisan('queue:clear', ['connection' => 'rabbitmq', '--force' => true])->assertSuccessful();
        $this->assertQueueIsEmpty($this->queue());
    }

    public function testSetupExplainsIncompatibleV1RetryQueues(): void
    {
        $legacy = 'rabbitmq-messages.tests.retry-v1';
        $channel = $this->channel();
        $channel->queue_delete($legacy);
        // v1 retry queues dead-lettered to the exchange
        $channel->queue_declare($legacy, false, true, false, false, false, new AMQPTable([
            'x-dead-letter-exchange' => 'amq.fanout',
            'x-message-ttl' => 3000,
        ]));

        Config::set('rabbitmq-messages.connections.rabbitmq.retry.queue', $legacy);

        try {
            $this->artisan('rabbitmq-messages:setup', ['connection' => ['rabbitmq']])
                ->expectsOutputToContain('PRECONDITION_FAILED')
                ->assertFailed();
        } finally {
            $this->channel()->queue_delete($legacy);
        }
    }

    public function testStopWhenEmpty(): void
    {
        $this->artisan('queue:work', [
            'connection' => 'rabbitmq',
            '--queue' => $this->queue(),
            '--stop-when-empty' => true,
        ])->assertSuccessful();
    }

    protected function queue(): string
    {
        return config('rabbitmq-messages.connections.rabbitmq.queue');
    }

    protected function retryQueue(): string
    {
        return config('rabbitmq-messages.connections.rabbitmq.retry.queue');
    }

    protected function deadLetterQueue(): string
    {
        return config('rabbitmq-messages.connections.rabbitmq.dead_letter.queue');
    }

    protected function waitForRetry(): void
    {
        usleep((int) (config('rabbitmq-messages.connections.rabbitmq.retry.delay') * 1000) + 300_000);
    }
}
