<?php

namespace SlothDevGuy\RabbitMQMessages\Pipes;

use Illuminate\Container\Container;
use Illuminate\Pipeline\Pipeline;
use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Builders\FromListenMessage;
use SlothDevGuy\RabbitMQMessages\Builders\FromRabbitMQMessage;
use SlothDevGuy\RabbitMQMessages\Exceptions\MessageRetriesExhaustedException;
use SlothDevGuy\RabbitMQMessages\Interfaces\IncomingMessageInterface;
use SlothDevGuy\RabbitMQMessages\Interfaces\NonRetriableMessageThrowable;
use SlothDevGuy\RabbitMQMessages\Interfaces\SkipListenMessageThrowable;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Pipes\Resiliency\DeadLetterMessage;
use SlothDevGuy\RabbitMQMessages\Pipes\Resiliency\DispatchMessage;
use SlothDevGuy\RabbitMQMessages\Pipes\Resiliency\RetryMessage;
use Throwable;

/**
 * Consume side entry point (the inbox):
 *
 *   validate -> resolve handler -> idempotency check -> retries exhausted?
 *   -> store (queued) -> handler (in a DB transaction) -> processed -> ack
 *
 * Failures are routed to skip (ack), retry (delayed retry queue) or dead letter.
 */
class MessageListener
{
    /**
     * @throws Throwable the reason the message was skipped, retried or dead-lettered
     */
    public function sendMessageThroughPipes(IncomingMessageInterface $message, string $connection): ListenMessageModel
    {
        $container = Container::getInstance();
        $fromRabbitMQMessage = new FromRabbitMQMessage($message, $connection);
        $listenedMessage = $fromRabbitMQMessage->buildListenMessageModel($container->make(ListenMessageModel::class));
        $connection = $fromRabbitMQMessage->getConnection();

        try {
            /** @var FromListenMessage $fromListenMessage */
            $listenedMessage = (new Pipeline($container))
                ->send($listenedMessage)
                ->through([
                    $container->make(MessageValidation::class),
                    $fromListenMessage = $container->make(FromListenMessage::class),
                    $container->make(VerifyMessageAlreadyRegister::class),
                    $container->make(MessageExhausted::class),
                    $container->make(StoreMessage::class),
                ])
                ->then(function (ListenMessageModel $listenedMessage) use ($fromListenMessage) {
                    $dispatchMessage = new DispatchMessage($fromListenMessage->getHandler());
                    $dispatchMessage->handle($listenedMessage);

                    return $listenedMessage;
                });

            $message->ack();

            return $listenedMessage;
        } catch (SkipListenMessageThrowable $ex) {
            $this->skipMessage($message, $ex);
            throw $ex;
        } catch (MessageRetriesExhaustedException|NonRetriableMessageThrowable $ex) {
            $deadLetterMessage = new DeadLetterMessage($listenedMessage, $message, $connection, $ex);
            $deadLetterMessage->handle();
            throw $ex;
        } catch (Throwable $ex) {
            $retryMessage = new RetryMessage($listenedMessage, $message, $connection, $ex);
            $retryMessage->handle();
            throw $ex;
        }
    }

    protected function skipMessage(IncomingMessageInterface $message, Throwable $ex): void
    {
        $reason = class_basename($ex);
        Log::debug("message skipped $reason: {$ex->getMessage()}", $message->getProperties());
        $message->ack();
    }
}
