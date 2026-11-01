<?php

namespace SlothDevGuy\RabbitMQMessages\Services;

use Illuminate\Support\Facades\Date;
use Illuminate\Support\Facades\Log;
use SlothDevGuy\RabbitMQMessages\Amqp\AmqpPublisher;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageResilientInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Throwable;

/**
 * Retry and dead letter strategies of the consume side.
 */
class MessageResilient implements MessageResilientInterface
{
    public const ORIGIN_EXCHANGE_HEADER = 'origin_exchange';

    public const ORIGIN_ROUTING_KEY_HEADER = 'origin_routing_key';

    public function __construct(
        protected AmqpPublisher $publisher,
    ) {}

    public function canRetry(?string $connection = null): bool
    {
        $configuration = MessagesConfig::connection($connection);

        return static::getMaxTries() > 0 && MessagesConfig::retryQueue($configuration) !== null;
    }

    /**
     * @throws Throwable
     */
    public function retry(ListenMessageModel $message, Throwable $reason, ?string $connection = null): ListenMessageModel
    {
        $connection = $connection ?? $message->metadata->get('connection');
        if (! $this->canRetry($connection)) {
            return $message;
        }

        $this->incrementRetry($message, $reason);
        $this->publisher->retry($message, $connection);

        Log::info('message-retried', $message->properties->toArray());

        return $message;
    }

    public function canDeadLetter(?string $connection = null): bool
    {
        return MessagesConfig::deadLetterQueue(MessagesConfig::connection($connection)) !== null;
    }

    /**
     * @throws Throwable
     */
    public function deadLetter(ListenMessageModel $message, ?string $connection = null): ListenMessageModel
    {
        $connection = $connection ?? $message->metadata->get('connection');
        if (! $this->canDeadLetter($connection)) {
            return $message;
        }

        $this->publisher->deadLetter($message, $connection);

        Log::info('message-dead-letter', $message->toArray());

        return $message;
    }

    /**
     * @return array{retry_queue: string|null, retry_queue_delay: int}
     */
    public function getRetryConfiguration(string $connection): array
    {
        $configuration = MessagesConfig::connection($connection);

        return [
            'retry_queue' => MessagesConfig::retryQueue($configuration),
            'retry_queue_delay' => MessagesConfig::retryDelay($configuration),
        ];
    }

    /**
     * @return array{dead_letter_queue: string|null}
     */
    public function getDeadLetterConfiguration(string $connection): array
    {
        return [
            'dead_letter_queue' => MessagesConfig::deadLetterQueue(MessagesConfig::connection($connection)),
        ];
    }

    /**
     * Increment the tries header, append the failure reason and remember the
     * original exchange/routing key (the retried copy travels through the
     * default exchange).
     */
    protected function incrementRetry(ListenMessageModel $message, Throwable $reason): void
    {
        $properties = $message->properties->toArray();
        $key = static::getTriesKey();
        data_set($properties, $key, data_get($properties, $key, 0) + 1);

        $exceptions = json_decode((string) data_get($properties, 'application_headers.exceptions', '[]'), true) ?: [];
        $exceptions[] = [
            'reason' => class_basename($reason),
            'message' => $reason->getMessage(),
            'file' => $reason->getFile(),
            'line' => $reason->getLine(),
            'failed_at' => Date::now()->toIso8601String(),
        ];
        data_set($properties, 'application_headers.exceptions', json_encode($exceptions));

        foreach ([static::ORIGIN_EXCHANGE_HEADER => 'exchange', static::ORIGIN_ROUTING_KEY_HEADER => 'routing_key'] as $header => $metadata) {
            if (! isset($properties['application_headers'][$header])) {
                $properties['application_headers'][$header] = (string) $message->metadata->get($metadata, '');
            }
        }

        $message->properties = $properties;
    }

    public static function getTriesKey(): string
    {
        return 'application_headers.tries';
    }

    public static function getMaxTries(): int
    {
        return MessagesConfig::maxTries();
    }
}
