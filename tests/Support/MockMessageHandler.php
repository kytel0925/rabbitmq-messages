<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Support;

use Exception;
use Illuminate\Database\QueryException;
use Illuminate\Support\Collection;
use SlothDevGuy\RabbitMQMessages\Interfaces\MessageHandlerInterface;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;

/**
 * Class MockMessageHandler
 *
 * @package SlothDevGuy\RabbitMQMessagesTests\Support
 */
class MockMessageHandler implements MessageHandlerInterface
{
    /**
     * {@inheritdoc}
     */
    public function handle(Collection $payload, ListenMessageModel $message): void
    {
        logger()->info('payload', $payload->toArray());
        logger()->info('properties', $message->properties->toArray());
        logger()->info('metadata', $message->metadata->toArray());
    }

    public static function mockListenMessageModel(): ListenMessageModel
    {
        return new class extends ListenMessageModel
        {
            public static array $fails = [
                'save' => false,
                'delete' => false,
                'find' => false,
            ];

            public static ?ListenMessageModel $findReturn = null;

            /**
             * @return void
             *
             * @noinspection PhpMissingReturnTypeInspection
             */
            public function save(array $options = [])
            {
                if (self::$fails['save']) {
                    self::$fails['save'] = false;
                    throw new QueryException('test-connection', 'select now()', [], new Exception);
                }
            }

            /**
             * @return void
             *
             * @noinspection PhpMissingReturnTypeInspection
             */
            public function delete()
            {
                if (self::$fails['delete']) {
                    self::$fails['delete'] = false;
                    throw new QueryException('test-connection', 'select now()', [], new Exception);
                }
            }

            /**
             * @return static|ListenMessageModel|null
             */
            public static function findByUuid(string $uuid): ?static
            {
                if (self::$fails['find']) {
                    self::$fails['find'] = false;
                    throw new QueryException('test-connection', 'select now()', [], new Exception);
                }

                return self::$findReturn;
            }
        };
    }
}
