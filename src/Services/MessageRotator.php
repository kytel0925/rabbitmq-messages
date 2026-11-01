<?php

namespace SlothDevGuy\RabbitMQMessages\Services;

use Illuminate\Container\Container;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * Deletes old outbox (dispatch_message) and inbox (listen_message) rows.
 *
 * Rows are selected by status and last update (updated_at) and deleted in
 * primary-key chunks, so the job never holds long table locks.
 */
class MessageRotator
{
    public const TABLES = [
        'dispatch_message' => 'dispatch_message',
        'listen_message' => 'listen_message',
    ];

    /**
     * @param  array<string, array<string, int|null>>|null  $retention  table => [status => days|null]
     * @param  callable(string $table, string $status, int $deleted): void|null  $progress
     * @return array<string, array<string, int>> table => [status => deleted rows]
     */
    public function rotate(
        ?array $retention = null,
        ?int $chunkSize = null,
        bool $pretend = false,
        ?Carbon $now = null,
        ?callable $progress = null,
    ): array {
        $retention ??= (array) MessagesConfig::rotation('retention', []);
        $chunkSize = max(1, $chunkSize ?? (int) MessagesConfig::rotation('chunk_size', 1000));
        $now ??= Carbon::now();
        $result = [];

        foreach ($retention as $table => $statuses) {
            $model = $this->model($table);

            foreach ((array) $statuses as $status => $days) {
                if ($days === null || $days === '' || (int) $days < 0) {
                    continue; // keep forever
                }

                $cutoff = $now->copy()->subDays((int) $days);
                $deleted = $pretend
                    ? $this->query($model, (string) $status, $cutoff)->count()
                    : $this->deleteInChunks($model, (string) $status, $cutoff, $chunkSize);

                $result[$table][$status] = $deleted;

                if ($progress) {
                    $progress($table, (string) $status, $deleted);
                }
            }
        }

        return $result;
    }

    protected function deleteInChunks(Model $model, string $status, Carbon $cutoff, int $chunkSize): int
    {
        $deleted = 0;

        do {
            $ids = $this->query($model, $status, $cutoff)
                ->orderBy($model->getKeyName())
                ->limit($chunkSize)
                ->pluck($model->getKeyName())
                ->all();

            if (empty($ids)) {
                break;
            }

            $deleted += $model->newQuery()->whereKey($ids)->delete();
        } while (count($ids) === $chunkSize);

        return $deleted;
    }

    protected function query(Model $model, string $status, Carbon $cutoff)
    {
        return $model->newQuery()
            ->where('status', $status)
            ->where($model->getUpdatedAtColumn() ?? 'updated_at', '<', $cutoff);
    }

    /**
     * Resolve the (configurable) model that owns a table.
     */
    protected function model(string $table): Model
    {
        $class = match ($table) {
            'dispatch_message' => Config::get('rabbitmq-messages.models.dispatch_message', DispatchMessageModel::class),
            'listen_message' => Config::get('rabbitmq-messages.models.listen_message', ListenMessageModel::class),
            default => throw new InvalidConfigurationException(
                "Unknown rotation table [$table]; expected one of: ".implode(', ', array_keys(static::TABLES))
            ),
        };

        return Container::getInstance()->make($class);
    }
}
