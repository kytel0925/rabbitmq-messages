<?php

namespace SlothDevGuy\RabbitMQMessages\Services;

use Illuminate\Container\Container;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\Exceptions\InvalidConfigurationException;
use SlothDevGuy\RabbitMQMessages\Models\DispatchMessageModel;
use SlothDevGuy\RabbitMQMessages\Models\Enums\DispatchMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\Models\Enums\ListenMessageStatusEnum;
use SlothDevGuy\RabbitMQMessages\Models\ListenMessageModel;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * One rotation for the outbox (dispatch_message) and the inbox (listen_message).
 *
 * Finished messages whose last update (updated_at) is older than the
 * retention are deleted in primary-key chunks, so the job never holds long
 * table locks. Messages still in flight (created / queued) are never deleted.
 */
class MessageRotator
{
    /**
     * Rotated tables and their finished (rotatable) statuses.
     */
    public const TABLES = [
        'dispatch_message' => [
            DispatchMessageStatusEnum::DISPATCHED->value,
            DispatchMessageStatusEnum::FAILED->value,
        ],
        'listen_message' => [
            ListenMessageStatusEnum::PROCESSED->value,
            ListenMessageStatusEnum::FAILED->value,
        ],
    ];

    /**
     * @param  int|null  $days  retention in days (default: rabbitmq-messages.rotation.retention_days)
     * @param  array<int, string>  $tables  only these tables (default: both)
     * @param  array<int, string>  $statuses  only these statuses (default: every finished status)
     * @param  callable(string $table, string $status, int $deleted): void|null  $progress
     * @return array<string, array<string, int>> table => [status => deleted rows]
     */
    public function rotate(
        ?int $days = null,
        array $tables = [],
        array $statuses = [],
        ?int $chunkSize = null,
        bool $pretend = false,
        ?Carbon $now = null,
        ?callable $progress = null,
    ): array {
        $days ??= static::retentionDays();
        if ($days === null || $days < 1) {
            throw new InvalidConfigurationException('The rotation retention must be at least 1 day (rabbitmq-messages.rotation.retention_days).');
        }

        $chunkSize = max(1, $chunkSize ?? (int) MessagesConfig::rotation('chunk_size', 1000));
        $cutoff = ($now ?? Carbon::now())->copy()->subDays($days);
        $result = [];

        foreach (static::TABLES as $table => $finished) {
            if ($tables && ! in_array($table, $tables, true)) {
                continue;
            }

            $model = $this->model($table);

            foreach ($finished as $status) {
                if ($statuses && ! in_array($status, $statuses, true)) {
                    continue;
                }

                $deleted = $pretend
                    ? $this->query($model, $status, $cutoff)->count()
                    : $this->deleteInChunks($model, $status, $cutoff, $chunkSize);

                $result[$table][$status] = $deleted;

                if ($progress) {
                    $progress($table, $status, $deleted);
                }
            }
        }

        return $result;
    }

    public static function retentionDays(): ?int
    {
        $days = MessagesConfig::rotation('retention_days', 30);

        return is_numeric($days) ? (int) $days : null;
    }

    /**
     * @return array<int, string>
     */
    public static function statuses(): array
    {
        return array_values(array_unique(array_merge(...array_values(static::TABLES))));
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
        };

        return Container::getInstance()->make($class);
    }
}
