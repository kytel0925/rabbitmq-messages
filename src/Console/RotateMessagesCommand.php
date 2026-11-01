<?php

namespace SlothDevGuy\RabbitMQMessages\Console;

use Illuminate\Console\Command;
use SlothDevGuy\RabbitMQMessages\Services\MessageRotator;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;

/**
 * php artisan rabbitmq-messages:rotate
 */
class RotateMessagesCommand extends Command
{
    protected $signature = 'rabbitmq-messages:rotate
                            {--days= : Retention in days (default: rabbitmq-messages.rotation.retention_days)}
                            {--table=* : Only rotate these tables (dispatch_message, listen_message)}
                            {--status=* : Only rotate these statuses (dispatched, processed, failed)}
                            {--chunk= : Rows deleted per query (default: rabbitmq-messages.rotation.chunk_size)}
                            {--pretend : Only count the rows that would be deleted}
                            {--force : Run even when rotation is disabled in the configuration}';

    protected $description = 'Delete finished outbox and inbox messages older than the retention to preserve database space';

    public function handle(MessageRotator $rotator): int
    {
        if (! $this->option('force') && ! MessagesConfig::rotation('enabled', true)) {
            $this->components->warn('Message rotation is disabled (rabbitmq-messages.rotation.enabled). Use --force to run it anyway.');

            return self::SUCCESS;
        }

        $days = $this->option('days') ?? MessageRotator::retentionDays();
        if (! is_numeric($days) || (int) $days < 1) {
            $this->components->error('The retention must be a whole number of days >= 1 (--days or rabbitmq-messages.rotation.retention_days).');

            return self::FAILURE;
        }

        $tables = array_values(array_filter((array) $this->option('table')));
        if ($unknown = array_diff($tables, array_keys(MessageRotator::TABLES))) {
            $this->components->error('Unknown table(s): '.implode(', ', $unknown).'. Expected: '.implode(', ', array_keys(MessageRotator::TABLES)).'.');

            return self::FAILURE;
        }

        $statuses = array_values(array_filter((array) $this->option('status')));
        if ($unknown = array_diff($statuses, MessageRotator::statuses())) {
            $this->components->error('Status(es) that cannot be rotated: '.implode(', ', $unknown).'. Expected: '.implode(', ', MessageRotator::statuses()).'.');

            return self::FAILURE;
        }

        $pretend = (bool) $this->option('pretend');
        $chunk = $this->option('chunk') !== null ? (int) $this->option('chunk') : null;
        $days = (int) $days;

        $this->components->info(($pretend ? 'Counting' : 'Deleting')." finished messages older than $days day(s).");

        $rotator->rotate($days, $tables, $statuses, $chunk, $pretend, null, function (string $table, string $status, int $rows) use ($pretend) {
            $this->components->twoColumnDetail("$table <fg=gray>[$status]</>", $rows.($pretend ? ' would be deleted' : ' deleted'));
        });

        return self::SUCCESS;
    }
}
