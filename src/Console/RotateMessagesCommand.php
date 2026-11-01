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
                            {--table=* : Only rotate these tables (dispatch_message, listen_message)}
                            {--status=* : Only rotate these statuses (e.g. processed, dispatched, failed)}
                            {--days= : Override the configured retention (in days) for every selected status}
                            {--chunk= : Rows deleted per query (default: rabbitmq-messages.rotation.chunk_size)}
                            {--pretend : Only count the rows that would be deleted}
                            {--force : Run even when rotation is disabled in the configuration}';

    protected $description = 'Delete old dispatched/processed/failed messages to preserve database space';

    public function handle(MessageRotator $rotator): int
    {
        if (! $this->option('force') && ! MessagesConfig::rotation('enabled', true)) {
            $this->components->warn('Message rotation is disabled (rabbitmq-messages.rotation.enabled). Use --force to run it anyway.');

            return self::SUCCESS;
        }

        $retention = $this->retention();
        if ($retention === null) {
            return self::FAILURE;
        }

        $pretend = (bool) $this->option('pretend');
        $chunk = $this->option('chunk') !== null ? (int) $this->option('chunk') : null;

        $result = $rotator->rotate($retention, $chunk, $pretend, null, function (string $table, string $status, int $rows) use ($pretend, $retention) {
            $days = $retention[$table][$status];
            $verb = $pretend ? 'would be deleted' : 'deleted';
            $this->components->twoColumnDetail("$table <fg=gray>[$status, older than {$days}d]</>", "$rows $verb");
        });

        if (empty($result)) {
            $this->components->info('Nothing to rotate: every selected status is configured to be kept forever.');
        }

        return self::SUCCESS;
    }

    /**
     * Configured retention filtered/overridden by the command options.
     */
    protected function retention(): ?array
    {
        $retention = (array) MessagesConfig::rotation('retention', []);
        $tables = array_filter((array) $this->option('table'));
        $statuses = array_filter((array) $this->option('status'));
        $days = $this->option('days');

        if ($unknown = array_diff($tables, array_keys(MessageRotator::TABLES))) {
            $this->components->error('Unknown table(s): '.implode(', ', $unknown).'. Expected: '.implode(', ', array_keys(MessageRotator::TABLES)));

            return null;
        }

        if ($days !== null && (! is_numeric($days) || (int) $days < 0)) {
            $this->components->error('--days must be a positive integer.');

            return null;
        }

        $result = [];
        foreach ($retention as $table => $configured) {
            if ($tables && ! in_array($table, $tables, true)) {
                continue;
            }

            foreach ((array) $configured as $status => $configuredDays) {
                if ($statuses && ! in_array($status, $statuses, true)) {
                    continue;
                }

                $result[$table][$status] = $days !== null ? (int) $days : $configuredDays;
            }
        }

        return $result;
    }
}
