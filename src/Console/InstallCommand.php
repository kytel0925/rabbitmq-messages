<?php

namespace SlothDevGuy\RabbitMQMessages\Console;

use Illuminate\Console\Command;
use Illuminate\Filesystem\Filesystem;

/**
 * php artisan rabbitmq-messages:install
 */
class InstallCommand extends Command
{
    protected $signature = 'rabbitmq-messages:install
                            {--connections= : Connections to keep in the published config: all, fanout or topic}
                            {--force : Overwrite previously published files}';

    protected $description = 'Publish the rabbitmq-messages configuration, migrations and worker scripts';

    /**
     * Config blocks delimited by "// <name>" and "// </name>" markers.
     */
    protected const CONNECTION_BLOCKS = ['fanout', 'topic'];

    protected const ENVIRONMENT = [
        'common' => [
            'RABBITMQ_APP_ID' => '"${APP_NAME}"',
            'RABBITMQ_HOST' => 'rabbitmq',
            'RABBITMQ_PORT' => '5672',
            'RABBITMQ_USERNAME' => 'username',
            'RABBITMQ_PASSWORD' => 'password',
            'RABBITMQ_VHOST' => '/',
            'RABBITMQ_QUEUE' => 'my-bounded-context.messages',
            'RABBITMQ_RETRY_QUEUE' => 'my-bounded-context.messages.retry',
            'RABBITMQ_RETRY_QUEUE_DELAY' => '3000',
            'RABBITMQ_DEAD_LETTER_QUEUE' => 'my-bounded-context.messages.dead-letter',
            'RABBITMQ_MESSAGES_MAX_TRIES' => '3',
        ],
        'fanout' => [
            'RABBITMQ_EXCHANGE' => 'amq.fanout',
        ],
        'topic' => [
            'RABBITMQ_TOPIC_EXCHANGE' => 'amq.topic',
            'RABBITMQ_TOPIC_ROUTING_KEY' => '"my-bounded-context.#"',
        ],
    ];

    public function handle(Filesystem $files): int
    {
        $connections = $this->option('connections')
            ?: $this->choice('Which connections do you want to use?', ['all', 'fanout', 'topic'], 'all');

        if (! in_array($connections, ['all', 'fanout', 'topic'], true)) {
            $this->components->error('--connections must be one of: all, fanout, topic.');

            return self::FAILURE;
        }

        $configPath = $this->laravel->configPath('rabbitmq-messages.php');
        $force = (bool) $this->option('force');

        if ($files->exists($configPath) && ! $force) {
            $force = $this->confirm('config/rabbitmq-messages.php already exists. Overwrite it (and the worker scripts)?', false);
        }

        $this->call('vendor:publish', ['--tag' => 'rabbitmq-messages-migrations']);
        $this->call('vendor:publish', ['--tag' => 'rabbitmq-messages-config', '--force' => $force]);
        $this->call('vendor:publish', ['--tag' => 'rabbitmq-messages-bin', '--force' => $force]);

        if ($force || ! $this->wasCustomized($files, $configPath)) {
            $this->keepConnections($files, $configPath, $connections);
        }

        $this->suggestEnvironment($connections);

        $this->components->info('Next steps: php artisan migrate && php artisan rabbitmq-messages:setup');

        return self::SUCCESS;
    }

    /**
     * Remove the connection blocks that were not selected from the published config.
     */
    protected function keepConnections(Filesystem $files, string $configPath, string $connections): void
    {
        if ($connections === 'all' || ! $files->exists($configPath)) {
            return;
        }

        $contents = $files->get($configPath);
        foreach (static::CONNECTION_BLOCKS as $block) {
            if ($block === $connections) {
                continue;
            }

            $contents = preg_replace("#[ \t]*// <$block>.*?// </$block>\R#s", '', $contents);
        }

        if ($connections === 'topic') {
            $contents = str_replace(
                "env('RABBITMQ_MESSAGES_CONNECTION', 'rabbitmq')",
                "env('RABBITMQ_MESSAGES_CONNECTION', 'rabbitmq_topic')",
                $contents
            );
        }

        $files->put($configPath, $contents);
    }

    /**
     * A config that still has both blocks has not been trimmed yet.
     */
    protected function wasCustomized(Filesystem $files, string $configPath): bool
    {
        if (! $files->exists($configPath)) {
            return false;
        }

        $contents = $files->get($configPath);

        foreach (static::CONNECTION_BLOCKS as $block) {
            if (! str_contains($contents, "// <$block>")) {
                return true;
            }
        }

        return false;
    }

    protected function suggestEnvironment(string $connections): void
    {
        $groups = $connections === 'all' ? ['common', 'fanout', 'topic'] : ['common', $connections];
        $lines = [];

        foreach ($groups as $group) {
            foreach (static::ENVIRONMENT[$group] as $key => $value) {
                $lines[] = "$key=$value";
            }
        }

        $this->components->info('Suggested variables for your .env / .env.example:');
        $this->line(implode(PHP_EOL, $lines).PHP_EOL);
    }
}
