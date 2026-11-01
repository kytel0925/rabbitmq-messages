<?php

namespace SlothDevGuy\RabbitMQMessages\Console;

use Illuminate\Console\Events\CommandStarting;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\Exceptions\UnsupportedOperationException;
use SlothDevGuy\RabbitMQMessages\Support\MessagesConfig;
use Symfony\Component\Console\Command\Command as SymfonyCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputInterface;
use Throwable;

/**
 * Explains, before they run, how `php artisan queue:*` commands behave
 * against a "rabbitmq-messages" connection:
 *
 * - supported commands run normally;
 * - options that have no effect on domain events print a warning;
 * - any other connection-scoped queue command aborts with "not supported".
 *
 * Commands that operate on Laravel's failed_jobs table (queue:failed,
 * queue:retry, queue:forget, queue:flush, queue:prune-failed) are global and
 * keep working for the outbox jobs; re-pushing a job onto a rabbitmq-messages
 * connection fails with an UnsupportedOperationException raised by the driver.
 */
class QueueCommandGuard
{
    /**
     * Supported commands => options accepted by Laravel but ignored by the driver.
     */
    public const SUPPORTED = [
        'queue:work' => ['tries', 'backoff', 'delay'],
        'queue:listen' => ['tries', 'backoff', 'delay'],
        'queue:clear' => [],
        'queue:monitor' => [],
        'queue:pause' => [],
        'queue:resume' => [],
        'queue:continue' => [],
    ];

    public const IGNORED_OPTION_HINTS = [
        'tries' => 'retries are governed by rabbitmq-messages.max_tries and the retry queue',
        'backoff' => 'the retry delay is rabbitmq-messages.connections.*.retry.delay',
        'delay' => 'the retry delay is rabbitmq-messages.connections.*.retry.delay',
    ];

    public function __construct(
        protected Container $container,
    ) {}

    /**
     * @throws UnsupportedOperationException
     */
    public function handle(CommandStarting $event): void
    {
        if (! str_starts_with($event->command, 'queue:')) {
            return;
        }

        $input = $this->boundInput($event);
        if (! $input) {
            return;
        }

        $connections = array_values(array_filter($this->targetConnections($input), [$this, 'usesDriver']));
        if (empty($connections)) {
            return;
        }

        $connection = $connections[0];

        if (! array_key_exists($event->command, static::SUPPORTED)) {
            throw UnsupportedOperationException::queueOperation(
                $event->command,
                $connection,
                'Supported queue commands: '.implode(', ', array_keys(static::SUPPORTED)).'.'
            );
        }

        foreach (static::SUPPORTED[$event->command] as $option) {
            if ($input->hasParameterOption("--$option", true)) {
                $hint = static::IGNORED_OPTION_HINTS[$option] ?? 'it has no effect on domain events';
                $event->output->writeln(
                    "<comment>[rabbitmq-messages] Option --$option is not supported for connection [$connection] and will be ignored: $hint.</comment>"
                );
            }
        }
    }

    /**
     * Connection names targeted by the command.
     *
     * @return array<int, string>
     */
    protected function targetConnections(InputInterface $input): array
    {
        $default = (string) Config::get('queue.default');

        if ($input->hasArgument('connection')) {
            return [(string) ($input->getArgument('connection') ?: $default)];
        }

        // queue:monitor {queues}, queue:pause {queue} ... use "connection:queue"
        foreach (['queues', 'queue'] as $argument) {
            if ($input->hasArgument($argument) && is_string($value = $input->getArgument($argument))) {
                return array_map(function (string $queue) use ($default) {
                    return str_contains($queue, ':') ? explode(':', $queue, 2)[0] : $default;
                }, array_filter(array_map('trim', explode(',', $value))));
            }
        }

        return [];
    }

    protected function usesDriver(string $connection): bool
    {
        return Config::get("queue.connections.$connection.driver") === MessagesConfig::DRIVER;
    }

    /**
     * Laravel >= 13 dispatches CommandStarting with the input already bound to
     * the command definition; older versions dispatch it before binding.
     */
    protected function boundInput(CommandStarting $event): ?InputInterface
    {
        $input = $event->input;

        if ($input->hasArgument('connection') || $input->hasArgument('queues') || $input->hasArgument('queue')) {
            return $input;
        }

        try {
            $command = $this->container->make(Kernel::class)->all()[$event->command] ?? null;
            if (! $command instanceof SymfonyCommand) {
                return null;
            }

            $definition = new InputDefinition;
            $application = $command->getApplication()?->getDefinition();

            $definition->addArguments($application?->getArguments() ?? [new InputArgument('command')]);
            $definition->addArguments($command->getDefinition()->getArguments());
            $definition->addOptions($application?->getOptions() ?? []);
            $definition->addOptions($command->getDefinition()->getOptions());

            $bound = clone $input;
            $bound->bind($definition);

            return $bound;
        } catch (Throwable) {
            // never get in the way of the command itself
            return null;
        }
    }
}
