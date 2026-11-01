<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Unit;

use Illuminate\Console\Events\CommandStarting;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Support\Facades\Config;
use SlothDevGuy\RabbitMQMessages\Console\QueueCommandGuard;
use SlothDevGuy\RabbitMQMessages\Exceptions\UnsupportedOperationException;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

class QueueCommandGuardTest extends TestCase
{
    public function testIgnoredWorkerOptionsPrintAWarning(): void
    {
        $output = $this->guard('queue:work', ['connection' => 'rabbitmq', '--tries' => 5, '--backoff' => 3]);

        $this->assertStringContainsString('Option --tries is not supported for connection [rabbitmq] and will be ignored', $output);
        $this->assertStringContainsString('Option --backoff is not supported', $output);
    }

    public function testUnboundArgvInputIsUnderstood(): void
    {
        // Laravel 11/12 dispatch CommandStarting before binding the input
        $input = new ArgvInput(['artisan', 'queue:work', 'rabbitmq', '--tries=5', '--once']);
        $output = new BufferedOutput;

        $this->app->make(QueueCommandGuard::class)->handle(new CommandStarting('queue:work', $input, $output));

        $this->assertStringContainsString('Option --tries is not supported', $output->fetch());
    }

    public function testSupportedUsageIsSilent(): void
    {
        $this->assertSame('', $this->guard('queue:work', ['connection' => 'rabbitmq', '--queue' => 'foo', '--once' => true]));
        $this->assertSame('', $this->guard('queue:clear', ['connection' => 'rabbitmq', '--force' => true]));
        $this->assertSame('', $this->guard('queue:monitor', ['queues' => 'rabbitmq:foo,rabbitmq_topic:bar']));
    }

    public function testOtherConnectionsAreIgnored(): void
    {
        $this->assertSame('', $this->guard('queue:work', ['connection' => 'database', '--tries' => 5]));
        $this->assertSame('', $this->guard('queue:work', ['--tries' => 5])); // default: sync
    }

    public function testUnsupportedConnectionScopedCommandsAreRejected(): void
    {
        $this->app->make(Kernel::class)->registerCommand(
            new class extends Command
            {
                public function __construct()
                {
                    parent::__construct('queue:custom');
                    $this->addArgument('connection');
                }
            }
        );

        $this->expectException(UnsupportedOperationException::class);
        $this->expectExceptionMessage('[queue:custom] is not supported by the rabbitmq-messages driver (connection [rabbitmq])');

        $this->guard('queue:custom', ['connection' => 'rabbitmq']);
    }

    public function testDefaultConnectionIsResolved(): void
    {
        Config::set('queue.default', 'rabbitmq');

        $this->assertStringContainsString('--tries', $this->guard('queue:work', ['--tries' => 5]));
    }

    protected function guard(string $command, array $parameters): string
    {
        $input = new ArrayInput(['command' => $command] + $parameters);
        $output = new BufferedOutput;

        $this->app->make(QueueCommandGuard::class)->handle(new CommandStarting($command, $input, $output));

        return $output->fetch();
    }
}
