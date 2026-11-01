<?php

namespace SlothDevGuy\RabbitMQMessagesTests\Unit;

use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use SlothDevGuy\RabbitMQMessagesTests\TestCase;

class MessageRotationTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Carbon::setTestNow('2026-11-01 12:00:00');

        $this->insert('listen_message', 'processed', 40);
        $this->insert('listen_message', 'processed', 10);
        $this->insert('listen_message', 'failed', 100);
        $this->insert('listen_message', 'failed', 40);
        $this->insert('listen_message', 'queued', 400);
        $this->insert('dispatch_message', 'dispatched', 31);
        $this->insert('dispatch_message', 'dispatched', 29);
        $this->insert('dispatch_message', 'created', 400);
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();

        parent::tearDown();
    }

    public function testRotateUsesTheConfiguredRetention(): void
    {
        $this->artisan('rabbitmq-messages:rotate')->assertSuccessful();

        // processed > 30 days and failed > 90 days are gone; queued/created are kept forever
        $this->assertEquals(['processed' => 1, 'failed' => 1, 'queued' => 1], $this->countByStatus('listen_message'));
        $this->assertEquals(['dispatched' => 1, 'created' => 1], $this->countByStatus('dispatch_message'));
    }

    public function testPretendDoesNotDelete(): void
    {
        $this->artisan('rabbitmq-messages:rotate', ['--pretend' => true])
            ->expectsOutputToContain('would be deleted')
            ->assertSuccessful();

        $this->assertDatabaseCount('listen_message', 5);
        $this->assertDatabaseCount('dispatch_message', 3);
    }

    public function testFiltersAndOverrides(): void
    {
        $this->artisan('rabbitmq-messages:rotate', [
            '--table' => ['listen_message'],
            '--status' => ['failed'],
            '--days' => 30,
            '--chunk' => 1,
        ])->assertSuccessful();

        $this->assertEquals(['processed' => 2, 'queued' => 1], $this->countByStatus('listen_message'));
        $this->assertDatabaseCount('dispatch_message', 3);
    }

    public function testInvalidOptionsFail(): void
    {
        $this->artisan('rabbitmq-messages:rotate', ['--table' => ['jobs']])->assertFailed();
        $this->artisan('rabbitmq-messages:rotate', ['--days' => 'abc'])->assertFailed();
    }

    public function testDisabledRotationDoesNothingUnlessForced(): void
    {
        Config::set('rabbitmq-messages.rotation.enabled', false);

        $this->artisan('rabbitmq-messages:rotate')->assertSuccessful();
        $this->assertDatabaseCount('listen_message', 5);

        $this->artisan('rabbitmq-messages:rotate', ['--force' => true])->assertSuccessful();
        $this->assertDatabaseCount('listen_message', 3);
    }

    public function testRotationIsScheduled(): void
    {
        $events = collect($this->app->make(Schedule::class)->events())
            ->filter(fn ($event) => str_contains((string) $event->command, 'rabbitmq-messages:rotate'));

        $this->assertCount(1, $events);
        $this->assertEquals('30 3 * * *', $events->first()->expression);
        $this->assertTrue($events->first()->withoutOverlapping);
    }

    protected function insert(string $table, string $status, int $daysAgo): void
    {
        $date = Carbon::now()->subDays($daysAgo);

        DB::table($table)->insert([
            'uuid' => (string) Str::uuid(),
            'name' => 'foo.bar.baz',
            'status' => $status,
            'properties' => '{}',
            'payload' => '{}',
            'metadata' => '{}',
            'created_at' => $date,
            'updated_at' => $date,
        ]);
    }

    protected function countByStatus(string $table): array
    {
        return DB::table($table)->selectRaw('status, count(*) as total')->groupBy('status')
            ->pluck('total', 'status')->map(fn ($total) => (int) $total)->all();
    }
}
