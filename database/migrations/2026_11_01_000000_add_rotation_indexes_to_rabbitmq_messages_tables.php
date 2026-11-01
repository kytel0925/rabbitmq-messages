<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Schema;

/**
 * Composite indexes used by `php artisan rabbitmq-messages:rotate`
 * (WHERE status = ? AND updated_at < ?).
 */
return new class extends Migration
{
    public function __construct()
    {
        $this->connection = Config::get('rabbitmq-messages.database_connection');
    }

    public function up(): void
    {
        Schema::connection($this->getConnection())->table('dispatch_message', function (Blueprint $table) {
            $table->index(['status', 'updated_at'], 'dispatch_message_status_updated_at_index');
        });

        Schema::connection($this->getConnection())->table('listen_message', function (Blueprint $table) {
            $table->index(['status', 'updated_at'], 'listen_message_status_updated_at_index');
        });
    }

    public function down(): void
    {
        Schema::connection($this->getConnection())->table('dispatch_message', function (Blueprint $table) {
            $table->dropIndex('dispatch_message_status_updated_at_index');
        });

        Schema::connection($this->getConnection())->table('listen_message', function (Blueprint $table) {
            $table->dropIndex('listen_message_status_updated_at_index');
        });
    }
};
