# Changelog

## 2.0.0 (unreleased)

Breaking release: the library no longer depends on `vladimir-yuldashev/laravel-queue-rabbitmq`.
See README, "Migrating from v1", for the upgrade steps and every behaviour change.

### Added
- Own AMQP layer on top of `php-amqplib` (`AmqpConnectionManager`, `AmqpTopology`, `AmqpPublisher`)
  with host fail-over, separate publish/consume connections, one reconnect on transient failures and
  publisher confirms.
- `rabbitmq-messages` Laravel queue driver: consume with `php artisan queue:work <connection>`;
  `queue:listen`, `queue:clear`, `queue:monitor`, `queue:pause/resume/restart` supported.
- Clear "not supported" errors for queue operations and commands that do not apply to domain events,
  and warnings for ignored `queue:work` options (`--tries`, `--backoff`, `--delay`).
- `rabbitmq-messages:setup` command (declares exchanges, queues, bindings, retry and dead letter queues).
- Message rotation: one `rabbitmq-messages:rotate` command and one retention for both inbox and
  outbox (the optional `--retention-days` option, defaulting to `RABBITMQ_MESSAGES_RETENTION_DAYS`), automatic scheduler registration (enabled by default, daily) and a migration
  with `(status, updated_at)` indexes.
- `IncomingMessageInterface` to decouple the listener pipeline from the AMQP client.
- `outbox.*` configuration (queue connection/name, after-commit dispatch).
- Orchestra Testbench based test suite, feature tests against a real broker, GitHub Actions matrix,
  Pint configuration.

### Changed
- PHP `^8.2` (8.2 – 8.5) and Laravel 11, 12, 13.
- All settings live in `config/rabbitmq-messages.php`; `config/queue.php` is no longer published.
- Retries return to the failing consumer queue only (retry queue dead-letters through the default exchange).
- Dead letters are published directly to the dead letter queue.
- `rabbitmq-messages:install` no longer edits files by line number and never overwrites without confirmation.
- Worker scripts use `rabbitmq-messages:setup` and `queue:work`.

### Fixed
- Message validation checked the properties three times instead of payload and metadata.
- Dead letter queue was bound and published using the exchange type as routing key.
- Non-retriable messages already stored stayed `queued` when no dead letter queue was configured.
- Config was not merged when it had not been published (`mergeConfigFrom`).
- Implicitly nullable parameters (deprecated in PHP 8.4).

### Removed
- `RabbitMQQueue`, `RabbitMQJob`, `src/helpers.php`, the `ConsumeCommand` override, Laravel 10 and PHP 8.1 support.
