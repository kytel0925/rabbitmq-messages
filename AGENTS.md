# AGENTS.md

Instructions for AI coding agents working on this repository.

**Read and follow [CONTRIBUTING.md](CONTRIBUTING.md).** It is the single source of truth for the
development workflow, the commit message style, authorship and signing, and pull requests. The rules
below summarize what agents most often get wrong; if they ever disagree, CONTRIBUTING.md wins.

## Non-negotiable rules

1. **Commit messages follow the [gitmoji specification](https://gitmoji.dev/specification)**:
   `<intention> [scope?][:?] <message>`, with a unicode emoji, an optional `(scope):`, an imperative subject of
   at most 72 characters and an optional body. See the table in CONTRIBUTING.md.
2. **Never add `Co-Authored-By` trailers** to commits, and never put `Co-Authored-By` lines in pull request
   descriptions.
3. **Author and committer** are the maintainer: `Telmo Riofrio <kytel0925@gmail.com>`, unless the human you are
   working for tells you otherwise.
4. **Signing:** commits must be signed with the maintainer's own key. An agent that does not have that key
   leaves the commits **unsigned** and tells the maintainer, so they can sign them locally with
   `git rebase --exec 'git commit --amend --no-edit -S' origin/main` and force-push the branch.
   Never sign with any other key, such as one provided by the agent's environment.
5. Run `vendor/bin/phpunit` and `vendor/bin/pint --test` before pushing. Make sure the broker-backed feature
   tests actually ran and were not skipped.
6. Never push to `main`, never force-push branches you did not create, and use `--force-with-lease`.

## Project map

| Path | What lives there |
|---|---|
| `src/Amqp` | php-amqplib integration: connections, topology, publisher, incoming message |
| `src/Queue` | `rabbitmq-messages` Laravel queue driver (consume side) |
| `src/Pipes`, `src/Builders` | Inbox pipeline: validation, handler lookup, idempotency, retry, dead letter |
| `src/Services`, `src/Jobs` | Outbox dispatcher and publish job, resiliency, message rotation |
| `src/Console` | `rabbitmq-messages:*` commands and the `queue:*` command guard |
| `config/rabbitmq-messages.php` | All configuration (never publish or edit `config/queue.php`) |
| `database/migrations` | `dispatch_message` / `listen_message` tables and indexes |
| `tests/Unit`, `tests/Feature` | Testbench tests; Feature tests need RabbitMQ (`RABBITMQ_*` env) |
