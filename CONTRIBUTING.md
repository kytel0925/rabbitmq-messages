# Contributing

Thanks for helping improve **rabbitmq-messages**. These rules apply to every contributor, human or AI agent.

## Development workflow

```bash
docker compose up -d                          # PHP 8.5 dev container + RabbitMQ
docker compose exec php.dev composer install
docker compose exec php.dev vendor/bin/phpunit     # unit + broker-backed feature tests
docker compose exec php.dev vendor/bin/pint        # code style (CI runs `pint --test`)
```

* Every change ships with tests. `tests/Unit` needs no broker; `tests/Feature` runs against a real RabbitMQ
  and is skipped when the broker is unreachable, so make sure it actually ran before you push.
* Keep `README.md` and `CHANGELOG.md` up to date with user-facing changes (configuration, commands,
  behaviour, supported versions).
* Supported versions: PHP 8.2 – 8.5 and Laravel 11, 12, 13. Do not use language features or framework APIs
  outside that range.

## Commit messages: gitmoji

Commits follow the [gitmoji specification](https://gitmoji.dev/specification):

```text
<intention> [scope?][:?] <message>
```

* **intention**: one emoji from the [gitmoji list](https://gitmoji.dev), written as the **unicode** character
  (not the `:shortcode:`) for a readable `git log`.
* **scope** (optional): the area of the code, in parentheses and followed by a colon, e.g. `(rotation):`.
* **message**: a short imperative summary starting with a capital letter, no trailing period. Keep the
  whole subject line within 72 characters.
* A body is optional. Separate it from the subject with a blank line and explain *what* and *why*.
* One intention per commit. Split unrelated changes into separate commits.

Common intentions in this repository:

| Emoji | Code | Use it for |
|---|---|---|
| 💥 | `:boom:` | Breaking changes (public API, configuration, broker topology, supported versions) |
| ✨ | `:sparkles:` | New features |
| 🐛 | `:bug:` | Bug fixes |
| ♻️ | `:recycle:` | Refactoring without behaviour changes |
| ✅ | `:white_check_mark:` | Adding or updating tests |
| 📝 | `:memo:` | Documentation |
| 🔧 | `:wrench:` | Configuration files |
| 👷 | `:construction_worker:` | CI |
| 🗃️ | `:card_file_box:` | Database migrations |
| ➕ / ➖ / ⬆️ | `:heavy_plus_sign:` / `:heavy_minus_sign:` / `:arrow_up:` | Add / remove / upgrade dependencies |
| 🔥 | `:fire:` | Removing code or files |
| 🎨 | `:art:` | Code structure or format |
| 🚚 | `:truck:` | Moving or renaming files |
| 🗑️ | `:wastebasket:` | Deprecations |
| 🔖 | `:bookmark:` | Releases and version tags |

Suggested scopes: `amqp`, `queue`, `outbox`, `inbox`, `resiliency`, `rotation`, `console`, `config`, `docs`,
`tests`, `ci`, `deps`.

Examples:

```text
✨ (rotation): Add --retention-days option to rabbitmq-messages:rotate
🐛 (amqp): Reopen the publish channel after a broker nack
📝 Document the queue:* compatibility table
💥 Drop Laravel 10 support
```

## Authorship and signing

* Commits are authored and committed with **your own** name and e-mail. For the maintainer that is
  `Telmo Riofrio <kytel0925@gmail.com>`.
* Commits must be **signed** with your own SSH or GPG key, and the matching public key must be registered in
  your GitHub account, so GitHub shows them as *Verified*. For example:

  ```bash
  git config user.name "Your Name"
  git config user.email "you@example.com"
  git config gpg.format ssh
  git config user.signingkey ~/.ssh/id_ed25519.pub
  git config commit.gpgsign true
  ```

* Never sign commits with somebody else's key.
* **No `Co-Authored-By` trailers** in commit messages, and no `Co-Authored-By` lines in pull request
  descriptions.

## Pull requests

* Open them against `main` from a topic branch. Never push directly to `main`.
* The PR title follows the same gitmoji format as commit subjects.
* The description explains what changed and why, the upgrade impact for existing users (if any), and how it
  was tested. It must state honestly what was *not* verified.
* Rewrite history only on your own branch. Use `git push --force-with-lease`, never a plain `--force`.
