# Kalam CLI Project Workflow

This document covers the project-oriented `kalam` workflow implemented in `cli/src/workflow/`.
It focuses on how to create a project, manage day-to-day development, and move changes through
development and production-like environments.

## Scope

The workflow commands covered here are:

- `kalam init`
- `kalam link`
- `kalam schema gen`
- `kalam schema pull`
- `kalam migration create`
- `kalam migration status`
- `kalam db migrate`
- `kalam db reset`
- `kalam dev`
- `kalam status`
- `kalam deploy`

These commands are project-scoped and expect a `kalam.toml` file at the project root.

## Current Status

The workflow surface exists and is usable, but some parts are intentionally v1-level:

- `kalam init` scaffolds a project and now supports interactive selection menus for schema mode,
  language targets, and server mode.
- `kalam dev` can manage a local KalamDB server when `dev.auto_start_db = true`, or connect to
  an existing server when it is `false`.
- `kalam schema gen` delegates TypeScript generation to `@kalamdb/orm` against the resolved
  KalamDB environment. Dart generation reads local `schema.sql` and writes `KalamTableSpec`
  row codecs to `lib/generated/kalam.dart` (no live server required).
- `kalam migration create` creates ordered SQL migration files using the current schema diff helper.
- `kalam migration status` and `kalam db migrate` use local file-based migration state.
- `kalam deploy` performs guardrails, applies pending local migrations, runs a lightweight rollout
  step, and finishes with a health check.

Important current limitations:

- `kalam schema pull` is still a placeholder and currently returns a "start the server and retry"
  style error instead of pulling live schema into local files.
- `kalam db migrate` validates and tracks migration application locally; it does not yet apply
  migrations to a live remote database.
- `kalam deploy` does not yet perform provider-specific rollout automation. Today it is mainly a
  guarded workflow wrapper around migration state checks plus a health check.

## Mental Model

The workflow is centered around one project file:

- `kalam.toml` stores project metadata, environment URLs and namespaces, schema settings,
  migration settings, local dev orchestration, and workflow logging.

Secrets do not belong in `kalam.toml`.

Credentials remain in the normal CLI credential store under `~/.kalam/`. Workflow environment
credential instances are resolved by environment name as:

- `dev` -> `kalam-dev`
- `prod` -> `kalam-prod`

## Project Layout

`kalam init` scaffolds a project into the current directory, or into `--project-dir` when provided.

Typical scaffold:

```text
my-app/
├── kalam.toml
├── schema.sql
├── .env.example
├── .kalam/
│   └── server.toml        # local server mode only
├── kalam/
│   └── migrations/
│       └── .gitkeep
├── src/
│   └── generated/
│       └── kalam.ts       # when TypeScript is enabled
└── lib/
    └── generated/
        └── kalam.dart     # when Dart is enabled
```

## `kalam.toml`

Example:

```toml
[project]
name = "my-app"
default_env = "dev"

[connection.dev]
url = "http://localhost:2900"
namespace = "my-app"

[connection.prod]
url = "https://db.example.com"
namespace = "my-app"

[schema]
mode = "sql"
path = "schema.sql"
watch = true
languages = ["typescript", "dart"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"

[schema.targets.dart]
output = "lib/generated/kalam.dart"

[migrations]
dir = "kalam/migrations"
auto_create = true

[dev]
auto_start_db = true
apply_schema = true
generate_types = true
watch = true

[dev.processes]
frontend = "pnpm dev"
worker = "cargo run --bin worker"

[logging]
file = true
path = ".kalam/logs/kalam.log"
capture_process_output = true
```

## Environment Resolution

Workflow commands resolve environment values in this order:

1. CLI flag
2. Environment variable
3. `kalam.toml`
4. Fallback default `dev`

The supported workflow environment variables are:

- `KALAM_ENV`
- `KALAM_URL`
- `KALAM_NAMESPACE`

`kalam status` shows both the resolved values and where each one came from.

## Lifecycle Overview

### Development lifecycle

The intended loop is:

1. `kalam init`
2. edit `schema.sql`
3. `kalam schema gen`
4. `kalam migration create <name>`
5. `kalam db migrate`
6. `kalam dev`
7. repeat as the schema or local app processes change

### Production-like lifecycle

The intended promotion loop is:

1. `kalam link --env prod --url ... --namespace ...`
2. commit schema changes and migration files
3. verify local migration state
4. `kalam status --env prod`
5. `kalam deploy --env prod`

Production-like deploy currently means `prod`, `production`, or `staging`.

## Command Reference

### `kalam init`

Scaffolds a new KalamDB project.

#### Interactive flow

When run in a TTY without `--yes`, `kalam init` asks:

1. Project name
2. Schema mode
3. Language targets
4. Project template (TypeScript and/or Dart/Flutter `simple-live`)
5. Package manager when TypeScript is selected and more than one manager is available
6. Server mode
7. Server URL when server mode is `remote`

The interactive menus support:

- `Up` / `Down` to move
- `Space` to toggle multi-select options
- `Enter` to confirm
- `Esc` to cancel

Schema mode is a single-choice menu:

- `SQL file (schema.sql)`
- `Remote database schema`

Language targets are a multi-select menu:

- `TypeScript`
- `Dart / Flutter`

The project template menu includes two starter sources:

- embedded templates compiled into the CLI from `cli/templates/typescript/*` and `cli/templates/dart/*`
- repository examples downloaded from `examples/*` in the KalamDB GitHub repository

- `simple-live` (TypeScript) - live subscription starter with sample inserts
- `simple-live` (Dart / Flutter) - `kalam_sync` starter with `lib/main.dart` and generated table specs
- `live-okf-context-sync` - OKF folder sync with live FILE columns
- `realtime-ops-feed` - small browser app with live SQL subscriptions
- `chat-with-ai` - realtime multi-user React chat with SHARED rooms, RLS, and a topic agent
- `react-ai-chat` - personal AI assistant with USER tables, STREAM tokens, and approvals
- `summarizer-agent` - worker-only topic consumer that enriches rows

Agents should list templates instead of guessing ids:

```bash
kalam init --list-templates --json
kalam init --yes --template chat-with-ai --languages typescript --package-manager npm
kalam dev start --agent
```

`--list-templates` does not require an empty directory. `--json` prints `{ ok, cli_version, default_template, next, templates: [{ id, kind, language, description }] }`.

Repository examples are not bundled into the binary. A released `kalam` CLI still shows them in
`kalam init`; when a user selects one, the CLI downloads the matching `examples/<name>` folder
from the repository archive and writes it into the target project directory. `file:` `@kalamdb/*`
dependencies in that example's `package.json` are rewritten to the CLI version, lockfiles that pin
those paths are removed, and `.env.example` is copied to `.env` when `.env` is missing.

Server mode is a single-choice menu:

- `Local` - `kalam dev` starts or reuses a local KalamDB server
- `Remote` - `kalam dev` connects to an existing server URL

#### Options

- `--name <NAME>`: project name
- `--schema-mode <sql|remote>`: active schema source mode
- `--languages <LIST>`: comma-separated language list (`typescript`, `dart`; `ts` and `flutter` are aliases)
- `--template <ID>`: embedded template or repository example id
- `--list-templates`: print embedded templates and repository examples, then exit
- `--server-mode <local|remote>`: local server management mode for `kalam dev`
- `--server-url <URL>`: server URL for the scaffolded `dev` environment
- `--yes`: non-interactive mode; uses defaults for unspecified values
- `--project-dir <PATH>`: scaffold into a specific directory

#### Defaults

When `--yes` is used and no explicit values are supplied:

- schema mode: `sql`
- languages: `typescript`
- template: `simple-live`
- server mode: `local`
- server URL: `http://localhost:2900`
- project name: current directory name, or `my-app`

#### Example

```bash
kalam init \
  --yes \
  --name my-app \
  --schema-mode sql \
  --languages typescript,dart \
  --server-mode local
```

Remote mode example:

```bash
kalam init \
  --yes \
  --name my-app \
  --schema-mode sql \
  --languages typescript \
  --server-mode remote \
  --server-url https://db.example.com
```

### `kalam link`

Adds or updates a named environment entry in `kalam.toml`.

#### Options

- `--env <ENV>`: target environment name; defaults to `project.default_env`
- `--url <URL>`: server URL to store
- `--namespace <NAMESPACE>`: namespace to store
- `--project-dir <PATH>`: explicit project root

Both `--url` and `--namespace` are required together.

#### Example

```bash
kalam link --env prod --url https://db.example.com --namespace my-app
```

### `kalam schema gen`

Generates enabled language artifacts for the resolved workflow environment.

Current behavior:

- `typescript` is generated through `@kalamdb/orm` against the resolved `url` + `namespace`
- `dart` is generated locally from `schema.sql` into `KalamTableSpec` row codecs (no server required)

#### Options

- `--languages <LIST>`: limit generation to selected configured targets
- `--project-dir <PATH>`
- `--env <ENV>`

#### Example

```bash
kalam schema gen
kalam schema gen --languages typescript
kalam schema gen --languages dart
```

### `kalam schema pull`

Intended to sync remote schema into local project artifacts for remote mode.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`

#### Current behavior

The command exists, but the live pull implementation is still incomplete. At the moment it
returns an error telling you to start the server and retry.

### `kalam migration create`

Creates an ordered migration file in `kalam/migrations/`.

Migration names are normalized:

- lowercase
- non-alphanumeric characters become `_`
- leading and trailing `_` are removed

#### Arguments and options

- positional `<name>`: migration name
- `--project-dir <PATH>`
- `--env <ENV>`

#### Output

Each file contains:

- metadata header
- `-- UP`
- `-- DOWN`

#### Example

```bash
kalam migration create add_profile_table
```

### `kalam migration status`

Shows each migration file and whether it is `pending` or `applied`, based on local state.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`

#### Example

```bash
kalam migration status
```

### `kalam db migrate`

Applies pending migrations according to local migration state tracking.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`

#### Current behavior

This is v1 local apply behavior:

- reads files from `kalam/migrations/`
- skips already-applied migrations
- reads the `-- UP` section
- marks migrations as applied in local state

It does not yet execute the SQL against a live server.

#### Example

```bash
kalam db migrate
```

### `kalam db reset`

Clears local dev project state and, when appropriate, drops the linked namespace on the server so the next `kalam dev` can re-apply migrations cleanly.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`
- `--yes` — drop the namespace on a remote or non-project server without prompting

#### Behavior

**Local files removed** (when present):

- `kalam/server/` — entire local server directory (data, logs, and `server.toml`)
- `kalam/.schema-baseline.sql` — schema diff baseline

**Kept:**

- `kalam/migrations/` — migration SQL files on disk

**Server namespace drop** (when the linked KalamDB server is reachable):

| Server | Namespace drop |
|--------|----------------|
| This project's `kalam/server` on localhost (existed before reset) | Automatic |
| Another KalamDB process on localhost (reused URL, no local `kalam/server`) | Prompt (default No); use `--yes` |
| Remote URL (non-loopback) | Prompt (default No); use `--yes` |

Dropping the namespace clears tables and server-side migration records for that namespace.

If you decline the prompt, or run non-interactively without `--yes`, local files are still cleared but the server namespace is unchanged. The next `kalam dev` may report `migration failed previously` until you run `kalam db reset --yes` or repair manually.

Stop `kalam dev` first if it is running, so RocksDB files are not locked.

After reset, run `kalam dev` again. Pending migrations re-apply against a fresh server or dropped namespace.

#### Example

```bash
kalam db reset
kalam dev

# Non-interactive or reused localhost server
kalam db reset --yes
kalam dev
```

### `kalam dev`

Runs the local development orchestration loop.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`
- `--namespace <NAMESPACE>`
- `--force`: retry a paused schema pipeline once on startup
- `--agent`: run in deterministic, non-interactive mode optimized for AI coding agents and automation

```bash
kalam init --yes
kalam dev --agent
kalam -c "<SQL>" --json
```

`--agent` never waits for stdin, auto-downloads a missing compatible server, reuses a healthy server before resolving a local binary, auto-applies ordinary development schema changes, and emits compact `KALAM_*` events with stable `KALAM_ERROR` codes. Destructive schema changes return `DESTRUCTIVE_SCHEMA_CHANGE` unless `--force` is also passed. Human `kalam dev` remains interactive.

#### Background session

`kalam dev` still runs in the foreground by default. These commands manage a detached copy of the same foreground loop (always spawned as `kalam dev --agent`, so the background session never waits for stdin):

```bash
kalam dev start --agent
kalam dev status
kalam dev logs
kalam dev logs --follow
kalam dev stop
```

- `start` is idempotent: a live session for the same project is reused (`KALAM_DEV_REUSED`)
- `status` reports `running` or `stopped` and exits 0 in both cases
- `stop` is idempotent
- session metadata is stored at `kalam/cli/dev.session.json`
- logs are the existing workflow log at `kalam/cli/logs/kalam.log`
- each `start` appends `--- kalam dev start ---`; ready/error waits ignore older `KALAM_*` lines above that marker

This is a PID file plus a detached child of the same `kalam` binary, not a machine-wide daemon.

#### Behavior

`kalam dev` does the following:

1. resolves the active environment
2. starts or reuses a local server when `dev.auto_start_db = true`
3. runs the schema pipeline when `dev.apply_schema` or `dev.generate_types` is enabled
4. starts all configured `[dev.processes]`
5. watches the schema file when watch is enabled
6. streams managed process logs with stable source prefixes
7. shuts down supervised processes on `Ctrl+C`

#### Local server mode

When `dev.auto_start_db = true`:

- `kalam dev` first checks whether the configured URL is already healthy
- if healthy, it reuses the running server
- otherwise it tries to start `kalamdb-server`

Local server requirements:

- `kalamdb-server` must be available on `PATH`, or
- `KALAMDB_SERVER_BIN` must point to the binary

The local config file used for this is:

- `.kalam/server.toml`

The generated local server config uses:

- host `127.0.0.1`
- the port from the configured environment URL
- storage under `kalam/server/data`
- logs under `kalam/server/logs`
- default credentials `root` / `kalamdb123` in `kalam/server/server.toml`

Scaffolded `.env` and `.env.example` files include `KALAM_USER=root` and
`KALAM_PASSWORD=kalamdb123` for local mode. `kalam dev` injects the project
`.env` into `[dev.processes]` commands, and the TypeScript starter loads `.env`
so `npm run dev` also sees `KALAM_PASSWORD`.

If something is already listening on the project URL (for example another
`kalamdb-server` or `cargo run`), `kalam dev` reuses that process and prints a
warning instead of starting `kalam/server`. Stop the other server if you
expected a fresh local instance for this project.

#### Accessing the local server

While `kalam dev` is running, the managed server uses your dev URL (default
`http://localhost:2900`). Sign in with **`root` / `kalamdb123`**:

- Admin UI: http://localhost:2900/ui
- CLI: `kalam --url http://127.0.0.1:2900 --user root --password kalamdb123`
- Health: `curl http://127.0.0.1:2900/v1/api/auth/status`

#### Remote server mode

When `dev.auto_start_db = false`:

- `kalam dev` never tries to start a server
- it just uses the resolved environment URL

#### Schema pipeline

The schema pipeline currently combines:

- `kalam db migrate`
- `kalam schema gen`
- schema baseline update used by migration diffing

If the schema pipeline fails:

- the failure is shown in the running console
- only the schema pipeline pauses
- managed child processes keep running
- `kalam dev --force` retries the pipeline once on startup

#### Examples

```bash
kalam dev
kalam dev --force
kalam dev --env prod
```

### `kalam status`

Reports the resolved project status for the active environment.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`
- `--namespace <NAMESPACE>`

#### Output includes

- project name
- resolved environment name
- resolved URL
- resolved namespace
- schema mode
- schema source path or `(remote)`
- generated targets
- migration counts

#### Example

```bash
kalam status
kalam status --env prod
```

### `kalam deploy`

Runs guarded deployment flow for the selected environment.

#### Options

- `--project-dir <PATH>`
- `--env <ENV>`

#### Current behavior

The deploy flow is:

1. resolve environment
2. validate deploy readiness
3. apply pending local migrations
4. run a lightweight rollout step
5. perform `GET {url}/ui` health check

#### Guardrails

Deploy is blocked when:

- there are pending migrations
- the target environment name is production-like and schema drift exists without committed
  migration history

Production-like means:

- `prod`
- `production`
- `staging`

#### Health check

Success is accepted for:

- any `2xx`
- any `3xx`

#### Example

```bash
kalam deploy --env prod
```

## Dev Environment Lifecycle

### Local dev with managed server

Use this when your app should run its own KalamDB instance during development.

Recommended flow:

```bash
kalam init

# edit schema.sql
kalam schema gen
kalam migration create init_schema
kalam db migrate

# run the local loop
kalam dev
```

Suggested `kalam.toml` shape:

```toml
[connection.dev]
url = "http://localhost:2900"
namespace = "my-app"

[dev]
auto_start_db = true
apply_schema = true
generate_types = true
watch = true

[dev.processes]
frontend = "pnpm dev"
```

Notes:

- if a server is already running at the configured URL, `kalam dev` reuses it
- otherwise it starts one using `.kalam/server.toml`
- if `kalamdb-server` cannot be found, `kalam dev` fails immediately

### Dev against an existing remote server

Use this when development happens against a shared or pre-existing server.

Recommended flow:

```bash
kalam init --server-mode remote --server-url http://localhost:2900
kalam link --env dev --url http://localhost:2900 --namespace my-app
kalam dev
```

Suggested `kalam.toml` shape:

```toml
[connection.dev]
url = "http://localhost:2900"
namespace = "my-app"

[dev]
auto_start_db = false
apply_schema = true
generate_types = true
watch = true
```

## Prod Environment Lifecycle

The recommended production-like flow today is conservative:

1. define and commit schema changes locally
2. create a migration file
3. verify migration state locally
4. link the production environment
5. inspect the resolved production state
6. deploy with guardrails

Example:

```bash
# one-time setup
kalam link --env prod --url https://db.example.com --namespace my-app

# for each change
kalam migration create add_billing_fields
kalam migration status
kalam status --env prod
kalam deploy --env prod
```

Because `kalam deploy` is still lightweight, treat it as a guarded workflow helper rather than a
complete release system.

## Day-to-Day Recipes

### Start a new local project

```bash
kalam init
kalam dev
```

### Add a schema change

```bash
# edit schema.sql
kalam schema gen
kalam migration create add_comments
kalam db migrate
```

### Add production environment wiring

```bash
kalam link --env prod --url https://db.example.com --namespace my-app
kalam status --env prod
```

### Deploy to production-like environment

```bash
kalam deploy --env prod
```

## Troubleshooting

### `interactive init requires a TTY`

You ran `kalam init` without a terminal attached.

Use:

```bash
kalam init --yes ...
```

### `kalamdb-server not found on PATH`

This happens when `dev.auto_start_db = true` and `kalam dev` cannot find the server binary.

Fix by either:

- installing `kalamdb-server` on `PATH`, or
- setting `KALAMDB_SERVER_BIN=/absolute/path/to/kalamdb-server`

### `deploy blocked: schema changes require a committed migration before production deploy`

Production-like environments (`prod`, `production`, `staging`) refuse to deploy when `schema.sql` differs from the baseline and no matching migration file exists.

Create and commit a migration first:

```bash
kalam db migrate
```

### `schema pipeline paused`

A migration or schema-generation step failed during `kalam dev`.

To retry once at startup:

```bash
kalam dev --force
```

## Related Files

- `cli/src/workflow/project/init.rs`
- `cli/src/workflow/project/config.rs`
- `cli/src/workflow/project/resolve.rs`
- `cli/src/workflow/project/link.rs`
- `cli/src/workflow/project/status.rs`
- `cli/src/workflow/schema/gen.rs`
- `cli/src/workflow/schema/load.rs`
- `cli/src/workflow/migration/create.rs`
- `cli/src/workflow/migration/apply.rs`
- `cli/src/workflow/dev/orchestrator.rs`
- `cli/src/workflow/dev/server.rs`
- `cli/src/workflow/deploy/mod.rs`
