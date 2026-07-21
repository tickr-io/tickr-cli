# Tickr CLI

Tickr CLI is a local runtime for authoring Tickr workflows, running execution
services, and observing runtime state. This repository provides:

- the `tickr` command;
- conductor, API, and executor services;
- a Nickel workflow DSL;
- public protobuf and OpenAPI contracts;
- the browser-based Tickr Console;
- a local development formation using PostgreSQL, NATS/JetStream, and
  S3-compatible object storage.

The current release is distributed from source and is intended for developers
building or integrating the Tickr runtime.

## Install the command

With Rust 1.92 and the Protocol Buffers compiler (`protoc`) installed:

```sh
git clone https://github.com/tickr-io/tickr-cli.git
cd tickr-cli
cargo install --locked --path .
tickr --help
```

The command provides the `conductor`, `api`, `executor`, and `migrate`
subcommands. A complete local formation also needs the services listed below.

## Quickstart

Prerequisites:

- Docker with modern `docker compose`;
- [Just](https://github.com/casey/just);
- [Overmind](https://github.com/DarthSim/overmind);
- Rust 1.92 and the Protocol Buffers compiler (`protoc`);
- Node.js 24 and npm;
- [Nix](https://nixos.org/download/) for building and running workflow tasks.

From a fresh clone:

```sh
just console-install
just up-bg
just verify
```

Then open the Console at <http://127.0.0.1:3000>. The API is available at
<http://127.0.0.1:6000>, with its generated OpenAPI document at
<http://127.0.0.1:6000/api-docs/openapi.json>.

Stop the formation without deleting state:

```sh
just down
```

Use `just fresh` when you explicitly want to delete this checkout's local
PostgreSQL, JetStream, object-storage, and log state before starting again.

## Validate a workflow

Install the [Nickel](https://nickel-lang.org/) CLI, then validate the checked-in
example:

```sh
just dsl-check examples/hello-world.ncl
```

The example demonstrates the authored workflow shape without requiring a
running formation. Registering, scheduling, and executing workflows requires a
compatible Tickr coordinator; see [Architecture](docs/architecture.md) for the
integration endpoints and standalone behavior.

## Runtime commands

```sh
tickr conductor
tickr api
tickr executor
tickr migrate
```

The repository formation runs these components through Overmind:

```sh
just up              # foreground
just up-bg           # background
just ps
just logs conductor
just restart api       # restart one application process
just restart all       # restart the complete formation; preserve stored state
just verify
```

`just restart all` restarts API, conductor, executor, Console, PostgreSQL,
NATS/JetStream, and MinIO. It uses the normal `down`/`up-bg` lifecycle and does
not delete volumes or the checkout's local state; only `just fresh` is destructive.

`just verify` checks exactly the local runtime: the three Rust services, Console,
PostgreSQL schema, NATS/JetStream, MinIO, API readiness, and Console readiness.
A coordinator is not started by this repository. Without one, coordinator-backed
health and live-data views are expected to report unavailable or degraded while
the local formation remains ready.

Checkout-local overrides belong in ignored `.env.local`. The tracked `.envrc`
contains loopback-only development defaults and is sourced by formation recipes,
so prior `direnv allow` is optional.

## Data-plane SQL storage

The Conductor writes Data-plane SQL state and the API reads the same selected
repository. Run `tickr migrate` with the same environment before starting either
process.

- **Postgres (default):** leave `TICKR_SQL_BACKEND` unset, or set it to
  `postgres`, and set `TICKR_CONDUCTOR_POSTGRES_URL`. Postgres does not require
  or interpret `TICKR_SQL_TOPOLOGY`.
- **SQLite (single node):** set `TICKR_SQL_BACKEND=sqlite`,
  `TICKR_SQL_TOPOLOGY=single-node`, and an explicit
  `TICKR_CONDUCTOR_SQLITE_URL`, for example
  `sqlite:///var/lib/tickr/data-plane.db`. Missing or different topology is
  rejected before the Conductor starts consumers or the API starts serving.

Place the SQLite file on durable storage local to the host. The migration
command creates and migrates it; the API opens it read-only and never creates
or repairs it. The supported formation has one Conductor writer and file-local
API readers. Network filesystems, multiple Conductor writers, replication, and
distributed SQLite are not supported.

Restart the Conductor and API against the same file to retain definitions,
terminal archives, Events, Signal audit state, Patches, replays, and Run
calendar placement. `/api/health` reports the selected implementation and
repository status under `data_plane_sql`.

Operators own backup and restore. Coordinate backups with SQLite's WAL: stop
SQL consumers cleanly or use a SQLite-aware online backup that includes
committed WAL state. Copying only the main database file while a writer is
active is not a valid backup. NATS JetStream and object storage remain separate
durable systems and require their own backup policies.

## Development

```sh
just build
just check
just test
just console-test
just console-build
just security
```

`just test` requires Docker and starts the shared development infrastructure so
integration coverage cannot silently pass without PostgreSQL. Generated OpenAPI,
TypeScript bindings, public protobuf identity, source-security policy, dependency
advisories, license attribution, and secret scanning are checked in CI.

Run `just install-hooks` after cloning if you want the repository checks enforced
locally before every commit.

## Documentation

- [Architecture](docs/architecture.md)
- [Production hardening](docs/production-hardening.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [License](LICENSE) and [third-party notices](THIRD_PARTY_NOTICES.md)

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
