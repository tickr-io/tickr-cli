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

Tagged releases publish self-contained Tickr Lite archives for Linux x86-64,
Linux ARM64, and Apple silicon. Each archive contains the `tickr` data-plane
executable, the `tickr-ctx` Task-context helper, the embedded Console, the
version-matched Core DSL, two runnable onboarding workflows, license notices,
and an adjacent SHA-256 checksum.

Download the archive for your platform from
[GitHub Releases](https://github.com/tickr-io/tickr-cli/releases), verify and
extract it, then follow its `INSTALL.md`. The guide connects Tickr Lite to an
existing compatible Control plane and takes both the Hello and deterministic
runtime-Patch workflows from registration through a completed Run.

The archive needs no source checkout or external Console/DSL asset directory.
Nix remains a host prerequisite for building and running workflow Tasks; the
guide installs the pinned Nickel evaluator used to parse workflow and Patch
source.

To install from source, use Rust 1.92, Node.js 24.16.0, npm 11.13.0, and the
Protocol Buffers compiler (`protoc`):

```sh
git clone https://github.com/tickr-io/tickr-cli.git
cd tickr-cli
cargo install --locked --path .
tickr --help
```

The same command also provides the distributed `conductor`, `api`, `executor`,
and `migrate` surfaces.

## Quickstart

Prerequisites:

- Docker with modern `docker compose`;
- [Just](https://github.com/casey/just);
- [Overmind](https://github.com/DarthSim/overmind);
- Rust 1.92 and the Protocol Buffers compiler (`protoc`);
- Node.js 24.16.0 and npm 11.13.0;
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
compatible Tickr Control plane; see [Architecture](docs/architecture.md) for the
HTTP subquery channel, Conductor relay, and standalone behavior.

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
A Control plane is not started by this repository. Without one, Control-plane
health and HTTP-subquery live-data views are expected to report unavailable or
degraded while the local formation remains ready.

Checkout-local overrides belong in ignored `.env.local`. The tracked `.envrc`
contains loopback-only development defaults and is sourced by formation recipes,
so prior `direnv allow` is optional.

## Data-plane SQL storage

The Conductor writes Data-plane SQL state and the API reads the same selected
repository. Run `tickr-cli migrate` with the same environment before starting either
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

- [Tickr Documentation](https://tickr-io.github.io/tickr-cli/) — install, author,
  operate, integrate, and reference guides for supported release lines
- [Architecture](docs/architecture.md)
- [Production hardening](docs/production-hardening.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [License](LICENSE) and [third-party notices](THIRD_PARTY_NOTICES.md)

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
