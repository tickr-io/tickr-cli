# Architecture

Tickr CLI is the tenant-side workflow runtime. It provides the `tickr` command,
three runtime processes, the Console, and the public contracts used to connect a
Tickr coordinator.

## Components

- **Conductor** accepts workflow commands, validates Nickel definitions, builds
  task specifications, maintains tenant coordination state in NATS, and archives
  terminal runs in PostgreSQL.
- **API** exposes the HTTP and OpenAPI surface used by operators and the Console.
  Archived reads come from PostgreSQL; live reads are merged from the configured
  coordinator when it is available.
- **Executor** pulls runnable tasks, executes them, publishes lifecycle events,
  and ships task logs.
- **Console** is the browser interface served by Vite during local development.
- **Infrastructure** consists of PostgreSQL, NATS/JetStream, and S3-compatible
  object storage. The repository-local Compose file provides development-only
  instances of these services.

## Coordinator interface

The local formation is useful on its own for development, contract validation,
archived-data APIs, and Console/API work. End-to-end scheduling and live cluster
views require a compatible Tickr coordinator.

Two role-based endpoints configure that integration:

- `TICKR_COORDINATOR_HTTP_URL` supplies live-query and coordinator-health reads.
- `TICKR_COORDINATOR_RELAY_URL` supplies the bidirectional conductor relay.

The relay's public wire contract is
[`proto/conductor-relay.proto`](../proto/conductor-relay.proto). When the
coordinator is absent, API readiness and `just verify` can still pass, while
coordinator-backed health and live-data views report unavailable or degraded.

## Data ownership

- PostgreSQL stores workflow definitions, build state, terminal run projections,
  patches, replays, signals, and the event archive.
- NATS/JetStream carries command traffic, live coordination state, and staged
  task logs.
- Object storage retains compressed terminal task logs.

Development state is kept under the ignored `infra/` directory and can be reset
with `just fresh`.

## Trust boundary

The checked-in formation binds published ports to loopback and uses development
credentials. It is not a production security profile. Production deployments
must provide authentication and authorization, private networking, TLS, managed
secrets, durable stores, and independently scanned images as described in
[`production-hardening.md`](production-hardening.md).
