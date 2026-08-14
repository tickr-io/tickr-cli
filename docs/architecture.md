# Architecture

Tickr CLI is the tenant-side Data plane. It provides the `tickr` command,
three runtime processes, the Console, and the public contracts used to connect
to the Tickr Control plane.

## Components

- **Conductor** accepts workflow commands, validates Nickel definitions, builds
  task specifications, maintains tenant coordination state in NATS, and archives
  terminal runs in PostgreSQL.
- **API** exposes the HTTP and OpenAPI surface used by operators and the Console.
  archived reads come from PostgreSQL; live reads are merged from the Control
  plane's HTTP subquery channel when it is available.
- **Executor** pulls runnable tasks, executes them, publishes lifecycle events,
  and ships task logs.
- **Console** is the browser interface served by Vite during local development.
- **Infrastructure** consists of PostgreSQL, NATS/JetStream, and S3-compatible
  object storage. The repository-local Compose file provides development-only
  instances of these services.

## Control-plane connection

The local formation is useful on its own for development, contract validation,
archived-data APIs, and Console/API work. End-to-end scheduling and live views
require a compatible Tickr Control plane.

Two role-based endpoints configure that connection:

- `TICKR_CTRL_HTTP_URL` supplies the HTTP subquery channel for live queries and
  Control-plane health reads.
- `TICKR_CTRL_RELAY_URL` supplies the bidirectional Conductor relay.

The relay's public wire contract is
[`proto/conductor-relay.proto`](../proto/conductor-relay.proto). When the
Control plane is absent, API readiness and `just verify` can still pass, while
Control-plane health and HTTP-subquery live-data views report unavailable or
degraded.

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
