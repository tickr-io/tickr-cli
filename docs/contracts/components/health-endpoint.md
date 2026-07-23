# Health endpoint contract

## Surfaces

`GET /health` is the load-balancer readiness surface. Distributed API processes return `200 {"status":"ok"}` while serving. Tickr Lite returns `200 {"status":"ok"}` only while the shared `LiteSupervisor` readiness flag is published; it returns `503 {"status":"not_ready"}` before admission and after readiness is withdrawn.

`GET /api/health` is the operator diagnostic surface. It remains callable while Tickr Lite is unready so an Operator can distinguish admission, critical-child, and substrate failures. Other `/api/*` work remains behind the Tickr Lite admission gate. Health observation is read-only: it cannot reserve Executor capacity, claim dispatch, acknowledge work, authorize a Command, or mutate durable state.

## Response compatibility

The existing component rows remain present with their existing `status`, `detail`, and `detection_window` fields: `api`, `data_plane_sql`, `nats_kv`, `executors`, `conductor`, and `control_plane`. Distributed formation responses omit the Tickr Lite-only additions, so existing Console readers retain the established response shape and meanings.

The Executor row additionally carries machine-readable `observed_executors`, `configured_process_slots`, and `in_flight_count`. These values are observational snapshots. Saturation changes detail and counts but does not reserve a slot or change the status band.

Tickr Lite adds:

- `formation`: the resolved profile, topology, SQL implementation, final Log store, writer topology, exact Executor count, substrate selections, and every coordination role's implementation and protocol identity;
- `readiness`: the current `LiteSupervisor` readiness boolean and status;
- `local_coordination`: the availability of the selected local SQLite, journal, notification, and observation roles; and
- `command_path`: the local request/reply implementation, protocol identity, and side-effect-free Ping result.

No field exposes a SQLite URL, filesystem path, data-directory location, socket path, credential, or retained backend error. SQL failures expose only the repository-owned error classification.

## Formation truthfulness

Tickr Lite reports `profile = tickr_lite`, `topology = single_node`, SQLite, local final files, one Conductor-owned writer, and exactly one Executor. Its substrate selection reports SQLite present and Postgres, NATS, Redis, and object storage absent. Disabled ingress roles remain explicit in the role list with their disabled protocol identity.

The compatibility `nats_kv` row is never healthy in Tickr Lite. It reports `degraded` with an explicit not-selected detail, while `formation.substrates.nats = false` is the typed selection. Tickr Lite performs no NATS probe and creates no NATS client for Health. No absent Redis, Postgres, or object-store row is synthesized as healthy.

The local Executor row reports exactly one observed Executor and its configured process-slot and in-flight counts from `ExecutorFleetStatus`. The interface exposes no permit, reservation, claim, acknowledgement, or lifecycle mutation.

## Status laws

The Tickr Lite formation and readiness rows are healthy exactly while `LiteSupervisor` readiness is published. Before admission and after critical-child failure they are unhealthy. Readiness is withdrawn before sibling cancellation, so any diagnostic request admitted during teardown observes unready rather than a green API masking a dead critical child.

Local coordination is healthy only when `LiteSupervisor` is ready, the selected SQLite repository health check succeeds, and the local Command writer answers Ping. A failure of any of those observations makes local coordination unhealthy.

The Command path uses a side-effect-free protobuf Ping over the selected Command bus. An answered Ping is healthy. Unavailable, timeout, oversized, cancelled, or malformed reply outcomes are unhealthy and preserve the existing public error classification.

Control-plane Health preserves the existing coordinator rollup meaning. A healthy rollup is healthy, a degraded rollup is degraded, and an unreachable or unhealthy rollup is unhealthy. Post-start Control-plane loss does not clear Tickr Lite readiness and does not alter or discard local durable state; the next successful request reports reconnection immediately.

## Verification scenarios

The live Health smoke holds one SQLite repository, local Command writer, HTTP API, and mutable Control-plane rollup. It observes pre-admission `503` readiness and unready diagnostics, admitted ready state, a degraded Control plane while local readiness remains true, unhealthy Control-plane loss with SQLite still healthy, successful reconnection, and readiness withdrawal before other API work is rejected during a simulated critical-child transition.
