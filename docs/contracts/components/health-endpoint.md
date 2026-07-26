# Health endpoint contract

## Surfaces

`GET /health` is the load-balancer readiness surface. Distributed API processes return `200 {"status":"ok"}` while serving. Tickr Lite returns `200 {"status":"ok"}` only while the shared `LiteSupervisor` readiness flag is published; it returns `503 {"status":"not_ready"}` before admission and after readiness is withdrawn.

`GET /api/health` is the operator diagnostic surface. It remains callable while Tickr Lite is unready so an Operator can distinguish admission, critical-child, and substrate failures. Other `/api/*` work remains behind the Tickr Lite admission gate. Health observation is read-only: it cannot reserve Executor capacity, claim or select dispatch, acknowledge work, authorize a Command, alter queue ordering, or mutate durable state.

## Response compatibility

The existing component rows remain present with their existing `status`, `detail`, and `detection_window` fields: `api`, `data_plane_sql`, `nats_kv`, `executors`, `conductor`, and `control_plane`. Distributed formation responses omit the Tickr Lite-only additions, so existing Console readers retain the established response shape and meanings.

The Executor row additionally carries machine-readable `observed_executors`, `configured_process_slots`, and `in_flight_count`. Its detail labels Executor count and load as observations, and `detection_window` states their freshness bound. No field represents available, reserved, promised, or dispatch-authorizing capacity.

Distributed observations are omitted when their adapter-owned lifetime expires, even if substrate cleanup lags. Missing observations produce the zero-observed row; stale observations produce the degraded band only while still inside their lifetime. Duplicated or contradictory values can affect diagnostic detail but cannot affect Health status outside the freshness law, reserve a slot, or enter Task dispatch and pickup admission.
Tickr Lite adds:

- `formation`: the resolved profile, topology, SQL implementation, final Log store, writer topology, exact Executor count, substrate selections, and every coordination role's implementation and protocol identity;
- `readiness`: the current `LiteSupervisor` readiness boolean and status;
- `local_coordination`: the availability of the selected local SQLite, journal, notification, and observation roles; and
- `command_path`: the local request/reply implementation, protocol identity, and side-effect-free Ping result.

No field exposes a SQLite URL, filesystem path, data-directory location, socket path, credential, or retained backend error. SQL failures expose only the repository-owned error classification.

## All-Redis capability and quota projection

An `all-redis` diagnostic projects the exact admitted capability fingerprint, `redis_oss` implementation and admitted Redis `7.4.x` server version, `single_writable_primary` topology class, all thirteen role protocol identities and operation-manifest identities, normalized calibrated role limits, capacity reserve, current per-role quota state, readiness fence generation, and the last capability failure. Its durability class is one local-primary AOF fsync with zero required replica acknowledgements; it never implies replicated durability.

The capacity projection reports configured and used Redis memory, required formation reserve, the admitted role-limit sum, and one row per Coordination role. Each row reports the admitted maximum, calibrated bounds, accounted protocol objects, terminal cleanup boundary, and the measured protocol-record, pending-delivery, script, AOF-progress, and restart-reconstruction components. Per-role live quota state reports used units, soft threshold, hard limit, accepted-identity count, and the current pressure band.

The projection is copied from the admitted descriptor and capability monitor; Health does not recompute limits or probe Redis. Missing limits, values outside calibrated bounds, reserve-consuming sums, `OOM`, accounting inconsistency, or a missing accepted identity cannot appear as a healthy admitted projection: admission fails or the runtime capability fence closes first. The projection exposes no endpoint or other location, username, password, query parameter, trust-root material, certificate bytes, Redis key, or script body.

## Formation truthfulness

Tickr Lite reports `profile = tickr_lite`, `topology = single_node`, SQLite, local final files, one Conductor-owned writer, and exactly one Executor. Its substrate selection reports SQLite present and Postgres, NATS, Redis, and object storage absent. Disabled ingress roles remain explicit in the role list with their disabled protocol identity.

The compatibility `nats_kv` row is never healthy in Tickr Lite. It reports `degraded` with an explicit not-selected detail, while `formation.substrates.nats = false` is the typed selection. Tickr Lite performs no NATS probe and creates no NATS client for Health. No absent Redis, Postgres, or object-store row is synthesized as healthy.

The local Executor row reports exactly one request-time observed Executor and its configured process-slot and in-flight counts from `ExecutorFleetStatus`. Tickr Lite retains no capacity report between Health reads. The interface exposes no permit, reservation, claim, acknowledgement, queue transition, or lifecycle mutation.

## Status laws

The Tickr Lite formation and readiness rows are healthy exactly while `LiteSupervisor` readiness is published. Before admission and after critical-child failure they are unhealthy. Readiness is withdrawn before sibling cancellation, so any diagnostic request admitted during teardown observes unready rather than a green API masking a dead critical child.

Local coordination is healthy only when `LiteSupervisor` is ready, the selected SQLite repository health check succeeds, and the local Command writer answers Ping. A failure of any of those observations makes local coordination unhealthy.

The Command path uses a side-effect-free protobuf Ping over the selected Command bus. An answered Ping is healthy. Unavailable, timeout, oversized, cancelled, or malformed reply outcomes are unhealthy and preserve the existing public error classification.

Control-plane Health preserves the existing coordinator rollup meaning. A healthy rollup is healthy, a degraded rollup is degraded, and an unreachable or unhealthy rollup is unhealthy. Post-start Control-plane loss does not clear Tickr Lite readiness and does not alter or discard local durable state; the next successful request reports reconnection immediately.

## Verification scenarios

The live Health smoke holds one SQLite repository, local Command writer, HTTP API, and mutable Control-plane rollup. It observes pre-admission `503` readiness and unready diagnostics, admitted ready state, a degraded Control plane while local readiness remains true, unhealthy Control-plane loss with SQLite still healthy, successful reconnection, and readiness withdrawal before other API work is rejected during a simulated critical-child transition.
