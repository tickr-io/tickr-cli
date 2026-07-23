//! Operator health surface — the data behind `GET /api/health`.
//!
//! Distinct from the stateless `GET /health` readiness probe (a load-balancer
//! liveness check that only answers "is this process accepting connections").
//! This surface reports the *platform's* component status: each row is computed
//! **fresh per request** from an individually-cheap check, with no cached health
//! table and no heavy probe. Two requests that bracket a state change reflect the
//! change, and a "recheck" is byte-for-byte the same work as a normal request —
//! caching health would let a stale row lie about a component that just died.
//!
//! The selected Data-plane SQL row consumes the repository bundle already
//! composed into the API process. Its repository-owned Health law performs a
//! trivial read and verifies schema compatibility; only classified failure
//! detail reaches the wire.
//!
//! Status **bands are derived, not tuned**: the executor row computes key age
//! against the single `TICKR_LIVENESS_TIMEOUT_SECS` knob (healthy `< TTL/4`,
//! degraded in the `TTL/4..TTL` slack window, unhealthy at expiry). Every other
//! row is instantaneous and carries the same per-row window field.

use async_nats::jetstream;
use async_nats::jetstream::kv::Operation;
use async_nats::Client;
use futures::StreamExt;
use serde::Serialize;
use std::time::Duration;
use utoipa::ToSchema;
// Consume the writer slice's key schema + bucket name off the published
// contract — never redefined here, so writer and reader can't drift.
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use tickr_proto::coord::{
    ComponentLivenessValue, COMPONENT_LIVENESS_BUCKET, DEFAULT_LIVENESS_TIMEOUT_SECS,
    LIVENESS_TIMEOUT_ENV,
};

use crate::commands::client::{ping_command_bus, CommandBus};
use crate::http::coordinator_client::CoordinatorClient;

/// The KV bucket the health surface probes for JetStream reachability. Later
/// slices populate it with executor component-liveness keys; here it is read-only
/// (its `status()`), and its absence is not itself unhealthy — see `check_nats_kv`.
const KV_PROBE_BUCKET: &str = COMPONENT_LIVENESS_BUCKET;

/// Key prefix namespacing executor component-liveness keys (`executor.<uuid>`) in
/// the shared bucket, so the pool read counts only executor processes.
const EXECUTOR_KEY_PREFIX: &str = "executor.";

/// Detection-window label for the instantaneous checks. A red row here flips back
/// to green on the very next request, so the window is the request itself — no
/// liveness slack to wait out (unlike the banded rows later slices add).
const INSTANT_WINDOW: &str = "instant";

/// Raw, instantaneous status for one component. Smoothing/debounce is a UI
/// concern; the endpoint reports what it observed this request.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// One row on the health page: the observed `status`, a human `detail`, and the
/// `detection_window` describing how quickly a red row would have flipped.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct ComponentHealth {
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
}

impl ComponentHealth {
    fn instant(status: ComponentStatus, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
            detection_window: INSTANT_WINDOW.to_string(),
        }
    }
}

/// Selected Data-plane SQL implementation reported only on the Health surface.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataPlaneSqlImplementation {
    Postgres,
    Sqlite,
}

impl DataPlaneSqlImplementation {
    fn from_repository(repositories: &ReadOnlyRepositoryBundle) -> Self {
        match repositories.implementation() {
            "postgres" => Self::Postgres,
            "sqlite" => Self::Sqlite,
            implementation => {
                unreachable!("repository returned unknown SQL implementation `{implementation}`")
            }
        }
    }
}

/// Backend-neutral selected SQL row. `implementation` is display metadata; the
/// status law is identical for Postgres and SQLite.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct DataPlaneSqlHealth {
    pub implementation: DataPlaneSqlImplementation,
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
}

impl DataPlaneSqlHealth {
    fn instant(
        implementation: DataPlaneSqlImplementation,
        status: ComponentStatus,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            implementation,
            status,
            detail: detail.into(),
            detection_window: INSTANT_WINDOW.to_string(),
        }
    }
}

/// Health-surface identity for the admitted formation profile.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthFormationProfile {
    TickrLite,
}

/// Health-surface identity for the admitted process topology.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthFormationTopology {
    SingleNode,
}

/// Health-surface identity for the selected final Log store.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthFinalLogStore {
    LocalFiles,
}

/// Health-surface identity for the selected SQL-writer topology.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthWriterTopology {
    ConductorOwned,
}

/// Coordination roles carried by the resolved formation descriptor.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthCoordinationRole {
    CommandBus,
    TaskDispatch,
    TaskEvents,
    TaskCancellation,
    CompactionStaging,
    LifecycleWork,
    LogStaging,
    ScopeStore,
    IngressIdempotencyStore,
    LivenessWatchdog,
    SignalAppliedNotifier,
    ExecutorFleetStatus,
    EventIngress,
}

/// Concrete role implementation selected during formation resolution.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthRoleImplementation {
    LocalRequestReply,
    LocalSqlite,
    LocalJournal,
    LocalNotification,
    LocalObservation,
    Disabled,
}

/// Stable identity for one selected coordination protocol.
#[derive(Serialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct HealthProtocolIdentity {
    pub name: String,
    pub version: u16,
}

/// One role from the immutable resolved formation descriptor.
#[derive(Serialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct HealthResolvedRole {
    pub role: HealthCoordinationRole,
    pub implementation: HealthRoleImplementation,
    pub protocol: HealthProtocolIdentity,
}

/// Explicit substrate selection. `false` means absent by formation design, not
/// an unreachable dependency.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthSubstrateSelection {
    pub sqlite: bool,
    pub postgres: bool,
    pub nats: bool,
    pub redis: bool,
    pub object_store: bool,
}

/// Immutable, backend-location-free projection of the admitted descriptor.
#[derive(Debug, Clone)]
pub struct ResolvedFormationHealth {
    pub profile: HealthFormationProfile,
    pub topology: HealthFormationTopology,
    pub sql: DataPlaneSqlImplementation,
    pub final_logs: HealthFinalLogStore,
    pub writer_topology: HealthWriterTopology,
    pub executor_count: u16,
    pub substrates: HealthSubstrateSelection,
    pub roles: Vec<HealthResolvedRole>,
}

/// Formation identity plus its current `LiteSupervisor` availability state.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct FormationHealth {
    pub profile: HealthFormationProfile,
    pub topology: HealthFormationTopology,
    pub sql: DataPlaneSqlImplementation,
    pub final_logs: HealthFinalLogStore,
    pub writer_topology: HealthWriterTopology,
    pub executor_count: u16,
    pub substrates: HealthSubstrateSelection,
    pub roles: Vec<HealthResolvedRole>,
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
}

impl ResolvedFormationHealth {
    fn observed(&self, status: ComponentStatus, detail: impl Into<String>) -> FormationHealth {
        FormationHealth {
            profile: self.profile,
            topology: self.topology,
            sql: self.sql,
            final_logs: self.final_logs,
            writer_topology: self.writer_topology,
            executor_count: self.executor_count,
            substrates: self.substrates,
            roles: self.roles.clone(),
            status,
            detail: detail.into(),
            detection_window: INSTANT_WINDOW.to_string(),
        }
    }
}

/// Top-level readiness as owned by `LiteSupervisor`.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct ReadinessHealth {
    pub ready: bool,
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
}

/// Command-path observation with its resolved implementation and protocol.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct CommandPathHealth {
    pub implementation: HealthRoleImplementation,
    pub protocol: HealthProtocolIdentity,
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
}

/// Executor observation with machine-readable configured and in-flight counts.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct ExecutorHealth {
    pub status: ComponentStatus,
    pub detail: String,
    pub detection_window: String,
    pub observed_executors: Option<usize>,
    pub configured_process_slots: Option<usize>,
    pub in_flight_count: Option<usize>,
}

impl ExecutorHealth {
    fn instant(
        status: ComponentStatus,
        detail: impl Into<String>,
        observed_executors: usize,
        configured_process_slots: usize,
        in_flight_count: usize,
    ) -> Self {
        Self {
            status,
            detail: detail.into(),
            detection_window: INSTANT_WINDOW.to_string(),
            observed_executors: Some(observed_executors),
            configured_process_slots: Some(configured_process_slots),
            in_flight_count: Some(in_flight_count),
        }
    }

    fn windowed(
        status: ComponentStatus,
        detail: impl Into<String>,
        detection_window: impl Into<String>,
        observed_executors: usize,
        configured_process_slots: usize,
        in_flight_count: usize,
    ) -> Self {
        Self {
            status,
            detail: detail.into(),
            detection_window: detection_window.into(),
            observed_executors: Some(observed_executors),
            configured_process_slots: Some(configured_process_slots),
            in_flight_count: Some(in_flight_count),
        }
    }
}

/// The typed body of `GET /api/health`. Existing component rows remain stable;
/// Tickr Lite adds formation, readiness, and local-role observations.
#[derive(Serialize, ToSchema, Debug, Clone)]
pub struct HealthResponse {
    /// RFC 3339 instant the whole report was computed (all rows are fresh as of here).
    pub checked_at: String,
    pub api: ComponentHealth,
    pub data_plane_sql: DataPlaneSqlHealth,
    pub nats_kv: ComponentHealth,
    pub executors: ExecutorHealth,
    pub conductor: ComponentHealth,
    pub control_plane: ComponentHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formation: Option<FormationHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_coordination: Option<ComponentHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_path: Option<CommandPathHealth>,
}

/// The API-self row. Reaching this code means the request got through the
/// gateway, which is the whole claim: constant `healthy` whenever the handler runs.
pub fn api_self() -> ComponentHealth {
    ComponentHealth::instant(ComponentStatus::Healthy, "handler reached; API answering")
}

/// The selected Data-plane SQL row. The repository owns both the trivial read
/// and schema-compatibility law. Failure detail exposes only its shared
/// classification, never the retained backend source.
pub async fn check_data_plane_sql(repositories: &ReadOnlyRepositoryBundle) -> DataPlaneSqlHealth {
    let implementation = DataPlaneSqlImplementation::from_repository(repositories);
    match repositories.health_check().await {
        Ok(()) => DataPlaneSqlHealth::instant(
            implementation,
            ComponentStatus::Healthy,
            "repository reachable; schema compatible",
        ),
        Err(error) => DataPlaneSqlHealth::instant(
            implementation,
            ComponentStatus::Unhealthy,
            format!("repository health check failed: {}", error.kind()),
        ),
    }
}

/// The NATS JetStream KV row — a `kv.status()` reachability probe on the shared
/// client. `get_key_value` cannot itself distinguish "probe bucket absent" from
/// "substrate unreachable", so we classify a lookup failure by the live
/// connection state (the same disambiguation `ctx_reader` uses): connected ⇒ the
/// bucket is merely absent and KV is reachable (`healthy`); disconnected ⇒
/// `unhealthy`. This is a coarse reachability check by design — a silent
/// JetStream delivery stall reads as healthy-idle; not detected in v1.
pub async fn check_nats_kv(nats: &Client) -> ComponentHealth {
    let js = jetstream::new(nats.clone());
    match js.get_key_value(KV_PROBE_BUCKET).await {
        Ok(kv) => match kv.status().await {
            Ok(_) => ComponentHealth::instant(ComponentStatus::Healthy, "kv.status() ok"),
            Err(e) => ComponentHealth::instant(
                ComponentStatus::Unhealthy,
                format!("kv.status() failed: {e}"),
            ),
        },
        Err(e) => {
            if nats.connection_state() == async_nats::connection::State::Connected {
                ComponentHealth::instant(
                    ComponentStatus::Healthy,
                    "JetStream KV reachable (probe bucket absent)",
                )
            } else {
                ComponentHealth::instant(
                    ComponentStatus::Unhealthy,
                    format!("JetStream KV unreachable: {e}"),
                )
            }
        }
    }
}

/// The liveness timeout (per-key TTL) driving the executor pool's bands, read
/// from the single `TICKR_LIVENESS_TIMEOUT_SECS` knob (whole seconds, default 2m;
/// a zero/unparseable value falls back to the default). The api crate can't
/// depend on the executor's `LivenessConfig`, so this mirrors its env read — the
/// same knob, so the read TTL always matches the executor's arm TTL.
fn liveness_timeout() -> Duration {
    let secs = std::env::var(LIVENESS_TIMEOUT_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_LIVENESS_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

/// Human label for the liveness detection window (e.g. `2m`, `90s`) shown as the
/// row's `detection_window`.
fn window_label(timeout: Duration) -> String {
    let secs = timeout.as_secs();
    if secs > 0 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// The Executors **pool** row — one aggregate row, no per-executor rows. Lists
/// `executor.*` keys in `tickr_component_liveness`, counts the non-expired keys as
/// **N alive**, and sums their `{cap, in_flight}` into **Y/X (used/total slots)**.
/// The read is O(fleet size), stateless, and self-cleaning: an expired/missing key
/// is simply not counted (NATS reaps it by TTL; we also skip any key whose own age
/// has already reached the TTL). It touches only this bucket — no task-liveness
/// scan, no cached table.
///
/// Band is **derived, not tuned**: each key's own age (`now − entry.created`)
/// against the single `TICKR_LIVENESS_TIMEOUT_SECS` knob — ≥1 fresh key (`< TTL/4`)
/// ⇒ healthy; keys exist but all sit in the `TTL/4..TTL` slack window ⇒ degraded;
/// no non-expired key (zero fleet) ⇒ unhealthy. Saturation `X/Y` is detail text,
/// never a band driver.
pub async fn check_executors(nats: &Client, timeout: Duration) -> ExecutorHealth {
    let window = window_label(timeout);
    let detection_window = format!("liveness window {window}");
    let ages = collect_executor_ages(nats, timeout).await;

    // Aggregate: N alive, plus summed used/total slots for the informational
    // saturation detail. Zero fleet ⇒ unhealthy regardless of slots.
    let n_alive = ages.len();
    let used: usize = ages.iter().map(|a| a.value.in_flight).sum();
    let total: usize = ages.iter().map(|a| a.value.cap).sum();
    let detail = format!("{n_alive} alive · {used}/{total} slots");

    let status = match ages.iter().map(|a| a.age).min() {
        // Freshest key drives the band: at least one `< TTL/4` ⇒ healthy; all
        // present keys in the `TTL/4..TTL` slack window ⇒ degraded.
        Some(freshest) if freshest < timeout / 4 => ComponentStatus::Healthy,
        Some(_) => ComponentStatus::Degraded,
        // No non-expired executor key ⇒ zero fleet.
        None => ComponentStatus::Unhealthy,
    };

    ExecutorHealth::windowed(status, detail, detection_window, n_alive, total, used)
}

/// One counted executor key: its `{cap, in_flight}` value and its age at read time.
struct ExecutorAge {
    value: ComponentLivenessValue,
    age: Duration,
}

/// List the non-expired `executor.*` keys and decode each key's `{cap, in_flight}`
/// value plus its age. A bucket that is absent or unreachable, or any per-key read
/// failure, yields an empty list — the row then reads zero-fleet unhealthy, which
/// is the honest signal when no executor can be observed.
async fn collect_executor_ages(nats: &Client, timeout: Duration) -> Vec<ExecutorAge> {
    let js = jetstream::new(nats.clone());
    let Ok(store) = js.get_key_value(COMPONENT_LIVENESS_BUCKET).await else {
        return Vec::new();
    };
    let Ok(mut keys) = store.keys().await else {
        return Vec::new();
    };

    // Whole-millisecond age against each key's own `created` — the freshest
    // beats the band. Seconds would truncate the `TTL/4` boundary too coarsely.
    let now_ms = chrono::Utc::now().timestamp_millis() as i128;
    let mut out = Vec::new();
    while let Some(item) = keys.next().await {
        let Ok(key) = item else { continue };
        if !key.starts_with(EXECUTOR_KEY_PREFIX) {
            continue;
        }
        let entry = match store.entry(&key).await {
            Ok(Some(e)) if e.operation == Operation::Put => e,
            // Tombstoned mid-scan, or a single-key read failure: not counted.
            _ => continue,
        };
        let Ok(value) = serde_json::from_slice::<ComponentLivenessValue>(&entry.value) else {
            continue;
        };
        let created_ms = entry.created.unix_timestamp_nanos() / 1_000_000;
        let age = Duration::from_millis((now_ms - created_ms).max(0) as u64);
        // Defensive: a key whose own age already reached the TTL is expired even
        // if NATS hasn't reaped it yet — not counted.
        if age < timeout {
            out.push(ExecutorAge { value, age });
        }
    }
    out
}

/// The Conductor row — a command-plane-responsive check over the selected
/// Command bus. It issues a side-effect-free `Ping` and maps any unavailable,
/// timeout, or malformed-reply outcome to `unhealthy`.
pub async fn check_command_bus(command_bus: &CommandBus, deadline: Duration) -> ComponentHealth {
    match ping_command_bus(command_bus, deadline).await {
        Ok(()) => ComponentHealth::instant(
            ComponentStatus::Healthy,
            "command-plane-responsive: consumer answered Ping",
        ),
        Err(e) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            format!("command-plane-responsive: no answer to Ping ({e:?})"),
        ),
    }
}

/// Distributed-formation compatibility wrapper for the NATS Core Command bus.
pub async fn check_conductor(nats: &Client, deadline: Duration) -> ComponentHealth {
    check_command_bus(&CommandBus::nats(nats.clone()), deadline).await
}

/// The Control plane row — one HTTP hop to the coordinator's `/api/internal/health`
/// route, whose body is the control-plane rollup (this coordinator + the
/// control plane it fronts). The UI reaches control plane **only** through the
/// coordinator, so the control plane is reported as a single rollup row, never
/// split per-component. The coordinator route reuses its existing live-store read
/// rather than adding a probe path, so this row mirrors that read's degrade
/// path: a reachable coordinator whose rollup is `healthy` ⇒ `healthy`; a coordinator
/// that is unreachable (transport error/timeout) ⇒ `unhealthy` — the same
/// posture as the coordinator's own "live store unreachable" degrade path.
///
/// Instantaneous like the other HTTP/command probes: a red row recovers on the
/// very next request, so its detection window is the request itself.
pub async fn check_control_plane(coordinator: &CoordinatorClient) -> ComponentHealth {
    match coordinator.internal_health().await {
        Ok(rollup) => match rollup.status.as_str() {
            "healthy" => ComponentHealth::instant(
                ComponentStatus::Healthy,
                "control plane up (coordinator + control plane)",
            ),
            "degraded" => ComponentHealth::instant(
                ComponentStatus::Degraded,
                "control plane degraded (coordinator up, live store degraded)",
            ),
            other => ComponentHealth::instant(
                ComponentStatus::Unhealthy,
                format!("control plane rollup unhealthy: {other}"),
            ),
        },
        Err(e) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            format!("control plane unreachable via coordinator: {e}"),
        ),
    }
}

/// Assemble the distributed formation's report.
pub async fn build_health_report(
    repositories: &ReadOnlyRepositoryBundle,
    nats: &Client,
    coordinator: &CoordinatorClient,
    ping_deadline: Duration,
) -> HealthResponse {
    build_health_report_with_command_bus(
        repositories,
        nats,
        &CommandBus::nats(nats.clone()),
        coordinator,
        ping_deadline,
    )
    .await
}

/// Assemble a report with the formation-selected Command bus. NATS remains an
/// independent input for the distributed KV and Executor rows.
pub async fn build_health_report_with_command_bus(
    repositories: &ReadOnlyRepositoryBundle,
    nats: &Client,
    command_bus: &CommandBus,
    coordinator: &CoordinatorClient,
    ping_deadline: Duration,
) -> HealthResponse {
    HealthResponse {
        checked_at: chrono::Utc::now().to_rfc3339(),
        api: api_self(),
        data_plane_sql: check_data_plane_sql(repositories).await,
        nats_kv: check_nats_kv(nats).await,
        executors: check_executors(nats, liveness_timeout()).await,
        conductor: check_command_bus(command_bus, ping_deadline).await,
        control_plane: check_control_plane(coordinator).await,
        formation: None,
        readiness: None,
        local_coordination: None,
        command_path: None,
    }
}

/// Tickr Lite health retains the existing Console-facing rows while reporting
/// the selected local roles and never probing an absent distributed substrate.
use tickr_executor::local_pickup::ExecutorFleetStatus;

pub async fn build_lite_health_report(
    repositories: &ReadOnlyRepositoryBundle,
    command_bus: &CommandBus,
    coordinator: &CoordinatorClient,
    executor_fleet: &tickr_executor::local_pickup::LocalExecutorFleetStatus,
    formation: &ResolvedFormationHealth,
    ready: bool,
    ping_deadline: Duration,
) -> HealthResponse {
    let data_plane_sql = check_data_plane_sql(repositories).await;
    let conductor = check_command_bus(command_bus, ping_deadline).await;
    let control_plane = check_control_plane(coordinator).await;
    let snapshot = executor_fleet.snapshot();
    let readiness_status = if ready {
        ComponentStatus::Healthy
    } else {
        ComponentStatus::Unhealthy
    };
    let local_status = if ready
        && data_plane_sql.status == ComponentStatus::Healthy
        && conductor.status == ComponentStatus::Healthy
    {
        ComponentStatus::Healthy
    } else {
        ComponentStatus::Unhealthy
    };
    let command_role = formation
        .roles
        .iter()
        .find(|role| role.role == HealthCoordinationRole::CommandBus)
        .expect("resolved Tickr Lite formation carries CommandBus");

    HealthResponse {
        checked_at: chrono::Utc::now().to_rfc3339(),
        api: api_self(),
        data_plane_sql,
        // Compatibility row for older Console clients. It is never green when
        // NATS is absent; formation.substrates.nats is the typed selection.
        nats_kv: ComponentHealth::instant(
            ComponentStatus::Degraded,
            "not selected: Tickr Lite uses local coordination",
        ),
        executors: ExecutorHealth::instant(
            readiness_status,
            format!(
                "1 executor: {} configured process slots, {} in flight",
                snapshot.configured_process_slots, snapshot.in_flight_count
            ),
            1,
            snapshot.configured_process_slots,
            snapshot.in_flight_count,
        ),
        conductor: conductor.clone(),
        control_plane,
        formation: Some(formation.observed(
            readiness_status,
            if ready {
                "Tickr Lite admitted; all critical children registered"
            } else {
                "Tickr Lite readiness withdrawn before formation cancellation"
            },
        )),
        readiness: Some(ReadinessHealth {
            ready,
            status: readiness_status,
            detail: if ready {
                "LiteSupervisor ready"
            } else {
                "LiteSupervisor not ready"
            }
            .to_string(),
            detection_window: INSTANT_WINDOW.to_string(),
        }),
        local_coordination: Some(ComponentHealth::instant(
            local_status,
            if ready {
                "local SQLite, journal, notification, and observation roles registered"
            } else {
                "local coordination unavailable while LiteSupervisor is not ready"
            },
        )),
        command_path: Some(CommandPathHealth {
            implementation: command_role.implementation,
            protocol: command_role.protocol.clone(),
            status: conductor.status,
            detail: conductor.detail,
            detection_window: conductor.detection_window,
        }),
    }
}
