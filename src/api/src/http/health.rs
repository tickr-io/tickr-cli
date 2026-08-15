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
use async_nats::Client;
use serde::Serialize;
use std::time::Duration;
use tickr_executor::component_liveness::NatsExecutorFleetStatus;
use tickr_executor::local_pickup::ExecutorFleetStatus;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use tickr_proto::coord::{
    COMPONENT_LIVENESS_BUCKET, DEFAULT_LIVENESS_TIMEOUT_SECS, LIVENESS_TIMEOUT_ENV,
};
use utoipa::ToSchema;

use crate::commands::client::{ping_command_bus, CommandBus};
use crate::http::control_plane_client::{ControlPlaneClient, ControlPlaneClientError};

/// The KV bucket the health surface probes for JetStream reachability. Later
/// slices populate it with executor component-liveness keys; here it is read-only
/// (its `status()`), and its absence is not itself unhealthy — see `check_nats_kv`.
const KV_PROBE_BUCKET: &str = COMPONENT_LIVENESS_BUCKET;

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

/// Capacity shown by Health is metadata, never a dispatch permit or reservation.
#[derive(Serialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorCapacityInterpretation {
    ObservationOnly,
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
    pub capacity_interpretation: ExecutorCapacityInterpretation,
    pub freshest_observation_age_ms: Option<u64>,
    pub oldest_observation_age_ms: Option<u64>,
    pub observation_ttl_ms: Option<u64>,
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
            capacity_interpretation: ExecutorCapacityInterpretation::ObservationOnly,
            freshest_observation_age_ms: None,
            oldest_observation_age_ms: None,
            observation_ttl_ms: None,
        }
    }
}

/// Project expiring backend-neutral observations into the Executors Health row.
///
/// The projection deliberately reports configured slots and observed load. It
/// never derives or serializes "available capacity", because dispatch remains
/// governed by each Executor's local pull-to-capacity decision.
pub fn check_executor_fleet_observations(
    snapshot: &tickr_executor::local_pickup::ExecutorFleetSnapshot,
) -> ExecutorHealth {
    let mut observed_executors = 0usize;
    let mut configured_process_slots = 0usize;
    let mut in_flight_count = 0usize;
    let mut freshest_age_ms: Option<u64> = None;
    let mut oldest_age_ms: Option<u64> = None;

    for observation in &snapshot.observations {
        if snapshot.observation_ttl_millis == 0
            || observation.expires_at_server_millis <= snapshot.server_time_millis
        {
            continue;
        }
        let age = observation.age_millis(snapshot.server_time_millis);
        observed_executors = observed_executors.saturating_add(1);
        configured_process_slots =
            configured_process_slots.saturating_add(observation.configured_process_slots);
        in_flight_count = in_flight_count.saturating_add(observation.in_flight_count);
        freshest_age_ms = Some(freshest_age_ms.map_or(age, |current| current.min(age)));
        oldest_age_ms = Some(oldest_age_ms.map_or(age, |current| current.max(age)));
    }

    let status = match freshest_age_ms {
        Some(age) if age < snapshot.observation_ttl_millis / 4 => ComponentStatus::Healthy,
        Some(_) => ComponentStatus::Degraded,
        None => ComponentStatus::Unhealthy,
    };
    let detail = match (freshest_age_ms, oldest_age_ms) {
        (Some(freshest), Some(oldest)) => format!(
            "{observed_executors} observed executors · observed load \
             {in_flight_count}/{configured_process_slots} configured slots · \
             observation age {freshest}..{oldest}ms · not guaranteed available capacity"
        ),
        _ => "0 observed executors · no guaranteed available capacity".to_string(),
    };

    ExecutorHealth {
        status,
        detail,
        detection_window: format!("observation expiry {}ms", snapshot.observation_ttl_millis),
        observed_executors: Some(observed_executors),
        configured_process_slots: Some(configured_process_slots),
        in_flight_count: Some(in_flight_count),
        capacity_interpretation: ExecutorCapacityInterpretation::ObservationOnly,
        freshest_observation_age_ms: freshest_age_ms,
        oldest_observation_age_ms: oldest_age_ms,
        observation_ttl_ms: Some(snapshot.observation_ttl_millis),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redis_capability: Option<serde_json::Value>,
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

/// Read the formation-selected observational role for the Executors Health row.
///
/// The projection sees no NATS or Redis client and cannot return an admission
/// permit or mutate queue state.
pub async fn check_executor_fleet(fleet_status: &dyn ExecutorFleetStatus) -> ExecutorHealth {
    match fleet_status.fleet_snapshot().await {
        Ok(snapshot) => check_executor_fleet_observations(&snapshot),
        Err(error) => ExecutorHealth {
            status: ComponentStatus::Unhealthy,
            detail: format!("executor fleet observation unavailable: {error}"),
            detection_window: format!(
                "observation expiry {}ms",
                fleet_status.observation_ttl().as_millis()
            ),
            observed_executors: Some(0),
            configured_process_slots: Some(0),
            in_flight_count: Some(0),
            capacity_interpretation: ExecutorCapacityInterpretation::ObservationOnly,
            freshest_observation_age_ms: None,
            oldest_observation_age_ms: None,
            observation_ttl_ms: Some(
                u64::try_from(fleet_status.observation_ttl().as_millis()).unwrap_or(u64::MAX),
            ),
        },
    }
}

/// Compatibility entry point for the existing all-NATS role-law evidence.
pub async fn check_executors(nats: &Client, timeout: Duration) -> ExecutorHealth {
    let fleet_status = NatsExecutorFleetStatus::new(nats.clone(), timeout);
    let snapshot = fleet_status
        .fleet_snapshot()
        .await
        .expect("all-NATS observations degrade to an empty snapshot");
    let mut health = check_executor_fleet_observations(&snapshot);
    health.detail = format!(
        "{} observed executors · observed load {}/{} configured slots",
        health.observed_executors.unwrap_or(0),
        health.in_flight_count.unwrap_or(0),
        health.configured_process_slots.unwrap_or(0)
    );
    let seconds = timeout.as_secs();
    let window = if seconds > 0 && seconds.is_multiple_of(60) {
        format!("{}m", seconds / 60)
    } else {
        format!("{seconds}s")
    };
    health.detection_window = format!("liveness window {window}");
    health.freshest_observation_age_ms = None;
    health.oldest_observation_age_ms = None;
    health.observation_ttl_ms = None;
    health
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

/// The Control plane row — one HTTP hop to the Frontend's `/api/internal/health`
/// route, whose body is the Control-plane rollup (the Frontend plus the Control
/// plane it fronts). The UI reaches the Control plane **only** through the
/// Frontend, so the Control plane is reported as a single rollup row, never
/// split per-component. The Frontend route reuses its existing live-store read
/// rather than adding a probe path, so this row mirrors that read's degrade
/// path: a reachable Frontend whose rollup is `healthy` ⇒ `healthy`; an
/// unreachable Frontend (transport error/timeout) ⇒ `unhealthy` — the same
/// posture as the Frontend's own "live store unreachable" degrade path.
///
/// Instantaneous like the other HTTP/command probes: a red row recovers on the
/// very next request, so its detection window is the request itself.
pub async fn check_control_plane(control_plane: &ControlPlaneClient) -> ComponentHealth {
    match control_plane.internal_health().await {
        Ok(rollup) => match rollup.status.as_str() {
            "healthy" => ComponentHealth::instant(
                ComponentStatus::Healthy,
                "Control plane up (Frontend + live store)",
            ),
            "degraded" => ComponentHealth::instant(
                ComponentStatus::Degraded,
                "Control plane degraded (Frontend up, live store degraded)",
            ),
            _ => ComponentHealth::instant(
                ComponentStatus::Unhealthy,
                "Control plane rollup unhealthy",
            ),
        },
        Err(ControlPlaneClientError::Unauthenticated) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane authentication rejected",
        ),
        Err(ControlPlaneClientError::Forbidden) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane Tenant authorization rejected",
        ),
        Err(ControlPlaneClientError::Timeout) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane request timed out",
        ),
        Err(ControlPlaneClientError::Unreachable) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane unavailable via Frontend",
        ),
        Err(ControlPlaneClientError::NotFound) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane health route unavailable",
        ),
        Err(ControlPlaneClientError::Server { .. }) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane health request rejected",
        ),
        Err(ControlPlaneClientError::Decode(_)) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane health response invalid",
        ),
        Err(
            ControlPlaneClientError::MissingBearerToken
            | ControlPlaneClientError::InvalidBearerToken
            | ControlPlaneClientError::InvalidEndpoint
            | ControlPlaneClientError::InsecureEndpoint,
        ) => ComponentHealth::instant(
            ComponentStatus::Unhealthy,
            "Control plane client configuration invalid",
        ),
    }
}

/// Assemble the all-NATS formation's report.
pub async fn build_health_report(
    repositories: &ReadOnlyRepositoryBundle,
    nats: &Client,
    control_plane: &ControlPlaneClient,
    ping_deadline: Duration,
) -> HealthResponse {
    build_health_report_with_command_bus(
        repositories,
        nats,
        &CommandBus::nats(nats.clone()),
        control_plane,
        ping_deadline,
    )
    .await
}

/// Compatibility wrapper for the existing all-NATS Command-bus tests.
pub async fn build_health_report_with_command_bus(
    repositories: &ReadOnlyRepositoryBundle,
    nats: &Client,
    command_bus: &CommandBus,
    control_plane: &ControlPlaneClient,
    ping_deadline: Duration,
) -> HealthResponse {
    let fleet_status = NatsExecutorFleetStatus::new(nats.clone(), liveness_timeout());
    build_health_report_with_fleet_status(
        repositories,
        Some(nats),
        command_bus,
        control_plane,
        &fleet_status,
        ping_deadline,
        None,
    )
    .await
}

/// Assemble a distributed report from selected role interfaces.
pub async fn build_health_report_with_fleet_status(
    repositories: &ReadOnlyRepositoryBundle,
    nats: Option<&Client>,
    command_bus: &CommandBus,
    control_plane: &ControlPlaneClient,
    executor_fleet: &dyn ExecutorFleetStatus,
    ping_deadline: Duration,
    redis_capability: Option<serde_json::Value>,
) -> HealthResponse {
    let nats_kv = match nats {
        Some(nats) => check_nats_kv(nats).await,
        None => ComponentHealth::instant(
            ComponentStatus::Degraded,
            "not selected: formation uses non-NATS coordination",
        ),
    };
    HealthResponse {
        checked_at: chrono::Utc::now().to_rfc3339(),
        api: api_self(),
        data_plane_sql: check_data_plane_sql(repositories).await,
        nats_kv,
        executors: check_executor_fleet(executor_fleet).await,
        conductor: check_command_bus(command_bus, ping_deadline).await,
        control_plane: check_control_plane(control_plane).await,
        formation: None,
        readiness: None,
        local_coordination: None,
        command_path: None,
        redis_capability,
    }
}

/// Tickr Lite health retains the existing Console-facing rows while reporting
/// the selected local roles and never probing an absent distributed substrate.

pub async fn build_lite_health_report(
    repositories: &ReadOnlyRepositoryBundle,
    command_bus: &CommandBus,
    control_plane: &ControlPlaneClient,
    executor_fleet: &dyn ExecutorFleetStatus,
    formation: &ResolvedFormationHealth,
    ready: bool,
    ping_deadline: Duration,
) -> HealthResponse {
    let data_plane_sql = check_data_plane_sql(repositories).await;
    let conductor = check_command_bus(command_bus, ping_deadline).await;
    let control_plane = check_control_plane(control_plane).await;
    let snapshot = executor_fleet.fleet_snapshot().await.unwrap_or_else(|_| {
        tickr_executor::local_pickup::ExecutorFleetSnapshot {
            server_time_millis: 0,
            observation_ttl_millis: 0,
            observations: Vec::new(),
        }
    });
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
                "{} observed executor · observed load {}/{} configured slots",
                snapshot.observed_executors(),
                snapshot.in_flight_count(),
                snapshot.configured_process_slots()
            ),
            snapshot.observed_executors(),
            snapshot.configured_process_slots(),
            snapshot.in_flight_count(),
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
        redis_capability: None,
    }
}
