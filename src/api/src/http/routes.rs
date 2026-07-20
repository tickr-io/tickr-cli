//! HTTP routes for the tickr API component.

use crate::commands::client::{
    bus_error_response, public_error_message, send_command, BusError, CommandDeadlines,
};
use anyhow::Result;
use async_nats::Client;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tickr_proto::tickr_api as api;
use tokio::sync::watch;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

/// Header name signaling whether the live half of a merged-list response was
/// successfully fetched. Picked instead of a body envelope so existing UI
/// consumers reading the array shape stay drop-in compatible.
const LIVE_DATA_AVAILABLE_HEADER: axum::http::HeaderName =
    axum::http::HeaderName::from_static("x-live-data-available");

/// Response returned by the hello endpoint.
#[derive(Serialize, ToSchema)]
struct HelloResponse {
    message: String,
}

/// Response returned by the readiness endpoint (`GET /health`). Distinct from
/// the operator health surface (`GET /api/health`, `health::HealthResponse`):
/// this is the stateless load-balancer liveness probe, not a platform status.
#[derive(Serialize, ToSchema)]
struct ReadinessResponse {
    status: String,
}

/// Response returned by the tenant endpoint. Identifies the single tenant this
/// API component serves (resolved from `TICKR_TENANT_SLUG` at startup): the
/// human-readable `slug`, the derived 36-char UUID `id`, and a live count of
/// workflow definitions registered under it. Snake-case on the wire, matching
/// the other DTOs.
#[derive(Serialize, ToSchema)]
#[schema(as = TenantInfo)]
struct TenantInfoResponse {
    slug: String,
    id: String,
    workflow_count: i64,
}

/// Application state shared across handlers. Holds the data-substrate handles
/// the read endpoints query. The Postgres pool and NATS client are constructed
/// at boot; subsequent fields (coordinator client, logs resolver) are added by the
/// slices that introduce the handlers consuming them.
#[derive(Clone)]
pub struct AppState {
    // Live log batches + the command-bus transport for write requests. Read by
    // the logs handler and the write handlers.
    nats: Arc<Client>,
    // Conductor-side Postgres archive. Read by the workflow/instance/signal
    // handlers.
    pg_pool: Arc<PgPool>,
    // HTTP client to the coordinator for live-state subqueries. Separate from
    // any relay so UI query load can't head-of-line block system comms.
    coordinator: Arc<super::coordinator_client::CoordinatorClient>,
    // Task-log dispatcher with both stores injected: MinIO (terminal blobs)
    // and NATS KV (live streamed batches).
    logs: Arc<super::logs_resolver::LogsResolver>,
    // Per-command deadlines for write requests forwarded over the command bus.
    deadlines: CommandDeadlines,
}

/// Hello endpoint — `GET /`. Names the API component so an operator hitting the
/// port directly can tell which binary answered.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/", responses((status = 200, body = HelloResponse)))]
async fn hello_handler() -> Json<HelloResponse> {
    Json(HelloResponse {
        message: "Hello from Tickr API".to_string(),
    })
}

/// Readiness probe — `GET /health`. A real readiness signal for operators,
/// distinct from the marginal hello. Returns `{"status": "ok"}` once the
/// server is accepting connections.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/health", responses((status = 200, body = ReadinessResponse)))]
async fn health_handler() -> Json<ReadinessResponse> {
    Json(ReadinessResponse {
        status: "ok".to_string(),
    })
}

/// Handler for `GET /api/health` — the operator health surface. Wired to app
/// state (the api pool + shared NATS client) and computes each component row
/// fresh per request; there is no cached health table, so a "recheck" is
/// byte-for-byte the same work as any other request. Distinct from the top-level
/// `/health` readiness probe above.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/health", responses((status = 200, body = super::health::HealthResponse)))]
async fn api_health_handler(
    State(state): State<Arc<AppState>>,
) -> Json<super::health::HealthResponse> {
    Json(
        super::health::build_health_report(
            &state.pg_pool,
            &state.nats,
            &state.coordinator,
            state.deadlines.ping,
        )
        .await,
    )
}

/// Handler for `GET /api/tenant`. Reflects the env-bound tenant this component
/// serves: `slug` and `id` come straight from the resolved environment (no DB),
/// and `workflow_count` is a live `count(DISTINCT id)` over the tenant-scoped
/// pool. The slug is resolved through the same rule the id derives from
/// (`TenantId`), so the two always agree.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/tenant", responses((status = 200, body = TenantInfoResponse), (status = 500)))]
async fn tenant_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match super::archive_queries::count_workflow_defs(&state.pg_pool).await {
        Ok(workflow_count) => (
            StatusCode::OK,
            Json(TenantInfoResponse {
                slug: tickr_proto::TenantId::slug_from_env(),
                id: tickr_proto::TenantId::from_env().to_string(),
                workflow_count,
            }),
        )
            .into_response(),
        Err(e) => {
            eprintln!("tenant_handler: workflow-count read failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "tenant info unavailable"})),
            )
                .into_response()
        }
    }
}

/// Handler for `GET /api/workflows`. Reads the conductor's canonical
/// `workflows` table and projects each registered workflow into the
/// UI-facing `WorkflowResponse`. Empty list on first start; no 404.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows", responses((status = 200, body = Vec<super::dto::WorkflowResponse>), (status = 500)))]
async fn list_workflows_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match super::archive_queries::list_workflow_defs(&state.pg_pool).await {
        Ok(workflows) => {
            // Resolve every workflow's latest fired-instance state in one batch
            // (one PG query + one live cluster subquery), then join per row.
            let ids: Vec<uuid::Uuid> = workflows
                .iter()
                .filter_map(|row| uuid::Uuid::parse_str(&row.workflow.id).ok())
                .collect();
            let mut latest_runs = super::latest_run_resolver::resolve_latest_run_states(
                &state.pg_pool,
                &state.coordinator,
                &ids,
            )
            .await;
            let completed_counts = super::archive_queries::completed_run_counts(&state.pg_pool)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("list_workflows_handler: completed-run count failed: {e}");
                    Default::default()
                });

            let response: Vec<super::dto::WorkflowResponse> = workflows
                .into_iter()
                .map(|row| {
                    let w = &row.workflow;
                    let id = uuid::Uuid::parse_str(&w.id).unwrap_or_default();
                    let latest_run_state = latest_runs.remove(&id).flatten();
                    // The stored definition is the published proto contract;
                    // project its trigger into the unchanged response shape.
                    let trigger = super::dto::workflow_trigger_from_proto(w.trigger.as_ref())
                        .unwrap_or_default();
                    super::dto::WorkflowResponse {
                        id: w.id.clone(),
                        namespace: w.namespace.clone(),
                        slug: w.slug.clone(),
                        name: w.name.clone(),
                        trigger,
                        version: row.live_version,
                        build_status: super::dto::WorkflowBuildStatus::from_status(
                            &row.build_status,
                        ),
                        build_version: Some(row.build_version),
                        latest_run_state,
                        completed_runs: completed_counts.get(&id).copied().unwrap_or(0),
                    }
                })
                .collect();
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            eprintln!("list_workflows_handler: failed to read workflows: {}", e);
            public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list workflows: {e}"),
            )
        }
    }
}

/// Query string for the workflow detail endpoint. `version` selects an explicit
/// version; when absent the handler resolves the Default version.
#[derive(Deserialize, ToSchema, IntoParams)]
struct WorkflowDetailQuery {
    version: Option<String>,
}

/// Handler for `GET /api/workflows/{workflow_id}?version=X`. Composes the
/// Workflow detail page's header + tab payload for one `(workflow_id, version)`.
///
/// Version resolution: an explicit `?version` if supplied; otherwise the
/// Default version (latest live, else latest by `inserted_at`) from the shared
/// resolver. A malformed UUID is a 400. An unknown workflow id is a 404 — and
/// so is a `?version` that names a version this workflow never had (the
/// per-version load returns no row). This is the parent 404 the calendar
/// endpoint relies on.
///
/// `latest_run_state` and `completed_runs` are workflow-aggregate (across every
/// version), reusing the same resolvers the list handler uses, so the picker
/// can re-scope the per-version cells without disturbing them.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/{workflow_id}", params(WorkflowDetailQuery), responses((status = 200, body = super::dto::WorkflowDetailResponse), (status = 400), (status = 404), (status = 500)))]
async fn get_workflow_detail_handler(
    Path(workflow_id): Path<String>,
    Query(q): Query<WorkflowDetailQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let wf = match Uuid::parse_str(&workflow_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid workflow id" })),
            )
                .into_response();
        }
    };

    // Resolve the effective version. With no `?version`, a `None` from the
    // resolver means the id has no rows at all → unknown workflow → 404.
    let effective_version: i64 = match q.version {
        Some(v) => match v.parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "invalid version" })),
                )
                    .into_response();
            }
        },
        None => match super::archive_queries::default_version(&state.pg_pool, wf).await {
            Ok(Some((version, _status))) => version,
            Ok(None) => return workflow_not_found(),
            Err(e) => {
                eprintln!("get_workflow_detail_handler: default-version resolve failed: {e}");
                return internal_error("resolve default version");
            }
        },
    };

    // Load the per-version artifacts. A miss here is a 404 whether the id is
    // unknown or the explicit `?version` names a version that never existed.
    let detail =
        match super::archive_queries::get_workflow_version(&state.pg_pool, wf, effective_version)
            .await
        {
            Ok(Some(d)) => d,
            Ok(None) => return workflow_not_found(),
            Err(e) => {
                eprintln!("get_workflow_detail_handler: version load failed: {e}");
                return internal_error("load workflow version");
            }
        };

    let available_versions =
        match super::archive_queries::list_workflow_versions(&state.pg_pool, wf).await {
            Ok(rows) => rows
                .into_iter()
                .map(|r| super::dto::AvailableVersion {
                    version: r.version,
                    status: r.status,
                    inserted_at: r.inserted_at.to_rfc3339(),
                })
                .collect(),
            Err(e) => {
                eprintln!("get_workflow_detail_handler: version list failed: {e}");
                return internal_error("list workflow versions");
            }
        };

    // Workflow-aggregate scalars — the same resolvers the list handler uses,
    // scoped to this one id. Both degrade soft: a failed live read folds to the
    // archive-only latest state, and a failed count folds to 0.
    // One resolver call yields the latest fired-instance candidate; the badge
    // reads its state, the calendar's landing year reads its `scheduled_at` —
    // derived from the same pick so they cannot name different instances.
    let latest_run =
        super::latest_run_resolver::resolve_latest_runs(&state.pg_pool, &state.coordinator, &[wf])
            .await
            .remove(&wf)
            .flatten();
    let latest_run_at = latest_run
        .as_ref()
        .and_then(|c| c.scheduled_at)
        .map(|dt| dt.to_rfc3339());
    let latest_run_state = latest_run.map(|c| c.state);
    let completed_runs = super::archive_queries::completed_run_counts(&state.pg_pool)
        .await
        .unwrap_or_else(|e| {
            eprintln!("get_workflow_detail_handler: completed-run count failed: {e}");
            Default::default()
        })
        .get(&wf)
        .copied()
        .unwrap_or(0);

    // `namespace`/`slug` ride inside the parsed `Workflow` JSONB (`definition`),
    // the inert display metadata the conductor stored at registration. Read them
    // off before the blob is moved into the response.
    let namespace = detail
        .definition
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let slug = detail
        .definition
        .get("slug")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let response = super::dto::WorkflowDetailResponse {
        workflow_id: wf.to_string(),
        namespace,
        slug,
        version: effective_version,
        nickel_source: detail.nickel_source,
        workflow_definition: detail.definition,
        available_versions,
        latest_run_state,
        latest_run_at,
        completed_runs,
    };
    (StatusCode::OK, Json(response)).into_response()
}

/// 404 body for an unknown workflow (or unknown `(id, version)`) on the detail
/// surface — distinct, clean shape so the UI renders a "workflow not found"
/// state rather than a confusing partial page.
fn workflow_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "workflow not found" })),
    )
        .into_response()
}

/// Render an error without leaking diagnostics across the HTTP trust boundary.
/// Client-error detail is preserved; every 5xx receives one stable message.
fn public_http_error(status: StatusCode, detail: impl Into<String>) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": public_error_message(status, detail.into()),
        })),
    )
        .into_response()
}

fn internal_error(step: &str) -> Response {
    public_http_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("failed to {step}"),
    )
}

/// Query string for the Run calendar endpoint.
#[derive(Deserialize, ToSchema, IntoParams)]
struct CalendarQuery {
    year: i32,
    tz: Option<String>,
}

/// Optional filters on the instance-list endpoint. `date` (with `tz`) scopes to
/// runs scheduled on a single local day — the Run calendar's click-through.
#[derive(Deserialize, ToSchema, IntoParams)]
struct InstancesQuery {
    date: Option<String>,
    tz: Option<String>,
}

/// Per-day accumulator while merging the two sources.
#[derive(Default)]
struct DayAcc {
    completed: i64,
    failed: i64,
    in_progress: i64,
    scheduled: i64,
}

/// Handler for `GET /api/workflows/{id}/calendar?year=YYYY&tz=<IANA>`.
///
/// Per-day instance counts for the Run calendar, bucketed in the client's IANA
/// timezone: terminal `completed`/`failed` from the PG archive, non-terminal
/// `in_progress`/`scheduled` from the live source, merged by date. The same
/// validated tz string threads to both halves so they agree on the bucketing.
///
/// 400 on a malformed id or unknown IANA name (before any query); 404 on an
/// unknown workflow id; PG-half failure is a 500 (load-bearing archive); a
/// live-half failure degrades to terminal-only counts with the header `false`.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/{id}/calendar", params(CalendarQuery), responses((status = 200, body = super::dto::CalendarResponse), (status = 400), (status = 404), (status = 500)))]
async fn workflow_calendar_handler(
    Path(workflow_id): Path<String>,
    Query(q): Query<CalendarQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let wf = match Uuid::parse_str(&workflow_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid workflow id" })),
            )
                .into_response();
        }
    };

    // Resolve + validate the tz once; the same string threads to PG and Rust.
    let tz_str = q.tz.clone().unwrap_or_else(|| "UTC".to_string());
    let tz: chrono_tz::Tz = match tz_str.parse() {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("unknown IANA timezone: {tz_str}") })),
            )
                .into_response();
        }
    };

    match super::archive_queries::workflow_exists(&state.pg_pool, wf).await {
        Ok(true) => {}
        Ok(false) => return workflow_not_found(),
        Err(e) => {
            eprintln!("workflow_calendar_handler: existence check failed: {e}");
            return internal_error("check workflow");
        }
    }

    let (pg_res, live_res) = tokio::join!(
        super::archive_queries::calendar_terminal_rollup(&state.pg_pool, wf, q.year, &tz_str),
        state.coordinator.list_workflow_instances(wf),
    );

    // The archive is load-bearing — no useful degraded response without it.
    let pg_rows = match pg_res {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("workflow_calendar_handler: PG rollup failed: {e}");
            return internal_error("roll up the calendar");
        }
    };

    let (live_rows, live_data_available) = match live_res {
        Ok(rows) => (rows, true),
        Err(super::coordinator_client::CoordinatorClientError::NotFound(_)) => (Vec::new(), true),
        Err(e) => {
            eprintln!(
                "workflow_calendar_handler: coordinator call failed: {:?}",
                e
            );
            (Vec::new(), false)
        }
    };

    let mut by_day: std::collections::BTreeMap<String, DayAcc> = std::collections::BTreeMap::new();
    for r in pg_rows {
        let acc = by_day.entry(r.date).or_default();
        acc.completed += r.completed;
        acc.failed += r.failed;
    }
    for inst in &live_rows {
        let Some(sched) = inst.scheduled_at.as_deref() else {
            continue;
        };
        let Ok(dt) = chrono::DateTime::parse_from_rfc3339(sched) else {
            continue;
        };
        let day = dt
            .with_timezone(&tz)
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        let acc = by_day.entry(day).or_default();
        match inst.state.as_str() {
            "InProgress" => acc.in_progress += 1,
            // Transient pre-creation states fold into `scheduled`; they exist
            // briefly during the scheduler's flow and are operator-invisible.
            "Scheduled" | "Triggered" | "PendingSchedule" => acc.scheduled += 1,
            // A terminal live row (rare; terminal live rows are retired) is already
            // counted by the PG half — don't double-count.
            _ => {}
        }
    }

    let days: Vec<super::dto::DayCounts> = by_day
        .into_iter()
        .map(|(date, a)| super::dto::DayCounts {
            date,
            completed: a.completed,
            failed: a.failed,
            in_progress: a.in_progress,
            scheduled: a.scheduled,
            total: a.completed + a.failed + a.in_progress + a.scheduled,
        })
        .collect();

    let body = super::dto::CalendarResponse {
        year: q.year,
        tz: tz_str,
        days,
        live_data_available,
    };
    let mut response = (StatusCode::OK, Json(body)).into_response();
    response.headers_mut().insert(
        LIVE_DATA_AVAILABLE_HEADER,
        axum::http::HeaderValue::from_static(if live_data_available { "true" } else { "false" }),
    );
    response
}

/// Status of a signal after the conductor accepted it.
///
/// For trigger signals: `pending` means the Postgres row exists but the
/// server hasn't yet replied with an instance-creation event;
/// `materialized` means the linkage is recorded; `terminal` means the
/// originating run reached a terminal state and the row is awaiting its
/// grace-window sweep. `captures_summary` lists the captured names.
///
/// For cancel signals: status is always `materialized`, `applied_count`
/// reports the impact surfaced by the server's relay-back (1 for Instance
/// targets, the matched-instance count for ByTag), and `captures_summary`
/// is empty.
#[derive(Serialize, ToSchema)]
struct SignalStatusResponse {
    signal_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_instance_id: Option<Uuid>,
    status: &'static str,
    /// Captured names only — values are never surfaced through this read
    /// endpoint. Values may carry sensitive data (PII, credentials passed
    /// through `inputs`); operators wanting them consult tickr-ctx directly
    /// with appropriate access controls.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    captures_summary: Vec<String>,
    /// Materialized impact count for cancel signals. Absent for trigger
    /// signals, where the lineage flows through `captures_summary` and
    /// `workflow_instance_id` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    applied_count: Option<i32>,
    /// Logical event name for wakeup signals. Absent for trigger /
    /// cancel signals.
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// Number of `waits-on-signal` workflows the wakeup fanned out to.
    /// Absent for trigger / cancel signals.
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_workflows: Option<i32>,
}

/// `GET /api/signals/{signal_id}`. Replaces the dropped `workflow_instance_id`
/// from the trigger response — consumers that previously read the instance
/// id synchronously now query this endpoint once the linkage materializes.
/// Returns 404 for unknown signal_ids and for rows the grace-window sweep
/// has already deleted.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/signals/{signal_id}", responses((status = 200, body = SignalStatusResponse), (status = 400), (status = 404), (status = 500)))]
async fn get_signal_status_handler(
    Path(signal_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let sid = match Uuid::parse_str(&signal_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid signal id"})),
            )
                .into_response();
        }
    };

    // The three tables share `signal_id` keyspace, but a wakeup ingress
    // that fans out into instances may have captures written too — so
    // signal_wakeups is checked first to disambiguate the wakeup
    // response shape from the trigger-captures shape. Cancel rows have
    // no overlap with the other two.
    match crate::signal_wakeups::read(state.pg_pool.as_ref(), sid).await {
        Ok(Some(row)) => {
            return (
                StatusCode::OK,
                Json(SignalStatusResponse {
                    signal_id: row.signal_id,
                    workflow_id: None,
                    workflow_instance_id: None,
                    status: "materialized",
                    captures_summary: Vec::new(),
                    applied_count: None,
                    name: Some(row.name),
                    matched_workflows: Some(row.matched_workflows),
                }),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!(
                "get_signal_status_handler: signal_wakeups read failed: {}",
                e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("signal lookup: {e}"),
            );
        }
    }

    match crate::signal_captures::read(state.pg_pool.as_ref(), sid).await {
        Ok(Some(row)) => {
            let status = if row.terminal_at.is_some() {
                "terminal"
            } else if row.materialized_run_id.is_some() {
                "materialized"
            } else {
                "pending"
            };
            let captures_summary = row.captures.iter().map(|c| c.name.clone()).collect();
            return (
                StatusCode::OK,
                Json(SignalStatusResponse {
                    signal_id: row.signal_id,
                    workflow_id: Some(row.workflow_id),
                    workflow_instance_id: row.materialized_run_id,
                    status,
                    captures_summary,
                    applied_count: None,
                    name: None,
                    matched_workflows: None,
                }),
            )
                .into_response();
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!(
                "get_signal_status_handler: signal_captures read failed: {}",
                e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("signal lookup: {e}"),
            );
        }
    }

    match crate::signal_cancels::read(state.pg_pool.as_ref(), sid).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(SignalStatusResponse {
                signal_id: row.signal_id,
                workflow_id: None,
                workflow_instance_id: None,
                status: "materialized",
                captures_summary: Vec::new(),
                applied_count: Some(row.applied_count),
                name: None,
                matched_workflows: None,
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "signal not found or purged"})),
        )
            .into_response(),
        Err(e) => {
            eprintln!(
                "get_signal_status_handler: signal_cancels read failed: {}",
                e
            );
            public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("signal lookup: {e}"),
            )
        }
    }
}

/// Query parameters for the dashboard route: optional `start_time` and
/// `end_time` (unix seconds). The legacy `live` boolean is intentionally
/// dropped — the route always returns merged counts.
#[derive(Deserialize, ToSchema, IntoParams)]
struct DashboardQuery {
    start_time: Option<i64>,
    end_time: Option<i64>,
}

/// Handler for `GET /api/dashboard/clock?start=&end=`. Runs the archive read
/// (PG, windowed by `scheduled_at`) and the live read (coordinator single-node
/// query) in parallel, merges them by id with archive-wins, and returns the
/// day's instances plus `live_data_available`. The flag is `false` only when
/// the *live* half failed; an archive failure degrades to an empty archive
/// half without flipping the flag (the live picture is still whole).
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/dashboard/clock", params(DashboardQuery), responses((status = 200, body = super::dto::ClockResponse), (status = 400)))]
async fn dashboard_clock_handler(
    axum::extract::Query(query): axum::extract::Query<DashboardQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // The browser sends the operator's local calendar-day boundaries as unix
    // seconds; treat negative or out-of-range values as a bad request rather
    // than silently coercing.
    let to_datetime = |s: Option<i64>| -> Result<Option<chrono::DateTime<chrono::Utc>>, String> {
        match s {
            None => Ok(None),
            Some(secs) => chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
                .map(Some)
                .ok_or_else(|| format!("timestamp {} out of range", secs)),
        }
    };
    let (start, end) = match (to_datetime(query.start_time), to_datetime(query.end_time)) {
        (Ok(s), Ok(e)) => (s, e),
        (Err(e), _) | (_, Err(e)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
                .into_response();
        }
    };

    let (archive_res, live_res) = tokio::join!(
        super::archive_queries::list_dashboard_instances(&state.pg_pool, start, end),
        state
            .coordinator
            .dashboard_clock(query.start_time, query.end_time),
    );

    let archive_rows: Vec<super::dto::ClockInstance> = match archive_res {
        Ok(rows) => rows
            .into_iter()
            .map(|r| super::dto::ClockInstance {
                id: r.id.to_string(),
                workflow_id: r.workflow_id.to_string(),
                workflow_name: r.workflow_name,
                scheduled_at: r.scheduled_at.map(|dt| dt.to_rfc3339()),
                state: r.state,
            })
            .collect(),
        Err(e) => {
            eprintln!("dashboard_clock_handler: archive query failed: {}", e);
            Vec::new()
        }
    };

    let (live_rows, live_data_available) = match live_res {
        Ok(rows) => (rows, true),
        Err(e) => {
            eprintln!("dashboard_clock_handler: coordinator call failed: {:?}", e);
            (Vec::new(), false)
        }
    };

    let instances = super::live_archive_merge::merge_clock_instances(live_rows, archive_rows);
    (
        StatusCode::OK,
        Json(super::dto::ClockResponse {
            instances,
            live_data_available,
        }),
    )
        .into_response()
}

/// Query parameters for the upcoming route: optional `limit` (default 3).
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
struct UpcomingQuery {
    limit: Option<u32>,
}

/// Handler for `GET /api/dashboard/upcoming`. Reads the live `Scheduled`
/// instances from the coordinator (single-node query), sorts them by `next_run_at`
/// ASC, and trims to `limit` (default 3). Fire-now and waits-on-signal
/// workflows never produce a `Scheduled` instance, so they are inherently
/// absent. Up next has no archive half — on a coordinator failure the strip is
/// empty and the UI renders its "Nothing scheduled." state.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/dashboard/upcoming", params(UpcomingQuery), responses((status = 200, body = Vec<super::dto::UpcomingInstanceResponse>)))]
async fn dashboard_upcoming_handler(
    axum::extract::Query(query): axum::extract::Query<UpcomingQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(20) as usize;
    let rows = match state.coordinator.dashboard_upcoming().await {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!(
                "dashboard_upcoming_handler: coordinator call failed: {:?}",
                e
            );
            Vec::new()
        }
    };
    (StatusCode::OK, Json(order_upcoming(rows, limit))).into_response()
}

/// Sort upcoming rows by their absolute next-run time ascending and trim to
/// `limit`. Rows missing a time sort last. Compared as parsed datetimes rather
/// than RFC3339 strings so variable fractional-second widths can't reorder runs.
fn order_upcoming(
    mut rows: Vec<super::dto::UpcomingInstanceResponse>,
    limit: usize,
) -> Vec<super::dto::UpcomingInstanceResponse> {
    let parse_ts = |s: &Option<String>| {
        s.as_deref()
            .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok())
    };
    rows.sort_by(
        |a, b| match (parse_ts(&a.next_run_at), parse_ts(&b.next_run_at)) {
            (Some(x), Some(y)) => x.cmp(&y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        },
    );
    rows.truncate(limit);
    rows
}

/// Handler for `GET /api/workflows/{id}/instances`. Merges archive (PG) with
/// live (coordinator HTTP). Issues both reads in parallel; on coordinator failure
/// or timeout, falls back to archive-only with `live_data_available: false`.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/{id}/instances", params(InstancesQuery), responses((status = 200, body = Vec<super::dto::WorkflowInstanceResponse>), (status = 400), (status = 500)))]
async fn list_workflow_instances_handler(
    Path(workflow_id): Path<String>,
    Query(q): Query<InstancesQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let wf = match uuid::Uuid::parse_str(&workflow_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workflow id"})),
            )
                .into_response();
        }
    };

    // Optional calendar click-through filter: only runs scheduled on `date`,
    // bucketed into `tz`. Validate the tz up front (when a date is supplied) so
    // PG and the client-side live filter agree; an unknown IANA name is a 400.
    let tz_str = q.tz.clone().unwrap_or_else(|| "UTC".to_string());
    if q.date.is_some() && tz_str.parse::<chrono_tz::Tz>().is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("unknown IANA timezone: {tz_str}")})),
        )
            .into_response();
    }

    // Parallel: archive (PG) and live (coordinator HTTP). Independent failure
    // domains; each can produce its half without waiting on the other.
    let (archive_res, coordinator_res) = tokio::join!(
        async {
            match &q.date {
                Some(date) => {
                    super::archive_queries::list_workflow_instances_on_date(
                        &state.pg_pool,
                        wf,
                        date,
                        &tz_str,
                    )
                    .await
                }
                None => {
                    super::archive_queries::list_workflow_instances_by_workflow(&state.pg_pool, wf)
                        .await
                }
            }
        },
        state.coordinator.list_workflow_instances(wf),
    );

    let archive_rows: Vec<super::dto::WorkflowInstanceResponse> = match archive_res {
        Ok(rows) => rows.into_iter().map(project_archived_instance).collect(),
        Err(e) => {
            eprintln!(
                "list_workflow_instances_handler: archive query failed for workflow {}: {}",
                wf, e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("archive query: {e}"),
            );
        }
    };

    let (live_rows, live_data_available) = match coordinator_res {
        Ok(rows) => (rows, true),
        Err(super::coordinator_client::CoordinatorClientError::NotFound(_)) => {
            // Coordinator says no live instances for this workflow id. Still a
            // successful answer — the live half is just empty.
            (Vec::new(), true)
        }
        Err(e) => {
            // Timeout / Unreachable / Server / Decode all degrade gracefully
            // — the archive half still serves. Log so operators can attribute
            // the degraded view to a specific upstream incident.
            eprintln!(
                "list_workflow_instances_handler: coordinator call failed for workflow {}: {:?}",
                wf, e
            );
            (Vec::new(), false)
        }
    };

    // Apply the same date predicate to the (small) live set client-side, so the
    // filtered list is consistent across both stores.
    let live_rows = match &q.date {
        Some(date) => {
            let tz: chrono_tz::Tz = tz_str.parse().expect("tz validated above");
            live_rows
                .into_iter()
                .filter(|inst| {
                    inst.scheduled_at
                        .as_deref()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| {
                            dt.with_timezone(&tz)
                                .date_naive()
                                .format("%Y-%m-%d")
                                .to_string()
                                == *date
                        })
                        .unwrap_or(false)
                })
                .collect()
        }
        None => live_rows,
    };

    let mut merged = super::live_archive_merge::merge_instances(live_rows, archive_rows);
    // Sort newest scheduled_at first so list views show recent runs at the
    // top regardless of which store produced each row.
    merged.sort_by(|a, b| b.scheduled_at.cmp(&a.scheduled_at));

    // `live_data_available` rides on a response header rather than wrapping
    // the array in an envelope, so existing UI clients consuming the array
    // shape stay drop-in compatible. Clients that want the degraded-state
    // indicator opt-in by reading the header.
    let mut response = (StatusCode::OK, Json(merged)).into_response();
    response.headers_mut().insert(
        LIVE_DATA_AVAILABLE_HEADER,
        axum::http::HeaderValue::from_static(if live_data_available { "true" } else { "false" }),
    );
    response
}

/// Projects an archive read row for a task instance into the UI's response
/// shape. The row is already the data-plane-visible projection (its `task_type`
/// and `state` are the rendered variant tokens), so this is a field copy.
fn project_archived_task(
    ti: tickr_proto::instance::ArchivedTaskInstance,
) -> super::dto::TaskInstanceResponse {
    super::dto::TaskInstanceResponse {
        id: ti.id,
        task_id: ti.task_id,
        workflow_instance_id: ti.workflow_instance_id,
        workflow_id: ti.workflow_id,
        name: ti.name,
        task_type: ti.task_type,
        state: ti.state,
        executor_id: ti.executor_id,
        attempt: ti.attempt,
    }
}

/// Handler for `GET /api/workflows/instances/{id}/tasks`. Merges archive (PG)
/// with live (coordinator HTTP). Mirrors `list_workflow_instances_handler`'s
/// shape: parallel reads, terminal-state merge, `live_data_available` on the
/// envelope so the UI can render a degraded indicator.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}/tasks", responses((status = 200, body = Vec<super::dto::TaskInstanceResponse>), (status = 400), (status = 500)))]
async fn list_task_instances_handler(
    Path(workflow_instance_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let wi = match uuid::Uuid::parse_str(&workflow_instance_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workflow instance id"})),
            )
                .into_response();
        }
    };

    let (archive_res, coordinator_res) = tokio::join!(
        super::archive_queries::list_task_instances(&state.pg_pool, wi),
        state.coordinator.list_task_instances(wi),
    );

    let archive_rows: Vec<super::dto::TaskInstanceResponse> = match archive_res {
        Ok(rows) => rows.into_iter().map(project_archived_task).collect(),
        Err(e) => {
            eprintln!(
                "list_task_instances_handler: archive query failed for instance {}: {}",
                wi, e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("archive query: {e}"),
            );
        }
    };

    let (live_rows, live_data_available) = match coordinator_res {
        Ok(rows) => (rows, true),
        Err(super::coordinator_client::CoordinatorClientError::NotFound(_)) => (Vec::new(), true),
        Err(e) => {
            eprintln!(
                "list_task_instances_handler: coordinator call failed for instance {}: {:?}",
                wi, e
            );
            (Vec::new(), false)
        }
    };

    let merged = super::live_archive_merge::merge_tasks(live_rows, archive_rows);

    let mut response = (StatusCode::OK, Json(merged)).into_response();
    response.headers_mut().insert(
        LIVE_DATA_AVAILABLE_HEADER,
        axum::http::HeaderValue::from_static(if live_data_available { "true" } else { "false" }),
    );
    response
}

/// Projects an archive read row for a workflow instance into the UI's response
/// shape. For terminal rows all known tasks have finished by definition, so
/// `task_count == completed_tasks` here; future slices may join with
/// `task_instances` for per-task state if the UI ever needs the distinction.
fn project_archived_instance(
    row: tickr_proto::instance::ArchivedInstanceRow,
) -> super::dto::WorkflowInstanceResponse {
    let task_count = row.task_count as usize;
    super::dto::WorkflowInstanceResponse {
        id: row.id,
        workflow_id: row.workflow_id,
        workflow_version: row.workflow_version,
        name: row.name,
        state: row.state,
        scheduled_at: row.scheduled_at,
        task_count,
        completed_tasks: task_count,
    }
}

/// Handler for `GET /api/workflows/instances/{id}` — the **instance
/// snapshot**. PG-first dispatch: hit the conductor's archive and project the
/// snapshot (`storage: archived`) from the JSONB instance plus its archived
/// task instances; on miss, fall through to the coordinator, which serves the
/// identical shape with `storage: live`. On PG miss + coordinator
/// timeout / unreachable / 503, return 503 so the UI can distinguish
/// "instance gone" from "live store unreachable".
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}", responses((status = 200, body = super::openapi::InstanceSnapshotDoc), (status = 400), (status = 404), (status = 500), (status = 503)))]
async fn get_workflow_instance_handler(
    Path(instance_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let id = match uuid::Uuid::parse_str(&instance_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid workflow instance id",
                })),
            )
                .into_response();
        }
    };

    // 1. Archive first. Terminal instances live here; the archive read seam
    //    rehydrates the archive-grade projection (the instance blob plus its
    //    task-instance blobs) already reduced to the data-plane-visible shape.
    //    Stamping it `storage: archived` yields the same instance-snapshot
    //    response the live path serves — identical by construction.
    match super::archive_queries::get_workflow_instance(&state.pg_pool, id).await {
        Ok(Some(archived)) => {
            let snapshot =
                tickr_proto::codec::archive::snapshot_from_archived(archived, "archived");
            return (StatusCode::OK, Json(snapshot)).into_response();
        }
        Ok(None) => { /* fall through to live */ }
        Err(e) => {
            eprintln!(
                "get_workflow_instance_handler: archive lookup failed for {}: {}",
                id, e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("archive lookup failed: {e}"),
            );
        }
    }

    // 2. Live fallback. Coordinator's cluster_query is the live source of truth.
    match state.coordinator.get_workflow_instance(id).await {
        Ok(live) => (StatusCode::OK, Json(live)).into_response(),
        Err(super::coordinator_client::CoordinatorClientError::NotFound(_)) => {
            // Neither PG nor coordinator has the instance. Genuinely gone.
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "workflow instance not found",
                })),
            )
                .into_response()
        }
        Err(e @ super::coordinator_client::CoordinatorClientError::Timeout)
        | Err(e @ super::coordinator_client::CoordinatorClientError::Unreachable(_))
        | Err(e @ super::coordinator_client::CoordinatorClientError::Server { status: 503 }) => {
            // Live store unreachable — either the coordinator itself is down
            // (timeout/connect) or the coordinator reported its cluster query
            // failed (its own 503). Surface as 503 so the UI shows "live
            // store unavailable" rather than "not found".
            eprintln!(
                "get_workflow_instance_handler: coordinator call failed for {}: {:?}",
                id, e
            );
            public_http_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("live store unreachable: {e}"),
            )
        }
        Err(e) => {
            eprintln!(
                "get_workflow_instance_handler: coordinator returned error for {}: {:?}",
                id, e
            );
            public_http_error(StatusCode::BAD_GATEWAY, format!("coordinator error: {e}"))
        }
    }
}

/// Satisfied signal-gate signal ids on an archived instance — the gate
/// scopes the Context tab groups by. Nil ids (predicate/timer satisfaction,
/// which has no wakeup lineage) are excluded because they key no ctx scope.
fn satisfied_gate_signal_ids(archived: &tickr_proto::instance::ArchivedInstance) -> Vec<String> {
    let mut ids: Vec<String> = archived
        .graph
        .iter()
        .flat_map(|g| g.edges.iter())
        .flat_map(|e| e.gates.iter())
        .filter(|gate| gate.state == "Satisfied")
        .filter_map(|gate| gate.signal_id.clone())
        .filter(|sid| {
            uuid::Uuid::parse_str(sid)
                .map(|u| !u.is_nil())
                .unwrap_or(false)
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Handler for `GET /api/workflows/instances/{id}/context` — the run's
/// tickr-ctx scope in run / trigger / gate groupings (the Context tab).
///
/// Archive-first like the snapshot: archived instances read the compaction
/// enrichment from `workflow_run_info`; live instances read the tenant's
/// NATS KV directly, read-only (the ctx KV reader). Both paths classify
/// through the same pure grouping function. A missing scope is an empty
/// grouping; a KV connection failure is a 503 — "no values" and "ctx store
/// unreachable" stay distinguishable.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}/context", responses((status = 200, body = super::ctx_reader::InstanceContextResponse), (status = 400), (status = 404), (status = 500), (status = 503)))]
async fn get_instance_context_handler(
    Path(instance_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let id = match uuid::Uuid::parse_str(&instance_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workflow instance id"})),
            )
                .into_response();
        }
    };

    // 1. Archive first: the enrichment is the terminal instance's scope dump.
    match super::archive_queries::get_workflow_instance(&state.pg_pool, id).await {
        Ok(Some(archived)) => {
            let enrichment =
                match super::archive_queries::get_run_info_ctx_envelope(&state.pg_pool, id).await {
                    Ok(v) => v.unwrap_or(serde_json::Value::Array(Vec::new())),
                    Err(e) => {
                        eprintln!(
                            "get_instance_context_handler: run-info lookup failed for {}: {}",
                            id, e
                        );
                        return public_http_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("archive lookup: {e}"),
                        );
                    }
                };
            let entries = super::ctx_reader::entries_from_enrichment(&enrichment);
            // The originating (trigger) signal id and the satisfied gate ids are
            // read from the archive projection's trigger provenance and
            // per-gate state, matching the live snapshot fields.
            let trigger_sid = archived
                .triggered_by
                .as_ref()
                .and_then(|p| p.signal_id.clone());
            let gate_sids = satisfied_gate_signal_ids(&archived);
            let groups = super::ctx_reader::classify_entries(
                &entries,
                &id.to_string(),
                trigger_sid.as_deref(),
                &gate_sids,
            );
            return (
                StatusCode::OK,
                Json(super::ctx_reader::InstanceContextResponse {
                    storage: "archived".to_string(),
                    run: groups.run,
                    trigger: groups.trigger,
                    gates: groups.gates,
                }),
            )
                .into_response();
        }
        Ok(None) => { /* fall through to live */ }
        Err(e) => {
            eprintln!(
                "get_instance_context_handler: archive lookup failed for {}: {}",
                id, e
            );
            return public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("archive lookup: {e}"),
            );
        }
    }

    // 2. Live: learn the scope ids from the live snapshot, then read the KV.
    let snapshot = match state.coordinator.get_workflow_instance(id).await {
        Ok(s) => s,
        Err(super::coordinator_client::CoordinatorClientError::NotFound(_)) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "workflow instance not found"})),
            )
                .into_response();
        }
        Err(e) => {
            eprintln!("get_instance_context_handler: live store failed for {id}: {e}");
            return public_http_error(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("live store unreachable: {e}"),
            );
        }
    };

    let trigger_sid = snapshot
        .triggered_by
        .as_ref()
        .and_then(|p| p.signal_id.clone());
    let nil = uuid::Uuid::nil().to_string();
    // The instance-graph gate rendering reads the proto snapshot's per-gate
    // state directly (`state` is the rendered token, `signal_id` the satisfied
    // lineage) — no server `GateState` is matched on the live-read path.
    let mut gate_sids: Vec<String> = snapshot
        .graph
        .iter()
        .flat_map(|g| g.edges.iter())
        .flat_map(|e| e.gates.iter())
        .filter(|g| g.state == "Satisfied")
        .filter_map(|g| g.signal_id.clone())
        .filter(|sid| sid != &nil)
        .collect();
    gate_sids.sort();
    gate_sids.dedup();

    let mut prefixes = vec![id.to_string()];
    if let Some(sid) = &trigger_sid {
        prefixes.push(sid.clone());
    }
    prefixes.extend(gate_sids.iter().cloned());

    match super::ctx_reader::read_live_entries(&state.nats, &prefixes).await {
        Ok(entries) => {
            let groups = super::ctx_reader::classify_entries(
                &entries,
                &id.to_string(),
                trigger_sid.as_deref(),
                &gate_sids,
            );
            (
                StatusCode::OK,
                Json(super::ctx_reader::InstanceContextResponse {
                    storage: "live".to_string(),
                    run: groups.run,
                    trigger: groups.trigger,
                    gates: groups.gates,
                }),
            )
                .into_response()
        }
        Err(e) => {
            eprintln!("get_instance_context_handler: live context read failed for {id}: {e}");
            public_http_error(StatusCode::SERVICE_UNAVAILABLE, e.to_string())
        }
    }
}

/// Handler for `GET /api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_instance_id}/logs`.
/// Dispatches to MinIO (terminal logs) or NATS KV (live logs) via the
/// `LogsResolver`; returns decompressed content as `TaskLogResponse { logs }`
/// matching the UI's existing schema.
/// Query surface of the task-logs endpoint. `after_seq` is the Log cursor;
/// `tail_batches` (+ optional `before_seq`) is the tail-first load and its
/// "load earlier" paging; `download` serves the full log as a file. With no
/// params the endpoint returns today's full-content JSON.
#[derive(Deserialize, ToSchema, IntoParams)]
struct TaskLogsQuery {
    after_seq: Option<u64>,
    tail_batches: Option<usize>,
    before_seq: Option<u64>,
    download: Option<bool>,
}

/// Shape a batch page into the wire response. Batch payloads are UTF-8 log
/// text by construction (the executor publishes joined lines); invalid bytes
/// are replaced rather than erroring a whole page.
fn batch_response(
    page: super::logs_resolver::LogBatchPage,
    last_seq: Option<u64>,
) -> super::dto::TaskLogBatchResponse {
    super::dto::TaskLogBatchResponse {
        batches: page
            .batches
            .into_iter()
            .map(|b| super::dto::TaskLogBatch {
                seq: b.seq,
                text: String::from_utf8_lossy(&b.bytes).into_owned(),
            })
            .collect(),
        last_seq,
        has_earlier: page.has_earlier,
        marker_present: page.marker.is_some(),
        exit_status: page.marker.as_ref().map(|m| m.exit_status),
        exit_reason: page.marker.and_then(|m| m.reason),
    }
}

fn logs_error_response(e: super::logs_resolver::LogsError, ti: Uuid) -> Response {
    match e {
        super::logs_resolver::LogsError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "logs not found"})),
        )
            .into_response(),
        e => {
            eprintln!("get_task_logs_handler: resolver error for {}: {:?}", ti, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "logs unavailable"})),
            )
                .into_response()
        }
    }
}

#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_instance_id}/logs", params(TaskLogsQuery), responses((status = 200, body = super::openapi::TaskLogsDocument), (status = 400), (status = 404), (status = 500)))]
async fn get_task_logs_handler(
    Path((workflow_id, workflow_instance_id, task_instance_id)): Path<(String, String, String)>,
    Query(query): Query<TaskLogsQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let parse = (
        uuid::Uuid::parse_str(&workflow_id),
        uuid::Uuid::parse_str(&workflow_instance_id),
        uuid::Uuid::parse_str(&task_instance_id),
    );
    let (wf, wi, ti) = match parse {
        (Ok(wf), Ok(wi), Ok(ti)) => (wf, wi, ti),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid uuid in path"})),
            )
                .into_response();
        }
    };

    // Download flag: the full log as a file, whichever store holds it.
    if query.download.unwrap_or(false) {
        return match state.logs.fetch_task_logs(wf, wi, ti).await {
            Ok(task_logs) => (
                StatusCode::OK,
                [
                    (
                        header::CONTENT_TYPE,
                        "text/plain; charset=utf-8".to_string(),
                    ),
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"task-{}.log\"", ti),
                    ),
                ],
                task_logs.content,
            )
                .into_response(),
            Err(e) => logs_error_response(e, ti),
        };
    }

    // Cursor poll: only batches past `after_seq`, with the advancing cursor.
    if let Some(after_seq) = query.after_seq {
        return match state.logs.fetch_batches_after(wf, wi, ti, after_seq).await {
            Ok(page) => {
                let last_seq = page.batches.iter().map(|b| b.seq).max().or(Some(after_seq));
                (StatusCode::OK, Json(batch_response(page, last_seq))).into_response()
            }
            Err(e) => logs_error_response(e, ti),
        };
    }

    // Tail-first load (optionally paging backwards via `before_seq`).
    if let Some(tail) = query.tail_batches {
        let tail = tail.clamp(1, 2_000);
        return match state
            .logs
            .fetch_tail(wf, wi, ti, tail, query.before_seq)
            .await
        {
            Ok(page) => {
                let last_seq = page.batches.iter().map(|b| b.seq).max();
                (StatusCode::OK, Json(batch_response(page, last_seq))).into_response()
            }
            Err(e) => logs_error_response(e, ti),
        };
    }

    match state.logs.fetch_task_logs(wf, wi, ti).await {
        Ok(task_logs) => {
            let logs = match String::from_utf8(task_logs.content) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "get_task_logs_handler: non-utf8 log content for {}: {}",
                        ti, e
                    );
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": "log content is not valid UTF-8"})),
                    )
                        .into_response();
                }
            };
            let response = super::dto::TaskLogResponse {
                logs,
                marker_present: task_logs.marker.is_some(),
                exit_status: task_logs.marker.as_ref().map(|m| m.exit_status),
                exit_reason: task_logs.marker.and_then(|m| m.reason),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(super::logs_resolver::LogsError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "logs not found"})),
        )
            .into_response(),
        Err(e) => {
            eprintln!("get_task_logs_handler: resolver error for {}: {:?}", ti, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "logs unavailable"})),
            )
                .into_response()
        }
    }
}

/// Request body for `POST /api/workflows/register`. `nickel_source` is the
/// submitted Core DSL source; `namespace` is the optional grouping segment CI
/// reads from a repo-level convention file. Absent `namespace` normalises to
/// `default` in the conductor before identity derivation.
#[derive(Deserialize, ToSchema)]
struct RegisterBody {
    nickel_source: String,
    #[serde(default)]
    namespace: String,
}

#[derive(Serialize, ToSchema)]
enum RegisterSettledStatus {
    NoOp,
    Refreshed,
}

/// Immediate, non-building registration outcome (HTTP 200).
#[derive(Serialize, ToSchema)]
struct RegisterSettledResponse {
    // Keep declaration order aligned with serde_json::Map's sorted-key output
    // so replacing json! does not perturb the response bytes.
    message: String,
    status: RegisterSettledStatus,
    workflow_id: String,
    workflow_version: i64,
}

#[derive(Serialize, ToSchema)]
enum RegisterQueuedStatus {
    Building,
    BuildRequeued,
}

/// Registration outcome that has build work queued (HTTP 202).
#[derive(Serialize, ToSchema)]
struct RegisterQueuedResponse {
    message: String,
    status: RegisterQueuedStatus,
    task_count: u32,
    workflow_id: String,
    workflow_version: i64,
}

/// `POST /api/workflows/register`. Forwards the submitted Nickel source to the
/// conductor as a `Register` command and renders the reply into today's
/// register response shape. The `Idempotency-Key` header has no register
/// semantics today and is ignored on this path.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/register", responses((status = 200, body = RegisterSettledResponse), (status = 202, body = RegisterQueuedResponse), (status = 400), (status = 408), (status = 409), (status = 500)))]
async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterBody>,
) -> Response {
    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Register(
            api::RegisterRequest {
                nickel_source: body.nickel_source,
                namespace: body.namespace,
            },
        )),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.register).await {
        Ok(resp) => render_register(resp),
        Err(e) => bus_error_response(e),
    }
}

/// Render a register `ApiCommandResponse` into the HTTP body, forwarding the
/// conductor's `status_code` verbatim. Successful outcomes use the precise
/// settled (200) or queued (202) response DTO; an `ErrorPayload` (400 / 408 /
/// 500) renders register's historical `{success:false, message}` shape — the
/// API supplies the `success:false` framing since the proto error variant
/// carries only the message.
fn render_register(resp: api::ApiCommandResponse) -> Response {
    use api::api_command_response::Payload;
    use api::register_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Register(rp)) => match rp.outcome {
            Some(Outcome::Inserted(i)) => (
                status,
                Json(RegisterQueuedResponse {
                    message: i.message,
                    status: RegisterQueuedStatus::Building,
                    task_count: i.task_count,
                    workflow_id: i.workflow_id,
                    workflow_version: i.workflow_version,
                }),
            )
                .into_response(),
            Some(Outcome::NoOp(n)) => (
                status,
                Json(RegisterSettledResponse {
                    message: n.message,
                    status: RegisterSettledStatus::NoOp,
                    workflow_id: n.workflow_id,
                    workflow_version: n.workflow_version,
                }),
            )
                .into_response(),
            Some(Outcome::Refreshed(r)) => (
                status,
                Json(RegisterSettledResponse {
                    message: r.message,
                    status: RegisterSettledStatus::Refreshed,
                    workflow_id: r.workflow_id,
                    workflow_version: r.workflow_version,
                }),
            )
                .into_response(),
            Some(Outcome::BuildRequeued(b)) => (
                status,
                Json(RegisterQueuedResponse {
                    message: b.message,
                    status: RegisterQueuedStatus::BuildRequeued,
                    task_count: b.task_count,
                    workflow_id: b.workflow_id,
                    workflow_version: b.workflow_version,
                }),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({
                "success": false,
                "message": public_error_message(status, ep.message),
            })),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Request body for `POST /api/workflows/{id}/trigger`. Same shape today's
/// conductor handler accepts: optional `scheduled_at` and `inputs`, both
/// defaulted so an empty body is valid.
#[derive(Deserialize, Default, ToSchema)]
struct TriggerBody {
    #[serde(default)]
    scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    inputs: Option<serde_json::Value>,
    /// Optional human-readable Run name for the materialized instance. A
    /// top-level sibling of `inputs` — run display metadata, not a task
    /// capture, so it never enters tickr-ctx scope.
    #[serde(default)]
    name: Option<String>,
}

/// Max Run name length, in characters. A supplied name longer than this is
/// rejected with a 400 rather than silently truncated.
const RUN_NAME_MAX_CHARS: usize = 200;

/// Whether a supplied Run name is within the length cap. Measured in `chars`
/// (not bytes) so a multibyte name is capped by visible length, and on the raw
/// value — trimming/default handling happens later, at materialization.
fn run_name_within_cap(name: &str) -> bool {
    name.chars().count() <= RUN_NAME_MAX_CHARS
}

/// Successful trigger response. Byte-identical to the conductor's
/// `TriggerResponse`: the `scheduled_at` echo is the request's value
/// serialized through the same chrono serde, so the wire string matches what
/// the HTTP path produced.
#[derive(Serialize, ToSchema)]
#[schema(as = TriggerResult)]
struct TriggerResponse {
    signal_id: Uuid,
    scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    deduplicated: bool,
}

/// `POST /api/workflows/{workflow_id}/trigger`. Forwards a one-shot trigger to
/// the conductor over the command bus. `SignalSource::Manual` is stamped
/// conductor-side (preserving today's HTTP behavior); the `Idempotency-Key`
/// header rides on the proto request.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/{workflow_id}/trigger", responses((status = 202, body = TriggerResponse), (status = 400), (status = 404), (status = 408), (status = 409), (status = 500)))]
async fn trigger_handler(
    Path(workflow_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<TriggerBody>>,
) -> Response {
    if Uuid::parse_str(&workflow_id).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid workflow id"})),
        )
            .into_response();
    }
    let req = body.map(|Json(b)| b).unwrap_or_default();
    // Reject an over-long Run name up front — no silent truncation. Measured
    // on the raw supplied value; trimming/default handling happens server-side
    // at materialization.
    if let Some(name) = req.name.as_ref() {
        if !run_name_within_cap(name) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("run name exceeds {RUN_NAME_MAX_CHARS} characters")
                })),
            )
                .into_response();
        }
    }
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());
    let inputs = match req.inputs.as_ref() {
        Some(v) => match serde_json::to_vec(v) {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "failed to serialize inputs"})),
                )
                    .into_response();
            }
        },
        None => None,
    };

    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Trigger(
            api::TriggerRequest {
                workflow_id,
                scheduled_at: req.scheduled_at.map(|dt| dt.to_rfc3339()),
                inputs,
                idempotency_key,
                name: req.name.clone(),
            },
        )),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.trigger).await {
        Ok(resp) => render_trigger(resp, req.scheduled_at),
        Err(e) => bus_error_response(e),
    }
}

/// Render a trigger `ApiCommandResponse` into today's HTTP body shape. The
/// `scheduled_at` echo uses the request's value so its serialization matches
/// the conductor's HTTP handler exactly.
fn render_trigger(
    resp: api::ApiCommandResponse,
    req_scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Response {
    use api::api_command_response::Payload;
    use api::trigger_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Trigger(tp)) => match tp.outcome {
            Some(Outcome::Fresh(f)) => (
                status,
                Json(TriggerResponse {
                    signal_id: Uuid::parse_str(&f.signal_id).unwrap_or_default(),
                    scheduled_at: req_scheduled_at,
                    deduplicated: false,
                }),
            )
                .into_response(),
            Some(Outcome::Deduplicated(d)) => (
                status,
                Json(TriggerResponse {
                    signal_id: Uuid::parse_str(&d.original_signal_id).unwrap_or_default(),
                    scheduled_at: req_scheduled_at,
                    deduplicated: true,
                }),
            )
                .into_response(),
            Some(Outcome::Conflict(c)) => (
                status,
                Json(serde_json::json!({
                    "error": "idempotency key reused with a different payload",
                    "original_signal_id": c.original_signal_id,
                    "original_input_hash": c.original_input_hash,
                    "your_input_hash": c.your_input_hash,
                })),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({"error": public_error_message(status, ep.message)})),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Request body for `POST /api/signals/wakeup`. `name` is the logical event;
/// `payload` is the free-form JSON the conductor evaluates predicates and
/// captures against. The conductor's `target` field is accepted-and-ignored
/// today (reserved), so it's simply not modelled here — extra body fields are
/// dropped by serde.
#[derive(Deserialize, Default, ToSchema)]
struct WakeupBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

/// Discriminated wakeup response. Byte-identical to the conductor's: the
/// `skip_serializing_if` set is what makes the Fresh / Deduplicated bodies
/// differ in field presence.
#[derive(Serialize, ToSchema)]
struct WakeupResponse {
    signal_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_workflows: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gates_matched: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deduplicated: Option<bool>,
}

/// `POST /api/signals/wakeup`. Forwards a named external event to the
/// conductor over the command bus. Name validation matches today's handler
/// (missing / empty -> 400) and short-circuits before the bus round-trip.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/signals/wakeup", responses((status = 202, body = WakeupResponse), (status = 400), (status = 408), (status = 409), (status = 500)))]
async fn wakeup_handler(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<WakeupBody>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let Some(name) = req.name else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing `name`"})),
        )
            .into_response();
    };
    if name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "`name` must not be empty"})),
        )
            .into_response();
    }
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());
    let payload = match req.payload.as_ref() {
        Some(v) => match serde_json::to_vec(v) {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "failed to serialize payload"})),
                )
                    .into_response();
            }
        },
        None => None,
    };

    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Wakeup(api::WakeupRequest {
            name,
            payload,
            idempotency_key,
        })),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.wakeup).await {
        Ok(resp) => render_wakeup(resp),
        Err(e) => bus_error_response(e),
    }
}

/// External Patch submission body: the raw Nickel document. The target
/// instance is the `{id}` path segment, so a Patch is always instance-targeted.
#[derive(Deserialize, ToSchema)]
struct PatchBody {
    /// Raw Nickel Patch document; evaluates to `{ ops, reason? }`.
    nickel_source: String,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum PatchAcceptedStatus {
    Accepted,
}

#[derive(Serialize, ToSchema)]
struct PatchAcceptedResponse {
    patch_id: String,
    status: PatchAcceptedStatus,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum PatchRejectedStatus {
    Rejected,
}

#[derive(Serialize, ToSchema)]
struct PatchRejectedResponse {
    patch_id: String,
    reason: String,
    status: PatchRejectedStatus,
}

/// The durable lifecycle values returned by the Patch status poll. The runtime
/// retains the database string verbatim; this type closes the documented set.
#[allow(dead_code)]
#[derive(ToSchema)]
enum PatchLifecycleStatus {
    Validating,
    Building,
    Submitted,
    Applied,
    Rejected,
    BuildFailed,
}

#[derive(Serialize, ToSchema)]
struct PatchStatusResponse {
    // Nullable lifecycle details are always present on the wire. Marking them
    // required keeps the schema from incorrectly permitting omitted keys.
    #[schema(required = true)]
    applied_version: Option<i64>,
    #[schema(required = true)]
    outcome: Option<String>,
    patch_id: Uuid,
    #[schema(required = true)]
    reason: Option<String>,
    #[schema(value_type = PatchLifecycleStatus)]
    status: String,
    updated_at: String,
    workflow_instance_id: Uuid,
}

/// `POST /api/workflows/instances/{id}/patch` — submit an externally-authored
/// Patch at one running instance (operator story 4). Forwards the raw document
/// on the command bus; the conductor owns the parser and the durable lifecycle
/// row. The reply only **acknowledges ingress** (202 with a `patch_id` to poll,
/// or 409 when another Patch for the instance is still unsettled) — the apply
/// happens asynchronously and is read off the row, never held across build
/// wall-clock.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/instances/{id}/patch", responses((status = 202, body = PatchAcceptedResponse), (status = 400), (status = 408), (status = 409, body = PatchRejectedResponse), (status = 500)))]
async fn patch_instance_handler(
    State(state): State<Arc<AppState>>,
    Path(instance_id): Path<String>,
    body: Option<Json<PatchBody>>,
) -> Response {
    let Some(Json(req)) = body else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing body: expected `{ nickel_source }`"})),
        )
            .into_response();
    };
    if req.nickel_source.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "`nickel_source` must not be empty"})),
        )
            .into_response();
    }

    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Patch(api::PatchRequest {
            workflow_instance_id: instance_id,
            nickel_source: req.nickel_source,
        })),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.patch).await {
        Ok(resp) => render_patch(resp),
        Err(e) => bus_error_response(e),
    }
}

/// `GET /api/patches/{patch_id}` — poll a submitted Patch's asynchronous
/// outcome off its durable lifecycle row. The POST ack is synchronous
/// (`patch_id`); the apply is not — the submitter reads terminal state here
/// (`Applied` carries the `applied_version` the Patch produced; `Rejected` /
/// `BuildFailed` carry the `outcome` detail). 404 for an unknown `patch_id`.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/patches/{patch_id}", responses((status = 200, body = PatchStatusResponse), (status = 400), (status = 404), (status = 500)))]
async fn get_patch_status_handler(
    State(state): State<Arc<AppState>>,
    Path(patch_id): Path<String>,
) -> Response {
    let Ok(id) = Uuid::parse_str(&patch_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid patch_id"})),
        )
            .into_response();
    };
    match super::archive_queries::get_patch_status(&state.pg_pool, id).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(PatchStatusResponse {
                applied_version: row.applied_version,
                outcome: row.outcome,
                patch_id: row.patch_id,
                reason: row.reason,
                status: row.status,
                updated_at: row.updated_at.to_rfc3339(),
                workflow_instance_id: row.workflow_instance_id,
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no patch with that id"})),
        )
            .into_response(),
        Err(e) => {
            eprintln!("get_patch_status_handler: read failed for {id}: {e}");
            public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("patch status read failed: {e}"),
            )
        }
    }
}

/// `GET /api/patches/{patch_id}/source` — the Patch's retained authored source,
/// exactly as submitted (Nickel for an external patch, the JSON document for a
/// self-patch). The code tab fetches this and renders it beside the workflow's
/// Nickel and the server's lowered effect; `applied_version` joins the source
/// to that effect by patch/version. 404 for an unknown `patch_id`.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/patches/{patch_id}/source", responses((status = 200, body = super::openapi::PatchSourceDoc), (status = 400), (status = 404), (status = 500)))]
async fn get_patch_source_handler(
    State(state): State<Arc<AppState>>,
    Path(patch_id): Path<String>,
) -> Response {
    let Ok(id) = Uuid::parse_str(&patch_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid patch_id"})),
        )
            .into_response();
    };
    match super::archive_queries::get_patch_source(&state.pg_pool, id).await {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "patch_id": row.patch_id,
                "workflow_instance_id": row.workflow_instance_id,
                "source": row.source,
                "source_format": row.source_format,
                "applied_version": row.applied_version,
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no patch with that id"})),
        )
            .into_response(),
        Err(e) => {
            eprintln!("get_patch_source_handler: read failed for {id}: {e}");
            public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("patch source read failed: {e}"),
            )
        }
    }
}

/// Replay request body. `{ resume_from?, name?, inputs?, idempotency_key? }`.
///
/// No-smuggle by construction: this shape has NO field able to carry a
/// `ReplaySeed`. The replay's carried state is minted conductor-side from the
/// archive, never from these bytes — a client cannot forge coordination state
/// into a run. `inputs` is the shadow lever — capture-name → fresh value — and
/// is NOT seeded state: it names a declared trigger capture of the pinned
/// version and re-supplies its value (validated conductor-side against the
/// version's declared-capture schema; undeclared or task-produced keys reject).
/// `#[serde(deny_unknown_fields)]` is deliberately NOT used (extra fields are
/// ignored, matching the trigger body); the guarantee is the structural absence
/// of a seed field, asserted by the tripwire test.
#[derive(Deserialize, ToSchema)]
#[schema(as = ReplayRequest)]
struct ReplayBody {
    #[serde(default)]
    resume_from: Vec<String>,
    #[serde(default)]
    name: Option<String>,
    /// Shadow lever: capture-name → fresh JSON value. Shadows a declared trigger
    /// capture of the pinned version only.
    #[serde(default)]
    inputs: HashMap<String, serde_json::Value>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

/// `POST /api/workflows/instances/{id}/replay` — replay a terminal source run
/// from its archive. The conductor mints the seed conductor-side, materialises
/// a born-Stalled instance under the deterministic id, re-hydrates its ctx
/// scope, and releases the Stall. The reply carries the `replay_instance_id`
/// the operator opens the replay's instance-detail page by.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/instances/{id}/replay", responses((status = 200, body = super::openapi::ReplayResultDoc), (status = 202), (status = 400), (status = 404), (status = 408), (status = 409), (status = 500)))]
async fn replay_instance_handler(
    State(state): State<Arc<AppState>>,
    Path(instance_id): Path<String>,
    body: Option<Json<ReplayBody>>,
) -> Response {
    // An absent body is a bare replay (resume from every failed HyperNode, no
    // name, no shadow inputs, no idempotency key) — the one-click Resume shape.
    let req = body.map(|Json(b)| b).unwrap_or(ReplayBody {
        resume_from: Vec::new(),
        name: None,
        inputs: HashMap::new(),
        idempotency_key: None,
    });

    if let Some(name) = req.name.as_deref() {
        if !run_name_within_cap(name) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("`name` exceeds {RUN_NAME_MAX_CHARS} characters")
                })),
            )
                .into_response();
        }
    }

    // The inputs shadow rides the command bus as a `map<string, string>` whose
    // values are JSON-encoded — the same wire convention as Wakeup captures, so
    // a value's structure round-trips intact through the bus.
    let inputs = req
        .inputs
        .into_iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect();
    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Replay(api::ReplayRequest {
            source_instance_id: instance_id,
            resume_from: req.resume_from,
            name: req.name,
            idempotency_key: req.idempotency_key,
            inputs,
        })),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.replay).await {
        Ok(resp) => render_replay(resp),
        Err(e) => bus_error_response(e),
    }
}

/// Handler for `GET /api/workflows/instances/{id}/replays` — the reverse link
/// from a terminal source run to the replays spawned from it. Served from the
/// `workflow_replays` pipeline row **indexed by `source_instance_id`** — never
/// a unbounded live-state scan of all instances (the priced-out `get_all_instances` path).
/// Newest first. An unknown source id is an empty list, not a 404: a run with
/// no replays is a valid, common state.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}/replays", responses((status = 200, body = Vec<super::dto::ReplayRowResponse>), (status = 400), (status = 500)))]
async fn list_instance_replays_handler(
    Path(instance_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let id = match Uuid::parse_str(&instance_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workflow instance id"})),
            )
                .into_response();
        }
    };
    match super::archive_queries::list_replays_for_source(&state.pg_pool, id).await {
        Ok(rows) => {
            let replays: Vec<super::dto::ReplayRowResponse> = rows
                .into_iter()
                .map(|r| super::dto::ReplayRowResponse {
                    replay_instance_id: r.replay_instance_id.to_string(),
                    source_instance_id: r.source_instance_id.to_string(),
                    status: r.status,
                    name: r.name,
                    resume_from: r
                        .resume_from
                        .iter()
                        .map(|node_id| super::dto::IdentityRefResponse {
                            id: node_id.to_string(),
                            code: tickr_proto::identity_code(node_id),
                        })
                        .collect(),
                    shadowed_keys: r.shadowed_keys,
                    created_at: r.created_at.to_rfc3339(),
                })
                .collect();
            (StatusCode::OK, Json(replays)).into_response()
        }
        Err(e) => {
            eprintln!("list_instance_replays_handler: replay list read failed for {id}: {e}");
            public_http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("replay list read: {e}"),
            )
        }
    }
}

/// Render a replay `ApiCommandResponse` into an HTTP body. Accepted → 202 with
/// the `replay_instance_id` (and any `doomed` HyperNodes to warn about);
/// Deduplicated → 200 with the existing id; VersionUnresolvable → 404; a
/// conductor error rides its own status.
fn render_replay(resp: api::ApiCommandResponse) -> Response {
    use api::api_command_response::Payload;
    use api::replay_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Replay(rp)) => match rp.outcome {
            Some(Outcome::Accepted(a)) => (
                status,
                Json(serde_json::json!({
                    "replay_instance_id": a.replay_instance_id,
                    "status": "accepted",
                    "doomed": a.doomed,
                })),
            )
                .into_response(),
            Some(Outcome::Deduplicated(d)) => (
                status,
                Json(serde_json::json!({
                    "replay_instance_id": d.replay_instance_id,
                    "status": "deduplicated",
                })),
            )
                .into_response(),
            Some(Outcome::VersionUnresolvable(v)) => (
                status,
                Json(serde_json::json!({
                    "replay_instance_id": v.replay_instance_id,
                    "status": "version_unresolvable",
                    "error": "source run's archived blob is absent — nothing to replay",
                })),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({"error": public_error_message(status, ep.message)})),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Render a patch `ApiCommandResponse` into an HTTP body. Accepted → 202 with
/// the `patch_id` to poll; Rejected (one-at-a-time) → 409 with its reason and
/// still-open `patch_id`; a conductor error rides its own status.
fn render_patch(resp: api::ApiCommandResponse) -> Response {
    use api::api_command_response::Payload;
    use api::patch_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Patch(pp)) => match pp.outcome {
            Some(Outcome::Accepted(a)) => (
                status,
                Json(PatchAcceptedResponse {
                    patch_id: a.patch_id,
                    status: PatchAcceptedStatus::Accepted,
                }),
            )
                .into_response(),
            Some(Outcome::Rejected(r)) => (
                status,
                Json(PatchRejectedResponse {
                    patch_id: r.patch_id,
                    reason: r.reason,
                    status: PatchRejectedStatus::Rejected,
                }),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({"error": public_error_message(status, ep.message)})),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Render a wakeup `ApiCommandResponse` into today's HTTP body shape.
fn render_wakeup(resp: api::ApiCommandResponse) -> Response {
    use api::api_command_response::Payload;
    use api::wakeup_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Wakeup(wp)) => match wp.outcome {
            Some(Outcome::Fresh(f)) => (
                status,
                Json(WakeupResponse {
                    signal_id: Uuid::parse_str(&f.signal_id).unwrap_or_default(),
                    matched_workflows: Some(f.matched_workflows),
                    gates_matched: Some(f.gates_matched),
                    deduplicated: None,
                }),
            )
                .into_response(),
            Some(Outcome::Deduplicated(d)) => (
                status,
                Json(WakeupResponse {
                    signal_id: Uuid::parse_str(&d.original_signal_id).unwrap_or_default(),
                    matched_workflows: None,
                    gates_matched: None,
                    deduplicated: Some(true),
                }),
            )
                .into_response(),
            Some(Outcome::Conflict(c)) => (
                status,
                Json(serde_json::json!({
                    "error": "idempotency key reused with a different payload",
                    "original_signal_id": c.original_signal_id,
                    "original_input_hash": c.original_input_hash,
                    "your_input_hash": c.your_input_hash,
                })),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({"error": public_error_message(status, ep.message)})),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Cancel target as it arrives in the canonical request body. Same serde shape
/// the conductor's HTTP route accepts; translated into the proto `CancelTarget`
/// before forwarding.
#[derive(Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CancelTargetBody {
    Instance {
        workflow_instance_id: Uuid,
        #[serde(default)]
        node_id: Option<Uuid>,
    },
    ByTag {
        filter: std::collections::HashMap<String, String>,
    },
}

/// Body of the canonical `POST /api/signals/cancel`.
#[derive(Deserialize, Default, ToSchema)]
struct GenericCancelBody {
    #[serde(default)]
    target: Option<CancelTargetBody>,
    #[serde(default)]
    note: Option<String>,
}

/// Body of the two path-encoded sugar routes — the target comes from the URL,
/// so only `note` is read from the body.
#[derive(Deserialize, Default, ToSchema)]
struct PathCancelBody {
    #[serde(default)]
    note: Option<String>,
}

/// Discriminated cancel response. Byte-identical to the conductor's
/// `CancelResponse`; the `skip_serializing_if` set is what differentiates the
/// Instance / ByTag / Deduplicated bodies.
#[derive(Serialize, ToSchema)]
struct CancelResponse {
    signal_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    applied: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instances_matched: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deduplicated: Option<bool>,
}

/// Translate a parsed body target into the proto `CancelTarget`.
fn to_proto_target(target: CancelTargetBody) -> api::CancelTarget {
    let inner = match target {
        CancelTargetBody::Instance {
            workflow_instance_id,
            node_id,
        } => api::cancel_target::Target::Instance(api::cancel_target::Instance {
            workflow_instance_id: workflow_instance_id.to_string(),
            node_id: node_id.map(|n| n.to_string()),
        }),
        CancelTargetBody::ByTag { filter } => {
            api::cancel_target::Target::ByTag(api::cancel_target::ByTag { filter })
        }
    };
    api::CancelTarget {
        target: Some(inner),
    }
}

/// Shared command-bus call for all three cancel routes.
async fn run_cancel(
    state: &AppState,
    target: api::CancelTarget,
    note: Option<String>,
    idempotency_key: Option<String>,
) -> Response {
    let request = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Cancel(api::CancelRequest {
            target: Some(target),
            note,
            idempotency_key,
        })),
    };
    match send_command(state.nats.as_ref(), request, state.deadlines.cancel).await {
        Ok(resp) => render_cancel(resp),
        Err(e) => bus_error_response(e),
    }
}

fn idempotency_key(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("Idempotency-Key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

/// `POST /api/signals/cancel` — canonical cancel. Missing `target` is a 400,
/// matching today's handler, before any bus round-trip.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/signals/cancel", responses((status = 200, body = CancelResponse), (status = 202), (status = 400), (status = 408), (status = 409), (status = 500)))]
async fn cancel_signal_handler(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<GenericCancelBody>>,
) -> Response {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let Some(target) = req.target else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing `target`"})),
        )
            .into_response();
    };
    run_cancel(
        &state,
        to_proto_target(target),
        req.note,
        idempotency_key(&headers),
    )
    .await
}

/// `POST /api/workflows/instances/{id}/cancel` (sugar) — the URL supplies an
/// Instance target with no node narrowing.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/instances/{id}/cancel", responses((status = 200, body = CancelResponse), (status = 202), (status = 400), (status = 408), (status = 409), (status = 500)))]
async fn cancel_workflow_instance_handler(
    Path(workflow_instance_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<PathCancelBody>>,
) -> Response {
    if Uuid::parse_str(&workflow_instance_id).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid workflow_instance_id"})),
        )
            .into_response();
    }
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let target = api::CancelTarget {
        target: Some(api::cancel_target::Target::Instance(
            api::cancel_target::Instance {
                workflow_instance_id,
                node_id: None,
            },
        )),
    };
    run_cancel(&state, target, req.note, idempotency_key(&headers)).await
}

/// `POST /api/workflows/instances/{id}/tasks/{task_id}/cancel` (sugar) — the
/// URL narrows the Instance target to one node.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", post, path = "/api/workflows/instances/{id}/tasks/{task_id}/cancel", responses((status = 200, body = CancelResponse), (status = 202), (status = 400), (status = 408), (status = 409), (status = 500)))]
async fn cancel_task_handler(
    Path((workflow_instance_id, task_id)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<PathCancelBody>>,
) -> Response {
    if Uuid::parse_str(&workflow_instance_id).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid workflow_instance_id"})),
        )
            .into_response();
    }
    if Uuid::parse_str(&task_id).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid task_id"})),
        )
            .into_response();
    }
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let target = api::CancelTarget {
        target: Some(api::cancel_target::Target::Instance(
            api::cancel_target::Instance {
                workflow_instance_id,
                node_id: Some(task_id),
            },
        )),
    };
    run_cancel(&state, target, req.note, idempotency_key(&headers)).await
}

/// Render a cancel `ApiCommandResponse` into today's HTTP body shape.
fn render_cancel(resp: api::ApiCommandResponse) -> Response {
    use api::api_command_response::Payload;
    use api::cancel_payload::Outcome;

    let status =
        StatusCode::from_u16(resp.status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match resp.payload {
        Some(Payload::Cancel(cp)) => match cp.outcome {
            Some(Outcome::Instance(i)) => (
                status,
                Json(CancelResponse {
                    signal_id: Uuid::parse_str(&i.signal_id).unwrap_or_default(),
                    applied: Some(true),
                    instances_matched: None,
                    deduplicated: None,
                }),
            )
                .into_response(),
            Some(Outcome::ByTag(b)) => (
                status,
                Json(CancelResponse {
                    signal_id: Uuid::parse_str(&b.signal_id).unwrap_or_default(),
                    applied: None,
                    instances_matched: Some(b.instances_matched),
                    deduplicated: None,
                }),
            )
                .into_response(),
            Some(Outcome::Deduplicated(d)) => (
                status,
                Json(CancelResponse {
                    signal_id: Uuid::parse_str(&d.original_signal_id).unwrap_or_default(),
                    applied: None,
                    instances_matched: None,
                    deduplicated: Some(true),
                }),
            )
                .into_response(),
            Some(Outcome::Conflict(c)) => (
                status,
                Json(serde_json::json!({
                    "error": "idempotency key reused with a different payload",
                    "original_signal_id": c.original_signal_id,
                    "original_input_hash": c.original_input_hash,
                    "your_input_hash": c.your_input_hash,
                })),
            )
                .into_response(),
            None => bus_error_response(BusError::Malformed),
        },
        Some(Payload::Error(ep)) => (
            status,
            Json(serde_json::json!({"error": public_error_message(status, ep.message)})),
        )
            .into_response(),
        _ => bus_error_response(BusError::Malformed),
    }
}

/// Query parameters for the Event log poll: optional `after` (a `seq`
/// cursor). Absent on first page load.
#[derive(Debug, Deserialize, ToSchema, IntoParams)]
struct EventsQuery {
    after: Option<i64>,
}

/// Cap on rows per Event log response — matches the page's buffer cap, so a
/// first load fills the buffer exactly and a poll can never overflow it.
const EVENTS_BATCH_LIMIT: i64 = 200;

/// Handler for `GET /api/events?after=<seq>`. Serves the tenant events
/// projection newest-first by `seq`: first load (no `after`) returns the
/// latest 200; subsequent polls pass the highest `seq` seen and receive only
/// strictly newer rows — no duplicates, no flicker. The cursor is `seq`
/// (arrival order), not `ts`: a late-arriving event from a slow control-plane source
/// would be skipped forever by a time cursor, and event UUIDs don't order.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/events", params(EventsQuery), responses((status = 200, body = Vec<super::dto::EventResponse>), (status = 500)))]
async fn list_events_handler(
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match super::archive_queries::list_events(&state.pg_pool, query.after, EVENTS_BATCH_LIMIT).await
    {
        Ok(rows) => (StatusCode::OK, Json(project_event_rows(rows))).into_response(),
        Err(e) => {
            eprintln!("list_events_handler: projection read failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "events projection unavailable"})),
            )
                .into_response()
        }
    }
}

/// Project archive `EventRow`s into the wire shape. Shared by the unscoped
/// Event log and the per-instance event reads so every events surface returns
/// the identical row shape.
fn project_event_rows(
    rows: Vec<super::archive_queries::EventRow>,
) -> Vec<super::dto::EventResponse> {
    rows.into_iter()
        .map(|r| super::dto::EventResponse {
            seq: r.seq,
            id: r.id.to_string(),
            ts: r.ts.to_rfc3339(),
            event_type: r.event_type,
            payload: r.payload,
        })
        .collect()
}

/// Handler for `GET /api/workflows/instances/{id}/events?after=<seq>`. The
/// per-instance Events section's read: the same tenant events projection
/// `/api/events` serves, scoped to this workflow instance, with the same
/// `seq` cursor (arrival order — `ts` is unsafe, a late event from a slow
/// node would be skipped forever by a time cursor). A malformed id is a 400.
///
/// Rollup caveat (carried, not silently shipped): events that name only a
/// task-instance id before delivery (`TaskInstanceCreated` / `TaskQueued`)
/// are absent here; they are served on the task-instance endpoint and every
/// task is listed on the Tasks tab.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}/events", params(EventsQuery), responses((status = 200, body = Vec<super::dto::EventResponse>), (status = 400), (status = 500)))]
async fn list_workflow_instance_events_handler(
    Path(instance_id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let id = match Uuid::parse_str(&instance_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workflow instance id"})),
            )
                .into_response();
        }
    };
    match super::archive_queries::list_workflow_instance_events(
        &state.pg_pool,
        id,
        query.after,
        EVENTS_BATCH_LIMIT,
    )
    .await
    {
        Ok(rows) => (StatusCode::OK, Json(project_event_rows(rows))).into_response(),
        Err(e) => {
            eprintln!("list_workflow_instance_events_handler: projection read failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "events projection unavailable"})),
            )
                .into_response()
        }
    }
}

/// Handler for `GET /api/workflows/instances/{id}/tasks/{task_id}/events`,
/// the task-instance Events section's read — nested under the instance like
/// the task-logs route. Filters on the task-instance id (`task_id`); the
/// parent instance id is contextual to the nesting and not a predicate, the
/// same single-filter model as the events projection itself.
#[utoipa::path(summary = "API operation", description = "Public HTTP operation.", get, path = "/api/workflows/instances/{id}/tasks/{task_id}/events", params(EventsQuery), responses((status = 200, body = Vec<super::dto::EventResponse>), (status = 400), (status = 500)))]
async fn list_task_instance_events_handler(
    Path((_instance_id, task_id)): Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let ti = match Uuid::parse_str(&task_id) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid task instance id"})),
            )
                .into_response();
        }
    };
    match super::archive_queries::list_task_instance_events(
        &state.pg_pool,
        ti,
        query.after,
        EVENTS_BATCH_LIMIT,
    )
    .await
    {
        Ok(rows) => (StatusCode::OK, Json(project_event_rows(rows))).into_response(),
        Err(e) => {
            eprintln!("list_task_instance_events_handler: projection read failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "events projection unavailable"})),
            )
                .into_response()
        }
    }
}

/// Stateless top-level routes (hello + health). These need no `AppState`, so
/// tests can exercise them without standing up Postgres or NATS.
pub fn meta_router() -> Router {
    Router::new()
        .route("/", get(hello_handler))
        .route("/health", get(health_handler))
}

/// Build the documented router before application state is supplied. Each
/// `routes!` entry contributes both the Axum route and its OpenAPI operation,
/// making this the single topology source for runtime and generated artifacts.
fn documented_router() -> OpenApiRouter<Arc<AppState>> {
    OpenApiRouter::new()
        .routes(routes!(hello_handler))
        .routes(routes!(health_handler))
        .routes(routes!(api_health_handler))
        .routes(routes!(list_workflows_handler))
        .routes(routes!(register_handler))
        .routes(routes!(get_workflow_detail_handler))
        .routes(routes!(trigger_handler))
        .routes(routes!(patch_instance_handler))
        .routes(routes!(replay_instance_handler))
        .routes(routes!(list_instance_replays_handler))
        .routes(routes!(get_patch_status_handler))
        .routes(routes!(get_patch_source_handler))
        .routes(routes!(cancel_signal_handler))
        .routes(routes!(cancel_workflow_instance_handler))
        .routes(routes!(cancel_task_handler))
        .routes(routes!(wakeup_handler))
        .routes(routes!(get_signal_status_handler))
        .routes(routes!(list_workflow_instances_handler))
        .routes(routes!(workflow_calendar_handler))
        .routes(routes!(get_workflow_instance_handler))
        .routes(routes!(list_task_instances_handler))
        .routes(routes!(get_instance_context_handler))
        .routes(routes!(list_workflow_instance_events_handler))
        .routes(routes!(list_task_instance_events_handler))
        .routes(routes!(tenant_handler))
        .routes(routes!(dashboard_clock_handler))
        .routes(routes!(dashboard_upcoming_handler))
        .routes(routes!(list_events_handler))
        .routes(routes!(get_task_logs_handler))
}

/// Generate the exact OpenAPI document paired with the runtime router without
/// constructing Postgres, NATS, or other live dependencies.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    let (_, openapi) = documented_router().split_for_parts();
    customize_openapi(openapi)
}

fn customize_openapi(mut openapi: utoipa::openapi::OpenApi) -> utoipa::openapi::OpenApi {
    openapi.info.title = "Tickr API".to_string();
    openapi.info.version = "0.2.0".to_string();
    openapi.info.description =
        Some("HTTP contract generated from the API component's registered routes.".to_string());
    openapi.info.contact = None;
    openapi.info.license = None;
    openapi
}

fn openapi_value(openapi: utoipa::openapi::OpenApi) -> serde_json::Value {
    // Rust doc comments remain useful beside handlers and DTOs, but many name
    // implementation details that are not part of the public contract. Keep
    // generated descriptions deliberately neutral at every level.
    let mut value = serde_json::to_value(openapi).expect("OpenAPI serializes");
    neutralize_descriptions(&mut value);
    value
}

fn neutralize_descriptions(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(description) = map.get_mut("description") {
                *description = serde_json::Value::String("Public API contract.".to_string());
            }
            for child in map.values_mut() {
                neutralize_descriptions(child);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                neutralize_descriptions(child);
            }
        }
        _ => {}
    }
}

/// Serialize the generated document deterministically for the committed
/// Console input. Exactly one trailing newline is retained.
pub fn openapi_yaml() -> Result<String> {
    let mut yaml = serde_yaml::to_string(&openapi_value(openapi_document()))?;
    while yaml.ends_with("\n\n") {
        yaml.pop();
    }
    if !yaml.ends_with('\n') {
        yaml.push('\n');
    }
    Ok(yaml)
}

/// Construct an `AppState` from its component parts. Public so tests can build
/// the router against a curated state.
pub fn build_app_state(
    nats: Arc<Client>,
    pg_pool: Arc<PgPool>,
    coordinator: Arc<super::coordinator_client::CoordinatorClient>,
    logs: Arc<super::logs_resolver::LogsResolver>,
) -> Arc<AppState> {
    Arc::new(AppState {
        nats,
        pg_pool,
        coordinator,
        logs,
        deadlines: CommandDeadlines::default(),
    })
}

/// Build the API's HTTP router around a fully-constructed `AppState`. Factored
/// out of `start_http_server` so integration tests can drive the routes against
/// an in-process `axum::serve` on an ephemeral port.
pub fn build_router(state: Arc<AppState>) -> Router {
    let (router, openapi) = documented_router().with_state(state).split_for_parts();
    let runtime_document = Arc::new(openapi_value(customize_openapi(openapi)));
    router.route(
        "/api-docs/openapi.json",
        get(move || {
            let document = runtime_document.clone();
            async move { Json((*document).clone()) }
        }),
    )
}

/// Start the API HTTP server with the configured routes.
#[cfg(not(madsim))]
pub async fn start_http_server(
    shutdown_rx: watch::Receiver<bool>,
    nats: Client,
    pg_pool: Arc<PgPool>,
) -> Result<()> {
    // Live-state subquery client. Constructed once at startup and shared across
    // handlers via the `AppState` so the connection pool and the 1.5s timeout
    // policy are uniform.
    let coordinator = Arc::new(super::coordinator_client::CoordinatorClient::new(
        tickr_proto::config::coordinator_http_url(),
    ));

    let storage = crate::config::LogStorageConfig::from_env()?;
    let minio = storage.operator()?;

    let logs = Arc::new(super::logs_resolver::LogsResolver::new(minio, nats.clone()));

    let state = build_app_state(Arc::new(nats), pg_pool, coordinator, logs);
    let app = build_router(state);

    let addr = crate::config::api_bind_addr()?;
    println!("Starting tickr API HTTP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    let mut shutdown_rx_clone = shutdown_rx.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_rx_clone.changed().await.ok();
            println!("API HTTP server received shutdown signal");
        })
        .await?;

    println!("API HTTP server stopped gracefully");
    Ok(())
}

/// The simulator compiles the API contract but does not provide Axum's TCP
/// listener integration, so the HTTP process cannot run in that harness.
#[cfg(madsim)]
pub async fn start_http_server(
    _shutdown_rx: watch::Receiver<bool>,
    _nats: Client,
    _pg_pool: Arc<PgPool>,
) -> Result<()> {
    anyhow::bail!("the API HTTP server is unavailable under madsim")
}

#[cfg(test)]
mod tests {
    use super::{
        logs_error_response, order_upcoming, public_http_error, run_name_within_cap,
        PatchAcceptedResponse, PatchAcceptedStatus, PatchRejectedResponse, PatchRejectedStatus,
        PatchStatusResponse, RegisterQueuedResponse, RegisterQueuedStatus, RegisterSettledResponse,
        RegisterSettledStatus, ReplayBody, TenantInfoResponse, TriggerBody, RUN_NAME_MAX_CHARS,
    };
    use crate::http::{dto::UpcomingInstanceResponse, logs_resolver::LogsError};
    use axum::{body::to_bytes, http::StatusCode};
    use uuid::Uuid;

    #[tokio::test]
    async fn public_http_errors_redact_5xx_and_preserve_4xx_detail() {
        let secret = "postgres://user:secret@host/db";
        let server = public_http_error(StatusCode::INTERNAL_SERVER_ERROR, secret);
        let server_body = to_bytes(server.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&server_body).unwrap()["error"],
            "internal server error"
        );
        assert!(!String::from_utf8_lossy(&server_body).contains(secret));

        let client = public_http_error(StatusCode::BAD_REQUEST, secret);
        let client_body = to_bytes(client.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&client_body).unwrap()["error"],
            secret
        );
    }

    /// Wire-contract guard: `TenantInfoResponse` must serialize to exactly the
    /// snake-case shape the openapi `TenantInfo` schema (and the UI type) expect
    /// — `slug`, `id`, `workflow_count`. If a field is renamed or re-cased here
    /// without updating `console/openapi.yaml`, the Console silently reads `undefined`;
    /// this pins the three keys so that drift fails a test instead.
    #[test]
    fn tenant_info_response_serializes_snake_case() {
        let json = serde_json::to_value(TenantInfoResponse {
            slug: "default".to_string(),
            id: "4f1c0000-0000-5000-8000-000000000000".to_string(),
            workflow_count: 12,
        })
        .expect("serialize");
        assert_eq!(json["slug"], "default");
        assert_eq!(json["id"], "4f1c0000-0000-5000-8000-000000000000");
        assert_eq!(json["workflow_count"], 12);
        // No camelCase leak.
        assert!(json.get("workflowCount").is_none());
    }

    #[test]
    fn register_outcome_dtos_preserve_exact_json_bytes() {
        let no_op = RegisterSettledResponse {
            message: "unchanged".to_string(),
            status: RegisterSettledStatus::NoOp,
            workflow_id: "wf-1".to_string(),
            workflow_version: 7,
        };
        assert_eq!(
            serde_json::to_string(&no_op).expect("serialize NoOp"),
            r#"{"message":"unchanged","status":"NoOp","workflow_id":"wf-1","workflow_version":7}"#
        );

        let refreshed = RegisterSettledResponse {
            message: "refreshed".to_string(),
            status: RegisterSettledStatus::Refreshed,
            workflow_id: "wf-1".to_string(),
            workflow_version: 7,
        };
        assert_eq!(
            serde_json::to_string(&refreshed).expect("serialize Refreshed"),
            r#"{"message":"refreshed","status":"Refreshed","workflow_id":"wf-1","workflow_version":7}"#
        );

        let inserted = RegisterQueuedResponse {
            message: "inserted".to_string(),
            status: RegisterQueuedStatus::Building,
            task_count: 3,
            workflow_id: "wf-2".to_string(),
            workflow_version: 8,
        };
        assert_eq!(
            serde_json::to_string(&inserted).expect("serialize Inserted"),
            r#"{"message":"inserted","status":"Building","task_count":3,"workflow_id":"wf-2","workflow_version":8}"#
        );

        let requeued = RegisterQueuedResponse {
            message: "requeued".to_string(),
            status: RegisterQueuedStatus::BuildRequeued,
            task_count: 2,
            workflow_id: "wf-2".to_string(),
            workflow_version: 8,
        };
        assert_eq!(
            serde_json::to_string(&requeued).expect("serialize BuildRequeued"),
            r#"{"message":"requeued","status":"BuildRequeued","task_count":2,"workflow_id":"wf-2","workflow_version":8}"#
        );
    }

    #[test]
    fn patch_outcome_and_status_dtos_preserve_exact_json_bytes() {
        let accepted = PatchAcceptedResponse {
            patch_id: "patch-1".to_string(),
            status: PatchAcceptedStatus::Accepted,
        };
        assert_eq!(
            serde_json::to_string(&accepted).expect("serialize accepted"),
            r#"{"patch_id":"patch-1","status":"accepted"}"#
        );

        let rejected = PatchRejectedResponse {
            patch_id: "patch-2".to_string(),
            reason: "already unsettled".to_string(),
            status: PatchRejectedStatus::Rejected,
        };
        assert_eq!(
            serde_json::to_string(&rejected).expect("serialize rejected"),
            r#"{"patch_id":"patch-2","reason":"already unsettled","status":"rejected"}"#
        );

        let status = PatchStatusResponse {
            applied_version: None,
            outcome: Some("awaiting build".to_string()),
            patch_id: uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111")
                .expect("patch uuid"),
            reason: None,
            status: "Building".to_string(),
            updated_at: "2026-01-02T03:04:05+00:00".to_string(),
            workflow_instance_id: uuid::Uuid::parse_str("22222222-2222-4222-8222-222222222222")
                .expect("instance uuid"),
        };
        assert_eq!(
            serde_json::to_string(&status).expect("serialize patch status"),
            r#"{"applied_version":null,"outcome":"awaiting build","patch_id":"11111111-1111-4111-8111-111111111111","reason":null,"status":"Building","updated_at":"2026-01-02T03:04:05+00:00","workflow_instance_id":"22222222-2222-4222-8222-222222222222"}"#
        );
    }

    /// No-smuggle tripwire (permanent), the replay-ingress sibling of the
    /// trigger-body tripwire below. The replay body is `{ resume_from?, name?,
    /// idempotency_key? }` — it has NO field able to carry a `ReplaySeed`, so a
    /// client-supplied `replay` / `seed` field is silently dropped and cannot
    /// smuggle forged coordination state into the run. If a future change adds a
    /// pass-through seed field to `ReplayBody`, this test must be revisited —
    /// that is the tripwire.
    #[test]
    fn http_replay_body_ignores_client_supplied_replay_seed() {
        let forged = serde_json::json!({
            "resume_from": [uuid::Uuid::new_v4().to_string()],
            "name": "recover",
            "idempotency_key": "k-1",
            // A malicious client tries to inject seeded state.
            "replay": {
                "replay_instance_id": uuid::Uuid::new_v4().to_string(),
                "pre_grounded": [uuid::Uuid::new_v4().to_string()],
            },
            "seed": { "pre_grounded": [uuid::Uuid::new_v4().to_string()] },
            "pre_grounded": [uuid::Uuid::new_v4().to_string()],
            "seeded_graph": { "nodes": {} },
        });
        let body: ReplayBody =
            serde_json::from_value(forged).expect("unknown fields ignored, not fatal");
        // Only the declared fields survive; ReplayBody structurally has no
        // seed/replay/pre_grounded field, so the forged bytes went nowhere.
        assert_eq!(body.name.as_deref(), Some("recover"));
        assert_eq!(body.idempotency_key.as_deref(), Some("k-1"));
        assert_eq!(body.resume_from.len(), 1);
    }

    /// No-smuggle tripwire (permanent). A replay's carried state is minted
    /// conductor-side from the archive, never accepted from a client. The HTTP
    /// trigger body hand-extracts only its declared fields, so a client-supplied
    /// `replay` / `seed` field is silently dropped — a caller cannot smuggle a
    /// `ReplaySeed` into a run through the trigger endpoint. If a future change
    /// adds a pass-through seed field to `TriggerBody`, this test must be
    /// revisited — that is the tripwire.
    #[test]
    fn http_trigger_body_ignores_client_supplied_replay_seed() {
        let forged = serde_json::json!({
            "inputs": { "x": 1 },
            "name": "recover",
            // A malicious client tries to inject replay state.
            "replay": {
                "replay_instance_id": uuid::Uuid::new_v4().to_string(),
                "pre_grounded": [uuid::Uuid::new_v4().to_string()],
            },
            "seed": { "pre_grounded": [uuid::Uuid::new_v4().to_string()] },
            "source_instance_id": uuid::Uuid::new_v4().to_string(),
            "resume_from": [uuid::Uuid::new_v4().to_string()],
        });
        let body: TriggerBody =
            serde_json::from_value(forged).expect("unknown fields ignored, not fatal");
        // Only the declared fields survive; TriggerBody structurally has no
        // seed/replay field, so the forged bytes went nowhere.
        assert_eq!(body.name.as_deref(), Some("recover"));
        assert!(body.inputs.is_some());
        assert!(body.scheduled_at.is_none());
    }

    #[test]
    fn run_name_cap_is_measured_in_chars_at_the_boundary() {
        // At the cap and below pass; one char over is rejected (the trigger
        // endpoint turns a false here into a 400 — no silent truncation).
        assert!(run_name_within_cap(""));
        assert!(run_name_within_cap(&"a".repeat(RUN_NAME_MAX_CHARS)));
        assert!(!run_name_within_cap(&"a".repeat(RUN_NAME_MAX_CHARS + 1)));
        // Multibyte chars count once each, not by byte length.
        assert!(run_name_within_cap(&"é".repeat(RUN_NAME_MAX_CHARS)));
        assert!(!run_name_within_cap(&"😀".repeat(RUN_NAME_MAX_CHARS + 1)));
    }

    fn row(name: &str, at: Option<&str>) -> UpcomingInstanceResponse {
        UpcomingInstanceResponse {
            workflow_instance_id: format!("inst-{name}"),
            workflow_id: format!("wf-{name}"),
            workflow_name: name.to_string(),
            name: format!("run-{name}"),
            next_run_at: at.map(|s| s.to_string()),
        }
    }

    #[test]
    fn order_upcoming_sorts_ascending_and_trims() {
        let rows = vec![
            row("c", Some("2026-06-05T11:00:00+00:00")),
            row("a", Some("2026-06-05T09:00:00+00:00")),
            row("b", Some("2026-06-05T10:00:00+00:00")),
        ];
        let out = order_upcoming(rows, 2);
        let names: Vec<_> = out.iter().map(|r| r.workflow_name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn logs_server_errors_do_not_disclose_backend_details() {
        let response = logs_error_response(
            LogsError::Minio("postgres://user:secret@host/db logs.private.subject".into()),
            Uuid::nil(),
        );
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(body.as_ref(), br#"{"error":"logs unavailable"}"#);
    }

    #[test]
    fn order_upcoming_sorts_missing_times_last() {
        let rows = vec![
            row("none", None),
            row("later", Some("2026-06-05T10:00:00+00:00")),
            row("soon", Some("2026-06-05T09:00:00+00:00")),
        ];
        let out = order_upcoming(rows, 10);
        let names: Vec<_> = out.iter().map(|r| r.workflow_name.as_str()).collect();
        assert_eq!(names, vec!["soon", "later", "none"]);
    }
}
