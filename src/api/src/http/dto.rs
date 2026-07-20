//! UI-facing response DTOs served by the API component's `/api/*` routes.
//!
//! DTOs are serializable for Console responses and deserializable for live
//! coordinator query results.

use serde::{Deserialize, Serialize};
use tickr_proto::workflow as wf;
use utoipa::ToSchema;

/// JSON rendering of a workflow trigger, derived from the published definition
/// message.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq, Default)]
#[schema(as = Trigger)]
pub enum WorkflowTrigger {
    Cron(String),
    #[default]
    FireNow,
    WaitsOnSignal(WaitsOnSignalResponse),
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct WaitsOnSignalResponse {
    pub signal_name: String,
    pub predicate: Option<String>,
    #[schema(required = false)]
    pub captures: Vec<CaptureDeclarationResponse>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct CaptureDeclarationResponse {
    pub name: String,
    pub from: CaptureSourceResponse,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub enum CaptureSourceResponse {
    Trigger { jsonpath: String },
}

/// Convert the published trigger projection to the response's established JSON
/// shape. Missing or malformed optional proto content returns `None`, allowing
/// the caller to retain the historical FireNow fallback.
pub fn workflow_trigger_from_proto(trigger: Option<&wf::Trigger>) -> Option<WorkflowTrigger> {
    match trigger?.kind.as_ref()? {
        wf::trigger::Kind::Cron(expr) => Some(WorkflowTrigger::Cron(expr.clone())),
        wf::trigger::Kind::FireNow(_) => Some(WorkflowTrigger::FireNow),
        wf::trigger::Kind::WaitsOnSignal(config) => {
            Some(WorkflowTrigger::WaitsOnSignal(WaitsOnSignalResponse {
                signal_name: config.signal_name.clone(),
                predicate: config.predicate.clone(),
                captures: config
                    .captures
                    .iter()
                    .map(capture_from_proto)
                    .collect::<Option<_>>()?,
            }))
        }
    }
}

fn capture_from_proto(capture: &wf::CaptureDeclaration) -> Option<CaptureDeclarationResponse> {
    let source = match capture.from.as_ref()?.source.as_ref()? {
        wf::capture_source::Source::Trigger(trigger) => CaptureSourceResponse::Trigger {
            jsonpath: trigger.jsonpath.clone(),
        },
    };
    Some(CaptureDeclarationResponse {
        name: capture.name.clone(),
        from: source,
    })
}

/// The workflow's build lifecycle, projected for the list view. Sourced from
/// the conductor's `workflows.status` column. `Submitted` (built and dispatched
/// to the executor) folds to `Ready` — a submitted workflow built successfully
/// and is live. Serialises as its variant name (`"Ready"` etc.).
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowBuildStatus {
    Building,
    Ready,
    BuildFailed,
}

impl WorkflowBuildStatus {
    /// Map the raw `workflows.status` value onto the wire enum. `Ready` and
    /// `Submitted` both surface as `Ready`; an unrecognised value defaults to
    /// `Building` (the register-time default — a row mid-flight).
    pub fn from_status(s: &str) -> Self {
        match s {
            "Ready" | "Submitted" => WorkflowBuildStatus::Ready,
            "BuildFailed" => WorkflowBuildStatus::BuildFailed,
            _ => WorkflowBuildStatus::Building,
        }
    }
}

/// Projection of a registered workflow definition for the workflows list view.
/// `trigger` is the workflow's firing projection, serialised in its canonical
/// shape (`{"Cron":"<expr>"}` | `"FireNow"` | `{"WaitsOnSignal":{..}}`);
/// the UI's Schedule cell discriminates on it.
///
/// `version` is the latest *live* version (the one that would run today);
/// `build_status`/`build_version` describe the latest registration's build
/// outcome. The definition's Active/Inactive `WorkflowStatus` is deliberately
/// absent — it lives on the Definition tab, never conflated with build state.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = Workflow)]
pub struct WorkflowResponse {
    pub id: String,
    /// Grouping + author-owned handle of the `namespace.slug` identity. Display
    /// metadata so operators can group and identify workflows without decoding
    /// the opaque `id`.
    pub namespace: String,
    pub slug: String,
    pub name: String,
    pub trigger: WorkflowTrigger,
    pub version: Option<i64>,
    pub build_status: WorkflowBuildStatus,
    pub build_version: Option<i64>,
    /// State of the newest *fired* instance (the `WorkflowState` Debug string),
    /// or `None` if the workflow has never fired. Future-armed instances are
    /// excluded from this universe.
    pub latest_run_state: Option<String>,
    /// Count of terminal instances (`Completed` ∪ `Failed`). `0` when the
    /// workflow has never reached a terminal run.
    pub completed_runs: i64,
}

/// One registered version of a workflow, as the Version picker lists it:
/// the version string, its raw build `status`
/// (`Building|Ready|BuildFailed|Submitted`), and the registration time
/// (RFC3339). Ordered newest-first by the detail endpoint.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct AvailableVersion {
    pub version: i64,
    pub status: String,
    pub inserted_at: String,
}

/// The Workflow detail page's header + tab payload for one `(workflow_id,
/// version)`. `version` is the version this response describes (the explicit
/// `?version`, else the Default version). `workflow_definition` is an opaque
/// pass-through blob — the parsed `Workflow` JSON, untyped on the wire because
/// the shape is still in flux; the UI walks it with its own view-model types.
/// `latest_run_state` and `completed_runs` are workflow-aggregate (across every
/// version), so they stay invariant as the operator moves the Version picker.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
#[schema(as = WorkflowDetail)]
pub struct WorkflowDetailResponse {
    pub workflow_id: String,
    /// `namespace.slug` identity segments, surfaced on the detail header so the
    /// operator sees the human identity alongside the opaque `workflow_id`.
    pub namespace: String,
    pub slug: String,
    pub version: i64,
    pub nickel_source: String,
    pub workflow_definition: serde_json::Value,
    pub available_versions: Vec<AvailableVersion>,
    pub latest_run_state: Option<String>,
    /// `scheduled_at` (RFC3339) of the newest *fired* instance — the same
    /// candidate `latest_run_state` is read from, so the two never disagree.
    /// `None` when the workflow has never fired. The UI seeds the run
    /// calendar's initial year from it, landing directly on the latest active
    /// year with zero clicks (a dormant-since-last-year workflow would
    /// otherwise open on a blank current year).
    pub latest_run_at: Option<String>,
    pub completed_runs: i64,
}

/// Projection of a workflow instance (live or terminal) for instance views.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = WorkflowInstance)]
pub struct WorkflowInstanceResponse {
    pub id: String,
    pub workflow_id: String,
    /// The workflow definition version this run materialized under (the
    /// system-assigned integer). Carried by both projection sources; the
    /// Instances tab shows it per row.
    #[serde(default)]
    pub workflow_version: i64,
    /// Run name — the instance's human-readable primary identity. Always
    /// populated (the server supplies a default), so no client fallback is
    /// needed. Carried by both projection sources (live and archived).
    #[serde(default)]
    pub name: String,
    pub state: String,
    pub scheduled_at: Option<String>,
    pub task_count: usize,
    pub completed_tasks: usize,
}

/// One entry in the dashboard's "Up next" strip — a pre-created `Scheduled`
/// workflow instance the wheel will fire next. `next_run_at` is the instance's
/// absolute `scheduled_at` (RFC3339). `name` is the Run name — the instance's
/// human-readable primary identity (DC-0015), rendered on every chip
/// (`#[serde(default)]` tolerates a not-yet-upgraded coordinator). Shared shape
/// across the coordinator→API hop and the API→UI response. `workflow_id` lets a
/// chip deep-link to the parent workflow's detail page.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = UpcomingInstance)]
pub struct UpcomingInstanceResponse {
    pub workflow_instance_id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    #[serde(default)]
    pub name: String,
    pub next_run_at: Option<String>,
}

/// One day's per-state instance counts for the Run calendar, keyed by date in
/// the requested IANA timezone. Terminal counts (`completed` / `failed`) come
/// from the Postgres archive; non-terminal (`in_progress` / `scheduled`) from
/// the live source. `total` is their sum. The UI maps these onto a single cell
/// colour via the Calendar colour tier rule — the wire carries counts, not a
/// colour or a "future" flag.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct DayCounts {
    pub date: String,
    pub completed: i64,
    pub failed: i64,
    pub in_progress: i64,
    pub scheduled: i64,
    pub total: i64,
}

/// The Run calendar payload: per-day counts for one workflow across `year`,
/// bucketed in `tz`. Zero-count days are omitted (the UI renders absent days
/// muted). `live_data_available` mirrors the instance-list degraded-read
/// contract — `false` when the live half failed and only terminal counts are
/// present.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct CalendarResponse {
    pub year: i32,
    pub tz: String,
    pub days: Vec<DayCounts>,
    pub live_data_available: bool,
}

/// Task-log payload returned to the UI. Decompressed content as a string —
/// the `logs_resolver` strips the gzip wrapper before returning so the UI can
/// render directly without bringing a JS decompressor. The marker fields
/// report the End-of-stream marker identically for live and archived reads;
/// `marker_present: false` on a terminal task means the stream was never
/// closed (abnormal end — executor may have died).
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = TaskLogs)]
pub struct TaskLogResponse {
    pub logs: String,
    pub marker_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
}

/// One staged log batch with its Log cursor position (the JetStream stream
/// sequence).
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct TaskLogBatch {
    pub seq: u64,
    pub text: String,
}

/// Batch-mode task-log payload — returned for cursor (`after_seq`) and tail
/// (`tail_batches`) reads. `last_seq` is the cursor to poll from next;
/// `has_earlier` drives the "load earlier" affordance on tail reads. Marker
/// fields mirror `TaskLogResponse`.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct TaskLogBatchResponse {
    pub batches: Vec<TaskLogBatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seq: Option<u64>,
    pub has_earlier: bool,
    pub marker_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
}

/// Projection of a task instance (live or terminal) for the per-instance task
/// list view. The `attempt` field replaces the legacy `num_retries` exposure;
/// the UI orders attempts within a task by this field.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = TaskInstance)]
pub struct TaskInstanceResponse {
    pub id: String,
    pub task_id: String,
    pub workflow_instance_id: String,
    pub workflow_id: String,
    pub name: String,
    /// Published task-kind token (`RegularTask` / `SensorTask` /
    /// `ShadowTask`), preserved verbatim from both live and archived
    /// projections so the established JSON response remains unchanged.
    pub task_type: String,
    pub state: String,
    pub executor_id: Option<String>,
    // 0-indexed attempt number for this task instance. Default `0` keeps
    // backward compatibility for archive rows that predate the per-attempt
    // model. Tolerates omitted JSON fields via serde default.
    #[serde(default)]
    pub attempt: u32,
}

/// One workflow instance the day-clock buckets — a single live or archived run
/// for the selected calendar day. `state` is the verbatim substrate state (no
/// folding at the API; the UI folds for its four UI buckets). `scheduled_at` is
/// RFC3339. Shared shape across the coordinator→API hop and the API→UI response.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct ClockInstance {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub scheduled_at: Option<String>,
    pub state: String,
}

/// Day-clock payload returned to the UI: the merged live + archive instance
/// list for the window, plus `live_data_available` (false when the live half
/// failed and the response is archive-only, so the UI can show a quiet
/// degraded indicator rather than silently rendering a partial day).
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
pub struct ClockResponse {
    pub instances: Vec<ClockInstance>,
    pub live_data_available: bool,
}

/// One Event log row served by `GET /api/events`. Mirrors the tenant events
/// projection row: `seq` is the poll cursor (delivery order), `ts` is
/// occurrence time (display order, RFC3339), `payload` is the event's
/// archived JSON payload verbatim. `seq` — not the UUID `id` — is the
/// cursor, because UUIDs don't order; `id` stays the row's identity for
/// client-side dedup.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = Event)]
pub struct EventResponse {
    pub seq: i64,
    pub id: String,
    pub ts: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// One structure's identity for the reverse-link list: the full UUID
/// (`id`, authoritative) plus its short identity code (`code`, for display),
/// mirroring the instance snapshot's `IdentityRef`.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = IdentityRef)]
pub struct IdentityRefResponse {
    pub id: String,
    pub code: String,
}

/// One replay of a source run, served by `GET /api/workflows/instances/{id}/replays`.
/// The reverse link from a terminal run to the replays spawned from it — served
/// from the indexed `workflow_replays` pipeline row, never a unbounded live-state scan.
/// `shadowed_keys` is names-only (never values); `resume_from` carries identity
/// codes for display.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq, Eq)]
#[schema(as = ReplayRow)]
pub struct ReplayRowResponse {
    pub replay_instance_id: String,
    pub source_instance_id: String,
    pub status: String,
    pub name: Option<String>,
    pub resume_from: Vec<IdentityRefResponse>,
    pub shadowed_keys: Vec<String>,
    pub created_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn proto_trigger_keeps_the_established_response_shape() {
        let cron = wf::Trigger {
            kind: Some(wf::trigger::Kind::Cron("0 * * * *".to_string())),
        };
        assert_eq!(
            serde_json::to_value(workflow_trigger_from_proto(Some(&cron)).unwrap()).unwrap(),
            json!({"Cron": "0 * * * *"})
        );

        let fire_now = wf::Trigger {
            kind: Some(wf::trigger::Kind::FireNow(wf::trigger::FireNow {})),
        };
        assert_eq!(
            serde_json::to_value(workflow_trigger_from_proto(Some(&fire_now)).unwrap()).unwrap(),
            json!("FireNow")
        );

        let waits = wf::Trigger {
            kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: "invoice-paid".to_string(),
                predicate: Some("$.paid".to_string()),
                captures: vec![wf::CaptureDeclaration {
                    name: "invoice".to_string(),
                    from: Some(wf::CaptureSource {
                        source: Some(wf::capture_source::Source::Trigger(
                            wf::capture_source::Trigger {
                                jsonpath: "$.invoice".to_string(),
                            },
                        )),
                    }),
                }],
            })),
        };
        assert_eq!(
            serde_json::to_value(workflow_trigger_from_proto(Some(&waits)).unwrap()).unwrap(),
            json!({
                "WaitsOnSignal": {
                    "signal_name": "invoice-paid",
                    "predicate": "$.paid",
                    "captures": [{
                        "name": "invoice",
                        "from": {"Trigger": {"jsonpath": "$.invoice"}}
                    }]
                }
            })
        );
    }

    #[test]
    fn task_response_keeps_published_task_token() {
        let response = TaskInstanceResponse {
            id: "task-instance".to_string(),
            task_id: "task".to_string(),
            workflow_instance_id: "instance".to_string(),
            workflow_id: "workflow".to_string(),
            name: "extract".to_string(),
            task_type: "SensorTask".to_string(),
            state: "Running".to_string(),
            executor_id: None,
            attempt: 0,
        };
        assert_eq!(
            serde_json::to_value(response).unwrap()["task_type"],
            json!("SensorTask")
        );
    }
}
