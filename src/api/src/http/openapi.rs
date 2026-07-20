//! API-owned documentation models for protobuf responses.
//!
//! These mirror the JSON projection served by the API without adding an HTTP
//! documentation dependency to the published `tickr_proto` boundary.

#![allow(dead_code)]

use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(ToSchema)]
#[schema(as = InstanceSnapshot)]
pub(crate) struct InstanceSnapshotDoc {
    id: String,
    workflow_id: String,
    name: String,
    workflow_name: String,
    workflow_version: i64,
    state: String,
    scheduled_at: Option<String>,
    triggered_at: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    transitions: Vec<StateTransitionViewDoc>,
    triggered_by: Option<TriggerProvenanceViewDoc>,
    tags: HashMap<String, String>,
    storage: String,
    task_count: u64,
    completed_tasks: u64,
    tasks: Vec<SnapshotTaskDefDoc>,
    task_instances: Vec<SnapshotTaskInstanceDoc>,
    graph: SnapshotGraphDoc,
    routing_variables: HashMap<String, RoutingValueViewDoc>,
    version: Option<u32>,
    applied_patches: Option<Vec<AppliedPatchViewDoc>>,
    version_snapshots: Option<HashMap<u32, SnapshotGraphDoc>>,
}

#[derive(ToSchema)]
pub(crate) struct StateTransitionViewDoc {
    from: String,
    to: String,
    at: String,
}

#[derive(ToSchema)]
#[schema(as = TriggerProvenanceView)]
pub(crate) struct TriggerProvenanceViewDoc {
    kind: String,
    signal_id: Option<String>,
    name: Option<String>,
    source_instance: Option<IdentityRefDoc>,
    resume_from: Option<Vec<IdentityRefDoc>>,
}

#[derive(ToSchema)]
pub(crate) struct IdentityRefDoc {
    id: String,
    code: String,
}

#[derive(ToSchema)]
#[schema(as = SnapshotTaskDef)]
pub(crate) struct SnapshotTaskDefDoc {
    id: String,
    name: String,
    task_type: String,
    max_attempts: u32,
    timeout_secs: Option<u64>,
    nix_expression_path: String,
    inputs: Vec<TaskInputViewDoc>,
    outputs: Vec<String>,
    secrets: Vec<String>,
    routing_vars: Vec<RoutingVarDeclViewDoc>,
    emits: Vec<TaskEmitViewDoc>,
}

#[derive(ToSchema)]
pub(crate) struct TaskInputViewDoc {
    name: String,
    source: Option<InputSourceViewDoc>,
}
#[derive(ToSchema)]
pub(crate) struct InputSourceViewDoc {
    kind: String,
    task: Option<String>,
    signal_name: Option<String>,
    gate_edge_id: Option<String>,
}
#[derive(ToSchema)]
pub(crate) struct RoutingVarDeclViewDoc {
    name: String,
    var_type: Option<String>,
}
#[derive(ToSchema)]
pub(crate) struct TaskEmitViewDoc {
    kind: String,
    signal_name: String,
    from_routing_var: Option<String>,
}

#[derive(ToSchema)]
#[schema(as = SnapshotTaskInstance)]
pub(crate) struct SnapshotTaskInstanceDoc {
    id: String,
    task_id: String,
    name: String,
    task_type: String,
    state: String,
    executor_id: Option<String>,
    attempt: u32,
    started_at: Option<String>,
    completed_at: Option<String>,
    cancel_reason: Option<String>,
    kill_confirmation: Option<String>,
    transitions: Vec<StateTransitionViewDoc>,
}

#[derive(ToSchema)]
#[schema(as = SnapshotGraph)]
pub(crate) struct SnapshotGraphDoc {
    start: String,
    end: String,
    nodes: Vec<SnapshotNodeDoc>,
    edges: Vec<SnapshotEdgeDoc>,
}
#[derive(ToSchema)]
pub(crate) struct SnapshotNodeDoc {
    code: String,
    id: String,
    kind: String,
    ground: String,
    grounded_at: Option<String>,
    ghost: Option<bool>,
    pre_grounded: Option<bool>,
}
#[derive(ToSchema)]
pub(crate) struct SnapshotEdgeDoc {
    code: String,
    id: String,
    sources: Vec<String>,
    targets: Vec<String>,
    kind: String,
    gates: Vec<GateViewDoc>,
}

#[derive(ToSchema)]
#[schema(as = GateView)]
pub(crate) struct GateViewDoc {
    kind: String,
    state: String,
    signal_id: Option<String>,
    signal_name: Option<String>,
    predicate: Option<String>,
    captures: Vec<String>,
    routing_var: Option<String>,
    op: Option<String>,
    value: Option<RoutingValueViewDoc>,
    timeout_secs: Option<u64>,
    duration_secs: Option<u64>,
    transitions: Vec<StateTransitionViewDoc>,
}

#[derive(ToSchema)]
#[schema(as = RoutingValueView)]
pub(crate) struct RoutingValueViewDoc {
    kind: String,
    value: serde_json::Value,
}

#[derive(ToSchema)]
#[schema(as = AppliedPatchView)]
pub(crate) struct AppliedPatchViewDoc {
    patch_key: String,
    prior_version: u32,
    version: u32,
    reason: Option<String>,
    provenance: String,
    applied_at: String,
    ops: Vec<PatchOpViewDoc>,
    minted_map: HashMap<String, String>,
}

#[derive(ToSchema)]
#[schema(as = PatchOpView)]
pub(crate) struct PatchOpViewDoc {
    op: String,
    node_id: Option<String>,
    edge_id: Option<String>,
    sources: Vec<String>,
    targets: Vec<String>,
}

#[derive(ToSchema)]
#[schema(as = PatchSource)]
pub(crate) struct PatchSourceDoc {
    patch_id: String,
    workflow_instance_id: String,
    source: String,
    source_format: String,
    applied_version: Option<i64>,
}

#[derive(ToSchema)]
#[schema(as = ReplayResult)]
pub(crate) struct ReplayResultDoc {
    replay_instance_id: String,
    deduplicated: Option<bool>,
    doomed: Vec<String>,
}

#[derive(ToSchema)]
#[serde(untagged)]
pub(crate) enum TaskLogsDocument {
    Full(super::dto::TaskLogResponse),
    Batch(super::dto::TaskLogBatchResponse),
}
