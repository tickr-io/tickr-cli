// Generated public protobuf contracts. The root `tickr` package contains only
// the conductor relay contract from proto/conductor-relay.proto.

pub mod tickr {
    tonic::include_proto!("tickr");
}

// Versioned, formation-neutral Installation bootstrap contract (package
// `tickr.installation`, from proto/installation-bootstrap.proto). The generated
// wire family and its fail-closed semantic validator share one public module.
pub mod installation {
    tonic::include_proto!("tickr.installation");
    include!("installation.rs");
}

// API<->conductor command contract (package `tickr.api`, from
// proto/tickr-api.proto). Kept in its own module so callers reach the command
// envelopes via `tickr_proto::tickr_api::*` without colliding with the `tickr`
// package's types.
pub mod tickr_api {
    tonic::include_proto!("tickr.api");
}

// Published workflow-definition contract (package `tickr.workflow`, from
// proto/workflow-definition.proto). Kept in its own module rather than glob
// re-exported so its field-number space stays explicit.
pub mod workflow {
    tonic::include_proto!("tickr.workflow");
}

// Published runtime-graph-patching contract (package `tickr.patch`, from
// proto/patch.proto). It carries authored patches and their terminal outcomes.
// Operations carrying node content reuse the `workflow`
// family's task/gate/edge-kind messages, so there is one wire model of a task.
// Kept in its own module so its `PatchEnvelope` / `PatchOperation` and friends
// own the `tickr.patch` field-number space.
pub mod patch {
    tonic::include_proto!("tickr.patch");
}

// Published instance snapshot contract (package `tickr.instance`, from
// proto/instance-snapshot.proto). This is the wire projection rendered by the
// instance detail page and consumed directly by the API component. Kept in its
// own module so its snapshot messages own the
// `tickr.instance` field-number space and don't collide with the runtime
// `WorkflowInstance` stub in the `tickr` package.
pub mod instance {
    tonic::include_proto!("tickr.instance");
}

// Published runnable-fidelity archive projection contract (package
// `tickr.runnable`, from proto/runnable-projection.proto). The replay-grade
// projection of one workflow instance's runnable graph — the workflow-definition
// shape plus the runtime overlays (per-node ground, per-gate state and
// transition history, graph head/tail) used to reconstruct a replay seed.
// Reuses the `workflow` family for the
// definition-shaped parts, so it is kept in its own module (not glob
// re-exported) and owns the `tickr.runnable` field-number space.
pub mod runnable {
    tonic::include_proto!("tickr.runnable");
}

// Published union archive-grade projection (package `tickr.archive`, from
// proto/archive-union.proto). The single persisted shape both archive reads
// reconstruct from — the instance-detail render read and the replay-seed read.
// It embeds the `runnable` family's graph/tasks section and reuses the
// `instance` family's render views, so one archived shape serves rendering and
// replay. Kept in its own module so the union message owns the `tickr.archive`
// field-number space.
pub mod archive {
    tonic::include_proto!("tickr.archive");
}

// Published task-coordination contract (package `tickr.task`, from
// proto/task-coordination.proto). Used to dispatch a task, report its lifecycle,
// evaluate a hyperedge gate's preconditions, and
// cancel a live attempt. The dispatch projection carries only the runtime
// execution slice of a task instance — replication/state-machine bookkeeping is
// structurally absent. Reuses the `workflow` family for the one wire model of a
// routing value and a capture declaration, so it is kept in its own module (not
// glob re-exported) and owns the `tickr.task` field-number space.
pub mod task {
    tonic::include_proto!("tickr.task");
}

// Published Signal-family contract (package `tickr.signal`, from
// proto/signal.proto). The conductor-authored live-run control envelopes — the
// Signal envelope and its Cancel/Wakeup/Trigger/Resume variants, GateOutcome,
// and SignalApplied. The `Trigger.replay` seed reuses the `runnable` and
// `workflow` families for its graph and task specs, so it is kept in its own
// module (not glob re-exported) and owns the `tickr.signal` field-number space.
pub mod signal {
    tonic::include_proto!("tickr.signal");
}

// JSON↔protobuf and protobuf-bytes↔protobuf contract codecs.
pub mod codec;
pub mod config;
// Task-coordination constants: NATS names and shared liveness key schemas.
// They live next to the task-coordination messages in `task`.
pub mod coord;
pub mod identity;
pub mod tenant;

// Re-export commonly used types at the crate root
pub use identity::identity_code;
pub use tenant::{derive_scheduled_workflow_instance_id, derive_workflow_id, TenantId};
pub use tickr::*;
