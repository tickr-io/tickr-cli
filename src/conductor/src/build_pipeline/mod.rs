//! Per-task build pipeline for workflow definitions.
//!
//! Registration commits one `Building` Workflow row plus its per-Task
//! `pending` rows in the selected SQL repository. NATS `TaskBuildJob`
//! delivery is advisory: each notification requests an earlier bounded scan,
//! while startup and periodic scans lease the same authoritative rows.
//!
//! Each reconciler invokes a [`BuildExecutor`] after its lease commits, then
//! conditionally settles the Task and aggregate parent lifecycle in one
//! repository transaction. Lease expiry permits another idempotent Nix
//! realization after process death; only the current lease can settle. The
//! single winning `Ready` row is itself authoritative submission work.

pub mod executor;
pub mod job;
pub mod local;

pub use executor::{BuildExecutor, BuildOutcome, NixBuildExecutor, TestBuildExecutor};
pub use job::TaskBuildJob;
pub use local::{
    definition_build_notifications, start_local_definition_build_worker,
    start_local_definition_build_worker_with_claim_admission, DefinitionBuildNotificationStream,
    DefinitionBuildNotifier, LocalDefinitionBuildWorkerConfig,
};

/// NATS subject the per-task build pipeline ships jobs over.
pub const BUILD_QUEUE_SUBJECT: &str = tickr_proto::coord::all_nats::BUILD_QUEUE_SUBJECT;
/// Queue group shared by all-NATS definition-build wakeup subscribers.
pub const BUILD_QUEUE_GROUP: &str = tickr_proto::coord::all_nats::BUILD_QUEUE_GROUP;
