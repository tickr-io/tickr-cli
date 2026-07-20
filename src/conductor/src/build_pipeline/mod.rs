//! Per-task build pipeline for workflow definitions.
//!
//! The HTTP registration handler decomposes a freshly-parsed workflow into
//! one workflow row at status `Building` plus N per-task rows at status
//! `pending`, all in one Postgres transaction. After commit it publishes
//! one `TaskBuildJob` message per task onto the `conductor_build_queue`
//! NATS subject. Build workers consume the queue with NATS queue-group
//! semantics — each job is processed by exactly one worker across the
//! cluster of conductor replicas.
//!
//! Each worker invokes a [`BuildExecutor`] (the seam that hides whether
//! the build runs `nix` for real or returns a deterministic test outcome),
//! commits the per-task row's status to `success` or `failure`, and then
//! runs the **last-one-out finalizer**: a single atomic conditional UPDATE
//! against the `workflows` row that succeeds only when the row is still
//! `Building` AND every per-task row is `success`. The conditional UPDATE
//! is the locking mechanism — concurrent finalizers race it; at most one
//! wins. The winning finalizer atomically inserts the registration outbox
//! row in the same transaction so the cross-plane hand-off is gated on
//! the lifecycle transition, not the build worker's wall-clock ordering.
//!
//! Any per-task `failure` short-circuits the workflow row to `BuildFailed`
//! (terminal); no outbox row is created.

pub mod executor;
pub mod finalizer;
pub mod job;
pub mod worker;

pub use executor::{BuildExecutor, BuildOutcome, NixBuildExecutor, TestBuildExecutor};
pub use finalizer::{finalize_after_task_outcome, load_workflow_definition, FinalizerOutcome};
pub use job::TaskBuildJob;
pub use worker::start_build_worker;

/// NATS subject the per-task build pipeline ships jobs over.
pub const BUILD_QUEUE_SUBJECT: &str = "conductor_build_queue";
