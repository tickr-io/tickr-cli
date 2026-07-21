//! Per-task build pipeline for workflow definitions.
//!
//! The HTTP registration handler decomposes a freshly-parsed workflow into
//! one workflow row at status `Building` plus N per-task rows at status
//! `pending`, all in one selected-repository transaction. After commit, it publishes
//! one `TaskBuildJob` message per task onto the `conductor_build_queue`
//! NATS subject. Build workers consume the queue with NATS queue-group
//! semantics — each job is processed by exactly one worker across the
//! cluster of conductor replicas.
//!
//! Each worker invokes a [`BuildExecutor`] (the seam that hides whether
//! the build runs `nix` for real or returns a deterministic test outcome),
//! settles the per-task result and aggregate lifecycle through one repository
//! operation. The repository serializes finalizers for a definition, makes
//! `BuildFailed` terminal, and returns the single winning `Ready` publication
//! intent only after that transition commits. The worker then publishes the
//! small submission pointer; the durable `Ready` row remains the recovery
//! anchor if publication fails.

pub mod executor;
pub mod job;
pub mod worker;

pub use executor::{BuildExecutor, BuildOutcome, NixBuildExecutor, TestBuildExecutor};
pub use job::TaskBuildJob;
pub use worker::start_build_worker;

/// NATS subject the per-task build pipeline ships jobs over.
pub const BUILD_QUEUE_SUBJECT: &str = "conductor_build_queue";
