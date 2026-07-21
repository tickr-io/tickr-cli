//! Conductor-to-server submission consumer.
//!
//! The per-task build repository commits a `Building -> Ready` transition
//! and returns the single winning publication intent. The worker then publishes a
//! small `{ workflow_id, workflow_version }` message onto a NATS
//! JetStream durable subject. This module consumes that subject with
//! queue-group semantics across replicas and ships the freshly-built
//! workflow definition over the relay as a `SubmitWorkflow` envelope.
//!
//! Idempotency anchor: the consumer reads through the selected repository and
//! ACKs without shipping when the definition is no longer at `Ready`. That
//! covers both the JetStream redelivery case (the message gets
//! re-delivered after a slow ACK) and the boot-time reconciliation case
//! where a duplicate publish is intentionally produced.
//!
//! Dual-write hazard (the repository commits `Ready`, NATS publish fails) is
//! bounded by [`reconcile_orphan_ready_rows`]: at startup, before the
//! consumer subscribes, the conductor scans for orphan `Ready` rows
//! and republishes a message per row. No periodic reconciliation runs
//! in steady state.

pub mod consumer;
pub mod message;
pub mod reconciliation;

pub use consumer::{publish_submission, start_submission_consumer};
pub use message::SubmissionMessage;
pub use reconciliation::reconcile_orphan_ready_rows;

/// NATS JetStream durable subject the submission consumer rides on.
pub const SUBMISSION_QUEUE_SUBJECT: &str = "conductor_submission_queue";

/// Queue-group name shared across replicas. NATS guarantees one
/// delivery per message across the group.
pub const SUBMISSION_QUEUE_GROUP: &str = "conductor-submission-consumers";
