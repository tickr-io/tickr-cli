//! Conductor-to-server definition submission.
//!
//! Distributed formations retain the NATS pointer queue and queue-group
//! consumer. Tickr Lite instead leases committed `Ready` lifecycle rows
//! directly from SQLite. Its notifications are latency hints: startup and
//! bounded steady-state scans recover missed hints and process restarts.
//!
//! Both paths preserve the relay-before-settlement boundary. Relay forwarding
//! projects the unchanged workflow definition family onto the Conductor relay;
//! only a successful forward permits the conditional `Ready -> Submitted`
//! settlement. The boundary does not claim Control-plane application.

pub mod consumer;
pub mod local;
pub mod message;
pub mod reconciliation;

pub use consumer::{publish_submission, start_submission_consumer};
pub use local::{
    definition_submission_notifications, start_local_definition_submission_worker,
    DefinitionSubmissionNotificationStream, DefinitionSubmissionNotifier,
    LocalDefinitionSubmissionWorkerConfig,
};
pub use message::SubmissionMessage;
pub use reconciliation::reconcile_orphan_ready_rows;

/// NATS JetStream durable subject the submission consumer rides on.
pub const SUBMISSION_QUEUE_SUBJECT: &str = "conductor_submission_queue";

/// Queue-group name shared across replicas. NATS guarantees one
/// delivery per message across the group.
pub const SUBMISSION_QUEUE_GROUP: &str = "conductor-submission-consumers";
