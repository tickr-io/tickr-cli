//! Conductor-to-server definition submission.
//!
//! Committed `Ready` lifecycle rows are authoritative in every formation.
//! Startup and bounded periodic scans acquire expiring leases, forward the
//! unchanged definition through the Conductor relay, and conditionally settle
//! `Ready -> Submitted`. NATS pointers and local channel notifications only
//! request earlier scans.
//!
//! Relay forwarding precedes settlement and does not claim Control-plane
//! application. A process death leaves the row reclaimable after lease expiry.

pub mod consumer;
pub mod local;
pub mod message;

pub use consumer::publish_submission;
pub use local::{
    definition_submission_notifications, start_local_definition_submission_worker,
    start_local_definition_submission_worker_with_claim_admission,
    DefinitionSubmissionNotificationStream, DefinitionSubmissionNotifier,
    LocalDefinitionSubmissionWorkerConfig,
};
pub use message::SubmissionMessage;

/// NATS JetStream durable subject the submission consumer rides on.
pub const SUBMISSION_QUEUE_SUBJECT: &str = tickr_proto::coord::all_nats::SUBMISSION_QUEUE_SUBJECT;

/// Queue-group name shared across replicas. NATS guarantees one
/// delivery per message across the group.
pub const SUBMISSION_QUEUE_GROUP: &str = tickr_proto::coord::all_nats::SUBMISSION_QUEUE_GROUP;
