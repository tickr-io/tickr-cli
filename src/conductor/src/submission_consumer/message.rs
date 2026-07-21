//! Submission queue message shape.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Minimal pointer the consumer dereferences through the selected repository;
/// the workflow definition stays in durable storage so the queue message stays
/// small and the consumer always reads the committed snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmissionMessage {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
}
