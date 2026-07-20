//! Submission queue message shape.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Minimal pointer the consumer dereferences against PG; the workflow
/// definition itself stays in the `workflows.definition` JSONB column
/// so the queue message stays small and the consumer always reads the
/// latest snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmissionMessage {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
}
