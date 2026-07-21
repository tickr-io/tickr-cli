//! Per-task build job message carried over NATS.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One build-job-per-task message. The conductor's HTTP registration
/// handler emits one of these for every Task after the repository transaction
/// that inserted the definition and per-Task rows commits. Workers consume the
/// queue with NATS queue-group semantics —
/// exactly one worker handles each job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskBuildJob {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
    pub task_id: Uuid,
    /// Nix derivation path the build executor should realize. Mirrors
    /// the `Task.nix_expression_path` field on the in-memory definition.
    /// This is the only build input — runtime args belong to `nix run`,
    /// not `nix build`, so they are deliberately not carried here.
    pub nix_expression_path: String,
}
