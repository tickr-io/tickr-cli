//! `BuildExecutor` — the seam between "real Nix build" and "test fake."
//!
//! The trait is intentionally narrow: take a `TaskBuildJob`, return a
//! `BuildOutcome`. Production wires [`NixBuildExecutor`] which shells out
//! to `nix build`. Tests inject [`TestBuildExecutor`] which returns
//! per-task outcomes deterministically.
//!
//! Tests assert on the *effect* of the executor on the workflow lifecycle
//! (per-task rows update, finalizer flips workflow row), not on the
//! executor's internals.

use crate::build_pipeline::job::TaskBuildJob;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

/// Per-task build outcome. `error` is a free-form message attached to a
/// `Failure` so the failed per-task PG row can carry diagnostic text the
/// author sees in the UI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildOutcome {
    Success,
    Failure { error: String },
}

#[async_trait]
pub trait BuildExecutor: Send + Sync {
    async fn build(&self, job: &TaskBuildJob) -> BuildOutcome;
}

/// Production executor. Shells out to `nix build <nix_expression_path>`
/// and treats a non-zero exit code as a failure, capturing stderr into
/// the failure's error text. The build realizes the derivation by
/// expression path alone — runtime args belong to `nix run`, not the
/// build. The wall-clock latency of a
/// real build is whatever `nix` takes — workers handle one job at a
/// time so a slow build doesn't head-of-line-block other workers in the
/// NATS queue group.
pub struct NixBuildExecutor;

#[async_trait]
impl BuildExecutor for NixBuildExecutor {
    async fn build(&self, job: &TaskBuildJob) -> BuildOutcome {
        use tokio::process::Command;
        let mut cmd = Command::new("nix");
        cmd.arg("build");
        cmd.arg(&job.nix_expression_path);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        match cmd.output().await {
            Ok(output) if output.status.success() => BuildOutcome::Success,
            Ok(output) => BuildOutcome::Failure {
                error: String::from_utf8_lossy(&output.stderr).to_string(),
            },
            Err(e) => BuildOutcome::Failure {
                error: format!("nix invocation error: {}", e),
            },
        }
    }
}

/// Deterministic test executor. The caller pre-populates a map of
/// per-task outcomes; `build` returns the configured outcome for the
/// job's `task_id`. Unconfigured tasks default to `Success` so tests can
/// configure only the failure cases they care about.
pub struct TestBuildExecutor {
    outcomes: Mutex<HashMap<Uuid, BuildOutcome>>,
}

impl TestBuildExecutor {
    pub fn new() -> Self {
        Self {
            outcomes: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_outcome(&self, task_id: Uuid, outcome: BuildOutcome) {
        self.outcomes.lock().unwrap().insert(task_id, outcome);
    }

    pub fn fail(&self, task_id: Uuid, error: &str) {
        self.set_outcome(
            task_id,
            BuildOutcome::Failure {
                error: error.to_string(),
            },
        );
    }
}

impl Default for TestBuildExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BuildExecutor for TestBuildExecutor {
    async fn build(&self, job: &TaskBuildJob) -> BuildOutcome {
        self.outcomes
            .lock()
            .unwrap()
            .get(&job.task_id)
            .cloned()
            .unwrap_or(BuildOutcome::Success)
    }
}
