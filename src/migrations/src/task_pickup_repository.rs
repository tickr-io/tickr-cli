use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, RepositoryError, RepositoryErrorKind,
    WriterRepositoryBundle,
};
use crate::encoding::{decode_timestamp, encode_json, encode_timestamp};

const MAX_DISPATCH_KEY_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 128;
pub type PickupTimestamp = DateTime<Utc>;

pub fn pickup_now() -> PickupTimestamp {
    Utc::now()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTaskDispatch {
    pub dispatch_key: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPickupClaim {
    pub dispatch_key: String,
    pub pickup_generation: i64,
    pub owner: String,
    pub liveness_deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct ClaimTaskPickupInput<'a> {
    pub dispatch_key: &'a str,
    pub owner: &'a str,
    pub liveness_deadline: DateTime<Utc>,
    pub assigned_event: &'a [u8],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimTaskPickupOutcome {
    Committed(TaskPickupClaim),
    NotPending,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPickupTerminalOutcome {
    ProcessExitedSuccess,
    ProcessExitedFailure,
    ProcessSetupFailed,
    LivenessExpired,
    CancellationKilled,
    CancellationAlreadyExited,
    CancellationNoProcess,
}

impl TaskPickupTerminalOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProcessExitedSuccess => "process-exited-success",
            Self::ProcessExitedFailure => "process-exited-failure",
            Self::ProcessSetupFailed => "process-setup-failed",
            Self::LivenessExpired => "liveness-expired",
            Self::CancellationKilled => "cancellation-killed",
            Self::CancellationAlreadyExited => "cancellation-already-exited",
            Self::CancellationNoProcess => "cancellation-no-process",
        }
    }

    fn event_kind(self) -> Option<&'static str> {
        match self {
            Self::ProcessExitedSuccess => Some("Completed"),
            Self::ProcessExitedFailure | Self::ProcessSetupFailed => Some("Failed"),
            Self::LivenessExpired => Some("Unhealthy"),
            Self::CancellationKilled
            | Self::CancellationAlreadyExited
            | Self::CancellationNoProcess => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPickupTerminalElection {
    Won,
    Settled(TaskPickupTerminalOutcome),
    NotClaimed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueTaskPickup {
    pub claim: TaskPickupClaim,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTaskEvent {
    pub dispatch_key: String,
    pub pickup_generation: i64,
    pub kind: String,
    pub event: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCancellationReconciliation {
    Killed,
    AlreadyExited,
    NoProcess,
}

impl TaskCancellationReconciliation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Killed => "killed",
            Self::AlreadyExited => "already-exited",
            Self::NoProcess => "no-process",
        }
    }

    fn terminal_outcome(self) -> TaskPickupTerminalOutcome {
        match self {
            Self::Killed => TaskPickupTerminalOutcome::CancellationKilled,
            Self::AlreadyExited => TaskPickupTerminalOutcome::CancellationAlreadyExited,
            Self::NoProcess => TaskPickupTerminalOutcome::CancellationNoProcess,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCancellationFence {
    pub acknowledgement_identity: String,
    pub task_instance_id: String,
    pub workflow_instance_id: String,
    pub dispatch_key: Option<String>,
    pub pickup_generation: Option<i64>,
    pub owner: Option<String>,
    pub owner_notified: bool,
    pub liveness_deadline: Option<DateTime<Utc>>,
    pub terminal_outcome: Option<TaskPickupTerminalOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTaskCancellationAck {
    pub acknowledgement_identity: String,
    pub acknowledgement: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPickupSnapshot {
    pub state: String,
    pub pickup_generation: i64,
    pub owner: Option<String>,
    pub liveness_deadline: Option<DateTime<Utc>>,
    pub liveness_armed_at: Option<DateTime<Utc>>,
    pub rejection_reason: Option<String>,
    pub staged_event_kinds: Vec<String>,
    pub quarantined: bool,
    pub terminal_outcome: Option<TaskPickupTerminalOutcome>,
    pub forwarded_event_kinds: Vec<String>,
    pub cancellation_fence: Option<TaskCancellationFence>,
    pub cancellation_reconciliation: Option<TaskCancellationReconciliation>,
    pub cancellation_ack_forwarded: bool,
}

/// One generation-qualified local Log staging stream selected from durable
/// task-pickup state for a terminal Workflow instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalTaskLogStream {
    pub task_instance_id: Uuid,
    pub pickup_generation: u64,
}

#[derive(Debug, thiserror::Error)]
enum TaskPickupRepositoryError {
    #[error("local task pickup operations require the SQLite writer")]
    RequiresSqlite,
    #[error("dispatch key must contain 1 to {MAX_DISPATCH_KEY_BYTES} bytes")]
    InvalidDispatchKey,
    #[error("pickup owner must contain 1 to {MAX_OWNER_BYTES} bytes")]
    InvalidOwner,
    #[error("liveness deadline must be later than the operation time")]
    InvalidDeadline,
    #[error("pickup generation overflowed for dispatch `{0}`")]
    GenerationOverflow(String),
    #[error("dispatch key `{0}` was reused for different payload bytes")]
    DispatchIdentityCollision(String),
    #[error("stored local task pickup value is corrupt: {0}")]
    CorruptStoredValue(String),
    #[error("task cancellation identity must contain 1 to {MAX_OWNER_BYTES} bytes")]
    InvalidCancellationIdentity,
}

impl WriterRepositoryBundle {
    pub async fn stage_task_dispatch(
        &self,
        dispatch_key: &str,
        payload: &[u8],
        task_instance_id: Option<&str>,
        workflow_instance_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_dispatch_key(dispatch_key)?;
        validate_optional_task_identity(task_instance_id, workflow_instance_id)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        stage_task_dispatch_sqlite(
            pool,
            dispatch_key,
            payload,
            task_instance_id,
            workflow_instance_id,
            now,
        )
        .await
    }

    /// Observe the oldest pending dispatch without changing durable ownership.
    pub async fn select_pending_task_dispatch(
        &self,
    ) -> Result<Option<PendingTaskDispatch>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        select_pending_task_dispatch_sqlite(pool).await
    }

    pub async fn reject_task_dispatch(
        &self,
        dispatch_key: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_dispatch_key(dispatch_key)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        reject_task_dispatch_sqlite(pool, dispatch_key, reason, now).await
    }

    /// Atomically claim one pending dispatch and stage its published `Assigned` event.
    pub async fn claim_task_pickup(
        &self,
        input: ClaimTaskPickupInput<'_>,
    ) -> Result<ClaimTaskPickupOutcome, RepositoryError> {
        validate_claim_input(input)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        claim_task_pickup_sqlite(pool, input).await
    }

    pub async fn prove_ambiguous_task_pickup(
        &self,
        dispatch_key: &str,
        owner: &str,
        assigned_event: &[u8],
    ) -> Result<Option<TaskPickupClaim>, RepositoryError> {
        validate_dispatch_key(dispatch_key)?;
        validate_owner(owner)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        prove_ambiguous_task_pickup_sqlite(pool, dispatch_key, owner, assigned_event).await
    }

    pub async fn arm_task_pickup_liveness(
        &self,
        claim: &TaskPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_claim(claim, deadline, now)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        let updated = sqlx::query(
            "UPDATE local_task_dispatches SET liveness_deadline = ?4, liveness_armed_at = ?5, updated_at = ?5 \
             WHERE dispatch_key = ?1 AND state = 'claimed' AND pickup_generation = ?2 AND owner = ?3 \
               AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
                   WHERE o.dispatch_key = local_task_dispatches.dispatch_key \
                     AND o.pickup_generation = local_task_dispatches.pickup_generation) \
               AND EXISTS (SELECT 1 FROM local_task_event_outbox e \
                   WHERE e.dispatch_key = local_task_dispatches.dispatch_key \
                     AND e.pickup_generation = local_task_dispatches.pickup_generation \
                     AND e.kind = 'Assigned')",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(&claim.owner)
        .bind(encode_timestamp(deadline))
        .bind(encode_timestamp(now))
        .execute(pool)
        .await
        .map_err(repository_sqlx_error)?;
        Ok(updated.rows_affected() == 1)
    }

    pub async fn prove_task_pickup_ready(
        &self,
        claim: &TaskPickupClaim,
        assigned_event: &[u8],
    ) -> Result<bool, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        let matches: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM local_task_dispatches d \
             JOIN local_task_event_outbox e ON e.dispatch_key = d.dispatch_key \
               AND e.pickup_generation = d.pickup_generation AND e.kind = 'Assigned' \
             WHERE d.dispatch_key = ?1 AND d.state = 'claimed' AND d.pickup_generation = ?2 \
               AND d.owner = ?3 AND d.liveness_armed_at IS NOT NULL AND e.event = ?4 \
               AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
                   WHERE o.dispatch_key = d.dispatch_key \
                     AND o.pickup_generation = d.pickup_generation) \
               AND NOT EXISTS (SELECT 1 FROM local_task_cancellation_fences f \
                   WHERE (f.dispatch_key = d.dispatch_key AND f.pickup_generation = d.pickup_generation) \
                      OR (f.dispatch_key IS NULL AND f.task_instance_id = d.task_instance_id \
                          AND f.workflow_instance_id = d.workflow_instance_id))",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(&claim.owner)
        .bind(encode_bytes(assigned_event))
        .fetch_one(pool)
        .await
        .map_err(repository_sqlx_error)?;
        Ok(matches == 1)
    }

    pub async fn stage_task_started(
        &self,
        claim: &TaskPickupClaim,
        started_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO local_task_event_outbox \
                 (dispatch_key, pickup_generation, kind, event, staged_at) \
             SELECT dispatch_key, pickup_generation, 'Started', ?4, ?5 \
             FROM local_task_dispatches \
             WHERE dispatch_key = ?1 AND state = 'claimed' AND pickup_generation = ?2 \
               AND owner = ?3 AND liveness_armed_at IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
                   WHERE o.dispatch_key = local_task_dispatches.dispatch_key \
                     AND o.pickup_generation = local_task_dispatches.pickup_generation)",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(&claim.owner)
        .bind(encode_bytes(started_event))
        .bind(encode_timestamp(now))
        .execute(pool)
        .await
        .map_err(repository_sqlx_error)?;
        Ok(inserted.rows_affected() == 1)
    }

    pub async fn renew_task_pickup_liveness(
        &self,
        claim: &TaskPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_claim(claim, deadline, now)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        let updated = sqlx::query(
            "UPDATE local_task_dispatches SET liveness_deadline = ?4, updated_at = ?5 \
             WHERE dispatch_key = ?1 AND state = 'claimed' AND pickup_generation = ?2 \
               AND owner = ?3 AND liveness_armed_at IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
                   WHERE o.dispatch_key = local_task_dispatches.dispatch_key \
                     AND o.pickup_generation = local_task_dispatches.pickup_generation)",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(&claim.owner)
        .bind(encode_timestamp(deadline))
        .bind(encode_timestamp(now))
        .execute(pool)
        .await
        .map_err(repository_sqlx_error)?;
        Ok(updated.rows_affected() == 1)
    }

    /// Make a failed liveness renewal durable without electing an outcome.
    /// Recovery observes the now-due claim through the normal terminal seam.
    pub async fn register_task_pickup_liveness_failure(
        &self,
        claim: &TaskPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_claim_identity(claim)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        let updated = sqlx::query(
            "UPDATE local_task_dispatches SET liveness_deadline = ?4, updated_at = ?4 \
             WHERE dispatch_key = ?1 AND state = 'claimed' AND pickup_generation = ?2 \
               AND owner = ?3 AND liveness_armed_at IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
                   WHERE o.dispatch_key = local_task_dispatches.dispatch_key \
                     AND o.pickup_generation = local_task_dispatches.pickup_generation)",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(&claim.owner)
        .bind(encode_timestamp(now))
        .execute(pool)
        .await
        .map_err(repository_sqlx_error)?;
        Ok(updated.rows_affected() == 1)
    }

    /// Observe one overdue unresolved generation without changing it.
    pub async fn select_due_task_pickup(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DueTaskPickup>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        select_due_task_pickup_sqlite(pool, now).await
    }

    /// Elect one generation-qualified terminal outcome and stage its published
    /// terminal TaskEvent in the same writer transaction.
    pub async fn elect_task_pickup_terminal(
        &self,
        claim: &TaskPickupClaim,
        outcome: TaskPickupTerminalOutcome,
        terminal_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<TaskPickupTerminalElection, RepositoryError> {
        validate_claim_identity(claim)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        elect_task_pickup_terminal_sqlite(pool, claim, outcome, terminal_event, now).await
    }

    pub async fn select_pending_task_event(
        &self,
    ) -> Result<Option<PendingTaskEvent>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        select_pending_task_event_sqlite(pool).await
    }

    pub async fn mark_task_event_forwarded(
        &self,
        event: &PendingTaskEvent,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        mark_task_event_forwarded_sqlite(pool, event, now).await
    }

    /// Commit a durable cancellation barrier before notifying the pickup owner.
    pub async fn commit_task_cancellation_fence(
        &self,
        acknowledgement_identity: &str,
        task_instance_id: &str,
        workflow_instance_id: &str,
        now: DateTime<Utc>,
    ) -> Result<TaskCancellationFence, RepositoryError> {
        validate_cancellation_identity(acknowledgement_identity)?;
        validate_cancellation_identity(task_instance_id)?;
        validate_cancellation_identity(workflow_instance_id)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        commit_task_cancellation_fence_sqlite(
            pool,
            acknowledgement_identity,
            task_instance_id,
            workflow_instance_id,
            now,
        )
        .await
    }

    pub async fn mark_task_cancellation_owner_notified(
        &self,
        fence: &TaskCancellationFence,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        mark_task_cancellation_owner_notified_sqlite(pool, fence, now).await
    }

    /// Enter cancellation reconciliation through the same generation-qualified
    /// terminal election guard, then stage one stable acknowledgement.
    pub async fn settle_task_cancellation(
        &self,
        fence: &TaskCancellationFence,
        reconciliation: TaskCancellationReconciliation,
        acknowledgement: &[u8],
        now: DateTime<Utc>,
    ) -> Result<Option<TaskPickupTerminalElection>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        settle_task_cancellation_sqlite(pool, fence, reconciliation, acknowledgement, now).await
    }

    pub async fn select_unresolved_task_cancellation(
        &self,
    ) -> Result<Option<TaskCancellationFence>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        select_unresolved_task_cancellation_sqlite(pool).await
    }

    pub async fn select_pending_task_cancellation_ack(
        &self,
    ) -> Result<Option<PendingTaskCancellationAck>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        select_pending_task_cancellation_ack_sqlite(pool).await
    }

    pub async fn mark_task_cancellation_ack_forwarded(
        &self,
        acknowledgement: &PendingTaskCancellationAck,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        mark_task_cancellation_ack_forwarded_sqlite(pool, acknowledgement, now).await
    }

    pub async fn task_pickup_snapshot(
        &self,
        dispatch_key: &str,
    ) -> Result<Option<TaskPickupSnapshot>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        task_pickup_snapshot_sqlite(pool, dispatch_key).await
    }

    /// Return the durable Log staging identities for one Workflow instance.
    ///
    /// Compaction derives its local log inventory from task-pickup ownership;
    /// it never invents a generation when a task was not durably claimed.
    pub async fn local_task_log_streams_for_workflow_instance(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<LocalTaskLogStream>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        local_task_log_streams_for_workflow_instance_sqlite(pool, workflow_instance_id).await
    }

    /// Return every durable Log staging identity for startup journal recovery.
    pub async fn all_local_task_log_streams(
        &self,
    ) -> Result<Vec<LocalTaskLogStream>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                TaskPickupRepositoryError::RequiresSqlite,
            ));
        };
        local_task_log_streams_sqlite(pool, None).await
    }
}

async fn stage_task_dispatch_sqlite(
    pool: &SqlitePool,
    dispatch_key: &str,
    payload: &[u8],
    task_instance_id: Option<&str>,
    workflow_instance_id: Option<&str>,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let encoded_payload = encode_bytes(payload);
    let encoded_now = encode_timestamp(now);
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO local_task_dispatches \
             (dispatch_key, payload, state, pickup_generation, task_instance_id, \
              workflow_instance_id, created_at, updated_at) \
         VALUES (?1, ?2, 'pending', 0, ?3, ?4, ?5, ?5)",
    )
    .bind(dispatch_key)
    .bind(&encoded_payload)
    .bind(task_instance_id)
    .bind(workflow_instance_id)
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    if inserted.rows_affected() == 1 {
        if let (Some(task_instance_id), Some(workflow_instance_id)) =
            (task_instance_id, workflow_instance_id)
        {
            sqlx::query(
                "UPDATE local_task_cancellation_fences \
                 SET dispatch_key = ?1, pickup_generation = 1 \
                 WHERE dispatch_key IS NULL AND task_instance_id = ?2 \
                   AND workflow_instance_id = ?3",
            )
            .bind(dispatch_key)
            .bind(task_instance_id)
            .bind(workflow_instance_id)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
        }
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(true);
    }
    let stored: Option<String> =
        sqlx::query_scalar("SELECT payload FROM local_task_dispatches WHERE dispatch_key = ?1")
            .bind(dispatch_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    if stored.as_deref() != Some(encoded_payload.as_str()) {
        return Err(RepositoryError::new(
            RepositoryErrorKind::ConstraintConflict,
            TaskPickupRepositoryError::DispatchIdentityCollision(dispatch_key.to_owned()),
        ));
    }
    Ok(false)
}

async fn select_pending_task_dispatch_sqlite(
    pool: &SqlitePool,
) -> Result<Option<PendingTaskDispatch>, RepositoryError> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT d.dispatch_key, d.payload FROM local_task_dispatches d \
         WHERE d.state = 'pending' \
           AND NOT EXISTS (SELECT 1 FROM local_task_cancellation_fences f \
               WHERE (f.dispatch_key = d.dispatch_key \
                       AND f.pickup_generation = d.pickup_generation + 1) \
                  OR (f.dispatch_key IS NULL AND f.task_instance_id = d.task_instance_id \
                      AND f.workflow_instance_id = d.workflow_instance_id)) \
         ORDER BY d.created_at, d.dispatch_key LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(dispatch_key, payload)| {
        Ok(PendingTaskDispatch {
            dispatch_key,
            payload: decode_bytes(&payload)?,
        })
    })
    .transpose()
}

async fn reject_task_dispatch_sqlite(
    pool: &SqlitePool,
    dispatch_key: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let payload: Option<String> = sqlx::query_scalar(
        "SELECT payload FROM local_task_dispatches WHERE dispatch_key = ?1 AND state = 'pending'",
    )
    .bind(dispatch_key)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(payload) = payload else {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(false);
    };
    let encoded_now = encode_timestamp(now);
    sqlx::query(
        "INSERT INTO local_task_dispatch_quarantine \
             (dispatch_key, payload, reason, quarantined_at) VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(dispatch_key)
    .bind(payload)
    .bind(reason)
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    sqlx::query(
        "UPDATE local_task_dispatches SET state = 'rejected', rejection_reason = ?2, updated_at = ?3 \
         WHERE dispatch_key = ?1 AND state = 'pending'",
    )
    .bind(dispatch_key)
    .bind(reason)
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(true)
}

async fn claim_task_pickup_sqlite(
    pool: &SqlitePool,
    input: ClaimTaskPickupInput<'_>,
) -> Result<ClaimTaskPickupOutcome, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let current_generation: Option<i64> = sqlx::query_scalar(
        "SELECT pickup_generation FROM local_task_dispatches \
         WHERE dispatch_key = ?1 AND state = 'pending'",
    )
    .bind(input.dispatch_key)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(current_generation) = current_generation else {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(ClaimTaskPickupOutcome::NotPending);
    };
    let pickup_generation = current_generation.checked_add(1).ok_or_else(|| {
        RepositoryError::new(
            RepositoryErrorKind::Internal,
            TaskPickupRepositoryError::GenerationOverflow(input.dispatch_key.to_owned()),
        )
    })?;
    let encoded_deadline = encode_timestamp(input.liveness_deadline);
    let encoded_now = encode_timestamp(input.now);
    let updated = sqlx::query(
        "UPDATE local_task_dispatches SET state = 'claimed', pickup_generation = ?2, owner = ?3, \
             liveness_deadline = ?4, updated_at = ?5 \
         WHERE dispatch_key = ?1 AND state = 'pending' AND pickup_generation = ?6 \
           AND NOT EXISTS (SELECT 1 FROM local_task_cancellation_fences f \
               WHERE (f.dispatch_key = local_task_dispatches.dispatch_key \
                       AND f.pickup_generation = ?2) \
                  OR (f.dispatch_key IS NULL \
                      AND f.task_instance_id = local_task_dispatches.task_instance_id \
                      AND f.workflow_instance_id = local_task_dispatches.workflow_instance_id))",
    )
    .bind(input.dispatch_key)
    .bind(pickup_generation)
    .bind(input.owner)
    .bind(encoded_deadline)
    .bind(encoded_now)
    .bind(current_generation)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    if updated.rows_affected() != 1 {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(ClaimTaskPickupOutcome::NotPending);
    }
    sqlx::query(
        "INSERT INTO local_task_event_outbox \
             (dispatch_key, pickup_generation, kind, event, staged_at) \
         VALUES (?1, ?2, 'Assigned', ?3, ?4)",
    )
    .bind(input.dispatch_key)
    .bind(pickup_generation)
    .bind(encode_bytes(input.assigned_event))
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(ClaimTaskPickupOutcome::Committed(TaskPickupClaim {
        dispatch_key: input.dispatch_key.to_owned(),
        pickup_generation,
        owner: input.owner.to_owned(),
        liveness_deadline: input.liveness_deadline,
    }))
}

async fn prove_ambiguous_task_pickup_sqlite(
    pool: &SqlitePool,
    dispatch_key: &str,
    owner: &str,
    assigned_event: &[u8],
) -> Result<Option<TaskPickupClaim>, RepositoryError> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT d.pickup_generation, d.liveness_deadline FROM local_task_dispatches d \
         JOIN local_task_event_outbox e ON e.dispatch_key = d.dispatch_key \
           AND e.pickup_generation = d.pickup_generation AND e.kind = 'Assigned' \
         WHERE d.dispatch_key = ?1 AND d.state = 'claimed' AND d.owner = ?2 AND e.event = ?3",
    )
    .bind(dispatch_key)
    .bind(owner)
    .bind(encode_bytes(assigned_event))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(pickup_generation, encoded_deadline)| {
        Ok(TaskPickupClaim {
            dispatch_key: dispatch_key.to_owned(),
            pickup_generation,
            owner: owner.to_owned(),
            liveness_deadline: decode_timestamp(encoded_deadline).map_err(corrupt_value)?,
        })
    })
    .transpose()
}

async fn select_due_task_pickup_sqlite(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<Option<DueTaskPickup>, RepositoryError> {
    let row: Option<(String, i64, String, i64, String)> = sqlx::query_as(
        "SELECT d.dispatch_key, d.pickup_generation, d.owner, d.liveness_deadline, d.payload \
         FROM local_task_dispatches d \
         WHERE d.state = 'claimed' AND d.liveness_deadline <= ?1 \
           AND NOT EXISTS (SELECT 1 FROM local_task_terminal_outcomes o \
               WHERE o.dispatch_key = d.dispatch_key \
                 AND o.pickup_generation = d.pickup_generation) \
         ORDER BY d.liveness_deadline, d.dispatch_key LIMIT 1",
    )
    .bind(encode_timestamp(now))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(
        |(dispatch_key, pickup_generation, owner, encoded_deadline, payload)| {
            Ok(DueTaskPickup {
                claim: TaskPickupClaim {
                    dispatch_key,
                    pickup_generation,
                    owner,
                    liveness_deadline: decode_timestamp(encoded_deadline).map_err(corrupt_value)?,
                },
                payload: decode_bytes(&payload)?,
            })
        },
    )
    .transpose()
}

async fn elect_task_pickup_terminal_sqlite(
    pool: &SqlitePool,
    claim: &TaskPickupClaim,
    outcome: TaskPickupTerminalOutcome,
    terminal_event: &[u8],
    now: DateTime<Utc>,
) -> Result<TaskPickupTerminalElection, RepositoryError> {
    let event_kind = outcome.event_kind().ok_or_else(|| {
        configuration_error(TaskPickupRepositoryError::CorruptStoredValue(
            "cancellation outcome requires the cancellation settlement path".to_owned(),
        ))
    })?;
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(now);
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO local_task_terminal_outcomes \
             (dispatch_key, pickup_generation, outcome, settled_at) \
         SELECT dispatch_key, pickup_generation, ?4, ?5 \
         FROM local_task_dispatches \
         WHERE dispatch_key = ?1 AND state = 'claimed' AND pickup_generation = ?2 AND owner = ?3",
    )
    .bind(&claim.dispatch_key)
    .bind(claim.pickup_generation)
    .bind(&claim.owner)
    .bind(outcome.as_str())
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    if inserted.rows_affected() == 1 {
        sqlx::query(
            "INSERT INTO local_task_event_outbox \
                 (dispatch_key, pickup_generation, kind, event, staged_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&claim.dispatch_key)
        .bind(claim.pickup_generation)
        .bind(event_kind)
        .bind(encode_bytes(terminal_event))
        .bind(encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(TaskPickupTerminalElection::Won);
    }

    let settled: Option<String> = sqlx::query_scalar(
        "SELECT outcome FROM local_task_terminal_outcomes \
         WHERE dispatch_key = ?1 AND pickup_generation = ?2",
    )
    .bind(&claim.dispatch_key)
    .bind(claim.pickup_generation)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    settled
        .map(|value| parse_terminal_outcome(&value).map(TaskPickupTerminalElection::Settled))
        .transpose()
        .map(|settled| settled.unwrap_or(TaskPickupTerminalElection::NotClaimed))
}

async fn select_pending_task_event_sqlite(
    pool: &SqlitePool,
) -> Result<Option<PendingTaskEvent>, RepositoryError> {
    let row: Option<(String, i64, String, String)> = sqlx::query_as(
        "SELECT dispatch_key, pickup_generation, kind, event \
         FROM local_task_event_outbox WHERE forwarded_at IS NULL \
         ORDER BY staged_at, dispatch_key, pickup_generation, \
           CASE kind WHEN 'Assigned' THEN 0 WHEN 'Started' THEN 1 ELSE 2 END \
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(dispatch_key, pickup_generation, kind, event)| {
        Ok(PendingTaskEvent {
            dispatch_key,
            pickup_generation,
            kind,
            event: decode_bytes(&event)?,
        })
    })
    .transpose()
}

async fn mark_task_event_forwarded_sqlite(
    pool: &SqlitePool,
    event: &PendingTaskEvent,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE local_task_event_outbox SET forwarded_at = ?5 \
         WHERE dispatch_key = ?1 AND pickup_generation = ?2 AND kind = ?3 \
           AND event = ?4 AND forwarded_at IS NULL",
    )
    .bind(&event.dispatch_key)
    .bind(event.pickup_generation)
    .bind(&event.kind)
    .bind(encode_bytes(&event.event))
    .bind(encode_timestamp(now))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(updated.rows_affected() == 1)
}

async fn commit_task_cancellation_fence_sqlite(
    pool: &SqlitePool,
    acknowledgement_identity: &str,
    task_instance_id: &str,
    workflow_instance_id: &str,
    now: DateTime<Utc>,
) -> Result<TaskCancellationFence, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let pickup: Option<(String, i64, String, Option<String>)> = sqlx::query_as(
        "SELECT dispatch_key, pickup_generation, state, owner \
         FROM local_task_dispatches \
         WHERE task_instance_id = ?1 AND workflow_instance_id = ?2",
    )
    .bind(task_instance_id)
    .bind(workflow_instance_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let (dispatch_key, pickup_generation, owner) = match pickup {
        Some((dispatch_key, generation, state, owner)) if state == "pending" => (
            Some(dispatch_key),
            Some(generation.checked_add(1).ok_or_else(|| {
                RepositoryError::new(
                    RepositoryErrorKind::Internal,
                    TaskPickupRepositoryError::GenerationOverflow(task_instance_id.to_owned()),
                )
            })?),
            None,
        ),
        Some((dispatch_key, generation, _, owner)) => (Some(dispatch_key), Some(generation), owner),
        None => (None, None, None),
    };
    sqlx::query(
        "INSERT OR IGNORE INTO local_task_cancellation_fences \
             (acknowledgement_identity, task_instance_id, workflow_instance_id, dispatch_key, \
              pickup_generation, owner, committed_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(acknowledgement_identity)
    .bind(task_instance_id)
    .bind(workflow_instance_id)
    .bind(dispatch_key)
    .bind(pickup_generation)
    .bind(owner)
    .bind(encode_timestamp(now))
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    load_task_cancellation_fence_sqlite(
        pool,
        "f.acknowledgement_identity = ?1",
        acknowledgement_identity,
    )
    .await?
    .ok_or_else(|| {
        corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(
            "committed cancellation fence could not be reloaded".to_owned(),
        ))
    })
}

async fn mark_task_cancellation_owner_notified_sqlite(
    pool: &SqlitePool,
    fence: &TaskCancellationFence,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE local_task_cancellation_fences SET owner_notified_at = COALESCE(owner_notified_at, ?2) \
         WHERE acknowledgement_identity = ?1 AND owner IS NOT NULL AND settled_at IS NULL",
    )
    .bind(&fence.acknowledgement_identity)
    .bind(encode_timestamp(now))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(updated.rows_affected() == 1)
}

async fn settle_task_cancellation_sqlite(
    pool: &SqlitePool,
    fence: &TaskCancellationFence,
    reconciliation: TaskCancellationReconciliation,
    acknowledgement: &[u8],
    now: DateTime<Utc>,
) -> Result<Option<TaskPickupTerminalElection>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(now);
    let inserted_terminal = if let (Some(dispatch_key), Some(pickup_generation)) =
        (&fence.dispatch_key, fence.pickup_generation)
    {
        sqlx::query(
            "INSERT OR IGNORE INTO local_task_terminal_outcomes \
                 (dispatch_key, pickup_generation, outcome, settled_at) \
             SELECT f.dispatch_key, f.pickup_generation, ?2, ?3 \
             FROM local_task_cancellation_fences f \
             JOIN local_task_dispatches d ON d.dispatch_key = f.dispatch_key \
             WHERE f.acknowledgement_identity = ?1 \
               AND ((d.state = 'claimed' AND d.pickup_generation = f.pickup_generation \
                     AND d.owner = f.owner) \
                 OR (d.state = 'pending' AND d.pickup_generation + 1 = f.pickup_generation \
                     AND f.owner IS NULL))",
        )
        .bind(&fence.acknowledgement_identity)
        .bind(reconciliation.terminal_outcome().as_str())
        .bind(encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?
        .rows_affected()
            == 1
            && !dispatch_key.is_empty()
            && pickup_generation > 0
    } else {
        false
    };
    let settled = if let (Some(dispatch_key), Some(pickup_generation)) =
        (&fence.dispatch_key, fence.pickup_generation)
    {
        sqlx::query_scalar::<_, String>(
            "SELECT outcome FROM local_task_terminal_outcomes \
             WHERE dispatch_key = ?1 AND pickup_generation = ?2",
        )
        .bind(dispatch_key)
        .bind(pickup_generation)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?
        .map(|value| parse_terminal_outcome(&value))
        .transpose()?
    } else {
        None
    };
    sqlx::query(
        "UPDATE local_task_cancellation_fences \
         SET reconciliation = COALESCE(reconciliation, ?2), \
             settled_at = COALESCE(settled_at, ?3) \
         WHERE acknowledgement_identity = ?1",
    )
    .bind(&fence.acknowledgement_identity)
    .bind(reconciliation.as_str())
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    sqlx::query(
        "INSERT OR IGNORE INTO local_task_cancellation_ack_outbox \
             (acknowledgement_identity, acknowledgement, staged_at) \
         VALUES (?1, ?2, ?3)",
    )
    .bind(&fence.acknowledgement_identity)
    .bind(encode_bytes(acknowledgement))
    .bind(encoded_now)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(settled.map(|outcome| {
        if inserted_terminal {
            TaskPickupTerminalElection::Won
        } else {
            TaskPickupTerminalElection::Settled(outcome)
        }
    }))
}

async fn select_unresolved_task_cancellation_sqlite(
    pool: &SqlitePool,
) -> Result<Option<TaskCancellationFence>, RepositoryError> {
    load_task_cancellation_fence_sqlite(pool, "f.settled_at IS NULL", "").await
}

async fn load_task_cancellation_fence_sqlite(
    pool: &SqlitePool,
    predicate: &str,
    identity: &str,
) -> Result<Option<TaskCancellationFence>, RepositoryError> {
    let statement = format!(
        "SELECT f.acknowledgement_identity, f.task_instance_id, f.workflow_instance_id, \
                f.dispatch_key, f.pickup_generation, f.owner, f.owner_notified_at, \
                d.liveness_deadline, \
                (SELECT outcome FROM local_task_terminal_outcomes o \
                 WHERE o.dispatch_key = f.dispatch_key \
                   AND o.pickup_generation = f.pickup_generation) AS terminal_outcome \
         FROM local_task_cancellation_fences f \
         LEFT JOIN local_task_dispatches d ON d.dispatch_key = f.dispatch_key \
         WHERE {predicate} ORDER BY f.committed_at, f.acknowledgement_identity LIMIT 1"
    );
    let row: Option<(
        String,
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    )> = sqlx::query_as(&statement)
        .bind(identity)
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)?;
    row.map(
        |(
            acknowledgement_identity,
            task_instance_id,
            workflow_instance_id,
            dispatch_key,
            pickup_generation,
            owner,
            owner_notified_at,
            liveness_deadline,
            terminal_outcome,
        )| {
            Ok(TaskCancellationFence {
                acknowledgement_identity,
                task_instance_id,
                workflow_instance_id,
                dispatch_key,
                pickup_generation,
                owner,
                owner_notified: owner_notified_at.is_some(),
                liveness_deadline: liveness_deadline
                    .map(decode_timestamp)
                    .transpose()
                    .map_err(corrupt_value)?,
                terminal_outcome: terminal_outcome
                    .map(|value| parse_terminal_outcome(&value))
                    .transpose()?,
            })
        },
    )
    .transpose()
}

async fn select_pending_task_cancellation_ack_sqlite(
    pool: &SqlitePool,
) -> Result<Option<PendingTaskCancellationAck>, RepositoryError> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT acknowledgement_identity, acknowledgement \
         FROM local_task_cancellation_ack_outbox WHERE forwarded_at IS NULL \
         ORDER BY staged_at, acknowledgement_identity LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(acknowledgement_identity, acknowledgement)| {
        Ok(PendingTaskCancellationAck {
            acknowledgement_identity,
            acknowledgement: decode_bytes(&acknowledgement)?,
        })
    })
    .transpose()
}

async fn mark_task_cancellation_ack_forwarded_sqlite(
    pool: &SqlitePool,
    acknowledgement: &PendingTaskCancellationAck,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE local_task_cancellation_ack_outbox SET forwarded_at = ?3 \
         WHERE acknowledgement_identity = ?1 AND acknowledgement = ?2 AND forwarded_at IS NULL",
    )
    .bind(&acknowledgement.acknowledgement_identity)
    .bind(encode_bytes(&acknowledgement.acknowledgement))
    .bind(encode_timestamp(now))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(updated.rows_affected() == 1)
}

async fn local_task_log_streams_for_workflow_instance_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Vec<LocalTaskLogStream>, RepositoryError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT task_instance_id, pickup_generation \
         FROM local_task_dispatches \
         WHERE workflow_instance_id = ?1 \
           AND task_instance_id IS NOT NULL \
           AND pickup_generation > 0 \
         ORDER BY task_instance_id",
    )
    .bind(workflow_instance_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(task_instance_id, pickup_generation)| {
            Ok(LocalTaskLogStream {
                task_instance_id: Uuid::parse_str(&task_instance_id).map_err(|_| {
                    corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(format!(
                        "local task dispatch has invalid task instance id `{task_instance_id}`"
                    )))
                })?,
                pickup_generation: u64::try_from(pickup_generation).map_err(|_| {
                    corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(format!(
                        "local task dispatch has invalid pickup generation `{pickup_generation}`"
                    )))
                })?,
            })
        })
        .collect()
}

async fn local_task_log_streams_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Option<Uuid>,
) -> Result<Vec<LocalTaskLogStream>, RepositoryError> {
    let rows: Vec<(String, i64)> = if let Some(workflow_instance_id) = workflow_instance_id {
        sqlx::query_as(
            "SELECT task_instance_id, pickup_generation \
             FROM local_task_dispatches \
             WHERE workflow_instance_id = ?1 \
               AND task_instance_id IS NOT NULL \
               AND pickup_generation > 0 \
             ORDER BY task_instance_id",
        )
        .bind(workflow_instance_id.to_string())
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query_as(
            "SELECT task_instance_id, pickup_generation \
             FROM local_task_dispatches \
             WHERE task_instance_id IS NOT NULL \
               AND pickup_generation > 0 \
             ORDER BY task_instance_id",
        )
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?
    };
    rows.into_iter()
        .map(|(task_instance_id, pickup_generation)| {
            Ok(LocalTaskLogStream {
                task_instance_id: Uuid::parse_str(&task_instance_id).map_err(|_| {
                    corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(format!(
                        "local task dispatch has invalid task instance id `{task_instance_id}`"
                    )))
                })?,
                pickup_generation: u64::try_from(pickup_generation).map_err(|_| {
                    corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(format!(
                        "local task dispatch has invalid pickup generation `{pickup_generation}`"
                    )))
                })?,
            })
        })
        .collect()
}

async fn task_pickup_snapshot_sqlite(
    pool: &SqlitePool,
    dispatch_key: &str,
) -> Result<Option<TaskPickupSnapshot>, RepositoryError> {
    let row = sqlx::query(
        "SELECT state, pickup_generation, owner, liveness_deadline, liveness_armed_at, rejection_reason \
         FROM local_task_dispatches WHERE dispatch_key = ?1",
    )
    .bind(dispatch_key)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT kind FROM local_task_event_outbox WHERE dispatch_key = ?1 \
         ORDER BY pickup_generation, \
           CASE kind WHEN 'Assigned' THEN 0 WHEN 'Started' THEN 1 ELSE 2 END",
    )
    .bind(dispatch_key)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let terminal_outcome: Option<String> = sqlx::query_scalar(
        "SELECT outcome FROM local_task_terminal_outcomes \
         WHERE dispatch_key = ?1 AND pickup_generation = ?2",
    )
    .bind(dispatch_key)
    .bind(
        row.try_get::<i64, _>("pickup_generation")
            .map_err(repository_sqlx_error)?,
    )
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let forwarded_event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT kind FROM local_task_event_outbox \
         WHERE dispatch_key = ?1 AND forwarded_at IS NOT NULL \
         ORDER BY pickup_generation, \
           CASE kind WHEN 'Assigned' THEN 0 WHEN 'Started' THEN 1 ELSE 2 END",
    )
    .bind(dispatch_key)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let quarantine_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM local_task_dispatch_quarantine WHERE dispatch_key = ?1",
    )
    .bind(dispatch_key)
    .fetch_one(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let cancellation_fence =
        load_task_cancellation_fence_sqlite(pool, "f.dispatch_key = ?1", dispatch_key).await?;
    let cancellation_reconciliation: Option<String> = sqlx::query_scalar(
        "SELECT reconciliation FROM local_task_cancellation_fences \
         WHERE dispatch_key = ?1",
    )
    .bind(dispatch_key)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let cancellation_ack_forwarded: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM local_task_cancellation_ack_outbox a \
         JOIN local_task_cancellation_fences f \
           ON f.acknowledgement_identity = a.acknowledgement_identity \
         WHERE f.dispatch_key = ?1 AND a.forwarded_at IS NOT NULL",
    )
    .bind(dispatch_key)
    .fetch_one(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let deadline: Option<i64> = row
        .try_get("liveness_deadline")
        .map_err(repository_sqlx_error)?;
    let armed_at: Option<i64> = row
        .try_get("liveness_armed_at")
        .map_err(repository_sqlx_error)?;
    Ok(Some(TaskPickupSnapshot {
        state: row.try_get("state").map_err(repository_sqlx_error)?,
        pickup_generation: row
            .try_get("pickup_generation")
            .map_err(repository_sqlx_error)?,
        owner: row.try_get("owner").map_err(repository_sqlx_error)?,
        liveness_deadline: deadline
            .map(decode_timestamp)
            .transpose()
            .map_err(corrupt_value)?,
        liveness_armed_at: armed_at
            .map(decode_timestamp)
            .transpose()
            .map_err(corrupt_value)?,
        rejection_reason: row
            .try_get("rejection_reason")
            .map_err(repository_sqlx_error)?,
        staged_event_kinds: event_kinds,
        quarantined: quarantine_count == 1,
        terminal_outcome: terminal_outcome
            .map(|value| parse_terminal_outcome(&value))
            .transpose()?,
        forwarded_event_kinds,
        cancellation_fence,
        cancellation_reconciliation: cancellation_reconciliation
            .map(|value| parse_cancellation_reconciliation(&value))
            .transpose()?,
        cancellation_ack_forwarded: cancellation_ack_forwarded == 1,
    }))
}

fn validate_dispatch_key(dispatch_key: &str) -> Result<(), RepositoryError> {
    if dispatch_key.is_empty() || dispatch_key.len() > MAX_DISPATCH_KEY_BYTES {
        return Err(configuration_error(
            TaskPickupRepositoryError::InvalidDispatchKey,
        ));
    }
    Ok(())
}

fn validate_owner(owner: &str) -> Result<(), RepositoryError> {
    if owner.is_empty() || owner.len() > MAX_OWNER_BYTES {
        return Err(configuration_error(TaskPickupRepositoryError::InvalidOwner));
    }
    Ok(())
}

fn validate_optional_task_identity(
    task_instance_id: Option<&str>,
    workflow_instance_id: Option<&str>,
) -> Result<(), RepositoryError> {
    match (task_instance_id, workflow_instance_id) {
        (None, None) => Ok(()),
        (Some(task_instance_id), Some(workflow_instance_id)) => {
            validate_cancellation_identity(task_instance_id)?;
            validate_cancellation_identity(workflow_instance_id)
        }
        _ => Err(configuration_error(
            TaskPickupRepositoryError::InvalidCancellationIdentity,
        )),
    }
}

fn validate_cancellation_identity(identity: &str) -> Result<(), RepositoryError> {
    if identity.is_empty() || identity.len() > MAX_OWNER_BYTES {
        return Err(configuration_error(
            TaskPickupRepositoryError::InvalidCancellationIdentity,
        ));
    }
    Ok(())
}

fn validate_claim_input(input: ClaimTaskPickupInput<'_>) -> Result<(), RepositoryError> {
    validate_dispatch_key(input.dispatch_key)?;
    validate_owner(input.owner)?;
    if input.liveness_deadline <= input.now {
        return Err(configuration_error(
            TaskPickupRepositoryError::InvalidDeadline,
        ));
    }
    Ok(())
}

fn validate_claim_identity(claim: &TaskPickupClaim) -> Result<(), RepositoryError> {
    validate_dispatch_key(&claim.dispatch_key)?;
    validate_owner(&claim.owner)?;
    if claim.pickup_generation <= 0 {
        return Err(configuration_error(
            TaskPickupRepositoryError::InvalidDeadline,
        ));
    }
    Ok(())
}

fn parse_terminal_outcome(value: &str) -> Result<TaskPickupTerminalOutcome, RepositoryError> {
    match value {
        "process-exited-success" => Ok(TaskPickupTerminalOutcome::ProcessExitedSuccess),
        "process-exited-failure" => Ok(TaskPickupTerminalOutcome::ProcessExitedFailure),
        "process-setup-failed" => Ok(TaskPickupTerminalOutcome::ProcessSetupFailed),
        "liveness-expired" => Ok(TaskPickupTerminalOutcome::LivenessExpired),
        "cancellation-killed" => Ok(TaskPickupTerminalOutcome::CancellationKilled),
        "cancellation-already-exited" => Ok(TaskPickupTerminalOutcome::CancellationAlreadyExited),
        "cancellation-no-process" => Ok(TaskPickupTerminalOutcome::CancellationNoProcess),
        other => Err(corrupt_value(
            TaskPickupRepositoryError::CorruptStoredValue(other.to_owned()),
        )),
    }
}

fn parse_cancellation_reconciliation(
    value: &str,
) -> Result<TaskCancellationReconciliation, RepositoryError> {
    match value {
        "killed" => Ok(TaskCancellationReconciliation::Killed),
        "already-exited" => Ok(TaskCancellationReconciliation::AlreadyExited),
        "no-process" => Ok(TaskCancellationReconciliation::NoProcess),
        other => Err(corrupt_value(
            TaskPickupRepositoryError::CorruptStoredValue(other.to_owned()),
        )),
    }
}

fn validate_claim(
    claim: &TaskPickupClaim,
    deadline: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    validate_dispatch_key(&claim.dispatch_key)?;
    validate_owner(&claim.owner)?;
    if claim.pickup_generation <= 0 || deadline <= now {
        return Err(configuration_error(
            TaskPickupRepositoryError::InvalidDeadline,
        ));
    }
    Ok(())
}

fn encode_bytes(bytes: &[u8]) -> String {
    encode_json(&Value::Array(
        bytes.iter().map(|byte| Value::from(*byte)).collect(),
    ))
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, RepositoryError> {
    serde_json::from_str(value).map_err(|error| {
        corrupt_value(TaskPickupRepositoryError::CorruptStoredValue(
            error.to_string(),
        ))
    })
}

fn configuration_error(error: TaskPickupRepositoryError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, error)
}

fn corrupt_value(error: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::CorruptStoredValue, error)
}
