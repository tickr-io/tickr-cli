//! Definition lifecycle persistence and read projections.
//!
//! Registration is one repository operation: identity/version resolution,
//! retained source, per-task build rows, and routing specifications commit in
//! one transaction. Read projections expose only domain values and use explicit
//! version ordering on both SQL implementations.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Sqlite, SqlitePool, Transaction};
use tickr_proto::workflow as wf;
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{decode_json, decode_timestamp, encode_json, encode_timestamp, encode_uuid};

const BUILD_STATUSES: &[&str] = &["Building", "Ready", "BuildFailed", "Submitted"];
pub const MAX_DEFINITION_BUILD_LEASE_BATCH: usize = 64;
pub const MAX_DEFINITION_SUBMISSION_LEASE_BATCH: usize = 64;

#[derive(Debug, Clone)]
pub struct DefinitionRegistrationInput {
    pub definition: wf::WorkflowDefinition,
    pub content_hash: String,
    pub cosmetic_hash: String,
    pub nickel_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionBuildTask {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
    pub task_id: Uuid,
    pub nix_expression_path: String,
}

#[derive(Debug, Clone, Copy)]
pub struct DefinitionBuildLeaseRequest<'a> {
    pub owner: &'a str,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedDefinitionBuildTask {
    pub task: DefinitionBuildTask,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionRegistrationOutcome {
    Inserted {
        workflow_id: Uuid,
        workflow_version: i64,
        tasks: Vec<DefinitionBuildTask>,
    },
    Refreshed {
        workflow_id: Uuid,
        workflow_version: i64,
    },
    BuildRequeued {
        workflow_id: Uuid,
        workflow_version: i64,
        tasks: Vec<DefinitionBuildTask>,
    },
    NoOp {
        workflow_id: Uuid,
        workflow_version: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionLifecycleStatus {
    Building,
    Ready,
    BuildFailed,
    Submitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionTaskBuildResult<'a> {
    Success,
    Failure { error: &'a str },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DefinitionSubmissionIntent {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
    pub definition: wf::WorkflowDefinition,
}

#[derive(Debug, Clone, Copy)]
pub struct DefinitionSubmissionLeaseRequest<'a> {
    pub owner: &'a str,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeasedDefinitionSubmission {
    pub intent: DefinitionSubmissionIntent,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DefinitionSubmissionPointer {
    pub workflow_id: Uuid,
    pub workflow_version: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DefinitionBuildSettlementOutcome {
    AwaitingTasks,
    Ready(DefinitionSubmissionIntent),
    BuildFailed,
    AlreadySettled(DefinitionLifecycleStatus),
    TaskAlreadySettled,
    Absent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LeasedDefinitionBuildSettlementOutcome {
    Settled(DefinitionBuildSettlementOutcome),
    LeaseLost,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DefinitionSubmissionCandidate {
    Ready(DefinitionSubmissionIntent),
    NotReady(DefinitionLifecycleStatus),
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionSubmissionReconciliationOutcome {
    Ready,
    NotReady(DefinitionLifecycleStatus),
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefinitionSubmissionSettlementOutcome {
    Submitted,
    AlreadySettled(DefinitionLifecycleStatus),
    Absent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LeasedDefinitionSubmissionSettlementOutcome {
    Settled(DefinitionSubmissionSettlementOutcome),
    LeaseLost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DefinitionListRow {
    pub workflow: wf::WorkflowDefinition,
    pub build_status: String,
    pub build_version: i64,
    pub live_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionVersionRow {
    pub version: i64,
    pub status: String,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DefinitionVersionDetail {
    pub status: String,
    pub definition: Value,
    pub nickel_source: String,
}

#[derive(Debug)]
struct LatestRow {
    version: i64,
    content_hash: String,
    cosmetic_hash: String,
    status: String,
}

#[derive(Debug, Clone, Copy)]
struct DefinitionBuildLeaseGuard<'a> {
    owner: &'a str,
    token: Uuid,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct DefinitionSubmissionLeaseGuard<'a> {
    owner: &'a str,
    token: Uuid,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationDecision {
    Insert(i64),
    Refreshed(i64),
    BuildRequeued(i64),
    NoOp(i64),
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored definition value: {0}")]
struct CorruptStoredValue(String);

#[derive(Debug, thiserror::Error)]
enum DefinitionBuildLeaseError {
    #[error("definition build lease owner must contain 1 to 128 bytes")]
    InvalidOwner,
    #[error("definition build lease expiry must be later than acquisition time")]
    InvalidExpiry,
    #[error(
        "definition build lease batch must contain 1 to {MAX_DEFINITION_BUILD_LEASE_BATCH} rows"
    )]
    InvalidLimit,
    #[error("unguarded definition build settlement reported lease loss")]
    UnexpectedLeaseLoss,
}

#[derive(Debug, thiserror::Error)]
enum DefinitionSubmissionLeaseError {
    #[error("definition submission lease owner must contain 1 to 128 bytes")]
    InvalidOwner,
    #[error("definition submission lease expiry must be later than acquisition time")]
    InvalidExpiry,
    #[error(
        "definition submission lease batch must contain 1 to {MAX_DEFINITION_SUBMISSION_LEASE_BATCH} rows"
    )]
    InvalidLimit,
}

impl WriterRepositoryBundle {
    pub async fn register_definition(
        &self,
        input: DefinitionRegistrationInput,
    ) -> Result<DefinitionRegistrationOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => register_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => register_sqlite(pool, input).await,
        }
    }

    /// Lease committed pending definition builds in durable stable order.
    pub async fn lease_definition_build_tasks(
        &self,
        request: DefinitionBuildLeaseRequest<'_>,
    ) -> Result<Vec<LeasedDefinitionBuildTask>, RepositoryError> {
        validate_definition_build_lease_request(request)?;
        match &self.pool {
            BackendPool::Postgres(pool) => {
                lease_definition_build_tasks_postgres(pool, request).await
            }
            BackendPool::Sqlite(pool) => lease_definition_build_tasks_sqlite(pool, request).await,
        }
    }

    /// Discover reclaimable definition-build work without acquiring a lease.
    pub async fn has_reclaimable_definition_build(
        &self,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                has_reclaimable_definition_build_postgres(pool, now).await
            }
            BackendPool::Sqlite(pool) => has_reclaimable_definition_build_sqlite(pool, now).await,
        }
    }

    pub async fn task_specification(
        &self,
        task_id: Uuid,
    ) -> Result<Option<Vec<wf::RoutingVarDecl>>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => task_spec_postgres(pool, task_id).await,
            BackendPool::Sqlite(pool) => task_spec_sqlite(pool, task_id).await,
        }
    }

    /// Read the highest live definition for one Workflow.
    pub async fn live_workflow_definition(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => live_definition_postgres(pool, workflow_id).await,
            BackendPool::Sqlite(pool) => live_definition_sqlite(pool, workflow_id).await,
        }
    }

    /// Read the highest stored definition version, regardless of lifecycle.
    pub async fn latest_workflow_definition(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => latest_definition_postgres(pool, workflow_id).await,
            BackendPool::Sqlite(pool) => latest_definition_sqlite(pool, workflow_id).await,
        }
    }

    /// Read the highest live definition for every Workflow in stable identity order.
    pub async fn live_workflow_definitions(
        &self,
    ) -> Result<Vec<wf::WorkflowDefinition>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => live_definitions_postgres(pool).await,
            BackendPool::Sqlite(pool) => live_definitions_sqlite(pool).await,
        }
    }

    pub async fn settle_definition_task_build(
        &self,
        workflow_id: Uuid,
        workflow_version: i64,
        task_id: Uuid,
        result: DefinitionTaskBuildResult<'_>,
    ) -> Result<DefinitionBuildSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                match settle_task_build_postgres(
                    pool,
                    workflow_id,
                    workflow_version,
                    task_id,
                    result,
                    None,
                )
                .await?
                {
                    LeasedDefinitionBuildSettlementOutcome::Settled(outcome) => Ok(outcome),
                    LeasedDefinitionBuildSettlementOutcome::LeaseLost => Err(RepositoryError::new(
                        RepositoryErrorKind::Internal,
                        DefinitionBuildLeaseError::UnexpectedLeaseLoss,
                    )),
                }
            }
            BackendPool::Sqlite(pool) => {
                match settle_task_build_sqlite(
                    pool,
                    workflow_id,
                    workflow_version,
                    task_id,
                    result,
                    None,
                )
                .await?
                {
                    LeasedDefinitionBuildSettlementOutcome::Settled(outcome) => Ok(outcome),
                    LeasedDefinitionBuildSettlementOutcome::LeaseLost => Err(RepositoryError::new(
                        RepositoryErrorKind::Internal,
                        DefinitionBuildLeaseError::UnexpectedLeaseLoss,
                    )),
                }
            }
        }
    }

    /// Settle one build only while the caller still owns its unexpired lease.
    pub async fn settle_leased_definition_task_build(
        &self,
        lease: &LeasedDefinitionBuildTask,
        result: DefinitionTaskBuildResult<'_>,
        now: DateTime<Utc>,
    ) -> Result<LeasedDefinitionBuildSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                settle_task_build_postgres(
                    pool,
                    lease.task.workflow_id,
                    lease.task.workflow_version,
                    lease.task.task_id,
                    result,
                    Some(DefinitionBuildLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    }),
                )
                .await
            }
            BackendPool::Sqlite(pool) => {
                settle_task_build_sqlite(
                    pool,
                    lease.task.workflow_id,
                    lease.task.workflow_version,
                    lease.task.task_id,
                    result,
                    Some(DefinitionBuildLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    }),
                )
                .await
            }
        }
    }

    pub async fn definition_submission_reconciliation_candidates(
        &self,
    ) -> Result<Vec<DefinitionSubmissionPointer>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                submission_reconciliation_candidates_postgres(pool).await
            }
            BackendPool::Sqlite(pool) => submission_reconciliation_candidates_sqlite(pool).await,
        }
    }

    pub async fn definition_submission_reconciliation_outcome(
        &self,
        pointer: DefinitionSubmissionPointer,
    ) -> Result<DefinitionSubmissionReconciliationOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                submission_reconciliation_outcome_postgres(pool, pointer).await
            }
            BackendPool::Sqlite(pool) => {
                submission_reconciliation_outcome_sqlite(pool, pointer).await
            }
        }
    }

    pub async fn definition_submission_candidate(
        &self,
        workflow_id: Uuid,
        workflow_version: i64,
    ) -> Result<DefinitionSubmissionCandidate, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                submission_candidate_postgres(pool, workflow_id, workflow_version).await
            }
            BackendPool::Sqlite(pool) => {
                submission_candidate_sqlite(pool, workflow_id, workflow_version).await
            }
        }
    }

    pub async fn settle_definition_submission(
        &self,
        workflow_id: Uuid,
        workflow_version: i64,
    ) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                settle_submission_postgres(pool, workflow_id, workflow_version).await
            }
            BackendPool::Sqlite(pool) => {
                settle_submission_sqlite(pool, workflow_id, workflow_version).await
            }
        }
    }

    /// Lease committed `Ready` definitions in stable identity order.
    pub async fn lease_definition_submissions(
        &self,
        request: DefinitionSubmissionLeaseRequest<'_>,
    ) -> Result<Vec<LeasedDefinitionSubmission>, RepositoryError> {
        validate_definition_submission_lease_request(request)?;
        match &self.pool {
            BackendPool::Postgres(pool) => {
                lease_definition_submissions_postgres(pool, request).await
            }
            BackendPool::Sqlite(pool) => lease_definition_submissions_sqlite(pool, request).await,
        }
    }

    /// Discover reclaimable definition-submission work without acquiring a lease.
    pub async fn has_reclaimable_definition_submission(
        &self,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                has_reclaimable_definition_submission_postgres(pool, now).await
            }
            BackendPool::Sqlite(pool) => {
                has_reclaimable_definition_submission_sqlite(pool, now).await
            }
        }
    }

    /// Settle one submission only while the caller still owns its unexpired lease.
    pub async fn settle_leased_definition_submission(
        &self,
        lease: &LeasedDefinitionSubmission,
        now: DateTime<Utc>,
    ) -> Result<LeasedDefinitionSubmissionSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                settle_leased_submission_postgres(
                    pool,
                    lease.intent.workflow_id,
                    lease.intent.workflow_version,
                    DefinitionSubmissionLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    },
                )
                .await
            }
            BackendPool::Sqlite(pool) => {
                settle_leased_submission_sqlite(
                    pool,
                    lease.intent.workflow_id,
                    lease.intent.workflow_version,
                    DefinitionSubmissionLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    },
                )
                .await
            }
        }
    }
}

impl ReadOnlyRepositoryBundle {
    pub async fn list_definitions(&self) -> Result<Vec<DefinitionListRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => list_definitions_postgres(pool).await,
            BackendPool::Sqlite(pool) => list_definitions_sqlite(pool).await,
        }
    }

    pub async fn count_definitions(&self) -> Result<i64, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query_scalar("SELECT count(DISTINCT id) FROM workflows")
                    .fetch_one(pool)
                    .await
                    .map_err(repository_sqlx_error)
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query_scalar("SELECT count(DISTINCT id) FROM workflows")
                    .fetch_one(pool)
                    .await
                    .map_err(repository_sqlx_error)
            }
        }
    }

    pub async fn default_definition_version(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<(i64, String)>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => default_version_postgres(pool, workflow_id).await,
            BackendPool::Sqlite(pool) => default_version_sqlite(pool, workflow_id).await,
        }
    }

    pub async fn definition_exists(&self, workflow_id: Uuid) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                let row: Option<i32> = sqlx::query_scalar(
                    "SELECT 1 FROM workflows WHERE id = $1 ORDER BY version DESC LIMIT 1",
                )
                .bind(workflow_id)
                .fetch_optional(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(row.is_some())
            }
            BackendPool::Sqlite(pool) => {
                let row: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM workflows WHERE id = ?1 ORDER BY version DESC LIMIT 1",
                )
                .bind(encode_uuid(workflow_id))
                .fetch_optional(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(row.is_some())
            }
        }
    }

    pub async fn list_definition_versions(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<DefinitionVersionRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => list_versions_postgres(pool, workflow_id).await,
            BackendPool::Sqlite(pool) => list_versions_sqlite(pool, workflow_id).await,
        }
    }

    pub async fn definition_version(
        &self,
        workflow_id: Uuid,
        version: i64,
    ) -> Result<Option<DefinitionVersionDetail>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => get_version_postgres(pool, workflow_id, version).await,
            BackendPool::Sqlite(pool) => get_version_sqlite(pool, workflow_id, version).await,
        }
    }
}

fn decide(input: &DefinitionRegistrationInput, latest: Option<&LatestRow>) -> RegistrationDecision {
    let Some(latest) = latest else {
        return RegistrationDecision::Insert(1);
    };
    if latest.content_hash != input.content_hash {
        return RegistrationDecision::Insert(latest.version + 1);
    }
    match latest.status.as_str() {
        "BuildFailed" => RegistrationDecision::BuildRequeued(latest.version),
        "Building" => RegistrationDecision::NoOp(latest.version),
        _ if latest.cosmetic_hash != input.cosmetic_hash => {
            RegistrationDecision::Refreshed(latest.version)
        }
        _ => RegistrationDecision::NoOp(latest.version),
    }
}

fn workflow_id(definition: &wf::WorkflowDefinition) -> Result<Uuid, RepositoryError> {
    Uuid::parse_str(&definition.id).map_err(corrupt_input)
}

fn task_ids(definition: &wf::WorkflowDefinition) -> Result<Vec<Uuid>, RepositoryError> {
    definition
        .tasks
        .iter()
        .map(|task| Uuid::parse_str(&task.id).map_err(corrupt_input))
        .collect()
}

fn build_tasks(
    definition: &wf::WorkflowDefinition,
    id: Uuid,
    version: i64,
    ids: &[Uuid],
) -> Vec<DefinitionBuildTask> {
    definition
        .tasks
        .iter()
        .zip(ids)
        .map(|(task, task_id)| DefinitionBuildTask {
            workflow_id: id,
            workflow_version: version,
            task_id: *task_id,
            nix_expression_path: task.nix_expression_path.clone(),
        })
        .collect()
}

fn validate_definition_build_lease_request(
    request: DefinitionBuildLeaseRequest<'_>,
) -> Result<(), RepositoryError> {
    if request.owner.is_empty() || request.owner.len() > 128 {
        return Err(definition_build_lease_error(
            DefinitionBuildLeaseError::InvalidOwner,
        ));
    }
    if request.expires_at <= request.now {
        return Err(definition_build_lease_error(
            DefinitionBuildLeaseError::InvalidExpiry,
        ));
    }
    if !(1..=MAX_DEFINITION_BUILD_LEASE_BATCH).contains(&request.limit) {
        return Err(definition_build_lease_error(
            DefinitionBuildLeaseError::InvalidLimit,
        ));
    }
    Ok(())
}

fn definition_build_lease_error(error: DefinitionBuildLeaseError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, error)
}

fn validate_definition_submission_lease_request(
    request: DefinitionSubmissionLeaseRequest<'_>,
) -> Result<(), RepositoryError> {
    if request.owner.is_empty() || request.owner.len() > 128 {
        return Err(definition_submission_lease_error(
            DefinitionSubmissionLeaseError::InvalidOwner,
        ));
    }
    if request.expires_at <= request.now {
        return Err(definition_submission_lease_error(
            DefinitionSubmissionLeaseError::InvalidExpiry,
        ));
    }
    if !(1..=MAX_DEFINITION_SUBMISSION_LEASE_BATCH).contains(&request.limit) {
        return Err(definition_submission_lease_error(
            DefinitionSubmissionLeaseError::InvalidLimit,
        ));
    }
    Ok(())
}

fn definition_submission_lease_error(error: DefinitionSubmissionLeaseError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, error)
}

async fn has_reclaimable_definition_build_postgres(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let candidate: Option<i32> = sqlx::query_scalar(
        "SELECT 1 \
         FROM workflow_task_builds b \
         JOIN workflows w ON w.id = b.workflow_id AND w.version = b.workflow_version \
         WHERE b.status = 'pending' AND w.status = 'Building' \
           AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= $1) \
         ORDER BY b.pending_since, b.workflow_id, b.workflow_version, b.task_id \
         LIMIT 1",
    )
    .bind(now)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(candidate.is_some())
}

async fn has_reclaimable_definition_build_sqlite(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let candidate: Option<i64> = sqlx::query_scalar(
        "SELECT 1 \
         FROM workflow_task_builds b \
         JOIN workflows w ON w.id = b.workflow_id AND w.version = b.workflow_version \
         WHERE b.status = 'pending' AND w.status = 'Building' \
           AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= ?1) \
         ORDER BY b.pending_since, b.workflow_id, b.workflow_version, b.task_id \
         LIMIT 1",
    )
    .bind(encode_timestamp(now))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(candidate.is_some())
}

async fn lease_definition_build_tasks_postgres(
    pool: &PgPool,
    request: DefinitionBuildLeaseRequest<'_>,
) -> Result<Vec<LeasedDefinitionBuildTask>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let rows: Vec<(Uuid, i64, Uuid, Value)> = sqlx::query_as(
        "SELECT b.workflow_id, b.workflow_version, b.task_id, w.definition \
         FROM workflow_task_builds b \
         JOIN workflows w ON w.id = b.workflow_id AND w.version = b.workflow_version \
         WHERE b.status = 'pending' AND w.status = 'Building' \
           AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= $1) \
         ORDER BY b.pending_since, b.workflow_id, b.workflow_version, b.task_id \
         LIMIT $2 FOR UPDATE OF b SKIP LOCKED",
    )
    .bind(request.now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let mut leases = Vec::with_capacity(rows.len());
    for (workflow_id, workflow_version, task_id, definition) in rows {
        let task = requeued_tasks(definition, workflow_id, workflow_version, vec![task_id])?
            .pop()
            .ok_or_else(|| {
                corrupt_value(CorruptStoredValue(format!(
                    "missing leased definition build task {task_id}"
                )))
            })?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflow_task_builds \
             SET lease_owner = $4, lease_token = $5, lease_expires_at = $6 \
             WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3 \
               AND status = 'pending' \
               AND (lease_expires_at IS NULL OR lease_expires_at <= $7)",
        )
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(task_id)
        .bind(request.owner)
        .bind(lease_token)
        .bind(request.expires_at)
        .bind(request.now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Internal,
                CorruptStoredValue(format!(
                    "selected definition build task {task_id} was not leaseable"
                )),
            ));
        }
        leases.push(LeasedDefinitionBuildTask {
            task,
            lease_owner: request.owner.to_owned(),
            lease_token,
            lease_expires_at: request.expires_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(leases)
}

async fn lease_definition_build_tasks_sqlite(
    pool: &SqlitePool,
    request: DefinitionBuildLeaseRequest<'_>,
) -> Result<Vec<LeasedDefinitionBuildTask>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(request.now);
    let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
        "SELECT b.workflow_id, b.workflow_version, b.task_id, w.definition \
         FROM workflow_task_builds b \
         JOIN workflows w ON w.id = b.workflow_id AND w.version = b.workflow_version \
         WHERE b.status = 'pending' AND w.status = 'Building' \
           AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= ?1) \
         ORDER BY b.pending_since, b.workflow_id, b.workflow_version, b.task_id \
         LIMIT ?2",
    )
    .bind(&encoded_now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let encoded_expiry = encode_timestamp(request.expires_at);
    let mut leases = Vec::with_capacity(rows.len());
    for (encoded_workflow_id, workflow_version, encoded_task_id, definition) in rows {
        let workflow_id =
            crate::encoding::decode_uuid(&encoded_workflow_id).map_err(corrupt_value)?;
        let task_id = crate::encoding::decode_uuid(&encoded_task_id).map_err(corrupt_value)?;
        let task = requeued_tasks(
            decode_json(&definition).map_err(corrupt_value)?,
            workflow_id,
            workflow_version,
            vec![task_id],
        )?
        .pop()
        .ok_or_else(|| {
            corrupt_value(CorruptStoredValue(format!(
                "missing leased definition build task {task_id}"
            )))
        })?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflow_task_builds \
             SET lease_owner = ?4, lease_token = ?5, lease_expires_at = ?6 \
             WHERE workflow_id = ?1 AND workflow_version = ?2 AND task_id = ?3 \
               AND status = 'pending' \
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?7)",
        )
        .bind(&encoded_workflow_id)
        .bind(workflow_version)
        .bind(&encoded_task_id)
        .bind(request.owner)
        .bind(encode_uuid(lease_token))
        .bind(&encoded_expiry)
        .bind(&encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Internal,
                CorruptStoredValue(format!(
                    "selected definition build task {task_id} was not leaseable"
                )),
            ));
        }
        leases.push(LeasedDefinitionBuildTask {
            task,
            lease_owner: request.owner.to_owned(),
            lease_token,
            lease_expires_at: request.expires_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(leases)
}

async fn settle_task_build_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
    task_id: Uuid,
    result: DefinitionTaskBuildResult<'_>,
    lease_guard: Option<DefinitionBuildLeaseGuard<'_>>,
) -> Result<LeasedDefinitionBuildSettlementOutcome, RepositoryError> {
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM workflows WHERE id = $1 AND version = $2 FOR UPDATE",
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .fetch_optional(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::Absent,
        ));
    };
    let status = lifecycle_status(&status)?;
    if status != DefinitionLifecycleStatus::Building {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::AlreadySettled(status),
        ));
    }

    let settled_at = lease_guard.map_or_else(Utc::now, |guard| guard.now);
    let (task_status, error) = task_build_values(result);
    let updated = if let Some(guard) = lease_guard {
        sqlx::query(
            "UPDATE workflow_task_builds \
             SET status = $4, error = $5, built_at = $6, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3 \
               AND status = 'pending' AND lease_owner = $7 AND lease_token = $8 \
               AND lease_expires_at > $9",
        )
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(task_id)
        .bind(task_status)
        .bind(error)
        .bind(settled_at)
        .bind(guard.owner)
        .bind(guard.token)
        .bind(guard.now)
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query(
            "UPDATE workflow_task_builds \
             SET status = $4, error = $5, built_at = $6, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3 \
               AND status = 'pending' AND lease_token IS NULL",
        )
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(task_id)
        .bind(task_status)
        .bind(error)
        .bind(settled_at)
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    };
    if updated.rows_affected() == 0 {
        let stored_status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM workflow_task_builds \
             WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3",
        )
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(match stored_status.as_deref() {
            Some("pending") if lease_guard.is_some() => {
                LeasedDefinitionBuildSettlementOutcome::LeaseLost
            }
            Some(_) => LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::TaskAlreadySettled,
            ),
            None => LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::Absent,
            ),
        });
    }

    match result {
        DefinitionTaskBuildResult::Failure { .. } => {
            sqlx::query(
                "UPDATE workflows SET status = 'BuildFailed', updated_at = $3 \
                 WHERE id = $1 AND version = $2 AND status = 'Building'",
            )
            .bind(workflow_id)
            .bind(workflow_version)
            .bind(settled_at)
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::BuildFailed,
            ))
        }
        DefinitionTaskBuildResult::Success => {
            let definition: Option<Value> = sqlx::query_scalar(
                "UPDATE workflows w SET status = 'Ready', updated_at = $3 \
                 WHERE w.id = $1 AND w.version = $2 AND w.status = 'Building' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM workflow_task_builds b \
                       WHERE b.workflow_id = w.id AND b.workflow_version = w.version \
                         AND b.status <> 'success' \
                   ) \
                 RETURNING w.definition",
            )
            .bind(workflow_id)
            .bind(workflow_version)
            .bind(settled_at)
            .fetch_optional(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
            let intent = definition
                .map(|definition| submission_intent(workflow_id, workflow_version, definition))
                .transpose()?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                match intent {
                    Some(intent) => DefinitionBuildSettlementOutcome::Ready(intent),
                    None => DefinitionBuildSettlementOutcome::AwaitingTasks,
                },
            ))
        }
    }
}

async fn settle_task_build_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    workflow_version: i64,
    task_id: Uuid,
    result: DefinitionTaskBuildResult<'_>,
    lease_guard: Option<DefinitionBuildLeaseGuard<'_>>,
) -> Result<LeasedDefinitionBuildSettlementOutcome, RepositoryError> {
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_workflow_id = encode_uuid(workflow_id);
    let encoded_task_id = encode_uuid(task_id);
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(&encoded_workflow_id)
            .bind(workflow_version)
            .fetch_optional(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::Absent,
        ));
    };
    let status = lifecycle_status(&status)?;
    if status != DefinitionLifecycleStatus::Building {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::AlreadySettled(status),
        ));
    }

    let settled_at = lease_guard.map_or_else(Utc::now, |guard| guard.now);
    let (task_status, error) = task_build_values(result);
    let updated = if let Some(guard) = lease_guard {
        sqlx::query(
            "UPDATE workflow_task_builds \
             SET status = ?4, error = ?5, built_at = ?6, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE workflow_id = ?1 AND workflow_version = ?2 AND task_id = ?3 \
               AND status = 'pending' AND lease_owner = ?7 AND lease_token = ?8 \
               AND lease_expires_at > ?9",
        )
        .bind(&encoded_workflow_id)
        .bind(workflow_version)
        .bind(&encoded_task_id)
        .bind(task_status)
        .bind(error)
        .bind(encode_timestamp(settled_at))
        .bind(guard.owner)
        .bind(encode_uuid(guard.token))
        .bind(encode_timestamp(guard.now))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query(
            "UPDATE workflow_task_builds \
             SET status = ?4, error = ?5, built_at = ?6, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE workflow_id = ?1 AND workflow_version = ?2 AND task_id = ?3 \
               AND status = 'pending' AND lease_token IS NULL",
        )
        .bind(&encoded_workflow_id)
        .bind(workflow_version)
        .bind(&encoded_task_id)
        .bind(task_status)
        .bind(error)
        .bind(encode_timestamp(settled_at))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    };
    if updated.rows_affected() == 0 {
        let stored_status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM workflow_task_builds \
             WHERE workflow_id = ?1 AND workflow_version = ?2 AND task_id = ?3",
        )
        .bind(&encoded_workflow_id)
        .bind(workflow_version)
        .bind(&encoded_task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(match stored_status.as_deref() {
            Some("pending") if lease_guard.is_some() => {
                LeasedDefinitionBuildSettlementOutcome::LeaseLost
            }
            Some(_) => LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::TaskAlreadySettled,
            ),
            None => LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::Absent,
            ),
        });
    }

    match result {
        DefinitionTaskBuildResult::Failure { .. } => {
            sqlx::query(
                "UPDATE workflows SET status = 'BuildFailed', updated_at = ?3 \
                 WHERE id = ?1 AND version = ?2 AND status = 'Building'",
            )
            .bind(&encoded_workflow_id)
            .bind(workflow_version)
            .bind(encode_timestamp(settled_at))
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::BuildFailed,
            ))
        }
        DefinitionTaskBuildResult::Success => {
            let definition: Option<String> = sqlx::query_scalar(
                "UPDATE workflows SET status = 'Ready', updated_at = ?3 \
                 WHERE id = ?1 AND version = ?2 AND status = 'Building' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM workflow_task_builds \
                       WHERE workflow_id = ?1 AND workflow_version = ?2 \
                         AND status <> 'success' \
                   ) \
                 RETURNING definition",
            )
            .bind(&encoded_workflow_id)
            .bind(workflow_version)
            .bind(encode_timestamp(settled_at))
            .fetch_optional(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
            let intent = definition
                .map(|definition| {
                    decode_json(&definition)
                        .map_err(corrupt_value)
                        .and_then(|definition| {
                            submission_intent(workflow_id, workflow_version, definition)
                        })
                })
                .transpose()?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                match intent {
                    Some(intent) => DefinitionBuildSettlementOutcome::Ready(intent),
                    None => DefinitionBuildSettlementOutcome::AwaitingTasks,
                },
            ))
        }
    }
}
async fn has_reclaimable_definition_submission_postgres(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let candidate: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM workflows \
         WHERE status = 'Ready' \
           AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= $1) \
         ORDER BY id, version LIMIT 1",
    )
    .bind(now)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(candidate.is_some())
}

async fn has_reclaimable_definition_submission_sqlite(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let candidate: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM workflows \
         WHERE status = 'Ready' \
           AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= ?1) \
         ORDER BY id, version LIMIT 1",
    )
    .bind(encode_timestamp(now))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(candidate.is_some())
}

async fn lease_definition_submissions_postgres(
    pool: &PgPool,
    request: DefinitionSubmissionLeaseRequest<'_>,
) -> Result<Vec<LeasedDefinitionSubmission>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let rows: Vec<(Uuid, i64, Value)> = sqlx::query_as(
        "SELECT id, version, definition FROM workflows \
         WHERE status = 'Ready' \
           AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= $1) \
         ORDER BY id, version \
         LIMIT $2 FOR UPDATE SKIP LOCKED",
    )
    .bind(request.now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let mut leases = Vec::with_capacity(rows.len());
    for (workflow_id, workflow_version, definition) in rows {
        let intent = submission_intent(workflow_id, workflow_version, definition)?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflows \
             SET submission_lease_owner = $3, submission_lease_token = $4, \
                 submission_lease_expires_at = $5 \
             WHERE id = $1 AND version = $2 AND status = 'Ready' \
               AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= $6)",
        )
        .bind(workflow_id)
        .bind(workflow_version)
        .bind(request.owner)
        .bind(lease_token)
        .bind(request.expires_at)
        .bind(request.now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Internal,
                CorruptStoredValue(format!(
                    "selected definition submission ({workflow_id}, {workflow_version}) was not leaseable"
                )),
            ));
        }
        leases.push(LeasedDefinitionSubmission {
            intent,
            lease_owner: request.owner.to_owned(),
            lease_token,
            lease_expires_at: request.expires_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(leases)
}

async fn settle_leased_submission_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
    guard: DefinitionSubmissionLeaseGuard<'_>,
) -> Result<LeasedDefinitionSubmissionSettlementOutcome, RepositoryError> {
    let won: Option<String> = sqlx::query_scalar(
        "UPDATE workflows \
         SET status = 'Submitted', updated_at = $3, \
             submission_lease_owner = NULL, submission_lease_token = NULL, \
             submission_lease_expires_at = NULL \
         WHERE id = $1 AND version = $2 AND status = 'Ready' \
           AND submission_lease_owner = $4 AND submission_lease_token = $5 \
           AND submission_lease_expires_at > $3 \
         RETURNING status",
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .bind(guard.now)
    .bind(guard.owner)
    .bind(guard.token)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    if won.is_some() {
        return Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Submitted,
        ));
    }

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        return Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Absent,
        ));
    };
    let status = lifecycle_status(&status)?;
    if status == DefinitionLifecycleStatus::Ready {
        Ok(LeasedDefinitionSubmissionSettlementOutcome::LeaseLost)
    } else {
        Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::AlreadySettled(status),
        ))
    }
}

async fn lease_definition_submissions_sqlite(
    pool: &SqlitePool,
    request: DefinitionSubmissionLeaseRequest<'_>,
) -> Result<Vec<LeasedDefinitionSubmission>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(request.now);
    let rows: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT id, version, definition FROM workflows \
         WHERE status = 'Ready' \
           AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= ?1) \
         ORDER BY id, version \
         LIMIT ?2",
    )
    .bind(&encoded_now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let encoded_expiry = encode_timestamp(request.expires_at);
    let mut leases = Vec::with_capacity(rows.len());
    for (encoded_workflow_id, workflow_version, definition) in rows {
        let workflow_id =
            crate::encoding::decode_uuid(&encoded_workflow_id).map_err(corrupt_value)?;
        let intent = submission_intent(
            workflow_id,
            workflow_version,
            decode_json(&definition).map_err(corrupt_value)?,
        )?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflows \
             SET submission_lease_owner = ?3, submission_lease_token = ?4, \
                 submission_lease_expires_at = ?5 \
             WHERE id = ?1 AND version = ?2 AND status = 'Ready' \
               AND (submission_lease_expires_at IS NULL OR submission_lease_expires_at <= ?6)",
        )
        .bind(&encoded_workflow_id)
        .bind(workflow_version)
        .bind(request.owner)
        .bind(encode_uuid(lease_token))
        .bind(&encoded_expiry)
        .bind(&encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Internal,
                CorruptStoredValue(format!(
                    "selected definition submission ({workflow_id}, {workflow_version}) was not leaseable"
                )),
            ));
        }
        leases.push(LeasedDefinitionSubmission {
            intent,
            lease_owner: request.owner.to_owned(),
            lease_token,
            lease_expires_at: request.expires_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(leases)
}

async fn settle_leased_submission_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    workflow_version: i64,
    guard: DefinitionSubmissionLeaseGuard<'_>,
) -> Result<LeasedDefinitionSubmissionSettlementOutcome, RepositoryError> {
    let encoded_workflow_id = encode_uuid(workflow_id);
    let encoded_now = encode_timestamp(guard.now);
    let won: Option<String> = sqlx::query_scalar(
        "UPDATE workflows \
         SET status = 'Submitted', updated_at = ?3, \
             submission_lease_owner = NULL, submission_lease_token = NULL, \
             submission_lease_expires_at = NULL \
         WHERE id = ?1 AND version = ?2 AND status = 'Ready' \
           AND submission_lease_owner = ?4 AND submission_lease_token = ?5 \
           AND submission_lease_expires_at > ?3 \
         RETURNING status",
    )
    .bind(&encoded_workflow_id)
    .bind(workflow_version)
    .bind(&encoded_now)
    .bind(guard.owner)
    .bind(encode_uuid(guard.token))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    if won.is_some() {
        return Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Submitted,
        ));
    }

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(encoded_workflow_id)
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        return Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Absent,
        ));
    };
    let status = lifecycle_status(&status)?;
    if status == DefinitionLifecycleStatus::Ready {
        Ok(LeasedDefinitionSubmissionSettlementOutcome::LeaseLost)
    } else {
        Ok(LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::AlreadySettled(status),
        ))
    }
}

async fn submission_reconciliation_candidates_postgres(
    pool: &PgPool,
) -> Result<Vec<DefinitionSubmissionPointer>, RepositoryError> {
    let rows: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT id, version FROM workflows WHERE status = 'Ready' ORDER BY id, version",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(
            |(workflow_id, workflow_version)| DefinitionSubmissionPointer {
                workflow_id,
                workflow_version,
            },
        )
        .collect())
}

async fn submission_reconciliation_candidates_sqlite(
    pool: &SqlitePool,
) -> Result<Vec<DefinitionSubmissionPointer>, RepositoryError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT id, version FROM workflows WHERE status = 'Ready' ORDER BY id, version",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(workflow_id, workflow_version)| {
            Ok(DefinitionSubmissionPointer {
                workflow_id: crate::encoding::decode_uuid(&workflow_id).map_err(corrupt_value)?,
                workflow_version,
            })
        })
        .collect()
}

async fn submission_reconciliation_outcome_postgres(
    pool: &PgPool,
    pointer: DefinitionSubmissionPointer,
) -> Result<DefinitionSubmissionReconciliationOutcome, RepositoryError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(pointer.workflow_id)
            .bind(pointer.workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    submission_reconciliation_outcome(status)
}

async fn submission_reconciliation_outcome_sqlite(
    pool: &SqlitePool,
    pointer: DefinitionSubmissionPointer,
) -> Result<DefinitionSubmissionReconciliationOutcome, RepositoryError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(encode_uuid(pointer.workflow_id))
            .bind(pointer.workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    submission_reconciliation_outcome(status)
}

fn submission_reconciliation_outcome(
    status: Option<String>,
) -> Result<DefinitionSubmissionReconciliationOutcome, RepositoryError> {
    let Some(status) = status else {
        return Ok(DefinitionSubmissionReconciliationOutcome::Absent);
    };
    let status = lifecycle_status(&status)?;
    Ok(if status == DefinitionLifecycleStatus::Ready {
        DefinitionSubmissionReconciliationOutcome::Ready
    } else {
        DefinitionSubmissionReconciliationOutcome::NotReady(status)
    })
}

async fn submission_candidate_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionCandidate, RepositoryError> {
    let row: Option<(String, Value)> =
        sqlx::query_as("SELECT status, definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    submission_candidate(workflow_id, workflow_version, row)
}

async fn submission_candidate_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionCandidate, RepositoryError> {
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT status, definition FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(encode_uuid(workflow_id))
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    let row = row
        .map(|(status, definition)| {
            decode_json(&definition)
                .map(|definition| (status, definition))
                .map_err(corrupt_value)
        })
        .transpose()?;
    submission_candidate(workflow_id, workflow_version, row)
}

async fn settle_submission_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
    let won: Option<String> = sqlx::query_scalar(
        "UPDATE workflows SET status = 'Submitted', updated_at = now(), \
             submission_lease_owner = NULL, submission_lease_token = NULL, \
             submission_lease_expires_at = NULL \
         WHERE id = $1 AND version = $2 AND status = 'Ready' RETURNING status",
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    if won.is_some() {
        return Ok(DefinitionSubmissionSettlementOutcome::Submitted);
    }
    submission_losing_status_postgres(pool, workflow_id, workflow_version).await
}

async fn settle_submission_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
    let won: Option<String> = sqlx::query_scalar(
        "UPDATE workflows SET status = 'Submitted', updated_at = ?3, \
             submission_lease_owner = NULL, submission_lease_token = NULL, \
             submission_lease_expires_at = NULL \
         WHERE id = ?1 AND version = ?2 AND status = 'Ready' RETURNING status",
    )
    .bind(encode_uuid(workflow_id))
    .bind(workflow_version)
    .bind(encode_timestamp(Utc::now()))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    if won.is_some() {
        return Ok(DefinitionSubmissionSettlementOutcome::Submitted);
    }
    submission_losing_status_sqlite(pool, workflow_id, workflow_version).await
}

async fn submission_losing_status_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    losing_submission_outcome(status)
}

async fn submission_losing_status_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(encode_uuid(workflow_id))
            .bind(workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    losing_submission_outcome(status)
}

fn task_build_values(result: DefinitionTaskBuildResult<'_>) -> (&'static str, Option<&str>) {
    match result {
        DefinitionTaskBuildResult::Success => ("success", None),
        DefinitionTaskBuildResult::Failure { error } => ("failure", Some(error)),
    }
}

fn lifecycle_status(status: &str) -> Result<DefinitionLifecycleStatus, RepositoryError> {
    match status {
        "Building" => Ok(DefinitionLifecycleStatus::Building),
        "Ready" => Ok(DefinitionLifecycleStatus::Ready),
        "BuildFailed" => Ok(DefinitionLifecycleStatus::BuildFailed),
        "Submitted" => Ok(DefinitionLifecycleStatus::Submitted),
        other => Err(corrupt_value(CorruptStoredValue(format!(
            "unknown definition lifecycle status `{other}`"
        )))),
    }
}

fn submission_intent(
    workflow_id: Uuid,
    workflow_version: i64,
    definition: Value,
) -> Result<DefinitionSubmissionIntent, RepositoryError> {
    Ok(DefinitionSubmissionIntent {
        workflow_id,
        workflow_version,
        definition: definition_from_json(definition)?,
    })
}

fn submission_candidate(
    workflow_id: Uuid,
    workflow_version: i64,
    row: Option<(String, Value)>,
) -> Result<DefinitionSubmissionCandidate, RepositoryError> {
    let Some((status, definition)) = row else {
        return Ok(DefinitionSubmissionCandidate::Absent);
    };
    let status = lifecycle_status(&status)?;
    if status == DefinitionLifecycleStatus::Ready {
        return submission_intent(workflow_id, workflow_version, definition)
            .map(DefinitionSubmissionCandidate::Ready);
    }
    Ok(DefinitionSubmissionCandidate::NotReady(status))
}

fn losing_submission_outcome(
    status: Option<String>,
) -> Result<DefinitionSubmissionSettlementOutcome, RepositoryError> {
    status
        .map(|status| {
            lifecycle_status(&status).map(DefinitionSubmissionSettlementOutcome::AlreadySettled)
        })
        .transpose()
        .map(|status| status.unwrap_or(DefinitionSubmissionSettlementOutcome::Absent))
}

async fn register_postgres(
    pool: &PgPool,
    mut input: DefinitionRegistrationInput,
) -> Result<DefinitionRegistrationOutcome, RepositoryError> {
    let id = workflow_id(&input.definition)?;
    let ids = task_ids(&input.definition)?;
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;

    let latest: Option<(i64, String, String, String)> = sqlx::query_as(
        "SELECT version, content_hash, cosmetic_hash, status FROM workflows \
         WHERE id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    let latest = latest.map(|(version, content_hash, cosmetic_hash, status)| LatestRow {
        version,
        content_hash,
        cosmetic_hash,
        status,
    });
    if let Some(row) = &latest {
        validate_status(&row.status)?;
    }

    match decide(&input, latest.as_ref()) {
        RegistrationDecision::NoOp(version) => {
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::NoOp {
                workflow_id: id,
                workflow_version: version,
            })
        }
        RegistrationDecision::Refreshed(version) => {
            refresh_postgres(&mut tx, id, version, &input).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::Refreshed {
                workflow_id: id,
                workflow_version: version,
            })
        }
        RegistrationDecision::BuildRequeued(version) => {
            let tasks = requeue_postgres(&mut tx, id, version).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::BuildRequeued {
                workflow_id: id,
                workflow_version: version,
                tasks,
            })
        }
        RegistrationDecision::Insert(version) => {
            input.definition.version = version;
            let definition = definition_to_json(&input.definition)?;
            let tasks = build_tasks(&input.definition, id, version, &ids);
            sqlx::query(
                "INSERT INTO workflows \
                 (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source) \
                 VALUES ($1, $2, $3, $4, $5, 'Building', $6, $7, $8, $9)",
            )
            .bind(id)
            .bind(version)
            .bind(&input.definition.namespace)
            .bind(&input.definition.slug)
            .bind(&input.definition.name)
            .bind(&input.content_hash)
            .bind(&input.cosmetic_hash)
            .bind(&definition)
            .bind(&input.nickel_source)
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;

            let pending_since = Utc::now();
            for (task, task_id) in input.definition.tasks.iter().zip(ids) {
                sqlx::query(
                    "INSERT INTO workflow_task_builds \
                     (workflow_id, workflow_version, task_id, status, pending_since) \
                     VALUES ($1, $2, $3, 'pending', $4)",
                )
                .bind(id)
                .bind(version)
                .bind(task_id)
                .bind(pending_since)
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?;
                let routing_vars =
                    serde_json::to_value(&task.routing_vars).map_err(corrupt_input)?;
                sqlx::query(
                    "INSERT INTO task_specs (task_id, routing_vars) VALUES ($1, $2) \
                     ON CONFLICT (task_id) DO NOTHING",
                )
                .bind(task_id)
                .bind(routing_vars)
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?;
            }
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::Inserted {
                workflow_id: id,
                workflow_version: version,
                tasks,
            })
        }
    }
}

async fn refresh_postgres(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    version: i64,
    input: &DefinitionRegistrationInput,
) -> Result<(), RepositoryError> {
    let mut definition: Value =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(version)
            .fetch_one(&mut **tx)
            .await
            .map_err(repository_sqlx_error)?;
    patch_cosmetics(&mut definition, &definition_to_json(&input.definition)?);
    sqlx::query(
        "UPDATE workflows SET name = $3, cosmetic_hash = $4, definition = $5, \
         nickel_source = $6, updated_at = CURRENT_TIMESTAMP WHERE id = $1 AND version = $2",
    )
    .bind(id)
    .bind(version)
    .bind(&input.definition.name)
    .bind(&input.cosmetic_hash)
    .bind(definition)
    .bind(&input.nickel_source)
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(())
}

async fn requeue_postgres(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    version: i64,
) -> Result<Vec<DefinitionBuildTask>, RepositoryError> {
    sqlx::query(
        "UPDATE workflows SET status = 'Building', updated_at = CURRENT_TIMESTAMP \
         WHERE id = $1 AND version = $2 AND status = 'BuildFailed'",
    )
    .bind(id)
    .bind(version)
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    sqlx::query(
        "UPDATE workflow_task_builds \
         SET lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE workflow_id = $1 AND workflow_version = $2 AND status = 'pending'",
    )
    .bind(id)
    .bind(version)
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    let reset: Vec<Uuid> = sqlx::query_scalar(
        "UPDATE workflow_task_builds \
         SET status = 'pending', error = NULL, built_at = NULL, \
             lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE workflow_id = $1 AND workflow_version = $2 AND status = 'failure' RETURNING task_id",
    )
    .bind(id)
    .bind(version)
    .fetch_all(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    let definition: Value =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(version)
            .fetch_one(&mut **tx)
            .await
            .map_err(repository_sqlx_error)?;
    requeued_tasks(definition, id, version, reset)
}

async fn register_sqlite(
    pool: &SqlitePool,
    mut input: DefinitionRegistrationInput,
) -> Result<DefinitionRegistrationOutcome, RepositoryError> {
    let id = workflow_id(&input.definition)?;
    let stored_id = encode_uuid(id);
    let ids = task_ids(&input.definition)?;
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let latest: Option<(i64, String, String, String)> = sqlx::query_as(
        "SELECT version, content_hash, cosmetic_hash, status FROM workflows \
         WHERE id = ?1 ORDER BY version DESC LIMIT 1",
    )
    .bind(&stored_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    let latest = latest.map(|(version, content_hash, cosmetic_hash, status)| LatestRow {
        version,
        content_hash,
        cosmetic_hash,
        status,
    });
    if let Some(row) = &latest {
        validate_status(&row.status)?;
    }

    match decide(&input, latest.as_ref()) {
        RegistrationDecision::NoOp(version) => {
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::NoOp {
                workflow_id: id,
                workflow_version: version,
            })
        }
        RegistrationDecision::Refreshed(version) => {
            refresh_sqlite(&mut tx, id, version, &input).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::Refreshed {
                workflow_id: id,
                workflow_version: version,
            })
        }
        RegistrationDecision::BuildRequeued(version) => {
            let tasks = requeue_sqlite(&mut tx, id, version).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::BuildRequeued {
                workflow_id: id,
                workflow_version: version,
                tasks,
            })
        }
        RegistrationDecision::Insert(version) => {
            input.definition.version = version;
            let definition = definition_to_json(&input.definition)?;
            let tasks = build_tasks(&input.definition, id, version, &ids);
            sqlx::query(
                "INSERT INTO workflows \
                 (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 'Building', ?6, ?7, ?8, ?9)",
            )
            .bind(&stored_id)
            .bind(version)
            .bind(&input.definition.namespace)
            .bind(&input.definition.slug)
            .bind(&input.definition.name)
            .bind(&input.content_hash)
            .bind(&input.cosmetic_hash)
            .bind(encode_json(&definition))
            .bind(&input.nickel_source)
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;

            let pending_since = encode_timestamp(Utc::now());
            for (task, task_id) in input.definition.tasks.iter().zip(ids) {
                let stored_task_id = encode_uuid(task_id);
                sqlx::query(
                    "INSERT INTO workflow_task_builds \
                     (workflow_id, workflow_version, task_id, status, pending_since) \
                     VALUES (?1, ?2, ?3, 'pending', ?4)",
                )
                .bind(&stored_id)
                .bind(version)
                .bind(&stored_task_id)
                .bind(&pending_since)
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?;
                let routing_vars =
                    serde_json::to_value(&task.routing_vars).map_err(corrupt_input)?;
                sqlx::query(
                    "INSERT INTO task_specs (task_id, routing_vars) VALUES (?1, ?2) \
                     ON CONFLICT (task_id) DO NOTHING",
                )
                .bind(stored_task_id)
                .bind(encode_json(&routing_vars))
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?;
            }
            tx.commit().await.map_err(repository_sqlx_error)?;
            Ok(DefinitionRegistrationOutcome::Inserted {
                workflow_id: id,
                workflow_version: version,
                tasks,
            })
        }
    }
}

async fn refresh_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
    version: i64,
    input: &DefinitionRegistrationInput,
) -> Result<(), RepositoryError> {
    let stored_id = encode_uuid(id);
    let stored: String =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(&stored_id)
            .bind(version)
            .fetch_one(&mut **tx)
            .await
            .map_err(repository_sqlx_error)?;
    let mut definition = decode_json(&stored).map_err(corrupt_value)?;
    patch_cosmetics(&mut definition, &definition_to_json(&input.definition)?);
    sqlx::query(
        "UPDATE workflows SET name = ?3, cosmetic_hash = ?4, definition = ?5, \
         nickel_source = ?6, updated_at = ?7 WHERE id = ?1 AND version = ?2",
    )
    .bind(stored_id)
    .bind(version)
    .bind(&input.definition.name)
    .bind(&input.cosmetic_hash)
    .bind(encode_json(&definition))
    .bind(&input.nickel_source)
    .bind(encode_timestamp(Utc::now()))
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(())
}

async fn requeue_sqlite(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
    version: i64,
) -> Result<Vec<DefinitionBuildTask>, RepositoryError> {
    let stored_id = encode_uuid(id);
    sqlx::query(
        "UPDATE workflows SET status = 'Building', updated_at = ?3 \
         WHERE id = ?1 AND version = ?2 AND status = 'BuildFailed'",
    )
    .bind(&stored_id)
    .bind(version)
    .bind(encode_timestamp(Utc::now()))
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    sqlx::query(
        "UPDATE workflow_task_builds \
         SET lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE workflow_id = ?1 AND workflow_version = ?2 AND status = 'pending'",
    )
    .bind(&stored_id)
    .bind(version)
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    let reset: Vec<String> = sqlx::query_scalar(
        "UPDATE workflow_task_builds \
         SET status = 'pending', error = NULL, built_at = NULL, \
             lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE workflow_id = ?1 AND workflow_version = ?2 AND status = 'failure' RETURNING task_id",
    )
    .bind(&stored_id)
    .bind(version)
    .fetch_all(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    let definition: String =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(stored_id)
            .bind(version)
            .fetch_one(&mut **tx)
            .await
            .map_err(repository_sqlx_error)?;
    let reset = reset
        .into_iter()
        .map(|value| crate::encoding::decode_uuid(&value).map_err(corrupt_value))
        .collect::<Result<Vec<_>, _>>()?;
    requeued_tasks(
        decode_json(&definition).map_err(corrupt_value)?,
        id,
        version,
        reset,
    )
}

fn requeued_tasks(
    definition: Value,
    id: Uuid,
    version: i64,
    reset: Vec<Uuid>,
) -> Result<Vec<DefinitionBuildTask>, RepositoryError> {
    let definition = definition_from_json(definition)?;
    reset
        .into_iter()
        .map(|task_id| {
            let needle = task_id.to_string();
            let task = definition
                .tasks
                .iter()
                .find(|task| task.id == needle)
                .ok_or_else(|| {
                    corrupt_value(CorruptStoredValue(format!("missing task {task_id}")))
                })?;
            Ok(DefinitionBuildTask {
                workflow_id: id,
                workflow_version: version,
                task_id,
                nix_expression_path: task.nix_expression_path.clone(),
            })
        })
        .collect()
}

fn patch_cosmetics(target: &mut Value, source: &Value) {
    if let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) {
        for key in ["name", "tags"] {
            if let Some(value) = source.get(key) {
                target.insert(key.to_owned(), value.clone());
            }
        }
    }
}

async fn task_spec_postgres(
    pool: &PgPool,
    task_id: Uuid,
) -> Result<Option<Vec<wf::RoutingVarDecl>>, RepositoryError> {
    let value: Option<Value> =
        sqlx::query_scalar("SELECT routing_vars FROM task_specs WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    value.map(decode_routing_specs).transpose()
}

async fn task_spec_sqlite(
    pool: &SqlitePool,
    task_id: Uuid,
) -> Result<Option<Vec<wf::RoutingVarDecl>>, RepositoryError> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT routing_vars FROM task_specs WHERE task_id = ?1")
            .bind(encode_uuid(task_id))
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    value
        .map(|value| {
            decode_json(&value)
                .map_err(corrupt_value)
                .and_then(decode_routing_specs)
        })
        .transpose()
}

fn decode_routing_specs(value: Value) -> Result<Vec<wf::RoutingVarDecl>, RepositoryError> {
    serde_json::from_value(value).map_err(corrupt_value)
}

async fn live_definition_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
    let definition: Option<Value> = sqlx::query_scalar(
        "SELECT definition FROM workflows \
         WHERE id = $1 AND status IN ('Ready', 'Submitted') \
         ORDER BY version DESC LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definition.map(definition_from_json).transpose()
}

async fn live_definition_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
    let definition: Option<String> = sqlx::query_scalar(
        "SELECT definition FROM workflows \
         WHERE id = ?1 AND status IN ('Ready', 'Submitted') \
         ORDER BY version DESC LIMIT 1",
    )
    .bind(encode_uuid(workflow_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definition
        .map(|value| {
            decode_json(&value)
                .map_err(corrupt_value)
                .and_then(definition_from_json)
        })
        .transpose()
}

async fn latest_definition_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
    let definition: Option<Value> = sqlx::query_scalar(
        "SELECT definition FROM workflows WHERE id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definition.map(definition_from_json).transpose()
}

async fn latest_definition_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>, RepositoryError> {
    let definition: Option<String> = sqlx::query_scalar(
        "SELECT definition FROM workflows WHERE id = ?1 ORDER BY version DESC LIMIT 1",
    )
    .bind(encode_uuid(workflow_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definition
        .map(|value| {
            decode_json(&value)
                .map_err(corrupt_value)
                .and_then(definition_from_json)
        })
        .transpose()
}

async fn live_definitions_postgres(
    pool: &PgPool,
) -> Result<Vec<wf::WorkflowDefinition>, RepositoryError> {
    let definitions: Vec<Value> = sqlx::query_scalar(
        "SELECT DISTINCT ON (id) definition FROM workflows \
         WHERE status IN ('Ready', 'Submitted') ORDER BY id, version DESC",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definitions.into_iter().map(definition_from_json).collect()
}

async fn live_definitions_sqlite(
    pool: &SqlitePool,
) -> Result<Vec<wf::WorkflowDefinition>, RepositoryError> {
    let definitions: Vec<String> = sqlx::query_scalar(
        "SELECT definition FROM (\
             SELECT definition, id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) AS rank \
             FROM workflows WHERE status IN ('Ready', 'Submitted')\
         ) WHERE rank = 1 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    definitions
        .into_iter()
        .map(|value| {
            decode_json(&value)
                .map_err(corrupt_value)
                .and_then(definition_from_json)
        })
        .collect()
}

async fn list_definitions_postgres(
    pool: &PgPool,
) -> Result<Vec<DefinitionListRow>, RepositoryError> {
    let rows: Vec<(Value, String, i64, Option<i64>)> = sqlx::query_as(
        "WITH latest_overall AS (\
             SELECT DISTINCT ON (id) id, version, status, definition, inserted_at \
             FROM workflows ORDER BY id, version DESC\
         ), latest_live AS (\
             SELECT DISTINCT ON (id) id, version AS live_version \
             FROM workflows WHERE status IN ('Ready', 'Submitted') ORDER BY id, version DESC\
         ) \
         SELECT lo.definition, lo.status, lo.version, ll.live_version \
         FROM latest_overall lo LEFT JOIN latest_live ll ON ll.id = lo.id \
         ORDER BY lo.inserted_at DESC, lo.id ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(definition, status, build_version, live_version)| {
            validate_status(&status)?;
            Ok(DefinitionListRow {
                workflow: definition_from_json(definition)?,
                build_status: status,
                build_version,
                live_version,
            })
        })
        .collect()
}

async fn list_definitions_sqlite(
    pool: &SqlitePool,
) -> Result<Vec<DefinitionListRow>, RepositoryError> {
    let rows: Vec<(String, String, i64, Option<i64>)> = sqlx::query_as(
        "WITH ranked AS (\
             SELECT id, version, status, definition, inserted_at, \
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) AS rank \
             FROM workflows\
         ), live_ranked AS (\
             SELECT id, version, ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) AS rank \
             FROM workflows WHERE status IN ('Ready', 'Submitted')\
         ) \
         SELECT ranked.definition, ranked.status, ranked.version, live_ranked.version \
         FROM ranked LEFT JOIN live_ranked ON live_ranked.id = ranked.id AND live_ranked.rank = 1 \
         WHERE ranked.rank = 1 ORDER BY ranked.inserted_at DESC, ranked.id ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(definition, status, build_version, live_version)| {
            validate_status(&status)?;
            Ok(DefinitionListRow {
                workflow: definition_from_json(decode_json(&definition).map_err(corrupt_value)?)?,
                build_status: status,
                build_version,
                live_version,
            })
        })
        .collect()
}

async fn default_version_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Option<(i64, String)>, RepositoryError> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT version, status FROM workflows WHERE id = $1 \
         ORDER BY (status IN ('Ready', 'Submitted')) DESC, version DESC LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    validate_optional_status(row)
}

async fn default_version_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
) -> Result<Option<(i64, String)>, RepositoryError> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT version, status FROM workflows WHERE id = ?1 \
         ORDER BY (status IN ('Ready', 'Submitted')) DESC, version DESC LIMIT 1",
    )
    .bind(encode_uuid(workflow_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    validate_optional_status(row)
}

fn validate_optional_status(
    row: Option<(i64, String)>,
) -> Result<Option<(i64, String)>, RepositoryError> {
    if let Some((_, status)) = &row {
        validate_status(status)?;
    }
    Ok(row)
}

async fn list_versions_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Vec<DefinitionVersionRow>, RepositoryError> {
    let rows: Vec<(i64, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT version, status, inserted_at FROM workflows \
         WHERE id = $1 ORDER BY version DESC",
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(version, status, inserted_at)| {
            validate_status(&status)?;
            Ok(DefinitionVersionRow {
                version,
                status,
                inserted_at,
            })
        })
        .collect()
}

async fn list_versions_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
) -> Result<Vec<DefinitionVersionRow>, RepositoryError> {
    let rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT version, status, inserted_at FROM workflows \
         WHERE id = ?1 ORDER BY version DESC",
    )
    .bind(encode_uuid(workflow_id))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(version, status, inserted_at)| {
            validate_status(&status)?;
            Ok(DefinitionVersionRow {
                version,
                status,
                inserted_at: decode_timestamp(inserted_at).map_err(corrupt_value)?,
            })
        })
        .collect()
}

async fn get_version_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    version: i64,
) -> Result<Option<DefinitionVersionDetail>, RepositoryError> {
    let row: Option<(String, Value, String)> = sqlx::query_as(
        "SELECT status, definition, nickel_source FROM workflows \
         WHERE id = $1 AND version = $2",
    )
    .bind(workflow_id)
    .bind(version)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(status, definition, nickel_source)| {
        validate_status(&status)?;
        Ok(DefinitionVersionDetail {
            status,
            definition,
            nickel_source,
        })
    })
    .transpose()
}

async fn get_version_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    version: i64,
) -> Result<Option<DefinitionVersionDetail>, RepositoryError> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT status, definition, nickel_source FROM workflows \
         WHERE id = ?1 AND version = ?2",
    )
    .bind(encode_uuid(workflow_id))
    .bind(version)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(status, definition, nickel_source)| {
        validate_status(&status)?;
        Ok(DefinitionVersionDetail {
            status,
            definition: decode_json(&definition).map_err(corrupt_value)?,
            nickel_source,
        })
    })
    .transpose()
}

fn definition_to_json(definition: &wf::WorkflowDefinition) -> Result<Value, RepositoryError> {
    tickr_proto::codec::definition::definition_proto_to_json(definition)
        .map_err(|error| corrupt_input(CorruptStoredValue(error.to_string())))
}

fn definition_from_json(definition: Value) -> Result<wf::WorkflowDefinition, RepositoryError> {
    tickr_proto::codec::definition::definition_proto_from_json(definition)
        .map_err(|error| corrupt_value(CorruptStoredValue(error.to_string())))
}

fn validate_status(status: &str) -> Result<(), RepositoryError> {
    if BUILD_STATUSES.contains(&status) {
        Ok(())
    } else {
        Err(corrupt_value(CorruptStoredValue(format!(
            "unknown workflow build status `{status}`"
        ))))
    }
}

fn corrupt_input(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Internal, source)
}

fn corrupt_value(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::CorruptStoredValue, source)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
    use tickr_proto::config::DataPlaneSql;

    use super::*;
    use crate::backend::RepositoryFactory;
    use crate::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};

    fn task(workflow_id: Uuid, task_id: Uuid) -> wf::TaskDefinition {
        wf::TaskDefinition {
            id: task_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "route".to_string(),
            task_type: wf::TaskType::Regular as i32,
            nix_expression_path: "/nix/store/route".to_string(),
            max_attempts: 3,
            routing_vars: vec![wf::RoutingVarDecl {
                name: "decision".to_string(),
                var_type: Some("string".to_string()),
            }],
            ..Default::default()
        }
    }

    fn registration(
        workflow_id: Uuid,
        task_ids: &[Uuid],
        content_hash: &str,
        source: &str,
    ) -> DefinitionRegistrationInput {
        DefinitionRegistrationInput {
            definition: wf::WorkflowDefinition {
                id: workflow_id.to_string(),
                tenant_id: Uuid::new_v4().to_string(),
                namespace: "default".to_string(),
                slug: "repository-law".to_string(),
                name: "Repository law".to_string(),
                tasks: task_ids
                    .iter()
                    .copied()
                    .map(|task_id| task(workflow_id, task_id))
                    .collect(),
                ..Default::default()
            },
            content_hash: content_hash.to_string(),
            cosmetic_hash: "cosmetic-a".to_string(),
            nickel_source: source.to_string(),
        }
    }

    async fn set_status(
        writer: &WriterRepositoryBundle,
        workflow_id: Uuid,
        version: i64,
        status: &str,
    ) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query("UPDATE workflows SET status = $3 WHERE id = $1 AND version = $2")
                    .bind(workflow_id)
                    .bind(version)
                    .bind(status)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query("UPDATE workflows SET status = ?3 WHERE id = ?1 AND version = ?2")
                    .bind(encode_uuid(workflow_id))
                    .bind(version)
                    .bind(status)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn fail_task_build(
        writer: &WriterRepositoryBundle,
        workflow_id: Uuid,
        version: i64,
        task_id: Uuid,
    ) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE workflow_task_builds SET status = 'failure' \
                     WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3",
                )
                .bind(workflow_id)
                .bind(version)
                .bind(task_id)
                .execute(pool)
                .await
                .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE workflow_task_builds SET status = 'failure' \
                     WHERE workflow_id = ?1 AND workflow_version = ?2 AND task_id = ?3",
                )
                .bind(encode_uuid(workflow_id))
                .bind(version)
                .bind(encode_uuid(task_id))
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn definition_row_count(writer: &WriterRepositoryBundle, workflow_id: Uuid) -> i64 {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query_scalar("SELECT count(*) FROM workflows WHERE id = $1")
                    .bind(workflow_id)
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query_scalar("SELECT count(*) FROM workflows WHERE id = ?1")
                    .bind(encode_uuid(workflow_id))
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
        }
    }

    async fn task_build_row_count(writer: &WriterRepositoryBundle, workflow_id: Uuid) -> i64 {
        match &writer.pool {
            BackendPool::Postgres(pool) => sqlx::query_scalar(
                "SELECT count(*) FROM workflow_task_builds WHERE workflow_id = $1",
            )
            .bind(workflow_id)
            .fetch_one(pool)
            .await
            .unwrap(),
            BackendPool::Sqlite(pool) => sqlx::query_scalar(
                "SELECT count(*) FROM workflow_task_builds WHERE workflow_id = ?1",
            )
            .bind(encode_uuid(workflow_id))
            .fetch_one(pool)
            .await
            .unwrap(),
        }
    }

    async fn register_lifecycle_definition(
        writer: &WriterRepositoryBundle,
        task_count: usize,
    ) -> (Uuid, Vec<Uuid>) {
        let workflow_id = Uuid::new_v4();
        let task_ids = (0..task_count).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let outcome = writer
            .register_definition(registration(
                workflow_id,
                &task_ids,
                &format!("lifecycle-{workflow_id}"),
                "lifecycle-source",
            ))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            DefinitionRegistrationOutcome::Inserted {
                workflow_version: 1,
                ..
            }
        ));
        (workflow_id, task_ids)
    }

    async fn run_lifecycle_laws(writer: &WriterRepositoryBundle) {
        let (success_id, success_tasks) = register_lifecycle_definition(writer, 2).await;
        assert_eq!(
            writer
                .settle_definition_task_build(
                    success_id,
                    1,
                    success_tasks[0],
                    DefinitionTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::AwaitingTasks
        );
        assert_eq!(
            writer
                .definition_submission_candidate(success_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::NotReady(DefinitionLifecycleStatus::Building)
        );

        let ready = writer
            .settle_definition_task_build(
                success_id,
                1,
                success_tasks[1],
                DefinitionTaskBuildResult::Success,
            )
            .await
            .unwrap();
        let intent = match ready {
            DefinitionBuildSettlementOutcome::Ready(intent) => intent,
            other => panic!("last successful task did not win Ready: {other:?}"),
        };
        assert_eq!(intent.workflow_id, success_id);
        assert_eq!(intent.workflow_version, 1);

        // A queue publication failure occurs after the repository commit. The
        // Ready row is still a durable publication intent for redelivery.
        drop(intent);
        tokio::task::yield_now().await;
        assert!(matches!(
            writer
                .definition_submission_candidate(success_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::Ready(_)
        ));
        assert_eq!(
            writer
                .settle_definition_task_build(
                    success_id,
                    1,
                    success_tasks[1],
                    DefinitionTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::AlreadySettled(DefinitionLifecycleStatus::Ready)
        );

        let first = writer.settle_definition_submission(success_id, 1);
        let second = writer.settle_definition_submission(success_id, 1);
        let (first, second) = tokio::join!(first, second);
        let submission_outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            submission_outcomes
                .iter()
                .filter(|outcome| { **outcome == DefinitionSubmissionSettlementOutcome::Submitted })
                .count(),
            1
        );
        assert_eq!(
            submission_outcomes
                .iter()
                .filter(|outcome| {
                    **outcome
                        == DefinitionSubmissionSettlementOutcome::AlreadySettled(
                            DefinitionLifecycleStatus::Submitted,
                        )
                })
                .count(),
            1
        );
        assert_eq!(
            writer
                .definition_submission_candidate(success_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::NotReady(DefinitionLifecycleStatus::Submitted)
        );

        let (failed_id, failed_tasks) = register_lifecycle_definition(writer, 2).await;
        assert_eq!(
            writer
                .settle_definition_task_build(
                    failed_id,
                    1,
                    failed_tasks[0],
                    DefinitionTaskBuildResult::Failure {
                        error: "nix build failed",
                    },
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::BuildFailed
        );
        assert_eq!(
            writer
                .settle_definition_task_build(
                    failed_id,
                    1,
                    failed_tasks[1],
                    DefinitionTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::AlreadySettled(
                DefinitionLifecycleStatus::BuildFailed,
            )
        );
        assert_eq!(
            writer
                .definition_submission_candidate(failed_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::NotReady(DefinitionLifecycleStatus::BuildFailed)
        );

        const FINALIZERS: usize = 8;
        let (concurrent_id, concurrent_tasks) =
            register_lifecycle_definition(writer, FINALIZERS).await;
        let handles = concurrent_tasks
            .into_iter()
            .map(|task_id| {
                let writer = writer.clone();
                tokio::spawn(async move {
                    writer
                        .settle_definition_task_build(
                            concurrent_id,
                            1,
                            task_id,
                            DefinitionTaskBuildResult::Success,
                        )
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut ready = 0;
        let mut awaiting = 0;
        for handle in handles {
            match handle.await.unwrap() {
                DefinitionBuildSettlementOutcome::Ready(_) => ready += 1,
                DefinitionBuildSettlementOutcome::AwaitingTasks => awaiting += 1,
                other => panic!("unexpected concurrent finalizer outcome: {other:?}"),
            }
        }
        assert_eq!(ready, 1);
        assert_eq!(awaiting, FINALIZERS - 1);
        assert!(matches!(
            writer
                .definition_submission_candidate(concurrent_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::Ready(_)
        ));

        let candidates = writer
            .definition_submission_reconciliation_candidates()
            .await
            .unwrap();
        assert!(
            candidates.windows(2).all(|pair| pair[0] < pair[1]),
            "reconciliation candidates must have a stable id/version order"
        );
        assert!(candidates.contains(&DefinitionSubmissionPointer {
            workflow_id: concurrent_id,
            workflow_version: 1,
        }));
        assert!(!candidates.iter().any(|pointer| {
            pointer.workflow_id == success_id || pointer.workflow_id == failed_id
        }));
        assert_eq!(
            writer
                .definition_submission_candidate(Uuid::new_v4(), 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::Absent
        );

        assert_eq!(
            writer
                .settle_definition_submission(concurrent_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionSettlementOutcome::Submitted
        );
        assert!(!writer
            .definition_submission_reconciliation_candidates()
            .await
            .unwrap()
            .contains(&DefinitionSubmissionPointer {
                workflow_id: concurrent_id,
                workflow_version: 1,
            }));
    }

    async fn assert_classified_pool_contention(selection: &DataPlaneSql) {
        let error = match selection {
            DataPlaneSql::Postgres { url } => {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_millis(50))
                    .connect(url)
                    .await
                    .unwrap();
                let repository = WriterRepositoryBundle::from_postgres_pool(pool.clone());
                let held = pool.acquire().await.unwrap();
                let error = repository
                    .definition_submission_candidate(Uuid::new_v4(), 1)
                    .await
                    .unwrap_err();
                drop(held);
                pool.close().await;
                error
            }
            DataPlaneSql::Sqlite { url } => {
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_millis(50))
                    .connect_with(sqlite_writer_options(url, false).unwrap())
                    .await
                    .unwrap();
                let repository = WriterRepositoryBundle {
                    pool: BackendPool::Sqlite(pool.clone()),
                };
                let held = pool.acquire().await.unwrap();
                let error = repository
                    .definition_submission_candidate(Uuid::new_v4(), 1)
                    .await
                    .unwrap_err();
                drop(held);
                pool.close().await;
                error
            }
        };
        assert_eq!(error.kind(), RepositoryErrorKind::ContentionTimeout);
        if let DataPlaneSql::Sqlite { url } = selection {
            let lock_pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(sqlite_writer_options(url, false).unwrap())
                .await
                .unwrap();
            let repository_pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(sqlite_writer_options(url, false).unwrap())
                .await
                .unwrap();
            sqlx::query("PRAGMA busy_timeout = 50")
                .execute(&repository_pool)
                .await
                .unwrap();
            let repository = WriterRepositoryBundle {
                pool: BackendPool::Sqlite(repository_pool.clone()),
            };
            let mut locker = lock_pool.acquire().await.unwrap();
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *locker)
                .await
                .unwrap();
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            let error = repository
                .register_definition(registration(
                    workflow_id,
                    &[task_id],
                    "busy-lock",
                    "busy-lock",
                ))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), RepositoryErrorKind::ContentionTimeout);
            sqlx::query("ROLLBACK").execute(&mut *locker).await.unwrap();
            drop(locker);
            repository_pool.close().await;
            lock_pool.close().await;
        }
    }

    async fn run_laws(selection: DataPlaneSql) {
        let factory = RepositoryFactory::new(selection.clone());
        let writer = factory.open_writer().await.unwrap();
        let reader = factory.open_read_only().await.unwrap();

        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let first = registration(workflow_id, &[task_id], "content-a", "source-a");
        let inserted = writer.register_definition(first.clone()).await.unwrap();
        assert!(matches!(
            inserted,
            DefinitionRegistrationOutcome::Inserted {
                workflow_version: 1,
                ..
            }
        ));
        assert_eq!(
            writer.task_specification(task_id).await.unwrap(),
            Some(vec![wf::RoutingVarDecl {
                name: "decision".to_string(),
                var_type: Some("string".to_string()),
            }])
        );
        let detail = reader
            .definition_version(workflow_id, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.status, "Building");
        assert_eq!(detail.nickel_source, "source-a");

        let redelivered = writer.register_definition(first.clone()).await.unwrap();
        assert!(matches!(
            redelivered,
            DefinitionRegistrationOutcome::NoOp {
                workflow_version: 1,
                ..
            }
        ));
        assert_eq!(definition_row_count(&writer, workflow_id).await, 1);

        set_status(&writer, workflow_id, 1, "BuildFailed").await;
        fail_task_build(&writer, workflow_id, 1, task_id).await;
        let requeued = writer.register_definition(first.clone()).await.unwrap();
        assert!(matches!(
            requeued,
            DefinitionRegistrationOutcome::BuildRequeued {
                workflow_version: 1,
                ref tasks,
                ..
            } if tasks.len() == 1 && tasks[0].task_id == task_id
        ));

        set_status(&writer, workflow_id, 1, "Submitted").await;
        let second_task_id = Uuid::new_v4();
        let second = registration(workflow_id, &[second_task_id], "content-b", "source-b");
        let inserted = writer.register_definition(second.clone()).await.unwrap();
        assert!(matches!(
            inserted,
            DefinitionRegistrationOutcome::Inserted {
                workflow_version: 2,
                ..
            }
        ));
        let versions = reader.list_definition_versions(workflow_id).await.unwrap();
        assert_eq!(
            versions.iter().map(|row| row.version).collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(
            reader
                .default_definition_version(workflow_id)
                .await
                .unwrap(),
            Some((1, "Submitted".to_string()))
        );
        assert_eq!(
            writer
                .live_workflow_definition(workflow_id)
                .await
                .unwrap()
                .unwrap()
                .version,
            1,
            "a newer Building version must not replace the live definition"
        );
        assert_eq!(
            writer
                .latest_workflow_definition(workflow_id)
                .await
                .unwrap()
                .unwrap()
                .version,
            2
        );
        assert!(writer
            .live_workflow_definitions()
            .await
            .unwrap()
            .iter()
            .any(|definition| definition.id == workflow_id.to_string() && definition.version == 1));
        set_status(&writer, workflow_id, 2, "Ready").await;
        assert_eq!(
            reader
                .default_definition_version(workflow_id)
                .await
                .unwrap(),
            Some((2, "Ready".to_string()))
        );
        assert_eq!(
            writer
                .live_workflow_definition(workflow_id)
                .await
                .unwrap()
                .unwrap()
                .version,
            2
        );

        let failed_workflow_id = Uuid::new_v4();
        let duplicate_task_id = Uuid::new_v4();
        let atomic_failure = registration(
            failed_workflow_id,
            &[duplicate_task_id, duplicate_task_id],
            "atomic-failure",
            "must-not-persist",
        );
        assert!(writer.register_definition(atomic_failure).await.is_err());
        assert_eq!(definition_row_count(&writer, failed_workflow_id).await, 0);
        assert_eq!(task_build_row_count(&writer, failed_workflow_id).await, 0);
        assert_eq!(
            writer.task_specification(duplicate_task_id).await.unwrap(),
            None
        );

        run_lifecycle_laws(&writer).await;
        assert_classified_pool_contention(&selection).await;

        set_status(&writer, workflow_id, 2, "CorruptStatus").await;
        let corrupt = reader
            .list_definition_versions(workflow_id)
            .await
            .unwrap_err();
        assert_eq!(corrupt.kind(), RepositoryErrorKind::CorruptStoredValue);
        set_status(&writer, workflow_id, 2, "Ready").await;

        reader.close().await;
        let unavailable = reader.list_definitions().await.unwrap_err();
        assert_eq!(unavailable.kind(), RepositoryErrorKind::Unavailable);
        writer.close().await;
        let unavailable = writer.register_definition(second).await.unwrap_err();
        assert_eq!(unavailable.kind(), RepositoryErrorKind::Unavailable);

        let reopened = RepositoryFactory::new(selection.clone())
            .open_read_only()
            .await
            .unwrap();
        let definitions = reopened.list_definitions().await.unwrap();
        let original = definitions
            .iter()
            .find(|row| row.workflow.id == workflow_id.to_string())
            .expect("original definition remains readable");
        assert_eq!(original.build_version, 2);
        assert_eq!(original.live_version, Some(2));
        reopened.close().await;

        let reopened_writer = RepositoryFactory::new(selection)
            .open_writer()
            .await
            .unwrap();
        assert!(reopened_writer
            .definition_submission_reconciliation_candidates()
            .await
            .unwrap()
            .contains(&DefinitionSubmissionPointer {
                workflow_id,
                workflow_version: 2,
            }));
        reopened_writer.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_definition_repository_laws() {
        let container = match Postgres::default()
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping: testcontainers Postgres unavailable: {error}");
                return;
            }
        };
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let migration_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        apply_target(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;
        run_laws(DataPlaneSql::Postgres { url }).await;
    }

    fn sqlite_url(path: &Path) -> String {
        format!("sqlite://{}", path.display())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_backed_sqlite_definition_repository_laws() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("definitions.db"));
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;
        run_laws(DataPlaneSql::Sqlite { url: url.clone() }).await;

        let corrupt_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, false).unwrap())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO workflows \
             (id, name, definition, version, status, nickel_source, namespace, slug, content_hash, cosmetic_hash) \
             VALUES ('not-a-uuid', 'corrupt', '{}', 1, 'Ready', '', 'default', 'corrupt', 'corrupt', 'corrupt')",
        )
        .execute(&corrupt_pool)
        .await
        .unwrap();
        corrupt_pool.close().await;

        let reopened = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap();
        let corrupt = reopened
            .definition_submission_reconciliation_candidates()
            .await
            .unwrap_err();
        assert_eq!(corrupt.kind(), RepositoryErrorKind::CorruptStoredValue);
        reopened.close().await;
    }
}
