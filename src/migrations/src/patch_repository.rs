//! Patch ingress persistence and read-only lifecycle projections.
//!
//! Ingress owns deduplication, one-active-Patch rejection, lifecycle insertion,
//! patched Task-specification persistence, and their transaction boundary. The
//! returned outcome is committed before callers publish build jobs or relay an
//! apply envelope.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Sqlite, SqliteConnection, SqlitePool, Transaction};
use tickr_proto::{patch as pp, workflow as wf};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{
    decode_json, decode_timestamp, decode_uuid, encode_json, encode_timestamp, encode_uuid,
};

const REJECT_IN_PROGRESS: &str = "rejected: patch already in progress for this instance";
pub const MAX_PATCH_BUILD_LEASE_BATCH: usize = 64;
pub const MAX_PATCH_LIFECYCLE_LEASE_BATCH: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchProvenance {
    SelfEmitted,
    External,
}

impl PatchProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SelfEmitted => "self",
            Self::External => "external",
        }
    }

    pub const fn to_proto(self) -> i32 {
        match self {
            Self::External => pp::PatchProvenance::External as i32,
            Self::SelfEmitted => pp::PatchProvenance::SelfEmitted as i32,
        }
    }

    pub fn from_wire(value: &str) -> Self {
        match value {
            "self" => Self::SelfEmitted,
            _ => Self::External,
        }
    }

    fn from_stored(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "self" => Ok(Self::SelfEmitted),
            "external" => Ok(Self::External),
            other => Err(corrupt_value(format!("unknown Patch provenance `{other}`"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchSourceFormat {
    Nickel,
    Json,
}

impl PatchSourceFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nickel => "nickel",
            Self::Json => "json",
        }
    }

    fn from_stored(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "nickel" => Ok(Self::Nickel),
            "json" => Ok(Self::Json),
            other => Err(corrupt_value(format!(
                "unknown Patch source format `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSource {
    pub text: String,
    pub format: PatchSourceFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchLifecycleStatus {
    Validating,
    Building,
    Submitted,
    Applied,
    Rejected,
    BuildFailed,
}

impl PatchLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validating => "Validating",
            Self::Building => "Building",
            Self::Submitted => "Submitted",
            Self::Applied => "Applied",
            Self::Rejected => "Rejected",
            Self::BuildFailed => "BuildFailed",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Applied | Self::Rejected | Self::BuildFailed)
    }

    fn from_stored(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "Validating" => Ok(Self::Validating),
            "Building" => Ok(Self::Building),
            "Submitted" => Ok(Self::Submitted),
            "Applied" => Ok(Self::Applied),
            "Rejected" => Ok(Self::Rejected),
            "BuildFailed" => Ok(Self::BuildFailed),
            other => Err(corrupt_value(format!(
                "unknown Patch lifecycle status `{other}`"
            ))),
        }
    }
}

impl PartialEq<&str> for PatchLifecycleStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(Debug)]
pub struct PatchTaskSpecification<'a> {
    pub task_id: Uuid,
    pub routing_vars: &'a [wf::RoutingVarDecl],
}

#[derive(Debug)]
pub struct PatchIngressInput<'a> {
    pub patch_key: Uuid,
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub ops: &'a [pp::AddressedPatchOp],
    pub operation: Option<&'a pp::PatchOperation>,
    pub reason: Option<&'a str>,
    pub provenance: PatchProvenance,
    pub source: &'a str,
    pub source_format: PatchSourceFormat,
    pub tasks: Vec<PatchTaskSpecification<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatchLifecycleRow {
    pub patch_key: Uuid,
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub status: PatchLifecycleStatus,
    pub ops: Vec<pp::AddressedPatchOp>,
    pub reason: Option<String>,
    pub outcome: Option<String>,
    pub applied_version: Option<i64>,
    pub provenance: PatchProvenance,
    pub operation: Option<pp::PatchOperation>,
}

impl PatchLifecycleRow {
    pub const fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchIngressOutcome {
    Accepted { status: PatchLifecycleStatus },
    RejectedInProgress { reason: String },
    Existing { row: PatchLifecycleRow },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchTaskBuildResult<'a> {
    Success,
    Failure { error: &'a str },
}

#[derive(Debug, Clone, Copy)]
pub struct PatchBuildLeaseRequest<'a> {
    pub owner: &'a str,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchBuildTask {
    pub patch_key: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_id: Uuid,
    pub nix_expression_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedPatchBuildTask {
    pub task: PatchBuildTask,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LeasedPatchBuildSettlementOutcome {
    Settled(PatchBuildSettlementOutcome),
    LeaseLost,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchBuildSettlementOutcome {
    AwaitingTasks,
    Submitted(PatchLifecycleRow),
    BuildFailed,
    AlreadySettled(PatchLifecycleStatus),
    TaskAlreadySettled,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchSubmissionOutcome {
    Submitted,
    AlreadySettled(PatchLifecycleStatus),
    Absent,
}

#[derive(Debug, Clone, Copy)]
pub struct PatchLifecycleLeaseRequest<'a> {
    pub owner: &'a str,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub eligible_before: DateTime<Utc>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeasedPatchLifecycle {
    pub row: PatchLifecycleRow,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasedPatchSubmissionOutcome {
    Settled(PatchSubmissionOutcome),
    LeaseLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchTerminalOutcome<'a> {
    Applied { version: i64 },
    Rejected { reason: &'a str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatchOutcomeInput<'a> {
    pub patch_key: Uuid,
    pub workflow_instance_id: Uuid,
    pub outcome: PatchTerminalOutcome<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOutcomeCorrelation {
    Won,
    AlreadySettled(PatchLifecycleStatus),
    Absent,
    Conflicted,
}

#[derive(Debug)]
pub enum PatchRedriveCandidate {
    Ready(PatchLifecycleRow),
    Corrupt {
        identity: String,
        error: RepositoryError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSettlementDiscrepancy {
    pub workflow_instance_id: Uuid,
    pub patch_key: Uuid,
    pub ledger_status: PatchLifecycleStatus,
    pub detail: String,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchStatusRow {
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub status: PatchLifecycleStatus,
    pub outcome: Option<String>,
    pub reason: Option<String>,
    pub applied_version: Option<i64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSourceRow {
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub source: Option<PatchSource>,
    pub applied_version: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored Patch value: {0}")]
struct CorruptPatchValue(String);

#[derive(Debug, Clone, Copy)]
struct PatchBuildLeaseGuard<'a> {
    owner: &'a str,
    token: Uuid,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct PatchLifecycleLeaseGuard<'a> {
    owner: &'a str,
    token: Uuid,
    now: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
enum PatchLeaseError {
    #[error("{0} leases require SQLite")]
    RequiresSqlite(&'static str),
    #[error("{0} lease owner must contain 1 to 128 bytes")]
    InvalidOwner(&'static str),
    #[error("{0} lease expiry must be later than acquisition time")]
    InvalidExpiry(&'static str),
    #[error("{0} lease batch must contain 1 to {1} rows")]
    InvalidLimit(&'static str, usize),
    #[error("unguarded {0} settlement reported lease loss")]
    UnexpectedLeaseLoss(&'static str),
}

impl WriterRepositoryBundle {
    pub async fn ingress_patch(
        &self,
        input: PatchIngressInput<'_>,
    ) -> Result<PatchIngressOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => ingress_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => ingress_sqlite(pool, input).await,
        }
    }

    /// Lease committed pending Patch builds in durable stable order.
    ///
    /// This operation belongs to Tickr Lite's one-writer SQLite formation;
    /// distributed Postgres processing retains its NATS queue-group protocol.
    pub async fn lease_patch_build_tasks(
        &self,
        request: PatchBuildLeaseRequest<'_>,
    ) -> Result<Vec<LeasedPatchBuildTask>, RepositoryError> {
        validate_patch_build_lease_request(request)?;
        match &self.pool {
            BackendPool::Postgres(_) => Err(patch_lease_error(PatchLeaseError::RequiresSqlite(
                "Patch build",
            ))),
            BackendPool::Sqlite(pool) => lease_patch_build_tasks_sqlite(pool, request).await,
        }
    }

    pub async fn settle_patch_task_build(
        &self,
        patch_key: Uuid,
        task_id: Uuid,
        result: PatchTaskBuildResult<'_>,
    ) -> Result<PatchBuildSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                settle_task_build_postgres(pool, patch_key, task_id, result).await
            }
            BackendPool::Sqlite(pool) => {
                match settle_task_build_sqlite(pool, patch_key, task_id, result, None).await? {
                    LeasedPatchBuildSettlementOutcome::Settled(outcome) => Ok(outcome),
                    LeasedPatchBuildSettlementOutcome::LeaseLost => Err(RepositoryError::new(
                        RepositoryErrorKind::Internal,
                        PatchLeaseError::UnexpectedLeaseLoss("Patch build"),
                    )),
                }
            }
        }
    }

    /// Settle one Patch build only while the caller owns its unexpired lease.
    pub async fn settle_leased_patch_task_build(
        &self,
        lease: &LeasedPatchBuildTask,
        result: PatchTaskBuildResult<'_>,
        now: DateTime<Utc>,
    ) -> Result<LeasedPatchBuildSettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(_) => Err(patch_lease_error(PatchLeaseError::RequiresSqlite(
                "Patch build",
            ))),
            BackendPool::Sqlite(pool) => {
                settle_task_build_sqlite(
                    pool,
                    lease.task.patch_key,
                    lease.task.task_id,
                    result,
                    Some(PatchBuildLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    }),
                )
                .await
            }
        }
    }

    /// Lease committed Patch apply/re-drive rows in durable stable order.
    pub async fn lease_patch_lifecycle(
        &self,
        request: PatchLifecycleLeaseRequest<'_>,
    ) -> Result<Vec<LeasedPatchLifecycle>, RepositoryError> {
        validate_patch_lifecycle_lease_request(request)?;
        match &self.pool {
            BackendPool::Postgres(_) => Err(patch_lease_error(PatchLeaseError::RequiresSqlite(
                "Patch lifecycle",
            ))),
            BackendPool::Sqlite(pool) => lease_patch_lifecycle_sqlite(pool, request).await,
        }
    }

    pub async fn mark_patch_submitted(
        &self,
        patch_key: Uuid,
    ) -> Result<PatchSubmissionOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => mark_submitted_postgres(pool, patch_key).await,
            BackendPool::Sqlite(pool) => mark_submitted_sqlite(pool, patch_key).await,
        }
    }

    /// Mark a leased Patch envelope as submitted only while its lease is live.
    pub async fn settle_leased_patch_submission(
        &self,
        lease: &LeasedPatchLifecycle,
        now: DateTime<Utc>,
    ) -> Result<LeasedPatchSubmissionOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(_) => Err(patch_lease_error(PatchLeaseError::RequiresSqlite(
                "Patch lifecycle",
            ))),
            BackendPool::Sqlite(pool) => {
                settle_leased_patch_submission_sqlite(
                    pool,
                    lease.row.patch_key,
                    PatchLifecycleLeaseGuard {
                        owner: &lease.lease_owner,
                        token: lease.lease_token,
                        now,
                    },
                )
                .await
            }
        }
    }

    /// Release a failed local drive without treating the transient send as work.
    pub async fn release_patch_lifecycle_lease(
        &self,
        lease: &LeasedPatchLifecycle,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(_) => Err(patch_lease_error(PatchLeaseError::RequiresSqlite(
                "Patch lifecycle",
            ))),
            BackendPool::Sqlite(pool) => release_patch_lifecycle_lease_sqlite(pool, lease).await,
        }
    }

    pub async fn correlate_patch_outcome(
        &self,
        input: PatchOutcomeInput<'_>,
    ) -> Result<PatchOutcomeCorrelation, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => correlate_outcome_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => correlate_outcome_sqlite(pool, input).await,
        }
    }

    pub async fn unsettled_patches_older_than(
        &self,
        min_age: Duration,
    ) -> Result<Vec<PatchRedriveCandidate>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => unsettled_patches_postgres(pool, min_age).await,
            BackendPool::Sqlite(pool) => unsettled_patches_sqlite(pool, min_age).await,
        }
    }

    pub async fn audit_patch_settlement(
        &self,
        workflow_instance_id: Uuid,
        applied_patch_keys: &HashSet<Uuid>,
    ) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                audit_patch_settlement_postgres(pool, workflow_instance_id, applied_patch_keys)
                    .await
            }
            BackendPool::Sqlite(pool) => {
                audit_patch_settlement_sqlite(pool, workflow_instance_id, applied_patch_keys).await
            }
        }
    }

    pub async fn patch_settlement_discrepancies(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                patch_settlement_discrepancies_postgres(pool, workflow_instance_id).await
            }
            BackendPool::Sqlite(pool) => {
                patch_settlement_discrepancies_sqlite(pool, workflow_instance_id).await
            }
        }
    }
}

impl ReadOnlyRepositoryBundle {
    pub async fn patch_status(
        &self,
        patch_id: Uuid,
    ) -> Result<Option<PatchStatusRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => patch_status_postgres(pool, patch_id).await,
            BackendPool::Sqlite(pool) => patch_status_sqlite(pool, patch_id).await,
        }
    }

    pub async fn patch_source(
        &self,
        patch_id: Uuid,
    ) -> Result<Option<PatchSourceRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => patch_source_postgres(pool, patch_id).await,
            BackendPool::Sqlite(pool) => patch_source_sqlite(pool, patch_id).await,
        }
    }
}

async fn ingress_postgres(
    pool: &PgPool,
    input: PatchIngressInput<'_>,
) -> Result<PatchIngressOutcome, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;

    // Every request for one Workflow instance takes the same transaction-scoped
    // lock. This closes the check-then-insert race without leaking lock details
    // to callers or requiring a local instance row.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
        .bind(input.workflow_instance_id)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;

    let outcome = ingress_postgres_transaction(&mut transaction, &input).await?;
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(outcome)
}

async fn ingress_postgres_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    input: &PatchIngressInput<'_>,
) -> Result<PatchIngressOutcome, RepositoryError> {
    if let Some(row) = patch_lifecycle_postgres(&mut **transaction, input.patch_key).await? {
        return Ok(PatchIngressOutcome::Existing { row });
    }

    let sibling_unsettled: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM workflow_patches WHERE workflow_instance_id = $1 AND status IN ('Validating', 'Building', 'Submitted'))",
    )
    .bind(input.workflow_instance_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let status = ingress_status(sibling_unsettled, input.tasks.is_empty());
    let outcome = sibling_unsettled.then_some(REJECT_IN_PROGRESS);
    let ops = serde_json::to_value(input.ops).map_err(internal_value)?;
    let operation = input
        .operation
        .map(serde_json::to_value)
        .transpose()
        .map_err(internal_value)?;

    sqlx::query(
        "INSERT INTO workflow_patches (patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, provenance, source, source_format, operation) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(input.patch_key)
    .bind(input.patch_id)
    .bind(input.workflow_instance_id)
    .bind(status.as_str())
    .bind(ops)
    .bind(input.reason)
    .bind(outcome)
    .bind(input.provenance.as_str())
    .bind(input.source)
    .bind(input.source_format.as_str())
    .bind(operation)
    .execute(&mut **transaction)
    .await
    .map_err(repository_sqlx_error)?;

    if !sibling_unsettled {
        for task in &input.tasks {
            sqlx::query(
                "INSERT INTO workflow_patch_task_builds (patch_key, task_id, status) VALUES ($1, $2, 'pending')",
            )
            .bind(input.patch_key)
            .bind(task.task_id)
            .execute(&mut **transaction)
            .await
            .map_err(repository_sqlx_error)?;

            let routing_vars = serde_json::to_value(task.routing_vars).map_err(internal_value)?;
            sqlx::query(
                "INSERT INTO task_specs (task_id, routing_vars) VALUES ($1, $2) ON CONFLICT (task_id) DO NOTHING",
            )
            .bind(task.task_id)
            .bind(routing_vars)
            .execute(&mut **transaction)
            .await
            .map_err(repository_sqlx_error)?;
        }
    }

    Ok(inserted_outcome(sibling_unsettled, status))
}

async fn ingress_sqlite(
    pool: &SqlitePool,
    input: PatchIngressInput<'_>,
) -> Result<PatchIngressOutcome, RepositoryError> {
    let mut connection = pool.acquire().await.map_err(repository_sqlx_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(repository_sqlx_error)?;

    let result = ingress_sqlite_transaction(&mut connection, &input).await;
    match result {
        Ok(outcome) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(repository_sqlx_error)?;
            Ok(outcome)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

async fn ingress_sqlite_transaction(
    connection: &mut SqliteConnection,
    input: &PatchIngressInput<'_>,
) -> Result<PatchIngressOutcome, RepositoryError> {
    if let Some(row) = patch_lifecycle_sqlite(&mut *connection, input.patch_key).await? {
        return Ok(PatchIngressOutcome::Existing { row });
    }

    let sibling_unsettled: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM workflow_patches WHERE workflow_instance_id = ?1 AND status IN ('Validating', 'Building', 'Submitted'))",
    )
    .bind(encode_uuid(input.workflow_instance_id))
    .fetch_one(&mut *connection)
    .await
    .map_err(repository_sqlx_error)?;

    let status = ingress_status(sibling_unsettled, input.tasks.is_empty());
    let outcome = sibling_unsettled.then_some(REJECT_IN_PROGRESS);
    let ops = serde_json::to_value(input.ops).map_err(internal_value)?;
    let operation = input
        .operation
        .map(serde_json::to_value)
        .transpose()
        .map_err(internal_value)?;

    sqlx::query(
        "INSERT INTO workflow_patches (patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, provenance, source, source_format, operation) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(encode_uuid(input.patch_key))
    .bind(encode_uuid(input.patch_id))
    .bind(encode_uuid(input.workflow_instance_id))
    .bind(status.as_str())
    .bind(encode_json(&ops))
    .bind(input.reason)
    .bind(outcome)
    .bind(input.provenance.as_str())
    .bind(input.source)
    .bind(input.source_format.as_str())
    .bind(operation.as_ref().map(encode_json))
    .execute(&mut *connection)
    .await
    .map_err(repository_sqlx_error)?;

    if !sibling_unsettled {
        for task in &input.tasks {
            sqlx::query(
                "INSERT INTO workflow_patch_task_builds (patch_key, task_id, status) VALUES (?1, ?2, 'pending')",
            )
            .bind(encode_uuid(input.patch_key))
            .bind(encode_uuid(task.task_id))
            .execute(&mut *connection)
            .await
            .map_err(repository_sqlx_error)?;

            let routing_vars = serde_json::to_value(task.routing_vars).map_err(internal_value)?;
            sqlx::query(
                "INSERT INTO task_specs (task_id, routing_vars) VALUES (?1, ?2) ON CONFLICT (task_id) DO NOTHING",
            )
            .bind(encode_uuid(task.task_id))
            .bind(encode_json(&routing_vars))
            .execute(&mut *connection)
            .await
            .map_err(repository_sqlx_error)?;
        }
    }

    Ok(inserted_outcome(sibling_unsettled, status))
}

fn ingress_status(sibling_unsettled: bool, tasks_empty: bool) -> PatchLifecycleStatus {
    if sibling_unsettled {
        PatchLifecycleStatus::Rejected
    } else if tasks_empty {
        PatchLifecycleStatus::Validating
    } else {
        PatchLifecycleStatus::Building
    }
}

fn inserted_outcome(sibling_unsettled: bool, status: PatchLifecycleStatus) -> PatchIngressOutcome {
    if sibling_unsettled {
        PatchIngressOutcome::RejectedInProgress {
            reason: REJECT_IN_PROGRESS.to_owned(),
        }
    } else {
        PatchIngressOutcome::Accepted { status }
    }
}

fn task_build_values(result: PatchTaskBuildResult<'_>) -> (&'static str, Option<&str>) {
    match result {
        PatchTaskBuildResult::Success => ("success", None),
        PatchTaskBuildResult::Failure { error } => ("failure", Some(error)),
    }
}

fn validate_patch_build_lease_request(
    request: PatchBuildLeaseRequest<'_>,
) -> Result<(), RepositoryError> {
    validate_patch_lease_request(
        "Patch build",
        request.owner,
        request.now,
        request.expires_at,
        request.limit,
        MAX_PATCH_BUILD_LEASE_BATCH,
    )
}

fn validate_patch_lifecycle_lease_request(
    request: PatchLifecycleLeaseRequest<'_>,
) -> Result<(), RepositoryError> {
    validate_patch_lease_request(
        "Patch lifecycle",
        request.owner,
        request.now,
        request.expires_at,
        request.limit,
        MAX_PATCH_LIFECYCLE_LEASE_BATCH,
    )
}

fn validate_patch_lease_request(
    kind: &'static str,
    owner: &str,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    limit: usize,
    max_limit: usize,
) -> Result<(), RepositoryError> {
    if owner.is_empty() || owner.len() > 128 {
        return Err(patch_lease_error(PatchLeaseError::InvalidOwner(kind)));
    }
    if expires_at <= now {
        return Err(patch_lease_error(PatchLeaseError::InvalidExpiry(kind)));
    }
    if !(1..=max_limit).contains(&limit) {
        return Err(patch_lease_error(PatchLeaseError::InvalidLimit(
            kind, max_limit,
        )));
    }
    Ok(())
}

fn patch_lease_error(error: PatchLeaseError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, error)
}

async fn lease_patch_build_tasks_sqlite(
    pool: &SqlitePool,
    request: PatchBuildLeaseRequest<'_>,
) -> Result<Vec<LeasedPatchBuildTask>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(request.now);
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT b.patch_key, b.task_id, p.workflow_instance_id, p.ops \
         FROM workflow_patch_task_builds b \
         JOIN workflow_patches p ON p.patch_key = b.patch_key \
         WHERE b.status = 'pending' AND p.status = 'Building' \
           AND (b.lease_expires_at IS NULL OR b.lease_expires_at <= ?1) \
         ORDER BY b.pending_since, b.patch_key, b.task_id \
         LIMIT ?2",
    )
    .bind(&encoded_now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let encoded_expiry = encode_timestamp(request.expires_at);
    let mut leases = Vec::with_capacity(rows.len());
    for (encoded_patch_key, encoded_task_id, encoded_instance_id, encoded_ops) in rows {
        let patch_key = decode_uuid(&encoded_patch_key).map_err(corrupt_value)?;
        let task_id = decode_uuid(&encoded_task_id).map_err(corrupt_value)?;
        let workflow_instance_id = decode_uuid(&encoded_instance_id).map_err(corrupt_value)?;
        let ops: Vec<pp::AddressedPatchOp> =
            serde_json::from_value(decode_json(&encoded_ops).map_err(corrupt_value)?)
                .map_err(corrupt_value)?;
        let nix_expression_path = ops
            .iter()
            .find_map(|op| match &op.op {
                Some(pp::addressed_patch_op::Op::AddNode(node))
                    if node.node_id == task_id.to_string() =>
                {
                    node.task
                        .as_ref()
                        .map(|task| task.nix_expression_path.clone())
                }
                _ => None,
            })
            .ok_or_else(|| {
                corrupt_value(CorruptPatchValue(format!(
                    "missing leased Patch build task {task_id}"
                )))
            })?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflow_patch_task_builds \
             SET lease_owner = ?3, lease_token = ?4, lease_expires_at = ?5 \
             WHERE patch_key = ?1 AND task_id = ?2 AND status = 'pending' \
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?6)",
        )
        .bind(&encoded_patch_key)
        .bind(&encoded_task_id)
        .bind(request.owner)
        .bind(encode_uuid(lease_token))
        .bind(&encoded_expiry)
        .bind(&encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(corrupt_value(CorruptPatchValue(format!(
                "selected Patch build task {task_id} was not leaseable"
            ))));
        }
        leases.push(LeasedPatchBuildTask {
            task: PatchBuildTask {
                patch_key,
                workflow_instance_id,
                task_id,
                nix_expression_path,
            },
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
    patch_key: Uuid,
    task_id: Uuid,
    result: PatchTaskBuildResult<'_>,
) -> Result<PatchBuildSettlementOutcome, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_patches WHERE patch_key = $1 FOR UPDATE")
            .bind(patch_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(PatchBuildSettlementOutcome::Absent);
    };
    let status = PatchLifecycleStatus::from_stored(&status)?;
    if status != PatchLifecycleStatus::Building {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(PatchBuildSettlementOutcome::AlreadySettled(status));
    }

    let (task_status, error) = task_build_values(result);
    let updated = sqlx::query(
        "UPDATE workflow_patch_task_builds \
         SET status = $3, error = $4, built_at = now() \
         WHERE patch_key = $1 AND task_id = $2 AND status = 'pending'",
    )
    .bind(patch_key)
    .bind(task_id)
    .bind(task_status)
    .bind(error)
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    if updated.rows_affected() == 0 {
        let task_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workflow_patch_task_builds \
             WHERE patch_key = $1 AND task_id = $2)",
        )
        .bind(patch_key)
        .bind(task_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(if task_exists {
            PatchBuildSettlementOutcome::TaskAlreadySettled
        } else {
            PatchBuildSettlementOutcome::Absent
        });
    }

    match result {
        PatchTaskBuildResult::Failure { error } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'BuildFailed', outcome = $2, updated_at = now() \
                 WHERE patch_key = $1 AND status = 'Building'",
            )
            .bind(patch_key)
            .bind(format!("build failed: {error}"))
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
            transaction.commit().await.map_err(repository_sqlx_error)?;
            Ok(PatchBuildSettlementOutcome::BuildFailed)
        }
        PatchTaskBuildResult::Success => {
            let updated = sqlx::query(
                "UPDATE workflow_patches p \
                 SET status = 'Submitted', updated_at = now() \
                 WHERE p.patch_key = $1 AND p.status = 'Building' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM workflow_patch_task_builds b \
                       WHERE b.patch_key = p.patch_key AND b.status <> 'success' \
                   )",
            )
            .bind(patch_key)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected();
            let intent = if updated == 1 {
                patch_lifecycle_postgres(&mut *transaction, patch_key).await?
            } else {
                None
            };
            transaction.commit().await.map_err(repository_sqlx_error)?;
            Ok(match intent {
                Some(row) => PatchBuildSettlementOutcome::Submitted(row),
                None => PatchBuildSettlementOutcome::AwaitingTasks,
            })
        }
    }
}

async fn settle_task_build_sqlite(
    pool: &SqlitePool,
    patch_key: Uuid,
    task_id: Uuid,
    result: PatchTaskBuildResult<'_>,
    lease_guard: Option<PatchBuildLeaseGuard<'_>>,
) -> Result<LeasedPatchBuildSettlementOutcome, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_patch_key = encode_uuid(patch_key);
    let encoded_task_id = encode_uuid(task_id);
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_patches WHERE patch_key = ?1")
            .bind(&encoded_patch_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
    let Some(status) = status else {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedPatchBuildSettlementOutcome::Settled(
            PatchBuildSettlementOutcome::Absent,
        ));
    };
    let status = PatchLifecycleStatus::from_stored(&status)?;
    if status != PatchLifecycleStatus::Building {
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(LeasedPatchBuildSettlementOutcome::Settled(
            PatchBuildSettlementOutcome::AlreadySettled(status),
        ));
    }

    let settled_at = lease_guard.map_or_else(Utc::now, |guard| guard.now);
    let (task_status, error) = task_build_values(result);
    let updated = if let Some(guard) = lease_guard {
        sqlx::query(
            "UPDATE workflow_patch_task_builds \
             SET status = ?3, error = ?4, built_at = ?5, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE patch_key = ?1 AND task_id = ?2 AND status = 'pending' \
               AND lease_owner = ?6 AND lease_token = ?7 AND lease_expires_at > ?8",
        )
        .bind(&encoded_patch_key)
        .bind(&encoded_task_id)
        .bind(task_status)
        .bind(error)
        .bind(encode_timestamp(settled_at))
        .bind(guard.owner)
        .bind(encode_uuid(guard.token))
        .bind(encode_timestamp(guard.now))
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query(
            "UPDATE workflow_patch_task_builds \
             SET status = ?3, error = ?4, built_at = ?5, \
                 lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
             WHERE patch_key = ?1 AND task_id = ?2 AND status = 'pending' \
               AND lease_token IS NULL",
        )
        .bind(&encoded_patch_key)
        .bind(&encoded_task_id)
        .bind(task_status)
        .bind(error)
        .bind(encode_timestamp(settled_at))
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?
    };
    if updated.rows_affected() == 0 {
        let stored_status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM workflow_patch_task_builds \
             WHERE patch_key = ?1 AND task_id = ?2",
        )
        .bind(&encoded_patch_key)
        .bind(&encoded_task_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        transaction.commit().await.map_err(repository_sqlx_error)?;
        return Ok(match stored_status.as_deref() {
            Some("pending") if lease_guard.is_some() => {
                LeasedPatchBuildSettlementOutcome::LeaseLost
            }
            Some(_) => LeasedPatchBuildSettlementOutcome::Settled(
                PatchBuildSettlementOutcome::TaskAlreadySettled,
            ),
            None => LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::Absent),
        });
    }

    match result {
        PatchTaskBuildResult::Failure { error } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'BuildFailed', outcome = ?2, updated_at = ?3 \
                 WHERE patch_key = ?1 AND status = 'Building'",
            )
            .bind(&encoded_patch_key)
            .bind(format!("build failed: {error}"))
            .bind(encode_timestamp(settled_at))
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?;
            transaction.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedPatchBuildSettlementOutcome::Settled(
                PatchBuildSettlementOutcome::BuildFailed,
            ))
        }
        PatchTaskBuildResult::Success => {
            let updated = sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'Submitted', updated_at = ?2 \
                 WHERE patch_key = ?1 AND status = 'Building' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM workflow_patch_task_builds \
                       WHERE patch_key = ?1 AND status <> 'success' \
                   )",
            )
            .bind(&encoded_patch_key)
            .bind(encode_timestamp(settled_at))
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected();
            let intent = if updated == 1 {
                patch_lifecycle_sqlite(&mut *transaction, patch_key).await?
            } else {
                None
            };
            transaction.commit().await.map_err(repository_sqlx_error)?;
            Ok(LeasedPatchBuildSettlementOutcome::Settled(match intent {
                Some(row) => PatchBuildSettlementOutcome::Submitted(row),
                None => PatchBuildSettlementOutcome::AwaitingTasks,
            }))
        }
    }
}

async fn mark_submitted_postgres(
    pool: &PgPool,
    patch_key: Uuid,
) -> Result<PatchSubmissionOutcome, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE workflow_patches SET status = 'Submitted', updated_at = now() WHERE patch_key = $1 AND status IN ('Validating', 'Submitted')",
    )
    .bind(patch_key)
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(PatchSubmissionOutcome::Submitted);
    }
    Ok(match patch_lifecycle_postgres(pool, patch_key).await? {
        Some(row) => PatchSubmissionOutcome::AlreadySettled(row.status),
        None => PatchSubmissionOutcome::Absent,
    })
}

async fn mark_submitted_sqlite(
    pool: &SqlitePool,
    patch_key: Uuid,
) -> Result<PatchSubmissionOutcome, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE workflow_patches SET status = 'Submitted', updated_at = CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER) WHERE patch_key = ?1 AND status IN ('Validating', 'Submitted')",
    )
    .bind(encode_uuid(patch_key))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(PatchSubmissionOutcome::Submitted);
    }
    Ok(match patch_lifecycle_sqlite(pool, patch_key).await? {
        Some(row) => PatchSubmissionOutcome::AlreadySettled(row.status),
        None => PatchSubmissionOutcome::Absent,
    })
}

async fn lease_patch_lifecycle_sqlite(
    pool: &SqlitePool,
    request: PatchLifecycleLeaseRequest<'_>,
) -> Result<Vec<LeasedPatchLifecycle>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(request.now);
    let rows: Vec<SqliteLifecycleTuple> = sqlx::query_as(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, \
                outcome, applied_version, provenance, operation \
         FROM workflow_patches \
         WHERE status IN ('Validating', 'Submitted') AND updated_at <= ?1 \
           AND (lifecycle_lease_expires_at IS NULL OR lifecycle_lease_expires_at <= ?2) \
         ORDER BY updated_at, patch_key \
         LIMIT ?3",
    )
    .bind(encode_timestamp(request.eligible_before))
    .bind(&encoded_now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let encoded_expiry = encode_timestamp(request.expires_at);
    let mut leases = Vec::with_capacity(rows.len());
    for row in rows {
        let lifecycle = decode_sqlite_lifecycle(row)?;
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflow_patches \
             SET lifecycle_lease_owner = ?2, lifecycle_lease_token = ?3, \
                 lifecycle_lease_expires_at = ?4 \
             WHERE patch_key = ?1 AND status IN ('Validating', 'Submitted') \
               AND (lifecycle_lease_expires_at IS NULL OR lifecycle_lease_expires_at <= ?5)",
        )
        .bind(encode_uuid(lifecycle.patch_key))
        .bind(request.owner)
        .bind(encode_uuid(lease_token))
        .bind(&encoded_expiry)
        .bind(&encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(corrupt_value(CorruptPatchValue(format!(
                "selected Patch lifecycle {} was not leaseable",
                lifecycle.patch_key
            ))));
        }
        leases.push(LeasedPatchLifecycle {
            row: lifecycle,
            lease_owner: request.owner.to_owned(),
            lease_token,
            lease_expires_at: request.expires_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(leases)
}

async fn settle_leased_patch_submission_sqlite(
    pool: &SqlitePool,
    patch_key: Uuid,
    guard: PatchLifecycleLeaseGuard<'_>,
) -> Result<LeasedPatchSubmissionOutcome, RepositoryError> {
    let encoded_patch_key = encode_uuid(patch_key);
    let updated = sqlx::query(
        "UPDATE workflow_patches \
         SET status = 'Submitted', updated_at = ?5, \
             lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, \
             lifecycle_lease_expires_at = NULL \
         WHERE patch_key = ?1 AND status IN ('Validating', 'Submitted') \
           AND lifecycle_lease_owner = ?2 AND lifecycle_lease_token = ?3 \
           AND lifecycle_lease_expires_at > ?4",
    )
    .bind(&encoded_patch_key)
    .bind(guard.owner)
    .bind(encode_uuid(guard.token))
    .bind(encode_timestamp(guard.now))
    .bind(encode_timestamp(guard.now))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(LeasedPatchSubmissionOutcome::Settled(
            PatchSubmissionOutcome::Submitted,
        ));
    }
    Ok(match patch_lifecycle_sqlite(pool, patch_key).await? {
        Some(row)
            if matches!(
                row.status,
                PatchLifecycleStatus::Validating | PatchLifecycleStatus::Submitted
            ) =>
        {
            LeasedPatchSubmissionOutcome::LeaseLost
        }
        Some(row) => LeasedPatchSubmissionOutcome::Settled(PatchSubmissionOutcome::AlreadySettled(
            row.status,
        )),
        None => LeasedPatchSubmissionOutcome::Settled(PatchSubmissionOutcome::Absent),
    })
}

async fn release_patch_lifecycle_lease_sqlite(
    pool: &SqlitePool,
    lease: &LeasedPatchLifecycle,
) -> Result<bool, RepositoryError> {
    Ok(sqlx::query(
        "UPDATE workflow_patches \
         SET lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, \
             lifecycle_lease_expires_at = NULL \
         WHERE patch_key = ?1 AND lifecycle_lease_owner = ?2 \
           AND lifecycle_lease_token = ?3",
    )
    .bind(encode_uuid(lease.row.patch_key))
    .bind(&lease.lease_owner)
    .bind(encode_uuid(lease.lease_token))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected()
        == 1)
}

async fn correlate_outcome_postgres(
    pool: &PgPool,
    input: PatchOutcomeInput<'_>,
) -> Result<PatchOutcomeCorrelation, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let updated = match input.outcome {
        PatchTerminalOutcome::Applied { version } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'Applied', outcome = 'applied', applied_version = $3, updated_at = now(), lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, lifecycle_lease_expires_at = NULL \
                 WHERE patch_key = $1 AND workflow_instance_id = $2 \
                   AND status IN ('Validating', 'Building', 'Submitted')",
            )
            .bind(input.patch_key)
            .bind(input.workflow_instance_id)
            .bind(version)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected()
        }
        PatchTerminalOutcome::Rejected { reason } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'Rejected', outcome = $3, updated_at = now(), lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, lifecycle_lease_expires_at = NULL \
                 WHERE patch_key = $1 AND workflow_instance_id = $2 \
                   AND status IN ('Validating', 'Building', 'Submitted')",
            )
            .bind(input.patch_key)
            .bind(input.workflow_instance_id)
            .bind(reason)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected()
        }
    };
    let outcome = if updated == 1 {
        PatchOutcomeCorrelation::Won
    } else {
        classify_losing_correlation(
            patch_lifecycle_postgres(&mut *transaction, input.patch_key).await?,
            input.workflow_instance_id,
        )
    };
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(outcome)
}

async fn correlate_outcome_sqlite(
    pool: &SqlitePool,
    input: PatchOutcomeInput<'_>,
) -> Result<PatchOutcomeCorrelation, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let patch_key = encode_uuid(input.patch_key);
    let workflow_instance_id = encode_uuid(input.workflow_instance_id);
    let now = encode_timestamp(Utc::now());
    let updated = match input.outcome {
        PatchTerminalOutcome::Applied { version } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'Applied', outcome = 'applied', applied_version = ?3, updated_at = ?4, lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, lifecycle_lease_expires_at = NULL \
                 WHERE patch_key = ?1 AND workflow_instance_id = ?2 \
                   AND status IN ('Validating', 'Building', 'Submitted')",
            )
            .bind(&patch_key)
            .bind(&workflow_instance_id)
            .bind(version)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected()
        }
        PatchTerminalOutcome::Rejected { reason } => {
            sqlx::query(
                "UPDATE workflow_patches \
                 SET status = 'Rejected', outcome = ?3, updated_at = ?4, lifecycle_lease_owner = NULL, lifecycle_lease_token = NULL, lifecycle_lease_expires_at = NULL \
                 WHERE patch_key = ?1 AND workflow_instance_id = ?2 \
                   AND status IN ('Validating', 'Building', 'Submitted')",
            )
            .bind(&patch_key)
            .bind(&workflow_instance_id)
            .bind(reason)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(repository_sqlx_error)?
            .rows_affected()
        }
    };
    let outcome = if updated == 1 {
        PatchOutcomeCorrelation::Won
    } else {
        classify_losing_correlation(
            patch_lifecycle_sqlite(&mut *transaction, input.patch_key).await?,
            input.workflow_instance_id,
        )
    };
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(outcome)
}

fn classify_losing_correlation(
    row: Option<PatchLifecycleRow>,
    workflow_instance_id: Uuid,
) -> PatchOutcomeCorrelation {
    match row {
        None => PatchOutcomeCorrelation::Absent,
        Some(row) if row.workflow_instance_id != workflow_instance_id => {
            PatchOutcomeCorrelation::Conflicted
        }
        Some(row) if row.status.is_terminal() => {
            PatchOutcomeCorrelation::AlreadySettled(row.status)
        }
        Some(_) => PatchOutcomeCorrelation::Conflicted,
    }
}

async fn unsettled_patches_postgres(
    pool: &PgPool,
    min_age: Duration,
) -> Result<Vec<PatchRedriveCandidate>, RepositoryError> {
    let cutoff = Utc::now()
        - chrono::Duration::from_std(min_age)
            .map_err(|error| RepositoryError::new(RepositoryErrorKind::Internal, error))?;
    let rows: Vec<PostgresLifecycleTuple> = sqlx::query_as(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, applied_version, provenance, operation \
         FROM workflow_patches \
         WHERE status IN ('Validating', 'Submitted') AND updated_at <= $1 \
         ORDER BY updated_at ASC, patch_key ASC",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let identity = row.0;
            match decode_postgres_lifecycle(row) {
                Ok(row) => PatchRedriveCandidate::Ready(row),
                Err(error) => PatchRedriveCandidate::Corrupt {
                    identity: identity.to_string(),
                    error,
                },
            }
        })
        .collect())
}

async fn unsettled_patches_sqlite(
    pool: &SqlitePool,
    min_age: Duration,
) -> Result<Vec<PatchRedriveCandidate>, RepositoryError> {
    let cutoff = Utc::now()
        - chrono::Duration::from_std(min_age)
            .map_err(|error| RepositoryError::new(RepositoryErrorKind::Internal, error))?;
    let rows: Vec<SqliteLifecycleTuple> = sqlx::query_as(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, applied_version, provenance, operation \
         FROM workflow_patches \
         WHERE status IN ('Validating', 'Submitted') AND updated_at <= ?1 \
         ORDER BY updated_at ASC, patch_key ASC",
    )
    .bind(encode_timestamp(cutoff))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(decode_sqlite_redrive_candidate)
        .collect())
}

fn decode_sqlite_redrive_candidate(row: SqliteLifecycleTuple) -> PatchRedriveCandidate {
    let (
        patch_key,
        patch_id,
        workflow_instance_id,
        status,
        ops,
        reason,
        outcome,
        applied_version,
        provenance,
        operation,
    ) = row;
    let decoded = (|| {
        decode_lifecycle(
            decode_uuid(&patch_key).map_err(corrupt_value)?,
            decode_uuid(&patch_id).map_err(corrupt_value)?,
            decode_uuid(&workflow_instance_id).map_err(corrupt_value)?,
            status,
            decode_json(&ops).map_err(corrupt_value)?,
            reason,
            outcome,
            applied_version,
            provenance,
            operation
                .map(|value| decode_json(&value).map_err(corrupt_value))
                .transpose()?,
        )
    })();
    match decoded {
        Ok(row) => PatchRedriveCandidate::Ready(row),
        Err(error) => PatchRedriveCandidate::Corrupt {
            identity: patch_key,
            error,
        },
    }
}

fn patch_discrepancy_detail(
    status: PatchLifecycleStatus,
    patch_key: Uuid,
    applied_patch_keys: &HashSet<Uuid>,
) -> Option<String> {
    match status {
        PatchLifecycleStatus::Validating
        | PatchLifecycleStatus::Building
        | PatchLifecycleStatus::Submitted => Some(format!(
            "patch unsettled at terminal compaction (ledger status {}); never reached an outcome",
            status.as_str()
        )),
        PatchLifecycleStatus::Applied if !applied_patch_keys.contains(&patch_key) => Some(
            "patch ledger records Applied but patch_key is absent from the terminal applied-patch log"
                .to_owned(),
        ),
        PatchLifecycleStatus::Applied
        | PatchLifecycleStatus::Rejected
        | PatchLifecycleStatus::BuildFailed => None,
    }
}

async fn audit_patch_settlement_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
    applied_patch_keys: &HashSet<Uuid>,
) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let ledger: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT patch_key, status FROM workflow_patches \
         WHERE workflow_instance_id = $1 ORDER BY patch_key ASC",
    )
    .bind(workflow_instance_id)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let detected_at = Utc::now();
    let mut discrepancies = Vec::new();
    for (patch_key, stored_status) in ledger {
        let ledger_status = PatchLifecycleStatus::from_stored(&stored_status)?;
        let Some(detail) = patch_discrepancy_detail(ledger_status, patch_key, applied_patch_keys)
        else {
            continue;
        };
        sqlx::query(
            "INSERT INTO workflow_patch_discrepancies \
                 (workflow_instance_id, patch_key, ledger_status, detail, detected_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (workflow_instance_id, patch_key) DO UPDATE SET \
                 ledger_status = EXCLUDED.ledger_status, detail = EXCLUDED.detail, \
                 detected_at = EXCLUDED.detected_at",
        )
        .bind(workflow_instance_id)
        .bind(patch_key)
        .bind(ledger_status.as_str())
        .bind(&detail)
        .bind(detected_at)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        discrepancies.push(PatchSettlementDiscrepancy {
            workflow_instance_id,
            patch_key,
            ledger_status,
            detail,
            detected_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(discrepancies)
}

async fn audit_patch_settlement_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
    applied_patch_keys: &HashSet<Uuid>,
) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_instance_id = encode_uuid(workflow_instance_id);
    let ledger: Vec<(String, String)> = sqlx::query_as(
        "SELECT patch_key, status FROM workflow_patches \
         WHERE workflow_instance_id = ?1 ORDER BY patch_key ASC",
    )
    .bind(&encoded_instance_id)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let detected_at = Utc::now();
    let encoded_detected_at = encode_timestamp(detected_at);
    let mut discrepancies = Vec::new();
    for (stored_patch_key, stored_status) in ledger {
        let patch_key = decode_uuid(&stored_patch_key).map_err(corrupt_value)?;
        let ledger_status = PatchLifecycleStatus::from_stored(&stored_status)?;
        let Some(detail) = patch_discrepancy_detail(ledger_status, patch_key, applied_patch_keys)
        else {
            continue;
        };
        sqlx::query(
            "INSERT INTO workflow_patch_discrepancies \
                 (workflow_instance_id, patch_key, ledger_status, detail, detected_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (workflow_instance_id, patch_key) DO UPDATE SET \
                 ledger_status = excluded.ledger_status, detail = excluded.detail, \
                 detected_at = excluded.detected_at",
        )
        .bind(&encoded_instance_id)
        .bind(&stored_patch_key)
        .bind(ledger_status.as_str())
        .bind(&detail)
        .bind(encoded_detected_at)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        discrepancies.push(PatchSettlementDiscrepancy {
            workflow_instance_id,
            patch_key,
            ledger_status,
            detail,
            detected_at,
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(discrepancies)
}

async fn patch_settlement_discrepancies_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
    let rows: Vec<(Uuid, Uuid, String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT workflow_instance_id, patch_key, ledger_status, detail, detected_at \
         FROM workflow_patch_discrepancies WHERE workflow_instance_id = $1 \
         ORDER BY patch_key ASC",
    )
    .bind(workflow_instance_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(workflow_instance_id, patch_key, ledger_status, detail, detected_at)| {
                Ok(PatchSettlementDiscrepancy {
                    workflow_instance_id,
                    patch_key,
                    ledger_status: PatchLifecycleStatus::from_stored(&ledger_status)?,
                    detail,
                    detected_at,
                })
            },
        )
        .collect()
}

async fn patch_settlement_discrepancies_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Vec<PatchSettlementDiscrepancy>, RepositoryError> {
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "SELECT workflow_instance_id, patch_key, ledger_status, detail, detected_at \
         FROM workflow_patch_discrepancies WHERE workflow_instance_id = ?1 \
         ORDER BY patch_key ASC",
    )
    .bind(encode_uuid(workflow_instance_id))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(workflow_instance_id, patch_key, ledger_status, detail, detected_at)| {
                Ok(PatchSettlementDiscrepancy {
                    workflow_instance_id: decode_uuid(&workflow_instance_id)
                        .map_err(corrupt_value)?,
                    patch_key: decode_uuid(&patch_key).map_err(corrupt_value)?,
                    ledger_status: PatchLifecycleStatus::from_stored(&ledger_status)?,
                    detail,
                    detected_at: decode_timestamp(detected_at).map_err(corrupt_value)?,
                })
            },
        )
        .collect()
}

type PostgresLifecycleTuple = (
    Uuid,
    Uuid,
    Uuid,
    String,
    Value,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    Option<Value>,
);

type SqliteLifecycleTuple = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    Option<String>,
);

async fn patch_lifecycle_postgres<'e, E>(
    executor: E,
    patch_key: Uuid,
) -> Result<Option<PatchLifecycleRow>, RepositoryError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let row: Option<PostgresLifecycleTuple> = sqlx::query_as(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, applied_version, provenance, operation FROM workflow_patches WHERE patch_key = $1",
    )
    .bind(patch_key)
    .fetch_optional(executor)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(decode_postgres_lifecycle).transpose()
}

async fn patch_lifecycle_sqlite<'e, E>(
    executor: E,
    patch_key: Uuid,
) -> Result<Option<PatchLifecycleRow>, RepositoryError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let row: Option<SqliteLifecycleTuple> = sqlx::query_as(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, applied_version, provenance, operation FROM workflow_patches WHERE patch_key = ?1",
    )
    .bind(encode_uuid(patch_key))
    .fetch_optional(executor)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(decode_sqlite_lifecycle).transpose()
}

fn decode_postgres_lifecycle(
    row: PostgresLifecycleTuple,
) -> Result<PatchLifecycleRow, RepositoryError> {
    let (
        patch_key,
        patch_id,
        workflow_instance_id,
        status,
        ops,
        reason,
        outcome,
        applied_version,
        provenance,
        operation,
    ) = row;
    decode_lifecycle(
        patch_key,
        patch_id,
        workflow_instance_id,
        status,
        ops,
        reason,
        outcome,
        applied_version,
        provenance,
        operation,
    )
}

fn decode_sqlite_lifecycle(
    row: SqliteLifecycleTuple,
) -> Result<PatchLifecycleRow, RepositoryError> {
    let (
        patch_key,
        patch_id,
        workflow_instance_id,
        status,
        ops,
        reason,
        outcome,
        applied_version,
        provenance,
        operation,
    ) = row;
    decode_lifecycle(
        decode_uuid(&patch_key).map_err(corrupt_value)?,
        decode_uuid(&patch_id).map_err(corrupt_value)?,
        decode_uuid(&workflow_instance_id).map_err(corrupt_value)?,
        status,
        decode_json(&ops).map_err(corrupt_value)?,
        reason,
        outcome,
        applied_version,
        provenance,
        operation
            .map(|value| decode_json(&value).map_err(corrupt_value))
            .transpose()?,
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_lifecycle(
    patch_key: Uuid,
    patch_id: Uuid,
    workflow_instance_id: Uuid,
    status: String,
    ops: Value,
    reason: Option<String>,
    outcome: Option<String>,
    applied_version: Option<i64>,
    provenance: String,
    operation: Option<Value>,
) -> Result<PatchLifecycleRow, RepositoryError> {
    Ok(PatchLifecycleRow {
        patch_key,
        patch_id,
        workflow_instance_id,
        status: PatchLifecycleStatus::from_stored(&status)?,
        ops: serde_json::from_value(ops).map_err(corrupt_value)?,
        reason,
        outcome,
        applied_version,
        provenance: PatchProvenance::from_stored(&provenance)?,
        operation: operation
            .map(|value| serde_json::from_value(value).map_err(corrupt_value))
            .transpose()?,
    })
}

async fn patch_status_postgres(
    pool: &PgPool,
    patch_id: Uuid,
) -> Result<Option<PatchStatusRow>, RepositoryError> {
    type Row = (
        Uuid,
        Uuid,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        DateTime<Utc>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at FROM workflow_patches WHERE patch_id = $1",
    )
    .bind(patch_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(
        |(patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at)| {
            Ok(PatchStatusRow {
                patch_id,
                workflow_instance_id,
                status: PatchLifecycleStatus::from_stored(&status)?,
                outcome,
                reason,
                applied_version,
                updated_at,
            })
        },
    )
    .transpose()
}

async fn patch_status_sqlite(
    pool: &SqlitePool,
    patch_id: Uuid,
) -> Result<Option<PatchStatusRow>, RepositoryError> {
    type Row = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        i64,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at FROM workflow_patches WHERE patch_id = ?1",
    )
    .bind(encode_uuid(patch_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(
        |(patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at)| {
            Ok(PatchStatusRow {
                patch_id: decode_uuid(&patch_id).map_err(corrupt_value)?,
                workflow_instance_id: decode_uuid(&workflow_instance_id).map_err(corrupt_value)?,
                status: PatchLifecycleStatus::from_stored(&status)?,
                outcome,
                reason,
                applied_version,
                updated_at: decode_timestamp(updated_at).map_err(corrupt_value)?,
            })
        },
    )
    .transpose()
}

async fn patch_source_postgres(
    pool: &PgPool,
    patch_id: Uuid,
) -> Result<Option<PatchSourceRow>, RepositoryError> {
    type Row = (Uuid, Uuid, Option<String>, Option<String>, Option<i64>);
    let row: Option<Row> = sqlx::query_as(
        "SELECT patch_id, workflow_instance_id, source, source_format, applied_version FROM workflow_patches WHERE patch_id = $1",
    )
    .bind(patch_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(
        |(patch_id, workflow_instance_id, source, source_format, applied_version)| {
            Ok(PatchSourceRow {
                patch_id,
                workflow_instance_id,
                source: decode_source(source, source_format)?,
                applied_version,
            })
        },
    )
    .transpose()
}

async fn patch_source_sqlite(
    pool: &SqlitePool,
    patch_id: Uuid,
) -> Result<Option<PatchSourceRow>, RepositoryError> {
    type Row = (String, String, Option<String>, Option<String>, Option<i64>);
    let row: Option<Row> = sqlx::query_as(
        "SELECT patch_id, workflow_instance_id, source, source_format, applied_version FROM workflow_patches WHERE patch_id = ?1",
    )
    .bind(encode_uuid(patch_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(
        |(patch_id, workflow_instance_id, source, source_format, applied_version)| {
            Ok(PatchSourceRow {
                patch_id: decode_uuid(&patch_id).map_err(corrupt_value)?,
                workflow_instance_id: decode_uuid(&workflow_instance_id).map_err(corrupt_value)?,
                source: decode_source(source, source_format)?,
                applied_version,
            })
        },
    )
    .transpose()
}

fn decode_source(
    source: Option<String>,
    source_format: Option<String>,
) -> Result<Option<PatchSource>, RepositoryError> {
    match (source, source_format) {
        (None, None) => Ok(None),
        (Some(text), Some(format)) => Ok(Some(PatchSource {
            text,
            format: PatchSourceFormat::from_stored(&format)?,
        })),
        _ => Err(corrupt_value(
            "Patch source and source_format must both be null or both be present".to_owned(),
        )),
    }
}

fn internal_value(error: serde_json::Error) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Internal, error)
}

fn corrupt_value(error: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CorruptStoredValue,
        CorruptPatchValue(error.to_string()),
    )
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

    fn patch_key(workflow_instance_id: Uuid, patch_id: Uuid) -> Uuid {
        Uuid::new_v5(&workflow_instance_id, patch_id.as_bytes())
    }

    async fn ingress(
        writer: &WriterRepositoryBundle,
        workflow_instance_id: Uuid,
        patch_id: Uuid,
        provenance: PatchProvenance,
        source: &str,
        source_format: PatchSourceFormat,
        routing_vars: Option<(Uuid, &[wf::RoutingVarDecl])>,
    ) -> Result<PatchIngressOutcome, RepositoryError> {
        let tasks = routing_vars
            .into_iter()
            .map(|(task_id, routing_vars)| PatchTaskSpecification {
                task_id,
                routing_vars,
            })
            .collect();
        writer
            .ingress_patch(PatchIngressInput {
                patch_key: patch_key(workflow_instance_id, patch_id),
                patch_id,
                workflow_instance_id,
                ops: &[],
                operation: None,
                reason: Some("repository law"),
                provenance,
                source,
                source_format,
                tasks,
            })
            .await
    }

    async fn ingress_build(
        writer: &WriterRepositoryBundle,
        workflow_instance_id: Uuid,
        patch_id: Uuid,
        task_ids: &[Uuid],
    ) -> PatchIngressOutcome {
        let tasks = task_ids
            .iter()
            .map(|task_id| PatchTaskSpecification {
                task_id: *task_id,
                routing_vars: &[],
            })
            .collect();
        writer
            .ingress_patch(PatchIngressInput {
                patch_key: patch_key(workflow_instance_id, patch_id),
                patch_id,
                workflow_instance_id,
                ops: &[],
                operation: None,
                reason: Some("build settlement law"),
                provenance: PatchProvenance::External,
                source: "build settlement law",
                source_format: PatchSourceFormat::Nickel,
                tasks,
            })
            .await
            .unwrap()
    }

    async fn corrupt_read_values(
        writer: &WriterRepositoryBundle,
        status_patch_id: Uuid,
        source_patch_id: Uuid,
    ) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "ALTER TABLE workflow_patches DROP CONSTRAINT workflow_patches_status_check",
                )
                .execute(pool)
                .await
                .unwrap();
                sqlx::query(
                    "UPDATE workflow_patches SET status = 'UnknownPatchState' WHERE patch_id = $1",
                )
                .bind(status_patch_id)
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("UPDATE workflow_patches SET source_format = NULL WHERE patch_id = $1")
                    .bind(source_patch_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                let mut connection = pool.acquire().await.unwrap();
                sqlx::query("PRAGMA ignore_check_constraints = ON")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE workflow_patches SET status = 'UnknownPatchState' WHERE patch_id = ?1",
                )
                .bind(encode_uuid(status_patch_id))
                .execute(&mut *connection)
                .await
                .unwrap();
                sqlx::query("UPDATE workflow_patches SET source_format = NULL WHERE patch_id = ?1")
                    .bind(encode_uuid(source_patch_id))
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            }
        }
    }

    async fn prepare_redrive_order_and_corruption(
        writer: &WriterRepositoryBundle,
        corrupt_patch_key: Uuid,
    ) {
        let stable_time = Utc::now() - chrono::Duration::hours(1);
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE workflow_patches SET updated_at = $1 \
                     WHERE status IN ('Validating', 'Submitted')",
                )
                .bind(stable_time)
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("UPDATE workflow_patches SET ops = $2 WHERE patch_key = $1")
                    .bind(corrupt_patch_key)
                    .bind(serde_json::json!({"not": "a Patch operation list"}))
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE workflow_patches SET updated_at = ?1 \
                     WHERE status IN ('Validating', 'Submitted')",
                )
                .bind(encode_timestamp(stable_time))
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("UPDATE workflow_patches SET ops = ?2 WHERE patch_key = ?1")
                    .bind(encode_uuid(corrupt_patch_key))
                    .bind(r#"{"not":"a Patch operation list"}"#)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn assert_classified_contention(
        selection: &DataPlaneSql,
        busy_patch_key: Uuid,
        busy_task_id: Uuid,
    ) {
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
                    .settle_patch_task_build(
                        busy_patch_key,
                        busy_task_id,
                        PatchTaskBuildResult::Success,
                    )
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
                    .settle_patch_task_build(
                        busy_patch_key,
                        busy_task_id,
                        PatchTaskBuildResult::Success,
                    )
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
            let error = repository
                .settle_patch_task_build(
                    busy_patch_key,
                    busy_task_id,
                    PatchTaskBuildResult::Success,
                )
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

        let external_instance = Uuid::new_v4();
        let external_id = Uuid::new_v4();
        let external_source = "{ ops = [{ op = 'remove_node, node_id = \"aB3d\" }] }";
        assert_eq!(
            ingress(
                &writer,
                external_instance,
                external_id,
                PatchProvenance::External,
                external_source,
                PatchSourceFormat::Nickel,
                None,
            )
            .await
            .unwrap(),
            PatchIngressOutcome::Accepted {
                status: PatchLifecycleStatus::Validating
            }
        );

        let duplicate = ingress(
            &writer,
            external_instance,
            external_id,
            PatchProvenance::External,
            "different redelivery payload",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        match duplicate {
            PatchIngressOutcome::Existing { row } => {
                assert_eq!(row.patch_id, external_id);
                assert_eq!(row.status, PatchLifecycleStatus::Validating);
            }
            other => panic!("redelivery did not reuse its row: {other:?}"),
        }

        let race_instance = Uuid::new_v4();
        let first_race_id = Uuid::new_v4();
        let second_race_id = Uuid::new_v4();
        let first = ingress(
            &writer,
            race_instance,
            first_race_id,
            PatchProvenance::External,
            "first",
            PatchSourceFormat::Nickel,
            None,
        );
        let second = ingress(
            &writer,
            race_instance,
            second_race_id,
            PatchProvenance::SelfEmitted,
            "{}",
            PatchSourceFormat::Json,
            None,
        );
        let (first, second) = tokio::join!(first, second);
        let outcomes = [first.unwrap(), second.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, PatchIngressOutcome::Accepted { .. }))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| {
                    matches!(outcome, PatchIngressOutcome::RejectedInProgress { .. })
                })
                .count(),
            1
        );

        let self_instance = Uuid::new_v4();
        let self_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let routing_vars = vec![wf::RoutingVarDecl {
            name: "decision".to_owned(),
            var_type: Some("string".to_owned()),
        }];
        let self_source = r#"{"ops":[{"AddNode":{"node_id":"fresh"}}]}"#;
        assert_eq!(
            ingress(
                &writer,
                self_instance,
                self_id,
                PatchProvenance::SelfEmitted,
                self_source,
                PatchSourceFormat::Json,
                Some((task_id, &routing_vars)),
            )
            .await
            .unwrap(),
            PatchIngressOutcome::Accepted {
                status: PatchLifecycleStatus::Building
            }
        );
        assert_eq!(
            writer.task_specification(task_id).await.unwrap(),
            Some(routing_vars.clone())
        );

        let built = writer
            .settle_patch_task_build(
                patch_key(self_instance, self_id),
                task_id,
                PatchTaskBuildResult::Success,
            )
            .await
            .unwrap();
        match built {
            PatchBuildSettlementOutcome::Submitted(row) => {
                assert_eq!(row.status, PatchLifecycleStatus::Submitted);
                assert_eq!(row.patch_id, self_id);
            }
            other => panic!("successful patched Task did not submit its Patch: {other:?}"),
        }
        assert_eq!(
            writer.task_specification(task_id).await.unwrap(),
            Some(routing_vars.clone()),
            "a built patched Task retains the shared routing specification"
        );

        let mixed_instance = Uuid::new_v4();
        let mixed_id = Uuid::new_v4();
        let mixed_tasks = [Uuid::new_v4(), Uuid::new_v4()];
        assert_eq!(
            ingress_build(&writer, mixed_instance, mixed_id, &mixed_tasks).await,
            PatchIngressOutcome::Accepted {
                status: PatchLifecycleStatus::Building
            }
        );
        assert_eq!(
            writer
                .settle_patch_task_build(
                    patch_key(mixed_instance, mixed_id),
                    mixed_tasks[0],
                    PatchTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            PatchBuildSettlementOutcome::AwaitingTasks
        );
        assert_eq!(
            writer
                .settle_patch_task_build(
                    patch_key(mixed_instance, mixed_id),
                    mixed_tasks[0],
                    PatchTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            PatchBuildSettlementOutcome::TaskAlreadySettled
        );
        assert_eq!(
            writer
                .settle_patch_task_build(
                    patch_key(mixed_instance, mixed_id),
                    mixed_tasks[1],
                    PatchTaskBuildResult::Failure {
                        error: "mixed outcome failure",
                    },
                )
                .await
                .unwrap(),
            PatchBuildSettlementOutcome::BuildFailed
        );
        assert_eq!(
            writer
                .settle_patch_task_build(
                    patch_key(mixed_instance, mixed_id),
                    mixed_tasks[1],
                    PatchTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            PatchBuildSettlementOutcome::AlreadySettled(PatchLifecycleStatus::BuildFailed)
        );

        const FINALIZERS: usize = 8;
        let concurrent_instance = Uuid::new_v4();
        let concurrent_id = Uuid::new_v4();
        let concurrent_tasks = (0..FINALIZERS).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        assert_eq!(
            ingress_build(
                &writer,
                concurrent_instance,
                concurrent_id,
                &concurrent_tasks,
            )
            .await,
            PatchIngressOutcome::Accepted {
                status: PatchLifecycleStatus::Building
            }
        );
        let handles = concurrent_tasks
            .iter()
            .copied()
            .map(|task_id| {
                let writer = writer.clone();
                tokio::spawn(async move {
                    writer
                        .settle_patch_task_build(
                            patch_key(concurrent_instance, concurrent_id),
                            task_id,
                            PatchTaskBuildResult::Success,
                        )
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut submitted = 0;
        let mut awaiting = 0;
        for handle in handles {
            match handle.await.unwrap() {
                PatchBuildSettlementOutcome::Submitted(intent) => {
                    submitted += 1;
                    assert_eq!(intent.status, PatchLifecycleStatus::Submitted);
                }
                PatchBuildSettlementOutcome::AwaitingTasks => awaiting += 1,
                other => panic!("unexpected concurrent Patch finalizer outcome: {other:?}"),
            }
        }
        assert_eq!(submitted, 1, "exactly one apply publication intent wins");
        assert_eq!(awaiting, FINALIZERS - 1);

        let correlation_instance = Uuid::new_v4();
        let correlation_id = Uuid::new_v4();
        let correlation_key = patch_key(correlation_instance, correlation_id);
        ingress(
            &writer,
            correlation_instance,
            correlation_id,
            PatchProvenance::External,
            "correlation race",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        let applied = writer.correlate_patch_outcome(PatchOutcomeInput {
            patch_key: correlation_key,
            workflow_instance_id: correlation_instance,
            outcome: PatchTerminalOutcome::Applied { version: 11 },
        });
        let rejected = writer.correlate_patch_outcome(PatchOutcomeInput {
            patch_key: correlation_key,
            workflow_instance_id: correlation_instance,
            outcome: PatchTerminalOutcome::Rejected {
                reason: "concurrent rejection",
            },
        });
        let (applied, rejected) = tokio::join!(applied, rejected);
        let correlations = [applied.unwrap(), rejected.unwrap()];
        assert_eq!(
            correlations
                .iter()
                .filter(|outcome| **outcome == PatchOutcomeCorrelation::Won)
                .count(),
            1,
            "exactly one terminal correlation wins"
        );
        assert_eq!(
            correlations
                .iter()
                .filter(|outcome| { matches!(outcome, PatchOutcomeCorrelation::AlreadySettled(_)) })
                .count(),
            1,
            "the concurrent loser observes the durable terminal state"
        );
        assert_eq!(
            writer
                .correlate_patch_outcome(PatchOutcomeInput {
                    patch_key: Uuid::new_v4(),
                    workflow_instance_id: Uuid::new_v4(),
                    outcome: PatchTerminalOutcome::Applied { version: 1 },
                })
                .await
                .unwrap(),
            PatchOutcomeCorrelation::Absent
        );

        let conflict_instance = Uuid::new_v4();
        let conflict_id = Uuid::new_v4();
        let conflict_key = patch_key(conflict_instance, conflict_id);
        ingress(
            &writer,
            conflict_instance,
            conflict_id,
            PatchProvenance::External,
            "identity conflict",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            writer
                .correlate_patch_outcome(PatchOutcomeInput {
                    patch_key: conflict_key,
                    workflow_instance_id: Uuid::new_v4(),
                    outcome: PatchTerminalOutcome::Applied { version: 1 },
                })
                .await
                .unwrap(),
            PatchOutcomeCorrelation::Conflicted
        );

        let audit_instance = Uuid::new_v4();
        let audit_applied_id = Uuid::new_v4();
        let audit_applied_key = patch_key(audit_instance, audit_applied_id);
        ingress(
            &writer,
            audit_instance,
            audit_applied_id,
            PatchProvenance::External,
            "applied audit row",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            writer
                .correlate_patch_outcome(PatchOutcomeInput {
                    patch_key: audit_applied_key,
                    workflow_instance_id: audit_instance,
                    outcome: PatchTerminalOutcome::Applied { version: 3 },
                })
                .await
                .unwrap(),
            PatchOutcomeCorrelation::Won
        );
        let audit_rejected_id = Uuid::new_v4();
        let audit_rejected_key = patch_key(audit_instance, audit_rejected_id);
        ingress(
            &writer,
            audit_instance,
            audit_rejected_id,
            PatchProvenance::External,
            "rejected audit row",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            writer
                .correlate_patch_outcome(PatchOutcomeInput {
                    patch_key: audit_rejected_key,
                    workflow_instance_id: audit_instance,
                    outcome: PatchTerminalOutcome::Rejected {
                        reason: "not applicable",
                    },
                })
                .await
                .unwrap(),
            PatchOutcomeCorrelation::Won
        );
        let audit_unsettled_id = Uuid::new_v4();
        let audit_unsettled_key = patch_key(audit_instance, audit_unsettled_id);
        ingress(
            &writer,
            audit_instance,
            audit_unsettled_id,
            PatchProvenance::External,
            "unsettled audit row",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        let applied_patch_keys = HashSet::from([audit_applied_key]);
        let discrepancies = writer
            .audit_patch_settlement(audit_instance, &applied_patch_keys)
            .await
            .unwrap();
        assert_eq!(discrepancies.len(), 1);
        assert_eq!(discrepancies[0].patch_key, audit_unsettled_key);
        assert_eq!(
            writer
                .audit_patch_settlement(audit_instance, &applied_patch_keys)
                .await
                .unwrap()
                .len(),
            1,
            "terminal Compaction redelivery reuses the discrepancy identity"
        );
        let stored_discrepancies = writer
            .patch_settlement_discrepancies(audit_instance)
            .await
            .unwrap();
        assert_eq!(stored_discrepancies.len(), 1);
        assert_eq!(stored_discrepancies[0].patch_key, audit_unsettled_key);

        let corrupt_instance = Uuid::new_v4();
        let corrupt_id = Uuid::new_v4();
        let corrupt_key = patch_key(corrupt_instance, corrupt_id);
        ingress(
            &writer,
            corrupt_instance,
            corrupt_id,
            PatchProvenance::External,
            "corrupt redrive row",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        let healthy_instance = Uuid::new_v4();
        let healthy_id = Uuid::new_v4();
        let healthy_key = patch_key(healthy_instance, healthy_id);
        ingress(
            &writer,
            healthy_instance,
            healthy_id,
            PatchProvenance::External,
            "healthy redrive row",
            PatchSourceFormat::Nickel,
            None,
        )
        .await
        .unwrap();
        prepare_redrive_order_and_corruption(&writer, corrupt_key).await;
        let candidates = writer
            .unsettled_patches_older_than(Duration::ZERO)
            .await
            .unwrap();
        let identities = candidates
            .iter()
            .map(|candidate| match candidate {
                PatchRedriveCandidate::Ready(row) => row.patch_key,
                PatchRedriveCandidate::Corrupt { identity, error } => {
                    assert_eq!(error.kind(), RepositoryErrorKind::CorruptStoredValue);
                    Uuid::parse_str(identity).unwrap()
                }
            })
            .collect::<Vec<_>>();
        let mut sorted_identities = identities.clone();
        sorted_identities.sort_unstable();
        assert_eq!(
            identities, sorted_identities,
            "equal-time re-drive candidates use the Patch identity tie-break"
        );
        assert!(candidates.iter().any(
            |candidate| matches!(candidate, PatchRedriveCandidate::Ready(row) if row.patch_key == healthy_key)
        ));
        assert!(candidates.iter().any(
            |candidate| matches!(candidate, PatchRedriveCandidate::Corrupt { identity, .. } if identity == &corrupt_key.to_string())
        ));

        let busy_instance = Uuid::new_v4();
        let busy_id = Uuid::new_v4();
        let busy_task_id = Uuid::new_v4();
        assert_eq!(
            ingress_build(&writer, busy_instance, busy_id, &[busy_task_id]).await,
            PatchIngressOutcome::Accepted {
                status: PatchLifecycleStatus::Building
            }
        );
        assert_classified_contention(&selection, patch_key(busy_instance, busy_id), busy_task_id)
            .await;

        let reader = factory.open_read_only().await.unwrap();
        let external_status = reader.patch_status(external_id).await.unwrap().unwrap();
        assert_eq!(external_status.status, PatchLifecycleStatus::Validating);
        let external_read = reader.patch_source(external_id).await.unwrap().unwrap();
        assert_eq!(
            external_read.source,
            Some(PatchSource {
                text: external_source.to_owned(),
                format: PatchSourceFormat::Nickel,
            })
        );
        let self_read = reader.patch_source(self_id).await.unwrap().unwrap();
        assert_eq!(
            self_read.source,
            Some(PatchSource {
                text: self_source.to_owned(),
                format: PatchSourceFormat::Json,
            })
        );
        assert_eq!(
            reader.patch_status(self_id).await.unwrap().unwrap().status,
            PatchLifecycleStatus::Submitted,
            "discarding the returned intent leaves the committed publication state"
        );
        assert_eq!(
            reader.patch_status(mixed_id).await.unwrap().unwrap().status,
            PatchLifecycleStatus::BuildFailed
        );
        assert_eq!(
            reader
                .patch_status(concurrent_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::Submitted
        );

        reader.close().await;
        writer.close().await;

        let reopened_writer = factory.open_writer().await.unwrap();
        let reopened_reader = factory.open_read_only().await.unwrap();
        assert_eq!(
            reopened_writer
                .patch_settlement_discrepancies(audit_instance)
                .await
                .unwrap()
                .len(),
            1,
            "the terminal audit survives a file-backed restart"
        );
        assert!(reopened_writer
            .unsettled_patches_older_than(Duration::ZERO)
            .await
            .unwrap()
            .iter()
            .any(
                |candidate| matches!(candidate, PatchRedriveCandidate::Ready(row) if row.patch_key == healthy_key)
            ));
        assert_eq!(
            reopened_reader
                .patch_status(external_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::Validating,
            "an unrelayed committed ingress remains available after restart"
        );
        assert_eq!(
            reopened_writer.task_specification(task_id).await.unwrap(),
            Some(routing_vars)
        );
        assert_eq!(
            reopened_reader
                .patch_status(self_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::Submitted
        );
        assert_eq!(
            reopened_reader
                .patch_status(mixed_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::BuildFailed
        );
        assert_eq!(
            reopened_reader
                .patch_status(concurrent_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::Submitted
        );
        assert!(matches!(
            ingress(
                &reopened_writer,
                external_instance,
                external_id,
                PatchProvenance::External,
                external_source,
                PatchSourceFormat::Nickel,
                None,
            )
            .await
            .unwrap(),
            PatchIngressOutcome::Existing { .. }
        ));
        assert_eq!(
            reopened_writer
                .mark_patch_submitted(patch_key(external_instance, external_id))
                .await
                .unwrap(),
            PatchSubmissionOutcome::Submitted
        );
        assert_eq!(
            reopened_reader
                .patch_status(external_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PatchLifecycleStatus::Submitted
        );

        corrupt_read_values(&reopened_writer, external_id, self_id).await;
        assert_eq!(
            reopened_reader
                .patch_status(external_id)
                .await
                .unwrap_err()
                .kind(),
            RepositoryErrorKind::CorruptStoredValue
        );
        assert_eq!(
            reopened_reader
                .patch_source(self_id)
                .await
                .unwrap_err()
                .kind(),
            RepositoryErrorKind::CorruptStoredValue
        );

        reopened_reader.close().await;
        reopened_writer.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_patch_repository_laws() {
        let container = match Postgres::default()
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping: Postgres testcontainer unavailable: {error}");
                return;
            }
        };
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let migration_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
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
    async fn file_backed_sqlite_patch_repository_laws() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("patches.db"));
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;
        run_laws(DataPlaneSql::Sqlite { url }).await;
    }
}
