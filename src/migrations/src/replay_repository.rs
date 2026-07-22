//! Replay archive composition and lifecycle persistence.
//!
//! Replay callers receive decoded, backend-neutral source state and lifecycle
//! outcomes. SQL dialects, transactions, rows, and storage encodings remain
//! private to the selected repository bundle.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, SqlitePool};
use tickr_proto::codec::archive::archive_projection_from_json;
use tickr_proto::runnable as rp;
use tickr_proto::workflow as wf;
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{
    decode_json, decode_timestamp, decode_uuid, encode_json, encode_timestamp, encode_uuid,
};

pub const STATUS_MATERIALIZING: &str = "Materializing";
pub const STATUS_RELEASED: &str = "Released";
pub const STATUS_VERSION_UNRESOLVABLE: &str = "VersionUnresolvable";
const REPLAY_STATUSES: &[&str] = &[
    STATUS_MATERIALIZING,
    STATUS_RELEASED,
    STATUS_VERSION_UNRESOLVABLE,
];
const TERMINAL_WORKFLOW_STATES: &[&str] = &["Completed", "Failed"];
const MAX_REPLAY_LEASE_BATCH: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayLifecycleStatus {
    Materializing,
    Released,
    VersionUnresolvable,
}

impl ReplayLifecycleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Materializing => STATUS_MATERIALIZING,
            Self::Released => STATUS_RELEASED,
            Self::VersionUnresolvable => STATUS_VERSION_UNRESOLVABLE,
        }
    }

    fn from_stored(value: &str) -> Result<Self, RepositoryError> {
        match value {
            STATUS_MATERIALIZING => Ok(Self::Materializing),
            STATUS_RELEASED => Ok(Self::Released),
            STATUS_VERSION_UNRESOLVABLE => Ok(Self::VersionUnresolvable),
            other => Err(corrupt_value(CorruptReplay(format!(
                "unknown lifecycle status `{other}`"
            )))),
        }
    }

    pub fn is_terminal(self) -> bool {
        self != Self::Materializing
    }
}

/// The terminal source state needed to validate and drive one replay.
#[derive(Debug, Clone)]
pub struct ReplaySource {
    pub workflow_id: Uuid,
    pub projection: rp::RunnableProjection,
    pub replay_source: Option<Uuid>,
    pub task_instances: Vec<ReplayArchivedTask>,
    pub ctx_envelope: Value,
    pub pinned_definition: Option<wf::WorkflowDefinition>,
}

/// Producer-attribution facts retained for each archived Task instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayArchivedTask {
    pub id: Uuid,
    pub node_id: Uuid,
}

/// One replay lifecycle row, decoded at the repository boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayLifecycleRow {
    pub replay_instance_id: Uuid,
    pub source_instance_id: Uuid,
    pub signal_id: Uuid,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub resume_from: Vec<Uuid>,
    pub pre_grounded: Vec<Uuid>,
    pub name: Option<String>,
    pub seed_sha256: Option<String>,
    pub shadowed_keys: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl ReplayLifecycleRow {
    pub fn is_settled(&self) -> bool {
        matches!(
            self.status.as_str(),
            STATUS_RELEASED | STATUS_VERSION_UNRESOLVABLE
        )
    }
}

/// Complete input for the replay row's repository-owned insert.
#[derive(Debug, Clone)]
pub struct ReplayLifecycleInput {
    pub replay_instance_id: Uuid,
    pub source_instance_id: Uuid,
    pub signal_id: Uuid,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub resume_from: Vec<Uuid>,
    pub pre_grounded: Vec<Uuid>,
    pub name: Option<String>,
    pub seed_sha256: Option<String>,
    pub outcome: Option<String>,
    pub shadowed_keys: Vec<String>,
}

/// The insert either commits this identity or returns the durable collision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayLifecycleInsertOutcome {
    Inserted,
    Existing(ReplayLifecycleRow),
}

/// One committed replay decision and the selected terminal source it names.
#[derive(Debug, Clone)]
pub struct ReplayDrive {
    pub lifecycle: ReplayLifecycleRow,
    pub source: ReplaySource,
}

/// Result of conditionally beginning one replay drive attempt.
#[derive(Debug, Clone)]
pub enum ReplayDriveLoadOutcome {
    Ready(ReplayDrive),
    SourceUnavailable(ReplayLifecycleRow),
    AlreadySettled(ReplayLifecycleStatus),
    Absent,
}

/// One independently decoded replay recovery candidate.
#[derive(Debug)]
pub enum ReplayRedriveCandidate {
    Ready(ReplayLifecycleRow),
    Corrupt {
        identity: String,
        error: RepositoryError,
    },
}

/// Result of the only terminal replay transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySettlementOutcome {
    Released,
    AlreadySettled(ReplayLifecycleStatus),
    Absent,
}

/// Bounded stable-selection request for Tickr Lite replay recovery.
#[derive(Debug, Clone, Copy)]
pub struct ReplayLeaseRequest<'a> {
    pub owner: &'a str,
    pub now: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub eligible_before: DateTime<Utc>,
    pub limit: usize,
}

/// One committed replay row and the lease that exclusively authorizes its
/// ordinary local drive attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct LeasedReplay {
    pub row: ReplayLifecycleRow,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

/// A bounded recovery scan isolates corrupt rows instead of blocking healthy
/// replay identities selected in the same transaction.
#[derive(Debug)]
pub enum ReplayLeaseCandidate {
    Ready(LeasedReplay),
    Corrupt {
        identity: String,
        error: RepositoryError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasedReplaySettlementOutcome {
    Settled(ReplaySettlementOutcome),
    LeaseLost,
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored replay value: {0}")]
struct CorruptReplay(String);

#[derive(Debug, thiserror::Error)]
enum ReplayLeaseError {
    #[error("replay lifecycle leases require SQLite")]
    RequiresSqlite,
    #[error("replay lifecycle lease owner must contain 1 to 128 bytes")]
    InvalidOwner,
    #[error("replay lifecycle lease expiry must be later than acquisition time")]
    InvalidExpiry,
    #[error("replay lifecycle lease batch must contain 1 to {MAX_REPLAY_LEASE_BATCH} rows")]
    InvalidLimit,
}

impl WriterRepositoryBundle {
    /// Compose the selected terminal archive and pinned definition reads.
    ///
    /// Absence means the terminal projection is unavailable. All other stored
    /// state is decoded and cross-checked before it reaches replay validation.
    pub async fn replay_source(
        &self,
        source_instance_id: Uuid,
    ) -> Result<Option<ReplaySource>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => replay_source_postgres(pool, source_instance_id).await,
            BackendPool::Sqlite(pool) => replay_source_sqlite(pool, source_instance_id).await,
        }
    }

    /// Insert exactly one lifecycle row, deduplicating by source/key inside the
    /// operation's transaction. Returning `Inserted` means the commit completed.
    pub async fn insert_replay_lifecycle(
        &self,
        input: &ReplayLifecycleInput,
    ) -> Result<ReplayLifecycleInsertOutcome, RepositoryError> {
        validate_lifecycle_input(input)?;
        match &self.pool {
            BackendPool::Postgres(pool) => insert_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => insert_sqlite(pool, input).await,
        }
    }

    pub async fn replay_lifecycle(
        &self,
        replay_instance_id: Uuid,
    ) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => lifecycle_postgres(pool, replay_instance_id).await,
            BackendPool::Sqlite(pool) => lifecycle_sqlite(pool, replay_instance_id).await,
        }
    }

    pub async fn replay_by_idempotency(
        &self,
        source_instance_id: Uuid,
        idempotency_key: &str,
    ) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                by_idempotency_postgres(pool, source_instance_id, idempotency_key).await
            }
            BackendPool::Sqlite(pool) => {
                by_idempotency_sqlite(pool, source_instance_id, idempotency_key).await
            }
        }
    }

    /// Conditionally begins one drive attempt and returns the committed
    /// lifecycle decisions with the selected terminal source.
    pub async fn load_replay_drive(
        &self,
        replay_instance_id: Uuid,
    ) -> Result<ReplayDriveLoadOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                load_replay_drive_postgres(pool, replay_instance_id).await
            }
            BackendPool::Sqlite(pool) => load_replay_drive_sqlite(pool, replay_instance_id).await,
        }
    }

    /// Conditionally commits `Materializing → Released`.
    pub async fn settle_replay_released(
        &self,
        replay_instance_id: Uuid,
    ) -> Result<ReplaySettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                settle_replay_released_postgres(pool, replay_instance_id).await
            }
            BackendPool::Sqlite(pool) => {
                settle_replay_released_sqlite(pool, replay_instance_id).await
            }
        }
    }

    /// Lease committed replay rows in durable stable order for Tickr Lite.
    pub async fn lease_replays(
        &self,
        request: ReplayLeaseRequest<'_>,
    ) -> Result<Vec<ReplayLeaseCandidate>, RepositoryError> {
        validate_replay_lease_request(request)?;
        match &self.pool {
            BackendPool::Postgres(_) => Err(replay_lease_error(ReplayLeaseError::RequiresSqlite)),
            BackendPool::Sqlite(pool) => lease_replays_sqlite(pool, request).await,
        }
    }

    /// Commit `Materializing → Released` only while this exact lease is live.
    pub async fn settle_leased_replay_released(
        &self,
        lease: &LeasedReplay,
        now: DateTime<Utc>,
    ) -> Result<LeasedReplaySettlementOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(_) => Err(replay_lease_error(ReplayLeaseError::RequiresSqlite)),
            BackendPool::Sqlite(pool) => {
                settle_leased_replay_released_sqlite(pool, lease, now).await
            }
        }
    }

    /// Release a failed local drive without acknowledging its durable row.
    pub async fn release_replay_lease(
        &self,
        lease: &LeasedReplay,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(_) => Err(replay_lease_error(ReplayLeaseError::RequiresSqlite)),
            BackendPool::Sqlite(pool) => release_replay_lease_sqlite(pool, lease).await,
        }
    }

    /// Stable scan of rows eligible for replay re-drive.
    pub async fn unsettled_replays_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<ReplayRedriveCandidate>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => unsettled_postgres(pool, cutoff).await,
            BackendPool::Sqlite(pool) => unsettled_sqlite(pool, cutoff).await,
        }
    }
}

impl ReadOnlyRepositoryBundle {
    /// Current replay status readback, newest first as in the public API.
    pub async fn replays_for_source(
        &self,
        source_instance_id: Uuid,
    ) -> Result<Vec<ReplayLifecycleRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                replays_for_source_postgres(pool, source_instance_id).await
            }
            BackendPool::Sqlite(pool) => replays_for_source_sqlite(pool, source_instance_id).await,
        }
    }
}

fn validate_replay_lease_request(request: ReplayLeaseRequest<'_>) -> Result<(), RepositoryError> {
    if request.owner.is_empty() || request.owner.len() > 128 {
        return Err(replay_lease_error(ReplayLeaseError::InvalidOwner));
    }
    if request.expires_at <= request.now {
        return Err(replay_lease_error(ReplayLeaseError::InvalidExpiry));
    }
    if !(1..=MAX_REPLAY_LEASE_BATCH).contains(&request.limit) {
        return Err(replay_lease_error(ReplayLeaseError::InvalidLimit));
    }
    Ok(())
}

fn replay_lease_error(error: ReplayLeaseError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, error)
}

async fn replay_source_postgres(
    pool: &PgPool,
    source_instance_id: Uuid,
) -> Result<Option<ReplaySource>, RepositoryError> {
    let row: Option<(Uuid, String, Value, Option<Value>)> = sqlx::query_as(
        "SELECT wi.workflow_id, wi.state, wi.instance, ri.ctx_envelope \
         FROM workflow_instances wi LEFT JOIN workflow_run_info ri \
           ON ri.workflow_instance_id = wi.id WHERE wi.id = $1",
    )
    .bind(source_instance_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some((workflow_id, state, blob, ctx_envelope)) = row else {
        return Ok(None);
    };
    let tasks: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, task_id FROM task_instances WHERE workflow_instance_id = $1 ORDER BY id",
    )
    .bind(source_instance_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let projection =
        decode_source_projection(source_instance_id, workflow_id, &state, blob.clone())?;
    let definition: Option<Value> =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(projection.workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    compose_source(
        projection,
        workflow_id,
        replay_source_from_blob(&blob),
        tasks
            .into_iter()
            .map(|(id, node_id)| ReplayArchivedTask { id, node_id })
            .collect(),
        ctx_envelope.unwrap_or_else(|| Value::Array(Vec::new())),
        definition,
    )
    .map(Some)
}

async fn replay_source_sqlite(
    pool: &SqlitePool,
    source_instance_id: Uuid,
) -> Result<Option<ReplaySource>, RepositoryError> {
    let source_id = encode_uuid(source_instance_id);
    let row: Option<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT wi.workflow_id, wi.state, wi.instance, ri.ctx_envelope \
         FROM workflow_instances wi LEFT JOIN workflow_run_info ri \
           ON ri.workflow_instance_id = wi.id WHERE wi.id = ?1",
    )
    .bind(&source_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some((stored_workflow_id, state, stored_blob, stored_ctx)) = row else {
        return Ok(None);
    };
    let workflow_id = decode_uuid(&stored_workflow_id).map_err(corrupt_value)?;
    let blob = decode_json(&stored_blob).map_err(corrupt_value)?;
    let tasks: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, task_id FROM task_instances WHERE workflow_instance_id = ?1 ORDER BY id",
    )
    .bind(&source_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let projection =
        decode_source_projection(source_instance_id, workflow_id, &state, blob.clone())?;
    let definition: Option<String> =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = ?1 AND version = ?2")
            .bind(&stored_workflow_id)
            .bind(projection.workflow_version)
            .fetch_optional(pool)
            .await
            .map_err(repository_sqlx_error)?;
    compose_source(
        projection,
        workflow_id,
        replay_source_from_blob(&blob),
        tasks
            .into_iter()
            .map(|(id, node_id)| {
                Ok(ReplayArchivedTask {
                    id: decode_uuid(&id).map_err(corrupt_value)?,
                    node_id: decode_uuid(&node_id).map_err(corrupt_value)?,
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?,
        stored_ctx
            .map(|value| decode_json(&value).map_err(corrupt_value))
            .transpose()?
            .unwrap_or_else(|| Value::Array(Vec::new())),
        definition
            .map(|value| decode_json(&value).map_err(corrupt_value))
            .transpose()?,
    )
    .map(Some)
}

fn decode_source_projection(
    source_instance_id: Uuid,
    workflow_id: Uuid,
    state: &str,
    blob: Value,
) -> Result<rp::RunnableProjection, RepositoryError> {
    if !TERMINAL_WORKFLOW_STATES.contains(&state) {
        return Err(corrupt_value(CorruptReplay(format!(
            "source {source_instance_id} has non-terminal state `{state}`"
        ))));
    }
    let archive = archive_projection_from_json(blob)
        .map_err(|error| corrupt_value(CorruptReplay(error.to_string())))?;
    let archive_id = Uuid::parse_str(&archive.id)
        .map_err(|error| corrupt_value(CorruptReplay(error.to_string())))?;
    let archive_workflow_id = Uuid::parse_str(&archive.workflow_id)
        .map_err(|error| corrupt_value(CorruptReplay(error.to_string())))?;
    if archive_id != source_instance_id || archive_workflow_id != workflow_id {
        return Err(corrupt_value(CorruptReplay(format!(
            "archive identity disagrees with source row {source_instance_id}"
        ))));
    }
    archive.runnable.ok_or_else(|| {
        corrupt_value(CorruptReplay(
            "archive carries no runnable projection".into(),
        ))
    })
}

fn compose_source(
    projection: rp::RunnableProjection,
    workflow_id: Uuid,
    replay_source: Option<Uuid>,
    task_instances: Vec<ReplayArchivedTask>,
    ctx_envelope: Value,
    definition: Option<Value>,
) -> Result<ReplaySource, RepositoryError> {
    let pinned_definition = definition
        .map(|value| {
            tickr_proto::codec::definition::definition_proto_from_json(value)
                .map_err(|error| corrupt_value(CorruptReplay(error.to_string())))
        })
        .transpose()?;
    Ok(ReplaySource {
        workflow_id,
        projection,
        replay_source,
        task_instances,
        ctx_envelope,
        pinned_definition,
    })
}

fn replay_source_from_blob(blob: &Value) -> Option<Uuid> {
    let provenance = blob.get("triggered_by")?;
    if provenance.get("kind")?.as_str()? != "Replay" {
        return None;
    }
    provenance
        .get("source_instance")?
        .get("id")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn validate_lifecycle_input(input: &ReplayLifecycleInput) -> Result<(), RepositoryError> {
    if !REPLAY_STATUSES.contains(&input.status.as_str()) {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Internal,
            CorruptReplay(format!(
                "invalid replay lifecycle status `{}`",
                input.status
            )),
        ));
    }
    Ok(())
}

type PgLifecycleTuple = (
    Uuid,
    Uuid,
    Uuid,
    Option<String>,
    String,
    Value,
    Value,
    Option<String>,
    Option<String>,
    Value,
    DateTime<Utc>,
);
type SqliteLifecycleTuple = (
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    i64,
);

fn decode_uuid_array(value: Value) -> Result<Vec<Uuid>, RepositoryError> {
    serde_json::from_value(value).map_err(|error| corrupt_value(CorruptReplay(error.to_string())))
}

fn decode_string_array(value: Value) -> Result<Vec<String>, RepositoryError> {
    serde_json::from_value(value).map_err(|error| corrupt_value(CorruptReplay(error.to_string())))
}

fn row_from_postgres(row: PgLifecycleTuple) -> Result<ReplayLifecycleRow, RepositoryError> {
    if !REPLAY_STATUSES.contains(&row.4.as_str()) {
        return Err(corrupt_value(CorruptReplay(format!(
            "unknown lifecycle status `{}`",
            row.4
        ))));
    }
    Ok(ReplayLifecycleRow {
        replay_instance_id: row.0,
        source_instance_id: row.1,
        signal_id: row.2,
        idempotency_key: row.3,
        status: row.4,
        resume_from: decode_uuid_array(row.5)?,
        pre_grounded: decode_uuid_array(row.6)?,
        name: row.7,
        seed_sha256: row.8,
        shadowed_keys: decode_string_array(row.9)?,
        created_at: row.10,
    })
}

fn row_from_sqlite(row: SqliteLifecycleTuple) -> Result<ReplayLifecycleRow, RepositoryError> {
    if !REPLAY_STATUSES.contains(&row.4.as_str()) {
        return Err(corrupt_value(CorruptReplay(format!(
            "unknown lifecycle status `{}`",
            row.4
        ))));
    }
    Ok(ReplayLifecycleRow {
        replay_instance_id: decode_uuid(&row.0).map_err(corrupt_value)?,
        source_instance_id: decode_uuid(&row.1).map_err(corrupt_value)?,
        signal_id: decode_uuid(&row.2).map_err(corrupt_value)?,
        idempotency_key: row.3,
        status: row.4,
        resume_from: decode_uuid_array(decode_json(&row.5).map_err(corrupt_value)?)?,
        pre_grounded: decode_uuid_array(decode_json(&row.6).map_err(corrupt_value)?)?,
        name: row.7,
        seed_sha256: row.8,
        shadowed_keys: decode_string_array(decode_json(&row.9).map_err(corrupt_value)?)?,
        created_at: decode_timestamp(row.10).map_err(corrupt_value)?,
    })
}

const ROW_COLUMNS: &str = "replay_instance_id, source_instance_id, signal_id, idempotency_key, \
status, resume_from, pre_grounded, name, seed_sha256, shadowed_keys, created_at";

async fn insert_postgres(
    pool: &PgPool,
    input: &ReplayLifecycleInput,
) -> Result<ReplayLifecycleInsertOutcome, RepositoryError> {
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let resume_from = serde_json::to_value(&input.resume_from).map_err(invalid_input)?;
    let pre_grounded = serde_json::to_value(&input.pre_grounded).map_err(invalid_input)?;
    let shadowed_keys = serde_json::to_value(&input.shadowed_keys).map_err(invalid_input)?;
    let inserted = sqlx::query(
        "INSERT INTO workflow_replays \
         (replay_instance_id, source_instance_id, signal_id, idempotency_key, status, \
          resume_from, pre_grounded, name, seed_sha256, outcome, shadowed_keys) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING",
    )
    .bind(input.replay_instance_id)
    .bind(input.source_instance_id)
    .bind(input.signal_id)
    .bind(input.idempotency_key.as_deref())
    .bind(&input.status)
    .bind(resume_from)
    .bind(pre_grounded)
    .bind(input.name.as_deref())
    .bind(input.seed_sha256.as_deref())
    .bind(input.outcome.as_deref())
    .bind(shadowed_keys)
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    if inserted.rows_affected() == 1 {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(ReplayLifecycleInsertOutcome::Inserted);
    }
    let row = if let Some(key) = input.idempotency_key.as_deref() {
        sqlx::query_as::<_, PgLifecycleTuple>(&format!(
            "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = $1 AND idempotency_key = $2"
        ))
        .bind(input.source_instance_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query_as::<_, PgLifecycleTuple>(&format!(
            "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE replay_instance_id = $1"
        ))
        .bind(input.replay_instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    };
    tx.commit().await.map_err(repository_sqlx_error)?;
    row.map(row_from_postgres)
        .transpose()?
        .map(ReplayLifecycleInsertOutcome::Existing)
        .ok_or_else(|| {
            RepositoryError::new(
                RepositoryErrorKind::ConstraintConflict,
                CorruptReplay("replay insert conflicted without a durable row".into()),
            )
        })
}

async fn insert_sqlite(
    pool: &SqlitePool,
    input: &ReplayLifecycleInput,
) -> Result<ReplayLifecycleInsertOutcome, RepositoryError> {
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let resume_from =
        encode_json(&serde_json::to_value(&input.resume_from).map_err(invalid_input)?);
    let pre_grounded =
        encode_json(&serde_json::to_value(&input.pre_grounded).map_err(invalid_input)?);
    let shadowed_keys =
        encode_json(&serde_json::to_value(&input.shadowed_keys).map_err(invalid_input)?);
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO workflow_replays \
         (replay_instance_id, source_instance_id, signal_id, idempotency_key, status, \
          resume_from, pre_grounded, name, seed_sha256, outcome, shadowed_keys) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
    )
    .bind(encode_uuid(input.replay_instance_id))
    .bind(encode_uuid(input.source_instance_id))
    .bind(encode_uuid(input.signal_id))
    .bind(input.idempotency_key.as_deref())
    .bind(&input.status)
    .bind(resume_from)
    .bind(pre_grounded)
    .bind(input.name.as_deref())
    .bind(input.seed_sha256.as_deref())
    .bind(input.outcome.as_deref())
    .bind(shadowed_keys)
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    if inserted.rows_affected() == 1 {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(ReplayLifecycleInsertOutcome::Inserted);
    }
    let row = if let Some(key) = input.idempotency_key.as_deref() {
        sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
            "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = ?1 AND idempotency_key = ?2"
        ))
        .bind(encode_uuid(input.source_instance_id))
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    } else {
        sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
            "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE replay_instance_id = ?1"
        ))
        .bind(encode_uuid(input.replay_instance_id))
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
    };
    tx.commit().await.map_err(repository_sqlx_error)?;
    row.map(row_from_sqlite)
        .transpose()?
        .map(ReplayLifecycleInsertOutcome::Existing)
        .ok_or_else(|| {
            RepositoryError::new(
                RepositoryErrorKind::ConstraintConflict,
                CorruptReplay("replay insert conflicted without a durable row".into()),
            )
        })
}

async fn lifecycle_postgres(
    pool: &PgPool,
    replay_instance_id: Uuid,
) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
    let row = sqlx::query_as::<_, PgLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE replay_instance_id = $1"
    ))
    .bind(replay_instance_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(row_from_postgres).transpose()
}

async fn lifecycle_sqlite(
    pool: &SqlitePool,
    replay_instance_id: Uuid,
) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
    let row = sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE replay_instance_id = ?1"
    ))
    .bind(encode_uuid(replay_instance_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(row_from_sqlite).transpose()
}

async fn by_idempotency_postgres(
    pool: &PgPool,
    source_instance_id: Uuid,
    key: &str,
) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
    let row = sqlx::query_as::<_, PgLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = $1 AND idempotency_key = $2"
    ))
    .bind(source_instance_id)
    .bind(key)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(row_from_postgres).transpose()
}

async fn by_idempotency_sqlite(
    pool: &SqlitePool,
    source_instance_id: Uuid,
    key: &str,
) -> Result<Option<ReplayLifecycleRow>, RepositoryError> {
    let row = sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = ?1 AND idempotency_key = ?2"
    ))
    .bind(encode_uuid(source_instance_id))
    .bind(key)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(row_from_sqlite).transpose()
}

async fn load_replay_drive_postgres(
    pool: &PgPool,
    replay_instance_id: Uuid,
) -> Result<ReplayDriveLoadOutcome, RepositoryError> {
    sqlx::query(
        "UPDATE workflow_replays SET updated_at = now() \
         WHERE replay_instance_id = $1 AND status = 'Materializing'",
    )
    .bind(replay_instance_id)
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(lifecycle) = lifecycle_postgres(pool, replay_instance_id).await? else {
        return Ok(ReplayDriveLoadOutcome::Absent);
    };
    if ReplayLifecycleStatus::from_stored(&lifecycle.status)?
        != ReplayLifecycleStatus::Materializing
    {
        return Ok(ReplayDriveLoadOutcome::AlreadySettled(
            ReplayLifecycleStatus::from_stored(&lifecycle.status)?,
        ));
    }
    Ok(
        match replay_source_postgres(pool, lifecycle.source_instance_id).await? {
            Some(source) => ReplayDriveLoadOutcome::Ready(ReplayDrive { lifecycle, source }),
            None => ReplayDriveLoadOutcome::SourceUnavailable(lifecycle),
        },
    )
}

async fn load_replay_drive_sqlite(
    pool: &SqlitePool,
    replay_instance_id: Uuid,
) -> Result<ReplayDriveLoadOutcome, RepositoryError> {
    sqlx::query(
        "UPDATE workflow_replays SET updated_at = ?2 \
         WHERE replay_instance_id = ?1 AND status = 'Materializing'",
    )
    .bind(encode_uuid(replay_instance_id))
    .bind(encode_timestamp(Utc::now()))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some(lifecycle) = lifecycle_sqlite(pool, replay_instance_id).await? else {
        return Ok(ReplayDriveLoadOutcome::Absent);
    };
    if ReplayLifecycleStatus::from_stored(&lifecycle.status)?
        != ReplayLifecycleStatus::Materializing
    {
        return Ok(ReplayDriveLoadOutcome::AlreadySettled(
            ReplayLifecycleStatus::from_stored(&lifecycle.status)?,
        ));
    }
    Ok(
        match replay_source_sqlite(pool, lifecycle.source_instance_id).await? {
            Some(source) => ReplayDriveLoadOutcome::Ready(ReplayDrive { lifecycle, source }),
            None => ReplayDriveLoadOutcome::SourceUnavailable(lifecycle),
        },
    )
}

async fn settle_replay_released_postgres(
    pool: &PgPool,
    replay_instance_id: Uuid,
) -> Result<ReplaySettlementOutcome, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE workflow_replays SET status = 'Released', outcome = 'released', \
         updated_at = now(), lease_owner = NULL, lease_token = NULL, \
         lease_expires_at = NULL \
         WHERE replay_instance_id = $1 AND status = 'Materializing'",
    )
    .bind(replay_instance_id)
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(ReplaySettlementOutcome::Released);
    }
    classify_settlement(lifecycle_postgres(pool, replay_instance_id).await?)
}

async fn settle_replay_released_sqlite(
    pool: &SqlitePool,
    replay_instance_id: Uuid,
) -> Result<ReplaySettlementOutcome, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE workflow_replays SET status = 'Released', outcome = 'released', \
         updated_at = ?2, lease_owner = NULL, lease_token = NULL, \
         lease_expires_at = NULL \
         WHERE replay_instance_id = ?1 AND status = 'Materializing'",
    )
    .bind(encode_uuid(replay_instance_id))
    .bind(encode_timestamp(Utc::now()))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(ReplaySettlementOutcome::Released);
    }
    classify_settlement(lifecycle_sqlite(pool, replay_instance_id).await?)
}

async fn lease_replays_sqlite(
    pool: &SqlitePool,
    request: ReplayLeaseRequest<'_>,
) -> Result<Vec<ReplayLeaseCandidate>, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let encoded_now = encode_timestamp(request.now);
    let rows: Vec<SqliteLifecycleTuple> = sqlx::query_as(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays \
         WHERE status = 'Materializing' AND updated_at <= ?1 \
           AND (lease_expires_at IS NULL OR lease_expires_at <= ?2) \
         ORDER BY updated_at ASC, replay_instance_id ASC LIMIT ?3"
    ))
    .bind(encode_timestamp(request.eligible_before))
    .bind(&encoded_now)
    .bind(request.limit as i64)
    .fetch_all(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;

    let encoded_expiry = encode_timestamp(request.expires_at);
    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let identity = row.0.clone();
        let lease_token = Uuid::new_v4();
        let updated = sqlx::query(
            "UPDATE workflow_replays \
             SET lease_owner = ?2, lease_token = ?3, lease_expires_at = ?4 \
             WHERE replay_instance_id = ?1 AND status = 'Materializing' \
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?5)",
        )
        .bind(&identity)
        .bind(request.owner)
        .bind(encode_uuid(lease_token))
        .bind(&encoded_expiry)
        .bind(&encoded_now)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if updated.rows_affected() != 1 {
            return Err(corrupt_value(CorruptReplay(format!(
                "selected replay lifecycle {identity} was not leaseable"
            ))));
        }
        candidates.push(match row_from_sqlite(row) {
            Ok(row) => ReplayLeaseCandidate::Ready(LeasedReplay {
                row,
                lease_owner: request.owner.to_owned(),
                lease_token,
                lease_expires_at: request.expires_at,
            }),
            Err(error) => ReplayLeaseCandidate::Corrupt { identity, error },
        });
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(candidates)
}

async fn settle_leased_replay_released_sqlite(
    pool: &SqlitePool,
    lease: &LeasedReplay,
    now: DateTime<Utc>,
) -> Result<LeasedReplaySettlementOutcome, RepositoryError> {
    let updated = sqlx::query(
        "UPDATE workflow_replays \
         SET status = 'Released', outcome = 'released', updated_at = ?5, \
             lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE replay_instance_id = ?1 AND status = 'Materializing' \
           AND lease_owner = ?2 AND lease_token = ?3 AND lease_expires_at > ?4",
    )
    .bind(encode_uuid(lease.row.replay_instance_id))
    .bind(&lease.lease_owner)
    .bind(encode_uuid(lease.lease_token))
    .bind(encode_timestamp(now))
    .bind(encode_timestamp(now))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected();
    if updated == 1 {
        return Ok(LeasedReplaySettlementOutcome::Settled(
            ReplaySettlementOutcome::Released,
        ));
    }
    Ok(
        match lifecycle_sqlite(pool, lease.row.replay_instance_id).await? {
            Some(row) if row.status == STATUS_MATERIALIZING => {
                LeasedReplaySettlementOutcome::LeaseLost
            }
            lifecycle => LeasedReplaySettlementOutcome::Settled(classify_settlement(lifecycle)?),
        },
    )
}

async fn release_replay_lease_sqlite(
    pool: &SqlitePool,
    lease: &LeasedReplay,
) -> Result<bool, RepositoryError> {
    Ok(sqlx::query(
        "UPDATE workflow_replays \
         SET lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL \
         WHERE replay_instance_id = ?1 AND lease_owner = ?2 AND lease_token = ?3",
    )
    .bind(encode_uuid(lease.row.replay_instance_id))
    .bind(&lease.lease_owner)
    .bind(encode_uuid(lease.lease_token))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?
    .rows_affected()
        == 1)
}

fn classify_settlement(
    lifecycle: Option<ReplayLifecycleRow>,
) -> Result<ReplaySettlementOutcome, RepositoryError> {
    Ok(match lifecycle {
        Some(row) => ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::from_stored(
            &row.status,
        )?),
        None => ReplaySettlementOutcome::Absent,
    })
}

async fn unsettled_postgres(
    pool: &PgPool,
    cutoff: DateTime<Utc>,
) -> Result<Vec<ReplayRedriveCandidate>, RepositoryError> {
    let rows = sqlx::query_as::<_, PgLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE status = 'Materializing' AND updated_at <= $1 ORDER BY updated_at ASC, replay_instance_id ASC"
    ))
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let identity = row.0;
            match row_from_postgres(row) {
                Ok(row) => ReplayRedriveCandidate::Ready(row),
                Err(error) => ReplayRedriveCandidate::Corrupt {
                    identity: identity.to_string(),
                    error,
                },
            }
        })
        .collect())
}

async fn unsettled_sqlite(
    pool: &SqlitePool,
    cutoff: DateTime<Utc>,
) -> Result<Vec<ReplayRedriveCandidate>, RepositoryError> {
    let rows = sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE status = 'Materializing' AND updated_at <= ?1 ORDER BY updated_at ASC, replay_instance_id ASC"
    ))
    .bind(encode_timestamp(cutoff))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let identity = row.0.clone();
            match row_from_sqlite(row) {
                Ok(row) => ReplayRedriveCandidate::Ready(row),
                Err(error) => ReplayRedriveCandidate::Corrupt { identity, error },
            }
        })
        .collect())
}

async fn replays_for_source_postgres(
    pool: &PgPool,
    source_instance_id: Uuid,
) -> Result<Vec<ReplayLifecycleRow>, RepositoryError> {
    let rows = sqlx::query_as::<_, PgLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = $1 ORDER BY created_at DESC, replay_instance_id DESC"
    ))
    .bind(source_instance_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter().map(row_from_postgres).collect()
}

async fn replays_for_source_sqlite(
    pool: &SqlitePool,
    source_instance_id: Uuid,
) -> Result<Vec<ReplayLifecycleRow>, RepositoryError> {
    let rows = sqlx::query_as::<_, SqliteLifecycleTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE source_instance_id = ?1 ORDER BY created_at DESC, replay_instance_id DESC"
    ))
    .bind(encode_uuid(source_instance_id))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter().map(row_from_sqlite).collect()
}

fn invalid_input(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Internal, source)
}

fn corrupt_value(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::CorruptStoredValue, source)
}
