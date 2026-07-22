//! Terminal workflow archive persistence and detail reconstruction.
//!
//! One writer operation owns the complete linked archive transaction. Read-only
//! operations reconstruct published projections with explicit stable ordering;
//! backend encodings and SQL types never escape the repository bundle.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, SqlitePool};
use tickr_proto::{archive as ap, instance as ip};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{
    decode_json, decode_timestamp, decode_uuid, encode_json, encode_timestamp, encode_uuid,
};

const TERMINAL_WORKFLOW_STATES: &[&str] = &["Completed", "Failed"];

/// The complete input to one terminal archive transaction.
pub struct ArchiveTerminalWorkflowInput<'a> {
    pub projection: &'a ap::ArchiveProjection,
    pub ctx_envelope: Value,
    pub runtime_params: Value,
    pub log_uris: Value,
    /// Stable archive time supplied by the staged compaction envelope. A
    /// redelivery reuses this value instead of changing archive ordering.
    pub archived_at: DateTime<Utc>,
}

/// Durable completion evidence coupled to a staged local Compaction archive.
#[derive(Debug, Clone)]
pub struct LocalCompactionArchiveCompletion {
    pub workflow_instance_id: Uuid,
    pub payload_digest: String,
    pub scope_id: Uuid,
    pub scope_digest: String,
    pub final_log_references: Value,
    pub completed_at: DateTime<Utc>,
}

/// Compaction enrichment linked to one terminal Workflow instance.
#[derive(Debug, Clone, PartialEq)]
pub struct ArchiveRunInfo {
    pub ctx_envelope: Value,
    pub runtime_params: Value,
    pub log_uris: Value,
    pub enriched_at: DateTime<Utc>,
}

/// The linked terminal detail reconstructed by the API read role.
#[derive(Debug, Clone)]
pub struct ArchivedWorkflowDetail {
    pub instance: ip::ArchivedInstance,
    pub task_instances: Vec<ip::ArchivedTaskInstance>,
    pub run_info: Option<ArchiveRunInfo>,
}

/// Stable pagination for one Workflow's terminal archive collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchivePage {
    pub limit: Option<u32>,
    pub offset: u32,
}

impl ArchivePage {
    pub const fn unbounded() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

/// One terminal candidate used to compose the latest fired run.
#[derive(Debug, Clone, PartialEq)]
pub struct ArchivedRunCandidate {
    pub workflow_id: Uuid,
    pub instance_id: Uuid,
    pub state: String,
    pub scheduled_at: Option<DateTime<Utc>>,
}

/// One terminal Workflow-instance selected through a conservative UTC calendar envelope.
#[derive(Debug, Clone)]
pub struct ArchivedCalendarCandidate {
    pub instance: ip::ArchivedInstanceRow,
    pub scheduled_at: DateTime<Utc>,
}

/// One archived row used by the dashboard clock.
#[derive(Debug, Clone, PartialEq)]
pub struct ArchivedDashboardInstance {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub state: String,
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored terminal archive: {0}")]
struct CorruptArchive(String);

impl WriterRepositoryBundle {
    /// Atomically replace the complete linked terminal projection.
    ///
    /// The operation commits the Workflow-instance row, every Task-instance
    /// row, and run-info row together. Existing Task rows for the instance are
    /// replaced so at-least-once redelivery converges to the staged projection.
    pub async fn archive_terminal_workflow(
        &self,
        input: ArchiveTerminalWorkflowInput<'_>,
    ) -> Result<(), RepositoryError> {
        validate_input(input.projection)?;
        match &self.pool {
            BackendPool::Postgres(pool) => archive_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => archive_sqlite(pool, input, None).await,
        }
    }

    /// Archive a staged Tickr Lite Compaction and mark its staging record
    /// complete in the same SQLite transaction. The caller may only purge
    /// logs, scope values, and envelope bytes after this returns successfully.
    pub async fn archive_staged_local_compaction(
        &self,
        input: ArchiveTerminalWorkflowInput<'_>,
        completion: LocalCompactionArchiveCompletion,
    ) -> Result<(), RepositoryError> {
        validate_input(input.projection)?;
        let projection_id = Uuid::parse_str(&input.projection.id).map_err(invalid_input)?;
        if projection_id != completion.workflow_instance_id {
            return Err(invalid_input(CorruptArchive(
                "staged Compaction identity does not match archive projection".to_owned(),
            )));
        }
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(RepositoryError::new(
                RepositoryErrorKind::Configuration,
                CorruptArchive("local Compaction archive requires SQLite".to_owned()),
            ));
        };
        archive_sqlite(pool, input, Some(completion)).await
    }
}

impl ReadOnlyRepositoryBundle {
    /// Read one complete terminal Workflow-instance detail.
    pub async fn archived_workflow_detail(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Option<ArchivedWorkflowDetail>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => detail_postgres(pool, workflow_instance_id).await,
            BackendPool::Sqlite(pool) => detail_sqlite(pool, workflow_instance_id).await,
        }
    }

    /// Read terminal Task-instance details in stable completion/UUID order.
    pub async fn archived_task_instances(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<ip::ArchivedTaskInstance>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => tasks_postgres(pool, workflow_instance_id).await,
            BackendPool::Sqlite(pool) => tasks_sqlite(pool, workflow_instance_id).await,
        }
    }

    /// Read the complete nullable enrichment record for one terminal run.
    pub async fn archive_run_info(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Option<ArchiveRunInfo>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => run_info_postgres(pool, workflow_instance_id).await,
            BackendPool::Sqlite(pool) => run_info_sqlite(pool, workflow_instance_id).await,
        }
    }

    /// Count terminal runs by Workflow identity.
    ///
    /// Workflows without a terminal run are absent from the returned map.
    pub async fn completed_run_counts(&self) -> Result<HashMap<Uuid, i64>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => completed_run_counts_postgres(pool).await,
            BackendPool::Sqlite(pool) => completed_run_counts_sqlite(pool).await,
        }
    }

    /// List one Workflow's terminal instances in stable archive/UUID order.
    pub async fn archived_workflow_instances(
        &self,
        workflow_id: Uuid,
        page: ArchivePage,
    ) -> Result<Vec<ip::ArchivedInstanceRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                workflow_instances_postgres(pool, workflow_id, page).await
            }
            BackendPool::Sqlite(pool) => workflow_instances_sqlite(pool, workflow_id, page).await,
        }
    }

    /// Read terminal calendar candidates for one Workflow inside a half-open UTC envelope.
    pub async fn archived_calendar_candidates(
        &self,
        workflow_id: Uuid,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ArchivedCalendarCandidate>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                calendar_candidates_postgres(pool, workflow_id, start, end).await
            }
            BackendPool::Sqlite(pool) => {
                calendar_candidates_sqlite(pool, workflow_id, start, end).await
            }
        }
    }

    /// List terminal archive rows for the dashboard's inclusive UTC window.
    pub async fn archived_dashboard_instances(
        &self,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<ArchivedDashboardInstance>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => dashboard_instances_postgres(pool, start, end).await,
            BackendPool::Sqlite(pool) => dashboard_instances_sqlite(pool, start, end).await,
        }
    }

    /// Read one deterministic latest terminal candidate per Workflow.
    pub async fn latest_archived_runs(&self) -> Result<Vec<ArchivedRunCandidate>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => latest_runs_postgres(pool).await,
            BackendPool::Sqlite(pool) => latest_runs_sqlite(pool).await,
        }
    }
}

async fn completed_run_counts_postgres(
    pool: &PgPool,
) -> Result<HashMap<Uuid, i64>, RepositoryError> {
    let rows: Vec<(Uuid, i64)> = sqlx::query_as(
        "SELECT workflow_id, COUNT(*) \
         FROM workflow_instances \
         WHERE state IN ('Completed', 'Failed') \
         GROUP BY workflow_id \
         ORDER BY workflow_id",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows.into_iter().collect())
}

async fn completed_run_counts_sqlite(
    pool: &SqlitePool,
) -> Result<HashMap<Uuid, i64>, RepositoryError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT workflow_id, COUNT(*) \
         FROM workflow_instances \
         WHERE state IN ('Completed', 'Failed') \
         GROUP BY workflow_id \
         ORDER BY workflow_id",
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(workflow_id, count)| Ok((decode_uuid(&workflow_id).map_err(corrupt_value)?, count)))
        .collect()
}

async fn workflow_instances_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    page: ArchivePage,
) -> Result<Vec<ip::ArchivedInstanceRow>, RepositoryError> {
    let rows: Vec<(Uuid, Uuid, String, Value, i64)> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.instance,
               (SELECT COUNT(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id)
        FROM workflow_instances wi
        WHERE wi.workflow_id = $1
          AND wi.state IN ('Completed', 'Failed')
        ORDER BY wi.archived_at DESC, wi.id DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(workflow_id)
    .bind(page.limit.map(i64::from))
    .bind(i64::from(page.offset))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(id, workflow_id, state, instance, task_count)| {
            decode_instance_row(id, workflow_id, &state, instance, task_count)
        })
        .collect()
}

async fn workflow_instances_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    page: ArchivePage,
) -> Result<Vec<ip::ArchivedInstanceRow>, RepositoryError> {
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.instance,
               (SELECT COUNT(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id)
        FROM workflow_instances wi
        WHERE wi.workflow_id = ?1
          AND wi.state IN ('Completed', 'Failed')
        ORDER BY wi.archived_at DESC, wi.id DESC
        LIMIT COALESCE(?2, -1) OFFSET ?3
        "#,
    )
    .bind(encode_uuid(workflow_id))
    .bind(page.limit.map(i64::from))
    .bind(i64::from(page.offset))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(id, workflow_id, state, instance, task_count)| {
            decode_instance_row(
                decode_uuid(&id).map_err(corrupt_value)?,
                decode_uuid(&workflow_id).map_err(corrupt_value)?,
                &state,
                decode_json(&instance).map_err(corrupt_value)?,
                task_count,
            )
        })
        .collect()
}

async fn calendar_candidates_postgres(
    pool: &PgPool,
    workflow_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<ArchivedCalendarCandidate>, RepositoryError> {
    let rows: Vec<(Uuid, Uuid, String, DateTime<Utc>, Value, i64)> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.scheduled_at, wi.instance,
               (SELECT COUNT(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id)
        FROM workflow_instances wi
        WHERE wi.workflow_id = $1
          AND wi.state IN ('Completed', 'Failed')
          AND wi.scheduled_at >= $2
          AND wi.scheduled_at < $3
        ORDER BY wi.scheduled_at DESC, wi.id DESC
        "#,
    )
    .bind(workflow_id)
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, workflow_id, state, scheduled_at, instance, task_count)| {
                decode_calendar_candidate(
                    id,
                    workflow_id,
                    &state,
                    scheduled_at,
                    instance,
                    task_count,
                )
            },
        )
        .collect()
}

async fn calendar_candidates_sqlite(
    pool: &SqlitePool,
    workflow_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<ArchivedCalendarCandidate>, RepositoryError> {
    let rows: Vec<(String, String, String, i64, String, i64)> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.scheduled_at, wi.instance,
               (SELECT COUNT(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id)
        FROM workflow_instances wi
        WHERE wi.workflow_id = ?1
          AND wi.state IN ('Completed', 'Failed')
          AND wi.scheduled_at >= ?2
          AND wi.scheduled_at < ?3
        ORDER BY wi.scheduled_at DESC, wi.id DESC
        "#,
    )
    .bind(encode_uuid(workflow_id))
    .bind(encode_timestamp(start))
    .bind(encode_timestamp(end))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, workflow_id, state, scheduled_at, instance, task_count)| {
                decode_calendar_candidate(
                    decode_uuid(&id).map_err(corrupt_value)?,
                    decode_uuid(&workflow_id).map_err(corrupt_value)?,
                    &state,
                    decode_timestamp(scheduled_at).map_err(corrupt_value)?,
                    decode_json(&instance).map_err(corrupt_value)?,
                    task_count,
                )
            },
        )
        .collect()
}

async fn dashboard_instances_postgres(
    pool: &PgPool,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<Vec<ArchivedDashboardInstance>, RepositoryError> {
    type Row = (
        Uuid,
        Uuid,
        String,
        Option<DateTime<Utc>>,
        DateTime<Utc>,
        Value,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.scheduled_at, wi.archived_at,
               wi.instance,
               (SELECT w.name FROM workflows w
                WHERE w.id = wi.workflow_id
                ORDER BY w.version DESC LIMIT 1)
        FROM workflow_instances wi
        WHERE wi.state IN ('Completed', 'Failed')
          AND ($1::timestamptz IS NULL OR wi.scheduled_at >= $1)
          AND ($2::timestamptz IS NULL OR wi.scheduled_at <= $2)
        ORDER BY wi.scheduled_at DESC NULLS LAST, wi.archived_at DESC, wi.id DESC
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, workflow_id, state, scheduled_at, _, instance, fallback_name)| {
                decode_dashboard_instance(
                    id,
                    workflow_id,
                    &state,
                    scheduled_at,
                    instance,
                    fallback_name,
                )
            },
        )
        .collect()
}

async fn dashboard_instances_sqlite(
    pool: &SqlitePool,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<Vec<ArchivedDashboardInstance>, RepositoryError> {
    type Row = (
        String,
        String,
        String,
        Option<i64>,
        i64,
        String,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        r#"
        SELECT wi.id, wi.workflow_id, wi.state, wi.scheduled_at, wi.archived_at,
               wi.instance,
               (SELECT w.name FROM workflows w
                WHERE w.id = wi.workflow_id
                ORDER BY w.version DESC LIMIT 1)
        FROM workflow_instances wi
        WHERE wi.state IN ('Completed', 'Failed')
          AND (?1 IS NULL OR wi.scheduled_at >= ?1)
          AND (?2 IS NULL OR wi.scheduled_at <= ?2)
        ORDER BY wi.scheduled_at IS NULL, wi.scheduled_at DESC,
                 wi.archived_at DESC, wi.id DESC
        "#,
    )
    .bind(start.map(encode_timestamp))
    .bind(end.map(encode_timestamp))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, workflow_id, state, scheduled_at, _, instance, fallback_name)| {
                decode_dashboard_instance(
                    decode_uuid(&id).map_err(corrupt_value)?,
                    decode_uuid(&workflow_id).map_err(corrupt_value)?,
                    &state,
                    scheduled_at
                        .map(decode_timestamp)
                        .transpose()
                        .map_err(corrupt_value)?,
                    decode_json(&instance).map_err(corrupt_value)?,
                    fallback_name,
                )
            },
        )
        .collect()
}

async fn latest_runs_postgres(pool: &PgPool) -> Result<Vec<ArchivedRunCandidate>, RepositoryError> {
    let rows: Vec<(Uuid, Uuid, String, Option<DateTime<Utc>>)> = sqlx::query_as(
        r#"
        SELECT workflow_id, id, state, scheduled_at
        FROM (
            SELECT workflow_id, id, state, scheduled_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY workflow_id
                       ORDER BY scheduled_at DESC NULLS LAST, archived_at DESC, id DESC
                   ) AS position
            FROM workflow_instances
            WHERE state IN ('Completed', 'Failed')
        ) candidates
        WHERE position = 1
        ORDER BY workflow_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(
            |(workflow_id, instance_id, state, scheduled_at)| ArchivedRunCandidate {
                workflow_id,
                instance_id,
                state,
                scheduled_at,
            },
        )
        .collect())
}

async fn latest_runs_sqlite(
    pool: &SqlitePool,
) -> Result<Vec<ArchivedRunCandidate>, RepositoryError> {
    let rows: Vec<(String, String, String, Option<i64>)> = sqlx::query_as(
        r#"
        SELECT workflow_id, id, state, scheduled_at
        FROM (
            SELECT workflow_id, id, state, scheduled_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY workflow_id
                       ORDER BY scheduled_at IS NULL, scheduled_at DESC,
                                archived_at DESC, id DESC
                   ) AS position
            FROM workflow_instances
            WHERE state IN ('Completed', 'Failed')
        ) candidates
        WHERE position = 1
        ORDER BY workflow_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|(workflow_id, instance_id, state, scheduled_at)| {
            Ok(ArchivedRunCandidate {
                workflow_id: decode_uuid(&workflow_id).map_err(corrupt_value)?,
                instance_id: decode_uuid(&instance_id).map_err(corrupt_value)?,
                state,
                scheduled_at: scheduled_at
                    .map(decode_timestamp)
                    .transpose()
                    .map_err(corrupt_value)?,
            })
        })
        .collect()
}

fn decode_instance_row(
    row_id: Uuid,
    row_workflow_id: Uuid,
    row_state: &str,
    instance: Value,
    task_count: i64,
) -> Result<ip::ArchivedInstanceRow, RepositoryError> {
    let projection: ap::ArchiveProjection =
        serde_json::from_value(instance).map_err(corrupt_value)?;
    if projection.id != row_id.to_string()
        || projection.workflow_id != row_workflow_id.to_string()
        || projection.state != row_state
        || !TERMINAL_WORKFLOW_STATES.contains(&row_state)
    {
        return Err(corrupt_value(CorruptArchive(
            "Workflow-instance list projection disagrees with relational identity or state"
                .to_string(),
        )));
    }
    Ok(ip::ArchivedInstanceRow {
        id: projection.id,
        workflow_id: projection.workflow_id,
        workflow_version: projection.workflow_version,
        name: projection.name,
        state: projection.state,
        scheduled_at: projection.scheduled_at,
        task_count: u64::try_from(task_count).map_err(corrupt_value)?,
    })
}

fn decode_calendar_candidate(
    row_id: Uuid,
    row_workflow_id: Uuid,
    row_state: &str,
    scheduled_at: DateTime<Utc>,
    instance: Value,
    task_count: i64,
) -> Result<ArchivedCalendarCandidate, RepositoryError> {
    let instance = decode_instance_row(row_id, row_workflow_id, row_state, instance, task_count)?;
    let projected_scheduled_at = parse_timestamp(instance.scheduled_at.as_deref())
        .map_err(corrupt_value)?
        .map(truncate_timestamp);
    let scheduled_at = truncate_timestamp(scheduled_at);
    if projected_scheduled_at != Some(scheduled_at) {
        return Err(corrupt_value(CorruptArchive(
            "Workflow-instance calendar projection disagrees with relational scheduled_at"
                .to_string(),
        )));
    }
    Ok(ArchivedCalendarCandidate {
        instance,
        scheduled_at,
    })
}

fn decode_dashboard_instance(
    row_id: Uuid,
    row_workflow_id: Uuid,
    row_state: &str,
    scheduled_at: Option<DateTime<Utc>>,
    instance: Value,
    fallback_name: Option<String>,
) -> Result<ArchivedDashboardInstance, RepositoryError> {
    let projection: ap::ArchiveProjection =
        serde_json::from_value(instance).map_err(corrupt_value)?;
    if projection.id != row_id.to_string()
        || projection.workflow_id != row_workflow_id.to_string()
        || projection.state != row_state
        || !TERMINAL_WORKFLOW_STATES.contains(&row_state)
    {
        return Err(corrupt_value(CorruptArchive(
            "dashboard projection disagrees with relational identity or state".to_string(),
        )));
    }
    Ok(ArchivedDashboardInstance {
        id: row_id,
        workflow_id: row_workflow_id,
        workflow_name: if projection.workflow_name.is_empty() {
            fallback_name.unwrap_or_default()
        } else {
            projection.workflow_name
        },
        scheduled_at,
        state: row_state.to_string(),
    })
}

fn validate_input(projection: &ap::ArchiveProjection) -> Result<(), RepositoryError> {
    Uuid::parse_str(&projection.id).map_err(invalid_input)?;
    Uuid::parse_str(&projection.workflow_id).map_err(invalid_input)?;
    if !TERMINAL_WORKFLOW_STATES.contains(&projection.state.as_str()) {
        return Err(invalid_input(CorruptArchive(format!(
            "workflow state `{}` is not terminal",
            projection.state
        ))));
    }
    parse_timestamp(projection.scheduled_at.as_deref()).map_err(invalid_input)?;
    for task in &projection.task_instances {
        Uuid::parse_str(&task.id).map_err(invalid_input)?;
        Uuid::parse_str(&task.task_id).map_err(invalid_input)?;
    }
    Ok(())
}

async fn archive_postgres(
    pool: &PgPool,
    input: ArchiveTerminalWorkflowInput<'_>,
) -> Result<(), RepositoryError> {
    let projection = input.projection;
    let instance_id = Uuid::parse_str(&projection.id).map_err(invalid_input)?;
    let workflow_id = Uuid::parse_str(&projection.workflow_id).map_err(invalid_input)?;
    let scheduled_at =
        parse_timestamp(projection.scheduled_at.as_deref()).map_err(invalid_input)?;
    let instance_json = serde_json::to_value(projection).map_err(invalid_input)?;
    let archived_at = truncate_timestamp(input.archived_at);
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;

    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, archived_at, instance)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET
            workflow_id = EXCLUDED.workflow_id,
            name = EXCLUDED.name,
            state = EXCLUDED.state,
            scheduled_at = EXCLUDED.scheduled_at,
            archived_at = EXCLUDED.archived_at,
            instance = EXCLUDED.instance
        "#,
    )
    .bind(instance_id)
    .bind(workflow_id)
    .bind(&projection.name)
    .bind(&projection.state)
    .bind(scheduled_at)
    .bind(archived_at)
    .bind(instance_json)
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;

    sqlx::query("DELETE FROM task_instances WHERE workflow_instance_id = $1")
        .bind(instance_id)
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;

    for task in &projection.task_instances {
        let task_instance_id = Uuid::parse_str(&task.id).map_err(invalid_input)?;
        let task_id = Uuid::parse_str(&task.task_id).map_err(invalid_input)?;
        let task_json = serde_json::to_value(task).map_err(invalid_input)?;
        sqlx::query(
            r#"
            INSERT INTO task_instances
                (id, workflow_instance_id, workflow_id, task_id, name, state,
                 archived_at, task_instance, attempt)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                workflow_instance_id = EXCLUDED.workflow_instance_id,
                workflow_id = EXCLUDED.workflow_id,
                task_id = EXCLUDED.task_id,
                name = EXCLUDED.name,
                state = EXCLUDED.state,
                archived_at = EXCLUDED.archived_at,
                task_instance = EXCLUDED.task_instance,
                attempt = EXCLUDED.attempt
            "#,
        )
        .bind(task_instance_id)
        .bind(instance_id)
        .bind(workflow_id)
        .bind(task_id)
        .bind(&task.name)
        .bind(&task.state)
        .bind(archived_at)
        .bind(task_json)
        .bind(i64::from(task.attempt))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
    }

    sqlx::query(
        r#"
        INSERT INTO workflow_run_info
            (workflow_instance_id, ctx_envelope, runtime_params, log_uris, enriched_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (workflow_instance_id) DO UPDATE SET
            ctx_envelope = EXCLUDED.ctx_envelope,
            runtime_params = EXCLUDED.runtime_params,
            log_uris = EXCLUDED.log_uris,
            enriched_at = EXCLUDED.enriched_at
        "#,
    )
    .bind(instance_id)
    .bind(input.ctx_envelope)
    .bind(input.runtime_params)
    .bind(input.log_uris)
    .bind(archived_at)
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;

    tx.commit().await.map_err(repository_sqlx_error)
}

async fn archive_sqlite(
    pool: &SqlitePool,
    input: ArchiveTerminalWorkflowInput<'_>,
    completion: Option<LocalCompactionArchiveCompletion>,
) -> Result<(), RepositoryError> {
    let projection = input.projection;
    let instance_id = Uuid::parse_str(&projection.id).map_err(invalid_input)?;
    let workflow_id = Uuid::parse_str(&projection.workflow_id).map_err(invalid_input)?;
    let scheduled_at =
        parse_timestamp(projection.scheduled_at.as_deref()).map_err(invalid_input)?;
    let instance_json = serde_json::to_value(projection).map_err(invalid_input)?;
    let archived_at = encode_timestamp(input.archived_at);
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;

    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, archived_at, instance)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT (id) DO UPDATE SET
            workflow_id = excluded.workflow_id,
            name = excluded.name,
            state = excluded.state,
            scheduled_at = excluded.scheduled_at,
            archived_at = excluded.archived_at,
            instance = excluded.instance
        "#,
    )
    .bind(encode_uuid(instance_id))
    .bind(encode_uuid(workflow_id))
    .bind(&projection.name)
    .bind(&projection.state)
    .bind(scheduled_at.map(encode_timestamp))
    .bind(archived_at)
    .bind(encode_json(&instance_json))
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;

    sqlx::query("DELETE FROM task_instances WHERE workflow_instance_id = ?1")
        .bind(encode_uuid(instance_id))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;

    for task in &projection.task_instances {
        let task_instance_id = Uuid::parse_str(&task.id).map_err(invalid_input)?;
        let task_id = Uuid::parse_str(&task.task_id).map_err(invalid_input)?;
        let task_json = serde_json::to_value(task).map_err(invalid_input)?;
        sqlx::query(
            r#"
            INSERT INTO task_instances
                (id, workflow_instance_id, workflow_id, task_id, name, state,
                 archived_at, task_instance, attempt)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT (id) DO UPDATE SET
                workflow_instance_id = excluded.workflow_instance_id,
                workflow_id = excluded.workflow_id,
                task_id = excluded.task_id,
                name = excluded.name,
                state = excluded.state,
                archived_at = excluded.archived_at,
                task_instance = excluded.task_instance,
                attempt = excluded.attempt
            "#,
        )
        .bind(encode_uuid(task_instance_id))
        .bind(encode_uuid(instance_id))
        .bind(encode_uuid(workflow_id))
        .bind(encode_uuid(task_id))
        .bind(&task.name)
        .bind(&task.state)
        .bind(archived_at)
        .bind(encode_json(&task_json))
        .bind(i64::from(task.attempt))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
    }

    sqlx::query(
        r#"
        INSERT INTO workflow_run_info
            (workflow_instance_id, ctx_envelope, runtime_params, log_uris, enriched_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT (workflow_instance_id) DO UPDATE SET
            ctx_envelope = excluded.ctx_envelope,
            runtime_params = excluded.runtime_params,
            log_uris = excluded.log_uris,
            enriched_at = excluded.enriched_at
        "#,
    )
    .bind(encode_uuid(instance_id))
    .bind(encode_json(&input.ctx_envelope))
    .bind(encode_json(&input.runtime_params))
    .bind(encode_json(&input.log_uris))
    .bind(archived_at)
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;

    if let Some(completion) = completion {
        sqlx::query(
            "UPDATE signal_captures SET terminal_at = ?2 \
             WHERE materialized_run_id = ?1 AND terminal_at IS NULL",
        )
        .bind(encode_uuid(completion.workflow_instance_id))
        .bind(encode_timestamp(completion.completed_at))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;

        let completed = sqlx::query(
            "UPDATE local_compaction_staging \
             SET state = 'complete', scope_id = ?3, scope_digest = ?4, \
                 final_log_references = ?5, completed_at = ?6 \
             WHERE workflow_instance_id = ?1 AND payload_digest = ?2 AND state = 'staged'",
        )
        .bind(encode_uuid(completion.workflow_instance_id))
        .bind(&completion.payload_digest)
        .bind(encode_uuid(completion.scope_id))
        .bind(&completion.scope_digest)
        .bind(encode_json(&completion.final_log_references))
        .bind(encode_timestamp(completion.completed_at))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        if completed.rows_affected() != 1 {
            return Err(invalid_input(CorruptArchive(
                "staged Compaction record is missing, complete, or has different payload bytes"
                    .to_owned(),
            )));
        }
    }

    tx.commit().await.map_err(repository_sqlx_error)
}

async fn detail_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Option<ArchivedWorkflowDetail>, RepositoryError> {
    let row: Option<(Uuid, Uuid, String, Value)> = sqlx::query_as(
        "SELECT id, workflow_id, state, instance FROM workflow_instances WHERE id = $1",
    )
    .bind(workflow_instance_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some((id, workflow_id, state, instance)) = row else {
        return Ok(None);
    };
    let instance = decode_instance(id, workflow_id, &state, instance)?;
    let task_instances = tasks_postgres(pool, workflow_instance_id).await?;
    let run_info = run_info_postgres(pool, workflow_instance_id).await?;
    Ok(Some(ArchivedWorkflowDetail {
        instance,
        task_instances,
        run_info,
    }))
}

async fn detail_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Option<ArchivedWorkflowDetail>, RepositoryError> {
    let row: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT id, workflow_id, state, instance FROM workflow_instances WHERE id = ?1",
    )
    .bind(encode_uuid(workflow_instance_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    let Some((id, workflow_id, state, instance)) = row else {
        return Ok(None);
    };
    let instance = decode_instance(
        decode_uuid(&id).map_err(corrupt_value)?,
        decode_uuid(&workflow_id).map_err(corrupt_value)?,
        &state,
        decode_json(&instance).map_err(corrupt_value)?,
    )?;
    let task_instances = tasks_sqlite(pool, workflow_instance_id).await?;
    let run_info = run_info_sqlite(pool, workflow_instance_id).await?;
    Ok(Some(ArchivedWorkflowDetail {
        instance,
        task_instances,
        run_info,
    }))
}

type PostgresTaskRow = (Uuid, Uuid, Uuid, Uuid, String, String, i64, Value);
type SqliteTaskRow = (String, String, String, String, String, String, i64, String);

async fn tasks_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Vec<ip::ArchivedTaskInstance>, RepositoryError> {
    let rows: Vec<PostgresTaskRow> = sqlx::query_as(
        r#"
        SELECT id, workflow_instance_id, workflow_id, task_id, name, state,
               attempt::bigint, task_instance
        FROM task_instances
        WHERE workflow_instance_id = $1
        ORDER BY archived_at ASC, id ASC
        "#,
    )
    .bind(workflow_instance_id)
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, instance_id, workflow_id, task_id, name, state, attempt, task)| {
                decode_task(
                    id,
                    instance_id,
                    workflow_id,
                    task_id,
                    &name,
                    &state,
                    attempt,
                    task,
                )
            },
        )
        .collect()
}

async fn tasks_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Vec<ip::ArchivedTaskInstance>, RepositoryError> {
    let rows: Vec<SqliteTaskRow> = sqlx::query_as(
        r#"
        SELECT id, workflow_instance_id, workflow_id, task_id, name, state,
               attempt, task_instance
        FROM task_instances
        WHERE workflow_instance_id = ?1
        ORDER BY archived_at ASC, id ASC
        "#,
    )
    .bind(encode_uuid(workflow_instance_id))
    .fetch_all(pool)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(
            |(id, instance_id, workflow_id, task_id, name, state, attempt, task)| {
                decode_task(
                    decode_uuid(&id).map_err(corrupt_value)?,
                    decode_uuid(&instance_id).map_err(corrupt_value)?,
                    decode_uuid(&workflow_id).map_err(corrupt_value)?,
                    decode_uuid(&task_id).map_err(corrupt_value)?,
                    &name,
                    &state,
                    attempt,
                    decode_json(&task).map_err(corrupt_value)?,
                )
            },
        )
        .collect()
}

async fn run_info_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Option<ArchiveRunInfo>, RepositoryError> {
    let row: Option<(Value, Value, Value, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT ctx_envelope, runtime_params, log_uris, enriched_at
        FROM workflow_run_info
        WHERE workflow_instance_id = $1
        "#,
    )
    .bind(workflow_instance_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(row.map(
        |(ctx_envelope, runtime_params, log_uris, enriched_at)| ArchiveRunInfo {
            ctx_envelope,
            runtime_params,
            log_uris,
            enriched_at,
        },
    ))
}

async fn run_info_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Option<ArchiveRunInfo>, RepositoryError> {
    let row: Option<(String, String, String, i64)> = sqlx::query_as(
        r#"
        SELECT ctx_envelope, runtime_params, log_uris, enriched_at
        FROM workflow_run_info
        WHERE workflow_instance_id = ?1
        "#,
    )
    .bind(encode_uuid(workflow_instance_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(ctx_envelope, runtime_params, log_uris, enriched_at)| {
        Ok(ArchiveRunInfo {
            ctx_envelope: decode_json(&ctx_envelope).map_err(corrupt_value)?,
            runtime_params: decode_json(&runtime_params).map_err(corrupt_value)?,
            log_uris: decode_json(&log_uris).map_err(corrupt_value)?,
            enriched_at: decode_timestamp(enriched_at).map_err(corrupt_value)?,
        })
    })
    .transpose()
}

fn decode_instance(
    row_id: Uuid,
    row_workflow_id: Uuid,
    row_state: &str,
    instance: Value,
) -> Result<ip::ArchivedInstance, RepositoryError> {
    let projection = tickr_proto::codec::archive::archive_projection_from_json(instance)
        .map_err(|error| corrupt_value(CorruptArchive(error.to_string())))?;
    let projection_id = Uuid::parse_str(&projection.id).map_err(corrupt_value)?;
    let projection_workflow_id = Uuid::parse_str(&projection.workflow_id).map_err(corrupt_value)?;
    if projection_id != row_id
        || projection_workflow_id != row_workflow_id
        || projection.state != row_state
        || !TERMINAL_WORKFLOW_STATES.contains(&row_state)
    {
        return Err(corrupt_value(CorruptArchive(
            "relational Workflow-instance fields disagree with its projection".to_string(),
        )));
    }
    tickr_proto::codec::archive::archived_instance_from_projection(&projection)
        .map_err(|error| corrupt_value(CorruptArchive(error.to_string())))
}

#[allow(clippy::too_many_arguments)]
fn decode_task(
    row_id: Uuid,
    row_instance_id: Uuid,
    row_workflow_id: Uuid,
    row_task_id: Uuid,
    row_name: &str,
    row_state: &str,
    row_attempt: i64,
    task: Value,
) -> Result<ip::ArchivedTaskInstance, RepositoryError> {
    let task: ip::SnapshotTaskInstance = serde_json::from_value(task).map_err(corrupt_value)?;
    let task_instance_id = Uuid::parse_str(&task.id).map_err(corrupt_value)?;
    let task_id = Uuid::parse_str(&task.task_id).map_err(corrupt_value)?;
    if task_instance_id != row_id
        || task_id != row_task_id
        || task.name != row_name
        || task.state != row_state
        || i64::from(task.attempt) != row_attempt
    {
        return Err(corrupt_value(CorruptArchive(
            "relational Task-instance fields disagree with its projection".to_string(),
        )));
    }
    Ok(ip::ArchivedTaskInstance {
        id: task.id,
        task_id: task.task_id,
        workflow_instance_id: row_instance_id.to_string(),
        workflow_id: row_workflow_id.to_string(),
        name: task.name,
        task_type: task.task_type,
        state: task.state,
        executor_id: task.executor_id,
        attempt: task.attempt,
    })
}

fn parse_timestamp(value: Option<&str>) -> Result<Option<DateTime<Utc>>, chrono::ParseError> {
    value
        .map(|value| DateTime::parse_from_rfc3339(value).map(|value| value.with_timezone(&Utc)))
        .transpose()
}

fn truncate_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid DateTime remains valid at microsecond precision")
}

fn invalid_input(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Internal, source)
}

fn corrupt_value(source: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::CorruptStoredValue, source)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
    use tickr_proto::config::DataPlaneSql;
    use tickr_proto::runnable as rp;

    use super::*;
    use crate::backend::RepositoryFactory;
    use crate::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};

    struct ArchiveFixture {
        projection: ap::ArchiveProjection,
        ctx_envelope: Value,
        runtime_params: Value,
        log_uris: Value,
        archived_at: DateTime<Utc>,
    }

    impl ArchiveFixture {
        fn input(&self) -> ArchiveTerminalWorkflowInput<'_> {
            ArchiveTerminalWorkflowInput {
                projection: &self.projection,
                ctx_envelope: self.ctx_envelope.clone(),
                runtime_params: self.runtime_params.clone(),
                log_uris: self.log_uris.clone(),
                archived_at: self.archived_at,
            }
        }

        fn instance_id(&self) -> Uuid {
            Uuid::parse_str(&self.projection.id).unwrap()
        }
    }

    fn fixture(state: &str) -> ArchiveFixture {
        let workflow_id = Uuid::new_v4();
        let instance_id = Uuid::new_v4();
        let lower_task_instance_id = Uuid::from_u128(1);
        let upper_task_instance_id = Uuid::from_u128(2);
        let task = |id: Uuid, name: &str, executor_id: Option<String>| ip::SnapshotTaskInstance {
            id: id.to_string(),
            task_id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            task_type: "RegularTask".to_string(),
            state: state.to_string(),
            executor_id,
            attempt: 0,
            ..Default::default()
        };
        ArchiveFixture {
            projection: ap::ArchiveProjection {
                runnable: Some(rp::RunnableProjection {
                    graph: Some(rp::RunnableGraph::default()),
                    ..Default::default()
                }),
                id: instance_id.to_string(),
                workflow_id: workflow_id.to_string(),
                workflow_name: "Repository law".to_string(),
                workflow_version: 42,
                name: "terminal-run".to_string(),
                state: state.to_string(),
                scheduled_at: Some("2026-07-20T01:02:03.123456789Z".to_string()),
                task_instances: vec![
                    task(upper_task_instance_id, "second-by-uuid", None),
                    task(
                        lower_task_instance_id,
                        "first-by-uuid",
                        Some(Uuid::new_v4().to_string()),
                    ),
                ],
                ..Default::default()
            },
            ctx_envelope: serde_json::json!([
                {"key": format!("{instance_id}/answer"), "envelope": {"value": 42}}
            ]),
            runtime_params: serde_json::json!({
                "workflow_id": workflow_id,
                "optional": null
            }),
            log_uris: serde_json::json!({
                lower_task_instance_id.to_string(): "s3://tickr-logs/first.gz",
                upper_task_instance_id.to_string(): null
            }),
            archived_at: DateTime::parse_from_rfc3339("2026-07-20T02:03:04.987654321Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn summary_fixture(
        workflow_id: Uuid,
        instance_id: Uuid,
        state: &str,
        scheduled_at: &str,
        archived_at: &str,
    ) -> ArchiveFixture {
        let mut archive = fixture(state);
        archive.projection.id = instance_id.to_string();
        archive.projection.workflow_id = workflow_id.to_string();
        archive.projection.workflow_name = "Summary law".to_string();
        archive.projection.name = format!("run-{instance_id}");
        archive.projection.scheduled_at = Some(scheduled_at.to_string());
        archive.projection.task_instances.clear();
        archive.archived_at = DateTime::parse_from_rfc3339(archived_at)
            .unwrap()
            .with_timezone(&Utc);
        archive
    }

    async fn insert_raw_instance(writer: &WriterRepositoryBundle, archive: &ArchiveFixture) {
        let projection = &archive.projection;
        let instance_id = Uuid::parse_str(&projection.id).unwrap();
        let workflow_id = Uuid::parse_str(&projection.workflow_id).unwrap();
        let scheduled_at = parse_timestamp(projection.scheduled_at.as_deref()).unwrap();
        let instance = serde_json::to_value(projection).unwrap();
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO workflow_instances \
                     (id, workflow_id, name, state, scheduled_at, archived_at, instance) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(instance_id)
                .bind(workflow_id)
                .bind(&projection.name)
                .bind(&projection.state)
                .bind(scheduled_at)
                .bind(archive.archived_at)
                .bind(instance)
                .execute(pool)
                .await
                .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO workflow_instances \
                     (id, workflow_id, name, state, scheduled_at, archived_at, instance) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .bind(encode_uuid(instance_id))
                .bind(encode_uuid(workflow_id))
                .bind(&projection.name)
                .bind(&projection.state)
                .bind(scheduled_at.map(encode_timestamp))
                .bind(encode_timestamp(archive.archived_at))
                .bind(encode_json(&instance))
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn archive_counts(
        writer: &WriterRepositoryBundle,
        workflow_instance_id: Uuid,
    ) -> (i64, i64, i64) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                let workflows =
                    sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = $1")
                        .bind(workflow_instance_id)
                        .fetch_one(pool)
                        .await
                        .unwrap();
                let tasks = sqlx::query_scalar(
                    "SELECT count(*) FROM task_instances WHERE workflow_instance_id = $1",
                )
                .bind(workflow_instance_id)
                .fetch_one(pool)
                .await
                .unwrap();
                let run_info = sqlx::query_scalar(
                    "SELECT count(*) FROM workflow_run_info WHERE workflow_instance_id = $1",
                )
                .bind(workflow_instance_id)
                .fetch_one(pool)
                .await
                .unwrap();
                (workflows, tasks, run_info)
            }
            BackendPool::Sqlite(pool) => {
                let id = encode_uuid(workflow_instance_id);
                let workflows =
                    sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                        .bind(&id)
                        .fetch_one(pool)
                        .await
                        .unwrap();
                let tasks = sqlx::query_scalar(
                    "SELECT count(*) FROM task_instances WHERE workflow_instance_id = ?1",
                )
                .bind(&id)
                .fetch_one(pool)
                .await
                .unwrap();
                let run_info = sqlx::query_scalar(
                    "SELECT count(*) FROM workflow_run_info WHERE workflow_instance_id = ?1",
                )
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap();
                (workflows, tasks, run_info)
            }
        }
    }

    async fn install_failure_trigger(writer: &WriterRepositoryBundle, table: &str) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "CREATE OR REPLACE FUNCTION archive_repository_law_fail() RETURNS trigger \
                     LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected archive failure'; END $$",
                )
                .execute(pool)
                .await
                .unwrap();
                sqlx::query(&format!(
                    "CREATE TRIGGER archive_repository_law_failure BEFORE INSERT OR UPDATE ON {table} \
                     FOR EACH ROW EXECUTE FUNCTION archive_repository_law_fail()"
                ))
                .execute(pool)
                .await
                .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(&format!(
                    "CREATE TRIGGER archive_repository_law_failure BEFORE INSERT ON {table} \
                     BEGIN SELECT RAISE(ABORT, 'injected archive failure'); END"
                ))
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn remove_failure_trigger(writer: &WriterRepositoryBundle, table: &str) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(&format!(
                    "DROP TRIGGER archive_repository_law_failure ON {table}"
                ))
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("DROP FUNCTION archive_repository_law_fail()")
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query("DROP TRIGGER archive_repository_law_failure")
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn corrupt_projection(writer: &WriterRepositoryBundle, workflow_instance_id: Uuid) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query("UPDATE workflow_instances SET instance = '{\"id\":\"broken\"}'::jsonb WHERE id = $1")
                    .bind(workflow_instance_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE workflow_instances SET instance = '{\"id\":\"broken\"}' WHERE id = ?1",
                )
                .bind(encode_uuid(workflow_instance_id))
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn delete_run_info(writer: &WriterRepositoryBundle, workflow_instance_id: Uuid) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query("DELETE FROM workflow_run_info WHERE workflow_instance_id = $1")
                    .bind(workflow_instance_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM workflow_run_info WHERE workflow_instance_id = ?1")
                    .bind(encode_uuid(workflow_instance_id))
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn run_laws(selection: DataPlaneSql) {
        let factory = RepositoryFactory::new(selection.clone());
        let writer = factory.open_writer().await.unwrap();
        let reader = factory.open_read_only().await.unwrap();
        let archive = fixture("Completed");
        let instance_id = archive.instance_id();

        writer
            .archive_terminal_workflow(archive.input())
            .await
            .unwrap();
        let detail = reader
            .archived_workflow_detail(instance_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.instance.id, archive.projection.id);
        assert_eq!(detail.instance.workflow_id, archive.projection.workflow_id);
        assert_eq!(detail.instance.workflow_version, 42);
        assert_eq!(detail.instance.state, "Completed");
        assert_eq!(detail.task_instances.len(), 2);
        assert_eq!(
            detail
                .task_instances
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                Uuid::from_u128(1).to_string(),
                Uuid::from_u128(2).to_string()
            ]
        );
        let run_info = detail.run_info.unwrap();
        assert_eq!(run_info.ctx_envelope, archive.ctx_envelope);
        assert_eq!(run_info.runtime_params, archive.runtime_params);
        assert_eq!(run_info.log_uris, archive.log_uris);
        assert_eq!(
            run_info.enriched_at,
            truncate_timestamp(archive.archived_at)
        );

        // Commit-then-redelivery is idempotent and preserves stable ordering.
        writer
            .archive_terminal_workflow(archive.input())
            .await
            .unwrap();
        assert_eq!(archive_counts(&writer, instance_id).await, (1, 2, 1));

        // Collection and summary reads share terminal filters, inclusive
        // dashboard bounds, and deterministic equal-time UUID tie-breaks.
        let summary_workflow_id = Uuid::from_u128(1_000);
        let lower_id = Uuid::from_u128(101);
        let middle_id = Uuid::from_u128(102);
        let upper_id = Uuid::from_u128(103);
        let older = summary_fixture(
            summary_workflow_id,
            lower_id,
            "Completed",
            "2026-07-20T10:00:00Z",
            "2026-07-20T12:00:00Z",
        );
        let equal_lower = summary_fixture(
            summary_workflow_id,
            middle_id,
            "Failed",
            "2026-07-20T11:00:00Z",
            "2026-07-20T13:00:00Z",
        );
        let equal_upper = summary_fixture(
            summary_workflow_id,
            upper_id,
            "Completed",
            "2026-07-20T11:00:00Z",
            "2026-07-20T13:00:00Z",
        );
        for candidate in [&older, &equal_lower, &equal_upper] {
            writer
                .archive_terminal_workflow(candidate.input())
                .await
                .unwrap();
        }

        let counts = reader.completed_run_counts().await.unwrap();
        assert_eq!(counts.get(&summary_workflow_id), Some(&3));
        let page = reader
            .archived_workflow_instances(
                summary_workflow_id,
                ArchivePage {
                    limit: Some(1),
                    offset: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, middle_id.to_string());
        assert_eq!(page[0].task_count, 0);
        assert!(reader
            .archived_workflow_instances(Uuid::from_u128(9_999), ArchivePage::unbounded())
            .await
            .unwrap()
            .is_empty());

        let equal_time = DateTime::parse_from_rfc3339("2026-07-20T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let dashboard = reader
            .archived_dashboard_instances(Some(equal_time), Some(equal_time))
            .await
            .unwrap();
        assert_eq!(
            dashboard.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![upper_id, middle_id]
        );
        assert!(dashboard
            .iter()
            .all(|row| row.workflow_name == "Summary law"));

        let latest = reader.latest_archived_runs().await.unwrap();
        let latest_summary = latest
            .iter()
            .find(|candidate| candidate.workflow_id == summary_workflow_id)
            .unwrap();
        assert_eq!(latest_summary.instance_id, upper_id);
        assert_eq!(latest_summary.state, "Completed");

        let calendar_start = DateTime::parse_from_rfc3339("2026-07-20T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let calendar_end = DateTime::parse_from_rfc3339("2026-07-20T11:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let calendar = reader
            .archived_calendar_candidates(summary_workflow_id, calendar_start, calendar_end)
            .await
            .unwrap();
        assert_eq!(
            calendar
                .iter()
                .map(|candidate| candidate.instance.id.clone())
                .collect::<Vec<_>>(),
            vec![upper_id.to_string(), middle_id.to_string()]
        );
        assert!(calendar
            .iter()
            .all(|candidate| candidate.scheduled_at == equal_time));

        let nonterminal_workflow_id = Uuid::from_u128(2_000);
        let nonterminal = summary_fixture(
            nonterminal_workflow_id,
            Uuid::from_u128(201),
            "InProgress",
            "2026-07-20T11:00:00Z",
            "2026-07-20T14:00:00Z",
        );
        insert_raw_instance(&writer, &nonterminal).await;
        assert!(!reader
            .completed_run_counts()
            .await
            .unwrap()
            .contains_key(&nonterminal_workflow_id));
        assert!(reader
            .archived_workflow_instances(nonterminal_workflow_id, ArchivePage::unbounded())
            .await
            .unwrap()
            .is_empty());
        assert!(!reader
            .latest_archived_runs()
            .await
            .unwrap()
            .iter()
            .any(|candidate| candidate.workflow_id == nonterminal_workflow_id));
        assert!(!reader
            .archived_dashboard_instances(Some(equal_time), Some(equal_time))
            .await
            .unwrap()
            .iter()
            .any(|row| row.workflow_id == nonterminal_workflow_id));
        assert!(reader
            .archived_calendar_candidates(nonterminal_workflow_id, calendar_start, calendar_end,)
            .await
            .unwrap()
            .is_empty());

        // Failure at each write boundary rolls the whole linked archive back.
        for table in ["workflow_instances", "task_instances", "workflow_run_info"] {
            let failing = fixture("Failed");
            install_failure_trigger(&writer, table).await;
            assert!(writer
                .archive_terminal_workflow(failing.input())
                .await
                .is_err());
            remove_failure_trigger(&writer, table).await;
            assert_eq!(
                archive_counts(&writer, failing.instance_id()).await,
                (0, 0, 0)
            );
        }

        let missing = Uuid::new_v4();
        assert!(reader
            .archived_workflow_detail(missing)
            .await
            .unwrap()
            .is_none());
        assert!(reader
            .archived_task_instances(missing)
            .await
            .unwrap()
            .is_empty());
        assert!(reader.archive_run_info(missing).await.unwrap().is_none());

        // A missing optional legacy enrichment row remains a readable detail.
        delete_run_info(&writer, instance_id).await;
        assert!(reader
            .archived_workflow_detail(instance_id)
            .await
            .unwrap()
            .unwrap()
            .run_info
            .is_none());
        writer
            .archive_terminal_workflow(archive.input())
            .await
            .unwrap();

        corrupt_projection(&writer, instance_id).await;
        let error = reader
            .archived_workflow_detail(instance_id)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), RepositoryErrorKind::CorruptStoredValue);
        let list_error = reader
            .archived_workflow_instances(
                Uuid::parse_str(&archive.projection.workflow_id).unwrap(),
                ArchivePage::unbounded(),
            )
            .await
            .unwrap_err();
        assert_eq!(list_error.kind(), RepositoryErrorKind::CorruptStoredValue);
        writer
            .archive_terminal_workflow(archive.input())
            .await
            .unwrap();

        reader.close().await;
        writer.close().await;
        let reopened = RepositoryFactory::new(selection)
            .open_read_only()
            .await
            .unwrap();
        let reopened_detail = reopened
            .archived_workflow_detail(instance_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reopened_detail.instance.workflow_version, 42);
        assert_eq!(reopened_detail.task_instances.len(), 2);
        assert_eq!(
            reopened
                .completed_run_counts()
                .await
                .unwrap()
                .get(&summary_workflow_id),
            Some(&3)
        );
        assert_eq!(
            reopened
                .archived_workflow_instances(summary_workflow_id, ArchivePage::unbounded())
                .await
                .unwrap()
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>(),
            vec![
                upper_id.to_string(),
                middle_id.to_string(),
                lower_id.to_string()
            ]
        );
        reopened.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_terminal_archive_repository_laws() {
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
    async fn file_backed_sqlite_terminal_archive_repository_laws() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("archive.db"));
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
