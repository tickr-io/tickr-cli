//! Tenant Event projection persistence and reads.
//!
//! The upstream Archive cursor and public arrival cursor are separate laws:
//! `(archived_at, id)` is derived from committed rows for Pull cycles, while
//! `seq` is assigned atomically for public newest-first pagination.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row, SqlitePool};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{
    decode_json, decode_timestamp, decode_uuid, encode_json, encode_timestamp, encode_uuid,
};

/// The derived position used to request the next control-plane Archive page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventArchiveCursor {
    pub archived_at: DateTime<Utc>,
    pub id: Uuid,
}

/// One control-plane Event ready for projection insertion.
#[derive(Debug, Clone)]
pub struct EventProjectionInput {
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub payload: Value,
    pub archived_at: DateTime<Utc>,
}

/// One public Event projection row.
#[derive(Debug, Clone, PartialEq)]
pub struct EventProjectionRow {
    pub seq: i64,
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub payload: Value,
}

/// Optional public Event scope. All variants retain the same `seq` cursor law.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFilter {
    All,
    WorkflowInstance(Uuid),
    TaskInstance(Uuid),
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored Event projection: {0}")]
struct CorruptEvent(String);

impl WriterRepositoryBundle {
    /// Derive the upstream Archive cursor from the greatest committed key.
    pub async fn event_archive_cursor(
        &self,
    ) -> Result<Option<EventArchiveCursor>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => archive_cursor_postgres(pool).await,
            BackendPool::Sqlite(pool) => archive_cursor_sqlite(pool).await,
        }
    }

    /// Atomically insert one fetched page and assign contiguous public `seq`s.
    ///
    /// Existing Event ids are ignored before sequence assignment. Concurrent
    /// insertions serialize inside the repository, so duplicate pages neither
    /// duplicate rows nor consume public sequence values.
    pub async fn insert_event_page(
        &self,
        events: &[EventProjectionInput],
    ) -> Result<u64, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => insert_page_postgres(pool, events).await,
            BackendPool::Sqlite(pool) => insert_page_sqlite(pool, events).await,
        }
    }

    /// Delete retained projection rows older than the supplied Archive time.
    ///
    /// The next Pull position is still derived from whatever rows remain. If
    /// retention empties the projection, the next cycle safely rebuilds from
    /// the beginning rather than advancing a copied high-water marker.
    pub async fn delete_events_archived_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => sqlx::query("DELETE FROM events WHERE archived_at < $1")
                .bind(cutoff)
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
                .map_err(repository_sqlx_error),
            BackendPool::Sqlite(pool) => sqlx::query("DELETE FROM events WHERE archived_at < ?1")
                .bind(encode_timestamp(cutoff))
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
                .map_err(repository_sqlx_error),
        }
    }
}

impl ReadOnlyRepositoryBundle {
    /// Read public Events newest-first by projection arrival sequence.
    pub async fn events(
        &self,
        filter: EventFilter,
        after: Option<i64>,
        limit: i64,
    ) -> Result<Vec<EventProjectionRow>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => events_postgres(pool, filter, after, limit).await,
            BackendPool::Sqlite(pool) => events_sqlite(pool, filter, after, limit).await,
        }
    }

    /// Count public Events under the same filter used by [`Self::events`].
    pub async fn event_count(&self, filter: EventFilter) -> Result<i64, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => event_count_postgres(pool, filter).await,
            BackendPool::Sqlite(pool) => event_count_sqlite(pool, filter).await,
        }
    }
}

async fn archive_cursor_postgres(
    pool: &PgPool,
) -> Result<Option<EventArchiveCursor>, RepositoryError> {
    sqlx::query("SELECT archived_at, id FROM events ORDER BY archived_at DESC, id DESC LIMIT 1")
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)
        .map(|row| {
            row.map(|row| EventArchiveCursor {
                archived_at: row.get("archived_at"),
                id: row.get("id"),
            })
        })
}

async fn archive_cursor_sqlite(
    pool: &SqlitePool,
) -> Result<Option<EventArchiveCursor>, RepositoryError> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT archived_at, id FROM events ORDER BY archived_at DESC, id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|(archived_at, id)| {
        Ok(EventArchiveCursor {
            archived_at: decode_timestamp(archived_at).map_err(corrupt_value)?,
            id: decode_uuid(&id).map_err(corrupt_value)?,
        })
    })
    .transpose()
}

async fn insert_page_postgres(
    pool: &PgPool,
    events: &[EventProjectionInput],
) -> Result<u64, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    sqlx::query("LOCK TABLE events IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
    let mut next_seq: i64 = sqlx::query_scalar(
        "SELECT GREATEST(\
             COALESCE(MAX(seq), 0), \
             (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM events_seq_seq)\
         ) + 1 FROM events",
    )
    .fetch_one(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let mut inserted = 0;
    for event in events {
        let result = sqlx::query(
            "INSERT INTO events (seq, id, ts, event_type, payload, archived_at) \
             SELECT $1, $2, $3, $4, $5, $6 \
             WHERE NOT EXISTS (SELECT 1 FROM events WHERE id = $2)",
        )
        .bind(next_seq)
        .bind(event.id)
        .bind(event.ts)
        .bind(&event.event_type)
        .bind(&event.payload)
        .bind(event.archived_at)
        .execute(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        if result.rows_affected() == 1 {
            inserted += 1;
            next_seq += 1;
        }
    }
    if inserted > 0 {
        sqlx::query_scalar::<_, i64>(
            "SELECT setval(pg_get_serial_sequence('events', 'seq'), (SELECT MAX(seq) FROM events), true)",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
    }
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(inserted)
}

async fn insert_page_sqlite(
    pool: &SqlitePool,
    events: &[EventProjectionInput],
) -> Result<u64, RepositoryError> {
    let mut connection = pool.acquire().await.map_err(repository_sqlx_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(repository_sqlx_error)?;
    let result = insert_page_sqlite_transaction(&mut connection, events).await;
    match result {
        Ok(inserted) => {
            sqlx::query("COMMIT")
                .execute(&mut *connection)
                .await
                .map_err(repository_sqlx_error)?;
            Ok(inserted)
        }
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
            Err(error)
        }
    }
}

async fn insert_page_sqlite_transaction(
    connection: &mut sqlx::SqliteConnection,
    events: &[EventProjectionInput],
) -> Result<u64, RepositoryError> {
    let mut next_seq: i64 = sqlx::query_scalar(
        "SELECT MAX(\
             COALESCE((SELECT MAX(seq) FROM events), 0), \
             COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)\
         ) + 1",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(repository_sqlx_error)?;
    let mut inserted = 0;
    for event in events {
        let result = sqlx::query(
            "INSERT INTO events (seq, id, ts, event_type, payload, archived_at) \
             SELECT ?1, ?2, ?3, ?4, ?5, ?6 \
             WHERE NOT EXISTS (SELECT 1 FROM events WHERE id = ?2)",
        )
        .bind(next_seq)
        .bind(encode_uuid(event.id))
        .bind(encode_timestamp(event.ts))
        .bind(&event.event_type)
        .bind(encode_json(&event.payload))
        .bind(encode_timestamp(event.archived_at))
        .execute(&mut *connection)
        .await
        .map_err(repository_sqlx_error)?;
        if result.rows_affected() == 1 {
            inserted += 1;
            next_seq += 1;
        }
    }
    Ok(inserted)
}

async fn events_postgres(
    pool: &PgPool,
    filter: EventFilter,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<EventProjectionRow>, RepositoryError> {
    let rows = match filter {
        EventFilter::All => {
            sqlx::query(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE ($1::bigint IS NULL OR seq > $1) ORDER BY seq DESC LIMIT $2",
            )
            .bind(after)
            .bind(limit)
            .fetch_all(pool)
            .await
        }
        EventFilter::WorkflowInstance(id) => {
            sqlx::query(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE ($1::bigint IS NULL OR seq > $1) \
                   AND jsonb_typeof(payload) = 'object' \
                   AND EXISTS (SELECT 1 FROM jsonb_each(payload) \
                               WHERE value->>'workflow_instance_id' = $3) \
                 ORDER BY seq DESC LIMIT $2",
            )
            .bind(after)
            .bind(limit)
            .bind(id.to_string())
            .fetch_all(pool)
            .await
        }
        EventFilter::TaskInstance(id) => {
            sqlx::query(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE ($1::bigint IS NULL OR seq > $1) \
                   AND jsonb_typeof(payload) = 'object' \
                   AND EXISTS (SELECT 1 FROM jsonb_each(payload) \
                               WHERE value->>'task_instance_id' = $3) \
                 ORDER BY seq DESC LIMIT $2",
            )
            .bind(after)
            .bind(limit)
            .bind(id.to_string())
            .fetch_all(pool)
            .await
        }
    }
    .map_err(repository_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(|row| EventProjectionRow {
            seq: row.get("seq"),
            id: row.get("id"),
            ts: row.get("ts"),
            event_type: row.get("event_type"),
            payload: row.get("payload"),
        })
        .collect())
}

async fn events_sqlite(
    pool: &SqlitePool,
    filter: EventFilter,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<EventProjectionRow>, RepositoryError> {
    let rows: Vec<(i64, String, i64, String, String)> = match filter {
        EventFilter::All => {
            sqlx::query_as(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE (?1 IS NULL OR seq > ?1) ORDER BY seq DESC LIMIT ?2",
            )
            .bind(after)
            .bind(limit)
            .fetch_all(pool)
            .await
        }
        EventFilter::WorkflowInstance(id) => {
            sqlx::query_as(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE (?1 IS NULL OR seq > ?1) \
                   AND json_type(payload) = 'object' \
                   AND EXISTS (SELECT 1 FROM json_each(payload) \
                               WHERE json_extract(value, '$.workflow_instance_id') = ?3) \
                 ORDER BY seq DESC LIMIT ?2",
            )
            .bind(after)
            .bind(limit)
            .bind(encode_uuid(id))
            .fetch_all(pool)
            .await
        }
        EventFilter::TaskInstance(id) => {
            sqlx::query_as(
                "SELECT seq, id, ts, event_type, payload FROM events \
                 WHERE (?1 IS NULL OR seq > ?1) \
                   AND json_type(payload) = 'object' \
                   AND EXISTS (SELECT 1 FROM json_each(payload) \
                               WHERE json_extract(value, '$.task_instance_id') = ?3) \
                 ORDER BY seq DESC LIMIT ?2",
            )
            .bind(after)
            .bind(limit)
            .bind(encode_uuid(id))
            .fetch_all(pool)
            .await
        }
    }
    .map_err(repository_sqlx_error)?;
    rows.into_iter().map(decode_sqlite_event).collect()
}

async fn event_count_postgres(pool: &PgPool, filter: EventFilter) -> Result<i64, RepositoryError> {
    let result = match filter {
        EventFilter::All => {
            sqlx::query_scalar("SELECT COUNT(*) FROM events")
                .fetch_one(pool)
                .await
        }
        EventFilter::WorkflowInstance(id) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM events \
             WHERE jsonb_typeof(payload) = 'object' \
               AND EXISTS (SELECT 1 FROM jsonb_each(payload) \
                           WHERE value->>'workflow_instance_id' = $1)",
            )
            .bind(id.to_string())
            .fetch_one(pool)
            .await
        }
        EventFilter::TaskInstance(id) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM events \
             WHERE jsonb_typeof(payload) = 'object' \
               AND EXISTS (SELECT 1 FROM jsonb_each(payload) \
                           WHERE value->>'task_instance_id' = $1)",
            )
            .bind(id.to_string())
            .fetch_one(pool)
            .await
        }
    };
    result.map_err(repository_sqlx_error)
}

async fn event_count_sqlite(
    pool: &SqlitePool,
    filter: EventFilter,
) -> Result<i64, RepositoryError> {
    let result = match filter {
        EventFilter::All => {
            sqlx::query_scalar("SELECT COUNT(*) FROM events")
                .fetch_one(pool)
                .await
        }
        EventFilter::WorkflowInstance(id) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM events \
             WHERE json_type(payload) = 'object' \
               AND EXISTS (SELECT 1 FROM json_each(payload) \
                           WHERE json_extract(value, '$.workflow_instance_id') = ?1)",
            )
            .bind(encode_uuid(id))
            .fetch_one(pool)
            .await
        }
        EventFilter::TaskInstance(id) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM events \
             WHERE json_type(payload) = 'object' \
               AND EXISTS (SELECT 1 FROM json_each(payload) \
                           WHERE json_extract(value, '$.task_instance_id') = ?1)",
            )
            .bind(encode_uuid(id))
            .fetch_one(pool)
            .await
        }
    };
    result.map_err(repository_sqlx_error)
}

fn decode_sqlite_event(
    (seq, id, ts, event_type, payload): (i64, String, i64, String, String),
) -> Result<EventProjectionRow, RepositoryError> {
    Ok(EventProjectionRow {
        seq,
        id: decode_uuid(&id).map_err(corrupt_value)?,
        ts: decode_timestamp(ts).map_err(corrupt_value)?,
        event_type,
        payload: decode_json(&payload).map_err(corrupt_value)?,
    })
}

fn corrupt_value(source: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CorruptStoredValue,
        CorruptEvent(source.to_string()),
    )
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

    use super::*;
    use crate::backend::RepositoryFactory;
    use crate::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};

    fn instant(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn event(
        id: u128,
        ts: &str,
        archived_at: &str,
        event_type: &str,
        payload: Value,
    ) -> EventProjectionInput {
        EventProjectionInput {
            id: Uuid::from_u128(id),
            ts: instant(ts),
            event_type: event_type.to_owned(),
            payload,
            archived_at: instant(archived_at),
        }
    }

    async fn install_failure_trigger(writer: &WriterRepositoryBundle) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "CREATE OR REPLACE FUNCTION event_repository_law_fail() RETURNS trigger \
                     LANGUAGE plpgsql AS $$ BEGIN \
                     IF NEW.event_type = 'FailInsertion' THEN \
                         RAISE EXCEPTION 'injected Event insertion failure'; \
                     END IF; \
                     RETURN NEW; \
                     END $$",
                )
                .execute(pool)
                .await
                .unwrap();
                sqlx::query(
                    "CREATE TRIGGER event_repository_law_failure BEFORE INSERT ON events \
                     FOR EACH ROW EXECUTE FUNCTION event_repository_law_fail()",
                )
                .execute(pool)
                .await
                .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "CREATE TRIGGER event_repository_law_failure \
                     BEFORE INSERT ON events WHEN NEW.event_type = 'FailInsertion' \
                     BEGIN SELECT RAISE(ABORT, 'injected Event insertion failure'); END",
                )
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn remove_failure_trigger(writer: &WriterRepositoryBundle) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query("DROP TRIGGER event_repository_law_failure ON events")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DROP FUNCTION event_repository_law_fail()")
                    .execute(pool)
                    .await
                    .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query("DROP TRIGGER event_repository_law_failure")
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
        let workflow_instance_id = Uuid::from_u128(10_000);
        let task_instance_id = Uuid::from_u128(20_000);
        let other_workflow_instance_id = Uuid::from_u128(30_000);
        let tied_archive_time = "2026-07-21T10:00:00Z";
        let first_page = vec![
            event(
                9,
                "2026-07-21T09:00:00Z",
                "2026-07-21T09:59:59Z",
                "NodesJoined",
                Value::String("NodesJoined".to_owned()),
            ),
            event(
                2,
                "2026-07-21T09:59:59Z",
                tied_archive_time,
                "WorkflowCompleted",
                serde_json::json!({
                    "WorkflowCompleted": {
                        "workflow_instance_id": workflow_instance_id,
                        "task_instance_id": task_instance_id
                    }
                }),
            ),
            event(
                1,
                "2026-07-21T09:59:58Z",
                tied_archive_time,
                "WorkflowStarted",
                serde_json::json!({
                    "WorkflowStarted": {
                        "workflow_instance_id": other_workflow_instance_id
                    }
                }),
            ),
        ];

        assert_eq!(writer.event_archive_cursor().await.unwrap(), None);
        assert_eq!(writer.insert_event_page(&[]).await.unwrap(), 0);
        assert!(reader
            .events(EventFilter::All, None, 200)
            .await
            .unwrap()
            .is_empty());

        assert_eq!(writer.insert_event_page(&first_page).await.unwrap(), 3);
        assert_eq!(writer.insert_event_page(&first_page).await.unwrap(), 0);
        assert_eq!(
            writer.event_archive_cursor().await.unwrap(),
            Some(EventArchiveCursor {
                archived_at: instant(tied_archive_time),
                id: Uuid::from_u128(2),
            })
        );

        // Public arrival order remains input/commit order, not Archive cursor
        // order: the lower tied UUID arrived after the higher cursor UUID.
        // The scalar Event also proves instance filters safely ignore variants
        // without an object payload.
        let rows = reader.events(EventFilter::All, None, 200).await.unwrap();
        assert_eq!(
            rows.iter().map(|row| (row.seq, row.id)).collect::<Vec<_>>(),
            vec![
                (3, Uuid::from_u128(1)),
                (2, Uuid::from_u128(2)),
                (1, Uuid::from_u128(9))
            ]
        );

        let late_occurrence = event(
            3,
            "2026-07-20T00:00:00Z",
            "2026-07-21T10:00:01Z",
            "TaskParked",
            serde_json::json!({
                "TaskParked": {
                    "workflow_instance_id": workflow_instance_id,
                    "task_instance_id": task_instance_id
                }
            }),
        );
        assert_eq!(
            writer
                .insert_event_page(std::slice::from_ref(&late_occurrence))
                .await
                .unwrap(),
            1
        );
        let rows = reader.events(EventFilter::All, Some(1), 200).await.unwrap();
        assert_eq!(
            rows.iter().map(|row| (row.seq, row.id)).collect::<Vec<_>>(),
            vec![
                (4, Uuid::from_u128(3)),
                (3, Uuid::from_u128(1)),
                (2, Uuid::from_u128(2))
            ]
        );
        assert!(
            rows[0].ts < rows[1].ts,
            "late occurrence retains its newest arrival position"
        );

        let workflow_rows = reader
            .events(
                EventFilter::WorkflowInstance(workflow_instance_id),
                None,
                200,
            )
            .await
            .unwrap();
        assert_eq!(
            workflow_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![Uuid::from_u128(3), Uuid::from_u128(2)]
        );
        let task_rows = reader
            .events(EventFilter::TaskInstance(task_instance_id), None, 200)
            .await
            .unwrap();
        assert_eq!(task_rows.len(), 2);
        assert_eq!(reader.event_count(EventFilter::All).await.unwrap(), 4);
        assert_eq!(
            reader
                .event_count(EventFilter::WorkflowInstance(workflow_instance_id))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            reader
                .event_count(EventFilter::TaskInstance(task_instance_id))
                .await
                .unwrap(),
            2
        );

        // Two cycles derived from the same cursor may race the same fetched
        // page. Exactly one atomic insertion wins and public seq remains dense.
        let concurrent_page = vec![
            event(
                4,
                "2026-07-21T10:00:02Z",
                "2026-07-21T10:00:02Z",
                "WorkflowStarted",
                serde_json::json!({"WorkflowStarted": {
                    "workflow_instance_id": workflow_instance_id
                }}),
            ),
            event(
                5,
                "2026-07-21T10:00:03Z",
                "2026-07-21T10:00:03Z",
                "WorkflowCompleted",
                serde_json::json!({"WorkflowCompleted": {
                    "workflow_instance_id": workflow_instance_id
                }}),
            ),
        ];
        let other_writer = factory.open_writer().await.unwrap();
        let (left, right) = tokio::join!(
            writer.insert_event_page(&concurrent_page),
            other_writer.insert_event_page(&concurrent_page)
        );
        assert_eq!(left.unwrap() + right.unwrap(), 2);
        let rows = reader.events(EventFilter::All, None, 200).await.unwrap();
        assert_eq!(
            rows.iter().rev().map(|row| row.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert_eq!(reader.event_count(EventFilter::All).await.unwrap(), 6);
        other_writer.close().await;

        // A failure after the first row rolls the complete page back and does
        // not consume a public sequence value.
        let failing_page = vec![
            event(
                6,
                "2026-07-21T10:00:04Z",
                "2026-07-21T10:00:04Z",
                "WorkflowStarted",
                Value::Null,
            ),
            event(
                7,
                "2026-07-21T10:00:05Z",
                "2026-07-21T10:00:05Z",
                "FailInsertion",
                Value::Null,
            ),
        ];
        install_failure_trigger(&writer).await;
        assert!(writer.insert_event_page(&failing_page).await.is_err());
        remove_failure_trigger(&writer).await;
        assert_eq!(reader.event_count(EventFilter::All).await.unwrap(), 6);
        assert_eq!(
            writer
                .insert_event_page(std::slice::from_ref(&failing_page[0]))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            reader.events(EventFilter::All, None, 1).await.unwrap()[0].seq,
            7
        );

        // Retention changes only committed rows. Retaining the newest row keeps
        // its derived Archive cursor; deleting all rows causes a safe rebuild,
        // while the independent public sequence remains monotonic.
        assert_eq!(
            writer
                .delete_events_archived_before(instant("2026-07-21T10:00:04Z"))
                .await
                .unwrap(),
            6
        );
        assert_eq!(
            writer.event_archive_cursor().await.unwrap().unwrap().id,
            Uuid::from_u128(6)
        );
        assert_eq!(
            writer
                .delete_events_archived_before(instant("2026-07-21T10:00:05Z"))
                .await
                .unwrap(),
            1
        );
        assert_eq!(writer.event_archive_cursor().await.unwrap(), None);
        assert_eq!(writer.insert_event_page(&first_page).await.unwrap(), 3);
        let rebuilt = reader.events(EventFilter::All, None, 200).await.unwrap();
        assert_eq!(
            rebuilt.iter().map(|row| row.seq).collect::<Vec<_>>(),
            vec![10, 9, 8]
        );

        reader.close().await;
        writer.close().await;
        let reopened_writer = factory.open_writer().await.unwrap();
        let reopened_reader = factory.open_read_only().await.unwrap();
        assert_eq!(
            reopened_writer.event_archive_cursor().await.unwrap(),
            Some(EventArchiveCursor {
                archived_at: instant(tied_archive_time),
                id: Uuid::from_u128(2),
            })
        );
        assert_eq!(
            reopened_reader
                .events(EventFilter::All, None, 200)
                .await
                .unwrap()
                .iter()
                .map(|row| row.seq)
                .collect::<Vec<_>>(),
            vec![10, 9, 8]
        );
        reopened_reader.close().await;
        reopened_writer.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_event_projection_repository_laws() {
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
    async fn file_backed_sqlite_event_projection_repository_laws() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("events.db"));
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
