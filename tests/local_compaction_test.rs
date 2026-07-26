use anyhow::Result;
use chrono::{Duration, Utc};
use prost::Message;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Row;
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;
use tickr::data_directory::DataDirectory;
use tickr::local_compaction::{LocalCompactionDrain, LocalCompactionStager};
use tickr::local_log_staging::{
    FinalLogReference, LocalLogStagingStream, LogExit, LogRecordIdentity, LogRecordSubmission,
    LogStreamIdentity,
};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput,
};
use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::instance::SnapshotTaskInstance;
use tickr_proto::ConductorRelayMessage;
use uuid::Uuid;

const TASK_ENVELOPE: &[u8] = br#"{"v":2,"type":"string","value":"terminal-value","secret":false,"producer":{"kind":"task","task_id":"task-7","task_name":"extract"},"created_at":"2026-07-22T00:00:00Z","sha256":"lineage-a"}"#;

async fn test_formation() -> Result<(TempDir, String, DataDirectory)> {
    let directory = tempfile::tempdir()?;
    let data_path = directory.path().join("data");
    std::fs::create_dir(&data_path)?;
    std::fs::set_permissions(&data_path, std::fs::Permissions::from_mode(0o700))?;
    let data_directory = DataDirectory::admit(&data_path)?;
    let url = format!("sqlite://{}", directory.path().join("tickr.db").display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&url, true)?)
        .await?;
    apply_sqlite(MigrationTarget::Conductor, &pool).await?;
    pool.close().await;
    Ok((directory, url, data_directory))
}

async fn open_writer(url: &str) -> Result<WriterRepositoryBundle> {
    Ok(RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: url.to_owned(),
    })
    .open_writer()
    .await?)
}

struct Fixture {
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
    payload: Vec<u8>,
}

fn fixture() -> Fixture {
    let workflow_id = Uuid::new_v4();
    let workflow_instance_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    let projection = ArchiveProjection {
        id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "local-compaction-recovery".to_owned(),
        state: "Failed".to_owned(),
        scheduled_at: Some(Utc::now().to_rfc3339()),
        task_instances: vec![SnapshotTaskInstance {
            id: task_instance_id.to_string(),
            task_id: Uuid::new_v4().to_string(),
            name: "terminal-task".to_owned(),
            task_type: "RegularTask".to_owned(),
            state: "Failed".to_owned(),
            executor_id: Some(Uuid::new_v4().to_string()),
            attempt: 0,
            ..Default::default()
        }],
        ..Default::default()
    };
    Fixture {
        workflow_instance_id,
        task_instance_id,
        payload: CompactionEnvelope {
            projection: Some(projection),
            correlation: "local-compaction-test".to_owned(),
            shipped_at: Some(Utc::now().to_rfc3339()),
        }
        .encode_to_vec(),
    }
}

async fn create_scope_and_claimed_log(
    writer: &WriterRepositoryBundle,
    data_directory: &DataDirectory,
    fixture: &Fixture,
) -> Result<LogStreamIdentity> {
    let now = Utc::now();
    let run_id = fixture.workflow_instance_id.to_string();
    let values = [ScopeValueInput {
        key: "terminal/value",
        envelope: TASK_ENVELOPE,
    }];
    assert!(matches!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: Uuid::new_v4(),
                namespace: "default",
                run_id: &run_id,
                claim_id: Uuid::new_v4(),
                values: &values,
                now,
            })
            .await?,
        ScopeCreationOutcome::Created
    ));
    let dispatch_key = format!("dispatch-{}", fixture.task_instance_id);
    assert!(
        writer
            .stage_task_dispatch(
                &dispatch_key,
                b"durable local dispatch",
                Some(&fixture.task_instance_id.to_string()),
                Some(&fixture.workflow_instance_id.to_string()),
                now,
            )
            .await?
    );
    let claim = writer
        .claim_task_pickup(
            tickr_migrations::task_pickup_repository::ClaimTaskPickupInput {
                dispatch_key: &dispatch_key,
                owner: "local-executor",
                liveness_deadline: now + Duration::minutes(1),
                assigned_event: b"assigned",
                now,
            },
        )
        .await?;
    let tickr_migrations::task_pickup_repository::ClaimTaskPickupOutcome::Committed(claim) = claim
    else {
        panic!("fresh local dispatch must be claimed");
    };
    let stream = LogStreamIdentity {
        task_instance_id: fixture.task_instance_id,
        pickup_generation: claim.pickup_generation.try_into()?,
    };
    let mut log = LocalLogStagingStream::open(data_directory, stream.clone())?;
    log.accept(LogRecordSubmission::new(
        LogRecordIdentity {
            stream: stream.clone(),
            sequence: 0,
        },
        b"failure evidence".to_vec(),
    ))?;
    log.finish_cleanly(LogExit::Status(1))?;
    Ok(stream)
}

async fn staging_state(url: &str, workflow_instance_id: Uuid) -> Result<(String, Option<Vec<u8>>)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(url, false)?)
        .await?;
    let row = sqlx::query(
        "SELECT state, payload FROM local_compaction_staging WHERE workflow_instance_id = ?1",
    )
    .bind(workflow_instance_id.to_string())
    .fetch_one(&pool)
    .await?;
    let result = (row.try_get("state")?, row.try_get("payload")?);
    pool.close().await;
    Ok(result)
}

async fn scope_state(url: &str, workflow_instance_id: Uuid) -> Result<String> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(url, false)?)
        .await?;
    let state = sqlx::query_scalar(
        "SELECT state FROM tickr_ctx_scopes WHERE namespace = ?1 AND run_id = ?2",
    )
    .bind("default")
    .bind(workflow_instance_id.to_string())
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(state)
}

#[tokio::test]
async fn local_compaction_replays_after_restart_and_purges_only_after_archive_commit() -> Result<()>
{
    let (_temporary, url, data_directory) = test_formation().await?;
    let fixture = fixture();
    let writer = open_writer(&url).await?;
    let stream = create_scope_and_claimed_log(&writer, &data_directory, &fixture).await?;

    let acknowledgement: ConductorRelayMessage = LocalCompactionStager::new(&writer)
        .stage_for_relay(&fixture.payload)
        .await?;
    assert!(!acknowledgement.payload.is_empty());
    assert_eq!(
        staging_state(&url, fixture.workflow_instance_id).await?.0,
        "staged"
    );
    writer.close().await;

    let writer = open_writer(&url).await?;
    let drain = LocalCompactionDrain::new(&writer, &data_directory);
    assert!(drain.drain_next().await?);
    assert!(!drain.drain_next().await?);
    assert_eq!(
        staging_state(&url, fixture.workflow_instance_id).await?,
        ("purged".to_owned(), None)
    );
    assert_eq!(
        scope_state(&url, fixture.workflow_instance_id).await?,
        "cleaned"
    );
    writer.close().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&url, false)?)
        .await?;
    let archive_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
            .bind(fixture.workflow_instance_id.to_string())
            .fetch_one(&pool)
            .await?;
    assert_eq!(archive_count, 1);
    let final_references: Vec<FinalLogReference> = serde_json::from_value(
        sqlx::query_scalar::<_, String>(
            "SELECT log_uris FROM workflow_run_info WHERE workflow_instance_id = ?1",
        )
        .bind(fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await?
        .parse::<Value>()?,
    )?;
    assert_eq!(final_references.len(), 1);
    assert_eq!(final_references[0].stream, stream);
    LocalLogStagingStream::verify_final(&data_directory, &final_references[0])?;
    pool.close().await;

    let writer = open_writer(&url).await?;
    LocalCompactionStager::new(&writer)
        .stage_for_relay(&fixture.payload)
        .await?;
    assert!(
        !LocalCompactionDrain::new(&writer, &data_directory)
            .drain_next()
            .await?
    );
    writer.close().await;
    Ok(())
}

#[tokio::test]
async fn local_compaction_refuses_to_invent_a_missing_log_generation() -> Result<()> {
    let (_temporary, url, data_directory) = test_formation().await?;
    let fixture = fixture();
    let writer = open_writer(&url).await?;
    let run_id = fixture.workflow_instance_id.to_string();
    let values = [ScopeValueInput {
        key: "terminal/value",
        envelope: TASK_ENVELOPE,
    }];
    assert!(matches!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: Uuid::new_v4(),
                namespace: "default",
                run_id: &run_id,
                claim_id: Uuid::new_v4(),
                values: &values,
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created
    ));
    LocalCompactionStager::new(&writer)
        .stage_for_relay(&fixture.payload)
        .await?;

    let error = LocalCompactionDrain::new(&writer, &data_directory)
        .drain_next()
        .await
        .expect_err("Compaction must reject a task without a durable pickup generation");
    assert!(error
        .to_string()
        .contains("local log stream inventory does not match"));
    assert_eq!(
        staging_state(&url, fixture.workflow_instance_id).await?.0,
        "staged"
    );
    writer.close().await;
    Ok(())
}
