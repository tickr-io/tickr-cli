//! Tickr Lite's local Compaction stage and drain roles.
//!
//! Relay handling only stores the published envelope and returns its ACK. The
//! drain owns every irreversible archive side effect, and it purges each local
//! source only after the linked archive transaction has committed.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tickr_conductor::system_tasks::build_ack;
use tickr_migrations::archive_repository::{
    ArchiveTerminalWorkflowInput, LocalCompactionArchiveCompletion,
};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::compaction_repository::{
    LocalCompactionDrainRecord, PurgeLocalCompactionOutcome, StageLocalCompactionInput,
    StageLocalCompactionOutcome,
};
use tickr_migrations::scope_repository::{
    decode_tickr_ctx_scope_snapshot, CreateTickrCtxScopeInput, ScopeCleanupOutcome,
    ScopeCreationOutcome, ScopeSnapshotOutcome,
};
use tickr_migrations::task_pickup_repository::LocalTaskLogStream;
use tickr_proto::codec::compaction::decode_envelope;
use tickr_proto::ConductorRelayMessage;
use uuid::Uuid;

use crate::data_directory::DataDirectory;
use crate::local_log_staging::{FinalLogReference, LocalLogStagingStream, LogStreamIdentity};

const DEFAULT_CTX_NAMESPACE: &str = "default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionBoundary {
    StagingCommitted,
    ScopeSnapshotted,
    LogSealed,
    FinalLogInstalled,
    ArchiveCommitted,
    CompletionRecorded,
    ScopeCleaned,
    LogStagingPurged,
    CompactionStagingPurged,
}

#[cfg(not(test))]
fn observe_compaction_boundary(_: CompactionBoundary) {}

#[cfg(test)]
fn observe_compaction_boundary(boundary: CompactionBoundary) {
    let Ok(requested) = std::env::var("TICKR_COMPACTION_CRASH_AT") else {
        return;
    };
    let actual = match boundary {
        CompactionBoundary::StagingCommitted => "staging-committed",
        CompactionBoundary::ScopeSnapshotted => "scope-snapshotted",
        CompactionBoundary::LogSealed => "log-sealed",
        CompactionBoundary::FinalLogInstalled => "final-log-installed",
        CompactionBoundary::ArchiveCommitted => "archive-committed",
        CompactionBoundary::CompletionRecorded => "completion-recorded",
        CompactionBoundary::ScopeCleaned => "scope-cleaned",
        CompactionBoundary::LogStagingPurged => "log-staging-purged",
        CompactionBoundary::CompactionStagingPurged => "compaction-staging-purged",
    };
    if requested == actual {
        std::process::exit(86);
    }
}

pub struct LocalCompactionStager<'a> {
    repository: &'a WriterRepositoryBundle,
}

impl<'a> LocalCompactionStager<'a> {
    pub const fn new(repository: &'a WriterRepositoryBundle) -> Self {
        Self { repository }
    }

    /// Stage bytes before returning `COMPACTION_ACK`; no terminal archive
    /// repository operation is reachable from this method.
    pub async fn stage_for_relay(&self, payload: &[u8]) -> Result<ConductorRelayMessage> {
        let envelope = decode_envelope(payload).context("decode Compaction envelope")?;
        let projection = envelope
            .projection
            .as_ref()
            .ok_or_else(|| anyhow!("Compaction envelope has no archive projection"))?;
        let workflow_instance_id = Uuid::parse_str(&projection.id)
            .context("Compaction archive projection has invalid workflow instance id")?;
        let scope_claim_id = Uuid::new_v5(&workflow_instance_id, b"tickr-lite-ctx-scope");
        match self
            .repository
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: workflow_instance_id,
                namespace: DEFAULT_CTX_NAMESPACE,
                run_id: &projection.id,
                claim_id: scope_claim_id,
                values: &[],
                now: Utc::now(),
            })
            .await
            .context("ensure tickr-ctx scope before Compaction staging")?
        {
            ScopeCreationOutcome::Created
            | ScopeCreationOutcome::Idempotent
            | ScopeCreationOutcome::Collision { .. } => {}
            outcome => return Err(anyhow!("ensure tickr-ctx scope: {outcome:?}")),
        }
        let payload_digest = digest(payload);
        let outcome = self
            .repository
            .stage_local_compaction(StageLocalCompactionInput {
                workflow_instance_id,
                payload,
                payload_digest: &payload_digest,
                now: Utc::now(),
            })
            .await
            .context("durably stage local Compaction")?;
        observe_compaction_boundary(CompactionBoundary::StagingCommitted);
        match outcome {
            StageLocalCompactionOutcome::Staged
            | StageLocalCompactionOutcome::AlreadyStaged
            | StageLocalCompactionOutcome::AlreadyComplete
            | StageLocalCompactionOutcome::AlreadyPurged => {
                Ok(build_ack(&projection.id, &envelope.correlation))
            }
        }
    }
}

pub struct LocalCompactionDrain<'a> {
    repository: &'a WriterRepositoryBundle,
    data_directory: &'a DataDirectory,
}

impl<'a> LocalCompactionDrain<'a> {
    pub const fn new(
        repository: &'a WriterRepositoryBundle,
        data_directory: &'a DataDirectory,
    ) -> Self {
        Self {
            repository,
            data_directory,
        }
    }

    /// Drain one staged record or resume cleanup for one committed archive.
    ///
    /// The drain derives every generation-qualified local Log staging stream
    /// from durable task-pickup ownership, so a formation cannot accidentally
    /// substitute an unqualified inventory.
    pub async fn drain_next(&self) -> Result<bool> {
        let Some(record) = self
            .repository
            .select_local_compaction_for_drain()
            .await
            .context("select local Compaction drain work")?
        else {
            return Ok(false);
        };
        match record {
            LocalCompactionDrainRecord::Staged {
                workflow_instance_id,
                payload,
                payload_digest,
            } => {
                self.drain_staged(workflow_instance_id, payload, payload_digest)
                    .await?
            }
            LocalCompactionDrainRecord::Complete {
                workflow_instance_id,
                payload,
                payload_digest,
                scope_id,
                scope_digest,
                final_log_references,
            } => {
                let references: Vec<FinalLogReference> =
                    serde_json::from_value(final_log_references)
                        .context("decode committed final-Log references")?;
                self.finish_cleanup(
                    workflow_instance_id,
                    &payload,
                    &payload_digest,
                    scope_id,
                    &scope_digest,
                    &references,
                )
                .await?;
            }
        }
        Ok(true)
    }

    async fn drain_staged(
        &self,
        staged_workflow_instance_id: Uuid,
        payload: Vec<u8>,
        payload_digest: String,
    ) -> Result<()> {
        if digest(&payload) != payload_digest {
            return Err(anyhow!("staged Compaction payload digest mismatch"));
        }
        let envelope = decode_envelope(&payload).context("decode staged Compaction envelope")?;
        let projection = envelope
            .projection
            .as_ref()
            .ok_or_else(|| anyhow!("staged Compaction envelope has no archive projection"))?;
        let workflow_instance_id = Uuid::parse_str(&projection.id)
            .context("staged Compaction archive projection has invalid workflow instance id")?;
        if workflow_instance_id != staged_workflow_instance_id {
            return Err(anyhow!(
                "staged Compaction identity does not match its payload"
            ));
        }
        let streams = self
            .repository
            .local_task_log_streams_for_workflow_instance(workflow_instance_id)
            .await
            .context("select local Log streams for Compaction")?
            .into_iter()
            .map(
                |LocalTaskLogStream {
                     task_instance_id,
                     pickup_generation,
                 }| LogStreamIdentity {
                    task_instance_id,
                    pickup_generation,
                },
            )
            .collect::<Vec<_>>();
        validate_streams(projection, &streams)?;

        let snapshot = match self
            .repository
            .snapshot_tickr_ctx_scope_for_run(DEFAULT_CTX_NAMESPACE, &projection.id, Utc::now())
            .await
            .context("snapshot tickr-ctx scope")?
        {
            ScopeSnapshotOutcome::Committed(snapshot)
            | ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
            ScopeSnapshotOutcome::Missing => return Err(anyhow!("tickr-ctx scope is missing")),
            ScopeSnapshotOutcome::Bound(bound) => {
                return Err(anyhow!("tickr-ctx scope exceeds its bound: {bound:?}"))
            }
            ScopeSnapshotOutcome::Quarantined { diagnostic, .. } => {
                return Err(anyhow!("tickr-ctx scope is quarantined: {diagnostic}"));
            }
        };
        let ctx_envelope = archive_scope_entries(&snapshot)?;
        observe_compaction_boundary(CompactionBoundary::ScopeSnapshotted);

        let mut references = Vec::with_capacity(streams.len());
        for stream in streams {
            let mut staging = LocalLogStagingStream::open_existing(self.data_directory, stream)
                .context("open existing local log staging stream")?;
            let seal = staging.seal().context("seal local log staging stream")?;
            observe_compaction_boundary(CompactionBoundary::LogSealed);
            references.push(
                LocalLogStagingStream::install_final(self.data_directory, &seal)
                    .context("install local final log")?,
            );
            observe_compaction_boundary(CompactionBoundary::FinalLogInstalled);
        }
        let final_log_references =
            serde_json::to_value(&references).context("encode local final-Log references")?;
        let archived_at = envelope
            .shipped_at
            .as_deref()
            .map(DateTime::parse_from_rfc3339)
            .transpose()
            .context("parse Compaction shipped_at")?
            .map(|time| time.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        let runtime_params = json!({
            "tickr_lite_scope": {
                "scope_id": snapshot.scope_id,
                "digest": snapshot.digest,
                "row_count": snapshot.row_count,
                "value_bytes": snapshot.value_bytes,
            }
        });
        self.repository
            .archive_staged_local_compaction(
                ArchiveTerminalWorkflowInput {
                    projection,
                    ctx_envelope,
                    runtime_params,
                    log_uris: final_log_references.clone(),
                    archived_at,
                },
                LocalCompactionArchiveCompletion {
                    workflow_instance_id,
                    payload_digest: payload_digest.clone(),
                    scope_id: snapshot.scope_id,
                    scope_digest: snapshot.digest.clone(),
                    final_log_references,
                    completed_at: Utc::now(),
                },
            )
            .await
            .context("commit local terminal archive")?;
        observe_compaction_boundary(CompactionBoundary::ArchiveCommitted);
        // Completion is part of the archive transaction. Separate observation
        // proves that no crash can expose one durable effect without the other.
        observe_compaction_boundary(CompactionBoundary::CompletionRecorded);

        self.finish_cleanup(
            workflow_instance_id,
            &payload,
            &payload_digest,
            snapshot.scope_id,
            &snapshot.digest,
            &references,
        )
        .await
    }

    async fn finish_cleanup(
        &self,
        workflow_instance_id: Uuid,
        payload: &[u8],
        payload_digest: &str,
        scope_id: Uuid,
        scope_digest: &str,
        references: &[FinalLogReference],
    ) -> Result<()> {
        if digest(payload) != payload_digest {
            return Err(anyhow!("completed Compaction payload digest mismatch"));
        }
        let envelope = decode_envelope(payload).context("decode completed Compaction envelope")?;
        let projection = envelope
            .projection
            .as_ref()
            .ok_or_else(|| anyhow!("completed Compaction envelope has no archive projection"))?;
        let projection_id = Uuid::parse_str(&projection.id)
            .context("completed Compaction projection has invalid workflow instance id")?;
        if projection_id != workflow_instance_id {
            return Err(anyhow!(
                "completed Compaction identity does not match its payload"
            ));
        }
        let reference_streams = references
            .iter()
            .map(|reference| reference.stream.clone())
            .collect::<Vec<_>>();
        validate_streams(projection, &reference_streams)?;

        let snapshot = match self
            .repository
            .snapshot_tickr_ctx_scope_for_run(DEFAULT_CTX_NAMESPACE, &projection.id, Utc::now())
            .await
            .context("verify committed tickr-ctx snapshot")?
        {
            ScopeSnapshotOutcome::Committed(snapshot)
            | ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
            ScopeSnapshotOutcome::Missing => return Err(anyhow!("tickr-ctx scope is missing")),
            ScopeSnapshotOutcome::Bound(bound) => {
                return Err(anyhow!("tickr-ctx scope exceeds its bound: {bound:?}"))
            }
            ScopeSnapshotOutcome::Quarantined { diagnostic, .. } => {
                return Err(anyhow!("tickr-ctx scope is quarantined: {diagnostic}"));
            }
        };
        if snapshot.scope_id != scope_id || snapshot.digest != scope_digest {
            return Err(anyhow!(
                "committed Compaction scope identity or digest mismatch"
            ));
        }
        decode_tickr_ctx_scope_snapshot(&snapshot)
            .context("verify committed tickr-ctx snapshot bytes")?;
        for reference in references {
            LocalLogStagingStream::verify_final(self.data_directory, reference)
                .context("verify committed final Log before cleanup")?;
        }

        match self
            .repository
            .cleanup_tickr_ctx_scope(scope_id, Utc::now())
            .await
            .context("clean tickr-ctx scope after archive")?
        {
            ScopeCleanupOutcome::Cleaned | ScopeCleanupOutcome::AlreadyCleaned => {}
            outcome => {
                return Err(anyhow!(
                    "cannot clean archived tickr-ctx scope: {outcome:?}"
                ))
            }
        }
        observe_compaction_boundary(CompactionBoundary::ScopeCleaned);
        for reference in references {
            LocalLogStagingStream::purge_staged(self.data_directory, reference)
                .context("purge local log staging stream")?;
        }
        observe_compaction_boundary(CompactionBoundary::LogStagingPurged);
        match self
            .repository
            .purge_completed_local_compaction(workflow_instance_id, Utc::now())
            .await
            .context("purge completed Compaction staging")?
        {
            PurgeLocalCompactionOutcome::Purged | PurgeLocalCompactionOutcome::AlreadyPurged => {
                observe_compaction_boundary(CompactionBoundary::CompactionStagingPurged);
                Ok(())
            }
            outcome => Err(anyhow!(
                "cannot purge completed Compaction staging: {outcome:?}"
            )),
        }
    }
}

fn archive_scope_entries(
    snapshot: &tickr_migrations::scope_repository::TickrCtxScopeSnapshot,
) -> Result<Value> {
    decode_tickr_ctx_scope_snapshot(snapshot)
        .context("decode committed tickr-ctx snapshot")?
        .into_iter()
        .map(|(key, envelope)| {
            Ok(json!({
                "key": key,
                "envelope": serde_json::from_slice::<Value>(&envelope)
                    .context("decode opaque tickr-ctx envelope for archive response")?,
            }))
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

fn validate_streams(
    projection: &tickr_proto::archive::ArchiveProjection,
    streams: &[LogStreamIdentity],
) -> Result<()> {
    let expected = projection
        .task_instances
        .iter()
        .map(|task| Uuid::parse_str(&task.id).context("archive task instance has invalid id"))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let actual = streams
        .iter()
        .map(|stream| stream.task_instance_id)
        .collect();
    if expected == actual && expected.len() == streams.len() {
        Ok(())
    } else {
        Err(anyhow!(
            "local log stream inventory does not match archived task instances"
        ))
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    use chrono::Duration;
    use prost::Message;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
    use tickr_migrations::scope_repository::{
        CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput,
    };
    use tickr_migrations::signal_repository::{SignalCapturesInput, SignalLinkageOutcome};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
    use tickr_proto::config::DataPlaneSql;
    use tickr_proto::instance::SnapshotTaskInstance;

    use super::*;
    use crate::local_log_staging::{LogExit, LogRecordIdentity};

    const TEST_ENVELOPE: &[u8] = br#"{"v":2,"type":"string","value":"terminal-value","secret":false,"producer":{"kind":"task","task_id":"task-7","task_name":"extract"},"created_at":"2026-07-22T00:00:00Z","sha256":"lineage-a"}"#;

    struct Fixture {
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        payload: Vec<u8>,
    }

    struct TestFormation {
        _temporary: TempDir,
        data_path: PathBuf,
        database_url: String,
        payload_path: PathBuf,
        acknowledgement_path: PathBuf,
        fixture: Fixture,
        stream: Option<LogStreamIdentity>,
        signal_id: Option<Uuid>,
    }

    fn fixture() -> Fixture {
        let workflow_instance_id = Uuid::new_v4();
        let task_instance_id = Uuid::new_v4();
        let workflow_id = Uuid::new_v4();
        let projection = ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "local-compaction-crash-test".to_owned(),
            state: "Failed".to_owned(),
            scheduled_at: Some(Utc::now().to_rfc3339()),
            task_instances: vec![SnapshotTaskInstance {
                id: task_instance_id.to_string(),
                task_id: Uuid::new_v4().to_string(),
                name: "terminal-task".to_owned(),
                task_type: "RegularTask".to_owned(),
                state: "Failed".to_owned(),
                executor_id: Some(Uuid::new_v4().to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        Fixture {
            workflow_id,
            workflow_instance_id,
            task_instance_id,
            payload: CompactionEnvelope {
                projection: Some(projection),
                correlation: "local-compaction-crash-test".to_owned(),
                shipped_at: Some(Utc::now().to_rfc3339()),
            }
            .encode_to_vec(),
        }
    }

    async fn empty_formation() -> TestFormation {
        let temporary = tempfile::tempdir().unwrap();
        let data_path = temporary.path().join("data");
        fs::create_dir(&data_path).unwrap();
        fs::set_permissions(&data_path, fs::Permissions::from_mode(0o700)).unwrap();
        let data_directory = DataDirectory::admit(&data_path).unwrap();
        let database_url = format!("sqlite://{}", temporary.path().join("tickr.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&database_url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &pool)
            .await
            .unwrap();
        pool.close().await;
        drop(data_directory);

        let fixture = fixture();
        let payload_path = temporary.path().join("compaction-envelope.bin");
        fs::write(&payload_path, &fixture.payload).unwrap();
        TestFormation {
            acknowledgement_path: temporary.path().join("compaction-ack.bin"),
            _temporary: temporary,
            data_path,
            database_url,
            payload_path,
            fixture,
            stream: None,
            signal_id: None,
        }
    }

    async fn open_writer(database_url: &str) -> WriterRepositoryBundle {
        RepositoryFactory::new(DataPlaneSql::Sqlite {
            url: database_url.to_owned(),
        })
        .open_writer()
        .await
        .unwrap()
    }

    async fn drain_formation() -> TestFormation {
        let mut formation = empty_formation().await;
        let data_directory = DataDirectory::admit(&formation.data_path).unwrap();
        let writer = open_writer(&formation.database_url).await;
        let now = Utc::now();
        let run_id = formation.fixture.workflow_instance_id.to_string();
        let values = [ScopeValueInput {
            key: "terminal/value",
            envelope: TEST_ENVELOPE,
        }];
        assert!(matches!(
            writer
                .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                    scope_id: Uuid::new_v4(),
                    namespace: DEFAULT_CTX_NAMESPACE,
                    run_id: &run_id,
                    claim_id: Uuid::new_v4(),
                    values: &values,
                    now,
                })
                .await
                .unwrap(),
            ScopeCreationOutcome::Created
        ));
        let signal_id = Uuid::new_v4();
        assert!(writer
            .insert_signal_captures(&SignalCapturesInput {
                signal_id,
                workflow_id: formation.fixture.workflow_id,
                workflow_version: Some(1),
                captures: json!([{
                    "name": "terminal_event",
                    "envelope": {
                        "present": true,
                        "value": {"result": "failed"},
                        "producer": {
                            "kind": "Signal",
                            "signal_id": signal_id,
                            "source": {"Manual": {}}
                        },
                        "lineage": [{"segment": "inputs.terminal_event"}]
                    }
                }]),
            })
            .await
            .unwrap());
        assert_eq!(
            writer
                .link_signal_captures(signal_id, formation.fixture.workflow_instance_id)
                .await
                .unwrap(),
            SignalLinkageOutcome::Linked
        );
        let dispatch_key = format!("dispatch-{}", formation.fixture.task_instance_id);
        assert!(writer
            .stage_task_dispatch(
                &dispatch_key,
                b"durable local dispatch",
                Some(&formation.fixture.task_instance_id.to_string()),
                Some(&formation.fixture.workflow_instance_id.to_string()),
                now,
            )
            .await
            .unwrap());
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
            .await
            .unwrap();
        let tickr_migrations::task_pickup_repository::ClaimTaskPickupOutcome::Committed(claim) =
            claim
        else {
            panic!("fresh local dispatch must be claimed");
        };
        let stream = LogStreamIdentity {
            task_instance_id: formation.fixture.task_instance_id,
            pickup_generation: claim.pickup_generation.try_into().unwrap(),
        };
        let mut log = LocalLogStagingStream::open(&data_directory, stream.clone()).unwrap();
        log.accept(
            LogRecordIdentity {
                stream: stream.clone(),
                sequence: 0,
            },
            b"failure evidence".to_vec(),
        )
        .unwrap();
        log.finish_cleanly(LogExit::Status(1)).unwrap();
        drop(log);
        LocalCompactionStager::new(&writer)
            .stage_for_relay(&formation.fixture.payload)
            .await
            .unwrap();
        writer.close().await;
        drop(data_directory);
        formation.stream = Some(stream);
        formation.signal_id = Some(signal_id);
        formation
    }

    fn child_status(
        formation: &TestFormation,
        operation: &str,
        crash_at: &str,
    ) -> std::process::ExitStatus {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local_compaction::tests::child_compaction_process",
                "--nocapture",
            ])
            .env("TICKR_COMPACTION_CHILD_ROOT", &formation.data_path)
            .env("TICKR_COMPACTION_CHILD_DATABASE", &formation.database_url)
            .env("TICKR_COMPACTION_CHILD_PAYLOAD", &formation.payload_path)
            .env(
                "TICKR_COMPACTION_CHILD_ACK",
                &formation.acknowledgement_path,
            )
            .env("TICKR_COMPACTION_CHILD_OPERATION", operation)
            .env("TICKR_COMPACTION_CRASH_AT", crash_at)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .unwrap()
    }

    #[test]
    fn child_compaction_process() {
        let Ok(root) = std::env::var("TICKR_COMPACTION_CHILD_ROOT") else {
            return;
        };
        let database_url = std::env::var("TICKR_COMPACTION_CHILD_DATABASE").unwrap();
        let operation = std::env::var("TICKR_COMPACTION_CHILD_OPERATION").unwrap();
        let payload = fs::read(std::env::var("TICKR_COMPACTION_CHILD_PAYLOAD").unwrap()).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let data_directory = DataDirectory::admit(root).unwrap();
            let writer = open_writer(&database_url).await;
            match operation.as_str() {
                "stage" => {
                    let acknowledgement = LocalCompactionStager::new(&writer)
                        .stage_for_relay(&payload)
                        .await
                        .unwrap();
                    assert!(!acknowledgement.payload.is_empty());
                    if std::env::var("TICKR_COMPACTION_CRASH_AT").as_deref() == Ok("after-ack") {
                        let mut marker =
                            fs::File::create(std::env::var("TICKR_COMPACTION_CHILD_ACK").unwrap())
                                .unwrap();
                        marker.write_all(&acknowledgement.payload).unwrap();
                        marker.sync_all().unwrap();
                        std::process::exit(86);
                    }
                }
                "drain" => {
                    assert!(LocalCompactionDrain::new(&writer, &data_directory)
                        .drain_next()
                        .await
                        .unwrap());
                }
                other => panic!("unknown child Compaction operation {other}"),
            }
            writer.close().await;
        });
    }

    async fn open_test_pool(database_url: &str) -> sqlx::SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(database_url, false).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn relay_stage_and_ack_survive_real_process_crashes_without_archiving() {
        let formation = empty_formation().await;
        assert_eq!(
            child_status(&formation, "stage", "staging-committed").code(),
            Some(86)
        );
        let pool = open_test_pool(&formation.database_url).await;
        let state: String = sqlx::query_scalar(
            "SELECT state FROM local_compaction_staging WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(state, "staged");
        let archives: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                .bind(formation.fixture.workflow_instance_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(archives, 0);
        pool.close().await;

        assert_eq!(
            child_status(&formation, "stage", "after-ack").code(),
            Some(86)
        );
        assert!(!fs::read(&formation.acknowledgement_path)
            .unwrap()
            .is_empty());
        let writer = open_writer(&formation.database_url).await;
        let duplicate_ack = LocalCompactionStager::new(&writer)
            .stage_for_relay(&formation.fixture.payload)
            .await
            .unwrap();
        assert!(!duplicate_ack.payload.is_empty());
        writer.close().await;

        let pool = open_test_pool(&formation.database_url).await;
        let archives: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                .bind(formation.fixture.workflow_instance_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(archives, 0);
        pool.close().await;
    }

    #[tokio::test]
    async fn real_process_crashes_at_every_drain_boundary_converge() {
        let boundaries = [
            "scope-snapshotted",
            "log-sealed",
            "final-log-installed",
            "archive-committed",
            "completion-recorded",
            "scope-cleaned",
            "log-staging-purged",
            "compaction-staging-purged",
        ];
        for boundary in boundaries {
            let formation = drain_formation().await;
            assert_eq!(
                child_status(&formation, "drain", boundary).code(),
                Some(86),
                "child did not crash at {boundary}"
            );
            assert_archive_atomic_state(&formation, boundary).await;

            let data_directory = DataDirectory::admit(&formation.data_path).unwrap();
            let writer = open_writer(&formation.database_url).await;
            let progressed = LocalCompactionDrain::new(&writer, &data_directory)
                .drain_next()
                .await
                .unwrap();
            assert_eq!(
                progressed,
                boundary != "compaction-staging-purged",
                "unexpected restart work at {boundary}"
            );
            assert!(!LocalCompactionDrain::new(&writer, &data_directory)
                .drain_next()
                .await
                .unwrap());
            writer.close().await;
            drop(data_directory);
            assert_converged(&formation).await;
        }
    }

    async fn assert_archive_atomic_state(formation: &TestFormation, boundary: &str) {
        let committed = matches!(
            boundary,
            "archive-committed"
                | "completion-recorded"
                | "scope-cleaned"
                | "log-staging-purged"
                | "compaction-staging-purged"
        );
        let pool = open_test_pool(&formation.database_url).await;
        let archive_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                .bind(formation.fixture.workflow_instance_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        let staging_state: String = sqlx::query_scalar(
            "SELECT state FROM local_compaction_staging WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        let terminal_at: Option<i64> =
            sqlx::query_scalar("SELECT terminal_at FROM signal_captures WHERE signal_id = ?1")
                .bind(formation.signal_id.unwrap().to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(archive_count, i64::from(committed));
        assert_eq!(terminal_at.is_some(), committed);
        assert_eq!(
            staging_state,
            if committed {
                if boundary == "compaction-staging-purged" {
                    "purged"
                } else {
                    "complete"
                }
            } else {
                "staged"
            }
        );
        pool.close().await;
    }

    async fn assert_converged(formation: &TestFormation) {
        let pool = open_test_pool(&formation.database_url).await;
        let (state, payload_is_null, staged_scope_digest, staging_references): (
            String,
            i64,
            String,
            String,
        ) = sqlx::query_as(
            "SELECT state, payload IS NULL, scope_digest, final_log_references \
             FROM local_compaction_staging WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(state, "purged");
        assert_eq!(payload_is_null, 1);
        let (scope_state, snapshot_digest): (String, String) = sqlx::query_as(
            "SELECT state, snapshot_digest FROM tickr_ctx_scopes \
             WHERE namespace = ?1 AND run_id = ?2",
        )
        .bind(DEFAULT_CTX_NAMESPACE)
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(scope_state, "cleaned");
        assert_eq!(snapshot_digest, staged_scope_digest);
        let workflow_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                .bind(formation.fixture.workflow_instance_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(workflow_count, 1);
        let run_info_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM workflow_run_info WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(run_info_count, 1);
        let terminal_at: Option<i64> =
            sqlx::query_scalar("SELECT terminal_at FROM signal_captures WHERE signal_id = ?1")
                .bind(formation.signal_id.unwrap().to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(terminal_at.is_some());
        let task_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_instances WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(task_count, 1);
        let run_info_references: String = sqlx::query_scalar(
            "SELECT log_uris FROM workflow_run_info WHERE workflow_instance_id = ?1",
        )
        .bind(formation.fixture.workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&staging_references).unwrap(),
            serde_json::from_str::<Value>(&run_info_references).unwrap()
        );
        let references: Vec<FinalLogReference> = serde_json::from_str(&staging_references).unwrap();
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].stream, formation.stream.clone().unwrap());
        pool.close().await;

        let data_directory = DataDirectory::admit(&formation.data_path).unwrap();
        LocalLogStagingStream::verify_final(&data_directory, &references[0]).unwrap();
        let writer = open_writer(&formation.database_url).await;
        LocalCompactionStager::new(&writer)
            .stage_for_relay(&formation.fixture.payload)
            .await
            .unwrap();
        assert!(!LocalCompactionDrain::new(&writer, &data_directory)
            .drain_next()
            .await
            .unwrap());
        writer.close().await;
    }

    #[derive(Clone, Copy)]
    enum Corruption {
        MissingScope,
        UnknownScopeVersion,
        MissingLog,
        UnreadableLog,
        CorruptLog,
        UnknownLogVersion,
        CorruptCompactionPayload,
        UnknownCompactionVersion,
    }

    impl Corruption {
        const fn expected(self) -> &'static str {
            match self {
                Self::MissingScope => "scope is missing",
                Self::UnknownScopeVersion => "uses unknown protocol version",
                Self::MissingLog => "open existing local log staging stream",
                Self::UnreadableLog => "open existing local log staging stream",
                Self::CorruptLog => "decode local log staging frame",
                Self::UnknownLogVersion => "unknown local log staging journal format",
                Self::CorruptCompactionPayload => "payload digest mismatch",
                Self::UnknownCompactionVersion => "unknown protocol version",
            }
        }
    }

    #[tokio::test]
    async fn invalid_scope_log_and_staging_state_never_archives_a_substitute() {
        let cases = [
            Corruption::MissingScope,
            Corruption::UnknownScopeVersion,
            Corruption::MissingLog,
            Corruption::UnreadableLog,
            Corruption::CorruptLog,
            Corruption::UnknownLogVersion,
            Corruption::CorruptCompactionPayload,
            Corruption::UnknownCompactionVersion,
        ];
        for corruption in cases {
            let formation = drain_formation().await;
            corrupt(&formation, corruption).await;
            let data_directory = DataDirectory::admit(&formation.data_path).unwrap();
            let writer = open_writer(&formation.database_url).await;
            let error = LocalCompactionDrain::new(&writer, &data_directory)
                .drain_next()
                .await
                .expect_err("corrupt Compaction input must not archive");
            assert!(
                format!("{error:#}").contains(corruption.expected()),
                "unexpected error for {}: {error:#}",
                corruption.expected()
            );
            writer.close().await;
            drop(data_directory);

            let pool = open_test_pool(&formation.database_url).await;
            let archives: i64 =
                sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
                    .bind(formation.fixture.workflow_instance_id.to_string())
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(archives, 0);
            let state: String = sqlx::query_scalar(
                "SELECT state FROM local_compaction_staging WHERE workflow_instance_id = ?1",
            )
            .bind(formation.fixture.workflow_instance_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(state, "staged");
            pool.close().await;
            if matches!(corruption, Corruption::MissingLog) {
                assert!(!log_journal_path(&formation).exists());
            }
        }
    }

    async fn corrupt(formation: &TestFormation, corruption: Corruption) {
        let pool = open_test_pool(&formation.database_url).await;
        match corruption {
            Corruption::MissingScope => {
                sqlx::query("DELETE FROM tickr_ctx_scopes WHERE namespace = ?1 AND run_id = ?2")
                    .bind(DEFAULT_CTX_NAMESPACE)
                    .bind(formation.fixture.workflow_instance_id.to_string())
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            Corruption::UnknownScopeVersion => {
                sqlx::query("PRAGMA ignore_check_constraints = ON")
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE tickr_ctx_scopes SET protocol_version = 99 \
                     WHERE namespace = ?1 AND run_id = ?2",
                )
                .bind(DEFAULT_CTX_NAMESPACE)
                .bind(formation.fixture.workflow_instance_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
            Corruption::CorruptCompactionPayload => {
                sqlx::query(
                    "UPDATE local_compaction_staging SET payload = '[0]' \
                     WHERE workflow_instance_id = ?1",
                )
                .bind(formation.fixture.workflow_instance_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
            Corruption::UnknownCompactionVersion => {
                sqlx::query("PRAGMA ignore_check_constraints = ON")
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE local_compaction_staging SET protocol_version = 99 \
                     WHERE workflow_instance_id = ?1",
                )
                .bind(formation.fixture.workflow_instance_id.to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
            Corruption::MissingLog
            | Corruption::UnreadableLog
            | Corruption::CorruptLog
            | Corruption::UnknownLogVersion => {}
        }
        pool.close().await;

        let journal = log_journal_path(formation);
        match corruption {
            Corruption::MissingLog => fs::remove_file(journal).unwrap(),
            Corruption::UnreadableLog => {
                fs::set_permissions(journal, fs::Permissions::from_mode(0o000)).unwrap();
            }
            Corruption::CorruptLog => {
                let mut bytes = b"tickr-local-log-v1\n".to_vec();
                bytes.extend_from_slice(&5_u32.to_le_bytes());
                bytes.extend_from_slice(b"xxxxx");
                fs::write(journal, bytes).unwrap();
            }
            Corruption::UnknownLogVersion => {
                fs::write(journal, b"tickr-local-log-v999\n").unwrap();
            }
            Corruption::MissingScope
            | Corruption::UnknownScopeVersion
            | Corruption::CorruptCompactionPayload
            | Corruption::UnknownCompactionVersion => {}
        }
    }

    fn log_journal_path(formation: &TestFormation) -> PathBuf {
        let stream = formation.stream.as_ref().unwrap();
        formation.data_path.join("logs/staged").join(format!(
            "{}-{}.journal",
            stream.task_instance_id, stream.pickup_generation
        ))
    }
}
