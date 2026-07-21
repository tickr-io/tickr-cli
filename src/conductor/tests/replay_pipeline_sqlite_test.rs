#![cfg(not(madsim))]

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use tickr_conductor::replay_pipeline::{
    process_replay, reconcile_orphan_replay_rows, redrive_unsettled, replay_instance_id,
    ReplayError, ReplayIngress, ReplayRelaySender, ReplayRequest, STATUS_MATERIALIZING,
    STATUS_RELEASED, STATUS_VERSION_UNRESOLVABLE,
};
use tickr_conductor::replay_rehydration::RehydrationPlan;
use tickr_migrations::backend::{RepositoryErrorKind, RepositoryFactory};
use tickr_migrations::encoding::{encode_json, encode_timestamp, encode_uuid};
use tickr_migrations::replay_repository::{
    ReplayLifecycleStatus, ReplayRedriveCandidate, ReplaySettlementOutcome,
};
use tickr_proto::archive as ap;
use tickr_proto::config::DataPlaneSql;
use tickr_proto::runnable as rp;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;
use tokio::sync::Mutex;
use uuid::Uuid;

struct FailingSender;

#[async_trait::async_trait]
impl ReplayRelaySender for FailingSender {
    async fn send(&self, _signal: &sp::Signal) -> Result<()> {
        anyhow::bail!("relay unavailable")
    }

    async fn rehydrate(&self, _replay_run_id: Uuid, _plan: &RehydrationPlan) -> Result<()> {
        anyhow::bail!("ctx unavailable")
    }
}

struct HydrationFailingSender(Mutex<Vec<sp::Signal>>);

impl HydrationFailingSender {
    fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }
}

#[async_trait::async_trait]
impl ReplayRelaySender for HydrationFailingSender {
    async fn send(&self, signal: &sp::Signal) -> Result<()> {
        self.0.lock().await.push(signal.clone());
        Ok(())
    }

    async fn rehydrate(&self, _replay_run_id: Uuid, _plan: &RehydrationPlan) -> Result<()> {
        anyhow::bail!("sentinel write failed")
    }
}

#[derive(Default)]
struct CountingSender(Mutex<usize>);

#[async_trait::async_trait]
impl ReplayRelaySender for CountingSender {
    async fn send(&self, _signal: &sp::Signal) -> Result<()> {
        *self.0.lock().await += 1;
        Ok(())
    }

    async fn rehydrate(&self, _replay_run_id: Uuid, _plan: &RehydrationPlan) -> Result<()> {
        *self.0.lock().await += 1;
        Ok(())
    }
}

struct Fixture {
    source_id: Uuid,
    workflow_id: Uuid,
    start: Uuid,
    failed: Uuid,
    projection: ap::ArchiveProjection,
    definition: wf::WorkflowDefinition,
}

fn fixture() -> Fixture {
    let source_id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    let start = Uuid::new_v4();
    let succeeded = Uuid::new_v4();
    let failed = Uuid::new_v4();
    let end = Uuid::new_v4();
    let edge = |source: Uuid, target: Uuid| rp::RunnableEdge {
        id: Uuid::new_v4().to_string(),
        sources: vec![source.to_string()],
        targets: vec![target.to_string()],
        kind: wf::EdgeKind::Control as i32,
        gates: Vec::new(),
    };
    let task = |id: Uuid, name: &str| wf::TaskDefinition {
        id: id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: name.to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: "fixture".to_string(),
        ..Default::default()
    };
    let tasks = vec![task(succeeded, "succeeded"), task(failed, "failed")];
    let graph = rp::RunnableGraph {
        nodes: vec![
            rp::RunnableNode {
                id: start.to_string(),
                node_type: wf::NodeType::Start as i32,
                ground: rp::GroundState::Success as i32,
                grounded_at: None,
            },
            rp::RunnableNode {
                id: succeeded.to_string(),
                node_type: wf::NodeType::Task as i32,
                ground: rp::GroundState::Success as i32,
                grounded_at: Some(chrono::Utc::now().to_rfc3339()),
            },
            rp::RunnableNode {
                id: failed.to_string(),
                node_type: wf::NodeType::Task as i32,
                ground: rp::GroundState::Failed as i32,
                grounded_at: Some(chrono::Utc::now().to_rfc3339()),
            },
            rp::RunnableNode {
                id: end.to_string(),
                node_type: wf::NodeType::End as i32,
                ground: rp::GroundState::Pending as i32,
                grounded_at: None,
            },
        ],
        edges: vec![
            edge(start, succeeded),
            edge(succeeded, failed),
            edge(failed, end),
        ],
        start: start.to_string(),
        end: end.to_string(),
        head: Vec::new(),
        tail: String::new(),
    };
    let projection = ap::ArchiveProjection {
        runnable: Some(rp::RunnableProjection {
            tasks: tasks.clone(),
            graph: Some(graph),
            workflow_version: 1,
        }),
        id: source_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "sqlite replay source".to_string(),
        workflow_name: "sqlite replay law".to_string(),
        workflow_version: 1,
        state: "Failed".to_string(),
        ..Default::default()
    };
    let definition = wf::WorkflowDefinition {
        id: workflow_id.to_string(),
        name: "sqlite replay law".to_string(),
        version: 1,
        tasks,
        ..Default::default()
    };
    Fixture {
        source_id,
        workflow_id,
        start,
        failed,
        projection,
        definition,
    }
}

async fn prepare() -> (TempDir, String, Fixture) {
    let directory = TempDir::new().unwrap();
    let url = format!("sqlite://{}", directory.path().join("replays.db").display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    let fixture = fixture();
    let archive = encode_json(&serde_json::to_value(&fixture.projection).unwrap());
    sqlx::query(
        "INSERT INTO workflow_instances (id, workflow_id, name, state, instance) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(encode_uuid(fixture.source_id))
    .bind(encode_uuid(fixture.workflow_id))
    .bind(&fixture.projection.name)
    .bind(&fixture.projection.state)
    .bind(archive)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workflow_run_info (workflow_instance_id, ctx_envelope) VALUES (?1, '[]')",
    )
    .bind(encode_uuid(fixture.source_id))
    .execute(&pool)
    .await
    .unwrap();
    let definition =
        tickr_proto::codec::definition::definition_proto_to_json(&fixture.definition).unwrap();
    sqlx::query(
        "INSERT INTO workflows \
         (id, name, definition, version, status, nickel_source, namespace, slug, content_hash, cosmetic_hash) \
         VALUES (?1, ?2, ?3, 1, 'Submitted', '', 'default', 'sqlite-replay-law', 'content', 'cosmetic')",
    )
    .bind(encode_uuid(fixture.workflow_id))
    .bind(&fixture.definition.name)
    .bind(encode_json(&definition))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    (directory, url, fixture)
}

fn request(source_instance_id: Uuid, key: Option<&str>) -> ReplayRequest {
    ReplayRequest {
        source_instance_id,
        resume_from: None,
        name: None,
        idempotency_key: key.map(str::to_owned),
        inputs: HashMap::new(),
    }
}

fn accepted_id(outcome: ReplayIngress) -> Uuid {
    match outcome {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected accepted replay, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_backed_sqlite_replay_ingress_and_status_laws_survive_reopen() {
    let (_directory, url, fixture) = prepare().await;
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() });
    let writer = factory.open_writer().await.unwrap();
    let reader = factory.open_read_only().await.unwrap();

    let source = writer
        .replay_source(fixture.source_id)
        .await
        .unwrap()
        .expect("terminal archive composition");
    assert_eq!(source.workflow_id, fixture.workflow_id);
    assert_eq!(source.projection.workflow_version, 1);
    assert_eq!(source.pinned_definition.unwrap().version, 1);

    let invalid = ReplayRequest {
        resume_from: Some(vec![fixture.start]),
        ..request(fixture.source_id, Some("invalid"))
    };
    assert!(matches!(
        process_replay(&writer, &FailingSender, invalid).await,
        Err(ReplayError::RootUnfireable { root }) if root == fixture.start
    ));
    assert!(reader
        .replays_for_source(fixture.source_id)
        .await
        .unwrap()
        .is_empty());

    let missing_source = Uuid::new_v4();
    let parked_id = match process_replay(
        &writer,
        &CountingSender::default(),
        request(missing_source, Some("missing")),
    )
    .await
    .unwrap()
    {
        ReplayIngress::VersionUnresolvable { replay_instance_id } => replay_instance_id,
        other => panic!("expected typed terminal park, got {other:?}"),
    };
    assert_eq!(
        writer
            .replay_lifecycle(parked_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_VERSION_UNRESOLVABLE
    );

    let keyed = accepted_id(
        process_replay(
            &writer,
            &FailingSender,
            request(fixture.source_id, Some("same-source-key")),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        writer
            .replay_lifecycle(keyed)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_MATERIALIZING,
        "the lifecycle commit precedes the failed relay effect"
    );
    let collision_sender = CountingSender::default();
    assert!(matches!(
        process_replay(
            &writer,
            &collision_sender,
            request(fixture.source_id, Some("same-source-key")),
        )
        .await
        .unwrap(),
        ReplayIngress::Deduplicated { replay_instance_id } if replay_instance_id == keyed
    ));
    assert_eq!(*collision_sender.0.lock().await, 0);

    tokio::time::sleep(Duration::from_millis(2)).await;
    let fresh_one = accepted_id(
        process_replay(&writer, &FailingSender, request(fixture.source_id, None))
            .await
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(2)).await;
    let fresh_two = accepted_id(
        process_replay(&writer, &FailingSender, request(fixture.source_id, None))
            .await
            .unwrap(),
    );
    assert_ne!(fresh_one, fresh_two, "an omitted key always mints a replay");

    let raw = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_replays SET created_at = ?2 WHERE source_instance_id = ?1")
        .bind(encode_uuid(fixture.source_id))
        .bind(encode_timestamp(Utc::now()))
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    let mut expected_order = vec![keyed, fresh_one, fresh_two];
    expected_order.sort_unstable_by(|left, right| right.cmp(left));

    let statuses = reader.replays_for_source(fixture.source_id).await.unwrap();
    assert_eq!(
        statuses
            .iter()
            .map(|row| row.replay_instance_id)
            .collect::<Vec<_>>(),
        expected_order,
        "equal-time reverse links use replay identity descending"
    );

    reader.close().await;
    writer.close().await;
    let reopened_writer = factory.open_writer().await.unwrap();
    let reopened_reader = factory.open_read_only().await.unwrap();
    assert!(reopened_writer
        .replay_source(fixture.source_id)
        .await
        .unwrap()
        .is_some());
    let reopened = reopened_reader
        .replays_for_source(fixture.source_id)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .iter()
            .map(|row| row.replay_instance_id)
            .collect::<Vec<_>>(),
        expected_order
    );
    reopened_reader.close().await;
    reopened_writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_backed_recovery_scan_is_stable_and_isolates_corrupt_rows_across_restarts() {
    let (_directory, url, fixture) = prepare().await;
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() });
    let writer = factory.open_writer().await.unwrap();

    let mut interrupted = Vec::new();
    for key in ["healthy-a", "corrupt", "healthy-b"] {
        interrupted.push(accepted_id(
            process_replay(
                &writer,
                &FailingSender,
                request(fixture.source_id, Some(key)),
            )
            .await
            .unwrap(),
        ));
    }
    let terminal = accepted_id(
        process_replay(
            &writer,
            &CountingSender::default(),
            request(fixture.source_id, Some("already-terminal")),
        )
        .await
        .unwrap(),
    );

    let raw = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    let equal_age = encode_timestamp(Utc::now() - chrono::Duration::seconds(1));
    sqlx::query("UPDATE workflow_replays SET updated_at = ?2 WHERE source_instance_id = ?1")
        .bind(encode_uuid(fixture.source_id))
        .bind(equal_age)
        .execute(&raw)
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_replays SET resume_from = '{}' WHERE replay_instance_id = ?1")
        .bind(encode_uuid(interrupted[1]))
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;

    let selected = writer.unsettled_replays_before(Utc::now()).await.unwrap();
    assert_eq!(selected.len(), 3, "terminal rows are excluded by the scan");
    let mut selected_ids = Vec::new();
    for candidate in selected {
        match candidate {
            ReplayRedriveCandidate::Ready(row) => selected_ids.push(row.replay_instance_id),
            ReplayRedriveCandidate::Corrupt { identity, error } => {
                assert_eq!(error.kind(), RepositoryErrorKind::CorruptStoredValue);
                selected_ids.push(Uuid::parse_str(&identity).unwrap());
            }
        }
    }
    let mut expected = interrupted.clone();
    expected.sort_unstable();
    assert_eq!(
        selected_ids, expected,
        "equal-age recovery candidates use the replay identity tie-break"
    );

    writer.close().await;
    let reopened = factory.open_writer().await.unwrap();
    let sender = CountingSender::default();
    assert_eq!(
        redrive_unsettled(&reopened, &sender, Duration::ZERO)
            .await
            .unwrap(),
        2,
        "the corrupt row does not block healthy rows after reopen"
    );
    assert_eq!(*sender.0.lock().await, 6);
    for replay_id in [interrupted[0], interrupted[2], terminal] {
        assert_eq!(
            reopened
                .replay_lifecycle(replay_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            STATUS_RELEASED
        );
    }
    assert_eq!(
        reopened
            .replay_lifecycle(interrupted[1])
            .await
            .unwrap_err()
            .kind(),
        RepositoryErrorKind::CorruptStoredValue
    );
    reopened.close().await;

    let reopened_again = factory.open_writer().await.unwrap();
    assert_eq!(
        redrive_unsettled(&reopened_again, &CountingSender::default(), Duration::ZERO)
            .await
            .unwrap(),
        0,
        "a second restart cannot regress or re-release terminal rows"
    );
    reopened_again.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_backed_concurrent_steady_state_drives_converge_on_one_terminal_replay() {
    let (_directory, url, fixture) = prepare().await;
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url });
    let writer = factory.open_writer().await.unwrap();
    let replay_id = accepted_id(
        process_replay(
            &writer,
            &FailingSender,
            request(fixture.source_id, Some("concurrent-drive")),
        )
        .await
        .unwrap(),
    );
    let sender = CountingSender::default();

    let (left, right) = tokio::join!(
        redrive_unsettled(&writer, &sender, Duration::ZERO),
        redrive_unsettled(&writer, &sender, Duration::ZERO)
    );
    assert!(left.unwrap() + right.unwrap() >= 1);
    assert_eq!(
        writer
            .replay_lifecycle(replay_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_RELEASED
    );
    assert_eq!(
        writer.settle_replay_released(replay_id).await.unwrap(),
        ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::Released)
    );
    assert_eq!(
        redrive_unsettled(&writer, &sender, Duration::ZERO)
            .await
            .unwrap(),
        0,
        "the terminal replay is not selected again"
    );
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_backed_sqlite_drive_and_settlement_laws_survive_interruption() {
    let (_directory, url, fixture) = prepare().await;
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() });
    let writer = factory.open_writer().await.unwrap();
    let failing = HydrationFailingSender::new();
    let outcome = process_replay(
        &writer,
        &failing,
        ReplayRequest {
            resume_from: Some(vec![fixture.failed]),
            ..request(fixture.source_id, Some("durable-drive"))
        },
    )
    .await
    .unwrap();
    let replay_id = accepted_id(outcome);
    let row = writer.replay_lifecycle(replay_id).await.unwrap().unwrap();
    assert_eq!(row.status, STATUS_MATERIALIZING);
    assert_eq!(row.resume_from, vec![fixture.failed]);
    assert!(!row.pre_grounded.is_empty());
    assert_eq!(
        replay_id,
        replay_instance_id(fixture.source_id, row.signal_id)
    );
    assert_eq!(
        failing.0.lock().await.len(),
        1,
        "hydration failure must not send the release Resume"
    );
    let witness = row.seed_sha256.expect("seed witness");

    let raw = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_replays SET seed_sha256 = 'mismatched' WHERE replay_instance_id = ?1",
    )
    .bind(encode_uuid(replay_id))
    .execute(&raw)
    .await
    .unwrap();
    raw.close().await;
    let blocked = CountingSender::default();
    assert_eq!(
        reconcile_orphan_replay_rows(&writer, &blocked)
            .await
            .unwrap(),
        0
    );
    assert_eq!(*blocked.0.lock().await, 0, "witness mismatch blocks relay");
    assert_eq!(
        writer
            .replay_lifecycle(replay_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_MATERIALIZING
    );

    let raw = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_replays SET seed_sha256 = ?2 WHERE replay_instance_id = ?1")
        .bind(encode_uuid(replay_id))
        .bind(witness)
        .execute(&raw)
        .await
        .unwrap();
    raw.close().await;
    writer.close().await;

    let reopened = factory.open_writer().await.unwrap();
    let recovered = CountingSender::default();
    assert_eq!(
        reconcile_orphan_replay_rows(&reopened, &recovered)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        *recovered.0.lock().await,
        3,
        "recovery relays Trigger, hydrates the sentinel, then releases"
    );
    assert_eq!(
        reopened
            .replay_lifecycle(replay_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_RELEASED
    );
    assert_eq!(
        reopened.settle_replay_released(replay_id).await.unwrap(),
        ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::Released)
    );
    assert_eq!(
        reopened
            .settle_replay_released(Uuid::new_v4())
            .await
            .unwrap(),
        ReplaySettlementOutcome::Absent
    );

    let racing_id = accepted_id(
        process_replay(
            &reopened,
            &FailingSender,
            request(fixture.source_id, Some("settlement-race")),
        )
        .await
        .unwrap(),
    );
    let (left, right) = tokio::join!(
        reopened.settle_replay_released(racing_id),
        reopened.settle_replay_released(racing_id)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ReplaySettlementOutcome::Released))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::Released)
            ))
            .count(),
        1
    );

    let parked_id = match process_replay(
        &reopened,
        &CountingSender::default(),
        request(Uuid::new_v4(), Some("late-park")),
    )
    .await
    .unwrap()
    {
        ReplayIngress::VersionUnresolvable { replay_instance_id } => replay_instance_id,
        other => panic!("expected VersionUnresolvable, got {other:?}"),
    };
    assert_eq!(
        reopened.settle_replay_released(parked_id).await.unwrap(),
        ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::VersionUnresolvable)
    );
    assert_eq!(
        reopened
            .replay_lifecycle(parked_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        STATUS_VERSION_UNRESOLVABLE
    );
    reopened.close().await;
}
