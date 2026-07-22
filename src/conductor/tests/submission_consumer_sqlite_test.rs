#![cfg(not(madsim))]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use prost::Message;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use tickr_conductor::proto::{ConductorRelayMessage, EntityType};
use tickr_conductor::relay::{forward_workflow_registration_bytes, init_relay_tx};
use tickr_conductor::submission_consumer::{
    definition_submission_notifications, start_local_definition_submission_worker,
    LocalDefinitionSubmissionWorkerConfig,
};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::definition_repository::{
    DefinitionBuildSettlementOutcome, DefinitionLifecycleStatus, DefinitionRegistrationInput,
    DefinitionRegistrationOutcome, DefinitionSubmissionLeaseRequest,
    DefinitionSubmissionReconciliationOutcome, DefinitionTaskBuildResult,
};
use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::workflow as wf;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static RELAY_SERIAL: Mutex<()> = Mutex::const_new(());

async fn migrated_sqlite() -> (TempDir, String) {
    let directory = TempDir::new().unwrap();
    let url = format!(
        "sqlite://{}",
        directory.path().join("definition-submissions.db").display()
    );
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;
    (directory, url)
}

async fn open_writer(url: &str) -> WriterRepositoryBundle {
    RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: url.to_owned(),
    })
    .open_writer()
    .await
    .unwrap()
}

async fn register_ready(writer: &WriterRepositoryBundle, workflow_id: Uuid) {
    let task_id = Uuid::from_u128(workflow_id.as_u128() + 1_000);
    let outcome = writer
        .register_definition(DefinitionRegistrationInput {
            definition: wf::WorkflowDefinition {
                id: workflow_id.to_string(),
                tenant_id: Uuid::from_u128(999).to_string(),
                namespace: "default".to_owned(),
                slug: format!("submission-{workflow_id}"),
                name: "Durable submission recovery".to_owned(),
                tasks: vec![wf::TaskDefinition {
                    id: task_id.to_string(),
                    workflow_id: workflow_id.to_string(),
                    name: "build".to_owned(),
                    nix_expression_path: "/nix/store/submission-build".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            content_hash: format!("content-{workflow_id}"),
            cosmetic_hash: format!("cosmetic-{workflow_id}"),
            nickel_source: "durable-submission-recovery".to_owned(),
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        DefinitionRegistrationOutcome::Inserted {
            workflow_version: 1,
            ..
        }
    ));
    assert!(matches!(
        writer
            .settle_definition_task_build(
                workflow_id,
                1,
                task_id,
                DefinitionTaskBuildResult::Success,
            )
            .await
            .unwrap(),
        DefinitionBuildSettlementOutcome::Ready(_)
    ));
}

fn worker_config(lease_duration: Duration) -> LocalDefinitionSubmissionWorkerConfig {
    LocalDefinitionSubmissionWorkerConfig {
        scan_interval: Duration::from_millis(20),
        lease_duration,
        batch_size: NonZeroUsize::new(8).unwrap(),
    }
}

async fn receive_workflow(receiver: &mut mpsc::Receiver<ConductorRelayMessage>) -> Uuid {
    let message = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
        .await
        .expect("definition was not forwarded")
        .expect("relay channel closed");
    assert_eq!(message.entity_type, EntityType::SubmitWorkflow as i32);
    let definition = wf::WorkflowDefinition::decode(message.payload.as_slice()).unwrap();
    Uuid::parse_str(&definition.id).unwrap()
}

async fn wait_until_submitted(writer: &WriterRepositoryBundle, workflow_id: Uuid) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if writer
                .definition_submission_reconciliation_outcome(
                    tickr_migrations::definition_repository::DefinitionSubmissionPointer {
                        workflow_id,
                        workflow_version: 1,
                    },
                )
                .await
                .unwrap()
                == DefinitionSubmissionReconciliationOutcome::NotReady(
                    DefinitionLifecycleStatus::Submitted,
                )
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("definition did not settle as Submitted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missed_and_duplicate_notifications_converge_in_stable_order() {
    let _serial = RELAY_SERIAL.lock().await;
    let (_directory, url) = migrated_sqlite().await;
    let writer = Arc::new(open_writer(&url).await);
    let ids = [Uuid::from_u128(3), Uuid::from_u128(1), Uuid::from_u128(2)];
    for workflow_id in ids {
        register_ready(writer.as_ref(), workflow_id).await;
    }

    let (relay_tx, mut relay_rx) = mpsc::channel(16);
    init_relay_tx(relay_tx).await;
    let (first_notifier, first_notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let (second_notifier, second_notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    for _ in 0..4 {
        first_notifier.notify();
        second_notifier.notify();
    }

    let cancel = CancellationToken::new();
    let first_worker = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "submission-worker-a".to_owned(),
        first_notifications,
        worker_config(Duration::from_millis(100)),
        cancel.clone(),
    ));
    let second_worker = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "submission-worker-b".to_owned(),
        second_notifications,
        worker_config(Duration::from_millis(100)),
        cancel.clone(),
    ));

    let mut forwarded = Vec::new();
    for _ in 0..3 {
        forwarded.push(receive_workflow(&mut relay_rx).await);
    }
    assert_eq!(
        forwarded,
        vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]
    );
    for workflow_id in ids {
        wait_until_submitted(writer.as_ref(), workflow_id).await;
    }

    let steady_state_id = Uuid::from_u128(4);
    register_ready(writer.as_ref(), steady_state_id).await;
    assert_eq!(receive_workflow(&mut relay_rx).await, steady_state_id);
    wait_until_submitted(writer.as_ref(), steady_state_id).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(relay_rx.try_recv().is_err());

    cancel.cancel();
    first_worker.await.unwrap().unwrap();
    second_worker.await.unwrap().unwrap();
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_before_forwarding_recovers_after_relay_reconnect() {
    let _serial = RELAY_SERIAL.lock().await;
    let (_directory, url) = migrated_sqlite().await;
    let workflow_id = Uuid::from_u128(10);
    let writer = Arc::new(open_writer(&url).await);
    register_ready(writer.as_ref(), workflow_id).await;

    let (disconnected_tx, disconnected_rx) = mpsc::channel(1);
    drop(disconnected_rx);
    init_relay_tx(disconnected_tx).await;
    let (_notifier, notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "disconnected-worker".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        writer
            .definition_submission_reconciliation_outcome(
                tickr_migrations::definition_repository::DefinitionSubmissionPointer {
                    workflow_id,
                    workflow_version: 1,
                },
            )
            .await
            .unwrap(),
        DefinitionSubmissionReconciliationOutcome::Ready
    );
    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
    drop(writer);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let writer = Arc::new(open_writer(&url).await);
    let (relay_tx, mut relay_rx) = mpsc::channel(4);
    init_relay_tx(relay_tx).await;
    let (_notifier, notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "reconnected-worker".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        cancel.clone(),
    ));
    assert_eq!(receive_workflow(&mut relay_rx).await, workflow_id);
    wait_until_submitted(writer.as_ref(), workflow_id).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_forward_before_settlement_redrives_idempotently() {
    let _serial = RELAY_SERIAL.lock().await;
    let (_directory, url) = migrated_sqlite().await;
    let workflow_id = Uuid::from_u128(20);
    let writer = open_writer(&url).await;
    register_ready(&writer, workflow_id).await;

    let now = Utc::now();
    let mut leases = writer
        .lease_definition_submissions(DefinitionSubmissionLeaseRequest {
            owner: "crashing-worker",
            now,
            expires_at: now + chrono::Duration::milliseconds(80),
            limit: 1,
        })
        .await
        .unwrap();
    let lease = leases.pop().expect("Ready definition was not leased");
    let (relay_tx, mut relay_rx) = mpsc::channel(4);
    init_relay_tx(relay_tx).await;
    forward_workflow_registration_bytes(lease.intent.definition.encode_to_vec())
        .await
        .unwrap();
    assert_eq!(receive_workflow(&mut relay_rx).await, workflow_id);
    assert_eq!(
        writer
            .definition_submission_reconciliation_outcome(
                tickr_migrations::definition_repository::DefinitionSubmissionPointer {
                    workflow_id,
                    workflow_version: 1,
                },
            )
            .await
            .unwrap(),
        DefinitionSubmissionReconciliationOutcome::Ready
    );
    writer.close().await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let writer = Arc::new(open_writer(&url).await);
    let (_notifier, notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "recovery-worker".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        cancel.clone(),
    ));
    assert_eq!(receive_workflow(&mut relay_rx).await, workflow_id);
    wait_until_submitted(writer.as_ref(), workflow_id).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
}
