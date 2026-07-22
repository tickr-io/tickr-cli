#![cfg(not(madsim))]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use tickr_conductor::build_pipeline::{
    definition_build_notifications, start_local_definition_build_worker, BuildExecutor,
    BuildOutcome, LocalDefinitionBuildWorkerConfig, TaskBuildJob,
};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::definition_repository::{
    DefinitionBuildLeaseRequest, DefinitionBuildSettlementOutcome, DefinitionRegistrationInput,
    DefinitionRegistrationOutcome, DefinitionSubmissionCandidate, DefinitionTaskBuildResult,
    LeasedDefinitionBuildSettlementOutcome,
};
use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::workflow as wf;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Default)]
struct SuccessExecutor {
    builds: AtomicUsize,
    built: Notify,
}

#[async_trait]
impl BuildExecutor for SuccessExecutor {
    async fn build(&self, _job: &TaskBuildJob) -> BuildOutcome {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.built.notify_waiters();
        BuildOutcome::Success
    }
}

impl SuccessExecutor {
    async fn wait_for(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self.builds.load(Ordering::SeqCst) >= expected {
                    break;
                }
                self.built.notified().await;
            }
        })
        .await
        .expect("definition builds did not complete");
    }
}

#[derive(Default)]
struct BlockingExecutor {
    builds: AtomicUsize,
    started: Notify,
    release: Notify,
}

#[async_trait]
impl BuildExecutor for BlockingExecutor {
    async fn build(&self, _job: &TaskBuildJob) -> BuildOutcome {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        self.release.notified().await;
        BuildOutcome::Success
    }
}

async fn migrated_sqlite() -> (TempDir, String) {
    let directory = TempDir::new().unwrap();
    let url = format!(
        "sqlite://{}",
        directory.path().join("definition-builds.db").display()
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

async fn register_definition(
    writer: &WriterRepositoryBundle,
    workflow_id: Uuid,
    task_ids: &[Uuid],
) {
    let tasks = task_ids
        .iter()
        .enumerate()
        .map(|(index, task_id)| wf::TaskDefinition {
            id: task_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: format!("build-{index}"),
            nix_expression_path: format!("/nix/store/build-{task_id}"),
            ..Default::default()
        })
        .collect();
    let outcome = writer
        .register_definition(DefinitionRegistrationInput {
            definition: wf::WorkflowDefinition {
                id: workflow_id.to_string(),
                tenant_id: Uuid::from_u128(999).to_string(),
                namespace: "default".to_owned(),
                slug: format!("build-{workflow_id}"),
                name: "Durable build recovery".to_owned(),
                tasks,
                ..Default::default()
            },
            content_hash: format!("content-{workflow_id}"),
            cosmetic_hash: format!("cosmetic-{workflow_id}"),
            nickel_source: "durable-build-recovery".to_owned(),
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
}

fn worker_config(lease_duration: Duration) -> LocalDefinitionBuildWorkerConfig {
    LocalDefinitionBuildWorkerConfig {
        scan_interval: Duration::from_millis(20),
        lease_duration,
        batch_size: NonZeroUsize::new(8).unwrap(),
    }
}

async fn wait_until_ready(writer: &WriterRepositoryBundle, workflow_id: Uuid) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                writer
                    .definition_submission_candidate(workflow_id, 1)
                    .await
                    .unwrap(),
                DefinitionSubmissionCandidate::Ready(_)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("definition did not reach Ready");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_and_bounded_scans_recover_lost_notification_hints() {
    let (_directory, url) = migrated_sqlite().await;
    let first = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    let third = Uuid::from_u128(3);
    let writer = open_writer(&url).await;
    register_definition(&writer, first, &[Uuid::from_u128(101)]).await;
    register_definition(&writer, second, &[Uuid::from_u128(102)]).await;
    register_definition(&writer, third, &[Uuid::from_u128(103)]).await;
    writer.close().await;

    let (notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    notifier.notify();
    notifier.notify();
    let (closed_notifier, closed_notifications) =
        definition_build_notifications(NonZeroUsize::new(1).unwrap());
    drop(closed_notifications);
    closed_notifier.notify();

    let writer = Arc::new(open_writer(&url).await);
    let executor = Arc::new(SuccessExecutor::default());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        writer.clone(),
        executor.clone(),
        "restart-worker".to_owned(),
        notifications,
        worker_config(Duration::from_secs(1)),
        cancel.clone(),
    ));
    drop(notifier);

    executor.wait_for(3).await;
    wait_until_ready(writer.as_ref(), first).await;
    wait_until_ready(writer.as_ref(), second).await;
    wait_until_ready(writer.as_ref(), third).await;

    let fourth = Uuid::from_u128(4);
    register_definition(writer.as_ref(), fourth, &[Uuid::from_u128(104)]).await;
    executor.wait_for(4).await;
    wait_until_ready(writer.as_ref(), fourth).await;

    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_worker_death_before_settlement_and_not_after_settlement() {
    let (_directory, url) = migrated_sqlite().await;
    let workflow_id = Uuid::from_u128(10);
    let writer = Arc::new(open_writer(&url).await);
    register_definition(writer.as_ref(), workflow_id, &[Uuid::from_u128(110)]).await;

    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let blocking = Arc::new(BlockingExecutor::default());
    let worker = tokio::spawn(start_local_definition_build_worker(
        writer.clone(),
        blocking.clone(),
        "worker-before-settlement".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        CancellationToken::new(),
    ));
    tokio::time::timeout(Duration::from_secs(3), blocking.started.notified())
        .await
        .expect("blocking build did not start");
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    writer.close().await;
    drop(writer);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let writer = Arc::new(open_writer(&url).await);
    let recovered = Arc::new(SuccessExecutor::default());
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        writer.clone(),
        recovered.clone(),
        "worker-after-restart".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        cancel.clone(),
    ));
    recovered.wait_for(1).await;
    wait_until_ready(writer.as_ref(), workflow_id).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
    drop(writer);

    let writer = Arc::new(open_writer(&url).await);
    let after_settlement = Arc::new(SuccessExecutor::default());
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        writer.clone(),
        after_settlement.clone(),
        "settled-restart".to_owned(),
        notifications,
        worker_config(Duration::from_millis(80)),
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(after_settlement.builds.load(Ordering::SeqCst), 0);
    wait_until_ready(writer.as_ref(), workflow_id).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
    writer.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_leases_preserve_exactly_one_competing_parent_finalizer() {
    let (_directory, url) = migrated_sqlite().await;
    let workflow_id = Uuid::from_u128(20);
    let writer = Arc::new(open_writer(&url).await);
    register_definition(
        writer.as_ref(),
        workflow_id,
        &[Uuid::from_u128(220), Uuid::from_u128(210)],
    )
    .await;

    let now = Utc::now();
    let leases = writer
        .lease_definition_build_tasks(DefinitionBuildLeaseRequest {
            owner: "competing-finalizers",
            now,
            expires_at: now + chrono::Duration::seconds(1),
            limit: 8,
        })
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    let keys = leases
        .iter()
        .map(|lease| {
            (
                lease.task.workflow_id,
                lease.task.workflow_version,
                lease.task.task_id,
            )
        })
        .collect::<Vec<_>>();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);

    let results = futures::future::join_all(leases.into_iter().map(|lease| {
        let writer = writer.clone();
        async move {
            writer
                .settle_leased_definition_task_build(
                    &lease,
                    DefinitionTaskBuildResult::Success,
                    Utc::now(),
                )
                .await
                .unwrap()
        }
    }))
    .await;
    assert_eq!(
        results
            .iter()
            .filter(|outcome| matches!(
                outcome,
                LeasedDefinitionBuildSettlementOutcome::Settled(
                    DefinitionBuildSettlementOutcome::Ready(_)
                )
            ))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|outcome| matches!(
                outcome,
                LeasedDefinitionBuildSettlementOutcome::Settled(
                    DefinitionBuildSettlementOutcome::AwaitingTasks
                )
            ))
            .count(),
        1
    );

    writer.close().await;
    drop(writer);
    let reopened = open_writer(&url).await;
    wait_until_ready(&reopened, workflow_id).await;
    reopened.close().await;
}
