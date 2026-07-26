//! Notification-free Patch-build reconciliation against ephemeral Postgres.

#![cfg(not(madsim))]

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tickr_conductor::build_pipeline::{BuildExecutor, BuildOutcome, TaskBuildJob};
use tickr_conductor::patch_pipeline::local::{
    patch_work_notifications, start_local_patch_worker, PatchReconcilerConfig,
};
use tickr_conductor::patch_pipeline::PatchRelaySender;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::patch_repository::{
    LeasedPatchBuildSettlementOutcome, PatchBuildLeaseRequest, PatchBuildSettlementOutcome,
    PatchIngressInput, PatchIngressOutcome, PatchProvenance, PatchSourceFormat,
    PatchTaskBuildResult, PatchTaskSpecification,
};
use tickr_proto::patch as pp;
use tickr_proto::workflow as wf;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod common;

async fn start_pg() -> Option<(common::DbGuard, PgPool)> {
    common::test_db().await
}

fn patch_task(task_id: Uuid, nix_expression_path: &str) -> wf::TaskDefinition {
    wf::TaskDefinition {
        id: task_id.to_string(),
        workflow_id: Uuid::nil().to_string(),
        name: format!("patch-{task_id}"),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: nix_expression_path.to_owned(),
        max_attempts: 1,
        ..Default::default()
    }
}

async fn seed_patch(pool: &PgPool, suffix: &str) -> anyhow::Result<(Uuid, Uuid)> {
    let patch_key = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let workflow_instance_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let task = patch_task(task_id, &format!("/patch/{suffix}.nix"));
    let ops = vec![pp::AddressedPatchOp {
        op: Some(pp::addressed_patch_op::Op::AddNode(
            pp::addressed_patch_op::AddNode {
                node_id: task_id.to_string(),
                task: Some(task),
            },
        )),
    }];
    let routing_vars = Vec::new();
    let tasks = vec![PatchTaskSpecification {
        task_id,
        routing_vars: &routing_vars,
    }];
    let repositories = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    let outcome = repositories
        .ingress_patch(PatchIngressInput {
            patch_key,
            patch_id,
            workflow_instance_id,
            ops: &ops,
            operation: None,
            reason: Some("reconciliation test"),
            provenance: PatchProvenance::External,
            source: "{ ops = [] }",
            source_format: PatchSourceFormat::Nickel,
            tasks,
        })
        .await?;
    assert!(matches!(outcome, PatchIngressOutcome::Accepted { .. }));
    Ok((patch_key, task_id))
}

async fn wait_for_patch_status(pool: &PgPool, patch_key: Uuid, expected: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: String =
                sqlx::query_scalar("SELECT status FROM workflow_patches WHERE patch_key = $1")
                    .bind(patch_key)
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if status == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Patch did not reach expected status");
}

fn reconciler_config(batch_size: usize, lease_duration: Duration) -> PatchReconcilerConfig {
    PatchReconcilerConfig {
        scan_interval: Duration::from_millis(20),
        build_lease_duration: lease_duration,
        lifecycle_lease_duration: Duration::from_secs(1),
        lifecycle_min_age: Duration::from_secs(3600),
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
    }
}

#[derive(Default)]
struct RecordingExecutor {
    builds: Mutex<Vec<Uuid>>,
    built: Notify,
}

#[async_trait]
impl BuildExecutor for RecordingExecutor {
    async fn build(&self, job: &TaskBuildJob) -> BuildOutcome {
        self.builds.lock().await.push(job.task_id);
        self.built.notify_waiters();
        BuildOutcome::Success
    }
}

impl RecordingExecutor {
    async fn wait_for(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.builds.lock().await.len() >= expected {
                    break;
                }
                self.built.notified().await;
            }
        })
        .await
        .expect("Patch builds did not complete");
    }
}

#[derive(Default)]
struct CountingSender(AtomicUsize);

#[async_trait]
impl PatchRelaySender for CountingSender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notification_free_scans_drain_bounded_ordered_patch_builds() -> anyhow::Result<()> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let mut patch_tasks = Vec::new();
    for suffix in ["ordered-a", "ordered-b", "ordered-c"] {
        patch_tasks.push(seed_patch(&pool, suffix).await?);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let expected: Vec<Uuid> = sqlx::query_scalar(
        "SELECT task_id FROM workflow_patch_task_builds \
         WHERE status = 'pending' ORDER BY pending_since, patch_key, task_id",
    )
    .fetch_all(&pool)
    .await?;

    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let executor = Arc::new(RecordingExecutor::default());
    let sender = Arc::new(CountingSender::default());
    let (notifier, notifications) = patch_work_notifications(NonZeroUsize::new(1).unwrap());
    drop(notifier);
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_patch_worker(
        repositories,
        executor.clone(),
        sender.clone(),
        "notification-free-order".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_secs(1)),
        cancel.clone(),
    ));

    executor.wait_for(3).await;
    for (patch_key, _) in &patch_tasks {
        wait_for_patch_status(&pool, *patch_key, "Submitted").await;
    }
    cancel.cancel();
    worker.await??;

    assert_eq!(*executor.builds.lock().await, expected);
    assert_eq!(sender.0.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_workers_reclaim_expired_lease_and_stale_settlement_loses() -> anyhow::Result<()>
{
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let (patch_key, task_id) = seed_patch(&pool, "expired-claim").await?;
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let leased_at = Utc::now();
    let stale_lease = repositories
        .lease_patch_build_tasks(PatchBuildLeaseRequest {
            owner: "crashed-claimant",
            now: leased_at,
            expires_at: leased_at + chrono::Duration::milliseconds(80),
            limit: 1,
        })
        .await?
        .pop()
        .expect("Patch build lease");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let executor = Arc::new(RecordingExecutor::default());
    let sender = Arc::new(CountingSender::default());
    let cancel = CancellationToken::new();
    let mut workers = Vec::new();
    for owner in ["competitor-a", "competitor-b"] {
        let (_notifier, notifications) = patch_work_notifications(NonZeroUsize::new(1).unwrap());
        workers.push(tokio::spawn(start_local_patch_worker(
            repositories.clone(),
            executor.clone(),
            sender.clone(),
            owner.to_owned(),
            notifications,
            reconciler_config(1, Duration::from_secs(1)),
            cancel.clone(),
        )));
    }

    executor.wait_for(1).await;
    wait_for_patch_status(&pool, patch_key, "Submitted").await;
    assert_eq!(executor.builds.lock().await.as_slice(), &[task_id]);
    assert_eq!(sender.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        repositories
            .settle_leased_patch_task_build(
                &stale_lease,
                PatchTaskBuildResult::Success,
                Utc::now(),
            )
            .await?,
        LeasedPatchBuildSettlementOutcome::Settled(
            PatchBuildSettlementOutcome::AlreadySettled(
                tickr_migrations::patch_repository::PatchLifecycleStatus::Submitted,
            ),
        )
    );

    cancel.cancel();
    for worker in workers {
        worker.await??;
    }
    Ok(())
}

struct ProcessBoundaryExecutor {
    started: Option<PathBuf>,
    release: Option<PathBuf>,
    finished: Option<PathBuf>,
}

#[async_trait]
impl BuildExecutor for ProcessBoundaryExecutor {
    async fn build(&self, _job: &TaskBuildJob) -> BuildOutcome {
        if let Some(path) = &self.started {
            std::fs::write(path, b"started").unwrap();
        }
        if let Some(path) = &self.release {
            wait_for_path(path).await;
        }
        if let Some(path) = &self.finished {
            std::fs::write(path, b"finished").unwrap();
        }
        BuildOutcome::Success
    }
}

struct ProcessBoundarySender {
    sent: Option<PathBuf>,
}

#[async_trait]
impl PatchRelaySender for ProcessBoundarySender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> anyhow::Result<()> {
        if let Some(path) = &self.sent {
            std::fs::write(path, b"sent")?;
        }
        Ok(())
    }
}

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process boundary marker was not observed");
}

fn spawn_reconciler_process(db_url: &str, paths: &[(&str, &Path)]) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("real_process_patch_reconciliation_helper")
        .arg("--nocapture")
        .env("TICKR_PATCH_HELPER", "1")
        .env("TICKR_PATCH_HELPER_DB_URL", db_url)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (name, path) in paths {
        command.env(name, path);
    }
    command.spawn().unwrap()
}

async fn recover_patch_in_process(db_url: &str, pool: &PgPool, patch_key: Uuid) {
    let mut recovered = spawn_reconciler_process(db_url, &[]);
    assert!(recovered.wait().unwrap().success());
    wait_for_patch_status(pool, patch_key, "Submitted").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by the real-process recovery test"]
async fn real_process_patch_reconciliation_helper() {
    if std::env::var_os("TICKR_PATCH_HELPER").is_none() {
        return;
    }
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var("TICKR_PATCH_HELPER_DB_URL").unwrap())
        .await
        .unwrap();
    if let Some(ready) = std::env::var_os("TICKR_PATCH_HELPER_READY") {
        std::fs::write(ready, b"ready").unwrap();
        wait_for_path(Path::new(
            &std::env::var("TICKR_PATCH_HELPER_START").unwrap(),
        ))
        .await;
    }

    let patch_key: Uuid = sqlx::query_scalar(
        "SELECT patch_key FROM workflow_patches WHERE status = 'Building' ORDER BY patch_key LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let executor = Arc::new(ProcessBoundaryExecutor {
        started: std::env::var_os("TICKR_PATCH_HELPER_STARTED").map(PathBuf::from),
        release: std::env::var_os("TICKR_PATCH_HELPER_RELEASE").map(PathBuf::from),
        finished: std::env::var_os("TICKR_PATCH_HELPER_FINISHED").map(PathBuf::from),
    });
    let sender = Arc::new(ProcessBoundarySender {
        sent: std::env::var_os("TICKR_PATCH_HELPER_SENT").map(PathBuf::from),
    });
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let (notifier, notifications) = patch_work_notifications(NonZeroUsize::new(1).unwrap());
    drop(notifier);
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_patch_worker(
        repositories,
        executor,
        sender,
        "real-process-patch-owner".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_millis(80)),
        cancel.clone(),
    ));
    wait_for_patch_status(&pool, patch_key, "Submitted").await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_crashes_reconcile_patch_without_notifications() -> anyhow::Result<()> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let base = std::env::var("TICKR_TEST_PG_URL")?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let db_url = format!("{}/{database}", base.trim_end_matches('/'));
    let directory = tempfile::TempDir::new()?;

    let (before_claim_key, before_claim_task) = seed_patch(&pool, "before-claim").await?;
    let helper_ready = directory.path().join("before-claim-ready");
    let start_gate = directory.path().join("before-claim-start");
    let mut before_claim = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_PATCH_HELPER_READY", &helper_ready),
            ("TICKR_PATCH_HELPER_START", &start_gate),
        ],
    );
    wait_for_path(&helper_ready).await;
    before_claim.kill()?;
    before_claim.wait()?;
    let lease_owner: Option<String> = sqlx::query_scalar(
        "SELECT lease_owner FROM workflow_patch_task_builds WHERE patch_key = $1 AND task_id = $2",
    )
    .bind(before_claim_key)
    .bind(before_claim_task)
    .fetch_one(&pool)
    .await?;
    assert!(lease_owner.is_none());
    recover_patch_in_process(&db_url, &pool, before_claim_key).await;

    let (during_build_key, _) = seed_patch(&pool, "during-build").await?;
    let build_started = directory.path().join("during-build-started");
    let build_release = directory.path().join("during-build-release");
    let mut during_build = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_PATCH_HELPER_STARTED", &build_started),
            ("TICKR_PATCH_HELPER_RELEASE", &build_release),
        ],
    );
    wait_for_path(&build_started).await;
    during_build.kill()?;
    during_build.wait()?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    recover_patch_in_process(&db_url, &pool, during_build_key).await;

    let (before_settlement_key, before_settlement_task) =
        seed_patch(&pool, "before-settlement").await?;
    let settlement_started = directory.path().join("before-settlement-started");
    let settlement_release = directory.path().join("before-settlement-release");
    let settlement_finished = directory.path().join("before-settlement-finished");
    let mut before_settlement = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_PATCH_HELPER_STARTED", &settlement_started),
            ("TICKR_PATCH_HELPER_RELEASE", &settlement_release),
            ("TICKR_PATCH_HELPER_FINISHED", &settlement_finished),
        ],
    );
    wait_for_path(&settlement_started).await;
    let mut task_lock = pool.begin().await?;
    sqlx::query(
        "SELECT status FROM workflow_patch_task_builds WHERE patch_key = $1 AND task_id = $2 FOR UPDATE",
    )
    .bind(before_settlement_key)
    .bind(before_settlement_task)
    .execute(&mut *task_lock)
    .await?;
    std::fs::write(&settlement_release, b"release")?;
    wait_for_path(&settlement_finished).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    before_settlement.kill()?;
    before_settlement.wait()?;
    task_lock.rollback().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    recover_patch_in_process(&db_url, &pool, before_settlement_key).await;

    let (before_finalization_key, _) = seed_patch(&pool, "before-finalization").await?;
    let finalization_started = directory.path().join("before-finalization-started");
    let finalization_release = directory.path().join("before-finalization-release");
    let finalization_finished = directory.path().join("before-finalization-finished");
    let mut before_finalization = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_PATCH_HELPER_STARTED", &finalization_started),
            ("TICKR_PATCH_HELPER_RELEASE", &finalization_release),
            ("TICKR_PATCH_HELPER_FINISHED", &finalization_finished),
        ],
    );
    wait_for_path(&finalization_started).await;
    let mut parent_lock = pool.begin().await?;
    sqlx::query("SELECT status FROM workflow_patches WHERE patch_key = $1 FOR UPDATE")
        .bind(before_finalization_key)
        .execute(&mut *parent_lock)
        .await?;
    std::fs::write(&finalization_release, b"release")?;
    wait_for_path(&finalization_finished).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    before_finalization.kill()?;
    before_finalization.wait()?;
    parent_lock.rollback().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    recover_patch_in_process(&db_url, &pool, before_finalization_key).await;

    let settled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_patches WHERE status = 'Submitted' AND patch_key = ANY($1)",
    )
    .bind(vec![
        before_claim_key,
        during_build_key,
        before_settlement_key,
        before_finalization_key,
    ])
    .fetch_one(&pool)
    .await?;
    assert_eq!(settled, 4);
    Ok(())
}
