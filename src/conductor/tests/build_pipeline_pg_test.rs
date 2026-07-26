//! Integration tests for the per-task build pipeline against ephemeral PG.
//!
//! Covers:
//! - Happy path: pre-insert workflow + N per-task `pending` rows; flip
//!   each per-task row to `success`; assert finalizer flips workflow
//!   to `Ready` exactly once on the last task.
//! - `BuildFailed` short-circuit: a single per-task failure transitions
//!   the workflow row to `BuildFailed` (terminal).
//! - Concurrent-finalizer atomicity (non-negotiable): N workers complete
//!   the last `pending` rows simultaneously against the same
//!   `(workflow_id, version)`; the test asserts exactly one `Ready`
//!   transition fires.

#![cfg(not(madsim))]

use async_trait::async_trait;
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tickr_conductor::build_pipeline::{
    definition_build_notifications, start_local_definition_build_worker, BuildExecutor,
    BuildOutcome, LocalDefinitionBuildWorkerConfig, TaskBuildJob,
};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionBuildLeaseRequest, DefinitionBuildSettlementOutcome,
    DefinitionSubmissionLeaseRequest, DefinitionSubmissionSettlementOutcome,
    DefinitionTaskBuildResult, LeasedDefinitionBuildSettlementOutcome,
    LeasedDefinitionSubmissionSettlementOutcome,
};
use tickr_proto::workflow as wf;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod common;

async fn start_pg() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

/// A minimal workflow fixture: the identity fields the seed/finalizer SQL binds,
/// plus the proto-JSON `definition` blob the `workflows.definition` column
/// stores (the shape registration persists).
struct WorkflowFixture {
    id: Uuid,
    version: i64,
    name: String,
    definition: serde_json::Value,
}

impl WorkflowFixture {
    fn get_id(&self) -> Uuid {
        self.id
    }
    fn get_version(&self) -> i64 {
        self.version
    }
    fn get_name(&self) -> &str {
        &self.name
    }
}

/// Build a workflow definition with N tasks under the same name-hashed id the
/// parser would produce, returning the fixture plus the per-task ids the build
/// rows key on. Task ids are minted fresh — the finalizer keys builds on them
/// and never re-parses the stored definition.
fn workflow_with_n_tasks(name: &str, version: i64, n: usize) -> (WorkflowFixture, Vec<Uuid>) {
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes());
    let mut task_ids = Vec::with_capacity(n);
    let tasks = (0..n)
        .map(|i| {
            let task_id = Uuid::new_v4();
            task_ids.push(task_id);
            wf::TaskDefinition {
                id: task_id.to_string(),
                workflow_id: id.to_string(),
                name: format!("task-{}", i),
                task_type: wf::TaskType::Regular as i32,
                nix_expression_path: "/path/to/nix/expression".to_string(),
                nix_args: vec!["hello".to_string()],
                ..Default::default()
            }
        })
        .collect();
    let def = wf::WorkflowDefinition {
        id: id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: name.to_string(),
        version,
        tasks,
        ..Default::default()
    };
    let definition = serde_json::to_value(&def).expect("serialize proto workflow definition");
    (
        WorkflowFixture {
            id,
            version,
            name: name.to_string(),
            definition,
        },
        task_ids,
    )
}

async fn seed_workflow_and_task_rows(
    pool: &PgPool,
    wf: &WorkflowFixture,
    task_ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
        VALUES ($1, $2, 'default', 'wf', $3, 'Building', 'testhash', 'testcos', $4, '')
        "#,
    )
    .bind(wf.get_id())
    .bind(wf.get_version())
    .bind(wf.get_name())
    .bind(&wf.definition)
    .execute(pool)
    .await?;
    for task_id in task_ids {
        sqlx::query(
            r#"
            INSERT INTO workflow_task_builds (workflow_id, workflow_version, task_id, status)
            VALUES ($1, $2, $3, 'pending')
            "#,
        )
        .bind(wf.get_id())
        .bind(wf.get_version())
        .bind(task_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

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
    started: Notify,
    release: Notify,
}

#[async_trait]
impl BuildExecutor for BlockingExecutor {
    async fn build(&self, _job: &TaskBuildJob) -> BuildOutcome {
        self.started.notify_waiters();
        self.release.notified().await;
        BuildOutcome::Success
    }
}

fn reconciler_config(
    batch_size: usize,
    lease_duration: Duration,
) -> LocalDefinitionBuildWorkerConfig {
    LocalDefinitionBuildWorkerConfig {
        scan_interval: Duration::from_millis(20),
        lease_duration,
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
    }
}

async fn wait_for_status(pool: &PgPool, workflow_id: Uuid, expected: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status: String =
                sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = 1")
                    .bind(workflow_id)
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
    .expect("definition did not reach expected status");
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

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
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
        .arg("real_process_definition_build_helper")
        .arg("--nocapture")
        .env("TICKR_BUILD_HELPER", "1")
        .env("TICKR_BUILD_HELPER_DB_URL", db_url)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (name, path) in paths {
        command.env(name, path);
    }
    command.spawn().unwrap()
}

async fn wait_for_ready_in_helper(pool: &PgPool, workflow_id: Uuid) {
    wait_for_status(pool, workflow_id, "Ready").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by the real-process recovery test"]
async fn real_process_definition_build_helper() {
    if std::env::var_os("TICKR_BUILD_HELPER").is_none() {
        return;
    }
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var("TICKR_BUILD_HELPER_DB_URL").unwrap())
        .await
        .unwrap();
    if let Some(ready) = std::env::var_os("TICKR_BUILD_HELPER_READY") {
        std::fs::write(ready, b"ready").unwrap();
        wait_for_path(Path::new(
            &std::env::var("TICKR_BUILD_HELPER_START").unwrap(),
        ))
        .await;
    }

    let workflow_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM workflows WHERE status = 'Building' ORDER BY id LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let executor = Arc::new(ProcessBoundaryExecutor {
        started: std::env::var_os("TICKR_BUILD_HELPER_STARTED").map(PathBuf::from),
        release: std::env::var_os("TICKR_BUILD_HELPER_RELEASE").map(PathBuf::from),
        finished: std::env::var_os("TICKR_BUILD_HELPER_FINISHED").map(PathBuf::from),
    });
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        repositories,
        executor,
        "real-process-owner".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_millis(80)),
        cancel.clone(),
    ));
    wait_for_ready_in_helper(&pool, workflow_id).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_crashes_reconcile_without_notifications(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let (workflow, tasks) = workflow_with_n_tasks("real-process-crashes", 1, 1);
    seed_workflow_and_task_rows(&pool, &workflow, &tasks).await?;
    let base = std::env::var("TICKR_TEST_PG_URL")?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let db_url = format!("{}/{database}", base.trim_end_matches('/'));
    let directory = tempfile::TempDir::new()?;

    let helper_ready = directory.path().join("helper-ready");
    let start_gate = directory.path().join("start-gate");
    let mut before_claim = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_BUILD_HELPER_READY", &helper_ready),
            ("TICKR_BUILD_HELPER_START", &start_gate),
        ],
    );
    wait_for_path(&helper_ready).await;
    before_claim.kill()?;
    before_claim.wait()?;
    let lease_owner: Option<String> = sqlx::query_scalar(
        "SELECT lease_owner FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 AND task_id = $2",
    )
    .bind(workflow.id)
    .bind(tasks[0])
    .fetch_one(&pool)
    .await?;
    assert!(
        lease_owner.is_none(),
        "death before claim must leave no lease"
    );

    let build_started = directory.path().join("build-started");
    let build_release = directory.path().join("build-release");
    let mut during_build = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_BUILD_HELPER_STARTED", &build_started),
            ("TICKR_BUILD_HELPER_RELEASE", &build_release),
        ],
    );
    wait_for_path(&build_started).await;
    during_build.kill()?;
    during_build.wait()?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let settlement_started = directory.path().join("settlement-build-started");
    let settlement_release = directory.path().join("settlement-release");
    let build_finished = directory.path().join("build-finished");
    let mut before_settlement = spawn_reconciler_process(
        &db_url,
        &[
            ("TICKR_BUILD_HELPER_STARTED", &settlement_started),
            ("TICKR_BUILD_HELPER_RELEASE", &settlement_release),
            ("TICKR_BUILD_HELPER_FINISHED", &build_finished),
        ],
    );
    wait_for_path(&settlement_started).await;
    let mut parent_lock = pool.begin().await?;
    sqlx::query("SELECT status FROM workflows WHERE id = $1 AND version = 1 FOR UPDATE")
        .bind(workflow.id)
        .execute(&mut *parent_lock)
        .await?;
    std::fs::write(&settlement_release, b"release")?;
    wait_for_path(&build_finished).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    before_settlement.kill()?;
    before_settlement.wait()?;
    parent_lock.rollback().await?;

    let status: String = sqlx::query_scalar(
        "SELECT status FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 AND task_id = $2",
    )
    .bind(workflow.id)
    .bind(tasks[0])
    .fetch_one(&pool)
    .await?;
    assert_eq!(status, "pending");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut recovered = spawn_reconciler_process(&db_url, &[]);
    assert!(recovered.wait()?.success());
    wait_for_status(&pool, workflow.id, "Ready").await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notification_free_reconciler_drains_bounded_batches(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let mut workflow_ids = Vec::new();
    for index in 0..3 {
        let (workflow, tasks) = workflow_with_n_tasks(&format!("notification-free-{index}"), 1, 1);
        seed_workflow_and_task_rows(&pool, &workflow, &tasks).await?;
        workflow_ids.push(workflow.id);
    }

    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let executor = Arc::new(SuccessExecutor::default());
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        repositories.clone(),
        executor.clone(),
        "notification-free".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_secs(1)),
        cancel.clone(),
    ));

    executor.wait_for(3).await;
    for workflow_id in &workflow_ids {
        wait_for_status(&pool, *workflow_id, "Ready").await;
    }
    cancel.cancel();
    worker.await??;

    let mut submitted = Vec::new();
    loop {
        let now = Utc::now();
        let leases = repositories
            .lease_definition_submissions(DefinitionSubmissionLeaseRequest {
                owner: "notification-free-submission",
                now,
                expires_at: now + chrono::Duration::seconds(1),
                limit: 1,
            })
            .await?;
        let Some(lease) = leases.into_iter().next() else {
            break;
        };
        submitted.push(lease.intent.workflow_id);
        assert_eq!(
            repositories
                .settle_leased_definition_submission(&lease, Utc::now())
                .await?,
            LeasedDefinitionSubmissionSettlementOutcome::Settled(
                DefinitionSubmissionSettlementOutcome::Submitted
            )
        );
    }
    let mut expected = workflow_ids.clone();
    expected.sort();
    assert_eq!(submitted, expected);
    for workflow_id in workflow_ids {
        wait_for_status(&pool, workflow_id, "Submitted").await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_lease_recovers_crashed_build_without_duplicate_settlement(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let (workflow, tasks) = workflow_with_n_tasks("crashed-build", 1, 1);
    seed_workflow_and_task_rows(&pool, &workflow, &tasks).await?;
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));

    let blocking = Arc::new(BlockingExecutor::default());
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let crashed = tokio::spawn(start_local_definition_build_worker(
        repositories.clone(),
        blocking.clone(),
        "crashed-owner".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_millis(80)),
        CancellationToken::new(),
    ));
    tokio::time::timeout(Duration::from_secs(3), blocking.started.notified())
        .await
        .expect("crashed worker did not begin the build");
    crashed.abort();
    assert!(crashed.await.unwrap_err().is_cancelled());

    let (status, lease_owner): (String, Option<String>) = sqlx::query_as(
        "SELECT status, lease_owner FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 AND task_id = $2",
    )
    .bind(workflow.id)
    .bind(tasks[0])
    .fetch_one(&pool)
    .await?;
    assert_eq!(status, "pending");
    assert_eq!(lease_owner.as_deref(), Some("crashed-owner"));

    tokio::time::sleep(Duration::from_millis(100)).await;
    let recovered = Arc::new(SuccessExecutor::default());
    let (_notifier, notifications) = definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_build_worker(
        repositories,
        recovered.clone(),
        "recovered-owner".to_owned(),
        notifications,
        reconciler_config(1, Duration::from_secs(1)),
        cancel.clone(),
    ));
    recovered.wait_for(1).await;
    wait_for_status(&pool, workflow.id, "Ready").await;
    cancel.cancel();
    worker.await??;

    let (status, built_at, lease_owner): (String, Option<chrono::DateTime<Utc>>, Option<String>) =
        sqlx::query_as(
            "SELECT status, built_at, lease_owner FROM workflow_task_builds \
             WHERE workflow_id = $1 AND workflow_version = 1 AND task_id = $2",
        )
        .bind(workflow.id)
        .bind(tasks[0])
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "success");
    assert!(built_at.is_some());
    assert!(lease_owner.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path_flips_to_ready() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };

    let (wf, task_ids) = workflow_with_n_tasks("happy-path", 1, 3);
    seed_workflow_and_task_rows(&pool, &wf, &task_ids).await?;

    let repository = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    for (i, task_id) in task_ids.iter().enumerate() {
        let outcome = repository
            .settle_definition_task_build(
                wf.get_id(),
                wf.get_version(),
                *task_id,
                DefinitionTaskBuildResult::Success,
            )
            .await?;
        if i < task_ids.len() - 1 {
            assert_eq!(
                outcome,
                DefinitionBuildSettlementOutcome::AwaitingTasks,
                "task {} of {} should not flip while siblings pend",
                i + 1,
                task_ids.len()
            );
        } else {
            assert!(
                matches!(outcome, DefinitionBuildSettlementOutcome::Ready(_)),
                "last task must flip workflow to Ready"
            );
        }
    }

    // Workflow row now Ready.
    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(wf.get_id())
            .bind(wf.get_version())
            .fetch_one(&pool)
            .await?;
    assert_eq!(status, "Ready");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn any_failure_flips_to_build_failed() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let (wf, task_ids) = workflow_with_n_tasks("failing-build", 1, 2);
    seed_workflow_and_task_rows(&pool, &wf, &task_ids).await?;

    let repository = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    let outcome = repository
        .settle_definition_task_build(
            wf.get_id(),
            wf.get_version(),
            task_ids[0],
            DefinitionTaskBuildResult::Failure {
                error: "synthetic test failure",
            },
        )
        .await?;
    assert_eq!(outcome, DefinitionBuildSettlementOutcome::BuildFailed);

    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(wf.get_id())
            .bind(wf.get_version())
            .fetch_one(&pool)
            .await?;
    assert_eq!(status, "BuildFailed");

    let stray = repository
        .settle_definition_task_build(
            wf.get_id(),
            wf.get_version(),
            task_ids[1],
            DefinitionTaskBuildResult::Success,
        )
        .await?;
    assert_eq!(
        stray,
        DefinitionBuildSettlementOutcome::AlreadySettled(
            tickr_migrations::definition_repository::DefinitionLifecycleStatus::BuildFailed
        )
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_finalizers_flip_to_ready_exactly_once() -> Result<(), Box<dyn std::error::Error>>
{
    // Competing Conductors claim the bounded row set without overlap. Their
    // guarded finalizers serialize so exactly the final committed Task wins.
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

    const N: usize = 8;
    let (wf, task_ids) = workflow_with_n_tasks("concurrent-finalizer", 1, N);
    seed_workflow_and_task_rows(&pool, &wf, &task_ids).await?;
    let repository = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));

    let now = Utc::now();
    let (first, second) = tokio::join!(
        repository.lease_definition_build_tasks(DefinitionBuildLeaseRequest {
            owner: "conductor-a",
            now,
            expires_at: now + chrono::Duration::seconds(1),
            limit: N,
        }),
        repository.lease_definition_build_tasks(DefinitionBuildLeaseRequest {
            owner: "conductor-b",
            now,
            expires_at: now + chrono::Duration::seconds(1),
            limit: N,
        }),
    );
    let mut leases = first?;
    leases.extend(second?);
    assert_eq!(leases.len(), N);
    assert_eq!(
        leases
            .iter()
            .map(|lease| lease.task.task_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        N,
        "competing claims must not overlap"
    );

    let mut handles = Vec::with_capacity(N);
    for lease in leases {
        let repository = Arc::clone(&repository);
        handles.push(tokio::spawn(async move {
            repository
                .settle_leased_definition_task_build(
                    &lease,
                    DefinitionTaskBuildResult::Success,
                    Utc::now(),
                )
                .await
        }));
    }

    let mut ready = 0usize;
    let mut awaiting = 0usize;
    for handle in handles {
        match handle.await? {
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::Ready(_),
            )) => ready += 1,
            Ok(LeasedDefinitionBuildSettlementOutcome::Settled(
                DefinitionBuildSettlementOutcome::AwaitingTasks,
            )) => awaiting += 1,
            Ok(other) => panic!("unexpected outcome: {other:?}"),
            Err(error) => panic!("settlement error: {error}"),
        }
    }

    assert_eq!(ready, 1, "exactly one finalizer must flip to Ready");
    assert_eq!(awaiting, N - 1, "the rest must await other tasks");

    // Verify the workflow row sits at Ready exactly once.
    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(wf.get_id())
            .bind(wf.get_version())
            .fetch_one(pool.as_ref())
            .await?;
    assert_eq!(
        status, "Ready",
        "the single winning finalizer must leave the row at Ready"
    );

    Ok(())
}

#[test]
fn test_build_executor_returns_configured_outcomes() {
    use tickr_conductor::build_pipeline::TaskBuildJob;
    use tickr_conductor::build_pipeline::TestBuildExecutor;

    // Sanity check the fake executor's contract — required reading for
    // anyone tempted to write a 'real Nix' acceptance test instead of
    // composing the existing pipeline against the fake.
    let exec = TestBuildExecutor::new();
    let succeed_id = Uuid::new_v4();
    let fail_id = Uuid::new_v4();
    exec.fail(fail_id, "boom");

    let job_ok = TaskBuildJob {
        workflow_id: Uuid::new_v4(),
        workflow_version: 1,
        task_id: succeed_id,
        nix_expression_path: "x".into(),
    };
    let job_fail = TaskBuildJob {
        workflow_id: Uuid::new_v4(),
        workflow_version: 1,
        task_id: fail_id,
        nix_expression_path: "x".into(),
    };

    // Drive the BuildExecutor trait directly. Unconfigured tasks
    // default to Success; configured failures carry their error text.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ok = rt.block_on(async {
        use tickr_conductor::build_pipeline::BuildExecutor;
        exec.build(&job_ok).await
    });
    let fail = rt.block_on(async {
        use tickr_conductor::build_pipeline::BuildExecutor;
        exec.build(&job_fail).await
    });
    assert_eq!(ok, BuildOutcome::Success);
    assert_eq!(
        fail,
        BuildOutcome::Failure {
            error: "boom".to_string()
        }
    );
}
