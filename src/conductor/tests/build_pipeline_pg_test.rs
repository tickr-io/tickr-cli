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

use sqlx::PgPool;
use std::sync::Arc;
use tickr_conductor::build_pipeline::{
    finalize_after_task_outcome, BuildOutcome, FinalizerOutcome,
};
use tickr_proto::workflow as wf;
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

async fn mark_task_success(pool: &PgPool, wf_id: Uuid, version: i64, task_id: Uuid) {
    sqlx::query(
        r#"UPDATE workflow_task_builds
              SET status = 'success', built_at = now()
            WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3"#,
    )
    .bind(wf_id)
    .bind(version)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("mark success");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path_flips_to_ready() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };

    let (wf, task_ids) = workflow_with_n_tasks("happy-path", 1, 3);
    seed_workflow_and_task_rows(&pool, &wf, &task_ids).await?;

    // Walk the tasks: each one flips its row to success, then runs the
    // finalizer. Only the last one should observe FlippedToReady.
    let mut outcomes = Vec::new();
    for (i, task_id) in task_ids.iter().enumerate() {
        mark_task_success(&pool, wf.get_id(), wf.get_version(), *task_id).await;
        let outcome = finalize_after_task_outcome(
            &pool,
            wf.get_id(),
            wf.get_version(),
            &BuildOutcome::Success,
        )
        .await?;
        outcomes.push(outcome.clone());
        if i < task_ids.len() - 1 {
            // Other tasks still pending; finalizer must be a no-op.
            assert_eq!(
                outcome,
                FinalizerOutcome::AlreadyTerminalOrNotReady,
                "task {} of {} should not flip while siblings pend",
                i + 1,
                task_ids.len()
            );
        } else {
            assert_eq!(
                outcome,
                FinalizerOutcome::FlippedToReady,
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

    // First task fails — workflow should short-circuit to BuildFailed
    // regardless of what the second task eventually does.
    let outcome = finalize_after_task_outcome(
        &pool,
        wf.get_id(),
        wf.get_version(),
        &BuildOutcome::Failure {
            error: "synthetic test failure".to_string(),
        },
    )
    .await?;
    assert_eq!(outcome, FinalizerOutcome::FlippedToBuildFailed);

    let (status,): (String,) =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(wf.get_id())
            .bind(wf.get_version())
            .fetch_one(&pool)
            .await?;
    assert_eq!(status, "BuildFailed");

    // A subsequent stray success-finalizer attempt must be a no-op —
    // BuildFailed is terminal.
    mark_task_success(&pool, wf.get_id(), wf.get_version(), task_ids[1]).await;
    let stray =
        finalize_after_task_outcome(&pool, wf.get_id(), wf.get_version(), &BuildOutcome::Success)
            .await?;
    assert_eq!(stray, FinalizerOutcome::AlreadyTerminalOrNotReady);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_finalizers_flip_to_ready_exactly_once() -> Result<(), Box<dyn std::error::Error>>
{
    // Non-negotiable atomicity test: N workers flip their last per-task
    // rows simultaneously and race the finalizer. The conditional
    // UPDATE is the locking mechanism — at most one finalizer wins, the
    // others see `Building` is no longer the row's status and exit.
    let Some((_pg, pool)) = start_pg().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

    const N: usize = 8;
    let (wf, task_ids) = workflow_with_n_tasks("concurrent-finalizer", 1, N);
    seed_workflow_and_task_rows(&pool, &wf, &task_ids).await?;

    // Pre-mark every task `success` so every spawned worker's finalizer
    // call is eligible to win on the eligibility predicate. The race
    // narrows to the conditional UPDATE itself.
    for task_id in &task_ids {
        mark_task_success(&pool, wf.get_id(), wf.get_version(), *task_id).await;
    }

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let pool = Arc::clone(&pool);
        let wf_id = wf.get_id();
        let version = wf.get_version();
        handles.push(tokio::spawn(async move {
            finalize_after_task_outcome(&pool, wf_id, version, &BuildOutcome::Success).await
        }));
    }

    let mut flipped = 0usize;
    let mut already = 0usize;
    for h in handles {
        match h.await? {
            Ok(FinalizerOutcome::FlippedToReady) => flipped += 1,
            Ok(FinalizerOutcome::AlreadyTerminalOrNotReady) => already += 1,
            Ok(other) => panic!("unexpected outcome: {:?}", other),
            Err(e) => panic!("finalizer error: {}", e),
        }
    }

    assert_eq!(flipped, 1, "exactly one finalizer must flip to Ready");
    assert_eq!(already, N - 1, "the rest must be AlreadyTerminalOrNotReady");

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
