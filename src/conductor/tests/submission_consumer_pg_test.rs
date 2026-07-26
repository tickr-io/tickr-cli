#![cfg(not(madsim))]

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use prost::Message;
use sqlx::{PgPool, Row};
use tickr_conductor::proto::ConductorRelayMessage;
use tickr_conductor::relay::{forward_workflow_registration_bytes, init_relay_tx};
use tickr_conductor::submission_consumer::{
    definition_submission_notifications, start_local_definition_submission_worker,
    LocalDefinitionSubmissionWorkerConfig,
};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionBuildSettlementOutcome, DefinitionRegistrationInput, DefinitionRegistrationOutcome,
    DefinitionSubmissionLeaseRequest, DefinitionTaskBuildResult,
};
use tickr_proto::workflow as wf;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod common;

const LEASE_DURATION: Duration = Duration::from_millis(500);

async fn register_building(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    let repositories = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    let outcome = repositories
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
}

async fn make_ready(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
    let repositories = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    assert!(matches!(
        repositories
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

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process boundary marker was not observed");
}

async fn wait_for_status(pool: &PgPool, workflow_id: Uuid, expected: &str) {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
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
    .await;
    if result.is_err() {
        let actual: String =
            sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = 1")
                .bind(workflow_id)
                .fetch_one(pool)
                .await
                .unwrap();
        panic!("definition {workflow_id} did not reach {expected}; current status is {actual}");
    }
}

async fn wait_for_lease(pool: &PgPool, workflow_id: Uuid) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let owner: Option<String> = sqlx::query_scalar(
                "SELECT submission_lease_owner FROM workflows WHERE id = $1 AND version = 1",
            )
            .bind(workflow_id)
            .fetch_one(pool)
            .await
            .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("definition submission lease was not acquired");
}

async fn database_url(pool: &PgPool) -> String {
    let base = std::env::var("TICKR_TEST_PG_URL").unwrap();
    let database = sqlx::query("SELECT current_database()")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<String, _>(0);
    format!("{}/{database}", base.trim_end_matches('/'))
}

struct HelperConfig<'a> {
    mode: &'a str,
    workflow_id: Uuid,
    task_id: Uuid,
    marker: Option<&'a Path>,
    gate: Option<&'a Path>,
    ready: Option<&'a Path>,
    start: Option<&'a Path>,
}

fn spawn_helper(db_url: &str, config: HelperConfig<'_>) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("real_process_submission_helper")
        .arg("--nocapture")
        .env("TICKR_SUBMISSION_HELPER", "1")
        .env("TICKR_SUBMISSION_HELPER_DB_URL", db_url)
        .env("TICKR_SUBMISSION_HELPER_MODE", config.mode)
        .env(
            "TICKR_SUBMISSION_HELPER_WORKFLOW_ID",
            config.workflow_id.to_string(),
        )
        .env(
            "TICKR_SUBMISSION_HELPER_TASK_ID",
            config.task_id.to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (name, path) in [
        ("TICKR_SUBMISSION_HELPER_MARKER", config.marker),
        ("TICKR_SUBMISSION_HELPER_GATE", config.gate),
        ("TICKR_SUBMISSION_HELPER_READY", config.ready),
        ("TICKR_SUBMISSION_HELPER_START", config.start),
    ] {
        if let Some(path) = path {
            command.env(name, path);
        }
    }
    command.spawn().unwrap()
}

async fn run_submission_worker(pool: PgPool, workflow_id: Uuid, mode: &str) {
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    let (relay_tx, mut relay_rx) = mpsc::channel(1);
    let blocked_relay = mode == "before-relay";
    if blocked_relay {
        relay_tx.try_send(ConductorRelayMessage::default()).unwrap();
    }
    init_relay_tx(relay_tx).await;

    let relay_drain = if blocked_relay {
        tokio::spawn(async move {
            let _receiver = relay_rx;
            futures::future::pending::<()>().await;
        })
    } else {
        let marker = std::env::var_os("TICKR_SUBMISSION_HELPER_MARKER").map(PathBuf::from);
        tokio::spawn(async move {
            let forwarded = relay_rx.recv().await.expect("forwarded definition");
            if let Some(marker) = marker {
                std::fs::write(marker, forwarded.payload).unwrap();
            }
        })
    };

    let (_notifier, notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_definition_submission_worker(
        repositories,
        format!("submission-helper-{}", std::process::id()),
        notifications,
        LocalDefinitionSubmissionWorkerConfig {
            scan_interval: Duration::from_millis(20),
            lease_duration: LEASE_DURATION,
            batch_size: NonZeroUsize::new(1).unwrap(),
        },
        cancel.clone(),
    ));

    if blocked_relay {
        futures::future::pending::<()>().await;
    }
    wait_for_status(&pool, workflow_id, "Submitted").await;
    cancel.cancel();
    worker.await.unwrap().unwrap();
    relay_drain.abort();
}

async fn forward_without_settlement(pool: PgPool, workflow_id: Uuid) {
    let repositories = WriterRepositoryBundle::from_postgres_pool(pool);
    let now = Utc::now();
    let lease = repositories
        .lease_definition_submissions(DefinitionSubmissionLeaseRequest {
            owner: "forward-without-settlement",
            now,
            expires_at: now + chrono::Duration::from_std(LEASE_DURATION).unwrap(),
            limit: 1,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|lease| lease.intent.workflow_id == workflow_id)
        .expect("target definition was not leased");

    let (relay_tx, mut relay_rx) = mpsc::channel(1);
    init_relay_tx(relay_tx).await;
    forward_workflow_registration_bytes(lease.intent.definition.encode_to_vec())
        .await
        .unwrap();
    let forwarded = relay_rx.recv().await.expect("forwarded definition");
    let marker = PathBuf::from(std::env::var("TICKR_SUBMISSION_HELPER_MARKER").unwrap());
    std::fs::write(marker, forwarded.payload).unwrap();
    futures::future::pending::<()>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by real-process submission recovery tests"]
async fn real_process_submission_helper() {
    if std::env::var_os("TICKR_SUBMISSION_HELPER").is_none() {
        return;
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect(&std::env::var("TICKR_SUBMISSION_HELPER_DB_URL").unwrap())
        .await
        .unwrap();
    let workflow_id =
        Uuid::parse_str(&std::env::var("TICKR_SUBMISSION_HELPER_WORKFLOW_ID").unwrap()).unwrap();
    let task_id =
        Uuid::parse_str(&std::env::var("TICKR_SUBMISSION_HELPER_TASK_ID").unwrap()).unwrap();
    let mode = std::env::var("TICKR_SUBMISSION_HELPER_MODE").unwrap();

    if let Some(ready) = std::env::var_os("TICKR_SUBMISSION_HELPER_READY") {
        std::fs::write(ready, b"ready").unwrap();
        wait_for_path(Path::new(
            &std::env::var("TICKR_SUBMISSION_HELPER_START").unwrap(),
        ))
        .await;
    }

    if mode == "after-forward" {
        forward_without_settlement(pool, workflow_id).await;
        return;
    }
    if mode == "before-parent-finalization" {
        let marker = PathBuf::from(std::env::var("TICKR_SUBMISSION_HELPER_MARKER").unwrap());
        std::fs::write(marker, b"finalizing").unwrap();
        let repositories = WriterRepositoryBundle::from_postgres_pool(pool);
        let _ = repositories
            .settle_definition_task_build(
                workflow_id,
                1,
                task_id,
                DefinitionTaskBuildResult::Success,
            )
            .await;
        return;
    }

    if mode == "finalize-and-submit" {
        let repositories = WriterRepositoryBundle::from_postgres_pool(pool.clone());
        assert!(matches!(
            repositories
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

    run_submission_worker(pool, workflow_id, &mode).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_crashes_reconcile_without_notifications() {
    let Some((_guard, pool)) = common::test_db().await else {
        return;
    };
    let db_url = database_url(&pool).await;
    let directory = tempfile::TempDir::new().unwrap();

    let before_relay_id = Uuid::from_u128(1);
    let before_relay_task = Uuid::from_u128(1_001);
    register_building(&pool, before_relay_id, before_relay_task).await;
    make_ready(&pool, before_relay_id, before_relay_task).await;
    let before_relay_marker = directory.path().join("before-relay-forwarded");
    let mut before_relay = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "before-relay",
            workflow_id: before_relay_id,
            task_id: before_relay_task,
            marker: Some(&before_relay_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    wait_for_lease(&pool, before_relay_id).await;
    before_relay.kill().unwrap();
    before_relay.wait().unwrap();
    assert!(!before_relay_marker.exists());
    wait_for_status(&pool, before_relay_id, "Ready").await;
    tokio::time::sleep(LEASE_DURATION + Duration::from_millis(100)).await;
    let recovered_marker = directory.path().join("before-relay-recovered");
    let mut recovered = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "normal",
            workflow_id: before_relay_id,
            task_id: before_relay_task,
            marker: Some(&recovered_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    assert!(recovered.wait().unwrap().success());
    assert!(recovered_marker.exists());
    wait_for_status(&pool, before_relay_id, "Submitted").await;

    let after_forward_id = Uuid::from_u128(2);
    let after_forward_task = Uuid::from_u128(1_002);
    register_building(&pool, after_forward_id, after_forward_task).await;
    make_ready(&pool, after_forward_id, after_forward_task).await;
    let forwarded_marker = directory.path().join("after-forward");
    let mut after_forward = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "after-forward",
            workflow_id: after_forward_id,
            task_id: after_forward_task,
            marker: Some(&forwarded_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    wait_for_path(&forwarded_marker).await;
    after_forward.kill().unwrap();
    after_forward.wait().unwrap();
    let lease_owner: Option<String> = sqlx::query_scalar(
        "SELECT submission_lease_owner FROM workflows WHERE id = $1 AND version = 1",
    )
    .bind(after_forward_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lease_owner.as_deref(), Some("forward-without-settlement"));
    wait_for_status(&pool, after_forward_id, "Ready").await;
    tokio::time::sleep(LEASE_DURATION + Duration::from_millis(100)).await;
    let redrive_marker = directory.path().join("after-forward-redrive");
    let mut redriven = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "normal",
            workflow_id: after_forward_id,
            task_id: after_forward_task,
            marker: Some(&redrive_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    assert!(redriven.wait().unwrap().success());
    assert!(redrive_marker.exists());
    wait_for_status(&pool, after_forward_id, "Submitted").await;

    let before_parent_id = Uuid::from_u128(3);
    let before_parent_task = Uuid::from_u128(1_003);
    register_building(&pool, before_parent_id, before_parent_task).await;
    let parent_marker = directory.path().join("before-parent-finalization");
    let mut parent_lock = pool.begin().await.unwrap();
    let _: String =
        sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = 1 FOR UPDATE")
            .bind(before_parent_id)
            .fetch_one(&mut *parent_lock)
            .await
            .unwrap();
    let mut before_parent = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "before-parent-finalization",
            workflow_id: before_parent_id,
            task_id: before_parent_task,
            marker: Some(&parent_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    wait_for_path(&parent_marker).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    before_parent.kill().unwrap();
    before_parent.wait().unwrap();
    parent_lock.rollback().await.unwrap();
    wait_for_status(&pool, before_parent_id, "Building").await;
    let final_marker = directory.path().join("parent-finalized-and-submitted");
    let mut finalized = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "finalize-and-submit",
            workflow_id: before_parent_id,
            task_id: before_parent_task,
            marker: Some(&final_marker),
            gate: None,
            ready: None,
            start: None,
        },
    );
    assert!(finalized.wait().unwrap().success());
    assert!(final_marker.exists());
    wait_for_status(&pool, before_parent_id, "Submitted").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_processes_commit_one_submission_relay_effect() {
    let Some((_guard, pool)) = common::test_db().await else {
        return;
    };
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    register_building(&pool, workflow_id, task_id).await;
    make_ready(&pool, workflow_id, task_id).await;

    let db_url = database_url(&pool).await;
    let directory = tempfile::TempDir::new().unwrap();
    let start = directory.path().join("start");
    let ready_a = directory.path().join("ready-a");
    let ready_b = directory.path().join("ready-b");
    let forwarded_a = directory.path().join("forwarded-a");
    let forwarded_b = directory.path().join("forwarded-b");
    let mut first = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "normal",
            workflow_id,
            task_id,
            marker: Some(&forwarded_a),
            gate: None,
            ready: Some(&ready_a),
            start: Some(&start),
        },
    );
    let mut second = spawn_helper(
        &db_url,
        HelperConfig {
            mode: "normal",
            workflow_id,
            task_id,
            marker: Some(&forwarded_b),
            gate: None,
            ready: Some(&ready_b),
            start: Some(&start),
        },
    );
    wait_for_path(&ready_a).await;
    wait_for_path(&ready_b).await;
    std::fs::write(&start, b"start").unwrap();
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
    wait_for_status(&pool, workflow_id, "Submitted").await;
    assert_eq!(
        usize::from(forwarded_a.exists()) + usize::from(forwarded_b.exists()),
        1
    );
}
