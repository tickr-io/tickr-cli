//! End-to-end test for the system-assigned-version register pipeline against a
//! real Postgres + NATS, driving `process_register` exactly as the command-bus
//! consumer does.
//!
//! Proves the content-hash register decision over the full stack:
//!   - first registration            → Inserted v1
//!   - byte-identical re-submission   → NoOp v1 (no new row)
//!   - changed content                → Inserted v2
//!   - re-submit the v1 content       → Inserted v3 (rollback-by-resubmit:
//!     matches an older version's hash but differs from the latest, so it mints
//!     a new MAX+1 row whose content_hash equals v1's)
//!
//! Requires Docker (testcontainers PG + NATS) and `nickel` on PATH; skips
//! cleanly when any is unavailable.

#![cfg(not(madsim))]

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use sqlx::PgPool;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::parser::nickel::DSL_PATHS_ENV;

mod common;
use tickr_conductor::register_pipeline::{process_register, RegisterOutcome, RegisterRequest};

fn nickel_available() -> bool {
    Command::new("nickel")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn set_dsl_path() {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("..");
    p.push("..");
    p.push("dsl");
    std::env::set_var(DSL_PATHS_ENV, p);
}

async fn start_pg() -> Option<(common::DbGuard, PgPool)> {
    common::test_db().await
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {e}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{port}");
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client?))
}

/// A minimal one-task workflow source with parameterised `nix_expression_path`
/// (identity-affecting) and display `name` (cosmetic), so tests can vary content
/// and cosmetics independently.
fn wf_source_named(nix_path: &str, name: &str) -> String {
    format!(
        r#"let utils = import "lib.ncl" in
utils.mkWorkflow {{
  slug = "e2e-versioning",
  name = "{name}",
  args = [],
  outputs = [],
  tasks = [ utils.mkTaskGroup {{
    name = "g",
    args = [],
    outputs = [],
    tasks = [ utils.mkTask {{ name = "t", args = [], nix_expression_path = "{nix_path}", outputs = [] }} ],
  }} ],
}}"#
    )
}

fn wf_source(nix_path: &str) -> String {
    wf_source_named(nix_path, "E2E-Versioning")
}

fn req(source: &str) -> RegisterRequest {
    RegisterRequest {
        nickel_source: source.to_string(),
        namespace: "default".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_assigns_versions_noops_identical_and_rolls_back() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let Some((_pg, pool)) = start_pg().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };
    set_dsl_path();
    let repository =
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(pool.clone());

    let src_a = wf_source("expr-a");
    let src_b = wf_source("expr-b");

    // First registration → Inserted at v1.
    let (id, v1) = match process_register(&repository, &nats, req(&src_a))
        .await
        .expect("register v1")
    {
        RegisterOutcome::Inserted {
            workflow_id,
            workflow_version,
            ..
        } => (workflow_id, workflow_version),
        _ => panic!("first registration must Insert, not NoOp"),
    };
    assert_eq!(v1, 1, "first registration is version 1");

    // Byte-identical re-submission → NoOp at v1, no new row.
    match process_register(&repository, &nats, req(&src_a))
        .await
        .expect("register identical")
    {
        RegisterOutcome::NoOp {
            workflow_version, ..
        } => assert_eq!(workflow_version, 1, "identical content no-ops at v1"),
        _ => panic!("identical content must NoOp, not Insert"),
    }

    // Changed content (different nix_expression_path) → Inserted at v2.
    match process_register(&repository, &nats, req(&src_b))
        .await
        .expect("register changed")
    {
        RegisterOutcome::Inserted {
            workflow_version, ..
        } => assert_eq!(workflow_version, 2, "changed content bumps to v2"),
        _ => panic!("changed content must Insert, not NoOp"),
    }

    // Rollback-by-resubmit: the v1 content again. It matches v1's hash but
    // differs from the latest (v2), so it mints a new MAX+1 row at v3.
    match process_register(&repository, &nats, req(&src_a))
        .await
        .expect("register rollback")
    {
        RegisterOutcome::Inserted {
            workflow_version, ..
        } => assert_eq!(workflow_version, 3, "rollback-by-resubmit is a new v3 row"),
        _ => panic!("rollback differs from latest and must Insert, not NoOp"),
    }

    // Storage: exactly versions [1, 2, 3] for this workflow_id.
    let versions: Vec<(i64,)> =
        sqlx::query_as("SELECT version FROM workflows WHERE id = $1 ORDER BY version ASC")
            .bind(id)
            .fetch_all(&pool)
            .await
            .expect("query versions");
    assert_eq!(
        versions.iter().map(|(v,)| *v).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // The rollback (v3) carries the same content_hash as v1, and v2 differs —
    // the content-hash mechanism, observed at the storage layer.
    let hashes: Vec<(i64, String)> = sqlx::query_as(
        "SELECT version, content_hash FROM workflows WHERE id = $1 ORDER BY version",
    )
    .bind(id)
    .fetch_all(&pool)
    .await
    .expect("query hashes");
    let h1 = &hashes[0].1;
    let h2 = &hashes[1].1;
    let h3 = &hashes[2].1;
    assert_eq!(
        h1, h3,
        "rollback resubmits v1's content, so v3's hash matches v1"
    );
    assert_ne!(h1, h2, "v2 is a genuine content change");

    // --- Refreshed: a cosmetic-only change on a successfully-built version ---
    // Simulate v3 building successfully (no build worker runs in this test).
    sqlx::query("UPDATE workflows SET status = 'Ready' WHERE id = $1 AND version = $2")
        .bind(id)
        .bind(3_i64)
        .execute(&pool)
        .await
        .expect("flip v3 Ready");
    // Resubmit v3's content (expr-a) with a different display name → Refreshed at
    // v3 (content hash matches; cosmetic hash differs). No new row.
    match process_register(
        &repository,
        &nats,
        req(&wf_source_named("expr-a", "Renamed-Display")),
    )
    .await
    .expect("register cosmetic change")
    {
        RegisterOutcome::Refreshed {
            workflow_version, ..
        } => assert_eq!(
            workflow_version, 3,
            "cosmetic-only change refreshes v3 in place"
        ),
        _ => panic!("cosmetic-only change on a Ready version must Refresh"),
    }
    let (row_count,): (i64,) = sqlx::query_as("SELECT count(*) FROM workflows WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(row_count, 3, "Refreshed must not add a row");
    let (name3,): (String,) =
        sqlx::query_as("SELECT name FROM workflows WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(3_i64)
            .fetch_one(&pool)
            .await
            .expect("read refreshed name");
    assert_eq!(
        name3, "Renamed-Display",
        "Refreshed updates the display name in place"
    );

    // --- BuildRequeued: identical resubmit on a failed version ---
    // Simulate v3's build failing: mark its task build row failure + flip the
    // workflow row to BuildFailed.
    sqlx::query(
        "UPDATE workflow_task_builds SET status = 'failure', error = 'boom' \
         WHERE workflow_id = $1 AND workflow_version = $2",
    )
    .bind(id)
    .bind(3_i64)
    .execute(&pool)
    .await
    .expect("mark task failure");
    sqlx::query("UPDATE workflows SET status = 'BuildFailed' WHERE id = $1 AND version = $2")
        .bind(id)
        .bind(3_i64)
        .execute(&pool)
        .await
        .expect("flip v3 BuildFailed");
    // Resubmit identical content → BuildRequeued at v3 (no bump).
    match process_register(&repository, &nats, req(&wf_source("expr-a")))
        .await
        .expect("register on failed build")
    {
        RegisterOutcome::BuildRequeued {
            workflow_version,
            task_count,
            ..
        } => {
            assert_eq!(workflow_version, 3, "requeue stays on v3");
            assert!(
                task_count >= 1,
                "at least one failed task build re-enqueued"
            );
        }
        _ => panic!("identical resubmit on a BuildFailed version must BuildRequeue"),
    }
    // The workflow row flipped back to Building and the failed task row to pending.
    let (status3,): (String,) =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(3_i64)
            .fetch_one(&pool)
            .await
            .expect("read requeued status");
    assert_eq!(
        status3, "Building",
        "requeue resets the workflow row to Building"
    );
    let (pending,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = $2 AND status = 'pending'",
    )
    .bind(id)
    .bind(3_i64)
    .fetch_one(&pool)
    .await
    .expect("count pending");
    assert!(pending >= 1, "requeue resets failed task rows to pending");

    // Still exactly versions [1, 2, 3] — neither Refreshed nor BuildRequeued bumps.
    let after: Vec<(i64,)> =
        sqlx::query_as("SELECT version FROM workflows WHERE id = $1 ORDER BY version ASC")
            .bind(id)
            .fetch_all(&pool)
            .await
            .expect("re-query versions");
    assert_eq!(
        after.iter().map(|(v,)| *v).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}
