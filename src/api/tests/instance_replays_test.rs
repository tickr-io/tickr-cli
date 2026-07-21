//! Integration test for the reverse-link "list a run's replays" read against a
//! real ephemeral Postgres (`testcontainers`) with the conductor migrations
//! applied.
//!
//! The rule under test: `list_replays_for_source` returns exactly the replays
//! whose `source_instance_id` matches, newest first — served from the
//! `workflow_replays_source_idx` indexed row, never a scan of unrelated rows.
//! An unrelated source's replays never leak into the answer, and the audit
//! surface stays names-only (`shadowed_keys`) with the resume-from frontier
//! preserved.
//!
//! Requires Docker. Skipped automatically when Docker isn't available.

#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
async fn insert_replay(
    pool: &sqlx::PgPool,
    replay_instance_id: Uuid,
    source_instance_id: Uuid,
    status: &str,
    name: Option<&str>,
    resume_from: &[Uuid],
    shadowed_keys: &[&str],
    created_at: &str,
) {
    let resume_json = serde_json::to_value(resume_from).unwrap();
    let shadowed_json = serde_json::to_value(shadowed_keys).unwrap();
    sqlx::query(
        r#"
        INSERT INTO workflow_replays
            (replay_instance_id, source_instance_id, signal_id, status,
             resume_from, name, shadowed_keys, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, ($8)::timestamptz, ($8)::timestamptz)
        "#,
    )
    .bind(replay_instance_id)
    .bind(source_instance_id)
    .bind(Uuid::new_v4())
    .bind(status)
    .bind(&resume_json)
    .bind(name)
    .bind(&shadowed_json)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("insert replay row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_a_source_runs_replays_indexed_and_scoped() -> Result<(), Box<dyn std::error::Error>>
{
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: testcontainers Postgres unavailable: {}", e);
            return Ok(());
        }
    };
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool).await?;
    let repositories = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());

    let source = Uuid::new_v4();
    let other_source = Uuid::new_v4();
    let node = Uuid::new_v4();

    // Two replays of `source` (an older one, then a newer one), plus a replay of
    // an unrelated `other_source` that must NOT leak into `source`'s answer.
    let older = Uuid::from_u128(1);
    let newer = Uuid::from_u128(2);
    insert_replay(
        &pool,
        older,
        source,
        "Released",
        Some("first recovery"),
        &[node],
        &[],
        "2026-07-13T10:00:00Z",
    )
    .await;
    insert_replay(
        &pool,
        newer,
        source,
        "Materializing",
        None,
        &[node],
        &["db_password"],
        "2026-07-13T10:00:00Z",
    )
    .await;
    insert_replay(
        &pool,
        Uuid::new_v4(),
        other_source,
        "Released",
        None,
        &[],
        &[],
        "2026-07-13T12:00:00Z",
    )
    .await;

    let rows = repositories.replays_for_source(source).await?;
    // Exactly the source's two replays — the unrelated source's row is excluded
    // by the indexed `WHERE source_instance_id = $1`, not filtered post-scan.
    assert_eq!(rows.len(), 2, "only this source's replays are returned");
    // Newest first, with replay identity descending as the equal-time tie-break.
    assert_eq!(rows[0].replay_instance_id, newer);
    assert_eq!(rows[1].replay_instance_id, older);
    assert!(rows.iter().all(|r| r.source_instance_id == source));

    // The resume-from frontier and the names-only shadow audit round-trip.
    assert_eq!(rows[0].resume_from, vec![node]);
    assert_eq!(rows[0].shadowed_keys, vec!["db_password".to_string()]);
    assert_eq!(rows[1].name.as_deref(), Some("first recovery"));
    assert!(rows[1].shadowed_keys.is_empty());

    // A source with no replays is an empty list, not an error.
    let empty = repositories.replays_for_source(Uuid::new_v4()).await?;
    assert!(empty.is_empty(), "a run with no replays lists empty");

    Ok(())
}
