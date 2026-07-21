//! Integration test for the Default-version resolver's PG query against a real
//! ephemeral Postgres (`testcontainers`) with the conductor migrations applied.
//!
//! The rule under test: the latest *live* version (`Ready`/`Submitted`) by
//! `inserted_at`; if none is live, the latest version overall by `inserted_at`;
//! `None` only for an unknown workflow id.
//!
//! Requires Docker. Skipped automatically when Docker isn't available.

#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use uuid::Uuid;

async fn insert_version(
    pool: &sqlx::PgPool,
    id: Uuid,
    version: i64,
    status: &str,
    inserted_at: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source, inserted_at)
        VALUES ($1, $2, 'default', 'wf', 'wf', $3, 'testhash', 'testcos', '{}'::jsonb, 'src', ($4)::timestamptz)
        "#,
    )
    .bind(id)
    .bind(version)
    .bind(status)
    .bind(inserted_at)
    .execute(pool)
    .await
    .expect("insert workflow version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolves_default_version_per_rule() -> Result<(), Box<dyn std::error::Error>> {
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
    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());

    // wf_a: a newer `Submitted` beats an older `Ready`, and a still-newer
    // `Building` does not win (it is not live). Default = the Submitted v2.0.0.
    let wf_a = Uuid::new_v4();
    insert_version(&pool, wf_a, 1, "Ready", "2026-01-01T00:00:00Z").await;
    insert_version(&pool, wf_a, 2, "Submitted", "2026-01-02T00:00:00Z").await;
    insert_version(&pool, wf_a, 3, "Building", "2026-01-03T00:00:00Z").await;

    // wf_b: nothing ever built live → fall back to the latest by inserted_at,
    // which is the BuildFailed v2.0.0.
    let wf_b = Uuid::new_v4();
    insert_version(&pool, wf_b, 1, "Building", "2026-01-01T00:00:00Z").await;
    insert_version(&pool, wf_b, 2, "BuildFailed", "2026-01-05T00:00:00Z").await;

    // wf_c: interleaved live/non-live. v4.0.0 (Ready) is the latest *live* even
    // though v3.0.0 (BuildFailed) was inserted later. Default = v4.0.0.
    let wf_c = Uuid::new_v4();
    insert_version(&pool, wf_c, 1, "Building", "2026-01-01T00:00:00Z").await;
    insert_version(&pool, wf_c, 2, "Ready", "2026-01-02T00:00:00Z").await;
    insert_version(&pool, wf_c, 4, "Ready", "2026-01-03T00:00:00Z").await;
    insert_version(&pool, wf_c, 3, "BuildFailed", "2026-01-04T00:00:00Z").await;

    let wf_unknown = Uuid::new_v4();

    assert_eq!(
        repository.default_definition_version(wf_a).await?,
        Some((2, "Submitted".to_string())),
        "latest live (Submitted) beats older Ready and newer Building"
    );
    assert_eq!(
        repository.default_definition_version(wf_b).await?,
        Some((2, "BuildFailed".to_string())),
        "no live version falls back to the highest explicit version"
    );
    assert_eq!(
        repository.default_definition_version(wf_c).await?,
        Some((4, "Ready".to_string())),
        "highest live version wins regardless of insertion order"
    );
    assert_eq!(
        repository.default_definition_version(wf_unknown).await?,
        None
    );
    Ok(())
}
