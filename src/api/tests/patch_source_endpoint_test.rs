//! Integration test for the patch authored-source read path against a real
//! ephemeral Postgres (`testcontainers`) with the conductor migrations applied.
//!
//! The read path returns a Patch's retained source exactly as submitted
//! (Nickel or JSON), alongside the `applied_version` that joins it to the
//! server-side applied-patch effect. `None` only for an unknown `patch_id`.
//!
//! Requires Docker. Skipped automatically when Docker isn't available.

#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::archive_queries::get_patch_source;
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
async fn insert_patch(
    pool: &sqlx::PgPool,
    patch_key: Uuid,
    patch_id: Uuid,
    workflow_instance_id: Uuid,
    status: &str,
    source: Option<&str>,
    source_format: Option<&str>,
    applied_version: Option<i64>,
) {
    sqlx::query(
        r#"
        INSERT INTO workflow_patches
            (patch_key, patch_id, workflow_instance_id, status, ops, source, source_format, applied_version)
        VALUES ($1, $2, $3, $4, '[]'::jsonb, $5, $6, $7)
        "#,
    )
    .bind(patch_key)
    .bind(patch_id)
    .bind(workflow_instance_id)
    .bind(status)
    .bind(source)
    .bind(source_format)
    .bind(applied_version)
    .execute(pool)
    .await
    .expect("insert patch row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_path_returns_retained_source() -> Result<(), Box<dyn std::error::Error>> {
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

    // An applied external patch: the authored Nickel comes back verbatim, and
    // `applied_version` is present so a reader can join to the server's effect.
    let wi = Uuid::new_v4();
    let external_id = Uuid::new_v4();
    let external_key = Uuid::new_v4();
    let nickel = "{ ops = [ mkInsert { anchor = \"aB3d\", task = enrich } ] }";
    insert_patch(
        &pool,
        external_key,
        external_id,
        wi,
        "Applied",
        Some(nickel),
        Some("nickel"),
        Some(9),
    )
    .await;

    // A self-patch retained as its evaluated JSON document.
    let self_id = Uuid::new_v4();
    let self_key = Uuid::new_v4();
    let json = r#"{"ops":[{"AddNode":{"node_id":"..."}}],"reason":"runtime fan-out"}"#;
    insert_patch(
        &pool,
        self_key,
        self_id,
        wi,
        "Applied",
        Some(json),
        Some("json"),
        Some(10),
    )
    .await;

    let external = get_patch_source(&pool, external_id)
        .await?
        .expect("external patch source");
    assert_eq!(external.source.as_deref(), Some(nickel));
    assert_eq!(external.source_format.as_deref(), Some("nickel"));
    assert_eq!(external.applied_version, Some(9));
    assert_eq!(external.workflow_instance_id, wi);

    let self_patch = get_patch_source(&pool, self_id)
        .await?
        .expect("self patch source");
    assert_eq!(self_patch.source.as_deref(), Some(json));
    assert_eq!(self_patch.source_format.as_deref(), Some("json"));
    assert_eq!(self_patch.applied_version, Some(10));

    assert!(
        get_patch_source(&pool, Uuid::new_v4()).await?.is_none(),
        "unknown patch_id resolves to None"
    );
    Ok(())
}
