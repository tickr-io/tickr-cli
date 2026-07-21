//! Integration tests for the definition read repository as served by the API
//! component. Spins up ephemeral Postgres, applies the conductor migrations,
//! inserts published workflow-definition fixtures, and asserts that the
//! repository rehydrates the same proto shape.
//!
//! Requires Docker (testcontainers). Skipped automatically when Docker isn't
//! available — the marker is the connection failure.

#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use tickr_migrations::definition_repository::DefinitionListRow;
use tickr_proto::workflow as wf;
use uuid::Uuid;

fn build_workflow(name: &str, cron: Option<&str>) -> wf::WorkflowDefinition {
    wf::WorkflowDefinition {
        id: Uuid::new_v4().to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: name.to_string(),
        trigger: Some(wf::Trigger {
            kind: Some(match cron {
                Some(expr) => wf::trigger::Kind::Cron(expr.to_string()),
                None => wf::trigger::Kind::FireNow(wf::trigger::FireNow {}),
            }),
        }),
        ..Default::default()
    }
}

fn trigger_of(row: &DefinitionListRow) -> Option<&wf::trigger::Kind> {
    row.workflow
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.kind.as_ref())
}

fn definition_repository(pool: &sqlx::PgPool) -> ReadOnlyRepositoryBundle {
    ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone())
}

/// Inserts a workflow row in the same shape the register handler writes —
/// (id, version) PK, name, status enum, definition as JSONB. The firing
/// trigger rides inside `definition`; there is no denormalised column.
async fn insert_workflow(pool: &sqlx::PgPool, workflow: &wf::WorkflowDefinition) {
    let definition = serde_json::to_value(workflow).expect("serialize workflow definition");

    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
        VALUES ($1, $2, 'default', 'wf', $3, 'Ready', 'testhash', 'testcos', $4, '')
        "#,
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(&definition)
    .execute(pool)
    .await
    .expect("insert workflow row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_when_table_has_no_rows() -> Result<(), Box<dyn std::error::Error>> {
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

    let workflows = definition_repository(&pool).list_definitions().await?;
    assert!(workflows.is_empty(), "fresh DB should yield no workflows");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_inserted_rows_rehydrated_through_jsonb() -> Result<(), Box<dyn std::error::Error>>
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

    let w1 = build_workflow("alpha", Some("* * * * *"));
    let w2 = build_workflow("beta", None);
    insert_workflow(&pool, &w1).await;
    insert_workflow(&pool, &w2).await;

    let rows = definition_repository(&pool).list_definitions().await?;
    assert_eq!(rows.len(), 2, "expected two workflow rows");

    // Round-trip: every field that was set must come back intact through the
    // JSONB column as the published proto definition message. The order is
    // `inserted_at DESC`, but with sub-millisecond inserts the relative order
    // can race; assert presence by id instead.
    let by_id: std::collections::HashMap<String, &DefinitionListRow> =
        rows.iter().map(|r| (r.workflow.id.clone(), r)).collect();
    let r1 = by_id.get(&w1.id).expect("w1 missing from rehydrated set");
    assert_eq!(r1.workflow.name, "alpha");
    assert!(matches!(
        trigger_of(r1),
        Some(wf::trigger::Kind::Cron(expr)) if expr == "* * * * *"
    ));
    let r2 = by_id.get(&w2.id).expect("w2 missing from rehydrated set");
    assert_eq!(r2.workflow.name, "beta");
    assert!(matches!(
        trigger_of(r2),
        Some(wf::trigger::Kind::FireNow(_))
    ));

    Ok(())
}

/// Inserts a single `(id, version)` row with an explicit timestamp so the test
/// can prove version reads do not depend on insertion-time ordering.
async fn insert_version(
    pool: &sqlx::PgPool,
    id: Uuid,
    name: &str,
    version: i64,
    status: &str,
    inserted_at: &str,
) {
    let workflow = wf::WorkflowDefinition {
        id: id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: name.to_string(),
        version,
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::FireNow(wf::trigger::FireNow {})),
        }),
        ..Default::default()
    };
    let definition = serde_json::to_value(&workflow).expect("serialize workflow definition");
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, inserted_at, nickel_source)
        VALUES ($1, $2, 'default', 'wf', $3, $4, 'testhash', 'testcos', $5, ($6)::timestamptz, '')
        "#,
    )
    .bind(id)
    .bind(version)
    .bind(name)
    .bind(status)
    .bind(&definition)
    .bind(inserted_at)
    .execute(pool)
    .await
    .expect("insert versioned workflow row");
}

/// A workflow with several `(id, version)` rows collapses to one list row.
/// `build_version` is the highest explicit version, and `live_version` is the
/// highest `Ready`/`Submitted` version, regardless of insertion timestamps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collapses_versions_and_picks_live_and_build_versions(
) -> Result<(), Box<dyn std::error::Error>> {
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

    let id = Uuid::new_v4();
    // Timestamps intentionally disagree with version order. Explicit version
    // remains the stable ordering key for both latest and latest-live rows.
    insert_version(&pool, id, "svc", 1, "Ready", "2026-01-01T00:00:00Z").await;
    insert_version(&pool, id, "svc", 3, "Ready", "2026-01-02T00:00:00Z").await;
    insert_version(&pool, id, "svc", 2, "Building", "2026-01-03T00:00:00Z").await;

    let rows = definition_repository(&pool).list_definitions().await?;
    assert_eq!(rows.len(), 1, "three versions collapse to exactly one row");
    let row = &rows[0];
    assert_eq!(row.build_version, 3, "highest explicit version is current");
    assert_eq!(row.build_status, "Ready", "status belongs to version 3");
    assert_eq!(
        row.live_version,
        Some(3),
        "highest Ready version wins regardless of insertion timestamp"
    );
    Ok(())
}

/// A workflow whose only live row is `Submitted` (built + dispatched) must
/// still report a `version` — it built successfully and is live. Filtering on
/// `status = 'Ready'` alone would wrongly read it as "never built".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submitted_row_yields_a_live_version() -> Result<(), Box<dyn std::error::Error>> {
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

    let id = Uuid::new_v4();
    insert_version(&pool, id, "live", 1, "Submitted", "2026-01-01T00:00:00Z").await;

    let rows = definition_repository(&pool).list_definitions().await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].live_version,
        Some(1),
        "a Submitted workflow is live and must report its version"
    );
    Ok(())
}

async fn insert_instance(pool: &sqlx::PgPool, workflow_id: Uuid, state: &str) {
    sqlx::query(
        r#"
        INSERT INTO workflow_instances (id, workflow_id, name, state, instance)
        VALUES ($1, $2, 'wf', $3, '{}'::jsonb)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(state)
    .execute(pool)
    .await
    .expect("insert instance");
}

/// DC-0014 Completed runs: the count is terminal-only (`Completed` ∪ `Failed`).
/// Non-terminal / future-armed instances are excluded; a workflow with none is
/// absent from the map (the handler defaults it to 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_run_counts_count_terminal_only() -> Result<(), Box<dyn std::error::Error>> {
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

    let terminal_wf = Uuid::new_v4();
    insert_instance(&pool, terminal_wf, "Completed").await;
    insert_instance(&pool, terminal_wf, "Completed").await;
    insert_instance(&pool, terminal_wf, "Failed").await;

    let nonterminal_wf = Uuid::new_v4();
    insert_instance(&pool, nonterminal_wf, "InProgress").await;
    insert_instance(&pool, nonterminal_wf, "Scheduled").await;

    let counts = definition_repository(&pool).completed_run_counts().await?;
    assert_eq!(
        counts.get(&terminal_wf),
        Some(&3),
        "two Completed + one Failed"
    );
    assert_eq!(
        counts.get(&nonterminal_wf),
        None,
        "only non-terminal instances → absent (handler maps to 0)"
    );
    Ok(())
}
