#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn migrate_subcommand_applies_conductor_schema_only() {
    let Ok(container) = Postgres::default().start().await else {
        return;
    };
    let Ok(port) = container.get_host_port_ipv4(5432).await else {
        return;
    };
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_tickr"))
        .arg("migrate")
        .env("TICKR_CONDUCTOR_POSTGRES_URL", &url)
        .output()
        .expect("invoke `tickr migrate`");
    assert!(
        output.status.success(),
        "tickr migrate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let pool = PgPoolOptions::new().connect(&url).await.unwrap();
    let present: bool =
        sqlx::query("SELECT to_regclass('workflow_instances') IS NOT NULL AS present")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("present");
    assert!(present);
}
