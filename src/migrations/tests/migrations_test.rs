#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_migrations::{apply_target, verify_current, MigrationTarget};

#[tokio::test]
async fn conductor_migrations_apply_and_are_idempotent() {
    let container = Postgres::default()
        .start()
        .await
        .expect("start PostgreSQL test container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("resolve PostgreSQL test port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .connect(&url)
        .await
        .expect("connect to PostgreSQL test container");
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_current(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
}
