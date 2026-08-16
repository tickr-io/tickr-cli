#![cfg(not(madsim))]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Row;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr::data_directory::DataDirectory;
use tickr_migrations::{
    sqlite_writer_options, verify_sqlite_current, verify_sqlite_schema, MigrationTarget,
};

fn migrate_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
    command
        .arg("migrate")
        .env_remove("TICKR_SQL_BACKEND")
        .env_remove("TICKR_SQL_TOPOLOGY")
        .env_remove("TICKR_CONDUCTOR_SQLITE_URL")
        .env_remove("TICKR_CONDUCTOR_POSTGRES_URL");
    command
}

fn run_sqlite_migration(path: &Path) -> Output {
    migrate_command()
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env(
            "TICKR_CONDUCTOR_SQLITE_URL",
            format!("sqlite://{}", path.display()),
        )
        .output()
        .expect("invoke `tickr-cli migrate` for SQLite")
}

fn run_tickr_lite_migration(path: &Path) -> Output {
    let mut command = migrate_command();
    command
        .args(["--formation", "tickr-lite"])
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env(
            "TICKR_CONDUCTOR_SQLITE_URL",
            format!("sqlite://{}", path.display()),
        )
        .output()
        .expect("invoke Tickr Lite offline migration")
}

#[tokio::test]
async fn migrate_subcommand_keeps_unset_backend_on_postgres() {
    let Ok(container) = Postgres::default().start().await else {
        return;
    };
    let Ok(port) = container.get_host_port_ipv4(5432).await else {
        return;
    };
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let output = migrate_command()
        .env("TICKR_CONDUCTOR_POSTGRES_URL", &url)
        .output()
        .expect("invoke `tickr-cli migrate`");
    assert!(
        output.status.success(),
        "tickr-cli migrate failed: {}",
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

#[test]
fn sqlite_configuration_is_rejected_before_a_file_is_created() {
    let directory = tempfile::tempdir().unwrap();
    for topology in [None, Some("distributed")] {
        let path = directory
            .path()
            .join(topology.unwrap_or("missing-topology").to_owned() + ".db");
        let mut command = migrate_command();
        command.env("TICKR_SQL_BACKEND", "sqlite").env(
            "TICKR_CONDUCTOR_SQLITE_URL",
            format!("sqlite://{}", path.display()),
        );
        if let Some(topology) = topology {
            command.env("TICKR_SQL_TOPOLOGY", topology);
        }
        let output = command.output().expect("invoke invalid SQLite migration");
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("TICKR_SQL_TOPOLOGY"),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !path.exists(),
            "invalid selection created {}",
            path.display()
        );
    }

    let output = migrate_command()
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .output()
        .expect("invoke SQLite migration without URL");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("TICKR_CONDUCTOR_SQLITE_URL"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn sqlite_migration_contends_on_the_runtime_data_directory_lock() {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("tickr.db");
    let runtime_lease = DataDirectory::admit(directory.path()).unwrap();
    let output = run_sqlite_migration(&path);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("already exclusively locked"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !path.exists(),
        "lock contender created SQLite state before admission"
    );
    drop(runtime_lease);
    assert!(run_sqlite_migration(&path).status.success());
}

#[tokio::test]
async fn sqlite_migrate_subcommand_is_repeatable_and_reopenable() {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("tickr.db");
    for _ in 0..2 {
        let output = run_sqlite_migration(&path);
        assert!(
            output.status.success(),
            "tickr-cli migrate failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlite_writer_options(&format!("sqlite://{}", path.display()), false).unwrap(),
        )
        .await
        .unwrap();
    verify_sqlite_current(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_sqlite_schema(&pool).await.unwrap();
    pool.close().await;
}

#[test]
fn tickr_lite_offline_migration_installs_and_verifies_the_manifest() {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("tickr.db");
    for _ in 0..2 {
        let output = run_tickr_lite_migration(&path);
        assert!(
            output.status.success(),
            "Tickr Lite migration failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let manifest = directory.path().join("formation-manifest.json");
    assert!(manifest.is_file());
    assert_eq!(
        fs::metadata(manifest).unwrap().permissions().mode() & 0o777,
        0o600
    );
}
