//! Paired Postgres and SQLite data-plane migration runner.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{PgPool, SqlitePool};

pub mod archive_repository;
pub mod backend;
pub mod compaction_repository;
pub mod definition_repository;
pub mod encoding;
pub mod event_repository;
pub mod patch_repository;
pub mod replay_repository;
mod schema;
pub mod scope_repository;
pub mod signal_repository;
pub mod task_pickup_repository;

pub use schema::{verify_postgres_schema, verify_sqlite_schema, SchemaVerificationError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationTarget {
    Conductor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalMigrationIdentity {
    pub version: i64,
    pub name: &'static str,
}

pub const LOGICAL_MIGRATIONS: &[LogicalMigrationIdentity] = &[
    LogicalMigrationIdentity {
        version: 1,
        name: "current_conductor_schema",
    },
    LogicalMigrationIdentity {
        version: 2,
        name: "definition_build_leases",
    },
    LogicalMigrationIdentity {
        version: 3,
        name: "definition_submission_leases",
    },
    LogicalMigrationIdentity {
        version: 4,
        name: "patch_lifecycle_leases",
    },
    LogicalMigrationIdentity {
        version: 5,
        name: "replay_lifecycle_leases",
    },
    LogicalMigrationIdentity {
        version: 6,
        name: "local_task_pickups",
    },
    LogicalMigrationIdentity {
        version: 7,
        name: "local_task_terminal_outcomes",
    },
    LogicalMigrationIdentity {
        version: 8,
        name: "local_task_cancellation_fences",
    },
    LogicalMigrationIdentity {
        version: 9,
        name: "tickr_ctx_scope_store",
    },
    LogicalMigrationIdentity {
        version: 10,
        name: "local_compaction_staging",
    },
    LogicalMigrationIdentity {
        version: 11,
        name: "workflow_calendar_replay_indexes",
    },
];

struct PairedMigrationSet {
    postgres: Migrator,
    sqlite: Migrator,
}

fn paired_migrations() -> &'static PairedMigrationSet {
    static MIGRATIONS: PairedMigrationSet = PairedMigrationSet {
        postgres: sqlx::migrate!("../conductor/migrations"),
        sqlite: sqlx::migrate!("./sqlite"),
    };
    &MIGRATIONS
}

impl MigrationTarget {
    fn postgres_migrator(self) -> &'static Migrator {
        &paired_migrations().postgres
    }

    fn cli_name(self) -> &'static str {
        "conductor"
    }
}

impl std::fmt::Display for MigrationTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.cli_name())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(transparent)]
    Registration(#[from] MigrationRegistrationError),
    #[error("failed to apply {target} {backend} migrations: {source}")]
    Apply {
        target: MigrationTarget,
        backend: &'static str,
        #[source]
        source: sqlx::migrate::MigrateError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationsDriftError {
    #[error(transparent)]
    Registration(#[from] MigrationRegistrationError),
    #[error("{target} schema is behind: {have} of {want} migrations applied; next pending is {next_version} ({next_description}). Run `just migrate` (or `tickr migrate`) before starting this component.")]
    Behind {
        target: MigrationTarget,
        have: usize,
        want: usize,
        next_version: i64,
        next_description: String,
        cli_name: &'static str,
    },
    #[error("{target} migration {version} ({description}) checksum mismatch")]
    ChecksumMismatch {
        target: MigrationTarget,
        version: i64,
        description: String,
    },
    #[error("{target} has unregistered migration identity {version}")]
    UnexpectedMigration {
        target: MigrationTarget,
        version: i64,
    },
    #[error("failed to read applied migrations for {target}: {source}")]
    Query {
        target: MigrationTarget,
        #[source]
        source: sqlx::Error,
    },
}

pub async fn apply_target(target: MigrationTarget, pool: &PgPool) -> Result<(), MigrationError> {
    validate_migration_registration()?;
    target
        .postgres_migrator()
        .run(pool)
        .await
        .map_err(|source| MigrationError::Apply {
            target,
            backend: "postgres",
            source,
        })
}

pub async fn apply_sqlite(
    target: MigrationTarget,
    pool: &SqlitePool,
) -> Result<(), MigrationError> {
    validate_migration_registration()?;
    paired_migrations()
        .sqlite
        .run(pool)
        .await
        .map_err(|source| MigrationError::Apply {
            target,
            backend: "sqlite",
            source,
        })
}

pub fn sqlite_writer_options(
    url: &str,
    create_if_missing: bool,
) -> Result<SqliteConnectOptions, sqlx::Error> {
    Ok(SqliteConnectOptions::from_str(url)?
        .create_if_missing(create_if_missing)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5)))
}

pub async fn verify_current(
    target: MigrationTarget,
    pool: &PgPool,
) -> Result<(), MigrationsDriftError> {
    validate_migration_registration()?;
    let applied = applied_checksums(target, pool).await?;
    verify_applied(target, target.postgres_migrator(), &applied)
}

pub async fn verify_sqlite_current(
    target: MigrationTarget,
    pool: &SqlitePool,
) -> Result<(), MigrationsDriftError> {
    validate_migration_registration()?;
    let applied = applied_sqlite_checksums(target, pool).await?;
    verify_applied(target, &paired_migrations().sqlite, &applied)
}

fn verify_applied(
    target: MigrationTarget,
    migrator: &Migrator,
    applied: &HashMap<i64, Vec<u8>>,
) -> Result<(), MigrationsDriftError> {
    let embedded = migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .collect::<Vec<_>>();
    for version in applied.keys() {
        if !embedded
            .iter()
            .any(|migration| migration.version == *version)
        {
            return Err(MigrationsDriftError::UnexpectedMigration {
                target,
                version: *version,
            });
        }
    }
    for migration in &embedded {
        if let Some(recorded) = applied.get(&migration.version) {
            if recorded.as_slice() != migration.checksum.as_ref() {
                return Err(MigrationsDriftError::ChecksumMismatch {
                    target,
                    version: migration.version,
                    description: migration.description.to_string(),
                });
            }
        }
    }
    if applied.len() < embedded.len() {
        let next = embedded
            .iter()
            .find(|migration| !applied.contains_key(&migration.version))
            .expect("an unapplied embedded migration exists");
        return Err(MigrationsDriftError::Behind {
            target,
            have: applied.len(),
            want: embedded.len(),
            next_version: next.version,
            next_description: next.description.to_string(),
            cli_name: target.cli_name(),
        });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error(
    "logical migration registration must pair versions {expected:?}; postgres has {postgres:?}, sqlite has {sqlite:?}"
)]
pub struct MigrationRegistrationError {
    expected: Vec<i64>,
    postgres: Vec<i64>,
    sqlite: Vec<i64>,
}

pub fn validate_migration_registration() -> Result<(), MigrationRegistrationError> {
    let expected = LOGICAL_MIGRATIONS
        .iter()
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    let postgres = migration_versions(&paired_migrations().postgres);
    let sqlite = migration_versions(&paired_migrations().sqlite);
    if postgres == expected && sqlite == expected {
        Ok(())
    } else {
        Err(MigrationRegistrationError {
            expected,
            postgres,
            sqlite,
        })
    }
}

fn migration_versions(migrator: &Migrator) -> Vec<i64> {
    migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| migration.version)
        .collect()
}

async fn applied_checksums(
    target: MigrationTarget,
    pool: &PgPool,
) -> Result<HashMap<i64, Vec<u8>>, MigrationsDriftError> {
    let rows: Vec<(i64, Vec<u8>)> =
        match sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_all(pool)
            .await
        {
            Ok(rows) => rows,
            Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42P01") => Vec::new(),
            Err(source) => return Err(MigrationsDriftError::Query { target, source }),
        };
    Ok(rows.into_iter().collect())
}

async fn applied_sqlite_checksums(
    target: MigrationTarget,
    pool: &SqlitePool,
) -> Result<HashMap<i64, Vec<u8>>, MigrationsDriftError> {
    let rows: Vec<(i64, Vec<u8>)> =
        match sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_all(pool)
            .await
        {
            Ok(rows) => rows,
            Err(sqlx::Error::Database(db)) if db.message().contains("no such table") => Vec::new(),
            Err(source) => return Err(MigrationsDriftError::Query { target, source }),
        };
    Ok(rows.into_iter().collect())
}
