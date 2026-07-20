//! Conductor-only Postgres migration runner.

use std::collections::HashMap;

use sqlx::migrate::Migrator;
use sqlx::PgPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationTarget {
    Conductor,
}

impl MigrationTarget {
    fn migrator(self) -> &'static Migrator {
        static CONDUCTOR: Migrator = sqlx::migrate!("../conductor/migrations");
        &CONDUCTOR
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
#[error("failed to apply {target} migrations: {source}")]
pub struct MigrationError {
    pub target: MigrationTarget,
    #[source]
    pub source: sqlx::migrate::MigrateError,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationsDriftError {
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
    #[error("failed to read applied migrations for {target}: {source}")]
    Query {
        target: MigrationTarget,
        #[source]
        source: sqlx::Error,
    },
}

pub async fn apply_target(target: MigrationTarget, pool: &PgPool) -> Result<(), MigrationError> {
    target
        .migrator()
        .run(pool)
        .await
        .map_err(|source| MigrationError { target, source })
}

pub async fn verify_current(
    target: MigrationTarget,
    pool: &PgPool,
) -> Result<(), MigrationsDriftError> {
    let embedded: Vec<_> = target
        .migrator()
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .collect();
    let applied = applied_checksums(target, pool).await?;

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
