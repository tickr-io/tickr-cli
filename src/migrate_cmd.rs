use std::collections::BTreeMap;
use std::path::Path;

use crate::data_directory::{sqlite_path_from_url, DataDirectory, FormationPath, RootRelativePath};
use crate::formation::{resolve_formation, FormationSelection};
use crate::formation_manifest::{
    install_or_verify_formation_manifest, FormationManifestSpec, ManifestAdmission,
    SqlMigrationSetIdentity,
};
use anyhow::{bail, Context, Result};
use clap::ValueEnum;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tickr_migrations::{
    apply_sqlite, apply_target, sqlite_writer_options, verify_postgres_schema,
    verify_sqlite_current, verify_sqlite_schema, MigrationTarget, LOGICAL_MIGRATIONS,
};
use tickr_proto::config::DataPlaneSql;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum MigrationFormation {
    #[default]
    Distributed,
    TickrLite,
}

pub async fn run(formation: MigrationFormation) -> Result<()> {
    let selection =
        tickr_proto::config::data_plane_sql().context("resolving data-plane SQL configuration")?;
    if formation == MigrationFormation::TickrLite
        && matches!(&selection, DataPlaneSql::Postgres { .. })
    {
        bail!("Tickr Lite requires SQLite before migration can inspect or mutate durable state");
    }
    match selection {
        DataPlaneSql::Postgres { url } => {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&url)
                .await
                .context("connecting to configured conductor Postgres")?;
            apply_target(MigrationTarget::Conductor, &pool).await?;
            tickr_migrations::verify_current(MigrationTarget::Conductor, &pool).await?;
            verify_postgres_schema(&pool).await?;
            pool.close().await;
            println!("conductor postgres migrations applied and verified.");
        }
        DataPlaneSql::Sqlite { url } => {
            let sqlite_path = sqlite_path_from_url(&url)
                .context("resolving SQLite path beneath data directory")?;
            let root_path = sqlite_path.parent().ok_or_else(|| {
                anyhow::anyhow!("configured SQLite path has no data-directory parent")
            })?;
            let sqlite_name = sqlite_path.file_name().ok_or_else(|| {
                anyhow::anyhow!("configured SQLite path has no database file name")
            })?;
            let data_directory = DataDirectory::admit(root_path)
                .context("admitting and locking Tickr Lite data directory")?;
            let sqlite_relative = RootRelativePath::new(Path::new(sqlite_name))
                .context("validating root-relative SQLite path")?;
            let sqlite_file = data_directory
                .open_or_create_file(&sqlite_relative)
                .context("opening SQLite state beneath locked data directory")?;
            drop(sqlite_file);
            let options = sqlite_writer_options(&url, false)
                .context("parsing configured conductor SQLite URL")?;
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .context("connecting to configured conductor SQLite")?;
            apply_sqlite(MigrationTarget::Conductor, &pool).await?;
            verify_sqlite_current(MigrationTarget::Conductor, &pool).await?;
            verify_sqlite_schema(&pool).await?;
            if formation == MigrationFormation::TickrLite {
                let descriptor = resolve_formation(&FormationSelection::tickr_lite())
                    .context("resolving the Tickr Lite formation")?;
                let spec = tickr_lite_manifest_spec(&descriptor, &url, &sqlite_relative)?;
                install_or_verify_formation_manifest(
                    &data_directory,
                    &spec,
                    ManifestAdmission::OfflineMigration,
                )
                .context("installing or verifying the Tickr Lite formation manifest")?;
            }
            pool.close().await;
            println!("conductor sqlite migrations applied and verified.");
        }
    }
    Ok(())
}

pub(crate) fn tickr_lite_manifest_spec(
    descriptor: &crate::formation::ResolvedFormationDescriptor,
    sqlite_url: &str,
    sqlite_file: &RootRelativePath,
) -> Result<FormationManifestSpec> {
    let latest = LOGICAL_MIGRATIONS
        .last()
        .context("the SQLite migration set has no logical identity")?;
    let format_version = u16::try_from(latest.version)
        .context("the SQLite logical migration version exceeds the manifest format")?;
    let sqlite_file_name = sqlite_file
        .as_path()
        .to_str()
        .context("the SQLite filename is not valid UTF-8")?;

    let mut configuration = BTreeMap::from([
        ("data-plane.sql.backend".to_owned(), "sqlite".to_owned()),
        (
            "data-plane.sql.busy-timeout-ms".to_owned(),
            "5000".to_owned(),
        ),
        (
            "data-plane.sql.file".to_owned(),
            sqlite_file_name.to_owned(),
        ),
        ("data-plane.sql.foreign-keys".to_owned(), "true".to_owned()),
        ("data-plane.sql.journal-mode".to_owned(), "wal".to_owned()),
        (
            "data-plane.sql.writer-connections".to_owned(),
            "1".to_owned(),
        ),
    ]);
    let mut query = sqlite_url.split_once('?').map_or(Vec::new(), |(_, query)| {
        query
            .split('&')
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
    });
    query.sort();
    configuration.insert(
        "data-plane.sql.query".to_owned(),
        if query.is_empty() {
            "none".to_owned()
        } else {
            query.join("&")
        },
    );

    let namespaces = BTreeMap::from([
        (
            "formation-manifest".to_owned(),
            FormationPath::FormationManifest
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "journals".to_owned(),
            FormationPath::Journals
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "logs-final".to_owned(),
            FormationPath::FinalLogs
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "logs-staged".to_owned(),
            FormationPath::StagedLogs
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "quarantine".to_owned(),
            FormationPath::Quarantine
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
        ("sqlite-state".to_owned(), sqlite_file_name.to_owned()),
        (
            "temporary-files".to_owned(),
            FormationPath::TemporaryFiles
                .relative_path()
                .to_string_lossy()
                .into_owned(),
        ),
    ]);

    FormationManifestSpec::new(
        descriptor,
        SqlMigrationSetIdentity::new(
            descriptor.sql_migration_identity.name,
            descriptor.sql_migration_identity.version,
            latest.name,
            latest.version,
        ),
        configuration,
        BTreeMap::from([
            ("formation-manifest".to_owned(), 1),
            ("sqlite-schema".to_owned(), format_version),
        ]),
        namespaces,
        vec![sqlite_file.clone()],
    )
    .context("normalizing the Tickr Lite formation manifest")
}
