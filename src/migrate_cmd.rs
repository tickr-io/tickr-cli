use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use tickr_migrations::{apply_target, MigrationTarget};

pub async fn run() -> Result<()> {
    let url = tickr_proto::config::conductor_postgres_url();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .context("connecting to configured conductor Postgres")?;
    apply_target(MigrationTarget::Conductor, &pool).await?;
    println!("conductor migrations applied.");
    Ok(())
}
