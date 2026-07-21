//! SQL-backed Cancel audit adapter.

use anyhow::{Context, Result};
use tickr_migrations::backend::WriterRepositoryBundle;
pub use tickr_migrations::signal_repository::SignalCancelInput as SignalCancelRow;

pub async fn insert(repositories: &WriterRepositoryBundle, row: &SignalCancelRow) -> Result<()> {
    repositories
        .insert_signal_cancel(row)
        .await
        .context("insert Signal Cancel audit")?;
    Ok(())
}
