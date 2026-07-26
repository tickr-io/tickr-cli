//! Durable Cancel Signal state backed by the selected SQL implementation.

use anyhow::{Context, Result};
use tickr_migrations::backend::WriterRepositoryBundle;
pub use tickr_migrations::signal_repository::SignalCancelInput as SignalCancelRow;
use tickr_migrations::signal_repository::PENDING_SIGNAL_CANCEL_APPLIED_COUNT;

pub async fn insert(repositories: &WriterRepositoryBundle, row: &SignalCancelRow) -> Result<()> {
    repositories
        .insert_signal_cancel(row)
        .await
        .context("insert Signal Cancel audit")?;
    Ok(())
}

pub async fn stage_pending(
    repositories: &WriterRepositoryBundle,
    row: &SignalCancelRow,
) -> Result<()> {
    anyhow::ensure!(
        row.applied_count == PENDING_SIGNAL_CANCEL_APPLIED_COUNT,
        "pending Signal Cancel has invalid applied count"
    );
    let inserted = repositories
        .insert_signal_cancel(row)
        .await
        .context("stage pending Signal Cancel")?;
    anyhow::ensure!(inserted, "Signal Cancel identity already exists");
    Ok(())
}

pub async fn materialize(
    repositories: &WriterRepositoryBundle,
    signal_id: uuid::Uuid,
    matched_count: u32,
) -> Result<bool> {
    let applied_count = i32::try_from(matched_count)
        .context("Signal Cancel matched count exceeds storage range")?;
    repositories
        .materialize_signal_cancel(signal_id, applied_count)
        .await
        .context("materialize Signal Cancel")
}

pub async fn materialized_count(
    repositories: &WriterRepositoryBundle,
    signal_id: uuid::Uuid,
) -> Result<Option<u32>> {
    let Some(record) = repositories
        .signal_cancel(signal_id)
        .await
        .context("read Signal Cancel")?
    else {
        return Ok(None);
    };
    if record.applied_count == PENDING_SIGNAL_CANCEL_APPLIED_COUNT {
        return Ok(None);
    }
    Ok(Some(u32::try_from(record.applied_count).context(
        "materialized Signal Cancel has invalid applied count",
    )?))
}
