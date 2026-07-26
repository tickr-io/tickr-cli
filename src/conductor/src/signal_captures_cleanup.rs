//! Terminal-state cleanup + grace-window sweep for `signal_captures`.
//!
//! Two entry points:
//!
//! - `on_workflow_terminal` runs as a hook off the compaction path. When a
//!   `WorkflowInstance` reaches a terminal state, every linked
//!   `signal_captures` row is flipped to `terminal_at = now()` and the
//!   matching NATS `<signal_id>/<name>` keys are deleted. The terminal flag
//!   keeps the audit trail visible briefly after run termination; the NATS
//!   deletes reclaim the working-set storage immediately.
//!
//! - `sweep_expired` runs periodically (configurable cadence, default 1h)
//!   and deletes `signal_captures` rows whose `terminal_at` has aged past
//!   the configurable grace window (default 24h). Idempotent: a row whose
//!   NATS deletes failed on the first pass is re-tried by re-issuing the
//!   deletes here, then the settled repository row goes.

use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use std::time::Duration;
use tickr_migrations::backend::WriterRepositoryBundle;
use uuid::Uuid;

/// Per-tenant tickr-ctx bucket namespace. Mirrors the HTTP handler's value
/// so deletes target the same bucket the writes landed in.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Default time a terminal row remains queryable before the repository sweep
/// deletes it. Keeps the audit trail visible long enough for post-mortem inspection
/// without indefinite storage growth.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

/// Default cadence for the sweep task.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Terminal-state cleanup hook. Runs alongside the existing compaction
/// archive write. For every `signal_captures` row whose
/// `materialized_run_id` matches the terminating instance: sets
/// `terminal_at = now()` and issues NATS KV deletes for each
/// `<signal_id>/<name>` key in the row's captures.
///
/// Returns the list of `signal_id`s the hook touched so callers can log /
/// instrument; an empty list is a valid outcome (cron-fired runs and runs
/// with no captures both produce zero linked rows).
pub async fn on_workflow_terminal(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    workflow_instance_id: Uuid,
) -> Result<Vec<Uuid>> {
    let rows = repositories
        .mark_signal_captures_terminal(workflow_instance_id)
        .await
        .context("settle Trigger-derived Event variables for terminal run")?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let js = jetstream::new(nats.clone());
    let bucket_name = tickr_ctx::scope::bucket_for_namespace(DEFAULT_CTX_NAMESPACE);
    let kv_opt = js.get_key_value(&bucket_name).await.ok();

    let mut touched = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(kv) = kv_opt.as_ref() {
            let signal_prefix = sanitize_segment(&row.signal_id.to_string());
            for name in row.capture_names() {
                let key = format!("{}/{}", signal_prefix, name);
                if let Err(e) = kv.delete(&key).await {
                    eprintln!(
                        "signal_captures_cleanup: NATS delete failed for {}/{}: {} (sweep will retry)",
                        signal_prefix, name, e
                    );
                }
            }
        }
        touched.push(row.signal_id);
    }

    Ok(touched)
}

/// Grace-window sweep. Deletes `signal_captures` rows whose `terminal_at`
/// is older than `grace`. Re-issues NATS deletes for any keys that didn't
/// land on the first pass (the row carries enough state to know the keys).
/// Returns the count of rows deleted from the repository so a caller can emit
/// telemetry.
pub async fn sweep_expired(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    grace: chrono::Duration,
) -> Result<usize> {
    let cutoff = chrono::Utc::now() - grace;
    let expired = repositories
        .expired_signal_captures(cutoff)
        .await
        .context("list expired Trigger-derived Event variables")?;

    if expired.is_empty() {
        return Ok(0);
    }

    let js = jetstream::new(nats.clone());
    let bucket_name = tickr_ctx::scope::bucket_for_namespace(DEFAULT_CTX_NAMESPACE);
    let kv_opt = js.get_key_value(&bucket_name).await.ok();

    let mut deleted = 0usize;
    for row in expired {
        if let Some(kv) = kv_opt.as_ref() {
            let signal_prefix = sanitize_segment(&row.signal_id.to_string());
            for name in row.capture_names() {
                let key = format!("{}/{}", signal_prefix, name);
                let _ = kv.delete(&key).await;
            }
        }
        if repositories
            .delete_expired_signal_captures(row.signal_id, cutoff)
            .await
            .context("delete expired Trigger-derived Event variables")?
        {
            deleted += 1;
        }
    }

    Ok(deleted)
}

/// Spawn the periodic sweep as a long-running task. Honors `interval` for
/// cadence and `grace` for the per-row age cutoff. The sweep continues
/// until `shutdown` flips to `true`.
pub fn spawn_periodic_sweep(
    repositories: std::sync::Arc<WriterRepositoryBundle>,
    nats: NatsClient,
    interval: Duration,
    grace: chrono::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it so the sweep doesn't run
        // during the conductor's startup race with bucket creation.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match sweep_expired(repositories.as_ref(), &nats, grace).await {
                        Ok(0) => {} // idle
                        Ok(n) => println!("signal_captures sweep: deleted {} expired rows", n),
                        Err(e) => eprintln!("signal_captures sweep: error: {}", e),
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

/// Mirror of `tickr_ctx::scope::sanitize_segment`. Inline rather than
/// dependency-injected for the same reason it lives inline in
/// `instance_creation_linkage` — one tiny character-class function.
fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '=' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_grace_is_24_hours() {
        assert_eq!(DEFAULT_GRACE, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn default_sweep_interval_is_one_hour() {
        assert_eq!(DEFAULT_SWEEP_INTERVAL, Duration::from_secs(60 * 60));
    }

    #[test]
    fn sanitize_segment_admits_uuid_shape() {
        let s = "11111111-2222-3333-4444-555555555555";
        assert_eq!(sanitize_segment(s), s);
    }

    #[test]
    fn sanitize_segment_replaces_illegal_chars() {
        assert_eq!(sanitize_segment("a/b c"), "a_b_c");
    }
}
