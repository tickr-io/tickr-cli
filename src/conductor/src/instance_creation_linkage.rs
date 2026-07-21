//! Conductor-side linkage between a wire `Signal::Trigger` and the
//! `WorkflowInstance` the server eventually materializes for it.
//!
//! Runs synchronously off the inbound `TaskQueueItem` relay arm — by the
//! time the conductor publishes a task into NATS for the executor to pull,
//! the captures keyed by `<signal_id>/<name>` must be present so the task
//! can read them via `tickr-ctx get --signal`. This is the integration
//! point that closes the HTTP-receive → schedule-due → task-dispatch loop:
//! captures land in NATS up front during the trigger HTTP handler, the
//! link from signal_id to the eventually-minted workflow_instance_id is
//! recorded here, and the NATS cache is rehydrated from the SQL repository
//! if a prior conductor crash left it incomplete.
//!
//! Idempotent: a second event for the same `signal_id` observes the durable
//! linkage without replacing it and at most repeats the NATS writes
//! (identical bytes).

use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::derive_scheduled_workflow_instance_id;
use uuid::Uuid;

use crate::signal_captures;

/// Per-tenant tickr-ctx bucket namespace. Must match the value the trigger
/// HTTP handler used at HTTP-receive, otherwise rehydration would write to
/// a different bucket than the executor reads from.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Reconcile a workflow-instance-creation event against the conductor's
/// signal_captures archive: record the (signal_id → run_id) linkage, and
/// rehydrate any missing NATS KV keys from the SQL source of truth.
///
/// Called once per `TaskQueueRepoItem` with a `Some(originating_signal_id)`
/// arriving on the conductor inbound relay arm, before the item is
/// forwarded to NATS. The function is cheap when the linkage already
/// exists (one repository linkage operation, one archive read, N NATS GETs) and idempotent on
/// re-invocation.
pub async fn link_and_rehydrate(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    signal_id: Uuid,
    workflow_instance_id: Uuid,
) -> Result<()> {
    // 1. Record the linkage. `mark_materialized` is a no-op when already set;
    //    no need for read-then-write.
    signal_captures::mark_materialized(repositories, signal_id, workflow_instance_id)
        .await
        .context("mark signal_captures materialized")?;

    // 2. Read the archived captures so we know which keys must exist in NATS.
    let row = match signal_captures::read(repositories, signal_id)
        .await
        .context("read signal_captures for rehydration")?
    {
        Some(r) => r,
        None => {
            // No archived captures for this signal_id — most plausibly a
            // cron-fired run with `Manual { signal_id }` shape provenance
            // that came from a non-HTTP path. Skip silently.
            return Ok(());
        }
    };

    if row.captures.is_empty() {
        // The trigger declared no captures; nothing to rehydrate.
        return Ok(());
    }

    // 3. Open the ctx bucket. Get-or-create matches the HTTP handler's path
    //    so a startup-ordering quirk (handler ran first or not at all)
    //    doesn't leave the rehydration loop unable to find the bucket.
    let js = jetstream::new(nats.clone());
    let bucket_name = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
    let kv = match js.get_key_value(&bucket_name).await {
        Ok(kv) => kv,
        Err(_) => js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket_name.clone(),
                history: 1,
                max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                ..Default::default()
            })
            .await
            .context("create ctx bucket during rehydration")?,
    };

    let signal_prefix = sanitize_segment(&signal_id.to_string());

    // 4. For every archived capture, check the NATS cache. If missing,
    //    write the envelope back from the row. A miss only happens when
    //    a conductor restart cut between SQL commit and NATS write at
    //    HTTP-receive time; the common case here is "all keys present"
    //    and the loop is a fast check.
    for cap in &row.captures {
        let key = format!("{}/{}", signal_prefix, cap.name);
        let present = match kv.get(&key).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                return Err(anyhow::anyhow!("nats kv get during rehydration: {}", e));
            }
        };
        if !present {
            let bytes = serde_json::to_vec(&cap.envelope)
                .context("serialize envelope for rehydration write")?;
            kv.put(&key, bytes.into())
                .await
                .map_err(|e| anyhow::anyhow!("nats kv put during rehydration: {}", e))?;
        }
    }

    Ok(())
}

/// Record the (signal_id → run_id) linkage for a future-dated trigger *up
/// front*, before the run fires, so the signals read-path surfaces the
/// scheduled instance's id while it is still pending — an operator needs a
/// target to call the run back before it fires.
///
/// The instance id is deterministic in `(workflow_id, scheduled_at)` — the
/// same seam the server's `WorkflowInstance::new_at` uses — so this records
/// exactly the id the fire-time [`link_and_rehydrate`] back-fill would later
/// record. Idempotent with that path: `mark_materialized`'s `WHERE
/// materialized_run_id IS NULL` guard means whichever runs first wins and the
/// other is a no-op, and both compute the identical id.
///
/// Returns the computed run id.
pub async fn backfill_pending_schedule_linkage(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
    workflow_id: Uuid,
    scheduled_at: chrono::DateTime<chrono::Utc>,
) -> Result<Uuid> {
    let run_id = derive_scheduled_workflow_instance_id(workflow_id, scheduled_at);
    signal_captures::mark_materialized(repositories, signal_id, run_id)
        .await
        .context("back-fill pending-schedule linkage")?;
    Ok(run_id)
}

/// Mirror of `tickr_ctx::scope::sanitize_segment`. Reproduced here so this
/// hot path doesn't take a runtime dependency on the CLI module's privates
/// for one tiny character-class function.
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
    fn sanitize_segment_admits_kv_key_charset() {
        assert_eq!(sanitize_segment("abc-123"), "abc-123");
        assert_eq!(sanitize_segment("uuid_v4=ok.txt"), "uuid_v4=ok.txt");
    }

    #[test]
    fn sanitize_segment_replaces_illegal_characters() {
        assert_eq!(sanitize_segment("hello world"), "hello_world");
        assert_eq!(sanitize_segment("a/b"), "a_b");
    }

    #[test]
    fn sanitize_segment_preserves_uuid_shape() {
        let s = "11111111-2222-3333-4444-555555555555";
        assert_eq!(sanitize_segment(s), s);
    }
}
