//! Lifecycle wiring around the `SubscriptionIndex`. The pure index
//! lives in `subscription_index`; this module owns the process-wide
//! singleton instance and the integration with the selected repository +
//! registration / build-completion call sites.
//!
//! The index is in-memory only; the authoritative source is the
//! conductor's `workflows` table. Three call sites drive it:
//!
//! - `apply_workflow_state(workflow)` — invoked when the build pipeline's
//!   finalizer flips a workflow to `Ready`. Looks at the workflow's
//!   `trigger_on` config and either registers or unregisters.
//! - `rebuild_from_repository(repositories)` — invoked on conductor startup.
//!   Reads the latest live definition rows and repopulates the index.
//! - `signal_subscription_index()` — process-wide accessor that the
//!   HTTP / NATS translators reach for to read the index hot-path.

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::workflow as wf;

use crate::captures_merge;
use crate::subscription_index::SubscriptionIndex;

static SUBSCRIPTION_INDEX: Lazy<SubscriptionIndex> = Lazy::new(SubscriptionIndex::new);

/// Shared handle to the process-wide subscription index. The wakeup
/// translator reaches for this when resolving `name → subscribers`.
pub fn signal_subscription_index() -> SubscriptionIndex {
    SUBSCRIPTION_INDEX.clone()
}

/// Project the workflow's current state onto the index. Idempotent:
/// safe to call from the build-completion path even when the workflow
/// doesn't subscribe to any signal. Callers are responsible for only
/// invoking this on workflows whose conductor-side lifecycle has
/// reached `Ready` (or beyond) — the function does not gate on PG
/// status itself.
///
/// Rules:
/// - `waits_on_signal = Some(...)`: register (or replace if the workflow
///   re-registered with a different config).
/// - `waits_on_signal = None`: unregister.
pub fn apply_workflow_state(workflow: &wf::WorkflowDefinition) -> Result<()> {
    let Some(wf::trigger::Kind::WaitsOnSignal(cfg)) = workflow
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.kind.as_ref())
    else {
        SUBSCRIPTION_INDEX.unregister(uuid::Uuid::parse_str(&workflow.id)?);
        return Ok(());
    };
    let merged = captures_merge::merge(&workflow.captures, &cfg.captures);
    SUBSCRIPTION_INDEX
        .register(
            uuid::Uuid::parse_str(&workflow.id)?,
            &cfg.signal_name,
            cfg.predicate.as_deref(),
            merged,
        )
        .map_err(|e| anyhow!("subscription index register: {}", e))
}

/// Rebuild the index from scratch by scanning the conductor's
/// `workflows` table. Called on conductor startup so a restart picks
/// up every ready waits-on-signal subscriber without waiting for the
/// next registration event.
///
/// Errors on the individual workflow row are logged and skipped — a
/// single malformed row shouldn't block the rest of the index from
/// rebuilding. (A row with a malformed predicate is itself a bug in
/// the conductor's earlier registration validation, so this path is
/// belt-and-suspenders against state drift.)
pub async fn rebuild_from_repository(repositories: &WriterRepositoryBundle) -> Result<usize> {
    let workflows = repositories
        .live_workflow_definitions()
        .await
        .map_err(anyhow::Error::new)?;

    let mut registered = 0usize;
    for workflow in workflows {
        if !matches!(
            workflow
                .trigger
                .as_ref()
                .and_then(|trigger| trigger.kind.as_ref()),
            Some(wf::trigger::Kind::WaitsOnSignal(_))
        ) {
            continue;
        }
        if let Err(e) = apply_workflow_state(&workflow) {
            eprintln!(
                "subscription_index rebuild: failed to register workflow {}: {}",
                workflow.id, e
            );
            continue;
        }
        registered += 1;
    }
    Ok(registered)
}
