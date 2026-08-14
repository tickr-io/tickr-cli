//! Events pull cycle: lands tenant-visible server events in the tenant
//! events projection (the conductor-side `events` table).
//!
//! Each tick derives the next upstream keyset cursor from committed projection
//! rows, fetches one page without holding a SQL transaction or writer lock, and
//! delegates atomic idempotent insertion to the selected repository.
//!
//! Concurrent cycles may fetch the same page. Correctness comes from the
//! repository's duplicate suppression and contiguous public `seq` assignment,
//! not from serializing network work. An empty projection means "pull from the
//! beginning" — the rebuild path is the boot path, with no stored high-water.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::event_repository::EventProjectionInput;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Tick interval — one leg of the Event log page's staleness budget
/// (sweep 5s + watermark 2s + pull 5s + UI poll 5s ≈ 17s worst case).
pub const PULL_INTERVAL: Duration = Duration::from_secs(5);

/// Hard client budget for the Control-plane HTTP call.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(3);

/// Batch cap requested per pull. The serve side caps at 1000; its default keeps
/// one transactionally inserted page bounded.
const PULL_BATCH_LIMIT: u32 = 500;

/// One served event row as the Control plane's JSON encodes it. Field names
/// match the Control plane's `EventResponse` / the archive's column names.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct PulledEvent {
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub archived_at: DateTime<Utc>,
}

/// What one tick did — surfaced for logs and tests.
#[derive(Debug, PartialEq, Eq)]
pub struct PullOutcome {
    pub fetched: usize,
    pub inserted: u64,
}

/// Run the pull cycle until shutdown. Spawned once per conductor replica;
/// replica timers are unsynchronized, which may produce a denser-than-5s
/// effective cadence across the fleet — harmless (smaller batches).
pub async fn run_events_pull(
    repositories: Arc<WriterRepositoryBundle>,
    control_plane_http_url: String,
    tenant: Uuid,
    shutdown: CancellationToken,
) {
    let client = reqwest::Client::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                println!("events pull: shutdown signal received, stopping.");
                return;
            }
            _ = tokio::time::sleep(PULL_INTERVAL) => {
                match pull_once(&repositories, &client, &control_plane_http_url, tenant).await {
                    Ok(PullOutcome { fetched, inserted }) => {
                        if fetched > 0 {
                            tracing::debug!(
                                "events pull: fetched {} row(s), {} new",
                                fetched,
                                inserted
                            );
                        }
                    }
                    // A failed fetch writes nothing; a failed insertion rolls
                    // back the complete page. The next tick re-derives its
                    // position from committed rows.
                    Err(e) => eprintln!("events pull cycle error: {}", e),
                }
            }
        }
    }
}

/// One Pull cycle. Cursor derivation and the control-plane fetch happen before
/// the repository opens its insertion transaction.
pub async fn pull_once(
    repositories: &WriterRepositoryBundle,
    client: &reqwest::Client,
    control_plane_http_url: &str,
    tenant: Uuid,
) -> anyhow::Result<PullOutcome> {
    let cursor = repositories
        .event_archive_cursor()
        .await?
        .map(|cursor| (cursor.archived_at, cursor.id));
    let batch = fetch_batch(client, control_plane_http_url, tenant, cursor).await?;
    let fetched = batch.len();
    let page = batch
        .into_iter()
        .map(|event| EventProjectionInput {
            id: event.id,
            ts: event.ts,
            event_type: event.event_type,
            payload: event.payload,
            archived_at: event.archived_at,
        })
        .collect::<Vec<_>>();
    let inserted = repositories.insert_event_page(&page).await?;
    Ok(PullOutcome { fetched, inserted })
}

/// `GET {control_plane_http_url}/api/internal/events` with the keyset cursor (absent
/// on first pull / after a rebuild). Non-2xx is an error, never an empty
/// batch — "no new events" and "serve path down" must stay distinguishable.
async fn fetch_batch(
    client: &reqwest::Client,
    control_plane_http_url: &str,
    tenant: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
) -> anyhow::Result<Vec<PulledEvent>> {
    let url = format!(
        "{}/api/internal/events",
        control_plane_http_url.trim_end_matches('/')
    );
    // Scope the pull to this conductor's own tenant — the archive is a shared
    // multi-tenant table, so the projection must receive only its tenant's slice.
    let mut request = client.get(&url).timeout(CONTROL_PLANE_TIMEOUT).query(&[
        ("tenant", tenant.to_string()),
        ("limit", PULL_BATCH_LIMIT.to_string()),
    ]);
    if let Some((archived_at, id)) = cursor {
        request = request.query(&[
            ("after_archived_at", archived_at.to_rfc3339()),
            ("after_id", id.to_string()),
        ]);
    }
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        // The response body crosses from the control plane and may contain
        // internal diagnostics or secrets; retain only the status for logs.
        anyhow::bail!("events serve path returned status {}", status);
    }
    Ok(response.json::<Vec<PulledEvent>>().await?)
}
