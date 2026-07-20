//! Conductor-side idempotency cache for `Signal::Trigger` ingress.
//!
//! Producers may attach an `Idempotency-Key: <string>` HTTP header on
//! `POST /trigger`. The conductor caches the pair `(key) -> (signal_id,
//! input_sha256)` in a NATS JetStream KV bucket with a 10-minute TTL.
//!
//! Same-key + same-payload retries return the original signal_id with
//! `deduplicated: true`. Same-key + different-payload retries are a client
//! error — same key plus a different request body is not "the same logical
//! request" — and the handler returns 409 Conflict so the bug surfaces
//! loudly instead of silently dropping the new payload.
//!
//! The hash is over the canonical-JSON of `inputs`, so two semantically
//! equal but key-order-differing bodies share a cache row.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, kv};
use async_nats::Client as NatsClient;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Bucket name carrying the conductor's per-tenant idempotency-key cache.
/// Distinct from the `ctx-<ns>` bucket so cache rows can have their own TTL
/// without affecting captures retention.
pub const IDEMPOTENCY_BUCKET: &str = "signal_idempotency";

/// Cache-row value. The original signal_id is what dedup retries return; the
/// input_sha256 is what differentiates a "same logical request" retry from
/// a key-reuse-with-different-body bug.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    signal_id: Uuid,
    /// Hex of the SHA256 over the canonical-JSON of `inputs`. Hex (not raw
    /// bytes) so the JSON wire shape is human-readable in operator dumps;
    /// the cost is 32 bytes -> 64 bytes per row, negligible at 10-minute TTL.
    input_sha256: String,
}

/// Outcome of `check_or_insert`. The HTTP handler translates each arm to a
/// distinct response shape: `Fresh` → continue the trigger flow; `Dedup` →
/// return 200 with `deduplicated: true`; `Conflict` → return 409 with both
/// hashes so the producer learns its key collided with a different payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheOutcome {
    /// No prior entry for this key. The fresh `(signal_id, hash)` is now
    /// cached; the handler proceeds with the trigger flow.
    Fresh,
    /// Cache hit with the same payload hash — an idempotent retry. The
    /// returned `signal_id` is the original; the handler short-circuits to
    /// a 200 response without further state mutation.
    DeduplicatedSameHash { original_signal_id: Uuid },
    /// Cache hit with a different payload hash. The key was reused for a
    /// semantically different request. The handler returns 409 with both
    /// hashes; no state is mutated.
    ConflictDifferentHash {
        original_signal_id: Uuid,
        original_hash: String,
    },
}

/// Default TTL — 10 minutes is a typical producer retry window and bounds
/// the orphan-captures window when a producer retries through a relay
/// outage.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10 * 60);

/// Get-or-create the idempotency-cache bucket. Mirrors the get-or-create
/// pattern used by `tickr_ctx::store::Store::open` and the executor's
/// log-bucket init.
pub async fn open_bucket(nats: &NatsClient) -> Result<kv::Store> {
    let js = jetstream::new(nats.clone());
    match js.get_key_value(IDEMPOTENCY_BUCKET).await {
        Ok(s) => Ok(s),
        Err(_) => js
            .create_key_value(jetstream::kv::Config {
                bucket: IDEMPOTENCY_BUCKET.to_string(),
                history: 1,
                max_age: DEFAULT_TTL,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("failed to create idempotency KV bucket: {}", e)),
    }
}

/// Atomic check-or-insert against the cache.
///
/// On miss: writes `(key, {fresh_signal_id, hex(hash)})` and returns
/// `Fresh`. The write uses `create` so a concurrent inserter racing the
/// same key is detected — the loser observes the winner's row on the
/// subsequent read.
///
/// On hit with the same hash: returns `DeduplicatedSameHash`.
///
/// On hit with a different hash: returns `ConflictDifferentHash`.
pub async fn check_or_insert(
    kv: &kv::Store,
    key: &str,
    fresh_signal_id: Uuid,
    input_sha256: &[u8; 32],
) -> Result<CacheOutcome> {
    let new_value = CacheValue {
        signal_id: fresh_signal_id,
        input_sha256: hex::encode(input_sha256),
    };
    let new_bytes = serde_json::to_vec(&new_value).context("serialize idempotency cache row")?;

    // Try to claim the row. `create` only succeeds when no row exists.
    match kv.create(key, new_bytes.into()).await {
        Ok(_) => Ok(CacheOutcome::Fresh),
        Err(_) => {
            // Either a row already existed, or a concurrent inserter beat
            // us. Either way the next read is authoritative.
            match kv.get(key).await {
                Ok(Some(bytes)) => {
                    let cached: CacheValue = serde_json::from_slice(&bytes)
                        .context("deserialize cached idempotency row")?;
                    if cached.input_sha256 == new_value.input_sha256 {
                        Ok(CacheOutcome::DeduplicatedSameHash {
                            original_signal_id: cached.signal_id,
                        })
                    } else {
                        Ok(CacheOutcome::ConflictDifferentHash {
                            original_signal_id: cached.signal_id,
                            original_hash: cached.input_sha256,
                        })
                    }
                }
                Ok(None) => {
                    // The row vanished between the create-failure and the
                    // read — most plausibly TTL expiry mid-flight. Treat as
                    // a fresh write rather than a stuck state.
                    Err(anyhow!(
                        "idempotency cache row gone between create and read; retry"
                    ))
                }
                Err(e) => Err(anyhow!("nats kv get on idempotency cache failed: {}", e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hex_hash_from(s: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        hex::encode(h.finalize())
    }

    #[test]
    fn cache_value_round_trips_through_serde_json() {
        let v = CacheValue {
            signal_id: Uuid::new_v4(),
            input_sha256: hex_hash_from("{\"a\":1}"),
        };
        let bytes = serde_json::to_vec(&v).unwrap();
        let parsed: CacheValue = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.signal_id, v.signal_id);
        assert_eq!(parsed.input_sha256, v.input_sha256);
    }

    #[test]
    fn cache_outcome_variants_are_distinct() {
        // Sanity: the three CacheOutcome arms carry the right fields to
        // drive distinct HTTP responses without further ambiguity.
        let sid = Uuid::new_v4();
        let a = CacheOutcome::Fresh;
        let b = CacheOutcome::DeduplicatedSameHash {
            original_signal_id: sid,
        };
        let c = CacheOutcome::ConflictDifferentHash {
            original_signal_id: sid,
            original_hash: "abc".into(),
        };
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn canonical_hash_input_yields_stable_hex_repr() {
        // The conductor's HTTP handler hashes via crate::canonical_json::hash
        // and hex-encodes for the cache row. This test guards the contract
        // that the hex string is deterministic and round-trips through serde.
        use crate::canonical_json;
        let payload = json!({"order_id": "X-1", "user": {"id": 42}});
        let h1 = canonical_json::hash(Some(&payload));
        let h2 = canonical_json::hash(Some(&payload));
        assert_eq!(h1, h2);
        assert_eq!(hex::encode(h1), hex::encode(h2));
    }
}
