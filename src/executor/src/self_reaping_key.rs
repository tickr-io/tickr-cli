//! A **self-reaping KV key** — a TTL'd NATS KV key that vanishes on its own once
//! re-arms stop. The producer re-PUTs the key on a sub-TTL cadence to keep it
//! alive; when the producer goes dark the per-key TTL elapses, NATS appends a
//! delete marker, and the key reaps itself.
//!
//! This primitive is extracted so two real consumers — the per-task liveness key
//! and the coming component-liveness key — share one tested `arm` interface
//! instead of two divergent copies of the same TTL'd-PUT logic.

use async_nats::jetstream;
use std::time::Duration;

/// Arm (or re-arm) a self-reaping KV key: publish directly to the KV subject with
/// the per-message `Nats-TTL` header (there is no public `put_with_ttl` in
/// async-nats 0.49.1; `create_with_ttl` is a CAS on revision 0, wrong for an
/// idempotent re-arm). Best-effort: a failed arm is logged, not fatal — the next
/// beat retries, and a genuinely dark producer is exactly what a self-reaping key
/// exists to surface.
pub async fn arm(js: &jetstream::Context, bucket: &str, key: &str, value: &[u8], ttl: Duration) {
    let subject = format!("$KV.{bucket}.{key}");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_TTL,
        ttl.as_secs().to_string().as_str(),
    );
    match js
        .publish_with_headers(subject, headers, value.to_vec().into())
        .await
    {
        Ok(ack) => {
            if let Err(e) = ack.await {
                eprintln!("self-reaping key arm publish-ack failed for {key}: {e}");
            }
        }
        Err(e) => eprintln!("self-reaping key arm publish failed for {key}: {e}"),
    }
}
