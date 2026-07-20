//! Thin wrapper around NATS JetStream KV for the ctx bucket.
//!
//! Modeled on `tickr/src/executor/src/task_handler.rs::init_kv_store` for the
//! `logs` bucket: get-or-create with a 1 MiB max value size and 1-revision
//! history. Object Store spill at >1 MiB is deferred to Phase 3.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, kv};

pub const MAX_VALUE_SIZE: i32 = 1024 * 1024; // 1 MiB

pub struct Store {
    pub kv: kv::Store,
}

impl Store {
    pub async fn open(bucket: &str) -> Result<Self> {
        let nats = async_nats::connect(tickr_proto::config::nats_url())
            .await
            .with_context(|| {
                format!(
                    "failed to connect to NATS at {}",
                    tickr_proto::config::nats_url()
                )
            })?;
        let js = jetstream::new(nats);

        // Get-or-create. Pattern matches `init_kv_store` in the executor.
        let kv = match js.get_key_value(bucket).await {
            Ok(s) => s,
            Err(_) => js
                .create_key_value(jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    max_value_size: MAX_VALUE_SIZE,
                    ..Default::default()
                })
                .await
                .map_err(|e| anyhow!("failed to create KV bucket {}: {}", bucket, e))?,
        };

        Ok(Store { kv })
    }
}
