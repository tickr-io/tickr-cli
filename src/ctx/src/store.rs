//! Backend-neutral tickr-ctx store access.
//!
//! Distributed formations use NATS JetStream KV. Tickr Lite task processes use
//! the authenticated root-local endpoint selected by `TICKR_CTX_ENDPOINT`.

use std::pin::Pin;

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, kv};
use futures_util::{stream, Stream, StreamExt};
use uuid::Uuid;

use crate::local::{
    failure_error, read_message, LocalClient, LocalEventOperation, LocalOperation, LocalResponse,
    MAX_LOCAL_RESPONSE_BYTES,
};
use crate::scope::Scope;

pub const MAX_VALUE_SIZE: i32 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreOperation {
    Put,
    Delete,
    Purge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreEvent {
    pub operation: StoreOperation,
    pub key: String,
    pub value: Vec<u8>,
}

pub type StoreWatch = Pin<Box<dyn Stream<Item = Result<StoreEvent>> + Send>>;

enum StoreBackend {
    Nats(kv::Store),
    Local(LocalClient),
}

pub struct Store {
    backend: StoreBackend,
}

impl Store {
    pub async fn open(scope: &Scope) -> Result<Self> {
        if let Some(client) =
            LocalClient::from_environment(&scope.ns, &scope.run_id, &scope.task_id)
                .context("resolving local tickr-ctx endpoint")?
        {
            return Ok(Self {
                backend: StoreBackend::Local(client),
            });
        }

        let bucket = scope.bucket();
        let nats = async_nats::connect(tickr_proto::config::nats_url())
            .await
            .with_context(|| {
                format!(
                    "failed to connect to NATS at {}",
                    tickr_proto::config::nats_url()
                )
            })?;
        let js = jetstream::new(nats);
        let kv = match js.get_key_value(&bucket).await {
            Ok(store) => store,
            Err(_) => js
                .create_key_value(jetstream::kv::Config {
                    bucket: bucket.clone(),
                    history: 1,
                    max_value_size: MAX_VALUE_SIZE,
                    ..Default::default()
                })
                .await
                .map_err(|error| anyhow!("failed to create KV bucket {bucket}: {error}"))?,
        };
        Ok(Self {
            backend: StoreBackend::Nats(kv),
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match &self.backend {
            StoreBackend::Nats(store) => store
                .get(key)
                .await
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .map_err(|error| anyhow!("nats kv get failed: {error}")),
            StoreBackend::Local(client) => match client
                .request(LocalOperation::Get {
                    key: key.to_owned(),
                })
                .await?
            {
                LocalResponse::Value { envelope } => Ok(Some(envelope)),
                LocalResponse::Missing => Ok(None),
                LocalResponse::Failure(failure) => Err(failure_error(failure).into()),
                response => Err(anyhow!(
                    "unexpected local tickr-ctx get response: {response:?}"
                )),
            },
        }
    }

    pub async fn put(&self, key: String, envelope: Vec<u8>) -> Result<()> {
        match &self.backend {
            StoreBackend::Nats(store) => {
                store
                    .put(key, envelope.into())
                    .await
                    .map_err(|error| anyhow!("nats kv put failed: {error}"))?;
                Ok(())
            }
            StoreBackend::Local(client) => match client
                .request(LocalOperation::Put {
                    key,
                    envelope,
                    claim_id: Uuid::new_v4(),
                })
                .await?
            {
                LocalResponse::Applied => Ok(()),
                LocalResponse::Failure(failure) => Err(failure_error(failure).into()),
                response => Err(anyhow!(
                    "unexpected local tickr-ctx put response: {response:?}"
                )),
            },
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        match &self.backend {
            StoreBackend::Nats(store) => store
                .delete(key)
                .await
                .map_err(|error| anyhow!("nats kv delete failed: {error}")),
            StoreBackend::Local(client) => match client
                .request(LocalOperation::Delete {
                    key: key.to_owned(),
                    claim_id: Uuid::new_v4(),
                })
                .await?
            {
                LocalResponse::Applied | LocalResponse::Missing => Ok(()),
                LocalResponse::Failure(failure) => Err(failure_error(failure).into()),
                response => Err(anyhow!(
                    "unexpected local tickr-ctx delete response: {response:?}"
                )),
            },
        }
    }

    pub async fn keys(&self, prefix: &str) -> Result<Vec<String>> {
        match &self.backend {
            StoreBackend::Nats(store) => {
                let mut keys = store
                    .keys()
                    .await
                    .map_err(|error| anyhow!("nats kv keys failed: {error}"))?;
                let mut collected = Vec::new();
                while let Some(key) = keys.next().await {
                    let key =
                        key.map_err(|error| anyhow!("nats kv keys stream failed: {error}"))?;
                    if key.starts_with(prefix) {
                        collected.push(key);
                    }
                }
                Ok(collected)
            }
            StoreBackend::Local(client) => match client
                .request(LocalOperation::List {
                    prefix: prefix.to_owned(),
                })
                .await?
            {
                LocalResponse::Keys { keys } => Ok(keys),
                LocalResponse::Failure(failure) => Err(failure_error(failure).into()),
                response => Err(anyhow!(
                    "unexpected local tickr-ctx list response: {response:?}"
                )),
            },
        }
    }

    pub async fn watch_all(&self) -> Result<StoreWatch> {
        self.watch_prefix(String::new()).await
    }

    pub async fn watch_key(&self, key: &str) -> Result<StoreWatch> {
        self.watch_prefix(key.to_owned()).await
    }

    async fn watch_prefix(&self, prefix: String) -> Result<StoreWatch> {
        match &self.backend {
            StoreBackend::Nats(store) => {
                let watch = if prefix.is_empty() {
                    store
                        .watch_all()
                        .await
                        .map_err(|error| anyhow!("nats kv watch failed: {error}"))?
                } else {
                    store
                        .watch(&prefix)
                        .await
                        .map_err(|error| anyhow!("nats kv watch failed: {error}"))?
                };
                Ok(Box::pin(watch.map(|item| {
                    let entry =
                        item.map_err(|error| anyhow!("nats kv watch stream failed: {error}"))?;
                    let operation = match entry.operation {
                        kv::Operation::Put => StoreOperation::Put,
                        kv::Operation::Delete => StoreOperation::Delete,
                        kv::Operation::Purge => StoreOperation::Purge,
                    };
                    Ok(StoreEvent {
                        operation,
                        key: entry.key,
                        value: entry.value.to_vec(),
                    })
                })))
            }
            StoreBackend::Local(client) => {
                let stream = client.watch(prefix).await?;
                let watch = stream::unfold(Some(stream), |state| async move {
                    let mut stream = state?;
                    match read_message::<_, LocalResponse>(&mut stream, MAX_LOCAL_RESPONSE_BYTES)
                        .await
                    {
                        Ok(LocalResponse::Event(event)) => {
                            let operation = match event.operation {
                                LocalEventOperation::Put => StoreOperation::Put,
                                LocalEventOperation::Delete => StoreOperation::Delete,
                            };
                            Some((
                                Ok(StoreEvent {
                                    operation,
                                    key: event.key,
                                    value: event.envelope,
                                }),
                                Some(stream),
                            ))
                        }
                        Ok(LocalResponse::Failure(failure)) => {
                            Some((Err(failure_error(failure).into()), None))
                        }
                        Ok(response) => Some((
                            Err(anyhow!(
                                "unexpected local tickr-ctx watch response: {response:?}"
                            )),
                            None,
                        )),
                        Err(error) => Some((
                            Err(anyhow!("local tickr-ctx endpoint unavailable: {error}")),
                            None,
                        )),
                    }
                });
                Ok(Box::pin(watch))
            }
        }
    }
}
