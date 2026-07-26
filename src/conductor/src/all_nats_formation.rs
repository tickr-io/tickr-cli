//! Bounded admission for the fresh hardened all-NATS resource set.
//!
//! Every lookup addresses one exact v2 name. This module has no discovery,
//! migration, compatibility, or legacy-resource path.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, consumer, kv, stream};
use async_nats::Client;
use std::collections::BTreeSet;
use std::time::Duration;
use tickr_proto::coord::{all_nats as names, LIVENESS_MARKER_TTL};

const ADMISSION_TIMEOUT: Duration = Duration::from_secs(20);
const LIVENESS_ACK_WAIT: Duration = Duration::from_secs(5);

/// Connect, admit the fresh identity, and create or verify the complete static
/// all-NATS resource set before a component starts any runtime work.
pub async fn connect_and_admit(url: &str) -> Result<()> {
    tokio::time::timeout(ADMISSION_TIMEOUT, async {
        let nats = async_nats::connect(url)
            .await
            .with_context(|| format!("connecting to all-NATS formation at {url}"))?;
        admit_and_provision(&nats).await
    })
    .await
    .map_err(|_| anyhow!("all-NATS resource admission exceeded {ADMISSION_TIMEOUT:?}"))?
}

/// Admit an already-connected isolated NATS instance.
pub async fn admit_and_provision(nats: &Client) -> Result<()> {
    tokio::time::timeout(ADMISSION_TIMEOUT, admit_and_provision_inner(nats))
        .await
        .map_err(|_| anyhow!("all-NATS resource admission exceeded {ADMISSION_TIMEOUT:?}"))?
}

async fn admit_and_provision_inner(nats: &Client) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let scope_bucket = tickr_ctx::scope::bucket_for_namespace(&scope_namespace());
    let identity = format!("{};scope={scope_bucket}", names::FORMATION_IDENTITY);

    admit_identity(&js, &scope_bucket, &identity).await?;
    ensure_kv(&js, KvSpec::new(names::FORMATION_IDENTITY_BUCKET)).await?;

    ensure_work_queue(
        &js,
        names::TASK_DISPATCH_STREAM,
        names::TASK_DISPATCH_SUBJECT,
    )
    .await?;
    ensure_work_queue(&js, names::TASK_EVENT_STREAM, names::TASK_EVENT_SUBJECT).await?;
    ensure_work_queue(&js, names::TASK_CANCEL_STREAM, names::TASK_CANCEL_SUBJECT).await?;
    ensure_work_queue(
        &js,
        names::TASK_CANCEL_ACK_STREAM,
        names::TASK_CANCEL_ACK_SUBJECT,
    )
    .await?;
    ensure_work_queue(&js, names::COMPACTION_STREAM, names::COMPACTION_SUBJECT).await?;
    ensure_log_stream(&js).await?;
    ensure_work_queue(
        &js,
        names::EVENT_INGRESS_STREAM,
        names::EVENT_INGRESS_SUBJECT,
    )
    .await?;

    ensure_kv(
        &js,
        KvSpec::new(&scope_bucket).max_value_size(names::SCOPE_MAX_VALUE_SIZE),
    )
    .await?;
    ensure_kv(
        &js,
        KvSpec::new(names::INGRESS_IDEMPOTENCY_BUCKET).max_age(names::INGRESS_IDEMPOTENCY_TTL),
    )
    .await?;
    ensure_kv(
        &js,
        KvSpec::new(names::LIVENESS_BUCKET).marker_ttl(LIVENESS_MARKER_TTL),
    )
    .await?;
    ensure_kv(&js, KvSpec::new(names::TASK_PICKUP_BUCKET)).await?;
    ensure_kv(&js, KvSpec::new(names::COMPACTION_STAGING_BUCKET)).await?;
    ensure_kv(
        &js,
        KvSpec::new(names::COMPONENT_LIVENESS_BUCKET).marker_ttl(names::COMPONENT_MARKER_TTL),
    )
    .await?;

    ensure_pull_consumer(
        &js,
        names::TASK_DISPATCH_STREAM,
        names::TASK_DISPATCH_CONSUMER,
        None,
        None,
    )
    .await?;
    ensure_pull_consumer(
        &js,
        names::TASK_EVENT_STREAM,
        names::TASK_EVENT_CONSUMER,
        None,
        None,
    )
    .await?;
    ensure_pull_consumer(
        &js,
        names::TASK_CANCEL_STREAM,
        names::TASK_CANCEL_CONSUMER,
        None,
        None,
    )
    .await?;
    ensure_pull_consumer(
        &js,
        names::TASK_CANCEL_ACK_STREAM,
        names::TASK_CANCEL_ACK_CONSUMER,
        None,
        None,
    )
    .await?;
    ensure_pull_consumer(
        &js,
        names::COMPACTION_STREAM,
        names::COMPACTION_CONSUMER,
        None,
        Some(names::COMPACTION_ACK_WAIT),
    )
    .await?;
    ensure_pull_consumer(
        &js,
        &format!("KV_{}", names::LIVENESS_BUCKET),
        names::LIVENESS_MARKER_CONSUMER,
        Some(&format!("$KV.{}.>", names::LIVENESS_BUCKET)),
        Some(LIVENESS_ACK_WAIT),
    )
    .await?;
    ensure_pull_consumer(
        &js,
        names::EVENT_INGRESS_STREAM,
        names::EVENT_INGRESS_CONSUMER,
        None,
        None,
    )
    .await?;

    Ok(())
}

fn scope_namespace() -> String {
    std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_owned())
}

async fn admit_identity(js: &jetstream::Context, scope_bucket: &str, expected: &str) -> Result<()> {
    let identity_store = match js.get_key_value(names::FORMATION_IDENTITY_BUCKET).await {
        Ok(store) => store,
        Err(_) => {
            if fresh_state_is_nonempty(js, scope_bucket).await? {
                return Err(anyhow!(
                    "fresh all-NATS state is nonempty but formation identity is missing"
                ));
            }
            match js
                .create_key_value(kv::Config {
                    bucket: names::FORMATION_IDENTITY_BUCKET.to_owned(),
                    history: 1,
                    storage: stream::StorageType::File,
                    ..Default::default()
                })
                .await
            {
                Ok(store) => store,
                Err(_) => js
                    .get_key_value(names::FORMATION_IDENTITY_BUCKET)
                    .await
                    .context("opening concurrently-created all-NATS identity bucket")?,
            }
        }
    };

    match identity_store
        .get(names::FORMATION_IDENTITY_KEY)
        .await
        .context("reading all-NATS formation identity")?
    {
        Some(actual) if actual.as_ref() == expected.as_bytes() => Ok(()),
        Some(_) => Err(anyhow!(
            "all-NATS formation identity does not match admitted protocol set"
        )),
        None => {
            if fresh_state_is_nonempty(js, scope_bucket).await?
                || identity_store
                    .status()
                    .await
                    .context("reading all-NATS identity bucket status")?
                    .values()
                    > 0
            {
                return Err(anyhow!(
                    "fresh all-NATS state is nonempty but formation identity is missing"
                ));
            }
            match identity_store
                .create(names::FORMATION_IDENTITY_KEY, expected.to_owned().into())
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => match identity_store
                    .get(names::FORMATION_IDENTITY_KEY)
                    .await
                    .context("reading concurrently-installed all-NATS identity")?
                {
                    Some(actual) if actual.as_ref() == expected.as_bytes() => Ok(()),
                    _ => Err(anyhow!(
                        "all-NATS formation identity was concurrently installed with a mismatch"
                    )),
                },
            }
        }
    }
}

async fn fresh_state_is_nonempty(js: &jetstream::Context, scope_bucket: &str) -> Result<bool> {
    for name in names::STREAM_NAMES {
        if let Ok(mut existing) = js.get_stream(name).await {
            if existing
                .info()
                .await
                .with_context(|| format!("reading exact stream {name}"))?
                .state
                .messages
                > 0
            {
                return Ok(true);
            }
        }
    }

    let mut buckets = BTreeSet::from([
        scope_bucket,
        names::DEFAULT_SCOPE_BUCKET,
        names::INGRESS_IDEMPOTENCY_BUCKET,
        names::LIVENESS_BUCKET,
        names::TASK_PICKUP_BUCKET,
        names::COMPONENT_LIVENESS_BUCKET,
        names::COMPACTION_STAGING_BUCKET,
    ]);
    buckets.remove(names::FORMATION_IDENTITY_BUCKET);
    for bucket in buckets {
        if let Ok(store) = js.get_key_value(bucket).await {
            if store
                .status()
                .await
                .with_context(|| format!("reading exact KV bucket {bucket}"))?
                .values()
                > 0
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn ensure_work_queue(js: &jetstream::Context, name: &str, subject: &str) -> Result<()> {
    let mut stream = js
        .get_or_create_stream(stream::Config {
            name: name.to_owned(),
            subjects: vec![subject.to_owned()],
            retention: stream::RetentionPolicy::WorkQueue,
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await
        .with_context(|| format!("creating or opening exact stream {name}"))?;
    let config = &stream
        .info()
        .await
        .with_context(|| format!("verifying exact stream {name}"))?
        .config;
    if config.name != name
        || config.subjects != [subject]
        || config.retention != stream::RetentionPolicy::WorkQueue
        || config.storage != stream::StorageType::File
    {
        return Err(anyhow!(
            "fresh all-NATS stream {name} has mismatched configuration"
        ));
    }
    Ok(())
}

async fn ensure_log_stream(js: &jetstream::Context) -> Result<()> {
    let mut stream = js
        .get_or_create_stream(stream::Config {
            name: names::LOG_STREAM.to_owned(),
            subjects: vec![names::LOG_STREAM_SUBJECTS.to_owned()],
            max_bytes: names::LOG_STREAM_MAX_BYTES,
            discard: stream::DiscardPolicy::New,
            duplicate_window: names::LOG_STREAM_DEDUP_WINDOW,
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await
        .context("creating or opening exact Log-staging stream")?;
    let config = &stream
        .info()
        .await
        .context("verifying exact Log-staging stream")?
        .config;
    if config.name != names::LOG_STREAM
        || config.subjects != [names::LOG_STREAM_SUBJECTS]
        || config.retention != stream::RetentionPolicy::Limits
        || config.max_bytes != names::LOG_STREAM_MAX_BYTES
        || config.discard != stream::DiscardPolicy::New
        || config.duplicate_window != names::LOG_STREAM_DEDUP_WINDOW
        || config.storage != stream::StorageType::File
    {
        return Err(anyhow!(
            "fresh all-NATS Log-staging stream has mismatched configuration"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct KvSpec<'a> {
    bucket: &'a str,
    max_value_size: i32,
    max_age: Duration,
    marker_ttl: Option<Duration>,
}

impl<'a> KvSpec<'a> {
    const fn new(bucket: &'a str) -> Self {
        Self {
            bucket,
            max_value_size: 0,
            max_age: Duration::ZERO,
            marker_ttl: None,
        }
    }

    const fn max_value_size(mut self, max_value_size: i32) -> Self {
        self.max_value_size = max_value_size;
        self
    }

    const fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    const fn marker_ttl(mut self, marker_ttl: Duration) -> Self {
        self.marker_ttl = Some(marker_ttl);
        self
    }
}

async fn ensure_kv(js: &jetstream::Context, expected: KvSpec<'_>) -> Result<()> {
    let store = match js.get_key_value(expected.bucket).await {
        Ok(store) => store,
        Err(_) => js
            .create_key_value(kv::Config {
                bucket: expected.bucket.to_owned(),
                history: 1,
                max_value_size: expected.max_value_size,
                max_age: expected.max_age,
                storage: stream::StorageType::File,
                limit_markers: expected.marker_ttl,
                ..Default::default()
            })
            .await
            .with_context(|| format!("creating exact KV bucket {}", expected.bucket))?,
    };
    let status = store
        .status()
        .await
        .with_context(|| format!("verifying exact KV bucket {}", expected.bucket))?;
    let config = &status.info.config;
    let expected_max_value_size = if expected.max_value_size == 0 {
        -1
    } else {
        expected.max_value_size
    };
    if status.bucket() != expected.bucket
        || status.history() != 1
        || status.max_age() != expected.max_age
        || config.max_message_size != expected_max_value_size
        || config.storage != stream::StorageType::File
        || config.subject_delete_marker_ttl != expected.marker_ttl
    {
        return Err(anyhow!(
            "fresh all-NATS KV bucket {} has mismatched configuration: history={}, max_age={:?}, max_value_size={}, storage={:?}, marker_ttl={:?}",
            expected.bucket,
            status.history(),
            status.max_age(),
            config.max_message_size,
            config.storage,
            config.subject_delete_marker_ttl
        ));
    }
    Ok(())
}

async fn ensure_pull_consumer(
    js: &jetstream::Context,
    stream_name: &str,
    consumer_name: &str,
    filter_subject: Option<&str>,
    ack_wait: Option<Duration>,
) -> Result<()> {
    let stream = js
        .get_stream(stream_name)
        .await
        .with_context(|| format!("opening exact stream {stream_name} for consumer admission"))?;
    let expected_filter = filter_subject.unwrap_or_default();
    let expected_ack_wait = ack_wait.unwrap_or_default();
    let mut consumer = stream
        .get_or_create_consumer(
            consumer_name,
            consumer::pull::Config {
                durable_name: Some(consumer_name.to_owned()),
                ack_policy: consumer::AckPolicy::Explicit,
                filter_subject: expected_filter.to_owned(),
                ack_wait: expected_ack_wait,
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("creating or opening exact consumer {consumer_name}"))?;
    let config = &consumer
        .info()
        .await
        .with_context(|| format!("verifying exact consumer {consumer_name}"))?
        .config;
    if config.durable_name.as_deref() != Some(consumer_name)
        || config.ack_policy != consumer::AckPolicy::Explicit
        || config.filter_subject != expected_filter
        || (ack_wait.is_some() && config.ack_wait != expected_ack_wait)
    {
        return Err(anyhow!(
            "fresh all-NATS consumer {consumer_name} has mismatched configuration"
        ));
    }
    Ok(())
}
