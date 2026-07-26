//! Task-coordination constants and key formatters.
//!
//! This module defines JetStream names for dispatch, events, and cancellation;
//! liveness-watchdog bucket, TTL, environment, and key conventions; and the
//! component-liveness values used by fleet health reporting.
//!
//! The proto *message* shapes these constants coordinate live next door in
//! [`crate::task`]; this module carries only the names and key helpers that are
//! not themselves protobuf.

use std::{future::Future, pin::Pin, time::Duration};

pub mod all_nats;
pub mod command_bus;
pub mod component_liveness;
pub mod liveness;
pub mod log_stream;

pub use component_liveness::{
    component_liveness_key, ComponentLivenessValue, COMPONENT_LIVENESS_BUCKET,
};
pub use liveness::{
    liveness_key, parse_liveness_key, LivenessIdentity, DEFAULT_LIVENESS_TIMEOUT_SECS,
    LIVENESS_BUCKET, LIVENESS_MARKER_CONSUMER, LIVENESS_MARKER_TTL, LIVENESS_TIMEOUT_ENV,
    MARKER_REASON_EXPIRY,
};

/// Boxed future used by the object-safe TaskEvents role interfaces.
pub type TaskEventFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Producer side of the formation-selected durable TaskEvents role.
///
/// The stable identity belongs to the choreography, while each adapter keeps
/// its substrate client, resource names, and durability proof private.
pub trait TaskEventWriter: Send + Sync {
    fn prepare(&self) -> TaskEventFuture<'_, Result<(), String>>;

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_task_event: &'a [u8],
    ) -> TaskEventFuture<'a, Result<(), String>>;
}

/// One durable TaskEvent delivery held until the Conductor decides whether the
/// existing relay-forward boundary was crossed.
pub trait TaskEventDelivery: Send {
    fn payload(&self) -> &[u8];

    fn complete(self: Box<Self>) -> TaskEventFuture<'static, Result<(), String>>;

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskEventFuture<'static, Result<(), String>>;
}

/// Consumer side of the formation-selected durable TaskEvents role.
pub trait TaskEventConsumer: Send + Sync {
    fn next(&self) -> TaskEventFuture<'_, Result<Option<Box<dyn TaskEventDelivery>>, String>>;
}

/// Boxed future used by the object-safe CompactionStaging role interfaces.
pub type CompactionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Immutable source-seal evidence retained beside one staged Compaction job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionStagingSeal {
    encoded: Vec<u8>,
    digest: String,
    source_references: u64,
}

impl CompactionStagingSeal {
    pub fn new(encoded: Vec<u8>, digest: String, source_references: u64) -> Self {
        Self {
            encoded,
            digest,
            source_references,
        }
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub const fn source_references(&self) -> u64 {
        self.source_references
    }
}

/// One durable Compaction delivery held until archive commit and source purge.
pub trait CompactionStagingDelivery: Send {
    fn payload(&self) -> &[u8];

    fn load_seal(&self) -> CompactionFuture<'_, Result<Option<CompactionStagingSeal>, String>>;

    fn record_seal<'a>(
        &'a self,
        seal: &'a CompactionStagingSeal,
    ) -> CompactionFuture<'a, Result<(), String>>;

    fn load_archive_identity(&self) -> CompactionFuture<'_, Result<Option<Vec<u8>>, String>>;

    fn record_archive_identity<'a>(
        &'a self,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>>;

    fn complete<'a>(
        self: Box<Self>,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>>;

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> CompactionFuture<'static, Result<(), String>>;
}

/// Relay producer and drain consumer sides of the selected CompactionStaging role.
pub trait CompactionStaging: Send + Sync {
    fn prepare(&self) -> CompactionFuture<'_, Result<(), String>>;

    fn stage<'a>(
        &'a self,
        encoded_compaction: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>>;

    fn next(
        &self,
    ) -> CompactionFuture<'_, Result<Option<Box<dyn CompactionStagingDelivery>>, String>>;
}

/// Boxed future used by the object-safe TaskDispatch publication interface.
pub type TaskDispatchFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Conductor side of the formation-selected durable TaskDispatch role.
///
/// The stable identity belongs to the handoff choreography. Each adapter keeps
/// its substrate client, resource names, and durability proof private.
pub trait TaskDispatchPublisher: Send + Sync {
    fn prepare(&self) -> TaskDispatchFuture<'_, Result<(), String>>;

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_dispatch: &'a [u8],
    ) -> TaskDispatchFuture<'a, Result<(), String>>;
}

/// Boxed future used by the object-safe TaskCancellation role interfaces.
pub type TaskCancellationFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Conductor side of the formation-selected durable TaskCancellation role.
///
/// The adapter binds the existing encoded request to its stable
/// acknowledgement identity before this call returns. Substrate clients and
/// owner-delivery details remain private to the adapter.
pub trait TaskCancellationPublisher: Send + Sync {
    fn prepare(&self) -> TaskCancellationFuture<'_, Result<(), String>>;

    fn stage<'a>(
        &'a self,
        encoded_cancellation: &'a [u8],
    ) -> TaskCancellationFuture<'a, Result<(), String>>;
}

/// One durable cancellation acknowledgement held until the Conductor forwards
/// the existing protobuf bytes onto the relay.
pub trait TaskCancellationAckDelivery: Send {
    fn payload(&self) -> &[u8];

    fn complete(self: Box<Self>) -> TaskCancellationFuture<'static, Result<(), String>>;

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskCancellationFuture<'static, Result<(), String>>;
}

/// Conductor drain side of the formation-selected TaskCancellation role.
pub trait TaskCancellationAckConsumer: Send + Sync {
    fn next(
        &self,
    ) -> TaskCancellationFuture<'_, Result<Option<Box<dyn TaskCancellationAckDelivery>>, String>>;
}

// --- Task-dispatch leg (conductor → executor) ---

/// JetStream stream name backing the durable task-dispatch leg. A **work
/// queue**: a dispatched task is removed once an executor acks it, and an
/// unpicked dispatch waits durably here instead of being lost.
pub const TASK_DISPATCH_STREAM: &str = all_nats::TASK_DISPATCH_STREAM;

/// JetStream subject the conductor publishes dispatched tasks to and the
/// executor's shared durable pull consumer drains.
pub const TASK_DISPATCH_SUBJECT: &str = all_nats::TASK_DISPATCH_SUBJECT;

/// Durable pull-consumer name the executors bind. Shared across executor
/// instances — NATS load-balances delivery across whoever binds the same
/// durable name, so the work queue hands each dispatch to exactly one executor.
pub const TASK_DISPATCH_CONSUMER: &str = all_nats::TASK_DISPATCH_CONSUMER;

/// Generation-qualified owner, deadline, and staged-TaskEvent evidence for the
/// hardened all-NATS pickup handoff.
pub const TASK_PICKUP_BUCKET: &str = all_nats::TASK_PICKUP_BUCKET;

// --- Task-event leg (executor → conductor) ---

/// JetStream stream name backing the durable executor→conductor update leg. A
/// work queue: a message is removed once the conductor acks it (ack-on-forward),
/// redelivered on un-ack so a relay/conductor blip can't drop a completion.
pub const TASK_EVENT_STREAM: &str = all_nats::TASK_EVENT_STREAM;

/// JetStream subject the executor publishes typed task events to and the
/// conductor's shared durable pull consumer drains.
pub const TASK_EVENT_SUBJECT: &str = all_nats::TASK_EVENT_SUBJECT;

/// Durable pull-consumer name the conductor binds for task events. Shared across
/// conductor instances so NATS load-balances the compaction-drain reads.
pub const TASK_EVENT_CONSUMER: &str = all_nats::TASK_EVENT_CONSUMER;

// --- Cancel-request leg (conductor → executor) ---

/// JetStream stream name backing the durable conductor→executor cancel-request
/// leg. A **work queue** (mirrors `TASK_DISPATCH_STREAM`): a cancel-request
/// waits durably here until an executor drains it, rather than being lost on
/// fire-and-forget core NATS.
pub const TASK_CANCEL_STREAM: &str = all_nats::TASK_CANCEL_STREAM;

/// JetStream subject the conductor publishes cancel-requests to and the
/// executor's shared durable pull consumer drains.
pub const TASK_CANCEL_SUBJECT: &str = all_nats::TASK_CANCEL_SUBJECT;

/// Durable pull-consumer name the executors bind for cancel-requests. Shared
/// across executor instances so delivery load-balances (the dispatch pattern).
pub const TASK_CANCEL_CONSUMER: &str = all_nats::TASK_CANCEL_CONSUMER;

// --- Cancel-ack leg (executor → conductor) ---

/// JetStream stream name backing the durable executor→conductor cancel-ack
/// leg. A **work queue** (mirrors `TASK_EVENT_STREAM`): an ack is removed once
/// a conductor acks it (ack-on-forward), redelivered on un-ack.
pub const TASK_CANCEL_ACK_STREAM: &str = all_nats::TASK_CANCEL_ACK_STREAM;

/// JetStream subject the executor publishes cancel-acks to and the conductor's
/// shared durable pull consumer drains.
pub const TASK_CANCEL_ACK_SUBJECT: &str = all_nats::TASK_CANCEL_ACK_SUBJECT;

/// Durable pull-consumer name the conductor binds for cancel-acks. Shared
/// across conductor instances (the task-event drain pattern).
pub const TASK_CANCEL_ACK_CONSUMER: &str = all_nats::TASK_CANCEL_ACK_CONSUMER;
