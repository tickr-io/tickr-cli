//! Task-coordination constants and key formatters.
//!
//! This module defines JetStream names for dispatch, events, and cancellation;
//! liveness-watchdog bucket, TTL, environment, and key conventions; and the
//! component-liveness values used by fleet health reporting.
//!
//! The proto *message* shapes these constants coordinate live next door in
//! [`crate::task`]; this module carries only the names and key helpers that are
//! not themselves protobuf.

pub mod component_liveness;
pub mod liveness;

pub use component_liveness::{
    component_liveness_key, ComponentLivenessValue, COMPONENT_LIVENESS_BUCKET,
};
pub use liveness::{
    liveness_key, parse_liveness_key, LivenessIdentity, DEFAULT_LIVENESS_TIMEOUT_SECS,
    LIVENESS_BUCKET, LIVENESS_MARKER_CONSUMER, LIVENESS_MARKER_TTL, LIVENESS_TIMEOUT_ENV,
    MARKER_REASON_EXPIRY,
};

// --- Task-dispatch leg (conductor → executor) ---

/// JetStream stream name backing the durable task-dispatch leg. A **work
/// queue**: a dispatched task is removed once an executor acks it, and an
/// unpicked dispatch waits durably here instead of being lost.
pub const TASK_DISPATCH_STREAM: &str = "tickr_task_dispatch";

/// JetStream subject the conductor publishes dispatched tasks to and the
/// executor's shared durable pull consumer drains.
pub const TASK_DISPATCH_SUBJECT: &str = "tickr.task.dispatch";

/// Durable pull-consumer name the executors bind. Shared across executor
/// instances — NATS load-balances delivery across whoever binds the same
/// durable name, so the work queue hands each dispatch to exactly one executor.
pub const TASK_DISPATCH_CONSUMER: &str = "tickr-executor-task-dispatch";

// --- Task-event leg (executor → conductor) ---

/// JetStream stream name backing the durable executor→conductor update leg. A
/// work queue: a message is removed once the conductor acks it (ack-on-forward),
/// redelivered on un-ack so a relay/conductor blip can't drop a completion.
pub const TASK_EVENT_STREAM: &str = "tickr_task_events";

/// JetStream subject the executor publishes typed task events to and the
/// conductor's shared durable pull consumer drains.
pub const TASK_EVENT_SUBJECT: &str = "tickr.task.events";

/// Durable pull-consumer name the conductor binds for task events. Shared across
/// conductor instances so NATS load-balances the compaction-drain reads.
pub const TASK_EVENT_CONSUMER: &str = "tickr-conductor-task-events";

// --- Cancel-request leg (conductor → executor) ---

/// JetStream stream name backing the durable conductor→executor cancel-request
/// leg. A **work queue** (mirrors `TASK_DISPATCH_STREAM`): a cancel-request
/// waits durably here until an executor drains it, rather than being lost on
/// fire-and-forget core NATS.
pub const TASK_CANCEL_STREAM: &str = "tickr_task_cancel";

/// JetStream subject the conductor publishes cancel-requests to and the
/// executor's shared durable pull consumer drains.
pub const TASK_CANCEL_SUBJECT: &str = "tickr.task.cancel";

/// Durable pull-consumer name the executors bind for cancel-requests. Shared
/// across executor instances so delivery load-balances (the dispatch pattern).
pub const TASK_CANCEL_CONSUMER: &str = "tickr-executor-task-cancel";

// --- Cancel-ack leg (executor → conductor) ---

/// JetStream stream name backing the durable executor→conductor cancel-ack
/// leg. A **work queue** (mirrors `TASK_EVENT_STREAM`): an ack is removed once
/// a conductor acks it (ack-on-forward), redelivered on un-ack.
pub const TASK_CANCEL_ACK_STREAM: &str = "tickr_task_cancel_acks";

/// JetStream subject the executor publishes cancel-acks to and the conductor's
/// shared durable pull consumer drains.
pub const TASK_CANCEL_ACK_SUBJECT: &str = "tickr.task.cancel.acks";

/// Durable pull-consumer name the conductor binds for cancel-acks. Shared
/// across conductor instances (the task-event drain pattern).
pub const TASK_CANCEL_ACK_CONSUMER: &str = "tickr-conductor-task-cancel-acks";
