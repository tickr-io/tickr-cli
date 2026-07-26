//! Hardened all-NATS protocol namespace.
//!
//! Every persisted or transported all-NATS resource is rooted in this profile-
//! and version-qualified namespace. The previous unqualified names are not
//! inputs to runtime admission and are intentionally absent from this module.

use crate::coord::log_stream::{
    content_digest, LogExit, LogRecordIdentity, LogStreamIdentity, LogTerminal, PreAcceptanceGap,
    ReplayedLogRecord,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// One terminal observation competing to settle a pickup generation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    ProcessExitedSuccess,
    ProcessExitedFailure,
    ProcessSetupFailed,
    LivenessExpired,
    CancellationKilled,
    CancellationAlreadyExited,
    CancellationNoProcess,
}

/// The durable result of the per-generation terminal election.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ElectedAttemptOutcome {
    pub outcome: AttemptOutcome,
    pub event: Vec<u8>,
    pub event_enqueued: bool,
}

/// Durable reconciliation evidence for one stable cancellation identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskCancellationReconciliation {
    Killed,
    AlreadyExited,
    NoProcess,
}

/// Restartable all-NATS cancellation fence and acknowledgement outbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCancellationRecord {
    pub acknowledgement_identity: String,
    pub task_instance_id: String,
    pub workflow_instance_id: String,
    pub dispatch_key: Option<String>,
    pub pickup_generation: Option<i64>,
    pub owner: Option<String>,
    pub owner_notified: bool,
    pub reconciliation: Option<TaskCancellationReconciliation>,
    pub acknowledgement: Option<Vec<u8>>,
    pub acknowledgement_enqueued: bool,
}

/// Durable all-NATS pickup, liveness, and outcome evidence.
///
/// The record is adapter-local JSON in the versioned pickup KV bucket. It is
/// shared by Executor contenders and the Conductor sweeper so every terminal
/// path applies the same generation/owner fence and one conditional election.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskPickupRecord {
    pub dispatch_key: String,
    pub payload: Vec<u8>,
    pub pickup_generation: i64,
    pub owner: String,
    pub liveness_deadline_ms: i64,
    pub assigned_event: Vec<u8>,
    pub assigned_staged: bool,
    pub liveness_armed: bool,
    pub source_completed: bool,
    pub started_event: Option<Vec<u8>>,
    pub terminal: Option<ElectedAttemptOutcome>,
    pub rejected_reason: Option<String>,
}

/// Result of applying a terminal contender to a pickup record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionDecision {
    Won,
    Settled(AttemptOutcome),
    Rejected,
}

impl TaskPickupRecord {
    pub fn matches_claim(&self, dispatch_key: &str, generation: i64, owner: &str) -> bool {
        self.dispatch_key == dispatch_key
            && self.pickup_generation == generation
            && self.owner == owner
            && self.rejected_reason.is_none()
            && self.terminal.is_none()
    }

    pub fn liveness_is_due(&self, server_time_ms: i64) -> bool {
        self.liveness_armed
            && self.rejected_reason.is_none()
            && self.terminal.is_none()
            && self.liveness_deadline_ms <= server_time_ms
    }

    pub fn elect(
        &mut self,
        dispatch_key: &str,
        generation: i64,
        owner: &str,
        outcome: AttemptOutcome,
        event: &[u8],
    ) -> ElectionDecision {
        if let Some(elected) = &self.terminal {
            return ElectionDecision::Settled(elected.outcome);
        }
        let requires_started = matches!(
            outcome,
            AttemptOutcome::ProcessExitedSuccess | AttemptOutcome::ProcessExitedFailure
        );
        if !self.matches_claim(dispatch_key, generation, owner)
            || (requires_started && self.started_event.is_none())
        {
            return ElectionDecision::Rejected;
        }
        let event_enqueued = matches!(
            outcome,
            AttemptOutcome::CancellationKilled
                | AttemptOutcome::CancellationAlreadyExited
                | AttemptOutcome::CancellationNoProcess
        );
        self.terminal = Some(ElectedAttemptOutcome {
            outcome,
            event: event.to_vec(),
            event_enqueued,
        });
        ElectionDecision::Won
    }

    pub fn elect_due_liveness(&mut self, server_time_ms: i64, event: &[u8]) -> ElectionDecision {
        if let Some(elected) = &self.terminal {
            return ElectionDecision::Settled(elected.outcome);
        }
        if !self.liveness_is_due(server_time_ms) {
            return ElectionDecision::Rejected;
        }
        self.terminal = Some(ElectedAttemptOutcome {
            outcome: AttemptOutcome::LivenessExpired,
            event: event.to_vec(),
            event_enqueued: false,
        });
        ElectionDecision::Won
    }
}

/// Version shared by every hardened all-NATS Coordination protocol identity.
pub const PROTOCOL_VERSION: u16 = 2;

/// Exact marker stored in the fresh namespace before runtime resources start.
pub const FORMATION_IDENTITY: &str = "tickr.all-nats.hardened-protocol-set/v2";
pub const FORMATION_IDENTITY_BUCKET: &str = "TICKR_ALL_NATS_V2_FORMATION";
pub const FORMATION_IDENTITY_KEY: &str = "identity";

pub const COMMAND_SUBJECT: &str = "tickr.all_nats.v2.command_bus.requests";
pub const COMMAND_QUEUE_GROUP: &str = "tickr-all-nats-v2-command-bus";

pub const TASK_DISPATCH_STREAM: &str = "TICKR_ALL_NATS_V2_TASK_DISPATCH";
pub const TASK_DISPATCH_SUBJECT: &str = "tickr.all_nats.v2.task_dispatch.work";
pub const TASK_DISPATCH_CONSUMER: &str = "tickr-all-nats-v2-task-dispatch";
pub const TASK_PICKUP_BUCKET: &str = "TICKR_ALL_NATS_V2_TASK_PICKUP_HANDOFF";

pub const TASK_EVENT_STREAM: &str = "TICKR_ALL_NATS_V2_TASK_EVENTS";
pub const TASK_EVENT_SUBJECT: &str = "tickr.all_nats.v2.task_events.work";
pub const TASK_EVENT_CONSUMER: &str = "tickr-all-nats-v2-task-events";

pub const TASK_CANCEL_STREAM: &str = "TICKR_ALL_NATS_V2_TASK_CANCELLATION";
pub const TASK_CANCEL_SUBJECT: &str = "tickr.all_nats.v2.task_cancellation.requests";
pub const TASK_CANCEL_CONSUMER: &str = "tickr-all-nats-v2-task-cancellation";

pub const TASK_CANCEL_ACK_STREAM: &str = "TICKR_ALL_NATS_V2_TASK_CANCELLATION_ACKS";
pub const TASK_CANCEL_ACK_SUBJECT: &str = "tickr.all_nats.v2.task_cancellation.acks";
pub const TASK_CANCEL_ACK_CONSUMER: &str = "tickr-all-nats-v2-task-cancellation-acks";

pub const COMPACTION_STREAM: &str = "TICKR_ALL_NATS_V2_COMPACTION_STAGING";
pub const COMPACTION_SUBJECT: &str = "tickr.all_nats.v2.compaction_staging.work";
pub const COMPACTION_CONSUMER: &str = "tickr-all-nats-v2-compaction-staging";
pub const COMPACTION_STAGING_BUCKET: &str = "TICKR_ALL_NATS_V2_COMPACTION_IDENTITIES";
pub const COMPACTION_ACK_WAIT: Duration = Duration::from_secs(2);

pub const BUILD_QUEUE_SUBJECT: &str = "tickr.all_nats.v2.lifecycle_work.definition_build";
pub const BUILD_QUEUE_GROUP: &str = "tickr-all-nats-v2-definition-build";
pub const PATCH_BUILD_QUEUE_SUBJECT: &str = "tickr.all_nats.v2.lifecycle_work.patch_build";
pub const PATCH_BUILD_QUEUE_GROUP: &str = "tickr-all-nats-v2-patch-build";
pub const SUBMISSION_QUEUE_SUBJECT: &str = "tickr.all_nats.v2.lifecycle_work.submission";
pub const SUBMISSION_QUEUE_GROUP: &str = "tickr-all-nats-v2-submission";

pub const LOG_STREAM: &str = "TICKR_ALL_NATS_V2_LOG_STAGING";
pub const LOG_SUBJECT_PREFIX: &str = "tickr.all_nats.v2.log_staging";
pub const LOG_STREAM_SUBJECTS: &str = "tickr.all_nats.v2.log_staging.>";
pub const LOG_STREAM_MAX_BYTES: i64 = 1024 * 1024 * 1024;
pub const LOG_STREAM_DEDUP_WINDOW: Duration = Duration::from_secs(120);
pub const LOG_PROTOCOL_HEADER: &str = "Tickr-Log-Protocol";
pub const LOG_PROTOCOL: &str = "tickr-all-nats-log-stream-v1";
pub const LOG_KIND_HEADER: &str = "Tickr-Log-Kind";
pub const LOG_KIND_ACCEPTED: &str = "accepted";
pub const LOG_KIND_GAP: &str = "pre-acceptance-gap";
pub const LOG_KIND_END: &str = "end-of-stream";
pub const LOG_KIND_ABNORMAL: &str = "abnormal-closure";
pub const LOG_TASK_INSTANCE_HEADER: &str = "Tickr-Log-Task-Instance";
pub const LOG_PICKUP_GENERATION_HEADER: &str = "Tickr-Log-Pickup-Generation";
pub const LOG_SEQUENCE_HEADER: &str = "Tickr-Log-Sequence";
pub const LOG_CONTENT_DIGEST_HEADER: &str = "Tickr-Log-Content-Sha256";
pub const LOG_GAP_FIRST_HEADER: &str = "Tickr-Log-Gap-First";
pub const LOG_GAP_LAST_HEADER: &str = "Tickr-Log-Gap-Last";
pub const LOG_GAP_DROPPED_HEADER: &str = "Tickr-Log-Gap-Dropped";
pub const LOG_COMMITTED_FRONTIER_HEADER: &str = "Tickr-Log-Committed-Frontier";
pub const LOG_EXIT_KIND_HEADER: &str = "Tickr-Log-Exit-Kind";
pub const LOG_EXIT_STATUS_HEADER: &str = "Tickr-Log-Exit-Status";
pub const LOG_EXIT_REASON_HEADER: &str = "Tickr-Log-Exit-Reason";

/// Decode one accepted-Log protocol message without exposing NATS client types.
pub fn decode_log_record(
    mut header: impl FnMut(&str) -> Option<String>,
    payload: &[u8],
) -> Result<ReplayedLogRecord, String> {
    if header(LOG_PROTOCOL_HEADER).as_deref() != Some(LOG_PROTOCOL) {
        return Err("unknown all-NATS Log staging protocol".to_owned());
    }
    let task_instance_id = required_header(&mut header, LOG_TASK_INSTANCE_HEADER)?
        .parse::<Uuid>()
        .map_err(|error| format!("invalid Log task-instance identity: {error}"))?;
    let pickup_generation = parse_u64_header(&mut header, LOG_PICKUP_GENERATION_HEADER)?;
    let stream = LogStreamIdentity {
        task_instance_id,
        pickup_generation,
    };
    let kind = required_header(&mut header, LOG_KIND_HEADER)?;
    match kind.as_str() {
        LOG_KIND_ACCEPTED => {
            let sequence = parse_u64_header(&mut header, LOG_SEQUENCE_HEADER)?;
            let recorded_digest = required_header(&mut header, LOG_CONTENT_DIGEST_HEADER)?;
            if recorded_digest != content_digest(payload) {
                return Err("Accepted Log record content digest mismatch".to_owned());
            }
            Ok(ReplayedLogRecord::Accepted {
                identity: LogRecordIdentity { stream, sequence },
                content_digest: recorded_digest,
                bytes: payload.to_vec(),
            })
        }
        LOG_KIND_GAP => Ok(ReplayedLogRecord::PreAcceptanceGap(PreAcceptanceGap {
            stream,
            first_sequence: parse_u64_header(&mut header, LOG_GAP_FIRST_HEADER)?,
            last_sequence: parse_u64_header(&mut header, LOG_GAP_LAST_HEADER)?,
            dropped_records: parse_u64_header(&mut header, LOG_GAP_DROPPED_HEADER)?,
        })),
        LOG_KIND_END => {
            let exit = match required_header(&mut header, LOG_EXIT_KIND_HEADER)?.as_str() {
                "status" => LogExit::Status(
                    required_header(&mut header, LOG_EXIT_STATUS_HEADER)?
                        .parse::<i32>()
                        .map_err(|error| format!("invalid Log exit status: {error}"))?,
                ),
                "no-status" => LogExit::NoStatus,
                "error" => LogExit::Error(
                    header(LOG_EXIT_REASON_HEADER)
                        .unwrap_or_else(|| "Executor reported an unspecified error".to_owned()),
                ),
                value => return Err(format!("unknown Log exit kind `{value}`")),
            };
            Ok(ReplayedLogRecord::Terminal {
                stream,
                terminal: LogTerminal::EndOfStream { exit },
            })
        }
        LOG_KIND_ABNORMAL => Ok(ReplayedLogRecord::Terminal {
            stream,
            terminal: LogTerminal::AbnormalClosure {
                committed_frontier: header(LOG_COMMITTED_FRONTIER_HEADER)
                    .map(|value| {
                        value
                            .parse::<u64>()
                            .map_err(|error| format!("invalid Log committed frontier: {error}"))
                    })
                    .transpose()?,
            },
        }),
        value => Err(format!("unknown all-NATS Log record kind `{value}`")),
    }
}

fn required_header(
    header: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<String, String> {
    header(name).ok_or_else(|| format!("missing required Log header `{name}`"))
}

fn parse_u64_header(
    header: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<u64, String> {
    required_header(header, name)?
        .parse::<u64>()
        .map_err(|error| format!("invalid Log header `{name}`: {error}"))
}

pub const SCOPE_BUCKET_PREFIX: &str = "TICKR_ALL_NATS_V2_SCOPE_";
pub const DEFAULT_SCOPE_BUCKET: &str = "TICKR_ALL_NATS_V2_SCOPE_default";
pub const SCOPE_MAX_VALUE_SIZE: i32 = 1024 * 1024;

pub const INGRESS_IDEMPOTENCY_BUCKET: &str = "TICKR_ALL_NATS_V2_INGRESS_IDEMPOTENCY";
pub const INGRESS_IDEMPOTENCY_TTL: Duration = Duration::from_secs(10 * 60);

pub const LIVENESS_BUCKET: &str = "TICKR_ALL_NATS_V2_LIVENESS_WATCHDOG";
pub const LIVENESS_MARKER_CONSUMER: &str = "tickr-all-nats-v2-liveness-watchdog";

pub const SIGNAL_APPLIED_SUBJECT_PREFIX: &str = "tickr.all_nats.v2.signal_applied_notifier.applied";

pub const COMPONENT_LIVENESS_BUCKET: &str = "TICKR_ALL_NATS_V2_EXECUTOR_FLEET_STATUS";
pub const COMPONENT_MARKER_TTL: Duration = Duration::from_secs(60);

pub const EVENT_INGRESS_STREAM: &str = "TICKR_ALL_NATS_V2_EVENT_INGRESS";
pub const EVENT_INGRESS_SUBJECT: &str = "tickr.all_nats.v2.event_ingress.events";
pub const EVENT_INGRESS_CONSUMER: &str = "tickr-all-nats-v2-event-ingress";

/// Static streams admitted before any all-NATS runtime consumer or producer.
pub const STREAM_NAMES: [&str; 7] = [
    TASK_DISPATCH_STREAM,
    TASK_EVENT_STREAM,
    TASK_CANCEL_STREAM,
    TASK_CANCEL_ACK_STREAM,
    COMPACTION_STREAM,
    LOG_STREAM,
    EVENT_INGRESS_STREAM,
];

/// Static durable consumers admitted before any all-NATS runtime loop starts.
pub const CONSUMER_NAMES: [&str; 7] = [
    TASK_DISPATCH_CONSUMER,
    TASK_EVENT_CONSUMER,
    TASK_CANCEL_CONSUMER,
    TASK_CANCEL_ACK_CONSUMER,
    COMPACTION_CONSUMER,
    LIVENESS_MARKER_CONSUMER,
    EVENT_INGRESS_CONSUMER,
];

/// Static KV buckets admitted before any all-NATS runtime claim or listener.
pub const KV_BUCKET_NAMES: [&str; 7] = [
    FORMATION_IDENTITY_BUCKET,
    DEFAULT_SCOPE_BUCKET,
    INGRESS_IDEMPOTENCY_BUCKET,
    LIVENESS_BUCKET,
    TASK_PICKUP_BUCKET,
    COMPACTION_STAGING_BUCKET,
    COMPONENT_LIVENESS_BUCKET,
];
