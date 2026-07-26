#![allow(async_fn_in_trait)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_nats::jetstream::{self, kv};
use async_nats::{Client as NatsClient, HeaderMap};
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use tickr_proto::coord::all_nats::{
    AttemptOutcome as NatsAttemptOutcome, ElectedAttemptOutcome as NatsElectedAttemptOutcome,
    ElectionDecision, TaskCancellationReconciliation, TaskCancellationRecord, TaskPickupRecord,
};
use tickr_proto::coord::{
    liveness_key, TaskEventFuture, TaskEventWriter, LIVENESS_BUCKET, TASK_CANCEL_ACK_SUBJECT,
    TASK_EVENT_STREAM, TASK_EVENT_SUBJECT,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::local_pickup::{
    CancellationReconciliation, ClaimLocalPickup, ClaimWriteError, ElectedAttemptOutcome,
    LocalAttemptOutcome, LocalCancellationFence, LocalPickupClaim, PendingLocalDispatch,
    SafeAttemptOutcomeHandoff, SafeCancellationFence, SafeCancellationRole, SafeLivenessWatchdog,
    SafePickupWriter, TerminalElection,
};
use crate::wire::{decode_dispatch, encode_unhealthy_task_event, CancelRequest};

const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";
const PICKUP_BUCKET: &str = tickr_proto::coord::all_nats::TASK_PICKUP_BUCKET;

/// Fresh all-NATS TaskEvents producer. The Executor and pickup choreography see
/// only [`TaskEventWriter`]; JetStream resource and header details stay here.
#[derive(Clone)]
pub struct NatsTaskEventWriter {
    js: jetstream::Context,
}

impl NatsTaskEventWriter {
    pub fn new(nats: &NatsClient) -> Self {
        Self {
            js: jetstream::new(nats.clone()),
        }
    }
}

impl TaskEventWriter for NatsTaskEventWriter {
    fn prepare(&self) -> TaskEventFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.js
                .get_or_create_stream(jetstream::stream::Config {
                    name: TASK_EVENT_STREAM.to_owned(),
                    subjects: vec![TASK_EVENT_SUBJECT.to_owned()],
                    retention: jetstream::stream::RetentionPolicy::WorkQueue,
                    ..Default::default()
                })
                .await
                .map(|_| ())
                .map_err(|error| format!("get_or_create task-event stream: {error}"))
        })
    }

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_task_event: &'a [u8],
    ) -> TaskEventFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mut headers = HeaderMap::new();
            headers.insert(MESSAGE_ID_HEADER, identity);
            self.js
                .publish_with_headers(
                    TASK_EVENT_SUBJECT,
                    headers,
                    encoded_task_event.to_vec().into(),
                )
                .await
                .map_err(|error| format!("stage TaskEvent: {error}"))?
                .await
                .map_err(|error| format!("prove staged TaskEvent: {error}"))?;
            Ok(())
        })
    }
}

trait PickupRecordExt {
    fn claim(&self) -> LocalPickupClaim;
    fn matches(&self, claim: &LocalPickupClaim) -> bool;
}

impl PickupRecordExt for TaskPickupRecord {
    fn claim(&self) -> LocalPickupClaim {
        LocalPickupClaim {
            dispatch_key: self.dispatch_key.clone(),
            pickup_generation: self.pickup_generation,
            owner: self.owner.clone(),
            liveness_deadline: DateTime::from_timestamp_millis(self.liveness_deadline_ms)
                .expect("NATS server timestamp must fit DateTime<Utc>"),
        }
    }

    fn matches(&self, claim: &LocalPickupClaim) -> bool {
        self.matches_claim(&claim.dispatch_key, claim.pickup_generation, &claim.owner)
    }
}

#[derive(Clone)]
pub struct NatsPickupHandoff {
    js: jetstream::Context,
    task_events: Arc<dyn TaskEventWriter>,
    pickup: kv::Store,
    liveness: Option<kv::Store>,
    source: Arc<Mutex<jetstream::Message>>,
    dispatch_key: String,
    payload: Vec<u8>,
    created_here: Arc<AtomicBool>,
}

impl NatsPickupHandoff {
    pub async fn from_message(
        nats: &NatsClient,
        pickup: kv::Store,
        liveness: Option<kv::Store>,
        message: jetstream::Message,
    ) -> Result<Self, String> {
        Self::from_message_with_task_events(
            nats,
            pickup,
            liveness,
            message,
            Arc::new(NatsTaskEventWriter::new(nats)),
        )
        .await
    }

    pub async fn from_message_with_task_events(
        nats: &NatsClient,
        pickup: kv::Store,
        liveness: Option<kv::Store>,
        message: jetstream::Message,
        task_events: Arc<dyn TaskEventWriter>,
    ) -> Result<Self, String> {
        let info = message
            .info()
            .map_err(|error| format!("read TaskDispatch delivery identity: {error}"))?;
        let dispatch_key = format!("dispatch.{}", info.stream_sequence);
        let payload = message.payload.to_vec();
        Ok(Self {
            js: jetstream::new(nats.clone()),
            task_events,
            pickup,
            liveness,
            source: Arc::new(Mutex::new(message)),
            dispatch_key,
            payload,
            created_here: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn load(&self) -> Result<Option<(TaskPickupRecord, u64, DateTime<Utc>)>, String> {
        let Some(entry) = self
            .pickup
            .entry(&self.dispatch_key)
            .await
            .map_err(|error| format!("read pickup operation `{}`: {error}", self.dispatch_key))?
        else {
            return Ok(None);
        };
        let record = serde_json::from_slice(&entry.value)
            .map_err(|error| format!("decode pickup operation `{}`: {error}", self.dispatch_key))?;
        let nanos = entry.created.unix_timestamp_nanos();
        let millis = i64::try_from(nanos / 1_000_000)
            .map_err(|_| "NATS server time does not fit DateTime<Utc>".to_owned())?;
        let server_time = DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| "NATS server time does not fit DateTime<Utc>".to_owned())?;
        Ok(Some((record, entry.revision, server_time)))
    }

    async fn cancellation_for_payload(
        &self,
    ) -> Result<Option<(String, TaskCancellationRecord, u64)>, String> {
        let Ok(task) = decode_dispatch(&self.payload) else {
            return Ok(None);
        };
        let request = CancelRequest {
            task_instance_id: task.task_instance_id,
            workflow_instance_id: task.workflow_instance_id,
        };
        let key = cancellation_key(request);
        let Some(entry) = self
            .pickup
            .entry(&key)
            .await
            .map_err(|error| format!("read queued cancellation fence `{key}`: {error}"))?
        else {
            return Ok(None);
        };
        let record = serde_json::from_slice(&entry.value)
            .map_err(|error| format!("decode queued cancellation fence `{key}`: {error}"))?;
        Ok(Some((key, record, entry.revision)))
    }

    async fn complete_cancelled_before_claim(&self) -> Result<bool, String> {
        let Some((key, mut cancellation, cancellation_revision)) =
            self.cancellation_for_payload().await?
        else {
            return Ok(false);
        };
        let mut pickup = TaskPickupRecord {
            dispatch_key: self.dispatch_key.clone(),
            payload: self.payload.clone(),
            pickup_generation: 1,
            owner: String::new(),
            liveness_deadline_ms: 0,
            assigned_event: Vec::new(),
            assigned_staged: false,
            liveness_armed: false,
            source_completed: false,
            started_event: None,
            terminal: Some(NatsElectedAttemptOutcome {
                outcome: NatsAttemptOutcome::CancellationNoProcess,
                event: Vec::new(),
                event_enqueued: true,
            }),
            rejected_reason: None,
        };
        if self.load().await?.is_none() {
            self.create_record(&pickup).await?;
        } else if let Some((existing, _, _)) = self.load().await? {
            pickup = existing;
        }
        if cancellation.dispatch_key.is_none() {
            cancellation.dispatch_key = Some(pickup.dispatch_key.clone());
            cancellation.pickup_generation = Some(pickup.pickup_generation);
            cancellation.owner = None;
            let _ = self
                .pickup
                .update(
                    &key,
                    serde_json::to_vec(&cancellation)
                        .map_err(|error| format!("encode queued cancellation binding: {error}"))?
                        .into(),
                    cancellation_revision,
                )
                .await;
        }
        if !pickup.source_completed {
            self.acknowledge_source().await?;
            if let Some((mut current, revision, _)) = self.load().await? {
                current.source_completed = true;
                self.update_record(&current, revision).await?;
            }
        }
        Ok(true)
    }

    async fn create_record(&self, record: &TaskPickupRecord) -> Result<(), String> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| format!("encode pickup operation: {error}"))?;
        self.pickup
            .create(&self.dispatch_key, bytes.into())
            .await
            .map_err(|error| format!("create pickup operation `{}`: {error}", self.dispatch_key))?;
        self.created_here.store(true, Ordering::Release);
        Ok(())
    }

    async fn update_record(&self, record: &TaskPickupRecord, revision: u64) -> Result<(), String> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| format!("encode pickup operation: {error}"))?;
        self.pickup
            .update(&self.dispatch_key, bytes.into(), revision)
            .await
            .map(|_| ())
            .map_err(|error| format!("update pickup operation `{}`: {error}", self.dispatch_key))
    }

    async fn publish_event(&self, kind: &str, payload: &[u8]) -> Result<(), String> {
        let identity = format!("{}.{kind}", self.dispatch_key);
        self.task_events.stage(&identity, payload).await
    }

    async fn ensure_assigned_staged(&self) -> Result<TaskPickupRecord, String> {
        let Some((mut record, revision, _)) = self.load().await? else {
            return Err(format!(
                "pickup operation `{}` is missing",
                self.dispatch_key
            ));
        };
        if record.rejected_reason.is_some() {
            return Err("rejected dispatch cannot stage Assigned".to_owned());
        }
        if !record.assigned_staged {
            self.publish_event("Assigned", &record.assigned_event)
                .await?;
            record.assigned_staged = true;
            self.update_record(&record, revision).await?;
        }
        Ok(record)
    }

    async fn set_server_deadline(
        &self,
        claim: &LocalPickupClaim,
        timeout: Duration,
        arm: bool,
    ) -> Result<Option<LocalPickupClaim>, String> {
        let Some((mut record, revision, server_time)) = self.load().await? else {
            return Ok(None);
        };
        if !record.matches(claim) || !record.assigned_staged {
            return Ok(None);
        }
        record.liveness_deadline_ms = (server_time + timeout).timestamp_millis();
        record.liveness_armed |= arm;
        self.update_record(&record, revision).await?;

        let Some((record, _, _)) = self.load().await? else {
            return Ok(None);
        };
        if !record.matches(claim) {
            return Ok(None);
        }
        if arm {
            if self.liveness.is_some() {
                let task = decode_dispatch(&record.payload).map_err(|error| {
                    format!("decode claimed TaskDispatch for liveness: {error}")
                })?;
                let key = liveness_key(
                    task.workflow_id,
                    task.workflow_instance_id,
                    task.task_instance_id,
                );
                let value = serde_json::to_vec(&record)
                    .map_err(|error| format!("encode generation-qualified liveness: {error}"))?;
                let subject = format!("$KV.{LIVENESS_BUCKET}.{key}");
                let mut headers = HeaderMap::new();
                headers.insert(
                    async_nats::header::NATS_MESSAGE_TTL,
                    timeout.num_seconds().max(1).to_string().as_str(),
                );
                let wakeup = async {
                    self.js
                        .publish_with_headers(subject, headers, value.into())
                        .await
                        .map_err(|error| format!("publish optional liveness wakeup: {error}"))?
                        .await
                        .map_err(|error| format!("confirm optional liveness wakeup: {error}"))
                }
                .await;
                if let Err(error) = wakeup {
                    eprintln!("{error}; durable pickup deadline remains authoritative");
                }
            }
        }
        Ok(Some(record.claim()))
    }

    async fn acknowledge_source(&self) -> Result<(), String> {
        self.source
            .lock()
            .await
            .ack()
            .await
            .map_err(|error| format!("acknowledge proved TaskDispatch: {error}"))
    }

    async fn recover_without_launch(&self, record: TaskPickupRecord) -> Result<(), String> {
        if self.cancellation_for_payload().await?.is_some() {
            if record.terminal.is_none() {
                return Err("cancellation fence is awaiting terminal reconciliation".to_owned());
            }
            self.acknowledge_source().await?;
            if !record.source_completed {
                if let Some((mut current, revision, _)) = self.load().await? {
                    current.source_completed = true;
                    self.update_record(&current, revision).await?;
                }
            }
            return Ok(());
        }
        if record.rejected_reason.is_none() {
            let record = self.ensure_assigned_staged().await?;
            if !record.liveness_armed {
                let claim = record.claim();
                let timeout = Duration::milliseconds(
                    (record.liveness_deadline_ms - Utc::now().timestamp_millis()).max(1),
                );
                let _ = self.set_server_deadline(&claim, timeout, true).await?;
            }
        }
        self.acknowledge_source().await?;
        if let Some((mut current, revision, _)) = self.load().await? {
            current.source_completed = true;
            let _ = self.update_record(&current, revision).await;
        }
        Ok(())
    }

    pub async fn stage_started(
        &self,
        claim: &LocalPickupClaim,
        started_event: &[u8],
    ) -> Result<bool, String> {
        let Some((mut record, revision, _)) = self.load().await? else {
            return Ok(false);
        };
        if !record.matches(claim)
            || !record.assigned_staged
            || !record.liveness_armed
            || !record.source_completed
        {
            return Ok(false);
        }
        self.publish_event("Started", started_event).await?;
        record.started_event = Some(started_event.to_vec());
        self.update_record(&record, revision).await?;
        Ok(true)
    }

    pub async fn renew(&self, claim: &LocalPickupClaim, timeout: Duration) -> Result<bool, String> {
        Ok(self
            .set_server_deadline(claim, timeout, true)
            .await?
            .is_some())
    }

    pub fn outcome_election(&self) -> NatsOutcomeElection {
        NatsOutcomeElection::new(self.pickup.clone())
    }

    pub async fn stop_liveness(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        let Some(liveness) = &self.liveness else {
            return Ok(true);
        };
        let Some((record, _, _)) = self.load().await? else {
            return Ok(false);
        };
        if record.dispatch_key != claim.dispatch_key
            || record.pickup_generation != claim.pickup_generation
            || record.owner != claim.owner
        {
            return Ok(false);
        }
        let task = decode_dispatch(&record.payload)
            .map_err(|error| format!("decode claimed TaskDispatch for liveness stop: {error}"))?;
        let key = liveness_key(
            task.workflow_id,
            task.workflow_instance_id,
            task.task_instance_id,
        );
        let value = liveness
            .get(&key)
            .await
            .map_err(|error| format!("read optional liveness wakeup: {error}"))?;
        if let Some(value) = value {
            let live: TaskPickupRecord = serde_json::from_slice(&value)
                .map_err(|error| format!("decode generation-qualified liveness: {error}"))?;
            if live.dispatch_key != claim.dispatch_key
                || live.pickup_generation != claim.pickup_generation
                || live.owner != claim.owner
            {
                return Ok(false);
            }
            liveness
                .delete(&key)
                .await
                .map_err(|error| format!("stop optional liveness wakeup: {error}"))?;
        }
        Ok(true)
    }
}

const OUTCOME_ELECTION_RETRIES: usize = 8;
const OUTCOME_SWEEP_SCAN_LIMIT: usize = 64;

#[derive(Clone)]
pub struct NatsOutcomeElection {
    pickup: kv::Store,
}

impl NatsOutcomeElection {
    pub fn new(pickup: kv::Store) -> Self {
        Self { pickup }
    }

    async fn load(
        &self,
        key: &str,
    ) -> Result<Option<(TaskPickupRecord, u64, DateTime<Utc>)>, String> {
        let Some(entry) = self
            .pickup
            .entry(key)
            .await
            .map_err(|error| format!("read pickup outcome `{key}`: {error}"))?
        else {
            return Ok(None);
        };
        let record = serde_json::from_slice(&entry.value)
            .map_err(|error| format!("decode pickup outcome `{key}`: {error}"))?;
        let millis = i64::try_from(entry.created.unix_timestamp_nanos() / 1_000_000)
            .map_err(|_| "NATS server time does not fit DateTime<Utc>".to_owned())?;
        let server_time = DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| "NATS server time does not fit DateTime<Utc>".to_owned())?;
        Ok(Some((record, entry.revision, server_time)))
    }

    async fn update(
        &self,
        key: &str,
        record: &TaskPickupRecord,
        revision: u64,
    ) -> Result<(), String> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| format!("encode pickup outcome `{key}`: {error}"))?;
        self.pickup
            .update(key, bytes.into(), revision)
            .await
            .map(|_| ())
            .map_err(|error| format!("update pickup outcome `{key}`: {error}"))
    }

    pub async fn server_time(&self) -> Result<DateTime<Utc>, String> {
        let key = format!("watchdog.clock.{}", Uuid::new_v4().simple());
        self.pickup
            .put(&key, Vec::new().into())
            .await
            .map_err(|error| format!("write NATS server-time probe: {error}"))?;
        let entry = self
            .pickup
            .entry(&key)
            .await
            .map_err(|error| format!("read NATS server-time probe: {error}"))?
            .ok_or_else(|| "NATS server-time probe disappeared".to_owned())?;
        let _ = self.pickup.delete(&key).await;
        let millis = i64::try_from(entry.created.unix_timestamp_nanos() / 1_000_000)
            .map_err(|_| "NATS server time does not fit DateTime<Utc>".to_owned())?;
        DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| "NATS server time does not fit DateTime<Utc>".to_owned())
    }

    pub async fn sweep_one_due(
        &self,
    ) -> Result<Option<(LocalPickupClaim, TerminalElection)>, String> {
        let server_time = self.server_time().await?;
        let Some(due) = self.select_due_liveness(server_time).await? else {
            return Ok(None);
        };
        let task = decode_dispatch(&due.payload)
            .map_err(|error| format!("decode due claimed TaskDispatch: {error}"))?;
        let event = encode_unhealthy_task_event(&task);
        let election = self
            .elect_terminal(
                &due.claim,
                LocalAttemptOutcome::LivenessExpired,
                &event,
                server_time,
            )
            .await?;
        Ok(Some((due.claim, election)))
    }
}

fn to_nats_outcome(outcome: LocalAttemptOutcome) -> NatsAttemptOutcome {
    match outcome {
        LocalAttemptOutcome::ProcessExitedSuccess => NatsAttemptOutcome::ProcessExitedSuccess,
        LocalAttemptOutcome::ProcessExitedFailure => NatsAttemptOutcome::ProcessExitedFailure,
        LocalAttemptOutcome::ProcessSetupFailed => NatsAttemptOutcome::ProcessSetupFailed,
        LocalAttemptOutcome::LivenessExpired => NatsAttemptOutcome::LivenessExpired,
        LocalAttemptOutcome::CancellationKilled => NatsAttemptOutcome::CancellationKilled,
        LocalAttemptOutcome::CancellationAlreadyExited => {
            NatsAttemptOutcome::CancellationAlreadyExited
        }
        LocalAttemptOutcome::CancellationNoProcess => NatsAttemptOutcome::CancellationNoProcess,
    }
}

fn from_nats_outcome(outcome: NatsAttemptOutcome) -> LocalAttemptOutcome {
    match outcome {
        NatsAttemptOutcome::ProcessExitedSuccess => LocalAttemptOutcome::ProcessExitedSuccess,
        NatsAttemptOutcome::ProcessExitedFailure => LocalAttemptOutcome::ProcessExitedFailure,
        NatsAttemptOutcome::ProcessSetupFailed => LocalAttemptOutcome::ProcessSetupFailed,
        NatsAttemptOutcome::LivenessExpired => LocalAttemptOutcome::LivenessExpired,
        NatsAttemptOutcome::CancellationKilled => LocalAttemptOutcome::CancellationKilled,
        NatsAttemptOutcome::CancellationAlreadyExited => {
            LocalAttemptOutcome::CancellationAlreadyExited
        }
        NatsAttemptOutcome::CancellationNoProcess => LocalAttemptOutcome::CancellationNoProcess,
    }
}

#[async_trait::async_trait]
impl SafeAttemptOutcomeHandoff for NatsOutcomeElection {
    async fn select_due_liveness(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<crate::local_pickup::DueLocalPickup>, String> {
        let mut keys = self
            .pickup
            .keys()
            .await
            .map_err(|error| format!("list all-NATS pickup deadlines: {error}"))?;
        let mut scanned = 0;
        while scanned < OUTCOME_SWEEP_SCAN_LIMIT {
            let Some(key) = keys.next().await else {
                break;
            };
            let key = key.map_err(|error| format!("scan all-NATS pickup deadline: {error}"))?;
            if !key.starts_with("dispatch.") {
                continue;
            }
            scanned += 1;
            let Some((record, _, _)) = self.load(&key).await? else {
                continue;
            };
            if record.liveness_is_due(now.timestamp_millis()) {
                return Ok(Some(crate::local_pickup::DueLocalPickup {
                    claim: record.claim(),
                    payload: record.payload,
                }));
            }
        }
        Ok(None)
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision, _)) = self.load(&claim.dispatch_key).await? else {
                return Ok(false);
            };
            if !record.matches(claim) {
                return Ok(false);
            }
            record.liveness_deadline_ms = now.timestamp_millis();
            if self
                .update(&claim.dispatch_key, &record, revision)
                .await
                .is_ok()
            {
                return Ok(true);
            }
        }
        Err(format!(
            "liveness failure registration kept losing generation election for `{}`",
            claim.dispatch_key
        ))
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<TerminalElection, String> {
        let outcome = to_nats_outcome(outcome);
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision, _)) = self.load(&claim.dispatch_key).await? else {
                return Err(format!(
                    "pickup operation `{}` is missing",
                    claim.dispatch_key
                ));
            };
            match record.elect(
                &claim.dispatch_key,
                claim.pickup_generation,
                &claim.owner,
                outcome,
                terminal_event,
            ) {
                ElectionDecision::Settled(elected) => {
                    return Ok(TerminalElection::Settled(from_nats_outcome(elected)));
                }
                ElectionDecision::Rejected => {
                    return Err(format!(
                        "terminal election rejected stale or non-owner pickup generation {}",
                        claim.pickup_generation
                    ));
                }
                ElectionDecision::Won => {}
            }
            if self
                .update(&claim.dispatch_key, &record, revision)
                .await
                .is_ok()
            {
                return Ok(TerminalElection::Won);
            }
        }
        Err(format!(
            "terminal election kept losing conditional updates for `{}`",
            claim.dispatch_key
        ))
    }
}

#[async_trait::async_trait]
impl SafePickupWriter for NatsPickupHandoff {
    async fn select_pending(&self) -> Result<Option<PendingLocalDispatch>, String> {
        if let Some((record, _, _)) = self.load().await? {
            self.recover_without_launch(record).await?;
            return Ok(None);
        }
        if self.complete_cancelled_before_claim().await? {
            return Ok(None);
        }
        Ok(Some(PendingLocalDispatch {
            dispatch_key: self.dispatch_key.clone(),
            payload: self.payload.clone(),
        }))
    }

    async fn reject_poison(
        &self,
        dispatch_key: &str,
        reason: &str,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        if dispatch_key != self.dispatch_key {
            return Ok(false);
        }
        let record = TaskPickupRecord {
            dispatch_key: dispatch_key.to_owned(),
            payload: self.payload.clone(),
            pickup_generation: 0,
            owner: String::new(),
            liveness_deadline_ms: 0,
            assigned_event: Vec::new(),
            assigned_staged: false,
            liveness_armed: false,
            source_completed: false,
            started_event: None,
            terminal: None,
            rejected_reason: Some(reason.to_owned()),
        };
        self.create_record(&record).await?;
        self.acknowledge_source().await?;
        if let Some((mut current, revision, _)) = self.load().await? {
            current.source_completed = true;
            let _ = self.update_record(&current, revision).await;
        }
        Ok(true)
    }

    async fn claim(
        &self,
        input: ClaimLocalPickup<'_>,
    ) -> Result<Option<LocalPickupClaim>, ClaimWriteError> {
        if input.dispatch_key != self.dispatch_key {
            return Ok(None);
        }
        if self
            .complete_cancelled_before_claim()
            .await
            .map_err(ClaimWriteError::Failed)?
        {
            return Ok(None);
        }
        if self
            .load()
            .await
            .map_err(ClaimWriteError::Failed)?
            .is_some()
        {
            return Ok(None);
        }
        let record = TaskPickupRecord {
            dispatch_key: input.dispatch_key.to_owned(),
            payload: self.payload.clone(),
            pickup_generation: 1,
            owner: input.owner.to_owned(),
            liveness_deadline_ms: input.liveness_deadline.timestamp_millis(),
            assigned_event: input.assigned_event.to_vec(),
            assigned_staged: false,
            liveness_armed: false,
            source_completed: false,
            started_event: None,
            terminal: None,
            rejected_reason: None,
        };
        self.create_record(&record)
            .await
            .map_err(ClaimWriteError::Failed)?;
        let claim = record.claim();
        let timeout = input.liveness_deadline - input.now;
        self.set_server_deadline(&claim, timeout, false)
            .await
            .map_err(|_| ClaimWriteError::Ambiguous)?;
        self.ensure_assigned_staged()
            .await
            .map_err(|_| ClaimWriteError::Ambiguous)?;
        self.load()
            .await
            .map_err(|_| ClaimWriteError::Ambiguous)?
            .map(|(record, _, _)| record.claim())
            .ok_or(ClaimWriteError::Ambiguous)
            .map(Some)
    }

    async fn prove_ambiguous_claim(
        &self,
        dispatch_key: &str,
        owner: &str,
        assigned_event: &[u8],
    ) -> Result<Option<LocalPickupClaim>, String> {
        if dispatch_key != self.dispatch_key || !self.created_here.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some((record, _, _)) = self.load().await? else {
            return Ok(None);
        };
        if record.owner != owner || record.assigned_event != assigned_event {
            return Ok(None);
        }
        let record = self.ensure_assigned_staged().await?;
        Ok(Some(record.claim()))
    }

    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        _payload: &[u8],
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        Ok(self
            .set_server_deadline(claim, deadline - now, true)
            .await?
            .is_some())
    }

    async fn prove_ready_to_launch(
        &self,
        claim: &LocalPickupClaim,
        assigned_event: &[u8],
    ) -> Result<bool, String> {
        if self.cancellation_for_payload().await?.is_some() {
            return Ok(false);
        }
        let Some((record, _, _)) = self.load().await? else {
            return Ok(false);
        };
        Ok(record.matches(claim)
            && record.assigned_event == assigned_event
            && record.assigned_staged
            && record.liveness_armed)
    }

    async fn complete_source(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        if self.cancellation_for_payload().await?.is_some() {
            return Ok(false);
        }
        let Some((record, _, _)) = self.load().await? else {
            return Ok(false);
        };
        if !record.matches(claim) || !record.assigned_staged || !record.liveness_armed {
            return Ok(false);
        }
        self.acknowledge_source().await?;
        if let Some((mut current, revision, _)) = self.load().await? {
            if current.matches(claim) {
                current.source_completed = true;
                self.update_record(&current, revision).await?;
            }
        }
        Ok(true)
    }

    async fn stage_started(
        &self,
        claim: &LocalPickupClaim,
        started_event: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        self.stage_started(claim, started_event).await
    }

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        self.renew(claim, deadline - now).await
    }

    async fn stop_liveness(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        NatsPickupHandoff::stop_liveness(self, claim).await
    }
}

#[async_trait::async_trait]
impl SafeAttemptOutcomeHandoff for NatsPickupHandoff {
    async fn select_due_liveness(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<crate::local_pickup::DueLocalPickup>, String> {
        self.outcome_election().select_due_liveness(now).await
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        self.outcome_election()
            .register_liveness_failure(claim, now)
            .await
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<TerminalElection, String> {
        self.outcome_election()
            .elect_terminal(claim, outcome, terminal_event, now)
            .await
    }
}

#[async_trait::async_trait]
impl SafeLivenessWatchdog for NatsPickupHandoff {
    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        payload: &[u8],
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        SafePickupWriter::arm_liveness(self, claim, payload, deadline, now).await
    }

    async fn prove_liveness_armed(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        Ok(self
            .load()
            .await?
            .map(|(record, _, _)| record.matches(claim) && record.liveness_armed)
            .unwrap_or(false))
    }

    async fn complete_source(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        SafePickupWriter::complete_source(self, claim).await
    }

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        SafePickupWriter::renew_liveness(self, claim, deadline, now).await
    }

    async fn select_due_liveness(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<crate::local_pickup::DueLocalPickup>, String> {
        SafeAttemptOutcomeHandoff::select_due_liveness(self, now).await
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        SafeAttemptOutcomeHandoff::register_liveness_failure(self, claim, now).await
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<ElectedAttemptOutcome, String> {
        let election =
            SafeAttemptOutcomeHandoff::elect_terminal(self, claim, outcome, terminal_event, now)
                .await?;
        let record = self
            .load()
            .await?
            .map(|(record, _, _)| record)
            .ok_or_else(|| "elected all-NATS pickup record is missing".to_owned())?;
        let terminal = record
            .terminal
            .ok_or_else(|| "elected all-NATS terminal outcome is missing".to_owned())?;
        Ok(ElectedAttemptOutcome {
            election,
            outcome: from_nats_outcome(terminal.outcome),
            terminal_event: terminal.event,
        })
    }
}

const CANCELLATION_PREFIX: &str = "cancellation.";
const CANCELLATION_SCAN_LIMIT: usize = 256;

pub fn cancellation_acknowledgement_identity(request: CancelRequest) -> String {
    format!(
        "cancel-task-ack-v1:{}:{}",
        request.workflow_instance_id, request.task_instance_id
    )
}

pub fn cancellation_owner_subject(owner: &str) -> String {
    format!("tickr.all_nats.v2.task_cancel.owner.{owner}")
}

fn cancellation_key(request: CancelRequest) -> String {
    format!(
        "{CANCELLATION_PREFIX}{}.{}",
        request.workflow_instance_id.simple(),
        request.task_instance_id.simple()
    )
}

fn to_record_reconciliation(
    reconciliation: CancellationReconciliation,
) -> TaskCancellationReconciliation {
    match reconciliation {
        CancellationReconciliation::Killed => TaskCancellationReconciliation::Killed,
        CancellationReconciliation::AlreadyExited => TaskCancellationReconciliation::AlreadyExited,
        CancellationReconciliation::NoProcess => TaskCancellationReconciliation::NoProcess,
    }
}

fn cancellation_outcome(reconciliation: CancellationReconciliation) -> LocalAttemptOutcome {
    match reconciliation {
        CancellationReconciliation::Killed => LocalAttemptOutcome::CancellationKilled,
        CancellationReconciliation::AlreadyExited => LocalAttemptOutcome::CancellationAlreadyExited,
        CancellationReconciliation::NoProcess => LocalAttemptOutcome::CancellationNoProcess,
    }
}

#[derive(Clone)]
pub struct NatsCancellationFence {
    js: jetstream::Context,
    pickup: kv::Store,
}

impl NatsCancellationFence {
    pub fn new(nats: &NatsClient, pickup: kv::Store) -> Self {
        Self {
            js: jetstream::new(nats.clone()),
            pickup,
        }
    }

    async fn load_record(
        &self,
        key: &str,
    ) -> Result<Option<(TaskCancellationRecord, u64)>, String> {
        let Some(entry) = self
            .pickup
            .entry(key)
            .await
            .map_err(|error| format!("read cancellation fence `{key}`: {error}"))?
        else {
            return Ok(None);
        };
        let record = serde_json::from_slice(&entry.value)
            .map_err(|error| format!("decode cancellation fence `{key}`: {error}"))?;
        Ok(Some((record, entry.revision)))
    }

    async fn update_record(
        &self,
        key: &str,
        record: &TaskCancellationRecord,
        revision: u64,
    ) -> Result<(), String> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| format!("encode cancellation fence `{key}`: {error}"))?;
        self.pickup
            .update(key, bytes.into(), revision)
            .await
            .map(|_| ())
            .map_err(|error| format!("update cancellation fence `{key}`: {error}"))
    }

    async fn find_current_pickup(
        &self,
        request: CancelRequest,
    ) -> Result<Option<TaskPickupRecord>, String> {
        let mut keys = self
            .pickup
            .keys()
            .await
            .map_err(|error| format!("list pickup generations for cancellation: {error}"))?;
        let mut scanned = 0;
        let mut current: Option<(u64, TaskPickupRecord)> = None;
        while scanned < CANCELLATION_SCAN_LIMIT {
            let Some(key) = keys.next().await else {
                break;
            };
            let key =
                key.map_err(|error| format!("scan pickup generation for cancellation: {error}"))?;
            if !key.starts_with("dispatch.") {
                continue;
            }
            scanned += 1;
            let Some(entry) = self
                .pickup
                .entry(&key)
                .await
                .map_err(|error| format!("read pickup generation `{key}`: {error}"))?
            else {
                continue;
            };
            let pickup: TaskPickupRecord = serde_json::from_slice(&entry.value)
                .map_err(|error| format!("decode pickup generation `{key}`: {error}"))?;
            let Ok(task) = decode_dispatch(&pickup.payload) else {
                continue;
            };
            if task.task_instance_id != request.task_instance_id
                || task.workflow_instance_id != request.workflow_instance_id
            {
                continue;
            }
            let sequence = key
                .strip_prefix("dispatch.")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            if current
                .as_ref()
                .is_none_or(|(selected, _)| sequence > *selected)
            {
                current = Some((sequence, pickup));
            }
        }
        Ok(current.map(|(_, pickup)| pickup))
    }

    async fn bind_current(&self, key: &str, request: CancelRequest) -> Result<(), String> {
        let Some(pickup) = self.find_current_pickup(request).await? else {
            return Ok(());
        };
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision)) = self.load_record(key).await? else {
                return Err(format!("cancellation fence `{key}` disappeared"));
            };
            if record.dispatch_key.is_some() {
                return Ok(());
            }
            record.dispatch_key = Some(pickup.dispatch_key.clone());
            record.pickup_generation = Some(pickup.pickup_generation);
            record.owner = (!pickup.owner.is_empty()).then_some(pickup.owner.clone());
            if self.update_record(key, &record, revision).await.is_ok() {
                return Ok(());
            }
        }
        Err(format!(
            "cancellation fence `{key}` kept losing pickup binding updates"
        ))
    }

    async fn local_fence(
        &self,
        record: TaskCancellationRecord,
    ) -> Result<LocalCancellationFence, String> {
        let request = CancelRequest {
            task_instance_id: record
                .task_instance_id
                .parse()
                .map_err(|error| format!("decode cancellation task identity: {error}"))?,
            workflow_instance_id: record
                .workflow_instance_id
                .parse()
                .map_err(|error| format!("decode cancellation workflow identity: {error}"))?,
        };
        let pickup =
            if let Some(dispatch_key) = record.dispatch_key.as_deref() {
                match self.pickup.entry(dispatch_key).await.map_err(|error| {
                    format!("read cancellation pickup `{dispatch_key}`: {error}")
                })? {
                    Some(entry) => Some(
                        serde_json::from_slice::<TaskPickupRecord>(&entry.value)
                            .map_err(|error| format!("decode cancellation pickup: {error}"))?,
                    ),
                    None => None,
                }
            } else {
                None
            };
        let terminal_outcome = pickup
            .as_ref()
            .and_then(|pickup| pickup.terminal.as_ref())
            .map(|terminal| from_nats_outcome(terminal.outcome));
        Ok(LocalCancellationFence {
            acknowledgement_identity: record.acknowledgement_identity,
            request,
            dispatch_key: record.dispatch_key,
            pickup_generation: record.pickup_generation,
            owner: record.owner,
            owner_notified: record.owner_notified,
            liveness_deadline: pickup
                .and_then(|pickup| DateTime::from_timestamp_millis(pickup.liveness_deadline_ms)),
            terminal_outcome,
        })
    }

    pub async fn load_cancellation(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<Option<LocalCancellationFence>, String> {
        let request = acknowledgement_identity
            .strip_prefix("cancel-task-ack-v1:")
            .and_then(|identity| identity.split_once(':'))
            .ok_or_else(|| "invalid cancellation acknowledgement identity".to_owned())
            .and_then(|(workflow, task)| {
                Ok(CancelRequest {
                    workflow_instance_id: workflow.parse().map_err(|error| {
                        format!("decode cancellation workflow identity: {error}")
                    })?,
                    task_instance_id: task
                        .parse()
                        .map_err(|error| format!("decode cancellation task identity: {error}"))?,
                })
            })?;
        let key = cancellation_key(request);
        let Some((record, _)) = self.load_record(&key).await? else {
            return Ok(None);
        };
        self.local_fence(record).await.map(Some)
    }

    async fn elect_cancellation(
        &self,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
    ) -> Result<Option<TerminalElection>, String> {
        let (Some(dispatch_key), Some(generation), Some(owner)) = (
            fence.dispatch_key.as_deref(),
            fence.pickup_generation,
            fence.owner.as_deref(),
        ) else {
            return Ok(None);
        };
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some(entry) =
                self.pickup.entry(dispatch_key).await.map_err(|error| {
                    format!("read cancellation election `{dispatch_key}`: {error}")
                })?
            else {
                return Ok(None);
            };
            let mut pickup: TaskPickupRecord = serde_json::from_slice(&entry.value)
                .map_err(|error| format!("decode cancellation election: {error}"))?;
            match pickup.elect(
                dispatch_key,
                generation,
                owner,
                to_nats_outcome(cancellation_outcome(reconciliation)),
                &[],
            ) {
                ElectionDecision::Settled(outcome) => {
                    return Ok(Some(TerminalElection::Settled(from_nats_outcome(outcome))));
                }
                ElectionDecision::Rejected => return Ok(None),
                ElectionDecision::Won => {}
            }
            let bytes = serde_json::to_vec(&pickup)
                .map_err(|error| format!("encode cancellation election: {error}"))?;
            if self
                .pickup
                .update(dispatch_key, bytes.into(), entry.revision)
                .await
                .is_ok()
            {
                return Ok(Some(TerminalElection::Won));
            }
        }
        Err(format!(
            "cancellation terminal election kept losing updates for `{dispatch_key}`"
        ))
    }

    pub async fn ensure_acknowledgement_enqueued(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<Vec<u8>, String> {
        let fence = self
            .load_cancellation(acknowledgement_identity)
            .await?
            .ok_or_else(|| "cancellation fence is missing".to_owned())?;
        let key = cancellation_key(fence.request);
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision)) = self.load_record(&key).await? else {
                return Err("cancellation fence disappeared".to_owned());
            };
            let acknowledgement = record
                .acknowledgement
                .clone()
                .ok_or_else(|| "cancellation acknowledgement is not staged".to_owned())?;
            if record.acknowledgement_enqueued {
                return Ok(acknowledgement);
            }
            let mut headers = HeaderMap::new();
            headers.insert(MESSAGE_ID_HEADER, acknowledgement_identity);
            self.js
                .publish_with_headers(
                    TASK_CANCEL_ACK_SUBJECT,
                    headers,
                    acknowledgement.clone().into(),
                )
                .await
                .map_err(|error| format!("stage cancellation acknowledgement: {error}"))?
                .await
                .map_err(|error| format!("prove cancellation acknowledgement staging: {error}"))?;
            record.acknowledgement_enqueued = true;
            if self.update_record(&key, &record, revision).await.is_ok() {
                return Ok(acknowledgement);
            }
        }
        Err("cancellation acknowledgement kept losing staging updates".to_owned())
    }
}

impl SafeCancellationFence for NatsCancellationFence {
    async fn commit_cancellation_fence(
        &self,
        acknowledgement_identity: &str,
        request: CancelRequest,
        _now: DateTime<Utc>,
    ) -> Result<LocalCancellationFence, String> {
        let key = cancellation_key(request);
        if let Some((record, _)) = self.load_record(&key).await? {
            if record.acknowledgement_identity != acknowledgement_identity
                || record.task_instance_id != request.task_instance_id.to_string()
                || record.workflow_instance_id != request.workflow_instance_id.to_string()
            {
                return Err("stable cancellation identity conflicts with durable bytes".to_owned());
            }
        } else {
            let record = TaskCancellationRecord {
                acknowledgement_identity: acknowledgement_identity.to_owned(),
                task_instance_id: request.task_instance_id.to_string(),
                workflow_instance_id: request.workflow_instance_id.to_string(),
                dispatch_key: None,
                pickup_generation: None,
                owner: None,
                owner_notified: false,
                reconciliation: None,
                acknowledgement: None,
                acknowledgement_enqueued: false,
            };
            let bytes = serde_json::to_vec(&record)
                .map_err(|error| format!("encode cancellation fence: {error}"))?;
            if self.pickup.create(&key, bytes.into()).await.is_err()
                && self.load_record(&key).await?.is_none()
            {
                return Err(format!("create cancellation fence `{key}` failed"));
            }
        }
        self.bind_current(&key, request).await?;
        let (record, _) = self
            .load_record(&key)
            .await?
            .ok_or_else(|| "committed cancellation fence could not be proved".to_owned())?;
        self.local_fence(record).await
    }

    async fn mark_cancellation_owner_notified(
        &self,
        fence: &LocalCancellationFence,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let key = cancellation_key(fence.request);
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision)) = self.load_record(&key).await? else {
                return Ok(false);
            };
            if record.owner.as_deref() != fence.owner.as_deref()
                || record.pickup_generation != fence.pickup_generation
                || record.reconciliation.is_some()
            {
                return Ok(false);
            }
            if record.owner_notified {
                return Ok(true);
            }
            record.owner_notified = true;
            if self.update_record(&key, &record, revision).await.is_ok() {
                return Ok(true);
            }
        }
        Err("cancellation owner notification kept losing updates".to_owned())
    }

    async fn settle_cancellation(
        &self,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
        acknowledgement: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<Option<TerminalElection>, String> {
        let election = self.elect_cancellation(fence, reconciliation).await?;
        let key = cancellation_key(fence.request);
        for _ in 0..OUTCOME_ELECTION_RETRIES {
            let Some((mut record, revision)) = self.load_record(&key).await? else {
                return Err("cancellation fence disappeared before settlement".to_owned());
            };
            if let Some(existing) = &record.acknowledgement {
                if existing != acknowledgement {
                    return Err(
                        "duplicate cancellation produced conflicting acknowledgement bytes"
                            .to_owned(),
                    );
                }
                return Ok(election);
            }
            record.reconciliation = Some(to_record_reconciliation(reconciliation));
            record.acknowledgement = Some(acknowledgement.to_vec());
            if self.update_record(&key, &record, revision).await.is_ok() {
                return Ok(election);
            }
        }
        Err("cancellation settlement kept losing conditional updates".to_owned())
    }

    async fn select_unresolved_cancellation(
        &self,
    ) -> Result<Option<LocalCancellationFence>, String> {
        let mut keys = self
            .pickup
            .keys()
            .await
            .map_err(|error| format!("list cancellation fences: {error}"))?;
        let mut scanned = 0;
        while scanned < CANCELLATION_SCAN_LIMIT {
            let Some(key) = keys.next().await else {
                break;
            };
            let key = key.map_err(|error| format!("scan cancellation fence: {error}"))?;
            if !key.starts_with(CANCELLATION_PREFIX) {
                continue;
            }
            scanned += 1;
            let Some((record, _)) = self.load_record(&key).await? else {
                continue;
            };
            if record.reconciliation.is_none() {
                return self.local_fence(record).await.map(Some);
            }
        }
        Ok(None)
    }
}

impl SafeCancellationRole for NatsCancellationFence {
    async fn select_owner_cancellation(
        &self,
        owner: &str,
    ) -> Result<Option<LocalCancellationFence>, String> {
        Ok(self
            .select_unresolved_cancellation()
            .await?
            .filter(|fence| fence.owner.as_deref() == Some(owner)))
    }
}

pub async fn open_pickup_bucket(nats: &NatsClient) -> Result<kv::Store, String> {
    let js = jetstream::new(nats.clone());
    match js.get_key_value(PICKUP_BUCKET).await {
        Ok(store) => Ok(store),
        Err(_) => js
            .create_key_value(kv::Config {
                bucket: PICKUP_BUCKET.to_owned(),
                history: 1,
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|error| format!("create Task pickup handoff bucket: {error}")),
    }
}
