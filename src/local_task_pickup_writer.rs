//! Tickr Lite's sole Conductor-owned task-pickup writer.
//!
//! The bounded client exposes role operations only. The receiver owns the
//! writer repository and serializes dispatch staging, claim, liveness, and
//! TaskEvent staging without exposing SQLite or channel internals.

use sha2::{Digest, Sha256};
use tickr_executor::local_pickup::{
    CancellationReconciliation, ClaimLocalPickup, ClaimWriteError, DueLocalPickup,
    LocalAttemptOutcome, LocalCancellationFence, LocalPickupClaim, PendingLocalDispatch,
    SafeAttemptOutcomeHandoff, SafeCancellationFence, SafePickupWriter, TerminalElection,
};
use tickr_executor::wire::{decode_dispatch, CancelRequest};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::task_pickup_repository::{
    pickup_now, ClaimTaskPickupInput, ClaimTaskPickupOutcome, DueTaskPickup,
    PendingTaskCancellationAck, PendingTaskEvent, PickupTimestamp, TaskCancellationFence,
    TaskCancellationReconciliation, TaskPickupClaim, TaskPickupTerminalElection,
    TaskPickupTerminalOutcome,
};
use tickr_proto::{ConductorRelayMessage, EntityType};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const DEFAULT_REQUEST_CAPACITY: usize = 64;

type WriterResult<T> = Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimAcknowledgement {
    Normal,
    AmbiguousOnce,
}

enum Request {
    StageDispatch {
        payload: Vec<u8>,
        response: oneshot::Sender<WriterResult<(String, bool)>>,
    },
    SelectPending {
        response: oneshot::Sender<WriterResult<Option<PendingLocalDispatch>>>,
    },
    RejectPoison {
        dispatch_key: String,
        reason: String,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    Claim {
        dispatch_key: String,
        owner: String,
        liveness_deadline: PickupTimestamp,
        assigned_event: Vec<u8>,
        now: PickupTimestamp,
        response: oneshot::Sender<Result<Option<LocalPickupClaim>, ClaimWriteError>>,
    },
    ProveAmbiguousClaim {
        dispatch_key: String,
        owner: String,
        assigned_event: Vec<u8>,
        response: oneshot::Sender<WriterResult<Option<LocalPickupClaim>>>,
    },
    ArmLiveness {
        claim: LocalPickupClaim,
        deadline: PickupTimestamp,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    ProveReadyToLaunch {
        claim: LocalPickupClaim,
        assigned_event: Vec<u8>,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    StageStarted {
        claim: LocalPickupClaim,
        started_event: Vec<u8>,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    RenewLiveness {
        claim: LocalPickupClaim,
        deadline: PickupTimestamp,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    RegisterLivenessFailure {
        claim: LocalPickupClaim,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    SelectDueLiveness {
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<Option<DueLocalPickup>>>,
    },
    ElectTerminal {
        claim: LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: Vec<u8>,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<TerminalElection>>,
    },
    SelectPendingTaskEvent {
        response: oneshot::Sender<WriterResult<Option<PendingTaskEvent>>>,
    },
    MarkTaskEventForwarded {
        event: PendingTaskEvent,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    CommitCancellationFence {
        acknowledgement_identity: String,
        request: CancelRequest,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<LocalCancellationFence>>,
    },
    MarkCancellationOwnerNotified {
        fence: LocalCancellationFence,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
    SettleCancellation {
        fence: LocalCancellationFence,
        reconciliation: CancellationReconciliation,
        acknowledgement: Vec<u8>,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<Option<TerminalElection>>>,
    },
    SelectUnresolvedCancellation {
        response: oneshot::Sender<WriterResult<Option<LocalCancellationFence>>>,
    },
    SelectPendingCancellationAck {
        response: oneshot::Sender<WriterResult<Option<PendingTaskCancellationAck>>>,
    },
    MarkCancellationAckForwarded {
        acknowledgement: PendingTaskCancellationAck,
        now: PickupTimestamp,
        response: oneshot::Sender<WriterResult<bool>>,
    },
}

/// Cloneable role client used by the local relay and sole Executor.
#[derive(Clone)]
pub struct LocalTaskPickupWriterClient {
    sender: mpsc::Sender<Request>,
}

/// Sole receiver that owns every local task-pickup mutation.
pub struct LocalTaskPickupWriter {
    repository: WriterRepositoryBundle,
    receiver: mpsc::Receiver<Request>,
    claim_acknowledgement: ClaimAcknowledgement,
    suppress_ambiguous_proof: bool,
    fail_initial_arm: bool,
    suppress_ready_proof: bool,
}

impl LocalTaskPickupWriter {
    pub fn new(repository: WriterRepositoryBundle) -> (LocalTaskPickupWriterClient, Self) {
        Self::with_capacity(repository, DEFAULT_REQUEST_CAPACITY)
    }

    pub fn with_capacity(
        repository: WriterRepositoryBundle,
        capacity: usize,
    ) -> (LocalTaskPickupWriterClient, Self) {
        assert!(
            capacity > 0,
            "local task-pickup writer capacity must be positive"
        );
        Self::configured(
            repository,
            capacity,
            ClaimAcknowledgement::Normal,
            false,
            false,
            false,
        )
    }

    fn configured(
        repository: WriterRepositoryBundle,
        capacity: usize,
        claim_acknowledgement: ClaimAcknowledgement,
        suppress_ambiguous_proof: bool,
        fail_initial_arm: bool,
        suppress_ready_proof: bool,
    ) -> (LocalTaskPickupWriterClient, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            LocalTaskPickupWriterClient { sender },
            Self {
                repository,
                receiver,
                claim_acknowledgement,
                suppress_ambiguous_proof,
                fail_initial_arm,
                suppress_ready_proof,
            },
        )
    }

    #[cfg(test)]
    fn with_test_faults(
        repository: WriterRepositoryBundle,
        claim_acknowledgement: ClaimAcknowledgement,
        suppress_ambiguous_proof: bool,
        fail_initial_arm: bool,
        suppress_ready_proof: bool,
    ) -> (LocalTaskPickupWriterClient, Self) {
        Self::configured(
            repository,
            DEFAULT_REQUEST_CAPACITY,
            claim_acknowledgement,
            suppress_ambiguous_proof,
            fail_initial_arm,
            suppress_ready_proof,
        )
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            let request = tokio::select! {
                _ = cancel.cancelled() => break,
                request = self.receiver.recv() => match request {
                    Some(request) => request,
                    None => break,
                },
            };
            self.handle(request).await;
        }
    }

    async fn handle(&mut self, request: Request) {
        match request {
            Request::StageDispatch { payload, response } => {
                let dispatch_key = dispatch_key(&payload);
                let task = decode_dispatch(&payload).ok();
                let task_instance_id = task.as_ref().map(|task| task.task_instance_id.to_string());
                let workflow_instance_id = task
                    .as_ref()
                    .map(|task| task.workflow_instance_id.to_string());
                let result = self
                    .repository
                    .stage_task_dispatch(
                        &dispatch_key,
                        &payload,
                        task_instance_id.as_deref(),
                        workflow_instance_id.as_deref(),
                        pickup_now(),
                    )
                    .await
                    .map(|inserted| (dispatch_key, inserted))
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::SelectPending { response } => {
                let result = self
                    .repository
                    .select_pending_task_dispatch()
                    .await
                    .map(|pending| {
                        pending.map(|pending| PendingLocalDispatch {
                            dispatch_key: pending.dispatch_key,
                            payload: pending.payload,
                        })
                    })
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::RejectPoison {
                dispatch_key,
                reason,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .reject_task_dispatch(&dispatch_key, &reason, now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::Claim {
                dispatch_key,
                owner,
                liveness_deadline,
                assigned_event,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .claim_task_pickup(ClaimTaskPickupInput {
                        dispatch_key: &dispatch_key,
                        owner: &owner,
                        liveness_deadline,
                        assigned_event: &assigned_event,
                        now,
                    })
                    .await
                    .map(|outcome| match outcome {
                        ClaimTaskPickupOutcome::Committed(claim) => Some(local_claim(claim)),
                        ClaimTaskPickupOutcome::NotPending => None,
                    });
                let result = match result {
                    Ok(_) if self.claim_acknowledgement == ClaimAcknowledgement::AmbiguousOnce => {
                        self.claim_acknowledgement = ClaimAcknowledgement::Normal;
                        Err(ClaimWriteError::Ambiguous)
                    }
                    Ok(outcome) => Ok(outcome),
                    Err(error) => Err(ClaimWriteError::Failed(error.to_string())),
                };
                let _ = response.send(result);
            }
            Request::ProveAmbiguousClaim {
                dispatch_key,
                owner,
                assigned_event,
                response,
            } => {
                let result = if self.suppress_ambiguous_proof {
                    Ok(None)
                } else {
                    self.repository
                        .prove_ambiguous_task_pickup(&dispatch_key, &owner, &assigned_event)
                        .await
                        .map(|claim| claim.map(local_claim))
                        .map_err(|error| error.to_string())
                };
                let _ = response.send(result);
            }
            Request::ArmLiveness {
                claim,
                deadline,
                now,
                response,
            } => {
                let result = if self.fail_initial_arm {
                    Ok(false)
                } else {
                    self.repository
                        .arm_task_pickup_liveness(&repository_claim(&claim), deadline, now)
                        .await
                        .map_err(|error| error.to_string())
                };
                let _ = response.send(result);
            }
            Request::ProveReadyToLaunch {
                claim,
                assigned_event,
                response,
            } => {
                let result = if self.suppress_ready_proof {
                    Ok(false)
                } else {
                    self.repository
                        .prove_task_pickup_ready(&repository_claim(&claim), &assigned_event)
                        .await
                        .map_err(|error| error.to_string())
                };
                let _ = response.send(result);
            }
            Request::StageStarted {
                claim,
                started_event,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .stage_task_started(&repository_claim(&claim), &started_event, now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::RenewLiveness {
                claim,
                deadline,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .renew_task_pickup_liveness(&repository_claim(&claim), deadline, now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::RegisterLivenessFailure {
                claim,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .register_task_pickup_liveness_failure(&repository_claim(&claim), now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::SelectDueLiveness { now, response } => {
                let result = self
                    .repository
                    .select_due_task_pickup(now)
                    .await
                    .map(|due| due.map(local_due))
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::ElectTerminal {
                claim,
                outcome,
                terminal_event,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .elect_task_pickup_terminal(
                        &repository_claim(&claim),
                        repository_outcome(outcome),
                        &terminal_event,
                        now,
                    )
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(local_election);
                let _ = response.send(result);
            }
            Request::SelectPendingTaskEvent { response } => {
                let result = self
                    .repository
                    .select_pending_task_event()
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::MarkTaskEventForwarded {
                event,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .mark_task_event_forwarded(&event, now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::CommitCancellationFence {
                acknowledgement_identity,
                request,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .commit_task_cancellation_fence(
                        &acknowledgement_identity,
                        &request.task_instance_id.to_string(),
                        &request.workflow_instance_id.to_string(),
                        now,
                    )
                    .await
                    .map(|fence| local_cancellation_fence(fence, request))
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::MarkCancellationOwnerNotified {
                fence,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .mark_task_cancellation_owner_notified(
                        &repository_cancellation_fence(&fence),
                        now,
                    )
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::SettleCancellation {
                fence,
                reconciliation,
                acknowledgement,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .settle_task_cancellation(
                        &repository_cancellation_fence(&fence),
                        repository_reconciliation(reconciliation),
                        &acknowledgement,
                        now,
                    )
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|election| election.map(local_election).transpose());
                let _ = response.send(result);
            }
            Request::SelectUnresolvedCancellation { response } => {
                let result = self
                    .repository
                    .select_unresolved_task_cancellation()
                    .await
                    .map(|fence| {
                        fence
                            .map(|fence| {
                                let request = cancellation_request(&fence)?;
                                Ok(local_cancellation_fence(fence, request))
                            })
                            .transpose()
                    })
                    .map_err(|error| error.to_string())
                    .and_then(|fence| fence);
                let _ = response.send(result);
            }
            Request::SelectPendingCancellationAck { response } => {
                let result = self
                    .repository
                    .select_pending_task_cancellation_ack()
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
            Request::MarkCancellationAckForwarded {
                acknowledgement,
                now,
                response,
            } => {
                let result = self
                    .repository
                    .mark_task_cancellation_ack_forwarded(&acknowledgement, now)
                    .await
                    .map_err(|error| error.to_string());
                let _ = response.send(result);
            }
        }
    }
}

impl LocalTaskPickupWriterClient {
    /// Durably accept one local relay dispatch. The payload digest is its stable
    /// identity, so an ambiguous relay retry cannot create a second pickup.
    pub async fn stage_dispatch(&self, payload: &[u8]) -> WriterResult<(String, bool)> {
        let (response, receive) = oneshot::channel();
        self.send(Request::StageDispatch {
            payload: payload.to_vec(),
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    /// Forward one staged TaskEvent and only then complete its local outbox row.
    /// A crash between those steps redelivers the unchanged published envelope.
    pub async fn forward_next_task_event(
        &self,
        relay_tx: &mpsc::Sender<ConductorRelayMessage>,
    ) -> WriterResult<bool> {
        let Some(event) = self.next_task_event().await? else {
            return Ok(false);
        };
        relay_tx
            .send(ConductorRelayMessage {
                entity_type: EntityType::TaskEvent as i32,
                payload: event.event.clone(),
                tenant_id: None,
            })
            .await
            .map_err(|error| format!("local TaskEvent relay closed before forward: {error}"))?;
        if !self.complete_task_event(&event, pickup_now()).await? {
            return Err("forwarded local TaskEvent was already completed".to_owned());
        }
        Ok(true)
    }

    /// Forward one staged cancellation acknowledgement and complete it only at
    /// the existing relay-channel boundary.
    pub async fn forward_next_cancellation_ack(
        &self,
        relay_tx: &mpsc::Sender<ConductorRelayMessage>,
    ) -> WriterResult<bool> {
        let Some(acknowledgement) = self.next_cancellation_ack().await? else {
            return Ok(false);
        };
        relay_tx
            .send(ConductorRelayMessage {
                entity_type: EntityType::CancelTaskAck as i32,
                payload: acknowledgement.acknowledgement.clone(),
                tenant_id: None,
            })
            .await
            .map_err(|error| {
                format!("local cancellation acknowledgement relay closed before forward: {error}")
            })?;
        if !self
            .complete_cancellation_ack(&acknowledgement, pickup_now())
            .await?
        {
            return Err(
                "forwarded local cancellation acknowledgement was already completed".to_owned(),
            );
        }
        Ok(true)
    }

    async fn next_task_event(&self) -> WriterResult<Option<PendingTaskEvent>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SelectPendingTaskEvent { response })
            .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn complete_task_event(
        &self,
        event: &PendingTaskEvent,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::MarkTaskEventForwarded {
            event: event.clone(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn next_cancellation_ack(&self) -> WriterResult<Option<PendingTaskCancellationAck>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SelectPendingCancellationAck { response })
            .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn complete_cancellation_ack(
        &self,
        acknowledgement: &PendingTaskCancellationAck,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::MarkCancellationAckForwarded {
            acknowledgement: acknowledgement.clone(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn send(&self, request: Request) -> WriterResult<()> {
        self.sender
            .send(request)
            .await
            .map_err(|_| writer_stopped())
    }
}

#[async_trait::async_trait]
impl SafePickupWriter for LocalTaskPickupWriterClient {
    async fn select_pending(&self) -> WriterResult<Option<PendingLocalDispatch>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SelectPending { response }).await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn reject_poison(
        &self,
        dispatch_key: &str,
        reason: &str,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::RejectPoison {
            dispatch_key: dispatch_key.to_owned(),
            reason: reason.to_owned(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn claim(
        &self,
        input: ClaimLocalPickup<'_>,
    ) -> Result<Option<LocalPickupClaim>, ClaimWriteError> {
        let (response, receive) = oneshot::channel();
        self.send(Request::Claim {
            dispatch_key: input.dispatch_key.to_owned(),
            owner: input.owner.to_owned(),
            liveness_deadline: input.liveness_deadline,
            assigned_event: input.assigned_event.to_vec(),
            now: input.now,
            response,
        })
        .await
        .map_err(ClaimWriteError::Failed)?;
        receive
            .await
            .map_err(|_| ClaimWriteError::Failed(writer_stopped()))?
    }

    async fn prove_ambiguous_claim(
        &self,
        dispatch_key: &str,
        owner: &str,
        assigned_event: &[u8],
    ) -> WriterResult<Option<LocalPickupClaim>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::ProveAmbiguousClaim {
            dispatch_key: dispatch_key.to_owned(),
            owner: owner.to_owned(),
            assigned_event: assigned_event.to_vec(),
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        _payload: &[u8],
        deadline: PickupTimestamp,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::ArmLiveness {
            claim: claim.clone(),
            deadline,
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn prove_ready_to_launch(
        &self,
        claim: &LocalPickupClaim,
        assigned_event: &[u8],
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::ProveReadyToLaunch {
            claim: claim.clone(),
            assigned_event: assigned_event.to_vec(),
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn stage_started(
        &self,
        claim: &LocalPickupClaim,
        started_event: &[u8],
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::StageStarted {
            claim: claim.clone(),
            started_event: started_event.to_vec(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: PickupTimestamp,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::RenewLiveness {
            claim: claim.clone(),
            deadline,
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }
}

#[async_trait::async_trait]
impl SafeAttemptOutcomeHandoff for LocalTaskPickupWriterClient {
    async fn select_due_liveness(
        &self,
        now: PickupTimestamp,
    ) -> WriterResult<Option<DueLocalPickup>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SelectDueLiveness { now, response })
            .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::RegisterLivenessFailure {
            claim: claim.clone(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        now: PickupTimestamp,
    ) -> WriterResult<TerminalElection> {
        let (response, receive) = oneshot::channel();
        self.send(Request::ElectTerminal {
            claim: claim.clone(),
            outcome,
            terminal_event: terminal_event.to_vec(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }
}

impl SafeCancellationFence for LocalTaskPickupWriterClient {
    async fn commit_cancellation_fence(
        &self,
        acknowledgement_identity: &str,
        request: CancelRequest,
        now: PickupTimestamp,
    ) -> WriterResult<LocalCancellationFence> {
        let (response, receive) = oneshot::channel();
        self.send(Request::CommitCancellationFence {
            acknowledgement_identity: acknowledgement_identity.to_owned(),
            request,
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn mark_cancellation_owner_notified(
        &self,
        fence: &LocalCancellationFence,
        now: PickupTimestamp,
    ) -> WriterResult<bool> {
        let (response, receive) = oneshot::channel();
        self.send(Request::MarkCancellationOwnerNotified {
            fence: fence.clone(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn settle_cancellation(
        &self,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
        acknowledgement: &[u8],
        now: PickupTimestamp,
    ) -> WriterResult<Option<TerminalElection>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SettleCancellation {
            fence: fence.clone(),
            reconciliation,
            acknowledgement: acknowledgement.to_vec(),
            now,
            response,
        })
        .await?;
        receive.await.map_err(|_| writer_stopped())?
    }

    async fn select_unresolved_cancellation(&self) -> WriterResult<Option<LocalCancellationFence>> {
        let (response, receive) = oneshot::channel();
        self.send(Request::SelectUnresolvedCancellation { response })
            .await?;
        receive.await.map_err(|_| writer_stopped())?
    }
}

fn dispatch_key(payload: &[u8]) -> String {
    format!("task-dispatch-v1:{:x}", Sha256::digest(payload))
}

fn local_claim(claim: TaskPickupClaim) -> LocalPickupClaim {
    LocalPickupClaim {
        dispatch_key: claim.dispatch_key,
        pickup_generation: claim.pickup_generation,
        owner: claim.owner,
        liveness_deadline: claim.liveness_deadline,
    }
}

fn repository_claim(claim: &LocalPickupClaim) -> TaskPickupClaim {
    TaskPickupClaim {
        dispatch_key: claim.dispatch_key.clone(),
        pickup_generation: claim.pickup_generation,
        owner: claim.owner.clone(),
        liveness_deadline: claim.liveness_deadline,
    }
}

fn local_due(due: DueTaskPickup) -> DueLocalPickup {
    DueLocalPickup {
        claim: local_claim(due.claim),
        payload: due.payload,
    }
}

fn repository_outcome(outcome: LocalAttemptOutcome) -> TaskPickupTerminalOutcome {
    match outcome {
        LocalAttemptOutcome::ProcessExitedSuccess => {
            TaskPickupTerminalOutcome::ProcessExitedSuccess
        }
        LocalAttemptOutcome::ProcessExitedFailure => {
            TaskPickupTerminalOutcome::ProcessExitedFailure
        }
        LocalAttemptOutcome::ProcessSetupFailed => TaskPickupTerminalOutcome::ProcessSetupFailed,
        LocalAttemptOutcome::LivenessExpired => TaskPickupTerminalOutcome::LivenessExpired,
        LocalAttemptOutcome::CancellationKilled => TaskPickupTerminalOutcome::CancellationKilled,
        LocalAttemptOutcome::CancellationAlreadyExited => {
            TaskPickupTerminalOutcome::CancellationAlreadyExited
        }
        LocalAttemptOutcome::CancellationNoProcess => {
            TaskPickupTerminalOutcome::CancellationNoProcess
        }
    }
}

fn local_outcome(outcome: TaskPickupTerminalOutcome) -> LocalAttemptOutcome {
    match outcome {
        TaskPickupTerminalOutcome::ProcessExitedSuccess => {
            LocalAttemptOutcome::ProcessExitedSuccess
        }
        TaskPickupTerminalOutcome::ProcessExitedFailure => {
            LocalAttemptOutcome::ProcessExitedFailure
        }
        TaskPickupTerminalOutcome::ProcessSetupFailed => LocalAttemptOutcome::ProcessSetupFailed,
        TaskPickupTerminalOutcome::LivenessExpired => LocalAttemptOutcome::LivenessExpired,
        TaskPickupTerminalOutcome::CancellationKilled => LocalAttemptOutcome::CancellationKilled,
        TaskPickupTerminalOutcome::CancellationAlreadyExited => {
            LocalAttemptOutcome::CancellationAlreadyExited
        }
        TaskPickupTerminalOutcome::CancellationNoProcess => {
            LocalAttemptOutcome::CancellationNoProcess
        }
    }
}

fn local_cancellation_fence(
    fence: TaskCancellationFence,
    request: CancelRequest,
) -> LocalCancellationFence {
    LocalCancellationFence {
        acknowledgement_identity: fence.acknowledgement_identity,
        request,
        dispatch_key: fence.dispatch_key,
        pickup_generation: fence.pickup_generation,
        owner: fence.owner,
        owner_notified: fence.owner_notified,
        liveness_deadline: fence.liveness_deadline,
        terminal_outcome: fence.terminal_outcome.map(local_outcome),
    }
}

fn repository_cancellation_fence(fence: &LocalCancellationFence) -> TaskCancellationFence {
    TaskCancellationFence {
        acknowledgement_identity: fence.acknowledgement_identity.clone(),
        task_instance_id: fence.request.task_instance_id.to_string(),
        workflow_instance_id: fence.request.workflow_instance_id.to_string(),
        dispatch_key: fence.dispatch_key.clone(),
        pickup_generation: fence.pickup_generation,
        owner: fence.owner.clone(),
        owner_notified: fence.owner_notified,
        liveness_deadline: fence.liveness_deadline,
        terminal_outcome: fence.terminal_outcome.map(repository_outcome),
    }
}

fn cancellation_request(fence: &TaskCancellationFence) -> WriterResult<CancelRequest> {
    Ok(CancelRequest {
        task_instance_id: fence
            .task_instance_id
            .parse()
            .map_err(|error| format!("stored cancellation task identity is invalid: {error}"))?,
        workflow_instance_id: fence.workflow_instance_id.parse().map_err(|error| {
            format!("stored cancellation workflow identity is invalid: {error}")
        })?,
    })
}

fn repository_reconciliation(
    reconciliation: CancellationReconciliation,
) -> TaskCancellationReconciliation {
    match reconciliation {
        CancellationReconciliation::Killed => TaskCancellationReconciliation::Killed,
        CancellationReconciliation::AlreadyExited => TaskCancellationReconciliation::AlreadyExited,
        CancellationReconciliation::NoProcess => TaskCancellationReconciliation::NoProcess,
    }
}

fn local_election(election: TaskPickupTerminalElection) -> WriterResult<TerminalElection> {
    match election {
        TaskPickupTerminalElection::Won => Ok(TerminalElection::Won),
        TaskPickupTerminalElection::Settled(outcome) => {
            Ok(TerminalElection::Settled(local_outcome(outcome)))
        }
        TaskPickupTerminalElection::NotClaimed => {
            Err("pickup generation is not the current claimed generation".to_owned())
        }
    }
}

fn writer_stopped() -> String {
    "local task-pickup writer stopped".to_owned()
}

#[cfg(test)]
#[path = "../tests/support/attempt_outcome_laws.rs"]
mod attempt_outcome_laws;

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use tickr_executor::local_pickup::{
        new_pickup_identity, CancellationReconciliation, LocalExecutorCapacity, LocalTaskHandler,
        NoopPickupCheckpoint, PickupBoundary, PickupCheckpoint, PickupOutcome,
        SafeCancellationCoordinator, SafePickupError, SafePickupExecutor, TaskProcessLauncher,
    };
    use tickr_executor::wire::{
        encode_cancel_ack, encode_dispatch, encode_task_event, DispatchedTask, EmitKind,
        KillOutcome,
    };
    use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
    use tickr_migrations::task_pickup_repository::TaskPickupSnapshot;
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::config::DataPlaneSql;
    use tokio::process::{Child, Command};
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};

    use super::*;

    #[derive(Clone)]
    struct RealChildLauncher {
        launch_log: PathBuf,
        run_for: Duration,
    }

    impl TaskProcessLauncher for RealChildLauncher {
        async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
            let launches_before = launch_count(&self.launch_log);
            let mut command = Command::new("sh");
            command
                .arg("-c")
                .arg(format!(
                    "sleep {} & child=$!; printf 'launch %s %s\\n' \"$$\" \"$child\" >> \"$1\"; wait \"$child\"",
                    self.run_for.as_secs_f64()
                ))
                .arg("tickr-task")
                .arg(&self.launch_log)
                .kill_on_drop(true);
            #[cfg(unix)]
            command.process_group(0);
            let mut child = command
                .spawn()
                .map_err(|error| format!("spawn real test Task process: {error}"))?;
            for _ in 0..100 {
                if launch_count(&self.launch_log) > launches_before {
                    return Ok(child);
                }
                if child
                    .try_wait()
                    .map_err(|error| error.to_string())?
                    .is_some()
                {
                    return Err("real test Task process exited before recording launch".to_owned());
                }
                sleep(Duration::from_millis(5)).await;
            }
            let _ = child.kill().await;
            Err("real test Task process did not record launch".to_owned())
        }
    }

    #[derive(Clone)]
    struct FailingLauncher;

    impl TaskProcessLauncher for FailingLauncher {
        async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
            Err("simulated task-process setup failure".to_owned())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FailAt(PickupBoundary);

    impl PickupCheckpoint for FailAt {
        fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
            if boundary == self.0 {
                Err("simulated formation crash".to_owned())
            } else {
                Ok(())
            }
        }
    }

    struct WriterRuntime {
        client: LocalTaskPickupWriterClient,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    }

    impl WriterRuntime {
        fn normal(repository: WriterRepositoryBundle) -> Self {
            let (client, writer) = LocalTaskPickupWriter::new(repository);
            Self::start(client, writer)
        }

        fn faults(
            repository: WriterRepositoryBundle,
            claim_acknowledgement: ClaimAcknowledgement,
            suppress_ambiguous_proof: bool,
            fail_initial_arm: bool,
            suppress_ready_proof: bool,
        ) -> Self {
            let (client, writer) = LocalTaskPickupWriter::with_test_faults(
                repository,
                claim_acknowledgement,
                suppress_ambiguous_proof,
                fail_initial_arm,
                suppress_ready_proof,
            );
            Self::start(client, writer)
        }

        fn start(client: LocalTaskPickupWriterClient, writer: LocalTaskPickupWriter) -> Self {
            let cancel = CancellationToken::new();
            let handle = tokio::spawn(writer.run(cancel.clone()));
            Self {
                client,
                cancel,
                handle,
            }
        }

        async fn stop(self) {
            self.cancel.cancel();
            self.handle.await.unwrap();
        }
    }

    fn valid_task() -> DispatchedTask {
        DispatchedTask {
            task_instance_id: new_pickup_identity(),
            task_id: new_pickup_identity(),
            workflow_instance_id: new_pickup_identity(),
            workflow_id: new_pickup_identity(),
            name: "real-child".to_owned(),
            nix_expression_path: ".#unused-by-test-launcher".to_owned(),
            nix_args: vec![],
            outputs: vec![],
            inputs: vec![],
            secrets: vec![],
            originating_signal_id: None,
            gate_signal_ids: HashMap::new(),
            gate_signal_ids_ambient: HashSet::new(),
        }
    }

    async fn open_repository(temp: &TempDir) -> WriterRepositoryBundle {
        let database = temp.path().join("tickr.sqlite");
        let url = format!("sqlite://{}", database.display());
        let options = sqlite_writer_options(&url, true).unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &pool)
            .await
            .unwrap();
        pool.close().await;
        RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap()
    }

    fn launch_count(path: &Path) -> usize {
        fs::read_to_string(path)
            .map(|contents| contents.lines().count())
            .unwrap_or(0)
    }

    fn executor<C: PickupCheckpoint>(
        client: LocalTaskPickupWriterClient,
        launch_log: PathBuf,
        capacity: LocalExecutorCapacity,
        checkpoint: C,
    ) -> SafePickupExecutor<LocalTaskPickupWriterClient, RealChildLauncher, C> {
        SafePickupExecutor::with_checkpoint(
            client,
            RealChildLauncher {
                launch_log,
                run_for: Duration::from_millis(150),
            },
            checkpoint,
            capacity,
            "executor-one",
            Duration::from_millis(200),
        )
    }

    async fn snapshot(
        repository: &WriterRepositoryBundle,
        dispatch_key: &str,
    ) -> TaskPickupSnapshot {
        repository
            .task_pickup_snapshot(dispatch_key)
            .await
            .unwrap()
            .unwrap()
    }

    async fn claim_without_spawn(
        client: &LocalTaskPickupWriterClient,
        task: &DispatchedTask,
    ) -> (LocalPickupClaim, Vec<u8>) {
        let assigned = encode_task_event(task, new_pickup_identity(), EmitKind::Assigned);
        let now = pickup_now();
        let claim = client
            .claim(ClaimLocalPickup {
                dispatch_key: &dispatch_key(&encode_dispatch(task)),
                owner: "executor-one",
                liveness_deadline: now + Duration::from_secs(5),
                assigned_event: &assigned,
                now,
            })
            .await
            .unwrap()
            .unwrap();
        assert!(client
            .arm_liveness(
                &claim,
                &[],
                now + Duration::from_secs(5),
                now + Duration::from_millis(1),
            )
            .await
            .unwrap());
        (claim, assigned)
    }

    fn cancel_request(task: &DispatchedTask) -> CancelRequest {
        CancelRequest {
            task_instance_id: task.task_instance_id,
            workflow_instance_id: task.workflow_instance_id,
        }
    }

    fn long_running_pickup(
        client: LocalTaskPickupWriterClient,
        launch_log: PathBuf,
    ) -> (
        SafePickupExecutor<LocalTaskPickupWriterClient, RealChildLauncher>,
        LocalTaskHandler<RealChildLauncher>,
    ) {
        let task_handler = LocalTaskHandler::new(RealChildLauncher {
            launch_log,
            run_for: Duration::from_secs(30),
        });
        let pickup = SafePickupExecutor::with_task_handler(
            client,
            task_handler.clone(),
            NoopPickupCheckpoint,
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            "executor-one",
            Duration::from_secs(5),
        );
        (pickup, task_handler)
    }

    fn launched_process_ids(path: &Path) -> Vec<u32> {
        fs::read_to_string(path)
            .unwrap()
            .split_whitespace()
            .skip(1)
            .map(|value| value.parse().unwrap())
            .collect()
    }

    async fn process_exists(pid: u32) -> bool {
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .await
            .unwrap()
            .success()
    }

    #[tokio::test]
    async fn full_capacity_and_stale_observation_leave_dispatch_pending() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let first_dispatch = runtime
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        assert!(first_dispatch.1);

        let executor_id = new_pickup_identity();
        let capacity = LocalExecutorCapacity::new(executor_id, NonZeroUsize::new(1).unwrap());
        let fleet_status = capacity.observation();
        let pickup = SafePickupExecutor::with_checkpoint(
            runtime.client.clone(),
            RealChildLauncher {
                launch_log: temp.path().join("launches"),
                run_for: Duration::from_millis(250),
            },
            NoopPickupCheckpoint,
            capacity,
            "executor-one",
            Duration::from_millis(200),
        );
        assert_eq!(
            fleet_status.snapshot(),
            tickr_executor::local_pickup::ExecutorCapacitySnapshot {
                executor_id,
                configured_process_slots: 1,
                in_flight_count: 0,
            }
        );

        let first_pickup = pickup.clone();
        let first = tokio::spawn(async move { first_pickup.run_one().await });
        timeout(Duration::from_secs(1), async {
            while fleet_status.snapshot().in_flight_count != 1 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let (second_dispatch_key, inserted) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        assert!(inserted);
        let stale_observation = fleet_status.snapshot();
        let second_pickup = pickup.clone();
        let second = tokio::spawn(async move { second_pickup.run_one().await });
        sleep(Duration::from_millis(50)).await;

        assert!(!second.is_finished(), "saturation must precede selection");
        assert_eq!(fleet_status.snapshot().in_flight_count, 1);
        assert_eq!(stale_observation.in_flight_count, 1);
        let pending = snapshot(&repository, &second_dispatch_key).await;
        assert_eq!(pending.state, "pending");
        assert!(pending.staged_event_kinds.is_empty());

        assert!(matches!(
            first.await.unwrap().unwrap(),
            PickupOutcome::Launched { .. }
        ));
        assert!(matches!(
            second.await.unwrap().unwrap(),
            PickupOutcome::Launched { .. }
        ));
        assert_eq!(fleet_status.snapshot().in_flight_count, 0);
        assert_eq!(launch_count(&temp.path().join("launches")), 2);
        runtime.stop().await;
    }

    #[derive(Debug, Clone, Copy)]
    enum FleetObservationCase {
        Missing,
        Stale,
        Duplicated,
        Contradictory,
    }

    impl FleetObservationCase {
        const ALL: [Self; 4] = [
            Self::Missing,
            Self::Stale,
            Self::Duplicated,
            Self::Contradictory,
        ];
    }

    fn local_observations(
        case: FleetObservationCase,
        capacity: &LocalExecutorCapacity,
    ) -> Vec<tickr_executor::local_pickup::ExecutorCapacitySnapshot> {
        let current = capacity.observation().snapshot();
        match case {
            FleetObservationCase::Missing => Vec::new(),
            FleetObservationCase::Stale => vec![current],
            FleetObservationCase::Duplicated => vec![current, current],
            FleetObservationCase::Contradictory => {
                vec![tickr_executor::local_pickup::ExecutorCapacitySnapshot {
                    executor_id: current.executor_id,
                    configured_process_slots: 0,
                    in_flight_count: usize::MAX,
                }]
            }
        }
    }

    #[tokio::test]
    async fn lite_dispatch_is_unchanged_by_fleet_observation_matrix() {
        for case in FleetObservationCase::ALL {
            let temp = TempDir::new().unwrap();
            let repository = open_repository(&temp).await;
            let runtime = WriterRuntime::normal(repository.clone());
            let (dispatch_key, inserted) = runtime
                .client
                .stage_dispatch(&encode_dispatch(&valid_task()))
                .await
                .unwrap();
            assert!(inserted);

            let capacity =
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap());
            let observations = local_observations(case, &capacity);
            let outcome = executor(
                runtime.client.clone(),
                temp.path().join("launches"),
                capacity,
                NoopPickupCheckpoint,
            )
            .run_one()
            .await
            .unwrap_or_else(|error| panic!("{case:?} fleet observation changed dispatch: {error}"));

            assert!(
                matches!(outcome, PickupOutcome::Launched { .. }),
                "{case:?} fleet observation changed Task execution"
            );
            let claimed = snapshot(&repository, &dispatch_key).await;
            assert_eq!(
                (
                    claimed.state.as_str(),
                    claimed.pickup_generation,
                    claimed.owner.as_deref()
                ),
                ("claimed", 1, Some("executor-one")),
                "{case:?} fleet observation changed queue ownership"
            );
            assert_eq!(launch_count(&temp.path().join("launches")), 1);
            match case {
                FleetObservationCase::Missing => assert!(observations.is_empty()),
                FleetObservationCase::Stale => assert_eq!(observations.len(), 1),
                FleetObservationCase::Duplicated => {
                    assert_eq!(observations, vec![observations[0], observations[0]])
                }
                FleetObservationCase::Contradictory => {
                    assert_eq!(observations[0].configured_process_slots, 0);
                    assert_eq!(observations[0].in_flight_count, usize::MAX);
                }
            }
            runtime.stop().await;
        }
    }

    #[tokio::test]
    async fn poison_is_rejected_and_quarantined_before_any_claim() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(b"not protobuf")
            .await
            .unwrap();
        let outcome = executor(
            runtime.client.clone(),
            temp.path().join("launches"),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        )
        .run_one()
        .await
        .unwrap();
        assert_eq!(
            outcome,
            PickupOutcome::PoisonRejected {
                dispatch_key: dispatch_key.clone()
            }
        );
        let rejected = snapshot(&repository, &dispatch_key).await;
        assert_eq!(rejected.state, "rejected");
        assert_eq!(rejected.pickup_generation, 0);
        assert!(rejected.quarantined);
        assert!(rejected.staged_event_kinds.is_empty());
        assert_eq!(launch_count(&temp.path().join("launches")), 0);
        runtime.stop().await;
    }

    #[tokio::test]
    async fn crash_boundaries_preserve_at_most_one_real_process_launch() {
        let cases = [
            PickupBoundary::BeforeSelection,
            PickupBoundary::AfterSelection,
            PickupBoundary::AfterValidation,
            PickupBoundary::AfterClaimCommit,
            PickupBoundary::AfterAssignedStaging,
            PickupBoundary::AfterInitialLivenessArm,
            PickupBoundary::AfterClaimProof,
            PickupBoundary::AfterSourceAcknowledgement,
            PickupBoundary::AfterSpawn,
            PickupBoundary::AfterStartedStaging,
            PickupBoundary::AfterFirstLivenessRenewal,
        ];

        for boundary in cases {
            let temp = TempDir::new().unwrap();
            let repository = open_repository(&temp).await;
            let launch_log = temp.path().join("launches");
            let first = WriterRuntime::normal(repository.clone());
            let (dispatch_key, _) = first
                .client
                .stage_dispatch(&encode_dispatch(&valid_task()))
                .await
                .unwrap();
            let error = executor(
                first.client.clone(),
                launch_log.clone(),
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
                FailAt(boundary),
            )
            .run_one()
            .await
            .unwrap_err();
            assert!(
                matches!(error, SafePickupError::Checkpoint { boundary: hit, .. } if hit == boundary)
            );
            first.stop().await;

            let after_crash = snapshot(&repository, &dispatch_key).await;
            let restart = WriterRuntime::normal(repository.clone());
            let restart_outcome = executor(
                restart.client.clone(),
                launch_log.clone(),
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
                NoopPickupCheckpoint,
            )
            .run_one()
            .await
            .unwrap();
            restart.stop().await;
            let final_state = snapshot(&repository, &dispatch_key).await;

            let before_claim = matches!(
                boundary,
                PickupBoundary::BeforeSelection
                    | PickupBoundary::AfterSelection
                    | PickupBoundary::AfterValidation
            );
            if before_claim {
                assert_eq!(after_crash.state, "pending", "{boundary:?}");
                assert!(matches!(restart_outcome, PickupOutcome::Launched { .. }));
                assert_eq!(
                    final_state.staged_event_kinds,
                    [
                        "Assigned".to_owned(),
                        "Started".to_owned(),
                        "Completed".to_owned(),
                    ],
                    "{boundary:?}"
                );
            } else {
                assert_eq!(after_crash.state, "claimed", "{boundary:?}");
                assert_eq!(after_crash.pickup_generation, 1, "{boundary:?}");
                assert_eq!(after_crash.owner.as_deref(), Some("executor-one"));
                assert_eq!(restart_outcome, PickupOutcome::NoWork, "{boundary:?}");
                assert_eq!(final_state.pickup_generation, 1, "{boundary:?}");
            }

            let expected_launches = if before_claim
                || matches!(
                    boundary,
                    PickupBoundary::AfterSpawn
                        | PickupBoundary::AfterStartedStaging
                        | PickupBoundary::AfterFirstLivenessRenewal
                ) {
                1
            } else {
                0
            };
            assert_eq!(launch_count(&launch_log), expected_launches, "{boundary:?}");

            let expected_events = if before_claim {
                vec![
                    "Assigned".to_owned(),
                    "Started".to_owned(),
                    "Completed".to_owned(),
                ]
            } else if matches!(
                boundary,
                PickupBoundary::AfterStartedStaging | PickupBoundary::AfterFirstLivenessRenewal
            ) {
                vec!["Assigned".to_owned(), "Started".to_owned()]
            } else {
                vec!["Assigned".to_owned()]
            };
            assert_eq!(
                final_state.staged_event_kinds, expected_events,
                "{boundary:?}"
            );
            if matches!(
                boundary,
                PickupBoundary::AfterInitialLivenessArm
                    | PickupBoundary::AfterClaimProof
                    | PickupBoundary::AfterSourceAcknowledgement
                    | PickupBoundary::AfterSpawn
                    | PickupBoundary::AfterStartedStaging
                    | PickupBoundary::AfterFirstLivenessRenewal
            ) {
                assert!(final_state.liveness_armed_at.is_some(), "{boundary:?}");
            }
        }
    }

    #[tokio::test]
    async fn ambiguous_unproved_claim_and_failed_arm_launch_nothing() {
        for fault in ["ambiguous-unproved", "failed-arm", "failed-proof"] {
            let temp = TempDir::new().unwrap();
            let repository = open_repository(&temp).await;
            let launch_log = temp.path().join("launches");
            let (ack, suppress_ambiguous, fail_arm, suppress_ready) = match fault {
                "ambiguous-unproved" => (ClaimAcknowledgement::AmbiguousOnce, true, false, false),
                "failed-arm" => (ClaimAcknowledgement::Normal, false, true, false),
                "failed-proof" => (ClaimAcknowledgement::Normal, false, false, true),
                _ => unreachable!(),
            };
            let runtime = WriterRuntime::faults(
                repository.clone(),
                ack,
                suppress_ambiguous,
                fail_arm,
                suppress_ready,
            );
            let (dispatch_key, _) = runtime
                .client
                .stage_dispatch(&encode_dispatch(&valid_task()))
                .await
                .unwrap();
            let result = executor(
                runtime.client.clone(),
                launch_log.clone(),
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
                NoopPickupCheckpoint,
            )
            .run_one()
            .await;
            assert!(result.is_err(), "{fault}");
            runtime.stop().await;

            let state = snapshot(&repository, &dispatch_key).await;
            assert_eq!(state.state, "claimed", "{fault}");
            assert_eq!(state.pickup_generation, 1, "{fault}");
            assert_eq!(state.staged_event_kinds, ["Assigned".to_owned()], "{fault}");
            assert_eq!(launch_count(&launch_log), 0, "{fault}");

            let restart = WriterRuntime::normal(repository.clone());
            let outcome = timeout(
                Duration::from_secs(1),
                executor(
                    restart.client.clone(),
                    launch_log.clone(),
                    LocalExecutorCapacity::new(
                        new_pickup_identity(),
                        NonZeroUsize::new(1).unwrap(),
                    ),
                    NoopPickupCheckpoint,
                )
                .run_one(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(outcome, PickupOutcome::NoWork, "{fault}");
            restart.stop().await;
            assert_eq!(launch_count(&launch_log), 0, "{fault}");
        }
    }

    #[tokio::test]
    async fn ambiguous_claim_that_is_proved_launches_exactly_once() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let launch_log = temp.path().join("launches");
        let runtime = WriterRuntime::faults(
            repository.clone(),
            ClaimAcknowledgement::AmbiguousOnce,
            false,
            false,
            false,
        );
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        let outcome = executor(
            runtime.client.clone(),
            launch_log.clone(),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        )
        .run_one()
        .await
        .unwrap();
        assert!(matches!(outcome, PickupOutcome::Launched { .. }));
        let state = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            state.staged_event_kinds,
            [
                "Assigned".to_owned(),
                "Started".to_owned(),
                "Completed".to_owned(),
            ]
        );
        assert!(state.liveness_armed_at.is_some());
        assert_eq!(launch_count(&launch_log), 1);
        runtime.stop().await;
    }

    #[tokio::test]
    async fn task_process_setup_failure_elects_failed_without_launch() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        let outcome = SafePickupExecutor::new(
            runtime.client.clone(),
            FailingLauncher,
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            "executor-one",
            Duration::from_millis(200),
        )
        .run_one()
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            PickupOutcome::ProcessSetupFailed {
                election: TerminalElection::Won,
                ..
            }
        ));
        let state = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            state.staged_event_kinds,
            ["Assigned".to_owned(), "Failed".to_owned()]
        );
        assert_eq!(
            state.terminal_outcome,
            Some(TaskPickupTerminalOutcome::ProcessSetupFailed)
        );
        runtime.stop().await;
    }

    #[tokio::test]
    async fn restart_preserves_claim_until_deadline_then_elects_liveness_without_relaunch() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let launch_log = temp.path().join("launches");
        let first = WriterRuntime::normal(repository.clone());
        let (dispatch_key, _) = first
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        let error = executor(
            first.client.clone(),
            launch_log.clone(),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            FailAt(PickupBoundary::AfterClaimProof),
        )
        .run_one()
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SafePickupError::Checkpoint {
                boundary: PickupBoundary::AfterClaimProof,
                ..
            }
        ));
        first.stop().await;

        let claimed = snapshot(&repository, &dispatch_key).await;
        let deadline = claimed.liveness_deadline.unwrap();
        let restart = WriterRuntime::normal(repository.clone());
        let recovered = executor(
            restart.client.clone(),
            launch_log.clone(),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        );
        assert_eq!(
            recovered
                .reconcile_one_due_liveness(deadline - Duration::from_millis(1))
                .await
                .unwrap(),
            None
        );
        assert_eq!(recovered.run_one().await.unwrap(), PickupOutcome::NoWork);
        assert_eq!(launch_count(&launch_log), 0);

        let (claim, election) = recovered
            .reconcile_one_due_liveness(deadline + Duration::from_millis(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.pickup_generation, 1);
        assert_eq!(election, TerminalElection::Won);
        assert_eq!(recovered.run_one().await.unwrap(), PickupOutcome::NoWork);
        let settled = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            settled.staged_event_kinds,
            ["Assigned".to_owned(), "Unhealthy".to_owned()]
        );
        assert_eq!(
            settled.terminal_outcome,
            Some(TaskPickupTerminalOutcome::LivenessExpired)
        );
        assert_eq!(launch_count(&launch_log), 0);
        restart.stop().await;
    }

    #[tokio::test]
    async fn liveness_failure_registration_is_durable_evidence_not_a_terminal_outcome() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let payload = encode_dispatch(&task);
        let (dispatch_key, _) = runtime.client.stage_dispatch(&payload).await.unwrap();
        let (claim, _) = claim_without_spawn(&runtime.client, &task).await;
        let failed_at = pickup_now();

        assert!(runtime
            .client
            .register_liveness_failure(&claim, failed_at)
            .await
            .unwrap());
        let failed = snapshot(&repository, &dispatch_key).await;
        assert_eq!(failed.terminal_outcome, None);
        assert_eq!(failed.liveness_deadline, Some(failed_at));
        runtime.stop().await;

        let restart = WriterRuntime::normal(repository.clone());
        let recovered = executor(
            restart.client.clone(),
            temp.path().join("launches"),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        )
        .reconcile_one_due_liveness(failed_at + Duration::from_millis(1))
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered.0.dispatch_key, claim.dispatch_key);
        assert_eq!(recovered.0.pickup_generation, claim.pickup_generation);
        assert_eq!(recovered.0.owner, claim.owner);
        assert_eq!(recovered.0.liveness_deadline, failed_at);
        assert_eq!(recovered.1, TerminalElection::Won);
        assert_eq!(launch_count(&temp.path().join("launches")), 0);
        restart.stop().await;
    }

    #[tokio::test]
    async fn lite_adapter_satisfies_backend_neutral_attempt_outcome_law() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&task))
            .await
            .unwrap();
        let (claim, _) = claim_without_spawn(&runtime.client, &task).await;
        assert!(runtime
            .client
            .stage_started(&claim, b"backend-neutral Started", pickup_now())
            .await
            .unwrap());

        let winner =
            attempt_outcome_laws::assert_attempt_outcome_law(runtime.client.clone(), &claim).await;
        let settled = snapshot(&repository, &dispatch_key).await;
        assert!(matches!(
            winner,
            LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::LivenessExpired
        ));
        assert_eq!(settled.staged_event_kinds.len(), 3);
        runtime.stop().await;
    }

    #[tokio::test]
    async fn terminal_crash_boundaries_settle_once_without_second_launch() {
        for boundary in [
            PickupBoundary::AfterProcessExitObservation,
            PickupBoundary::AfterTerminalElection,
            PickupBoundary::AfterTerminalEventStaging,
        ] {
            let temp = TempDir::new().unwrap();
            let repository = open_repository(&temp).await;
            let launch_log = temp.path().join("launches");
            let first = WriterRuntime::normal(repository.clone());
            let (dispatch_key, _) = first
                .client
                .stage_dispatch(&encode_dispatch(&valid_task()))
                .await
                .unwrap();
            let error = executor(
                first.client.clone(),
                launch_log.clone(),
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
                FailAt(boundary),
            )
            .run_one()
            .await
            .unwrap_err();
            assert!(
                matches!(error, SafePickupError::Checkpoint { boundary: hit, .. } if hit == boundary)
            );
            first.stop().await;

            let after_crash = snapshot(&repository, &dispatch_key).await;
            let restart = WriterRuntime::normal(repository.clone());
            let recovered = executor(
                restart.client.clone(),
                launch_log.clone(),
                LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
                NoopPickupCheckpoint,
            );
            if boundary == PickupBoundary::AfterProcessExitObservation {
                assert_eq!(after_crash.terminal_outcome, None);
                let deadline = after_crash.liveness_deadline.unwrap();
                assert!(recovered
                    .reconcile_one_due_liveness(deadline + Duration::from_millis(1))
                    .await
                    .unwrap()
                    .is_some());
            } else {
                assert_eq!(
                    after_crash.terminal_outcome,
                    Some(TaskPickupTerminalOutcome::ProcessExitedSuccess)
                );
                assert_eq!(
                    after_crash.staged_event_kinds,
                    [
                        "Assigned".to_owned(),
                        "Started".to_owned(),
                        "Completed".to_owned(),
                    ]
                );
                assert_eq!(
                    recovered
                        .reconcile_one_due_liveness(
                            after_crash.liveness_deadline.unwrap() + Duration::from_millis(1)
                        )
                        .await
                        .unwrap(),
                    None
                );
            }
            assert_eq!(recovered.run_one().await.unwrap(), PickupOutcome::NoWork);
            assert_eq!(launch_count(&launch_log), 1, "{boundary:?}");
            restart.stop().await;
        }
    }

    #[tokio::test]
    async fn terminal_winner_replays_until_forward_completion_and_absorbs_late_contenders() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let launch_log = temp.path().join("launches");
        let runtime = WriterRuntime::normal(repository.clone());
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&valid_task()))
            .await
            .unwrap();
        let outcome = executor(
            runtime.client.clone(),
            launch_log.clone(),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        )
        .run_one()
        .await
        .unwrap();
        let claim = match outcome {
            PickupOutcome::Launched {
                claim,
                election: TerminalElection::Won,
                ..
            } => claim,
            other => panic!("unexpected pickup outcome: {other:?}"),
        };
        assert_eq!(
            runtime
                .client
                .elect_terminal(
                    &claim,
                    LocalAttemptOutcome::LivenessExpired,
                    b"late event must not stage",
                    pickup_now(),
                )
                .await
                .unwrap(),
            TerminalElection::Settled(LocalAttemptOutcome::ProcessExitedSuccess)
        );
        assert!(!runtime
            .client
            .stage_started(&claim, b"late Started", pickup_now())
            .await
            .unwrap());

        let (relay_tx, mut relay_rx) = mpsc::channel(8);
        for _ in 0..2 {
            assert!(runtime
                .client
                .forward_next_task_event(&relay_tx)
                .await
                .unwrap());
            assert_eq!(
                relay_rx.recv().await.unwrap().entity_type,
                EntityType::TaskEvent as i32
            );
        }
        let terminal = runtime.client.next_task_event().await.unwrap().unwrap();
        assert_eq!(terminal.kind, "Completed");
        relay_tx
            .send(ConductorRelayMessage {
                entity_type: EntityType::TaskEvent as i32,
                payload: terminal.event.clone(),
                tenant_id: None,
            })
            .await
            .unwrap();
        let forwarded_before_crash = relay_rx.recv().await.unwrap();
        runtime.stop().await;

        let restart = WriterRuntime::normal(repository.clone());
        assert!(restart
            .client
            .forward_next_task_event(&relay_tx)
            .await
            .unwrap());
        let replayed = relay_rx.recv().await.unwrap();
        assert_eq!(replayed.payload, forwarded_before_crash.payload);
        assert!(!restart
            .client
            .forward_next_task_event(&relay_tx)
            .await
            .unwrap());
        let settled = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            settled.forwarded_event_kinds,
            [
                "Assigned".to_owned(),
                "Started".to_owned(),
                "Completed".to_owned(),
            ]
        );
        assert_eq!(
            settled.staged_event_kinds,
            [
                "Assigned".to_owned(),
                "Started".to_owned(),
                "Completed".to_owned(),
            ]
        );
        assert_eq!(launch_count(&launch_log), 1);
        restart.stop().await;
    }

    #[tokio::test]
    async fn cancellation_before_claim_fences_spawn_and_replays_ack_at_relay_boundary() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let payload = encode_dispatch(&task);
        let (dispatch_key, _) = runtime.client.stage_dispatch(&payload).await.unwrap();
        let (pickup, task_handler) =
            long_running_pickup(runtime.client.clone(), temp.path().join("launches"));
        let coordinator = SafeCancellationCoordinator::new(runtime.client.clone());

        let outcome = coordinator
            .cancel_request(&task_handler, cancel_request(&task))
            .await
            .unwrap();
        assert_eq!(
            outcome.reconciliation,
            CancellationReconciliation::NoProcess
        );
        assert_eq!(outcome.election, Some(TerminalElection::Won));
        assert_eq!(pickup.run_one().await.unwrap(), PickupOutcome::NoWork);
        assert_eq!(launch_count(&temp.path().join("launches")), 0);

        let fenced = snapshot(&repository, &dispatch_key).await;
        let fence = fenced.cancellation_fence.as_ref().unwrap();
        assert_eq!(fence.pickup_generation, Some(1));
        assert_eq!(fence.owner, None);
        assert!(!fence.owner_notified);
        assert_eq!(
            fence.terminal_outcome,
            Some(TaskPickupTerminalOutcome::CancellationNoProcess)
        );
        assert_eq!(
            fenced.cancellation_reconciliation,
            Some(TaskCancellationReconciliation::NoProcess)
        );
        assert!(!fenced.cancellation_ack_forwarded);

        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        assert!(runtime
            .client
            .forward_next_cancellation_ack(&closed_tx)
            .await
            .is_err());
        assert!(
            !snapshot(&repository, &dispatch_key)
                .await
                .cancellation_ack_forwarded
        );

        let expected_ack = encode_cancel_ack(
            task.task_instance_id,
            task.workflow_instance_id,
            KillOutcome::NoSuchTask,
        );
        let pending = runtime
            .client
            .next_cancellation_ack()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.acknowledgement, expected_ack);
        let (relay_tx, mut relay_rx) = mpsc::channel(2);
        relay_tx
            .send(ConductorRelayMessage {
                entity_type: EntityType::CancelTaskAck as i32,
                payload: pending.acknowledgement.clone(),
                tenant_id: None,
            })
            .await
            .unwrap();
        let forwarded_before_crash = relay_rx.recv().await.unwrap();
        runtime.stop().await;

        let restart = WriterRuntime::normal(repository.clone());
        assert!(restart
            .client
            .forward_next_cancellation_ack(&relay_tx)
            .await
            .unwrap());
        let replayed = relay_rx.recv().await.unwrap();
        assert_eq!(replayed.entity_type, EntityType::CancelTaskAck as i32);
        assert_eq!(replayed.payload, forwarded_before_crash.payload);
        assert!(!restart
            .client
            .forward_next_cancellation_ack(&relay_tx)
            .await
            .unwrap());
        assert!(
            snapshot(&repository, &dispatch_key)
                .await
                .cancellation_ack_forwarded
        );

        let duplicate = SafeCancellationCoordinator::new(restart.client.clone())
            .cancel_request(&task_handler, cancel_request(&task))
            .await
            .unwrap();
        assert_eq!(
            duplicate.fence.acknowledgement_identity,
            outcome.fence.acknowledgement_identity
        );
        assert!(!restart
            .client
            .forward_next_cancellation_ack(&relay_tx)
            .await
            .unwrap());
        restart.stop().await;
    }

    #[tokio::test]
    async fn cancellation_after_claim_blocks_spawn_proof_and_wins_terminal_election_once() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&task))
            .await
            .unwrap();
        let (claim, assigned) = claim_without_spawn(&runtime.client, &task).await;
        let request = cancel_request(&task);
        let acknowledgement_identity = format!(
            "cancel-task-ack-v1:{}:{}",
            request.workflow_instance_id, request.task_instance_id
        );
        let fence = runtime
            .client
            .commit_cancellation_fence(&acknowledgement_identity, request, pickup_now())
            .await
            .unwrap();
        assert_eq!(fence.owner.as_deref(), Some("executor-one"));
        assert!(!fence.owner_notified);
        assert!(!runtime
            .client
            .prove_ready_to_launch(&claim, &assigned)
            .await
            .unwrap());

        let task_handler = LocalTaskHandler::new(RealChildLauncher {
            launch_log: temp.path().join("launches"),
            run_for: Duration::from_secs(30),
        });
        let outcome = SafeCancellationCoordinator::new(runtime.client.clone())
            .cancel_request(&task_handler, request)
            .await
            .unwrap();
        assert_eq!(
            outcome.reconciliation,
            CancellationReconciliation::NoProcess
        );
        assert_eq!(outcome.election, Some(TerminalElection::Won));
        let settled = snapshot(&repository, &dispatch_key).await;
        assert!(settled.cancellation_fence.unwrap().owner_notified);
        assert_eq!(
            runtime
                .client
                .elect_terminal(
                    &claim,
                    LocalAttemptOutcome::ProcessExitedSuccess,
                    b"late process exit",
                    pickup_now(),
                )
                .await
                .unwrap(),
            TerminalElection::Settled(LocalAttemptOutcome::CancellationNoProcess)
        );
        assert_eq!(launch_count(&temp.path().join("launches")), 0);
        runtime.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_during_execution_signals_and_reaps_the_owned_process_group() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&task))
            .await
            .unwrap();
        let launch_log = temp.path().join("launches");
        let (pickup, task_handler) =
            long_running_pickup(runtime.client.clone(), launch_log.clone());
        let pickup_task = tokio::spawn(async move { pickup.run_one().await });
        timeout(Duration::from_secs(5), async {
            while launch_count(&launch_log) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let outcome = SafeCancellationCoordinator::new(runtime.client.clone())
            .cancel_request(&task_handler, cancel_request(&task))
            .await
            .unwrap();
        assert_eq!(outcome.reconciliation, CancellationReconciliation::Killed);
        assert_eq!(outcome.election, Some(TerminalElection::Won));
        assert!(matches!(
            pickup_task.await.unwrap().unwrap(),
            PickupOutcome::Cancelled {
                reconciliation: CancellationReconciliation::Killed,
                ..
            }
        ));
        for pid in launched_process_ids(&launch_log) {
            assert!(
                !process_exists(pid).await,
                "process {pid} survived cancellation"
            );
        }
        let settled = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            settled.terminal_outcome,
            Some(TaskPickupTerminalOutcome::CancellationKilled)
        );
        assert_eq!(
            settled.cancellation_reconciliation,
            Some(TaskCancellationReconciliation::Killed)
        );
        assert!(settled.cancellation_fence.unwrap().owner_notified);
        let pending = runtime
            .client
            .next_cancellation_ack()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pending.acknowledgement,
            encode_cancel_ack(
                task.task_instance_id,
                task.workflow_instance_id,
                KillOutcome::Killed,
            )
        );
        runtime.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn formation_stop_reaps_process_group_and_registers_due_liveness() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let runtime = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let (dispatch_key, _) = runtime
            .client
            .stage_dispatch(&encode_dispatch(&task))
            .await
            .unwrap();
        let launch_log = temp.path().join("launches");
        let (pickup, task_handler) =
            long_running_pickup(runtime.client.clone(), launch_log.clone());
        let pickup_task = tokio::spawn(async move { pickup.run_one().await });
        timeout(Duration::from_secs(5), async {
            while launch_count(&launch_log) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        task_handler.stop_all().await;
        let claim = match pickup_task.await.unwrap().unwrap() {
            PickupOutcome::Cancelled {
                claim,
                reconciliation: CancellationReconciliation::Killed,
            } => claim,
            outcome => panic!("unexpected formation-stop outcome: {outcome:?}"),
        };
        for pid in launched_process_ids(&launch_log) {
            assert!(
                !process_exists(pid).await,
                "process {pid} survived formation stop"
            );
        }
        let stopped = snapshot(&repository, &dispatch_key).await;
        let stopped_at = stopped.liveness_deadline.unwrap();
        assert_eq!(stopped.terminal_outcome, None);
        runtime.stop().await;

        let restart = WriterRuntime::normal(repository.clone());
        let recovered = executor(
            restart.client.clone(),
            temp.path().join("restart-launches"),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        )
        .reconcile_one_due_liveness(stopped_at + Duration::from_millis(1))
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered.0.dispatch_key, claim.dispatch_key);
        assert_eq!(recovered.0.pickup_generation, claim.pickup_generation);
        assert_eq!(recovered.1, TerminalElection::Won);
        assert_eq!(launch_count(&temp.path().join("restart-launches")), 0);
        restart.stop().await;
    }

    #[tokio::test]
    async fn restart_leaves_uncertain_owner_fenced_until_liveness_supplies_evidence() {
        let temp = TempDir::new().unwrap();
        let repository = open_repository(&temp).await;
        let first = WriterRuntime::normal(repository.clone());
        let task = valid_task();
        let (dispatch_key, _) = first
            .client
            .stage_dispatch(&encode_dispatch(&task))
            .await
            .unwrap();
        let (claim, _) = claim_without_spawn(&first.client, &task).await;
        let request = cancel_request(&task);
        let acknowledgement_identity = format!(
            "cancel-task-ack-v1:{}:{}",
            request.workflow_instance_id, request.task_instance_id
        );
        let fence = first
            .client
            .commit_cancellation_fence(&acknowledgement_identity, request, pickup_now())
            .await
            .unwrap();
        assert!(!fence.owner_notified);
        first.stop().await;

        let restart = WriterRuntime::normal(repository.clone());
        let recovered = executor(
            restart.client.clone(),
            temp.path().join("launches"),
            LocalExecutorCapacity::new(new_pickup_identity(), NonZeroUsize::new(1).unwrap()),
            NoopPickupCheckpoint,
        );
        let coordinator = SafeCancellationCoordinator::new(restart.client.clone());
        assert_eq!(coordinator.reconcile_one().await.unwrap(), None);
        assert!(recovered
            .reconcile_one_due_liveness(claim.liveness_deadline + Duration::from_millis(1))
            .await
            .unwrap()
            .is_some());
        let outcome = coordinator.reconcile_one().await.unwrap().unwrap();
        assert_eq!(
            outcome.reconciliation,
            CancellationReconciliation::AlreadyExited
        );
        assert_eq!(
            outcome.election,
            Some(TerminalElection::Settled(
                LocalAttemptOutcome::LivenessExpired
            ))
        );
        assert_eq!(recovered.run_one().await.unwrap(), PickupOutcome::NoWork);
        assert_eq!(launch_count(&temp.path().join("launches")), 0);
        let settled = snapshot(&repository, &dispatch_key).await;
        assert_eq!(
            settled.terminal_outcome,
            Some(TaskPickupTerminalOutcome::LivenessExpired)
        );
        assert_eq!(
            settled.cancellation_reconciliation,
            Some(TaskCancellationReconciliation::AlreadyExited)
        );
        assert!(!settled.cancellation_fence.unwrap().owner_notified);
        let acknowledgement = restart
            .client
            .next_cancellation_ack()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            acknowledgement.acknowledgement,
            encode_cancel_ack(
                task.task_instance_id,
                task.workflow_instance_id,
                KillOutcome::NoSuchTask,
            )
        );
        restart.stop().await;
    }
}
