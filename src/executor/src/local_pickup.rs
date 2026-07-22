#![allow(async_fn_in_trait)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::process::{Child, Command};
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::time::sleep;
use uuid::Uuid;

use crate::task_handler::{teardown_own_group, CANCEL_GRACE};
use crate::wire::{
    decode_cancel_request, decode_dispatch, encode_cancel_ack, encode_task_event,
    encode_unhealthy_task_event, CancelRequest, DispatchedTask, EmitKind, KillOutcome,
};
pub fn new_pickup_identity() -> Uuid {
    Uuid::new_v4()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLocalDispatch {
    pub dispatch_key: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPickupClaim {
    pub dispatch_key: String,
    pub pickup_generation: i64,
    pub owner: String,
    pub liveness_deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ClaimLocalPickup<'a> {
    pub dispatch_key: &'a str,
    pub owner: &'a str,
    pub liveness_deadline: DateTime<Utc>,
    pub assigned_event: &'a [u8],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimWriteError {
    /// The durable commit may have succeeded, but its acknowledgement was lost.
    Ambiguous,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueLocalPickup {
    pub claim: LocalPickupClaim,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAttemptOutcome {
    ProcessExitedSuccess,
    ProcessExitedFailure,
    ProcessSetupFailed,
    LivenessExpired,
    CancellationKilled,
    CancellationAlreadyExited,
    CancellationNoProcess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalElection {
    Won,
    Settled(LocalAttemptOutcome),
}

impl fmt::Display for ClaimWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ambiguous => formatter.write_str("pickup claim acknowledgement is ambiguous"),
            Self::Failed(message) => formatter.write_str(message),
        }
    }
}

pub trait SafePickupWriter: Clone + Send + Sync + 'static {
    async fn select_pending(&self) -> Result<Option<PendingLocalDispatch>, String>;

    async fn reject_poison(
        &self,
        dispatch_key: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    async fn claim(
        &self,
        input: ClaimLocalPickup<'_>,
    ) -> Result<Option<LocalPickupClaim>, ClaimWriteError>;

    async fn prove_ambiguous_claim(
        &self,
        dispatch_key: &str,
        owner: &str,
        assigned_event: &[u8],
    ) -> Result<Option<LocalPickupClaim>, String>;

    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    async fn prove_ready_to_launch(
        &self,
        claim: &LocalPickupClaim,
        assigned_event: &[u8],
    ) -> Result<bool, String>;

    async fn stage_started(
        &self,
        claim: &LocalPickupClaim,
        started_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;
}

/// One conditional terminal transition shared by every local contender for a
/// claimed pickup generation.
pub trait SafeAttemptOutcomeHandoff: Clone + Send + Sync + 'static {
    async fn select_due_liveness(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DueLocalPickup>, String>;

    /// Persist failure evidence for a claimed generation before formation-wide
    /// cancellation. Recovery, not the dying process, elects any later outcome.
    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        now: DateTime<Utc>,
    ) -> Result<TerminalElection, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReconciliation {
    Killed,
    AlreadyExited,
    NoProcess,
}

impl CancellationReconciliation {
    fn kill_outcome(self) -> KillOutcome {
        match self {
            Self::Killed => KillOutcome::Killed,
            Self::AlreadyExited | Self::NoProcess => KillOutcome::NoSuchTask,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalCancellationFence {
    pub acknowledgement_identity: String,
    pub request: CancelRequest,
    pub dispatch_key: Option<String>,
    pub pickup_generation: Option<i64>,
    pub owner: Option<String>,
    pub owner_notified: bool,
    pub liveness_deadline: Option<DateTime<Utc>>,
    pub terminal_outcome: Option<LocalAttemptOutcome>,
}

impl LocalCancellationFence {
    fn pickup_owner_key(&self) -> Option<PickupOwnerKey> {
        Some(PickupOwnerKey {
            dispatch_key: self.dispatch_key.clone()?,
            pickup_generation: self.pickup_generation?,
            owner: self.owner.clone()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationOutcome {
    pub fence: LocalCancellationFence,
    pub reconciliation: CancellationReconciliation,
    pub election: Option<TerminalElection>,
}

/// Durable generation-qualified cancellation barrier and acknowledgement outbox.
pub trait SafeCancellationFence: Clone + Send + Sync + 'static {
    async fn commit_cancellation_fence(
        &self,
        acknowledgement_identity: &str,
        request: CancelRequest,
        now: DateTime<Utc>,
    ) -> Result<LocalCancellationFence, String>;

    async fn mark_cancellation_owner_notified(
        &self,
        fence: &LocalCancellationFence,
        now: DateTime<Utc>,
    ) -> Result<bool, String>;

    async fn settle_cancellation(
        &self,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
        acknowledgement: &[u8],
        now: DateTime<Utc>,
    ) -> Result<Option<TerminalElection>, String>;

    async fn select_unresolved_cancellation(
        &self,
    ) -> Result<Option<LocalCancellationFence>, String>;
}

/// Read-only capacity observation for the sole local Executor.
///
/// The interface intentionally exposes no permit, reservation, dispatch, or
/// lifecycle operation. Queue ownership remains with `SafePickupExecutor`.
pub trait ExecutorFleetStatus: Clone + Send + Sync + 'static {
    fn snapshot(&self) -> ExecutorCapacitySnapshot;
}

/// One Executor's configured process slots and currently held slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorCapacitySnapshot {
    pub executor_id: Uuid,
    pub configured_process_slots: usize,
    pub in_flight_count: usize,
}

/// Tickr Lite's observational status handle for exactly one Executor.
#[derive(Debug, Clone)]
pub struct LocalExecutorFleetStatus {
    executor_id: Uuid,
    configured_process_slots: NonZeroUsize,
    process_slots: Arc<Semaphore>,
}

impl LocalExecutorFleetStatus {
    pub fn new(executor_id: Uuid, configured_process_slots: NonZeroUsize) -> Self {
        Self {
            executor_id,
            configured_process_slots,
            process_slots: Arc::new(Semaphore::new(configured_process_slots.get())),
        }
    }
}

impl ExecutorFleetStatus for LocalExecutorFleetStatus {
    fn snapshot(&self) -> ExecutorCapacitySnapshot {
        ExecutorCapacitySnapshot {
            executor_id: self.executor_id,
            configured_process_slots: self.configured_process_slots.get(),
            in_flight_count: self
                .configured_process_slots
                .get()
                .saturating_sub(self.process_slots.available_permits()),
        }
    }
}

pub trait TaskProcessLauncher: Clone + Send + Sync + 'static {
    async fn spawn(&self, task: &DispatchedTask) -> Result<Child, String>;

    async fn spawn_claimed(
        &self,
        task: &DispatchedTask,
        _claim: &LocalPickupClaim,
    ) -> Result<Child, String> {
        self.spawn(task).await
    }

    async fn process_exited(
        &self,
        _task: &DispatchedTask,
        _claim: &LocalPickupClaim,
        _status: &std::process::ExitStatus,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn process_stopped(
        &self,
        _task: &DispatchedTask,
        _claim: &LocalPickupClaim,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PickupOwnerKey {
    dispatch_key: String,
    pickup_generation: i64,
    owner: String,
}

impl From<&LocalPickupClaim> for PickupOwnerKey {
    fn from(claim: &LocalPickupClaim) -> Self {
        Self {
            dispatch_key: claim.dispatch_key.clone(),
            pickup_generation: claim.pickup_generation,
            owner: claim.owner.clone(),
        }
    }
}

#[derive(Clone)]
struct RunningCancellation {
    token: tokio_util::sync::CancellationToken,
    completion: watch::Receiver<Option<CancellationReconciliation>>,
}

#[derive(Default)]
struct LocalTaskHandlerState {
    running: HashMap<PickupOwnerKey, RunningCancellation>,
    fenced: HashSet<PickupOwnerKey>,
    stopping: bool,
}

/// Owns every local Task process from spawn through process-group teardown and reap.
#[derive(Clone)]
pub struct LocalTaskHandler<L> {
    launcher: L,
    state: Arc<Mutex<LocalTaskHandlerState>>,
}

impl<L> LocalTaskHandler<L>
where
    L: TaskProcessLauncher,
{
    pub fn new(launcher: L) -> Self {
        Self {
            launcher,
            state: Arc::new(Mutex::new(LocalTaskHandlerState::default())),
        }
    }

    /// Request process-group teardown for every registered task and wait until
    /// each owning handler has reaped its child.
    pub async fn stop_all(&self) {
        let completions = {
            let mut state = self.state.lock().await;
            state.stopping = true;
            state
                .running
                .values()
                .map(|running| {
                    running.token.cancel();
                    running.completion.clone()
                })
                .collect::<Vec<_>>()
        };
        for mut completion in completions {
            while completion.borrow().is_none() && completion.changed().await.is_ok() {}
        }
    }
}

/// Production launcher for a local Task instance. Process containment remains
/// the task handler's responsibility; dropping this child kills it rather than
/// leaving an untracked process after a failed durable handoff.
#[derive(Debug, Clone)]
pub struct NixTaskProcessLauncher {
    executable: PathBuf,
}

impl Default for NixTaskProcessLauncher {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("nix"),
        }
    }
}

impl NixTaskProcessLauncher {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }

    /// Spawn the selected Nix task with formation-provided environment values.
    pub async fn spawn_with_environment(
        &self,
        task: &DispatchedTask,
        environment: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Child, String> {
        let mut command = Command::new(&self.executable);
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("run")
            .arg(&task.nix_expression_path)
            .args(&task.nix_args)
            .envs(environment)
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        command
            .spawn()
            .map_err(|error| format!("spawn Task instance process: {error}"))
    }
}

impl TaskProcessLauncher for NixTaskProcessLauncher {
    async fn spawn(&self, task: &DispatchedTask) -> Result<Child, String> {
        let mut command = Command::new(&self.executable);
        command
            .arg("run")
            .arg(&task.nix_expression_path)
            .args(&task.nix_args)
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        command
            .spawn()
            .map_err(|error| format!("spawn Task instance process: {error}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickupBoundary {
    BeforeSelection,
    AfterSelection,
    AfterValidation,
    AfterClaimCommit,
    AfterAssignedStaging,
    AfterInitialLivenessArm,
    AfterClaimProof,
    AfterSpawn,
    AfterStartedStaging,
    AfterFirstLivenessRenewal,
    AfterProcessExitObservation,
    AfterTerminalElection,
    AfterTerminalEventStaging,
}

pub trait PickupCheckpoint: Clone + Send + Sync + 'static {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPickupCheckpoint;

impl PickupCheckpoint for NoopPickupCheckpoint {
    fn reached(&self, _boundary: PickupBoundary) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickupOutcome {
    NoWork,
    PoisonRejected {
        dispatch_key: String,
    },
    ClaimUnavailable {
        dispatch_key: String,
    },
    ProcessSetupFailed {
        claim: LocalPickupClaim,
        election: TerminalElection,
        message: String,
    },
    Launched {
        claim: LocalPickupClaim,
        exit_success: bool,
        election: TerminalElection,
    },
    Cancelled {
        claim: LocalPickupClaim,
        reconciliation: CancellationReconciliation,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SafePickupError {
    #[error("local task pickup writer failed: {0}")]
    Writer(String),
    #[error("pickup claim acknowledgement was ambiguous and could not be proved")]
    AmbiguousClaimUnproved,
    #[error("committed pickup claim could not be armed for liveness")]
    InitialLivenessArmFailed,
    #[error("committed pickup claim or Assigned staging record could not be proved")]
    ClaimProofFailed,
    #[error("formation shutdown began before Task process registration")]
    FormationStopping,
    #[error("task process launch failed: {0}")]
    Launch(String),
    #[error("Started staging failed after process launch")]
    StartedStagingFailed,
    #[error("liveness renewal failed for claimed pickup {claim:?}: {reason}")]
    LivenessRenewalFailed {
        claim: LocalPickupClaim,
        reason: String,
    },
    #[error("terminal outcome election failed: {0}")]
    TerminalElection(String),
    #[error("pickup checkpoint {boundary:?} interrupted the handler: {message}")]
    Checkpoint {
        boundary: PickupBoundary,
        message: String,
    },
}

#[derive(Clone)]
pub struct SafePickupExecutor<W, L, C = NoopPickupCheckpoint> {
    writer: W,
    task_handler: LocalTaskHandler<L>,
    checkpoint: C,
    fleet_status: LocalExecutorFleetStatus,
    owner: String,
    liveness_timeout: chrono::Duration,
}

impl<W, L> SafePickupExecutor<W, L, NoopPickupCheckpoint>
where
    W: SafePickupWriter + SafeAttemptOutcomeHandoff,
    L: TaskProcessLauncher,
{
    pub fn new(
        writer: W,
        launcher: L,
        fleet_status: LocalExecutorFleetStatus,
        owner: impl Into<String>,
        liveness_timeout: Duration,
    ) -> Self {
        Self::with_checkpoint(
            writer,
            launcher,
            NoopPickupCheckpoint,
            fleet_status,
            owner,
            liveness_timeout,
        )
    }
}

impl<W, L, C> SafePickupExecutor<W, L, C>
where
    W: SafePickupWriter + SafeAttemptOutcomeHandoff,
    L: TaskProcessLauncher,
    C: PickupCheckpoint,
{
    pub fn with_checkpoint(
        writer: W,
        launcher: L,
        checkpoint: C,
        fleet_status: LocalExecutorFleetStatus,
        owner: impl Into<String>,
        liveness_timeout: Duration,
    ) -> Self {
        Self::with_task_handler(
            writer,
            LocalTaskHandler::new(launcher),
            checkpoint,
            fleet_status,
            owner,
            liveness_timeout,
        )
    }

    pub fn with_task_handler(
        writer: W,
        task_handler: LocalTaskHandler<L>,
        checkpoint: C,
        fleet_status: LocalExecutorFleetStatus,
        owner: impl Into<String>,
        liveness_timeout: Duration,
    ) -> Self {
        let liveness_timeout = chrono::Duration::from_std(liveness_timeout)
            .expect("liveness timeout must fit chrono::Duration");
        assert!(
            liveness_timeout > chrono::Duration::zero(),
            "liveness timeout must be positive"
        );
        Self {
            writer,
            task_handler,
            checkpoint,
            fleet_status,
            owner: owner.into(),
            liveness_timeout,
        }
    }

    pub fn task_handler(&self) -> LocalTaskHandler<L> {
        self.task_handler.clone()
    }

    /// Handle at most one durable dispatch. Capacity is acquired before
    /// selection and held through process exit, so saturation cannot mutate or
    /// release the pending record.
    pub async fn run_one(&self) -> Result<PickupOutcome, SafePickupError> {
        let _permit = self
            .fleet_status
            .process_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| SafePickupError::Writer(error.to_string()))?;
        self.reach(PickupBoundary::BeforeSelection)?;
        let Some(pending) = self
            .writer
            .select_pending()
            .await
            .map_err(SafePickupError::Writer)?
        else {
            return Ok(PickupOutcome::NoWork);
        };
        self.reach(PickupBoundary::AfterSelection)?;

        let task = match decode_dispatch(&pending.payload).and_then(validate_dispatch) {
            Ok(task) => task,
            Err(error) => {
                let reason = error.to_string();
                let rejected = self
                    .writer
                    .reject_poison(&pending.dispatch_key, &reason, Utc::now())
                    .await
                    .map_err(SafePickupError::Writer)?;
                if !rejected {
                    return Err(SafePickupError::Writer(
                        "selected poison dispatch was no longer pending".to_owned(),
                    ));
                }
                return Ok(PickupOutcome::PoisonRejected {
                    dispatch_key: pending.dispatch_key,
                });
            }
        };
        self.reach(PickupBoundary::AfterValidation)?;

        let assigned_event =
            encode_task_event(&task, self.fleet_status.executor_id, EmitKind::Assigned);
        let claim_now = Utc::now();
        let initial_deadline = claim_now + self.liveness_timeout;
        let claim = match self
            .writer
            .claim(ClaimLocalPickup {
                dispatch_key: &pending.dispatch_key,
                owner: &self.owner,
                liveness_deadline: initial_deadline,
                assigned_event: &assigned_event,
                now: claim_now,
            })
            .await
        {
            Ok(Some(claim)) => claim,
            Ok(None) => {
                return Ok(PickupOutcome::ClaimUnavailable {
                    dispatch_key: pending.dispatch_key,
                });
            }
            Err(ClaimWriteError::Ambiguous) => self
                .writer
                .prove_ambiguous_claim(&pending.dispatch_key, &self.owner, &assigned_event)
                .await
                .map_err(SafePickupError::Writer)?
                .ok_or(SafePickupError::AmbiguousClaimUnproved)?,
            Err(ClaimWriteError::Failed(message)) => {
                return Err(SafePickupError::Writer(message));
            }
        };
        self.reach(PickupBoundary::AfterClaimCommit)?;
        // Assigned is part of the same transaction as the claim. Separate
        // checkpoint names make that indivisible boundary explicit in crash tests.
        self.reach(PickupBoundary::AfterAssignedStaging)?;

        let arm_now = Utc::now();
        let armed_deadline = arm_now + self.liveness_timeout;
        if !self
            .writer
            .arm_liveness(&claim, armed_deadline, arm_now)
            .await
            .map_err(SafePickupError::Writer)?
        {
            return Err(SafePickupError::InitialLivenessArmFailed);
        }
        self.reach(PickupBoundary::AfterInitialLivenessArm)?;

        let pickup_owner = PickupOwnerKey::from(&claim);
        let mut handler_state = self.task_handler.state.lock().await;
        if handler_state.stopping {
            return Err(SafePickupError::FormationStopping);
        }
        if handler_state.fenced.remove(&pickup_owner) {
            return Ok(PickupOutcome::Cancelled {
                claim,
                reconciliation: CancellationReconciliation::NoProcess,
            });
        }
        if !self
            .writer
            .prove_ready_to_launch(&claim, &assigned_event)
            .await
            .map_err(SafePickupError::Writer)?
        {
            return Err(SafePickupError::ClaimProofFailed);
        }
        self.reach(PickupBoundary::AfterClaimProof)?;

        let mut child = match self
            .task_handler
            .launcher
            .spawn_claimed(&task, &claim)
            .await
        {
            Ok(child) => child,
            Err(message) => {
                let election = self
                    .elect_outcome(&task, &claim, LocalAttemptOutcome::ProcessSetupFailed)
                    .await?;
                return Ok(PickupOutcome::ProcessSetupFailed {
                    claim,
                    election,
                    message,
                });
            }
        };
        let process_group = child.id().map(|id| id as i32);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let (completion_tx, completion_rx) = watch::channel(None);
        handler_state.running.insert(
            pickup_owner.clone(),
            RunningCancellation {
                token: cancel_token.clone(),
                completion: completion_rx,
            },
        );
        drop(handler_state);

        if let Err(error) = self.reach(PickupBoundary::AfterSpawn) {
            self.stop_registered_process(
                &task,
                &claim,
                &pickup_owner,
                process_group,
                &mut child,
                &completion_tx,
            )
            .await;
            return Err(error);
        }

        let started_event =
            encode_task_event(&task, self.fleet_status.executor_id, EmitKind::Started);
        let started = self
            .writer
            .stage_started(&claim, &started_event, Utc::now())
            .await
            .map_err(SafePickupError::Writer)?;
        if !started {
            self.stop_registered_process(
                &task,
                &claim,
                &pickup_owner,
                process_group,
                &mut child,
                &completion_tx,
            )
            .await;
            return Err(SafePickupError::StartedStagingFailed);
        }
        if let Err(error) = self.reach(PickupBoundary::AfterStartedStaging) {
            self.stop_registered_process(
                &task,
                &claim,
                &pickup_owner,
                process_group,
                &mut child,
                &completion_tx,
            )
            .await;
            return Err(error);
        }

        if let Err(error) = self.renew_or_register_failure(&claim).await {
            self.stop_registered_process(
                &task,
                &claim,
                &pickup_owner,
                process_group,
                &mut child,
                &completion_tx,
            )
            .await;
            return Err(error);
        }
        if let Err(error) = self.reach(PickupBoundary::AfterFirstLivenessRenewal) {
            self.stop_registered_process(
                &task,
                &claim,
                &pickup_owner,
                process_group,
                &mut child,
                &completion_tx,
            )
            .await;
            return Err(error);
        }

        let cadence = self
            .liveness_timeout
            .to_std()
            .expect("positive liveness timeout")
            / 4;
        loop {
            tokio::select! {
                status = child.wait() => {
                    let status = status.map_err(|error| SafePickupError::Launch(error.to_string()))?;
                    self.task_handler
                        .launcher
                        .process_exited(&task, &claim, &status)
                        .await
                        .map_err(SafePickupError::Launch)?;
                    self.reach(PickupBoundary::AfterProcessExitObservation)?;
                    let mut handler_state = self.task_handler.state.lock().await;
                    handler_state.running.remove(&pickup_owner);
                    if cancel_token.is_cancelled() {
                        let formation_stopping = handler_state.stopping;
                        drop(handler_state);
                        if formation_stopping {
                            self.register_formation_stop(&claim).await;
                        }
                        let _ = completion_tx.send(Some(CancellationReconciliation::AlreadyExited));
                        return Ok(PickupOutcome::Cancelled {
                            claim,
                            reconciliation: CancellationReconciliation::AlreadyExited,
                        });
                    }
                    let exit_success = status.success();
                    let outcome = if exit_success {
                        LocalAttemptOutcome::ProcessExitedSuccess
                    } else {
                        LocalAttemptOutcome::ProcessExitedFailure
                    };
                    let election = self.elect_outcome(&task, &claim, outcome).await?;
                    drop(handler_state);
                    return Ok(PickupOutcome::Launched {
                        claim,
                        exit_success,
                        election,
                    });
                }
                _ = cancel_token.cancelled() => {
                    let formation_stopping = self.task_handler.state.lock().await.stopping;
                    self.stop_registered_process(
                        &task,
                        &claim,
                        &pickup_owner,
                        process_group,
                        &mut child,
                        &completion_tx,
                    ).await;
                    if formation_stopping {
                        self.register_formation_stop(&claim).await;
                    }
                    return Ok(PickupOutcome::Cancelled {
                        claim,
                        reconciliation: CancellationReconciliation::Killed,
                    });
                }
                _ = sleep(cadence) => {
                    if let Err(error) = self.renew_or_register_failure(&claim).await {
                        self.stop_registered_process(
                            &task,
                            &claim,
                            &pickup_owner,
                            process_group,
                            &mut child,
                            &completion_tx,
                        )
                        .await;
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn stop_registered_process(
        &self,
        task: &DispatchedTask,
        claim: &LocalPickupClaim,
        pickup_owner: &PickupOwnerKey,
        process_group: Option<i32>,
        child: &mut Child,
        completion: &watch::Sender<Option<CancellationReconciliation>>,
    ) {
        teardown_own_group(process_group, child, CANCEL_GRACE).await;
        if let Err(error) = self
            .task_handler
            .launcher
            .process_stopped(task, claim)
            .await
        {
            eprintln!("Task process cleanup failed: {error}");
        }
        let _ = completion.send(Some(CancellationReconciliation::Killed));
        self.task_handler
            .state
            .lock()
            .await
            .running
            .remove(pickup_owner);
    }

    /// Reconcile at most one overdue claimed generation. This path never calls
    /// the process launcher; a claimed generation is always potentially launched.
    pub async fn reconcile_one_due_liveness(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<(LocalPickupClaim, TerminalElection)>, SafePickupError> {
        let Some(due) = self
            .writer
            .select_due_liveness(now)
            .await
            .map_err(SafePickupError::Writer)?
        else {
            return Ok(None);
        };
        let task = decode_dispatch(&due.payload)
            .and_then(validate_dispatch)
            .map_err(|error| {
                SafePickupError::Writer(format!(
                    "claimed dispatch `{}` became invalid: {error}",
                    due.claim.dispatch_key
                ))
            })?;
        let event = encode_unhealthy_task_event(&task);
        let election = self
            .writer
            .elect_terminal(
                &due.claim,
                LocalAttemptOutcome::LivenessExpired,
                &event,
                now,
            )
            .await
            .map_err(SafePickupError::TerminalElection)?;
        Ok(Some((due.claim, election)))
    }

    async fn elect_outcome(
        &self,
        task: &DispatchedTask,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
    ) -> Result<TerminalElection, SafePickupError> {
        let event = match outcome {
            LocalAttemptOutcome::ProcessExitedSuccess => {
                encode_task_event(task, self.fleet_status.executor_id, EmitKind::Completed)
            }
            LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::ProcessSetupFailed => {
                encode_task_event(task, self.fleet_status.executor_id, EmitKind::Failed)
            }
            LocalAttemptOutcome::LivenessExpired => encode_unhealthy_task_event(task),
            LocalAttemptOutcome::CancellationKilled
            | LocalAttemptOutcome::CancellationAlreadyExited
            | LocalAttemptOutcome::CancellationNoProcess => {
                return Err(SafePickupError::TerminalElection(
                    "cancellation must use the cancellation settlement path".to_owned(),
                ));
            }
        };
        let election = self
            .writer
            .elect_terminal(claim, outcome, &event, Utc::now())
            .await
            .map_err(SafePickupError::TerminalElection)?;
        self.reach(PickupBoundary::AfterTerminalElection)?;
        // The outcome and terminal event share one transaction. Two named
        // checkpoints make the indivisible crash boundary explicit.
        self.reach(PickupBoundary::AfterTerminalEventStaging)?;
        Ok(election)
    }

    async fn renew_or_register_failure(
        &self,
        claim: &LocalPickupClaim,
    ) -> Result<(), SafePickupError> {
        let reason = match self.renew(claim).await {
            Ok(true) => return Ok(()),
            Ok(false) => "writer rejected liveness renewal".to_owned(),
            Err(error) => error.to_string(),
        };
        let now = Utc::now();
        let registration = self
            .writer
            .register_liveness_failure(claim, now)
            .await
            .map_err(|error| SafePickupError::LivenessRenewalFailed {
                claim: claim.clone(),
                reason: format!("{reason}; failed to register durable failure evidence: {error}"),
            })?;
        let reason = if registration {
            reason
        } else {
            format!("{reason}; claimed generation was already settled")
        };
        Err(SafePickupError::LivenessRenewalFailed {
            claim: claim.clone(),
            reason,
        })
    }

    async fn register_formation_stop(&self, claim: &LocalPickupClaim) {
        match self
            .writer
            .register_liveness_failure(claim, Utc::now())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                eprintln!(
                    "Task pickup `{}` generation {} settled before formation-stop liveness registration",
                    claim.dispatch_key, claim.pickup_generation
                );
            }
            Err(error) => {
                eprintln!(
                    "Failed to register formation-stop liveness for Task pickup `{}` generation {}: {error}",
                    claim.dispatch_key, claim.pickup_generation
                );
            }
        }
    }

    async fn renew(&self, claim: &LocalPickupClaim) -> Result<bool, SafePickupError> {
        let now = Utc::now();
        self.writer
            .renew_liveness(claim, now + self.liveness_timeout, now)
            .await
            .map_err(SafePickupError::Writer)
    }

    fn reach(&self, boundary: PickupBoundary) -> Result<(), SafePickupError> {
        self.checkpoint
            .reached(boundary)
            .map_err(|message| SafePickupError::Checkpoint { boundary, message })
    }
}

#[derive(Clone)]
pub struct SafeCancellationCoordinator<W, L> {
    writer: W,
    task_handler: LocalTaskHandler<L>,
}

impl<W, L> SafeCancellationCoordinator<W, L>
where
    W: SafeCancellationFence,
    L: TaskProcessLauncher,
{
    pub fn new(writer: W, task_handler: LocalTaskHandler<L>) -> Self {
        Self {
            writer,
            task_handler,
        }
    }

    /// Commit the fence while holding the task handler's launch gate, then notify
    /// the committed owner. This ordering closes the proof-to-spawn race.
    pub async fn cancel(&self, payload: &[u8]) -> Result<CancellationOutcome, String> {
        let request = decode_cancel_request(payload).map_err(|error| error.to_string())?;
        self.cancel_request(request).await
    }

    pub async fn cancel_request(
        &self,
        request: CancelRequest,
    ) -> Result<CancellationOutcome, String> {
        let acknowledgement_identity = cancellation_acknowledgement_identity(request);
        let mut handler_state = self.task_handler.state.lock().await;
        let fence = self
            .writer
            .commit_cancellation_fence(&acknowledgement_identity, request, Utc::now())
            .await?;

        let mut completion = None;
        let reconciliation = if let Some(outcome) = fence.terminal_outcome {
            Some(reconciliation_from_terminal(outcome))
        } else if let Some(pickup_owner) = fence.pickup_owner_key() {
            if let Some(running) = handler_state.running.get(&pickup_owner) {
                completion = Some(running.completion.clone());
                running.token.cancel();
            } else {
                handler_state.fenced.insert(pickup_owner);
            }
            if !self
                .writer
                .mark_cancellation_owner_notified(&fence, Utc::now())
                .await?
            {
                return Err("committed pickup owner could not be notified".to_owned());
            }
            if completion.is_none() {
                Some(CancellationReconciliation::NoProcess)
            } else {
                None
            }
        } else {
            Some(CancellationReconciliation::NoProcess)
        };
        drop(handler_state);

        let reconciliation = match (reconciliation, completion) {
            (Some(reconciliation), _) => reconciliation,
            (None, Some(completion)) => wait_for_cancellation_completion(completion).await?,
            (None, None) => {
                return Err("cancellation owner notification had no completion path".to_owned())
            }
        };
        self.settle(fence, reconciliation).await
    }

    /// Reconcile one unresolved fence from durable evidence. A claimed owner
    /// with no elected outcome remains fenced and uncertain; it is never relaunched
    /// and is left for liveness election.
    pub async fn reconcile_one(&self) -> Result<Option<CancellationOutcome>, String> {
        let Some(fence) = self.writer.select_unresolved_cancellation().await? else {
            return Ok(None);
        };
        let reconciliation = if let Some(outcome) = fence.terminal_outcome {
            reconciliation_from_terminal(outcome)
        } else if fence.owner.is_none() {
            CancellationReconciliation::NoProcess
        } else {
            return Ok(None);
        };
        self.settle(fence, reconciliation).await.map(Some)
    }

    async fn settle(
        &self,
        fence: LocalCancellationFence,
        reconciliation: CancellationReconciliation,
    ) -> Result<CancellationOutcome, String> {
        let acknowledgement = encode_cancel_ack(
            fence.request.task_instance_id,
            fence.request.workflow_instance_id,
            reconciliation.kill_outcome(),
        );
        let election = self
            .writer
            .settle_cancellation(&fence, reconciliation, &acknowledgement, Utc::now())
            .await?;
        Ok(CancellationOutcome {
            fence,
            reconciliation,
            election,
        })
    }
}

fn cancellation_acknowledgement_identity(request: CancelRequest) -> String {
    format!(
        "cancel-task-ack-v1:{}:{}",
        request.workflow_instance_id, request.task_instance_id
    )
}

fn reconciliation_from_terminal(outcome: LocalAttemptOutcome) -> CancellationReconciliation {
    match outcome {
        LocalAttemptOutcome::CancellationKilled => CancellationReconciliation::Killed,
        LocalAttemptOutcome::CancellationAlreadyExited => CancellationReconciliation::AlreadyExited,
        LocalAttemptOutcome::CancellationNoProcess => CancellationReconciliation::NoProcess,
        LocalAttemptOutcome::ProcessExitedSuccess
        | LocalAttemptOutcome::ProcessExitedFailure
        | LocalAttemptOutcome::ProcessSetupFailed
        | LocalAttemptOutcome::LivenessExpired => CancellationReconciliation::AlreadyExited,
    }
}

async fn wait_for_cancellation_completion(
    mut completion: watch::Receiver<Option<CancellationReconciliation>>,
) -> Result<CancellationReconciliation, String> {
    loop {
        if let Some(reconciliation) = *completion.borrow() {
            return Ok(reconciliation);
        }
        completion
            .changed()
            .await
            .map_err(|_| "task handler stopped before cancellation reconciliation".to_owned())?;
    }
}

fn validate_dispatch(task: DispatchedTask) -> anyhow::Result<DispatchedTask> {
    if task.name.trim().is_empty() {
        anyhow::bail!("TaskDispatch name is empty");
    }
    if task.nix_expression_path.trim().is_empty() {
        anyhow::bail!("TaskDispatch nix_expression_path is empty");
    }
    let contains_nul = std::iter::once(&task.name)
        .chain(std::iter::once(&task.nix_expression_path))
        .chain(task.nix_args.iter())
        .any(|value| value.contains('\0'));
    if contains_nul {
        anyhow::bail!("TaskDispatch process argument contains NUL");
    }
    Ok(task)
}
