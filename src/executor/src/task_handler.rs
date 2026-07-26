use anyhow::{Context, Result};
use async_nats::jetstream;
use async_nats::Client as NatsClient;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tickr_proto::coord::{
    TaskEventWriter, TASK_CANCEL_ACK_STREAM, TASK_CANCEL_ACK_SUBJECT, TASK_CANCEL_CONSUMER,
    TASK_CANCEL_STREAM, TASK_CANCEL_SUBJECT, TASK_DISPATCH_CONSUMER, TASK_DISPATCH_STREAM,
    TASK_DISPATCH_SUBJECT,
};
use tokio::process::{Child, Command};
use tokio::sync::{watch, Semaphore};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// Default per-executor concurrency cap: how many tasks an executor pulls and
/// runs at once. Unpulled dispatch waits durably in the work queue, so a burst
/// of dispatch can't overrun an executor.
const DEFAULT_DISPATCH_CONCURRENCY: usize = 10;
const OUTCOME_SWEEP_BATCH: usize = 16;

/// Env override for the per-executor concurrency cap. A non-numeric or
/// zero value falls back to the default — a zero cap would wedge the puller.
const DISPATCH_CONCURRENCY_ENV: &str = "TICKR_EXECUTOR_CONCURRENCY";

/// Resolve the concurrency cap from the environment, defaulting to
/// [`DEFAULT_DISPATCH_CONCURRENCY`].
pub fn dispatch_concurrency_cap() -> usize {
    std::env::var(DISPATCH_CONCURRENCY_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_DISPATCH_CONCURRENCY)
}

use crate::component_liveness::spawn_executor_fleet_reporting;
use crate::local_pickup::{
    prepare_pickup, CancellationReconciliation, ExecutorFleetStatus, LocalAttemptOutcome,
    LocalCancellationFence, LocalExecutorCapacity, LocalPickupClaim, NoopPickupCheckpoint,
    PickupPreparation, PreparedPickup, SafeAttemptOutcomeHandoff, SafeCancellationFence,
    SafeCancellationRole, SafeHandoffCoordinator, SafeLivenessWatchdog, SafePickupWriter,
    TerminalElection,
};
use crate::log_stream::{AllNatsLogStreamProvider, LogStream, LogStreamProvider};
use crate::nats_pickup::{
    cancellation_acknowledgement_identity, cancellation_owner_subject, open_pickup_bucket,
    NatsCancellationFence, NatsOutcomeElection, NatsPickupHandoff, NatsTaskEventWriter,
};
use crate::task_liveness::{ensure_liveness_bucket, LivenessConfig};
use crate::task_log_shipper::{LogIdentity, ShipperConfig, TaskExit, TaskLogShipper};
use crate::wire::{
    decode_cancel_request, encode_cancel_ack, encode_task_event, DispatchedTask, EmitKind,
    KillOutcome,
};

/// Build the `nix run <expr> [args...]` argument vector for a task.
///
/// Runtime args belong to `nix run` — the executed task — never to the
/// per-task `nix build`, which realizes the derivation by expression path
/// alone. This is the sole consumer of a dispatched task's `nix_args`.
fn build_run_command(task: &DispatchedTask) -> Vec<String> {
    let mut command_args = vec!["run".to_string()];
    command_args.push(task.nix_expression_path.clone());
    command_args.extend(task.nix_args.clone());
    command_args
}

/// Build the env-var map handed to the nix subprocess for a task.
///
/// `ns` and `nats_url` are passed in (rather than read from std::env) so the
/// function is unit-testable without leaking process-wide env into other tests
/// running in parallel. Callers resolve them at the appropriate boundary.
///
/// `originating_signal_id` is `Some` when this run was caused by a wire
/// `Signal::Trigger` (the conductor's HTTP `/trigger` ingress threads the
/// minted id through the wheel onto the queued task). The injected
/// `TICKR_TRIGGER_SIGNAL_ID` is what `tickr-ctx`'s scope resolver consults
/// when an input declaration says it comes from the trigger payload — the
/// reader path picks `<signal_id>/<name>` instead of `<run_id>/<name>`.
pub fn build_task_environment(
    task: &DispatchedTask,
    ns: &str,
    originating_signal_id: Option<Uuid>,
    gate_signal_ids: &HashMap<String, Uuid>,
    gate_signal_ids_ambient: &std::collections::HashSet<Uuid>,
) -> HashMap<String, String> {
    // These vars are read by `tickr-ctx` (and by user task code) to know which
    // workflow run and task they belong to, and what context keys/secrets were
    // declared in the DSL. See notes/secrets-handling-idea.md.
    let mut env = HashMap::new();
    env.insert("TICKR_NS".to_string(), ns.to_string());
    env.insert(
        "TICKR_RUN_ID".to_string(),
        task.workflow_instance_id.to_string(),
    );
    env.insert(
        "TICKR_TASK_ID".to_string(),
        task.task_instance_id.to_string(),
    );
    env.insert("TICKR_TASK_NAME".to_string(), task.name.clone());
    env.insert(
        "TICKR_WORKFLOW_ID".to_string(),
        task.workflow_id.to_string(),
    );
    env.insert("TICKR_OUTPUTS".to_string(), task.outputs.join(","));
    env.insert("TICKR_INPUTS".to_string(), task.inputs.join(","));
    env.insert("TICKR_SECRETS".to_string(), task.secrets.join(","));
    for secret_name in &task.secrets {
        // Identity pass-through: the env value is the same logical name. Tasks
        // use this to drive their own secret-store lookups without hardcoding
        // names. tickr never reads secret values.
        env.insert(
            format!("TICKR_SECRET_KEY_{}", secret_name.to_uppercase()),
            secret_name.clone(),
        );
    }
    // DEPRECATED: kept for one release as an alias. Note that this var
    // historically carried `workflow_instance_id` despite its name; new code
    // should use TICKR_RUN_ID instead.
    env.insert(
        "TICKR_TASK_INSTANCE_ID".to_string(),
        task.workflow_instance_id.to_string(),
    );
    // Only emitted when the run was Signal::Trigger-originated. Absent for
    // cron-fired runs so the resolver's fallback path stays unambiguous —
    // an input declared `from.trigger` against a cron-fired run is a DSL
    // misuse caught by the empty env var rather than by silent shadowing.
    if let Some(signal_id) = originating_signal_id {
        env.insert("TICKR_TRIGGER_SIGNAL_ID".to_string(), signal_id.to_string());
    }
    // Per-input gate-signal pointers — populated when the task
    // declared `from.signal = <gate>`. Tasks read these via the
    // `tickr-ctx get <input>` resolver, which picks the
    // `<gate_signal_id>/<input>` scope when the env var is set,
    // falling back to today's bare-name behaviour otherwise.
    // `TICKR_GATE_INPUTS` is a comma-separated list of the input
    // names for which a gate-signal is available — saves the
    // resolver from probing env-var names speculatively.
    if !gate_signal_ids.is_empty() {
        let mut names: Vec<&String> = gate_signal_ids.keys().collect();
        names.sort();
        env.insert(
            "TICKR_GATE_INPUTS".to_string(),
            names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(","),
        );
        for (input_name, signal_id) in gate_signal_ids {
            env.insert(
                format!("TICKR_GATE_SIGNAL_ID_{}", input_name.to_uppercase()),
                signal_id.to_string(),
            );
        }
    }
    // Ambient gate set: every Satisfied gate signal_id on edges
    // incident to this task. `tickr-ctx`'s ambient resolver walks
    // each of these alongside the trigger and run scopes; multi-
    // scope collisions error loudly. Comma-separated UUIDs.
    if !gate_signal_ids_ambient.is_empty() {
        let mut ids: Vec<String> = gate_signal_ids_ambient
            .iter()
            .map(|u| u.to_string())
            .collect();
        ids.sort();
        env.insert("TICKR_GATE_AMBIENT_SIGNAL_IDS".to_string(), ids.join(","));
    }
    env
}

#[cfg(test)]
fn build_nix_env(
    task: &DispatchedTask,
    ns: &str,
    nats_url: &str,
    originating_signal_id: Option<Uuid>,
    gate_signal_ids: &HashMap<String, Uuid>,
    gate_signal_ids_ambient: &std::collections::HashSet<Uuid>,
) -> HashMap<String, String> {
    let mut environment = build_task_environment(
        task,
        ns,
        originating_signal_id,
        gate_signal_ids,
        gate_signal_ids_ambient,
    );
    environment.insert("TICKR_NATS_URL".to_owned(), nats_url.to_owned());
    environment
}

#[async_trait::async_trait]
pub trait TaskContextProvider: Send + Sync {
    async fn register_task(&self, task: &DispatchedTask)
        -> Result<HashMap<String, String>, String>;

    async fn revoke_task(&self, task_instance_id: Uuid);
}

/// Grace period between SIGTERM and SIGKILL when tearing down an in-flight task
/// process group on executor shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Grace period between SIGTERM and SIGKILL when killing a task on an operator
/// cancel-request. Shorter than shutdown's — a cancel is a deliberate "stop
/// this now", so we escalate to SIGKILL faster.
pub(crate) const CANCEL_GRACE: Duration = Duration::from_secs(2);

/// Exact generation-and-owner key used by both launch registration and
/// owner-directed cancellation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    token: CancellationToken,
    completion: watch::Receiver<Option<CancellationReconciliation>>,
}

/// Process ownership state. Durable fences live in the all-NATS pickup bucket;
/// this map only directs a committed fence to the exact local process group.
#[derive(Default)]
struct CancelState {
    running: HashMap<PickupOwnerKey, RunningCancellation>,
    fenced: HashSet<PickupOwnerKey>,
}

impl CancelState {
    fn register_or_skip(
        &mut self,
        claim: &LocalPickupClaim,
        token: CancellationToken,
        completion: watch::Receiver<Option<CancellationReconciliation>>,
    ) -> bool {
        let key = PickupOwnerKey::from(claim);
        if self.fenced.remove(&key) {
            true
        } else {
            self.running
                .insert(key, RunningCancellation { token, completion });
            false
        }
    }

    fn finish(&mut self, claim: &LocalPickupClaim) {
        self.running.remove(&PickupOwnerKey::from(claim));
    }

    fn notify_owner(
        &mut self,
        fence: &LocalCancellationFence,
    ) -> Option<watch::Receiver<Option<CancellationReconciliation>>> {
        let key = PickupOwnerKey {
            dispatch_key: fence.dispatch_key.clone()?,
            pickup_generation: fence.pickup_generation?,
            owner: fence.owner.clone()?,
        };
        if let Some(running) = self.running.get(&key) {
            running.token.cancel();
            Some(running.completion.clone())
        } else {
            self.fenced.insert(key);
            None
        }
    }
}

/// Collapse a `child.wait()` (or `try_wait`) outcome into a `TaskExit`.
fn exit_from_wait(res: std::io::Result<std::process::ExitStatus>) -> TaskExit {
    match res {
        Ok(status) => match status.code() {
            Some(code) => TaskExit::Status(code),
            None => TaskExit::NoStatus,
        },
        Err(e) => TaskExit::Error(format!("failed to wait on task process: {e}")),
    }
}

fn cancellation_reconciliation(outcome: LocalAttemptOutcome) -> CancellationReconciliation {
    match outcome {
        LocalAttemptOutcome::CancellationKilled => CancellationReconciliation::Killed,
        LocalAttemptOutcome::CancellationNoProcess => CancellationReconciliation::NoProcess,
        LocalAttemptOutcome::CancellationAlreadyExited
        | LocalAttemptOutcome::ProcessExitedSuccess
        | LocalAttemptOutcome::ProcessExitedFailure
        | LocalAttemptOutcome::ProcessSetupFailed
        | LocalAttemptOutcome::LivenessExpired => CancellationReconciliation::AlreadyExited,
    }
}

/// Signal a recorded task process group. Refuses non-positive ids: `0` would
/// hit the executor's own group and `-1` every process on the host — only a
/// specific recorded pgid may be signalled. Returns whether `killpg` was
/// actually attempted (i.e. the id passed the guard).
#[cfg(unix)]
fn signal_group(pgid: Option<i32>, sig: nix::sys::signal::Signal) -> bool {
    let Some(pgid) = pgid else { return false };
    if pgid <= 0 {
        eprintln!("Refusing to signal non-positive process group {pgid}");
        return false;
    }
    if let Err(e) = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pgid), sig) {
        // ESRCH simply means the group already exited; log and move on.
        eprintln!("killpg({pgid}, {sig:?}) failed: {e}");
    }
    true
}

/// Tear down a task's own process group — `SIGTERM` → `grace` → `SIGKILL` — and
/// reap the leader, returning the observed exit. The handler owns its `Child`
/// and is its sole reaper, so signalling the group here and reaping below means
/// the kill always precedes the reap: there is no window in which the pgid
/// could be signalled after its pid was reaped and recycled onto an unrelated
/// process. Signalling the group (not the bare child) takes the whole
/// `nix → bash → …` tree down together.
pub(crate) async fn teardown_own_group(
    pgid: Option<i32>,
    child: &mut Child,
    grace: Duration,
) -> TaskExit {
    #[cfg(unix)]
    {
        use nix::sys::signal::Signal;
        signal_group(pgid, Signal::SIGTERM);
        sleep(grace).await;
        match child.try_wait() {
            // Exited within the grace window (the SIGTERM, or a natural exit).
            Ok(Some(status)) => return exit_from_wait(Ok(status)),
            // Still alive — escalate, then reap below.
            Ok(None) => {
                signal_group(pgid, Signal::SIGKILL);
            }
            Err(e) => return TaskExit::Error(format!("wait failed during teardown: {e}")),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (pgid, grace);
        let _ = child.kill().await;
    }
    exit_from_wait(child.wait().await)
}

/// Get-or-create the durable task-dispatch **work queue** and the shared pull
/// consumer the executors drain. The stream is a JetStream work queue (a
/// dispatched task is removed once an executor acks it), and the consumer binds
/// one durable name so multiple executor instances binding it load-balance —
/// each dispatch is handed to exactly one executor. `get_or_create` is
/// idempotent; the stream config matches the conductor's publish-side
/// `ensure_task_dispatch_stream` exactly.
pub async fn dispatch_consumer(nats: &NatsClient) -> Result<jetstream::consumer::PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: TASK_DISPATCH_STREAM.to_string(),
            subjects: vec![TASK_DISPATCH_SUBJECT.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-dispatch stream: {}", e))?;
    let consumer = stream
        .get_or_create_consumer(
            TASK_DISPATCH_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(TASK_DISPATCH_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-dispatch consumer: {}", e))?;
    Ok(consumer)
}

/// Get-or-create the durable conductor→executor cancel-request **work queue**
/// and the shared pull consumer the executors drain. Mirrors `dispatch_consumer`
/// exactly — a shared durable name so multiple executors load-balance
/// cancel-requests, `get_or_create` idempotent, config matching the conductor's
/// `ensure_task_cancel_stream`.
pub async fn cancel_consumer(nats: &NatsClient) -> Result<jetstream::consumer::PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: TASK_CANCEL_STREAM.to_string(),
            subjects: vec![TASK_CANCEL_SUBJECT.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-cancel stream: {}", e))?;
    let consumer = stream
        .get_or_create_consumer(
            TASK_CANCEL_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(TASK_CANCEL_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-cancel consumer: {}", e))?;
    Ok(consumer)
}

/// Ensure the durable executor→conductor cancel-ack work queue exists before
/// the executor publishes acks onto it. `get_or_create` is idempotent and the
/// config matches the conductor's `cancel_ack_consumer` side exactly.
pub async fn ensure_task_cancel_ack_stream(nats: &NatsClient) -> Result<()> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: TASK_CANCEL_ACK_STREAM.to_string(),
        subjects: vec![TASK_CANCEL_ACK_SUBJECT.to_string()],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_or_create task-cancel-ack stream: {}", e))?;
    Ok(())
}

/// Pull-to-capacity drain of the durable task-dispatch work queue.
///
/// A slot is acquired *before* each pull and held until that task's handler
/// finishes, so the executor pulls at most `cap` tasks concurrently — unpulled
/// dispatch genuinely waits in the substrate instead of overrunning the
/// executor. The transport message remains unacknowledged until its handler has
/// durably proved the generation-qualified pickup handoff. Each message is
/// handed to `handle`, spawned onto `tracker` so the loop keeps draining up to
/// `cap` and graceful shutdown can still wait for in-flight tasks to tear down.
pub async fn drain_dispatch_to_capacity<F, Fut>(
    consumer: jetstream::consumer::PullConsumer,
    semaphore: Arc<Semaphore>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
    handle: F,
) where
    F: Fn(jetstream::Message) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // The semaphore is now constructed at executor boot and shared with the
    // component-liveness re-arm loop, which reads `cap − available_permits` off
    // it as the in-flight gauge. Its permit count is the concurrency cap.
    loop {
        // Pull-to-capacity: block for a free slot before pulling the next task,
        // so the queue holds the remainder while we are at the cap.
        let permit = tokio::select! {
            _ = shutdown.cancelled() => break,
            p = semaphore.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break, // semaphore closed — should not happen
            },
        };

        // Pull exactly one message. A bounded expiry keeps shutdown responsive
        // and means an empty queue just re-loops (releasing the slot).
        let mut batch = match consumer
            .batch()
            .max_messages(1)
            .expires(Duration::from_secs(5))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(e) => {
                eprintln!("task-dispatch pull error: {}", e);
                drop(permit);
                continue;
            }
        };
        let msg = tokio::select! {
            _ = shutdown.cancelled() => { drop(permit); break; }
            next = batch.next() => match next {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    eprintln!("task-dispatch batch error: {}", e);
                    drop(permit);
                    continue;
                }
                None => { drop(permit); continue; } // batch expired with no message
            }
        };

        let handle = handle.clone();
        tracker.spawn(async move {
            handle(msg).await;
            // Free the slot only when the task fully finishes, so the cap bounds
            // in-flight tasks, not merely in-flight pulls.
            drop(permit);
        });
    }
}

async fn renew_generation_liveness<H>(
    handoff: &H,
    claim: &LocalPickupClaim,
    timeout: chrono::Duration,
) -> Result<bool, String>
where
    H: SafePickupWriter,
{
    let now = chrono::Utc::now();
    handoff.renew_liveness(claim, now + timeout, now).await
}

#[derive(Clone)]
pub struct TaskHandler<L = NatsPickupHandoff, C = NatsCancellationFence> {
    nats: Option<Arc<NatsClient>>,
    /// Formation-selected TaskEvents producer; substrate details stay in the adapter.
    task_events: Arc<dyn TaskEventWriter>,
    executor_id: Uuid,
    /// Formation-selected LogStaging entry point; substrate details stay in the adapter.
    log_streams: Arc<dyn LogStreamProvider>,
    /// Root-local task context broker backed by the selected ScopeStore.
    task_context: Option<Arc<dyn TaskContextProvider>>,
    /// Optional all-NATS wakeup state. A role-backed watchdog never opens it.
    liveness_kv: Option<jetstream::kv::Store>,
    /// The selected formation's liveness role, with its substrate hidden.
    liveness_watchdog: Option<L>,
    /// Durable generation-qualified handoff records for fresh all-NATS pickup.
    pickup_kv: Option<jetstream::kv::Store>,
    /// The system-internal liveness timeout / derived re-arm cadence.
    liveness_config: LivenessConfig,
    /// Bounded pre-acceptance Log settings.
    config: ShipperConfig,
    /// Tracks every in-flight task handler so graceful shutdown can wait for
    /// each to tear down its own process group before the executor exits.
    tracker: TaskTracker,
    /// Tripped on SIGTERM/SIGINT. Each handler observes it to tear down its
    /// task subprocess and to cap its log flush.
    shutdown: CancellationToken,
    /// Cancel registry + cancelled-set behind one lock, so a pickup and a
    /// cancel-request can't interleave into a lost cancel. Cloned Arc → shared
    /// across the dispatch-drain and cancel-drain loops.
    cancel_state: Arc<Mutex<CancelState>>,
    /// Formation-selected TaskCancellation owner role. `None` retains all-NATS.
    cancellation_role: Option<C>,
}

impl TaskHandler<NatsPickupHandoff, NatsCancellationFence> {
    pub fn new(nats: Arc<NatsClient>, executor_id: Uuid) -> Self {
        let task_events = Arc::new(NatsTaskEventWriter::new(nats.as_ref()));
        Self::build(nats, executor_id, None, task_events, None)
    }
    pub fn with_selected_task_dispatch(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        task_events: Arc<dyn TaskEventWriter>,
    ) -> Self {
        Self::build(nats, executor_id, None, task_events, None)
    }
}

impl<L> TaskHandler<L, NatsCancellationFence>
where
    L: SafeLivenessWatchdog + Clone,
{
    /// Construct an Executor task path with an admitted formation liveness role.
    pub fn with_liveness_watchdog(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        liveness_watchdog: L,
    ) -> Self {
        let task_events = Arc::new(NatsTaskEventWriter::new(nats.as_ref()));
        Self::build(
            nats,
            executor_id,
            Some(liveness_watchdog),
            task_events,
            None,
        )
    }

    /// Construct an Executor task path from admitted role-specific interfaces.
    pub fn with_task_events(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        liveness_watchdog: L,
        task_events: Arc<dyn TaskEventWriter>,
    ) -> Self {
        Self::build(
            nats,
            executor_id,
            Some(liveness_watchdog),
            task_events,
            None,
        )
    }
}

impl<C> TaskHandler<NatsPickupHandoff, C>
where
    C: SafeCancellationRole + Clone,
{
    pub fn with_selected_task_roles(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        task_events: Arc<dyn TaskEventWriter>,
        cancellation_role: C,
    ) -> Self {
        Self::build(
            nats,
            executor_id,
            None,
            task_events,
            Some(cancellation_role),
        )
    }

    /// Construct the all-Redis Executor without opening a NATS client.
    pub fn with_substrate_neutral_roles(
        executor_id: Uuid,
        task_events: Arc<dyn TaskEventWriter>,
        cancellation_role: C,
        log_streams: Arc<dyn LogStreamProvider>,
        task_context: Arc<dyn TaskContextProvider>,
    ) -> Self {
        Self {
            nats: None,
            task_events,
            executor_id,
            log_streams,
            task_context: Some(task_context),
            liveness_kv: None,
            liveness_watchdog: None,
            pickup_kv: None,
            liveness_config: LivenessConfig::from_env(),
            config: ShipperConfig::from_env(),
            tracker: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            cancel_state: Arc::new(Mutex::new(CancelState::default())),
            cancellation_role: Some(cancellation_role),
        }
    }
}

impl<L, C> TaskHandler<L, C>
where
    L: SafeLivenessWatchdog + Clone,
    C: SafeCancellationRole + Clone,
{
    pub fn with_roles(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        liveness_watchdog: L,
        task_events: Arc<dyn TaskEventWriter>,
        cancellation_role: C,
    ) -> Self {
        Self::build(
            nats,
            executor_id,
            Some(liveness_watchdog),
            task_events,
            Some(cancellation_role),
        )
    }

    /// Replace the default fresh all-NATS LogStaging adapter with the selected
    /// formation provider before any Task pickup begins.
    pub fn with_log_streams(mut self, log_streams: Arc<dyn LogStreamProvider>) -> Self {
        self.log_streams = log_streams;
        self
    }

    fn build(
        nats: Arc<NatsClient>,
        executor_id: Uuid,
        liveness_watchdog: Option<L>,
        task_events: Arc<dyn TaskEventWriter>,
        cancellation_role: Option<C>,
    ) -> Self {
        let config = ShipperConfig::from_env();
        let log_streams = Arc::new(AllNatsLogStreamProvider::new(
            Arc::clone(&nats),
            config.publish_timeout,
        ));
        Self {
            nats: Some(nats),
            task_events,
            executor_id,
            log_streams,
            task_context: None,
            liveness_kv: None,
            liveness_watchdog,
            pickup_kv: None,
            liveness_config: LivenessConfig::from_env(),
            config,
            tracker: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            cancel_state: Arc::new(Mutex::new(CancelState::default())),
            cancellation_role,
        }
    }

    fn nats(&self) -> &NatsClient {
        self.nats
            .as_deref()
            .expect("all-NATS TaskHandler operation requires a NATS client")
    }

    /// Prepare only the selected LogStaging provider. Redis reconstruction has
    /// already run before this component becomes eligible for Task pickup.
    pub async fn init_log_stream(&mut self) -> Result<()> {
        self.log_streams.prepare().await?;
        println!("Log staging provider ready");
        Ok(())
    }

    pub async fn poll_and_handle_tasks(
        &mut self,
        shutdown: CancellationToken,
        fleet_status: Arc<dyn ExecutorFleetStatus>,
    ) -> Result<()> {
        // Prepare only the selected TaskEvents adapter. Redis never creates the
        // all-NATS TaskEvent stream through this component boundary.
        self.task_events
            .prepare()
            .await
            .map_err(anyhow::Error::msg)?;

        // Only the all-NATS adapter opens its optional marker wakeup bucket.
        // Role-backed liveness owns its substrate and deadline index privately.
        if self.liveness_watchdog.is_none() {
            let liveness = ensure_liveness_bucket(self.nats())
                .await
                .context("open all-NATS liveness bucket")?;
            self.liveness_kv = Some(liveness);
        }
        let pickup = open_pickup_bucket(self.nats())
            .await
            .map_err(anyhow::Error::msg)?;
        self.pickup_kv = Some(pickup.clone());
        let outcome_election = self
            .liveness_watchdog
            .is_none()
            .then(|| NatsOutcomeElection::new(pickup.clone()));
        let cancellation = NatsCancellationFence::new(self.nats(), pickup);
        println!("Safe pickup and cancellation handoff stores ready");

        // Ensure the cancel-ack work queue exists before any ack is published.
        ensure_task_cancel_ack_stream(self.nats()).await?;

        // Adopt the shared shutdown token so spawned handlers observe it.
        self.shutdown = shutdown.clone();

        // The fresh all-NATS Executor competes in its deadline sweep. A
        // role-backed watchdog is swept by the Conductor through the same
        // formation-neutral contract.
        if let Some(outcome_election) = outcome_election {
            let outcome_shutdown = shutdown.clone();
            self.tracker.spawn(async move {
                let mut cadence = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = outcome_shutdown.cancelled() => break,
                        _ = cadence.tick() => {
                            for _ in 0..OUTCOME_SWEEP_BATCH {
                                match outcome_election.sweep_one_due().await {
                                    Ok(Some((_claim, TerminalElection::Won))) => {}
                                    Ok(Some((_claim, TerminalElection::Settled(_)))) => continue,
                                    Ok(None) => break,
                                    Err(error) => {
                                        eprintln!("all-NATS outcome deadline sweep failed: {error}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        // Owner delivery is an advisory wakeup over the durable cancellation
        // fence. Startup and redelivery reconstruct unresolved fences.
        let owner_handler = self.clone();
        let owner_cancellation = cancellation.clone();
        let owner_shutdown = shutdown.clone();
        self.tracker.spawn(async move {
            if let Err(error) = owner_handler
                .poll_owner_cancellations(owner_cancellation, owner_shutdown)
                .await
            {
                eprintln!("owner cancellation delivery exited with error: {error:?}");
            }
        });

        let cancel_handler = self.clone();
        let cancel_shutdown = shutdown.clone();
        self.tracker.spawn(async move {
            if let Err(error) = cancel_handler
                .poll_and_handle_cancels(cancellation, cancel_shutdown)
                .await
            {
                eprintln!("cancel-request drain exited with error: {error:?}");
            }
        });

        // Bind the durable task-dispatch work queue and drain it pull-to-capacity:
        // pull at most `cap` tasks concurrently, ack each on pickup, and let
        // unpicked dispatch wait durably in the queue rather than be dropped.
        let consumer = dispatch_consumer(self.nats()).await?;
        let cap = NonZeroUsize::new(dispatch_concurrency_cap())
            .expect("dispatch concurrency is always positive");
        println!(
            "Draining task-dispatch work queue (concurrency cap: {})",
            cap
        );

        // Hoist the local capacity handle to boot scope so fleet reporting and
        // dispatch observe the same semaphore without making the observation
        // an admission authority.
        let capacity = LocalExecutorCapacity::new(self.executor_id, cap);
        let semaphore = capacity.process_slots();
        let reporter_shutdown = shutdown.child_token();
        let component_handle =
            spawn_executor_fleet_reporting(fleet_status, capacity, reporter_shutdown.clone());

        let handler = self.clone();
        drain_dispatch_to_capacity(
            consumer,
            semaphore,
            self.tracker.clone(),
            shutdown.clone(),
            move |message| {
                let handler = handler.clone();
                async move { handler.on_pulled_message(message).await }
            },
        )
        .await;

        // Shutdown has fired: wait for the re-arm loop to observe the cancel and
        // stop, so re-arming has definitely ceased and the component key will
        // self-reap by TTL.
        reporter_shutdown.cancel();
        let _ = component_handle.await;

        println!("Shutdown signal received, stopping task handler...");
        Ok(())
    }
    /// Drain a formation-selected TaskDispatch role through the common safe
    /// handoff. Local capacity is acquired before selection and held until the
    /// Task process and its terminal evidence settle.
    pub async fn poll_and_handle_selected_dispatch<H>(
        &mut self,
        handoff: H,
        fleet_status: Arc<dyn ExecutorFleetStatus>,
        shutdown: CancellationToken,
    ) -> Result<()>
    where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        self.task_events
            .prepare()
            .await
            .map_err(anyhow::Error::msg)?;
        self.shutdown = shutdown.clone();
        let cancellation = self
            .cancellation_role
            .clone()
            .ok_or_else(|| anyhow::anyhow!("selected TaskDispatch requires TaskCancellation"))?;
        let cancellation_handler = self.clone();
        let cancellation_shutdown = shutdown.clone();
        let cancellation_loop = cancellation_handler
            .poll_selected_owner_cancellations(cancellation, cancellation_shutdown);
        tokio::pin!(cancellation_loop);

        let cap = NonZeroUsize::new(dispatch_concurrency_cap())
            .expect("dispatch concurrency is always positive");
        let capacity = LocalExecutorCapacity::new(self.executor_id, cap);
        println!(
            "Draining formation-selected TaskDispatch (concurrency cap: {})",
            cap
        );
        let reporter_shutdown = shutdown.child_token();
        let component_handle = spawn_executor_fleet_reporting(
            fleet_status,
            capacity.clone(),
            reporter_shutdown.clone(),
        );

        loop {
            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = &mut cancellation_loop => break,
                permit = capacity.acquire_process_slot() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };
            let handler = self.clone();
            let handoff = handoff.clone();
            self.tracker.spawn(async move {
                handler.prepare_and_run_handoff(handoff).await;
                drop(permit);
            });
        }

        reporter_shutdown.cancel();
        let _ = component_handle.await;

        println!("Shutdown signal received, stopping task handler...");
        Ok(())
    }

    /// Run the shared safe-pickup choreography for one substrate delivery.
    /// Transport state stays inside TaskDispatch; liveness receives only the
    /// generation-qualified claim and encoded event bytes.
    async fn on_pulled_message(&self, message: jetstream::Message) {
        let Some(pickup) = self.pickup_kv.clone() else {
            eprintln!("safe pickup store unavailable; leaving TaskDispatch pending");
            return;
        };
        if self.liveness_watchdog.is_none() && self.liveness_kv.is_none() {
            eprintln!("all-NATS liveness store unavailable; leaving TaskDispatch pending");
            return;
        }
        let handoff = match NatsPickupHandoff::from_message_with_task_events(
            self.nats(),
            pickup,
            self.liveness_kv.clone(),
            message,
            Arc::clone(&self.task_events),
        )
        .await
        {
            Ok(handoff) => handoff,
            Err(error) => {
                eprintln!("read TaskDispatch delivery identity: {error}");
                return;
            }
        };
        if let Some(liveness) = self.liveness_watchdog.clone() {
            self.prepare_and_run_handoff(SafeHandoffCoordinator::new(handoff, liveness))
                .await;
        } else {
            self.prepare_and_run_handoff(handoff).await;
        }
    }

    async fn prepare_and_run_handoff<H>(&self, handoff: H)
    where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        let timeout = chrono::Duration::from_std(self.liveness_config.timeout)
            .expect("liveness timeout must fit chrono::Duration");
        let owner = self.executor_id.to_string();
        let prepared = match prepare_pickup(
            &handoff,
            &NoopPickupCheckpoint,
            &owner,
            self.executor_id,
            timeout,
        )
        .await
        {
            Ok(PickupPreparation::Ready(prepared)) => prepared,
            Ok(PickupPreparation::NoWork) => return,
            Ok(PickupPreparation::PoisonRejected { dispatch_key }) => {
                eprintln!("durably rejected poison TaskDispatch `{dispatch_key}`");
                return;
            }
            Ok(PickupPreparation::ClaimUnavailable { dispatch_key }) => {
                eprintln!("pickup claim unavailable for TaskDispatch `{dispatch_key}`");
                return;
            }
            Err(error) => {
                eprintln!("safe Task pickup failed before launch: {error}");
                return;
            }
        };
        self.on_prepared_task(prepared, handoff).await;
    }

    async fn on_prepared_task<H>(&self, prepared: PreparedPickup, handoff: H)
    where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        let ti_id = prepared.task.task_instance_id;
        let claim = prepared.claim.clone();
        println!(
            "Proved Task pickup: instance={ti_id} generation={}",
            claim.pickup_generation
        );
        let token = CancellationToken::new();
        let (completion_tx, completion_rx) = watch::channel(None);
        if self
            .cancel_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .register_or_skip(&claim, token.clone(), completion_rx)
        {
            let _ = completion_tx.send(Some(CancellationReconciliation::NoProcess));
            println!("Skipping proved Task pickup {ti_id} — fenced before spawn");
            return;
        }

        self.run_pulled_task(prepared, handoff, token, completion_tx)
            .await;
        self.cancel_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .finish(&claim);
    }

    async fn run_pulled_task<H>(
        &self,
        prepared: PreparedPickup,
        handoff: H,
        token: CancellationToken,
        completion: watch::Sender<Option<CancellationReconciliation>>,
    ) where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        if let Err(error) = self.handle_task(prepared, handoff, token, completion).await {
            eprintln!("Error handling Task pickup: {error:?}");
        }
    }

    /// Commit the stable fence before owner delivery. The source delivery stays
    /// pending until the same acknowledgement bytes are durable and enqueued.
    async fn poll_and_handle_cancels(
        &self,
        cancellation: NatsCancellationFence,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let consumer = cancel_consumer(self.nats()).await?;
        let mut messages = consumer
            .batch()
            .max_messages(1)
            .expires(Duration::from_secs(5))
            .messages()
            .await
            .map_err(|error| anyhow::anyhow!("open cancel consumer stream: {error}"))?;
        loop {
            let msg = tokio::select! {
                _ = shutdown.cancelled() => break,
                next = messages.next() => match next {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        eprintln!("cancel-request pull error: {error}");
                        messages = match consumer.batch().max_messages(1)
                            .expires(Duration::from_secs(5)).messages().await {
                            Ok(messages) => messages,
                            Err(error) => {
                                eprintln!("reopen cancel stream: {error}");
                                break;
                            }
                        };
                        continue;
                    }
                    None => {
                        messages = match consumer.batch().max_messages(1)
                            .expires(Duration::from_secs(5)).messages().await {
                            Ok(messages) => messages,
                            Err(error) => {
                                eprintln!("reopen cancel stream: {error}");
                                break;
                            }
                        };
                        continue;
                    }
                },
            };
            let request = match decode_cancel_request(&msg.payload) {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("invalid cancellation remains unacknowledged: {error}");
                    continue;
                }
            };
            let identity = cancellation_acknowledgement_identity(request);
            let fence = match cancellation
                .commit_cancellation_fence(&identity, request, chrono::Utc::now())
                .await
            {
                Ok(fence) => fence,
                Err(error) => {
                    eprintln!("commit cancellation fence failed: {error}");
                    continue;
                }
            };

            if let Some(outcome) = fence.terminal_outcome {
                self.settle_and_stage_cancellation(
                    &cancellation,
                    &fence,
                    cancellation_reconciliation(outcome),
                )
                .await
                .map_err(anyhow::Error::msg)?;
            } else if fence.owner.is_none() {
                self.settle_and_stage_cancellation(
                    &cancellation,
                    &fence,
                    CancellationReconciliation::NoProcess,
                )
                .await
                .map_err(anyhow::Error::msg)?;
            } else {
                self.notify_cancellation_owner(&cancellation, &fence)
                    .await
                    .map_err(anyhow::Error::msg)?;
            }

            if self
                .await_cancellation_acknowledgement(&cancellation, &fence, &shutdown)
                .await
                .map_err(anyhow::Error::msg)?
            {
                msg.ack()
                    .await
                    .map_err(|error| anyhow::anyhow!("complete cancellation source: {error}"))?;
            }
        }
        Ok(())
    }

    async fn poll_selected_owner_cancellations(
        &self,
        cancellation: C,
        shutdown: CancellationToken,
    ) {
        let owner = self.executor_id.to_string();
        loop {
            let selected = tokio::select! {
                _ = shutdown.cancelled() => break,
                selected = cancellation.select_owner_cancellation(&owner) => selected,
            };
            let fence = match selected {
                Ok(Some(fence)) => fence,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => {
                    eprintln!("selected TaskCancellation owner scan failed: {error}");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            };
            if fence.owner.as_deref() != Some(owner.as_str()) {
                eprintln!("selected TaskCancellation returned a fence for another owner");
                continue;
            }
            if let Err(error) = self
                .handle_selected_owner_cancellation(&cancellation, &fence)
                .await
            {
                eprintln!("selected owner cancellation reconciliation failed: {error}");
            }
        }
    }

    async fn handle_selected_owner_cancellation(
        &self,
        cancellation: &C,
        fence: &LocalCancellationFence,
    ) -> Result<(), String> {
        let _ = cancellation
            .mark_cancellation_owner_notified(fence, chrono::Utc::now())
            .await?;
        let reconciliation = if let Some(outcome) = fence.terminal_outcome {
            cancellation_reconciliation(outcome)
        } else {
            let completion = self
                .cancel_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .notify_owner(fence);
            match completion {
                None => CancellationReconciliation::NoProcess,
                Some(mut completion) => loop {
                    if let Some(reconciliation) = *completion.borrow() {
                        break reconciliation;
                    }
                    completion.changed().await.map_err(|_| {
                        "owner process ended before cancellation evidence was recorded".to_owned()
                    })?;
                },
            }
        };
        let kill_outcome = match reconciliation {
            CancellationReconciliation::Killed => KillOutcome::Killed,
            CancellationReconciliation::AlreadyExited | CancellationReconciliation::NoProcess => {
                KillOutcome::NoSuchTask
            }
        };
        let acknowledgement = encode_cancel_ack(
            fence.request.task_instance_id,
            fence.request.workflow_instance_id,
            kill_outcome,
        );
        cancellation
            .settle_cancellation(fence, reconciliation, &acknowledgement, chrono::Utc::now())
            .await?;
        Ok(())
    }

    async fn poll_owner_cancellations(
        &self,
        cancellation: NatsCancellationFence,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let subject = cancellation_owner_subject(&self.executor_id.to_string());
        let mut notifications = self.nats().subscribe(subject).await?;
        loop {
            let Some(message) = (tokio::select! {
                _ = shutdown.cancelled() => break,
                message = notifications.next() => message,
            }) else {
                break;
            };
            let identity = match std::str::from_utf8(&message.payload) {
                Ok(identity) => identity,
                Err(error) => {
                    eprintln!("invalid owner cancellation identity: {error}");
                    continue;
                }
            };
            if let Err(error) = self
                .handle_owner_cancellation(&cancellation, identity)
                .await
            {
                eprintln!("owner cancellation reconciliation failed: {error}");
            }
        }
        Ok(())
    }

    async fn handle_owner_cancellation(
        &self,
        cancellation: &NatsCancellationFence,
        identity: &str,
    ) -> Result<(), String> {
        let Some(fence) = cancellation.load_cancellation(identity).await? else {
            return Ok(());
        };
        if fence.owner.as_deref() != Some(self.executor_id.to_string().as_str()) {
            return Ok(());
        }
        let _ = cancellation
            .mark_cancellation_owner_notified(&fence, chrono::Utc::now())
            .await?;
        let reconciliation = if let Some(outcome) = fence.terminal_outcome {
            cancellation_reconciliation(outcome)
        } else {
            let completion = self
                .cancel_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .notify_owner(&fence);
            match completion {
                None => CancellationReconciliation::NoProcess,
                Some(mut completion) => loop {
                    if let Some(reconciliation) = *completion.borrow() {
                        break reconciliation;
                    }
                    completion.changed().await.map_err(|_| {
                        "owner process ended before cancellation evidence was recorded".to_owned()
                    })?;
                },
            }
        };
        self.settle_and_stage_cancellation(cancellation, &fence, reconciliation)
            .await
            .map(|_| ())
    }

    async fn notify_cancellation_owner(
        &self,
        cancellation: &NatsCancellationFence,
        fence: &LocalCancellationFence,
    ) -> Result<(), String> {
        let owner = fence
            .owner
            .as_deref()
            .ok_or_else(|| "cancellation fence has no pickup owner".to_owned())?;
        self.nats()
            .publish(
                cancellation_owner_subject(owner),
                fence.acknowledgement_identity.clone().into(),
            )
            .await
            .map_err(|error| format!("notify cancellation owner `{owner}`: {error}"))?;
        self.nats()
            .flush()
            .await
            .map_err(|error| format!("flush cancellation owner notification: {error}"))?;
        let _ = cancellation
            .mark_cancellation_owner_notified(fence, chrono::Utc::now())
            .await?;
        Ok(())
    }

    async fn settle_and_stage_cancellation(
        &self,
        cancellation: &NatsCancellationFence,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
    ) -> Result<Vec<u8>, String> {
        let kill_outcome = match reconciliation {
            CancellationReconciliation::Killed => KillOutcome::Killed,
            CancellationReconciliation::AlreadyExited | CancellationReconciliation::NoProcess => {
                KillOutcome::NoSuchTask
            }
        };
        let acknowledgement = encode_cancel_ack(
            fence.request.task_instance_id,
            fence.request.workflow_instance_id,
            kill_outcome,
        );
        cancellation
            .settle_cancellation(fence, reconciliation, &acknowledgement, chrono::Utc::now())
            .await?;
        cancellation
            .ensure_acknowledgement_enqueued(&fence.acknowledgement_identity)
            .await
    }

    async fn await_cancellation_acknowledgement(
        &self,
        cancellation: &NatsCancellationFence,
        fence: &LocalCancellationFence,
        shutdown: &CancellationToken,
    ) -> Result<bool, String> {
        loop {
            if cancellation
                .ensure_acknowledgement_enqueued(&fence.acknowledgement_identity)
                .await
                .is_ok()
            {
                return Ok(true);
            }
            let Some(current) = cancellation
                .load_cancellation(&fence.acknowledgement_identity)
                .await?
            else {
                return Err("committed cancellation fence disappeared".to_owned());
            };
            if let Some(outcome) = current.terminal_outcome {
                self.settle_and_stage_cancellation(
                    cancellation,
                    &current,
                    cancellation_reconciliation(outcome),
                )
                .await?;
                continue;
            }
            if current.owner.is_none() {
                self.settle_and_stage_cancellation(
                    cancellation,
                    &current,
                    CancellationReconciliation::NoProcess,
                )
                .await?;
                continue;
            }
            self.notify_cancellation_owner(cancellation, &current)
                .await?;
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(false),
                _ = sleep(Duration::from_millis(250)) => {}
            }
        }
    }

    async fn handle_task<H>(
        &self,
        prepared: PreparedPickup,
        handoff: H,
        cancel_token: CancellationToken,
        completion: watch::Sender<Option<CancellationReconciliation>>,
    ) -> Result<()>
    where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        let PreparedPickup { task, claim } = prepared;
        let ns = std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_string());
        let mut process_environment = build_task_environment(
            &task,
            &ns,
            task.originating_signal_id,
            &task.gate_signal_ids,
            &task.gate_signal_ids_ambient,
        );
        if let Some(task_context) = &self.task_context {
            process_environment.extend(
                task_context
                    .register_task(&task)
                    .await
                    .map_err(anyhow::Error::msg)?,
            );
        } else {
            process_environment
                .insert("TICKR_NATS_URL".to_owned(), tickr_proto::config::nats_url());
        }

        let command_args = build_run_command(&task);
        self.run_task(
            command_args,
            &process_environment,
            &task,
            &claim,
            &handoff,
            cancel_token,
            completion,
        )
        .await;
        if let Some(task_context) = &self.task_context {
            task_context.revoke_task(task.task_instance_id).await;
        }
        Ok(())
    }

    async fn make_log_stream(&self, identity: &LogIdentity) -> Result<Box<dyn LogStream>> {
        self.log_streams
            .open(identity.route(), identity.stream_identity())
            .await
    }

    /// Spawn only after the proved handoff, then stage `Started` and perform the
    /// first generation-qualified renewal before entering the process loop.
    async fn run_task<H>(
        &self,
        args: Vec<String>,
        envs: &HashMap<String, String>,
        task: &DispatchedTask,
        claim: &LocalPickupClaim,
        handoff: &H,
        cancel_token: CancellationToken,
        completion: watch::Sender<Option<CancellationReconciliation>>,
    ) where
        H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    {
        let pickup_generation = match u64::try_from(claim.pickup_generation) {
            Ok(generation) => generation,
            Err(_) => {
                let exit = TaskExit::Error("pickup generation must be non-negative".to_owned());
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                return;
            }
        };
        let identity = LogIdentity {
            workflow_id: task.workflow_id,
            workflow_instance_id: task.workflow_instance_id,
            task_instance_id: task.task_instance_id,
            pickup_generation,
        };
        let mut log_stream = match self.make_log_stream(&identity).await {
            Ok(stream) => stream,
            Err(error) => {
                let exit = TaskExit::Error(format!("open Accepted Log stream: {error}"));
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                eprintln!("Accepted Log stream unavailable: {error}");
                return;
            }
        };
        if cancel_token.is_cancelled() {
            let _ = completion.send(Some(CancellationReconciliation::NoProcess));
            let _ = log_stream.finish_cleanly(TaskExit::NoStatus).await;
            return;
        }

        let mut command = Command::new("nix");
        command.args(&args).envs(envs);
        command.stdout(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                let exit = TaskExit::Error(format!("failed to spawn task process: {error}"));
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                let _ = log_stream.finish_cleanly(exit).await;
                eprintln!("Task spawn failed: {error}");
                return;
            }
        };
        let pgid = child.id().map(|pid| pid as i32);

        let started_event = encode_task_event(task, self.executor_id, EmitKind::Started);
        match handoff
            .stage_started(claim, &started_event, chrono::Utc::now())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                let exit = teardown_own_group(pgid, &mut child, CANCEL_GRACE).await;
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                let _ = log_stream.finish_cleanly(exit).await;
                eprintln!("Started staging rejected the proved pickup generation");
                return;
            }
            Err(error) => {
                let exit = teardown_own_group(pgid, &mut child, CANCEL_GRACE).await;
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                let _ = log_stream.finish_cleanly(exit).await;
                eprintln!("Started staging failed: {error}");
                return;
            }
        }

        let renewal_timeout = chrono::Duration::from_std(self.liveness_config.timeout)
            .expect("liveness timeout must fit chrono::Duration");
        match renew_generation_liveness(handoff, claim, renewal_timeout).await {
            Ok(true) => {}
            Ok(false) => {
                let exit = teardown_own_group(pgid, &mut child, CANCEL_GRACE).await;
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                let _ = log_stream.finish_cleanly(exit).await;
                eprintln!("first liveness renewal rejected the proved pickup generation");
                return;
            }
            Err(error) => {
                let exit = teardown_own_group(pgid, &mut child, CANCEL_GRACE).await;
                self.report_terminal(
                    task,
                    claim,
                    handoff,
                    LocalAttemptOutcome::ProcessSetupFailed,
                    &exit,
                )
                .await;
                let _ = handoff.stop_liveness(claim).await;
                let _ = log_stream.finish_cleanly(exit).await;
                eprintln!("first liveness renewal failed: {error}");
                return;
            }
        }

        let mut log_stream = Some(log_stream);
        let shipper = child.stdout.take().map(|stdout| {
            TaskLogShipper::start(
                log_stream
                    .take()
                    .expect("Accepted Log stream is moved into one shipper"),
                &self.config,
                stdout,
            )
        });
        let mut renewal = tokio::time::interval(self.liveness_config.cadence());
        renewal.tick().await;
        let (exit, cancellation) = loop {
            tokio::select! {
                result = child.wait() => {
                    let reconciliation = cancel_token
                        .is_cancelled()
                        .then_some(CancellationReconciliation::AlreadyExited);
                    break (exit_from_wait(result), reconciliation);
                }
                _ = self.shutdown.cancelled() => {
                    break (
                        teardown_own_group(pgid, &mut child, SHUTDOWN_GRACE).await,
                        None,
                    );
                }
                _ = cancel_token.cancelled() => {
                    break (
                        teardown_own_group(pgid, &mut child, CANCEL_GRACE).await,
                        Some(CancellationReconciliation::Killed),
                    );
                }
                _ = renewal.tick() => {
                    match renew_generation_liveness(handoff, claim, renewal_timeout).await {
                        Ok(true) => {}
                        Ok(false) => {
                            eprintln!("liveness renewal rejected stale or non-owner pickup generation");
                            break (
                                teardown_own_group(pgid, &mut child, CANCEL_GRACE).await,
                                None,
                            );
                        }
                        Err(error) => {
                            eprintln!("liveness renewal failed: {error}");
                            break (
                                teardown_own_group(pgid, &mut child, CANCEL_GRACE).await,
                                None,
                            );
                        }
                    }
                }
            }
        };

        if let Some(reconciliation) = cancellation {
            let reconciliation = self
                .report_cancellation_terminal(claim, handoff, reconciliation)
                .await;
            let _ = completion.send(Some(reconciliation));
        } else {
            let outcome = match exit {
                TaskExit::Status(0) => LocalAttemptOutcome::ProcessExitedSuccess,
                _ => LocalAttemptOutcome::ProcessExitedFailure,
            };
            self.report_terminal(task, claim, handoff, outcome, &exit)
                .await;
        }
        if let Err(error) = handoff.stop_liveness(claim).await {
            eprintln!("generation-qualified liveness stop failed: {error}");
        }

        match shipper {
            Some(shipper) => shipper.finish(exit, &self.shutdown).await,
            None => {
                if let Some(mut stream) = log_stream {
                    let _ = stream.finish_cleanly(exit).await;
                }
            }
        }
    }

    async fn report_cancellation_terminal<H>(
        &self,
        claim: &LocalPickupClaim,
        handoff: &H,
        reconciliation: CancellationReconciliation,
    ) -> CancellationReconciliation
    where
        H: SafeAttemptOutcomeHandoff,
    {
        let outcome = match reconciliation {
            CancellationReconciliation::Killed => LocalAttemptOutcome::CancellationKilled,
            CancellationReconciliation::AlreadyExited => {
                LocalAttemptOutcome::CancellationAlreadyExited
            }
            CancellationReconciliation::NoProcess => LocalAttemptOutcome::CancellationNoProcess,
        };
        match handoff
            .elect_terminal(claim, outcome, &[], chrono::Utc::now())
            .await
        {
            Ok(TerminalElection::Won) => reconciliation,
            Ok(TerminalElection::Settled(elected)) => cancellation_reconciliation(elected),
            Err(error) => {
                eprintln!("cancellation terminal election failed: {error}");
                reconciliation
            }
        }
    }

    async fn report_terminal<H>(
        &self,
        task: &DispatchedTask,
        claim: &crate::local_pickup::LocalPickupClaim,
        handoff: &H,
        outcome: LocalAttemptOutcome,
        exit: &TaskExit,
    ) where
        H: SafeAttemptOutcomeHandoff,
    {
        let kind = match outcome {
            LocalAttemptOutcome::ProcessExitedSuccess => EmitKind::Completed,
            LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::ProcessSetupFailed => {
                EmitKind::Failed
            }
            _ => {
                eprintln!("non-process outcome passed to process terminal reporter");
                return;
            }
        };
        let event = encode_task_event(task, self.executor_id, kind);
        match handoff
            .elect_terminal(claim, outcome, &event, chrono::Utc::now())
            .await
        {
            Ok(TerminalElection::Won) if matches!(kind, EmitKind::Completed) => {
                println!("Task completed successfully");
            }
            Ok(TerminalElection::Won) => eprintln!("Task execution failed: {exit:?}"),
            Ok(TerminalElection::Settled(elected)) => eprintln!(
                "terminal contender observed elected outcome {elected:?} for generation {}",
                claim.pickup_generation
            ),
            Err(error) => eprintln!("terminal outcome election failed: {error}"),
        }
    }

    /// Wait for every in-flight task handler to tear down its own process group
    /// before the executor exits. The shutdown token is already cancelled by
    /// the caller, so each handler signals its group (`SIGTERM` → grace →
    /// `SIGKILL`) and reaps — leaving no `nix` task tree reparented to init and
    /// no zombie. Call once, after the poll loop stops.
    pub async fn shutdown_running_tasks(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// A dispatched task decoded off the published contract, carrying the
    /// execution slice the executor runs on. Fields the executor never reads
    /// (task_type, tenant) have no member here to set.
    fn dispatched_task_with(
        outputs: Vec<&str>,
        inputs: Vec<&str>,
        secrets: Vec<&str>,
    ) -> DispatchedTask {
        let workflow_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"executor-test-workflow");
        DispatchedTask {
            task_instance_id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            workflow_instance_id: Uuid::new_v4(),
            workflow_id,
            name: "test-task".to_string(),
            nix_expression_path: "/bin/echo".to_string(),
            nix_args: vec![],
            outputs: outputs.iter().map(|s| s.to_string()).collect(),
            inputs: inputs.iter().map(|s| s.to_string()).collect(),
            secrets: secrets.iter().map(|s| s.to_string()).collect(),
            originating_signal_id: None,
            gate_signal_ids: HashMap::new(),
            gate_signal_ids_ambient: HashSet::new(),
        }
    }

    #[test]
    fn build_run_command_appends_runtime_args_to_nix_run() {
        // Runtime args reach the executed task via `nix run`, not the build.
        let mut ti = dispatched_task_with(vec![], vec![], vec![]);
        ti.nix_expression_path = "/path#expr".to_string();
        ti.nix_args = vec!["--flag".to_string(), "value".to_string()];

        let args = build_run_command(&ti);

        assert_eq!(
            args,
            vec![
                "run".to_string(),
                "/path#expr".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ],
            "runtime args must land on the nix run invocation, after the expression path"
        );
    }

    #[test]
    fn build_nix_env_populates_static_keys() {
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let env = build_nix_env(
            &ti,
            "tenant-A",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert_eq!(env.get("TICKR_NS").map(String::as_str), Some("tenant-A"));
        assert_eq!(
            env.get("TICKR_NATS_URL").map(String::as_str),
            Some("nats://localhost:4222")
        );
        assert_eq!(
            env.get("TICKR_RUN_ID").map(String::as_str),
            Some(ti.workflow_instance_id.to_string()).as_deref()
        );
        assert_eq!(
            env.get("TICKR_TASK_ID").map(String::as_str),
            Some(ti.task_instance_id.to_string()).as_deref()
        );
        assert_eq!(
            env.get("TICKR_TASK_NAME").map(String::as_str),
            Some("test-task")
        );
        assert_eq!(
            env.get("TICKR_WORKFLOW_ID").map(String::as_str),
            Some(ti.workflow_id.to_string()).as_deref()
        );
        // The deprecated alias historically carried workflow_instance_id; preserve it.
        assert_eq!(
            env.get("TICKR_TASK_INSTANCE_ID").map(String::as_str),
            Some(ti.workflow_instance_id.to_string()).as_deref()
        );
    }

    #[test]
    fn build_nix_env_joins_outputs_inputs_secrets_with_commas() {
        let ti = dispatched_task_with(
            vec!["out_one", "out_two"],
            vec!["in_one"],
            vec!["sec_a", "sec_b", "sec_c"],
        );
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert_eq!(
            env.get("TICKR_OUTPUTS").map(String::as_str),
            Some("out_one,out_two")
        );
        assert_eq!(env.get("TICKR_INPUTS").map(String::as_str), Some("in_one"));
        assert_eq!(
            env.get("TICKR_SECRETS").map(String::as_str),
            Some("sec_a,sec_b,sec_c")
        );
    }

    #[test]
    fn build_nix_env_emits_per_secret_pass_through_keys() {
        let ti = dispatched_task_with(vec![], vec![], vec!["api_token", "db_password"]);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );
        // Identity pass-through: TICKR_SECRET_KEY_<UPPER> = original logical name.
        assert_eq!(
            env.get("TICKR_SECRET_KEY_API_TOKEN").map(String::as_str),
            Some("api_token")
        );
        assert_eq!(
            env.get("TICKR_SECRET_KEY_DB_PASSWORD").map(String::as_str),
            Some("db_password")
        );
    }

    #[test]
    fn build_nix_env_injects_trigger_signal_id_when_present() {
        // When the queued task was caused by a wire `Signal::Trigger`, the
        // conductor's HTTP `/trigger` ingress threads the minted signal id
        // through. The executor must surface it as `TICKR_TRIGGER_SIGNAL_ID`
        // so `tickr-ctx` routes `from.trigger` reads to the right namespace.
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let signal_id = Uuid::new_v4();
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            Some(signal_id),
            &HashMap::new(),
            &HashSet::new(),
        );
        assert_eq!(
            env.get("TICKR_TRIGGER_SIGNAL_ID").map(String::as_str),
            Some(signal_id.to_string()).as_deref()
        );
    }

    #[test]
    fn build_nix_env_omits_trigger_signal_id_for_cron_fired_runs() {
        // A cron-fired run carries no `originating_signal_id`. The env var
        // must be absent (not empty-string) so `tickr-ctx` can use a plain
        // `env::var` lookup to discriminate trigger-originated runs.
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(
            !env.contains_key("TICKR_TRIGGER_SIGNAL_ID"),
            "cron-fired run must not emit TICKR_TRIGGER_SIGNAL_ID"
        );
    }

    #[test]
    fn build_nix_env_injects_per_input_gate_signal_id_envvars() {
        // A task that declared `inputs = [{ name = "approver";
        // from.signal = approvalGate; }]` produces a queue item
        // whose `gate_signal_ids["approver"] = <signal_id>`. The
        // executor must surface that as
        // `TICKR_GATE_SIGNAL_ID_APPROVER=<signal_id>` so
        // `tickr-ctx get approver` resolves to the gate's scope.
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let gate_sid = Uuid::new_v4();
        let mut gate_signal_ids = HashMap::new();
        gate_signal_ids.insert("approver".to_string(), gate_sid);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &gate_signal_ids,
            &HashSet::new(),
        );
        assert_eq!(
            env.get("TICKR_GATE_SIGNAL_ID_APPROVER").map(String::as_str),
            Some(gate_sid.to_string()).as_deref(),
            "gate-signal id must be exposed via env var keyed on uppercase input name"
        );
        assert_eq!(
            env.get("TICKR_GATE_INPUTS").map(String::as_str),
            Some("approver"),
            "TICKR_GATE_INPUTS must list the gate-bearing input names"
        );
    }

    #[test]
    fn build_nix_env_emits_ambient_gate_signal_ids_when_present() {
        // Tasks with incident-edge gates carry an ambient set on
        // `TaskQueueRepoItem.gate_signal_ids_ambient`. The executor
        // surfaces this as a comma-separated env var so the
        // ambient resolver in tickr-ctx can walk every Satisfied
        // gate's scope alongside trigger + run.
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let g1 = Uuid::new_v4();
        let g2 = Uuid::new_v4();
        let mut ambient = HashSet::new();
        ambient.insert(g1);
        ambient.insert(g2);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &ambient,
        );
        let raw = env
            .get("TICKR_GATE_AMBIENT_SIGNAL_IDS")
            .expect("ambient env var must be set when the set is non-empty");
        let parsed: HashSet<String> = raw.split(',').map(String::from).collect();
        assert!(parsed.contains(&g1.to_string()));
        assert!(parsed.contains(&g2.to_string()));
    }

    #[test]
    fn build_nix_env_omits_gate_envvars_when_no_signal_inputs() {
        // Tasks without `from.signal` inputs must not emit any
        // gate-shaped envvars so the resolver's fallback path
        // stays unambiguous.
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(
            !env.contains_key("TICKR_GATE_INPUTS"),
            "TICKR_GATE_INPUTS must be absent when no gate inputs"
        );
        assert!(
            env.keys().all(|k| !k.starts_with("TICKR_GATE_SIGNAL_ID_")),
            "no TICKR_GATE_SIGNAL_ID_* keys when no gate inputs"
        );
    }

    #[test]
    fn build_nix_env_with_no_outputs_inputs_secrets_emits_empty_strings() {
        // The join-by-comma convention produces empty strings when the source
        // Vec is empty. tickr-ctx must treat empty as "no declared X".
        let ti = dispatched_task_with(vec![], vec![], vec![]);
        let env = build_nix_env(
            &ti,
            "default",
            "nats://localhost:4222",
            None,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert_eq!(env.get("TICKR_OUTPUTS").map(String::as_str), Some(""));
        assert_eq!(env.get("TICKR_INPUTS").map(String::as_str), Some(""));
        assert_eq!(env.get("TICKR_SECRETS").map(String::as_str), Some(""));
        // No TICKR_SECRET_KEY_* keys when there are no secrets.
        assert!(env.keys().all(|k| !k.starts_with("TICKR_SECRET_KEY_")));
    }

    fn cancellation_claim(owner: &str) -> LocalPickupClaim {
        LocalPickupClaim {
            dispatch_key: format!("dispatch.{}", Uuid::new_v4().simple()),
            pickup_generation: 1,
            owner: owner.to_owned(),
            liveness_deadline: chrono::Utc::now() + chrono::Duration::seconds(30),
        }
    }

    fn cancellation_fence(claim: &LocalPickupClaim) -> LocalCancellationFence {
        LocalCancellationFence {
            acknowledgement_identity: format!("ack.{}", Uuid::new_v4().simple()),
            request: crate::wire::CancelRequest {
                task_instance_id: Uuid::new_v4(),
                workflow_instance_id: Uuid::new_v4(),
            },
            dispatch_key: Some(claim.dispatch_key.clone()),
            pickup_generation: Some(claim.pickup_generation),
            owner: Some(claim.owner.clone()),
            owner_notified: false,
            liveness_deadline: Some(claim.liveness_deadline),
            terminal_outcome: None,
        }
    }

    #[test]
    fn cancel_registry_targets_the_exact_generation_and_owner() {
        let mut state = CancelState::default();
        let claim_a = cancellation_claim("executor-a");
        let claim_b = cancellation_claim("executor-b");
        let token_a = CancellationToken::new();
        let token_b = CancellationToken::new();
        let (_completion_a, receiver_a) = watch::channel(None);
        let (_completion_b, receiver_b) = watch::channel(None);
        assert!(!state.register_or_skip(&claim_a, token_a.clone(), receiver_a));
        assert!(!state.register_or_skip(&claim_b, token_b.clone(), receiver_b));

        assert!(state.notify_owner(&cancellation_fence(&claim_a)).is_some());
        assert!(
            token_a.is_cancelled(),
            "the exact owner generation is killed"
        );
        assert!(
            !token_b.is_cancelled(),
            "a different owner remains untouched"
        );

        let mut stale = claim_b.clone();
        stale.pickup_generation += 1;
        assert!(
            state.notify_owner(&cancellation_fence(&stale)).is_none(),
            "a stale generation cannot signal the current process"
        );
        assert!(!token_b.is_cancelled());
    }

    #[test]
    fn committed_fence_catches_a_claim_before_spawn() {
        let mut state = CancelState::default();
        let claim = cancellation_claim("executor-a");
        assert!(state.notify_owner(&cancellation_fence(&claim)).is_none());

        let token = CancellationToken::new();
        let (_completion, receiver) = watch::channel(None);
        assert!(
            state.register_or_skip(&claim, token, receiver),
            "a generation fenced before registration cannot spawn"
        );
    }

    #[test]
    fn finish_removes_only_the_exact_owner_generation() {
        let mut state = CancelState::default();
        let claim = cancellation_claim("executor-a");
        let (_completion, receiver) = watch::channel(None);
        state.register_or_skip(&claim, CancellationToken::new(), receiver);
        state.finish(&claim);
        assert!(
            state.notify_owner(&cancellation_fence(&claim)).is_none(),
            "an exited generation has no process to signal"
        );
    }

    #[cfg(unix)]
    #[test]
    fn signal_group_refuses_unsafe_and_absent_ids() {
        use nix::sys::signal::Signal;
        // None / 0 / -1 must never reach killpg: 0 hits the executor's own
        // group, -1 every process on the host. The guard returns false (no
        // signal attempted) — and this test surviving is itself proof, since
        // a regression would SIGTERM the test runner's own process group.
        assert!(!signal_group(None, Signal::SIGTERM));
        assert!(!signal_group(Some(0), Signal::SIGTERM));
        assert!(!signal_group(Some(-1), Signal::SIGTERM));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn teardown_kills_the_whole_process_group() {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};

        // `sh` leads its own group and backgrounds a `sleep`; the sleep is a
        // child *in the same group* but is NOT a child of the executor. It
        // prints the sleep's pid, then waits. Killing only `sh` would reparent
        // the sleep to init — the bug the process-group teardown closes.
        // Killing the *group* takes both down.
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 300 & echo $!; wait");
        cmd.stdout(Stdio::piped());
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn sh");
        let pgid = child.id().map(|p| p as i32);
        assert!(
            pgid.is_some(),
            "child must report a pid to record its group"
        );

        let stdout = child.stdout.take().expect("capture stdout");
        let mut lines = BufReader::new(stdout).lines();
        let line = lines
            .next_line()
            .await
            .expect("read grandchild pid")
            .expect("grandchild pid line");
        let grandchild = Pid::from_raw(line.trim().parse::<i32>().expect("parse pid"));

        // The grandchild is alive before teardown (signal 0 = existence probe).
        assert!(
            kill(grandchild, None).is_ok(),
            "backgrounded sleep should be running before teardown"
        );

        // The handler is the sole owner/reaper of its Child; teardown signals
        // the group and reaps in one place.
        let exit = teardown_own_group(pgid, &mut child, Duration::from_millis(200)).await;

        // After teardown the whole group is gone — no orphaned sleep survives.
        assert!(
            kill(grandchild, None).is_err(),
            "backgrounded sleep must be killed with its group, not orphaned"
        );
        // And we observed an exit for the reaped leader (signal-terminated).
        assert!(matches!(exit, TaskExit::NoStatus | TaskExit::Status(_)));
    }
}
