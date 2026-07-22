use anyhow::{Context, Result};
use async_nats::jetstream;
use async_nats::Client as NatsClient;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tickr_proto::coord::{
    TASK_CANCEL_ACK_STREAM, TASK_CANCEL_ACK_SUBJECT, TASK_CANCEL_CONSUMER, TASK_CANCEL_STREAM,
    TASK_CANCEL_SUBJECT, TASK_DISPATCH_CONSUMER, TASK_DISPATCH_STREAM, TASK_DISPATCH_SUBJECT,
    TASK_EVENT_STREAM, TASK_EVENT_SUBJECT,
};
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

/// Default per-executor concurrency cap: how many tasks an executor pulls and
/// runs at once. Unpulled dispatch waits durably in the work queue, so a burst
/// of dispatch can't overrun an executor.
const DEFAULT_DISPATCH_CONCURRENCY: usize = 10;

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

use crate::component_liveness::{ensure_component_liveness_bucket, spawn_component_liveness};
use crate::task_liveness::{ensure_liveness_bucket, LivenessConfig, LivenessHeartbeat};
use crate::task_log_shipper::{
    ensure_log_stream, sweep_spill_dir, DiscardSink, JetStreamSink, LogBatchSink, LogIdentity,
    ShipperConfig, TaskExit, TaskLogShipper,
};
use crate::wire::{
    decode_cancel_request, decode_dispatch, encode_cancel_ack, encode_task_event, CancelRequest,
    DispatchedTask, EmitKind, KillOutcome,
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

/// Grace period between SIGTERM and SIGKILL when tearing down an in-flight task
/// process group on executor shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Grace period between SIGTERM and SIGKILL when killing a task on an operator
/// cancel-request. Shorter than shutdown's — a cancel is a deliberate "stop
/// this now", so we escalate to SIGKILL faster.
pub(crate) const CANCEL_GRACE: Duration = Duration::from_secs(2);

/// The executor's cancel bookkeeping, shared across the dispatch-drain and
/// cancel-drain loops behind one lock so a pickup and a cancel-request can never
/// interleave into a lost cancel.
///
/// - `registry` maps a **running** task's instance id to the token that tears
///   down its process group. A cancel-request for a registered id trips the
///   token — the handler's `run_task` select sees it and `killpg`s the group.
/// - `cancelled` holds ids told to cancel **before** they were pulled. Because
///   dispatch is an acked-on-pickup work queue, a task pulled the instant before
///   the cancel is already this executor's responsibility; the pickup path
///   checks this set as it spawns and skips a task it has been told to kill.
#[derive(Default)]
struct CancelState {
    registry: HashMap<Uuid, CancellationToken>,
    cancelled: HashSet<Uuid>,
}

impl CancelState {
    /// At pickup: if the id was told to cancel before it was pulled, remove the
    /// marker and report `true` (skip spawning — the cancel wins the race).
    /// Otherwise register the task's teardown token and report `false` (run it).
    /// One method, one lock acquisition, so the pickup/cancel race can't slip a
    /// cancel between the set-check and the token-install.
    fn register_or_skip(&mut self, id: Uuid, token: CancellationToken) -> bool {
        if self.cancelled.remove(&id) {
            true
        } else {
            self.registry.insert(id, token);
            false
        }
    }

    /// Drop a finished task's teardown token so a completed id can't linger and
    /// absorb a stale cancel-request.
    fn finish(&mut self, id: Uuid) {
        self.registry.remove(&id);
    }

    /// Handle a cancel-request for `id`. A **running** task (registered) has its
    /// teardown token tripped — the handler's `run_task` select then `killpg`s
    /// the group — and we report `Killed`. An id with no running task is
    /// recorded in the cancelled-set so a task pulled the instant before this
    /// cancel is caught as it spawns, and we report `NoSuchTask`. Either outcome
    /// confirms the kill on the server: there is no surviving process.
    fn handle_cancel(&mut self, id: Uuid) -> KillOutcome {
        if let Some(token) = self.registry.get(&id) {
            token.cancel();
            KillOutcome::Killed
        } else {
            self.cancelled.insert(id);
            KillOutcome::NoSuchTask
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

/// Ensure the durable task-event update stream exists before the executor
/// publishes onto it. A JetStream **work queue**: the conductor's shared
/// durable consumer drains it and acks on forward, so a relay/conductor blip
/// re-parks the executor's update in the substrate and redelivers it on
/// recovery rather than dropping it. `get_or_create` is idempotent and the
/// config matches the conductor's consumer-init side exactly.
pub async fn ensure_task_event_stream(nats: &NatsClient) -> Result<()> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: TASK_EVENT_STREAM.to_string(),
        subjects: vec![TASK_EVENT_SUBJECT.to_string()],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_or_create task-event stream: {}", e))?;
    Ok(())
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
/// executor. Each pulled message is **acked on pickup** (at-most-once
/// execution): a crashed executor's task is recovered by the existing
/// per-attempt timeout, never redelivered into a double-run. Each task is
/// handed to `handle`, spawned onto `tracker` so the loop keeps draining up to
/// `cap` and graceful shutdown can still wait for in-flight tasks to tear down.
pub async fn drain_dispatch_to_capacity<F, Fut>(
    consumer: jetstream::consumer::PullConsumer,
    semaphore: Arc<Semaphore>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
    handle: F,
) where
    F: Fn(DispatchedTask) -> Fut + Clone + Send + 'static,
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

        // Ack-on-pickup: at-most-once execution. A crashed executor's task is
        // caught by the per-attempt timeout, not by redelivery into a double-run.
        if let Err(e) = msg.ack().await {
            eprintln!("task-dispatch ack-on-pickup failed: {}", e);
        }

        // Decode the published task-dispatch contract.
        let item = match decode_dispatch(&msg.payload) {
            Ok(item) => item,
            Err(e) => {
                eprintln!("Failed to deserialize task: {}", e);
                drop(permit);
                continue;
            }
        };

        let handle = handle.clone();
        tracker.spawn(async move {
            handle(item).await;
            // Free the slot only when the task fully finishes, so the cap bounds
            // in-flight tasks, not merely in-flight pulls.
            drop(permit);
        });
    }
}

#[derive(Clone)]
pub struct TaskHandler {
    nats: Arc<NatsClient>,
    executor_id: Uuid,
    jetstream: Option<Arc<jetstream::Context>>,
    /// The liveness KV bucket handle, set at startup (`ensure_liveness_bucket`).
    /// `None` until init, or if the bucket couldn't be opened — in which case the
    /// task runs without a liveness key (degrades to no watchdog rather than
    /// back-pressuring the workload, mirroring the log sink's discard fallback).
    liveness_kv: Option<jetstream::kv::Store>,
    /// The system-internal liveness timeout / derived re-arm cadence.
    liveness_config: LivenessConfig,
    /// Buffer/batch/publish tuning for each task's log shipper.
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
}

impl TaskHandler {
    pub fn new(nats: Arc<NatsClient>, executor_id: Uuid) -> Self {
        Self {
            nats,
            executor_id,
            jetstream: None,
            liveness_kv: None,
            liveness_config: LivenessConfig::from_env(),
            config: ShipperConfig::from_env(),
            tracker: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            cancel_state: Arc::new(Mutex::new(CancelState::default())),
        }
    }

    /// Sweep stale task-log spill files a prior crash orphaned. Orphans are
    /// never read — only deleted — so the executor's local-disk use does not
    /// accrete across crashes. Call once at startup.
    pub fn sweep_orphaned_spills(&self) {
        sweep_spill_dir(&self.config.spill_dir);
    }

    /// Ensure the Log staging stream exists and hold a JetStream context for
    /// publishing batches. Idempotent — an existing stream is reused.
    pub async fn init_log_stream(&mut self) -> Result<()> {
        let js = ensure_log_stream(self.nats.as_ref()).await?;
        println!("Log staging stream ready");
        self.jetstream = Some(js);
        Ok(())
    }

    pub async fn poll_and_handle_tasks(&mut self, shutdown: CancellationToken) -> Result<()> {
        // Initialize the log stream if not already initialized
        if self.jetstream.is_none() {
            self.init_log_stream().await?;
        }

        // Ensure the durable task-event update stream exists before we publish
        // typed `TaskEvent`s onto it.
        ensure_task_event_stream(self.nats.as_ref()).await?;

        // Ensure the liveness KV bucket exists and hold its store, so each task
        // picked up arms a liveness key the conductor's marker-consumer watches.
        // A failure here degrades to running without a watchdog (logged) rather
        // than wedging the executor — the same fail-open posture as the log path.
        match ensure_liveness_bucket(self.nats.as_ref()).await {
            Ok(store) => {
                println!("Liveness KV bucket ready");
                self.liveness_kv = Some(store);
            }
            Err(e) => eprintln!("liveness bucket unavailable: {e} (running without watchdog)"),
        }

        // Ensure the cancel-ack work queue exists before any ack is published.
        ensure_task_cancel_ack_stream(self.nats.as_ref()).await?;

        // Adopt the shared shutdown token so spawned handlers observe it.
        self.shutdown = shutdown.clone();

        // Drain the cancel-request work queue concurrently with dispatch: a
        // cancel-request kills a running task (via its registry token) or is
        // recorded in the cancelled-set to catch a just-pulled task at spawn.
        let cancel_handler = self.clone();
        let cancel_shutdown = shutdown.clone();
        self.tracker.spawn(async move {
            if let Err(e) = cancel_handler
                .poll_and_handle_cancels(cancel_shutdown)
                .await
            {
                eprintln!("cancel-request drain exited with error: {:?}", e);
            }
        });

        // Bind the durable task-dispatch work queue and drain it pull-to-capacity:
        // pull at most `cap` tasks concurrently, ack each on pickup, and let
        // unpicked dispatch wait durably in the queue rather than be dropped.
        let consumer = dispatch_consumer(self.nats.as_ref()).await?;
        let cap = dispatch_concurrency_cap();
        println!(
            "Draining task-dispatch work queue (concurrency cap: {})",
            cap
        );

        // Hoist the dispatch semaphore to boot scope so it is shared between the
        // dispatch drain and the component-liveness re-arm loop below — the loop
        // reads `cap − available_permits` off this very semaphore as its in-flight
        // gauge, so there is no second counter to drift.
        let semaphore = Arc::new(Semaphore::new(cap));

        // Spawn the process-lifetime component-liveness re-arm loop so the fleet
        // can be counted and its saturation read. Fail-open: if the bucket can't
        // be ensured, the executor runs without a component key (logged) and is
        // simply not counted — the same posture as the task watchdog and log
        // shipper.
        let component_handle = match ensure_component_liveness_bucket(self.nats.as_ref()).await {
            Ok(_store) => {
                println!("Component-liveness KV bucket ready");
                Some(spawn_component_liveness(
                    Arc::clone(&self.nats),
                    self.liveness_config.clone(),
                    Arc::clone(&semaphore),
                    cap,
                    self.executor_id,
                    shutdown.clone(),
                ))
            }
            Err(e) => {
                eprintln!(
                    "component-liveness bucket unavailable: {e} (running without component key)"
                );
                None
            }
        };

        let handler = self.clone();
        drain_dispatch_to_capacity(
            consumer,
            semaphore,
            self.tracker.clone(),
            shutdown.clone(),
            move |item| {
                let handler = handler.clone();
                async move { handler.on_pulled_task(item).await }
            },
        )
        .await;

        // Shutdown has fired: wait for the re-arm loop to observe the cancel and
        // stop, so re-arming has definitely ceased and the component key will
        // self-reap by TTL.
        if let Some(handle) = component_handle {
            let _ = handle.await;
        }

        println!("Shutdown signal received, stopping task handler...");
        Ok(())
    }

    /// Handle one task pulled off the dispatch work queue: publish the
    /// `Assigned` event — the executor pulled it up, off which the server arms
    /// the per-attempt execution timeout — then run it. The originating signal
    /// id and the per-input gate-signal-id map ride along so the tickr-ctx
    /// env-var injection can route `from.trigger` reads to `<signal_id>/<name>`
    /// and `from.signal` reads to `<gate_signal_id>/<name>`.
    async fn on_pulled_task(&self, item: DispatchedTask) {
        let ti_id = item.task_instance_id;
        println!("Pulled task: {:?}", ti_id);

        // Register this task's cancel token AND check the cancelled-set under
        // one lock. Dispatch is acked-on-pickup, so a cancel-request that
        // arrived the instant before this pickup already ran — it recorded the
        // id in `cancelled` (and its consumer acked `NoSuchTask`). If we see it
        // there, skip spawning entirely: the task is stopped before it runs.
        // Otherwise we install the token, so any later cancel-request finds a
        // running task to kill. The single lock makes the two paths exclusive.
        let token = CancellationToken::new();
        if self
            .cancel_state
            .lock()
            .unwrap()
            .register_or_skip(ti_id, token.clone())
        {
            println!("Skipping pulled task {ti_id} — cancelled before pickup");
            return;
        }

        // Run the task; whatever the outcome, drop the registry entry at the
        // end so a completed task's id can't linger and absorb a stale cancel.
        self.run_pulled_task(item, token).await;
        self.cancel_state.lock().unwrap().finish(ti_id);
    }

    /// The pickup body proper — publish `Assigned`, arm liveness, and run —
    /// with the per-task cancel `token` threaded through so a cancel-request
    /// can tear the process group down mid-run.
    async fn run_pulled_task(&self, item: DispatchedTask, token: CancellationToken) {
        if let Err(e) = self.publish_task_event(&item, EmitKind::Assigned).await {
            eprintln!("Failed to publish Assigned event: {:?}", e);
            return;
        }

        // Arm the liveness key at pickup — the moment the task becomes this
        // executor's responsibility, before the process spawns. Arming lazily
        // (on the first refresh tick) would leave a sub-cadence crash window
        // invisible: an executor that died between pickup and the first beat
        // would never have armed the switch.
        let liveness = self.start_liveness(&item).await;

        if let Err(e) = self.handle_task(item, liveness, token).await {
            eprintln!("Error handling task: {:?}", e);
        }
    }

    /// Drain the durable cancel-request work queue for the lifetime of the
    /// executor. Each request either trips a running task's registry token (the
    /// handler's `run_task` select then `killpg`s the group) or, if no such
    /// task is running here, is recorded in the cancelled-set to catch a
    /// just-pulled task at spawn. Either way the executor publishes a
    /// `CancelTaskAck` — `Killed` or `NoSuchTask` — so the server flips the
    /// task's kill-confirmation to `Confirmed`. A request is acked-on-pickup:
    /// cancellation is idempotent, so a redelivered request is a harmless
    /// re-cancel; never a double-run hazard the way a dispatch redelivery is.
    async fn poll_and_handle_cancels(&self, shutdown: CancellationToken) -> Result<()> {
        let consumer = cancel_consumer(self.nats.as_ref()).await?;
        let mut messages = consumer
            .batch()
            .max_messages(1)
            .expires(Duration::from_secs(5))
            .messages()
            .await
            .map_err(|e| anyhow::anyhow!("open cancel consumer stream: {}", e))?;
        // Re-open the batch each loop; a bounded expiry keeps shutdown snappy.
        loop {
            let msg = tokio::select! {
                _ = shutdown.cancelled() => break,
                next = messages.next() => match next {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        eprintln!("cancel-request pull error: {}", e);
                        messages = match consumer.batch().max_messages(1)
                            .expires(Duration::from_secs(5)).messages().await {
                            Ok(m) => m,
                            Err(e) => { eprintln!("reopen cancel stream: {}", e); break; }
                        };
                        continue;
                    }
                    None => {
                        messages = match consumer.batch().max_messages(1)
                            .expires(Duration::from_secs(5)).messages().await {
                            Ok(m) => m,
                            Err(e) => { eprintln!("reopen cancel stream: {}", e); break; }
                        };
                        continue;
                    }
                },
            };
            // Ack-on-pickup: a cancel is idempotent, so redelivery is a harmless
            // re-cancel, never a double-run.
            let _ = msg.ack().await;
            let request = match decode_cancel_request(&msg.payload) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("dropping undeserializable cancel-request: {}", e);
                    continue;
                }
            };
            let outcome = self
                .cancel_state
                .lock()
                .unwrap()
                .handle_cancel(request.task_instance_id);
            self.publish_cancel_ack(&request, outcome).await;
        }
        Ok(())
    }

    /// Publish a `CancelTaskAck` onto the durable cancel-ack work queue, awaiting
    /// the publish ack so the conductor can drain it. Best-effort: a publish
    /// failure only leaves the server's kill-confirmation `Unconfirmed`.
    async fn publish_cancel_ack(&self, request: &CancelRequest, outcome: KillOutcome) {
        // Author the ack on the published task-coordination contract.
        let payload = encode_cancel_ack(
            request.task_instance_id,
            request.workflow_instance_id,
            outcome,
        );
        let js = jetstream::new(self.nats.as_ref().clone());
        match js.publish(TASK_CANCEL_ACK_SUBJECT, payload.into()).await {
            Ok(ack_future) => {
                if let Err(e) = ack_future.await {
                    eprintln!("await cancel-ack publish ack failed: {} (unconfirmed)", e);
                }
            }
            Err(e) => eprintln!("publish CancelTaskAck failed: {} (unconfirmed)", e),
        }
    }

    /// Arm a liveness heartbeat for a task at pickup, if the liveness bucket is
    /// available. `None` degrades to running without a watchdog (the bucket
    /// couldn't be opened at boot) — never back-pressures the task.
    async fn start_liveness(&self, task: &DispatchedTask) -> Option<LivenessHeartbeat> {
        let kv = self.liveness_kv.clone()?;
        let identity = LogIdentity {
            workflow_id: task.workflow_id,
            workflow_instance_id: task.workflow_instance_id,
            task_instance_id: task.task_instance_id,
        };
        Some(
            LivenessHeartbeat::start(identity, kv, self.nats.as_ref(), &self.liveness_config).await,
        )
    }

    async fn handle_task(
        &self,
        task: DispatchedTask,
        liveness: Option<LivenessHeartbeat>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        // Publish the Started event — the executor's process is about to run.
        self.publish_task_event(&task, EmitKind::Started).await?;

        let ns = std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_string());
        let nats_url = tickr_proto::config::nats_url();
        let nix_envs = build_nix_env(
            &task,
            &ns,
            &nats_url,
            task.originating_signal_id,
            &task.gate_signal_ids,
            &task.gate_signal_ids_ambient,
        );

        let command_args = build_run_command(&task);
        self.run_task(command_args, &nix_envs, &task, liveness, cancel_token)
            .await;
        Ok(())
    }

    /// The log batch sink for this run: the JetStream adapter when the stream is
    /// available, otherwise a discard sink so the drain still empties the
    /// stdout pipe (the workload must not back-pressure just because logs can't
    /// be shipped — matching boot's "continuing anyway, logs not stored").
    fn make_sink(&self) -> Arc<dyn LogBatchSink> {
        match &self.jetstream {
            Some(js) => Arc::new(JetStreamSink::new(js.clone(), self.config.publish_timeout)),
            None => Arc::new(DiscardSink),
        }
    }

    /// Run the task process. Completion is observed via `child.wait()`,
    /// authoritatively and *concurrently with* the log drain — so the terminal
    /// status update is sent the moment the process exits, never gated on the
    /// log path or on stdout reaching end-of-file (a descendant holding stdout
    /// open can no longer freeze completion). The log shipper then drains the
    /// remainder and publishes the End-of-stream marker last.
    async fn run_task(
        &self,
        args: Vec<String>,
        envs: &HashMap<String, String>,
        task: &DispatchedTask,
        liveness: Option<LivenessHeartbeat>,
        cancel_token: CancellationToken,
    ) {
        let identity = LogIdentity {
            workflow_id: task.workflow_id,
            workflow_instance_id: task.workflow_instance_id,
            task_instance_id: task.task_instance_id,
        };
        let sink = self.make_sink();

        let mut command = Command::new("nix");
        command.args(&args).envs(envs);
        command.stdout(Stdio::piped());
        // Launch the task as the leader of its own process group (pgid == child
        // pid) so shutdown can tear down the whole `nix → bash → …` tree by
        // signalling the group; killing only the direct child would re-orphan
        // its descendants. Unix-only — the executor only runs on Unix.
        #[cfg(unix)]
        command.process_group(0);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                // Spawn failed: no process, no logs. Still report a terminal
                // state and publish a marker so the run doesn't dangle.
                let exit = TaskExit::Error(format!("failed to spawn task process: {e}"));
                self.report_terminal(task, &exit).await;
                // Terminal teardown order holds on the spawn-failure path too:
                // terminal-update → delete-key → finish-logs.
                if let Some(liveness) = liveness {
                    liveness.stop().await;
                }
                let _ = sink.publish_marker(&identity.subject(), &exit, false).await;
                eprintln!("Task spawn failed: {e}");
                return;
            }
        };

        let pgid = child.id().map(|pid| pid as i32);
        // Start the shipper draining stdout. If stdout wasn't captured (it is
        // always piped, so this is defensive), we simply run without shipping.
        let shipper = child.stdout.take().map(|stdout| {
            TaskLogShipper::start(identity.clone(), sink.clone(), &self.config, stdout)
        });

        // Observe exit concurrently with shutdown AND an operator cancel.
        // `child.wait()` is authoritative. On either shutdown or a cancel the
        // handler tears down its *own* group and reaps — sole owner, so kill
        // always precedes reap. The two teardown paths differ only in grace: a
        // cancel escalates to SIGKILL faster (it's a deliberate stop-now).
        let exit = tokio::select! {
            res = child.wait() => exit_from_wait(res),
            _ = self.shutdown.cancelled() => {
                teardown_own_group(pgid, &mut child, SHUTDOWN_GRACE).await
            }
            _ = cancel_token.cancelled() => {
                teardown_own_group(pgid, &mut child, CANCEL_GRACE).await
            }
        };

        // On an operator cancel the server has already grounded the task
        // `Cancelled` (state is authoritative regardless of the kill), and the
        // `CancelTaskAck` is the executor's report. Publishing a terminal exit
        // event here would only be a no-op on the terminal-wins task SM — but
        // suppress it anyway so a killed task never surfaces a spurious
        // `Failed`/`Completed` exit-observation racing its `Cancelled` grounding.
        if !cancel_token.is_cancelled() {
            // Send the terminal event immediately — decoupled from log drainage.
            self.report_terminal(task, &exit).await;
        }

        // Liveness teardown slots into the terminal sequence as
        // terminal-update → delete-key → finish-logs: delete the liveness key
        // and stop the refresh after the terminal `TaskEvent` is durably sent
        // and before the log shipper finishes. The refresh stopping on terminal
        // is the one hard invariant of the watchdog.
        if let Some(liveness) = liveness {
            liveness.stop().await;
        }

        // Drain the remainder and publish the marker last (bounded by the
        // shipper's safety cap and by shutdown).
        match shipper {
            Some(shipper) => shipper.finish(exit, &self.shutdown).await,
            None => {
                let _ = sink.publish_marker(&identity.subject(), &exit, false).await;
            }
        }
    }

    /// Publish the terminal `TaskEvent` — the executor's honest *exit
    /// observation*: `Completed` for a clean exit (process exit 0), `Failed`
    /// otherwise (exit ≠ 0). It is never a lifecycle verdict — the server owns
    /// the lifecycle (e.g. a loop turn's `completed` becomes `TaskParked`). A
    /// telemetry/IO fault never reaches here as a Failed: only the task
    /// process's own exit status decides the outcome — a stdout read error no
    /// longer fails the task, deliberately, under the workload-sacred invariant.
    async fn report_terminal(&self, task: &DispatchedTask, exit: &TaskExit) {
        // The executor reports a bare completion; the conductor's enrichment
        // stamps the declared routing variables (and any self-patch presence +
        // stall TTL) onto it.
        let kind = match exit {
            TaskExit::Status(0) => EmitKind::Completed,
            _ => EmitKind::Failed,
        };
        let completed = matches!(kind, EmitKind::Completed);
        if let Err(e) = self.publish_task_event(task, kind).await {
            eprintln!("Failed to publish terminal task event: {:?}", e);
        }
        if completed {
            println!("Task completed successfully");
        } else {
            eprintln!("Task execution failed: {:?}", exit);
        }
    }

    /// Publish a typed `TaskEvent` onto the durable JetStream update stream,
    /// awaiting the publish ack so the update is durably staged before the
    /// executor proceeds — a relay or conductor blip can't drop it.
    async fn publish_task_event(&self, task: &DispatchedTask, kind: EmitKind) -> Result<()> {
        // Author the event on the published task-coordination contract, stamping
        // this executor's id onto the events it emits.
        let payload = encode_task_event(task, self.executor_id, kind);

        let js = jetstream::new(self.nats.as_ref().clone());
        js.publish(TASK_EVENT_SUBJECT, payload.into())
            .await
            .context("Failed to publish task event to JetStream")?
            .await
            .context("Failed to await JetStream publish ack for task event")?;

        println!("Published task event: kind={:?}", kind);
        Ok(())
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

    #[test]
    fn cancel_registry_kills_the_right_task_only() {
        // Two running tasks registered; a cancel-request for one trips exactly
        // that task's teardown token — the registry finds the right task and
        // leaves the other untouched.
        let mut st = CancelState::default();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let tok_a = CancellationToken::new();
        let tok_b = CancellationToken::new();
        assert!(!st.register_or_skip(a, tok_a.clone()), "a runs");
        assert!(!st.register_or_skip(b, tok_b.clone()), "b runs");

        assert_eq!(st.handle_cancel(a), KillOutcome::Killed);
        assert!(tok_a.is_cancelled(), "the targeted task's group is killed");
        assert!(!tok_b.is_cancelled(), "the other task is untouched");
    }

    #[test]
    fn cancelled_set_catches_a_just_pulled_task() {
        // A cancel-request lands before the task is pulled: no registry entry,
        // so it is recorded in the cancelled-set and acked `NoSuchTask`. When
        // the task is then pulled, `register_or_skip` reports skip — the task is
        // stopped as it spawns, never left running.
        let mut st = CancelState::default();
        let id = Uuid::new_v4();

        assert_eq!(
            st.handle_cancel(id),
            KillOutcome::NoSuchTask,
            "not running yet → no-such-task, recorded for pickup"
        );

        let token = CancellationToken::new();
        assert!(
            st.register_or_skip(id, token),
            "a just-pulled task told to cancel is skipped, not run"
        );
        // The set entry is consumed on skip — a second pickup of a fresh id runs.
        let other = Uuid::new_v4();
        assert!(!st.register_or_skip(other, CancellationToken::new()));
    }

    #[test]
    fn finish_drops_the_registry_entry() {
        // A completed task's id must not linger and absorb a stale cancel: after
        // `finish`, a cancel-request for that id falls through to `NoSuchTask`.
        let mut st = CancelState::default();
        let id = Uuid::new_v4();
        st.register_or_skip(id, CancellationToken::new());
        st.finish(id);
        assert_eq!(st.handle_cancel(id), KillOutcome::NoSuchTask);
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
