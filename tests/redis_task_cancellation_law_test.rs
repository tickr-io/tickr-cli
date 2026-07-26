#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command as StdCommand,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use prost::Message;
use redis::{ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_durability::RedisDurabilityGuard,
    redis_task_cancellation::{
        RedisTaskCancellation, RedisTaskCancellationBoundary, RedisTaskCancellationCapability,
        RedisTaskCancellationCheckpoint, RedisTaskCancellationConfig, RedisTaskCancellationError,
        RedisTaskCancellationQuotaState,
    },
    redis_task_pickup::{
        RedisTaskDispatch, RedisTaskDispatchAcceptance, RedisTaskDispatchCapability,
        RedisTaskDispatchConfig, RedisTaskDispatchError, RedisTaskDispatchQuotaState,
    },
};
use tickr_executor::{
    local_pickup::{
        prepare_pickup, CancellationReconciliation, LocalAttemptOutcome, LocalCancellationFence,
        LocalExecutorCapacity, LocalPickupClaim, LocalTaskHandler, NoopPickupCheckpoint,
        PickupOutcome, PickupPreparation, SafeAttemptOutcomeHandoff, SafeCancellationCoordinator,
        SafeCancellationFence, SafeCancellationRole, SafePickupExecutor, SafePickupWriter,
        TaskProcessLauncher, TerminalElection,
    },
    wire::{decode_dispatch, encode_task_event, CancelRequest, DispatchedTask, EmitKind},
};
use tickr_proto::{
    coord::{TaskCancellationAckConsumer, TaskCancellationPublisher},
    task as tc,
};
use tokio::{
    process::{Child, Command},
    sync::Notify,
};
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const DISPATCH_PASSWORD: &str = "redis-task-dispatch-secret";
const CANCELLATION_PASSWORD: &str = "redis-task-cancellation-secret";
const ADMIN_PASSWORD: &str = "redis-task-cancellation-admin";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct DispatchCapability {
    open: AtomicBool,
}

impl DispatchCapability {
    fn open() -> Self {
        Self {
            open: AtomicBool::new(true),
        }
    }
}

impl RedisTaskDispatchCapability for DispatchCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskDispatchError> {
        self.open
            .load(Ordering::Acquire)
            .then_some(1)
            .ok_or(RedisTaskDispatchError::Unavailable)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskDispatchError> {
        if generation == 1 && self.open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RedisTaskDispatchError::Unavailable)
        }
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisTaskDispatchQuotaState) {}
}

#[derive(Default)]
struct CancellationCapability {
    open: AtomicBool,
    fail_next_acknowledgement: AtomicBool,
}

impl CancellationCapability {
    fn open() -> Self {
        Self {
            open: AtomicBool::new(true),
            fail_next_acknowledgement: AtomicBool::new(false),
        }
    }

    fn reopen(&self) {
        self.open.store(true, Ordering::Release);
    }

    fn fail_next_acknowledgement(&self) {
        self.fail_next_acknowledgement
            .store(true, Ordering::Release);
    }
}

impl RedisTaskCancellationCapability for CancellationCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskCancellationError> {
        self.open
            .load(Ordering::Acquire)
            .then_some(1)
            .ok_or(RedisTaskCancellationError::Unavailable)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskCancellationError> {
        if self.fail_next_acknowledgement.swap(false, Ordering::AcqRel) {
            self.open.store(false, Ordering::Release);
            return Err(RedisTaskCancellationError::Unavailable);
        }
        if generation == 1 && self.open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RedisTaskCancellationError::Unavailable)
        }
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisTaskCancellationQuotaState) {}
}

struct RedisFixture {
    _directory: tempfile::TempDir,
    name: String,
    port: u16,
    trust_roots: String,
}

impl RedisFixture {
    async fn start(namespace: &str) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("redis-task-cancellation-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-task-cancellation-{}-{sequence}",
            std::process::id()
        );
        fs::write(
            path.join("redis.conf"),
            format!(
                "port 0\n\
                 tls-port 6379\n\
                 tls-cert-file /tls/server.crt\n\
                 tls-key-file /tls/server.key\n\
                 tls-ca-cert-file /tls/ca.crt\n\
                 tls-auth-clients no\n\
                 protected-mode no\n\
                 appendonly yes\n\
                 appendfsync always\n\
                 maxmemory 1000000000\n\
                 maxmemory-policy noeviction\n\
                 user default off\n\
                 user task-dispatch on >{DISPATCH_PASSWORD} ~tickr:{{{namespace}}}:task-dispatch:* -@all \
                 +eval +get +hdel +hget +hincrby +hmget +hset +set +time +waitaof \
                 +xack +xadd +xautoclaim +xdel +xgroup|create +xrange +xreadgroup \
                 +zadd +zrangebyscore +zrem\n\
                 user task-cancellation on >{CANCELLATION_PASSWORD} ~tickr:{{{namespace}}}:task-cancellation:* -@all \
                 +eval +get +hget +hincrby +hmget +hscan +hset +set +waitaof\n\
                 user task-cancellation-admin on >{ADMIN_PASSWORD} ~* &* +@all\n"
            ),
        )
        .expect("write Redis fixture configuration");
        let mount = format!("{}:/tls:ro", path.display());
        run(
            StdCommand::new("docker").args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &name,
                "--publish",
                "127.0.0.1::6379",
                "--volume",
                &mount,
                REDIS_IMAGE,
                "redis-server",
                "/tls/redis.conf",
            ]),
            "start Redis TaskCancellation fixture",
        );
        let output = StdCommand::new("docker")
            .args(["port", &name, "6379/tcp"])
            .output()
            .expect("query Redis port");
        assert!(output.status.success(), "query Redis port failed");
        let port = String::from_utf8(output.stdout)
            .expect("Docker port is UTF-8")
            .trim()
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse().ok())
            .expect("Docker returned Redis port");
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return Self {
                    _directory: directory,
                    name,
                    port,
                    trust_roots,
                };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("Redis TaskCancellation fixture did not become ready");
    }

    fn dispatch_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "task-dispatch",
            DISPATCH_PASSWORD,
            &self.trust_roots,
        )
    }

    fn cancellation_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "task-cancellation",
            CANCELLATION_PASSWORD,
            &self.trust_roots,
        )
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        let _ = StdCommand::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

fn tls_client(port: u16, user: &str, password: &str, roots: &str) -> redis::Client {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let connection = format!("rediss://{user}:{password}@localhost:{port}/")
        .parse::<ConnectionInfo>()
        .expect("Redis role connection");
    redis::Client::build_with_tls(
        connection,
        TlsCertificates {
            client_tls: None,
            root_cert: Some(roots.as_bytes().to_vec()),
        },
    )
    .expect("Redis role client")
}

fn dispatch_config(namespace: &str, consumer: &str) -> RedisTaskDispatchConfig {
    let mut config = RedisTaskDispatchConfig::new(namespace, consumer);
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(1024).unwrap();
    config.max_dispatches = NonZeroUsize::new(64).unwrap();
    config.max_active_claims = NonZeroUsize::new(64).unwrap();
    config.max_staged_events = NonZeroUsize::new(128).unwrap();
    config.soft_limit_bytes = 4 * 1024 * 1024;
    config.hard_limit_bytes = 8 * 1024 * 1024;
    config
}

async fn adapters(
    fixture: &RedisFixture,
    namespace: &str,
    consumer: &str,
    dispatch_capability: Arc<DispatchCapability>,
    cancellation_capability: Arc<CancellationCapability>,
) -> (RedisTaskDispatch, RedisTaskCancellation) {
    adapters_from_clients(
        fixture.dispatch_client(),
        fixture.cancellation_client(),
        namespace,
        consumer,
        dispatch_capability,
        cancellation_capability,
    )
    .await
}

async fn adapters_from_clients(
    dispatch_client: redis::Client,
    cancellation_client: redis::Client,
    namespace: &str,
    consumer: &str,
    dispatch_capability: Arc<DispatchCapability>,
    cancellation_capability: Arc<CancellationCapability>,
) -> (RedisTaskDispatch, RedisTaskCancellation) {
    let dispatch = RedisTaskDispatch::connect(
        dispatch_client,
        dispatch_config(namespace, consumer),
        RedisDurabilityGuard::default(),
        dispatch_capability,
    )
    .await
    .unwrap();
    let cancellation = RedisTaskCancellation::connect(
        cancellation_client,
        dispatch.clone(),
        RedisTaskCancellationConfig::new(namespace),
        RedisDurabilityGuard::default(),
        cancellation_capability,
    )
    .await
    .unwrap();
    (dispatch, cancellation)
}

fn dispatch() -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "redis-cancellation-task".to_owned(),
        nix_expression_path: "/p".to_owned(),
        ..Default::default()
    }
}

fn request(dispatch: &tc::TaskDispatch) -> CancelRequest {
    CancelRequest {
        task_instance_id: Uuid::parse_str(&dispatch.task_instance_id).unwrap(),
        workflow_instance_id: Uuid::parse_str(&dispatch.workflow_instance_id).unwrap(),
    }
}

fn acknowledgement_identity(request: CancelRequest) -> String {
    format!(
        "cancel-task-ack-v1:{}:{}",
        request.workflow_instance_id, request.task_instance_id
    )
}

async fn append(
    adapter: &RedisTaskDispatch,
    identity: &str,
    dispatch: &tc::TaskDispatch,
) -> RedisTaskDispatchAcceptance {
    adapter
        .append(identity, dispatch.encode_to_vec())
        .await
        .unwrap()
}

async fn prepare_claim(
    adapter: &RedisTaskDispatch,
    dispatch: &tc::TaskDispatch,
    owner: &str,
) -> tickr_executor::local_pickup::PreparedPickup {
    assert_eq!(
        append(adapter, &format!("dispatch:{}", Uuid::new_v4()), dispatch).await,
        RedisTaskDispatchAcceptance::Appended
    );
    match prepare_pickup(
        adapter,
        &NoopPickupCheckpoint,
        owner,
        Uuid::new_v4(),
        chrono::Duration::seconds(2),
    )
    .await
    .unwrap()
    {
        PickupPreparation::Ready(prepared) => prepared,
        other => panic!("expected prepared Redis pickup, got {other:?}"),
    }
}

fn cancellation_message(dispatch: &tc::TaskDispatch) -> Vec<u8> {
    tc::CancelTaskRequest {
        task_instance_id: dispatch.task_instance_id.clone(),
        task_id: dispatch.task_id.clone(),
        workflow_instance_id: dispatch.workflow_instance_id.clone(),
        workflow_id: dispatch.workflow_id.clone(),
    }
    .encode_to_vec()
}

fn claim_from_fence(fence: &LocalCancellationFence) -> LocalPickupClaim {
    LocalPickupClaim {
        dispatch_key: fence.dispatch_key.clone().expect("claimed cancellation"),
        pickup_generation: fence.pickup_generation.expect("claimed cancellation"),
        owner: fence.owner.clone().expect("claimed cancellation"),
        liveness_deadline: fence.liveness_deadline.expect("claimed cancellation"),
    }
}

fn terminal_task_event(dispatch: &tc::TaskDispatch, executor_id: Uuid) -> Vec<u8> {
    encode_task_event(
        &decode_dispatch(&dispatch.encode_to_vec()).unwrap(),
        executor_id,
        EmitKind::Failed,
    )
}

async fn retained_terminal_event(
    fixture: &RedisFixture,
    namespace: &str,
    dispatch_key: &str,
) -> Vec<u8> {
    let mut connection = fixture
        .dispatch_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    redis::cmd("HGET")
        .arg(format!(
            "tickr:{{{namespace}}}:task-dispatch:terminal-events"
        ))
        .arg(dispatch_key)
        .query_async::<Option<Vec<u8>>>(&mut connection)
        .await
        .unwrap()
        .expect("one retained terminal TaskEvent")
}

#[derive(Clone)]
struct BlockingLauncher {
    spawned: Arc<Notify>,
}

impl TaskProcessLauncher for BlockingLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30")
            .kill_on_drop(true)
            .process_group(0);
        let child = command.spawn().map_err(|error| error.to_string())?;
        self.spawned.notify_one();
        Ok(child)
    }
}

#[derive(Clone)]
struct UnusedLauncher;

impl TaskProcessLauncher for UnusedLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
        panic!("launcher is not used by this cancellation law")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_cancellation_laws_cover_fences_kill_restart_and_terminal_races() {
    let namespace = format!(
        "task-cancellation-law-{}",
        NEXT_REDIS.load(Ordering::Relaxed)
    );
    let fixture = RedisFixture::start(&namespace).await;
    let dispatch_capability = Arc::new(DispatchCapability::open());
    let cancellation_capability = Arc::new(CancellationCapability::open());
    let (dispatch_adapter, cancellation_adapter) = adapters(
        &fixture,
        &namespace,
        "executor-a",
        dispatch_capability.clone(),
        cancellation_capability.clone(),
    )
    .await;

    // A cancellation arriving before dispatch is a durable fence: no later
    // generation may launch for the Task identity.
    let before_dispatch = dispatch();
    let before_request = request(&before_dispatch);
    let before_handler = LocalTaskHandler::new(UnusedLauncher);
    let before_coordinator = SafeCancellationCoordinator::new(cancellation_adapter.clone());
    let before = before_coordinator
        .cancel_request(&before_handler, before_request)
        .await
        .unwrap();
    assert_eq!(before.reconciliation, CancellationReconciliation::NoProcess);
    let before_identity = before.fence.acknowledgement_identity.clone();
    let mut dispatch_connection = fixture
        .dispatch_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let stored_fence: Option<String> = redis::cmd("HGET")
        .arg(format!(
            "tickr:{{{namespace}}}:task-dispatch:cancellation-fences"
        ))
        .arg(format!(
            "{}:{}",
            before_request.workflow_instance_id, before_request.task_instance_id
        ))
        .query_async(&mut dispatch_connection)
        .await
        .unwrap();
    assert_eq!(stored_fence.as_deref(), Some(before_identity.as_str()));
    assert_eq!(
        dispatch_adapter
            .append("dispatch:after-cancel", before_dispatch.encode_to_vec())
            .await
            .unwrap_err(),
        RedisTaskDispatchError::CancellationFenced
    );
    let before_ack = cancellation_adapter
        .acknowledgement(&before_identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tc::CancelTaskAck::decode(before_ack.as_slice())
            .unwrap()
            .outcome,
        tc::KillOutcome::NoSuchTask as i32
    );

    // Same-request replay returns the retained result. Reusing its stable
    // acknowledgement identity for another Task conflicts without replacement.
    let replay = before_coordinator
        .cancel_request(&before_handler, before_request)
        .await
        .unwrap();
    assert_eq!(replay.reconciliation, before.reconciliation);
    assert_eq!(
        cancellation_adapter
            .acknowledgement(&before_identity)
            .await
            .unwrap(),
        Some(before_ack.clone())
    );
    let conflict_request = request(&dispatch());
    assert!(cancellation_adapter
        .commit_cancellation_fence(&before_identity, conflict_request, Utc::now())
        .await
        .unwrap_err()
        .contains("conflict"));
    assert_eq!(
        cancellation_adapter
            .acknowledgement(&before_identity)
            .await
            .unwrap(),
        Some(before_ack)
    );

    // A queued dispatch is atomically removed while the cancellation identity
    // is bound, so it cannot be claimed between fence proof and notification.
    let queued_dispatch = dispatch();
    assert_eq!(
        append(&dispatch_adapter, "dispatch:queued", &queued_dispatch).await,
        RedisTaskDispatchAcceptance::Appended
    );
    let queued = before_coordinator
        .cancel_request(&before_handler, request(&queued_dispatch))
        .await
        .unwrap();
    assert_eq!(queued.reconciliation, CancellationReconciliation::NoProcess);
    assert_eq!(
        dispatch_adapter
            .quota_state()
            .await
            .unwrap()
            .dispatch_entries,
        0
    );

    // A claimed generation whose process is absent still elects one shared
    // terminal result; process absence alone is never the durable completion.
    let missing_dispatch = dispatch();
    let missing_claim = prepare_claim(&dispatch_adapter, &missing_dispatch, "executor-a").await;
    assert!(dispatch_adapter
        .stage_started(&missing_claim.claim, b"started", Utc::now())
        .await
        .unwrap());
    let missing = before_coordinator
        .cancel_request(&before_handler, request(&missing_dispatch))
        .await
        .unwrap();
    assert_eq!(
        missing.reconciliation,
        CancellationReconciliation::NoProcess
    );
    assert_eq!(missing.election, Some(TerminalElection::Won));

    // A pre-existing process-exit winner remains the shared terminal winner;
    // cancellation reconstructs NoSuchTask without overwriting it.
    let exited_dispatch = dispatch();
    let exited_claim = prepare_claim(&dispatch_adapter, &exited_dispatch, "executor-a").await;
    assert_eq!(
        dispatch_adapter
            .elect_terminal(
                &exited_claim.claim,
                LocalAttemptOutcome::ProcessExitedSuccess,
                b"completed",
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Won
    );
    let exited = before_coordinator
        .cancel_request(&before_handler, request(&exited_dispatch))
        .await
        .unwrap();
    assert_eq!(
        exited.reconciliation,
        CancellationReconciliation::AlreadyExited
    );
    assert_eq!(
        exited.election,
        Some(TerminalElection::Settled(
            LocalAttemptOutcome::ProcessExitedSuccess
        ))
    );

    // The common coordinator cancels the exact registered owner and waits for
    // process-group teardown before the Redis acknowledgement is settled.
    let running_dispatch = dispatch();
    assert_eq!(
        append(&dispatch_adapter, "dispatch:running", &running_dispatch).await,
        RedisTaskDispatchAcceptance::Appended
    );
    let spawned = Arc::new(Notify::new());
    let running_owner = "executor-running";
    let running_executor_id = Uuid::new_v4();
    let executor = SafePickupExecutor::new(
        dispatch_adapter.clone(),
        BlockingLauncher {
            spawned: spawned.clone(),
        },
        LocalExecutorCapacity::new(running_executor_id, NonZeroUsize::new(1).unwrap()),
        running_owner,
        Duration::from_secs(2),
    );
    let running_task = tokio::spawn({
        let executor = executor.clone();
        async move { executor.run_one().await.unwrap() }
    });
    spawned.notified().await;
    let running_handler = executor.task_handler();
    let running_request = request(&running_dispatch);
    let running_identity = acknowledgement_identity(running_request);
    let committed_running_fence = cancellation_adapter
        .commit_cancellation_fence(&running_identity, running_request, Utc::now())
        .await
        .unwrap();
    let selected_running_fence = cancellation_adapter
        .select_owner_cancellation(running_owner)
        .await
        .unwrap()
        .expect("committed cancellation is visible to its exact owner");
    assert_eq!(
        selected_running_fence.acknowledgement_identity,
        committed_running_fence.acknowledgement_identity
    );
    assert_eq!(
        selected_running_fence.pickup_generation,
        committed_running_fence.pickup_generation
    );
    assert_eq!(selected_running_fence.owner, committed_running_fence.owner);
    let running_coordinator = SafeCancellationCoordinator::new(cancellation_adapter.clone());
    let running = running_coordinator
        .cancel_request(&running_handler, running_request)
        .await
        .unwrap();
    assert_eq!(running.fence.owner.as_deref(), Some(running_owner));
    assert!(running.fence.pickup_generation.is_some());
    assert!(running.fence.dispatch_key.is_some());
    assert!(running.fence.terminal_outcome.is_none());
    assert_eq!(running.reconciliation, CancellationReconciliation::Killed);
    assert_eq!(running.election, Some(TerminalElection::Won));
    let running_pickup = running_task.await.unwrap();
    let running_claim = match running_pickup {
        PickupOutcome::Cancelled {
            claim,
            reconciliation: CancellationReconciliation::Killed,
        } => claim,
        other => panic!("expected killed pickup, got {other:?}"),
    };
    let running_ack = cancellation_adapter
        .acknowledgement(&running.fence.acknowledgement_identity)
        .await
        .unwrap()
        .unwrap();
    let decoded_running_ack = tc::CancelTaskAck::decode(running_ack.as_slice()).unwrap();
    assert_eq!(decoded_running_ack.outcome, tc::KillOutcome::Killed as i32);
    assert_eq!(
        decoded_running_ack.task_instance_id,
        running_dispatch.task_instance_id
    );
    assert_eq!(
        decoded_running_ack.workflow_instance_id,
        running_dispatch.workflow_instance_id
    );
    let running_event = retained_terminal_event(
        &fixture,
        &namespace,
        running.fence.dispatch_key.as_deref().unwrap(),
    )
    .await;
    let decoded_running_event = tc::TaskEvent::decode(running_event.as_slice()).unwrap();
    assert_eq!(
        decoded_running_event.task_instance_id,
        running_dispatch.task_instance_id
    );
    assert_eq!(
        decoded_running_event.workflow_instance_id,
        running_dispatch.workflow_instance_id
    );
    assert_eq!(
        decoded_running_event.executor_id.as_deref(),
        Some(running_executor_id.to_string().as_str())
    );
    assert!(matches!(
        decoded_running_event.kind,
        Some(tc::task_event::Kind::Failed(_))
    ));
    assert!(!running_event
        .windows(namespace.len())
        .any(|window| window == namespace.as_bytes()));
    assert!(!running_ack
        .windows(namespace.len())
        .any(|window| window == namespace.as_bytes()));
    assert_eq!(
        dispatch_adapter
            .elect_terminal(
                &running_claim,
                LocalAttemptOutcome::ProcessExitedFailure,
                &terminal_task_event(&running_dispatch, running_executor_id),
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Settled(LocalAttemptOutcome::CancellationKilled)
    );
    assert_eq!(
        retained_terminal_event(
            &fixture,
            &namespace,
            running.fence.dispatch_key.as_deref().unwrap(),
        )
        .await,
        running_event
    );

    // Death after the fence but before acknowledgement is restart-safe. The
    // owner remains uncertain until the shared watchdog election supplies
    // durable evidence, then a fresh coordinator reconstructs the same ack.
    let restart_dispatch = dispatch();
    let restart_claim = prepare_claim(&dispatch_adapter, &restart_dispatch, "dead-owner").await;
    let restart_request = request(&restart_dispatch);
    let restart_identity = acknowledgement_identity(restart_request);
    let restart_fence = cancellation_adapter
        .commit_cancellation_fence(&restart_identity, restart_request, Utc::now())
        .await
        .unwrap();
    assert_eq!(restart_fence.owner.as_deref(), Some("dead-owner"));
    assert!(restart_fence.terminal_outcome.is_none());
    drop(restart_fence);
    drop(cancellation_adapter);

    let (_, recovered_cancellation) = adapters(
        &fixture,
        &namespace,
        "executor-recovery",
        dispatch_capability.clone(),
        cancellation_capability.clone(),
    )
    .await;
    let recovered_handler = LocalTaskHandler::new(UnusedLauncher);
    let recovered_coordinator = SafeCancellationCoordinator::new(recovered_cancellation.clone());
    assert!(recovered_coordinator
        .reconcile_one()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        dispatch_adapter
            .elect_terminal(
                &restart_claim.claim,
                LocalAttemptOutcome::LivenessExpired,
                b"unhealthy",
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Won
    );
    let recovered = recovered_coordinator
        .reconcile_one()
        .await
        .unwrap()
        .expect("restart reconciles durable terminal evidence");
    assert_eq!(
        recovered.reconciliation,
        CancellationReconciliation::AlreadyExited
    );
    assert_eq!(
        recovered.election,
        Some(TerminalElection::Settled(
            LocalAttemptOutcome::LivenessExpired
        ))
    );
    let recovered_ack = recovered_cancellation
        .acknowledgement(&restart_identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tc::CancelTaskAck::decode(recovered_ack.as_slice())
            .unwrap()
            .outcome,
        tc::KillOutcome::NoSuchTask as i32
    );

    // Generation-qualified transitions reject a stale replay. Capability loss
    // after the request mutation leaves the fence pending and unacknowledged;
    // reopening resumes that exact identity rather than creating another.
    let mut stale = recovered.fence.clone();
    stale.pickup_generation = stale.pickup_generation.map(|generation| generation + 1);
    assert!(!recovered_cancellation
        .mark_cancellation_owner_notified(&stale, Utc::now())
        .await
        .unwrap());
    let later_dispatch = dispatch();
    let later_claim = prepare_claim(&dispatch_adapter, &later_dispatch, "executor-later").await;
    assert_eq!(later_claim.claim.pickup_generation, 1);
    assert_eq!(later_claim.claim.owner, "executor-later");
    let interrupted_dispatch = dispatch();
    let interrupted_request = request(&interrupted_dispatch);
    let interrupted_identity = acknowledgement_identity(interrupted_request);
    cancellation_capability.fail_next_acknowledgement();
    assert!(recovered_coordinator
        .cancel_request(&recovered_handler, interrupted_request)
        .await
        .unwrap_err()
        .contains("unavailable"));
    assert!(recovered_cancellation
        .acknowledgement(&interrupted_identity)
        .await
        .unwrap()
        .is_none());
    cancellation_capability.reopen();
    let restored = recovered_coordinator
        .reconcile_one()
        .await
        .unwrap()
        .expect("capability restoration resumes the pending fence");
    assert_eq!(
        restored.fence.acknowledgement_identity,
        interrupted_identity
    );
    assert_eq!(
        restored.reconciliation,
        CancellationReconciliation::NoProcess
    );

    // Acknowledgement delivery is a separate durable outbox transition.
    assert!(recovered_cancellation
        .mark_acknowledgement_forwarded(&restored.fence)
        .await
        .unwrap());
    assert!(recovered_cancellation
        .complete_source(&restored.fence)
        .await
        .unwrap());
    assert!(recovered_cancellation
        .source_completed(&restored.fence.acknowledgement_identity)
        .await
        .unwrap());
    let quota = recovered_cancellation.quota_state().await.unwrap();
    assert!(quota.acknowledgement_records >= 1);

    // Exact role pressure fences before acceptance. Completing the first
    // acknowledged source releases its request/fence charge, after which the
    // previously denied identity can be admitted without weakening its fence.
    let baseline = recovered_cancellation.quota_state().await.unwrap();
    let mut pressure_config = RedisTaskCancellationConfig::new(&namespace);
    pressure_config.soft_limit_bytes = baseline.used_bytes + 1;
    pressure_config.hard_limit_bytes = baseline.used_bytes + 1_000;
    let pressure_cancellation = RedisTaskCancellation::connect(
        fixture.cancellation_client(),
        dispatch_adapter.clone(),
        pressure_config,
        RedisDurabilityGuard::default(),
        cancellation_capability.clone(),
    )
    .await
    .unwrap();
    let pressure_handler = LocalTaskHandler::new(UnusedLauncher);
    let pressure_coordinator = SafeCancellationCoordinator::new(pressure_cancellation.clone());
    let pressure_first = pressure_coordinator
        .cancel_request(&pressure_handler, request(&dispatch()))
        .await
        .unwrap();
    assert_eq!(
        pressure_cancellation.quota_state().await.unwrap().pressure,
        RedisQuotaPressure::SoftThreshold
    );
    let pressure_second_request = request(&dispatch());
    let pressure_second_identity = acknowledgement_identity(pressure_second_request);
    assert!(pressure_coordinator
        .cancel_request(&pressure_handler, pressure_second_request)
        .await
        .unwrap_err()
        .contains("capacity is fenced"));
    assert!(pressure_cancellation
        .acknowledgement(&pressure_second_identity)
        .await
        .unwrap()
        .is_none());
    assert!(pressure_cancellation
        .mark_acknowledgement_forwarded(&pressure_first.fence)
        .await
        .unwrap());
    assert!(pressure_cancellation
        .complete_source(&pressure_first.fence)
        .await
        .unwrap());
    let pressure_second = pressure_coordinator
        .cancel_request(&pressure_handler, pressure_second_request)
        .await
        .unwrap();
    assert_eq!(
        pressure_second.fence.acknowledgement_identity,
        pressure_second_identity
    );
}

#[derive(Clone)]
struct CrashCheckpoint {
    target: RedisTaskCancellationBoundary,
    phase: PathBuf,
}

impl RedisTaskCancellationCheckpoint for CrashCheckpoint {
    fn reached(&self, boundary: RedisTaskCancellationBoundary) -> Result<(), String> {
        if boundary == self.target {
            fs::write(&self.phase, format!("{boundary:?}")).map_err(|error| error.to_string())?;
            loop {
                std::thread::sleep(Duration::from_secs(60));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct MarkedProcessLauncher {
    pid_file: PathBuf,
}

impl TaskProcessLauncher for MarkedProcessLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
        Command::new("sh")
            .arg("-c")
            .arg("echo $$ > \"$TICKR_TEST_TASK_PID\"; exec sleep 60")
            .env("TICKR_TEST_TASK_PID", &self.pid_file)
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .map_err(|error| error.to_string())
    }
}

fn cancellation_boundary(name: &str) -> RedisTaskCancellationBoundary {
    match name {
        "AfterFenceCommit" => RedisTaskCancellationBoundary::AfterFenceCommit,
        "AfterOwnerNotification" => RedisTaskCancellationBoundary::AfterOwnerNotification,
        "BeforeTerminalElection" => RedisTaskCancellationBoundary::BeforeTerminalElection,
        "AfterTerminalElection" => RedisTaskCancellationBoundary::AfterTerminalElection,
        "AfterAcknowledgementStaging" => RedisTaskCancellationBoundary::AfterAcknowledgementStaging,
        other => panic!("unknown cancellation boundary {other}"),
    }
}

async fn wait_for_release(path: &Path) {
    for _ in 0..800 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("parent did not release cancellation child");
}

async fn wait_for_task_pid(path: &Path) {
    for _ in 0..800 {
        if fs::read_to_string(path)
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok())
            .is_some()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Task process did not publish its process-group id");
}

#[test]
#[ignore = "spawned by redis_task_cancellation_real_process_crash_matrix"]
fn redis_task_cancellation_process_child() {
    if std::env::var_os("TICKR_REDIS_TASK_CANCELLATION_CHILD").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let port = std::env::var("TICKR_REDIS_TASK_CANCELLATION_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let roots = std::env::var("TICKR_REDIS_TASK_CANCELLATION_ROOTS").unwrap();
        let namespace = std::env::var("TICKR_REDIS_TASK_CANCELLATION_NAMESPACE").unwrap();
        let action = std::env::var("TICKR_REDIS_TASK_CANCELLATION_ACTION").unwrap();
        let target = cancellation_boundary(
            &std::env::var("TICKR_REDIS_TASK_CANCELLATION_BOUNDARY").unwrap(),
        );
        let phase = PathBuf::from(std::env::var("TICKR_REDIS_TASK_CANCELLATION_PHASE").unwrap());
        let go = PathBuf::from(std::env::var("TICKR_REDIS_TASK_CANCELLATION_GO").unwrap());
        let pid_file = PathBuf::from(std::env::var("TICKR_REDIS_TASK_CANCELLATION_PID").unwrap());
        let forwarded =
            PathBuf::from(std::env::var("TICKR_REDIS_TASK_CANCELLATION_FORWARDED").unwrap());
        let task_dispatch = tc::TaskDispatch {
            task_instance_id: std::env::var("TICKR_REDIS_TASK_INSTANCE_ID").unwrap(),
            task_id: std::env::var("TICKR_REDIS_TASK_ID").unwrap(),
            workflow_instance_id: std::env::var("TICKR_REDIS_WORKFLOW_INSTANCE_ID").unwrap(),
            workflow_id: std::env::var("TICKR_REDIS_WORKFLOW_ID").unwrap(),
            ..Default::default()
        };
        let owner = std::env::var("TICKR_REDIS_TASK_CANCELLATION_OWNER").unwrap();
        let dispatch_capability = Arc::new(DispatchCapability::open());
        let cancellation_capability = Arc::new(CancellationCapability::open());
        let dispatch_client = tls_client(port, "task-dispatch", DISPATCH_PASSWORD, &roots);
        let cancellation_client =
            tls_client(port, "task-cancellation", CANCELLATION_PASSWORD, &roots);
        let (dispatch_adapter, cancellation_adapter) = adapters_from_clients(
            dispatch_client,
            cancellation_client,
            &namespace,
            &format!("child-{}", std::process::id()),
            dispatch_capability,
            cancellation_capability,
        )
        .await;
        let cancellation_adapter =
            cancellation_adapter.with_checkpoint(Arc::new(CrashCheckpoint {
                target,
                phase: phase.clone(),
            }));
        fs::write(&phase, "connected").unwrap();

        match action.as_str() {
            "stage" => {
                wait_for_release(&go).await;
                TaskCancellationPublisher::stage(
                    &cancellation_adapter,
                    &cancellation_message(&task_dispatch),
                )
                .await
                .unwrap();
                panic!("Conductor child did not stop at {target:?}");
            }
            "executor" => {
                let executor = SafePickupExecutor::new(
                    dispatch_adapter,
                    MarkedProcessLauncher {
                        pid_file: pid_file.clone(),
                    },
                    LocalExecutorCapacity::new(
                        Uuid::parse_str(&owner).unwrap(),
                        NonZeroUsize::new(1).unwrap(),
                    ),
                    owner,
                    Duration::from_secs(2),
                );
                let task_handler = executor.task_handler();
                let running = tokio::spawn(async move { executor.run_one().await.unwrap() });
                wait_for_task_pid(&pid_file).await;
                fs::write(&phase, "running").unwrap();
                wait_for_release(&go).await;
                let coordinator = SafeCancellationCoordinator::new(cancellation_adapter);
                let _ = coordinator
                    .cancel_request(&task_handler, request(&task_dispatch))
                    .await
                    .unwrap();
                let _ = running.await.unwrap();
                panic!("Executor child did not stop at {target:?}");
            }
            "forward" | "complete" => {
                let delivery = TaskCancellationAckConsumer::next(&cancellation_adapter)
                    .await
                    .unwrap()
                    .expect("durable cancellation acknowledgement");
                fs::write(&forwarded, delivery.payload()).unwrap();
                if action == "forward" {
                    fs::write(&phase, "forwarded").unwrap();
                    loop {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
                delivery.complete().await.unwrap();
                fs::write(&phase, "completed").unwrap();
            }
            other => panic!("unknown cancellation child action {other}"),
        }
    });
}

struct ChildPaths<'a> {
    phase: &'a Path,
    go: &'a Path,
    pid: &'a Path,
    forwarded: &'a Path,
}

fn spawn_cancellation_child(
    fixture: &RedisFixture,
    namespace: &str,
    action: &str,
    target: RedisTaskCancellationBoundary,
    task_dispatch: &tc::TaskDispatch,
    owner: &str,
    paths: ChildPaths<'_>,
) -> std::process::Child {
    let _ = fs::remove_file(paths.phase);
    let _ = fs::remove_file(paths.go);
    let _ = fs::remove_file(paths.pid);
    StdCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "redis_task_cancellation_process_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TICKR_REDIS_TASK_CANCELLATION_CHILD", "1")
        .env(
            "TICKR_REDIS_TASK_CANCELLATION_PORT",
            fixture.port.to_string(),
        )
        .env("TICKR_REDIS_TASK_CANCELLATION_ROOTS", &fixture.trust_roots)
        .env("TICKR_REDIS_TASK_CANCELLATION_NAMESPACE", namespace)
        .env("TICKR_REDIS_TASK_CANCELLATION_ACTION", action)
        .env(
            "TICKR_REDIS_TASK_CANCELLATION_BOUNDARY",
            format!("{target:?}"),
        )
        .env("TICKR_REDIS_TASK_CANCELLATION_PHASE", paths.phase)
        .env("TICKR_REDIS_TASK_CANCELLATION_GO", paths.go)
        .env("TICKR_REDIS_TASK_CANCELLATION_PID", paths.pid)
        .env("TICKR_REDIS_TASK_CANCELLATION_FORWARDED", paths.forwarded)
        .env("TICKR_REDIS_TASK_CANCELLATION_OWNER", owner)
        .env(
            "TICKR_REDIS_TASK_INSTANCE_ID",
            &task_dispatch.task_instance_id,
        )
        .env("TICKR_REDIS_TASK_ID", &task_dispatch.task_id)
        .env(
            "TICKR_REDIS_WORKFLOW_INSTANCE_ID",
            &task_dispatch.workflow_instance_id,
        )
        .env("TICKR_REDIS_WORKFLOW_ID", &task_dispatch.workflow_id)
        .spawn()
        .expect("spawn Redis cancellation process")
}

async fn await_cancellation_phase(child: &mut std::process::Child, phase: &Path, expected: &str) {
    for _ in 0..800 {
        if fs::read_to_string(phase).is_ok_and(|value| value == expected) {
            return;
        }
        if let Some(status) = child.try_wait().expect("query cancellation child status") {
            panic!("Redis cancellation child exited before {expected}: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Redis cancellation child did not reach {expected}");
}

fn crash_cancellation_child(child: &mut std::process::Child) {
    child.kill().expect("crash Redis cancellation process");
    let _ = child.wait().expect("reap Redis cancellation process");
}

fn process_group_alive(process_group: i32) -> bool {
    StdCommand::new("sh")
        .arg("-c")
        .arg(format!("kill -0 -{process_group} 2>/dev/null"))
        .output()
        .is_ok_and(|output| output.status.success())
}

async fn assert_process_group_gone(process_group: i32) {
    for _ in 0..200 {
        if !process_group_alive(process_group) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = StdCommand::new("sh")
        .arg("-c")
        .arg(format!("kill -KILL -{process_group} 2>/dev/null"))
        .status();
    panic!("Task process group {process_group} survived cancellation reconciliation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_task_cancellation_real_process_crash_matrix() {
    let namespace = format!(
        "task-cancellation-crash-{}",
        NEXT_REDIS.load(Ordering::Relaxed)
    );
    let fixture = RedisFixture::start(&namespace).await;
    let dispatch_capability = Arc::new(DispatchCapability::open());
    let cancellation_capability = Arc::new(CancellationCapability::open());
    let (dispatch_adapter, cancellation_adapter) = adapters(
        &fixture,
        &namespace,
        "parent",
        dispatch_capability.clone(),
        cancellation_capability.clone(),
    )
    .await;
    let scratch = tempfile::tempdir().unwrap();
    let phase = scratch.path().join("phase");
    let go = scratch.path().join("go");
    let pid_file = scratch.path().join("task-pid");
    let forwarded = scratch.path().join("forwarded");
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000056")
        .unwrap()
        .to_string();

    for target in [
        RedisTaskCancellationBoundary::AfterFenceCommit,
        RedisTaskCancellationBoundary::AfterOwnerNotification,
    ] {
        let task_dispatch = dispatch();
        let prepared = prepare_claim(&dispatch_adapter, &task_dispatch, &owner).await;
        let mut child = spawn_cancellation_child(
            &fixture,
            &namespace,
            "stage",
            target,
            &task_dispatch,
            &owner,
            ChildPaths {
                phase: &phase,
                go: &go,
                pid: &pid_file,
                forwarded: &forwarded,
            },
        );
        await_cancellation_phase(&mut child, &phase, "connected").await;
        fs::write(&go, "go").unwrap();
        await_cancellation_phase(&mut child, &phase, &format!("{target:?}")).await;
        crash_cancellation_child(&mut child);

        let identity = acknowledgement_identity(request(&task_dispatch));
        let fence = cancellation_adapter
            .load(&identity)
            .await
            .unwrap()
            .expect("Conductor crash retained cancellation fence");
        assert_eq!(
            fence.dispatch_key.as_deref(),
            Some(prepared.claim.dispatch_key.as_str())
        );
        assert_eq!(
            fence.pickup_generation,
            Some(prepared.claim.pickup_generation)
        );
        assert_eq!(fence.owner.as_deref(), Some(owner.as_str()));
        let outcome = SafeCancellationCoordinator::new(cancellation_adapter.clone())
            .resume_fence(&LocalTaskHandler::new(UnusedLauncher), fence)
            .await
            .unwrap();
        assert_eq!(outcome.election, Some(TerminalElection::Won));
        let event = retained_terminal_event(
            &fixture,
            &namespace,
            outcome.fence.dispatch_key.as_deref().unwrap(),
        )
        .await;
        let decoded = tc::TaskEvent::decode(event.as_slice()).unwrap();
        assert!(matches!(
            decoded.kind,
            Some(tc::task_event::Kind::Failed(_))
        ));
        assert_eq!(decoded.task_instance_id, task_dispatch.task_instance_id);
        let acknowledgement = cancellation_adapter
            .acknowledgement(&identity)
            .await
            .unwrap()
            .unwrap();
        let acknowledgement = tc::CancelTaskAck::decode(acknowledgement.as_slice()).unwrap();
        assert_eq!(acknowledgement.outcome, tc::KillOutcome::NoSuchTask as i32);
        assert!(cancellation_adapter
            .mark_acknowledgement_forwarded(&outcome.fence)
            .await
            .unwrap());
        assert!(cancellation_adapter
            .complete_source(&outcome.fence)
            .await
            .unwrap());
    }

    let mut pending_forward = None;
    for target in [
        RedisTaskCancellationBoundary::BeforeTerminalElection,
        RedisTaskCancellationBoundary::AfterTerminalElection,
        RedisTaskCancellationBoundary::AfterAcknowledgementStaging,
    ] {
        let task_dispatch = dispatch();
        assert_eq!(
            append(
                &dispatch_adapter,
                &format!("crash-dispatch:{target:?}"),
                &task_dispatch,
            )
            .await,
            RedisTaskDispatchAcceptance::Appended
        );
        let mut child = spawn_cancellation_child(
            &fixture,
            &namespace,
            "executor",
            target,
            &task_dispatch,
            &owner,
            ChildPaths {
                phase: &phase,
                go: &go,
                pid: &pid_file,
                forwarded: &forwarded,
            },
        );
        await_cancellation_phase(&mut child, &phase, "running").await;
        let process_group = fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert!(process_group_alive(process_group));
        fs::write(&go, "go").unwrap();
        await_cancellation_phase(&mut child, &phase, &format!("{target:?}")).await;
        assert_process_group_gone(process_group).await;
        crash_cancellation_child(&mut child);
        assert_process_group_gone(process_group).await;

        let identity = acknowledgement_identity(request(&task_dispatch));
        let fence = cancellation_adapter
            .load(&identity)
            .await
            .unwrap()
            .expect("Executor crash retained cancellation fence");
        assert_eq!(fence.owner.as_deref(), Some(owner.as_str()));
        let claim = claim_from_fence(&fence);
        let before = if target == RedisTaskCancellationBoundary::BeforeTerminalElection {
            None
        } else {
            Some(retained_terminal_event(&fixture, &namespace, &claim.dispatch_key).await)
        };
        let process_event = terminal_task_event(&task_dispatch, Uuid::parse_str(&owner).unwrap());
        let coordinator = SafeCancellationCoordinator::new(cancellation_adapter.clone());
        let handler = LocalTaskHandler::new(UnusedLauncher);
        let (cancellation, process_exit) = tokio::join!(
            coordinator.resume_fence(&handler, fence.clone()),
            dispatch_adapter.elect_terminal(
                &claim,
                LocalAttemptOutcome::ProcessExitedFailure,
                &process_event,
                Utc::now(),
            ),
        );
        let cancellation = cancellation.unwrap();
        let process_exit = process_exit.unwrap();
        if target == RedisTaskCancellationBoundary::BeforeTerminalElection {
            let winners = usize::from(cancellation.election == Some(TerminalElection::Won))
                + usize::from(process_exit == TerminalElection::Won);
            assert_eq!(winners, 1);
        } else {
            assert!(matches!(
                cancellation.election,
                Some(TerminalElection::Settled(
                    LocalAttemptOutcome::CancellationKilled
                ))
            ));
            assert_eq!(
                process_exit,
                TerminalElection::Settled(LocalAttemptOutcome::CancellationKilled)
            );
        }

        let terminal = retained_terminal_event(&fixture, &namespace, &claim.dispatch_key).await;
        if let Some(before) = before {
            assert_eq!(terminal, before);
        }
        let decoded = tc::TaskEvent::decode(terminal.as_slice()).unwrap();
        assert!(matches!(
            decoded.kind,
            Some(tc::task_event::Kind::Failed(_))
        ));
        assert_eq!(decoded.task_instance_id, task_dispatch.task_instance_id);
        assert_eq!(decoded.executor_id.as_deref(), Some(owner.as_str()));
        assert!(!terminal
            .windows(namespace.len())
            .any(|window| window == namespace.as_bytes()));

        let acknowledgement = cancellation_adapter
            .acknowledgement(&identity)
            .await
            .unwrap()
            .expect("restart reconstructed one acknowledgement");
        let (_, restarted_cancellation) = adapters(
            &fixture,
            &namespace,
            &format!("restart-{target:?}"),
            dispatch_capability.clone(),
            cancellation_capability.clone(),
        )
        .await;
        assert_eq!(
            restarted_cancellation
                .acknowledgement(&identity)
                .await
                .unwrap(),
            Some(acknowledgement.clone())
        );
        let decoded_ack = tc::CancelTaskAck::decode(acknowledgement.as_slice()).unwrap();
        let expected = if target == RedisTaskCancellationBoundary::BeforeTerminalElection {
            tc::KillOutcome::NoSuchTask
        } else {
            tc::KillOutcome::Killed
        };
        assert_eq!(decoded_ack.outcome, expected as i32);
        assert_eq!(decoded_ack.task_instance_id, task_dispatch.task_instance_id);
        assert_eq!(
            decoded_ack.workflow_instance_id,
            task_dispatch.workflow_instance_id
        );
        assert!(!acknowledgement
            .windows(namespace.len())
            .any(|window| window == namespace.as_bytes()));

        if target == RedisTaskCancellationBoundary::AfterAcknowledgementStaging {
            pending_forward = Some((task_dispatch, cancellation.fence, acknowledgement));
        } else {
            assert!(cancellation_adapter
                .mark_acknowledgement_forwarded(&cancellation.fence)
                .await
                .unwrap());
            assert!(cancellation_adapter
                .complete_source(&cancellation.fence)
                .await
                .unwrap());
        }
    }

    let (task_dispatch, fence, acknowledgement) =
        pending_forward.expect("acknowledgement forwarding scenario");
    let forwarded_before_crash = scratch.path().join("forwarded-before-crash");
    let mut conductor = spawn_cancellation_child(
        &fixture,
        &namespace,
        "forward",
        RedisTaskCancellationBoundary::AfterAcknowledgementStaging,
        &task_dispatch,
        &owner,
        ChildPaths {
            phase: &phase,
            go: &go,
            pid: &pid_file,
            forwarded: &forwarded_before_crash,
        },
    );
    await_cancellation_phase(&mut conductor, &phase, "forwarded").await;
    crash_cancellation_child(&mut conductor);
    assert_eq!(fs::read(&forwarded_before_crash).unwrap(), acknowledgement);
    assert!(!cancellation_adapter
        .source_completed(&fence.acknowledgement_identity)
        .await
        .unwrap());

    let forwarded_after_restart = scratch.path().join("forwarded-after-restart");
    let mut recovered_conductor = spawn_cancellation_child(
        &fixture,
        &namespace,
        "complete",
        RedisTaskCancellationBoundary::AfterAcknowledgementStaging,
        &task_dispatch,
        &owner,
        ChildPaths {
            phase: &phase,
            go: &go,
            pid: &pid_file,
            forwarded: &forwarded_after_restart,
        },
    );
    await_cancellation_phase(&mut recovered_conductor, &phase, "completed").await;
    let status = recovered_conductor
        .wait()
        .expect("reap recovered Conductor process");
    assert!(status.success());
    assert_eq!(fs::read(&forwarded_after_restart).unwrap(), acknowledgement);
    assert!(cancellation_adapter
        .source_completed(&fence.acknowledgement_identity)
        .await
        .unwrap());
}

fn generate_tls(path: &PathBuf) -> String {
    let ca_key = path.join("ca.key");
    let ca_cert = path.join("ca.crt");
    let server_key = path.join("server.key");
    let server_request = path.join("server.csr");
    let server_cert = path.join("server.crt");
    let extensions = path.join("server.ext");
    run(
        StdCommand::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes"])
            .arg("-keyout")
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca_cert)
            .args([
                "-subj",
                "/CN=Tickr Redis TaskCancellation Test CA",
                "-days",
                "1",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]),
        "generate Redis test CA",
    );
    run(
        StdCommand::new("openssl")
            .args(["req", "-newkey", "rsa:2048", "-nodes"])
            .arg("-keyout")
            .arg(&server_key)
            .arg("-out")
            .arg(&server_request)
            .args(["-subj", "/CN=localhost"]),
        "generate Redis server request",
    );
    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n",
    )
    .expect("write certificate extensions");
    run(
        StdCommand::new("openssl")
            .args(["x509", "-req"])
            .arg("-in")
            .arg(&server_request)
            .arg("-CA")
            .arg(&ca_cert)
            .arg("-CAkey")
            .arg(&ca_key)
            .arg("-CAcreateserial")
            .arg("-out")
            .arg(&server_cert)
            .args(["-days", "1", "-sha256", "-extfile"])
            .arg(&extensions),
        "sign Redis server certificate",
    );
    fs::read_to_string(ca_cert).expect("read Redis test CA")
}

fn run(command: &mut StdCommand, operation: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{operation} could not start: {error}"));
    assert!(status.success(), "{operation} failed with {status}");
}
