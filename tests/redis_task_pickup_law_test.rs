#![cfg(not(madsim))]

#[path = "support/attempt_outcome_laws.rs"]
mod attempt_outcome_laws;

use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command as StdCommand,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use chrono::Utc;
use prost::Message;
use redis::{ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_durability::RedisDurabilityGuard,
    redis_task_liveness::{
        RedisLivenessWatchdog, RedisLivenessWatchdogCapability, RedisLivenessWatchdogConfig,
        RedisLivenessWatchdogError, RedisLivenessWatchdogQuotaState,
    },
    redis_task_pickup::{
        RedisTaskDispatch, RedisTaskDispatchAcceptance, RedisTaskDispatchCapability,
        RedisTaskDispatchConfig, RedisTaskDispatchError, RedisTaskDispatchQuotaState,
    },
};
use tickr_executor::local_pickup::{
    prepare_pickup, LocalAttemptOutcome, LocalExecutorCapacity, LocalPickupClaim,
    NoopPickupCheckpoint, PickupBoundary, PickupCheckpoint, PickupOutcome, PickupPreparation,
    SafeAttemptOutcomeHandoff, SafeHandoffCoordinator, SafeLivenessWatchdog, SafePickupExecutor,
    SafePickupWriter, TaskProcessLauncher, TerminalElection,
};
use tickr_proto::{coord::TaskDispatchPublisher, task as tc};
use tokio::process::{Child, Command};
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ROLE_PASSWORD: &str = "redis-task-dispatch-secret";
const LIVENESS_PASSWORD: &str = "redis-liveness-watchdog-secret";
const ADMIN_PASSWORD: &str = "redis-task-dispatch-admin";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct GateCapability {
    open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisTaskDispatchQuotaState>>,
}

impl Default for GateCapability {
    fn default() -> Self {
        Self {
            open: AtomicBool::new(true),
            failures: Mutex::new(Vec::new()),
            quotas: Mutex::new(Vec::new()),
        }
    }
}

impl RedisTaskDispatchCapability for GateCapability {
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

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisTaskDispatchQuotaState) {
        lock(&self.quotas).push(state);
    }
}

impl RedisLivenessWatchdogCapability for GateCapability {
    fn guard_admission(&self) -> Result<u64, RedisLivenessWatchdogError> {
        self.open
            .load(Ordering::Acquire)
            .then_some(1)
            .ok_or(RedisLivenessWatchdogError::Unavailable)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLivenessWatchdogError> {
        if generation == 1 && self.open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RedisLivenessWatchdogError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, _state: RedisLivenessWatchdogQuotaState) {}
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
            .prefix("redis-task-dispatch-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-task-dispatch-{}-{sequence}",
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
                 user task-dispatch on >{ROLE_PASSWORD} ~tickr:{{{namespace}}}:task-dispatch:* -@all \
                 +eval +get +hdel +hget +hincrby +hmget +hset +set +time +waitaof \
                 +xack +xadd +xautoclaim +xdel +xgroup|create +xrange +xreadgroup \
                 +zadd +zrangebyscore +zrem\n\
                 user liveness-watchdog on >{LIVENESS_PASSWORD} ~tickr:{{{namespace}}}:liveness-watchdog:* -@all \
                 +eval +get +hdel +hget +hincrby +hset +set +time +waitaof \
                 +zadd +zrangebyscore +zrem\n\
                 user task-dispatch-admin on >{ADMIN_PASSWORD} ~* &* +@all\n"
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
            "start Redis TaskDispatch fixture",
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
        panic!("Redis TaskDispatch fixture did not become ready");
    }

    fn client(&self) -> redis::Client {
        tls_client(self.port, "task-dispatch", ROLE_PASSWORD, &self.trust_roots)
    }

    fn liveness_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "liveness-watchdog",
            LIVENESS_PASSWORD,
            &self.trust_roots,
        )
    }

    fn admin_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "task-dispatch-admin",
            ADMIN_PASSWORD,
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

async fn namespace_snapshot(fixture: &RedisFixture, namespace: &str) -> Vec<(String, Vec<u8>)> {
    let mut connection = fixture
        .admin_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("tickr:{{{namespace}}}:task-dispatch:*"))
        .query_async(&mut connection)
        .await
        .unwrap();
    keys.sort();
    let mut snapshot = Vec::with_capacity(keys.len());
    for key in keys {
        let dump: Vec<u8> = redis::cmd("DUMP")
            .arg(&key)
            .query_async(&mut connection)
            .await
            .unwrap();
        snapshot.push((key, dump));
    }
    snapshot
}

async fn claim_metadata(
    fixture: &RedisFixture,
    namespace: &str,
    claim: &LocalPickupClaim,
) -> (i64, String, i64, i64) {
    let mut connection = fixture
        .admin_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let prefix = format!("tickr:{{{namespace}}}:task-dispatch");
    let generation: i64 = redis::cmd("HGET")
        .arg(format!("{prefix}:generations"))
        .arg(&claim.dispatch_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let owner: String = redis::cmd("HGET")
        .arg(format!("{prefix}:owners"))
        .arg(&claim.dispatch_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let deadline: i64 = redis::cmd("HGET")
        .arg(format!("{prefix}:deadlines"))
        .arg(&claim.dispatch_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    let indexed_deadline: i64 = redis::cmd("ZSCORE")
        .arg(format!("{prefix}:deadline-index"))
        .arg(&claim.dispatch_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    (generation, owner, deadline, indexed_deadline)
}

fn config(namespace: &str, consumer: &str) -> RedisTaskDispatchConfig {
    let mut config = RedisTaskDispatchConfig::new(namespace, consumer);
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(1024).unwrap();
    config.max_dispatches = NonZeroUsize::new(3).unwrap();
    config.max_active_claims = NonZeroUsize::new(1).unwrap();
    config.max_staged_events = NonZeroUsize::new(32).unwrap();
    config.soft_limit_bytes = 12_000;
    config.hard_limit_bytes = 20_000;
    config
}

async fn adapter(
    fixture: &RedisFixture,
    namespace: &str,
    consumer: &str,
    capability: Arc<GateCapability>,
) -> RedisTaskDispatch {
    RedisTaskDispatch::connect(
        fixture.client(),
        config(namespace, consumer),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap()
}

async fn liveness_watchdog(
    fixture: &RedisFixture,
    namespace: &str,
    capability: Arc<GateCapability>,
) -> RedisLivenessWatchdog {
    let mut config = RedisLivenessWatchdogConfig::new(namespace);
    config.max_records = NonZeroUsize::new(1).unwrap();
    RedisLivenessWatchdog::connect(
        fixture.liveness_client(),
        config,
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap()
}

fn dispatch() -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "redis-dispatch-task".to_owned(),
        task_type: 0,
        nix_expression_path: "/p".to_owned(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        tenant_id: "test".to_owned(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: Vec::new(),
    }
}

async fn prepare_claim(
    adapter: &RedisTaskDispatch,
    owner: &str,
    timeout: Duration,
) -> tickr_executor::local_pickup::PreparedPickup {
    adapter
        .append(
            &format!("dispatch:{}", Uuid::new_v4()),
            dispatch().encode_to_vec(),
        )
        .await
        .unwrap();
    let prepared = match prepare_pickup(
        adapter,
        &NoopPickupCheckpoint,
        owner,
        Uuid::new_v4(),
        chrono::Duration::from_std(timeout).unwrap(),
    )
    .await
    .unwrap()
    {
        PickupPreparation::Ready(prepared) => prepared,
        other => panic!("expected prepared Redis pickup, got {other:?}"),
    };
    assert!(adapter
        .stage_started(&prepared.claim, b"backend-neutral Started", Utc::now())
        .await
        .unwrap());
    prepared
}

#[derive(Clone)]
struct ShellLauncher {
    launches: Arc<AtomicUsize>,
    delay: Duration,
}

impl TaskProcessLauncher for ShellLauncher {
    async fn spawn(&self, _task: &tickr_executor::wire::DispatchedTask) -> Result<Child, String> {
        self.launches.fetch_add(1, Ordering::AcqRel);
        Command::new("sh")
            .arg("-c")
            .arg(format!("sleep {}", self.delay.as_secs_f64()))
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
struct MissingLauncher;

impl TaskProcessLauncher for MissingLauncher {
    async fn spawn(&self, _task: &tickr_executor::wire::DispatchedTask) -> Result<Child, String> {
        Command::new("/definitely/missing/tickr-task")
            .spawn()
            .map(|_| unreachable!())
            .map_err(|error| error.to_string())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_pickup_laws_cover_capacity_handoff_fences_and_pressure() {
    let namespace = format!("task-dispatch-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(GateCapability::default());
    let adapter = adapter(&fixture, &namespace, "executor-a", capability.clone()).await;

    let first = dispatch().encode_to_vec();
    let second = dispatch().encode_to_vec();
    TaskDispatchPublisher::stage(&adapter, "dispatch:first", &first)
        .await
        .unwrap();
    assert_eq!(
        adapter.append("dispatch:second", second).await.unwrap(),
        RedisTaskDispatchAcceptance::Appended
    );

    let launches = Arc::new(AtomicUsize::new(0));
    let executor = SafePickupExecutor::new(
        adapter.clone(),
        ShellLauncher {
            launches: launches.clone(),
            delay: Duration::from_millis(150),
        },
        LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap()),
        "executor-a",
        Duration::from_secs(2),
    );

    let first_run = tokio::spawn({
        let executor = executor.clone();
        async move { executor.run_one().await.unwrap() }
    });
    for _ in 0..100 {
        let state = adapter.quota_state().await.unwrap();
        if state.active_claims == 1 && state.dispatch_entries == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let second_run = tokio::spawn({
        let executor = executor.clone();
        async move { executor.run_one().await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let saturated = adapter.quota_state().await.unwrap();
    assert_eq!(saturated.active_claims, 1);
    assert_eq!(saturated.dispatch_entries, 1);
    assert_eq!(launches.load(Ordering::Acquire), 1);

    let first_outcome = first_run.await.unwrap();
    let second_outcome = second_run.await.unwrap();
    let first_claim = launched_claim(first_outcome);
    let second_claim = launched_claim(second_outcome);
    assert_eq!(launches.load(Ordering::Acquire), 2);
    let settled = adapter.quota_state().await.unwrap();
    assert_eq!(settled.dispatch_entries, 0);
    assert_eq!(settled.active_claims, 0);
    assert_eq!(settled.staged_events, 6);

    let stale = LocalPickupClaim {
        pickup_generation: first_claim.pickup_generation + 1,
        owner: "stale-owner".to_owned(),
        ..first_claim.clone()
    };
    let now = Utc::now();
    assert!(!adapter
        .renew_liveness(&stale, now + chrono::Duration::seconds(1), now)
        .await
        .unwrap());
    assert!(!adapter.complete_source(&stale).await.unwrap());
    assert!(!adapter
        .stage_started(&stale, b"stale-started", now)
        .await
        .unwrap());
    assert_eq!(
        adapter
            .elect_terminal(
                &stale,
                LocalAttemptOutcome::CancellationKilled,
                b"stale-cancel",
                now,
            )
            .await
            .unwrap(),
        TerminalElection::Settled(LocalAttemptOutcome::ProcessExitedSuccess)
    );
    assert_eq!(
        adapter
            .elect_terminal(
                &stale,
                LocalAttemptOutcome::ProcessExitedFailure,
                b"stale-failure",
                now,
            )
            .await
            .unwrap(),
        TerminalElection::Settled(LocalAttemptOutcome::ProcessExitedSuccess)
    );

    assert!(adapter.complete_staged_handoff(&first_claim).await.unwrap());
    assert!(adapter
        .complete_staged_handoff(&second_claim)
        .await
        .unwrap());
    assert!(adapter.complete_staged_handoff(&first_claim).await.unwrap());
    assert_eq!(adapter.quota_state().await.unwrap().staged_events, 0);

    adapter.append("dispatch:poison", vec![0xff]).await.unwrap();
    let poison_executor = SafePickupExecutor::new(
        adapter.clone(),
        ShellLauncher {
            launches: launches.clone(),
            delay: Duration::from_millis(1),
        },
        LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap()),
        "executor-a",
        Duration::from_secs(2),
    );
    assert!(matches!(
        poison_executor.run_one().await.unwrap(),
        PickupOutcome::PoisonRejected { .. }
    ));
    assert_eq!(launches.load(Ordering::Acquire), 2);
    let rejected = adapter.quota_state().await.unwrap();
    assert_eq!(rejected.dispatch_entries, 0);
    assert_eq!(rejected.active_claims, 0);
    assert_eq!(rejected.staged_events, 1);

    for index in 0..3 {
        adapter
            .append(&format!("pressure:{index}"), dispatch().encode_to_vec())
            .await
            .unwrap();
    }
    assert_eq!(
        adapter
            .append("pressure:fenced", dispatch().encode_to_vec())
            .await
            .unwrap_err(),
        RedisTaskDispatchError::CapacityFenced
    );
    assert_eq!(adapter.quota_state().await.unwrap().dispatch_entries, 3);

    capability.open.store(false, Ordering::Release);
    assert!(executor.run_one().await.is_err());
    assert_eq!(adapter.quota_state().await.unwrap().dispatch_entries, 3);
    capability.open.store(true, Ordering::Release);
    assert_eq!(adapter.quota_state().await.unwrap().dispatch_entries, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_outcome_laws_cover_deadlines_races_restart_and_capability_restoration() {
    let namespace = format!("task-outcome-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(GateCapability::default());
    let role = adapter(&fixture, &namespace, "executor-a", capability.clone()).await;

    let prepared = prepare_claim(&role, "executor-a", Duration::from_secs(2)).await;
    let before_renew = claim_metadata(&fixture, &namespace, &prepared.claim).await;
    let now = Utc::now();
    assert!(role
        .renew_liveness(&prepared.claim, now + chrono::Duration::seconds(2), now,)
        .await
        .unwrap());
    let after_renew = claim_metadata(&fixture, &namespace, &prepared.claim).await;
    assert_eq!(after_renew.0, before_renew.0);
    assert_eq!(after_renew.1, before_renew.1);
    assert!(after_renew.2 > before_renew.2);
    assert_eq!(after_renew.2, after_renew.3);
    assert!(role
        .select_due_liveness(Utc::now() + chrono::Duration::days(1))
        .await
        .unwrap()
        .is_none());

    let unchanged = namespace_snapshot(&fixture, &namespace).await;
    let mut stale_generation = prepared.claim.clone();
    stale_generation.pickup_generation += 1;
    let mut non_owner = prepared.claim.clone();
    non_owner.owner = "executor-b".to_owned();
    let mut missing = prepared.claim.clone();
    missing.dispatch_key = "999999-0".to_owned();
    for rejected in [&stale_generation, &non_owner, &missing] {
        assert!(!role
            .renew_liveness(rejected, now + chrono::Duration::seconds(2), now,)
            .await
            .unwrap());
    }
    assert_eq!(
        namespace_snapshot(&fixture, &namespace).await,
        unchanged,
        "rejected renewals must not install stable-operation evidence",
    );

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let restarted = adapter(&fixture, &namespace, "executor-b", capability.clone()).await;
    let due = restarted
        .select_due_liveness(Utc::now() - chrono::Duration::days(1))
        .await
        .unwrap()
        .expect("Redis server time, not the supplied client time, makes the deadline due");
    assert_eq!(due.claim.dispatch_key, prepared.claim.dispatch_key);
    assert_eq!(
        due.claim.pickup_generation,
        prepared.claim.pickup_generation
    );
    assert_eq!(due.claim.owner, prepared.claim.owner);
    assert_eq!(
        due.claim.liveness_deadline.timestamp_millis(),
        after_renew.2
    );
    assert_eq!(
        restarted.sweep_one_due().await.unwrap().unwrap().1,
        TerminalElection::Won,
    );
    let staged_during_conductor_outage = restarted.quota_state().await.unwrap();
    assert_eq!(staged_during_conductor_outage.active_claims, 0);
    assert_eq!(staged_during_conductor_outage.staged_events, 3);
    assert_eq!(
        restarted
            .elect_terminal(
                &stale_generation,
                LocalAttemptOutcome::ProcessExitedFailure,
                b"late process exit",
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired),
    );
    let elected = namespace_snapshot(&fixture, &namespace).await;
    assert!(!restarted
        .renew_liveness(
            &prepared.claim,
            Utc::now() + chrono::Duration::seconds(1),
            Utc::now(),
        )
        .await
        .unwrap());
    assert_eq!(namespace_snapshot(&fixture, &namespace).await, elected);
    assert!(restarted
        .complete_staged_handoff(&prepared.claim)
        .await
        .unwrap());
    assert_eq!(restarted.quota_state().await.unwrap().staged_events, 0);

    let race = prepare_claim(&restarted, "executor-b", Duration::from_secs(2)).await;
    let status = Command::new("sh")
        .args(["-c", "exit 17"])
        .status()
        .await
        .expect("spawn real task process");
    assert!(!status.success());
    let winner =
        attempt_outcome_laws::assert_attempt_outcome_law(restarted.clone(), &race.claim).await;
    assert!(matches!(
        winner,
        LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::LivenessExpired
    ));

    restarted
        .append(
            &format!("dispatch:setup:{}", Uuid::new_v4()),
            dispatch().encode_to_vec(),
        )
        .await
        .unwrap();
    let setup_executor = SafePickupExecutor::new(
        restarted.clone(),
        MissingLauncher,
        LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap()),
        "executor-b",
        Duration::from_secs(2),
    );
    let setup_result = setup_executor.run_one().await.unwrap();
    assert!(matches!(
        setup_result,
        PickupOutcome::ProcessSetupFailed {
            election: TerminalElection::Won,
            ..
        }
    ));

    let unresolved = prepare_claim(&restarted, "executor-b", Duration::from_millis(500)).await;
    let pending_before_loss = namespace_snapshot(&fixture, &namespace).await;
    capability.open.store(false, Ordering::Release);
    assert!(restarted
        .elect_terminal(
            &unresolved.claim,
            LocalAttemptOutcome::ProcessExitedFailure,
            b"must remain pending while fenced",
            Utc::now(),
        )
        .await
        .is_err());
    assert_eq!(
        namespace_snapshot(&fixture, &namespace).await,
        pending_before_loss,
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    capability.open.store(true, Ordering::Release);
    let restored = adapter(&fixture, &namespace, "executor-c", capability).await;
    assert_eq!(
        restored.sweep_one_due().await.unwrap().unwrap().1,
        TerminalElection::Won,
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_liveness_role_isolated_election_laws() {
    let namespace = format!("liveness-role-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(GateCapability::default());
    let dispatch_role = adapter(
        &fixture,
        &namespace,
        "executor-liveness",
        capability.clone(),
    )
    .await;
    let liveness_role = liveness_watchdog(&fixture, &namespace, capability.clone()).await;
    let handoff = SafeHandoffCoordinator::new(dispatch_role.clone(), liveness_role.clone());

    let dispatch_manifest = RedisTaskDispatch::operation_manifest().unwrap();
    let liveness_manifest = RedisLivenessWatchdog::operation_manifest().unwrap();
    assert_ne!(dispatch_manifest.protocol(), liveness_manifest.protocol());
    assert!(dispatch_manifest
        .key_patterns()
        .iter()
        .all(|pattern| !pattern.contains(":liveness-watchdog:")));
    assert!(liveness_manifest
        .key_patterns()
        .iter()
        .all(|pattern| !pattern.contains(":task-dispatch:")));

    let mut dispatch_connection = fixture
        .client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let denied_liveness: redis::RedisResult<Option<Vec<u8>>> = redis::cmd("GET")
        .arg(format!(
            "tickr:{{{namespace}}}:liveness-watchdog:generations"
        ))
        .query_async(&mut dispatch_connection)
        .await;
    assert!(denied_liveness.is_err());
    let mut liveness_connection = fixture
        .liveness_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let denied_dispatch: redis::RedisResult<Option<Vec<u8>>> = redis::cmd("GET")
        .arg(format!("tickr:{{{namespace}}}:task-dispatch:generations"))
        .query_async(&mut liveness_connection)
        .await;
    assert!(denied_dispatch.is_err());

    let encoded = dispatch().encode_to_vec();
    dispatch_role
        .append("liveness-role:first", encoded.clone())
        .await
        .unwrap();
    let prepared = match prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-liveness",
        Uuid::new_v4(),
        chrono::Duration::milliseconds(150),
    )
    .await
    .unwrap()
    {
        PickupPreparation::Ready(prepared) => prepared,
        other => panic!("expected isolated-role pickup, got {other:?}"),
    };

    let renewal_now = Utc::now();
    assert!(handoff
        .renew_liveness(
            &prepared.claim,
            renewal_now + chrono::Duration::milliseconds(150),
            renewal_now,
        )
        .await
        .unwrap());

    let mut stale = prepared.claim.clone();
    stale.pickup_generation += 1;
    let mut non_owner = prepared.claim.clone();
    non_owner.owner = "another-executor".to_owned();
    let mut missing = prepared.claim.clone();
    missing.dispatch_key = "missing-dispatch".to_owned();
    for rejected in [&stale, &non_owner, &missing] {
        assert!(!handoff
            .renew_liveness(
                rejected,
                renewal_now + chrono::Duration::seconds(1),
                renewal_now,
            )
            .await
            .unwrap());
    }

    let pressure_claim = LocalPickupClaim {
        dispatch_key: "pressure-dispatch".to_owned(),
        pickup_generation: 1,
        owner: "pressure-owner".to_owned(),
        liveness_deadline: renewal_now + chrono::Duration::seconds(1),
    };
    assert!(liveness_role
        .arm_liveness(
            &pressure_claim,
            &encoded,
            pressure_claim.liveness_deadline,
            renewal_now,
        )
        .await
        .is_err());

    tokio::time::sleep(Duration::from_millis(200)).await;
    let restarted_liveness = liveness_watchdog(&fixture, &namespace, capability.clone()).await;
    let restarted = SafeHandoffCoordinator::new(dispatch_role.clone(), restarted_liveness.clone());
    let competing_sweeper = restarted.clone();
    let (first_sweep, second_sweep) =
        tokio::join!(restarted.sweep_one_due(), competing_sweeper.sweep_one_due(),);
    let first_election = first_sweep.unwrap().unwrap().1;
    let second_election = second_sweep.unwrap().unwrap().1;
    assert!(matches!(
        (first_election, second_election),
        (
            TerminalElection::Won,
            TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired)
        ) | (
            TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired),
            TerminalElection::Won
        )
    ));
    assert!(!restarted
        .renew_liveness(
            &prepared.claim,
            Utc::now() + chrono::Duration::seconds(1),
            Utc::now(),
        )
        .await
        .unwrap());
    assert_eq!(
        restarted
            .elect_terminal(
                &prepared.claim,
                LocalAttemptOutcome::ProcessExitedFailure,
                b"late process exit",
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired)
    );

    let retained = restarted_liveness.quota_state().await.unwrap();
    assert_eq!(retained.liveness_records, 1);
    assert_eq!(retained.deadline_entries, 0);
    assert_eq!(retained.durable_outcomes, 1);
    assert_eq!(retained.staged_events, 1);
    assert!(dispatch_role
        .complete_staged_handoff(&prepared.claim)
        .await
        .unwrap());
    assert!(restarted_liveness
        .complete_staged_handoff(&prepared.claim)
        .await
        .unwrap());
    assert_eq!(
        restarted_liveness
            .quota_state()
            .await
            .unwrap()
            .liveness_records,
        0
    );

    assert!(restarted_liveness
        .arm_liveness(
            &pressure_claim,
            &encoded,
            pressure_claim.liveness_deadline,
            renewal_now,
        )
        .await
        .unwrap());
    assert!(restarted_liveness
        .complete_source(&pressure_claim)
        .await
        .unwrap());
    assert_eq!(
        restarted_liveness
            .elect_terminal(
                &pressure_claim,
                LocalAttemptOutcome::ProcessExitedFailure,
                b"pressure terminal event",
                Utc::now(),
            )
            .await
            .unwrap()
            .election,
        TerminalElection::Won
    );
    assert!(restarted_liveness
        .complete_staged_handoff(&pressure_claim)
        .await
        .unwrap());

    let race_payload = dispatch().encode_to_vec();
    dispatch_role
        .append("liveness-role:race", race_payload)
        .await
        .unwrap();
    let race = match prepare_pickup(
        &restarted,
        &NoopPickupCheckpoint,
        "executor-liveness",
        Uuid::new_v4(),
        chrono::Duration::seconds(1),
    )
    .await
    .unwrap()
    {
        PickupPreparation::Ready(prepared) => prepared,
        other => panic!("expected race pickup, got {other:?}"),
    };
    let status = Command::new("sh")
        .args(["-c", "exit 17"])
        .status()
        .await
        .expect("spawn real task process");
    assert!(!status.success());
    let winner =
        attempt_outcome_laws::assert_attempt_outcome_law(restarted.clone(), &race.claim).await;
    assert!(matches!(
        winner,
        LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::LivenessExpired
    ));

    capability.open.store(false, Ordering::Release);
    let before_restore = restarted_liveness.quota_state().await.unwrap();
    assert!(restarted
        .elect_terminal(
            &race.claim,
            LocalAttemptOutcome::ProcessSetupFailed,
            b"fenced duplicate",
            Utc::now(),
        )
        .await
        .is_err());
    assert_eq!(
        restarted_liveness.quota_state().await.unwrap(),
        before_restore
    );
    capability.open.store(true, Ordering::Release);
    assert_eq!(
        restarted
            .elect_terminal(
                &race.claim,
                LocalAttemptOutcome::ProcessSetupFailed,
                b"restored duplicate",
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Settled(winner)
    );
}

fn launched_claim(outcome: PickupOutcome) -> LocalPickupClaim {
    match outcome {
        PickupOutcome::Launched { claim, .. } => claim,
        other => panic!("expected launched pickup, got {other:?}"),
    }
}

fn crash_config(namespace: &str, consumer: &str) -> RedisTaskDispatchConfig {
    let mut config = RedisTaskDispatchConfig::new(namespace, consumer);
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(1024).unwrap();
    config.max_dispatches = NonZeroUsize::new(64).unwrap();
    config.max_active_claims = NonZeroUsize::new(64).unwrap();
    config.max_staged_events = NonZeroUsize::new(64).unwrap();
    config.soft_limit_bytes = 40 * 1024 * 1024;
    config.hard_limit_bytes = 48 * 1024 * 1024;
    config
}

#[derive(Clone)]
struct MarkerLauncher {
    launches: PathBuf,
}

impl TaskProcessLauncher for MarkerLauncher {
    async fn spawn(&self, _task: &tickr_executor::wire::DispatchedTask) -> Result<Child, String> {
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.launches)
            .map_err(|error| error.to_string())?;
        writeln!(file, "launch").map_err(|error| error.to_string())?;
        Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
struct CrashCheckpoint {
    target: PickupBoundary,
    ready: PathBuf,
}

impl PickupCheckpoint for CrashCheckpoint {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
        if boundary == self.target {
            fs::write(&self.ready, format!("{boundary:?}")).map_err(|error| error.to_string())?;
            loop {
                std::thread::sleep(Duration::from_secs(60));
            }
        }
        Ok(())
    }
}

fn boundary(name: &str) -> PickupBoundary {
    match name {
        "BeforeSelection" => PickupBoundary::BeforeSelection,
        "AfterSelection" => PickupBoundary::AfterSelection,
        "AfterValidation" => PickupBoundary::AfterValidation,
        "AfterClaimCommit" => PickupBoundary::AfterClaimCommit,
        "AfterAssignedStaging" => PickupBoundary::AfterAssignedStaging,
        "AfterInitialLivenessArm" => PickupBoundary::AfterInitialLivenessArm,
        "AfterClaimProof" => PickupBoundary::AfterClaimProof,
        "AfterSourceAcknowledgement" => PickupBoundary::AfterSourceAcknowledgement,
        "AfterSpawn" => PickupBoundary::AfterSpawn,
        "AfterStartedStaging" => PickupBoundary::AfterStartedStaging,
        "AfterFirstLivenessRenewal" => PickupBoundary::AfterFirstLivenessRenewal,
        "AfterProcessExitObservation" => PickupBoundary::AfterProcessExitObservation,
        "AfterTerminalElection" => PickupBoundary::AfterTerminalElection,
        "AfterTerminalEventStaging" => PickupBoundary::AfterTerminalEventStaging,
        other => panic!("unknown pickup boundary {other}"),
    }
}

#[test]
#[ignore = "spawned by redis_task_pickup_real_process_crash_matrix"]
fn redis_task_pickup_process_child() {
    if std::env::var_os("TICKR_REDIS_TASK_PICKUP_CHILD").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let port = std::env::var("TICKR_REDIS_TASK_PICKUP_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let roots = std::env::var("TICKR_REDIS_TASK_PICKUP_ROOTS").unwrap();
        let namespace = std::env::var("TICKR_REDIS_TASK_PICKUP_NAMESPACE").unwrap();
        let target = boundary(&std::env::var("TICKR_REDIS_TASK_PICKUP_BOUNDARY").unwrap());
        let ready = PathBuf::from(std::env::var("TICKR_REDIS_TASK_PICKUP_READY").unwrap());
        let launches = PathBuf::from(std::env::var("TICKR_REDIS_TASK_PICKUP_LAUNCHES").unwrap());
        let capability = Arc::new(GateCapability::default());
        let adapter = RedisTaskDispatch::connect(
            tls_client(port, "task-dispatch", ROLE_PASSWORD, &roots),
            crash_config(&namespace, &format!("child-{}", std::process::id())),
            RedisDurabilityGuard::new(Duration::from_secs(30), Duration::from_secs(30)),
            capability,
        )
        .await
        .unwrap();
        let executor = SafePickupExecutor::with_checkpoint(
            adapter,
            MarkerLauncher { launches },
            CrashCheckpoint { target, ready },
            LocalExecutorCapacity::new(
                Uuid::parse_str("00000000-0000-0000-0000-000000000027").unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            "crash-owner",
            Duration::from_secs(2),
        );
        let _ = executor.run_one().await.unwrap();
        panic!("pickup child did not stop at requested boundary");
    });
}

fn spawn_pickup_child(
    fixture: &RedisFixture,
    namespace: &str,
    target: PickupBoundary,
    ready: &Path,
    launches: &Path,
) -> std::process::Child {
    let _ = fs::remove_file(ready);
    StdCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "redis_task_pickup_process_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TICKR_REDIS_TASK_PICKUP_CHILD", "1")
        .env("TICKR_REDIS_TASK_PICKUP_PORT", fixture.port.to_string())
        .env("TICKR_REDIS_TASK_PICKUP_ROOTS", &fixture.trust_roots)
        .env("TICKR_REDIS_TASK_PICKUP_NAMESPACE", namespace)
        .env("TICKR_REDIS_TASK_PICKUP_BOUNDARY", format!("{target:?}"))
        .env("TICKR_REDIS_TASK_PICKUP_READY", ready)
        .env("TICKR_REDIS_TASK_PICKUP_LAUNCHES", launches)
        .spawn()
        .expect("spawn Redis pickup owner process")
}

async fn await_pickup_boundary(
    child: &mut std::process::Child,
    ready: &Path,
    expected: PickupBoundary,
) {
    for _ in 0..800 {
        if fs::read_to_string(ready).is_ok_and(|value| value == format!("{expected:?}")) {
            return;
        }
        if let Some(status) = child.try_wait().expect("query child status") {
            panic!("Redis pickup owner exited before {expected:?}: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Redis pickup owner did not reach {expected:?}");
}

fn kill_pickup_child(child: &mut std::process::Child) {
    child.kill().expect("crash Redis pickup owner");
    let _ = child.wait().expect("reap Redis pickup owner");
}

fn launch_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|value| value.lines().count())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_task_pickup_real_process_crash_matrix() {
    let namespace = format!("task-dispatch-crash-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(GateCapability::default());
    let adapter = RedisTaskDispatch::connect(
        fixture.client(),
        crash_config(&namespace, "parent"),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let boundaries = [
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
        PickupBoundary::AfterProcessExitObservation,
        PickupBoundary::AfterTerminalElection,
        PickupBoundary::AfterTerminalEventStaging,
    ];

    for (index, target) in boundaries.into_iter().enumerate() {
        adapter
            .append(&format!("crash:{index}"), dispatch().encode_to_vec())
            .await
            .unwrap();
        let ready = scratch.path().join(format!("ready-{index}"));
        let launches = scratch.path().join(format!("launches-{index}"));
        let mut child = spawn_pickup_child(&fixture, &namespace, target, &ready, &launches);
        await_pickup_boundary(&mut child, &ready, target).await;
        kill_pickup_child(&mut child);
        tokio::time::sleep(Duration::from_millis(30)).await;

        let recovery = SafePickupExecutor::new(
            adapter.clone(),
            MarkerLauncher {
                launches: launches.clone(),
            },
            LocalExecutorCapacity::new(
                Uuid::parse_str("00000000-0000-0000-0000-000000000027").unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            "crash-owner",
            Duration::from_secs(2),
        )
        .run_one()
        .await
        .unwrap();

        let before_claim = matches!(
            target,
            PickupBoundary::BeforeSelection
                | PickupBoundary::AfterSelection
                | PickupBoundary::AfterValidation
        );
        let after_spawn = matches!(
            target,
            PickupBoundary::AfterSpawn
                | PickupBoundary::AfterStartedStaging
                | PickupBoundary::AfterFirstLivenessRenewal
                | PickupBoundary::AfterProcessExitObservation
                | PickupBoundary::AfterTerminalElection
                | PickupBoundary::AfterTerminalEventStaging
        );
        if before_claim {
            assert!(matches!(recovery, PickupOutcome::Launched { .. }));
        } else {
            assert!(matches!(recovery, PickupOutcome::NoWork));
        }
        assert_eq!(
            launch_count(&launches),
            usize::from(before_claim || after_spawn),
            "boundary {target:?} launched more than once"
        );
    }
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
                "/CN=Tickr Redis TaskDispatch Test CA",
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
