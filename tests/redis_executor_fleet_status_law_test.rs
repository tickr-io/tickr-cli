#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use prost::Message;
use redis::{ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_durability::RedisDurabilityGuard,
    redis_executor_fleet_status::{
        RedisExecutorFleetStatus, RedisExecutorFleetStatusCapability,
        RedisExecutorFleetStatusConfig, RedisExecutorFleetStatusQuotaState,
        RedisExecutorObservationOutcome,
    },
    redis_task_pickup::{
        RedisTaskDispatch, RedisTaskDispatchAcceptance, RedisTaskDispatchCapability,
        RedisTaskDispatchConfig, RedisTaskDispatchError, RedisTaskDispatchQuotaState,
    },
};
use tickr_api::http::health::{
    check_executor_fleet, check_executor_fleet_observations, ComponentStatus,
    ExecutorCapacityInterpretation,
};
use tickr_executor::local_pickup::{
    prepare_pickup, ExecutorCapacityObservation, ExecutorFleetSnapshot, NoopPickupCheckpoint,
    PickupPreparation,
};
use tickr_proto::task as tc;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const FLEET_PASSWORD: &str = "redis-executor-fleet-secret";
const ADMIN_PASSWORD: &str = "redis-executor-fleet-admin";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct FleetCapability {
    open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisExecutorFleetStatusQuotaState>>,
}

impl Default for FleetCapability {
    fn default() -> Self {
        Self {
            open: AtomicBool::new(true),
            failures: Mutex::new(Vec::new()),
            quotas: Mutex::new(Vec::new()),
        }
    }
}

impl RedisExecutorFleetStatusCapability for FleetCapability {
    fn observation_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisExecutorFleetStatusQuotaState) {
        lock(&self.quotas).push(state);
    }
}

#[derive(Default)]
struct DispatchCapability;

impl RedisTaskDispatchCapability for DispatchCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskDispatchError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskDispatchError> {
        (generation == 1)
            .then_some(())
            .ok_or(RedisTaskDispatchError::Unavailable)
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisTaskDispatchQuotaState) {}
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
            .prefix("redis-executor-fleet-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-executor-fleet-{}-{sequence}",
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
                 user default on >{ADMIN_PASSWORD} ~* &* +@all\n\
                 user executor-fleet on >{FLEET_PASSWORD} ~tickr:{{{namespace}}}:executor-fleet-status:* -@all \
                 +eval +hdel +hget +hgetall +hset +time +zadd +zrangebyscore +zrem\n"
            ),
        )
        .expect("write Redis fixture configuration");
        let mount = format!("{}:/tls:ro", path.display());
        run(
            Command::new("docker").args([
                "run",
                "--detach",
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
            "start Redis ExecutorFleetStatus fixture",
        );
        let port = docker_port(&name);
        wait_for_port(&name, port).await;
        Self {
            _directory: directory,
            name,
            port,
            trust_roots,
        }
    }

    fn role_client(&self) -> redis::Client {
        self.client("executor-fleet", FLEET_PASSWORD)
    }

    fn admin_client(&self) -> redis::Client {
        self.client("default", ADMIN_PASSWORD)
    }

    fn client(&self, username: &str, password: &str) -> redis::Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let connection = format!("rediss://{username}:{password}@localhost:{}/", self.port)
            .parse::<ConnectionInfo>()
            .expect("Redis connection");
        redis::Client::build_with_tls(
            connection,
            TlsCertificates {
                client_tls: None,
                root_cert: Some(self.trust_roots.as_bytes().to_vec()),
            },
        )
        .expect("Redis client")
    }

    async fn restart(&mut self) {
        run(
            Command::new("docker").args(["restart", &self.name]),
            "restart Redis ExecutorFleetStatus fixture",
        );
        self.port = docker_port(&self.name);
        wait_for_port(&self.name, self.port).await;
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

fn observation(
    executor_id: Uuid,
    reporter_id: Uuid,
    sequence: u64,
    configured_process_slots: usize,
    in_flight_count: usize,
) -> ExecutorCapacityObservation {
    ExecutorCapacityObservation {
        executor_id,
        reporter_id,
        sequence,
        configured_process_slots,
        in_flight_count,
        observed_at_server_millis: 0,
        expires_at_server_millis: 1,
    }
}

fn dispatch() -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "fleet-status-independent-dispatch".to_owned(),
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

fn assert_observational_health(snapshot: &ExecutorFleetSnapshot) {
    let health = check_executor_fleet_observations(snapshot);
    assert_ne!(health.status, ComponentStatus::Unhealthy);
    assert_eq!(
        health.capacity_interpretation,
        ExecutorCapacityInterpretation::ObservationOnly
    );
    assert!(health.detail.contains("not guaranteed available capacity"));
    let json = serde_json::to_value(health).unwrap();
    assert_eq!(json["capacity_interpretation"], "observation_only");
    assert!(json.get("available_capacity").is_none());
}

#[test]
fn backend_neutral_health_law_keeps_local_capacity_observational() {
    assert_observational_health(&ExecutorFleetSnapshot {
        server_time_millis: 50,
        observation_ttl_millis: 100,
        observations: vec![
            observation(Uuid::new_v4(), Uuid::new_v4(), 1, 2, 1).with_server_times(45, 145)
        ],
    });
}

trait ObservationTestTimes {
    fn with_server_times(self, observed_at: u64, expires_at: u64) -> Self;
}

impl ObservationTestTimes for ExecutorCapacityObservation {
    fn with_server_times(mut self, observed_at: u64, expires_at: u64) -> Self {
        self.observed_at_server_millis = observed_at;
        self.expires_at_server_millis = expires_at;
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_fleet_laws_cover_duplicates_pressure_isolation_restart_and_dispatch_independence(
) {
    let namespace = format!("executor-fleet-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let mut fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(FleetCapability::default());
    let mut config = RedisExecutorFleetStatusConfig::new(&namespace);
    config.observation_ttl = Duration::from_millis(180);
    config.soft_observation_limit = NonZeroUsize::new(2).unwrap();
    config.hard_observation_limit = NonZeroUsize::new(3).unwrap();
    let role = RedisExecutorFleetStatus::connect(
        fixture.role_client(),
        config.clone(),
        capability.clone(),
    )
    .await
    .unwrap();

    let executor = Uuid::new_v4();
    let reporter = Uuid::new_v4();
    let first = observation(executor, reporter, 2, 4, 1);
    assert_eq!(
        role.report(first).await.unwrap(),
        RedisExecutorObservationOutcome::Accepted
    );
    assert_eq!(
        role.report(first).await.unwrap(),
        RedisExecutorObservationOutcome::Duplicate
    );
    assert_eq!(
        role.report(observation(executor, reporter, 1, 4, 1))
            .await
            .unwrap(),
        RedisExecutorObservationOutcome::Stale
    );
    assert_eq!(
        role.report(observation(executor, reporter, 2, 4, 2))
            .await
            .unwrap(),
        RedisExecutorObservationOutcome::Conflict
    );
    assert_eq!(
        role.report(observation(executor, Uuid::new_v4(), 1, 8, 0))
            .await
            .unwrap(),
        RedisExecutorObservationOutcome::Conflict
    );

    let second = observation(Uuid::new_v4(), Uuid::new_v4(), 1, 8, 3);
    let third = observation(Uuid::new_v4(), Uuid::new_v4(), 1, 2, 0);
    let fenced = observation(Uuid::new_v4(), Uuid::new_v4(), 1, 16, 0);
    assert_eq!(
        role.report(second).await.unwrap(),
        RedisExecutorObservationOutcome::Accepted
    );
    assert_eq!(
        role.report(third).await.unwrap(),
        RedisExecutorObservationOutcome::Accepted
    );
    assert_eq!(
        role.report(fenced).await.unwrap(),
        RedisExecutorObservationOutcome::FencedAtHardLimit
    );
    let pressure = role.quota_state().await.unwrap();
    assert_eq!(pressure.observed_executors, 3);
    assert_eq!(pressure.pressure, RedisQuotaPressure::HardLimit);
    assert_observational_health(&role.snapshot().await.unwrap());
    let production_health = check_executor_fleet(&role).await;
    assert_eq!(production_health.status, ComponentStatus::Healthy);
    assert_eq!(
        production_health.capacity_interpretation,
        ExecutorCapacityInterpretation::ObservationOnly
    );

    let mut admin = fixture
        .admin_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let other_role_key = format!("tickr:{{{namespace}}}:task-dispatch:fleet-sentinel");
    redis::cmd("SET")
        .arg(&other_role_key)
        .arg("untouched")
        .query_async::<()>(&mut admin)
        .await
        .unwrap();
    role.sweep_expired().await.unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg(&other_role_key)
            .query_async::<String>(&mut admin)
            .await
            .unwrap(),
        "untouched"
    );
    let mut restricted = fixture
        .role_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    assert!(redis::cmd("HSET")
        .arg(format!("tickr:{{{namespace}}}:task-dispatch:owners"))
        .arg("dispatch")
        .arg("executor")
        .query_async::<redis::Value>(&mut restricted)
        .await
        .is_err());
    assert!(redis::cmd("SET")
        .arg(format!(
            "tickr:{{{namespace}}}:executor-fleet-status:unregistered"
        ))
        .arg("value")
        .query_async::<redis::Value>(&mut restricted)
        .await
        .is_err());

    let dispatch_role = RedisTaskDispatch::connect(
        fixture.admin_client(),
        RedisTaskDispatchConfig::new(&namespace, "executor-a"),
        RedisDurabilityGuard::default(),
        Arc::new(DispatchCapability),
    )
    .await
    .unwrap();
    assert_eq!(
        dispatch_role
            .append("fleet-pressure-dispatch", dispatch().encode_to_vec())
            .await
            .unwrap(),
        RedisTaskDispatchAcceptance::Appended
    );
    let prepared = prepare_pickup(
        &dispatch_role,
        &NoopPickupCheckpoint,
        "executor-a",
        Uuid::new_v4(),
        chrono::Duration::seconds(2),
    )
    .await
    .unwrap();
    assert!(matches!(prepared, PickupPreparation::Ready(_)));

    capability.open.store(false, Ordering::Release);
    assert_eq!(
        role.report(observation(Uuid::new_v4(), Uuid::new_v4(), 1, 1, 0))
            .await
            .unwrap(),
        RedisExecutorObservationOutcome::SuppressedByCapabilityFence
    );
    assert_eq!(
        dispatch_role
            .append("fleet-capability-loss-dispatch", dispatch().encode_to_vec())
            .await
            .unwrap(),
        RedisTaskDispatchAcceptance::Appended
    );
    capability.open.store(true, Ordering::Release);

    tokio::time::sleep(Duration::from_millis(220)).await;
    assert_eq!(role.sweep_expired().await.unwrap(), 0);
    let released = role.quota_state().await.unwrap();
    assert_eq!(released.observed_executors, 0);
    assert!(released.expired_observations >= 3);
    assert_eq!(
        role.report(fenced).await.unwrap(),
        RedisExecutorObservationOutcome::Accepted
    );

    let mut restart_config = config;
    restart_config.observation_ttl = Duration::from_secs(5);
    let restart_namespace = format!("{namespace}-restart");
    restart_config.namespace.clone_from(&restart_namespace);
    let restart_role = RedisExecutorFleetStatus::connect(
        fixture.admin_client(),
        restart_config.clone(),
        capability.clone(),
    )
    .await
    .unwrap();
    let retained = observation(Uuid::new_v4(), Uuid::new_v4(), 1, 3, 1);
    assert_eq!(
        restart_role.report(retained).await.unwrap(),
        RedisExecutorObservationOutcome::Accepted
    );
    drop(restart_role);
    fixture.restart().await;
    let recovered =
        RedisExecutorFleetStatus::connect(fixture.admin_client(), restart_config, capability)
            .await
            .unwrap();
    let recovered_snapshot = recovered.snapshot().await.unwrap();
    assert_eq!(recovered_snapshot.observations.len(), 1);
    assert_eq!(
        recovered_snapshot.observations[0].executor_id,
        retained.executor_id
    );
    assert_observational_health(&recovered_snapshot);
}

fn docker_port(name: &str) -> u16 {
    let output = Command::new("docker")
        .args(["port", name, "6379/tcp"])
        .output()
        .expect("query Redis port");
    assert!(output.status.success(), "query Redis port failed");
    String::from_utf8(output.stdout)
        .expect("Docker port is UTF-8")
        .trim()
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .expect("Docker returned Redis port")
}

async fn wait_for_port(name: &str, port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let output = Command::new("docker")
        .args(["logs", name])
        .output()
        .expect("read Redis fixture logs");
    panic!(
        "Redis ExecutorFleetStatus fixture did not become ready: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn generate_tls(path: &PathBuf) -> String {
    let ca_key = path.join("ca.key");
    let ca_cert = path.join("ca.crt");
    let server_key = path.join("server.key");
    let server_request = path.join("server.csr");
    let server_cert = path.join("server.crt");
    let extensions = path.join("server.ext");
    run(
        Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes"])
            .arg("-keyout")
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca_cert)
            .args([
                "-subj",
                "/CN=Tickr Redis ExecutorFleetStatus Test CA",
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
        Command::new("openssl")
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
        Command::new("openssl")
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

fn run(command: &mut Command, operation: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{operation} could not start: {error}"));
    assert!(status.success(), "{operation} failed with {status}");
}
