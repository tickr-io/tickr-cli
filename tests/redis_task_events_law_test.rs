#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use redis::{ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_durability::RedisDurabilityGuard,
    redis_task_events::{
        RedisTaskEventAcceptance, RedisTaskEventCapability, RedisTaskEventError,
        RedisTaskEventForwardOutcome, RedisTaskEventQuotaState, RedisTaskEvents,
        RedisTaskEventsConfig,
    },
};
use tickr_conductor::{
    patch_pipeline::ParsedPatch,
    relay::{drain_task_event_source, TaskEventProjection, TaskEventProjector},
};
use tickr_executor::wire::{encode_task_event, DispatchedTask, EmitKind};
use tickr_proto::{
    coord::{TaskEventConsumer, TaskEventFuture, TaskEventWriter},
    task as tc,
};
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ROLE_PASSWORD: &str = "redis-task-events-secret";
const ADMIN_PASSWORD: &str = "redis-task-events-admin";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct PassthroughProjector;

impl TaskEventProjector for PassthroughProjector {
    fn project<'a>(
        &'a self,
        _task_event: &'a mut tc::TaskEvent,
    ) -> TaskEventFuture<'a, TaskEventProjection> {
        Box::pin(async { TaskEventProjection::Forward(None) })
    }

    fn after_forwarded(
        &self,
        _workflow_instance_id: Uuid,
        _task_id: Uuid,
        _patch: ParsedPatch,
    ) -> TaskEventFuture<'static, ()> {
        Box::pin(async {})
    }
}

#[derive(Default)]
struct OpenCapability {
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisTaskEventQuotaState>>,
}

impl RedisTaskEventCapability for OpenCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskEventError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskEventError> {
        if generation == 1 {
            Ok(())
        } else {
            Err(RedisTaskEventError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisTaskEventQuotaState) {
        lock(&self.quotas).push(state);
    }
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
            .prefix("redis-task-events-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-task-events-{}-{sequence}", std::process::id());
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
                 user task-events on >{ROLE_PASSWORD} ~tickr:{{{namespace}}}:task-events:* -@all \
                 +eval +get +hdel +hget +hincrby +hmget +hset +set +waitaof \
                 +xack +xadd +xautoclaim +xdel +xgroup|create +xrange +xreadgroup\n\
                 user task-events-admin on >{ADMIN_PASSWORD} ~* &* +@all\n"
            ),
        )
        .expect("write Redis fixture configuration");
        let mount = format!("{}:/tls:ro", path.display());
        run(
            Command::new("docker").args([
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
            "start Redis TaskEvent fixture",
        );
        let output = Command::new("docker")
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
        panic!("Redis TaskEvent fixture did not become ready");
    }

    fn client(&self) -> redis::Client {
        tls_client(self.port, "task-events", ROLE_PASSWORD, &self.trust_roots)
    }

    fn admin_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "task-events-admin",
            ADMIN_PASSWORD,
            &self.trust_roots,
        )
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        let _ = Command::new("docker")
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

fn config(namespace: &str, consumer: &str) -> RedisTaskEventsConfig {
    let mut config = RedisTaskEventsConfig::new(namespace, consumer);
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(256).unwrap();
    config.max_records = NonZeroUsize::new(8).unwrap();
    config.soft_limit_bytes = 450;
    config.hard_limit_bytes = 600;
    config.completion_retention = Duration::from_secs(60);
    config
}

async fn adapter(
    fixture: &RedisFixture,
    namespace: &str,
    consumer: &str,
    capability: Arc<OpenCapability>,
) -> RedisTaskEvents {
    RedisTaskEvents::connect(
        fixture.client(),
        config(namespace, consumer),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_task_event_laws_preserve_bytes_redelivery_and_pressure() {
    let namespace = format!("task-events-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenCapability::default());
    let first = adapter(&fixture, &namespace, "conductor-a", Arc::clone(&capability)).await;
    let first_payload = vec![1_u8; 64];
    let second_payload = vec![2_u8; 64];
    let third_payload = vec![3_u8; 64];

    assert_eq!(
        first
            .append("dispatch:1:assigned", first_payload.clone())
            .await,
        Ok(RedisTaskEventAcceptance::Appended)
    );
    assert_eq!(
        first
            .append("dispatch:1:assigned", first_payload.clone())
            .await,
        Ok(RedisTaskEventAcceptance::ReplayedPending)
    );
    assert_eq!(
        first
            .append("dispatch:1:assigned", b"conflict".to_vec())
            .await,
        Err(RedisTaskEventError::IdentityConflict)
    );
    assert_eq!(
        first
            .append("dispatch:1:started", second_payload.clone())
            .await,
        Ok(RedisTaskEventAcceptance::Appended)
    );
    let soft = first.quota_state().await.unwrap();
    assert_eq!(soft.used_bytes, 512);
    assert_eq!(soft.accepted_records, 2);
    assert_eq!(soft.pending_deliveries, 0);
    assert_eq!(
        first
            .append("dispatch:1:terminal", third_payload.clone())
            .await,
        Err(RedisTaskEventError::CapacityFenced)
    );

    assert_eq!(
        first.forward_one(|_| async { Err(()) }).await,
        Err(RedisTaskEventError::ForwardingUnavailable)
    );
    let relay_loss = first.quota_state().await.unwrap();
    assert_eq!(relay_loss.accepted_records, 2);
    assert_eq!(relay_loss.pending_deliveries, 1);

    drop(first);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second = adapter(&fixture, &namespace, "conductor-b", Arc::clone(&capability)).await;
    let relayed = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&relayed);
    assert_eq!(
        second
            .forward_one(move |payload| {
                lock(&observed).push(payload);
                async { Ok(()) }
            })
            .await,
        Ok(RedisTaskEventForwardOutcome::Forwarded)
    );
    assert_eq!(*lock(&relayed), vec![first_payload.clone()]);
    let released = second.quota_state().await.unwrap();
    assert_eq!(released.used_bytes, 256);
    assert_eq!(released.accepted_records, 1);
    assert_eq!(released.pending_deliveries, 0);
    assert_eq!(
        second
            .append("dispatch:1:terminal", third_payload.clone())
            .await,
        Ok(RedisTaskEventAcceptance::Appended)
    );

    let ambiguous = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&ambiguous);
    assert_eq!(
        second
            .forward_one(move |payload| {
                lock(&observed).push(payload);
                async { Err(()) }
            })
            .await,
        Err(RedisTaskEventError::ForwardingUnavailable)
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    let third = adapter(&fixture, &namespace, "conductor-c", Arc::clone(&capability)).await;
    let observed = Arc::clone(&ambiguous);
    assert_eq!(
        third
            .forward_one(move |payload| {
                lock(&observed).push(payload);
                async { Ok(()) }
            })
            .await,
        Ok(RedisTaskEventForwardOutcome::Forwarded)
    );
    let forwarded = lock(&ambiguous);
    assert_eq!(forwarded.len(), 2);
    assert_eq!(forwarded[0], forwarded[1]);
    drop(forwarded);

    assert_eq!(
        third.append("dispatch:1:started", second_payload).await,
        Ok(RedisTaskEventAcceptance::ReplayedCompleted)
    );
    assert!(lock(&capability.failures).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn production_interfaces_preserve_encoded_nonterminal_and_terminal_envelopes() {
    let namespace = format!(
        "task-events-production-{}",
        NEXT_REDIS.load(Ordering::Relaxed)
    );
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenCapability::default());
    let role = Arc::new(
        adapter(
            &fixture,
            &namespace,
            "production-conductor",
            Arc::clone(&capability),
        )
        .await,
    );
    let writer: Arc<dyn TaskEventWriter> = role.clone();
    let consumer: Arc<dyn TaskEventConsumer> = role;
    writer.prepare().await.unwrap();

    let task = DispatchedTask {
        task_instance_id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        workflow_instance_id: Uuid::new_v4(),
        workflow_id: Uuid::new_v4(),
        name: "production-task-event".to_owned(),
        nix_expression_path: "unused.nix".to_owned(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: Default::default(),
    };
    let executor_id = Uuid::new_v4();
    let nonterminal = encode_task_event(&task, executor_id, EmitKind::Started);
    writer
        .stage("dispatch.production.started", &nonterminal)
        .await
        .unwrap();

    let (closed_relay, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    drain_task_event_source(
        Arc::clone(&consumer),
        closed_relay,
        Arc::new(PassthroughProjector),
        tokio_util::sync::CancellationToken::new(),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(30)).await;
    let (relay, mut forwarded) = tokio::sync::mpsc::channel(2);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let drain = tokio::spawn(drain_task_event_source(
        consumer,
        relay,
        Arc::new(PassthroughProjector),
        cancellation.clone(),
    ));
    let redelivered = tokio::time::timeout(Duration::from_secs(2), forwarded.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(redelivered.payload, nonterminal);

    let terminal = encode_task_event(&task, executor_id, EmitKind::Failed);
    writer
        .stage("dispatch.production.terminal", &terminal)
        .await
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(2), forwarded.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.payload, terminal);
    cancellation.cancel();
    drain.await.unwrap();

    for _ in 0..100 {
        let settled = lock(&capability.quotas)
            .last()
            .is_some_and(|state| state.accepted_records == 0 && state.pending_deliveries == 0);
        if settled {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("forwarding completion did not settle Redis TaskEvents state");
}

fn spawn_child(
    fixture: &RedisFixture,
    namespace: &str,
    action: &str,
    stage: &str,
    phase: &Path,
    go: &Path,
    forwarded: &Path,
) -> std::process::Child {
    let _ = fs::remove_file(phase);
    let _ = fs::remove_file(go);
    Command::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "redis_task_event_process_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TICKR_REDIS_TASK_EVENT_CHILD", "1")
        .env("TICKR_REDIS_TASK_EVENT_PORT", fixture.port.to_string())
        .env("TICKR_REDIS_TASK_EVENT_ROOTS", &fixture.trust_roots)
        .env("TICKR_REDIS_TASK_EVENT_NAMESPACE", namespace)
        .env("TICKR_REDIS_TASK_EVENT_ACTION", action)
        .env("TICKR_REDIS_TASK_EVENT_STAGE", stage)
        .env("TICKR_REDIS_TASK_EVENT_PHASE", phase)
        .env("TICKR_REDIS_TASK_EVENT_GO", go)
        .env("TICKR_REDIS_TASK_EVENT_FORWARDED", forwarded)
        .spawn()
        .expect("spawn Redis TaskEvent owner process")
}

async fn await_phase(child: &mut std::process::Child, phase: &Path, expected: &str) {
    for _ in 0..400 {
        if fs::read_to_string(phase).is_ok_and(|value| value == expected) {
            return;
        }
        if let Some(status) = child.try_wait().expect("query child status") {
            panic!("Redis TaskEvent owner exited before {expected}: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Redis TaskEvent owner did not reach {expected}");
}

async fn await_stream_record(
    child: &mut std::process::Child,
    connection: &mut redis::aio::MultiplexedConnection,
    stream: &str,
) {
    for _ in 0..400 {
        let length: u64 = redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut *connection)
            .await
            .unwrap();
        if length > 0 {
            return;
        }
        if let Some(status) = child.try_wait().expect("query child status") {
            panic!("Redis TaskEvent owner exited before append was observed: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Redis TaskEvent append was not observed");
}

fn kill_child(child: &mut std::process::Child) {
    child.kill().expect("crash Redis TaskEvent owner");
    let _ = child.wait().expect("reap Redis TaskEvent owner");
}

async fn wait_for_go(path: &Path) {
    for _ in 0..2400 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("parent did not release Redis TaskEvent child");
}

#[test]
#[ignore = "spawned by redis_task_event_real_process_crash_boundaries"]
fn redis_task_event_process_child() {
    if std::env::var_os("TICKR_REDIS_TASK_EVENT_CHILD").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let port = std::env::var("TICKR_REDIS_TASK_EVENT_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let roots = std::env::var("TICKR_REDIS_TASK_EVENT_ROOTS").unwrap();
        let namespace = std::env::var("TICKR_REDIS_TASK_EVENT_NAMESPACE").unwrap();
        let action = std::env::var("TICKR_REDIS_TASK_EVENT_ACTION").unwrap();
        let stage = std::env::var("TICKR_REDIS_TASK_EVENT_STAGE").unwrap();
        let phase = PathBuf::from(std::env::var("TICKR_REDIS_TASK_EVENT_PHASE").unwrap());
        let go = PathBuf::from(std::env::var("TICKR_REDIS_TASK_EVENT_GO").unwrap());
        let forwarded = PathBuf::from(std::env::var("TICKR_REDIS_TASK_EVENT_FORWARDED").unwrap());
        let capability = Arc::new(OpenCapability::default());
        let adapter = RedisTaskEvents::connect(
            tls_client(port, "task-events", ROLE_PASSWORD, &roots),
            config(&namespace, &format!("child-{}", std::process::id())),
            RedisDurabilityGuard::new(Duration::from_secs(30), Duration::from_secs(30)),
            capability,
        )
        .await
        .unwrap();
        fs::write(&phase, "connected").unwrap();
        wait_for_go(&go).await;

        if action == "append" {
            if stage == "before-append" {
                fs::write(&phase, "before-append").unwrap();
                tokio::time::sleep(Duration::from_secs(60)).await;
                return;
            }
            let acceptance = adapter
                .append("crash:task-event", b"durable-task-event".to_vec())
                .await
                .unwrap();
            fs::write(&phase, "proved").unwrap();
            if stage == "after-proof" {
                tokio::time::sleep(Duration::from_secs(60)).await;
                return;
            }
            fs::write(&phase, format!("replied:{acceptance:?}")).unwrap();
            if stage == "after-reply" {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            return;
        }

        let outcome = adapter
            .forward_one(|payload| async {
                if stage == "before-relay" {
                    fs::write(&phase, "before-relay").unwrap();
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                fs::write(&forwarded, payload).unwrap();
                fs::write(&phase, "relayed").unwrap();
                if stage == "after-relay" {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(outcome, RedisTaskEventForwardOutcome::Forwarded);
        fs::write(&phase, "completed").unwrap();
        if stage == "after-completion" {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_task_event_real_process_crash_boundaries() {
    let namespace = format!("task-events-crash-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let temp = tempfile::tempdir().unwrap();
    let phase = temp.path().join("phase");
    let go = temp.path().join("go");
    let forwarded = temp.path().join("forwarded");
    let stream = format!("tickr:{{{namespace}}}:task-events:stream");
    let mut admin = fixture
        .admin_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    let mut before_append = spawn_child(
        &fixture,
        &namespace,
        "append",
        "before-append",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut before_append, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut before_append, &phase, "before-append").await;
    kill_child(&mut before_append);
    let length: u64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(length, 0);

    let mut after_append = spawn_child(
        &fixture, &namespace, "append", "run", &phase, &go, &forwarded,
    );
    await_phase(&mut after_append, &phase, "connected").await;
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("no")
        .query_async(&mut admin)
        .await
        .unwrap();
    fs::write(&go, "go").unwrap();
    await_stream_record(&mut after_append, &mut admin, &stream).await;
    kill_child(&mut after_append);
    assert_eq!(
        fs::read_to_string(&phase).unwrap(),
        "connected",
        "producer reply crossed an unproved fsync"
    );
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("always")
        .query_async(&mut admin)
        .await
        .unwrap();

    let mut after_proof = spawn_child(
        &fixture,
        &namespace,
        "append",
        "after-proof",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut after_proof, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut after_proof, &phase, "proved").await;
    kill_child(&mut after_proof);

    let mut after_reply = spawn_child(
        &fixture,
        &namespace,
        "append",
        "after-reply",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut after_reply, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut after_reply, &phase, "replied:ReplayedPending").await;
    kill_child(&mut after_reply);

    let mut before_relay = spawn_child(
        &fixture,
        &namespace,
        "forward",
        "before-relay",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut before_relay, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut before_relay, &phase, "before-relay").await;
    kill_child(&mut before_relay);
    assert!(!forwarded.exists());

    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut after_relay = spawn_child(
        &fixture,
        &namespace,
        "forward",
        "after-relay",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut after_relay, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut after_relay, &phase, "relayed").await;
    kill_child(&mut after_relay);
    assert_eq!(fs::read(&forwarded).unwrap(), b"durable-task-event");
    let length: u64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        length, 1,
        "relay forwarding alone must not complete the source"
    );

    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut after_completion = spawn_child(
        &fixture,
        &namespace,
        "forward",
        "after-completion",
        &phase,
        &go,
        &forwarded,
    );
    await_phase(&mut after_completion, &phase, "connected").await;
    fs::write(&go, "go").unwrap();
    await_phase(&mut after_completion, &phase, "completed").await;
    kill_child(&mut after_completion);
    let length: u64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(length, 0);

    let recovered = adapter(
        &fixture,
        &namespace,
        "parent-recovery",
        Arc::new(OpenCapability::default()),
    )
    .await;
    let quota = recovered.quota_state().await.unwrap();
    assert_eq!(quota.used_bytes, 0);
    assert_eq!(quota.accepted_records, 0);
    assert_eq!(quota.pending_deliveries, 0);
    assert_eq!(
        recovered
            .append("crash:task-event", b"durable-task-event".to_vec())
            .await,
        Ok(RedisTaskEventAcceptance::ReplayedCompleted)
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
                "/CN=Tickr Redis TaskEvent Test CA",
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
