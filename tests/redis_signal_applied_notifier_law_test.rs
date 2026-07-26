#![cfg(not(madsim))]

use std::{
    collections::HashMap,
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

use redis::{ConnectionInfo, TlsCertificates};
use sqlx::sqlite::SqlitePoolOptions;
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_signal_applied_notifier::{
        RedisSignalAppliedNotifierCapability, RedisSignalAppliedNotifierConfig,
        RedisSignalAppliedNotifierQuotaState, RedisSignalAppliedNotifierRole,
        RedisSignalAppliedPublishOutcome,
    },
};
use tickr_conductor::{
    cancel_pipeline::{
        process_cancel_with_notifications, CancelOutcome, CancelRequest, CancelTargetBody,
    },
    signal_applied_notifier::{SignalAppliedNotifier, SignalAppliedReconciliationWake},
};
use tickr_migrations::{
    apply_sqlite,
    backend::{RepositoryFactory, WriterRepositoryBundle},
    sqlite_writer_options, MigrationTarget,
};
use tickr_proto::{codec::signal::decode_signal, config::DataPlaneSql, ConductorRelayMessage};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ROLE_PASSWORD: &str = "redis-signal-applied-secret";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct TestCapability {
    open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisSignalAppliedNotifierQuotaState>>,
}

impl Default for TestCapability {
    fn default() -> Self {
        Self {
            open: AtomicBool::new(true),
            failures: Mutex::new(Vec::new()),
            quotas: Mutex::new(Vec::new()),
        }
    }
}

impl RedisSignalAppliedNotifierCapability for TestCapability {
    fn delivery_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisSignalAppliedNotifierQuotaState) {
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
            .prefix("redis-signal-applied-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-signal-applied-{}-{sequence}",
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
                 user default on >redis-signal-applied-aof-loader ~* &* +@all\n\
                 user signal-applied on >{ROLE_PASSWORD} ~tickr:{{{namespace}}}:signal-applied-notifier:* &tickr:{{{namespace}}}:signal-applied-notifier:materialized:* -@all \
                 +eval +hdel +hget +hincrby +hset +psubscribe +publish +time +zadd +zrem +zrangebyscore\n"
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
            "start Redis SignalAppliedNotifier fixture",
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

    fn client(&self) -> redis::Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let connection = format!(
            "rediss://signal-applied:{ROLE_PASSWORD}@localhost:{}/",
            self.port
        )
        .parse::<ConnectionInfo>()
        .expect("Redis role connection");
        redis::Client::build_with_tls(
            connection,
            TlsCertificates {
                client_tls: None,
                root_cert: Some(self.trust_roots.as_bytes().to_vec()),
            },
        )
        .expect("Redis role client")
    }

    async fn restart(&mut self) {
        run(
            Command::new("docker").args(["restart", &self.name]),
            "restart Redis SignalAppliedNotifier fixture",
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
        "Redis SignalAppliedNotifier fixture did not become ready: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[derive(Clone, Copy)]
enum HintMode {
    Suppressed,
    Duplicated,
}

async fn exercise_durable_cancel(
    writer: Arc<WriterRepositoryBundle>,
    role: RedisSignalAppliedNotifierRole,
    notifications: Arc<
        AsyncMutex<tickr::redis_signal_applied_notifier::RedisSignalAppliedNotificationStream>,
    >,
    mode: HintMode,
) {
    let (relay_tx, mut relay_rx) = mpsc::channel::<ConductorRelayMessage>(1);
    tickr_conductor::relay::init_relay_tx(relay_tx).await;
    let materialization_repository = Arc::clone(&writer);
    let materialize = tokio::spawn(async move {
        let message = tokio::time::timeout(Duration::from_secs(2), relay_rx.recv())
            .await
            .expect("cancel reaches relay")
            .expect("relay remains open");
        let signal = decode_signal(&message.payload).expect("decode Cancel Signal");
        let signal_id = Uuid::parse_str(&signal.signal_id).expect("Signal identity is a UUID");
        assert!(tickr_conductor::signal_cancels::materialize(
            materialization_repository.as_ref(),
            signal_id,
            7,
        )
        .await
        .expect("persist materialization"));
        if matches!(mode, HintMode::Duplicated) {
            assert_eq!(
                role.publish(signal_id).await,
                Ok(RedisSignalAppliedPublishOutcome::Published)
            );
            assert!(matches!(
                role.publish(signal_id).await,
                Ok(RedisSignalAppliedPublishOutcome::Published
                    | RedisSignalAppliedPublishOutcome::Coalesced)
            ));
        }
        signal_id
    });

    let started = tokio::time::Instant::now();
    let outcome = process_cancel_with_notifications(
        writer.as_ref(),
        notifications.as_ref(),
        CancelRequest {
            target: CancelTargetBody::ByTag {
                filter: HashMap::from([("env".to_owned(), "prod".to_owned())]),
            },
            note: Some("Redis notifier role law".to_owned()),
            idempotency_key: None,
        },
    )
    .await
    .expect("ByTag cancellation converges");
    let signal_id = materialize.await.expect("materialization worker");
    match outcome {
        CancelOutcome::ByTag {
            signal_id: outcome_id,
            instances_matched,
        } => {
            assert_eq!(outcome_id, signal_id);
            assert_eq!(instances_matched, 7);
        }
        _ => panic!("Redis notifier returned the wrong cancel outcome"),
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        tickr_conductor::signal_cancels::materialized_count(writer.as_ref(), signal_id)
            .await
            .expect("read durable materialization"),
        Some(7)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_signal_notifier_laws_cover_acl_pressure_restart_and_reconciliation() {
    let namespace = format!("signal-applied-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let mut fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(TestCapability::default());
    let mut config = RedisSignalAppliedNotifierConfig::new(&namespace);
    config.hint_ttl = Duration::from_millis(100);
    config.sweep_interval = Duration::from_millis(10);
    config.soft_hint_limit = NonZeroUsize::new(1).unwrap();
    config.hard_hint_limit = NonZeroUsize::new(2).unwrap();
    let role = RedisSignalAppliedNotifierRole::connect(
        fixture.client(),
        config.clone(),
        capability.clone(),
    )
    .await
    .unwrap();
    let mut notifications = role.subscribe().await.unwrap();

    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let omitted = Uuid::new_v4();
    assert_eq!(
        role.publish(first).await,
        Ok(RedisSignalAppliedPublishOutcome::Published)
    );
    assert_eq!(
        role.publish(first).await,
        Ok(RedisSignalAppliedPublishOutcome::Coalesced)
    );
    assert_eq!(
        role.publish(second).await,
        Ok(RedisSignalAppliedPublishOutcome::Published)
    );
    assert_eq!(
        role.publish(omitted).await,
        Ok(RedisSignalAppliedPublishOutcome::OmittedAtHardLimit)
    );
    let pressure = role.quota_state().await.unwrap();
    assert_eq!(pressure.admitted_hints, 2);
    assert_eq!(pressure.coalesced_hints, 1);
    assert_eq!(pressure.omitted_hints, 1);
    assert_eq!(pressure.pressure, RedisQuotaPressure::HardLimit);

    assert_eq!(
        notifications
            .next_reconciliation(Duration::from_secs(1))
            .await,
        SignalAppliedReconciliationWake::Notification(
            tickr_conductor::signal_applied_notifier::ByTagCancelMaterialization {
                signal_id: first,
            }
        )
    );
    assert_eq!(
        notifications
            .next_reconciliation(Duration::from_secs(1))
            .await,
        SignalAppliedReconciliationWake::Notification(
            tickr_conductor::signal_applied_notifier::ByTagCancelMaterialization {
                signal_id: second,
            }
        )
    );
    assert_eq!(role.quota_state().await.unwrap().admitted_hints, 0);

    assert_eq!(
        role.publish(omitted).await,
        Ok(RedisSignalAppliedPublishOutcome::Published)
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    role.sweep_expired().await.unwrap();
    let expired = role.quota_state().await.unwrap();
    assert_eq!(expired.admitted_hints, 0);
    assert!(expired.expired_hints >= 1);

    capability.open.store(false, Ordering::Release);
    assert_eq!(
        role.publish(Uuid::new_v4()).await,
        Ok(RedisSignalAppliedPublishOutcome::SuppressedByCapabilityFence)
    );
    capability.open.store(true, Ordering::Release);

    let role_client = fixture.client();
    let mut restricted = role_client
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let unregistered = redis::cmd("SET")
        .arg(format!(
            "tickr:{{{namespace}}}:signal-applied-notifier:unregistered"
        ))
        .arg("value")
        .query_async::<redis::Value>(&mut restricted)
        .await;
    assert!(unregistered.is_err());
    let cross_role = redis::cmd("PUBLISH")
        .arg(format!(
            "tickr:{{{namespace}}}:task-cancellation:notification"
        ))
        .arg("value")
        .query_async::<redis::Value>(&mut restricted)
        .await;
    assert!(cross_role.is_err());

    let restart_lost = Uuid::new_v4();
    assert_eq!(
        role.publish(restart_lost).await,
        Ok(RedisSignalAppliedPublishOutcome::Published)
    );
    drop(notifications);
    fixture.restart().await;
    let recovered =
        RedisSignalAppliedNotifierRole::connect(fixture.client(), config, capability.clone())
            .await
            .unwrap();
    let mut recovered_notifications = recovered.subscribe().await.unwrap();
    assert_eq!(
        recovered_notifications
            .next_reconciliation(Duration::from_millis(30))
            .await,
        SignalAppliedReconciliationWake::Deadline
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    recovered.sweep_expired().await.unwrap();
    assert_eq!(recovered.quota_state().await.unwrap().admitted_hints, 0);

    let (notifier, publisher) = recovered.bounded_notifier(NonZeroUsize::new(1).unwrap());
    let local_hint = Uuid::new_v4();
    notifier.notify_bytag_cancel_materialized(local_hint);
    notifier.notify_bytag_cancel_materialized(Uuid::new_v4());
    let publisher_cancel = CancellationToken::new();
    let publisher_task = tokio::spawn(publisher.run(publisher_cancel.clone()));
    assert_eq!(
        recovered_notifications
            .next_reconciliation(Duration::from_secs(1))
            .await,
        SignalAppliedReconciliationWake::Notification(
            tickr_conductor::signal_applied_notifier::ByTagCancelMaterialization {
                signal_id: local_hint,
            }
        )
    );
    publisher_cancel.cancel();
    publisher_task.await.unwrap();

    let directory = tempfile::TempDir::new().unwrap();
    let url = format!(
        "sqlite://{}",
        directory.path().join("signal-applied.db").display()
    );
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;
    let writer = Arc::new(
        RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap(),
    );
    let notifications = Arc::new(AsyncMutex::new(recovered_notifications));
    exercise_durable_cancel(
        Arc::clone(&writer),
        recovered.clone(),
        Arc::clone(&notifications),
        HintMode::Suppressed,
    )
    .await;
    exercise_durable_cancel(
        Arc::clone(&writer),
        recovered,
        notifications,
        HintMode::Duplicated,
    )
    .await;
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
                "/CN=Tickr Redis SignalAppliedNotifier Test CA",
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
