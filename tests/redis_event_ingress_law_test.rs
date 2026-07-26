#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use redis::{ConnectionInfo, TlsCertificates};
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqlitePoolOptions;
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_durability::RedisDurabilityGuard,
    redis_event_ingress::{
        RedisEventIngress, RedisEventIngressAcceptance, RedisEventIngressCapability,
        RedisEventIngressConfig, RedisEventIngressError, RedisEventIngressQuotaState,
    },
    redis_ingress_idempotency::{
        RedisIngressIdempotencyCapability, RedisIngressIdempotencyConfig,
        RedisIngressIdempotencyError, RedisIngressIdempotencyQuotaState,
        RedisIngressIdempotencyStore, RedisIngressReservationOutcome,
    },
};
use tickr_conductor::{
    ingress_idempotency::IngressCoordinator,
    nats_ingress::{run_event_consumer, IngressWorkingSet, RelaySendOutcome, RelaySender},
    trigger_pipeline::{ReservedTriggerEffects, TriggerError, TriggerRequest},
    wakeup_translator::{WakeupOutcome, WakeupRelaySender, WakeupRequest},
};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
use tickr_proto::{config::DataPlaneSql, signal as sp};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const EVENT_PASSWORD: &str = "redis-event-ingress-secret";
const IDEMPOTENCY_PASSWORD: &str = "redis-ingress-idempotency-secret";
const ADMIN_PASSWORD: &str = "redis-ingress-admin";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct OpenEventCapability {
    open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisEventIngressQuotaState>>,
}

impl Default for OpenEventCapability {
    fn default() -> Self {
        Self {
            open: AtomicBool::new(true),
            failures: Mutex::new(Vec::new()),
            quotas: Mutex::new(Vec::new()),
        }
    }
}

impl RedisEventIngressCapability for OpenEventCapability {
    fn guard_admission(&self) -> Result<u64, RedisEventIngressError> {
        if self.open.load(Ordering::SeqCst) {
            Ok(1)
        } else {
            Err(RedisEventIngressError::Unavailable)
        }
    }
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisEventIngressError> {
        if self.open.load(Ordering::SeqCst) && generation == 1 {
            Ok(())
        } else {
            Err(RedisEventIngressError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisEventIngressQuotaState) {
        lock(&self.quotas).push(state);
    }
}

#[derive(Default)]
struct OpenIdempotencyCapability {
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisIngressIdempotencyQuotaState>>,
}

impl RedisIngressIdempotencyCapability for OpenIdempotencyCapability {
    fn guard_admission(&self) -> Result<u64, RedisIngressIdempotencyError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisIngressIdempotencyError> {
        if generation == 1 {
            Ok(())
        } else {
            Err(RedisIngressIdempotencyError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisIngressIdempotencyQuotaState) {
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
            .prefix("redis-event-ingress-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-event-ingress-{}-{sequence}",
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
                 user event-ingress on >{EVENT_PASSWORD} ~tickr:{{{namespace}}}:event-ingress:* -@all \
                 +eval +hdel +hget +hincrby +hmget +hset +waitaof +xack +xadd +xautoclaim \
                 +xdel +xgroup|create +xrange +xreadgroup\n\
                 user ingress-idempotency on >{IDEMPOTENCY_PASSWORD} ~tickr:{{{namespace}}}:ingress-idempotency-store:* -@all \
                 +del +eval +hget +hincrby +hmget +hset +time +waitaof\n\
                 user ingress-admin on >{ADMIN_PASSWORD} ~* &* +@all\n"
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
            "start Redis Event ingress fixture",
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
        panic!("Redis Event ingress fixture did not become ready");
    }

    fn event_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "event-ingress",
            EVENT_PASSWORD,
            &self.trust_roots,
        )
    }

    fn idempotency_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "ingress-idempotency",
            IDEMPOTENCY_PASSWORD,
            &self.trust_roots,
        )
    }

    fn admin_client(&self) -> redis::Client {
        tls_client(
            self.port,
            "ingress-admin",
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

fn event_config(namespace: &str, consumer: &str) -> RedisEventIngressConfig {
    let mut config = RedisEventIngressConfig::new(namespace, consumer);
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(256).unwrap();
    config.max_producer_key_bytes = NonZeroUsize::new(64).unwrap();
    config.max_deliveries = NonZeroUsize::new(8).unwrap();
    config.soft_limit_bytes = 300;
    config.hard_limit_bytes = 1000;
    config
}

fn idempotency_config(namespace: &str) -> RedisIngressIdempotencyConfig {
    let mut config = RedisIngressIdempotencyConfig::new(namespace);
    config.claim_lease = Duration::from_millis(20);
    config.terminal_retention = Duration::from_millis(20);
    config.max_producer_records = NonZeroUsize::new(8).unwrap();
    config.max_effect_bytes = NonZeroUsize::new(256).unwrap();
    config.max_result_bytes = NonZeroUsize::new(256).unwrap();
    config.max_intent_bytes = NonZeroUsize::new(1024).unwrap();
    config.soft_limit_bytes = 700;
    config.hard_limit_bytes = 4096;
    config
}

async fn event_adapter(
    fixture: &RedisFixture,
    namespace: &str,
    consumer: &str,
    capability: Arc<OpenEventCapability>,
) -> RedisEventIngress {
    RedisEventIngress::connect(
        fixture.event_client(),
        event_config(namespace, consumer),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap()
}

async fn idempotency_adapter(
    fixture: &RedisFixture,
    namespace: &str,
    capability: Arc<OpenIdempotencyCapability>,
) -> RedisIngressIdempotencyStore {
    RedisIngressIdempotencyStore::connect(
        fixture.idempotency_client(),
        idempotency_config(namespace),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap()
}

fn payload_hash(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[derive(Default)]
struct RecordingIngressWorkingSet {
    trigger_calls: AtomicU64,
}

#[async_trait]
impl IngressWorkingSet for RecordingIngressWorkingSet {
    async fn process_trigger(
        &self,
        _repositories: &WriterRepositoryBundle,
        request: TriggerRequest,
        signal_id: Uuid,
    ) -> Result<ReservedTriggerEffects, TriggerError> {
        self.trigger_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReservedTriggerEffects {
            signal: sp::Signal {
                signal_id: signal_id.to_string(),
                idempotency_key: request.idempotency_key,
                variant: None,
            },
            event_results: br#"{"capture":"persisted"}"#.to_vec(),
        })
    }

    async fn process_wakeup(
        &self,
        _repositories: &WriterRepositoryBundle,
        _sender: &dyn WakeupRelaySender,
        _request: WakeupRequest,
        _signal_id: Uuid,
    ) -> anyhow::Result<WakeupOutcome> {
        Err(anyhow!("unexpected wakeup in Trigger ingress law"))
    }
}

#[derive(Default)]
struct RecordingIngressRelay {
    sent: AtomicU64,
}

#[async_trait]
impl RelaySender for RecordingIngressRelay {
    async fn try_send(&self, _signal: &sp::Signal) -> RelaySendOutcome {
        self.sent.fetch_add(1, Ordering::SeqCst);
        RelaySendOutcome::Sent
    }
}

async fn open_test_writer() -> (tempfile::TempDir, Arc<WriterRepositoryBundle>) {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}",
        directory.path().join("event-ingress.db").display()
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
    let writer = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
        .open_writer()
        .await
        .unwrap();
    (directory, Arc::new(writer))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_delivery_crosses_the_production_consumer_once() {
    let namespace = format!(
        "event-ingress-production-{}",
        NEXT_REDIS.load(Ordering::Relaxed)
    );
    let fixture = RedisFixture::start(&namespace).await;
    let event_ingress = Arc::new(
        event_adapter(
            &fixture,
            &namespace,
            "production-consumer",
            Arc::new(OpenEventCapability::default()),
        )
        .await,
    );
    let producer_store = Arc::new(
        idempotency_adapter(
            &fixture,
            &namespace,
            Arc::new(OpenIdempotencyCapability::default()),
        )
        .await,
    );
    let ingress_coordinator = IngressCoordinator::new(producer_store.clone());
    let producer_key = "producer:production";
    let payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": producer_key,
        "workflow_id": Uuid::new_v4(),
    }))
    .unwrap();
    event_ingress
        .append("transport:production", producer_key, payload.clone())
        .await
        .unwrap();

    let (_directory, repositories) = open_test_writer().await;
    let working_set = Arc::new(RecordingIngressWorkingSet::default());
    let relay = Arc::new(RecordingIngressRelay::default());
    let shutdown = CancellationToken::new();
    let consumer = tokio::spawn(run_event_consumer(
        event_ingress.clone(),
        ingress_coordinator,
        repositories,
        working_set.clone(),
        relay.clone(),
        shutdown.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if event_ingress
                .quota_state()
                .await
                .is_ok_and(|quota| quota.pending_deliveries == 0 && quota.accepted_deliveries == 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("production consumer did not complete the Redis delivery");
    shutdown.cancel();
    consumer.await.unwrap().unwrap();

    assert_eq!(working_set.trigger_calls.load(Ordering::SeqCst), 1);
    assert_eq!(relay.sent.load(Ordering::SeqCst), 1);
    let canonical_payload: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    let hash = tickr_conductor::canonical_json::hash(Some(&canonical_payload));
    assert!(matches!(
        producer_store.reserve(producer_key, &hash).await.unwrap(),
        RedisIngressReservationOutcome::Complete(proof)
            if proof.outcome() == tickr::redis_ingress_idempotency::RedisIngressTerminalOutcome::Accepted
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_event_ingress_laws_cover_replay_reclaim_pressure_rejection_and_ack() {
    let namespace = format!("event-ingress-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let event_capability = Arc::new(OpenEventCapability::default());
    let idempotency_capability = Arc::new(OpenIdempotencyCapability::default());
    let first_event = event_adapter(
        &fixture,
        &namespace,
        "conductor-a",
        Arc::clone(&event_capability),
    )
    .await;
    let first_store =
        idempotency_adapter(&fixture, &namespace, Arc::clone(&idempotency_capability)).await;

    let payload = br#"{"v":1,"variant":"trigger"}"#.to_vec();
    let hash = payload_hash(&payload);
    let (accepted, stream_id) = first_event
        .append("transport:1", "producer:1", payload.clone())
        .await
        .unwrap();
    assert_eq!(accepted, RedisEventIngressAcceptance::Appended);
    let (replayed, replayed_stream_id) = first_event
        .append("transport:1", "producer:1", payload.clone())
        .await
        .unwrap();
    assert_eq!(replayed, RedisEventIngressAcceptance::ReplayedPending);
    assert_eq!(replayed_stream_id, stream_id);

    let delivery = first_event.next_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.stream_id, stream_id);
    assert_eq!(delivery.producer_key, "producer:1");
    assert_eq!(delivery.payload, payload);
    let mut reservation = match first_store.reserve("producer:1", &hash).await.unwrap() {
        RedisIngressReservationOutcome::Acquired(reservation) => reservation,
        _ => panic!("first producer claim was not acquired"),
    };
    let signal_id = reservation.signal_id();
    match first_store.reserve("producer:1", &hash).await.unwrap() {
        RedisIngressReservationOutcome::Pending => {}
        RedisIngressReservationOutcome::Acquired(reclaimed) => {
            assert_eq!(reclaimed.signal_id(), signal_id);
            reservation = reclaimed;
        }
        _ => panic!("duplicate producer claim did not remain pending or reclaim"),
    }
    let effects = reservation
        .persist_effects(
            b"stable-signal-effect".to_vec(),
            b"event-variable-and-capture-result".to_vec(),
            vec![b"relay-intent-1".to_vec(), b"relay-intent-2".to_vec()],
        )
        .await
        .unwrap();
    assert_eq!(effects.relay_intents.len(), 2);

    let before_ack = first_event.quota_state().await.unwrap();
    assert_eq!(before_ack.delivery_records, 1);
    assert_eq!(before_ack.pending_deliveries, 1);
    drop(first_event);
    drop(first_store);

    tokio::time::sleep(Duration::from_millis(30)).await;
    let recovered_event = event_adapter(
        &fixture,
        &namespace,
        "conductor-b",
        Arc::clone(&event_capability),
    )
    .await;
    let recovered_store =
        idempotency_adapter(&fixture, &namespace, Arc::clone(&idempotency_capability)).await;
    let recovered_delivery = recovered_event.next_delivery().await.unwrap().unwrap();
    assert_eq!(recovered_delivery.stream_id, stream_id);
    let (operation, recovered_effects) =
        match recovered_store.reserve("producer:1", &hash).await.unwrap() {
            RedisIngressReservationOutcome::Ready(operation, effects) => (operation, effects),
            _ => panic!("durable effects were not recovered"),
        };
    assert_eq!(recovered_effects, effects);
    assert_eq!(
        recovered_store.quota_state().await.unwrap().effect_records,
        1,
        "recovery must not emit a second Signal effect"
    );
    let proof = operation.mark_relayed().await.unwrap();
    assert!(!recovered_store
        .cleanup_terminal("producer:1", &hash)
        .await
        .unwrap());
    event_capability.open.store(false, Ordering::SeqCst);
    assert_eq!(
        recovered_event.complete(&recovered_delivery, &proof).await,
        Err(RedisEventIngressError::Unavailable)
    );
    let capability_loss = recovered_event.quota_state().await.unwrap();
    assert_eq!(capability_loss.delivery_records, 1);
    assert_eq!(capability_loss.pending_deliveries, 1);
    event_capability.open.store(true, Ordering::SeqCst);
    recovered_event
        .complete(&recovered_delivery, &proof)
        .await
        .unwrap();
    let after_ack = recovered_event.quota_state().await.unwrap();
    assert_eq!(after_ack.delivery_records, 0);
    assert_eq!(after_ack.pending_deliveries, 0);
    assert_eq!(after_ack.accepted_deliveries, 1);
    assert!(matches!(
        recovered_store.reserve("producer:1", &hash).await.unwrap(),
        RedisIngressReservationOutcome::Complete(_)
    ));

    let conflicting_payload = br#"{"v":1,"variant":"cancel"}"#.to_vec();
    let conflicting_hash = payload_hash(&conflicting_payload);
    recovered_event
        .append("transport:2", "producer:1", conflicting_payload)
        .await
        .unwrap();
    let conflicting_delivery = recovered_event.next_delivery().await.unwrap().unwrap();
    let conflict = match recovered_store
        .reserve("producer:1", &conflicting_hash)
        .await
        .unwrap()
    {
        RedisIngressReservationOutcome::Conflict(conflict) => conflict,
        _ => panic!("different payload hash did not conflict"),
    };
    assert_eq!(conflict.original_signal_id, signal_id);
    recovered_event
        .complete(&conflicting_delivery, &conflict.proof)
        .await
        .unwrap();
    assert_eq!(
        recovered_event
            .quota_state()
            .await
            .unwrap()
            .rejected_deliveries,
        1
    );

    let rejected_payload = br#"{"v":999}"#.to_vec();
    let rejected_hash = payload_hash(&rejected_payload);
    recovered_event
        .append("transport:3", "producer:3", rejected_payload)
        .await
        .unwrap();
    let rejected_delivery = recovered_event.next_delivery().await.unwrap().unwrap();
    let rejected_reservation = match recovered_store
        .reserve("producer:3", &rejected_hash)
        .await
        .unwrap()
    {
        RedisIngressReservationOutcome::Acquired(reservation) => reservation,
        _ => panic!("permanent-rejection reservation was not acquired"),
    };
    let rejection_proof = rejected_reservation
        .reject("unsupported envelope version")
        .await
        .unwrap();
    recovered_event
        .complete(&rejected_delivery, &rejection_proof)
        .await
        .unwrap();
    assert_eq!(
        recovered_store
            .quota_state()
            .await
            .unwrap()
            .rejection_records,
        1
    );

    let malformed_payload = b"not-json".to_vec();
    recovered_event
        .append("transport:4", "producer:4", malformed_payload)
        .await
        .unwrap();
    let malformed_delivery = recovered_event.next_delivery().await.unwrap().unwrap();
    recovered_event
        .complete_permanent_rejection(&malformed_delivery, "malformed Event envelope")
        .await
        .unwrap();
    assert_eq!(
        recovered_event
            .quota_state()
            .await
            .unwrap()
            .rejected_deliveries,
        3
    );

    let reclaim_payload = payload_hash(b"reclaim");
    let first_reclaim = match recovered_store
        .reserve("producer:reclaim", &reclaim_payload)
        .await
        .unwrap()
    {
        RedisIngressReservationOutcome::Acquired(reservation) => reservation,
        _ => panic!("reclaim reservation was not acquired"),
    };
    let reclaim_signal = first_reclaim.signal_id();
    first_reclaim.abandon().await.unwrap();
    let reclaimed = match recovered_store
        .reserve("producer:reclaim", &reclaim_payload)
        .await
        .unwrap()
    {
        RedisIngressReservationOutcome::Acquired(reservation) => reservation,
        _ => panic!("abandoned reservation was not reclaimed"),
    };
    assert_eq!(reclaimed.signal_id(), reclaim_signal);

    let pressure_payload = vec![7_u8; 200];
    assert!(recovered_event
        .append("pressure:1", "pressure-key", pressure_payload.clone())
        .await
        .is_ok());
    assert!(recovered_event
        .append("pressure:2", "pressure-key", pressure_payload.clone())
        .await
        .is_ok());
    assert_eq!(
        recovered_event
            .append("pressure:3", "pressure-key", pressure_payload)
            .await,
        Err(RedisEventIngressError::CapacityFenced)
    );
    let pressure = recovered_event.quota_state().await.unwrap();
    assert_eq!(pressure.delivery_records, 2);
    assert!(pressure.used_bytes >= pressure.soft_limit_bytes);

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(recovered_store
        .cleanup_terminal("producer:1", &hash)
        .await
        .unwrap());
    assert!(lock(&event_capability.failures).is_empty());
    assert!(lock(&idempotency_capability.failures).is_empty());

    let mut event_connection = fixture
        .event_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let cross_role: redis::RedisResult<Option<String>> = redis::cmd("HGET")
        .arg(format!(
            "tickr:{{{namespace}}}:ingress-idempotency-store:quota"
        ))
        .arg("used_bytes")
        .query_async(&mut event_connection)
        .await;
    assert!(
        cross_role.is_err(),
        "EventIngress credential crossed its role namespace"
    );

    let mut idempotency_connection = fixture
        .idempotency_client()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();
    let cross_role: redis::RedisResult<Option<String>> = redis::cmd("HGET")
        .arg(format!("tickr:{{{namespace}}}:event-ingress:quota"))
        .arg("used_bytes")
        .query_async(&mut idempotency_connection)
        .await;
    assert!(
        cross_role.is_err(),
        "IngressIdempotencyStore credential crossed its role namespace"
    );

    let _ = fixture.admin_client();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_ingress_idempotency_hard_limit_preserves_accepted_reservation() {
    let namespace = format!(
        "event-ingress-pressure-{}",
        NEXT_REDIS.load(Ordering::Relaxed)
    );
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenIdempotencyCapability::default());
    let mut config = idempotency_config(&namespace);
    config.max_producer_records = NonZeroUsize::new(1).unwrap();
    config.soft_limit_bytes = 400;
    config.hard_limit_bytes = 700;
    let store = RedisIngressIdempotencyStore::connect(
        fixture.idempotency_client(),
        config,
        RedisDurabilityGuard::default(),
        Arc::clone(&capability) as Arc<dyn RedisIngressIdempotencyCapability>,
    )
    .await
    .unwrap();
    let first_hash = payload_hash(b"first");
    let first_signal = match store.reserve("first", &first_hash).await.unwrap() {
        RedisIngressReservationOutcome::Acquired(reservation) => reservation.signal_id(),
        _ => panic!("first reservation was not acquired"),
    };
    assert!(matches!(
        store.reserve("second", &payload_hash(b"second")).await,
        Err(RedisIngressIdempotencyError::CapacityFenced)
    ));
    let replay = store.reserve("first", &first_hash).await.unwrap();
    match replay {
        RedisIngressReservationOutcome::Pending => {}
        RedisIngressReservationOutcome::Acquired(reservation) => {
            assert_eq!(reservation.signal_id(), first_signal)
        }
        _ => panic!("accepted reservation was lost under pressure"),
    }
    assert_eq!(store.quota_state().await.unwrap().producer_records, 1);
    assert!(lock(&capability.failures).is_empty());
}

#[test]
#[ignore = "spawned by real_process_redis_ingress_crash_boundaries_converge"]
fn redis_ingress_process_child() {
    if std::env::var_os("TICKR_REDIS_INGRESS_CHILD").is_none() {
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let port = std::env::var("TICKR_REDIS_INGRESS_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let roots = std::env::var("TICKR_REDIS_INGRESS_ROOTS").unwrap();
        let namespace = std::env::var("TICKR_REDIS_INGRESS_NAMESPACE").unwrap();
        let boundary = std::env::var("TICKR_REDIS_INGRESS_BOUNDARY").unwrap();
        let marker = PathBuf::from(std::env::var("TICKR_REDIS_INGRESS_MARKER").unwrap());
        let event_capability = Arc::new(OpenEventCapability::default());
        let idempotency_capability = Arc::new(OpenIdempotencyCapability::default());
        let event = RedisEventIngress::connect(
            tls_client(port, "event-ingress", EVENT_PASSWORD, &roots),
            event_config(&namespace, "crashing-conductor"),
            RedisDurabilityGuard::default(),
            event_capability,
        )
        .await
        .unwrap();
        let store = RedisIngressIdempotencyStore::connect(
            tls_client(port, "ingress-idempotency", IDEMPOTENCY_PASSWORD, &roots),
            idempotency_config(&namespace),
            RedisDurabilityGuard::default(),
            idempotency_capability,
        )
        .await
        .unwrap();
        let payload = b"crash-boundary-event".to_vec();
        let hash = payload_hash(&payload);
        if boundary == "reservation" {
            event
                .append("crash:reservation", "crash-producer", payload)
                .await
                .unwrap();
            let _ = event.next_delivery().await.unwrap().unwrap();
            let _ = store.reserve("crash-producer", &hash).await.unwrap();
        } else if boundary == "effects-and-intent" {
            let reservation = match store.reserve("crash-producer", &hash).await.unwrap() {
                RedisIngressReservationOutcome::Acquired(reservation) => reservation,
                _ => panic!("crash effects reservation unavailable"),
            };
            reservation
                .persist_effects(
                    b"one-signal-effect".to_vec(),
                    b"one-capture-result".to_vec(),
                    vec![b"one-relay-intent".to_vec()],
                )
                .await
                .unwrap();
        } else if boundary == "permanent-rejection" {
            event
                .append("crash:rejection", "rejected-producer", payload)
                .await
                .unwrap();
            let _ = event.next_delivery().await.unwrap().unwrap();
            let reservation = match store.reserve("rejected-producer", &hash).await.unwrap() {
                RedisIngressReservationOutcome::Acquired(reservation) => reservation,
                _ => panic!("crash rejection reservation unavailable"),
            };
            let _ = reservation.reject("permanent").await.unwrap();
        } else if boundary == "delivery-ack" {
            event
                .append("crash:ack", "acked-producer", payload)
                .await
                .unwrap();
            let delivery = event.next_delivery().await.unwrap().unwrap();
            let reservation = match store.reserve("acked-producer", &hash).await.unwrap() {
                RedisIngressReservationOutcome::Acquired(reservation) => reservation,
                _ => panic!("crash ACK reservation unavailable"),
            };
            reservation
                .persist_effects(
                    b"one-signal-effect".to_vec(),
                    b"one-capture-result".to_vec(),
                    vec![b"one-relay-intent".to_vec()],
                )
                .await
                .unwrap();
            let proof = reservation.operation().mark_relayed().await.unwrap();
            event.complete(&delivery, &proof).await.unwrap();
        }
        fs::write(&marker, boundary).unwrap();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, OpenSSL, and subprocess execution"]
async fn real_process_redis_ingress_crash_boundaries_converge() {
    for boundary in [
        "reservation",
        "effects-and-intent",
        "permanent-rejection",
        "delivery-ack",
    ] {
        let namespace = format!(
            "event-ingress-crash-{boundary}-{}",
            NEXT_REDIS.load(Ordering::Relaxed)
        );
        let fixture = RedisFixture::start(&namespace).await;
        let marker_dir = tempfile::tempdir().unwrap();
        let marker = marker_dir.path().join("boundary");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "redis_ingress_process_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("TICKR_REDIS_INGRESS_CHILD", "1")
            .env("TICKR_REDIS_INGRESS_PORT", fixture.port.to_string())
            .env("TICKR_REDIS_INGRESS_ROOTS", &fixture.trust_roots)
            .env("TICKR_REDIS_INGRESS_NAMESPACE", &namespace)
            .env("TICKR_REDIS_INGRESS_BOUNDARY", boundary)
            .env("TICKR_REDIS_INGRESS_MARKER", &marker)
            .spawn()
            .unwrap();
        await_marker(&mut child, &marker, boundary).await;
        child.kill().unwrap();
        let _ = child.wait().unwrap();

        let event_capability = Arc::new(OpenEventCapability::default());
        let idempotency_capability = Arc::new(OpenIdempotencyCapability::default());
        let event =
            event_adapter(&fixture, &namespace, "recovery-conductor", event_capability).await;
        let store = idempotency_adapter(&fixture, &namespace, idempotency_capability).await;
        let hash = payload_hash(b"crash-boundary-event");
        match boundary {
            "reservation" => {
                tokio::time::sleep(Duration::from_millis(30)).await;
                let delivery = event.next_delivery().await.unwrap().unwrap();
                let reservation = match store.reserve("crash-producer", &hash).await.unwrap() {
                    RedisIngressReservationOutcome::Acquired(reservation) => reservation,
                    _ => panic!("reservation did not reclaim after process crash"),
                };
                reservation
                    .persist_effects(
                        b"one-signal-effect".to_vec(),
                        b"one-capture-result".to_vec(),
                        vec![b"one-relay-intent".to_vec()],
                    )
                    .await
                    .unwrap();
                let proof = reservation.operation().mark_relayed().await.unwrap();
                event.complete(&delivery, &proof).await.unwrap();
            }
            "effects-and-intent" => {
                let (operation, effects) =
                    match store.reserve("crash-producer", &hash).await.unwrap() {
                        RedisIngressReservationOutcome::Ready(operation, effects) => {
                            (operation, effects)
                        }
                        _ => panic!("effects and relay intent did not recover"),
                    };
                assert_eq!(effects.signal_effect, b"one-signal-effect");
                assert_eq!(effects.relay_intents, vec![b"one-relay-intent".to_vec()]);
                let _ = operation.mark_relayed().await.unwrap();
                assert_eq!(store.quota_state().await.unwrap().effect_records, 1);
            }
            "permanent-rejection" => {
                assert!(matches!(
                    store.reserve("rejected-producer", &hash).await.unwrap(),
                    RedisIngressReservationOutcome::Rejected(_)
                ));
                tokio::time::sleep(Duration::from_millis(30)).await;
                let delivery = event.next_delivery().await.unwrap().unwrap();
                let proof = match store.reserve("rejected-producer", &hash).await.unwrap() {
                    RedisIngressReservationOutcome::Rejected(proof) => proof,
                    _ => panic!("permanent rejection did not recover"),
                };
                event.complete(&delivery, &proof).await.unwrap();
            }
            "delivery-ack" => {
                let (acceptance, _) = event
                    .append(
                        "crash:ack",
                        "acked-producer",
                        b"crash-boundary-event".to_vec(),
                    )
                    .await
                    .unwrap();
                assert_eq!(acceptance, RedisEventIngressAcceptance::ReplayedCompleted);
                assert!(event.next_delivery().await.unwrap().is_none());
                assert_eq!(event.quota_state().await.unwrap().accepted_deliveries, 1);
            }
            _ => unreachable!(),
        }
    }
}

async fn await_marker(child: &mut std::process::Child, marker: &Path, expected: &str) {
    for _ in 0..400 {
        if fs::read_to_string(marker).is_ok_and(|value| value == expected) {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("Redis ingress child exited before {expected}: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("Redis ingress child did not reach {expected}");
}

fn generate_tls(path: &Path) -> String {
    run(
        Command::new("openssl").args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            path.join("ca.key").to_str().unwrap(),
            "-out",
            path.join("ca.crt").to_str().unwrap(),
            "-subj",
            "/CN=tickr-test-ca",
            "-days",
            "1",
        ]),
        "generate Redis ingress CA",
    );
    run(
        Command::new("openssl").args([
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            path.join("server.key").to_str().unwrap(),
            "-out",
            path.join("server.csr").to_str().unwrap(),
            "-subj",
            "/CN=localhost",
        ]),
        "generate Redis ingress server CSR",
    );
    fs::write(
        path.join("server.ext"),
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )
    .unwrap();
    run(
        Command::new("openssl").args([
            "x509",
            "-req",
            "-in",
            path.join("server.csr").to_str().unwrap(),
            "-CA",
            path.join("ca.crt").to_str().unwrap(),
            "-CAkey",
            path.join("ca.key").to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            path.join("server.crt").to_str().unwrap(),
            "-days",
            "1",
            "-extfile",
            path.join("server.ext").to_str().unwrap(),
        ]),
        "sign Redis ingress server certificate",
    );
    fs::read_to_string(path.join("ca.crt")).unwrap()
}

fn run(command: &mut Command, description: &str) {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{description}: {error}"));
    assert!(
        output.status.success(),
        "{description} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
