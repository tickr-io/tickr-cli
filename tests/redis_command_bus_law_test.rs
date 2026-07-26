#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{http::StatusCode, response::IntoResponse};
use prost::Message as _;
use redis::{ConnectionInfo, TlsCertificates};
use testcontainers_modules::{
    nats::{Nats, NatsServerCmd},
    testcontainers::{runners::AsyncRunner, ImageExt},
};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaState,
    redis_command_bus::{
        RedisCommandBus, RedisCommandBusConfig, RedisCommandBusError, RedisCommandCapability,
    },
    redis_durability::RedisDurabilityGuard,
};
use tickr_api::commands::{
    client::{bus_error_response, BusError, CommandBus},
    local::LocalCommandBusConfig,
};
use tickr_proto::{coord::command_bus::DEFAULT_MAX_PAYLOAD_BYTES, tickr_api as api};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const REDIS_PASSWORD: &str = "redis-command-bus-secret";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
enum BackendKind {
    AllNats,
    Local,
    AllRedis,
}

#[derive(Clone, Copy)]
enum HandlerMode {
    Success,
    TypedFailure,
    Malformed,
    Blocked,
}

#[derive(Clone)]
struct LawHandler {
    mode: Arc<Mutex<HandlerMode>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl LawHandler {
    fn new() -> Self {
        Self {
            mode: Arc::new(Mutex::new(HandlerMode::Success)),
            calls: Arc::new(AtomicUsize::new(0)),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn set_mode(&self, mode: HandlerMode) {
        *self.mode.lock().expect("handler mode") = mode;
    }

    fn reset_calls(&self) {
        self.calls.store(0, Ordering::SeqCst);
    }

    async fn handle(&self, _payload: Vec<u8>) -> Vec<u8> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = *self.mode.lock().expect("handler mode");
        match mode {
            HandlerMode::Success => success_response(),
            HandlerMode::TypedFailure => api::ApiCommandResponse {
                status_code: 422,
                payload: Some(api::api_command_response::Payload::Error(
                    api::ErrorPayload {
                        code: api::CommandErrorCode::BadRequest as i32,
                        message: "typed command failure".to_owned(),
                    },
                )),
            }
            .encode_to_vec(),
            HandlerMode::Malformed => vec![0xff],
            HandlerMode::Blocked => {
                self.entered.notify_one();
                self.release.notified().await;
                success_response()
            }
        }
    }
}

#[derive(Default)]
struct OpenCapability {
    quota: Mutex<Vec<RedisQuotaState>>,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
}

impl OpenCapability {
    fn latest_quota(&self) -> Option<RedisQuotaState> {
        self.quota
            .lock()
            .expect("quota observations")
            .last()
            .copied()
    }
}

impl RedisCommandCapability for OpenCapability {
    fn guard_admission(&self) -> Result<u64, RedisCommandBusError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCommandBusError> {
        (generation == 1)
            .then_some(())
            .ok_or(RedisCommandBusError::Unavailable)
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.failures
            .lock()
            .expect("capability failures")
            .push(failure);
    }

    fn report_quota(&self, state: RedisQuotaState) {
        self.quota.lock().expect("quota observations").push(state);
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
            .prefix("redis-command-bus-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-command-bus-{}-{sequence}", std::process::id());
        let config_name = "redis.conf";
        fs::write(
            path.join(config_name),
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
                 user command-bus on >{REDIS_PASSWORD} ~tickr:{{{namespace}}}:command-bus:* -@all \
                 +eval +get +hget +hincrby +hmget +hset +set +del +time +waitaof \
                 +xack +xadd +xautoclaim +xdel +xgroup|create +xreadgroup \
                 +zadd +zcard +zrangebyscore +zrem +zremrangebyscore +zscore\n"
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
            "start Redis command-bus fixture",
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
        panic!("Redis command-bus fixture did not become ready");
    }

    fn client(&self) -> redis::Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let connection = format!(
            "rediss://command-bus:{REDIS_PASSWORD}@localhost:{}/",
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
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

struct RunningBackend {
    bus: CommandBus,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    nats: Option<async_nats::Client>,
    _nats_container: Option<testcontainers_modules::testcontainers::ContainerAsync<Nats>>,
    _redis_fixture: Option<RedisFixture>,
}

impl RunningBackend {
    async fn start(
        kind: BackendKind,
        handler: LawHandler,
        in_flight_limit: NonZeroUsize,
    ) -> Option<Self> {
        let cancel = CancellationToken::new();
        match kind {
            BackendKind::Local => {
                let (bus, writer) = CommandBus::local(LocalCommandBusConfig {
                    capacity: in_flight_limit,
                    max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                        .expect("non-zero constant"),
                });
                let writer_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    writer
                        .run(writer_cancel, move |payload| {
                            let handler = handler.clone();
                            async move { handler.handle(payload).await }
                        })
                        .await;
                });
                Some(Self {
                    bus,
                    cancel,
                    task: Some(task),
                    nats: None,
                    _nats_container: None,
                    _redis_fixture: None,
                })
            }
            BackendKind::AllNats => {
                let container = Nats::default()
                    .with_cmd(&NatsServerCmd::default().with_jetstream())
                    .start()
                    .await
                    .expect("start all-NATS law fixture");
                let port = container.get_host_port_ipv4(4222).await.ok()?;
                let nats = async_nats::connect(format!("nats://127.0.0.1:{port}"))
                    .await
                    .ok()?;
                let bus = CommandBus::nats_with_in_flight_limit(nats.clone(), in_flight_limit);
                let consumer_nats = nats.clone();
                let consumer_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    tickr_conductor::api_commands_consumer::start_with_handler(
                        consumer_nats,
                        consumer_cancel,
                        move |payload| {
                            let handler = handler.clone();
                            async move { handler.handle(payload).await }
                        },
                    )
                    .await
                    .expect("all-NATS command consumer");
                });
                let backend = Self {
                    bus,
                    cancel,
                    task: Some(task),
                    nats: Some(nats),
                    _nats_container: Some(container),
                    _redis_fixture: None,
                };
                backend.await_ready().await;
                Some(backend)
            }
            BackendKind::AllRedis => {
                let namespace = format!("command-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
                let fixture = RedisFixture::start(&namespace).await;
                let capability = Arc::new(OpenCapability::default());
                let mut config = RedisCommandBusConfig::new(&namespace, "conductor-a");
                config.consumer_lease = Duration::from_millis(150);
                config.reply_retention = Duration::from_millis(40);
                let redis = RedisCommandBus::connect(
                    fixture.client(),
                    config,
                    RedisDurabilityGuard::default(),
                    capability,
                )
                .await
                .expect("connect Redis command bus");
                let bus = CommandBus::redis(Arc::new(redis.clone()), in_flight_limit);
                let consumer_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    redis
                        .serve_with_handler(consumer_cancel, move |payload| {
                            let handler = handler.clone();
                            async move { handler.handle(payload).await }
                        })
                        .await
                        .expect("all-Redis command consumer");
                });
                let backend = Self {
                    bus,
                    cancel,
                    task: Some(task),
                    nats: None,
                    _nats_container: None,
                    _redis_fixture: Some(fixture),
                };
                backend.await_ready().await;
                Some(backend)
            }
        }
    }

    async fn await_ready(&self) {
        for _ in 0..100 {
            match self
                .bus
                .send(ping_request(), Duration::from_millis(250))
                .await
            {
                Ok(_) => return,
                Err(BusError::Unavailable) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("command backend readiness failed: {error:?}"),
            }
        }
        panic!("command backend did not become ready");
    }

    async fn stop(mut self) -> Self {
        self.cancel.cancel();
        self.task
            .take()
            .expect("command backend task")
            .await
            .expect("command backend task");
        if let Some(nats) = &self.nats {
            nats.flush().await.expect("flush consumer unsubscribe");
        }
        self
    }
}

fn ping_request() -> api::ApiCommandRequest {
    api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Ping(api::PingRequest {})),
    }
}

fn success_response() -> Vec<u8> {
    api::ApiCommandResponse {
        status_code: 200,
        payload: Some(api::api_command_response::Payload::Ping(
            api::PingPayload {},
        )),
    }
    .encode_to_vec()
}

async fn exercise_backend(kind: BackendKind) {
    let handler = LawHandler::new();
    let backend = RunningBackend::start(
        kind,
        handler.clone(),
        NonZeroUsize::new(2).expect("non-zero constant"),
    )
    .await
    .expect("start command backend");
    handler.reset_calls();

    let success = backend
        .bus
        .send(ping_request(), Duration::from_secs(1))
        .await
        .expect("typed success");
    assert_eq!(success.status_code, 200, "backend: {kind:?}");

    handler.set_mode(HandlerMode::TypedFailure);
    let failure = backend
        .bus
        .send(ping_request(), Duration::from_secs(1))
        .await
        .expect("typed failure");
    assert_eq!(failure.status_code, 422, "backend: {kind:?}");
    assert!(matches!(
        failure.payload,
        Some(api::api_command_response::Payload::Error(_))
    ));

    handler.set_mode(HandlerMode::Malformed);
    assert!(matches!(
        backend
            .bus
            .send(ping_request(), Duration::from_secs(1))
            .await,
        Err(BusError::Malformed)
    ));

    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let duplicate = Uuid::new_v4();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(1), duplicate)
                .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(ping_request(), Duration::from_secs(1), duplicate)
            .await,
        Err(BusError::DuplicateCorrelation)
    ));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.release.notify_one();
    first
        .await
        .expect("blocked request task")
        .expect("first reply");

    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let first_correlation = Uuid::new_v4();
    let expired_correlation = Uuid::new_v4();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(1), first_correlation)
                .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(
                ping_request(),
                Duration::from_millis(20),
                expired_correlation,
            )
            .await,
        Err(BusError::Timeout)
    ));
    handler.release.notify_one();
    first
        .await
        .expect("blocked request task")
        .expect("first reply");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        1,
        "expired command reached handler on {kind:?}"
    );

    handler.set_mode(HandlerMode::Success);
    let mut cleaned = false;
    for _ in 0..50 {
        match backend
            .bus
            .send_with_correlation(ping_request(), Duration::from_secs(1), expired_correlation)
            .await
        {
            Ok(reply) if reply.status_code == 200 => {
                cleaned = true;
                break;
            }
            Err(BusError::DuplicateCorrelation) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            other => panic!("unexpected correlation cleanup result: {other:?}"),
        }
    }
    assert!(cleaned, "expired correlation was not cleaned on {kind:?}");

    let oversized = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Register(
            api::RegisterRequest {
                nickel_source: "x".repeat(DEFAULT_MAX_PAYLOAD_BYTES + 1),
                namespace: String::new(),
            },
        )),
    };
    assert!(matches!(
        backend.bus.send(oversized, Duration::from_secs(1)).await,
        Err(BusError::TooLarge)
    ));

    let stopped = backend.stop().await;
    let unavailable = stopped
        .bus
        .send(ping_request(), Duration::from_millis(250))
        .await;
    assert!(
        matches!(unavailable, Err(BusError::Unavailable)),
        "stopped backend remained available on {kind:?}: {unavailable:?}"
    );
}

async fn exercise_saturation(kind: BackendKind) {
    let handler = LawHandler::new();
    let backend = RunningBackend::start(
        kind,
        handler.clone(),
        NonZeroUsize::new(1).expect("non-zero constant"),
    )
    .await
    .expect("start command backend");
    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(1), Uuid::new_v4())
                .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(ping_request(), Duration::from_secs(1), Uuid::new_v4())
            .await,
        Err(BusError::Unavailable)
    ));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.release.notify_one();
    first.await.expect("saturation task").expect("first reply");
    backend.stop().await;
}

async fn exercise_redis_restart_and_pressure() {
    let namespace = format!("command-restart-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenCapability::default());
    let mut config = RedisCommandBusConfig::new(&namespace, "conductor-restart");
    config.consumer_lease = Duration::from_millis(150);
    config.reply_retention = Duration::from_millis(30);
    let redis = RedisCommandBus::connect(
        fixture.client(),
        config,
        RedisDurabilityGuard::default(),
        capability.clone(),
    )
    .await
    .expect("connect restart Redis bus");
    let bus = CommandBus::redis(
        Arc::new(redis.clone()),
        NonZeroUsize::new(4).expect("non-zero constant"),
    );
    assert!(matches!(
        bus.send(ping_request(), Duration::from_millis(100)).await,
        Err(BusError::Unavailable)
    ));

    let handler = LawHandler::new();
    let first_cancel = CancellationToken::new();
    let first_task = tokio::spawn({
        let redis = redis.clone();
        let handler = handler.clone();
        let cancel = first_cancel.clone();
        async move {
            redis
                .serve_with_handler(cancel, move |payload| {
                    let handler = handler.clone();
                    async move { handler.handle(payload).await }
                })
                .await
        }
    });
    for _ in 0..50 {
        if bus
            .send(ping_request(), Duration::from_millis(250))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handler.reset_calls();
    first_task.abort();
    let _ = first_task.await;

    let expired_correlation = Uuid::new_v4();
    assert!(matches!(
        bus.send_with_correlation(
            ping_request(),
            Duration::from_millis(30),
            expired_correlation,
        )
        .await,
        Err(BusError::Timeout)
    ));
    tokio::time::sleep(Duration::from_millis(180)).await;

    let restart_cancel = CancellationToken::new();
    let restart_task = tokio::spawn({
        let redis = redis.clone();
        let handler = handler.clone();
        let cancel = restart_cancel.clone();
        async move {
            redis
                .serve_with_handler(cancel, move |payload| {
                    let handler = handler.clone();
                    async move { handler.handle(payload).await }
                })
                .await
                .expect("restarted Redis consumer");
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        0,
        "expired request was applied after consumer restart"
    );
    for _ in 0..20 {
        redis.cleanup_expired().await.expect("expiry cleanup");
        if redis.quota_state().await.expect("quota state").used == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(redis.quota_state().await.expect("quota state").used, 0);
    assert_eq!(
        bus.send(ping_request(), Duration::from_secs(1))
            .await
            .expect("restarted consumer reply")
            .status_code,
        200
    );
    restart_cancel.cancel();
    restart_task.await.expect("restart task");
    drop(fixture);

    let namespace = format!("command-pressure-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenCapability::default());
    let request_bytes = ping_request().encode_to_vec().len() as u64;
    let unit = 256 + 128 + request_bytes;
    let mut config = RedisCommandBusConfig::new(&namespace, "conductor-pressure");
    config.consumer_lease = Duration::from_millis(150);
    config.reply_retention = Duration::from_millis(30);
    config.max_payload_bytes = NonZeroUsize::new(128).expect("non-zero constant");
    config.max_records = NonZeroUsize::new(6).expect("non-zero constant");
    config.soft_limit_bytes = unit;
    config.hard_limit_bytes = unit * 2;
    let redis = RedisCommandBus::connect(
        fixture.client(),
        config,
        RedisDurabilityGuard::default(),
        capability.clone(),
    )
    .await
    .expect("connect pressure Redis bus");
    let bus = CommandBus::redis(
        Arc::new(redis.clone()),
        NonZeroUsize::new(3).expect("non-zero constant"),
    );
    let handler = LawHandler::new();
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let redis = redis.clone();
        let handler = handler.clone();
        let cancel = cancel.clone();
        async move {
            redis
                .serve_with_handler(cancel, move |payload| {
                    let handler = handler.clone();
                    async move { handler.handle(payload).await }
                })
                .await
                .expect("pressure Redis consumer");
        }
    });
    for _ in 0..50 {
        if bus
            .send(ping_request(), Duration::from_millis(250))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let first_entered = handler.entered.notified();
    let first = {
        let bus = bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(2), Uuid::new_v4())
                .await
        })
    };
    first_entered.await;
    let second = {
        let bus = bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(2), Uuid::new_v4())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        capability.latest_quota().expect("hard quota").pressure,
        tickr::redis_capacity::RedisQuotaPressure::HardLimit
    );
    assert!(matches!(
        bus.send_with_correlation(ping_request(), Duration::from_secs(1), Uuid::new_v4(),)
            .await,
        Err(BusError::Unavailable)
    ));
    handler.release.notify_one();
    let second_entered = handler.entered.notified();
    second_entered.await;
    handler.release.notify_one();
    first
        .await
        .expect("first pressure task")
        .expect("first reply");
    second
        .await
        .expect("second pressure task")
        .expect("second reply");
    for _ in 0..20 {
        if redis.quota_state().await.expect("quota state").used == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(redis.quota_state().await.expect("quota state").used, 0);
    cancel.cancel();
    task.await.expect("pressure consumer task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_backends_obey_the_same_command_bus_laws() {
    for backend in [
        BackendKind::AllNats,
        BackendKind::Local,
        BackendKind::AllRedis,
    ] {
        exercise_backend(backend).await;
        exercise_saturation(backend).await;
    }
    exercise_redis_restart_and_pressure().await;
    assert_eq!(
        bus_error_response(BusError::DuplicateCorrelation)
            .into_response()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        bus_error_response(BusError::Unavailable)
            .into_response()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
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
                "/CN=Tickr Redis Command Test CA",
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
