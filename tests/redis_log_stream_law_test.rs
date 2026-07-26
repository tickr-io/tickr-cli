#![cfg(not(madsim))]

#[path = "support/log_stream_laws.rs"]
mod log_stream_laws;

use std::{
    fs,
    io::{Read as _, Write as _},
    net::TcpListener,
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use anyhow::Result;
use redis::{AsyncCommands as _, ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_durability::RedisDurabilityGuard,
    redis_log_staging::{
        RedisArchiveCommitOutcome, RedisLogPurgeOutcome, RedisLogStagingCapability,
        RedisLogStagingConfig, RedisLogStagingError, RedisLogStagingQuotaState,
        RedisLogStagingStream, RedisLogStreamProvider,
    },
};
use tickr_api::{config::LogStorageConfig, http::logs_resolver::LogsResolver};
use tickr_executor::{
    log_stream::{LogStream, LogStreamProvider, LogStreamRoute},
    task_log_shipper::{ShipperConfig, TaskLogShipper},
};
use tickr_proto::coord::log_stream::{
    AcceptOutcome, GapOutcome, LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity,
    LogTerminal, PreAcceptanceGap, ReplayedLogRecord, TerminalOutcome,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ROLE_PASSWORD: &str = "redis-log-staging-secret";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn not_found_object_store_endpoint() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    std::thread::spawn(move || {
        for connection in listener.incoming().take(2) {
            let Ok(mut connection) = connection else {
                return;
            };
            let mut request = [0_u8; 4096];
            let _ = connection.read(&mut request);
            let body = "<Error><Code>NoSuchKey</Code><Message>Not Found</Message></Error>";
            let response = format!(
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = connection.write_all(response.as_bytes());
        }
    });
    Ok(format!("http://{address}"))
}

#[derive(Default)]
struct ToggleCapability {
    acknowledgement_open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisLogStagingQuotaState>>,
}

impl ToggleCapability {
    fn open() -> Self {
        Self {
            acknowledgement_open: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn lose_next_reply(&self) {
        self.acknowledgement_open.store(false, Ordering::SeqCst);
    }

    fn restore_replies(&self) {
        self.acknowledgement_open.store(true, Ordering::SeqCst);
    }
}

impl RedisLogStagingCapability for ToggleCapability {
    fn guard_admission(&self) -> Result<u64, RedisLogStagingError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLogStagingError> {
        if generation == 1 && self.acknowledgement_open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RedisLogStagingError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisLogStagingQuotaState) {
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
    async fn start(_namespace: &str) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("redis-log-staging-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-log-staging-{}-{sequence}", std::process::id());
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
                 user default on >redis-log-staging-aof-loader ~* &* +@all\n\
                 user logstaging on >{ROLE_PASSWORD} ~tickr:{{*}}:log-staging:* -@all \
                 +del +eval +get +hdel +hget +hgetall +hincrby +hmget +hset +set +waitaof\n"
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
            "start Redis LogStaging fixture",
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
            "rediss://logstaging:{ROLE_PASSWORD}@localhost:{}/",
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
            "restart Redis LogStaging fixture",
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

async fn open_stream(
    client: redis::Client,
    identity: LogStreamIdentity,
    config: RedisLogStagingConfig,
    capability: Arc<ToggleCapability>,
) -> Result<RedisLogStagingStream, RedisLogStagingError> {
    RedisLogStagingStream::connect(
        client,
        identity,
        config,
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_log_stream_laws_cover_crash_pressure_seal_archive_and_purge() -> Result<()> {
    let namespace = format!("log-law-{}", Uuid::new_v4().simple());
    let mut fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(ToggleCapability::open());
    let config = RedisLogStagingConfig::new(namespace.clone());

    log_stream_laws::assert_log_stream_laws({
        let client = fixture.client();
        let capability = Arc::clone(&capability);
        let config = config.clone();
        move |identity, _| {
            let client = client.clone();
            let capability = Arc::clone(&capability);
            let config = config.clone();
            Box::pin(async move {
                Ok(
                    Box::new(open_stream(client, identity, config, capability).await?)
                        as Box<dyn LogStream>,
                )
            })
        }
    })
    .await?;

    let crash_identity = log_stream_laws::identity(Uuid::new_v4(), 3);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    capability.lose_next_reply();
    assert_eq!(
        crash_stream
            .accept(log_stream_laws::submission(
                &crash_identity,
                1,
                b"mutation fsynced before lost reply",
            ))
            .await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    fixture.restart().await;
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    assert_eq!(
        crash_stream
            .accept(log_stream_laws::submission(
                &crash_identity,
                1,
                b"mutation fsynced before lost reply",
            ))
            .await?,
        AcceptOutcome::AlreadyAccepted
    );

    capability.lose_next_reply();
    assert_eq!(
        crash_stream
            .declare_pre_acceptance_gap(tickr_proto::coord::log_stream::PreAcceptanceGap {
                stream: crash_identity.clone(),
                first_sequence: 0,
                last_sequence: 0,
                dropped_records: 1,
            })
            .await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    assert_eq!(
        crash_stream
            .declare_pre_acceptance_gap(tickr_proto::coord::log_stream::PreAcceptanceGap {
                stream: crash_identity.clone(),
                first_sequence: 0,
                last_sequence: 0,
                dropped_records: 1,
            })
            .await?,
        GapOutcome::AlreadyDeclared
    );

    capability.lose_next_reply();
    assert_eq!(
        crash_stream.finish_cleanly(LogExit::Status(0)).await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    assert_eq!(
        crash_stream.finish_cleanly(LogExit::Status(0)).await?,
        TerminalOutcome::AlreadyRecorded
    );

    capability.lose_next_reply();
    assert_eq!(
        crash_stream.seal().await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    let seal = crash_stream.seal().await?;
    let archive_identity = b"verified-final-log:crash-stream";
    assert_eq!(
        crash_stream
            .purge_after_verified_archive_commit(&seal, archive_identity)
            .await,
        Err(RedisLogStagingError::ArchiveNotCommitted)
    );

    capability.lose_next_reply();
    assert_eq!(
        crash_stream
            .record_verified_archive_commit(&seal, archive_identity)
            .await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity.clone(),
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    assert_eq!(
        crash_stream
            .record_verified_archive_commit(&seal, archive_identity)
            .await?,
        RedisArchiveCommitOutcome::AlreadyRecorded
    );

    let before_purge = crash_stream.quota_state().await?;
    capability.lose_next_reply();
    assert_eq!(
        crash_stream
            .purge_after_verified_archive_commit(&seal, archive_identity)
            .await,
        Err(RedisLogStagingError::Unavailable)
    );
    capability.restore_replies();
    drop(crash_stream);
    let mut crash_stream = open_stream(
        fixture.client(),
        crash_identity,
        config.clone(),
        Arc::clone(&capability),
    )
    .await?;
    assert_eq!(
        crash_stream
            .purge_after_verified_archive_commit(&seal, archive_identity)
            .await?,
        RedisLogPurgeOutcome::AlreadyPurged
    );
    assert!(crash_stream.quota_state().await?.used_bytes < before_purge.used_bytes);

    let pressure_identity = log_stream_laws::identity(Uuid::new_v4(), 9);
    let pressure_namespace = format!("pressure-{}", Uuid::new_v4().simple());
    let mut pressure_config = RedisLogStagingConfig::new(pressure_namespace);
    pressure_config.max_record_bytes = NonZeroUsize::new(20 * 1024).expect("non-zero");
    pressure_config.soft_limit_bytes = 6 * 1024;
    pressure_config.hard_limit_bytes = 52 * 1024;
    let mut pressure_stream = open_stream(
        fixture.client(),
        pressure_identity.clone(),
        pressure_config,
        Arc::clone(&capability),
    )
    .await?;
    let baseline = pressure_stream.quota_state().await?;
    assert_eq!(
        pressure_stream
            .accept(LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: pressure_identity.clone(),
                    sequence: 0,
                },
                vec![b'a'; 7 * 1024],
            ))
            .await?,
        AcceptOutcome::Accepted
    );
    assert_eq!(
        pressure_stream.quota_state().await?.pressure,
        RedisQuotaPressure::SoftThreshold
    );
    assert_eq!(
        pressure_stream
            .accept(LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: pressure_identity.clone(),
                    sequence: 1,
                },
                vec![b'b'; 20 * 1024],
            ))
            .await,
        Err(RedisLogStagingError::CapacityFenced)
    );
    assert_eq!(pressure_stream.committed_frontier(), Some(0));
    pressure_stream.finish_cleanly(LogExit::Status(0)).await?;
    let pressure_seal = pressure_stream.seal().await?;
    let pressure_archive = b"verified-final-log:pressure-stream";
    pressure_stream
        .record_verified_archive_commit(&pressure_seal, pressure_archive)
        .await?;
    let retained = pressure_stream.quota_state().await?;
    assert!(retained.used_bytes > baseline.used_bytes);
    pressure_stream
        .purge_after_verified_archive_commit(&pressure_seal, pressure_archive)
        .await?;
    assert_eq!(
        pressure_stream.quota_state().await?.used_bytes,
        baseline.used_bytes
    );

    let production_namespace = format!("production-{}", Uuid::new_v4().simple());
    let production_config = RedisLogStagingConfig::new(production_namespace);
    let provider = Arc::new(
        RedisLogStreamProvider::connect(
            fixture.client(),
            production_config.clone(),
            RedisDurabilityGuard::default(),
            capability.clone(),
        )
        .await?,
    );
    provider.prepare().await?;
    let route = LogStreamRoute {
        workflow_id: Uuid::new_v4(),
        workflow_instance_id: Uuid::new_v4(),
        task_instance_id: Uuid::new_v4(),
    };
    let shipper_identity = LogStreamIdentity {
        task_instance_id: route.task_instance_id,
        pickup_generation: 11,
    };
    let shipper_stream = provider
        .open(route.clone(), shipper_identity.clone())
        .await?;
    let shipper_config = ShipperConfig {
        buffer_capacity: 8,
        record_max_bytes: 6,
        publish_timeout: Duration::from_secs(2),
        publish_backoff_max: Duration::from_millis(20),
        flush_deadline: Duration::from_secs(5),
    };
    let (mut stdout_writer, stdout_reader) = tokio::io::duplex(64);
    let shipper = TaskLogShipper::start_readers(
        shipper_stream,
        &shipper_config,
        vec![Box::new(stdout_reader)],
    );
    stdout_writer.write_all(b"first\nsecond\n").await?;
    drop(stdout_writer);
    shipper
        .finish(LogExit::Status(0), &CancellationToken::new())
        .await;

    let gap_identity = LogStreamIdentity {
        task_instance_id: route.task_instance_id,
        pickup_generation: 12,
    };
    let mut gap_stream = provider.open(route.clone(), gap_identity.clone()).await?;
    gap_stream
        .accept(log_stream_laws::submission(
            &gap_identity,
            1,
            b"accepted after bounded loss",
        ))
        .await?;
    gap_stream
        .declare_pre_acceptance_gap(PreAcceptanceGap {
            stream: gap_identity,
            first_sequence: 0,
            last_sequence: 0,
            dropped_records: 1,
        })
        .await?;
    assert_eq!(gap_stream.committed_frontier(), Some(1));
    gap_stream.recover_abnormal_closure().await?;
    drop(gap_stream);
    drop(provider);

    fixture.restart().await;
    let recovered = Arc::new(
        RedisLogStreamProvider::connect(
            fixture.client(),
            production_config,
            RedisDurabilityGuard::default(),
            capability.clone(),
        )
        .await?,
    );
    recovered.prepare().await?;
    let log_streams: Arc<dyn LogStreamProvider> = recovered.clone();
    let minio = LogStorageConfig {
        endpoint: not_found_object_store_endpoint()?,
        bucket: "logs".to_owned(),
        access_key_id: "test".to_owned(),
        secret_access_key: "test".to_owned(),
        region: "us-east-1".to_owned(),
    }
    .operator()?;
    let resolver = LogsResolver::new(minio, log_streams);
    let live_logs = resolver
        .fetch_task_logs(
            route.workflow_id,
            route.workflow_instance_id,
            route.task_instance_id,
        )
        .await?;
    assert_eq!(
        live_logs.content,
        b"first\nsecond\naccepted after bounded loss".as_slice()
    );
    let page = resolver
        .fetch_tail(
            route.workflow_id,
            route.workflow_instance_id,
            route.task_instance_id,
            usize::MAX,
            None,
        )
        .await?;
    assert_eq!(
        page.batches
            .iter()
            .map(|batch| batch.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 5],
        "the missing cursor position declares the bounded pre-acceptance gap"
    );
    assert_eq!(
        page.marker.and_then(|marker| marker.reason).as_deref(),
        Some("Executor closed without controlled End-of-stream")
    );
    let replay = recovered.replay_task(route).await?;
    let accepted = replay
        .iter()
        .filter_map(|record| match record {
            ReplayedLogRecord::Accepted { bytes, .. } => Some(bytes),
            _ => None,
        })
        .flat_map(|bytes| bytes.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(
        accepted,
        b"first\nsecond\naccepted after bounded loss".as_slice()
    );
    assert!(replay
        .iter()
        .any(|record| matches!(record, ReplayedLogRecord::PreAcceptanceGap(gap) if gap.first_sequence == 0 && gap.last_sequence == 0)));
    assert!(replay.iter().any(|record| matches!(
        record,
        ReplayedLogRecord::Terminal {
            terminal: LogTerminal::EndOfStream {
                exit: LogExit::Status(0)
            },
            ..
        }
    )));
    assert!(replay.iter().any(|record| matches!(
        record,
        ReplayedLogRecord::Terminal {
            terminal: LogTerminal::AbnormalClosure {
                committed_frontier: Some(1)
            },
            ..
        }
    )));

    let mut role_connection = fixture.client().get_multiplexed_tokio_connection().await?;
    let cross_role: redis::RedisResult<()> = role_connection
        .set("tickr:{denied}:task-dispatch:claim", "forbidden")
        .await;
    assert!(cross_role.is_err());
    let administrative: redis::RedisResult<()> = redis::cmd("FLUSHALL")
        .query_async(&mut role_connection)
        .await;
    assert!(administrative.is_err());

    Ok(())
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
        "Redis LogStaging fixture did not become ready: {}{}",
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
                "/CN=Tickr Redis LogStaging Test CA",
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
