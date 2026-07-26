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

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use prost::Message as _;
use redis::{AsyncCommands as _, ConnectionInfo, TlsCertificates};
use sha2::{Digest, Sha256};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_compaction_staging::{
        RedisCompactionArchive, RedisCompactionArchiveError, RedisCompactionArchiveInstallation,
        RedisCompactionStageOutcome, RedisCompactionStaging, RedisCompactionStagingCapability,
        RedisCompactionStagingConfig, RedisCompactionStagingError,
        RedisCompactionStagingQuotaState,
    },
    redis_durability::RedisDurabilityGuard,
    redis_log_staging::{
        RedisLogStagingCapability, RedisLogStagingConfig, RedisLogStagingError,
        RedisLogStagingQuotaState, RedisLogStagingStream,
    },
    redis_scope_store::{
        RedisScopeStore, RedisScopeStoreCapability, RedisScopeStoreConfig, RedisScopeStoreError,
        RedisScopeStoreQuotaState,
    },
};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeReadOutcome,
};
use tickr_proto::{
    archive::{ArchiveProjection, CompactionEnvelope},
    coord::log_stream::{
        AcceptOutcome, LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity,
        ReplayedLogRecord, TerminalOutcome,
    },
    instance::SnapshotTaskInstance,
};
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const COMPACTION_PASSWORD: &str = "redis-compaction-staging-secret";
const LOG_PASSWORD: &str = "redis-log-staging-secret";
const SCOPE_PASSWORD: &str = "redis-scope-store-secret";
const ADMIN_PASSWORD: &str = "redis-compaction-admin-secret";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default)]
struct ToggleCompactionCapability {
    admissions_open: AtomicBool,
    acknowledgements_open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisCompactionStagingQuotaState>>,
}

impl ToggleCompactionCapability {
    fn open() -> Self {
        Self {
            admissions_open: AtomicBool::new(true),
            acknowledgements_open: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn lose_reply(&self) {
        self.acknowledgements_open.store(false, Ordering::SeqCst);
    }

    fn fence(&self) {
        self.admissions_open.store(false, Ordering::SeqCst);
        self.acknowledgements_open.store(false, Ordering::SeqCst);
    }

    fn restore(&self) {
        self.admissions_open.store(true, Ordering::SeqCst);
        self.acknowledgements_open.store(true, Ordering::SeqCst);
    }
}

impl RedisCompactionStagingCapability for ToggleCompactionCapability {
    fn guard_admission(&self) -> Result<u64, RedisCompactionStagingError> {
        if self.admissions_open.load(Ordering::SeqCst) {
            Ok(1)
        } else {
            Err(RedisCompactionStagingError::Unavailable)
        }
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCompactionStagingError> {
        if generation == 1 && self.acknowledgements_open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RedisCompactionStagingError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisCompactionStagingQuotaState) {
        lock(&self.quotas).push(state);
    }
}

#[derive(Default)]
struct OpenLogCapability;

impl RedisLogStagingCapability for OpenLogCapability {
    fn guard_admission(&self) -> Result<u64, RedisLogStagingError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLogStagingError> {
        (generation == 1)
            .then_some(())
            .ok_or(RedisLogStagingError::Unavailable)
    }

    fn report_failure(&self, _: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _: RedisLogStagingQuotaState) {}
}

#[derive(Default)]
struct OpenScopeCapability;

impl RedisScopeStoreCapability for OpenScopeCapability {
    fn guard_admission(&self) -> Result<u64, RedisScopeStoreError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisScopeStoreError> {
        (generation == 1)
            .then_some(())
            .ok_or(RedisScopeStoreError::Unavailable)
    }

    fn report_failure(&self, _: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _: RedisScopeStoreQuotaState) {}
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
            .prefix("redis-compaction-staging-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tickr-redis-compaction-staging-{}-{sequence}",
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
                 user compactionstaging on >{COMPACTION_PASSWORD} ~tickr:{{{namespace}}}:compaction-staging:* -@all \
                 +eval +hdel +hget +hgetall +hincrby +hlen +hmget +hset +hvals +waitaof \
                 +xack +xadd +xautoclaim +xdel +xgroup|create +xrange +xreadgroup\n\
                 user logstaging on >{LOG_PASSWORD} ~tickr:{{{namespace}}}:log-staging:* -@all \
                 +del +eval +get +hdel +hget +hgetall +hincrby +hmget +hset +set +waitaof\n\
                 user scopestore on >{SCOPE_PASSWORD} ~tickr:{{{namespace}}}:scope-store:* -@all \
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
            "start Redis CompactionStaging fixture",
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

    fn client(&self, username: &str, password: &str) -> redis::Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let connection = format!("rediss://{username}:{password}@localhost:{}/", self.port)
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

struct FileArchive {
    root: PathBuf,
    steps: Mutex<Vec<&'static str>>,
}

impl FileArchive {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            steps: Mutex::new(Vec::new()),
        }
    }

    fn steps(&self) -> Vec<&'static str> {
        lock(&self.steps).clone()
    }
}

#[async_trait]
impl RedisCompactionArchive for FileArchive {
    async fn write_final_logs(
        &self,
        envelope: &[u8],
        seal: &tickr::redis_compaction_staging::RedisCompactionSeal,
    ) -> Result<RedisCompactionArchiveInstallation, RedisCompactionArchiveError> {
        lock(&self.steps).push("write");
        fs::create_dir_all(&self.root).map_err(|_| RedisCompactionArchiveError)?;
        let payload_path = self.root.join("compaction-envelope.bin");
        fs::write(&payload_path, envelope).map_err(|_| RedisCompactionArchiveError)?;
        for log in seal.logs() {
            let bytes = log
                .accepted_records()
                .iter()
                .flat_map(|record| record.bytes.iter().copied())
                .collect::<Vec<_>>();
            fs::write(
                self.root
                    .join(format!("{}.log", log.stream().task_instance_id)),
                bytes,
            )
            .map_err(|_| RedisCompactionArchiveError)?;
        }
        fs::write(self.root.join("scope.snapshot"), &seal.scope().bytes)
            .map_err(|_| RedisCompactionArchiveError)?;
        RedisCompactionArchiveInstallation::new(
            serde_json::to_vec(&(payload_path, seal.digest()))
                .map_err(|_| RedisCompactionArchiveError)?,
        )
    }

    async fn verify_final_logs(
        &self,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<(), RedisCompactionArchiveError> {
        lock(&self.steps).push("verify");
        let (path, _): (PathBuf, String) = serde_json::from_slice(installation.identity())
            .map_err(|_| RedisCompactionArchiveError)?;
        let bytes = fs::read(path).map_err(|_| RedisCompactionArchiveError)?;
        if bytes.is_empty() {
            return Err(RedisCompactionArchiveError);
        }
        Ok(())
    }

    async fn commit_archive(
        &self,
        envelope: &[u8],
        seal: &tickr::redis_compaction_staging::RedisCompactionSeal,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<Vec<u8>, RedisCompactionArchiveError> {
        lock(&self.steps).push("commit");
        if self.steps() != ["write", "verify", "commit"] {
            return Err(RedisCompactionArchiveError);
        }
        let mut digest = Sha256::new();
        digest.update(envelope);
        digest.update(seal.digest().as_bytes());
        digest.update(installation.identity());
        let identity = format!("verified-archive:{:x}", digest.finalize()).into_bytes();
        fs::write(self.root.join("archive.commit"), &identity)
            .map_err(|_| RedisCompactionArchiveError)?;
        Ok(identity)
    }
}

struct ProcessArchive {
    root: PathBuf,
}

#[async_trait]
impl RedisCompactionArchive for ProcessArchive {
    async fn write_final_logs(
        &self,
        envelope: &[u8],
        seal: &tickr::redis_compaction_staging::RedisCompactionSeal,
    ) -> Result<RedisCompactionArchiveInstallation, RedisCompactionArchiveError> {
        fs::create_dir_all(&self.root).map_err(|_| RedisCompactionArchiveError)?;
        let payload_path = self.root.join("compaction-envelope.bin");
        fs::write(&payload_path, envelope).map_err(|_| RedisCompactionArchiveError)?;
        for log in seal.logs() {
            let bytes = log
                .accepted_records()
                .iter()
                .flat_map(|record| record.bytes.iter().copied())
                .collect::<Vec<_>>();
            fs::write(
                self.root
                    .join(format!("{}.log", log.stream().task_instance_id)),
                bytes,
            )
            .map_err(|_| RedisCompactionArchiveError)?;
        }
        fs::write(self.root.join("scope.snapshot"), &seal.scope().bytes)
            .map_err(|_| RedisCompactionArchiveError)?;
        RedisCompactionArchiveInstallation::new(
            serde_json::to_vec(&(payload_path, seal.digest()))
                .map_err(|_| RedisCompactionArchiveError)?,
        )
    }

    async fn verify_final_logs(
        &self,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<(), RedisCompactionArchiveError> {
        let (path, _): (PathBuf, String) = serde_json::from_slice(installation.identity())
            .map_err(|_| RedisCompactionArchiveError)?;
        let bytes = fs::read(path).map_err(|_| RedisCompactionArchiveError)?;
        (!bytes.is_empty())
            .then_some(())
            .ok_or(RedisCompactionArchiveError)
    }

    async fn commit_archive(
        &self,
        envelope: &[u8],
        seal: &tickr::redis_compaction_staging::RedisCompactionSeal,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<Vec<u8>, RedisCompactionArchiveError> {
        let mut digest = Sha256::new();
        digest.update(envelope);
        digest.update(seal.digest().as_bytes());
        digest.update(installation.identity());
        let identity = format!("verified-archive:{:x}", digest.finalize()).into_bytes();
        let commit_path = self.root.join("archive.commit");
        match fs::read(&commit_path) {
            Ok(existing) if existing == identity => {}
            Ok(_) => return Err(RedisCompactionArchiveError),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::write(commit_path, &identity).map_err(|_| RedisCompactionArchiveError)?;
            }
            Err(_) => return Err(RedisCompactionArchiveError),
        }
        Ok(identity)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_compaction_laws_cover_staging_redelivery_archive_pressure_and_acl() -> Result<()>
{
    let namespace = format!("compaction-law-{}", Uuid::new_v4().simple());
    let fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(ToggleCompactionCapability::open());
    let compaction_client = fixture.client("compactionstaging", COMPACTION_PASSWORD);
    let mut config = RedisCompactionStagingConfig::new(&namespace, "conductor-a");
    config.reclaim_idle = Duration::from_millis(1);
    let staging = RedisCompactionStaging::connect(
        compaction_client.clone(),
        config.clone(),
        RedisDurabilityGuard::default(),
        Arc::clone(&capability) as Arc<dyn RedisCompactionStagingCapability>,
    )
    .await
    .map_err(|error| anyhow::anyhow!("connect CompactionStaging: {error:?}"))?;

    let workflow_instance_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    let payload = compaction_payload(workflow_instance_id, task_instance_id, "terminal");
    assert_eq!(
        staging
            .stage(payload.clone())
            .await
            .map_err(|error| anyhow::anyhow!("stage first Compaction: {error:?}"))?,
        RedisCompactionStageOutcome::Staged
    );
    assert_eq!(
        staging.stage(payload.clone()).await?,
        RedisCompactionStageOutcome::ReplayedPending
    );
    assert_eq!(
        staging
            .stage(compaction_payload(
                workflow_instance_id,
                task_instance_id,
                "conflicting-correlation"
            ))
            .await,
        Err(RedisCompactionStagingError::IdentityConflict)
    );

    let lost_reply_instance = Uuid::new_v4();
    let lost_reply_payload = compaction_payload(lost_reply_instance, Uuid::new_v4(), "lost-reply");
    capability.lose_reply();
    assert_eq!(
        staging.stage(lost_reply_payload.clone()).await,
        Err(RedisCompactionStagingError::Unavailable)
    );
    capability.restore();
    assert_eq!(
        staging.stage(lost_reply_payload).await?,
        RedisCompactionStageOutcome::ReplayedPending
    );

    let first_delivery = staging
        .claim_next()
        .await?
        .expect("first staged envelope is delivered");
    assert_eq!(first_delivery.identity(), workflow_instance_id.to_string());
    drop(first_delivery);
    drop(staging);
    tokio::time::sleep(Duration::from_millis(5)).await;

    config.consumer_id = "conductor-b".to_owned();
    let staging = RedisCompactionStaging::connect(
        compaction_client.clone(),
        config,
        RedisDurabilityGuard::default(),
        Arc::clone(&capability) as Arc<dyn RedisCompactionStagingCapability>,
    )
    .await?;
    let delivery = staging
        .claim_next()
        .await?
        .expect("dead consumer delivery is reclaimed");
    assert_eq!(delivery.identity(), workflow_instance_id.to_string());
    assert_eq!(staging.quota_state().await?.pending_deliveries, 1);

    let scope_store = RedisScopeStore::connect(
        fixture.client("scopestore", SCOPE_PASSWORD),
        RedisScopeStoreConfig::new(&namespace),
        RedisDurabilityGuard::default(),
        Arc::new(OpenScopeCapability),
    )
    .await?;
    assert!(matches!(
        scope_store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: workflow_instance_id,
                namespace: "default",
                run_id: &workflow_instance_id.to_string(),
                claim_id: Uuid::new_v4(),
                values: &[],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent
    ));

    let log_identity = LogStreamIdentity {
        task_instance_id,
        pickup_generation: 1,
    };
    let mut log_stream = RedisLogStagingStream::connect(
        fixture.client("logstaging", LOG_PASSWORD),
        log_identity.clone(),
        RedisLogStagingConfig::new(&namespace),
        RedisDurabilityGuard::default(),
        Arc::new(OpenLogCapability),
    )
    .await?;
    assert_eq!(
        log_stream
            .accept(LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: log_identity,
                    sequence: 0,
                },
                b"accepted terminal log\n".to_vec(),
            ))
            .await?,
        AcceptOutcome::Accepted
    );
    assert_eq!(
        log_stream.finish_cleanly(LogExit::Status(0)).await?,
        TerminalOutcome::Recorded
    );

    let archive_directory = tempfile::tempdir()?;
    let archive = FileArchive::new(archive_directory.path().to_path_buf());
    capability.fence();
    assert_eq!(
        staging
            .drain_claimed(
                delivery.clone(),
                "default",
                &scope_store,
                vec![log_stream],
                &archive,
            )
            .await,
        Err(RedisCompactionStagingError::Unavailable)
    );
    let retained = staging.quota_state().await?;
    assert_eq!(retained.staged_envelopes, 2);
    assert_eq!(retained.pending_deliveries, 1);
    assert_eq!(archive.steps(), Vec::<&'static str>::new());

    capability.restore();
    let reopened_log = RedisLogStagingStream::connect(
        fixture.client("logstaging", LOG_PASSWORD),
        LogStreamIdentity {
            task_instance_id,
            pickup_generation: 1,
        },
        RedisLogStagingConfig::new(&namespace),
        RedisDurabilityGuard::default(),
        Arc::new(OpenLogCapability),
    )
    .await?;
    staging
        .drain_claimed(
            delivery,
            "default",
            &scope_store,
            vec![reopened_log],
            &archive,
        )
        .await?;
    assert_eq!(archive.steps(), ["write", "verify", "commit"]);
    let after_commit = staging.quota_state().await?;
    assert_eq!(after_commit.staged_envelopes, 1);
    assert_eq!(after_commit.pending_deliveries, 0);
    assert_eq!(after_commit.sealed_references, 0);
    assert_eq!(after_commit.archive_commits, 0);
    assert_eq!(
        staging.stage(payload.clone()).await?,
        RedisCompactionStageOutcome::ReplayedCompleted
    );
    assert!(matches!(
        scope_store
            .read_tickr_ctx_scope(workflow_instance_id, Utc::now())
            .await?,
        ScopeReadOutcome::Archived(_)
    ));

    let mut pressure_config = RedisCompactionStagingConfig::new(&namespace, "pressure-consumer");
    pressure_config.max_payload_bytes = NonZeroUsize::new(5 * 1024).expect("non-zero");
    pressure_config.max_envelopes = NonZeroUsize::new(8).expect("non-zero");
    pressure_config.soft_limit_bytes = 1024;
    pressure_config.hard_limit_bytes = 12 * 1024;
    let pressure = RedisCompactionStaging::connect(
        compaction_client.clone(),
        pressure_config,
        RedisDurabilityGuard::default(),
        Arc::clone(&capability) as Arc<dyn RedisCompactionStagingCapability>,
    )
    .await?;
    let pressure_payload = compaction_payload(Uuid::new_v4(), Uuid::new_v4(), &"p".repeat(2000));
    assert_eq!(
        pressure.stage(pressure_payload.clone()).await?,
        RedisCompactionStageOutcome::Staged
    );
    assert_eq!(
        pressure.quota_state().await?.pressure,
        RedisQuotaPressure::SoftThreshold
    );
    let blocked_payload = compaction_payload(Uuid::new_v4(), Uuid::new_v4(), &"q".repeat(2000));
    assert_eq!(
        pressure.stage(blocked_payload).await,
        Err(RedisCompactionStagingError::CapacityFenced)
    );
    assert_eq!(
        pressure.stage(pressure_payload).await?,
        RedisCompactionStageOutcome::ReplayedPending
    );

    let mut role_connection = compaction_client.get_multiplexed_tokio_connection().await?;
    let cross_role: redis::RedisResult<()> = role_connection
        .hset(
            format!("tickr:{{{namespace}}}:log-staging:quota"),
            "forbidden",
            1,
        )
        .await;
    assert!(cross_role.is_err());
    let administrative: redis::RedisResult<()> = redis::cmd("FLUSHALL")
        .query_async(&mut role_connection)
        .await;
    assert!(administrative.is_err());

    Ok(())
}

#[test]
#[ignore = "requires Docker, OpenSSL, and subprocess crash recovery"]
fn real_process_crashes_at_every_redis_compaction_boundary_converge() {
    let runtime = tokio::runtime::Runtime::new().expect("create crash-matrix runtime");
    runtime.block_on(async {
        let namespace = format!("compaction-crash-{}", Uuid::new_v4().simple());
        let fixture = RedisFixture::start(&namespace).await;
        let archive_root = tempfile::tempdir().expect("create crash archive directory");
        let executable = std::env::current_exe().expect("resolve current test executable");
        let boundaries = [
            "after-staging-mutation",
            "after-fsync-proof",
            "before-cross-plane-ack",
            "after-cross-plane-ack",
            "after-drain-receipt",
            "after-scope-seal",
            "after-log-seal",
            "after-archive-write",
            "after-archive-verification",
            "after-archive-commit",
            "after-log-purge",
            "after-scope-purge",
            "before-staging-completion",
            "after-staging-completion",
        ];
        for (index, boundary) in boundaries.into_iter().enumerate() {
            let workflow_instance_id =
                Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("workflow-{index}").as_bytes());
            let task_instance_id =
                Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("task-{index}").as_bytes());
            let archive_directory = archive_root.path().join(workflow_instance_id.to_string());
            let mut crash = Command::new(&executable);
            configure_child(
                &mut crash,
                &fixture,
                &namespace,
                workflow_instance_id,
                task_instance_id,
                &archive_directory,
            );
            crash.env("TICKR_REDIS_COMPACTION_CRASH_AT", boundary);
            let status = crash.status().expect("run crashing Compaction child");
            assert_eq!(
                status.code(),
                Some(86),
                "boundary `{boundary}` did not crash at the requested point"
            );
            std::thread::sleep(Duration::from_millis(5));

            let mut recover = Command::new(&executable);
            configure_child(
                &mut recover,
                &fixture,
                &namespace,
                workflow_instance_id,
                task_instance_id,
                &archive_directory,
            );
            recover.env_remove("TICKR_REDIS_COMPACTION_CRASH_AT");
            let status = recover.status().expect("run recovering Compaction child");
            assert!(status.success(), "boundary `{boundary}` did not converge");
        }
    });
}

fn configure_child(
    command: &mut Command,
    fixture: &RedisFixture,
    namespace: &str,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
    archive_directory: &Path,
) {
    command
        .args([
            "--ignored",
            "--exact",
            "child_redis_compaction_process",
            "--nocapture",
        ])
        .env("TICKR_REDIS_COMPACTION_CHILD", "1")
        .env("TICKR_REDIS_PORT", fixture.port.to_string())
        .env("TICKR_REDIS_TRUST_ROOTS", &fixture.trust_roots)
        .env("TICKR_REDIS_NAMESPACE", namespace)
        .env(
            "TICKR_REDIS_WORKFLOW_INSTANCE_ID",
            workflow_instance_id.to_string(),
        )
        .env("TICKR_REDIS_TASK_INSTANCE_ID", task_instance_id.to_string())
        .env("TICKR_REDIS_ARCHIVE_DIRECTORY", archive_directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess entry point"]
async fn child_redis_compaction_process() -> Result<()> {
    if std::env::var("TICKR_REDIS_COMPACTION_CHILD").as_deref() != Ok("1") {
        return Ok(());
    }
    let port: u16 = std::env::var("TICKR_REDIS_PORT")?.parse()?;
    let trust_roots = std::env::var("TICKR_REDIS_TRUST_ROOTS")?;
    let namespace = std::env::var("TICKR_REDIS_NAMESPACE")?;
    let workflow_instance_id = std::env::var("TICKR_REDIS_WORKFLOW_INSTANCE_ID")?.parse()?;
    let task_instance_id = std::env::var("TICKR_REDIS_TASK_INSTANCE_ID")?.parse()?;
    let archive_directory = PathBuf::from(std::env::var("TICKR_REDIS_ARCHIVE_DIRECTORY")?);
    let capability = Arc::new(ToggleCompactionCapability::open());
    let mut config =
        RedisCompactionStagingConfig::new(&namespace, format!("child-{}", std::process::id()));
    config.reclaim_idle = Duration::from_millis(1);
    let staging = RedisCompactionStaging::connect(
        external_client(port, &trust_roots, "compactionstaging", COMPACTION_PASSWORD),
        config,
        RedisDurabilityGuard::default(),
        capability as Arc<dyn RedisCompactionStagingCapability>,
    )
    .await
    .map_err(|error| anyhow::anyhow!("child connect staging: {error:?}"))?;
    let payload = stable_compaction_payload(workflow_instance_id, task_instance_id);
    staging
        .stage_for_relay(payload)
        .await
        .map_err(|error| anyhow::anyhow!("child stage: {error:?}"))?;

    let scope_store = RedisScopeStore::connect(
        external_client(port, &trust_roots, "scopestore", SCOPE_PASSWORD),
        RedisScopeStoreConfig::new(&namespace),
        RedisDurabilityGuard::default(),
        Arc::new(OpenScopeCapability),
    )
    .await?;
    match scope_store
        .read_tickr_ctx_scope(workflow_instance_id, Utc::now())
        .await?
    {
        ScopeReadOutcome::Missing => {
            let outcome = scope_store
                .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                    scope_id: workflow_instance_id,
                    namespace: "default",
                    run_id: &workflow_instance_id.to_string(),
                    claim_id: Uuid::new_v5(&workflow_instance_id, b"redis-compaction-scope"),
                    values: &[],
                    now: Utc::now(),
                })
                .await?;
            assert!(matches!(
                outcome,
                ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent
            ));
        }
        ScopeReadOutcome::Present(_) | ScopeReadOutcome::Archived(_) => {}
        ScopeReadOutcome::Bound(_) | ScopeReadOutcome::Quarantined { .. } => {
            return Err(anyhow::anyhow!("scope is not drainable"));
        }
    }

    let log_identity = LogStreamIdentity {
        task_instance_id,
        pickup_generation: 1,
    };
    let mut log_stream = RedisLogStagingStream::connect(
        external_client(port, &trust_roots, "logstaging", LOG_PASSWORD),
        log_identity.clone(),
        RedisLogStagingConfig::new(&namespace),
        RedisDurabilityGuard::default(),
        Arc::new(OpenLogCapability),
    )
    .await?;
    let replay = match log_stream.replay().await {
        Ok(replay) => replay,
        Err(RedisLogStagingError::Purged) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let already_purged = matches!(log_stream.replay().await, Err(RedisLogStagingError::Purged));
    if !already_purged {
        let has_accepted = replay.iter().any(|record| {
            matches!(
                record,
                ReplayedLogRecord::Accepted { bytes, .. }
                    if bytes == b"stable accepted log\n"
            )
        });
        let has_terminal = replay
            .iter()
            .any(|record| matches!(record, ReplayedLogRecord::Terminal { .. }));
        if !has_accepted {
            log_stream
                .accept(LogRecordSubmission::new(
                    LogRecordIdentity {
                        stream: log_identity,
                        sequence: 0,
                    },
                    b"stable accepted log\n".to_vec(),
                ))
                .await?;
        }
        if !has_terminal {
            log_stream.finish_cleanly(LogExit::Status(0)).await?;
        }
    }

    let Some(delivery) = staging
        .claim_next()
        .await
        .map_err(|error| anyhow::anyhow!("child claim: {error:?}"))?
    else {
        return Ok(());
    };
    staging
        .drain_claimed(
            delivery,
            "default",
            &scope_store,
            vec![log_stream],
            &ProcessArchive {
                root: archive_directory,
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("child drain: {error:?}"))?;
    Ok(())
}

fn external_client(port: u16, trust_roots: &str, username: &str, password: &str) -> redis::Client {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let connection = format!("rediss://{username}:{password}@localhost:{port}/")
        .parse::<ConnectionInfo>()
        .expect("Redis child role connection");
    redis::Client::build_with_tls(
        connection,
        TlsCertificates {
            client_tls: None,
            root_cert: Some(trust_roots.as_bytes().to_vec()),
        },
    )
    .expect("Redis child role client")
}

fn stable_compaction_payload(workflow_instance_id: Uuid, task_instance_id: Uuid) -> Vec<u8> {
    CompactionEnvelope {
        projection: Some(ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: Uuid::new_v5(&workflow_instance_id, b"workflow").to_string(),
            name: format!("redis-compaction-{workflow_instance_id}"),
            state: "Completed".to_owned(),
            scheduled_at: Some("2026-07-24T00:00:00Z".to_owned()),
            task_instances: vec![SnapshotTaskInstance {
                id: task_instance_id.to_string(),
                task_id: Uuid::new_v5(&task_instance_id, b"task").to_string(),
                name: "redis-compaction-task".to_owned(),
                task_type: "Regular".to_owned(),
                state: "Completed".to_owned(),
                executor_id: Some(Uuid::new_v5(&task_instance_id, b"executor").to_string()),
                attempt: 0,
                ..Default::default()
            }],
            ..Default::default()
        }),
        correlation: format!("correlation-{workflow_instance_id}"),
        shipped_at: Some("2026-07-24T00:00:01Z".to_owned()),
    }
    .encode_to_vec()
}

fn compaction_payload(
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
    correlation: &str,
) -> Vec<u8> {
    CompactionEnvelope {
        projection: Some(ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: Uuid::new_v4().to_string(),
            name: format!("redis-compaction-{correlation}"),
            state: "Completed".to_owned(),
            scheduled_at: Some(Utc::now().to_rfc3339()),
            task_instances: vec![SnapshotTaskInstance {
                id: task_instance_id.to_string(),
                task_id: Uuid::new_v4().to_string(),
                name: "redis-compaction-task".to_owned(),
                task_type: "Regular".to_owned(),
                state: "Completed".to_owned(),
                executor_id: Some(Uuid::new_v4().to_string()),
                attempt: 0,
                ..Default::default()
            }],
            ..Default::default()
        }),
        correlation: correlation.to_owned(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec()
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
        "Redis CompactionStaging fixture did not become ready: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn generate_tls(path: &Path) -> String {
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
                "/CN=Tickr Redis CompactionStaging Test CA",
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
