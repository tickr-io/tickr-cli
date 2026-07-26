#![cfg(not(madsim))]

use std::{
    fs,
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use redis::{ConnectionInfo, TlsCertificates};
use sqlx::sqlite::SqlitePoolOptions;
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_lifecycle_work::{
        RedisLifecyclePublishOutcome, RedisLifecycleWork, RedisLifecycleWorkCapability,
        RedisLifecycleWorkConfig, RedisLifecycleWorkQuotaState,
    },
};
use tickr_conductor::{
    build_pipeline::{
        start_local_definition_build_worker_with_claim_admission, BuildExecutor, BuildOutcome,
        LocalDefinitionBuildWorkerConfig, TaskBuildJob,
    },
    lifecycle_work::{LifecycleClaimAdmission, LifecyclePipeline, LifecycleWork},
    patch_pipeline::{
        local::{start_local_patch_worker_with_claim_admission, PatchReconcilerConfig},
        process_patch, ParsedPatch, PatchIngress, PatchProvenance, PatchRelaySender, PatchSource,
    },
    proto::{ConductorRelayMessage, EntityType},
    relay::init_relay_tx,
    submission_consumer::{
        start_local_definition_submission_worker_with_claim_admission,
        LocalDefinitionSubmissionWorkerConfig,
    },
};
use tickr_migrations::{
    apply_sqlite,
    backend::{RepositoryFactory, WriterRepositoryBundle},
    definition_repository::{
        DefinitionBuildSettlementOutcome, DefinitionLifecycleStatus, DefinitionRegistrationInput,
        DefinitionRegistrationOutcome, DefinitionSubmissionPointer,
        DefinitionSubmissionReconciliationOutcome, DefinitionTaskBuildResult,
    },
    sqlite_writer_options, MigrationTarget,
};
use tickr_proto::{config::DataPlaneSql, patch as pp, workflow as wf};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ROLE_PASSWORD: &str = "redis-lifecycle-secret";
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default)]
struct OpenCapability {
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisLifecycleWorkQuotaState>>,
}

impl RedisLifecycleWorkCapability for OpenCapability {
    fn delivery_open(&self) -> bool {
        true
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisLifecycleWorkQuotaState) {
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
            .prefix("redis-lifecycle-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-lifecycle-{}-{sequence}", std::process::id());
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
                 user default on >redis-lifecycle-aof-loader ~* &* +@all\n\
                 user lifecycle on >{ROLE_PASSWORD} ~tickr:{{{namespace}}}:lifecycle-work:* &tickr:{{{namespace}}}:lifecycle-work:wakeup:* -@all \
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
            "start Redis LifecycleWork fixture",
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
            "rediss://lifecycle:{ROLE_PASSWORD}@localhost:{}/",
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
            "restart Redis LifecycleWork fixture",
        );
        let output = Command::new("docker")
            .args(["port", &self.name, "6379/tcp"])
            .output()
            .expect("query restarted Redis port");
        assert!(output.status.success(), "query restarted Redis port failed");
        self.port = String::from_utf8(output.stdout)
            .expect("Docker port is UTF-8")
            .trim()
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse().ok())
            .expect("Docker returned restarted Redis port");
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
        "Redis LifecycleWork fixture did not become ready: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[derive(Default)]
struct ToggleClaims {
    open: AtomicBool,
    calls: Mutex<Vec<LifecyclePipeline>>,
}

impl ToggleClaims {
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
    }

    fn calls(&self) -> Vec<LifecyclePipeline> {
        lock(&self.calls).clone()
    }
}

impl LifecycleClaimAdmission for ToggleClaims {
    fn claims_open(&self, pipeline: LifecyclePipeline) -> bool {
        lock(&self.calls).push(pipeline);
        self.open.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct SuccessExecutor {
    builds: AtomicUsize,
    changed: Notify,
}

#[async_trait]
impl BuildExecutor for SuccessExecutor {
    async fn build(&self, _job: &TaskBuildJob) -> BuildOutcome {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        BuildOutcome::Success
    }
}

impl SuccessExecutor {
    async fn wait_for(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.builds.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.changed.notified().await;
            }
        })
        .await
        .expect("lifecycle build did not complete");
    }
}

#[derive(Default)]
struct CountingPatchSender(Mutex<Vec<pp::PatchEnvelope>>);

#[async_trait]
impl PatchRelaySender for CountingPatchSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()> {
        lock(&self.0).push(envelope.clone());
        Ok(())
    }
}

struct FailingPatchSender;

#[async_trait]
impl PatchRelaySender for FailingPatchSender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> Result<()> {
        anyhow::bail!("relay suppressed during ingress")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn real_redis_lifecycle_laws_bound_hints_and_recover_all_sql_pipelines() {
    let namespace = format!("lifecycle-law-{}", NEXT_REDIS.load(Ordering::Relaxed));
    let mut fixture = RedisFixture::start(&namespace).await;
    let capability = Arc::new(OpenCapability::default());
    let mut config = RedisLifecycleWorkConfig::new(&namespace);
    config.hint_ttl = Duration::from_millis(80);
    config.sweep_interval = Duration::from_millis(10);
    config.soft_hint_limit = NonZeroUsize::new(1).unwrap();
    config.hard_hint_limit = NonZeroUsize::new(2).unwrap();
    let role = RedisLifecycleWork::connect(fixture.client(), config.clone(), capability.clone())
        .await
        .unwrap();
    let mut wakeups = role.subscribe().await.unwrap();

    assert_eq!(
        role.publish(LifecyclePipeline::DefinitionBuild).await,
        Ok(RedisLifecyclePublishOutcome::Queued)
    );
    assert_eq!(
        role.publish(LifecyclePipeline::DefinitionBuild).await,
        Ok(RedisLifecyclePublishOutcome::Coalesced)
    );
    assert_eq!(
        role.publish(LifecyclePipeline::PatchBuild).await,
        Ok(RedisLifecyclePublishOutcome::Queued)
    );
    assert_eq!(
        role.publish(LifecyclePipeline::Submission).await,
        Ok(RedisLifecyclePublishOutcome::DroppedAtHardLimit)
    );
    let pressured = role.quota_state().await.unwrap();
    assert_eq!(pressured.queued_hints, 2);
    assert_eq!(pressured.coalesced_hints, 1);
    assert_eq!(pressured.dropped_hints, 1);
    assert_eq!(pressured.pressure, RedisQuotaPressure::HardLimit);

    let first = wakeups.recv().await.unwrap();
    let second = wakeups.recv().await.unwrap();
    assert_eq!(
        [first, second],
        [
            LifecyclePipeline::DefinitionBuild,
            LifecyclePipeline::PatchBuild
        ]
    );
    assert_eq!(role.quota_state().await.unwrap().queued_hints, 0);

    assert_eq!(
        role.publish(LifecyclePipeline::Submission).await,
        Ok(RedisLifecyclePublishOutcome::Queued)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    role.sweep_expired().await.unwrap();
    let expired = role.quota_state().await.unwrap();
    assert_eq!(expired.queued_hints, 0);
    assert!(expired.expired_hints >= 1);

    fixture.restart().await;
    let recovered = RedisLifecycleWork::connect(fixture.client(), config, capability.clone())
        .await
        .unwrap();
    let mut recovered_wakeups = recovered.subscribe().await.unwrap();
    assert_eq!(
        recovered.publish(LifecyclePipeline::Submission).await,
        Ok(RedisLifecyclePublishOutcome::Queued)
    );
    assert_eq!(
        recovered_wakeups.recv().await,
        Ok(LifecyclePipeline::Submission)
    );

    let directory = tempfile::TempDir::new().unwrap();
    let url = format!(
        "sqlite://{}",
        directory.path().join("lifecycle.db").display()
    );
    let migration_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    apply_sqlite(MigrationTarget::Conductor, &migration_pool)
        .await
        .unwrap();
    migration_pool.close().await;
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url });
    let writer = factory.open_writer().await.unwrap();

    let build_workflow = Uuid::new_v4();
    register_definition(&writer, build_workflow, Uuid::new_v4()).await;
    let submission_workflow = Uuid::new_v4();
    let submission_task = Uuid::new_v4();
    register_definition(&writer, submission_workflow, submission_task).await;
    assert!(matches!(
        writer
            .settle_definition_task_build(
                submission_workflow,
                1,
                submission_task,
                DefinitionTaskBuildResult::Success,
            )
            .await
            .unwrap(),
        DefinitionBuildSettlementOutcome::Ready(_)
    ));

    let patch_instance = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    assert!(matches!(
        process_patch(
            &writer,
            &FailingPatchSender,
            patch_instance,
            patch_id,
            parsed_patch_with_task(),
            PatchProvenance::External,
        )
        .await
        .unwrap(),
        PatchIngress::Accepted { .. }
    ));
    assert!(matches!(
        process_patch(
            &writer,
            &FailingPatchSender,
            Uuid::new_v4(),
            Uuid::new_v4(),
            parsed_patch_without_task(),
            PatchProvenance::External,
        )
        .await
        .unwrap(),
        PatchIngress::Accepted { .. }
    ));
    writer.close().await;

    let writer = Arc::new(factory.open_writer().await.unwrap());
    let gate = Arc::new(ToggleClaims::default());
    let definition_executor = Arc::new(SuccessExecutor::default());
    let patch_executor = Arc::new(SuccessExecutor::default());
    let patch_sender = Arc::new(CountingPatchSender::default());
    let (relay_tx, mut relay_rx) = mpsc::channel::<ConductorRelayMessage>(8);
    init_relay_tx(relay_tx).await;

    let lifecycle_work = LifecycleWork::new(
        Box::new(recovered.subscribe().await.unwrap()),
        gate.clone(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (mut lifecycle_source, lifecycle_wakeups, lifecycle_inputs) = lifecycle_work.into_parts();
    let (build_notifications, patch_notifications, submission_notifications, claim_admission) =
        lifecycle_inputs.into_parts();

    let cancel = CancellationToken::new();
    let lifecycle_source_cancel = cancel.clone();
    let lifecycle_source_worker = tokio::spawn(async move {
        lifecycle_source
            .run(lifecycle_wakeups, lifecycle_source_cancel)
            .await
    });
    let build_worker = tokio::spawn(start_local_definition_build_worker_with_claim_admission(
        writer.clone(),
        definition_executor.clone(),
        "redis-lifecycle-definition".to_owned(),
        build_notifications,
        claim_admission.clone(),
        LocalDefinitionBuildWorkerConfig {
            scan_interval: Duration::from_millis(20),
            lease_duration: Duration::from_secs(1),
            batch_size: NonZeroUsize::new(8).unwrap(),
        },
        cancel.clone(),
    ));
    let patch_worker = tokio::spawn(start_local_patch_worker_with_claim_admission(
        writer.clone(),
        patch_executor.clone(),
        patch_sender.clone(),
        "redis-lifecycle-patch".to_owned(),
        patch_notifications,
        claim_admission.clone(),
        PatchReconcilerConfig {
            scan_interval: Duration::from_millis(20),
            build_lease_duration: Duration::from_secs(1),
            lifecycle_lease_duration: Duration::from_secs(1),
            lifecycle_min_age: Duration::ZERO,
            batch_size: NonZeroUsize::new(8).unwrap(),
        },
        cancel.clone(),
    ));
    let submission_worker = tokio::spawn(
        start_local_definition_submission_worker_with_claim_admission(
            writer.clone(),
            "redis-lifecycle-submission".to_owned(),
            submission_notifications,
            claim_admission,
            LocalDefinitionSubmissionWorkerConfig {
                scan_interval: Duration::from_millis(20),
                lease_duration: Duration::from_secs(1),
                batch_size: NonZeroUsize::new(8).unwrap(),
            },
            cancel.clone(),
        ),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(definition_executor.builds.load(Ordering::SeqCst), 0);
    assert_eq!(patch_executor.builds.load(Ordering::SeqCst), 0);
    assert!(lock(&patch_sender.0).is_empty());
    assert!(relay_rx.try_recv().is_err());
    let now = chrono::Utc::now();
    assert!(writer.has_reclaimable_definition_build(now).await.unwrap());
    assert!(writer.has_reclaimable_patch_build(now).await.unwrap());
    assert!(writer
        .has_reclaimable_patch_lifecycle(now, now)
        .await
        .unwrap());
    assert!(writer
        .has_reclaimable_definition_submission(now)
        .await
        .unwrap());
    let fenced_discovery = gate.calls();
    assert!(fenced_discovery.contains(&LifecyclePipeline::DefinitionBuild));
    assert!(
        fenced_discovery
            .iter()
            .filter(|pipeline| **pipeline == LifecyclePipeline::PatchBuild)
            .count()
            >= 2
    );
    assert!(fenced_discovery.contains(&LifecyclePipeline::Submission));

    gate.open();
    definition_executor.wait_for(1).await;
    patch_executor.wait_for(1).await;
    let mut relayed = Vec::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(5), relay_rx.recv())
            .await
            .expect("submission was not relayed")
            .expect("relay channel closed");
        assert_eq!(message.entity_type, EntityType::SubmitWorkflow as i32);
        let definition =
            <wf::WorkflowDefinition as prost::Message>::decode(message.payload.as_slice()).unwrap();
        relayed.push(Uuid::parse_str(&definition.id).unwrap());
    }
    relayed.sort_unstable();
    let mut expected = vec![build_workflow, submission_workflow];
    expected.sort_unstable();
    assert_eq!(relayed, expected);
    wait_until_submitted(writer.as_ref(), build_workflow).await;
    wait_until_submitted(writer.as_ref(), submission_workflow).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !lock(&patch_sender.0).is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Patch apply intent was not recovered");

    cancel.cancel();
    build_worker.await.unwrap().unwrap();
    patch_worker.await.unwrap().unwrap();
    submission_worker.await.unwrap().unwrap();
    lifecycle_source_worker.await.unwrap().unwrap();
    writer.close().await;
    assert!(lock(&capability.failures).is_empty());
}

async fn register_definition(writer: &WriterRepositoryBundle, workflow_id: Uuid, task_id: Uuid) {
    let outcome = writer
        .register_definition(DefinitionRegistrationInput {
            definition: wf::WorkflowDefinition {
                id: workflow_id.to_string(),
                tenant_id: Uuid::from_u128(999).to_string(),
                namespace: "default".to_owned(),
                slug: format!("lifecycle-{workflow_id}"),
                name: "Lifecycle recovery".to_owned(),
                tasks: vec![wf::TaskDefinition {
                    id: task_id.to_string(),
                    workflow_id: workflow_id.to_string(),
                    name: "build".to_owned(),
                    nix_expression_path: format!("flake#{task_id}"),
                    ..Default::default()
                }],
                ..Default::default()
            },
            content_hash: format!("content-{workflow_id}"),
            cosmetic_hash: format!("cosmetic-{workflow_id}"),
            nickel_source: "lifecycle-recovery".to_owned(),
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        DefinitionRegistrationOutcome::Inserted {
            workflow_version: 1,
            ..
        }
    ));
}

fn parsed_patch_with_task() -> ParsedPatch {
    let task = wf::TaskDefinition {
        id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::nil().to_string(),
        name: "patched".to_owned(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: "flake#patched".to_owned(),
        max_attempts: 3,
        ..Default::default()
    };
    ParsedPatch {
        ops: vec![pp::AddressedPatchOp {
            op: Some(pp::addressed_patch_op::Op::AddNode(
                pp::addressed_patch_op::AddNode {
                    node_id: task.id.clone(),
                    task: Some(task),
                },
            )),
        }],
        operation: None,
        reason: Some("recover Patch build".to_owned()),
        stall_ttl: None,
        source: PatchSource::nickel("{ ops = [ patched ] }"),
    }
}

fn parsed_patch_without_task() -> ParsedPatch {
    ParsedPatch {
        ops: Vec::new(),
        operation: None,
        reason: Some("recover Patch lifecycle".to_owned()),
        stall_ttl: None,
        source: PatchSource::json("{}"),
    }
}

async fn wait_until_submitted(writer: &WriterRepositoryBundle, workflow_id: Uuid) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if writer
                .definition_submission_reconciliation_outcome(DefinitionSubmissionPointer {
                    workflow_id,
                    workflow_version: 1,
                })
                .await
                .unwrap()
                == DefinitionSubmissionReconciliationOutcome::NotReady(
                    DefinitionLifecycleStatus::Submitted,
                )
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("definition did not settle as Submitted");
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
                "/CN=Tickr Redis Lifecycle Test CA",
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
