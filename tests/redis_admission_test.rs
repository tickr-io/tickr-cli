use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use prost::Message;
use redis::{ConnectionInfo, TlsCertificates};
use tickr::formation::{
    resolve_formation, CoordinationRole, FormationSelection, ALL_COORDINATION_ROLES,
};
use tickr::redis_admission::{
    admit_redis_capability, prove_redis_primary_local_durability, RedisAdmissionFailure,
    RedisConnectionDescriptor, RedisTopology,
};
use tickr::redis_capability_monitor::{
    RedisAdmissionCapabilityProbe, RedisBaseCapabilityFailure, RedisCapabilityFenceState,
    RedisCapabilityMonitor, RedisCapabilityMonitorError, RedisCapabilityObservation,
    RedisFormationCapabilityProbe, RedisGenerationPermit, RedisReconstructionCallback,
    RedisReconstructionFailure, RedisRoleCapabilityFailure, RedisRoleCapabilityProbe,
    RedisRoleCapabilityRegistration, RedisRoleProbeContext,
};
use tickr::redis_capacity::{
    calibrated_role_capacity, terminal_cleanup_boundary, RedisCapacityFailure,
    RedisCapacityProfile, RedisQuotaAdmission, RedisQuotaCleanupBoundary, RedisQuotaFailure,
    RedisQuotaGuard, RedisQuotaPressure, CALIBRATED_ROLE_CAPACITIES, ROLE_MEMORY_LIMIT_NAME,
};
use tickr::redis_durability::{
    RedisConditionalSetMutation, RedisDurabilityFailure, RedisDurabilityGuard,
    RedisMutationDisposition,
};
use tickr::redis_formation_identity::{
    inspect_redis_namespace, prove_redis_probe_canary, RedisDurabilityConfiguration,
    RedisFormationAdmissionCandidate, RedisNamespaceIdentity, RedisNamespaceInspection,
    RedisRoleLimits,
};
use tickr::redis_operation_manifest::{RedisOperationManifest, RedisOperationManifestSet};
use tickr::{
    redis_command_bus::redis_command_bus_operation_manifest,
    redis_compaction_staging::redis_compaction_staging_operation_manifest,
    redis_event_ingress::redis_event_ingress_operation_manifest,
    redis_executor_fleet_status::redis_executor_fleet_status_operation_manifest,
    redis_ingress_idempotency::redis_ingress_idempotency_operation_manifest,
    redis_lifecycle_work::redis_lifecycle_work_operation_manifest,
    redis_log_staging::redis_log_staging_operation_manifest,
    redis_scope_store::redis_scope_store_operation_manifest,
    redis_signal_applied_notifier::redis_signal_applied_notifier_operation_manifest,
    redis_task_cancellation::redis_task_cancellation_operation_manifest,
    redis_task_events::redis_task_events_operation_manifest,
    redis_task_liveness::redis_liveness_watchdog_operation_manifest,
    redis_task_pickup::{
        redis_task_dispatch_operation_manifest, MonitoredRedisTaskDispatchCapability,
        RedisTaskDispatch, RedisTaskDispatchAcceptance, RedisTaskDispatchConfig,
    },
};
use tickr_executor::{
    local_pickup::{
        prepare_pickup, LocalAttemptOutcome, LocalPickupClaim, NoopPickupCheckpoint,
        PickupPreparation, SafeAttemptOutcomeHandoff, SafePickupWriter, TaskProcessLauncher,
        TerminalElection,
    },
    wire::{decode_dispatch, encode_task_event, DispatchedTask, EmitKind},
};
use tickr_proto::task as tc;
use tokio::process::{Child, Command as TokioCommand};
use uuid::Uuid;

const REDIS_74_IMAGE: &str = "redis:7.4.2";
const PASSWORD: &str = "redis-admission-secret";
const QUOTA_ROLE_PASSWORD: &str = "redis-quota-secret";
const ADMITTED_CAPACITY_BYTES: u64 = 2_000_000_000;
const ADMITTED_DURABILITY: &str = "appendonly yes\nappendfsync always\nmaxmemory 2000000000\nmaxmemory-policy noeviction\nenable-debug-command yes\nuser task-events on >redis-quota-secret ~tickr:{quota-pressure:*}:quota:* -@all +eval +get +set +hget +hset +hdel +hlen +hvals";
static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

struct TlsFixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    trust_roots: String,
}

impl TlsFixture {
    fn generate() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("redis-admission-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis admission fixture directory");
        let path = directory.path().to_path_buf();
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
                    "/CN=Tickr Redis Admission Test CA",
                    "-days",
                    "1",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                ]),
            "generate test CA",
        );
        run(
            Command::new("openssl")
                .args(["req", "-newkey", "rsa:2048", "-nodes"])
                .arg("-keyout")
                .arg(&server_key)
                .arg("-out")
                .arg(&server_request)
                .args(["-subj", "/CN=localhost"]),
            "generate server certificate request",
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
            "sign server certificate",
        );

        let trust_roots = fs::read_to_string(ca_cert).expect("read test CA");
        Self {
            _directory: directory,
            path,
            trust_roots,
        }
    }
}

struct RedisProcess {
    name: String,
    port: u16,
}

impl RedisProcess {
    async fn start(
        fixture: &TlsFixture,
        image: &str,
        server_binary: &str,
        extra_config: &str,
    ) -> Self {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-admission-{}-{sequence}", std::process::id());
        let config_name = format!("redis-{sequence}.conf");
        fs::write(
            fixture.path.join(&config_name),
            format!(
                "port 0\n\
                 tls-port 6379\n\
                 tls-cert-file /tls/server.crt\n\
                 tls-key-file /tls/server.key\n\
                 tls-ca-cert-file /tls/ca.crt\n\
                 tls-auth-clients no\n\
                 protected-mode no\n\
                 requirepass {PASSWORD}\n\
                 {extra_config}\n"
            ),
        )
        .expect("write Redis configuration");

        let mount = format!("{}:/tls:ro", fixture.path.display());
        run(
            Command::new("docker")
                .args([
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &name,
                    "--publish",
                    "127.0.0.1::6379",
                    "--volume",
                    &mount,
                    image,
                    server_binary,
                ])
                .arg(format!("/tls/{config_name}")),
            "start Redis test process",
        );

        let output = Command::new("docker")
            .args(["port", &name, "6379/tcp"])
            .output()
            .expect("query Redis test port");
        assert!(output.status.success(), "query Redis test port failed");
        let binding = String::from_utf8(output.stdout).expect("Docker port output is UTF-8");
        let port = binding
            .trim()
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .expect("Docker returned a TCP port");

        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return Self { name, port };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("Redis TLS listener did not become ready");
    }

    fn descriptor(&self, fixture: &TlsFixture, password: &str) -> RedisConnectionDescriptor {
        parse_descriptor(
            &[format!("rediss://localhost:{}/", self.port)],
            "direct",
            &fixture.trust_roots,
            password,
        )
    }
}

impl Drop for RedisProcess {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

fn run(command: &mut Command, operation: &str) {
    let status = command.status().unwrap_or_else(|error| {
        panic!("{operation} could not start: {error}");
    });
    assert!(status.success(), "{operation} failed with {status}");
}

fn parse_descriptor(
    urls: &[String],
    topology: &str,
    trust_roots: &str,
    password: &str,
) -> RedisConnectionDescriptor {
    let endpoints = urls
        .iter()
        .map(|url| {
            serde_json::json!({
                "url": url,
                "username": "default",
                "password": password,
            })
        })
        .collect::<Vec<_>>();
    RedisConnectionDescriptor::parse_json(
        &serde_json::json!({
            "topology": topology,
            "endpoints": endpoints,
            "trust_roots_pem": trust_roots,
        })
        .to_string(),
    )
    .expect("parse test descriptor")
}

async fn await_admission(
    descriptor: &RedisConnectionDescriptor,
) -> Result<
    tickr::redis_admission::AdmittedRedisCapability,
    tickr::redis_admission::RedisAdmissionError,
> {
    let formation = identity_candidate();
    let mut last_error = None;
    for _ in 0..100 {
        match admit_redis_capability(descriptor, &formation).await {
            Ok(capability) => return Ok(capability),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last_error.expect("admission attempted"))
}

fn test_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct TestFormationProbe {
    fingerprint: Mutex<String>,
    capability: tickr::redis_admission::AdmittedRedisCapability,
    failure: Mutex<Option<RedisBaseCapabilityFailure>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl TestFormationProbe {
    fn new(
        candidate: &RedisFormationAdmissionCandidate,
        events: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            fingerprint: Mutex::new(candidate.capability_fingerprint().as_str().to_owned()),
            capability: tickr::redis_admission::AdmittedRedisCapability {
                server_version: "7.4.2".to_owned(),
                topology: RedisTopology::SingleWritablePrimary,
                server_time_micros: 1,
                capacity_profile: candidate.capacity_profile().clone(),
                used_memory_bytes: 1,
            },
            failure: Mutex::new(None),
            events,
        }
    }
}

#[async_trait]
impl RedisFormationCapabilityProbe for TestFormationProbe {
    async fn probe(&self) -> Result<RedisCapabilityObservation, RedisBaseCapabilityFailure> {
        test_lock(&self.events).push("complete-probe");
        if let Some(failure) = *test_lock(&self.failure) {
            return Err(failure);
        }
        Ok(RedisCapabilityObservation::new(
            test_lock(&self.fingerprint).clone(),
            self.capability.clone(),
        ))
    }
}

struct RecordingAdmissionProbe {
    inner: RedisAdmissionCapabilityProbe,
    events: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl RedisFormationCapabilityProbe for RecordingAdmissionProbe {
    async fn probe(&self) -> Result<RedisCapabilityObservation, RedisBaseCapabilityFailure> {
        test_lock(&self.events).push("complete-probe");
        self.inner.probe().await
    }
}

struct TestRoleProbe {
    role: CoordinationRole,
    required_failure: Mutex<Option<RedisRoleCapabilityFailure>>,
    denial_failure: Mutex<Option<RedisRoleCapabilityFailure>>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl TestRoleProbe {
    fn new(role: CoordinationRole, events: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            role,
            required_failure: Mutex::new(None),
            denial_failure: Mutex::new(None),
            events,
        }
    }
}

#[async_trait]
impl RedisRoleCapabilityProbe for TestRoleProbe {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        assert_eq!(context.role(), self.role);
        assert!(!context.manifest_identity().as_str().is_empty());
        test_lock(&self.events).push("required-operation");
        match *test_lock(&self.required_failure) {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    async fn probe_representative_denials(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        assert_eq!(context.role(), self.role);
        test_lock(&self.events).push("representative-denial");
        match *test_lock(&self.denial_failure) {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }
}

struct TestReconstruction {
    role: CoordinationRole,
    failure: Mutex<Option<RedisReconstructionFailure>>,
    running_process: Arc<AtomicBool>,
    unresolved_evidence: Arc<AtomicU64>,
    calls: AtomicU64,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl TestReconstruction {
    fn new(
        role: CoordinationRole,
        running_process: Arc<AtomicBool>,
        unresolved_evidence: Arc<AtomicU64>,
        events: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            role,
            failure: Mutex::new(None),
            running_process,
            unresolved_evidence,
            calls: AtomicU64::new(0),
            events,
        }
    }
}

#[async_trait]
impl RedisReconstructionCallback for TestReconstruction {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        assert_eq!(context.role(), self.role);
        assert!(self.running_process.load(Ordering::SeqCst));
        test_lock(&self.events).push("reconstruction");
        self.calls.fetch_add(1, Ordering::SeqCst);
        match *test_lock(&self.failure) {
            Some(failure) => Err(failure),
            None => {
                self.unresolved_evidence.store(0, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_oss_74_tls_admission_matrix() {
    let fixture = TlsFixture::generate();
    let untrusted_fixture = TlsFixture::generate();
    let primary = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let formation = identity_candidate();

    let descriptor = primary.descriptor(&fixture, PASSWORD);
    let capability = await_admission(&descriptor).await.unwrap();
    assert!(capability.server_version.starts_with("7.4."));
    assert_eq!(capability.topology, RedisTopology::SingleWritablePrimary);
    assert!(capability.server_time_micros > 0);
    assert_eq!(
        capability.capacity_profile.configured_capacity_bytes(),
        ADMITTED_CAPACITY_BYTES
    );
    assert!(capability.capacity_profile.required_reserve_bytes() > 0);
    assert!(capability.used_memory_bytes > 0);

    let aof_disabled = RedisProcess::start(&fixture, REDIS_74_IMAGE, "redis-server", "").await;
    let aof_disabled_error = await_admission(&aof_disabled.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        aof_disabled_error.failure(),
        RedisAdmissionFailure::AofDisabled
    );

    let non_always = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        "appendonly yes\nappendfsync everysec",
    )
    .await;
    let non_always_error = await_admission(&non_always.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        non_always_error.failure(),
        RedisAdmissionFailure::AppendFsyncNotAlways
    );
    let unbounded = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        "appendonly yes\nappendfsync always\nmaxmemory-policy noeviction",
    )
    .await;
    let unbounded_error = await_admission(&unbounded.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        unbounded_error.failure(),
        RedisAdmissionFailure::InvalidCapacity(RedisCapacityFailure::UnboundedCapacity)
    );

    let evicting = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        "appendonly yes\nappendfsync always\nmaxmemory 1000000000\nmaxmemory-policy allkeys-lru",
    )
    .await;
    let evicting_error = await_admission(&evicting.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        evicting_error.failure(),
        RedisAdmissionFailure::InvalidCapacity(RedisCapacityFailure::EvictionPolicy)
    );

    let plaintext = RedisConnectionDescriptor::parse_json(
        &serde_json::json!({
            "topology": "direct",
            "endpoints": [{
                "url": format!("redis://localhost:{}/", primary.port),
                "username": "default",
                "password": PASSWORD,
            }],
            "trust_roots_pem": fixture.trust_roots,
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(
        plaintext.failure(),
        RedisAdmissionFailure::PlaintextTransport
    );

    let absent_roots = RedisConnectionDescriptor::parse_json(
        &serde_json::json!({
            "topology": "direct",
            "endpoints": [{
                "url": format!("rediss://localhost:{}/", primary.port),
                "username": "default",
                "password": PASSWORD,
            }],
            "trust_roots_pem": "",
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(
        absent_roots.failure(),
        RedisAdmissionFailure::MissingTrustRoots
    );

    let untrusted = primary.descriptor(&untrusted_fixture, PASSWORD);
    let untrusted_error = admit_redis_capability(&untrusted, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        untrusted_error.failure(),
        RedisAdmissionFailure::TlsValidation
    );

    let wrong_host = parse_descriptor(
        &[format!("rediss://127.0.0.1:{}/", primary.port)],
        "direct",
        &fixture.trust_roots,
        PASSWORD,
    );
    let hostname_error = admit_redis_capability(&wrong_host, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        hostname_error.failure(),
        RedisAdmissionFailure::TlsValidation
    );

    let wrong_password = primary.descriptor(&fixture, "wrong-credential");
    let credential_error = admit_redis_capability(&wrong_password, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        credential_error.failure(),
        RedisAdmissionFailure::CredentialRejected
    );

    let duplicate_primary = parse_descriptor(
        &[
            format!("rediss://localhost:{}/", primary.port),
            format!("rediss://localhost:{}/", primary.port),
        ],
        "direct",
        &fixture.trust_roots,
        PASSWORD,
    );
    let duplicate_error = admit_redis_capability(&duplicate_primary, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        duplicate_error.failure(),
        RedisAdmissionFailure::MultipleWritablePrimaries
    );

    let sentinel = RedisConnectionDescriptor::parse_json(
        &serde_json::json!({
            "topology": "sentinel",
            "endpoints": [{
                "url": format!("rediss://localhost:{}/", primary.port),
                "username": "default",
                "password": PASSWORD,
            }],
            "trust_roots_pem": fixture.trust_roots,
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(sentinel.failure(), RedisAdmissionFailure::SentinelTopology);

    let replica = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        "appendonly yes\nappendfsync always\nreplicaof 127.0.0.1 1\nreplica-serve-stale-data yes",
    )
    .await;
    let replica_error = await_admission(&replica.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        replica_error.failure(),
        RedisAdmissionFailure::ReadOnlyOrReplica
    );

    let cluster = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        "appendonly yes\nappendfsync always\ncluster-enabled yes\ncluster-config-file /data/nodes.conf\ncluster-node-timeout 5000",
    )
    .await;
    let cluster_error = await_admission(&cluster.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        cluster_error.failure(),
        RedisAdmissionFailure::ClusterTopology
    );

    for (image, expected_failure) in [
        ("redis:7.2.6", RedisAdmissionFailure::ServerVersion),
        ("redis:8.0.0", RedisAdmissionFailure::ServerVersion),
    ] {
        let process =
            RedisProcess::start(&fixture, image, "redis-server", ADMITTED_DURABILITY).await;
        let error = await_admission(&process.descriptor(&fixture, PASSWORD))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), expected_failure);
    }

    let compatible = RedisProcess::start(
        &fixture,
        "valkey/valkey:8.0.2",
        "valkey-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let compatible_error = await_admission(&compatible.descriptor(&fixture, PASSWORD))
        .await
        .unwrap_err();
    assert_eq!(
        compatible_error.failure(),
        RedisAdmissionFailure::ServerIdentity
    );

    let diagnostics = [
        plaintext.to_string(),
        absent_roots.to_string(),
        aof_disabled_error.to_string(),
        non_always_error.to_string(),
        untrusted_error.to_string(),
        hostname_error.to_string(),
        credential_error.to_string(),
        duplicate_error.to_string(),
        replica_error.to_string(),
        cluster_error.to_string(),
        compatible_error.to_string(),
    ]
    .join(" ");
    for forbidden in [
        "localhost",
        "127.0.0.1",
        PASSWORD,
        "wrong-credential",
        "BEGIN CERTIFICATE",
        "Tickr Redis Admission Test CA",
        "credential=",
    ] {
        assert!(
            !diagnostics.contains(forbidden),
            "diagnostic leaked {forbidden}"
        );
    }
}

#[test]
fn fixture_paths_are_never_part_of_admission_diagnostics() {
    let secret_path = Path::new("/secret/redis/trust-roots.pem");
    let error = RedisConnectionDescriptor::parse_json("not-json").unwrap_err();
    assert!(!error
        .to_string()
        .contains(&secret_path.display().to_string()));
}

fn operation_manifests() -> Vec<RedisOperationManifest> {
    vec![
        redis_command_bus_operation_manifest().unwrap(),
        redis_task_dispatch_operation_manifest().unwrap(),
        redis_task_events_operation_manifest().unwrap(),
        redis_task_cancellation_operation_manifest().unwrap(),
        redis_compaction_staging_operation_manifest().unwrap(),
        redis_lifecycle_work_operation_manifest().unwrap(),
        redis_log_staging_operation_manifest().unwrap(),
        redis_scope_store_operation_manifest().unwrap(),
        redis_ingress_idempotency_operation_manifest().unwrap(),
        redis_liveness_watchdog_operation_manifest().unwrap(),
        redis_signal_applied_notifier_operation_manifest().unwrap(),
        redis_executor_fleet_status_operation_manifest().unwrap(),
        redis_event_ingress_operation_manifest().unwrap(),
    ]
}

fn identity_candidate() -> RedisFormationAdmissionCandidate {
    identity_candidate_with_capacity(ADMITTED_CAPACITY_BYTES, false)
}

fn identity_candidate_with_capacity(
    configured_capacity_bytes: u64,
    use_calibrated_minima: bool,
) -> RedisFormationAdmissionCandidate {
    let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
    let role_limits = ALL_COORDINATION_ROLES
        .iter()
        .map(|role| {
            let calibration = calibrated_role_capacity(*role);
            RedisRoleLimits::new(
                *role,
                BTreeMap::from([
                    (
                        ROLE_MEMORY_LIMIT_NAME.to_owned(),
                        if use_calibrated_minima {
                            calibration.minimum_bytes
                        } else {
                            calibration.default_bytes
                        },
                    ),
                    ("max-records".to_owned(), 1_000),
                ]),
                BTreeMap::from([("completed-seconds".to_owned(), 3_600)]),
            )
            .unwrap()
        })
        .collect();
    RedisFormationAdmissionCandidate::construct(
        &descriptor,
        operation_manifests(),
        RedisNamespaceIdentity::new("integration-formation").unwrap(),
        role_limits,
        RedisDurabilityConfiguration::primary_local_aof(configured_capacity_bytes),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_identity_inspection_and_canary_are_namespace_scoped() {
    let fixture = TlsFixture::generate();
    let process = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let descriptor = process.descriptor(&fixture, PASSWORD);
    await_admission(&descriptor).await.unwrap();

    let connection_info = format!("rediss://default:{PASSWORD}@localhost:{}/", process.port)
        .parse::<ConnectionInfo>()
        .unwrap();
    let certificates = TlsCertificates {
        client_tls: None,
        root_cert: Some(fixture.trust_roots.as_bytes().to_vec()),
    };
    let client = redis::Client::build_with_tls(connection_info, certificates).unwrap();
    let mut connection = client.get_multiplexed_tokio_connection().await.unwrap();
    let candidate = identity_candidate();

    let _: () = redis::cmd("SET")
        .arg("operator-owned-key")
        .arg("untouched")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        inspect_redis_namespace(&mut connection, &candidate)
            .await
            .unwrap(),
        RedisNamespaceInspection::Empty
    );

    prove_redis_probe_canary(&mut connection, &candidate, "namespace")
        .await
        .unwrap();
    let remaining_canaries: Vec<String> = redis::cmd("KEYS")
        .arg("tickr:integration-formation:admission:canary:*")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(remaining_canaries.is_empty());
    let outside_value: String = redis::cmd("GET")
        .arg("operator-owned-key")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(outside_value, "untouched");

    let _: () = redis::cmd("SET")
        .arg(candidate.identity_key())
        .arg(candidate.normalized_identity_json())
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(candidate.fingerprint_key())
        .arg(candidate.capability_fingerprint().as_str())
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        inspect_redis_namespace(&mut connection, &candidate)
            .await
            .unwrap(),
        RedisNamespaceInspection::Matching
    );
}

async fn durability_connection(port: u16, trust_roots: &str) -> redis::aio::MultiplexedConnection {
    redis_connection(port, trust_roots, "default", PASSWORD).await
}

async fn redis_connection(
    port: u16,
    trust_roots: &str,
    username: &str,
    password: &str,
) -> redis::aio::MultiplexedConnection {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let connection_info = format!("rediss://{username}:{password}@localhost:{port}/")
        .parse::<ConnectionInfo>()
        .unwrap();
    let certificates = TlsCertificates {
        client_tls: None,
        root_cert: Some(trust_roots.as_bytes().to_vec()),
    };
    redis::Client::build_with_tls(connection_info, certificates)
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap()
}

fn info_u64(info: &str, field: &str) -> u64 {
    info.lines()
        .filter_map(|line| line.strip_suffix('\r').unwrap_or(line).split_once(':'))
        .find_map(|(name, value)| (name == field).then(|| value.parse().unwrap()))
        .unwrap()
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_role_quota_calibration_covers_all_roles_and_real_pressure() {
    let resolved_formation = resolve_formation(&FormationSelection::all_redis()).unwrap();
    let production_manifests = operation_manifests();
    assert_eq!(production_manifests.len(), ALL_COORDINATION_ROLES.len());
    for role in ALL_COORDINATION_ROLES {
        let manifest = production_manifests
            .iter()
            .find(|manifest| manifest.role() == role)
            .unwrap_or_else(|| panic!("{role:?} lacks its production operation manifest"));
        assert_eq!(
            manifest.protocol(),
            resolved_formation.roles.get(role).protocol,
            "{role:?} production manifest protocol differs from the resolved formation"
        );
    }
    let manifests =
        RedisOperationManifestSet::admit(&resolved_formation, production_manifests).unwrap();

    let fixture = TlsFixture::generate();
    let process = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let descriptor = process.descriptor(&fixture, PASSWORD);
    await_admission(&descriptor).await.unwrap();
    let mut admin_connection = durability_connection(process.port, &fixture.trust_roots).await;
    let mut connection = redis_connection(
        process.port,
        &fixture.trust_roots,
        "task-events",
        QUOTA_ROLE_PASSWORD,
    )
    .await;
    let projection = RedisCapacityProfile::default_candidate(ADMITTED_CAPACITY_BYTES)
        .unwrap()
        .projection(0);

    assert_eq!(
        CALIBRATED_ROLE_CAPACITIES.len(),
        ALL_COORDINATION_ROLES.len()
    );
    for (index, calibration) in CALIBRATED_ROLE_CAPACITIES.iter().enumerate() {
        let role = ALL_COORDINATION_ROLES[index];
        assert_eq!(calibration.role, role);
        let manifest = manifests.get(role);
        assert_eq!(manifest.role(), role);
        assert_eq!(
            manifest.protocol(),
            resolved_formation.roles.get(role).protocol
        );
        assert_eq!(
            calibration.protocol_identity,
            format!(
                "{}/{}",
                manifest.protocol().name,
                manifest.protocol().version
            )
        );
        assert!(!manifest.commands().is_empty());
        assert!(!calibration.accounted_objects.is_empty());
        assert!(calibration
            .accounted_objects
            .split(',')
            .all(|object| !object.trim().is_empty()));
        assert_eq!(
            calibration.cleanup_boundary,
            terminal_cleanup_boundary(role)
        );
        let projected = &projection.role_limits[index];
        assert_eq!(projected.protocol_identity, calibration.protocol_identity);
        assert_eq!(projected.accounted_objects, calibration.accounted_objects);
        assert_eq!(projected.max_bytes, calibration.default_bytes);
        assert!(calibration.measurement.total_bytes() <= calibration.minimum_bytes);
        assert!(calibration.minimum_bytes < calibration.default_bytes);
        assert_eq!(calibration.default_bytes, calibration.maximum_bytes);

        let role_name = &projected.role;
        let guard = RedisQuotaGuard::new(
            "quota-pressure",
            role,
            calibration.measurement.protocol_records_bytes
                + calibration.measurement.pending_delivery_metadata_bytes,
            calibration.measurement.total_bytes(),
        )
        .unwrap();
        let objects = [
            (
                "protocol-records",
                calibration.measurement.protocol_records_bytes,
            ),
            (
                "pending-delivery-metadata",
                calibration.measurement.pending_delivery_metadata_bytes,
            ),
            (
                "script-overhead",
                calibration.measurement.script_overhead_bytes,
            ),
            (
                "aof-progress-headroom",
                calibration.measurement.aof_progress_headroom_bytes,
            ),
            (
                "restart-reconstruction-headroom",
                calibration
                    .measurement
                    .restart_reconstruction_headroom_bytes,
            ),
        ];

        let memory_before = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory",
        );
        let script_before = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory_scripts_eval",
        );
        let aof_before = info_u64(
            &redis::cmd("INFO")
                .arg("persistence")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "aof_current_size",
        );

        for (object_index, (identity, units)) in objects.iter().enumerate() {
            let RedisQuotaAdmission::Accepted(state) = guard
                .accept(&mut connection, identity, *units)
                .await
                .unwrap()
            else {
                panic!(
                    "{} did not accept calibrated object {identity}",
                    calibration.protocol_identity
                );
            };
            let expected_pressure = if object_index == 0 {
                RedisQuotaPressure::BelowSoftThreshold
            } else if object_index + 1 == objects.len() {
                RedisQuotaPressure::HardLimit
            } else {
                RedisQuotaPressure::SoftThreshold
            };
            assert_eq!(state.pressure, expected_pressure);
        }

        let RedisQuotaAdmission::Fenced(fenced) = guard
            .accept(&mut connection, "beyond-hard-limit", 1)
            .await
            .unwrap()
        else {
            panic!(
                "{} accepted state beyond its hard limit",
                calibration.protocol_identity
            );
        };
        assert_eq!(fenced.used, calibration.measurement.total_bytes());
        for (identity, units) in objects {
            guard
                .verify_accepted(&mut connection, identity, units)
                .await
                .unwrap();
        }

        let wrong_boundary =
            if calibration.cleanup_boundary == RedisQuotaCleanupBoundary::DispatchTerminal {
                RedisQuotaCleanupBoundary::TaskEventRelayed
            } else {
                RedisQuotaCleanupBoundary::DispatchTerminal
            };
        let unsafe_cleanup = guard
            .release_at_terminal_boundary(
                &mut connection,
                objects[0].0,
                objects[0].1,
                wrong_boundary,
            )
            .await
            .unwrap_err();
        assert_eq!(
            unsafe_cleanup.failure(),
            RedisQuotaFailure::UnsafeCleanupBoundary
        );
        guard
            .verify_accepted(&mut connection, objects[0].0, objects[0].1)
            .await
            .unwrap();

        let mut reconstructed = redis_connection(
            process.port,
            &fixture.trust_roots,
            "task-events",
            QUOTA_ROLE_PASSWORD,
        )
        .await;
        let reconstructed_state = guard.audit_exact(&mut reconstructed).await.unwrap();
        assert_eq!(
            reconstructed_state.used,
            calibration.measurement.total_bytes()
        );
        for (identity, units) in objects {
            guard
                .verify_accepted(&mut reconstructed, identity, units)
                .await
                .unwrap();
        }

        let memory_after = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory",
        );
        let script_after = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory_scripts_eval",
        );
        let aof_after = info_u64(
            &redis::cmd("INFO")
                .arg("persistence")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "aof_current_size",
        );
        assert!(
            memory_after.saturating_sub(memory_before)
                <= calibration.measurement.protocol_records_bytes
                    + calibration.measurement.pending_delivery_metadata_bytes
        );
        assert!(
            script_after.saturating_sub(script_before)
                <= calibration.measurement.script_overhead_bytes
        );
        assert!(
            aof_after.saturating_sub(aof_before)
                <= calibration.measurement.aof_progress_headroom_bytes
        );
        let reconstruction_memory = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory",
        );
        assert!(
            reconstruction_memory.saturating_sub(memory_after)
                <= calibration
                    .measurement
                    .restart_reconstruction_headroom_bytes
        );

        let missing = guard
            .verify_accepted(&mut connection, "missing-accepted-identity", 1)
            .await
            .unwrap_err();
        assert_eq!(
            missing.failure(),
            RedisQuotaFailure::MissingAcceptedIdentity
        );
        assert!(missing.failure().is_capability_failure());

        guard
            .release_at_terminal_boundary(
                &mut connection,
                objects[4].0,
                objects[4].1,
                calibration.cleanup_boundary,
            )
            .await
            .unwrap();
        let used_memory = info_u64(
            &redis::cmd("INFO")
                .arg("memory")
                .query_async::<String>(&mut admin_connection)
                .await
                .unwrap(),
            "used_memory",
        );
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(used_memory)
            .query_async(&mut admin_connection)
            .await
            .unwrap();
        let oom = guard
            .accept(&mut connection, "oom-attempt", 1)
            .await
            .unwrap_err();
        assert_eq!(oom.failure(), RedisQuotaFailure::OutOfMemory);
        assert!(oom.failure().is_capability_failure());
        guard
            .verify_accepted(&mut connection, objects[0].0, objects[0].1)
            .await
            .unwrap();
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxmemory")
            .arg(ADMITTED_CAPACITY_BYTES)
            .query_async(&mut admin_connection)
            .await
            .unwrap();
        assert!(matches!(
            guard
                .accept(&mut connection, "cleanup-reopened", 1)
                .await
                .unwrap(),
            RedisQuotaAdmission::Accepted(_)
        ));

        let used_key = format!("tickr:{{quota-pressure:{role_name}}}:quota:used");
        let exact_used = calibration.measurement.total_bytes()
            - calibration
                .measurement
                .restart_reconstruction_headroom_bytes
            + 1;
        let _: () = redis::cmd("SET")
            .arg(&used_key)
            .arg(exact_used + 1)
            .query_async(&mut admin_connection)
            .await
            .unwrap();
        let inconsistent = guard.audit_exact(&mut connection).await.unwrap_err();
        assert_eq!(
            inconsistent.failure(),
            RedisQuotaFailure::AccountingInconsistent
        );
        assert!(inconsistent.failure().is_capability_failure());
        let _: () = redis::cmd("SET")
            .arg(&used_key)
            .arg(exact_used)
            .query_async(&mut admin_connection)
            .await
            .unwrap();
        assert_eq!(
            guard.audit_exact(&mut connection).await.unwrap().used,
            exact_used
        );

        for (identity, units) in &objects[..4] {
            guard
                .release_at_terminal_boundary(
                    &mut connection,
                    identity,
                    *units,
                    calibration.cleanup_boundary,
                )
                .await
                .unwrap();
        }
        let empty = guard
            .release_at_terminal_boundary(
                &mut connection,
                "cleanup-reopened",
                1,
                calibration.cleanup_boundary,
            )
            .await
            .unwrap();
        assert_eq!(empty.used, 0);
        let RedisQuotaAdmission::Accepted(reopened) = guard
            .accept(
                &mut connection,
                "post-terminal-cleanup",
                calibration.measurement.protocol_records_bytes,
            )
            .await
            .unwrap()
        else {
            panic!(
                "{} did not reopen after terminal cleanup",
                calibration.protocol_identity
            );
        };
        assert_eq!(
            reopened.used,
            calibration.measurement.protocol_records_bytes
        );
        guard
            .release_at_terminal_boundary(
                &mut connection,
                "post-terminal-cleanup",
                calibration.measurement.protocol_records_bytes,
                calibration.cleanup_boundary,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_admission_rejects_capacity_without_real_reserve() {
    let minimum_sum = CALIBRATED_ROLE_CAPACITIES
        .iter()
        .map(|calibration| calibration.minimum_bytes)
        .sum::<u64>();
    let minimum_profile_capacity = (minimum_sum * 20 + 18) / 19;

    let fixture = TlsFixture::generate();
    let config = format!(
        "appendonly yes\nappendfsync always\nmaxmemory {minimum_profile_capacity}\nmaxmemory-policy noeviction"
    );
    let process = RedisProcess::start(&fixture, REDIS_74_IMAGE, "redis-server", &config).await;
    let descriptor = process.descriptor(&fixture, PASSWORD);
    let formation = identity_candidate_with_capacity(minimum_profile_capacity, true);

    let mut observed = None;
    for _ in 0..100 {
        match admit_redis_capability(&descriptor, &formation).await {
            Err(error) if error.failure() == RedisAdmissionFailure::TlsValidation => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            result => {
                observed = Some(result);
                break;
            }
        }
    }
    let error = observed
        .expect("capacity admission was attempted")
        .unwrap_err();
    assert_eq!(
        error.failure(),
        RedisAdmissionFailure::InvalidCapacity(RedisCapacityFailure::InsufficientReserve)
    );
}

fn spawn_durability_child(
    process: &RedisProcess,
    fixture: &TlsFixture,
    prefix: &str,
    payload: &str,
    stage: &str,
    recover: bool,
    phase_path: &Path,
) -> std::process::Child {
    let _ = fs::remove_file(phase_path);
    Command::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "redis_durability_process_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TICKR_REDIS_DURABILITY_CHILD", "1")
        .env("TICKR_REDIS_DURABILITY_PORT", process.port.to_string())
        .env("TICKR_REDIS_DURABILITY_ROOTS", &fixture.trust_roots)
        .env("TICKR_REDIS_DURABILITY_PREFIX", prefix)
        .env("TICKR_REDIS_DURABILITY_PAYLOAD", payload)
        .env("TICKR_REDIS_DURABILITY_STAGE", stage)
        .env(
            "TICKR_REDIS_DURABILITY_RECOVER",
            if recover { "1" } else { "0" },
        )
        .env("TICKR_REDIS_DURABILITY_PHASE", phase_path)
        .spawn()
        .expect("spawn durability owner process")
}

async fn await_child_phase(
    child: &mut std::process::Child,
    phase_path: &Path,
    expected: &str,
) -> String {
    for _ in 0..400 {
        if let Ok(phase) = fs::read_to_string(phase_path) {
            if phase.starts_with(expected) {
                return phase;
            }
        }
        if let Some(status) = child.try_wait().expect("query child status") {
            panic!("durability owner exited before {expected}: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("durability owner did not reach {expected}");
}

async fn await_child_mutation(
    child: &mut std::process::Child,
    connection: &mut redis::aio::MultiplexedConnection,
    target_key: &str,
    payload: &[u8],
) {
    for _ in 0..400 {
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(target_key)
            .query_async(&mut *connection)
            .await
            .unwrap();
        if value.as_deref() == Some(payload) {
            return;
        }
        if let Some(status) = child.try_wait().expect("query child status") {
            panic!("durability owner exited before mutation was observed: {status}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("stable Redis mutation was not observed");
}

fn kill_child(child: &mut std::process::Child) {
    child.kill().expect("crash durability owner");
    let _ = child.wait().expect("reap durability owner");
}

fn wait_for_child(child: &mut std::process::Child) {
    let status = child.wait().expect("wait for durability owner");
    assert!(status.success(), "durability owner failed: {status}");
}

#[test]
#[ignore = "spawned by redis_primary_local_durability_crash_boundaries"]
fn redis_durability_process_child() {
    if std::env::var_os("TICKR_REDIS_DURABILITY_CHILD").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let port = std::env::var("TICKR_REDIS_DURABILITY_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let roots = std::env::var("TICKR_REDIS_DURABILITY_ROOTS").unwrap();
        let prefix = std::env::var("TICKR_REDIS_DURABILITY_PREFIX").unwrap();
        let payload = std::env::var("TICKR_REDIS_DURABILITY_PAYLOAD")
            .unwrap()
            .into_bytes();
        let stage = std::env::var("TICKR_REDIS_DURABILITY_STAGE").unwrap();
        let recover = std::env::var("TICKR_REDIS_DURABILITY_RECOVER").unwrap() == "1";
        let phase_path = PathBuf::from(std::env::var("TICKR_REDIS_DURABILITY_PHASE").unwrap());

        if stage == "before-mutation" {
            fs::write(&phase_path, "before-mutation").unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
            return;
        }

        let mutation = RedisConditionalSetMutation::new(
            format!("{prefix}:operation"),
            format!("{prefix}:value"),
            payload,
            Duration::from_secs(60),
        )
        .unwrap();
        let mut connection = durability_connection(port, &roots).await;
        let guard = RedisDurabilityGuard::new(Duration::from_secs(30), Duration::from_secs(30));
        let committed = if recover {
            guard
                .resolve_ambiguous(&mut connection, &mutation)
                .await
                .unwrap()
        } else {
            guard.execute(&mut connection, &mutation).await.unwrap()
        };
        let disposition = committed.into_output();

        fs::write(&phase_path, "proved").unwrap();
        if stage == "after-proof" {
            tokio::time::sleep(Duration::from_secs(60)).await;
            return;
        }

        fs::write(&phase_path, format!("accepted:{disposition:?}")).unwrap();
        if stage == "after-acceptance" {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn redis_primary_local_durability_crash_boundaries() {
    let fixture = TlsFixture::generate();
    let process = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let descriptor = process.descriptor(&fixture, PASSWORD);
    await_admission(&descriptor).await.unwrap();
    let formation = identity_candidate();
    let mut connection = durability_connection(process.port, &fixture.trust_roots).await;

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("no")
        .query_async(&mut connection)
        .await
        .unwrap();
    let canary_error = prove_redis_primary_local_durability(&mut connection, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        canary_error.failure(),
        RedisAdmissionFailure::LocalFsyncProofFailed
    );
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("always")
        .query_async(&mut connection)
        .await
        .unwrap();

    let phase = fixture.path.join("durability-phase");
    let before_prefix = "tickr:integration-formation:durability-test:before";
    let mut before = spawn_durability_child(
        &process,
        &fixture,
        before_prefix,
        "before-payload",
        "before-mutation",
        false,
        &phase,
    );
    await_child_phase(&mut before, &phase, "before-mutation").await;
    kill_child(&mut before);
    for key in [
        format!("{before_prefix}:operation"),
        format!("{before_prefix}:value"),
    ] {
        let absent: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(absent.is_none());
    }

    let mut before_retry = spawn_durability_child(
        &process,
        &fixture,
        before_prefix,
        "before-payload",
        "run",
        false,
        &phase,
    );
    wait_for_child(&mut before_retry);
    assert_eq!(
        fs::read_to_string(&phase).unwrap(),
        format!("accepted:{:?}", RedisMutationDisposition::Applied)
    );

    let after_mutation_prefix = "tickr:integration-formation:durability-test:after-mutation";
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("no")
        .query_async(&mut connection)
        .await
        .unwrap();
    let mut after_mutation = spawn_durability_child(
        &process,
        &fixture,
        after_mutation_prefix,
        "stable-payload",
        "run",
        false,
        &phase,
    );
    await_child_mutation(
        &mut after_mutation,
        &mut connection,
        &format!("{after_mutation_prefix}:value"),
        b"stable-payload",
    )
    .await;
    kill_child(&mut after_mutation);
    assert!(!phase.exists(), "acknowledgement crossed an unproved fsync");
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("always")
        .query_async(&mut connection)
        .await
        .unwrap();

    let mut recovered = spawn_durability_child(
        &process,
        &fixture,
        after_mutation_prefix,
        "stable-payload",
        "run",
        true,
        &phase,
    );
    wait_for_child(&mut recovered);
    assert_eq!(
        fs::read_to_string(&phase).unwrap(),
        format!("accepted:{:?}", RedisMutationDisposition::Replayed)
    );

    let conflict = RedisConditionalSetMutation::new(
        format!("{after_mutation_prefix}:operation"),
        format!("{after_mutation_prefix}:value"),
        b"different-payload".to_vec(),
        Duration::from_secs(60),
    )
    .unwrap();
    let conflict_error = RedisDurabilityGuard::default()
        .execute(&mut connection, &conflict)
        .await
        .err()
        .expect("different payload must conflict");
    assert_eq!(
        conflict_error.failure(),
        RedisDurabilityFailure::IdentityConflict
    );
    let retained: Vec<u8> = redis::cmd("GET")
        .arg(format!("{after_mutation_prefix}:value"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(retained, b"stable-payload");

    let after_proof_prefix = "tickr:integration-formation:durability-test:after-proof";
    let mut after_proof = spawn_durability_child(
        &process,
        &fixture,
        after_proof_prefix,
        "proved-payload",
        "after-proof",
        false,
        &phase,
    );
    assert_eq!(
        await_child_phase(&mut after_proof, &phase, "proved").await,
        "proved"
    );
    kill_child(&mut after_proof);
    assert_eq!(fs::read_to_string(&phase).unwrap(), "proved");

    let mut proof_recovery = spawn_durability_child(
        &process,
        &fixture,
        after_proof_prefix,
        "proved-payload",
        "run",
        true,
        &phase,
    );
    wait_for_child(&mut proof_recovery);
    assert_eq!(
        fs::read_to_string(&phase).unwrap(),
        format!("accepted:{:?}", RedisMutationDisposition::Replayed)
    );

    let after_acceptance_prefix = "tickr:integration-formation:durability-test:after-acceptance";
    let mut after_acceptance = spawn_durability_child(
        &process,
        &fixture,
        after_acceptance_prefix,
        "accepted-payload",
        "after-acceptance",
        false,
        &phase,
    );
    assert_eq!(
        await_child_phase(&mut after_acceptance, &phase, "accepted").await,
        format!("accepted:{:?}", RedisMutationDisposition::Applied)
    );
    kill_child(&mut after_acceptance);

    let _: () = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg(1)
        .query_async(&mut connection)
        .await
        .unwrap();
    let read_only_error = prove_redis_primary_local_durability(&mut connection, &formation)
        .await
        .unwrap_err();
    assert_eq!(
        read_only_error.failure(),
        RedisAdmissionFailure::ReadOnlyOrReplica
    );
}

#[tokio::test]
async fn capability_monitor_fences_immediate_failures_and_reconstructs_every_role() {
    let candidate = identity_candidate();
    let events = Arc::new(Mutex::new(Vec::new()));
    let formation_probe = Arc::new(TestFormationProbe::new(&candidate, Arc::clone(&events)));
    let monitor = RedisCapabilityMonitor::new(candidate.clone(), formation_probe.clone());
    let running_process = Arc::new(AtomicBool::new(true));
    let task_events_evidence = Arc::new(AtomicU64::new(1));
    let command_bus_evidence = Arc::new(AtomicU64::new(1));
    let task_events_probe = Arc::new(TestRoleProbe::new(
        CoordinationRole::TaskEvents,
        Arc::clone(&events),
    ));
    let command_bus_probe = Arc::new(TestRoleProbe::new(
        CoordinationRole::CommandBus,
        Arc::clone(&events),
    ));
    let task_events_reconstruction = Arc::new(TestReconstruction::new(
        CoordinationRole::TaskEvents,
        Arc::clone(&running_process),
        Arc::clone(&task_events_evidence),
        Arc::clone(&events),
    ));
    let command_bus_reconstruction = Arc::new(TestReconstruction::new(
        CoordinationRole::CommandBus,
        Arc::clone(&running_process),
        Arc::clone(&command_bus_evidence),
        Arc::clone(&events),
    ));
    let task_events_reporter = monitor
        .register_role(RedisRoleCapabilityRegistration::new(
            CoordinationRole::TaskEvents,
            task_events_probe.clone(),
            task_events_reconstruction.clone(),
        ))
        .unwrap();
    monitor
        .register_role(RedisRoleCapabilityRegistration::new(
            CoordinationRole::CommandBus,
            command_bus_probe,
            command_bus_reconstruction.clone(),
        ))
        .unwrap();

    assert_eq!(
        monitor.reconstruct_before_readiness().await.unwrap_err(),
        RedisCapabilityMonitorError::IncompleteRoleSet
    );
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Closed
    );

    monitor.run_once().await.unwrap();
    assert_eq!(
        *test_lock(&events),
        vec![
            "complete-probe",
            "required-operation",
            "representative-denial",
            "required-operation",
            "representative-denial",
            "reconstruction",
            "reconstruction",
        ]
    );
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Open
    );

    for failure in [
        RedisRoleCapabilityFailure::ReadOnly,
        RedisRoleCapabilityFailure::OutOfMemory,
        RedisRoleCapabilityFailure::LocalFsync,
        RedisRoleCapabilityFailure::Accounting,
        RedisRoleCapabilityFailure::MissingAcceptedIdentity,
        RedisRoleCapabilityFailure::UnexpectedTrim,
    ] {
        let permit = monitor.fence().guard_admission().unwrap();
        task_events_evidence.store(1, Ordering::SeqCst);
        command_bus_evidence.store(1, Ordering::SeqCst);
        task_events_reporter.report(failure);
        assert_eq!(
            monitor.fence().snapshot().state,
            RedisCapabilityFenceState::Closed
        );
        assert!(monitor.fence().guard_admission().is_err());
        assert!(monitor.fence().guard_acknowledgement(permit).is_err());
        assert!(running_process.load(Ordering::SeqCst));
        assert_eq!(task_events_evidence.load(Ordering::SeqCst), 1);

        monitor.run_once().await.unwrap();
        assert_eq!(
            monitor.fence().snapshot().state,
            RedisCapabilityFenceState::Open
        );
        assert!(monitor.fence().guard_acknowledgement(permit).is_err());
    }
    let hard_quota_permit = monitor.fence().guard_admission().unwrap();
    let failure_before_hard_quota = monitor.diagnostics().last_capability_failure;
    task_events_reporter.report_quota_state(tickr::redis_capacity::RedisQuotaState {
        used: 100,
        soft_threshold: 80,
        hard_limit: 100,
        accepted_identities: 10,
        pressure: RedisQuotaPressure::HardLimit,
    });
    let hard_quota_diagnostics = monitor.diagnostics();
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Open
    );
    assert!(monitor
        .fence()
        .guard_acknowledgement(hard_quota_permit)
        .is_ok());
    assert_eq!(
        hard_quota_diagnostics.last_capability_failure,
        failure_before_hard_quota
    );
    assert!(hard_quota_diagnostics.quota_state.iter().any(|quota| {
        quota.role == "task-events" && quota.state.pressure == RedisQuotaPressure::HardLimit
    }));

    *test_lock(&task_events_probe.required_failure) =
        Some(RedisRoleCapabilityFailure::RequiredOperation);
    assert!(monitor.run_once().await.is_err());
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Closed
    );
    *test_lock(&task_events_probe.required_failure) = None;
    task_events_evidence.store(1, Ordering::SeqCst);
    command_bus_evidence.store(1, Ordering::SeqCst);
    monitor.run_once().await.unwrap();

    *test_lock(&task_events_probe.denial_failure) =
        Some(RedisRoleCapabilityFailure::RepresentativeDenial);
    assert!(monitor.run_once().await.is_err());
    *test_lock(&task_events_probe.denial_failure) = None;
    task_events_evidence.store(1, Ordering::SeqCst);
    command_bus_evidence.store(1, Ordering::SeqCst);
    monitor.run_once().await.unwrap();

    task_events_reporter.report(RedisRoleCapabilityFailure::LocalFsync);
    task_events_evidence.store(1, Ordering::SeqCst);
    command_bus_evidence.store(1, Ordering::SeqCst);
    *test_lock(&command_bus_reconstruction.failure) =
        Some(RedisReconstructionFailure::PendingEvidenceUnavailable);
    assert!(monitor.run_once().await.is_err());
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Closed
    );
    *test_lock(&command_bus_reconstruction.failure) = None;
    monitor.run_once().await.unwrap();

    *test_lock(&formation_probe.fingerprint) = "changed-fingerprint".to_owned();
    assert!(monitor.run_once().await.is_err());
    assert_eq!(
        candidate.capability_fingerprint().as_str(),
        monitor.diagnostics().capability_fingerprint
    );
    *test_lock(&formation_probe.fingerprint) =
        candidate.capability_fingerprint().as_str().to_owned();
    task_events_evidence.store(1, Ordering::SeqCst);
    command_bus_evidence.store(1, Ordering::SeqCst);
    monitor.run_once().await.unwrap();

    task_events_reporter.report_quota_state(tickr::redis_capacity::RedisQuotaState {
        used: 90,
        soft_threshold: 80,
        hard_limit: 100,
        accepted_identities: 9,
        pressure: RedisQuotaPressure::SoftThreshold,
    });
    let diagnostics = monitor.diagnostics();
    assert_eq!(diagnostics.profile, "all-redis");
    assert_eq!(
        diagnostics.redis_implementation.as_deref(),
        Some("redis_oss")
    );
    assert_eq!(diagnostics.redis_version.as_deref(), Some("7.4.2"));
    assert_eq!(
        diagnostics.topology_class.as_deref(),
        Some("single_writable_primary")
    );
    assert_eq!(
        diagnostics.role_protocols.len(),
        ALL_COORDINATION_ROLES.len()
    );
    assert_eq!(
        diagnostics.operation_manifests.len(),
        ALL_COORDINATION_ROLES.len()
    );
    assert_eq!(
        diagnostics.normalized_limits.len(),
        ALL_COORDINATION_ROLES.len()
    );
    assert!(diagnostics
        .durability_class
        .contains("local-primary AOF fsync"));
    assert!(diagnostics
        .durability_class
        .contains("zero required replica acknowledgements"));
    assert_eq!(diagnostics.quota_state.len(), 1);
    assert!(diagnostics.capacity.is_some());
    assert!(diagnostics.last_capability_failure.is_some());
    let serialized = serde_json::to_string(&diagnostics).unwrap();
    assert!(!serialized.contains(PASSWORD));
    assert!(!serialized.contains("rediss://"));
    assert!(!serialized.contains("trust_roots"));
    assert!(task_events_reconstruction.calls.load(Ordering::SeqCst) >= 10);
    assert!(command_bus_reconstruction.calls.load(Ordering::SeqCst) >= 10);
}

#[tokio::test]
async fn complete_role_set_reconstructs_before_opening_readiness() {
    let candidate = identity_candidate();
    let events = Arc::new(Mutex::new(Vec::new()));
    let formation_probe = Arc::new(TestFormationProbe::new(&candidate, Arc::clone(&events)));
    let monitor = RedisCapabilityMonitor::new(candidate, formation_probe);
    let running_process = Arc::new(AtomicBool::new(true));
    let unresolved_evidence = Arc::new(AtomicU64::new(1));

    for role in ALL_COORDINATION_ROLES {
        monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                role,
                Arc::new(TestRoleProbe::new(role, Arc::clone(&events))),
                Arc::new(TestReconstruction::new(
                    role,
                    Arc::clone(&running_process),
                    Arc::clone(&unresolved_evidence),
                    Arc::clone(&events),
                )),
            ))
            .unwrap();
    }

    monitor.reconstruct_before_readiness().await.unwrap();

    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Open
    );
    assert_eq!(
        test_lock(&events)
            .iter()
            .filter(|event| **event == "reconstruction")
            .count(),
        ALL_COORDINATION_ROLES.len()
    );
}

#[tokio::test]
async fn periodic_capability_monitor_revalidates_registered_role_probes() {
    let candidate = identity_candidate();
    let events = Arc::new(Mutex::new(Vec::new()));
    let formation_probe = Arc::new(TestFormationProbe::new(&candidate, Arc::clone(&events)));
    let monitor = Arc::new(RedisCapabilityMonitor::new(candidate, formation_probe));
    let running_process = Arc::new(AtomicBool::new(true));
    let unresolved_evidence = Arc::new(AtomicU64::new(1));
    let role_probe = Arc::new(TestRoleProbe::new(
        CoordinationRole::TaskEvents,
        Arc::clone(&events),
    ));
    let reconstruction = Arc::new(TestReconstruction::new(
        CoordinationRole::TaskEvents,
        running_process,
        Arc::clone(&unresolved_evidence),
        events,
    ));
    monitor
        .register_role(RedisRoleCapabilityRegistration::new(
            CoordinationRole::TaskEvents,
            role_probe.clone(),
            reconstruction,
        ))
        .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let monitor_task = {
        let monitor = Arc::clone(&monitor);
        let shutdown = shutdown.clone();
        tokio::spawn(async move { monitor.run(Duration::from_millis(5), shutdown).await })
    };

    for _ in 0..100 {
        if monitor.fence().snapshot().state == RedisCapabilityFenceState::Open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Open
    );
    let ready_generation = monitor.fence().snapshot().generation;
    unresolved_evidence.store(1, Ordering::SeqCst);
    *test_lock(&role_probe.required_failure) = Some(RedisRoleCapabilityFailure::RequiredOperation);
    for _ in 0..100 {
        let snapshot = monitor.fence().snapshot();
        if snapshot.state == RedisCapabilityFenceState::Closed
            && snapshot.generation > ready_generation
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Closed
    );

    *test_lock(&role_probe.required_failure) = None;
    for _ in 0..100 {
        if monitor.fence().snapshot().state == RedisCapabilityFenceState::Open {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        monitor.fence().snapshot().state,
        RedisCapabilityFenceState::Open
    );
    shutdown.cancel();
    monitor_task.await.unwrap().unwrap();
}

async fn run_monitor_until_ready(monitor: &RedisCapabilityMonitor) {
    for _ in 0..100 {
        let _ = monitor.run_once().await;
        if monitor.fence().snapshot().state == RedisCapabilityFenceState::Open {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Redis capability monitor did not reopen readiness");
}

async fn run_monitor_until_fenced(monitor: &RedisCapabilityMonitor) {
    for _ in 0..100 {
        if monitor.run_once().await.is_err()
            && monitor.fence().snapshot().state == RedisCapabilityFenceState::Closed
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Redis capability monitor did not close its fence");
}
const RECOVERY_TASK_NAMESPACE: &str = "integration-formation";

#[derive(Clone)]
struct ExpectedTaskEvidence {
    claim: LocalPickupClaim,
    task_instance_id: String,
}

struct TaskEvidenceReconstruction {
    connection: redis::aio::MultiplexedConnection,
    expected: Mutex<Option<ExpectedTaskEvidence>>,
    calls: AtomicU64,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl TaskEvidenceReconstruction {
    fn new(
        connection: redis::aio::MultiplexedConnection,
        events: Arc<Mutex<Vec<&'static str>>>,
    ) -> Self {
        Self {
            connection,
            expected: Mutex::new(None),
            calls: AtomicU64::new(0),
            events,
        }
    }

    fn expect(&self, claim: LocalPickupClaim, task_instance_id: String) {
        *test_lock(&self.expected) = Some(ExpectedTaskEvidence {
            claim,
            task_instance_id,
        });
    }
}

#[async_trait]
impl RedisReconstructionCallback for TaskEvidenceReconstruction {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if context.role() != CoordinationRole::TaskDispatch {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let Some(expected) = test_lock(&self.expected).clone() else {
            test_lock(&self.events).push("reconstruction");
            self.calls.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        };
        let prefix = format!("tickr:{{{RECOVERY_TASK_NAMESPACE}}}:task-dispatch");
        let mut connection = self.connection.clone();
        let values: (
            Option<u64>,
            Option<String>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<u64>,
        ) = redis::pipe()
            .cmd("HGET")
            .arg(format!("{prefix}:generations"))
            .arg(&expected.claim.dispatch_key)
            .cmd("HGET")
            .arg(format!("{prefix}:owners"))
            .arg(&expected.claim.dispatch_key)
            .cmd("HGET")
            .arg(format!("{prefix}:assigned"))
            .arg(&expected.claim.dispatch_key)
            .cmd("HGET")
            .arg(format!("{prefix}:started"))
            .arg(&expected.claim.dispatch_key)
            .cmd("HGET")
            .arg(format!("{prefix}:source-completed"))
            .arg(&expected.claim.dispatch_key)
            .query_async(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        if values.0 != u64::try_from(expected.claim.pickup_generation).ok()
            || values.1.as_deref() != Some(expected.claim.owner.as_str())
            || values.4 != Some(1)
        {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        let assigned = values
            .2
            .and_then(|bytes| tc::TaskEvent::decode(bytes.as_slice()).ok())
            .filter(|event| {
                event.task_instance_id == expected.task_instance_id
                    && matches!(event.kind, Some(tc::task_event::Kind::Assigned(_)))
            });
        let started = values
            .3
            .and_then(|bytes| tc::TaskEvent::decode(bytes.as_slice()).ok())
            .filter(|event| {
                event.task_instance_id == expected.task_instance_id
                    && matches!(event.kind, Some(tc::task_event::Kind::Started(_)))
            });
        if assigned.is_none() || started.is_none() {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        test_lock(&self.events).push("reconstruction");
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone)]
struct RecoveryTaskLauncher {
    exit_signal: PathBuf,
}

impl TaskProcessLauncher for RecoveryTaskLauncher {
    async fn spawn(&self, task: &DispatchedTask) -> Result<Child, String> {
        if task.name != "redis-capability-recovery-task" {
            return Err("unexpected recovery Task payload".to_owned());
        }
        TokioCommand::new("sh")
            .arg("-c")
            .arg("while [ ! -f \"$1\" ]; do sleep 0.05; done; exit 17")
            .arg("tickr-task")
            .arg(&self.exit_signal)
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| error.to_string())
    }
}

fn recovery_task_dispatch() -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "redis-capability-recovery-task".to_owned(),
        task_type: 0,
        nix_expression_path: "/recovery".to_owned(),
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

fn recovery_task_config() -> RedisTaskDispatchConfig {
    let mut config =
        RedisTaskDispatchConfig::new(RECOVERY_TASK_NAMESPACE, "capability-recovery-executor");
    config.reclaim_idle = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_payload_bytes = NonZeroUsize::new(4096).unwrap();
    config.max_dispatches = NonZeroUsize::new(4).unwrap();
    config.max_active_claims = NonZeroUsize::new(2).unwrap();
    config.max_staged_events = NonZeroUsize::new(16).unwrap();
    config.soft_limit_bytes = 24_000;
    config.hard_limit_bytes = 40_000;
    config
}

fn recovery_redis_client(port: u16, trust_roots: &str) -> redis::Client {
    let connection_info = format!("rediss://default:{PASSWORD}@localhost:{port}/")
        .parse::<ConnectionInfo>()
        .unwrap();
    redis::Client::build_with_tls(
        connection_info,
        TlsCertificates {
            client_tls: None,
            root_cert: Some(trust_roots.as_bytes().to_vec()),
        },
    )
    .unwrap()
}

async fn task_namespace_snapshot(
    connection: &mut redis::aio::MultiplexedConnection,
) -> Vec<(String, Vec<u8>)> {
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!(
            "tickr:{{{RECOVERY_TASK_NAMESPACE}}}:task-dispatch:*"
        ))
        .query_async(connection)
        .await
        .unwrap();
    keys.sort();
    let mut snapshot = Vec::with_capacity(keys.len());
    for key in keys {
        let bytes: Vec<u8> = redis::cmd("DUMP")
            .arg(&key)
            .query_async(connection)
            .await
            .unwrap();
        snapshot.push((key, bytes));
    }
    snapshot
}

fn assert_failure_projection(
    monitor: &RedisCapabilityMonitor,
    capability: &str,
    reason: &str,
    original_fingerprint: &str,
) {
    let diagnostics = monitor.diagnostics();
    assert_eq!(diagnostics.fence.state, RedisCapabilityFenceState::Closed);
    assert!(!diagnostics.fence.ready);
    assert_eq!(diagnostics.capability_fingerprint, original_fingerprint);
    let failure = diagnostics
        .last_capability_failure
        .expect("Health retains the precise capability failure");
    assert_eq!(failure.capability, capability);
    assert_eq!(failure.reason, reason);
    let serialized = serde_json::to_string(&failure).unwrap();
    for forbidden in [PASSWORD, "rediss://", "localhost", RECOVERY_TASK_NAMESPACE] {
        assert!(!serialized.contains(forbidden));
    }
}

async fn assert_task_work_fenced(
    monitor: &RedisCapabilityMonitor,
    stale_permit: RedisGenerationPermit,
    adapter: &RedisTaskDispatch,
    claim: &LocalPickupClaim,
    terminal_event: &[u8],
) {
    assert!(monitor.fence().guard_admission().is_err());
    assert!(monitor.fence().guard_acknowledgement(stale_permit).is_err());
    assert!(adapter
        .append(
            &format!("fenced-dispatch:{}", Uuid::new_v4()),
            recovery_task_dispatch().encode_to_vec(),
        )
        .await
        .is_err());
    let now = Utc::now();
    assert!(adapter
        .renew_liveness(claim, now + chrono::Duration::seconds(30), now)
        .await
        .is_err());
    assert!(adapter
        .stage_started(claim, b"must remain pending", Utc::now())
        .await
        .is_err());
    assert!(adapter
        .elect_terminal(
            claim,
            LocalAttemptOutcome::ProcessExitedFailure,
            terminal_event,
            Utc::now(),
        )
        .await
        .is_err());
}

fn assert_reconstruction_precedes_readiness(events: &Mutex<Vec<&'static str>>) {
    assert_eq!(
        *test_lock(events),
        vec![
            "complete-probe",
            "required-operation",
            "representative-denial",
            "reconstruction",
        ]
    );
}

async fn renew_active_task(adapter: &RedisTaskDispatch, claim: &LocalPickupClaim) {
    let now = Utc::now();
    assert!(adapter
        .renew_liveness(claim, now + chrono::Duration::seconds(120), now)
        .await
        .unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, OpenSSL, and a real Task process"]
async fn redis_capability_monitor_real_process_loss_and_recovery_matrix() {
    let fixture = TlsFixture::generate();
    let process = RedisProcess::start(
        &fixture,
        REDIS_74_IMAGE,
        "redis-server",
        ADMITTED_DURABILITY,
    )
    .await;
    let candidate = identity_candidate();
    let descriptor = Arc::new(process.descriptor(&fixture, PASSWORD));
    let events = Arc::new(Mutex::new(Vec::new()));
    let formation_probe = Arc::new(RecordingAdmissionProbe {
        inner: RedisAdmissionCapabilityProbe::new(descriptor, candidate.clone()),
        events: Arc::clone(&events),
    });
    let monitor = RedisCapabilityMonitor::new(candidate.clone(), formation_probe);
    let mut admin = durability_connection(process.port, &fixture.trust_roots).await;
    let role_probe = Arc::new(TestRoleProbe::new(
        CoordinationRole::TaskDispatch,
        Arc::clone(&events),
    ));
    let reconstruction = Arc::new(TaskEvidenceReconstruction::new(
        admin.clone(),
        Arc::clone(&events),
    ));
    let reporter = monitor
        .register_role(RedisRoleCapabilityRegistration::new(
            CoordinationRole::TaskDispatch,
            role_probe,
            reconstruction.clone(),
        ))
        .unwrap();

    run_monitor_until_ready(&monitor).await;
    assert_reconstruction_precedes_readiness(&events);
    let original_fingerprint = monitor.diagnostics().capability_fingerprint;
    assert_eq!(
        monitor.diagnostics().durability_class,
        "one local-primary AOF fsync, zero required replica acknowledgements"
    );

    let capability = Arc::new(MonitoredRedisTaskDispatchCapability::new(
        monitor.fence(),
        reporter,
    ));
    let adapter = RedisTaskDispatch::connect(
        recovery_redis_client(process.port, &fixture.trust_roots),
        recovery_task_config(),
        RedisDurabilityGuard::default(),
        capability,
    )
    .await
    .unwrap();
    let dispatch = recovery_task_dispatch();
    assert_eq!(
        adapter
            .append(
                &format!("active-dispatch:{}", dispatch.task_instance_id),
                dispatch.encode_to_vec(),
            )
            .await
            .unwrap(),
        RedisTaskDispatchAcceptance::Appended
    );
    let owner = Uuid::new_v4().to_string();
    let prepared = match prepare_pickup(
        &adapter,
        &NoopPickupCheckpoint,
        &owner,
        Uuid::parse_str(&owner).unwrap(),
        chrono::Duration::seconds(120),
    )
    .await
    .unwrap()
    {
        PickupPreparation::Ready(prepared) => prepared,
        other => panic!("expected a prepared Redis Task pickup, got {other:?}"),
    };
    let started_event = encode_task_event(
        &prepared.task,
        Uuid::parse_str(&owner).unwrap(),
        EmitKind::Started,
    );
    assert!(adapter
        .stage_started(&prepared.claim, &started_event, Utc::now())
        .await
        .unwrap());
    reconstruction.expect(prepared.claim.clone(), dispatch.task_instance_id.clone());

    let scratch = tempfile::tempdir().unwrap();
    let exit_signal = scratch.path().join("exit-task");
    let launcher = RecoveryTaskLauncher {
        exit_signal: exit_signal.clone(),
    };
    let mut task_process = launcher.spawn(&prepared.task).await.unwrap();
    assert!(task_process.try_wait().unwrap().is_none());
    let terminal_event = encode_task_event(
        &decode_dispatch(&dispatch.encode_to_vec()).unwrap(),
        Uuid::parse_str(&owner).unwrap(),
        EmitKind::Failed,
    );

    let prefix = format!("tickr:{{{RECOVERY_TASK_NAMESPACE}}}:task-dispatch");
    let source_completed: Option<u64> = redis::cmd("HGET")
        .arg(format!("{prefix}:source-completed"))
        .arg(&prepared.claim.dispatch_key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(source_completed, Some(1));

    test_lock(&events).clear();
    let before_transport = task_namespace_snapshot(&mut admin).await;
    let transport_permit = monitor.fence().guard_admission().unwrap();
    run(
        Command::new("docker").args(["pause", &process.name]),
        "pause Redis capability process",
    );
    run_monitor_until_fenced(&monitor).await;
    assert_failure_projection(
        &monitor,
        "transport",
        "Redis transport capability was lost",
        &original_fingerprint,
    );
    assert!(task_process.try_wait().unwrap().is_none());
    assert_task_work_fenced(
        &monitor,
        transport_permit,
        &adapter,
        &prepared.claim,
        &terminal_event,
    )
    .await;
    run(
        Command::new("docker").args(["unpause", &process.name]),
        "unpause Redis capability process",
    );
    assert_eq!(
        task_namespace_snapshot(&mut admin).await,
        before_transport,
        "transport fencing must reject before Redis mutation",
    );
    test_lock(&events).clear();
    run_monitor_until_ready(&monitor).await;
    assert_reconstruction_precedes_readiness(&events);
    renew_active_task(&adapter, &prepared.claim).await;

    test_lock(&events).clear();
    let before_read_only = task_namespace_snapshot(&mut admin).await;
    let read_only_permit = monitor.fence().guard_admission().unwrap();
    let _: () = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg(1)
        .query_async(&mut admin)
        .await
        .unwrap();
    run_monitor_until_fenced(&monitor).await;
    assert_failure_projection(
        &monitor,
        "topology",
        "the admitted writable-primary topology was lost",
        &original_fingerprint,
    );
    assert!(task_process.try_wait().unwrap().is_none());
    assert_task_work_fenced(
        &monitor,
        read_only_permit,
        &adapter,
        &prepared.claim,
        &terminal_event,
    )
    .await;
    let _: () = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        task_namespace_snapshot(&mut admin).await,
        before_read_only,
        "read-only fencing must reject before Redis mutation",
    );
    test_lock(&events).clear();
    run_monitor_until_ready(&monitor).await;
    assert_reconstruction_precedes_readiness(&events);
    renew_active_task(&adapter, &prepared.claim).await;

    test_lock(&events).clear();
    let before_durability_drift = task_namespace_snapshot(&mut admin).await;
    let drift_permit = monitor.fence().guard_admission().unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("everysec")
        .query_async(&mut admin)
        .await
        .unwrap();
    run_monitor_until_fenced(&monitor).await;
    assert_failure_projection(
        &monitor,
        "aof_fsync",
        "the admitted primary-local AOF fsync capability was lost",
        &original_fingerprint,
    );
    assert!(task_process.try_wait().unwrap().is_none());
    assert_task_work_fenced(
        &monitor,
        drift_permit,
        &adapter,
        &prepared.claim,
        &terminal_event,
    )
    .await;
    assert_eq!(
        monitor.run_once().await,
        Err(RedisCapabilityMonitorError::CapabilityLost),
        "a weaker appendfsync policy must not reopen readiness",
    );
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendfsync")
        .arg("always")
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        task_namespace_snapshot(&mut admin).await,
        before_durability_drift,
        "durability drift fencing must reject before Redis mutation",
    );
    test_lock(&events).clear();
    run_monitor_until_ready(&monitor).await;
    assert_reconstruction_precedes_readiness(&events);
    renew_active_task(&adapter, &prepared.claim).await;

    test_lock(&events).clear();
    let before_fsync_loss = task_namespace_snapshot(&mut admin).await;
    let fsync_permit = monitor.fence().guard_admission().unwrap();
    let _: () = redis::cmd("DEBUG")
        .arg("AOF-FLUSH-SLEEP")
        .arg(3_000_000)
        .query_async(&mut admin)
        .await
        .unwrap();
    run_monitor_until_fenced(&monitor).await;
    assert_failure_projection(
        &monitor,
        "aof_fsync",
        "the admitted primary-local AOF fsync capability was lost",
        &original_fingerprint,
    );
    assert!(task_process.try_wait().unwrap().is_none());
    assert_task_work_fenced(
        &monitor,
        fsync_permit,
        &adapter,
        &prepared.claim,
        &terminal_event,
    )
    .await;

    fs::write(&exit_signal, "exit").unwrap();
    let task_status = task_process.wait().await.unwrap();
    assert_eq!(task_status.code(), Some(17));
    assert!(adapter
        .elect_terminal(
            &prepared.claim,
            LocalAttemptOutcome::ProcessExitedFailure,
            &terminal_event,
            Utc::now(),
        )
        .await
        .is_err());
    let retained_while_fenced: Option<Vec<u8>> = redis::cmd("HGET")
        .arg(format!("{prefix}:terminal-events"))
        .arg(&prepared.claim.dispatch_key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert!(retained_while_fenced.is_none());

    let _: () = redis::cmd("DEBUG")
        .arg("AOF-FLUSH-SLEEP")
        .arg(0)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        task_namespace_snapshot(&mut admin).await,
        before_fsync_loss,
        "failed local-fsync proof must retain the pre-loss durable Task evidence",
    );
    test_lock(&events).clear();
    run_monitor_until_ready(&monitor).await;
    assert_reconstruction_precedes_readiness(&events);
    assert_eq!(
        adapter
            .elect_terminal(
                &prepared.claim,
                LocalAttemptOutcome::ProcessExitedFailure,
                &terminal_event,
                Utc::now(),
            )
            .await
            .unwrap(),
        TerminalElection::Won,
    );
    let retained_terminal: Option<Vec<u8>> = redis::cmd("HGET")
        .arg(format!("{prefix}:terminal-events"))
        .arg(&prepared.claim.dispatch_key)
        .query_async(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        retained_terminal.as_deref(),
        Some(terminal_event.as_slice())
    );
    let decoded_terminal = tc::TaskEvent::decode(terminal_event.as_slice()).unwrap();
    assert_eq!(decoded_terminal.task_instance_id, dispatch.task_instance_id);
    assert!(matches!(
        decoded_terminal.kind,
        Some(tc::task_event::Kind::Failed(_))
    ));
    assert!(adapter
        .complete_staged_handoff(&prepared.claim)
        .await
        .unwrap());

    assert_eq!(
        monitor.diagnostics().capability_fingerprint,
        original_fingerprint
    );
    assert!(monitor.fence().guard_admission().is_ok());
    assert_eq!(reconstruction.calls.load(Ordering::SeqCst), 5);
}
