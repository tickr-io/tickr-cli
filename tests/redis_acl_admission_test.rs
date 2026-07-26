use std::{
    collections::BTreeMap,
    fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use prost::Message as _;
use redis::{aio::MultiplexedConnection, ConnectionInfo, TlsCertificates};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::ExecCommand;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr::all_redis_formation::AllRedisProcessAdmission;
use tickr::tickr_ctx_endpoint::{DistributedTickrCtx, TickrCtxEndpoint, TickrCtxScopeWriter};
use tickr::{
    formation::{
        resolve_formation, CoordinationRole, FinalLogStore, FormationProfile, FormationSelection,
        SqlImplementation, ALL_COORDINATION_ROLES,
    },
    redis_acl_admission::{
        admit_complete_redis_formation, canonical_redis_operation_manifests,
        compose_and_reconstruct_all_redis, RedisAclAdmissionFailure, RedisCanonicalAclPolicy,
        RedisRoleCredential, RedisRoleCredentialSet,
    },
    redis_admission::RedisConnectionDescriptor,
    redis_capability_monitor::{
        RedisAdmissionCapabilityProbe, RedisCapabilityMonitor, RedisRoleCapabilityFailure,
    },
    redis_capacity::{calibrated_role_capacity, RedisQuotaPressure, ROLE_MEMORY_LIMIT_NAME},
    redis_durability::RedisDurabilityGuard,
    redis_event_ingress::{
        RedisEventIngress, RedisEventIngressAcceptance, RedisEventIngressCapability,
        RedisEventIngressConfig, RedisEventIngressError, RedisEventIngressQuotaState,
    },
    redis_formation_identity::{
        inspect_redis_namespace, RedisDurabilityConfiguration, RedisFormationAdmissionCandidate,
        RedisNamespaceIdentity, RedisNamespaceInspection, RedisRoleLimits,
    },
    redis_operation_manifest::RedisForbiddenTarget,
};
use tickr_api::{
    commands::client::BusError, config::LogStorageConfig, http::logs_resolver::LogsResolver,
};
use tickr_conductor::{
    api_commands_consumer::CommandBusHandler,
    build_pipeline::{
        definition_build_notifications, start_local_definition_build_worker,
        LocalDefinitionBuildWorkerConfig, TestBuildExecutor,
    },
    ingress_idempotency::{
        IngressEffects, IngressTerminalOutcome, RelayIntent, ReservationOutcome,
    },
    nats_ingress::{run_event_consumer, IngressWorkingSet, RelaySendOutcome, RelaySender},
    proto::ConductorRelayMessage,
    register_pipeline::{process_register_local, RegisterOutcome, RegisterRequest},
    relay::init_relay_tx,
    signal_applied_notifier::SignalAppliedReconciliationWake,
    signal_captures,
    submission_consumer::{
        definition_submission_notifications, start_local_definition_submission_worker,
        LocalDefinitionSubmissionWorkerConfig,
    },
    system_tasks::run_selected_compaction_drain,
    trigger_pipeline::{
        process_reserved_trigger_with_scope_store, ReservedTriggerEffects, TriggerError,
        TriggerRequest,
    },
    wakeup_translator::{WakeupOutcome, WakeupRelaySender, WakeupRequest},
};
use tickr_executor::{
    app::run_executor_with_formation_roles,
    local_pickup::{
        LocalExecutorCapacity, LocalPickupClaim, PickupBoundary, PickupCheckpoint, PickupOutcome,
        SafePickupExecutor, TaskProcessLauncher,
    },
    log_stream::LogStreamRoute,
    wire::DispatchedTask,
};
use tickr_migrations::{
    apply_target,
    backend::WriterRepositoryBundle,
    scope_repository::{CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput},
    MigrationTarget,
};
use tickr_proto::{
    archive::{ArchiveProjection, CompactionEnvelope},
    coord::log_stream::{
        AcceptOutcome, LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity,
        ReplayedLogRecord, TerminalOutcome,
    },
    derive_scheduled_workflow_instance_id,
    instance::SnapshotTaskInstance,
    signal as sp, task as tc, tickr_api as api,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ADMIN_PASSWORD: &str = "redis-acl-admission-admin-secret";
const CAPACITY_BYTES: u64 = 2_000_000_000;
static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

struct TlsFixture {
    _directory: tempfile::TempDir,
    path: PathBuf,
    trust_roots: String,
}

impl TlsFixture {
    fn generate() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("redis-acl-admission-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create ACL fixture directory");
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
                    "/CN=Tickr Redis ACL Test CA",
                    "-days",
                    "1",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                ]),
            "generate ACL test CA",
        );
        run(
            Command::new("openssl")
                .args(["req", "-newkey", "rsa:2048", "-nodes"])
                .arg("-keyout")
                .arg(&server_key)
                .arg("-out")
                .arg(&server_request)
                .args(["-subj", "/CN=localhost"]),
            "generate ACL server certificate request",
        );
        fs::write(
            &extensions,
            "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n",
        )
        .expect("write ACL certificate extensions");
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
            "sign ACL server certificate",
        );

        Self {
            trust_roots: fs::read_to_string(ca_cert).expect("read ACL test CA"),
            _directory: directory,
            path,
        }
    }
}

struct RedisProcess {
    name: String,
    port: u16,
}

impl RedisProcess {
    async fn start(fixture: &TlsFixture, role_policy: &str) -> Self {
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-acl-{}-{sequence}", std::process::id());
        let config_name = format!("redis-acl-{sequence}.conf");
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
                 requirepass {ADMIN_PASSWORD}\n\
                 appendonly yes\n\
                 appendfsync always\n\
                 maxmemory {CAPACITY_BYTES}\n\
                 maxmemory-policy noeviction\n\
                 enable-debug-command yes\n\
                 {role_policy}\n"
            ),
        )
        .expect("write Redis ACL configuration");
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
                    REDIS_IMAGE,
                    "redis-server",
                ])
                .arg(format!("/tls/{config_name}")),
            "start Redis ACL fixture",
        );
        let output = Command::new("docker")
            .args(["port", &name, "6379/tcp"])
            .output()
            .expect("query Redis ACL test port");
        assert!(output.status.success(), "query Redis ACL test port failed");
        let binding = String::from_utf8(output.stdout).expect("Docker port output is UTF-8");
        let port = binding
            .trim()
            .rsplit_once(':')
            .and_then(|(_, value)| value.parse().ok())
            .expect("Docker returned a TCP port");
        let process = Self { name, port };
        for _ in 0..100 {
            if admin_connection(&process, fixture).await.is_ok() {
                return process;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("Redis ACL TLS listener did not become ready");
    }

    fn descriptor_json(&self, fixture: &TlsFixture) -> String {
        serde_json::json!({
            "topology": "direct",
            "endpoints": [{
                "url": format!("rediss://localhost:{}/", self.port),
                "username": "default",
                "password": ADMIN_PASSWORD,
            }],
            "trust_roots_pem": fixture.trust_roots,
        })
        .to_string()
    }

    fn descriptor(&self, fixture: &TlsFixture) -> RedisConnectionDescriptor {
        RedisConnectionDescriptor::parse_json(&self.descriptor_json(fixture))
            .expect("parse ACL admission descriptor")
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
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("{operation} could not start: {error}"));
    assert!(status.success(), "{operation} failed with {status}");
}

async fn admin_connection(
    process: &RedisProcess,
    fixture: &TlsFixture,
) -> Result<MultiplexedConnection, redis::RedisError> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let connection_info = format!(
        "rediss://default:{ADMIN_PASSWORD}@localhost:{}/",
        process.port
    )
    .parse::<ConnectionInfo>()
    .expect("parse admin connection");
    let client = redis::Client::build_with_tls(
        connection_info,
        TlsCertificates {
            client_tls: None,
            root_cert: Some(fixture.trust_roots.as_bytes().to_vec()),
        },
    )
    .expect("build admin TLS client");
    client.get_multiplexed_tokio_connection().await
}

#[derive(Clone)]
struct RoleMaterial {
    role: CoordinationRole,
    identity: String,
    secret: String,
}

struct PingCommandHandler;

#[async_trait]
impl CommandBusHandler for PingCommandHandler {
    async fn handle(&self, payload: Vec<u8>) -> Vec<u8> {
        let request =
            api::ApiCommandRequest::decode(payload.as_slice()).expect("production Command request");
        assert!(matches!(
            request.body,
            Some(api::api_command_request::Body::Ping(_))
        ));
        api::ApiCommandResponse {
            status_code: 200,
            payload: Some(api::api_command_response::Payload::Ping(
                api::PingPayload {},
            )),
        }
        .encode_to_vec()
    }
}

#[derive(Clone)]
struct SmokeTaskLauncher {
    exited: Arc<AtomicBool>,
}

impl TaskProcessLauncher for SmokeTaskLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<tokio::process::Child, String> {
        tokio::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .map_err(|error| error.to_string())
    }

    async fn process_exited(
        &self,
        _task: &DispatchedTask,
        _claim: &LocalPickupClaim,
        status: &std::process::ExitStatus,
    ) -> Result<(), String> {
        if !status.success() {
            return Err(format!("smoke Task exited with {status}"));
        }
        self.exited.store(true, Ordering::Release);
        Ok(())
    }
}

fn role_material() -> Vec<RoleMaterial> {
    ALL_COORDINATION_ROLES
        .iter()
        .enumerate()
        .map(|(index, role)| RoleMaterial {
            role: *role,
            identity: format!("tickr-role-{index}"),
            secret: format!("tickr-role-secret-{index}"),
        })
        .collect()
}

fn credentials(
    material: &[RoleMaterial],
    wrong_secret_for: Option<CoordinationRole>,
) -> RedisRoleCredentialSet {
    RedisRoleCredentialSet::admit(
        material
            .iter()
            .map(|entry| {
                RedisRoleCredential::new(
                    entry.role,
                    entry.identity.clone(),
                    if wrong_secret_for == Some(entry.role) {
                        "wrong-role-secret".to_owned()
                    } else {
                        entry.secret.clone()
                    },
                )
            })
            .collect(),
    )
    .expect("admit complete test credentials")
}

fn candidate(namespace: &str) -> RedisFormationAdmissionCandidate {
    let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
    let role_limits = ALL_COORDINATION_ROLES
        .iter()
        .map(|role| {
            RedisRoleLimits::new(
                *role,
                BTreeMap::from([
                    (
                        ROLE_MEMORY_LIMIT_NAME.to_owned(),
                        calibrated_role_capacity(*role).default_bytes,
                    ),
                    ("max-records".to_owned(), 1_000),
                ]),
                BTreeMap::from([("retention-seconds".to_owned(), 3_600)]),
            )
            .unwrap()
        })
        .collect();
    RedisFormationAdmissionCandidate::construct(
        &descriptor,
        canonical_redis_operation_manifests().unwrap(),
        RedisNamespaceIdentity::new(namespace).unwrap(),
        role_limits,
        RedisDurabilityConfiguration::primary_local_aof(CAPACITY_BYTES),
    )
    .unwrap()
}

#[derive(Clone, Copy)]
enum AclMutation {
    None,
    RevokeEval(CoordinationRole),
    BroadenForbidden(CoordinationRole),
    AddUnregisteredPing(CoordinationRole),
}

fn fixture_policy(
    candidate: &RedisFormationAdmissionCandidate,
    material: &[RoleMaterial],
    mutation: AclMutation,
) -> String {
    let policy = RedisCanonicalAclPolicy::generate(
        candidate.operation_manifests(),
        candidate.namespace().as_str(),
    )
    .unwrap();
    let broadened_pattern = match mutation {
        AclMutation::BroadenForbidden(role) => {
            let target_role = candidate
                .operation_manifests()
                .get(role)
                .forbidden_operations()
                .iter()
                .find_map(|forbidden| match forbidden.target() {
                    RedisForbiddenTarget::CoordinationRole(target) => Some(target),
                    RedisForbiddenTarget::Administrative => None,
                })
                .expect("role has a cross-role denial");
            Some(
                policy
                    .get(target_role)
                    .key_patterns()
                    .first()
                    .expect("cross-role target has a key pattern")
                    .clone(),
            )
        }
        _ => None,
    };

    policy
        .roles()
        .iter()
        .map(|grants| {
            let role_material = material
                .iter()
                .find(|entry| entry.role == grants.role())
                .expect("role material exists");
            let mut command_rules = grants.command_rules().to_vec();
            let mut key_patterns = grants.key_patterns().to_vec();
            match mutation {
                AclMutation::RevokeEval(role) if role == grants.role() => {
                    command_rules.retain(|rule| rule != "+eval");
                }
                AclMutation::BroadenForbidden(role) if role == grants.role() => {
                    key_patterns.push(broadened_pattern.clone().unwrap());
                }
                AclMutation::AddUnregisteredPing(role) if role == grants.role() => {
                    command_rules.push("+ping".to_owned());
                }
                _ => {}
            }
            let keys = key_patterns
                .iter()
                .map(|pattern| format!("~{pattern}"))
                .collect::<Vec<_>>()
                .join(" ");
            let channels = grants
                .channel_patterns()
                .iter()
                .map(|pattern| format!("&{pattern}"))
                .collect::<Vec<_>>()
                .join(" ");
            format!(
                "user {} on sanitize-payload >{} resetkeys resetchannels -@all {} {} {}",
                role_material.identity,
                role_material.secret,
                command_rules.join(" "),
                keys,
                channels,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn assert_empty_namespace(
    process: &RedisProcess,
    fixture: &TlsFixture,
    candidate: &RedisFormationAdmissionCandidate,
) {
    let mut connection = admin_connection(process, fixture).await.unwrap();
    assert_eq!(
        inspect_redis_namespace(&mut connection, candidate)
            .await
            .unwrap(),
        RedisNamespaceInspection::Empty
    );
    let identity: Option<String> = redis::cmd("GET")
        .arg(candidate.identity_key())
        .query_async(&mut connection)
        .await
        .unwrap();
    let fingerprint: Option<String> = redis::cmd("GET")
        .arg(candidate.fingerprint_key())
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(identity.is_none());
    assert!(fingerprint.is_none());
}

async fn assert_acl_failure(
    fixture: &TlsFixture,
    namespace: &str,
    mutation: AclMutation,
    expected_role: CoordinationRole,
    expected_failure: RedisAclAdmissionFailure,
) {
    let material = role_material();
    let policy_candidate = candidate(namespace);
    let process = RedisProcess::start(
        fixture,
        &fixture_policy(&policy_candidate, &material, mutation),
    )
    .await;
    let error = match admit_complete_redis_formation(
        &process.descriptor(fixture),
        candidate(namespace),
        credentials(&material, None),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("ACL mutation was unexpectedly admitted"),
    };
    assert_eq!(error.role_context(), Some(expected_role));
    assert_eq!(error.failure(), expected_failure);
    let diagnostics = format!("{error:?} {error}");
    for entry in &material {
        assert!(!diagnostics.contains(&entry.identity));
        assert!(!diagnostics.contains(&entry.secret));
    }
    assert_empty_namespace(&process, fixture, &candidate(namespace)).await;
}

fn assert_runtime_role_failure(
    monitor: &RedisCapabilityMonitor,
    expected_role: &str,
    expected_reason: &str,
    material: &[RoleMaterial],
) {
    let diagnostics = monitor.diagnostics();
    assert!(!diagnostics.fence.ready);
    let failure = diagnostics
        .last_capability_failure
        .expect("runtime capability failure is retained");
    assert_eq!(failure.capability, "role_operation");
    assert_eq!(failure.role.as_deref(), Some(expected_role));
    assert_eq!(failure.reason, expected_reason);
    let serialized = serde_json::to_string(&failure).unwrap();
    assert!(!serialized.contains(ADMIN_PASSWORD));
    assert!(!serialized.contains("rediss://"));
    assert!(!serialized.contains("localhost"));
    for entry in material {
        assert!(!serialized.contains(&entry.identity));
        assert!(!serialized.contains(&entry.secret));
    }
}

fn runtime_quota_state(
    monitor: &RedisCapabilityMonitor,
    role: &str,
) -> tickr::redis_capacity::RedisQuotaState {
    monitor
        .diagnostics()
        .quota_state
        .into_iter()
        .find(|quota| quota.role == role)
        .unwrap_or_else(|| panic!("{role} quota state was not projected"))
        .state
}

fn redis_info_u64(info: &str, field: &str) -> u64 {
    info.lines()
        .filter_map(|line| line.strip_suffix('\r').unwrap_or(line).split_once(':'))
        .find_map(|(name, value)| (name == field).then(|| value.parse().unwrap()))
        .unwrap()
}

fn assert_complete_reconstruction(
    monitor: &RedisCapabilityMonitor,
    candidate: &RedisFormationAdmissionCandidate,
    original_fingerprint: &str,
) {
    let diagnostics = monitor.diagnostics();
    assert!(diagnostics.fence.ready);
    assert_eq!(diagnostics.capability_fingerprint, original_fingerprint);
    assert!(monitor.matches_candidate(candidate));
    let capacity = diagnostics
        .capacity
        .expect("complete capability recovery validates capacity and reserve");
    assert_eq!(capacity.configured_capacity_bytes, CAPACITY_BYTES);
    assert!(
        capacity
            .used_memory_bytes
            .checked_add(capacity.role_capacity_sum_bytes)
            .and_then(|used| used.checked_add(capacity.required_reserve_bytes))
            .is_some_and(|required| required <= capacity.configured_capacity_bytes),
        "complete capability recovery preserves calibrated limits and reserve"
    );
    assert!(
        monitor.has_complete_role_set(),
        "readiness requires every real role reconstruction callback"
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
    for quota in &diagnostics.quota_state {
        assert!(
            ALL_COORDINATION_ROLES
                .iter()
                .any(|role| recovery_role_name(*role) == quota.role),
            "unknown reconstructed role {}",
            quota.role
        );
        assert!(quota.state.used <= quota.state.hard_limit);
    }
}

#[tokio::test]
#[ignore = "requires Docker and OpenSSL"]
async fn real_tls_acl_matrix_proves_complete_probe_commit_and_recovery() {
    let fixture = TlsFixture::generate();
    let material = role_material();
    let namespace = "acl-complete";
    let policy_candidate = candidate(namespace);
    let process = RedisProcess::start(
        &fixture,
        &fixture_policy(&policy_candidate, &material, AclMutation::None),
    )
    .await;

    let wrong_role = CoordinationRole::TaskEvents;
    let credential_error = match admit_complete_redis_formation(
        &process.descriptor(&fixture),
        candidate(namespace),
        credentials(&material, Some(wrong_role)),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("wrong role secret was unexpectedly admitted"),
    };
    assert_eq!(credential_error.role_context(), Some(wrong_role));
    assert_eq!(
        credential_error.failure(),
        RedisAclAdmissionFailure::RoleCredentialRejected
    );
    let diagnostic = format!("{credential_error:?} {credential_error}");
    assert!(!diagnostic.contains("wrong-role-secret"));
    assert_empty_namespace(&process, &fixture, &candidate(namespace)).await;

    let descriptor = Arc::new(process.descriptor(&fixture));
    let runtime_candidate = candidate(namespace);

    let monitor = Arc::new(RedisCapabilityMonitor::new(
        runtime_candidate.clone(),
        Arc::new(RedisAdmissionCapabilityProbe::new(
            Arc::clone(&descriptor),
            runtime_candidate.clone(),
        )),
    ));
    let mut admitted = admit_complete_redis_formation(
        descriptor.as_ref(),
        runtime_candidate,
        credentials(&material, None),
    )
    .await
    .unwrap();
    admitted
        .compose_command_bus(&monitor, "conductor-production")
        .unwrap();
    admitted
        .compose_task_dispatch(&monitor, "executor-production")
        .unwrap();
    admitted.compose_task_cancellation(&monitor).unwrap();
    admitted
        .compose_task_events(&monitor, "conductor-production")
        .unwrap();
    admitted.compose_liveness_watchdog(&monitor).unwrap();
    admitted.compose_lifecycle_work(&monitor, None).unwrap();
    admitted.compose_executor_fleet_status(&monitor).unwrap();
    let notifier_shutdown = CancellationToken::new();
    admitted
        .compose_signal_applied_notifier(&monitor, notifier_shutdown.clone())
        .await
        .unwrap();
    admitted
        .compose_compaction_staging(&monitor, "conductor-production")
        .await
        .unwrap();
    admitted.compose_log_staging(&monitor).unwrap();
    admitted.compose_scope_store(&monitor).await.unwrap();
    admitted
        .compose_event_ingress(&monitor, "conductor-production")
        .await
        .unwrap();
    admitted
        .compose_ingress_idempotency_store(&monitor)
        .unwrap();
    let ready = admitted
        .reconstruct_before_readiness(Arc::clone(&monitor))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Redis Command-bus reconstruction failed: {error:?}; diagnostics: {:?}",
                monitor.diagnostics()
            )
        });
    let role_bundle = ready.into_role_bundle().await.unwrap();
    let original_fingerprint = monitor.diagnostics().capability_fingerprint;
    assert_complete_reconstruction(&monitor, &policy_candidate, &original_fingerprint);
    assert!(monitor.fence().snapshot().ready);

    assert_eq!(role_bundle.role_inventory(), ALL_COORDINATION_ROLES);
    assert_eq!(role_bundle.descriptor(), candidate(namespace).descriptor());
    let expected_descriptor = candidate(namespace);
    assert_eq!(
        role_bundle.admitted_descriptor().capability_fingerprint(),
        expected_descriptor.capability_fingerprint()
    );
    assert_eq!(
        role_bundle.admitted_descriptor().fingerprint_projection(),
        expected_descriptor.fingerprint_projection()
    );
    assert!(role_bundle.is_ready());
    assert!(role_bundle.executor_fleet_status().is_some());
    let signal_applied = role_bundle
        .signal_applied_roles()
        .expect("admitted SignalAppliedNotifier role");
    let signal_id = uuid::Uuid::new_v4();
    signal_applied
        .notifier()
        .notify_bytag_cancel_materialized(signal_id);
    assert!(matches!(
        signal_applied
            .reconciliation()
            .lock()
            .await
            .next_reconciliation(Duration::from_secs(2))
            .await,
        SignalAppliedReconciliationWake::Notification(notification)
            if notification.signal_id == signal_id
    ));
    notifier_shutdown.cancel();
    role_bundle
        .task_dispatch_publisher()
        .expect("admitted Conductor TaskDispatch role")
        .prepare()
        .await
        .unwrap();
    assert!(role_bundle.executor_task_handoff().is_some());
    assert!(role_bundle.task_cancellation_publisher().is_some());
    assert!(role_bundle.task_cancellation_ack_consumer().is_some());
    assert!(role_bundle.executor_task_cancellation().is_some());
    assert!(role_bundle.task_event_writer().is_some());
    assert!(role_bundle.task_event_consumer().is_some());
    role_bundle
        .compaction_staging()
        .expect("admitted Conductor CompactionStaging role")
        .prepare()
        .await
        .unwrap();
    assert!(role_bundle.compaction_log_staging().is_some());
    role_bundle
        .log_stream_provider()
        .expect("admitted Task/API LogStream provider")
        .prepare()
        .await
        .unwrap();
    assert!(role_bundle.scope_store().is_some());
    let scope_store = role_bundle
        .scope_store()
        .expect("admitted Executor ScopeStore role");
    let (writer_client, writer) = TickrCtxScopeWriter::new(Arc::clone(&scope_store));
    let (ctx_handle, endpoint) =
        TickrCtxEndpoint::bind_distributed_after_recovery(writer_client).unwrap();
    ctx_handle.mark_ready();
    let task_context = Arc::new(DistributedTickrCtx::new(ctx_handle, scope_store, namespace));
    let executor_shutdown = CancellationToken::new();
    let writer_task = tokio::spawn(writer.run(executor_shutdown.child_token()));
    let endpoint_task = tokio::spawn(endpoint.run(executor_shutdown.child_token()));
    executor_shutdown.cancel();
    run_executor_with_formation_roles(
        role_bundle.executor_task_handoff().unwrap(),
        role_bundle.task_event_writer().unwrap(),
        role_bundle.executor_task_cancellation().unwrap(),
        role_bundle.log_stream_provider().unwrap(),
        role_bundle.executor_fleet_status().unwrap(),
        task_context,
        executor_shutdown,
    )
    .await
    .unwrap();
    endpoint_task.await.unwrap().unwrap();
    writer_task.await.unwrap().unwrap();
    assert!(role_bundle.compaction_scope_reader().is_some());
    assert!(
        role_bundle.event_ingress().is_some(),
        "admitted EventIngress role must be released to the Conductor"
    );
    let ingress = role_bundle
        .ingress_coordinator()
        .expect("admitted IngressIdempotencyStore coordinator");
    let payload_hash = [0x5a; 32];
    let reservation = match ingress
        .reserve("acl-composed-producer", &payload_hash)
        .await
        .unwrap()
    {
        ReservationOutcome::Acquired(reservation) => reservation,
        _ => panic!("fresh composed producer reservation was not acquired"),
    };
    let signal_id = reservation.signal_id();
    assert!(matches!(
        ingress
            .reserve("acl-composed-producer", &payload_hash)
            .await
            .unwrap(),
        ReservationOutcome::Pending
    ));
    let effects = IngressEffects {
        signal_effect: b"stable-signal-effect".to_vec(),
        event_results: b"event-variable-results".to_vec(),
        relay_intents: vec![RelayIntent::Signal(b"relay-intent".to_vec())],
    };
    assert_eq!(
        reservation.persist_effects(effects.clone()).await.unwrap(),
        effects
    );
    let proof = reservation.operation().mark_relayed().await.unwrap();
    assert_eq!(proof.outcome(), IngressTerminalOutcome::Accepted);
    assert!(matches!(
        ingress
            .reserve("acl-composed-producer", &payload_hash)
            .await
            .unwrap(),
        ReservationOutcome::Complete(replayed)
            if replayed.outcome() == IngressTerminalOutcome::Accepted
    ));
    assert!(matches!(
        ingress
            .reserve("acl-composed-producer", &[0xa5; 32])
            .await
            .unwrap(),
        ReservationOutcome::Conflict {
            original_signal_id,
            ..
        } if original_signal_id == signal_id
    ));
    let executor_liveness = role_bundle
        .executor_liveness_watchdog()
        .expect("admitted Executor liveness role");
    assert!(executor_liveness
        .select_due_liveness(chrono::Utc::now())
        .await
        .unwrap()
        .is_none());
    let conductor_sweeper = role_bundle
        .conductor_liveness_sweeper()
        .expect("admitted Conductor liveness role");
    assert!(conductor_sweeper.sweep_one_due().await.unwrap().is_none());

    let command_bus = role_bundle
        .command_bus_client()
        .expect("admitted API Command-bus role");
    assert!(matches!(
        command_bus
            .send(
                api::ApiCommandRequest {
                    body: Some(api::api_command_request::Body::Ping(api::PingRequest {},)),
                },
                Duration::from_millis(40),
            )
            .await,
        Err(BusError::Unavailable)
    ));
    let consumer = role_bundle
        .command_bus_consumer()
        .expect("admitted Conductor Command-bus role");
    let cancel = CancellationToken::new();
    let consumer_cancel = cancel.clone();
    let consumer_task = tokio::spawn(async move {
        consumer
            .serve(Arc::new(PingCommandHandler), consumer_cancel)
            .await
            .expect("serve admitted Redis Command bus");
    });
    let response = loop {
        match command_bus
            .send(
                api::ApiCommandRequest {
                    body: Some(api::api_command_request::Body::Ping(api::PingRequest {})),
                },
                Duration::from_millis(250),
            )
            .await
        {
            Ok(response) => break response,
            Err(BusError::Unavailable) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => panic!("production Redis Command bus failed: {error:?}"),
        }
    };
    assert_eq!(response.status_code, 200);
    assert!(matches!(
        response.payload,
        Some(api::api_command_response::Payload::Ping(_))
    ));
    cancel.cancel();
    consumer_task.await.unwrap();

    let mut connection = admin_connection(&process, &fixture).await.unwrap();
    assert!(monitor.has_complete_role_set());
    let liveness_identity = &material
        .iter()
        .find(|entry| entry.role == CoordinationRole::LivenessWatchdog)
        .expect("liveness role material")
        .identity;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(liveness_identity)
        .arg("-eval")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(monitor.run_once().await.is_err());
    assert!(!monitor.fence().snapshot().ready);
    assert_runtime_role_failure(
        &monitor,
        "liveness-watchdog",
        "a registered required-operation probe failed",
        &material,
    );
    let mut capability_generation = monitor.fence().snapshot().generation;
    assert!(
        capability_generation > 0,
        "required-grant loss must advance the common generation fence"
    );
    assert!(executor_liveness
        .select_due_liveness(chrono::Utc::now())
        .await
        .is_err());

    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(liveness_identity)
        .arg("+eval")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    monitor.reconstruct_before_readiness().await.unwrap();
    assert!(monitor.fence().snapshot().ready);
    assert_eq!(
        monitor.diagnostics().capability_fingerprint,
        original_fingerprint
    );
    assert_complete_reconstruction(&monitor, &policy_candidate, &original_fingerprint);
    assert!(executor_liveness
        .select_due_liveness(chrono::Utc::now())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        inspect_redis_namespace(&mut connection, &candidate(namespace))
            .await
            .unwrap(),
        RedisNamespaceInspection::Matching
    );

    let installed_policy = RedisCanonicalAclPolicy::generate(
        policy_candidate.operation_manifests(),
        policy_candidate.namespace().as_str(),
    )
    .unwrap();
    let task_events_material = material
        .iter()
        .find(|entry| entry.role == CoordinationRole::TaskEvents)
        .expect("TaskEvents role material");
    let task_events_grants = installed_policy.get(CoordinationRole::TaskEvents);
    let task_dispatch_pattern = format!("tickr:{{{namespace}}}:task-dispatch:*");
    let periodic_shutdown = CancellationToken::new();
    let periodic_task = {
        let monitor = Arc::clone(&monitor);
        let shutdown = periodic_shutdown.clone();
        tokio::spawn(async move { monitor.run(Duration::from_millis(10), shutdown).await })
    };
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&task_events_material.identity)
        .arg(format!("~{task_dispatch_pattern}"))
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    for _ in 0..100 {
        if !monitor.fence().snapshot().ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_runtime_role_failure(
        &monitor,
        "task-events",
        "a registered representative forbidden operation succeeded",
        &material,
    );
    let forbidden_grant_generation = monitor.fence().snapshot().generation;
    assert!(
        forbidden_grant_generation > capability_generation,
        "unexpected forbidden access must close the same generation fence independently"
    );
    capability_generation = forbidden_grant_generation;
    assert!(monitor.fence().guard_admission().is_err());

    let mut restore_task_events_acl = redis::cmd("ACL");
    restore_task_events_acl
        .arg("SETUSER")
        .arg(&task_events_material.identity)
        .arg("resetkeys");
    for pattern in task_events_grants.key_patterns() {
        restore_task_events_acl.arg(format!("~{pattern}"));
    }
    restore_task_events_acl
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    for _ in 0..100 {
        if monitor.fence().snapshot().ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(monitor.fence().snapshot().ready);
    assert_eq!(
        monitor.diagnostics().capability_fingerprint,
        original_fingerprint
    );
    assert_complete_reconstruction(&monitor, &policy_candidate, &original_fingerprint);
    periodic_shutdown.cancel();
    periodic_task.await.unwrap().unwrap();

    let task_event_writer = role_bundle
        .task_event_writer()
        .expect("admitted TaskEvents writer");
    let task_event_consumer = role_bundle
        .task_event_consumer()
        .expect("admitted TaskEvents consumer");
    let task_event_stream = format!("tickr:{{{namespace}}}:task-events:stream");
    let task_event_quota = format!("tickr:{{{namespace}}}:task-events:quota");
    let task_event_units = format!("tickr:{{{namespace}}}:task-events:units");
    let max_payload = vec![0x5a; 1024 * 1024];
    task_event_writer
        .stage("capacity-retained-0", &max_payload)
        .await
        .unwrap();
    let first_quota = runtime_quota_state(&monitor, "task-events");
    let per_record_overhead = first_quota
        .used
        .checked_sub(max_payload.len() as u64)
        .expect("TaskEvents quota includes its payload");
    let mut accepted_records = 1_u64;
    loop {
        let quota = runtime_quota_state(&monitor, "task-events");
        let remaining = quota.hard_limit - quota.used;
        let full_record_units = max_payload.len() as u64 + per_record_overhead;
        if remaining <= full_record_units {
            break;
        }
        task_event_writer
            .stage(
                &format!("capacity-retained-{accepted_records}"),
                &max_payload,
            )
            .await
            .unwrap();
        accepted_records += 1;
    }
    let before_hard_limit = runtime_quota_state(&monitor, "task-events");
    let final_payload_bytes = before_hard_limit
        .hard_limit
        .checked_sub(before_hard_limit.used)
        .and_then(|remaining| remaining.checked_sub(per_record_overhead))
        .expect("calibrated hard limit admits a final retained record");
    assert!(final_payload_bytes > 0);
    assert!(final_payload_bytes <= max_payload.len() as u64);
    let capability_failure_before_hard_limit = monitor.diagnostics().last_capability_failure;
    let hard_limit_permit = monitor.fence().guard_admission().unwrap();
    let final_identity = format!("capacity-retained-{accepted_records}");
    task_event_writer
        .stage(
            &final_identity,
            &vec![0x6b; usize::try_from(final_payload_bytes).unwrap()],
        )
        .await
        .unwrap();
    accepted_records += 1;
    let hard_quota = runtime_quota_state(&monitor, "task-events");
    assert_eq!(hard_quota.used, hard_quota.hard_limit);
    assert_eq!(hard_quota.accepted_identities, accepted_records);
    assert_eq!(hard_quota.pressure, RedisQuotaPressure::HardLimit);
    assert!(monitor.fence().snapshot().ready);
    assert!(monitor
        .fence()
        .guard_acknowledgement(hard_limit_permit)
        .is_ok());
    assert_eq!(
        monitor.diagnostics().last_capability_failure,
        capability_failure_before_hard_limit
    );
    assert_eq!(
        monitor.fence().snapshot().generation,
        capability_generation,
        "hard quota pressure must not masquerade as capability loss"
    );
    let retained_at_limit: u64 = redis::cmd("XLEN")
        .arg(&task_event_stream)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(retained_at_limit, accepted_records);
    assert!(task_event_writer
        .stage("capacity-must-not-trim", b"must remain unaccepted")
        .await
        .is_err());
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&task_event_stream)
            .query_async::<u64>(&mut connection)
            .await
            .unwrap(),
        retained_at_limit,
    );
    assert!(monitor.fence().snapshot().ready);
    assert_eq!(
        monitor.diagnostics().last_capability_failure,
        capability_failure_before_hard_limit
    );

    monitor.reconstruct_before_readiness().await.unwrap();
    assert!(monitor.fence().snapshot().ready);
    assert_eq!(
        monitor.diagnostics().capability_fingerprint,
        original_fingerprint
    );
    for _ in 0..accepted_records {
        let delivery = task_event_consumer
            .next()
            .await
            .unwrap()
            .expect("retained TaskEvent reconstructs for relay");
        assert!(!delivery.payload().is_empty());
        delivery.complete().await.unwrap();
    }
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&task_event_stream)
            .query_async::<u64>(&mut connection)
            .await
            .unwrap(),
        0,
    );
    assert_eq!(runtime_quota_state(&monitor, "task-events").used, 0);
    task_event_writer
        .stage(
            "capacity-after-terminal-cleanup",
            b"accepted only after TaskEvent relay cleanup",
        )
        .await
        .unwrap();
    task_event_consumer
        .next()
        .await
        .unwrap()
        .expect("post-cleanup TaskEvent")
        .complete()
        .await
        .unwrap();

    let retained_before_oom: u64 = redis::cmd("XLEN")
        .arg(&task_event_stream)
        .query_async(&mut connection)
        .await
        .unwrap();
    let used_memory = redis_info_u64(
        &redis::cmd("INFO")
            .arg("memory")
            .query_async::<String>(&mut connection)
            .await
            .unwrap(),
        "used_memory",
    );
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(used_memory)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let oom_permit = monitor.fence().guard_admission().unwrap();
    assert!(task_event_writer
        .stage("oom-must-not-acknowledge", &max_payload)
        .await
        .is_err());
    assert_runtime_role_failure(
        &monitor,
        "task-events",
        "a role operation observed Redis OOM",
        &material,
    );
    let oom_generation = monitor.fence().snapshot().generation;
    assert!(
        oom_generation > capability_generation,
        "OOM must independently close the common generation fence"
    );
    capability_generation = oom_generation;
    assert!(monitor.fence().guard_acknowledgement(oom_permit).is_err());
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(CAPACITY_BYTES)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&task_event_stream)
            .query_async::<u64>(&mut connection)
            .await
            .unwrap(),
        retained_before_oom,
    );
    monitor.reconstruct_before_readiness().await.unwrap();
    assert!(monitor.fence().snapshot().ready);
    assert_complete_reconstruction(&monitor, &policy_candidate, &original_fingerprint);

    let accounting_identity = "accounting-retained";
    let accounting_payload = b"accepted TaskEvent accounting evidence";
    task_event_writer
        .stage(accounting_identity, accounting_payload)
        .await
        .unwrap();
    let accounting_delivery = task_event_consumer
        .next()
        .await
        .unwrap()
        .expect("accounting TaskEvent delivery");
    let exact_units: u64 = redis::cmd("HGET")
        .arg(&task_event_units)
        .arg(accounting_identity)
        .query_async(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&task_event_units)
        .arg(accounting_identity)
        .arg(exact_units + 1)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let accounting_permit = monitor.fence().guard_admission().unwrap();
    assert!(accounting_delivery.complete().await.is_err());
    assert_runtime_role_failure(
        &monitor,
        "task-events",
        "a role observed inconsistent exact quota accounting",
        &material,
    );
    let accounting_generation = monitor.fence().snapshot().generation;
    assert!(
        accounting_generation > capability_generation,
        "accounting inconsistency must independently close the common generation fence"
    );
    assert!(monitor
        .fence()
        .guard_acknowledgement(accounting_permit)
        .is_err());
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&task_event_stream)
            .query_async::<u64>(&mut connection)
            .await
            .unwrap(),
        1,
    );
    redis::cmd("HSET")
        .arg(&task_event_units)
        .arg(accounting_identity)
        .arg(exact_units)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    monitor.reconstruct_before_readiness().await.unwrap();
    assert!(monitor.fence().snapshot().ready);
    assert_complete_reconstruction(&monitor, &policy_candidate, &original_fingerprint);
    tokio::time::sleep(Duration::from_millis(150)).await;
    task_event_consumer
        .next()
        .await
        .unwrap()
        .expect("restored accounting TaskEvent reconstructs")
        .complete()
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("HMGET")
            .arg(&task_event_quota)
            .arg(&["used_bytes", "accepted_records", "pending_deliveries"])
            .query_async::<Vec<Option<u64>>>(&mut connection)
            .await
            .unwrap(),
        vec![Some(0), Some(0), Some(0)],
    );
    assert_eq!(
        monitor.diagnostics().capability_fingerprint,
        original_fingerprint
    );

    let restarted = admit_complete_redis_formation(
        &process.descriptor(&fixture),
        candidate(namespace),
        credentials(&material, None),
    )
    .await
    .unwrap();
    assert_eq!(
        restarted.candidate().capability_fingerprint().as_str(),
        policy_candidate.capability_fingerprint().as_str()
    );

    assert_acl_failure(
        &fixture,
        "acl-revoked",
        AclMutation::RevokeEval(CoordinationRole::LogStaging),
        CoordinationRole::LogStaging,
        RedisAclAdmissionFailure::RequiredScriptOperation,
    )
    .await;
    assert_acl_failure(
        &fixture,
        "acl-broadened",
        AclMutation::BroadenForbidden(CoordinationRole::CommandBus),
        CoordinationRole::CommandBus,
        RedisAclAdmissionFailure::ForbiddenOperation,
    )
    .await;
    assert_acl_failure(
        &fixture,
        "acl-drift",
        AclMutation::AddUnregisteredPing(CoordinationRole::ScopeStore),
        CoordinationRole::ScopeStore,
        RedisAclAdmissionFailure::PolicyCommandDrift,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and OpenSSL"]
async fn all_redis_diagnostics_startup_smoke_has_no_nats_dependency() {
    let postgres = Postgres::default()
        .start()
        .await
        .expect("start smoke Postgres");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("map smoke Postgres port");
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .expect("connect smoke Postgres");
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .expect("migrate smoke Postgres");
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool));
    repositories
        .verify_schema()
        .await
        .expect("verify smoke Postgres schema");

    let minio = MinIO::default().start().await.expect("start smoke MinIO");
    let mut create_bucket = minio
        .exec(ExecCommand::new(["mkdir", "-p", "/data/tickr-logs"]))
        .await
        .expect("create smoke final-Log bucket");
    create_bucket
        .stdout_to_vec()
        .await
        .expect("read MinIO bucket command");
    assert_eq!(create_bucket.exit_code().await.unwrap(), Some(0));
    minio
        .get_host_port_ipv4(9000)
        .await
        .expect("map smoke MinIO port");

    let fixture = TlsFixture::generate();
    let material = role_material();
    let namespace = format!("all-redis-smoke-{}", NEXT_FIXTURE.load(Ordering::Relaxed));
    let runtime_candidate = candidate(&namespace);
    assert_eq!(
        runtime_candidate.descriptor().profile,
        FormationProfile::AllRedis
    );
    assert_eq!(
        runtime_candidate.descriptor().sql,
        SqlImplementation::Postgres
    );
    assert_eq!(
        runtime_candidate.descriptor().final_logs,
        FinalLogStore::ObjectStore
    );
    let redis = RedisProcess::start(
        &fixture,
        &fixture_policy(&runtime_candidate, &material, AclMutation::None),
    )
    .await;
    let descriptor = Arc::new(redis.descriptor(&fixture));
    let monitor = Arc::new(RedisCapabilityMonitor::new(
        runtime_candidate.clone(),
        Arc::new(RedisAdmissionCapabilityProbe::new(
            Arc::clone(&descriptor),
            runtime_candidate.clone(),
        )),
    ));
    assert!(
        !monitor.fence().snapshot().ready,
        "readiness opens only after admission and reconstruction"
    );
    let admitted = admit_complete_redis_formation(
        descriptor.as_ref(),
        runtime_candidate.clone(),
        credentials(&material, None),
    )
    .await
    .expect("admit canonical all-Redis fixture");
    let shutdown = CancellationToken::new();
    let mut bundle = compose_and_reconstruct_all_redis(
        admitted,
        Arc::clone(&monitor),
        Some(Arc::clone(&repositories)),
        shutdown.clone(),
    )
    .await
    .expect("compose and reconstruct all-Redis formation");
    assert!(
        bundle.is_ready(),
        "complete role reconstruction opens readiness"
    );

    let command_bus = bundle
        .command_bus_client()
        .expect("smoke CommandBus client");
    let command_consumer = bundle
        .command_bus_consumer()
        .expect("smoke CommandBus consumer");
    let command_shutdown = shutdown.child_token();
    let command_task = tokio::spawn({
        let command_shutdown = command_shutdown.clone();
        async move {
            command_consumer
                .serve(Arc::new(PingCommandHandler), command_shutdown)
                .await
        }
    });
    let response = loop {
        match command_bus
            .send(
                api::ApiCommandRequest {
                    body: Some(api::api_command_request::Body::Ping(api::PingRequest {})),
                },
                Duration::from_millis(500),
            )
            .await
        {
            Ok(response) => break response,
            Err(BusError::Unavailable) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => panic!("typed smoke Command failed: {error:?}"),
        }
    };
    assert_eq!(response.status_code, 200);
    assert!(matches!(
        response.payload,
        Some(api::api_command_response::Payload::Ping(_))
    ));

    let task = DispatchedTask {
        task_instance_id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        workflow_instance_id: Uuid::new_v4(),
        workflow_id: Uuid::new_v4(),
        name: "all-redis-smoke-task".to_owned(),
        nix_expression_path: "unused.nix".to_owned(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: Default::default(),
    };
    let dispatch = tc::TaskDispatch {
        task_instance_id: task.task_instance_id.to_string(),
        task_id: task.task_id.to_string(),
        workflow_instance_id: task.workflow_instance_id.to_string(),
        workflow_id: task.workflow_id.to_string(),
        name: task.name.clone(),
        task_type: 0,
        nix_expression_path: task.nix_expression_path.clone(),
        nix_args: task.nix_args.clone(),
        outputs: task.outputs.clone(),
        inputs: task.inputs.clone(),
        secrets: task.secrets.clone(),
        tenant_id: "smoke".to_owned(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: Vec::new(),
    };
    let dispatch_key = format!("dispatch:{}", task.task_instance_id);
    let publisher = bundle
        .task_dispatch_publisher()
        .expect("smoke TaskDispatch publisher");
    publisher.prepare().await.expect("prepare TaskDispatch");
    publisher
        .stage(&dispatch_key, &dispatch.encode_to_vec())
        .await
        .expect("stage smoke TaskDispatch");
    let task_events = bundle.task_event_writer().expect("smoke TaskEvents writer");
    task_events.prepare().await.expect("prepare TaskEvents");
    let task_event_consumer = bundle
        .task_event_consumer()
        .expect("smoke TaskEvents consumer");
    let exited = Arc::new(AtomicBool::new(false));
    let executor = SafePickupExecutor::new(
        bundle
            .executor_task_handoff()
            .expect("smoke TaskDispatch handoff"),
        SmokeTaskLauncher {
            exited: Arc::clone(&exited),
        },
        LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap()),
        "smoke-executor",
        Duration::from_secs(2),
    );
    let outcome = tokio::time::timeout(Duration::from_secs(5), executor.run_one())
        .await
        .expect("smoke Task pickup is bounded")
        .expect("smoke Task pickup succeeds");
    assert!(matches!(
        outcome,
        PickupOutcome::Launched {
            exit_success: true,
            ..
        }
    ));
    assert!(
        exited.load(Ordering::Acquire),
        "the owned Task process was observed and reaped"
    );

    let mut observed_kinds = Vec::new();
    for _ in 0..3 {
        let delivery = tokio::time::timeout(Duration::from_secs(2), task_event_consumer.next())
            .await
            .expect("TaskEvent delivery is bounded")
            .expect("read smoke TaskEvent")
            .expect("smoke TaskEvent is present");
        let event = tc::TaskEvent::decode(delivery.payload()).expect("decode typed TaskEvent");
        observed_kinds.push(match event.kind {
            Some(tc::task_event::Kind::Assigned(_)) => "assigned",
            Some(tc::task_event::Kind::Started(_)) => "started",
            Some(tc::task_event::Kind::Completed(_)) => "terminal",
            other => panic!("unexpected smoke TaskEvent: {other:?}"),
        });
        delivery.complete().await.expect("complete smoke TaskEvent");
    }
    assert_eq!(observed_kinds, ["assigned", "started", "terminal"]);

    let diagnostics = monitor.diagnostics();
    assert_eq!(
        diagnostics.capability_fingerprint,
        runtime_candidate.capability_fingerprint().as_str()
    );
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
    assert!(diagnostics.capacity.is_some());
    assert!(
        !diagnostics.quota_state.is_empty(),
        "live Command and Task paths publish quota state"
    );
    assert!(diagnostics.last_capability_failure.is_none());
    assert_eq!(diagnostics.fence.ready, true);
    assert!(diagnostics
        .durability_class
        .contains("one local-primary AOF fsync"));
    assert!(diagnostics
        .durability_class
        .contains("zero required replica acknowledgements"));
    let projection = serde_json::to_string(&diagnostics).expect("serialize smoke diagnostics");
    assert!(projection.contains("\"last_capability_failure\":null"));
    for forbidden in [
        "endpoint",
        "username",
        "password",
        "query",
        "trust_root",
        "certificate",
        ADMIN_PASSWORD,
        fixture.path.to_string_lossy().as_ref(),
    ] {
        assert!(
            !projection.contains(forbidden),
            "projection leaked {forbidden}"
        );
    }
    for role in &material {
        assert!(!projection.contains(&role.identity));
        assert!(!projection.contains(&role.secret));
    }

    command_shutdown.cancel();
    command_task
        .await
        .expect("join smoke Command consumer")
        .expect("stop smoke Command consumer");
    tokio::time::timeout(Duration::from_secs(2), bundle.shutdown_critical_children())
        .await
        .expect("role child shutdown is bounded")
        .expect("join all composed role children");
    shutdown.cancel();
    repositories.close().await;
    drop(bundle);
    drop(redis);
    drop(minio);
    drop(postgres);
}

const RECOVERY_SCOPE_ENVELOPE: &[u8] = br#"{ "v": 2, "type": "string", "value": "all-redis", "secret": false, "producer": { "kind": "task", "task_id": "recovery", "task_name": "recovery-task" }, "created_at": "2026-07-23T00:00:00Z", "sha256": "all-redis-recovery-scope" }"#;
const RECOVERY_LOG_FIRST: &[u8] = b"accepted before Executor restart\n";
const RECOVERY_LOG_SECOND: &[u8] = b"accepted after Executor restart\n";
const RECOVERY_UNHEALTHY_LOG: &[u8] = b"pickup recovered without duplicate launch\n";

fn recovery_role_name(role: CoordinationRole) -> &'static str {
    match role {
        CoordinationRole::CommandBus => "command-bus",
        CoordinationRole::TaskDispatch => "task-dispatch",
        CoordinationRole::TaskEvents => "task-events",
        CoordinationRole::TaskCancellation => "task-cancellation",
        CoordinationRole::CompactionStaging => "compaction-staging",
        CoordinationRole::LifecycleWork => "lifecycle-work",
        CoordinationRole::LogStaging => "log-staging",
        CoordinationRole::ScopeStore => "scope-store",
        CoordinationRole::IngressIdempotencyStore => "ingress-idempotency-store",
        CoordinationRole::LivenessWatchdog => "liveness-watchdog",
        CoordinationRole::SignalAppliedNotifier => "signal-applied-notifier",
        CoordinationRole::ExecutorFleetStatus => "executor-fleet-status",
        CoordinationRole::EventIngress => "event-ingress",
    }
}

fn recovery_process_candidate(namespace: &str) -> RedisFormationAdmissionCandidate {
    let descriptor = resolve_formation(&FormationSelection::all_redis())
        .expect("resolve explicit all-Redis formation");
    let role_limits = ALL_COORDINATION_ROLES
        .iter()
        .map(|role| {
            RedisRoleLimits::new(
                *role,
                BTreeMap::from([(
                    ROLE_MEMORY_LIMIT_NAME.to_owned(),
                    calibrated_role_capacity(*role).default_bytes,
                )]),
                BTreeMap::from([("retention-seconds".to_owned(), 3_600)]),
            )
            .expect("construct process role limit")
        })
        .collect();
    RedisFormationAdmissionCandidate::construct(
        &descriptor,
        canonical_redis_operation_manifests().expect("construct process operation manifests"),
        RedisNamespaceIdentity::new(namespace).expect("construct process namespace"),
        role_limits,
        RedisDurabilityConfiguration::primary_local_aof(CAPACITY_BYTES),
    )
    .expect("construct process admission candidate")
}

fn recovery_credentials_json(material: &[RoleMaterial]) -> String {
    serde_json::to_string(
        &material
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "role": recovery_role_name(entry.role),
                    "identity": entry.identity,
                    "secret": entry.secret,
                })
            })
            .collect::<Vec<_>>(),
    )
    .expect("serialize process role credentials")
}

fn recovery_workflow_source() -> String {
    r#"let utils = import "lib.ncl" in
utils.mkWorkflow {
  slug = "all-redis-recovery",
  name = "all-redis-recovery",
  args = [],
  outputs = [],
  tasks = [ utils.mkTaskGroup {
    name = "recovery",
    args = [],
    outputs = [],
    tasks = [ utils.mkTask {
      name = "recovery-task",
      args = [],
      nix_expression_path = "recovery-expression",
      outputs = [],
    } ],
  } ],
}"#
    .to_owned()
}

fn recovery_task_dispatch(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_id: Uuid,
    task_instance_id: Uuid,
) -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "recovery-task".to_owned(),
        task_type: 0,
        nix_expression_path: "recovery-expression".to_owned(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        tenant_id: "all-redis-recovery".to_owned(),
        originating_signal_id: Some(workflow_instance_id.to_string()),
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: Vec::new(),
    }
}

fn recovery_compaction_payload(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    completed_task: Uuid,
    recovered_task: Uuid,
) -> Vec<u8> {
    let task = |id: Uuid, state: &str| SnapshotTaskInstance {
        id: id.to_string(),
        task_id: Uuid::new_v4().to_string(),
        name: "recovery-task".to_owned(),
        task_type: "Regular".to_owned(),
        state: state.to_owned(),
        executor_id: Some(Uuid::new_v4().to_string()),
        attempt: 0,
        ..Default::default()
    };
    CompactionEnvelope {
        projection: Some(ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "all-Redis recovery instance".to_owned(),
            state: "Failed".to_owned(),
            scheduled_at: Some(Utc::now().to_rfc3339()),
            task_instances: vec![
                task(completed_task, "Completed"),
                task(recovered_task, "Unhealthy"),
            ],
            ..Default::default()
        }),
        correlation: "all-redis-recovery".to_owned(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec()
}

fn recovery_log_route(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> LogStreamRoute {
    LogStreamRoute {
        workflow_id,
        workflow_instance_id,
        task_instance_id,
    }
}

fn recovery_env_uuid(name: &str) -> Uuid {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be a UUID"))
}

#[derive(Default)]
struct RecoveryEventIngressCapability;

impl RedisEventIngressCapability for RecoveryEventIngressCapability {
    fn guard_admission(&self) -> Result<u64, RedisEventIngressError> {
        Ok(0)
    }

    fn guard_acknowledgement(&self, _generation: u64) -> Result<(), RedisEventIngressError> {
        Ok(())
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisEventIngressQuotaState) {}
}

fn recovery_role_client(
    process: &RedisProcess,
    fixture: &TlsFixture,
    material: &[RoleMaterial],
    role: CoordinationRole,
) -> redis::Client {
    let role = material
        .iter()
        .find(|entry| entry.role == role)
        .expect("recovery role material");
    let connection_info = format!(
        "rediss://{}:{}@localhost:{}/",
        role.identity, role.secret, process.port
    )
    .parse::<ConnectionInfo>()
    .expect("parse recovery role connection");
    redis::Client::build_with_tls(
        connection_info,
        TlsCertificates {
            client_tls: None,
            root_cert: Some(fixture.trust_roots.as_bytes().to_vec()),
        },
    )
    .expect("build recovery role TLS client")
}

fn append_recovery_ingress_evidence(path: &Path, evidence: &str) {
    use std::io::Write as _;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open External Event recovery evidence");
    writeln!(file, "{evidence}").expect("write External Event recovery evidence");
}

struct RecoveryIngressWorkingSet {
    behavior: String,
    evidence: PathBuf,
    calls: AtomicU64,
}

#[async_trait]
impl IngressWorkingSet for RecoveryIngressWorkingSet {
    async fn process_trigger(
        &self,
        _repositories: &WriterRepositoryBundle,
        request: TriggerRequest,
        signal_id: Uuid,
    ) -> Result<ReservedTriggerEffects, TriggerError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior.as_str() {
            "success" => {
                append_recovery_ingress_evidence(&self.evidence, "trigger");
                Ok(ReservedTriggerEffects {
                    signal: sp::Signal {
                        signal_id: signal_id.to_string(),
                        idempotency_key: request.idempotency_key,
                        variant: None,
                    },
                    event_results: br#"{"capture":"durable"}"#.to_vec(),
                })
            }
            "transient-then-block" if call == 0 => {
                append_recovery_ingress_evidence(&self.evidence, "transient");
                Err(TriggerError::WorkflowLookup(anyhow::anyhow!(
                    "transient External Event lookup failure"
                )))
            }
            "transient-then-block" => {
                append_recovery_ingress_evidence(&self.evidence, "redelivery");
                std::future::pending::<()>().await;
                unreachable!("blocked External Event processing resumed")
            }
            "permanent" => {
                append_recovery_ingress_evidence(&self.evidence, "permanent");
                Err(TriggerError::WorkflowNotFound {
                    workflow_id: request.workflow_id,
                })
            }
            behavior => panic!("unknown External Event recovery behavior {behavior}"),
        }
    }

    async fn process_wakeup(
        &self,
        _repositories: &WriterRepositoryBundle,
        _sender: &dyn WakeupRelaySender,
        _request: WakeupRequest,
        _signal_id: Uuid,
    ) -> anyhow::Result<WakeupOutcome> {
        Err(anyhow::anyhow!(
            "unexpected Wakeup in External Event recovery"
        ))
    }
}

struct RecoveryIngressRelay {
    evidence: PathBuf,
}

#[async_trait]
impl RelaySender for RecoveryIngressRelay {
    async fn try_send(&self, _signal: &sp::Signal) -> RelaySendOutcome {
        append_recovery_ingress_evidence(&self.evidence, "relay");
        RelaySendOutcome::Sent
    }
}

async fn wait_for_recovery_ingress_evidence(path: &Path, expected: &str) {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if fs::read_to_string(path).is_ok_and(|content| content.contains(expected)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "External Event recovery evidence `{expected}` missing: {}",
            path.display()
        )
    });
}

async fn wait_for_recovery_ingress_completion(
    producer: &RedisEventIngress,
    transport_identity: &str,
    producer_key: &str,
    payload: &[u8],
    expected_stream_id: &str,
) {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let (acceptance, stream_id) = producer
                .append(transport_identity, producer_key, payload.to_vec())
                .await
                .expect("replay External Event delivery");
            assert_eq!(stream_id, expected_stream_id);
            if acceptance == RedisEventIngressAcceptance::ReplayedCompleted {
                break;
            }
            assert_eq!(acceptance, RedisEventIngressAcceptance::ReplayedPending);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("External Event delivery `{transport_identity}` was not ACKed"));
}

#[derive(Clone)]
struct RecoveryTaskLauncher {
    launches: PathBuf,
}

impl TaskProcessLauncher for RecoveryTaskLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<tokio::process::Child, String> {
        tokio::process::Command::new("sh")
            .args([
                "-c",
                "printf 'launch\\n' >> \"$1\"",
                "sh",
                self.launches.to_string_lossy().as_ref(),
            ])
            .spawn()
            .map_err(|error| error.to_string())
    }

    async fn process_exited(
        &self,
        _task: &DispatchedTask,
        _claim: &LocalPickupClaim,
        status: &std::process::ExitStatus,
    ) -> Result<(), String> {
        if status.success() {
            Ok(())
        } else {
            Err(format!("recovery Task exited with {status}"))
        }
    }
}

#[derive(Clone)]
struct BlockAtClaimProof {
    marker: PathBuf,
}

impl PickupCheckpoint for BlockAtClaimProof {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
        if boundary == PickupBoundary::AfterClaimProof {
            fs::write(&self.marker, b"claim-proved").map_err(|error| error.to_string())?;
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

fn spawn_recovery_helper(
    mode: &str,
    process_env: &[(&str, String)],
    extra_env: &[(&str, String)],
) -> Child {
    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("all_redis_recovery_process_helper")
        .arg("--nocapture")
        .env_clear()
        .env(
            "PATH",
            std::env::var("PATH").expect("release smoke requires PATH"),
        )
        .env("TICKR_RECOVERY_HELPER_MODE", mode)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (name, value) in process_env.iter().chain(extra_env.iter()) {
        command.env(name, value);
    }
    command.spawn().expect("spawn all-Redis recovery process")
}

async fn wait_for_recovery_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("recovery boundary marker missing: {}", path.display()));
}

async fn wait_for_recovery_workflow_status(pool: &sqlx::PgPool, workflow_id: Uuid, expected: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status: String =
                sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = 1")
                    .bind(workflow_id)
                    .fetch_one(pool)
                    .await
                    .expect("read recovery Workflow status");
            if status == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("recovery Workflow did not reach {expected}"));
}

fn launch_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|content| content.lines().count())
        .unwrap_or(0)
}

fn encode_recovery_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_recovery_hex(encoded: &str) -> Vec<u8> {
    assert!(
        encoded.len().is_multiple_of(2),
        "hex evidence has even length"
    );
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => panic!("invalid hex evidence"),
            };
            (digit(pair[0]) << 4) | digit(pair[1])
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawned by the all-Redis Workflow recovery release smoke"]
async fn all_redis_recovery_process_helper() {
    let mode = std::env::var("TICKR_RECOVERY_HELPER_MODE").expect("recovery helper mode");
    let shutdown = CancellationToken::new();
    let (repositories, compaction_pool) = if mode == "conductor-compaction-recover"
        || mode == "conductor-ingress"
    {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&std::env::var("TICKR_RECOVERY_POSTGRES_URL").expect("recovery Postgres URL"))
            .await
            .expect("connect recovery Postgres");
        (
            Some(Arc::new(WriterRepositoryBundle::from_postgres_pool(
                pool.clone(),
            ))),
            Some(pool),
        )
    } else {
        (None, None)
    };
    let admitted = AllRedisProcessAdmission::from_environment()
        .expect("parse all-Redis process inputs")
        .admit()
        .await
        .expect("admit all-Redis recovery process");
    let mut bundle = compose_and_reconstruct_all_redis(
        admitted.formation,
        admitted.monitor,
        repositories.clone(),
        shutdown.clone(),
    )
    .await
    .expect("compose all-Redis recovery process");
    assert!(bundle.is_ready());

    match mode.as_str() {
        "conductor-ingress" => {
            let repositories = repositories.expect("External Event recovery repositories");
            let evidence = PathBuf::from(
                std::env::var("TICKR_RECOVERY_INGRESS_EVIDENCE")
                    .expect("External Event recovery evidence path"),
            );
            let working_set = Arc::new(RecoveryIngressWorkingSet {
                behavior: std::env::var("TICKR_RECOVERY_INGRESS_BEHAVIOR")
                    .expect("External Event recovery behavior"),
                evidence: evidence.clone(),
                calls: AtomicU64::new(0),
            });
            let relay = Arc::new(RecoveryIngressRelay { evidence });
            run_event_consumer(
                bundle
                    .event_ingress()
                    .expect("selected External Event ingress"),
                bundle
                    .ingress_coordinator()
                    .expect("selected ingress idempotency coordinator"),
                repositories,
                working_set,
                relay,
                shutdown.clone(),
            )
            .await
            .expect("run selected External Event consumer");
        }
        "executor-normal" | "executor-crash" | "executor-recover" => {
            bundle
                .task_event_writer()
                .expect("Executor TaskEvents writer")
                .prepare()
                .await
                .expect("prepare Executor TaskEvents");
            let handoff = bundle
                .executor_task_handoff()
                .expect("Executor TaskDispatch handoff");
            let launches = PathBuf::from(
                std::env::var("TICKR_RECOVERY_LAUNCH_PATH").expect("Task launch path"),
            );
            let launcher = RecoveryTaskLauncher { launches };
            let capacity =
                LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap());
            let outcome = if mode == "executor-crash" {
                let checkpoint = BlockAtClaimProof {
                    marker: PathBuf::from(
                        std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("claim boundary path"),
                    ),
                };
                SafePickupExecutor::with_checkpoint(
                    handoff,
                    launcher,
                    checkpoint,
                    capacity,
                    "crashing-all-redis-executor",
                    Duration::from_millis(500),
                )
                .run_one()
                .await
            } else {
                SafePickupExecutor::new(
                    handoff,
                    launcher,
                    capacity,
                    format!("{mode}-all-redis-executor"),
                    Duration::from_millis(500),
                )
                .run_one()
                .await
            }
            .expect("run one all-Redis Task pickup");

            if mode == "executor-normal" {
                assert!(matches!(
                    outcome,
                    PickupOutcome::Launched {
                        exit_success: true,
                        ..
                    }
                ));
            } else if mode == "executor-recover" {
                assert_eq!(
                    outcome,
                    PickupOutcome::NoWork,
                    "a proved ambiguous pickup cannot authorize a second launch"
                );
                fs::write(
                    std::env::var("TICKR_RECOVERY_BOUNDARY_PATH")
                        .expect("Executor recovery marker"),
                    b"recovered-without-launch",
                )
                .expect("write Executor recovery marker");
            } else {
                unreachable!("the crashing Executor must be killed at the claim boundary");
            }
        }
        "log-crash" | "log-recover" => {
            let workflow_id = recovery_env_uuid("TICKR_RECOVERY_WORKFLOW_ID");
            let workflow_instance_id = recovery_env_uuid("TICKR_RECOVERY_WORKFLOW_INSTANCE_ID");
            let task_instance_id = recovery_env_uuid("TICKR_RECOVERY_COMPLETED_TASK_ID");
            let provider = bundle
                .log_stream_provider()
                .expect("Executor LogStaging provider");
            provider.prepare().await.expect("prepare LogStaging");
            let identity = LogStreamIdentity {
                task_instance_id,
                pickup_generation: 1,
            };
            let mut stream = provider
                .open(
                    recovery_log_route(workflow_id, workflow_instance_id, task_instance_id),
                    identity.clone(),
                )
                .await
                .expect("open accepted Log stream");
            let first = LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: identity.clone(),
                    sequence: 0,
                },
                RECOVERY_LOG_FIRST.to_vec(),
            );
            if mode == "log-crash" {
                assert_eq!(
                    stream.accept(first).await.expect("accept pre-restart Log"),
                    AcceptOutcome::Accepted
                );
                fs::write(
                    std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("Log boundary path"),
                    b"first-log-accepted",
                )
                .expect("write Log boundary marker");
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }

            assert_eq!(
                stream.accept(first).await.expect("replay accepted Log"),
                AcceptOutcome::AlreadyAccepted
            );
            assert_eq!(
                stream
                    .accept(LogRecordSubmission::new(
                        LogRecordIdentity {
                            stream: identity,
                            sequence: 1,
                        },
                        RECOVERY_LOG_SECOND.to_vec(),
                    ))
                    .await
                    .expect("accept post-restart Log"),
                AcceptOutcome::Accepted
            );
            assert_eq!(stream.committed_frontier(), Some(1));
            assert_eq!(
                stream
                    .finish_cleanly(LogExit::Status(0))
                    .await
                    .expect("seal accepted Log"),
                TerminalOutcome::Recorded
            );
            let replay = stream.replay().await.expect("replay accepted Log");
            let accepted = replay
                .iter()
                .filter_map(|record| match record {
                    ReplayedLogRecord::Accepted {
                        identity, bytes, ..
                    } => Some((identity.sequence, bytes.as_slice())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                accepted,
                vec![(0, RECOVERY_LOG_FIRST), (1, RECOVERY_LOG_SECOND),]
            );
            assert!(matches!(
                replay.last(),
                Some(ReplayedLogRecord::Terminal { .. })
            ));
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("Log recovery marker"),
                [RECOVERY_LOG_FIRST, RECOVERY_LOG_SECOND].concat(),
            )
            .expect("write Log recovery evidence");
        }
        "conductor-sweep" => {
            let sweeper = bundle
                .conductor_liveness_sweeper()
                .expect("Conductor LivenessWatchdog sweeper");
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if sweeper
                        .sweep_one_due()
                        .await
                        .expect("sweep due pickup generation")
                        .is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("restarted Conductor did not reconstruct due liveness");
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("liveness recovery marker"),
                b"terminal-elected",
            )
            .expect("write liveness recovery marker");
        }
        "conductor-events-crash" => {
            let consumer = bundle
                .task_event_consumer()
                .expect("Conductor TaskEvents consumer");
            let _held = tokio::time::timeout(Duration::from_secs(10), consumer.next())
                .await
                .expect("TaskEvent crash delivery timed out")
                .expect("read TaskEvent crash delivery")
                .expect("TaskEvent crash delivery missing");
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("TaskEvent boundary marker"),
                b"task-event-held",
            )
            .expect("write TaskEvent boundary marker");
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        "conductor-events-recover" => {
            let consumer = bundle
                .task_event_consumer()
                .expect("restarted Conductor TaskEvents consumer");
            let expected: usize = std::env::var("TICKR_RECOVERY_EVENT_COUNT")
                .expect("expected TaskEvent count")
                .parse()
                .expect("numeric TaskEvent count");
            let payloads = tokio::time::timeout(Duration::from_secs(20), async {
                let mut payloads = Vec::new();
                let mut last_delivery = tokio::time::Instant::now();
                loop {
                    if let Some(delivery) =
                        consumer.next().await.expect("read reconstructed TaskEvent")
                    {
                        payloads.push(delivery.payload().to_vec());
                        delivery
                            .complete()
                            .await
                            .expect("complete reconstructed TaskEvent");
                        last_delivery = tokio::time::Instant::now();
                        if payloads.len() == expected {
                            break;
                        }
                    } else if !payloads.is_empty()
                        && last_delivery.elapsed() >= Duration::from_secs(2)
                    {
                        break;
                    } else {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                }
                payloads
            })
            .await
            .expect("restarted Conductor did not reconstruct any TaskEvents");
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("TaskEvent recovery evidence"),
                payloads
                    .iter()
                    .map(|payload| encode_recovery_hex(payload))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .expect("write TaskEvent recovery evidence");
        }
        "conductor-compaction-crash" => {
            let staging = bundle
                .compaction_staging()
                .expect("Conductor CompactionStaging");
            staging.prepare().await.expect("prepare CompactionStaging");
            let _held = tokio::time::timeout(Duration::from_secs(10), staging.next())
                .await
                .expect("Compaction crash delivery timed out")
                .expect("read Compaction crash delivery")
                .expect("Compaction crash delivery missing");
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("Compaction boundary marker"),
                b"compaction-held",
            )
            .expect("write Compaction boundary marker");
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        "conductor-compaction-recover" => {
            let compaction_pool = compaction_pool.expect("Compaction recovery Postgres pool");
            let repositories = repositories.expect("Compaction recovery repositories");
            let staging = bundle
                .compaction_staging()
                .expect("restarted CompactionStaging");
            let log_probe = bundle
                .log_stream_provider()
                .expect("restarted LogStaging probe");
            let logs = bundle
                .compaction_log_staging()
                .expect("restarted Compaction LogStaging");
            let scopes = bundle
                .scope_store()
                .expect("restarted Compaction ScopeStore");
            let storage = LogStorageConfig {
                endpoint: std::env::var("TICKR_RECOVERY_LOG_ENDPOINT").expect("final-Log endpoint"),
                bucket: "tickr-logs".to_owned(),
                access_key_id: "minioadmin".to_owned(),
                secret_access_key: "minioadmin".to_owned(),
                region: "us-east-1".to_owned(),
            }
            .operator()
            .expect("construct final-Log operator");
            let drain_shutdown = CancellationToken::new();
            let drain_handle = tokio::spawn(run_selected_compaction_drain(
                staging,
                logs,
                scopes,
                Arc::clone(&repositories),
                storage,
                drain_shutdown.clone(),
            ));
            let workflow_instance_id = recovery_env_uuid("TICKR_RECOVERY_WORKFLOW_INSTANCE_ID");
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let archived: i64 =
                        sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = $1")
                            .bind(workflow_instance_id)
                            .fetch_one(&compaction_pool)
                            .await
                            .expect("read recovered archive");
                    if archived == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("restarted Conductor did not finish Compaction");
            let workflow_id = recovery_env_uuid("TICKR_RECOVERY_WORKFLOW_ID");
            let completed_task_id = recovery_env_uuid("TICKR_RECOVERY_COMPLETED_TASK_ID");
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    match log_probe
                        .replay_task(recovery_log_route(
                            workflow_id,
                            workflow_instance_id,
                            completed_task_id,
                        ))
                        .await
                    {
                        Ok(records) if records.is_empty() => break,
                        Ok(_) | Err(_) => {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    }
                }
            })
            .await
            .expect("restarted Conductor did not purge committed Log staging");
            drain_shutdown.cancel();
            drain_handle
                .await
                .expect("join selected Compaction drain")
                .expect("selected Compaction drain");
            fs::write(
                std::env::var("TICKR_RECOVERY_BOUNDARY_PATH").expect("Compaction recovery marker"),
                b"archive-committed-and-staging-purged",
            )
            .expect("write Compaction recovery marker");
        }
        other => panic!("unknown all-Redis recovery helper mode {other}"),
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    tokio::time::timeout(Duration::from_secs(5), bundle.shutdown_critical_children())
        .await
        .expect("all-Redis helper child shutdown timed out")
        .expect("join all-Redis helper children");
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, OpenSSL, Nickel, and real-process recovery"]
async fn all_redis_workflow_recovery_release_smoke() {
    assert!(
        Command::new("nickel")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false),
        "Nickel is required for the real Workflow registration path"
    );

    let postgres = Postgres::default()
        .start()
        .await
        .expect("start recovery Postgres");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("map recovery Postgres port");
    let postgres_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&postgres_url)
        .await
        .expect("connect recovery Postgres");
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .expect("migrate recovery Postgres");
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));

    let minio = MinIO::default()
        .start()
        .await
        .expect("start recovery MinIO");
    let mut create_bucket = minio
        .exec(ExecCommand::new(["mkdir", "-p", "/data/tickr-logs"]))
        .await
        .expect("create recovery final-Log bucket");
    create_bucket
        .stdout_to_vec()
        .await
        .expect("read recovery MinIO bucket command");
    assert_eq!(create_bucket.exit_code().await.unwrap(), Some(0));
    let minio_port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("map recovery MinIO port");
    let minio_endpoint = format!("http://127.0.0.1:{minio_port}");
    let storage = LogStorageConfig {
        endpoint: minio_endpoint.clone(),
        bucket: "tickr-logs".to_owned(),
        access_key_id: "minioadmin".to_owned(),
        secret_access_key: "minioadmin".to_owned(),
        region: "us-east-1".to_owned(),
    }
    .operator()
    .expect("construct recovery final-Log operator");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if storage
                .write("recovery-readiness", b"ready".to_vec())
                .await
                .is_ok()
            {
                storage
                    .delete("recovery-readiness")
                    .await
                    .expect("remove MinIO readiness object");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("recovery MinIO did not become ready");

    let tls = TlsFixture::generate();
    let material = role_material();
    let namespace = format!("all-redis-recovery-{}", Uuid::new_v4().simple());
    let runtime_candidate = recovery_process_candidate(&namespace);
    assert_eq!(
        runtime_candidate.descriptor().profile,
        FormationProfile::AllRedis
    );
    assert_eq!(
        runtime_candidate.descriptor().sql,
        SqlImplementation::Postgres
    );
    assert_eq!(
        runtime_candidate.descriptor().final_logs,
        FinalLogStore::ObjectStore
    );
    let redis = RedisProcess::start(
        &tls,
        &fixture_policy(&runtime_candidate, &material, AclMutation::None),
    )
    .await;
    let descriptor = Arc::new(redis.descriptor(&tls));
    let monitor = Arc::new(RedisCapabilityMonitor::new(
        runtime_candidate.clone(),
        Arc::new(RedisAdmissionCapabilityProbe::new(
            Arc::clone(&descriptor),
            runtime_candidate.clone(),
        )),
    ));
    let admitted = admit_complete_redis_formation(
        descriptor.as_ref(),
        runtime_candidate.clone(),
        credentials(&material, None),
    )
    .await
    .expect("admit recovery all-Redis formation");
    let shutdown = CancellationToken::new();
    let mut bundle = compose_and_reconstruct_all_redis(
        admitted,
        Arc::clone(&monitor),
        Some(Arc::clone(&repositories)),
        shutdown.clone(),
    )
    .await
    .expect("compose recovery all-Redis formation");
    assert!(bundle.is_ready());

    std::env::set_var(
        tickr_conductor::parser::nickel::DSL_PATHS_ENV,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dsl"),
    );
    let registration = process_register_local(
        repositories.as_ref(),
        RegisterRequest {
            nickel_source: recovery_workflow_source(),
            namespace: "default".to_owned(),
        },
    )
    .await
    .expect("register recovery Workflow without a NATS notifier");
    let (workflow_id, workflow_version) = match registration {
        RegisterOutcome::Inserted {
            workflow_id,
            workflow_version,
            ..
        } => (workflow_id, workflow_version),
        _ => panic!("fresh recovery Workflow registration was not inserted"),
    };
    assert_eq!(workflow_version, 1);

    let (_definition_notifier, definition_notifications) =
        definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let definition_shutdown = CancellationToken::new();
    let definition_handle = tokio::spawn(start_local_definition_build_worker(
        Arc::clone(&repositories),
        Arc::new(TestBuildExecutor::new()),
        "all-redis-recovery-definition".to_owned(),
        definition_notifications,
        LocalDefinitionBuildWorkerConfig {
            scan_interval: Duration::from_millis(25),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).unwrap(),
        },
        definition_shutdown.clone(),
    ));
    wait_for_recovery_workflow_status(&pool, workflow_id, "Ready").await;
    definition_shutdown.cancel();
    definition_handle
        .await
        .expect("join recovery definition worker")
        .expect("recovery definition worker");

    let (definition_relay, mut definition_relay_rx) = mpsc::channel::<ConductorRelayMessage>(4);
    init_relay_tx(definition_relay).await;
    let (_submission_notifier, submission_notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let submission_shutdown = CancellationToken::new();
    let submission_handle = tokio::spawn(start_local_definition_submission_worker(
        Arc::clone(&repositories),
        "all-redis-recovery-submission".to_owned(),
        submission_notifications,
        LocalDefinitionSubmissionWorkerConfig {
            scan_interval: Duration::from_millis(25),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).unwrap(),
        },
        submission_shutdown.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), definition_relay_rx.recv())
        .await
        .expect("recovery definition submission was not forwarded")
        .expect("recovery definition relay closed");
    wait_for_recovery_workflow_status(&pool, workflow_id, "Submitted").await;
    submission_shutdown.cancel();
    submission_handle
        .await
        .expect("join recovery submission worker")
        .expect("recovery submission worker");

    let scheduled_at = Utc::now() + chrono::Duration::minutes(1);
    let workflow_instance_id = derive_scheduled_workflow_instance_id(workflow_id, scheduled_at);
    let scopes = bundle.scope_store().expect("recovery ScopeStore");
    let ReservedTriggerEffects { signal, .. } = process_reserved_trigger_with_scope_store(
        repositories.as_ref(),
        scopes.as_ref(),
        TriggerRequest {
            workflow_id,
            scheduled_at: Some(scheduled_at),
            inputs: None,
            idempotency_key: None,
            source: tickr_ctx::envelope::SignalSource::Manual,
            hash_payload: serde_json::json!({}),
            name: Some("all-Redis recovery instance".to_owned()),
        },
        workflow_instance_id,
    )
    .await
    .expect("materialize recovery Workflow identity");
    assert_eq!(signal.signal_id, workflow_instance_id.to_string());
    let linkage = signal_captures::read(repositories.as_ref(), workflow_instance_id)
        .await
        .expect("read materialization linkage")
        .expect("materialization linkage exists");
    assert_eq!(linkage.materialized_run_id, Some(workflow_instance_id));

    let scope_key = format!("{workflow_instance_id}/result");
    assert!(matches!(
        scopes
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: workflow_instance_id,
                namespace: "default",
                run_id: &workflow_instance_id.to_string(),
                claim_id: Uuid::new_v4(),
                values: &[ScopeValueInput {
                    key: &scope_key,
                    envelope: RECOVERY_SCOPE_ENVELOPE,
                }],
                now: Utc::now(),
            })
            .await
            .expect("create recovery scope"),
        ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent
    ));

    let task_id: Uuid = sqlx::query_scalar(
        "SELECT task_id FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 ORDER BY task_id LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("read recovery Task identity");
    let completed_task_id = Uuid::new_v4();
    let recovered_task_id = Uuid::new_v4();
    let dispatch = bundle
        .task_dispatch_publisher()
        .expect("recovery TaskDispatch publisher");
    dispatch.prepare().await.expect("prepare recovery dispatch");
    dispatch
        .stage(
            &format!("dispatch:{completed_task_id}"),
            &recovery_task_dispatch(
                workflow_id,
                workflow_instance_id,
                task_id,
                completed_task_id,
            )
            .encode_to_vec(),
        )
        .await
        .expect("stage completing Task");
    dispatch
        .stage(
            &format!("dispatch:{recovered_task_id}"),
            &recovery_task_dispatch(
                workflow_id,
                workflow_instance_id,
                task_id,
                recovered_task_id,
            )
            .encode_to_vec(),
        )
        .await
        .expect("stage crash-recovery Task");

    let directory = tempfile::tempdir().expect("recovery process evidence directory");
    let launch_path = directory.path().join("launches");
    let process_env = vec![
        (
            "TICKR_REDIS_CONNECTION_DESCRIPTOR",
            redis.descriptor_json(&tls),
        ),
        ("TICKR_REDIS_NAMESPACE", namespace.clone()),
        ("TICKR_REDIS_CAPACITY_BYTES", CAPACITY_BYTES.to_string()),
        (
            "TICKR_REDIS_ROLE_CREDENTIALS",
            recovery_credentials_json(&material),
        ),
    ];
    assert!(
        process_env.iter().all(|(name, value)| {
            !name.contains("NATS") && !value.to_ascii_lowercase().contains("nats")
        }),
        "the all-Redis process environment must contain no NATS selector or fallback"
    );

    let mut ingress_producer_config =
        RedisEventIngressConfig::new(&namespace, "external-release-producer");
    let ingress_hard_limit = runtime_candidate
        .capacity_profile()
        .role_capacity_bytes(CoordinationRole::EventIngress);
    ingress_producer_config.hard_limit_bytes = ingress_hard_limit;
    ingress_producer_config.soft_limit_bytes = ingress_hard_limit.saturating_mul(4) / 5;
    ingress_producer_config.max_deliveries = NonZeroUsize::new(2).unwrap();
    let ingress_producer = RedisEventIngress::connect(
        recovery_role_client(&redis, &tls, &material, CoordinationRole::EventIngress),
        ingress_producer_config,
        RedisDurabilityGuard::default(),
        Arc::new(RecoveryEventIngressCapability),
    )
    .await
    .expect("connect real External Event producer");
    let ingress_coordinator = bundle
        .ingress_coordinator()
        .expect("all-Redis ingress idempotency coordinator");
    let ingress_evidence = directory.path().join("external-event-evidence");
    let producer_key = "producer:all-redis-recovery";
    let ingress_payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": producer_key,
        "workflow_id": workflow_id,
    }))
    .unwrap();
    let canonical_payload: serde_json::Value =
        serde_json::from_slice(&ingress_payload).expect("canonical External Event payload");
    let ingress_payload_hash = tickr_conductor::canonical_json::hash(Some(&canonical_payload));
    let (acceptance, ingress_stream_id) = ingress_producer
        .append(
            "transport:all-redis-crash",
            producer_key,
            ingress_payload.clone(),
        )
        .await
        .expect("deliver real External Event");
    assert_eq!(acceptance, RedisEventIngressAcceptance::Appended);
    assert_ne!(
        ingress_stream_id, producer_key,
        "transport and producer identities must remain distinct"
    );

    let mut crashing_ingress = spawn_recovery_helper(
        "conductor-ingress",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            ("TICKR_RECOVERY_INGRESS_BEHAVIOR", "success".to_owned()),
            (
                "TICKR_RECOVERY_INGRESS_EVIDENCE",
                ingress_evidence.to_string_lossy().into_owned(),
            ),
            (
                "TICKR_TEST_INGRESS_CRASH_BOUNDARY",
                "after-relay-intent-persistence".to_owned(),
            ),
        ],
    );
    let crash_status = crashing_ingress
        .wait()
        .expect("wait for External Event crash boundary");
    assert_eq!(
        crash_status.code(),
        Some(86),
        "Conductor must crash after durable effects and relay intent"
    );

    let (signal_id, recovered_effects) = match ingress_coordinator
        .reserve(producer_key, &ingress_payload_hash)
        .await
        .expect("read durable ingress reservation")
    {
        ReservationOutcome::Ready(_operation, effects) => {
            let signal = sp::Signal::decode(effects.signal_effect.as_slice())
                .expect("decode recovered Signal effect");
            (
                signal
                    .signal_id
                    .parse::<Uuid>()
                    .expect("recovered Signal identity is a UUID"),
                effects,
            )
        }
        _ => panic!("durable External Event effects and relay intent were not recoverable"),
    };
    assert_eq!(recovered_effects.relay_intents.len(), 1);
    assert!(matches!(
        recovered_effects.relay_intents.as_slice(),
        [RelayIntent::Signal(bytes)] if bytes == &recovered_effects.signal_effect
    ));
    assert_eq!(
        fs::read_to_string(&ingress_evidence).expect("read crash-boundary ingress evidence"),
        "trigger\n",
        "the crashed process must commit one deterministic Signal effect before relay"
    );

    let mut recovered_ingress = spawn_recovery_helper(
        "conductor-ingress",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            ("TICKR_RECOVERY_INGRESS_BEHAVIOR", "success".to_owned()),
            (
                "TICKR_RECOVERY_INGRESS_EVIDENCE",
                ingress_evidence.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_ingress_completion(
        &ingress_producer,
        "transport:all-redis-crash",
        producer_key,
        &ingress_payload,
        &ingress_stream_id,
    )
    .await;
    recovered_ingress
        .kill()
        .expect("stop recovered External Event Conductor");
    recovered_ingress
        .wait()
        .expect("reap recovered External Event Conductor");
    assert_eq!(
        fs::read_to_string(&ingress_evidence).expect("read recovered ingress evidence"),
        "trigger\nrelay\n",
        "restart must replay one relay intent without a second Signal effect"
    );
    assert!(matches!(
        ingress_coordinator
            .reserve(producer_key, &ingress_payload_hash)
            .await
            .expect("read completed producer result"),
        ReservationOutcome::Complete(proof)
            if proof.outcome() == IngressTerminalOutcome::Accepted
    ));

    let (retry_acceptance, retry_stream_id) = ingress_producer
        .append(
            "transport:all-redis-producer-retry",
            producer_key,
            ingress_payload.clone(),
        )
        .await
        .expect("deliver same-hash producer retry");
    assert_eq!(retry_acceptance, RedisEventIngressAcceptance::Appended);
    let conflicting_payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": producer_key,
        "workflow_id": Uuid::new_v4(),
    }))
    .unwrap();
    let conflicting_json: serde_json::Value = serde_json::from_slice(&conflicting_payload).unwrap();
    let conflicting_hash = tickr_conductor::canonical_json::hash(Some(&conflicting_json));
    let (conflict_acceptance, conflict_stream_id) = ingress_producer
        .append(
            "transport:all-redis-producer-conflict",
            producer_key,
            conflicting_payload.clone(),
        )
        .await
        .expect("deliver different-hash producer retry");
    assert_eq!(conflict_acceptance, RedisEventIngressAcceptance::Appended);
    let mut retry_ingress = spawn_recovery_helper(
        "conductor-ingress",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            ("TICKR_RECOVERY_INGRESS_BEHAVIOR", "success".to_owned()),
            (
                "TICKR_RECOVERY_INGRESS_EVIDENCE",
                ingress_evidence.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_ingress_completion(
        &ingress_producer,
        "transport:all-redis-producer-retry",
        producer_key,
        &ingress_payload,
        &retry_stream_id,
    )
    .await;
    wait_for_recovery_ingress_completion(
        &ingress_producer,
        "transport:all-redis-producer-conflict",
        producer_key,
        &conflicting_payload,
        &conflict_stream_id,
    )
    .await;
    retry_ingress.kill().expect("stop producer-retry Conductor");
    retry_ingress.wait().expect("reap producer-retry Conductor");
    assert_eq!(
        fs::read_to_string(&ingress_evidence).unwrap(),
        "trigger\nrelay\n",
        "same-hash deduplication and conflict must not repeat effects or relay"
    );
    let (conflict_signal_id, original_hash) = match ingress_coordinator
        .reserve(producer_key, &conflicting_hash)
        .await
        .expect("read producer conflict")
    {
        ReservationOutcome::Conflict {
            original_signal_id,
            original_hash,
            ..
        } => (original_signal_id, original_hash),
        _ => panic!("different-hash producer retry did not conflict"),
    };
    assert_eq!(conflict_signal_id, signal_id);
    assert_eq!(original_hash, encode_recovery_hex(&ingress_payload_hash));

    let rejection_evidence = directory.path().join("external-event-rejection-evidence");
    let rejected_key = "producer:all-redis-rejection";
    let rejected_payload = serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": rejected_key,
        "workflow_id": Uuid::new_v4(),
    }))
    .unwrap();
    let rejected_json: serde_json::Value = serde_json::from_slice(&rejected_payload).unwrap();
    let rejected_hash = tickr_conductor::canonical_json::hash(Some(&rejected_json));
    let (rejection_acceptance, rejection_stream_id) = ingress_producer
        .append(
            "transport:all-redis-rejection",
            rejected_key,
            rejected_payload.clone(),
        )
        .await
        .expect("deliver transient-then-rejected External Event");
    assert_eq!(rejection_acceptance, RedisEventIngressAcceptance::Appended);
    let mut transient_ingress = spawn_recovery_helper(
        "conductor-ingress",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            (
                "TICKR_RECOVERY_INGRESS_BEHAVIOR",
                "transient-then-block".to_owned(),
            ),
            (
                "TICKR_RECOVERY_INGRESS_EVIDENCE",
                rejection_evidence.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_ingress_evidence(&rejection_evidence, "redelivery").await;
    transient_ingress
        .kill()
        .expect("crash before durable permanent rejection");
    transient_ingress
        .wait()
        .expect("reap transient External Event Conductor");
    assert_eq!(
        fs::read_to_string(&rejection_evidence).unwrap(),
        "transient\nredelivery\n"
    );

    let mut rejection_ingress = spawn_recovery_helper(
        "conductor-ingress",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            ("TICKR_RECOVERY_INGRESS_BEHAVIOR", "permanent".to_owned()),
            (
                "TICKR_RECOVERY_INGRESS_EVIDENCE",
                rejection_evidence.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_ingress_completion(
        &ingress_producer,
        "transport:all-redis-rejection",
        rejected_key,
        &rejected_payload,
        &rejection_stream_id,
    )
    .await;
    rejection_ingress
        .kill()
        .expect("stop permanent-rejection Conductor");
    rejection_ingress
        .wait()
        .expect("reap permanent-rejection Conductor");
    assert_eq!(
        fs::read_to_string(&rejection_evidence).unwrap(),
        "transient\nredelivery\npermanent\n"
    );
    assert!(matches!(
        ingress_coordinator
            .reserve(rejected_key, &rejected_hash)
            .await
            .expect("read durable permanent rejection"),
        ReservationOutcome::Rejected(proof)
            if proof.outcome() == IngressTerminalOutcome::Rejected
    ));
    let (stable_rejection, stable_rejection_stream) = ingress_producer
        .append(
            "transport:all-redis-rejection",
            rejected_key,
            rejected_payload.clone(),
        )
        .await
        .expect("replay durably rejected delivery");
    assert_eq!(
        stable_rejection,
        RedisEventIngressAcceptance::ReplayedCompleted
    );
    assert_eq!(stable_rejection_stream, rejection_stream_id);

    let pressure_payload = |producer_key: &str| {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "variant": "Trigger",
            "idempotency_key": producer_key,
            "workflow_id": Uuid::new_v4(),
        }))
        .unwrap()
    };
    let pressure_one = pressure_payload("producer:pressure:one");
    let pressure_two = pressure_payload("producer:pressure:two");
    let pressure_three = pressure_payload("producer:pressure:three");
    let (_, pressure_one_stream) = ingress_producer
        .append(
            "transport:pressure:one",
            "producer:pressure:one",
            pressure_one.clone(),
        )
        .await
        .expect("accept first pressured ingress identity");
    let (_, pressure_two_stream) = ingress_producer
        .append(
            "transport:pressure:two",
            "producer:pressure:two",
            pressure_two.clone(),
        )
        .await
        .expect("accept second pressured ingress identity");
    assert_eq!(
        ingress_producer
            .append(
                "transport:pressure:three",
                "producer:pressure:three",
                pressure_three,
            )
            .await,
        Err(RedisEventIngressError::CapacityFenced)
    );
    let (pressure_one_replay, pressure_one_replayed_stream) = ingress_producer
        .append(
            "transport:pressure:one",
            "producer:pressure:one",
            pressure_one,
        )
        .await
        .expect("replay first pressured ingress identity");
    let (pressure_two_replay, pressure_two_replayed_stream) = ingress_producer
        .append(
            "transport:pressure:two",
            "producer:pressure:two",
            pressure_two,
        )
        .await
        .expect("replay second pressured ingress identity");
    assert_eq!(
        (pressure_one_replay, pressure_one_replayed_stream),
        (
            RedisEventIngressAcceptance::ReplayedPending,
            pressure_one_stream
        )
    );
    assert_eq!(
        (pressure_two_replay, pressure_two_replayed_stream),
        (
            RedisEventIngressAcceptance::ReplayedPending,
            pressure_two_stream
        )
    );

    let mut normal_executor = spawn_recovery_helper(
        "executor-normal",
        &process_env,
        &[(
            "TICKR_RECOVERY_LAUNCH_PATH",
            launch_path.to_string_lossy().into_owned(),
        )],
    );
    assert!(
        normal_executor
            .wait()
            .expect("wait for completing Executor")
            .success(),
        "completing all-Redis Executor failed"
    );
    assert_eq!(launch_count(&launch_path), 1);

    let dispatch_boundary = directory.path().join("dispatch-boundary");
    let mut crashing_executor = spawn_recovery_helper(
        "executor-crash",
        &process_env,
        &[
            (
                "TICKR_RECOVERY_LAUNCH_PATH",
                launch_path.to_string_lossy().into_owned(),
            ),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                dispatch_boundary.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_path(&dispatch_boundary).await;
    crashing_executor
        .kill()
        .expect("crash Executor at dispatch");
    crashing_executor.wait().expect("reap crashed Executor");

    let dispatch_recovery = directory.path().join("dispatch-recovered");
    let mut replacement_executor = spawn_recovery_helper(
        "executor-recover",
        &process_env,
        &[
            (
                "TICKR_RECOVERY_LAUNCH_PATH",
                launch_path.to_string_lossy().into_owned(),
            ),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                dispatch_recovery.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_executor
            .wait()
            .expect("wait for replacement Executor")
            .success(),
        "replacement all-Redis Executor failed"
    );
    wait_for_recovery_path(&dispatch_recovery).await;
    assert_eq!(
        launch_count(&launch_path),
        1,
        "ambiguous dispatch recovery must not launch a second Task process"
    );

    let liveness_recovery = directory.path().join("liveness-recovered");
    let mut liveness_conductor = spawn_recovery_helper(
        "conductor-sweep",
        &process_env,
        &[(
            "TICKR_RECOVERY_BOUNDARY_PATH",
            liveness_recovery.to_string_lossy().into_owned(),
        )],
    );
    assert!(
        liveness_conductor
            .wait()
            .expect("wait for restarted liveness Conductor")
            .success(),
        "restarted liveness Conductor failed"
    );
    wait_for_recovery_path(&liveness_recovery).await;

    let logs = bundle
        .log_stream_provider()
        .expect("recovery LogStaging provider");
    logs.prepare().await.expect("prepare recovery LogStaging");
    let recovered_log_identity = LogStreamIdentity {
        task_instance_id: recovered_task_id,
        pickup_generation: 1,
    };
    let mut recovered_log = logs
        .open(
            recovery_log_route(workflow_id, workflow_instance_id, recovered_task_id),
            recovered_log_identity.clone(),
        )
        .await
        .expect("open recovered Task Log");
    assert_eq!(
        recovered_log
            .accept(LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: recovered_log_identity,
                    sequence: 0,
                },
                RECOVERY_UNHEALTHY_LOG.to_vec(),
            ))
            .await
            .expect("accept recovered Task Log"),
        AcceptOutcome::Accepted
    );
    assert_eq!(
        recovered_log
            .recover_abnormal_closure()
            .await
            .expect("record recovered Task abnormal closure"),
        TerminalOutcome::Recorded
    );

    let log_boundary = directory.path().join("log-boundary");
    let common_log_env = [
        ("TICKR_RECOVERY_WORKFLOW_ID", workflow_id.to_string()),
        (
            "TICKR_RECOVERY_WORKFLOW_INSTANCE_ID",
            workflow_instance_id.to_string(),
        ),
        (
            "TICKR_RECOVERY_COMPLETED_TASK_ID",
            completed_task_id.to_string(),
        ),
    ];
    let mut crashing_log_executor = spawn_recovery_helper(
        "log-crash",
        &process_env,
        &[
            common_log_env[0].clone(),
            common_log_env[1].clone(),
            common_log_env[2].clone(),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                log_boundary.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_recovery_path(&log_boundary).await;
    crashing_log_executor
        .kill()
        .expect("crash Executor at accepted Log boundary");
    crashing_log_executor
        .wait()
        .expect("reap Log-crashed Executor");

    let log_recovery = directory.path().join("log-recovered");
    let mut replacement_log_executor = spawn_recovery_helper(
        "log-recover",
        &process_env,
        &[
            common_log_env[0].clone(),
            common_log_env[1].clone(),
            common_log_env[2].clone(),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                log_recovery.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_log_executor
            .wait()
            .expect("wait for replacement Log Executor")
            .success(),
        "replacement Log Executor failed"
    );
    wait_for_recovery_path(&log_recovery).await;
    assert_eq!(
        fs::read(&log_recovery).expect("read ordered Log recovery evidence"),
        [RECOVERY_LOG_FIRST, RECOVERY_LOG_SECOND].concat()
    );

    let event_boundary = directory.path().join("task-event-boundary");
    let mut crashing_event_conductor = spawn_recovery_helper(
        "conductor-events-crash",
        &process_env,
        &[(
            "TICKR_RECOVERY_BOUNDARY_PATH",
            event_boundary.to_string_lossy().into_owned(),
        )],
    );
    wait_for_recovery_path(&event_boundary).await;
    crashing_event_conductor
        .kill()
        .expect("crash Conductor at TaskEvent boundary");
    crashing_event_conductor
        .wait()
        .expect("reap TaskEvent-crashed Conductor");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let event_recovery = directory.path().join("task-events-recovered");
    let mut replacement_event_conductor = spawn_recovery_helper(
        "conductor-events-recover",
        &process_env,
        &[
            ("TICKR_RECOVERY_EVENT_COUNT", "5".to_owned()),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                event_recovery.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_event_conductor
            .wait()
            .expect("wait for replacement TaskEvent Conductor")
            .success(),
        "replacement TaskEvent Conductor failed"
    );
    let events = fs::read_to_string(&event_recovery)
        .expect("read reconstructed TaskEvents")
        .lines()
        .map(|line| {
            tc::TaskEvent::decode(decode_recovery_hex(line).as_slice())
                .expect("decode typed TaskEvent")
        })
        .collect::<Vec<_>>();
    let completed_events = events
        .iter()
        .filter(|event| event.task_instance_id == completed_task_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(completed_events.len(), 3);
    assert_eq!(
        completed_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Assigned(_))))
            .count(),
        1,
        "the completing pickup generation has one durable owner"
    );
    assert_eq!(
        completed_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Completed(_))))
            .count(),
        1
    );
    let recovered_events = events
        .iter()
        .filter(|event| event.task_instance_id == recovered_task_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(recovered_events.len(), 2);
    assert_eq!(
        recovered_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Assigned(_))))
            .count(),
        1,
        "the recovered pickup generation has one durable owner"
    );
    assert_eq!(
        recovered_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Unhealthy(_))))
            .count(),
        1,
        "the recovered pickup generation has one terminal TaskEvent"
    );

    let staged_before_archive = logs
        .replay_task(recovery_log_route(
            workflow_id,
            workflow_instance_id,
            completed_task_id,
        ))
        .await
        .expect("read pre-archive accepted Log");
    assert_eq!(
        staged_before_archive
            .iter()
            .filter(|record| matches!(record, ReplayedLogRecord::Accepted { .. }))
            .count(),
        2
    );
    assert!(matches!(
        staged_before_archive.last(),
        Some(ReplayedLogRecord::Terminal { .. })
    ));

    let compaction = bundle
        .compaction_staging()
        .expect("recovery CompactionStaging");
    compaction
        .prepare()
        .await
        .expect("prepare recovery CompactionStaging");
    compaction
        .stage(&recovery_compaction_payload(
            workflow_id,
            workflow_instance_id,
            completed_task_id,
            recovered_task_id,
        ))
        .await
        .expect("stage recovery Compaction");

    let compaction_boundary = directory.path().join("compaction-boundary");
    let mut crashing_compaction_conductor = spawn_recovery_helper(
        "conductor-compaction-crash",
        &process_env,
        &[(
            "TICKR_RECOVERY_BOUNDARY_PATH",
            compaction_boundary.to_string_lossy().into_owned(),
        )],
    );
    wait_for_recovery_path(&compaction_boundary).await;
    crashing_compaction_conductor
        .kill()
        .expect("crash Conductor at Compaction boundary");
    crashing_compaction_conductor
        .wait()
        .expect("reap Compaction-crashed Conductor");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_instances WHERE id = $1")
            .bind(workflow_instance_id)
            .fetch_one(&pool)
            .await
            .expect("read archive state before restart"),
        0,
        "a held Compaction delivery is not an archive commit"
    );
    assert!(
        !logs
            .replay_task(recovery_log_route(
                workflow_id,
                workflow_instance_id,
                completed_task_id,
            ))
            .await
            .expect("read retained Log before Compaction restart")
            .is_empty(),
        "accepted Log staging must remain before archive commit"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    let compaction_recovery = directory.path().join("compaction-recovered");
    let mut replacement_compaction_conductor = spawn_recovery_helper(
        "conductor-compaction-recover",
        &process_env,
        &[
            ("TICKR_RECOVERY_POSTGRES_URL", postgres_url.clone()),
            ("TICKR_RECOVERY_LOG_ENDPOINT", minio_endpoint),
            ("TICKR_RECOVERY_WORKFLOW_ID", workflow_id.to_string()),
            (
                "TICKR_RECOVERY_COMPLETED_TASK_ID",
                completed_task_id.to_string(),
            ),
            (
                "TICKR_RECOVERY_WORKFLOW_INSTANCE_ID",
                workflow_instance_id.to_string(),
            ),
            (
                "TICKR_RECOVERY_BOUNDARY_PATH",
                compaction_recovery.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_compaction_conductor
            .wait()
            .expect("wait for replacement Compaction Conductor")
            .success(),
        "replacement Compaction Conductor failed"
    );
    wait_for_recovery_path(&compaction_recovery).await;

    let resolver = LogsResolver::new(storage, logs.clone());
    let completed_final_log = resolver
        .fetch_task_logs(workflow_id, workflow_instance_id, completed_task_id)
        .await
        .expect("read completed final Log");
    assert_eq!(
        completed_final_log.content,
        [RECOVERY_LOG_FIRST, RECOVERY_LOG_SECOND].concat()
    );
    assert!(completed_final_log.marker.is_some());
    let recovered_final_log = resolver
        .fetch_task_logs(workflow_id, workflow_instance_id, recovered_task_id)
        .await
        .expect("read recovered final Log");
    assert_eq!(recovered_final_log.content, RECOVERY_UNHEALTHY_LOG);
    assert!(
        logs.replay_task(recovery_log_route(
            workflow_id,
            workflow_instance_id,
            completed_task_id,
        ))
        .await
        .expect("read post-archive Log staging")
        .is_empty(),
        "Redis Log staging purges only after the final Log is installed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM task_instances WHERE workflow_instance_id = $1",
        )
        .bind(workflow_instance_id)
        .fetch_one(&pool)
        .await
        .expect("read archived Task instances"),
        2
    );

    let diagnostics = monitor.diagnostics();
    assert_eq!(
        diagnostics.capability_fingerprint,
        runtime_candidate.capability_fingerprint().as_str()
    );
    assert_eq!(diagnostics.profile, "all-redis");
    assert!(diagnostics.fence.ready);
    assert!(diagnostics
        .durability_class
        .contains("one local-primary AOF fsync"));
    assert!(diagnostics
        .durability_class
        .contains("zero required replica acknowledgements"));
    let health_projection =
        serde_json::to_string(&diagnostics).expect("serialize recovery Health diagnostics");
    for forbidden in [
        "endpoint",
        "username",
        "password",
        "query",
        "trust_root",
        "certificate",
        ADMIN_PASSWORD,
        tls.path.to_string_lossy().as_ref(),
    ] {
        assert!(
            !health_projection.contains(forbidden),
            "Health capability projection leaked {forbidden}"
        );
    }
    for role in &material {
        assert!(!health_projection.contains(&role.identity));
        assert!(!health_projection.contains(&role.secret));
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), bundle.shutdown_critical_children())
        .await
        .expect("release-smoke role shutdown timed out")
        .expect("join release-smoke role children");
    repositories.close().await;
    drop(bundle);
    drop(redis);
    drop(minio);
    drop(postgres);
}
