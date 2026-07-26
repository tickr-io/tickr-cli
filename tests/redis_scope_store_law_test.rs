#![cfg(not(madsim))]

use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use anyhow::Result;
use chrono::Utc;
use redis::{AsyncCommands as _, ConnectionInfo, TlsCertificates};
use tickr::{
    redis_capability_monitor::RedisRoleCapabilityFailure,
    redis_capacity::RedisQuotaPressure,
    redis_durability::RedisDurabilityGuard,
    redis_scope_store::{
        RedisScopeArchiveCommitOutcome, RedisScopeStore, RedisScopeStoreCapability,
        RedisScopeStoreConfig, RedisScopeStoreError, RedisScopeStoreQuotaState,
    },
};
use tickr_conductor::{captures_extractor::NamedEnvelope, scope_working_set::write_event_captures};
use tickr_ctx::envelope::{Envelope, Producer, SignalSource};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, DeleteTickrCtxScopeInput, ScopeCleanupOutcome, ScopeCreationOutcome,
    ScopeDeleteOutcome, ScopeMutationRejection, ScopeReadOutcome, ScopeSnapshotOutcome,
    ScopeValueInput, ScopeWriteOutcome, WriteTickrCtxScopeInput, MAX_SCOPE_VALUE_BYTES,
};
use uuid::Uuid;

const REDIS_IMAGE: &str = "redis:7.4.2";
const ADMIN_PASSWORD: &str = "redis-scope-admin-secret";
const ROLE_PASSWORD: &str = "redis-scope-store-secret";
const INITIAL: &[u8] = br#"{ "v": 2, "type": "string", "value": "original", "secret": false, "producer": { "kind": "task", "task_id": "task-7" }, "lineage": "exact-a" }"#;
const REPLACEMENT: &[u8] = br#"{  "v": 2, "type": "string", "value": "replacement", "secret": false, "producer": { "kind": "task", "task_id": "task-7" }, "lineage": "exact-b" }"#;
static NEXT_REDIS: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default)]
struct ToggleCapability {
    acknowledgement_open: AtomicBool,
    failures: Mutex<Vec<RedisRoleCapabilityFailure>>,
    quotas: Mutex<Vec<RedisScopeStoreQuotaState>>,
}

impl ToggleCapability {
    fn open() -> Self {
        Self {
            acknowledgement_open: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn lose_replies(&self) {
        self.acknowledgement_open.store(false, Ordering::SeqCst);
    }

    fn restore_replies(&self) {
        self.acknowledgement_open.store(true, Ordering::SeqCst);
    }
}

impl RedisScopeStoreCapability for ToggleCapability {
    fn guard_admission(&self) -> Result<u64, RedisScopeStoreError> {
        Ok(1)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisScopeStoreError> {
        if generation == 1 && self.acknowledgement_open.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RedisScopeStoreError::Unavailable)
        }
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        lock(&self.failures).push(failure);
    }

    fn report_quota(&self, state: RedisScopeStoreQuotaState) {
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
    async fn start() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("redis-scope-store-")
            .tempdir_in(std::env::current_dir().expect("current directory"))
            .expect("create Redis fixture directory");
        let path = directory.path().to_path_buf();
        let trust_roots = generate_tls(&path);
        let sequence = NEXT_REDIS.fetch_add(1, Ordering::Relaxed);
        let name = format!("tickr-redis-scope-store-{}-{sequence}", std::process::id());
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
                 user scopestore on >{ROLE_PASSWORD} ~tickr:{{*}}:scope-store:* -@all \
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
            "start Redis ScopeStore fixture",
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
        self.client_for("scopestore", ROLE_PASSWORD)
    }

    fn admin_client(&self) -> redis::Client {
        self.client_for("default", ADMIN_PASSWORD)
    }

    fn client_for(&self, username: &str, password: &str) -> redis::Client {
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
        .expect("Redis TLS client")
    }

    async fn restart(&mut self) {
        run(
            Command::new("docker").args(["restart", &self.name]),
            "restart Redis ScopeStore fixture",
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

async fn open_store(
    client: redis::Client,
    config: RedisScopeStoreConfig,
    capability: Arc<ToggleCapability>,
) -> Result<RedisScopeStore> {
    Ok(
        RedisScopeStore::connect(client, config, RedisDurabilityGuard::default(), capability)
            .await?,
    )
}

fn value<'a>(key: &'a str, envelope: &'a [u8]) -> ScopeValueInput<'a> {
    ScopeValueInput { key, envelope }
}

async fn exact_values(store: &RedisScopeStore, scope_id: Uuid) -> Vec<(String, Vec<u8>)> {
    match store
        .read_tickr_ctx_scope(scope_id, Utc::now())
        .await
        .expect("read scope")
    {
        ScopeReadOutcome::Present(values) => values
            .into_iter()
            .map(|value| (value.key, value.envelope))
            .collect(),
        outcome => panic!("scope must be active, got {outcome:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_redis_scope_laws_cover_bytes_atomicity_limits_restart_seal_corruption_and_cleanup(
) -> Result<()> {
    let mut fixture = RedisFixture::start().await;
    let capability = Arc::new(ToggleCapability::open());
    let namespace = format!("scope-law-{}", Uuid::new_v4().simple());
    let config = RedisScopeStoreConfig::new(namespace.clone());
    let mut store = open_store(fixture.client(), config.clone(), Arc::clone(&capability)).await?;
    let baseline = store.quota_state().await?;
    let wiring_store = open_store(
        fixture.client(),
        RedisScopeStoreConfig::new(format!("scope-wiring-{}", Uuid::new_v4().simple())),
        Arc::clone(&capability),
    )
    .await?;

    let signal_id = Uuid::new_v4();
    let event_capture = NamedEnvelope {
        name: "event-value".to_owned(),
        envelope: Envelope::new(
            "string",
            serde_json::Value::String("accepted".to_owned()),
            false,
            Producer::Signal {
                signal_id,
                source: SignalSource::Wakeup {
                    name: "payment-received".to_owned(),
                },
            },
        ),
    };
    let event_bytes = serde_json::to_vec(&event_capture.envelope)?;
    write_event_captures(
        &wiring_store,
        "default",
        signal_id,
        &signal_id.to_string(),
        signal_id,
        &[event_capture.clone()],
    )
    .await?;
    write_event_captures(
        &wiring_store,
        "default",
        signal_id,
        &signal_id.to_string(),
        signal_id,
        &[event_capture],
    )
    .await?;
    assert_eq!(
        exact_values(&wiring_store, signal_id).await,
        vec![(format!("{signal_id}/event-value"), event_bytes.clone())]
    );
    let changed_capture = NamedEnvelope {
        name: "event-value".to_owned(),
        envelope: Envelope::new(
            "string",
            serde_json::Value::String("changed".to_owned()),
            false,
            Producer::Signal {
                signal_id,
                source: SignalSource::Wakeup {
                    name: "payment-received".to_owned(),
                },
            },
        ),
    };
    assert!(
        write_event_captures(
            &wiring_store,
            "default",
            signal_id,
            &signal_id.to_string(),
            signal_id,
            &[changed_capture],
        )
        .await
        .is_err(),
        "changed Event-variable bytes must conflict under the stable capture identity"
    );
    assert_eq!(
        exact_values(&wiring_store, signal_id).await,
        vec![(format!("{signal_id}/event-value"), event_bytes)]
    );

    let scope_id = Uuid::new_v4();
    let run_id = scope_id.to_string();
    let create_claim = Uuid::new_v4();
    assert_eq!(
        store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id,
                namespace: "default",
                run_id: &run_id,
                claim_id: create_claim,
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created
    );
    assert_eq!(
        exact_values(&store, scope_id).await,
        vec![("result".to_owned(), INITIAL.to_vec())]
    );
    let active_quota = store.quota_state().await?;
    assert_eq!(active_quota.namespace_records, 1);
    assert_eq!(active_quota.scope_values, 1);
    let oversized_namespace = "n".repeat(129);
    assert_eq!(
        store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: Uuid::new_v4(),
                namespace: &oversized_namespace,
                run_id: "oversized-namespace",
                claim_id: Uuid::new_v4(),
                values: &[],
                now: Utc::now(),
            })
            .await,
        Err(RedisScopeStoreError::InvalidOperation)
    );

    let conflicting_scope = Uuid::new_v4();
    assert_eq!(
        store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: conflicting_scope,
                namespace: "default",
                run_id: &run_id,
                claim_id: Uuid::new_v4(),
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Collision {
            existing_scope_id: scope_id
        }
    );
    assert_eq!(
        store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: conflicting_scope,
                namespace: "other",
                run_id: "other-run",
                claim_id: create_claim,
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::ClaimConflict
    );

    let replace_claim = Uuid::new_v4();
    capability.lose_replies();
    assert_eq!(
        store
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: replace_claim,
                values: &[value("result", REPLACEMENT)],
                now: Utc::now(),
            })
            .await,
        Err(RedisScopeStoreError::Unavailable)
    );
    capability.restore_replies();
    assert_eq!(
        store
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: replace_claim,
                values: &[value("result", REPLACEMENT)],
                now: Utc::now(),
            })
            .await?,
        ScopeWriteOutcome::Idempotent
    );
    assert_eq!(
        exact_values(&store, scope_id).await,
        vec![("result".to_owned(), REPLACEMENT.to_vec())],
        "the adapter must not decode and re-encode opaque lineage envelopes"
    );

    let mut oversized = br#"{"v":2,"value":""#.to_vec();
    oversized.resize(MAX_SCOPE_VALUE_BYTES + 1, b'x');
    assert!(matches!(
        store
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: Uuid::new_v4(),
                values: &[value("result", &oversized)],
                now: Utc::now(),
            })
            .await?,
        ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(_))
    ));
    assert_eq!(
        exact_values(&store, scope_id).await,
        vec![("result".to_owned(), REPLACEMENT.to_vec())]
    );

    assert_eq!(
        store
            .cleanup_tickr_ctx_scope(scope_id, &"0".repeat(64), b"archive", Utc::now())
            .await?,
        ScopeCleanupOutcome::SnapshotRequired
    );
    let first = match store.snapshot_tickr_ctx_scope(scope_id, Utc::now()).await? {
        ScopeSnapshotOutcome::Committed(snapshot) => snapshot,
        outcome => panic!("scope must seal, got {outcome:?}"),
    };
    drop(store);
    fixture.restart().await;
    store = open_store(fixture.client(), config.clone(), Arc::clone(&capability)).await?;
    let repeated = match store
        .snapshot_tickr_ctx_scope_for_run("default", &run_id, Utc::now())
        .await?
    {
        ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
        outcome => panic!("scope seal must reconstruct, got {outcome:?}"),
    };
    assert_eq!(first, repeated);
    assert_eq!(
        store
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: Uuid::new_v4(),
                values: &[value("late", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeWriteOutcome::NotWritable(
            tickr_migrations::scope_repository::TickrCtxScopeState::Snapshotted
        )
    );
    assert!(
        store.quota_state().await?.used_bytes > baseline.used_bytes,
        "sealing alone must not release accepted scope capacity"
    );
    assert_eq!(
        store
            .record_verified_archive_commit(
                scope_id,
                &"1".repeat(64),
                b"wrong-archive",
                Utc::now(),
            )
            .await,
        Err(RedisScopeStoreError::IdentityConflict)
    );
    assert_eq!(
        store
            .cleanup_tickr_ctx_scope(scope_id, &first.digest, b"uncommitted-archive", Utc::now(),)
            .await,
        Err(RedisScopeStoreError::ArchiveNotCommitted)
    );

    let archive_identity = b"verified-workflow-archive";
    assert_eq!(
        store
            .record_verified_archive_commit(scope_id, &first.digest, archive_identity, Utc::now(),)
            .await?,
        RedisScopeArchiveCommitOutcome::Recorded
    );
    capability.lose_replies();
    assert_eq!(
        store
            .cleanup_tickr_ctx_scope(scope_id, &first.digest, archive_identity, Utc::now())
            .await,
        Err(RedisScopeStoreError::Unavailable)
    );
    capability.restore_replies();
    drop(store);
    store = open_store(fixture.client(), config, Arc::clone(&capability)).await?;
    assert_eq!(
        store
            .cleanup_tickr_ctx_scope(scope_id, &first.digest, archive_identity, Utc::now())
            .await?,
        ScopeCleanupOutcome::AlreadyCleaned
    );
    assert_eq!(
        store
            .snapshot_tickr_ctx_scope_for_run("default", &run_id, Utc::now())
            .await?,
        ScopeSnapshotOutcome::Idempotent(first.clone())
    );
    assert_eq!(store.quota_state().await?.used_bytes, baseline.used_bytes);

    let deleted_scope = Uuid::new_v4();
    let deleted_run = deleted_scope.to_string();
    store
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id: deleted_scope,
            namespace: "default",
            run_id: &deleted_run,
            claim_id: Uuid::new_v4(),
            values: &[value("result", INITIAL)],
            now: Utc::now(),
        })
        .await?;
    assert_eq!(
        store
            .delete_tickr_ctx_scope_value(DeleteTickrCtxScopeInput {
                scope_id: deleted_scope,
                claim_id: Uuid::new_v4(),
                key: "result",
                now: Utc::now(),
            })
            .await?,
        ScopeDeleteOutcome::Deleted
    );
    assert!(exact_values(&store, deleted_scope).await.is_empty());

    assert_eq!(
        store
            .snapshot_tickr_ctx_scope(Uuid::new_v4(), Utc::now())
            .await?,
        ScopeSnapshotOutcome::Missing
    );
    let corrupt_scope = Uuid::new_v4();
    let corrupt_run = corrupt_scope.to_string();
    store
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id: corrupt_scope,
            namespace: "default",
            run_id: &corrupt_run,
            claim_id: Uuid::new_v4(),
            values: &[value("result", INITIAL)],
            now: Utc::now(),
        })
        .await?;
    let mut admin = fixture
        .admin_client()
        .get_multiplexed_tokio_connection()
        .await?;
    let corrupt_key = format!("tickr:{{{namespace}}}:scope-store:scopes:{corrupt_scope}");
    admin.set::<_, _, ()>(&corrupt_key, b"not-a-scope").await?;
    assert_eq!(
        store
            .snapshot_tickr_ctx_scope(corrupt_scope, Utc::now())
            .await,
        Err(RedisScopeStoreError::CorruptScope)
    );
    let missing_accepted_scope = Uuid::new_v4();
    store
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id: missing_accepted_scope,
            namespace: "default",
            run_id: &missing_accepted_scope.to_string(),
            claim_id: Uuid::new_v4(),
            values: &[value("result", INITIAL)],
            now: Utc::now(),
        })
        .await?;
    let missing_key = format!("tickr:{{{namespace}}}:scope-store:scopes:{missing_accepted_scope}");
    admin.del::<_, ()>(&missing_key).await?;
    assert_eq!(
        store
            .snapshot_tickr_ctx_scope(missing_accepted_scope, Utc::now())
            .await,
        Err(RedisScopeStoreError::Accounting),
        "missing accepted state must fail Compaction rather than become an empty scope"
    );

    let pressure_namespace = format!("scope-pressure-{}", Uuid::new_v4().simple());
    let mut pressure_config = RedisScopeStoreConfig::new(pressure_namespace);
    pressure_config.soft_limit_bytes = 1;
    pressure_config.hard_limit_bytes = 8 * 1024 * 1024;
    pressure_config.hard_limit_scopes = 2;
    let pressure_store =
        open_store(fixture.client(), pressure_config, Arc::clone(&capability)).await?;
    let pressure_scope = Uuid::new_v4();
    let pressure_run = pressure_scope.to_string();
    pressure_store
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id: pressure_scope,
            namespace: "default",
            run_id: &pressure_run,
            claim_id: Uuid::new_v4(),
            values: &[value("result", INITIAL)],
            now: Utc::now(),
        })
        .await?;
    assert_eq!(
        pressure_store.quota_state().await?.pressure,
        RedisQuotaPressure::SoftThreshold
    );
    let retained_scope = Uuid::new_v4();
    assert_eq!(
        pressure_store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: retained_scope,
                namespace: "default",
                run_id: &retained_scope.to_string(),
                claim_id: Uuid::new_v4(),
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created
    );
    assert_eq!(
        pressure_store.quota_state().await?.pressure,
        RedisQuotaPressure::HardLimit
    );
    let fenced_scope = Uuid::new_v4();
    assert_eq!(
        pressure_store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: fenced_scope,
                namespace: "default",
                run_id: &fenced_scope.to_string(),
                claim_id: Uuid::new_v4(),
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await,
        Err(RedisScopeStoreError::CapacityFenced)
    );
    assert_eq!(
        exact_values(&pressure_store, pressure_scope).await,
        vec![("result".to_owned(), INITIAL.to_vec())]
    );
    let pressure_snapshot = match pressure_store
        .snapshot_tickr_ctx_scope(pressure_scope, Utc::now())
        .await?
    {
        ScopeSnapshotOutcome::Committed(snapshot) => snapshot,
        outcome => panic!("pressure scope must seal, got {outcome:?}"),
    };
    pressure_store
        .record_verified_archive_commit(
            pressure_scope,
            &pressure_snapshot.digest,
            b"pressure-archive",
            Utc::now(),
        )
        .await?;
    pressure_store
        .cleanup_tickr_ctx_scope(
            pressure_scope,
            &pressure_snapshot.digest,
            b"pressure-archive",
            Utc::now(),
        )
        .await?;
    assert_eq!(pressure_store.quota_state().await?.namespace_records, 1);
    assert_eq!(
        pressure_store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: fenced_scope,
                namespace: "default",
                run_id: &fenced_scope.to_string(),
                claim_id: Uuid::new_v4(),
                values: &[value("result", INITIAL)],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created
    );

    let mut role_connection = fixture.client().get_multiplexed_tokio_connection().await?;
    let cross_role: redis::RedisResult<()> = role_connection
        .set("tickr:{denied}:log-staging:stream", "forbidden")
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
        "Redis ScopeStore fixture did not become ready: {}{}",
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
                "/CN=Tickr Redis ScopeStore Test CA",
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

fn run(command: &mut Command, context: &str) {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{context}: {error}"));
    assert!(
        output.status.success(),
        "{context}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
