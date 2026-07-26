use std::{
    collections::HashMap,
    fmt,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    redis_capacity::{RedisCapacityFailure, RedisCapacityProfile},
    redis_durability::{RedisConditionalSetMutation, RedisDurabilityFailure, RedisDurabilityGuard},
    redis_formation_identity::RedisFormationAdmissionCandidate,
};
use redis::{
    aio::MultiplexedConnection, ConnectionInfo, ErrorKind, FromRedisValue, TlsCertificates, Value,
};
use serde::Deserialize;
use uuid::Uuid;

const ENDPOINT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SERVER_TIME_SKEW: Duration = Duration::from_secs(30);
const LOCAL_FSYNC_CANARY_TIMEOUT: Duration = Duration::from_secs(2);
static REDIS_TLS_PROVIDER: LazyLock<()> = LazyLock::new(|| {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
});

/// An external Redis connection descriptor parsed before any connection is created.
pub struct RedisConnectionDescriptor {
    endpoints: Vec<RedisEndpoint>,
    trust_roots_pem: Vec<u8>,
}

struct RedisEndpoint {
    connection_info: ConnectionInfo,
}

impl fmt::Debug for RedisConnectionDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisConnectionDescriptor")
            .field("endpoint_count", &self.endpoints.len())
            .field("trust_roots", &"[REDACTED]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalDescriptor {
    topology: ExternalTopology,
    endpoints: Vec<ExternalEndpoint>,
    trust_roots_pem: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExternalTopology {
    Direct,
    Sentinel,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalEndpoint {
    url: String,
    username: String,
    password: String,
}

impl RedisConnectionDescriptor {
    /// Parses the complete connection descriptor without opening sockets or creating clients.
    pub fn parse_json(input: &str) -> Result<Self, RedisAdmissionError> {
        let external: ExternalDescriptor = serde_json::from_str(input)
            .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::MalformedDescriptor))?;

        if matches!(external.topology, ExternalTopology::Sentinel) {
            return Err(RedisAdmissionError::new(
                RedisAdmissionFailure::SentinelTopology,
            ));
        }
        if external.endpoints.is_empty() {
            return Err(RedisAdmissionError::new(RedisAdmissionFailure::NoEndpoints));
        }
        if external.trust_roots_pem.trim().is_empty() {
            return Err(RedisAdmissionError::new(
                RedisAdmissionFailure::MissingTrustRoots,
            ));
        }

        let endpoints = external
            .endpoints
            .into_iter()
            .map(parse_endpoint)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            endpoints,
            trust_roots_pem: external.trust_roots_pem.into_bytes(),
        })
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub(crate) async fn connect_probe(&self) -> Result<MultiplexedConnection, RedisAdmissionError> {
        let endpoint = self.single_endpoint()?;
        self.connect(endpoint.connection_info.clone()).await
    }

    pub(crate) async fn connect_with_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<MultiplexedConnection, RedisAdmissionError> {
        self.client_with_credentials(username, password)?
            .get_multiplexed_tokio_connection()
            .await
            .map_err(classify_connection_error)
    }

    pub(crate) fn client_with_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<redis::Client, RedisAdmissionError> {
        if username.is_empty() || password.is_empty() {
            return Err(RedisAdmissionError::new(
                RedisAdmissionFailure::MissingCredentials,
            ));
        }
        let endpoint = self.single_endpoint()?;
        let mut connection_info = endpoint.connection_info.clone();
        connection_info.redis.username = Some(username.to_owned());
        connection_info.redis.password = Some(password.to_owned());
        self.build_client(connection_info)
    }

    fn single_endpoint(&self) -> Result<&RedisEndpoint, RedisAdmissionError> {
        if self.endpoints.len() != 1 {
            return Err(RedisAdmissionError::new(
                RedisAdmissionFailure::MultipleWritablePrimaries,
            ));
        }
        Ok(&self.endpoints[0])
    }

    async fn connect(
        &self,
        connection_info: ConnectionInfo,
    ) -> Result<MultiplexedConnection, RedisAdmissionError> {
        self.build_client(connection_info)?
            .get_multiplexed_tokio_connection()
            .await
            .map_err(classify_connection_error)
    }

    fn build_client(
        &self,
        connection_info: ConnectionInfo,
    ) -> Result<redis::Client, RedisAdmissionError> {
        LazyLock::force(&REDIS_TLS_PROVIDER);
        let certificates = TlsCertificates {
            client_tls: None,
            root_cert: Some(self.trust_roots_pem.clone()),
        };
        redis::Client::build_with_tls(connection_info, certificates)
            .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::TlsValidation))
    }
}

fn parse_endpoint(endpoint: ExternalEndpoint) -> Result<RedisEndpoint, RedisAdmissionError> {
    let mut url = redis::parse_redis_url(&endpoint.url)
        .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::MalformedDescriptor))?;

    if url.scheme() != "rediss" {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::PlaintextTransport,
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::EndpointParameters,
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::CredentialsInEndpoint,
        ));
    }
    if url.host_str().is_none() || !matches!(url.path(), "" | "/") {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::MalformedDescriptor,
        ));
    }
    if endpoint.username.is_empty() || endpoint.password.is_empty() {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::MissingCredentials,
        ));
    }

    url.set_username(&endpoint.username)
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::MalformedDescriptor))?;
    url.set_password(Some(&endpoint.password))
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::MalformedDescriptor))?;
    let connection_info = url
        .as_str()
        .parse::<ConnectionInfo>()
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::MalformedDescriptor))?;

    Ok(RedisEndpoint { connection_info })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedRedisCapability {
    pub server_version: String,
    pub topology: RedisTopology,
    pub server_time_micros: i64,
    pub capacity_profile: RedisCapacityProfile,
    pub used_memory_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisTopology {
    SingleWritablePrimary,
}

/// Establishes probe-only connections after pure descriptor and operation-manifest admission.
/// It returns no runtime Redis client.
pub async fn admit_redis_capability(
    descriptor: &RedisConnectionDescriptor,
    formation: &RedisFormationAdmissionCandidate,
) -> Result<AdmittedRedisCapability, RedisAdmissionError> {
    LazyLock::force(&REDIS_TLS_PROVIDER);
    let certificates = TlsCertificates {
        client_tls: None,
        root_cert: Some(descriptor.trust_roots_pem.clone()),
    };
    let mut admitted = Vec::with_capacity(descriptor.endpoints.len());

    for endpoint in &descriptor.endpoints {
        let probe = probe_endpoint(endpoint, certificates.clone(), formation);
        let result = tokio::time::timeout(ENDPOINT_PROBE_TIMEOUT, probe)
            .await
            .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ProbeTimedOut))??;
        admitted.push(result);
    }

    if admitted.len() != 1 {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::MultipleWritablePrimaries,
        ));
    }

    let endpoint = admitted.pop().expect("one admitted Redis endpoint");
    Ok(AdmittedRedisCapability {
        server_version: endpoint.version,
        topology: RedisTopology::SingleWritablePrimary,
        capacity_profile: formation.capacity_profile().clone(),
        used_memory_bytes: endpoint.used_memory_bytes,
        server_time_micros: endpoint.server_time_micros,
    })
}

struct EndpointCapability {
    version: String,
    server_time_micros: i64,
    used_memory_bytes: u64,
}

async fn probe_endpoint(
    endpoint: &RedisEndpoint,
    certificates: TlsCertificates,
    formation: &RedisFormationAdmissionCandidate,
) -> Result<EndpointCapability, RedisAdmissionError> {
    let client = redis::Client::build_with_tls(endpoint.connection_info.clone(), certificates)
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::TlsValidation))?;
    let mut connection = client
        .get_multiplexed_tokio_connection()
        .await
        .map_err(classify_connection_error)?;

    let facts = probe_server_facts(&mut connection).await?;
    validate_server_facts(&facts, formation.capacity_profile())?;
    let server_time_micros = probe_server_time(&mut connection).await?;
    prove_redis_primary_local_durability(&mut connection, formation).await?;

    Ok(EndpointCapability {
        version: facts.hello_version,
        used_memory_bytes: facts.used_memory_bytes,
        server_time_micros,
    })
}

fn classify_connection_error(error: redis::RedisError) -> RedisAdmissionError {
    if error.kind() == ErrorKind::AuthenticationFailed {
        RedisAdmissionError::new(RedisAdmissionFailure::CredentialRejected)
    } else {
        RedisAdmissionError::new(RedisAdmissionFailure::TlsValidation)
    }
}

struct ServerFacts {
    hello_server: String,
    hello_version: String,
    hello_mode: String,
    hello_role: String,
    info_version: String,
    info_mode: String,
    replication_role: String,
    role_command: String,
    cluster_enabled: String,
    aof_enabled: String,
    appendonly: String,
    appendfsync: String,
    required_commands_present: bool,
    maxmemory_bytes: u64,
    maxmemory_policy: String,
    used_memory_bytes: u64,
}

async fn probe_server_facts(
    connection: &mut MultiplexedConnection,
) -> Result<ServerFacts, RedisAdmissionError> {
    let hello: HashMap<String, Value> = redis::cmd("HELLO")
        .arg(3)
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;
    let info_server: String = redis::cmd("INFO")
        .arg("server")
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;
    let info_replication: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;
    let info_cluster: String = redis::cmd("INFO")
        .arg("cluster")
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;
    let info_persistence: String = redis::cmd("INFO")
        .arg("persistence")
        .query_async(&mut *connection)
        .await
        .map_err(classify_probe_error)?;
    let info_memory: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut *connection)
        .await
        .map_err(classify_probe_error)?;
    let persistence_config: HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .arg("appendfsync")
        .arg("maxmemory")
        .arg("maxmemory-policy")
        .query_async(&mut *connection)
        .await
        .map_err(classify_probe_error)?;
    let role: Value = redis::cmd("ROLE")
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;
    let command_info: Value = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("WAITAOF")
        .arg("TIME")
        .query_async(connection)
        .await
        .map_err(classify_probe_error)?;

    Ok(ServerFacts {
        hello_server: hello_string(&hello, "server")?,
        hello_version: hello_string(&hello, "version")?,
        hello_mode: hello_string(&hello, "mode")?,
        hello_role: hello_string(&hello, "role")?,
        info_version: info_field(&info_server, "redis_version")?,
        info_mode: info_field(&info_server, "redis_mode")?,
        replication_role: info_field(&info_replication, "role")?,
        role_command: role_name(&role)?,
        cluster_enabled: info_field(&info_cluster, "cluster_enabled")?,
        aof_enabled: info_field(&info_persistence, "aof_enabled")?,
        appendonly: persistence_config
            .get("appendonly")
            .cloned()
            .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))?,
        appendfsync: persistence_config
            .get("appendfsync")
            .cloned()
            .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))?,
        maxmemory_bytes: persistence_config
            .get("maxmemory")
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))?,
        maxmemory_policy: persistence_config
            .get("maxmemory-policy")
            .cloned()
            .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))?,
        used_memory_bytes: info_field(&info_memory, "used_memory")?
            .parse()
            .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))?,
        required_commands_present: command_info_has_all(&command_info, 2),
    })
}

fn classify_probe_error(error: redis::RedisError) -> RedisAdmissionError {
    if error.kind() == ErrorKind::AuthenticationFailed {
        RedisAdmissionError::new(RedisAdmissionFailure::CredentialRejected)
    } else {
        RedisAdmissionError::new(RedisAdmissionFailure::ProbeProtocol)
    }
}

fn hello_string(
    hello: &HashMap<String, Value>,
    field: &'static str,
) -> Result<String, RedisAdmissionError> {
    hello
        .get(field)
        .and_then(|value| String::from_redis_value(value).ok())
        .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))
}

fn info_field(info: &str, field: &'static str) -> Result<String, RedisAdmissionError> {
    info.lines()
        .filter_map(|line| line.strip_suffix('\r').unwrap_or(line).split_once(':'))
        .find_map(|(name, value)| (name == field).then(|| value.to_owned()))
        .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity))
}

fn role_name(role: &Value) -> Result<String, RedisAdmissionError> {
    match role {
        Value::Array(values) => values
            .first()
            .and_then(|value| String::from_redis_value(value).ok())
            .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerIdentity)),
        _ => Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ServerIdentity,
        )),
    }
}

fn command_info_has_all(value: &Value, expected: usize) -> bool {
    match value {
        Value::Array(commands) => {
            commands.len() == expected
                && commands
                    .iter()
                    .all(|command| !matches!(command, Value::Nil))
        }
        _ => false,
    }
}

fn validate_server_facts(
    facts: &ServerFacts,
    capacity_profile: &RedisCapacityProfile,
) -> Result<(), RedisAdmissionError> {
    if facts.hello_server != "redis" || facts.info_version != facts.hello_version {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ServerIdentity,
        ));
    }
    let mut version_parts = facts.hello_version.split('.');
    let version = (
        version_parts
            .next()
            .and_then(|part| part.parse::<u64>().ok()),
        version_parts
            .next()
            .and_then(|part| part.parse::<u64>().ok()),
        version_parts
            .next()
            .and_then(|part| part.parse::<u64>().ok()),
        version_parts.next(),
    );
    if !matches!(version, (Some(7), Some(4), Some(_), None)) {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ServerVersion,
        ));
    }
    if !facts.required_commands_present {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::RequiredCommandBehavior,
        ));
    }
    if facts.hello_mode != "standalone"
        || facts.info_mode != "standalone"
        || facts.cluster_enabled != "0"
    {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ClusterTopology,
        ));
    }
    if facts.hello_role != "master"
        || facts.replication_role != "master"
        || facts.role_command != "master"
    {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ReadOnlyOrReplica,
        ));
    }
    if facts.aof_enabled != "1" || facts.appendonly != "yes" {
        return Err(RedisAdmissionError::new(RedisAdmissionFailure::AofDisabled));
    }
    if facts.appendfsync != "always" {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::AppendFsyncNotAlways,
        ));
    }
    capacity_profile
        .validate_server(
            facts.maxmemory_bytes,
            &facts.maxmemory_policy,
            facts.used_memory_bytes,
        )
        .map_err(|failure| {
            RedisAdmissionError::new(RedisAdmissionFailure::InvalidCapacity(failure))
        })?;
    Ok(())
}

/// Proves one namespace-scoped mutation reaches the primary's local AOF before admission.
pub async fn prove_redis_primary_local_durability(
    connection: &mut MultiplexedConnection,
    formation: &RedisFormationAdmissionCandidate,
) -> Result<(), RedisAdmissionError> {
    let suffix = Uuid::new_v4().simple();
    let prefix = format!(
        "tickr:{}:admission:canary:durability:{suffix}",
        formation.namespace().as_str()
    );
    let token = Uuid::new_v4().simple().to_string().into_bytes();
    let mutation = RedisConditionalSetMutation::new(
        format!("{prefix}:operation"),
        format!("{prefix}:value"),
        token.clone(),
        Duration::from_secs(30),
    )
    .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::DurabilityCanaryFailed))?;
    let guard = RedisDurabilityGuard::new(LOCAL_FSYNC_CANARY_TIMEOUT, LOCAL_FSYNC_CANARY_TIMEOUT);
    guard
        .execute(&mut *connection, &mutation)
        .await
        .map_err(classify_durability_error)?;

    let installed: Option<Vec<u8>> = redis::cmd("GET")
        .arg(mutation.target_key())
        .query_async(&mut *connection)
        .await
        .map_err(classify_post_mutation_error)?;
    let role: Value = redis::cmd("ROLE")
        .query_async(&mut *connection)
        .await
        .map_err(classify_post_mutation_error)?;
    let removed: u64 = redis::cmd("DEL")
        .arg(mutation.identity_key())
        .arg(mutation.target_key())
        .query_async(&mut *connection)
        .await
        .map_err(classify_post_mutation_error)?;
    if installed.as_deref() != Some(token.as_slice())
        || role_name(&role)? != "master"
        || removed != 2
    {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::DurabilityCanaryFailed,
        ));
    }
    Ok(())
}

fn classify_durability_error(
    error: crate::redis_durability::RedisDurabilityError,
) -> RedisAdmissionError {
    let failure = match error.failure() {
        RedisDurabilityFailure::ReadOnlyPrimary => RedisAdmissionFailure::ReadOnlyOrReplica,
        RedisDurabilityFailure::AmbiguousLocalFsync
        | RedisDurabilityFailure::LocalFsyncUnavailable => {
            RedisAdmissionFailure::LocalFsyncProofFailed
        }
        RedisDurabilityFailure::InvalidOperation
        | RedisDurabilityFailure::AmbiguousMutation
        | RedisDurabilityFailure::MutationRejected
        | RedisDurabilityFailure::OutOfMemory
        | RedisDurabilityFailure::IdentityConflict => RedisAdmissionFailure::DurabilityCanaryFailed,
    };
    RedisAdmissionError::new(failure)
}

fn classify_post_mutation_error(error: redis::RedisError) -> RedisAdmissionError {
    if error.kind() == ErrorKind::ReadOnly {
        RedisAdmissionError::new(RedisAdmissionFailure::ReadOnlyOrReplica)
    } else {
        RedisAdmissionError::new(RedisAdmissionFailure::DurabilityCanaryFailed)
    }
}

async fn probe_server_time(
    connection: &mut MultiplexedConnection,
) -> Result<i64, RedisAdmissionError> {
    let local_before = system_time_micros()?;
    let first: (i64, i64) = redis::cmd("TIME")
        .query_async(connection)
        .await
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeUnavailable))?;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second: (i64, i64) = redis::cmd("TIME")
        .query_async(connection)
        .await
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeUnavailable))?;
    let local_after = system_time_micros()?;

    let first = redis_time_micros(first)?;
    let second = redis_time_micros(second)?;
    let skew = i64::try_from(MAX_SERVER_TIME_SKEW.as_micros())
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeInvalid))?;
    if second < first
        || first < local_before.saturating_sub(skew)
        || second > local_after.saturating_add(skew)
    {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ServerTimeInvalid,
        ));
    }
    Ok(second)
}

fn redis_time_micros(time: (i64, i64)) -> Result<i64, RedisAdmissionError> {
    let (seconds, micros) = time;
    if seconds <= 0 || !(0..1_000_000).contains(&micros) {
        return Err(RedisAdmissionError::new(
            RedisAdmissionFailure::ServerTimeInvalid,
        ));
    }
    seconds
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(micros))
        .ok_or_else(|| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeInvalid))
}

fn system_time_micros() -> Result<i64, RedisAdmissionError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeInvalid))?;
    i64::try_from(elapsed.as_micros())
        .map_err(|_| RedisAdmissionError::new(RedisAdmissionFailure::ServerTimeInvalid))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisAdmissionFailure {
    MalformedDescriptor,
    NoEndpoints,
    PlaintextTransport,
    MissingTrustRoots,
    MissingCredentials,
    EndpointParameters,
    CredentialsInEndpoint,
    SentinelTopology,
    TlsValidation,
    CredentialRejected,
    ProbeTimedOut,
    ProbeProtocol,
    ServerIdentity,
    ServerVersion,
    RequiredCommandBehavior,
    ReadOnlyOrReplica,
    ClusterTopology,
    MultipleWritablePrimaries,
    ServerTimeUnavailable,
    ServerTimeInvalid,
    AofDisabled,
    AppendFsyncNotAlways,
    LocalFsyncProofFailed,
    DurabilityCanaryFailed,
    InvalidCapacity(RedisCapacityFailure),
}

impl RedisAdmissionFailure {
    fn description(self) -> &'static str {
        match self {
            Self::MalformedDescriptor => "connection descriptor is malformed",
            Self::NoEndpoints => "no Redis endpoint is configured",
            Self::PlaintextTransport => "certificate-validated TLS is required",
            Self::MissingTrustRoots => "TLS trust roots are required",
            Self::MissingCredentials => "Redis credentials are required",
            Self::EndpointParameters => "endpoint query parameters and fragments are not admitted",
            Self::CredentialsInEndpoint => "credentials must use dedicated descriptor fields",
            Self::SentinelTopology => "Sentinel-mediated topology is not admitted",
            Self::TlsValidation => "certificate-validated TLS connection failed",
            Self::CredentialRejected => "Redis credentials were rejected",
            Self::ProbeTimedOut => "Redis capability probe timed out",
            Self::ProbeProtocol => "required Redis probe command failed",
            Self::ServerIdentity => "Redis OSS server identity was not proved",
            Self::ServerVersion => "Redis OSS 7.4.x is required",
            Self::RequiredCommandBehavior => "required Redis command behavior is unavailable",
            Self::ReadOnlyOrReplica => "a writable primary is required",
            Self::ClusterTopology => "cluster mode must be disabled",
            Self::MultipleWritablePrimaries => "exactly one writable primary is required",
            Self::ServerTimeUnavailable => "Redis server time is unavailable",
            Self::ServerTimeInvalid => "Redis server time is unsuitable for deadline arbitration",
            Self::AofDisabled => "Redis AOF persistence must be enabled",
            Self::AppendFsyncNotAlways => "Redis appendfsync must be always",
            Self::LocalFsyncProofFailed => "local-primary AOF fsync proof failed",
            Self::DurabilityCanaryFailed => "the local-primary durability canary failed",
            Self::InvalidCapacity(failure) => failure.description(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RedisAdmissionError {
    failure: RedisAdmissionFailure,
}

impl RedisAdmissionError {
    fn new(failure: RedisAdmissionFailure) -> Self {
        Self { failure }
    }

    pub fn failure(&self) -> RedisAdmissionFailure {
        self.failure
    }
}

impl fmt::Display for RedisAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Redis capability admission failed: {}",
            self.failure.description()
        )
    }
}

impl fmt::Debug for RedisAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAdmissionError")
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisAdmissionError {}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "-----BEGIN CERTIFICATE-----\nnot-a-real-root\n-----END CERTIFICATE-----";

    fn descriptor(url: &str, topology: &str, roots: &str) -> String {
        serde_json::json!({
            "topology": topology,
            "endpoints": [{
                "url": url,
                "username": "admission-user",
                "password": "very-secret-password"
            }],
            "trust_roots_pem": roots
        })
        .to_string()
    }

    fn valid_facts(version: &str) -> ServerFacts {
        ServerFacts {
            hello_server: "redis".to_owned(),
            hello_version: version.to_owned(),
            hello_mode: "standalone".to_owned(),
            hello_role: "master".to_owned(),
            info_version: version.to_owned(),
            info_mode: "standalone".to_owned(),
            replication_role: "master".to_owned(),
            role_command: "master".to_owned(),
            cluster_enabled: "0".to_owned(),
            aof_enabled: "1".to_owned(),
            appendonly: "yes".to_owned(),
            appendfsync: "always".to_owned(),
            maxmemory_bytes: 2_000_000_000,
            maxmemory_policy: "noeviction".to_owned(),
            used_memory_bytes: 0,
            required_commands_present: true,
        }
    }

    fn validate_test_facts(facts: &ServerFacts) -> Result<(), RedisAdmissionError> {
        validate_server_facts(
            facts,
            &RedisCapacityProfile::default_candidate(2_000_000_000).unwrap(),
        )
    }

    #[test]
    fn descriptor_rejects_plaintext_absent_roots_and_sentinel() {
        let plaintext = RedisConnectionDescriptor::parse_json(&descriptor(
            "redis://localhost:6379",
            "direct",
            ROOT,
        ))
        .unwrap_err();
        assert_eq!(
            plaintext.failure(),
            RedisAdmissionFailure::PlaintextTransport
        );

        let roots = RedisConnectionDescriptor::parse_json(&descriptor(
            "rediss://localhost:6379",
            "direct",
            "",
        ))
        .unwrap_err();
        assert_eq!(roots.failure(), RedisAdmissionFailure::MissingTrustRoots);

        let sentinel = RedisConnectionDescriptor::parse_json(&descriptor(
            "rediss://localhost:6379",
            "sentinel",
            ROOT,
        ))
        .unwrap_err();
        assert_eq!(sentinel.failure(), RedisAdmissionFailure::SentinelTopology);
    }

    #[test]
    fn descriptor_keeps_credentials_out_of_endpoints_and_diagnostics() {
        let error = RedisConnectionDescriptor::parse_json(&descriptor(
            "rediss://embedded:credential@secret.example:6379/?credential=leak",
            "direct",
            ROOT,
        ))
        .unwrap_err();
        let diagnostic = format!("{error:?} {error}");
        for secret in [
            "secret.example",
            "embedded",
            "credential",
            "very-secret-password",
            "not-a-real-root",
        ] {
            assert!(!diagnostic.contains(secret));
        }
    }

    #[test]
    fn only_redis_oss_7_4_with_required_behavior_is_admitted() {
        validate_test_facts(&valid_facts("7.4.0")).unwrap();
        validate_test_facts(&valid_facts("7.4.99")).unwrap();

        for version in ["7.3.9", "7.5.0", "8.0.0", "7.4.0-rc1"] {
            let error = validate_test_facts(&valid_facts(version)).unwrap_err();
            assert_eq!(error.failure(), RedisAdmissionFailure::ServerVersion);
        }

        let mut compatible = valid_facts("7.4.2");
        compatible.hello_server = "compatible-server".to_owned();
        assert_eq!(
            validate_test_facts(&compatible).unwrap_err().failure(),
            RedisAdmissionFailure::ServerIdentity
        );

        let mut missing_behavior = valid_facts("7.4.2");
        missing_behavior.required_commands_present = false;
        assert_eq!(
            validate_test_facts(&missing_behavior)
                .unwrap_err()
                .failure(),
            RedisAdmissionFailure::RequiredCommandBehavior
        );
    }
    #[test]
    fn admission_requires_aof_with_appendfsync_always() {
        let mut disabled = valid_facts("7.4.2");
        disabled.aof_enabled = "0".to_owned();
        disabled.appendonly = "no".to_owned();
        assert_eq!(
            validate_test_facts(&disabled).unwrap_err().failure(),
            RedisAdmissionFailure::AofDisabled
        );

        let mut every_second = valid_facts("7.4.2");
        every_second.appendfsync = "everysec".to_owned();
        assert_eq!(
            validate_test_facts(&every_second).unwrap_err().failure(),
            RedisAdmissionFailure::AppendFsyncNotAlways
        );
    }

    #[test]
    fn topology_requires_non_cluster_writable_primary() {
        let mut replica = valid_facts("7.4.2");
        replica.hello_role = "replica".to_owned();
        assert_eq!(
            validate_test_facts(&replica).unwrap_err().failure(),
            RedisAdmissionFailure::ReadOnlyOrReplica
        );

        let mut cluster = valid_facts("7.4.2");
        cluster.cluster_enabled = "1".to_owned();
        assert_eq!(
            validate_test_facts(&cluster).unwrap_err().failure(),
            RedisAdmissionFailure::ClusterTopology
        );
    }

    #[test]
    fn redis_time_requires_valid_monotonic_components() {
        assert_eq!(redis_time_micros((1, 2)).unwrap(), 1_000_002);
        for invalid in [(0, 0), (1, -1), (1, 1_000_000), (i64::MAX, 0)] {
            assert_eq!(
                redis_time_micros(invalid).unwrap_err().failure(),
                RedisAdmissionFailure::ServerTimeInvalid
            );
        }
    }
}
