use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use redis::{aio::MultiplexedConnection, FromRedisValue};
use tickr_executor::local_pickup::{
    ExecutorCapacityObservation, ExecutorFleetSnapshot, ExecutorFleetStatus,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    formation::{CoordinationRole, ProtocolIdentity},
    redis_capability_monitor::{
        RedisCapabilityFenceState, RedisGenerationFence, RedisReconstructionCallback,
        RedisReconstructionFailure, RedisRoleCapabilityFailure, RedisRoleCapabilityProbe,
        RedisRoleCapabilityReporter, RedisRoleProbeContext,
    },
    redis_capacity::{RedisQuotaPressure, RedisQuotaState},
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
};

pub const REDIS_EXECUTOR_FLEET_STATUS_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.executor-fleet-status.redis-expiring-observation", 1);

const DEFAULT_OBSERVATION_TTL: Duration = Duration::from_secs(30);
const DEFAULT_SOFT_OBSERVATIONS: usize = 256;
const DEFAULT_HARD_OBSERVATIONS: usize = 512;
const MAX_OBSERVATIONS: usize = 65_536;
const MAX_CONFIGURED_PROCESS_SLOTS: usize = 1_000_000;

const REDIS_EXECUTOR_FLEET_STATUS_COMMANDS: &[&str] = &[
    "EVAL",
    "HDEL",
    "HGET",
    "HGETALL",
    "HSET",
    "TIME",
    "ZADD",
    "ZRANGEBYSCORE",
    "ZREM",
];
const EXECUTOR_FLEET_STATUS_SCRIPT_NAME: &str = "executor-fleet-status-v1";
const EXECUTOR_FLEET_STATUS_SCRIPT_SHA256: &str =
    "38b82b73332ef14e7d2868b6fc5951a1ef21396c79a1f4f019444785250ed864";

const EXECUTOR_FLEET_STATUS_SCRIPT: &str = r#"local operation = ARGV[1]

local function server_millis()
  local parts = redis.call('TIME')
  return (tonumber(parts[1]) * 1000) + math.floor(tonumber(parts[2]) / 1000)
end

local function split_record(record)
  local parts = {}
  for part in string.gmatch(record, '([^|]+)') do
    table.insert(parts, part)
  end
  if #parts ~= 6 then
    return nil
  end
  return parts
end

local function counter(name)
  return tonumber(redis.call('HGET', KEYS[3], name) or '0')
end

local function sweep(now)
  local used = counter('used')
  local expired_total = counter('expired')
  local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
  for _, executor_id in ipairs(due) do
    local record = redis.call('HGET', KEYS[1], executor_id)
    if not record then
      redis.call('ZREM', KEYS[2], executor_id)
    else
      local parts = split_record(record)
      if not parts then
        return nil, nil
      end
      local expires_at = tonumber(parts[6])
      if not expires_at then
        return nil, nil
      end
      if expires_at <= now then
        redis.call('HDEL', KEYS[1], executor_id)
        redis.call('ZREM', KEYS[2], executor_id)
        used = math.max(0, used - 1)
        expired_total = expired_total + 1
      end
    end
  end
  redis.call('HSET', KEYS[3], 'used', used, 'expired', expired_total)
  return used, expired_total
end

local now = server_millis()
local used, expired_total = sweep(now)
if not used then
  return {'corrupt', '0', '0', tostring(now), '0'}
end

if operation == 'report' then
  local executor_id = ARGV[2]
  local reporter_id = ARGV[3]
  local sequence = tonumber(ARGV[4])
  local configured_slots = tonumber(ARGV[5])
  local in_flight = tonumber(ARGV[6])
  local ttl_millis = tonumber(ARGV[7])
  local hard_limit = tonumber(ARGV[8])
  local existing = redis.call('HGET', KEYS[1], executor_id)
  local status = 'accepted'

  if existing then
    local parts = split_record(existing)
    if not parts then
      return {'corrupt', tostring(used), tostring(expired_total), tostring(now), '0'}
    end
    local existing_reporter = parts[1]
    local existing_sequence = tonumber(parts[2])
    local existing_slots = tonumber(parts[3])
    local existing_in_flight = tonumber(parts[4])
    if not existing_sequence or not existing_slots or not existing_in_flight then
      return {'corrupt', tostring(used), tostring(expired_total), tostring(now), '0'}
    end
    if existing_reporter ~= reporter_id then
      return {'conflict', tostring(used), tostring(expired_total), tostring(now), parts[6]}
    elseif sequence < existing_sequence then
      return {'stale', tostring(used), tostring(expired_total), tostring(now), parts[6]}
    elseif sequence == existing_sequence then
      if configured_slots ~= existing_slots or in_flight ~= existing_in_flight then
        return {'conflict', tostring(used), tostring(expired_total), tostring(now), parts[6]}
      end
      status = 'duplicate'
    else
      status = 'replaced'
    end
  elseif used >= hard_limit then
    return {'hard_limit', tostring(used), tostring(expired_total), tostring(now), '0'}
  else
    used = used + 1
  end

  local expires_at = now + ttl_millis
  local record = table.concat({
    reporter_id,
    tostring(sequence),
    tostring(configured_slots),
    tostring(in_flight),
    tostring(now),
    tostring(expires_at)
  }, '|')
  redis.call('HSET', KEYS[1], executor_id, record)
  redis.call('ZADD', KEYS[2], expires_at, executor_id)
  redis.call('HSET', KEYS[3], 'used', used, 'expired', expired_total)
  return {status, tostring(used), tostring(expired_total), tostring(now), tostring(expires_at)}
elseif operation == 'snapshot' then
  local response = {'snapshot', tostring(used), tostring(expired_total), tostring(now), '0'}
  local observations = redis.call('HGETALL', KEYS[1])
  for _, value in ipairs(observations) do
    table.insert(response, value)
  end
  return response
elseif operation == 'sweep' then
  return {'swept', tostring(used), tostring(expired_total), tostring(now), '0'}
end

return redis.error_reply('unknown executor-fleet-status operation')"#;

#[derive(Clone, Debug)]
pub struct RedisExecutorFleetStatusConfig {
    pub namespace: String,
    pub observation_ttl: Duration,
    pub soft_observation_limit: NonZeroUsize,
    pub hard_observation_limit: NonZeroUsize,
}

impl RedisExecutorFleetStatusConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            observation_ttl: DEFAULT_OBSERVATION_TTL,
            soft_observation_limit: NonZeroUsize::new(DEFAULT_SOFT_OBSERVATIONS)
                .expect("non-zero constant"),
            hard_observation_limit: NonZeroUsize::new(DEFAULT_HARD_OBSERVATIONS)
                .expect("non-zero constant"),
        }
    }

    fn validate(&self) -> Result<(), RedisExecutorFleetStatusError> {
        let valid_namespace = !self.namespace.is_empty()
            && self.namespace.len() <= 127
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid_namespace
            || self.observation_ttl.is_zero()
            || duration_millis(self.observation_ttl) == 0
            || self.soft_observation_limit >= self.hard_observation_limit
            || self.hard_observation_limit.get() > MAX_OBSERVATIONS
        {
            return Err(RedisExecutorFleetStatusError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisExecutorFleetStatusKeys {
    observations: String,
    expiries: String,
    quota: String,
}

impl RedisExecutorFleetStatusKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:executor-fleet-status");
        Self {
            observations: format!("{prefix}:observations"),
            expiries: format!("{prefix}:expiries"),
            quota: format!("{prefix}:quota"),
        }
    }
}

pub trait RedisExecutorFleetStatusCapability: Send + Sync {
    fn observation_open(&self) -> bool;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisExecutorFleetStatusQuotaState);
}

pub struct MonitoredRedisExecutorFleetStatusCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisExecutorFleetStatusCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisExecutorFleetStatusCapability for MonitoredRedisExecutorFleetStatusCapability {
    fn observation_open(&self) -> bool {
        self.fence.snapshot().state == RedisCapabilityFenceState::Open
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisExecutorFleetStatusQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisExecutorObservationOutcome {
    Accepted,
    Replaced,
    Duplicate,
    Stale,
    Conflict,
    FencedAtHardLimit,
    SuppressedByCapabilityFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisExecutorFleetStatusQuotaState {
    pub observed_executors: u64,
    pub expired_observations: u64,
    pub soft_observation_limit: u64,
    pub hard_observation_limit: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisExecutorFleetStatusQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.observed_executors,
            soft_threshold: self.soft_observation_limit,
            hard_limit: self.hard_observation_limit,
            accepted_identities: self.observed_executors,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisExecutorFleetStatus {
    client: redis::Client,
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
    keys: RedisExecutorFleetStatusKeys,
    config: Arc<RedisExecutorFleetStatusConfig>,
    capability: Arc<dyn RedisExecutorFleetStatusCapability>,
}

impl RedisExecutorFleetStatus {
    pub async fn connect(
        client: redis::Client,
        config: RedisExecutorFleetStatusConfig,
        capability: Arc<dyn RedisExecutorFleetStatusCapability>,
    ) -> Result<Self, RedisExecutorFleetStatusError> {
        config.validate()?;
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisExecutorFleetStatusError::Unavailable)?;
        Self::from_admitted(client, connection, config, capability)
    }

    pub(crate) fn from_admitted(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisExecutorFleetStatusConfig,
        capability: Arc<dyn RedisExecutorFleetStatusCapability>,
    ) -> Result<Self, RedisExecutorFleetStatusError> {
        config.validate()?;
        Ok(Self {
            client,
            connection: Arc::new(Mutex::new(Some(connection))),
            keys: RedisExecutorFleetStatusKeys::new(&config.namespace),
            config: Arc::new(config),
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_executor_fleet_status_operation_manifest()
    }

    pub async fn report(
        &self,
        observation: ExecutorCapacityObservation,
    ) -> Result<RedisExecutorObservationOutcome, RedisExecutorFleetStatusError> {
        if !self.capability.observation_open() {
            return Ok(RedisExecutorObservationOutcome::SuppressedByCapabilityFence);
        }
        if observation.configured_process_slots == 0
            || observation.configured_process_slots > MAX_CONFIGURED_PROCESS_SLOTS
            || observation.in_flight_count > observation.configured_process_slots
        {
            return Err(RedisExecutorFleetStatusError::InvalidObservation);
        }
        let response = self
            .execute(&[
                "report".to_owned(),
                observation.executor_id.to_string(),
                observation.reporter_id.to_string(),
                observation.sequence.to_string(),
                observation.configured_process_slots.to_string(),
                observation.in_flight_count.to_string(),
                duration_millis(self.config.observation_ttl).to_string(),
                self.config.hard_observation_limit.get().to_string(),
            ])
            .await?;
        let outcome = match response.status.as_str() {
            "accepted" => RedisExecutorObservationOutcome::Accepted,
            "replaced" => RedisExecutorObservationOutcome::Replaced,
            "duplicate" => RedisExecutorObservationOutcome::Duplicate,
            "stale" => RedisExecutorObservationOutcome::Stale,
            "conflict" => RedisExecutorObservationOutcome::Conflict,
            "hard_limit" => RedisExecutorObservationOutcome::FencedAtHardLimit,
            "corrupt" => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                return Err(RedisExecutorFleetStatusError::Accounting);
            }
            _ => return Err(RedisExecutorFleetStatusError::ProtocolViolation),
        };
        self.report_quota(response.quota_state(&self.config));
        Ok(outcome)
    }

    pub async fn snapshot(&self) -> Result<ExecutorFleetSnapshot, RedisExecutorFleetStatusError> {
        if !self.capability.observation_open() {
            return Err(RedisExecutorFleetStatusError::CapabilityFenced);
        }
        let response = self.execute(&["snapshot".to_owned()]).await?;
        if response.status == "corrupt" {
            self.capability
                .report_failure(RedisRoleCapabilityFailure::Accounting);
            return Err(RedisExecutorFleetStatusError::Accounting);
        }
        if response.status != "snapshot" || response.payload.len() % 2 != 0 {
            return Err(RedisExecutorFleetStatusError::ProtocolViolation);
        }
        let mut observations = Vec::with_capacity(response.payload.len() / 2);
        for pair in response.payload.chunks_exact(2) {
            let executor_id = Uuid::parse_str(&pair[0])
                .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
            observations.push(parse_observation(executor_id, &pair[1])?);
        }
        observations.sort_unstable_by_key(|observation| observation.executor_id);
        let quota = response.quota_state(&self.config);
        if observations.len() as u64 != quota.observed_executors {
            self.capability
                .report_failure(RedisRoleCapabilityFailure::Accounting);
            return Err(RedisExecutorFleetStatusError::Accounting);
        }
        self.report_quota(quota);
        Ok(ExecutorFleetSnapshot {
            server_time_millis: response.server_time_millis,
            observation_ttl_millis: duration_millis(self.config.observation_ttl),
            observations,
        })
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisExecutorFleetStatusQuotaState, RedisExecutorFleetStatusError> {
        let response = self.execute(&["snapshot".to_owned()]).await?;
        if response.status != "snapshot" {
            return Err(RedisExecutorFleetStatusError::ProtocolViolation);
        }
        let state = response.quota_state(&self.config);
        self.report_quota(state);
        Ok(state)
    }

    pub async fn sweep_expired(&self) -> Result<usize, RedisExecutorFleetStatusError> {
        let response = self.execute(&["sweep".to_owned()]).await?;
        if response.status == "corrupt" {
            self.capability
                .report_failure(RedisRoleCapabilityFailure::Accounting);
            return Err(RedisExecutorFleetStatusError::Accounting);
        }
        if response.status != "swept" {
            return Err(RedisExecutorFleetStatusError::ProtocolViolation);
        }
        let quota = response.quota_state(&self.config);
        self.report_quota(quota);
        usize::try_from(quota.observed_executors)
            .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)
    }

    async fn execute(
        &self,
        arguments: &[String],
    ) -> Result<ScriptResponse, RedisExecutorFleetStatusError> {
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(
                self.client
                    .get_multiplexed_tokio_connection()
                    .await
                    .map_err(|_| RedisExecutorFleetStatusError::Unavailable)?,
            );
        }
        let result = redis::cmd("EVAL")
            .arg(EXECUTOR_FLEET_STATUS_SCRIPT)
            .arg(3)
            .arg(&self.keys.observations)
            .arg(&self.keys.expiries)
            .arg(&self.keys.quota)
            .arg(arguments)
            .query_async::<Vec<redis::Value>>(connection.as_mut().expect("connection installed"))
            .await;
        match result {
            Ok(values) => ScriptResponse::from_values(values),
            Err(_) => {
                *connection = None;
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::RequiredOperation);
                Err(RedisExecutorFleetStatusError::Unavailable)
            }
        }
    }

    fn report_quota(&self, state: RedisExecutorFleetStatusQuotaState) {
        self.capability.report_quota(state);
    }
}
#[async_trait::async_trait]
impl ExecutorFleetStatus for RedisExecutorFleetStatus {
    fn observation_ttl(&self) -> Duration {
        self.config.observation_ttl
    }

    async fn report(&self, observation: ExecutorCapacityObservation) -> Result<(), String> {
        RedisExecutorFleetStatus::report(self, observation)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn fleet_snapshot(&self) -> Result<ExecutorFleetSnapshot, String> {
        RedisExecutorFleetStatus::snapshot(self)
            .await
            .map_err(|error| error.to_string())
    }
}

pub(crate) struct RedisExecutorFleetStatusRoleRegistration {
    client: redis::Client,
    connection: MultiplexedConnection,
    keys: RedisExecutorFleetStatusKeys,
    config: RedisExecutorFleetStatusConfig,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisExecutorFleetStatusRoleRegistration {
    pub(crate) fn new(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisExecutorFleetStatusConfig,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisExecutorFleetStatusError> {
        config.validate()?;
        Ok(Self {
            client,
            connection,
            keys: RedisExecutorFleetStatusKeys::new(&config.namespace),
            config,
            manifest_identity,
        })
    }

    pub(crate) fn build_role(
        &self,
        capability: Arc<dyn RedisExecutorFleetStatusCapability>,
    ) -> Result<RedisExecutorFleetStatus, RedisExecutorFleetStatusError> {
        RedisExecutorFleetStatus::from_admitted(
            self.client.clone(),
            self.connection.clone(),
            self.config.clone(),
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::ExecutorFleetStatus
            && context.manifest_identity() == &self.manifest_identity
            && redis_executor_fleet_status_operation_manifest()
                .map(|manifest| manifest.identity() == context.manifest_identity())
                .unwrap_or(false)
    }

    async fn representative_denial(&self, command: &mut redis::Cmd) -> bool {
        let mut connection = self.connection.clone();
        match command.query_async::<redis::Value>(&mut connection).await {
            Err(error) => error.code() == Some("NOPERM"),
            Ok(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl RedisRoleCapabilityProbe for RedisExecutorFleetStatusRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        let mut connection = self.connection.clone();
        redis::cmd("EVAL")
            .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
            .arg(1)
            .arg(&self.keys.quota)
            .arg("runtime-capability-canary")
            .query_async::<redis::Value>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|_| RedisRoleCapabilityFailure::RequiredOperation)
    }

    async fn probe_representative_denials(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::RepresentativeDenial);
        }
        let cross_role_key = format!(
            "tickr:{{{}}}:task-dispatch:runtime-capability-canary",
            self.config.namespace
        );
        let cross_role_denied = self
            .representative_denial(redis::cmd("GET").arg(cross_role_key))
            .await;
        let admin_denied = self
            .representative_denial(redis::cmd("ACL").arg("LIST"))
            .await;
        (cross_role_denied && admin_denied)
            .then_some(())
            .ok_or(RedisRoleCapabilityFailure::RepresentativeDenial)
    }
}

#[async_trait::async_trait]
impl RedisReconstructionCallback for RedisExecutorFleetStatusRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mut connection = self.connection.clone();
        let values = redis::cmd("EVAL")
            .arg(EXECUTOR_FLEET_STATUS_SCRIPT)
            .arg(3)
            .arg(&self.keys.observations)
            .arg(&self.keys.expiries)
            .arg(&self.keys.quota)
            .arg("sweep")
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        ScriptResponse::from_values(values)
            .and_then(|response| {
                (response.status == "swept")
                    .then_some(())
                    .ok_or(RedisExecutorFleetStatusError::ProtocolViolation)
            })
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_executor_fleet_status_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        EXECUTOR_FLEET_STATUS_SCRIPT_NAME,
        EXECUTOR_FLEET_STATUS_SCRIPT_SHA256,
    )?;
    RedisOperationManifest::new(
        CoordinationRole::ExecutorFleetStatus,
        REDIS_EXECUTOR_FLEET_STATUS_PROTOCOL,
        REDIS_EXECUTOR_FLEET_STATUS_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:executor-fleet-status:expiries",
            "tickr:{namespace}:executor-fleet-status:observations",
            "tickr:{namespace}:executor-fleet-status:quota",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:executor-fleet-status:observations"),
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::TaskDispatch,
            ),
            RedisForbiddenOperation::administrative("ACL LIST"),
        ],
    )
}

struct ScriptResponse {
    status: String,
    observed_executors: u64,
    expired_observations: u64,
    server_time_millis: u64,
    payload: Vec<String>,
}

impl ScriptResponse {
    fn from_values(values: Vec<redis::Value>) -> Result<Self, RedisExecutorFleetStatusError> {
        if values.len() < 5 {
            return Err(RedisExecutorFleetStatusError::ProtocolViolation);
        }
        let mut values = values.into_iter();
        let status = next_string(&mut values)?;
        let observed_executors = next_string(&mut values)?
            .parse()
            .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
        let expired_observations = next_string(&mut values)?
            .parse()
            .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
        let server_time_millis = next_string(&mut values)?
            .parse()
            .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
        let _expires_at = next_string(&mut values)?;
        let payload = values
            .map(|value| {
                String::from_redis_value(&value)
                    .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            status,
            observed_executors,
            expired_observations,
            server_time_millis,
            payload,
        })
    }

    fn quota_state(
        &self,
        config: &RedisExecutorFleetStatusConfig,
    ) -> RedisExecutorFleetStatusQuotaState {
        let pressure = if self.observed_executors >= config.hard_observation_limit.get() as u64 {
            RedisQuotaPressure::HardLimit
        } else if self.observed_executors >= config.soft_observation_limit.get() as u64 {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisExecutorFleetStatusQuotaState {
            observed_executors: self.observed_executors,
            expired_observations: self.expired_observations,
            soft_observation_limit: config.soft_observation_limit.get() as u64,
            hard_observation_limit: config.hard_observation_limit.get() as u64,
            pressure,
        }
    }
}

fn next_string(
    values: &mut impl Iterator<Item = redis::Value>,
) -> Result<String, RedisExecutorFleetStatusError> {
    String::from_redis_value(
        &values
            .next()
            .ok_or(RedisExecutorFleetStatusError::ProtocolViolation)?,
    )
    .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)
}

fn parse_observation(
    executor_id: Uuid,
    record: &str,
) -> Result<ExecutorCapacityObservation, RedisExecutorFleetStatusError> {
    let mut parts = record.split('|');
    let reporter_id = parts
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(RedisExecutorFleetStatusError::ProtocolViolation)?;
    let mut next_u64 = || {
        parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(RedisExecutorFleetStatusError::ProtocolViolation)
    };
    let sequence = next_u64()?;
    let configured_process_slots = usize::try_from(next_u64()?)
        .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
    let in_flight_count = usize::try_from(next_u64()?)
        .map_err(|_| RedisExecutorFleetStatusError::ProtocolViolation)?;
    let observed_at_server_millis = next_u64()?;
    let expires_at_server_millis = next_u64()?;
    if parts.next().is_some()
        || configured_process_slots == 0
        || in_flight_count > configured_process_slots
        || expires_at_server_millis <= observed_at_server_millis
    {
        return Err(RedisExecutorFleetStatusError::ProtocolViolation);
    }
    Ok(ExecutorCapacityObservation {
        executor_id,
        reporter_id,
        sequence,
        configured_process_slots,
        in_flight_count,
        observed_at_server_millis,
        expires_at_server_millis,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisExecutorFleetStatusError {
    InvalidConfiguration,
    InvalidObservation,
    CapabilityFenced,
    Unavailable,
    Accounting,
    ProtocolViolation,
}

impl fmt::Display for RedisExecutorFleetStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "invalid Redis ExecutorFleetStatus configuration",
            Self::InvalidObservation => "invalid Executor fleet observation",
            Self::CapabilityFenced => "Redis ExecutorFleetStatus capability is fenced",
            Self::Unavailable => "Redis ExecutorFleetStatus is unavailable",
            Self::Accounting => "Redis ExecutorFleetStatus accounting is inconsistent",
            Self::ProtocolViolation => "Redis ExecutorFleetStatus returned an invalid response",
        })
    }
}

impl std::error::Error for RedisExecutorFleetStatusError {}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_executor_fleet_status_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::ExecutorFleetStatus);
        assert_eq!(manifest.protocol(), REDIS_EXECUTOR_FLEET_STATUS_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_EXECUTOR_FLEET_STATUS_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(
            manifest.scripts()[0].name(),
            EXECUTOR_FLEET_STATUS_SCRIPT_NAME
        );
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(EXECUTOR_FLEET_STATUS_SCRIPT.as_bytes())
            ),
            EXECUTOR_FLEET_STATUS_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"SET"));
        assert!(!manifest.commands().contains(&"XADD"));
        assert_eq!(manifest.key_patterns().len(), 3);
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);

        let script = RedisScriptIdentity::new(
            EXECUTOR_FLEET_STATUS_SCRIPT_NAME,
            EXECUTOR_FLEET_STATUS_SCRIPT_SHA256,
        )
        .unwrap();
        let rejected = RedisOperationManifest::new(
            CoordinationRole::ExecutorFleetStatus,
            REDIS_EXECUTOR_FLEET_STATUS_PROTOCOL,
            REDIS_EXECUTOR_FLEET_STATUS_COMMANDS.to_vec(),
            vec![script],
            vec!["tickr:{namespace}:executor-fleet-status:observations"],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key("tickr:{namespace}:executor-fleet-status:observations"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::script(script),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("ACL LIST"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            rejected.failure(),
            crate::redis_operation_manifest::RedisOperationManifestFailure::UnregisteredOperation
        );
    }

    #[test]
    fn configuration_is_bounded_and_ordered() {
        let mut config = RedisExecutorFleetStatusConfig::new("formation");
        assert!(config.validate().is_ok());
        config.soft_observation_limit = NonZeroUsize::new(2).unwrap();
        config.hard_observation_limit = NonZeroUsize::new(2).unwrap();
        assert_eq!(
            config.validate(),
            Err(RedisExecutorFleetStatusError::InvalidConfiguration)
        );
    }
}
