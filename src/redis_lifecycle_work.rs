use std::{
    fmt,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use redis::{aio::MultiplexedConnection, FromRedisValue};
use tickr_conductor::lifecycle_work::{
    LifecycleClaimAdmission, LifecyclePipeline, LifecycleWakeupSource, LifecycleWakeups,
    LifecycleWork,
};
use tickr_migrations::backend::WriterRepositoryBundle;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
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

pub const REDIS_LIFECYCLE_WORK_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.lifecycle-work.redis-advisory-notification", 1);

const DEFAULT_HINT_TTL: Duration = Duration::from_secs(5);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SOFT_HINTS: usize = 2;
const DEFAULT_HARD_HINTS: usize = 3;

const REDIS_LIFECYCLE_WORK_COMMANDS: &[&str] = &[
    "EVAL",
    "HDEL",
    "HGET",
    "HINCRBY",
    "HSET",
    "PSUBSCRIBE",
    "PUBLISH",
    "TIME",
    "ZADD",
    "ZRANGEBYSCORE",
    "ZREM",
];
const LIFECYCLE_WORK_SCRIPT_NAME: &str = "lifecycle-work-v1";
const LIFECYCLE_WORK_SCRIPT_SHA256: &str =
    "5ba9c91a61dfe13ecb45589f99ac9362ff0ef7271e78cab5ec8170712b2a83dc";

const LIFECYCLE_WORK_SCRIPT: &str = r#"local operation = ARGV[1]

local function server_millis()
  local parts = redis.call('TIME')
  return (tonumber(parts[1]) * 1000) + math.floor(tonumber(parts[2]) / 1000)
end

local function counter(name)
  return tonumber(redis.call('HGET', KEYS[3], name) or '0')
end

local function purge_expired(now)
  local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
  local released = 0
  for _, pipeline in ipairs(expired) do
    if redis.call('HDEL', KEYS[1], pipeline) == 1 then
      released = released + 1
    end
    redis.call('ZREM', KEYS[2], pipeline)
  end
  if released > 0 then
    redis.call('HINCRBY', KEYS[3], 'queued', -released)
    redis.call('HINCRBY', KEYS[3], 'expired', released)
  end
end

local function state(status)
  return {
    status,
    counter('queued'),
    counter('coalesced'),
    counter('dropped'),
    counter('expired')
  }
end

if operation == 'publish' then
  local pipeline = ARGV[2]
  local ticket = ARGV[3]
  local ttl_ms = tonumber(ARGV[4])
  local hard_limit = tonumber(ARGV[5])
  local channel = ARGV[6]
  local now = server_millis()
  purge_expired(now)

  local existing = redis.call('HGET', KEYS[1], pipeline)
  if existing then
    redis.call('HINCRBY', KEYS[3], 'coalesced', 1)
    return state('coalesced')
  end

  if counter('queued') >= hard_limit then
    redis.call('HINCRBY', KEYS[3], 'dropped', 1)
    return state('hard_limit')
  end

  redis.call('HSET', KEYS[1], pipeline, ticket)
  redis.call('ZADD', KEYS[2], now + ttl_ms, pipeline)
  redis.call('HINCRBY', KEYS[3], 'queued', 1)
  local subscribers = redis.call('PUBLISH', channel, ticket)
  if subscribers == 0 then
    redis.call('HDEL', KEYS[1], pipeline)
    redis.call('ZREM', KEYS[2], pipeline)
    redis.call('HINCRBY', KEYS[3], 'queued', -1)
    redis.call('HINCRBY', KEYS[3], 'dropped', 1)
    return state('no_subscriber')
  end
  return state('queued')
end

if operation == 'release' then
  local pipeline = ARGV[2]
  local ticket = ARGV[3]
  local existing = redis.call('HGET', KEYS[1], pipeline)
  if existing == ticket then
    redis.call('HDEL', KEYS[1], pipeline)
    redis.call('ZREM', KEYS[2], pipeline)
    redis.call('HINCRBY', KEYS[3], 'queued', -1)
  end
  return state('released')
end

if operation == 'sweep' then
  purge_expired(server_millis())
  return state('swept')
end

if operation == 'state' then
  purge_expired(server_millis())
  return state('state')
end

return redis.error_reply('unknown lifecycle-work operation')"#;

#[derive(Clone, Debug)]
pub struct RedisLifecycleWorkConfig {
    pub namespace: String,
    pub hint_ttl: Duration,
    pub sweep_interval: Duration,
    pub soft_hint_limit: NonZeroUsize,
    pub hard_hint_limit: NonZeroUsize,
}

impl RedisLifecycleWorkConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            hint_ttl: DEFAULT_HINT_TTL,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            soft_hint_limit: NonZeroUsize::new(DEFAULT_SOFT_HINTS).expect("non-zero constant"),
            hard_hint_limit: NonZeroUsize::new(DEFAULT_HARD_HINTS).expect("non-zero constant"),
        }
    }

    fn validate(&self) -> Result<(), RedisLifecycleWorkError> {
        let valid_namespace = !self.namespace.is_empty()
            && self.namespace.len() <= 127
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid_namespace
            || self.hint_ttl.is_zero()
            || self.sweep_interval.is_zero()
            || self.soft_hint_limit >= self.hard_hint_limit
            || self.hard_hint_limit.get() > LifecyclePipeline::ALL.len()
            || duration_millis(self.hint_ttl) == 0
        {
            return Err(RedisLifecycleWorkError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisLifecycleWorkKeys {
    hints: String,
    expiries: String,
    quota: String,
    channel_pattern: String,
}

impl RedisLifecycleWorkKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:lifecycle-work");
        Self {
            hints: format!("{prefix}:hints"),
            expiries: format!("{prefix}:expiries"),
            quota: format!("{prefix}:quota"),
            channel_pattern: format!("{prefix}:wakeup:*"),
        }
    }

    fn channel(&self, pipeline: LifecyclePipeline) -> String {
        format!(
            "{}{}",
            self.channel_pattern.trim_end_matches('*'),
            pipeline.as_str()
        )
    }
}

pub trait RedisLifecycleWorkCapability: Send + Sync {
    fn delivery_open(&self) -> bool;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisLifecycleWorkQuotaState);
}

pub struct MonitoredRedisLifecycleWorkCapability {
    fence: RedisGenerationFence,
    reporter: RwLock<Option<RedisRoleCapabilityReporter>>,
}

impl MonitoredRedisLifecycleWorkCapability {
    pub fn new(fence: RedisGenerationFence) -> Self {
        Self {
            fence,
            reporter: RwLock::new(None),
        }
    }

    pub fn install_reporter(&self, reporter: RedisRoleCapabilityReporter) {
        *self
            .reporter
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reporter);
    }

    fn with_reporter(&self, report: impl FnOnce(&RedisRoleCapabilityReporter)) {
        if let Some(reporter) = self
            .reporter
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            report(reporter);
        }
    }
}

impl RedisLifecycleWorkCapability for MonitoredRedisLifecycleWorkCapability {
    fn delivery_open(&self) -> bool {
        self.fence.snapshot().state == RedisCapabilityFenceState::Open
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.with_reporter(|reporter| reporter.report(failure));
    }

    fn report_quota(&self, state: RedisLifecycleWorkQuotaState) {
        self.with_reporter(|reporter| reporter.report_quota_state(state.role_projection()));
    }
}

#[derive(Clone)]
pub struct RedisLifecycleClaimAdmission {
    fence: RedisGenerationFence,
}

impl RedisLifecycleClaimAdmission {
    pub fn new(fence: RedisGenerationFence) -> Self {
        Self { fence }
    }
}

impl LifecycleClaimAdmission for RedisLifecycleClaimAdmission {
    fn claims_open(&self, _pipeline: LifecyclePipeline) -> bool {
        self.fence.snapshot().state == RedisCapabilityFenceState::Open
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisLifecyclePublishOutcome {
    Queued,
    Coalesced,
    DroppedAtHardLimit,
    DroppedWithoutSubscriber,
    SuppressedByCapabilityFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisLifecycleWorkQuotaState {
    pub queued_hints: u64,
    pub coalesced_hints: u64,
    pub dropped_hints: u64,
    pub expired_hints: u64,
    pub soft_hint_limit: u64,
    pub hard_hint_limit: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisLifecycleWorkQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.queued_hints,
            soft_threshold: self.soft_hint_limit,
            hard_limit: self.hard_hint_limit,
            accepted_identities: self.queued_hints,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisLifecycleWork {
    client: redis::Client,
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
    keys: RedisLifecycleWorkKeys,
    config: Arc<RedisLifecycleWorkConfig>,
    capability: Arc<dyn RedisLifecycleWorkCapability>,
}

impl RedisLifecycleWork {
    pub async fn connect(
        client: redis::Client,
        config: RedisLifecycleWorkConfig,
        capability: Arc<dyn RedisLifecycleWorkCapability>,
    ) -> Result<Self, RedisLifecycleWorkError> {
        config.validate()?;
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisLifecycleWorkError::Unavailable)?;
        Self::from_admitted(client, connection, config, capability)
    }

    fn from_admitted(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisLifecycleWorkConfig,
        capability: Arc<dyn RedisLifecycleWorkCapability>,
    ) -> Result<Self, RedisLifecycleWorkError> {
        config.validate()?;
        Ok(Self {
            client,
            connection: Arc::new(Mutex::new(Some(connection))),
            keys: RedisLifecycleWorkKeys::new(&config.namespace),
            config: Arc::new(config),
            capability,
        })
    }

    pub async fn publish(
        &self,
        pipeline: LifecyclePipeline,
    ) -> Result<RedisLifecyclePublishOutcome, RedisLifecycleWorkError> {
        if !self.capability.delivery_open() {
            return Ok(RedisLifecyclePublishOutcome::SuppressedByCapabilityFence);
        }
        let ticket = Uuid::new_v4().to_string();
        let response = self
            .execute(&[
                "publish".to_owned(),
                pipeline.as_str().to_owned(),
                ticket,
                duration_millis(self.config.hint_ttl).to_string(),
                self.config.hard_hint_limit.get().to_string(),
                self.keys.channel(pipeline),
            ])
            .await?;
        let outcome = match response.status.as_str() {
            "queued" => RedisLifecyclePublishOutcome::Queued,
            "coalesced" => RedisLifecyclePublishOutcome::Coalesced,
            "hard_limit" => RedisLifecyclePublishOutcome::DroppedAtHardLimit,
            "no_subscriber" => RedisLifecyclePublishOutcome::DroppedWithoutSubscriber,
            _ => return Err(RedisLifecycleWorkError::ProtocolViolation),
        };
        self.report_quota(response);
        Ok(outcome)
    }

    pub async fn subscribe(&self) -> Result<RedisLifecycleWakeupStream, RedisLifecycleWorkError> {
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|_| RedisLifecycleWorkError::Unavailable)?;
        pubsub
            .psubscribe(&self.keys.channel_pattern)
            .await
            .map_err(|_| RedisLifecycleWorkError::Unavailable)?;
        Ok(RedisLifecycleWakeupStream {
            role: self.clone(),
            pubsub,
        })
    }

    /// Bind this admitted Redis role to the production Conductor interfaces.
    /// Subscription remains lazy so reconstruction opens readiness first.
    pub fn conductor_lifecycle_work(
        &self,
        fence: RedisGenerationFence,
        capacity: NonZeroUsize,
    ) -> LifecycleWork {
        LifecycleWork::new(
            Box::new(RedisLifecycleWakeupSource { role: self.clone() }),
            Arc::new(RedisLifecycleClaimAdmission::new(fence)),
            capacity,
        )
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisLifecycleWorkQuotaState, RedisLifecycleWorkError> {
        let response = self.execute(&["state".to_owned()]).await?;
        let state = self.quota(response);
        self.capability.report_quota(state);
        Ok(state)
    }

    pub async fn sweep_expired(&self) -> Result<(), RedisLifecycleWorkError> {
        let response = self.execute(&["sweep".to_owned()]).await?;
        self.report_quota(response);
        Ok(())
    }

    pub async fn run_expiry_sweeper(&self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.config.sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    let _ = self.sweep_expired().await;
                }
            }
        }
    }

    async fn release(&self, pipeline: LifecyclePipeline, ticket: &str) {
        if let Ok(response) = self
            .execute(&[
                "release".to_owned(),
                pipeline.as_str().to_owned(),
                ticket.to_owned(),
            ])
            .await
        {
            self.report_quota(response);
        }
    }

    async fn execute(
        &self,
        arguments: &[String],
    ) -> Result<ScriptResponse, RedisLifecycleWorkError> {
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(
                self.client
                    .get_multiplexed_tokio_connection()
                    .await
                    .map_err(|_| RedisLifecycleWorkError::Unavailable)?,
            );
        }
        let result = redis::cmd("EVAL")
            .arg(LIFECYCLE_WORK_SCRIPT)
            .arg(3)
            .arg(&self.keys.hints)
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
                Err(RedisLifecycleWorkError::Unavailable)
            }
        }
    }

    fn quota(&self, response: ScriptResponse) -> RedisLifecycleWorkQuotaState {
        let queued_hints = response.queued;
        RedisLifecycleWorkQuotaState {
            queued_hints,
            coalesced_hints: response.coalesced,
            dropped_hints: response.dropped,
            expired_hints: response.expired,
            soft_hint_limit: self.config.soft_hint_limit.get() as u64,
            hard_hint_limit: self.config.hard_hint_limit.get() as u64,
            pressure: if queued_hints >= self.config.hard_hint_limit.get() as u64 {
                RedisQuotaPressure::HardLimit
            } else if queued_hints >= self.config.soft_hint_limit.get() as u64 {
                RedisQuotaPressure::SoftThreshold
            } else {
                RedisQuotaPressure::BelowSoftThreshold
            },
        }
    }

    fn report_quota(&self, response: ScriptResponse) {
        self.capability.report_quota(self.quota(response));
    }
}

pub struct RedisLifecycleWakeupStream {
    role: RedisLifecycleWork,
    pubsub: redis::aio::PubSub,
}

impl RedisLifecycleWakeupStream {
    pub async fn recv(&mut self) -> Result<LifecyclePipeline, RedisLifecycleWorkError> {
        loop {
            let message = self
                .pubsub
                .on_message()
                .next()
                .await
                .ok_or(RedisLifecycleWorkError::Unavailable)?;
            let channel = message.get_channel_name();
            let pipeline = channel
                .strip_prefix(self.role.keys.channel_pattern.trim_end_matches('*'))
                .and_then(LifecyclePipeline::parse)
                .ok_or(RedisLifecycleWorkError::ProtocolViolation)?;
            let ticket: String = message
                .get_payload()
                .map_err(|_| RedisLifecycleWorkError::ProtocolViolation)?;
            self.role.release(pipeline, &ticket).await;
            if self.role.capability.delivery_open() {
                return Ok(pipeline);
            }
        }
    }
}

pub struct RedisLifecycleWakeupSource {
    role: RedisLifecycleWork,
}

#[async_trait]
impl LifecycleWakeupSource for RedisLifecycleWakeupSource {
    async fn run(
        &mut self,
        wakeups: LifecycleWakeups,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut stream = self.role.subscribe().await?;
        stream.run(wakeups, cancel).await
    }
}

#[async_trait]
impl LifecycleWakeupSource for RedisLifecycleWakeupStream {
    async fn run(
        &mut self,
        wakeups: LifecycleWakeups,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                wakeup = self.recv() => {
                    let pipeline = wakeup.map_err(anyhow::Error::new)?;
                    wakeups.notify(pipeline);
                }
            }
        }
    }
}

pub(crate) struct RedisLifecycleWorkRoleRegistration {
    client: redis::Client,
    connection: MultiplexedConnection,
    keys: RedisLifecycleWorkKeys,
    config: RedisLifecycleWorkConfig,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisLifecycleWorkRoleRegistration {
    pub(crate) fn new(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisLifecycleWorkConfig,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisLifecycleWorkError> {
        config.validate()?;
        Ok(Self {
            client,
            connection,
            keys: RedisLifecycleWorkKeys::new(&config.namespace),
            config,
            manifest_identity,
        })
    }

    pub(crate) fn build_role(
        &self,
        capability: Arc<dyn RedisLifecycleWorkCapability>,
    ) -> Result<RedisLifecycleWork, RedisLifecycleWorkError> {
        RedisLifecycleWork::from_admitted(
            self.client.clone(),
            self.connection.clone(),
            self.config.clone(),
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::LifecycleWork
            && context.manifest_identity() == &self.manifest_identity
            && redis_lifecycle_work_operation_manifest()
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

#[async_trait]
impl RedisRoleCapabilityProbe for RedisLifecycleWorkRoleRegistration {
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

#[async_trait]
impl RedisReconstructionCallback for RedisLifecycleWorkRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mut connection = self.connection.clone();
        let values = redis::cmd("EVAL")
            .arg(LIFECYCLE_WORK_SCRIPT)
            .arg(3)
            .arg(&self.keys.hints)
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
                    .ok_or(RedisLifecycleWorkError::ProtocolViolation)
            })
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub struct RedisLifecycleReconstruction {
    role: Arc<RedisLifecycleWorkRoleRegistration>,
    repositories: Option<Arc<WriterRepositoryBundle>>,
    wakeups: LifecycleWakeups,
}

impl RedisLifecycleReconstruction {
    pub(crate) fn new(
        role: Arc<RedisLifecycleWorkRoleRegistration>,
        repositories: Option<Arc<WriterRepositoryBundle>>,
        wakeups: LifecycleWakeups,
    ) -> Self {
        Self {
            role,
            repositories,
            wakeups,
        }
    }
}

#[async_trait]
impl RedisReconstructionCallback for RedisLifecycleReconstruction {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        self.role.reconstruct(context).await?;
        let Some(repositories) = &self.repositories else {
            return Ok(());
        };
        let now = chrono::Utc::now();
        let definition_build = repositories
            .has_reclaimable_definition_build(now)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let patch_build = repositories
            .has_reclaimable_patch_build(now)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let patch_lifecycle = repositories
            .has_reclaimable_patch_lifecycle(now, now)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let submission = repositories
            .has_reclaimable_definition_submission(now)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;

        if definition_build {
            self.wakeups.notify(LifecyclePipeline::DefinitionBuild);
        }
        if patch_build || patch_lifecycle {
            self.wakeups.notify(LifecyclePipeline::PatchBuild);
        }
        if submission {
            self.wakeups.notify(LifecyclePipeline::Submission);
        }
        Ok(())
    }
}

pub fn redis_lifecycle_work_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script =
        RedisScriptIdentity::new(LIFECYCLE_WORK_SCRIPT_NAME, LIFECYCLE_WORK_SCRIPT_SHA256)?;
    RedisOperationManifest::new(
        CoordinationRole::LifecycleWork,
        REDIS_LIFECYCLE_WORK_PROTOCOL,
        REDIS_LIFECYCLE_WORK_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:lifecycle-work:hints",
            "tickr:{namespace}:lifecycle-work:expiries",
            "tickr:{namespace}:lifecycle-work:quota",
        ],
        vec!["tickr:{namespace}:lifecycle-work:wakeup:*"],
        vec![
            RedisRequiredOperationCanary::new(
                RedisOperation::script(script),
                RedisNamespacePattern::key("tickr:{namespace}:lifecycle-work:hints"),
            ),
            RedisRequiredOperationCanary::new(
                RedisOperation::command("PSUBSCRIBE"),
                RedisNamespacePattern::channel("tickr:{namespace}:lifecycle-work:wakeup:*"),
            ),
        ],
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
    queued: u64,
    coalesced: u64,
    dropped: u64,
    expired: u64,
}

impl ScriptResponse {
    fn from_values(values: Vec<redis::Value>) -> Result<Self, RedisLifecycleWorkError> {
        if values.len() != 5 {
            return Err(RedisLifecycleWorkError::ProtocolViolation);
        }
        let mut values = values.into_iter();
        let status = String::from_redis_value(
            &values
                .next()
                .ok_or(RedisLifecycleWorkError::ProtocolViolation)?,
        )
        .map_err(|_| RedisLifecycleWorkError::ProtocolViolation)?;
        let mut next_u64 = || {
            u64::from_redis_value(
                &values
                    .next()
                    .ok_or(RedisLifecycleWorkError::ProtocolViolation)?,
            )
            .map_err(|_| RedisLifecycleWorkError::ProtocolViolation)
        };
        Ok(Self {
            status,
            queued: next_u64()?,
            coalesced: next_u64()?,
            dropped: next_u64()?,
            expired: next_u64()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisLifecycleWorkError {
    InvalidConfiguration,
    Unavailable,
    ProtocolViolation,
}

impl fmt::Display for RedisLifecycleWorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "invalid Redis LifecycleWork configuration",
            Self::Unavailable => "Redis LifecycleWork is unavailable",
            Self::ProtocolViolation => "Redis LifecycleWork returned an invalid response",
        })
    }
}

impl std::error::Error for RedisLifecycleWorkError {}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_lifecycle_work_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::LifecycleWork);
        assert_eq!(manifest.protocol(), REDIS_LIFECYCLE_WORK_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_LIFECYCLE_WORK_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), LIFECYCLE_WORK_SCRIPT_NAME);
        assert_eq!(
            format!("{:x}", Sha256::digest(LIFECYCLE_WORK_SCRIPT.as_bytes())),
            LIFECYCLE_WORK_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"SET"));
        assert!(!manifest.commands().contains(&"XADD"));
        assert_eq!(manifest.required_canaries().len(), 2);
        assert_eq!(manifest.forbidden_operations().len(), 2);

        let script =
            RedisScriptIdentity::new(LIFECYCLE_WORK_SCRIPT_NAME, LIFECYCLE_WORK_SCRIPT_SHA256)
                .unwrap();
        let rejected = RedisOperationManifest::new(
            CoordinationRole::LifecycleWork,
            REDIS_LIFECYCLE_WORK_PROTOCOL,
            REDIS_LIFECYCLE_WORK_COMMANDS.to_vec(),
            vec![script],
            vec!["tickr:{namespace}:lifecycle-work:hints"],
            vec!["tickr:{namespace}:lifecycle-work:wakeup:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key("tickr:{namespace}:lifecycle-work:hints"),
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
    fn pipeline_names_and_configuration_are_bounded() {
        for pipeline in LifecyclePipeline::ALL {
            assert_eq!(LifecyclePipeline::parse(pipeline.as_str()), Some(pipeline));
        }
        let mut config = RedisLifecycleWorkConfig::new("formation");
        assert!(config.validate().is_ok());
        config.hard_hint_limit = NonZeroUsize::new(4).unwrap();
        assert_eq!(
            config.validate(),
            Err(RedisLifecycleWorkError::InvalidConfiguration)
        );
    }
}
