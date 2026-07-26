use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use redis::{aio::MultiplexedConnection, FromRedisValue};
use tickr_conductor::signal_applied_notifier::{
    ByTagCancelMaterialization, SignalAppliedNotifier, SignalAppliedReconciliationStream,
    SignalAppliedReconciliationWake,
};
use tokio::sync::{mpsc, Mutex};
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

pub const REDIS_SIGNAL_APPLIED_NOTIFIER_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.signal-applied.redis-pubsub", 1);

const DEFAULT_HINT_TTL: Duration = Duration::from_secs(5);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SOFT_HINTS: usize = 64;
const DEFAULT_HARD_HINTS: usize = 128;
const MAX_HINTS: usize = 65_536;

const REDIS_SIGNAL_APPLIED_NOTIFIER_COMMANDS: &[&str] = &[
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
const SIGNAL_APPLIED_NOTIFIER_SCRIPT_NAME: &str = "signal-applied-notifier-v1";
const SIGNAL_APPLIED_NOTIFIER_SCRIPT_SHA256: &str =
    "6857c9f194879209244bccc883be5b2169fef24eb825bcad8e8d4d6268ce0b81";

const SIGNAL_APPLIED_NOTIFIER_SCRIPT: &str = r#"local operation = ARGV[1]

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
  for _, signal_id in ipairs(expired) do
    if redis.call('HDEL', KEYS[1], signal_id) == 1 then
      released = released + 1
    end
    redis.call('ZREM', KEYS[2], signal_id)
  end
  if released > 0 then
    redis.call('HINCRBY', KEYS[3], 'admitted', -released)
    redis.call('HINCRBY', KEYS[3], 'expired', released)
  end
end

local function state(status)
  return {
    status,
    counter('admitted'),
    counter('coalesced'),
    counter('omitted'),
    counter('expired')
  }
end

if operation == 'publish' then
  local signal_id = ARGV[2]
  local ticket = ARGV[3]
  local ttl_ms = tonumber(ARGV[4])
  local hard_limit = tonumber(ARGV[5])
  local channel = ARGV[6]
  local now = server_millis()
  purge_expired(now)

  local existing = redis.call('HGET', KEYS[1], signal_id)
  if existing then
    redis.call('HINCRBY', KEYS[3], 'coalesced', 1)
    return state('coalesced')
  end

  if counter('admitted') >= hard_limit then
    redis.call('HINCRBY', KEYS[3], 'omitted', 1)
    return state('hard_limit')
  end

  redis.call('HSET', KEYS[1], signal_id, ticket)
  redis.call('ZADD', KEYS[2], now + ttl_ms, signal_id)
  redis.call('HINCRBY', KEYS[3], 'admitted', 1)
  local subscribers = redis.call('PUBLISH', channel, ticket)
  if subscribers == 0 then
    redis.call('HDEL', KEYS[1], signal_id)
    redis.call('ZREM', KEYS[2], signal_id)
    redis.call('HINCRBY', KEYS[3], 'admitted', -1)
    redis.call('HINCRBY', KEYS[3], 'omitted', 1)
    return state('no_subscriber')
  end
  return state('published')
end

if operation == 'release' then
  local signal_id = ARGV[2]
  local ticket = ARGV[3]
  local existing = redis.call('HGET', KEYS[1], signal_id)
  if existing == ticket then
    redis.call('HDEL', KEYS[1], signal_id)
    redis.call('ZREM', KEYS[2], signal_id)
    redis.call('HINCRBY', KEYS[3], 'admitted', -1)
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

return redis.error_reply('unknown signal-applied-notifier operation')"#;

#[derive(Clone, Debug)]
pub struct RedisSignalAppliedNotifierConfig {
    pub namespace: String,
    pub hint_ttl: Duration,
    pub sweep_interval: Duration,
    pub soft_hint_limit: NonZeroUsize,
    pub hard_hint_limit: NonZeroUsize,
}

impl RedisSignalAppliedNotifierConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            hint_ttl: DEFAULT_HINT_TTL,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            soft_hint_limit: NonZeroUsize::new(DEFAULT_SOFT_HINTS).expect("non-zero constant"),
            hard_hint_limit: NonZeroUsize::new(DEFAULT_HARD_HINTS).expect("non-zero constant"),
        }
    }

    fn validate(&self) -> Result<(), RedisSignalAppliedNotifierError> {
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
            || self.hard_hint_limit.get() > MAX_HINTS
            || duration_millis(self.hint_ttl) == 0
        {
            return Err(RedisSignalAppliedNotifierError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisSignalAppliedNotifierKeys {
    hints: String,
    expiries: String,
    quota: String,
    channel_pattern: String,
}

impl RedisSignalAppliedNotifierKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:signal-applied-notifier");
        Self {
            hints: format!("{prefix}:hints"),
            expiries: format!("{prefix}:expiries"),
            quota: format!("{prefix}:quota"),
            channel_pattern: format!("{prefix}:materialized:*"),
        }
    }

    fn channel(&self, signal_id: Uuid) -> String {
        format!(
            "{}{}",
            self.channel_pattern.trim_end_matches('*'),
            signal_id
        )
    }
}

pub trait RedisSignalAppliedNotifierCapability: Send + Sync {
    fn delivery_open(&self) -> bool;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisSignalAppliedNotifierQuotaState);
}

pub struct MonitoredRedisSignalAppliedNotifierCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisSignalAppliedNotifierCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisSignalAppliedNotifierCapability for MonitoredRedisSignalAppliedNotifierCapability {
    fn delivery_open(&self) -> bool {
        self.fence.snapshot().state == RedisCapabilityFenceState::Open
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisSignalAppliedNotifierQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisSignalAppliedPublishOutcome {
    Published,
    Coalesced,
    OmittedAtHardLimit,
    OmittedWithoutSubscriber,
    SuppressedByCapabilityFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisSignalAppliedNotifierQuotaState {
    pub admitted_hints: u64,
    pub coalesced_hints: u64,
    pub omitted_hints: u64,
    pub expired_hints: u64,
    pub soft_hint_limit: u64,
    pub hard_hint_limit: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisSignalAppliedNotifierQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.admitted_hints,
            soft_threshold: self.soft_hint_limit,
            hard_limit: self.hard_hint_limit,
            accepted_identities: self.admitted_hints,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisSignalAppliedNotifierRole {
    client: redis::Client,
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
    keys: RedisSignalAppliedNotifierKeys,
    config: Arc<RedisSignalAppliedNotifierConfig>,
    capability: Arc<dyn RedisSignalAppliedNotifierCapability>,
}

impl RedisSignalAppliedNotifierRole {
    pub async fn connect(
        client: redis::Client,
        config: RedisSignalAppliedNotifierConfig,
        capability: Arc<dyn RedisSignalAppliedNotifierCapability>,
    ) -> Result<Self, RedisSignalAppliedNotifierError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisSignalAppliedNotifierError::Unavailable)?;
        Self::from_admitted(client, connection, config, capability)
    }

    pub(crate) fn from_admitted(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisSignalAppliedNotifierConfig,
        capability: Arc<dyn RedisSignalAppliedNotifierCapability>,
    ) -> Result<Self, RedisSignalAppliedNotifierError> {
        config.validate()?;
        Ok(Self {
            client,
            connection: Arc::new(Mutex::new(Some(connection))),
            keys: RedisSignalAppliedNotifierKeys::new(&config.namespace),
            config: Arc::new(config),
            capability,
        })
    }

    pub fn bounded_notifier(
        &self,
        capacity: NonZeroUsize,
    ) -> (RedisSignalAppliedNotifier, RedisSignalAppliedPublisher) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (
            RedisSignalAppliedNotifier { sender },
            RedisSignalAppliedPublisher {
                role: self.clone(),
                receiver,
            },
        )
    }

    pub async fn publish(
        &self,
        signal_id: Uuid,
    ) -> Result<RedisSignalAppliedPublishOutcome, RedisSignalAppliedNotifierError> {
        if !self.capability.delivery_open() {
            return Ok(RedisSignalAppliedPublishOutcome::SuppressedByCapabilityFence);
        }
        let response = self
            .execute(&[
                "publish".to_owned(),
                signal_id.to_string(),
                Uuid::new_v4().to_string(),
                duration_millis(self.config.hint_ttl).to_string(),
                self.config.hard_hint_limit.get().to_string(),
                self.keys.channel(signal_id),
            ])
            .await?;
        let outcome = match response.status.as_str() {
            "published" => RedisSignalAppliedPublishOutcome::Published,
            "coalesced" => RedisSignalAppliedPublishOutcome::Coalesced,
            "hard_limit" => RedisSignalAppliedPublishOutcome::OmittedAtHardLimit,
            "no_subscriber" => RedisSignalAppliedPublishOutcome::OmittedWithoutSubscriber,
            _ => return Err(RedisSignalAppliedNotifierError::ProtocolViolation),
        };
        self.report_quota(response);
        Ok(outcome)
    }

    pub async fn subscribe(
        &self,
    ) -> Result<RedisSignalAppliedNotificationStream, RedisSignalAppliedNotifierError> {
        let pubsub = self.open_pubsub().await?;
        Ok(RedisSignalAppliedNotificationStream {
            role: self.clone(),
            pubsub,
            closed: false,
        })
    }

    pub fn lazy_reconciliation_stream(&self) -> RedisSignalAppliedLazyNotificationStream {
        RedisSignalAppliedLazyNotificationStream {
            role: self.clone(),
            inner: None,
        }
    }

    async fn open_pubsub(&self) -> Result<redis::aio::PubSub, RedisSignalAppliedNotifierError> {
        let mut pubsub = self.client.get_async_pubsub().await.map_err(|_| {
            self.capability
                .report_failure(RedisRoleCapabilityFailure::RequiredOperation);
            RedisSignalAppliedNotifierError::Unavailable
        })?;
        pubsub
            .psubscribe(&self.keys.channel_pattern)
            .await
            .map_err(|_| {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::RequiredOperation);
                RedisSignalAppliedNotifierError::Unavailable
            })?;
        Ok(pubsub)
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisSignalAppliedNotifierQuotaState, RedisSignalAppliedNotifierError> {
        let response = self.execute(&["state".to_owned()]).await?;
        let state = self.quota(response);
        self.capability.report_quota(state);
        Ok(state)
    }

    pub async fn sweep_expired(&self) -> Result<(), RedisSignalAppliedNotifierError> {
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

    async fn release(&self, signal_id: Uuid, ticket: &str) {
        if let Ok(response) = self
            .execute(&[
                "release".to_owned(),
                signal_id.to_string(),
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
    ) -> Result<ScriptResponse, RedisSignalAppliedNotifierError> {
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(
                self.client
                    .get_multiplexed_tokio_connection()
                    .await
                    .map_err(|_| RedisSignalAppliedNotifierError::Unavailable)?,
            );
        }
        let result = redis::cmd("EVAL")
            .arg(SIGNAL_APPLIED_NOTIFIER_SCRIPT)
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
                Err(RedisSignalAppliedNotifierError::Unavailable)
            }
        }
    }

    fn quota(&self, response: ScriptResponse) -> RedisSignalAppliedNotifierQuotaState {
        let admitted_hints = response.admitted;
        RedisSignalAppliedNotifierQuotaState {
            admitted_hints,
            coalesced_hints: response.coalesced,
            omitted_hints: response.omitted,
            expired_hints: response.expired,
            soft_hint_limit: self.config.soft_hint_limit.get() as u64,
            hard_hint_limit: self.config.hard_hint_limit.get() as u64,
            pressure: if admitted_hints >= self.config.hard_hint_limit.get() as u64 {
                RedisQuotaPressure::HardLimit
            } else if admitted_hints >= self.config.soft_hint_limit.get() as u64 {
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

pub struct RedisSignalAppliedLazyNotificationStream {
    role: RedisSignalAppliedNotifierRole,
    inner: Option<RedisSignalAppliedNotificationStream>,
}

#[async_trait]
impl SignalAppliedReconciliationStream for RedisSignalAppliedLazyNotificationStream {
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        if self.inner.is_none() {
            match self.role.subscribe().await {
                Ok(stream) => self.inner = Some(stream),
                Err(_) => {
                    tokio::time::sleep(maximum_delay).await;
                    return SignalAppliedReconciliationWake::Deadline;
                }
            }
        }
        self.inner
            .as_mut()
            .expect("lazy Redis notification stream initialized")
            .next_reconciliation(maximum_delay)
            .await
    }
}

/// Admitted notifier role registered before either production side receives
/// its advisory-only interfaces.
pub(crate) struct RedisSignalAppliedNotifierRoleRegistration {
    client: redis::Client,
    connection: MultiplexedConnection,
    keys: RedisSignalAppliedNotifierKeys,
    config: RedisSignalAppliedNotifierConfig,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisSignalAppliedNotifierRoleRegistration {
    pub(crate) fn new(
        client: redis::Client,
        connection: MultiplexedConnection,
        config: RedisSignalAppliedNotifierConfig,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisSignalAppliedNotifierError> {
        config.validate()?;
        Ok(Self {
            client,
            connection,
            keys: RedisSignalAppliedNotifierKeys::new(&config.namespace),
            config,
            manifest_identity,
        })
    }

    pub(crate) fn build_role(
        &self,
        capability: Arc<dyn RedisSignalAppliedNotifierCapability>,
    ) -> Result<RedisSignalAppliedNotifierRole, RedisSignalAppliedNotifierError> {
        RedisSignalAppliedNotifierRole::from_admitted(
            self.client.clone(),
            self.connection.clone(),
            self.config.clone(),
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::SignalAppliedNotifier
            && context.manifest_identity() == &self.manifest_identity
            && redis_signal_applied_notifier_operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisSignalAppliedNotifierRoleRegistration {
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
            "tickr:{{{}}}:task-cancellation:runtime-capability-canary",
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
impl RedisReconstructionCallback for RedisSignalAppliedNotifierRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mut connection = self.connection.clone();
        let values = redis::cmd("EVAL")
            .arg(SIGNAL_APPLIED_NOTIFIER_SCRIPT)
            .arg(3)
            .arg(&self.keys.hints)
            .arg(&self.keys.expiries)
            .arg(&self.keys.quota)
            .arg("sweep")
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        ScriptResponse::from_values(values)
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

#[derive(Clone)]
pub struct RedisSignalAppliedNotifier {
    sender: mpsc::Sender<Uuid>,
}

impl SignalAppliedNotifier for RedisSignalAppliedNotifier {
    fn notify_bytag_cancel_materialized(&self, signal_id: Uuid) {
        let _ = self.sender.try_send(signal_id);
    }
}

pub struct RedisSignalAppliedPublisher {
    role: RedisSignalAppliedNotifierRole,
    receiver: mpsc::Receiver<Uuid>,
}

impl RedisSignalAppliedPublisher {
    pub async fn run(mut self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                signal_id = self.receiver.recv() => {
                    let Some(signal_id) = signal_id else { return; };
                    let _ = self.role.publish(signal_id).await;
                }
            }
        }
    }
}

pub struct RedisSignalAppliedNotificationStream {
    role: RedisSignalAppliedNotifierRole,
    pubsub: redis::aio::PubSub,
    closed: bool,
}

impl RedisSignalAppliedNotificationStream {
    pub async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        let deadline = tokio::time::Instant::now() + maximum_delay;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return SignalAppliedReconciliationWake::Deadline;
            }
            if self.closed {
                if !self.role.capability.delivery_open() {
                    tokio::time::sleep(remaining).await;
                    return SignalAppliedReconciliationWake::Deadline;
                }
                match self.role.open_pubsub().await {
                    Ok(pubsub) => {
                        self.pubsub = pubsub;
                        self.closed = false;
                    }
                    Err(_) => {
                        tokio::time::sleep(remaining).await;
                        return SignalAppliedReconciliationWake::Deadline;
                    }
                }
            }

            let message =
                match tokio::time::timeout(remaining, self.pubsub.on_message().next()).await {
                    Err(_) => return SignalAppliedReconciliationWake::Deadline,
                    Ok(Some(message)) => message,
                    Ok(None) => {
                        self.closed = true;
                        self.role
                            .capability
                            .report_failure(RedisRoleCapabilityFailure::RequiredOperation);
                        return SignalAppliedReconciliationWake::Deadline;
                    }
                };
            let Some(signal_id) = message
                .get_channel_name()
                .strip_prefix(self.role.keys.channel_pattern.trim_end_matches('*'))
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                continue;
            };
            let Ok(ticket) = message.get_payload::<String>() else {
                continue;
            };
            self.role.release(signal_id, &ticket).await;
            if self.role.capability.delivery_open() {
                return SignalAppliedReconciliationWake::Notification(ByTagCancelMaterialization {
                    signal_id,
                });
            }
        }
    }
}

#[async_trait]
impl SignalAppliedReconciliationStream for RedisSignalAppliedNotificationStream {
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        RedisSignalAppliedNotificationStream::next_reconciliation(self, maximum_delay).await
    }
}

pub fn redis_signal_applied_notifier_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        SIGNAL_APPLIED_NOTIFIER_SCRIPT_NAME,
        SIGNAL_APPLIED_NOTIFIER_SCRIPT_SHA256,
    )?;
    RedisOperationManifest::new(
        CoordinationRole::SignalAppliedNotifier,
        REDIS_SIGNAL_APPLIED_NOTIFIER_PROTOCOL,
        REDIS_SIGNAL_APPLIED_NOTIFIER_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:signal-applied-notifier:hints",
            "tickr:{namespace}:signal-applied-notifier:expiries",
            "tickr:{namespace}:signal-applied-notifier:quota",
        ],
        vec!["tickr:{namespace}:signal-applied-notifier:materialized:*"],
        vec![
            RedisRequiredOperationCanary::new(
                RedisOperation::script(script),
                RedisNamespacePattern::key("tickr:{namespace}:signal-applied-notifier:hints"),
            ),
            RedisRequiredOperationCanary::new(
                RedisOperation::command("PSUBSCRIBE"),
                RedisNamespacePattern::channel(
                    "tickr:{namespace}:signal-applied-notifier:materialized:*",
                ),
            ),
        ],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::TaskCancellation,
            ),
            RedisForbiddenOperation::administrative("ACL LIST"),
        ],
    )
}

struct ScriptResponse {
    status: String,
    admitted: u64,
    coalesced: u64,
    omitted: u64,
    expired: u64,
}

impl ScriptResponse {
    fn from_values(values: Vec<redis::Value>) -> Result<Self, RedisSignalAppliedNotifierError> {
        if values.len() != 5 {
            return Err(RedisSignalAppliedNotifierError::ProtocolViolation);
        }
        let mut values = values.into_iter();
        let status = String::from_redis_value(
            &values
                .next()
                .ok_or(RedisSignalAppliedNotifierError::ProtocolViolation)?,
        )
        .map_err(|_| RedisSignalAppliedNotifierError::ProtocolViolation)?;
        let mut next_u64 = || {
            u64::from_redis_value(
                &values
                    .next()
                    .ok_or(RedisSignalAppliedNotifierError::ProtocolViolation)?,
            )
            .map_err(|_| RedisSignalAppliedNotifierError::ProtocolViolation)
        };
        Ok(Self {
            status,
            admitted: next_u64()?,
            coalesced: next_u64()?,
            omitted: next_u64()?,
            expired: next_u64()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisSignalAppliedNotifierError {
    InvalidConfiguration,
    Unavailable,
    ProtocolViolation,
}

impl fmt::Display for RedisSignalAppliedNotifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "invalid Redis SignalAppliedNotifier configuration",
            Self::Unavailable => "Redis SignalAppliedNotifier is unavailable",
            Self::ProtocolViolation => "Redis SignalAppliedNotifier returned an invalid response",
        })
    }
}

impl std::error::Error for RedisSignalAppliedNotifierError {}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_signal_applied_notifier_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::SignalAppliedNotifier);
        assert_eq!(manifest.protocol(), REDIS_SIGNAL_APPLIED_NOTIFIER_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_SIGNAL_APPLIED_NOTIFIER_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(
            manifest.scripts()[0].name(),
            SIGNAL_APPLIED_NOTIFIER_SCRIPT_NAME
        );
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(SIGNAL_APPLIED_NOTIFIER_SCRIPT.as_bytes())
            ),
            SIGNAL_APPLIED_NOTIFIER_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"SET"));
        assert!(!manifest.commands().contains(&"XADD"));
        assert_eq!(manifest.required_canaries().len(), 2);
        assert_eq!(manifest.forbidden_operations().len(), 2);

        let script = RedisScriptIdentity::new(
            SIGNAL_APPLIED_NOTIFIER_SCRIPT_NAME,
            SIGNAL_APPLIED_NOTIFIER_SCRIPT_SHA256,
        )
        .unwrap();
        let rejected = RedisOperationManifest::new(
            CoordinationRole::SignalAppliedNotifier,
            REDIS_SIGNAL_APPLIED_NOTIFIER_PROTOCOL,
            REDIS_SIGNAL_APPLIED_NOTIFIER_COMMANDS.to_vec(),
            vec![script],
            vec!["tickr:{namespace}:signal-applied-notifier:hints"],
            vec!["tickr:{namespace}:signal-applied-notifier:materialized:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key("tickr:{namespace}:signal-applied-notifier:hints"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::script(script),
                    CoordinationRole::TaskCancellation,
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
    fn configuration_is_finite_and_ordered() {
        let mut config = RedisSignalAppliedNotifierConfig::new("formation");
        assert!(config.validate().is_ok());
        config.soft_hint_limit = NonZeroUsize::new(2).unwrap();
        config.hard_hint_limit = NonZeroUsize::new(2).unwrap();
        assert_eq!(
            config.validate(),
            Err(RedisSignalAppliedNotifierError::InvalidConfiguration)
        );
    }
}
