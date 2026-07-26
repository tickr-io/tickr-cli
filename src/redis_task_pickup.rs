#![allow(async_fn_in_trait)]

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use redis::{
    aio::MultiplexedConnection,
    streams::{
        StreamAutoClaimOptions, StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply,
    },
    AsyncCommands as _,
};
use sha2::{Digest, Sha256};
use tickr_executor::{
    local_pickup::{
        ClaimLocalPickup, ClaimWriteError, DueLocalPickup, LocalAttemptOutcome, LocalPickupClaim,
        PendingLocalDispatch, SafeAttemptOutcomeHandoff, SafePickupWriter, TerminalElection,
    },
    wire::{decode_dispatch, encode_unhealthy_task_event, CancelRequest},
};
use tickr_proto::{
    coord::{TaskDispatchFuture, TaskDispatchPublisher},
    task as tc,
};

use crate::{
    formation::{CoordinationRole, ProtocolIdentity},
    redis_capability_monitor::{
        RedisCapabilityFenceState, RedisGenerationFence, RedisReconstructionCallback,
        RedisReconstructionFailure, RedisRoleCapabilityFailure, RedisRoleCapabilityProbe,
        RedisRoleCapabilityReporter, RedisRoleProbeContext,
    },
    redis_capacity::{RedisQuotaPressure, RedisQuotaState},
    redis_durability::{
        RedisDurabilityError, RedisDurabilityFailure, RedisDurabilityGuard, RedisMutationError,
        RedisStableMutation, RedisStableMutationOutcome, RedisStableMutationRecovery,
        RedisStableOperation,
    },
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
};

pub const REDIS_TASK_DISPATCH_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.task-dispatch.redis-stream", 1);
pub const REDIS_TASK_DISPATCH_GROUP: &str = "tickr-task-dispatch-v1";

const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_millis(100);
const DEFAULT_POLL: Duration = Duration::from_millis(25);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_DISPATCHES: usize = 4096;
const DEFAULT_MAX_ACTIVE_CLAIMS: usize = 256;
const DEFAULT_MAX_STAGED_EVENTS: usize = 16_384;
const DEFAULT_SOFT_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 56 * 1024 * 1024;
const DISPATCH_ACCOUNTED_BYTES: u64 = 192;
const CLAIM_ACCOUNTED_BYTES: u64 = 384;
const EVENT_ACCOUNTED_BYTES: u64 = 128;

const REDIS_TASK_DISPATCH_COMMANDS: &[&str] = &[
    "EVAL",
    "GET",
    "HDEL",
    "HGET",
    "HINCRBY",
    "HMGET",
    "HSET",
    "SET",
    "TIME",
    "WAITAOF",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XDEL",
    "XGROUP CREATE",
    "XRANGE",
    "XREADGROUP",
    "ZADD",
    "ZRANGEBYSCORE",
    "ZREM",
];

const TASK_DISPATCH_SCRIPT_NAME: &str = "task-dispatch-v1";
const TASK_DISPATCH_SCRIPT_SHA256: &str =
    "550a7257073815675c71a71204c32b90760280a58428340b8b1a2af4d4997afc";

const TASK_DISPATCH_SCRIPT: &str = r#"local operation = ARGV[1]
local stable_digest = ARGV[2]
local dispatch_key = ARGV[3]
local identity = ARGV[4]
local digest = ARGV[5]
local payload = ARGV[6]
local owner = ARGV[7]
local timeout_ms = tonumber(ARGV[8])
local event = ARGV[9]
local event_digest = ARGV[10]
local reason = ARGV[11]
local outcome = ARGV[12]
local dispatch_units = tonumber(ARGV[13])
local staged_units = tonumber(ARGV[14])
local max_dispatches = tonumber(ARGV[15])
local max_active_claims = tonumber(ARGV[16])
local max_staged_events = tonumber(ARGV[17])
local hard_bytes = tonumber(ARGV[18])
local group = ARGV[19]
local generation = tonumber(ARGV[20])
local task_key = ARGV[21]


local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function record_generation()
    return tonumber(redis.call('HGET', KEYS[5], dispatch_key) or '0')
end

local function record_owner()
    return redis.call('HGET', KEYS[6], dispatch_key) or ''
end

local function state(status, detail)
    return {
        status,
        detail or '',
        tostring(number_field(KEYS[17], 'used_bytes')),
        tostring(number_field(KEYS[17], 'dispatch_entries')),
        tostring(number_field(KEYS[17], 'active_claims')),
        tostring(number_field(KEYS[17], 'staged_events')),
        tostring(record_generation()),
        record_owner(),
        redis.call('HGET', KEYS[7], dispatch_key) or '0',
        redis.call('HGET', KEYS[10], dispatch_key) or '',
        redis.call('HGET', KEYS[12], dispatch_key) or '0',
        redis.call('HGET', KEYS[14], dispatch_key) or ''
    }
end


local function stream_entry(id)
    local rows = redis.call('XRANGE', KEYS[1], id, id, 'COUNT', 1)
    if #rows ~= 1 then
        return nil
    end
    local values = rows[1][2]
    local fields = {}
    for i = 1, #values, 2 do
        fields[values[i]] = values[i + 1]
    end
    return fields
end

local function exact_claim()
    return record_generation() == generation and record_owner() == owner
end
if operation == 'terminal' then
    local elected = redis.call('HGET', KEYS[14], dispatch_key)
    if elected then
        return state('settled', elected)
    end
    if not exact_claim() then
        return state('stale', '')
    end
end

if operation == 'renew' or operation == 'register_failure' then
    if not exact_claim() or redis.call('HGET', KEYS[14], dispatch_key)
        or redis.call('HGET', KEYS[11], dispatch_key) ~= '1' then
        return state('stale', '')
    end
end

local prior_operation = redis.call('GET', KEYS[19])
if prior_operation and prior_operation ~= stable_digest then
    return {'conflict'}
end
if not prior_operation then
    redis.call('SET', KEYS[19], stable_digest)
end

local function server_millis()
    local now = redis.call('TIME')
    return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

local function within_capacity(extra_bytes, extra_dispatches, extra_claims, extra_events)
    local used = number_field(KEYS[17], 'used_bytes')
    local dispatches = number_field(KEYS[17], 'dispatch_entries')
    local claims = number_field(KEYS[17], 'active_claims')
    local events = number_field(KEYS[17], 'staged_events')
    return extra_bytes <= hard_bytes and used <= hard_bytes - extra_bytes
        and dispatches + extra_dispatches <= max_dispatches
        and claims + extra_claims <= max_active_claims
        and events + extra_events <= max_staged_events
end

local function release_dispatch(fields)
    local accepted_identity = fields['identity']
    local accepted_units = tonumber(fields['units'] or '-1')
    if not accepted_identity or accepted_units < 0 then
        return false
    end
    redis.call('XACK', KEYS[1], group, dispatch_key)
    redis.call('XDEL', KEYS[1], dispatch_key)
    redis.call('HDEL', KEYS[2], accepted_identity)
    redis.call('HDEL', KEYS[3], accepted_identity)
    redis.call('HDEL', KEYS[4], accepted_identity)
    redis.call('HINCRBY', KEYS[17], 'used_bytes', -accepted_units)
    redis.call('HINCRBY', KEYS[17], 'dispatch_entries', -1)
    redis.call('HSET', KEYS[12], dispatch_key, '1')
    return true
end

if operation == 'ensure_group' then
    local created = redis.pcall('XGROUP', 'CREATE', KEYS[1], group, '0', 'MKSTREAM')
    if type(created) == 'table' and created.err then
        if string.find(created.err, 'BUSYGROUP', 1, true) then
            return state('replayed', '')
        end
        return redis.error_reply(created.err)
    end
    return state('created', '')
end

if operation == 'append' then
    if task_key ~= '' then
        local cancellation = redis.call('HGET', KEYS[23], task_key)
        if cancellation then
            return state('cancelled', cancellation)
        end
    end
    local prior = redis.call('HGET', KEYS[2], identity)
    if prior then
        if redis.call('HGET', KEYS[3], identity) ~= digest then
            return state('conflict', '')
        end
        local prior_units = tonumber(redis.call('HGET', KEYS[4], identity) or '-1')
        local prior_task = redis.call('HGET', KEYS[22], prior) or ''
        if prior_units ~= dispatch_units or prior_task ~= task_key or not stream_entry(prior) then
            return state('accounting', prior)
        end
        return state('replayed', prior)
    end
    if not within_capacity(dispatch_units, 1, 0, 0) then
        return state('fenced', '')
    end
    local stream_id = redis.call(
        'XADD', KEYS[1], '*',
        'identity', identity,
        'digest', digest,
        'units', tostring(dispatch_units),
        'payload', payload
    )
    redis.call('HSET', KEYS[2], identity, stream_id)
    redis.call('HSET', KEYS[3], identity, digest)
    redis.call('HSET', KEYS[4], identity, tostring(dispatch_units))
    if task_key ~= '' then
        redis.call('HSET', KEYS[21], task_key, stream_id)
        redis.call('HSET', KEYS[22], stream_id, task_key)
    end
    redis.call('HINCRBY', KEYS[17], 'used_bytes', dispatch_units)
    redis.call('HINCRBY', KEYS[17], 'dispatch_entries', 1)
    return state('appended', stream_id)
end

if operation == 'bind_cancellation' then
    local prior_task = redis.call('HGET', KEYS[24], identity)
    if prior_task then
        if prior_task ~= task_key then
            return state('conflict', '')
        end
        dispatch_key = redis.call('HGET', KEYS[25], identity) or ''
        return state('replayed', redis.call('HGET', KEYS[28], identity) or '')
    end
    dispatch_key = redis.call('HGET', KEYS[21], task_key) or ''
    local bound_generation = ''
    local bound_owner = ''
    local bound_outcome = ''
    local bound_deadline = ''
    if dispatch_key ~= '' then
        bound_generation = redis.call('HGET', KEYS[5], dispatch_key) or ''
        bound_owner = redis.call('HGET', KEYS[6], dispatch_key) or ''
        bound_outcome = redis.call('HGET', KEYS[14], dispatch_key) or ''
        bound_deadline = redis.call('HGET', KEYS[7], dispatch_key) or ''
        if bound_generation == '' and bound_outcome == '' then
            local fields = stream_entry(dispatch_key)
            if fields and release_dispatch(fields) then
                bound_outcome = 'cancellation-no-process'
            else
                dispatch_key = ''
                bound_owner = ''
                bound_deadline = ''
            end
        end
    end
    redis.call('HSET', KEYS[23], task_key, identity)
    redis.call('HSET', KEYS[24], identity, task_key)
    redis.call('HSET', KEYS[25], identity, dispatch_key)
    redis.call('HSET', KEYS[26], identity, bound_generation)
    redis.call('HSET', KEYS[27], identity, bound_owner)
    redis.call('HSET', KEYS[28], identity, bound_outcome)
    redis.call('HSET', KEYS[29], identity, bound_deadline)
    return state('bound', bound_outcome)
end

if operation == 'reject' then
    if record_generation() > 0 then
        return state('stale', '')
    end
    local prior = redis.call('HGET', KEYS[16], dispatch_key)
    if prior and prior ~= reason then
        return state('conflict', '')
    end
    if prior and redis.call('HGET', KEYS[12], dispatch_key) == '1' then
        return state('rejected', '')
    end
    local fields = stream_entry(dispatch_key)
    if not fields or fields['digest'] ~= digest or fields['payload'] ~= payload then
        return state('missing', '')
    end
    if prior then
        -- The durable rejection already owns its staged charge.
    elseif not within_capacity(staged_units, 0, 0, 1) then
        return state('fenced', '')
    else
        redis.call('HSET', KEYS[8], dispatch_key, payload)
        redis.call('HSET', KEYS[16], dispatch_key, reason)
        redis.call('HSET', KEYS[20], dispatch_key, tostring(staged_units))
        redis.call('HINCRBY', KEYS[17], 'used_bytes', staged_units)
        redis.call('HINCRBY', KEYS[17], 'staged_events', 1)
    end
    if not release_dispatch(fields) then
        return state('accounting', '')
    end
    return state('rejected', '')
end

if operation == 'claim' then
    local current_generation = record_generation()
    if current_generation > 0 then
        if record_owner() == owner and redis.call('HGET', KEYS[10], dispatch_key) == event_digest then
            return state('replayed', '')
        end
        return state('unavailable', '')
    end
    local fields = stream_entry(dispatch_key)
    if not fields or fields['digest'] ~= digest or fields['payload'] ~= payload then
        return state('missing', '')
    end
    if not within_capacity(staged_units, 0, 1, 1) then
        return state('fenced', '')
    end
    local next_generation = redis.call('HINCRBY', KEYS[5], dispatch_key, 1)
    local deadline = server_millis() + timeout_ms
    redis.call('HSET', KEYS[6], dispatch_key, owner)
    redis.call('HSET', KEYS[7], dispatch_key, tostring(deadline))
    redis.call('HSET', KEYS[8], dispatch_key, payload)
    redis.call('HSET', KEYS[9], dispatch_key, event)
    redis.call('HSET', KEYS[10], dispatch_key, event_digest)
    redis.call('HSET', KEYS[11], dispatch_key, '1')
    redis.call('HSET', KEYS[12], dispatch_key, '0')
    redis.call('HSET', KEYS[20], dispatch_key, tostring(staged_units))
    redis.call('ZADD', KEYS[18], deadline, dispatch_key)
    redis.call('HINCRBY', KEYS[17], 'used_bytes', staged_units)
    redis.call('HINCRBY', KEYS[17], 'active_claims', 1)
    redis.call('HINCRBY', KEYS[17], 'staged_events', 1)
    return state('claimed', tostring(next_generation))
end

if operation == 'arm' then
    if not exact_claim() or redis.call('HGET', KEYS[14], dispatch_key) then
        return state('stale', '')
    end
    if redis.call('HGET', KEYS[10], dispatch_key) ~= event_digest then
        return state('stale', '')
    end
    local deadline = server_millis() + timeout_ms
    redis.call('HSET', KEYS[7], dispatch_key, tostring(deadline))
    redis.call('HSET', KEYS[11], dispatch_key, '1')
    redis.call('ZADD', KEYS[18], deadline, dispatch_key)
    return state('armed', '')
end

if operation == 'complete' then
    if not exact_claim() or redis.call('HGET', KEYS[14], dispatch_key) then
        return state('stale', '')
    end
    if redis.call('HGET', KEYS[10], dispatch_key) ~= event_digest
        or redis.call('HGET', KEYS[11], dispatch_key) ~= '1' then
        return state('stale', '')
    end
    if redis.call('HGET', KEYS[12], dispatch_key) == '1' then
        return state('completed', '')
    end
    local fields = stream_entry(dispatch_key)
    if not fields or not release_dispatch(fields) then
        return state('missing', '')
    end
    return state('completed', '')
end

if operation == 'started' then
    if not exact_claim() or redis.call('HGET', KEYS[14], dispatch_key)
        or redis.call('HGET', KEYS[11], dispatch_key) ~= '1'
        or redis.call('HGET', KEYS[12], dispatch_key) ~= '1' then
        return state('stale', '')
    end
    local prior = redis.call('HGET', KEYS[13], dispatch_key)
    if prior then
        if redis.sha1hex(prior) == redis.sha1hex(event) then
            return state('replayed', '')
        end
        return state('conflict', '')
    end
    if not within_capacity(staged_units, 0, 0, 1) then
        return state('fenced', '')
    end
    redis.call('HSET', KEYS[13], dispatch_key, event)
    redis.call('HINCRBY', KEYS[20], dispatch_key, staged_units)
    redis.call('HINCRBY', KEYS[17], 'used_bytes', staged_units)
    redis.call('HINCRBY', KEYS[17], 'staged_events', 1)
    return state('started', '')
end

if operation == 'renew' or operation == 'register_failure' then
    if not exact_claim() or redis.call('HGET', KEYS[14], dispatch_key)
        or redis.call('HGET', KEYS[11], dispatch_key) ~= '1' then
        return state('stale', '')
    end
    local deadline = server_millis()
    if operation == 'renew' then
        deadline = deadline + timeout_ms
    end
    redis.call('HSET', KEYS[7], dispatch_key, tostring(deadline))
    redis.call('ZADD', KEYS[18], deadline, dispatch_key)
    return state(operation == 'renew' and 'renewed' or 'registered', '')
end

if operation == 'terminal' then
    local elected = redis.call('HGET', KEYS[14], dispatch_key)
    if elected then
        return state('settled', elected)
    end
    if not exact_claim() then
        return state('stale', '')
    end
    if not within_capacity(staged_units, 0, 0, 1) then
        return state('fenced', '')
    end
    redis.call('HSET', KEYS[14], dispatch_key, outcome)
    redis.call('HSET', KEYS[15], dispatch_key, event)
    local cancellation_task = redis.call('HGET', KEYS[22], dispatch_key)
    if cancellation_task then
        local cancellation = redis.call('HGET', KEYS[23], cancellation_task)
        if cancellation then
            redis.call('HSET', KEYS[28], cancellation, outcome)
        end
    end
    redis.call('ZREM', KEYS[18], dispatch_key)
    redis.call('HINCRBY', KEYS[20], dispatch_key, staged_units)
    redis.call('HINCRBY', KEYS[17], 'used_bytes', staged_units)
    redis.call('HINCRBY', KEYS[17], 'active_claims', -1)
    redis.call('HINCRBY', KEYS[17], 'staged_events', 1)
    return state('won', outcome)
end

if operation == 'cleanup' then
    if not exact_claim() or redis.call('HGET', KEYS[12], dispatch_key) ~= '1'
        or not redis.call('HGET', KEYS[14], dispatch_key) then
        return state('stale', '')
    end
    local units = tonumber(redis.call('HGET', KEYS[20], dispatch_key) or '0')
    if units == 0
        and not redis.call('HGET', KEYS[9], dispatch_key)
        and not redis.call('HGET', KEYS[13], dispatch_key)
        and not redis.call('HGET', KEYS[15], dispatch_key) then
        return state('cleaned', '')
    end
    local events = 1
    if redis.call('HGET', KEYS[13], dispatch_key) then events = events + 1 end
    if redis.call('HGET', KEYS[15], dispatch_key) then events = events + 1 end
    redis.call('HDEL', KEYS[8], dispatch_key)
    redis.call('HDEL', KEYS[9], dispatch_key)
    redis.call('HDEL', KEYS[13], dispatch_key)
    redis.call('HDEL', KEYS[15], dispatch_key)
    redis.call('HDEL', KEYS[20], dispatch_key)
    redis.call('HINCRBY', KEYS[17], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[17], 'staged_events', -events)
    return state('cleaned', '')
end

return redis.error_reply('unknown task-dispatch operation')"#;

#[derive(Clone, Debug)]
pub struct RedisTaskDispatchConfig {
    pub namespace: String,
    pub consumer_id: String,
    pub reclaim_idle: Duration,
    pub poll_interval: Duration,
    pub max_payload_bytes: NonZeroUsize,
    pub max_dispatches: NonZeroUsize,
    pub max_active_claims: NonZeroUsize,
    pub max_staged_events: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisTaskDispatchConfig {
    pub fn new(namespace: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            consumer_id: consumer_id.into(),
            reclaim_idle: DEFAULT_RECLAIM_IDLE,
            poll_interval: DEFAULT_POLL,
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_dispatches: NonZeroUsize::new(DEFAULT_MAX_DISPATCHES).expect("non-zero constant"),
            max_active_claims: NonZeroUsize::new(DEFAULT_MAX_ACTIVE_CLAIMS)
                .expect("non-zero constant"),
            max_staged_events: NonZeroUsize::new(DEFAULT_MAX_STAGED_EVENTS)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisTaskDispatchError> {
        let valid_symbol = |value: &str| {
            !value.is_empty()
                && value.len() <= 127
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        };
        let minimum = DISPATCH_ACCOUNTED_BYTES
            .saturating_add(CLAIM_ACCOUNTED_BYTES)
            .saturating_add(EVENT_ACCOUNTED_BYTES.saturating_mul(3))
            .saturating_add(self.max_payload_bytes.get() as u64);
        if !valid_symbol(&self.namespace)
            || !valid_symbol(&self.consumer_id)
            || self.reclaim_idle.is_zero()
            || self.poll_interval.is_zero()
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < minimum
            || millis(self.reclaim_idle) == 0
            || millis(self.poll_interval) == 0
        {
            return Err(RedisTaskDispatchError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisTaskDispatchKeys {
    stream: String,
    identities: String,
    digests: String,
    dispatch_units: String,
    generations: String,
    owners: String,
    deadlines: String,
    payloads: String,
    assigned: String,
    assigned_digests: String,
    liveness: String,
    source_completed: String,
    started: String,
    terminal_outcomes: String,
    terminal_events: String,
    rejections: String,
    quota: String,
    deadline_index: String,
    operations_prefix: String,
    staged_units: String,
    task_instances: String,
    dispatch_tasks: String,
    cancellation_fences: String,
    cancellation_tasks: String,
    cancellation_dispatches: String,
    cancellation_generations: String,
    cancellation_owners: String,
    cancellation_outcomes: String,
    cancellation_deadlines: String,
}

impl RedisTaskDispatchKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:task-dispatch");
        Self {
            stream: format!("{prefix}:stream"),
            identities: format!("{prefix}:identities"),
            digests: format!("{prefix}:digests"),
            dispatch_units: format!("{prefix}:dispatch-units"),
            generations: format!("{prefix}:generations"),
            owners: format!("{prefix}:owners"),
            deadlines: format!("{prefix}:deadlines"),
            payloads: format!("{prefix}:payloads"),
            assigned: format!("{prefix}:assigned"),
            assigned_digests: format!("{prefix}:assigned-digests"),
            liveness: format!("{prefix}:liveness"),
            source_completed: format!("{prefix}:source-completed"),
            started: format!("{prefix}:started"),
            terminal_outcomes: format!("{prefix}:terminal-outcomes"),
            terminal_events: format!("{prefix}:terminal-events"),
            rejections: format!("{prefix}:rejections"),
            quota: format!("{prefix}:quota"),
            deadline_index: format!("{prefix}:deadline-index"),
            operations_prefix: format!("{prefix}:operations:"),
            staged_units: format!("{prefix}:staged-units"),
            task_instances: format!("{prefix}:task-instances"),
            dispatch_tasks: format!("{prefix}:dispatch-tasks"),
            cancellation_fences: format!("{prefix}:cancellation-fences"),
            cancellation_tasks: format!("{prefix}:cancellation-tasks"),
            cancellation_dispatches: format!("{prefix}:cancellation-dispatches"),
            cancellation_generations: format!("{prefix}:cancellation-generations"),
            cancellation_owners: format!("{prefix}:cancellation-owners"),
            cancellation_outcomes: format!("{prefix}:cancellation-outcomes"),
            cancellation_deadlines: format!("{prefix}:cancellation-deadlines"),
        }
    }

    fn operation(&self, identity: &str, phase: &str) -> String {
        format!(
            "{}{}:{phase}",
            self.operations_prefix,
            digest_hex(identity.as_bytes())
        )
    }
}

pub trait RedisTaskDispatchCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisTaskDispatchError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskDispatchError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisTaskDispatchQuotaState);
}

pub struct MonitoredRedisTaskDispatchCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisTaskDispatchCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisTaskDispatchCapability for MonitoredRedisTaskDispatchCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskDispatchError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisTaskDispatchError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisTaskDispatchError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskDispatchError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisTaskDispatchError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisTaskDispatchQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskDispatchAcceptance {
    Appended,
    Replayed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisTaskDispatchQuotaState {
    pub used_bytes: u64,
    pub dispatch_entries: u64,
    pub active_claims: u64,
    pub staged_events: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisTaskDispatchQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.dispatch_entries + self.active_claims + self.staged_events,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisTaskDispatch {
    connection: MultiplexedConnection,
    keys: RedisTaskDispatchKeys,
    config: Arc<RedisTaskDispatchConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisTaskDispatchCapability>,
}

impl RedisTaskDispatch {
    pub async fn connect(
        client: redis::Client,
        config: RedisTaskDispatchConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskDispatchCapability>,
    ) -> Result<Self, RedisTaskDispatchError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisTaskDispatchError::Unavailable)?;
        let adapter = Self::from_connection(connection, config, durability, capability)?;
        adapter.ensure_group().await?;
        Ok(adapter)
    }

    pub(crate) fn from_connection(
        connection: MultiplexedConnection,
        config: RedisTaskDispatchConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskDispatchCapability>,
    ) -> Result<Self, RedisTaskDispatchError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisTaskDispatchKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_task_dispatch_operation_manifest()
    }

    pub async fn append(
        &self,
        identity: &str,
        encoded_dispatch: Vec<u8>,
    ) -> Result<RedisTaskDispatchAcceptance, RedisTaskDispatchError> {
        if !valid_identity(identity)
            || encoded_dispatch.is_empty()
            || encoded_dispatch.len() > self.config.max_payload_bytes.get()
        {
            return Err(RedisTaskDispatchError::InvalidOperation);
        }
        let task_key = decode_dispatch(&encoded_dispatch)
            .ok()
            .map(|task| format!("{}:{}", task.workflow_instance_id, task.task_instance_id))
            .unwrap_or_default();
        let generation = self.capability.guard_admission()?;
        let digest = digest_hex(&encoded_dispatch);
        let units = DISPATCH_ACCOUNTED_BYTES
            .checked_add(encoded_dispatch.len() as u64)
            .ok_or(RedisTaskDispatchError::InvalidOperation)?;
        let mutation = ScriptMutation::append(
            &self.keys,
            identity,
            digest,
            task_key,
            encoded_dispatch,
            units,
            &self.config,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "appended" => Ok(RedisTaskDispatchAcceptance::Appended),
            "replayed" => Ok(RedisTaskDispatchAcceptance::Replayed),
            "fenced" => Err(RedisTaskDispatchError::CapacityFenced),
            "cancelled" => Err(RedisTaskDispatchError::CancellationFenced),
            "conflict" => Err(RedisTaskDispatchError::IdentityConflict),
            "accounting" => Err(self.accounting_failure()),
            _ => Err(self.accounting_failure()),
        }
    }

    pub async fn bind_cancellation(
        &self,
        acknowledgement_identity: &str,
        request: CancelRequest,
    ) -> Result<RedisCancellationBinding, RedisTaskDispatchError> {
        if !valid_identity(acknowledgement_identity) {
            return Err(RedisTaskDispatchError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let task_key = format!(
            "{}:{}",
            request.workflow_instance_id, request.task_instance_id
        );
        let mutation = ScriptMutation::bind_cancellation(
            &self.keys,
            acknowledgement_identity,
            task_key,
            &self.config,
        )?;
        let state = self.decode_state(&self.commit_mutation(&mutation).await?)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "bound" | "replayed" => self
                .load_cancellation_binding(acknowledgement_identity)
                .await?
                .ok_or_else(|| self.accounting_failure()),
            "conflict" => Err(RedisTaskDispatchError::IdentityConflict),
            _ => Err(self.accounting_failure()),
        }
    }

    pub async fn load_cancellation_binding(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<Option<RedisCancellationBinding>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let values: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = redis::pipe()
            .cmd("HGET")
            .arg(&self.keys.cancellation_tasks)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.cancellation_dispatches)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.cancellation_generations)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.cancellation_owners)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.cancellation_outcomes)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.cancellation_deadlines)
            .arg(acknowledgement_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let Some(task_key) = values.0 else {
            return Ok(None);
        };
        Ok(Some(RedisCancellationBinding {
            acknowledgement_identity: acknowledgement_identity.to_owned(),
            task_key,
            dispatch_key: values.1.filter(|value| !value.is_empty()),
            pickup_generation: values.2.and_then(|value| value.parse().ok()),
            owner: values.3.filter(|value| !value.is_empty()),
            terminal_outcome: values.4.as_deref().and_then(parse_outcome),
            liveness_deadline: values
                .5
                .and_then(|value| value.parse().ok())
                .and_then(DateTime::from_timestamp_millis),
        }))
    }

    pub async fn elect_cancellation(
        &self,
        binding: &RedisCancellationBinding,
        reconciliation: tickr_executor::local_pickup::CancellationReconciliation,
    ) -> Result<Option<TerminalElection>, RedisTaskDispatchError> {
        let (Some(dispatch_key), Some(pickup_generation), Some(owner)) = (
            binding.dispatch_key.clone(),
            binding.pickup_generation,
            binding.owner.clone(),
        ) else {
            return Ok(None);
        };
        let claim = LocalPickupClaim {
            dispatch_key,
            pickup_generation,
            owner,
            liveness_deadline: binding
                .liveness_deadline
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        };
        let outcome = match reconciliation {
            tickr_executor::local_pickup::CancellationReconciliation::Killed => {
                LocalAttemptOutcome::CancellationKilled
            }
            tickr_executor::local_pickup::CancellationReconciliation::AlreadyExited => {
                LocalAttemptOutcome::CancellationAlreadyExited
            }
            tickr_executor::local_pickup::CancellationReconciliation::NoProcess => {
                LocalAttemptOutcome::CancellationNoProcess
            }
        };
        let mut connection = self.connection.clone();
        let assigned_event: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&self.keys.assigned)
            .arg(&claim.dispatch_key)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let mut terminal_event = tc::TaskEvent::decode(
            assigned_event
                .ok_or_else(|| self.missing_identity())?
                .as_slice(),
        )
        .map_err(|_| self.accounting_failure())?;
        terminal_event.kind = Some(tc::task_event::Kind::Failed(tc::task_event::Failed {}));
        self.elect_terminal(&claim, outcome, &terminal_event.encode_to_vec(), Utc::now())
            .await
            .map(Some)
            .map_err(|_| RedisTaskDispatchError::Unavailable)
    }

    pub async fn quota_state(&self) -> Result<RedisTaskDispatchQuotaState, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "dispatch_entries",
                "active_claims",
                "staged_events",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 4 {
            return Err(self.accounting_failure());
        }
        Ok(self.quota_state_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
            values[3].unwrap_or(0),
        ))
    }

    pub async fn server_time(&self) -> Result<DateTime<Utc>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let millis = seconds
            .checked_mul(1000)
            .and_then(|value| value.checked_add(micros / 1000))
            .ok_or(RedisTaskDispatchError::Accounting)?;
        DateTime::from_timestamp_millis(millis).ok_or(RedisTaskDispatchError::Accounting)
    }

    pub async fn sweep_one_due(
        &self,
    ) -> Result<Option<(LocalPickupClaim, TerminalElection)>, RedisTaskDispatchError> {
        let server_time = self.server_time().await?;
        let Some(due) = self
            .select_due_liveness(server_time)
            .await
            .map_err(|_| RedisTaskDispatchError::Unavailable)?
        else {
            return Ok(None);
        };
        let task = decode_dispatch(&due.payload).map_err(|_| RedisTaskDispatchError::Accounting)?;
        let event = encode_unhealthy_task_event(&task);
        let election = self
            .elect_terminal(
                &due.claim,
                LocalAttemptOutcome::LivenessExpired,
                &event,
                server_time,
            )
            .await
            .map_err(|_| RedisTaskDispatchError::Unavailable)?;
        Ok(Some((due.claim, election)))
    }

    pub async fn complete_staged_handoff(
        &self,
        claim: &LocalPickupClaim,
    ) -> Result<bool, RedisTaskDispatchError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::cleanup(&self.keys, claim, &self.config)?;
        let state = self.decode_state(&self.commit_mutation(&mutation).await?)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "cleaned" => Ok(true),
            "stale" => Ok(false),
            "accounting" => Err(self.accounting_failure()),
            _ => Err(self.accounting_failure()),
        }
    }

    async fn ensure_group(&self) -> Result<(), RedisTaskDispatchError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::ensure_group(&self.keys, &self.config)?;
        self.commit_mutation(&mutation).await?;
        self.capability.guard_acknowledgement(generation)
    }

    async fn next_entry(&self) -> Result<Option<DispatchDelivery>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let claimed: redis::streams::StreamAutoClaimReply = connection
            .xautoclaim_options(
                &self.keys.stream,
                REDIS_TASK_DISPATCH_GROUP,
                &self.config.consumer_id,
                millis(self.config.reclaim_idle),
                "0-0",
                StreamAutoClaimOptions::default().count(1),
            )
            .await
            .map_err(|error| self.redis_error(error))?;
        if let Some(entry) = claimed.claimed.into_iter().next() {
            return self.decode_delivery(entry).map(Some);
        }

        let options = StreamReadOptions::default()
            .group(REDIS_TASK_DISPATCH_GROUP, &self.config.consumer_id)
            .count(1)
            .block(usize::try_from(millis(self.config.poll_interval)).unwrap_or(usize::MAX));
        let reply: Option<StreamReadReply> = connection
            .xread_options(&[&self.keys.stream], &[">"], &options)
            .await
            .map_err(|error| self.redis_error(error))?;
        reply
            .and_then(|reply| reply.keys.into_iter().next())
            .and_then(|stream| stream.ids.into_iter().next())
            .map(|entry| self.decode_delivery(entry))
            .transpose()
    }

    fn decode_delivery(&self, entry: StreamId) -> Result<DispatchDelivery, RedisTaskDispatchError> {
        let identity = entry
            .get::<String>("identity")
            .filter(|identity| valid_identity(identity))
            .ok_or_else(|| self.missing_identity())?;
        let digest = entry
            .get::<String>("digest")
            .filter(|digest| valid_digest(digest))
            .ok_or_else(|| self.missing_identity())?;
        let units = entry
            .get::<u64>("units")
            .filter(|units| *units >= DISPATCH_ACCOUNTED_BYTES)
            .ok_or_else(|| self.missing_identity())?;
        let payload = entry
            .get::<Vec<u8>>("payload")
            .filter(|payload| {
                !payload.is_empty() && payload.len() <= self.config.max_payload_bytes.get()
            })
            .ok_or_else(|| self.missing_identity())?;
        if digest_hex(&payload) != digest
            || units != DISPATCH_ACCOUNTED_BYTES.saturating_add(payload.len() as u64)
        {
            return Err(self.missing_identity());
        }
        Ok(DispatchDelivery {
            dispatch_key: entry.id,
            identity,
            digest,
            units,
            payload,
        })
    }

    async fn load_record(
        &self,
        dispatch_key: &str,
    ) -> Result<Option<PickupRecord>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let mut pipe = redis::pipe();
        pipe.cmd("HMGET")
            .arg(&self.keys.generations)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.owners)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.deadlines)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.payloads)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.assigned_digests)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.liveness)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.source_completed)
            .arg(dispatch_key)
            .cmd("HMGET")
            .arg(&self.keys.terminal_outcomes)
            .arg(dispatch_key);
        let values: (
            Vec<Option<i64>>,
            Vec<Option<String>>,
            Vec<Option<i64>>,
            Vec<Option<Vec<u8>>>,
            Vec<Option<String>>,
            Vec<Option<u8>>,
            Vec<Option<u8>>,
            Vec<Option<String>>,
        ) = pipe
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let Some(generation) = values.0.into_iter().next().flatten() else {
            return Ok(None);
        };
        let owner = values
            .1
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| self.accounting_failure())?;
        let deadline_ms = values
            .2
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| self.accounting_failure())?;
        let payload = values
            .3
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| self.accounting_failure())?;
        let assigned_digest = values
            .4
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| self.accounting_failure())?;
        let liveness_armed = values.5.into_iter().next().flatten() == Some(1);
        let source_completed = values.6.into_iter().next().flatten() == Some(1);
        let terminal_outcome = values.7.into_iter().next().flatten();
        let liveness_deadline = DateTime::from_timestamp_millis(deadline_ms)
            .ok_or_else(|| self.accounting_failure())?;
        Ok(Some(PickupRecord {
            claim: LocalPickupClaim {
                dispatch_key: dispatch_key.to_owned(),
                pickup_generation: generation,
                owner,
                liveness_deadline,
            },
            payload,
            assigned_digest,
            liveness_armed,
            source_completed,
            terminal_outcome,
        }))
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        match self.durability.execute(&mut connection, mutation).await {
            Ok(committed) => Ok(committed.into_output()),
            Err(error)
                if matches!(
                    error.failure(),
                    RedisDurabilityFailure::AmbiguousMutation
                        | RedisDurabilityFailure::AmbiguousLocalFsync
                ) =>
            {
                self.durability
                    .resolve_ambiguous(&mut connection, mutation)
                    .await
                    .map(|committed| committed.into_output())
                    .map_err(|error| self.durability_error(error))
            }
            Err(error) => Err(self.durability_error(error)),
        }
    }

    fn decode_state(&self, output: &[Vec<u8>]) -> Result<MutationState, RedisTaskDispatchError> {
        if output.len() == 1 && output[0].as_slice() == b"conflict" {
            return Ok(MutationState::conflict(self.quota_state_from(0, 0, 0, 0)));
        }
        if output.len() != 12 {
            return Err(self.accounting_failure());
        }
        let text = |index: usize| {
            std::str::from_utf8(&output[index])
                .map(str::to_owned)
                .map_err(|_| self.accounting_failure())
        };
        let number = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| self.accounting_failure())
        };
        let deadline_ms = std::str::from_utf8(&output[8])
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let _ = text(9)?;
        let _ = text(11)?;
        Ok(MutationState {
            status: text(0)?,
            detail: text(1)?,
            quota: self.quota_state_from(number(2)?, number(3)?, number(4)?, number(5)?),
            pickup_generation: number(6)? as i64,
            owner: text(7)?,
            liveness_deadline: DateTime::from_timestamp_millis(deadline_ms)
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        })
    }

    fn quota_state_from(
        &self,
        used_bytes: u64,
        dispatch_entries: u64,
        active_claims: u64,
        staged_events: u64,
    ) -> RedisTaskDispatchQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || dispatch_entries >= self.config.max_dispatches.get() as u64
            || active_claims >= self.config.max_active_claims.get() as u64
            || staged_events >= self.config.max_staged_events.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisTaskDispatchQuotaState {
            used_bytes,
            dispatch_entries,
            active_claims,
            staged_events,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            pressure,
        }
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisTaskDispatchError {
        use crate::redis_durability::RedisMutationFailure;
        match RedisMutationError::from_redis(error).failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisTaskDispatchError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisTaskDispatchError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisTaskDispatchError::IdentityConflict
            }
            RedisDurabilityFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisDurabilityFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisDurabilityFailure::AmbiguousLocalFsync
            | RedisDurabilityFailure::LocalFsyncUnavailable => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::LocalFsync),
            RedisDurabilityFailure::InvalidOperation
            | RedisDurabilityFailure::AmbiguousMutation
            | RedisDurabilityFailure::MutationRejected => {}
        }
        RedisTaskDispatchError::Durability(error.failure())
    }

    fn accounting_failure(&self) -> RedisTaskDispatchError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisTaskDispatchError::Accounting
    }

    fn missing_identity(&self) -> RedisTaskDispatchError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
        RedisTaskDispatchError::Accounting
    }
}

#[async_trait::async_trait]
impl SafePickupWriter for RedisTaskDispatch {
    async fn select_pending(&self) -> Result<Option<PendingLocalDispatch>, String> {
        self.capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let Some(delivery) = self.next_entry().await.map_err(|error| error.to_string())? else {
            return Ok(None);
        };
        if let Some(record) = self
            .load_record(&delivery.dispatch_key)
            .await
            .map_err(|error| error.to_string())?
        {
            if !record.source_completed && record.liveness_armed {
                let completed = self
                    .complete_source(&record.claim)
                    .await
                    .map_err(|error| error.to_string())?;
                if !completed {
                    return Err(
                        "Redis pickup recovery could not complete its exact source".to_owned()
                    );
                }
            }
            return Ok(None);
        }
        Ok(Some(PendingLocalDispatch {
            dispatch_key: delivery.dispatch_key,
            payload: delivery.payload,
        }))
    }

    async fn reject_poison(
        &self,
        dispatch_key: &str,
        reason: &str,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        if reason.is_empty() || reason.len() > 4096 {
            return Err(RedisTaskDispatchError::InvalidOperation.to_string());
        }
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let delivery = self
            .load_stream_delivery(dispatch_key)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "selected Redis poison dispatch disappeared".to_owned())?;
        let staged_units = CLAIM_ACCOUNTED_BYTES
            .saturating_add(delivery.payload.len() as u64)
            .saturating_add(reason.len() as u64);
        let mutation =
            ScriptMutation::reject(&self.keys, &delivery, reason, staged_units, &self.config)
                .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(
                &self
                    .commit_mutation(&mutation)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "rejected" => Ok(true),
            "stale" | "missing" => Ok(false),
            "fenced" => Err(RedisTaskDispatchError::CapacityFenced.to_string()),
            "conflict" => Err(RedisTaskDispatchError::IdentityConflict.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn claim(
        &self,
        input: ClaimLocalPickup<'_>,
    ) -> Result<Option<LocalPickupClaim>, ClaimWriteError> {
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| ClaimWriteError::Failed(error.to_string()))?;
        let delivery = self
            .load_stream_delivery(input.dispatch_key)
            .await
            .map_err(|error| ClaimWriteError::Failed(error.to_string()))?
            .ok_or_else(|| {
                ClaimWriteError::Failed("selected Redis dispatch disappeared".to_owned())
            })?;
        let timeout_ms = positive_timeout_millis(input.liveness_deadline - input.now)
            .map_err(|error| ClaimWriteError::Failed(error.to_string()))?;
        let assigned_digest = digest_hex(input.assigned_event);
        let staged_units = CLAIM_ACCOUNTED_BYTES
            .saturating_add(delivery.payload.len() as u64)
            .saturating_add(EVENT_ACCOUNTED_BYTES)
            .saturating_add(input.assigned_event.len() as u64);
        let mutation = ScriptMutation::claim(
            &self.keys,
            &delivery,
            input.owner,
            timeout_ms,
            input.assigned_event,
            assigned_digest,
            staged_units,
            &self.config,
        )
        .map_err(|error| ClaimWriteError::Failed(error.to_string()))?;
        let output = self
            .commit_mutation(&mutation)
            .await
            .map_err(|error| match error {
                RedisTaskDispatchError::Durability(
                    RedisDurabilityFailure::AmbiguousMutation
                    | RedisDurabilityFailure::AmbiguousLocalFsync,
                ) => ClaimWriteError::Ambiguous,
                other => ClaimWriteError::Failed(other.to_string()),
            })?;
        let state = self
            .decode_state(&output)
            .map_err(|error| ClaimWriteError::Failed(error.to_string()))?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| ClaimWriteError::Failed(error.to_string()))?;
        match state.status.as_str() {
            "claimed" => Ok(Some(state.claim(input.dispatch_key))),
            "replayed" | "unavailable" => Ok(None),
            "fenced" => Err(ClaimWriteError::Failed(
                RedisTaskDispatchError::CapacityFenced.to_string(),
            )),
            "conflict" => Err(ClaimWriteError::Failed(
                RedisTaskDispatchError::IdentityConflict.to_string(),
            )),
            "missing" => Ok(None),
            _ => Err(ClaimWriteError::Failed(
                self.accounting_failure().to_string(),
            )),
        }
    }

    async fn prove_ambiguous_claim(
        &self,
        dispatch_key: &str,
        owner: &str,
        assigned_event: &[u8],
    ) -> Result<Option<LocalPickupClaim>, String> {
        let Some(record) = self
            .load_record(dispatch_key)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        Ok((record.claim.owner == owner
            && record.assigned_digest == digest_hex(assigned_event)
            && record.liveness_armed
            && record.terminal_outcome.is_none())
        .then_some(record.claim))
    }

    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        _payload: &[u8],
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let record = self
            .load_record(&claim.dispatch_key)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Redis pickup record is missing".to_owned())?;
        let timeout_ms =
            positive_timeout_millis(deadline - now).map_err(|error| error.to_string())?;
        self.commit_claim_phase(
            claim,
            ScriptMutation::arm(
                &self.keys,
                claim,
                timeout_ms,
                record.assigned_digest,
                &self.config,
            )
            .map_err(|error| error.to_string())?,
            "armed",
        )
        .await
    }

    async fn prove_ready_to_launch(
        &self,
        claim: &LocalPickupClaim,
        assigned_event: &[u8],
    ) -> Result<bool, String> {
        let Some(record) = self
            .load_record(&claim.dispatch_key)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        Ok(record.claim.dispatch_key == claim.dispatch_key
            && record.claim.pickup_generation == claim.pickup_generation
            && record.claim.owner == claim.owner
            && record.assigned_digest == digest_hex(assigned_event)
            && record.liveness_armed
            && record.terminal_outcome.is_none())
    }

    async fn complete_source(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        let record = self
            .load_record(&claim.dispatch_key)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Redis pickup record is missing".to_owned())?;
        self.commit_claim_phase(
            claim,
            ScriptMutation::complete(&self.keys, claim, record.assigned_digest, &self.config)
                .map_err(|error| error.to_string())?,
            "completed",
        )
        .await
    }

    async fn stage_started(
        &self,
        claim: &LocalPickupClaim,
        started_event: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let staged_units = EVENT_ACCOUNTED_BYTES.saturating_add(started_event.len() as u64);
        self.commit_claim_phase(
            claim,
            ScriptMutation::started(&self.keys, claim, started_event, staged_units, &self.config)
                .map_err(|error| error.to_string())?,
            "started",
        )
        .await
    }

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let timeout_ms =
            positive_timeout_millis(deadline - now).map_err(|error| error.to_string())?;
        self.commit_claim_phase(
            claim,
            ScriptMutation::renew(&self.keys, claim, timeout_ms, &self.config)
                .map_err(|error| error.to_string())?,
            "renewed",
        )
        .await
    }
}

impl RedisTaskDispatch {
    async fn load_stream_delivery(
        &self,
        dispatch_key: &str,
    ) -> Result<Option<DispatchDelivery>, RedisTaskDispatchError> {
        let mut connection = self.connection.clone();
        let entries: StreamRangeReply = redis::cmd("XRANGE")
            .arg(&self.keys.stream)
            .arg(dispatch_key)
            .arg(dispatch_key)
            .arg("COUNT")
            .arg(1)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        entries
            .ids
            .into_iter()
            .next()
            .map(|entry| self.decode_delivery(entry))
            .transpose()
    }

    async fn commit_claim_phase(
        &self,
        _claim: &LocalPickupClaim,
        mutation: ScriptMutation,
        success: &str,
    ) -> Result<bool, String> {
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(
                &self
                    .commit_mutation(&mutation)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            actual if actual == success || actual == "replayed" => Ok(true),
            "stale" | "missing" => Ok(false),
            "fenced" => Err(RedisTaskDispatchError::CapacityFenced.to_string()),
            "conflict" => Err(RedisTaskDispatchError::IdentityConflict.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }
}

#[async_trait::async_trait]
impl SafeAttemptOutcomeHandoff for RedisTaskDispatch {
    async fn select_due_liveness(
        &self,
        _now: DateTime<Utc>,
    ) -> Result<Option<DueLocalPickup>, String> {
        // Redis server time is the only clock admitted to arbitrate deadlines.
        let server_time = self
            .server_time()
            .await
            .map_err(|error| error.to_string())?;
        let mut connection = self.connection.clone();
        let keys: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&self.keys.deadline_index)
            .arg("-inf")
            .arg(server_time.timestamp_millis())
            .arg("LIMIT")
            .arg(0)
            .arg(1)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error).to_string())?;
        let Some(dispatch_key) = keys.into_iter().next() else {
            return Ok(None);
        };
        let Some(record) = self
            .load_record(&dispatch_key)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Err(self.missing_identity().to_string());
        };
        if !record.liveness_armed
            || !record.source_completed
            || record.terminal_outcome.is_some()
            || record.claim.liveness_deadline > server_time
        {
            return Ok(None);
        }
        Ok(Some(DueLocalPickup {
            claim: record.claim,
            payload: record.payload,
        }))
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        self.commit_claim_phase(
            claim,
            ScriptMutation::register_failure(&self.keys, claim, &self.config)
                .map_err(|error| error.to_string())?,
            "registered",
        )
        .await
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<TerminalElection, String> {
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let staged_units = EVENT_ACCOUNTED_BYTES.saturating_add(terminal_event.len() as u64);
        let mutation = ScriptMutation::terminal(
            &self.keys,
            claim,
            outcome,
            terminal_event,
            staged_units,
            &self.config,
        )
        .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(
                &self
                    .commit_mutation(&mutation)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "won" => Ok(TerminalElection::Won),
            "settled" => parse_outcome(&state.detail)
                .map(TerminalElection::Settled)
                .ok_or_else(|| self.accounting_failure().to_string()),
            "stale" => Err(format!(
                "terminal election rejected stale or non-owner Redis pickup generation {}",
                claim.pickup_generation
            )),
            "fenced" => Err(RedisTaskDispatchError::CapacityFenced.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }
}

impl TaskDispatchPublisher for RedisTaskDispatch {
    fn prepare(&self) -> TaskDispatchFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.quota_state()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_dispatch: &'a [u8],
    ) -> TaskDispatchFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.append(identity, encoded_dispatch.to_vec())
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }
}

/// Admitted TaskDispatch role registered before publication or pickup can
/// receive its formation-neutral interfaces.
pub(crate) struct RedisTaskDispatchRoleRegistration {
    connection: MultiplexedConnection,
    keys: RedisTaskDispatchKeys,
    config: RedisTaskDispatchConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisTaskDispatchRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisTaskDispatchConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisTaskDispatchError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisTaskDispatchKeys::new(&config.namespace),
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_adapter(
        &self,
        capability: Arc<dyn RedisTaskDispatchCapability>,
    ) -> Result<RedisTaskDispatch, RedisTaskDispatchError> {
        RedisTaskDispatch::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::TaskDispatch
            && context.manifest_identity() == &self.manifest_identity
            && RedisTaskDispatch::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisTaskDispatchRoleRegistration {
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
            "tickr:{{{}}}:liveness-watchdog:runtime-capability-canary",
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
impl RedisReconstructionCallback for RedisTaskDispatchRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mutation = ScriptMutation::ensure_group(&self.keys, &self.config)
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let mut connection = self.connection.clone();
        let committed = match self.durability.execute(&mut connection, &mutation).await {
            Ok(committed) => committed,
            Err(error) if error.failure() == RedisDurabilityFailure::AmbiguousMutation => self
                .durability
                .resolve_ambiguous(&mut connection, &mutation)
                .await
                .map_err(|_| RedisReconstructionFailure::DurabilityUnproved)?,
            Err(_) => return Err(RedisReconstructionFailure::DurabilityUnproved),
        };
        if !matches!(
            committed.into_output().first().map(Vec::as_slice),
            Some(b"created" | b"replayed")
        ) {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        redis::cmd("XRANGE")
            .arg(&self.keys.stream)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(self.config.max_dispatches.get())
            .query_async::<redis::Value>(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "dispatch_entries",
                "active_claims",
                "staged_events",
            ])
            .query_async::<Vec<Option<u64>>>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_task_dispatch_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(TASK_DISPATCH_SCRIPT_NAME, TASK_DISPATCH_SCRIPT_SHA256)?;
    let stream_pattern = RedisNamespacePattern::key("tickr:{namespace}:task-dispatch:stream");
    RedisOperationManifest::new(
        CoordinationRole::TaskDispatch,
        REDIS_TASK_DISPATCH_PROTOCOL,
        REDIS_TASK_DISPATCH_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:task-dispatch:stream",
            "tickr:{namespace}:task-dispatch:identities",
            "tickr:{namespace}:task-dispatch:digests",
            "tickr:{namespace}:task-dispatch:dispatch-units",
            "tickr:{namespace}:task-dispatch:generations",
            "tickr:{namespace}:task-dispatch:owners",
            "tickr:{namespace}:task-dispatch:deadlines",
            "tickr:{namespace}:task-dispatch:payloads",
            "tickr:{namespace}:task-dispatch:assigned",
            "tickr:{namespace}:task-dispatch:assigned-digests",
            "tickr:{namespace}:task-dispatch:liveness",
            "tickr:{namespace}:task-dispatch:source-completed",
            "tickr:{namespace}:task-dispatch:started",
            "tickr:{namespace}:task-dispatch:terminal-outcomes",
            "tickr:{namespace}:task-dispatch:terminal-events",
            "tickr:{namespace}:task-dispatch:rejections",
            "tickr:{namespace}:task-dispatch:quota",
            "tickr:{namespace}:task-dispatch:deadline-index",
            "tickr:{namespace}:task-dispatch:operations:*",
            "tickr:{namespace}:task-dispatch:staged-units",
            "tickr:{namespace}:task-dispatch:task-instances",
            "tickr:{namespace}:task-dispatch:dispatch-tasks",
            "tickr:{namespace}:task-dispatch:cancellation-fences",
            "tickr:{namespace}:task-dispatch:cancellation-tasks",
            "tickr:{namespace}:task-dispatch:cancellation-dispatches",
            "tickr:{namespace}:task-dispatch:cancellation-generations",
            "tickr:{namespace}:task-dispatch:cancellation-owners",
            "tickr:{namespace}:task-dispatch:cancellation-outcomes",
            "tickr:{namespace}:task-dispatch:cancellation-deadlines",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            stream_pattern,
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::TaskEvents,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCancellationBinding {
    pub acknowledgement_identity: String,
    pub task_key: String,
    pub dispatch_key: Option<String>,
    pub pickup_generation: Option<i64>,
    pub owner: Option<String>,
    pub terminal_outcome: Option<LocalAttemptOutcome>,
    pub liveness_deadline: Option<DateTime<Utc>>,
}

#[derive(Clone)]
struct DispatchDelivery {
    dispatch_key: String,
    identity: String,
    digest: String,
    units: u64,
    payload: Vec<u8>,
}

struct PickupRecord {
    claim: LocalPickupClaim,
    payload: Vec<u8>,
    assigned_digest: String,
    liveness_armed: bool,
    source_completed: bool,
    terminal_outcome: Option<String>,
}

struct MutationState {
    status: String,
    detail: String,
    quota: RedisTaskDispatchQuotaState,
    pickup_generation: i64,
    owner: String,
    liveness_deadline: DateTime<Utc>,
}

impl MutationState {
    fn conflict(quota: RedisTaskDispatchQuotaState) -> Self {
        Self {
            status: "conflict".to_owned(),
            detail: String::new(),
            quota,
            pickup_generation: 0,
            owner: String::new(),
            liveness_deadline: DateTime::<Utc>::UNIX_EPOCH,
        }
    }

    fn claim(&self, dispatch_key: &str) -> LocalPickupClaim {
        LocalPickupClaim {
            dispatch_key: dispatch_key.to_owned(),
            pickup_generation: self.pickup_generation,
            owner: self.owner.clone(),
            liveness_deadline: self.liveness_deadline,
        }
    }
}

#[derive(Clone)]
struct ScriptMutation {
    operation: RedisStableOperation,
    stable_digest: String,
    operation_key: String,
    keys: RedisTaskDispatchKeys,
    kind: MutationKind,
    config: MutationConfig,
}

#[derive(Clone)]
enum MutationKind {
    EnsureGroup,
    BindCancellation {
        identity: String,
        task_key: String,
    },
    Append {
        identity: String,
        task_key: String,
        digest: String,
        payload: Vec<u8>,
        dispatch_units: u64,
    },
    Reject {
        delivery: DispatchDelivery,
        reason: String,
        staged_units: u64,
    },
    Claim {
        delivery: DispatchDelivery,
        owner: String,
        timeout_ms: u64,
        event: Vec<u8>,
        event_digest: String,
        staged_units: u64,
    },
    Arm {
        claim: LocalPickupClaim,
        timeout_ms: u64,
        event_digest: String,
    },
    Complete {
        claim: LocalPickupClaim,
        event_digest: String,
    },
    Started {
        claim: LocalPickupClaim,
        event: Vec<u8>,
        staged_units: u64,
    },
    Renew {
        claim: LocalPickupClaim,
        timeout_ms: u64,
    },
    RegisterFailure {
        claim: LocalPickupClaim,
    },
    Terminal {
        claim: LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        event: Vec<u8>,
        staged_units: u64,
    },
    Cleanup {
        claim: LocalPickupClaim,
    },
}

#[derive(Clone, Copy)]
struct MutationConfig {
    max_dispatches: u64,
    max_active_claims: u64,
    max_staged_events: u64,
    hard_limit_bytes: u64,
}

impl ScriptMutation {
    fn ensure_group(
        keys: &RedisTaskDispatchKeys,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation("group", "ensure"),
            b"ensure-group".to_vec(),
            MutationKind::EnsureGroup,
            config,
        )
    }

    fn append(
        keys: &RedisTaskDispatchKeys,
        identity: &str,
        digest: String,
        task_key: String,
        payload: Vec<u8>,
        dispatch_units: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation(identity, "append"),
            [
                digest.as_bytes(),
                task_key.as_bytes(),
                &dispatch_units.to_be_bytes(),
            ]
            .concat(),
            MutationKind::Append {
                identity: identity.to_owned(),
                task_key,
                digest,
                payload,
                dispatch_units,
            },
            config,
        )
    }

    fn bind_cancellation(
        keys: &RedisTaskDispatchKeys,
        identity: &str,
        task_key: String,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation(identity, "bind-cancellation"),
            task_key.as_bytes().to_vec(),
            MutationKind::BindCancellation {
                identity: identity.to_owned(),
                task_key,
            },
            config,
        )
    }

    fn reject(
        keys: &RedisTaskDispatchKeys,
        delivery: &DispatchDelivery,
        reason: &str,
        staged_units: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation(&delivery.dispatch_key, "reject"),
            [delivery.digest.as_bytes(), reason.as_bytes()].concat(),
            MutationKind::Reject {
                delivery: delivery.clone(),
                reason: reason.to_owned(),
                staged_units,
            },
            config,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn claim(
        keys: &RedisTaskDispatchKeys,
        delivery: &DispatchDelivery,
        owner: &str,
        timeout_ms: u64,
        event: &[u8],
        event_digest: String,
        staged_units: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation(&delivery.dispatch_key, "claim"),
            [
                delivery.digest.as_bytes(),
                owner.as_bytes(),
                event_digest.as_bytes(),
                &timeout_ms.to_be_bytes(),
            ]
            .concat(),
            MutationKind::Claim {
                delivery: delivery.clone(),
                owner: owner.to_owned(),
                timeout_ms,
                event: event.to_vec(),
                event_digest,
                staged_units,
            },
            config,
        )
    }

    fn arm(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        timeout_ms: u64,
        event_digest: String,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "arm",
            [event_digest.as_bytes(), &timeout_ms.to_be_bytes()].concat(),
            MutationKind::Arm {
                claim: claim.clone(),
                timeout_ms,
                event_digest,
            },
            config,
        )
    }

    fn complete(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        event_digest: String,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "complete",
            event_digest.as_bytes().to_vec(),
            MutationKind::Complete {
                claim: claim.clone(),
                event_digest,
            },
            config,
        )
    }

    fn started(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        event: &[u8],
        staged_units: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "started",
            event.to_vec(),
            MutationKind::Started {
                claim: claim.clone(),
                event: event.to_vec(),
                staged_units,
            },
            config,
        )
    }

    fn renew(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        timeout_ms: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "renew",
            timeout_ms.to_be_bytes().to_vec(),
            MutationKind::Renew {
                claim: claim.clone(),
                timeout_ms,
            },
            config,
        )
    }

    fn register_failure(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "register-failure",
            b"due".to_vec(),
            MutationKind::RegisterFailure {
                claim: claim.clone(),
            },
            config,
        )
    }

    fn terminal(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        event: &[u8],
        staged_units: u64,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "terminal",
            [outcome_name(outcome).as_bytes(), event].concat(),
            MutationKind::Terminal {
                claim: claim.clone(),
                outcome,
                event: event.to_vec(),
                staged_units,
            },
            config,
        )
    }

    fn cleanup(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new_claim(
            keys,
            claim,
            "cleanup",
            b"forwarded".to_vec(),
            MutationKind::Cleanup {
                claim: claim.clone(),
            },
            config,
        )
    }

    fn new_claim(
        keys: &RedisTaskDispatchKeys,
        claim: &LocalPickupClaim,
        phase: &str,
        payload: Vec<u8>,
        kind: MutationKind,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        Self::new(
            keys,
            keys.operation(
                &format!(
                    "{}:{}:{}",
                    claim.dispatch_key, claim.pickup_generation, claim.owner
                ),
                phase,
            ),
            payload,
            kind,
            config,
        )
    }

    fn new(
        keys: &RedisTaskDispatchKeys,
        operation_key: String,
        stable_payload: Vec<u8>,
        kind: MutationKind,
        config: &RedisTaskDispatchConfig,
    ) -> Result<Self, RedisTaskDispatchError> {
        let stable_digest = digest_hex(&stable_payload);
        let operation = RedisStableOperation::new(operation_key.clone(), &stable_payload)
            .map_err(|_| RedisTaskDispatchError::InvalidOperation)?;
        Ok(Self {
            operation,
            stable_digest,
            operation_key,
            keys: keys.clone(),
            kind,
            config: MutationConfig {
                max_dispatches: config.max_dispatches.get() as u64,
                max_active_claims: config.max_active_claims.get() as u64,
                max_staged_events: config.max_staged_events.get() as u64,
                hard_limit_bytes: config.hard_limit_bytes,
            },
        })
    }

    fn arguments(&self) -> ScriptArguments<'_> {
        match &self.kind {
            MutationKind::EnsureGroup => ScriptArguments::operation("ensure_group"),
            MutationKind::BindCancellation { identity, task_key } => ScriptArguments {
                operation: "bind_cancellation",
                identity,
                task_key,
                ..ScriptArguments::default()
            },
            MutationKind::Append {
                identity,
                task_key,
                digest,
                payload,
                dispatch_units,
            } => ScriptArguments {
                operation: "append",
                identity,
                task_key,
                digest,
                payload,
                dispatch_units: *dispatch_units,
                ..ScriptArguments::default()
            },
            MutationKind::Reject {
                delivery,
                reason,
                staged_units,
            } => ScriptArguments {
                operation: "reject",
                dispatch_key: &delivery.dispatch_key,
                identity: &delivery.identity,
                digest: &delivery.digest,
                payload: &delivery.payload,
                reason,
                dispatch_units: delivery.units,
                staged_units: *staged_units,
                ..ScriptArguments::default()
            },
            MutationKind::Claim {
                delivery,
                owner,
                timeout_ms,
                event,
                event_digest,
                staged_units,
            } => ScriptArguments {
                operation: "claim",
                dispatch_key: &delivery.dispatch_key,
                identity: &delivery.identity,
                digest: &delivery.digest,
                payload: &delivery.payload,
                owner,
                timeout_ms: *timeout_ms,
                event,
                event_digest,
                dispatch_units: delivery.units,
                staged_units: *staged_units,
                ..ScriptArguments::default()
            },
            MutationKind::Arm {
                claim,
                timeout_ms,
                event_digest,
            } => ScriptArguments::claim("arm", claim, *timeout_ms, event_digest),
            MutationKind::Complete {
                claim,
                event_digest,
            } => ScriptArguments::claim("complete", claim, 0, event_digest),
            MutationKind::Started {
                claim,
                event,
                staged_units,
            } => ScriptArguments {
                operation: "started",
                dispatch_key: &claim.dispatch_key,
                owner: &claim.owner,
                event,
                staged_units: *staged_units,
                generation: claim.pickup_generation,
                ..ScriptArguments::default()
            },
            MutationKind::Renew { claim, timeout_ms } => {
                ScriptArguments::claim("renew", claim, *timeout_ms, "")
            }
            MutationKind::RegisterFailure { claim } => {
                ScriptArguments::claim("register_failure", claim, 0, "")
            }
            MutationKind::Terminal {
                claim,
                outcome,
                event,
                staged_units,
            } => ScriptArguments {
                operation: "terminal",
                dispatch_key: &claim.dispatch_key,
                owner: &claim.owner,
                event,
                outcome: outcome_name(*outcome),
                staged_units: *staged_units,
                generation: claim.pickup_generation,
                ..ScriptArguments::default()
            },
            MutationKind::Cleanup { claim } => ScriptArguments::claim("cleanup", claim, 0, ""),
        }
    }
}

#[derive(Default)]
struct ScriptArguments<'a> {
    operation: &'a str,
    dispatch_key: &'a str,
    identity: &'a str,
    task_key: &'a str,
    digest: &'a str,
    payload: &'a [u8],
    owner: &'a str,
    timeout_ms: u64,
    event: &'a [u8],
    event_digest: &'a str,
    reason: &'a str,
    outcome: &'a str,
    dispatch_units: u64,
    staged_units: u64,
    generation: i64,
}

impl<'a> ScriptArguments<'a> {
    fn operation(operation: &'a str) -> Self {
        Self {
            operation,
            ..Self::default()
        }
    }

    fn claim(
        operation: &'a str,
        claim: &'a LocalPickupClaim,
        timeout_ms: u64,
        event_digest: &'a str,
    ) -> Self {
        Self {
            operation,
            dispatch_key: &claim.dispatch_key,
            owner: &claim.owner,
            timeout_ms,
            event_digest,
            generation: claim.pickup_generation,
            ..Self::default()
        }
    }
}

#[async_trait]
impl RedisStableMutation for ScriptMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let args = self.arguments();
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(TASK_DISPATCH_SCRIPT)
            .arg(29)
            .arg(&self.keys.stream)
            .arg(&self.keys.identities)
            .arg(&self.keys.digests)
            .arg(&self.keys.dispatch_units)
            .arg(&self.keys.generations)
            .arg(&self.keys.owners)
            .arg(&self.keys.deadlines)
            .arg(&self.keys.payloads)
            .arg(&self.keys.assigned)
            .arg(&self.keys.assigned_digests)
            .arg(&self.keys.liveness)
            .arg(&self.keys.source_completed)
            .arg(&self.keys.started)
            .arg(&self.keys.terminal_outcomes)
            .arg(&self.keys.terminal_events)
            .arg(&self.keys.rejections)
            .arg(&self.keys.quota)
            .arg(&self.keys.deadline_index)
            .arg(&self.operation_key)
            .arg(&self.keys.staged_units)
            .arg(&self.keys.task_instances)
            .arg(&self.keys.dispatch_tasks)
            .arg(&self.keys.cancellation_fences)
            .arg(&self.keys.cancellation_tasks)
            .arg(&self.keys.cancellation_dispatches)
            .arg(&self.keys.cancellation_generations)
            .arg(&self.keys.cancellation_owners)
            .arg(&self.keys.cancellation_outcomes)
            .arg(&self.keys.cancellation_deadlines)
            .arg(args.operation)
            .arg(&self.stable_digest)
            .arg(args.dispatch_key)
            .arg(args.identity)
            .arg(args.digest)
            .arg(args.payload)
            .arg(args.owner)
            .arg(args.timeout_ms)
            .arg(args.event)
            .arg(args.event_digest)
            .arg(args.reason)
            .arg(args.outcome)
            .arg(args.dispatch_units)
            .arg(args.staged_units)
            .arg(self.config.max_dispatches)
            .arg(self.config.max_active_claims)
            .arg(self.config.max_staged_events)
            .arg(self.config.hard_limit_bytes)
            .arg(REDIS_TASK_DISPATCH_GROUP)
            .arg(args.generation)
            .arg(args.task_key)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match output.first().map(Vec::as_slice) {
            Some(b"conflict") => Ok(RedisStableMutationOutcome::IdentityConflict),
            Some(b"replayed" | b"completed" | b"settled" | b"rejected") => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            Some(
                b"created" | b"appended" | b"bound" | b"fenced" | b"cancelled" | b"accounting"
                | b"stale" | b"missing" | b"unavailable" | b"claimed" | b"armed" | b"started"
                | b"renewed" | b"registered" | b"won" | b"cleaned",
            ) => Ok(RedisStableMutationOutcome::Applied(output)),
            _ => Err(RedisMutationError::rejected()),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let actual: Option<String> = redis::cmd("GET")
            .arg(&self.operation_key)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(match actual {
            Some(actual) if actual == self.stable_digest => RedisStableMutationRecovery::Matching,
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
            None => RedisStableMutationRecovery::Missing,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskDispatchError {
    InvalidConfiguration,
    InvalidOperation,
    Unavailable,
    IdentityConflict,
    CapacityFenced,
    CancellationFenced,
    Accounting,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisTaskDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis TaskDispatch configuration is invalid",
            Self::InvalidOperation => "Redis TaskDispatch operation is invalid",
            Self::Unavailable => "Redis TaskDispatch role is unavailable",
            Self::IdentityConflict => "Redis TaskDispatch identity conflicts with accepted bytes",
            Self::CapacityFenced => "Redis TaskDispatch capacity is fenced",
            Self::CancellationFenced => "Redis TaskDispatch is fenced by TaskCancellation",
            Self::Accounting => "Redis TaskDispatch accounting is inconsistent",
            Self::Durability(_) => "Redis TaskDispatch durability was not proved",
        })
    }
}

impl std::error::Error for RedisTaskDispatchError {}

fn positive_timeout_millis(duration: chrono::Duration) -> Result<u64, RedisTaskDispatchError> {
    u64::try_from(duration.num_milliseconds())
        .ok()
        .filter(|value| *value > 0)
        .ok_or(RedisTaskDispatchError::InvalidOperation)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn outcome_name(outcome: LocalAttemptOutcome) -> &'static str {
    match outcome {
        LocalAttemptOutcome::ProcessExitedSuccess => "process-exited-success",
        LocalAttemptOutcome::ProcessExitedFailure => "process-exited-failure",
        LocalAttemptOutcome::ProcessSetupFailed => "process-setup-failed",
        LocalAttemptOutcome::LivenessExpired => "liveness-expired",
        LocalAttemptOutcome::CancellationKilled => "cancellation-killed",
        LocalAttemptOutcome::CancellationAlreadyExited => "cancellation-already-exited",
        LocalAttemptOutcome::CancellationNoProcess => "cancellation-no-process",
    }
}

fn parse_outcome(value: &str) -> Option<LocalAttemptOutcome> {
    Some(match value {
        "process-exited-success" => LocalAttemptOutcome::ProcessExitedSuccess,
        "process-exited-failure" => LocalAttemptOutcome::ProcessExitedFailure,
        "process-setup-failed" => LocalAttemptOutcome::ProcessSetupFailed,
        "liveness-expired" => LocalAttemptOutcome::LivenessExpired,
        "cancellation-killed" => LocalAttemptOutcome::CancellationKilled,
        "cancellation-already-exited" => LocalAttemptOutcome::CancellationAlreadyExited,
        "cancellation-no-process" => LocalAttemptOutcome::CancellationNoProcess,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_task_dispatch_operation_manifest().expect("valid manifest");
        assert_eq!(manifest.role(), CoordinationRole::TaskDispatch);
        assert_eq!(manifest.protocol(), REDIS_TASK_DISPATCH_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_TASK_DISPATCH_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), TASK_DISPATCH_SCRIPT_NAME);
        assert_eq!(manifest.scripts()[0].sha256(), TASK_DISPATCH_SCRIPT_SHA256);
        assert!(manifest.commands().contains(&"XREADGROUP"));
        assert!(!manifest.commands().contains(&"PUBLISH"));
        assert!(manifest
            .key_patterns()
            .contains(&"tickr:{namespace}:task-dispatch:owners"));
        assert!(!manifest
            .key_patterns()
            .contains(&"tickr:{namespace}:task-events:stream"));
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
    }

    #[test]
    fn versioned_script_identity_matches_exact_source() {
        assert_eq!(
            digest_hex(TASK_DISPATCH_SCRIPT.as_bytes()),
            TASK_DISPATCH_SCRIPT_SHA256
        );
    }
}
