use std::{collections::BTreeMap, fmt};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::formation::{
    CoordinationRole, FormationProfile, ProtocolIdentity, ResolvedFormationDescriptor,
    RoleImplementation, ALL_COORDINATION_ROLES,
};

const MANIFEST_SCHEMA: &str = "tickr.all-redis.operation-manifest/v1";

/// Stable identity of one adapter-owned Lua script.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisScriptIdentity {
    name: &'static str,
    sha256: &'static str,
}

impl RedisScriptIdentity {
    pub fn new(
        name: &'static str,
        sha256: &'static str,
    ) -> Result<Self, RedisOperationManifestError> {
        if !valid_symbol(name)
            || sha256.len() != 64
            || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            || sha256.bytes().any(|byte| byte.is_ascii_uppercase())
            || contains_sensitive_value(name)
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::MalformedManifest,
            ));
        }
        Ok(Self { name, sha256 })
    }

    pub fn name(&self) -> &str {
        self.name
    }

    pub fn sha256(&self) -> &str {
        self.sha256
    }
}

/// An operation the role adapter may execute through its role credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisOperation {
    Command(&'static str),
    Script(RedisScriptIdentity),
}

impl RedisOperation {
    pub const fn command(command: &'static str) -> Self {
        Self::Command(command)
    }

    pub const fn script(script: RedisScriptIdentity) -> Self {
        Self::Script(script)
    }

    fn normalized_identity(self) -> String {
        match self {
            Self::Command(command) => format!("command:{command}"),
            Self::Script(script) => format!("script:{}:{}", script.name, script.sha256),
        }
    }
}

/// A key or channel pattern owned by one Coordination role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisNamespacePattern {
    Key(&'static str),
    Channel(&'static str),
}

impl RedisNamespacePattern {
    pub const fn key(pattern: &'static str) -> Self {
        Self::Key(pattern)
    }

    pub const fn channel(pattern: &'static str) -> Self {
        Self::Channel(pattern)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Key(pattern) | Self::Channel(pattern) => pattern,
        }
    }

    fn normalized_identity(self) -> String {
        match self {
            Self::Key(pattern) => format!("key:{pattern}"),
            Self::Channel(pattern) => format!("channel:{pattern}"),
        }
    }
}

/// One representative probe proving an operation required by the adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisRequiredOperationCanary {
    operation: RedisOperation,
    target: RedisNamespacePattern,
}

impl RedisRequiredOperationCanary {
    pub const fn new(operation: RedisOperation, target: RedisNamespacePattern) -> Self {
        Self { operation, target }
    }

    pub const fn operation(&self) -> RedisOperation {
        self.operation
    }

    pub const fn target(&self) -> RedisNamespacePattern {
        self.target
    }
}

/// The namespace class against which a role credential must be denied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisForbiddenTarget {
    CoordinationRole(CoordinationRole),
    Administrative,
}

/// A representative probe proving cross-role or administrative denial.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisForbiddenOperation {
    operation: RedisOperation,
    target: RedisForbiddenTarget,
}

impl RedisForbiddenOperation {
    pub const fn cross_role(operation: RedisOperation, role: CoordinationRole) -> Self {
        Self {
            operation,
            target: RedisForbiddenTarget::CoordinationRole(role),
        }
    }

    pub const fn administrative(command: &'static str) -> Self {
        Self {
            operation: RedisOperation::Command(command),
            target: RedisForbiddenTarget::Administrative,
        }
    }

    pub const fn operation(&self) -> RedisOperation {
        self.operation
    }

    pub const fn target(&self) -> RedisForbiddenTarget {
        self.target
    }
}

/// One role adapter's exact, secret-free Redis operation contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisOperationManifest {
    role: CoordinationRole,
    protocol: ProtocolIdentity,
    commands: Vec<&'static str>,
    scripts: Vec<RedisScriptIdentity>,
    key_patterns: Vec<&'static str>,
    channel_patterns: Vec<&'static str>,
    required_canaries: Vec<RedisRequiredOperationCanary>,
    forbidden_operations: Vec<RedisForbiddenOperation>,
    identity: RedisOperationManifestIdentity,
}

impl RedisOperationManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        role: CoordinationRole,
        protocol: ProtocolIdentity,
        mut commands: Vec<&'static str>,
        mut scripts: Vec<RedisScriptIdentity>,
        mut key_patterns: Vec<&'static str>,
        mut channel_patterns: Vec<&'static str>,
        required_canaries: Vec<RedisRequiredOperationCanary>,
        forbidden_operations: Vec<RedisForbiddenOperation>,
    ) -> Result<Self, RedisOperationManifestError> {
        if !valid_protocol(protocol)
            || commands.iter().any(|command| !valid_command(command))
            || scripts
                .iter()
                .any(|script| contains_sensitive_value(script.name))
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::MalformedManifest,
            ));
        }

        commands.sort_unstable();
        scripts.sort_unstable_by_key(|script| script.name);
        key_patterns.sort_unstable();
        channel_patterns.sort_unstable();
        if has_adjacent_duplicate(&commands)
            || scripts.windows(2).any(|pair| pair[0].name == pair[1].name)
            || has_adjacent_duplicate(&key_patterns)
            || has_adjacent_duplicate(&channel_patterns)
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::DuplicateManifestEntry,
            ));
        }
        if commands.is_empty() && scripts.is_empty() {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::MalformedManifest,
            ));
        }

        let expected_prefix = format!("tickr:{{namespace}}:{}:", role_name(role));
        if key_patterns
            .iter()
            .chain(channel_patterns.iter())
            .any(|pattern| !valid_namespace_pattern(pattern, &expected_prefix))
            || key_patterns.is_empty() && channel_patterns.is_empty()
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::CrossRoleNamespace,
            ));
        }

        let operation_is_registered = |operation: RedisOperation| match operation {
            RedisOperation::Command(command) => commands.binary_search(&command).is_ok(),
            RedisOperation::Script(script) => scripts
                .binary_search_by_key(&script.name, |s| s.name)
                .is_ok_and(|index| scripts[index] == script),
        };
        let target_is_registered = |target: RedisNamespacePattern| match target {
            RedisNamespacePattern::Key(pattern) => key_patterns.binary_search(&pattern).is_ok(),
            RedisNamespacePattern::Channel(pattern) => {
                channel_patterns.binary_search(&pattern).is_ok()
            }
        };

        if required_canaries.is_empty()
            || required_canaries.iter().any(|canary| {
                !operation_is_registered(canary.operation) || !target_is_registered(canary.target)
            })
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::UnregisteredOperation,
            ));
        }

        let mut has_cross_role_probe = false;
        let mut has_administrative_probe = false;
        for forbidden in &forbidden_operations {
            match forbidden.target {
                RedisForbiddenTarget::CoordinationRole(target_role) => {
                    has_cross_role_probe = true;
                    if target_role == role || !operation_is_registered(forbidden.operation) {
                        return Err(RedisOperationManifestError::new(
                            RedisOperationManifestFailure::CrossRoleManifest,
                        ));
                    }
                }
                RedisForbiddenTarget::Administrative => {
                    has_administrative_probe = true;
                    match forbidden.operation {
                        RedisOperation::Command(command)
                            if valid_command(command)
                                && !operation_is_registered(forbidden.operation) => {}
                        _ => {
                            return Err(RedisOperationManifestError::new(
                                RedisOperationManifestFailure::MalformedManifest,
                            ));
                        }
                    }
                }
            }
        }
        if !has_cross_role_probe || !has_administrative_probe {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::MissingForbiddenOperation,
            ));
        }

        let projection = normalize_manifest(
            role,
            protocol,
            &commands,
            &scripts,
            &key_patterns,
            &channel_patterns,
            &required_canaries,
            &forbidden_operations,
        )?;
        let bytes = serde_json::to_vec(&projection).map_err(|_| {
            RedisOperationManifestError::new(RedisOperationManifestFailure::NormalizationFailed)
        })?;
        let identity = RedisOperationManifestIdentity(format!("{:x}", Sha256::digest(bytes)));

        Ok(Self {
            role,
            protocol,
            commands,
            scripts,
            key_patterns,
            channel_patterns,
            required_canaries,
            forbidden_operations,
            identity,
        })
    }

    pub const fn role(&self) -> CoordinationRole {
        self.role
    }

    pub const fn protocol(&self) -> ProtocolIdentity {
        self.protocol
    }

    pub fn commands(&self) -> &[&'static str] {
        &self.commands
    }

    pub fn scripts(&self) -> &[RedisScriptIdentity] {
        &self.scripts
    }

    pub fn key_patterns(&self) -> &[&'static str] {
        &self.key_patterns
    }

    pub fn channel_patterns(&self) -> &[&'static str] {
        &self.channel_patterns
    }

    pub fn required_canaries(&self) -> &[RedisRequiredOperationCanary] {
        &self.required_canaries
    }

    pub fn forbidden_operations(&self) -> &[RedisForbiddenOperation] {
        &self.forbidden_operations
    }

    pub fn identity(&self) -> &RedisOperationManifestIdentity {
        &self.identity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisOperationManifestIdentity(String);

impl RedisOperationManifestIdentity {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The complete adapter-supplied operation contract for all-Redis admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisOperationManifestSet {
    manifests: Vec<RedisOperationManifest>,
}

impl RedisOperationManifestSet {
    pub fn admit(
        descriptor: &ResolvedFormationDescriptor,
        manifests: Vec<RedisOperationManifest>,
    ) -> Result<Self, RedisOperationManifestError> {
        if descriptor.profile != FormationProfile::AllRedis
            || descriptor
                .roles
                .iter()
                .any(|role| role.implementation != RoleImplementation::Redis)
        {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::NotAllRedisFormation,
            ));
        }

        let mut by_role = BTreeMap::new();
        for manifest in manifests {
            let role = manifest.role;
            if by_role.insert(role as usize, manifest).is_some() {
                return Err(RedisOperationManifestError::new(
                    RedisOperationManifestFailure::DuplicateRole,
                ));
            }
        }

        let mut normalized = Vec::with_capacity(ALL_COORDINATION_ROLES.len());
        for role in ALL_COORDINATION_ROLES {
            let manifest = by_role.remove(&(role as usize)).ok_or_else(|| {
                RedisOperationManifestError::new(RedisOperationManifestFailure::MissingRole)
            })?;
            let resolved = descriptor.roles.get(role);
            if manifest.protocol != resolved.protocol {
                return Err(RedisOperationManifestError::new(
                    RedisOperationManifestFailure::ProtocolMismatch,
                ));
            }
            if normalized
                .iter()
                .any(|registered: &RedisOperationManifest| registered.identity == manifest.identity)
            {
                return Err(RedisOperationManifestError::new(
                    RedisOperationManifestFailure::DuplicateManifestIdentity,
                ));
            }
            normalized.push(manifest);
        }
        if !by_role.is_empty() {
            return Err(RedisOperationManifestError::new(
                RedisOperationManifestFailure::CrossRoleManifest,
            ));
        }
        Ok(Self {
            manifests: normalized,
        })
    }

    pub fn get(&self, role: CoordinationRole) -> &RedisOperationManifest {
        &self.manifests[role as usize]
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &RedisOperationManifest> {
        self.manifests.iter()
    }

    pub(crate) fn identity_projection(&self) -> Vec<RedisOperationManifestProjection> {
        self.manifests
            .iter()
            .map(|manifest| RedisOperationManifestProjection {
                role: role_name(manifest.role).to_owned(),
                protocol: ProtocolProjection {
                    name: manifest.protocol.name.to_owned(),
                    version: manifest.protocol.version,
                },
                manifest_identity: manifest.identity.as_str().to_owned(),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisOperationManifestProjection {
    pub role: String,
    pub protocol: ProtocolProjection,
    pub manifest_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProtocolProjection {
    pub name: String,
    pub version: u16,
}

#[derive(Serialize)]
struct NormalizedManifestProjection {
    schema: &'static str,
    role: &'static str,
    protocol: ProtocolProjection,
    commands: Vec<&'static str>,
    scripts: Vec<ScriptProjection>,
    key_patterns: Vec<&'static str>,
    channel_patterns: Vec<&'static str>,
    required_canaries: Vec<CanaryProjection>,
    forbidden_operations: Vec<ForbiddenProjection>,
}

#[derive(Serialize)]
struct ScriptProjection {
    name: &'static str,
    sha256: &'static str,
}

#[derive(Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct CanaryProjection {
    operation: String,
    target: String,
}

#[derive(Serialize, Ord, PartialOrd, Eq, PartialEq)]
struct ForbiddenProjection {
    operation: String,
    target: String,
}

#[allow(clippy::too_many_arguments)]
fn normalize_manifest(
    role: CoordinationRole,
    protocol: ProtocolIdentity,
    commands: &[&'static str],
    scripts: &[RedisScriptIdentity],
    key_patterns: &[&'static str],
    channel_patterns: &[&'static str],
    required_canaries: &[RedisRequiredOperationCanary],
    forbidden_operations: &[RedisForbiddenOperation],
) -> Result<NormalizedManifestProjection, RedisOperationManifestError> {
    let mut required_canaries = required_canaries
        .iter()
        .map(|canary| CanaryProjection {
            operation: canary.operation.normalized_identity(),
            target: canary.target.normalized_identity(),
        })
        .collect::<Vec<_>>();
    required_canaries.sort_unstable();
    if required_canaries.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(RedisOperationManifestError::new(
            RedisOperationManifestFailure::DuplicateManifestEntry,
        ));
    }

    let mut forbidden_operations = forbidden_operations
        .iter()
        .map(|forbidden| ForbiddenProjection {
            operation: forbidden.operation.normalized_identity(),
            target: match forbidden.target {
                RedisForbiddenTarget::CoordinationRole(role) => {
                    format!("coordination-role:{}", role_name(role))
                }
                RedisForbiddenTarget::Administrative => "administrative".to_owned(),
            },
        })
        .collect::<Vec<_>>();
    forbidden_operations.sort_unstable();
    if forbidden_operations
        .windows(2)
        .any(|pair| pair[0] == pair[1])
    {
        return Err(RedisOperationManifestError::new(
            RedisOperationManifestFailure::DuplicateManifestEntry,
        ));
    }

    Ok(NormalizedManifestProjection {
        schema: MANIFEST_SCHEMA,
        role: role_name(role),
        protocol: ProtocolProjection {
            name: protocol.name.to_owned(),
            version: protocol.version,
        },
        commands: commands.to_vec(),
        scripts: scripts
            .iter()
            .map(|script| ScriptProjection {
                name: script.name,
                sha256: script.sha256,
            })
            .collect(),
        key_patterns: key_patterns.to_vec(),
        channel_patterns: channel_patterns.to_vec(),
        required_canaries,
        forbidden_operations,
    })
}

fn has_adjacent_duplicate<T: PartialEq>(values: &[T]) -> bool {
    values.windows(2).any(|pair| pair[0] == pair[1])
}

fn valid_protocol(protocol: ProtocolIdentity) -> bool {
    protocol.version > 0
        && valid_protocol_name(protocol.name)
        && !contains_sensitive_value(protocol.name)
}

fn valid_protocol_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 127
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
        })
}

fn valid_command(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with(' ')
        && !value.ends_with(' ')
        && !value.contains("  ")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b' ')
        && !contains_sensitive_value(value)
}

fn valid_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 127
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

fn valid_namespace_pattern(value: &str, expected_prefix: &str) -> bool {
    value.starts_with(expected_prefix)
        && value.len() > expected_prefix.len()
        && value.len() <= 255
        && value[expected_prefix.len()..].bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'*')
        })
        && !contains_sensitive_value(value)
}

fn contains_sensitive_value(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    [
        "://",
        "endpoint",
        "username",
        "password",
        "credential",
        "trust-root",
        "trust_root",
        "certificate",
        "private-key",
        "private_key",
        "-----begin",
    ]
    .iter()
    .any(|forbidden| normalized.contains(forbidden))
}

fn role_name(role: CoordinationRole) -> &'static str {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisOperationManifestFailure {
    NotAllRedisFormation,
    MissingRole,
    DuplicateRole,
    DuplicateManifestIdentity,
    ProtocolMismatch,
    MalformedManifest,
    DuplicateManifestEntry,
    CrossRoleNamespace,
    CrossRoleManifest,
    UnregisteredOperation,
    MissingForbiddenOperation,
    NormalizationFailed,
}

impl RedisOperationManifestFailure {
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::NotAllRedisFormation => "the Resolved formation descriptor is not all-Redis",
            Self::MissingRole => "a Coordination role operation manifest is missing",
            Self::DuplicateRole => "a Coordination role operation manifest is duplicated",
            Self::DuplicateManifestIdentity => "operation manifest identities are not unique",
            Self::ProtocolMismatch => "an operation manifest protocol identity does not match",
            Self::MalformedManifest => "an operation manifest value is malformed or sensitive",
            Self::DuplicateManifestEntry => "an operation manifest entry is duplicated",
            Self::CrossRoleNamespace => "an operation manifest contains a cross-role namespace",
            Self::CrossRoleManifest => "an operation manifest contains a cross-role operation",
            Self::UnregisteredOperation => {
                "a canary references an unregistered operation or pattern"
            }
            Self::MissingForbiddenOperation => {
                "cross-role and administrative denial probes are required"
            }
            Self::NormalizationFailed => "operation manifest normalization failed",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RedisOperationManifestError {
    failure: RedisOperationManifestFailure,
}

impl RedisOperationManifestError {
    const fn new(failure: RedisOperationManifestFailure) -> Self {
        Self { failure }
    }

    pub const fn failure(&self) -> RedisOperationManifestFailure {
        self.failure
    }
}

impl fmt::Display for RedisOperationManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Redis operation manifest admission failed: {}",
            self.failure.description()
        )
    }
}

impl fmt::Debug for RedisOperationManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisOperationManifestError")
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisOperationManifestError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formation::{resolve_formation, FormationSelection};

    const SCRIPT_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const CHANGED_SCRIPT_HASH: &str =
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn pattern(role: CoordinationRole) -> &'static str {
        match role {
            CoordinationRole::CommandBus => "tickr:{namespace}:command-bus:records:*",
            CoordinationRole::TaskDispatch => "tickr:{namespace}:task-dispatch:records:*",
            CoordinationRole::TaskEvents => "tickr:{namespace}:task-events:records:*",
            CoordinationRole::TaskCancellation => "tickr:{namespace}:task-cancellation:records:*",
            CoordinationRole::CompactionStaging => "tickr:{namespace}:compaction-staging:records:*",
            CoordinationRole::LifecycleWork => "tickr:{namespace}:lifecycle-work:records:*",
            CoordinationRole::LogStaging => "tickr:{namespace}:log-staging:records:*",
            CoordinationRole::ScopeStore => "tickr:{namespace}:scope-store:records:*",
            CoordinationRole::IngressIdempotencyStore => {
                "tickr:{namespace}:ingress-idempotency-store:records:*"
            }
            CoordinationRole::LivenessWatchdog => "tickr:{namespace}:liveness-watchdog:records:*",
            CoordinationRole::SignalAppliedNotifier => {
                "tickr:{namespace}:signal-applied-notifier:records:*"
            }
            CoordinationRole::ExecutorFleetStatus => {
                "tickr:{namespace}:executor-fleet-status:records:*"
            }
            CoordinationRole::EventIngress => "tickr:{namespace}:event-ingress:records:*",
        }
    }

    fn other_role(role: CoordinationRole) -> CoordinationRole {
        if role == CoordinationRole::CommandBus {
            CoordinationRole::TaskDispatch
        } else {
            CoordinationRole::CommandBus
        }
    }

    fn manifest(
        descriptor: &ResolvedFormationDescriptor,
        role: CoordinationRole,
    ) -> RedisOperationManifest {
        let key_pattern = pattern(role);
        RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["GET", "SET"],
            vec![],
            vec![key_pattern],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key(key_pattern),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    other_role(role),
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap()
    }

    fn manifests(descriptor: &ResolvedFormationDescriptor) -> Vec<RedisOperationManifest> {
        ALL_COORDINATION_ROLES
            .iter()
            .map(|role| manifest(descriptor, *role))
            .collect()
    }

    #[test]
    fn normalization_is_stable_and_contract_changes_change_identity() {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        let role = CoordinationRole::CommandBus;
        let key_pattern = pattern(role);
        let script = RedisScriptIdentity::new("apply", SCRIPT_HASH).unwrap();
        let first = RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["SET", "GET"],
            vec![script],
            vec![key_pattern],
            vec!["tickr:{namespace}:command-bus:replies:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::script(script),
                RedisNamespacePattern::key(key_pattern),
            )],
            vec![
                RedisForbiddenOperation::administrative("CONFIG GET"),
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
            ],
        )
        .unwrap();
        let reordered = RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["GET", "SET"],
            vec![script],
            vec![key_pattern],
            vec!["tickr:{namespace}:command-bus:replies:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::script(script),
                RedisNamespacePattern::key(key_pattern),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap();
        assert_eq!(first.identity(), reordered.identity());

        let changed_command = RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["GET", "SET", "TIME"],
            vec![script],
            vec![key_pattern],
            vec!["tickr:{namespace}:command-bus:replies:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::script(script),
                RedisNamespacePattern::key(key_pattern),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap();
        assert_ne!(first.identity(), changed_command.identity());

        let changed_script = RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["GET", "SET"],
            vec![RedisScriptIdentity::new("apply", CHANGED_SCRIPT_HASH).unwrap()],
            vec![key_pattern],
            vec!["tickr:{namespace}:command-bus:replies:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key(key_pattern),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap();
        assert_ne!(first.identity(), changed_script.identity());

        let changed_pattern = RedisOperationManifest::new(
            role,
            descriptor.roles.get(role).protocol,
            vec!["GET", "SET"],
            vec![script],
            vec!["tickr:{namespace}:command-bus:changed:*"],
            vec!["tickr:{namespace}:command-bus:replies:*"],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key("tickr:{namespace}:command-bus:changed:*"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap();
        assert_ne!(first.identity(), changed_pattern.identity());
    }

    #[test]
    fn complete_set_requires_one_protocol_matching_manifest_per_role() {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        let complete =
            RedisOperationManifestSet::admit(&descriptor, manifests(&descriptor)).unwrap();
        assert_eq!(complete.iter().len(), ALL_COORDINATION_ROLES.len());
        let mut reversed = manifests(&descriptor);
        reversed.reverse();
        assert_eq!(
            complete,
            RedisOperationManifestSet::admit(&descriptor, reversed).unwrap()
        );

        let mut missing = manifests(&descriptor);
        missing.pop();
        assert_eq!(
            RedisOperationManifestSet::admit(&descriptor, missing)
                .unwrap_err()
                .failure(),
            RedisOperationManifestFailure::MissingRole
        );

        let mut duplicate = manifests(&descriptor);
        duplicate.push(manifest(&descriptor, CoordinationRole::CommandBus));
        assert_eq!(
            RedisOperationManifestSet::admit(&descriptor, duplicate)
                .unwrap_err()
                .failure(),
            RedisOperationManifestFailure::DuplicateRole
        );

        let mut mismatched = manifests(&descriptor);
        mismatched[CoordinationRole::CommandBus as usize] = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            ProtocolIdentity::new("tickr.all-redis.command-bus", 2),
            vec!["GET"],
            vec![],
            vec![pattern(CoordinationRole::CommandBus)],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("GET"),
                RedisNamespacePattern::key(pattern(CoordinationRole::CommandBus)),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap();
        assert_eq!(
            RedisOperationManifestSet::admit(&descriptor, mismatched)
                .unwrap_err()
                .failure(),
            RedisOperationManifestFailure::ProtocolMismatch
        );
    }

    #[test]
    fn malformed_cross_role_sensitive_and_unregistered_values_fail_closed() {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        let protocol = descriptor.roles.get(CoordinationRole::CommandBus).protocol;

        let cross_role = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            protocol,
            vec!["GET"],
            vec![],
            vec!["tickr:{namespace}:task-dispatch:records:*"],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("GET"),
                RedisNamespacePattern::key("tickr:{namespace}:task-dispatch:records:*"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            cross_role.failure(),
            RedisOperationManifestFailure::CrossRoleNamespace
        );

        let sensitive = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            protocol,
            vec!["GET"],
            vec![],
            vec!["tickr:{namespace}:command-bus:password:*"],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("GET"),
                RedisNamespacePattern::key("tickr:{namespace}:command-bus:password:*"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            sensitive.failure(),
            RedisOperationManifestFailure::CrossRoleNamespace
        );

        let sensitive_admin_probe = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            protocol,
            vec!["GET"],
            vec![],
            vec![pattern(CoordinationRole::CommandBus)],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("GET"),
                RedisNamespacePattern::key(pattern(CoordinationRole::CommandBus)),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("PASSWORD"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            sensitive_admin_probe.failure(),
            RedisOperationManifestFailure::MalformedManifest
        );

        let unregistered = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            protocol,
            vec!["GET"],
            vec![],
            vec![pattern(CoordinationRole::CommandBus)],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("SET"),
                RedisNamespacePattern::key(pattern(CoordinationRole::CommandBus)),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::command("GET"),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("CONFIG GET"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            unregistered.failure(),
            RedisOperationManifestFailure::UnregisteredOperation
        );
    }
}
