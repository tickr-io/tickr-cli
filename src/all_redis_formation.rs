use std::{collections::BTreeMap, env, sync::Arc};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use crate::{
    formation::{resolve_formation, CoordinationRole, FormationSelection, ALL_COORDINATION_ROLES},
    redis_acl_admission::{
        admit_complete_redis_formation, canonical_redis_operation_manifests,
        AdmittedRedisFormation, RedisRoleCredential, RedisRoleCredentialSet,
    },
    redis_admission::RedisConnectionDescriptor,
    redis_capability_monitor::{RedisAdmissionCapabilityProbe, RedisCapabilityMonitor},
    redis_capacity::{calibrated_role_capacity, ROLE_MEMORY_LIMIT_NAME},
    redis_formation_identity::{
        RedisDurabilityConfiguration, RedisFormationAdmissionCandidate, RedisNamespaceIdentity,
        RedisRoleLimits,
    },
};

pub const REDIS_CONNECTION_DESCRIPTOR_ENV: &str = "TICKR_REDIS_CONNECTION_DESCRIPTOR";
pub const REDIS_NAMESPACE_ENV: &str = "TICKR_REDIS_NAMESPACE";
pub const REDIS_CAPACITY_BYTES_ENV: &str = "TICKR_REDIS_CAPACITY_BYTES";
pub const REDIS_ROLE_CREDENTIALS_ENV: &str = "TICKR_REDIS_ROLE_CREDENTIALS";
const REDIS_DEFAULT_RETENTION_SECONDS: u64 = 3_600;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalRoleCredential {
    role: String,
    identity: String,
    secret: String,
}

/// Purely parsed all-Redis process inputs. Constructing this value opens no
/// repository, Redis client, NATS client, listener, consumer, or producer.
pub struct AllRedisProcessAdmission {
    descriptor: Arc<RedisConnectionDescriptor>,
    candidate: RedisFormationAdmissionCandidate,
    credentials: RedisRoleCredentialSet,
}

pub struct AdmittedAllRedisProcess {
    pub formation: AdmittedRedisFormation,
    pub monitor: Arc<RedisCapabilityMonitor>,
}

impl AllRedisProcessAdmission {
    pub fn from_environment() -> Result<Self> {
        let descriptor_json = required_env(REDIS_CONNECTION_DESCRIPTOR_ENV)?;
        let descriptor = Arc::new(
            RedisConnectionDescriptor::parse_json(&descriptor_json)
                .context("parsing the all-Redis connection descriptor")?,
        );
        let namespace = RedisNamespaceIdentity::new(required_env(REDIS_NAMESPACE_ENV)?)
            .context("parsing the all-Redis namespace")?;
        let capacity_bytes = required_env(REDIS_CAPACITY_BYTES_ENV)?
            .parse::<u64>()
            .with_context(|| format!("parsing {REDIS_CAPACITY_BYTES_ENV} as bytes"))?;
        let credentials = parse_role_credentials(&required_env(REDIS_ROLE_CREDENTIALS_ENV)?)?;
        let descriptor_authority = resolve_formation(&FormationSelection::all_redis())
            .context("resolving the all-Redis formation descriptor")?;
        let role_limits = ALL_COORDINATION_ROLES
            .into_iter()
            .map(|role| {
                RedisRoleLimits::new(
                    role,
                    BTreeMap::from([(
                        ROLE_MEMORY_LIMIT_NAME.to_owned(),
                        calibrated_role_capacity(role).default_bytes,
                    )]),
                    BTreeMap::from([(
                        "retention-seconds".to_owned(),
                        REDIS_DEFAULT_RETENTION_SECONDS,
                    )]),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .context("constructing calibrated all-Redis role limits")?;
        let candidate = RedisFormationAdmissionCandidate::construct(
            &descriptor_authority,
            canonical_redis_operation_manifests()
                .context("constructing all-Redis operation manifests")?,
            namespace,
            role_limits,
            RedisDurabilityConfiguration::primary_local_aof(capacity_bytes),
        )
        .context("constructing the all-Redis admission candidate")?;

        Ok(Self {
            descriptor,
            candidate,
            credentials,
        })
    }

    /// Completes every external capability and ACL probe before returning any
    /// runtime role client or allowing a component repository to be opened.
    pub async fn admit(self) -> Result<AdmittedAllRedisProcess> {
        let formation_probe = Arc::new(RedisAdmissionCapabilityProbe::new(
            Arc::clone(&self.descriptor),
            self.candidate.clone(),
        ));
        let monitor = Arc::new(RedisCapabilityMonitor::new(
            self.candidate.clone(),
            formation_probe,
        ));
        let formation = admit_complete_redis_formation(
            self.descriptor.as_ref(),
            self.candidate,
            self.credentials,
        )
        .await
        .context("admitting the complete all-Redis formation")?;
        Ok(AdmittedAllRedisProcess { formation, monitor })
    }
}

fn required_env(name: &'static str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{name} is required for the all-Redis formation"))
}

fn parse_role_credentials(input: &str) -> Result<RedisRoleCredentialSet> {
    let external: Vec<ExternalRoleCredential> =
        serde_json::from_str(input).context("parsing all-Redis role credentials")?;
    let credentials = external
        .into_iter()
        .map(|credential| {
            let role = parse_role(&credential.role)
                .ok_or_else(|| anyhow!("unknown all-Redis Coordination role"))?;
            Ok(RedisRoleCredential::new(
                role,
                credential.identity,
                credential.secret,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    RedisRoleCredentialSet::admit(credentials).context("admitting all-Redis role credentials")
}

fn parse_role(value: &str) -> Option<CoordinationRole> {
    match value {
        "command-bus" => Some(CoordinationRole::CommandBus),
        "task-dispatch" => Some(CoordinationRole::TaskDispatch),
        "task-events" => Some(CoordinationRole::TaskEvents),
        "task-cancellation" => Some(CoordinationRole::TaskCancellation),
        "compaction-staging" => Some(CoordinationRole::CompactionStaging),
        "lifecycle-work" => Some(CoordinationRole::LifecycleWork),
        "log-staging" => Some(CoordinationRole::LogStaging),
        "scope-store" => Some(CoordinationRole::ScopeStore),
        "ingress-idempotency-store" => Some(CoordinationRole::IngressIdempotencyStore),
        "liveness-watchdog" => Some(CoordinationRole::LivenessWatchdog),
        "signal-applied-notifier" => Some(CoordinationRole::SignalAppliedNotifier),
        "executor-fleet-status" => Some(CoordinationRole::ExecutorFleetStatus),
        "event-ingress" => Some(CoordinationRole::EventIngress),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_credentials_require_the_complete_distinct_role_set() {
        let json = serde_json::to_string(
            &ALL_COORDINATION_ROLES
                .into_iter()
                .enumerate()
                .map(|(index, role)| {
                    serde_json::json!({
                        "role": role_name(role),
                        "identity": format!("role-{index}"),
                        "secret": format!("secret-{index}"),
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(parse_role_credentials(&json).is_ok());

        let incomplete = r#"[{"role":"command-bus","identity":"one","secret":"secret"}]"#;
        assert!(parse_role_credentials(incomplete).is_err());
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
}
