use std::{array, error::Error, fmt};

const COORDINATION_ROLE_COUNT: usize = 13;

/// A named Data-plane formation profile.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FormationProfile {
    /// The default distributed all-NATS formation.
    #[default]
    AllNats,
    /// The explicit single-node Tickr Lite formation.
    LiteLocal,
    /// The explicit distributed all-Redis formation.
    AllRedis,
}

impl FormationProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::AllNats => "all-nats",
            Self::LiteLocal => "lite-local",
            Self::AllRedis => "all-redis",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Topology {
    Distributed,
    SingleNode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImplementation {
    Postgres,
    Sqlite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalLogStore {
    ObjectStore,
    LocalFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterTopology {
    Distributed,
    ConductorOwned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorTopology {
    DistributedFleet,
    Exactly(u16),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpCommandIngress {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CoordinationRole {
    CommandBus = 0,
    TaskDispatch = 1,
    TaskEvents = 2,
    TaskCancellation = 3,
    CompactionStaging = 4,
    LifecycleWork = 5,
    LogStaging = 6,
    ScopeStore = 7,
    IngressIdempotencyStore = 8,
    LivenessWatchdog = 9,
    SignalAppliedNotifier = 10,
    ExecutorFleetStatus = 11,
    EventIngress = 12,
}

pub const ALL_COORDINATION_ROLES: [CoordinationRole; COORDINATION_ROLE_COUNT] = [
    CoordinationRole::CommandBus,
    CoordinationRole::TaskDispatch,
    CoordinationRole::TaskEvents,
    CoordinationRole::TaskCancellation,
    CoordinationRole::CompactionStaging,
    CoordinationRole::LifecycleWork,
    CoordinationRole::LogStaging,
    CoordinationRole::ScopeStore,
    CoordinationRole::IngressIdempotencyStore,
    CoordinationRole::LivenessWatchdog,
    CoordinationRole::SignalAppliedNotifier,
    CoordinationRole::ExecutorFleetStatus,
    CoordinationRole::EventIngress,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleImplementation {
    NatsJetStream,
    Redis,
    LocalRequestReply,
    LocalSqlite,
    LocalJournal,
    LocalNotification,
    LocalObservation,
    Disabled,
}

/// Stable identity for one role's persisted or transported protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolIdentity {
    pub name: &'static str,
    pub version: u16,
}

impl ProtocolIdentity {
    pub const fn new(name: &'static str, version: u16) -> Self {
        Self { name, version }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRole {
    pub role: CoordinationRole,
    pub implementation: RoleImplementation,
    pub protocol: ProtocolIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedCoordinationRoles {
    roles: [ResolvedRole; COORDINATION_ROLE_COUNT],
}

impl ResolvedCoordinationRoles {
    pub fn get(&self, role: CoordinationRole) -> &ResolvedRole {
        &self.roles[role as usize]
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ResolvedRole> {
        self.roles.iter()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChoreographyCapability {
    SafePickupHandoff,
    SafeAttemptOutcomeHandoff,
    SafeCancellationFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChoreographyCapabilities {
    pub safe_pickup_handoff: bool,
    pub safe_attempt_outcome_handoff: bool,
    pub safe_cancellation_fence: bool,
}

impl ChoreographyCapabilities {
    pub const fn all() -> Self {
        Self {
            safe_pickup_handoff: true,
            safe_attempt_outcome_handoff: true,
            safe_cancellation_fence: true,
        }
    }

    const fn has(self, capability: ChoreographyCapability) -> bool {
        match capability {
            ChoreographyCapability::SafePickupHandoff => self.safe_pickup_handoff,
            ChoreographyCapability::SafeAttemptOutcomeHandoff => self.safe_attempt_outcome_handoff,
            ChoreographyCapability::SafeCancellationFence => self.safe_cancellation_fence,
        }
    }
}

/// The immutable runtime authority produced by formation admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedFormationDescriptor {
    pub profile: FormationProfile,
    pub topology: Topology,
    pub sql: SqlImplementation,
    pub sql_migration_identity: ProtocolIdentity,
    pub final_logs: FinalLogStore,
    pub writer_topology: WriterTopology,
    pub executors: ExecutorTopology,
    pub http_commands: HttpCommandIngress,
    pub roles: ResolvedCoordinationRoles,
    pub choreography: ChoreographyCapabilities,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProtocolOverride {
    #[default]
    ProfileDefault,
    Identity(ProtocolIdentity),
    Missing,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoleOverride {
    pub implementation: Option<RoleImplementation>,
    pub protocol: ProtocolOverride,
}

/// Raw profile selection and behavior-affecting overrides. Resolution is pure:
/// callers must admit this value before constructing any runtime resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FormationSelection {
    pub profile: Option<FormationProfile>,
    pub topology: Option<Topology>,
    pub sql: Option<SqlImplementation>,
    pub final_logs: Option<FinalLogStore>,
    pub writer_topology: Option<WriterTopology>,
    pub executor_count: Option<u16>,
    pub choreography: Option<ChoreographyCapabilities>,
    pub http_commands: Option<HttpCommandIngress>,
    role_overrides: [Option<RoleOverride>; COORDINATION_ROLE_COUNT],
}

impl Default for FormationSelection {
    fn default() -> Self {
        Self {
            profile: None,
            topology: None,
            sql: None,
            final_logs: None,
            writer_topology: None,
            executor_count: None,
            choreography: None,
            http_commands: None,
            role_overrides: [None; COORDINATION_ROLE_COUNT],
        }
    }
}

impl FormationSelection {
    pub fn all_nats() -> Self {
        Self {
            profile: Some(FormationProfile::AllNats),
            ..Self::default()
        }
    }

    pub fn lite_local() -> Self {
        Self {
            profile: Some(FormationProfile::LiteLocal),
            ..Self::default()
        }
    }

    pub fn all_redis() -> Self {
        Self {
            profile: Some(FormationProfile::AllRedis),
            ..Self::default()
        }
    }

    pub fn with_role_override(
        mut self,
        role: CoordinationRole,
        role_override: RoleOverride,
    ) -> Self {
        self.role_overrides[role as usize] = Some(role_override);
        self
    }

    pub fn role_override(&self, role: CoordinationRole) -> Option<RoleOverride> {
        self.role_overrides[role as usize]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormationAdmissionError {
    IncompatibleTopology {
        expected: Topology,
        actual: Topology,
    },
    IncompatibleSql {
        expected: SqlImplementation,
        actual: SqlImplementation,
    },
    IncompatibleFinalLogStore {
        expected: FinalLogStore,
        actual: FinalLogStore,
    },
    IncompatibleWriterTopology {
        expected: WriterTopology,
        actual: WriterTopology,
    },
    IncompatibleExecutorTopology {
        expected: ExecutorTopology,
        actual: ExecutorTopology,
    },
    IncompatibleHttpCommandIngress {
        expected: HttpCommandIngress,
        actual: HttpCommandIngress,
    },
    IncompatibleRole {
        role: CoordinationRole,
        expected: RoleImplementation,
        actual: RoleImplementation,
    },
    ExternalEventIngressEnabled {
        actual: RoleImplementation,
    },
    MissingProtocolIdentity {
        role: CoordinationRole,
    },
    UnsupportedProtocolIdentity {
        role: CoordinationRole,
        expected: ProtocolIdentity,
        actual: ProtocolIdentity,
    },
    MissingCapability {
        capability: ChoreographyCapability,
    },
}

impl fmt::Display for FormationAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompatibleTopology { expected, actual } => {
                write!(
                    formatter,
                    "formation requires {expected:?} topology, got {actual:?}"
                )
            }
            Self::IncompatibleSql { expected, actual } => {
                write!(
                    formatter,
                    "formation requires {expected:?} SQL, got {actual:?}"
                )
            }
            Self::IncompatibleFinalLogStore { expected, actual } => write!(
                formatter,
                "formation requires {expected:?} final logs, got {actual:?}"
            ),
            Self::IncompatibleWriterTopology { expected, actual } => write!(
                formatter,
                "formation requires {expected:?} writer topology, got {actual:?}"
            ),
            Self::IncompatibleExecutorTopology { expected, actual } => write!(
                formatter,
                "formation requires {expected:?} Executor topology, got {actual:?}"
            ),
            Self::IncompatibleHttpCommandIngress { expected, actual } => write!(
                formatter,
                "formation requires {expected:?} HTTP Command ingress, got {actual:?}"
            ),
            Self::IncompatibleRole {
                role,
                expected,
                actual,
            } => write!(
                formatter,
                "formation role {role:?} requires {expected:?}, got {actual:?}"
            ),
            Self::ExternalEventIngressEnabled { actual } => write!(
                formatter,
                "Tickr Lite External Event ingress must be disabled, got {actual:?}"
            ),
            Self::MissingProtocolIdentity { role } => {
                write!(
                    formatter,
                    "formation role {role:?} has no protocol identity"
                )
            }
            Self::UnsupportedProtocolIdentity {
                role,
                expected,
                actual,
            } => write!(
                formatter,
                "formation role {role:?} requires protocol {} v{}, got {} v{}",
                expected.name, expected.version, actual.name, actual.version
            ),
            Self::MissingCapability { capability } => {
                write!(formatter, "formation does not prove {capability:?}")
            }
        }
    }
}

impl Error for FormationAdmissionError {}

/// Expands and admits a formation without opening repositories, clients,
/// listeners, consumers, claims, loops, endpoints, or notification channels.
pub fn resolve_formation(
    selection: &FormationSelection,
) -> Result<ResolvedFormationDescriptor, FormationAdmissionError> {
    let profile = selection.profile.unwrap_or_default();
    let expected = profile_descriptor(profile);

    let topology = selection.topology.unwrap_or(expected.topology);
    require_equal_topology(expected.topology, topology)?;

    let sql = selection.sql.unwrap_or(expected.sql);
    require_equal_sql(expected.sql, sql)?;

    let final_logs = selection.final_logs.unwrap_or(expected.final_logs);
    require_equal_final_logs(expected.final_logs, final_logs)?;

    let writer_topology = selection
        .writer_topology
        .unwrap_or(expected.writer_topology);
    require_equal_writer_topology(expected.writer_topology, writer_topology)?;

    let executors = selection
        .executor_count
        .map_or(expected.executors, ExecutorTopology::Exactly);
    if executors != expected.executors {
        return Err(FormationAdmissionError::IncompatibleExecutorTopology {
            expected: expected.executors,
            actual: executors,
        });
    }

    let http_commands = selection.http_commands.unwrap_or(expected.http_commands);
    if http_commands != expected.http_commands {
        return Err(FormationAdmissionError::IncompatibleHttpCommandIngress {
            expected: expected.http_commands,
            actual: http_commands,
        });
    }

    let choreography = selection.choreography.unwrap_or(expected.choreography);
    for capability in [
        ChoreographyCapability::SafePickupHandoff,
        ChoreographyCapability::SafeAttemptOutcomeHandoff,
        ChoreographyCapability::SafeCancellationFence,
    ] {
        if !choreography.has(capability) {
            return Err(FormationAdmissionError::MissingCapability { capability });
        }
    }

    let mut resolved_roles = expected.roles.roles;
    for (index, role) in resolved_roles.iter_mut().enumerate() {
        *role = resolve_role(profile, *role, selection.role_overrides[index])?;
    }

    Ok(ResolvedFormationDescriptor {
        profile,
        topology,
        sql,
        sql_migration_identity: expected.sql_migration_identity,
        final_logs,
        writer_topology,
        executors,
        http_commands,
        roles: ResolvedCoordinationRoles {
            roles: resolved_roles,
        },
        choreography,
    })
}

fn resolve_role(
    profile: FormationProfile,
    expected: ResolvedRole,
    role_override: Option<RoleOverride>,
) -> Result<ResolvedRole, FormationAdmissionError> {
    let role_override = role_override.unwrap_or_default();
    let implementation = role_override
        .implementation
        .unwrap_or(expected.implementation);

    if implementation != expected.implementation {
        if profile == FormationProfile::LiteLocal && expected.role == CoordinationRole::EventIngress
        {
            return Err(FormationAdmissionError::ExternalEventIngressEnabled {
                actual: implementation,
            });
        }
        return Err(FormationAdmissionError::IncompatibleRole {
            role: expected.role,
            expected: expected.implementation,
            actual: implementation,
        });
    }

    let protocol = match role_override.protocol {
        ProtocolOverride::ProfileDefault => expected.protocol,
        ProtocolOverride::Missing => {
            return Err(FormationAdmissionError::MissingProtocolIdentity {
                role: expected.role,
            });
        }
        ProtocolOverride::Identity(actual) if actual != expected.protocol => {
            return Err(FormationAdmissionError::UnsupportedProtocolIdentity {
                role: expected.role,
                expected: expected.protocol,
                actual,
            });
        }
        ProtocolOverride::Identity(actual) => actual,
    };

    Ok(ResolvedRole {
        role: expected.role,
        implementation,
        protocol,
    })
}

fn require_equal_topology(
    expected: Topology,
    actual: Topology,
) -> Result<(), FormationAdmissionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(FormationAdmissionError::IncompatibleTopology { expected, actual })
    }
}

fn require_equal_sql(
    expected: SqlImplementation,
    actual: SqlImplementation,
) -> Result<(), FormationAdmissionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(FormationAdmissionError::IncompatibleSql { expected, actual })
    }
}

fn require_equal_final_logs(
    expected: FinalLogStore,
    actual: FinalLogStore,
) -> Result<(), FormationAdmissionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(FormationAdmissionError::IncompatibleFinalLogStore { expected, actual })
    }
}

fn require_equal_writer_topology(
    expected: WriterTopology,
    actual: WriterTopology,
) -> Result<(), FormationAdmissionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(FormationAdmissionError::IncompatibleWriterTopology { expected, actual })
    }
}

fn profile_descriptor(profile: FormationProfile) -> ResolvedFormationDescriptor {
    let (topology, sql, migration, final_logs, writer_topology, executors) = match profile {
        FormationProfile::AllNats => (
            Topology::Distributed,
            SqlImplementation::Postgres,
            ProtocolIdentity::new("tickr.postgres-migrations", 1),
            FinalLogStore::ObjectStore,
            WriterTopology::Distributed,
            ExecutorTopology::DistributedFleet,
        ),
        FormationProfile::LiteLocal => (
            Topology::SingleNode,
            SqlImplementation::Sqlite,
            ProtocolIdentity::new("tickr.sqlite-migrations", 1),
            FinalLogStore::LocalFiles,
            WriterTopology::ConductorOwned,
            ExecutorTopology::Exactly(1),
        ),
        FormationProfile::AllRedis => (
            Topology::Distributed,
            SqlImplementation::Postgres,
            ProtocolIdentity::new("tickr.postgres-migrations", 1),
            FinalLogStore::ObjectStore,
            WriterTopology::Distributed,
            ExecutorTopology::DistributedFleet,
        ),
    };

    ResolvedFormationDescriptor {
        profile,
        topology,
        sql,
        sql_migration_identity: migration,
        final_logs,
        writer_topology,
        executors,
        http_commands: HttpCommandIngress::Enabled,
        roles: profile_roles(profile),
        choreography: ChoreographyCapabilities::all(),
    }
}

fn profile_roles(profile: FormationProfile) -> ResolvedCoordinationRoles {
    ResolvedCoordinationRoles {
        roles: array::from_fn(|index| {
            let role = ALL_COORDINATION_ROLES[index];
            let implementation = match profile {
                FormationProfile::AllNats => RoleImplementation::NatsJetStream,
                FormationProfile::AllRedis => RoleImplementation::Redis,
                FormationProfile::LiteLocal => match role {
                    CoordinationRole::CommandBus => RoleImplementation::LocalRequestReply,
                    CoordinationRole::LogStaging => RoleImplementation::LocalJournal,
                    CoordinationRole::SignalAppliedNotifier => {
                        RoleImplementation::LocalNotification
                    }
                    CoordinationRole::ExecutorFleetStatus => RoleImplementation::LocalObservation,
                    CoordinationRole::IngressIdempotencyStore | CoordinationRole::EventIngress => {
                        RoleImplementation::Disabled
                    }
                    CoordinationRole::TaskDispatch
                    | CoordinationRole::TaskEvents
                    | CoordinationRole::TaskCancellation
                    | CoordinationRole::CompactionStaging
                    | CoordinationRole::LifecycleWork
                    | CoordinationRole::ScopeStore
                    | CoordinationRole::LivenessWatchdog => RoleImplementation::LocalSqlite,
                },
            };
            ResolvedRole {
                role,
                implementation,
                protocol: protocol_identity(profile, role),
            }
        }),
    }
}

fn protocol_identity(profile: FormationProfile, role: CoordinationRole) -> ProtocolIdentity {
    let name = match (profile, role) {
        (FormationProfile::AllNats, CoordinationRole::CommandBus) => {
            "tickr.all-nats.command-bus.nats-request-reply"
        }
        (FormationProfile::AllNats, CoordinationRole::TaskDispatch) => {
            "tickr.all-nats.task-dispatch.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::TaskEvents) => {
            "tickr.all-nats.task-events.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::TaskCancellation) => {
            "tickr.all-nats.task-cancellation.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::CompactionStaging) => {
            "tickr.all-nats.compaction-staging.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::LifecycleWork) => {
            "tickr.all-nats.lifecycle-work.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::LogStaging) => {
            "tickr.all-nats.log-staging.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::ScopeStore) => {
            "tickr.all-nats.scope-store.jetstream-kv"
        }
        (FormationProfile::AllNats, CoordinationRole::IngressIdempotencyStore) => {
            "tickr.all-nats.ingress-idempotency.jetstream-kv"
        }
        (FormationProfile::AllNats, CoordinationRole::LivenessWatchdog) => {
            "tickr.all-nats.liveness-watchdog.jetstream-kv"
        }
        (FormationProfile::AllNats, CoordinationRole::SignalAppliedNotifier) => {
            "tickr.all-nats.signal-applied.jetstream"
        }
        (FormationProfile::AllNats, CoordinationRole::ExecutorFleetStatus) => {
            "tickr.all-nats.executor-fleet-status.jetstream-kv"
        }
        (FormationProfile::AllNats, CoordinationRole::EventIngress) => {
            "tickr.all-nats.event-ingress.jetstream"
        }
        (FormationProfile::AllRedis, CoordinationRole::CommandBus) => {
            "tickr.command-bus.redis-request-reply"
        }
        (FormationProfile::AllRedis, CoordinationRole::TaskDispatch) => {
            "tickr.task-dispatch.redis-stream"
        }
        (FormationProfile::AllRedis, CoordinationRole::TaskEvents) => {
            "tickr.task-events.redis-stream"
        }
        (FormationProfile::AllRedis, CoordinationRole::TaskCancellation) => {
            "tickr.task-cancellation.redis-fence"
        }
        (FormationProfile::AllRedis, CoordinationRole::CompactionStaging) => {
            "tickr.compaction-staging.redis-stream"
        }
        (FormationProfile::AllRedis, CoordinationRole::LifecycleWork) => {
            "tickr.lifecycle-work.redis-advisory-notification"
        }
        (FormationProfile::AllRedis, CoordinationRole::LogStaging) => {
            "tickr.log-staging.redis-accepted-stream"
        }
        (FormationProfile::AllRedis, CoordinationRole::ScopeStore) => {
            "tickr.scope-store.redis-opaque-snapshot"
        }
        (FormationProfile::AllRedis, CoordinationRole::IngressIdempotencyStore) => {
            "tickr.ingress-idempotency.redis-lease"
        }
        (FormationProfile::AllRedis, CoordinationRole::LivenessWatchdog) => {
            "tickr.liveness-watchdog.redis-deadline-election"
        }
        (FormationProfile::AllRedis, CoordinationRole::SignalAppliedNotifier) => {
            "tickr.signal-applied.redis-pubsub"
        }
        (FormationProfile::AllRedis, CoordinationRole::ExecutorFleetStatus) => {
            "tickr.executor-fleet-status.redis-expiring-observation"
        }
        (FormationProfile::AllRedis, CoordinationRole::EventIngress) => {
            "tickr.event-ingress.redis-stream"
        }
        (FormationProfile::LiteLocal, CoordinationRole::CommandBus) => {
            "tickr.command-bus.local-request-reply"
        }
        (FormationProfile::LiteLocal, CoordinationRole::TaskDispatch) => {
            "tickr.task-dispatch.sqlite"
        }
        (FormationProfile::LiteLocal, CoordinationRole::TaskEvents) => "tickr.task-events.sqlite",
        (FormationProfile::LiteLocal, CoordinationRole::TaskCancellation) => {
            "tickr.task-cancellation.sqlite"
        }
        (FormationProfile::LiteLocal, CoordinationRole::CompactionStaging) => {
            "tickr.compaction-staging.sqlite"
        }
        (FormationProfile::LiteLocal, CoordinationRole::LifecycleWork) => {
            "tickr.lifecycle-work.sqlite"
        }
        (FormationProfile::LiteLocal, CoordinationRole::LogStaging) => {
            "tickr.log-staging.local-journal"
        }
        (FormationProfile::LiteLocal, CoordinationRole::ScopeStore) => "tickr.scope-store.sqlite",
        (FormationProfile::LiteLocal, CoordinationRole::IngressIdempotencyStore) => {
            "tickr.ingress-idempotency.disabled"
        }
        (FormationProfile::LiteLocal, CoordinationRole::LivenessWatchdog) => {
            "tickr.liveness-watchdog.sqlite"
        }
        (FormationProfile::LiteLocal, CoordinationRole::SignalAppliedNotifier) => {
            "tickr.signal-applied.local-notification"
        }
        (FormationProfile::LiteLocal, CoordinationRole::ExecutorFleetStatus) => {
            "tickr.executor-fleet-status.local-observation"
        }
        (FormationProfile::LiteLocal, CoordinationRole::EventIngress) => {
            "tickr.event-ingress.disabled"
        }
    };
    let version = if profile == FormationProfile::AllNats {
        tickr_proto::coord::all_nats::PROTOCOL_VERSION
    } else {
        1
    };
    ProtocolIdentity::new(name, version)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_distributed_descriptor(
        descriptor: &ResolvedFormationDescriptor,
        profile: FormationProfile,
        implementation: RoleImplementation,
    ) {
        assert_eq!(descriptor.profile, profile);
        assert_eq!(descriptor.topology, Topology::Distributed);
        assert_eq!(descriptor.sql, SqlImplementation::Postgres);
        assert_eq!(
            descriptor.sql_migration_identity,
            ProtocolIdentity::new("tickr.postgres-migrations", 1)
        );
        assert_eq!(descriptor.final_logs, FinalLogStore::ObjectStore);
        assert_eq!(descriptor.writer_topology, WriterTopology::Distributed);
        assert_eq!(descriptor.executors, ExecutorTopology::DistributedFleet);
        assert_eq!(descriptor.http_commands, HttpCommandIngress::Enabled);
        assert_eq!(descriptor.roles.iter().len(), ALL_COORDINATION_ROLES.len());
        assert!(descriptor
            .roles
            .iter()
            .all(|role| role.implementation == implementation));
        assert_eq!(descriptor.choreography, ChoreographyCapabilities::all());
    }

    #[test]
    fn profile_names_and_omission_are_stable() {
        assert_eq!(FormationProfile::AllNats.name(), "all-nats");
        assert_eq!(FormationProfile::LiteLocal.name(), "lite-local");
        assert_eq!(FormationProfile::AllRedis.name(), "all-redis");

        let omitted = resolve_formation(&FormationSelection::default()).unwrap();
        let explicit = resolve_formation(&FormationSelection::all_nats()).unwrap();

        assert_eq!(omitted, explicit);
        assert_eq!(omitted.profile, FormationProfile::AllNats);
    }

    #[test]
    fn named_profiles_expand_to_complete_exact_descriptors() {
        let all_nats = resolve_formation(&FormationSelection::all_nats()).unwrap();
        assert_distributed_descriptor(
            &all_nats,
            FormationProfile::AllNats,
            RoleImplementation::NatsJetStream,
        );

        let lite_local = resolve_formation(&FormationSelection::lite_local()).unwrap();
        assert_eq!(lite_local.profile, FormationProfile::LiteLocal);
        assert_eq!(lite_local.topology, Topology::SingleNode);
        assert_eq!(lite_local.sql, SqlImplementation::Sqlite);
        assert_eq!(lite_local.final_logs, FinalLogStore::LocalFiles);
        assert_eq!(lite_local.writer_topology, WriterTopology::ConductorOwned);
        assert_eq!(lite_local.executors, ExecutorTopology::Exactly(1));
        assert_eq!(lite_local.http_commands, HttpCommandIngress::Enabled);
        assert_eq!(
            lite_local
                .roles
                .get(CoordinationRole::CommandBus)
                .implementation,
            RoleImplementation::LocalRequestReply
        );
        assert_eq!(
            lite_local
                .roles
                .get(CoordinationRole::EventIngress)
                .implementation,
            RoleImplementation::Disabled
        );
        assert_eq!(lite_local.choreography, ChoreographyCapabilities::all());

        let all_redis = resolve_formation(&FormationSelection::all_redis()).unwrap();
        assert_distributed_descriptor(
            &all_redis,
            FormationProfile::AllRedis,
            RoleImplementation::Redis,
        );
    }

    #[test]
    fn all_nats_protocol_identities_are_fresh_exact_and_versioned_together() {
        let descriptor = resolve_formation(&FormationSelection::all_nats()).unwrap();
        let expected = [
            "tickr.all-nats.command-bus.nats-request-reply",
            "tickr.all-nats.task-dispatch.jetstream",
            "tickr.all-nats.task-events.jetstream",
            "tickr.all-nats.task-cancellation.jetstream",
            "tickr.all-nats.compaction-staging.jetstream",
            "tickr.all-nats.lifecycle-work.jetstream",
            "tickr.all-nats.log-staging.jetstream",
            "tickr.all-nats.scope-store.jetstream-kv",
            "tickr.all-nats.ingress-idempotency.jetstream-kv",
            "tickr.all-nats.liveness-watchdog.jetstream-kv",
            "tickr.all-nats.signal-applied.jetstream",
            "tickr.all-nats.executor-fleet-status.jetstream-kv",
            "tickr.all-nats.event-ingress.jetstream",
        ];

        for (resolved, expected_name) in descriptor.roles.iter().zip(expected) {
            assert_eq!(
                resolved.protocol,
                ProtocolIdentity::new(
                    expected_name,
                    tickr_proto::coord::all_nats::PROTOCOL_VERSION
                )
            );
            assert!(resolved.protocol.name.starts_with("tickr.all-nats."));
        }
    }

    #[test]
    fn all_redis_protocol_identities_are_exact_stable_and_secret_free() {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        let expected = [
            "tickr.command-bus.redis-request-reply",
            "tickr.task-dispatch.redis-stream",
            "tickr.task-events.redis-stream",
            "tickr.task-cancellation.redis-fence",
            "tickr.compaction-staging.redis-stream",
            "tickr.lifecycle-work.redis-advisory-notification",
            "tickr.log-staging.redis-accepted-stream",
            "tickr.scope-store.redis-opaque-snapshot",
            "tickr.ingress-idempotency.redis-lease",
            "tickr.liveness-watchdog.redis-deadline-election",
            "tickr.signal-applied.redis-pubsub",
            "tickr.executor-fleet-status.redis-expiring-observation",
            "tickr.event-ingress.redis-stream",
        ];

        for (resolved, expected_name) in descriptor.roles.iter().zip(expected) {
            assert_eq!(resolved.protocol, ProtocolIdentity::new(expected_name, 1));
            assert!(!resolved.protocol.name.contains("://"));
            assert!(!resolved.protocol.name.contains('@'));
            assert!(!resolved.protocol.name.contains("password"));
            assert!(!resolved.protocol.name.contains("certificate"));
        }
    }

    #[test]
    fn all_redis_rejects_every_non_role_descriptor_override_class() {
        let cases = [
            (
                FormationSelection {
                    topology: Some(Topology::SingleNode),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleTopology {
                    expected: Topology::Distributed,
                    actual: Topology::SingleNode,
                },
            ),
            (
                FormationSelection {
                    sql: Some(SqlImplementation::Sqlite),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleSql {
                    expected: SqlImplementation::Postgres,
                    actual: SqlImplementation::Sqlite,
                },
            ),
            (
                FormationSelection {
                    final_logs: Some(FinalLogStore::LocalFiles),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleFinalLogStore {
                    expected: FinalLogStore::ObjectStore,
                    actual: FinalLogStore::LocalFiles,
                },
            ),
            (
                FormationSelection {
                    writer_topology: Some(WriterTopology::ConductorOwned),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleWriterTopology {
                    expected: WriterTopology::Distributed,
                    actual: WriterTopology::ConductorOwned,
                },
            ),
            (
                FormationSelection {
                    executor_count: Some(1),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleExecutorTopology {
                    expected: ExecutorTopology::DistributedFleet,
                    actual: ExecutorTopology::Exactly(1),
                },
            ),
            (
                FormationSelection {
                    http_commands: Some(HttpCommandIngress::Disabled),
                    ..FormationSelection::all_redis()
                },
                FormationAdmissionError::IncompatibleHttpCommandIngress {
                    expected: HttpCommandIngress::Enabled,
                    actual: HttpCommandIngress::Disabled,
                },
            ),
        ];

        for (selection, expected) in cases {
            assert_eq!(resolve_formation(&selection), Err(expected));
        }
    }

    #[test]
    fn all_redis_rejects_every_role_and_protocol_departure() {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();

        for role in ALL_COORDINATION_ROLES {
            let mixed = FormationSelection::all_redis().with_role_override(
                role,
                RoleOverride {
                    implementation: Some(RoleImplementation::NatsJetStream),
                    ..RoleOverride::default()
                },
            );
            assert_eq!(
                resolve_formation(&mixed),
                Err(FormationAdmissionError::IncompatibleRole {
                    role,
                    expected: RoleImplementation::Redis,
                    actual: RoleImplementation::NatsJetStream,
                })
            );

            let missing = FormationSelection::all_redis().with_role_override(
                role,
                RoleOverride {
                    protocol: ProtocolOverride::Missing,
                    ..RoleOverride::default()
                },
            );
            assert_eq!(
                resolve_formation(&missing),
                Err(FormationAdmissionError::MissingProtocolIdentity { role })
            );

            let expected = *descriptor.roles.get(role);
            let unknown =
                ProtocolIdentity::new(expected.protocol.name, expected.protocol.version + 1);
            let unsupported = FormationSelection::all_redis().with_role_override(
                role,
                RoleOverride {
                    protocol: ProtocolOverride::Identity(unknown),
                    ..RoleOverride::default()
                },
            );
            assert_eq!(
                resolve_formation(&unsupported),
                Err(FormationAdmissionError::UnsupportedProtocolIdentity {
                    role,
                    expected: expected.protocol,
                    actual: unknown,
                })
            );

            let exact = FormationSelection::all_redis().with_role_override(
                role,
                RoleOverride {
                    implementation: Some(expected.implementation),
                    protocol: ProtocolOverride::Identity(expected.protocol),
                },
            );
            assert_eq!(resolve_formation(&exact), Ok(descriptor));
        }
    }

    #[test]
    fn all_redis_rejects_each_missing_choreography_proof() {
        let cases = [
            (
                ChoreographyCapabilities {
                    safe_pickup_handoff: false,
                    ..ChoreographyCapabilities::all()
                },
                ChoreographyCapability::SafePickupHandoff,
            ),
            (
                ChoreographyCapabilities {
                    safe_attempt_outcome_handoff: false,
                    ..ChoreographyCapabilities::all()
                },
                ChoreographyCapability::SafeAttemptOutcomeHandoff,
            ),
            (
                ChoreographyCapabilities {
                    safe_cancellation_fence: false,
                    ..ChoreographyCapabilities::all()
                },
                ChoreographyCapability::SafeCancellationFence,
            ),
        ];

        for (choreography, capability) in cases {
            let selection = FormationSelection {
                choreography: Some(choreography),
                ..FormationSelection::all_redis()
            };
            assert_eq!(
                resolve_formation(&selection),
                Err(FormationAdmissionError::MissingCapability { capability })
            );
        }
    }

    #[test]
    fn lite_local_runs_disabled_role_admission_laws_for_both_ingress_roles() {
        let descriptor = resolve_formation(&FormationSelection::lite_local()).unwrap();
        let disabled = [
            (
                CoordinationRole::IngressIdempotencyStore,
                "tickr.ingress-idempotency.disabled",
            ),
            (
                CoordinationRole::EventIngress,
                "tickr.event-ingress.disabled",
            ),
        ];

        for (role, protocol_name) in disabled {
            let resolved = descriptor.roles.get(role);
            assert_eq!(resolved.implementation, RoleImplementation::Disabled);
            assert_eq!(resolved.protocol, ProtocolIdentity::new(protocol_name, 1));

            let selection = FormationSelection::lite_local().with_role_override(
                role,
                RoleOverride {
                    implementation: Some(RoleImplementation::Redis),
                    ..RoleOverride::default()
                },
            );
            let error = resolve_formation(&selection).unwrap_err();
            match role {
                CoordinationRole::IngressIdempotencyStore => assert_eq!(
                    error,
                    FormationAdmissionError::IncompatibleRole {
                        role,
                        expected: RoleImplementation::Disabled,
                        actual: RoleImplementation::Redis,
                    }
                ),
                CoordinationRole::EventIngress => assert_eq!(
                    error,
                    FormationAdmissionError::ExternalEventIngressEnabled {
                        actual: RoleImplementation::Redis,
                    }
                ),
                _ => unreachable!("only disabled ingress roles are exercised"),
            }
        }
    }
}
