use std::{array, error::Error, fmt};

const COORDINATION_ROLE_COUNT: usize = 13;

/// A named Data-plane formation profile.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FormationProfile {
    /// The existing independently deployed component formation.
    #[default]
    Distributed,
    /// The explicit single-node Tickr Lite formation.
    TickrLite,
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
            role_overrides: [None; COORDINATION_ROLE_COUNT],
        }
    }
}

impl FormationSelection {
    pub fn tickr_lite() -> Self {
        Self {
            profile: Some(FormationProfile::TickrLite),
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
    InvalidExecutorCount {
        expected: u16,
        actual: u16,
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
            Self::InvalidExecutorCount { expected, actual } => write!(
                formatter,
                "formation requires exactly {expected} Executor, got {actual}"
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

    let sql = selection.sql.unwrap_or(expected.sql);
    if profile == FormationProfile::TickrLite {
        require_equal_sql(expected.sql, sql)?;
    }

    let expected_topology = match (profile, sql) {
        (FormationProfile::Distributed, SqlImplementation::Sqlite) => Topology::SingleNode,
        _ => expected.topology,
    };
    let topology = selection.topology.unwrap_or(expected_topology);
    require_equal_topology(expected_topology, topology)?;

    let final_logs = selection.final_logs.unwrap_or(expected.final_logs);
    require_equal_final_logs(expected.final_logs, final_logs)?;

    let expected_writer_topology = match (profile, sql) {
        (FormationProfile::Distributed, SqlImplementation::Sqlite) => {
            WriterTopology::ConductorOwned
        }
        _ => expected.writer_topology,
    };
    let writer_topology = selection
        .writer_topology
        .unwrap_or(expected_writer_topology);
    require_equal_writer_topology(expected_writer_topology, writer_topology)?;

    let executors = match expected.executors {
        ExecutorTopology::DistributedFleet => match selection.executor_count {
            Some(count) => ExecutorTopology::Exactly(count),
            None => ExecutorTopology::DistributedFleet,
        },
        ExecutorTopology::Exactly(expected_count) => {
            let actual = selection.executor_count.unwrap_or(expected_count);
            if actual != expected_count {
                return Err(FormationAdmissionError::InvalidExecutorCount {
                    expected: expected_count,
                    actual,
                });
            }
            ExecutorTopology::Exactly(actual)
        }
    };

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
    let roles = ResolvedCoordinationRoles {
        roles: resolved_roles,
    };

    Ok(ResolvedFormationDescriptor {
        profile,
        topology,
        sql,
        sql_migration_identity: match sql {
            SqlImplementation::Postgres => ProtocolIdentity::new("tickr.postgres-migrations", 1),
            SqlImplementation::Sqlite => ProtocolIdentity::new("tickr.sqlite-migrations", 1),
        },
        final_logs,
        writer_topology,
        executors,
        http_commands: HttpCommandIngress::Enabled,
        roles,
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
        if profile == FormationProfile::TickrLite && expected.role == CoordinationRole::EventIngress
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
        FormationProfile::Distributed => (
            Topology::Distributed,
            SqlImplementation::Postgres,
            ProtocolIdentity::new("tickr.postgres-migrations", 1),
            FinalLogStore::ObjectStore,
            WriterTopology::Distributed,
            ExecutorTopology::DistributedFleet,
        ),
        FormationProfile::TickrLite => (
            Topology::SingleNode,
            SqlImplementation::Sqlite,
            ProtocolIdentity::new("tickr.sqlite-migrations", 1),
            FinalLogStore::LocalFiles,
            WriterTopology::ConductorOwned,
            ExecutorTopology::Exactly(1),
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
                FormationProfile::Distributed => RoleImplementation::NatsJetStream,
                FormationProfile::TickrLite => match role {
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
        (FormationProfile::Distributed, CoordinationRole::CommandBus) => {
            "tickr.command-bus.nats-request-reply"
        }
        (FormationProfile::Distributed, CoordinationRole::TaskDispatch) => {
            "tickr.task-dispatch.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::TaskEvents) => {
            "tickr.task-events.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::TaskCancellation) => {
            "tickr.task-cancellation.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::CompactionStaging) => {
            "tickr.compaction-staging.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::LifecycleWork) => {
            "tickr.lifecycle-work.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::LogStaging) => {
            "tickr.log-staging.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::ScopeStore) => {
            "tickr.scope-store.jetstream-kv"
        }
        (FormationProfile::Distributed, CoordinationRole::IngressIdempotencyStore) => {
            "tickr.ingress-idempotency.jetstream-kv"
        }
        (FormationProfile::Distributed, CoordinationRole::LivenessWatchdog) => {
            "tickr.liveness-watchdog.jetstream-kv"
        }
        (FormationProfile::Distributed, CoordinationRole::SignalAppliedNotifier) => {
            "tickr.signal-applied.jetstream"
        }
        (FormationProfile::Distributed, CoordinationRole::ExecutorFleetStatus) => {
            "tickr.executor-fleet-status.jetstream-kv"
        }
        (FormationProfile::Distributed, CoordinationRole::EventIngress) => {
            "tickr.event-ingress.jetstream"
        }
        (FormationProfile::TickrLite, CoordinationRole::CommandBus) => {
            "tickr.command-bus.local-request-reply"
        }
        (FormationProfile::TickrLite, CoordinationRole::TaskDispatch) => {
            "tickr.task-dispatch.sqlite"
        }
        (FormationProfile::TickrLite, CoordinationRole::TaskEvents) => "tickr.task-events.sqlite",
        (FormationProfile::TickrLite, CoordinationRole::TaskCancellation) => {
            "tickr.task-cancellation.sqlite"
        }
        (FormationProfile::TickrLite, CoordinationRole::CompactionStaging) => {
            "tickr.compaction-staging.sqlite"
        }
        (FormationProfile::TickrLite, CoordinationRole::LifecycleWork) => {
            "tickr.lifecycle-work.sqlite"
        }
        (FormationProfile::TickrLite, CoordinationRole::LogStaging) => {
            "tickr.log-staging.local-journal"
        }
        (FormationProfile::TickrLite, CoordinationRole::ScopeStore) => "tickr.scope-store.sqlite",
        (FormationProfile::TickrLite, CoordinationRole::IngressIdempotencyStore) => {
            "tickr.ingress-idempotency.disabled"
        }
        (FormationProfile::TickrLite, CoordinationRole::LivenessWatchdog) => {
            "tickr.liveness-watchdog.sqlite"
        }
        (FormationProfile::TickrLite, CoordinationRole::SignalAppliedNotifier) => {
            "tickr.signal-applied.local-notification"
        }
        (FormationProfile::TickrLite, CoordinationRole::ExecutorFleetStatus) => {
            "tickr.executor-fleet-status.local-observation"
        }
        (FormationProfile::TickrLite, CoordinationRole::EventIngress) => {
            "tickr.event-ingress.disabled"
        }
    };
    ProtocolIdentity::new(name, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omission_preserves_the_distributed_formation() {
        let descriptor = resolve_formation(&FormationSelection::default()).unwrap();

        assert_eq!(descriptor.profile, FormationProfile::Distributed);
        assert_eq!(descriptor.topology, Topology::Distributed);
        assert_eq!(descriptor.sql, SqlImplementation::Postgres);
        assert_eq!(descriptor.final_logs, FinalLogStore::ObjectStore);
        assert_eq!(descriptor.executors, ExecutorTopology::DistributedFleet);
        assert_eq!(descriptor.http_commands, HttpCommandIngress::Enabled);
        assert!(descriptor
            .roles
            .iter()
            .all(|role| role.implementation == RoleImplementation::NatsJetStream));
    }

    #[test]
    fn distributed_coordination_with_sqlite_remains_admissible() {
        let selection = FormationSelection {
            topology: Some(Topology::SingleNode),
            sql: Some(SqlImplementation::Sqlite),
            writer_topology: Some(WriterTopology::ConductorOwned),
            ..FormationSelection::default()
        };

        let descriptor = resolve_formation(&selection).unwrap();

        assert_eq!(descriptor.profile, FormationProfile::Distributed);
        assert_eq!(descriptor.topology, Topology::SingleNode);
        assert_eq!(descriptor.sql, SqlImplementation::Sqlite);
        assert_eq!(
            descriptor.sql_migration_identity,
            ProtocolIdentity::new("tickr.sqlite-migrations", 1)
        );
        assert_eq!(descriptor.writer_topology, WriterTopology::ConductorOwned);
        assert!(descriptor
            .roles
            .iter()
            .all(|role| role.implementation == RoleImplementation::NatsJetStream));
    }

    #[test]
    fn tickr_lite_resolves_the_complete_local_descriptor() {
        let descriptor = resolve_formation(&FormationSelection::tickr_lite()).unwrap();

        assert_eq!(descriptor.profile, FormationProfile::TickrLite);
        assert_eq!(descriptor.topology, Topology::SingleNode);
        assert_eq!(descriptor.sql, SqlImplementation::Sqlite);
        assert_eq!(descriptor.final_logs, FinalLogStore::LocalFiles);
        assert_eq!(descriptor.writer_topology, WriterTopology::ConductorOwned);
        assert_eq!(descriptor.executors, ExecutorTopology::Exactly(1));
        assert_eq!(descriptor.http_commands, HttpCommandIngress::Enabled);
        assert_eq!(descriptor.roles.iter().len(), ALL_COORDINATION_ROLES.len());
        assert!(descriptor
            .roles
            .iter()
            .all(|role| !role.protocol.name.is_empty() && role.protocol.version > 0));
        assert_eq!(
            descriptor
                .roles
                .get(CoordinationRole::CommandBus)
                .implementation,
            RoleImplementation::LocalRequestReply
        );
        assert_eq!(
            descriptor
                .roles
                .get(CoordinationRole::EventIngress)
                .implementation,
            RoleImplementation::Disabled
        );
        assert_eq!(descriptor.choreography, ChoreographyCapabilities::all());
    }

    #[test]
    fn tickr_lite_rejects_incompatible_substrates_and_topology() {
        let cases = [
            (
                FormationSelection {
                    topology: Some(Topology::Distributed),
                    ..FormationSelection::tickr_lite()
                },
                FormationAdmissionError::IncompatibleTopology {
                    expected: Topology::SingleNode,
                    actual: Topology::Distributed,
                },
            ),
            (
                FormationSelection {
                    sql: Some(SqlImplementation::Postgres),
                    ..FormationSelection::tickr_lite()
                },
                FormationAdmissionError::IncompatibleSql {
                    expected: SqlImplementation::Sqlite,
                    actual: SqlImplementation::Postgres,
                },
            ),
            (
                FormationSelection {
                    final_logs: Some(FinalLogStore::ObjectStore),
                    ..FormationSelection::tickr_lite()
                },
                FormationAdmissionError::IncompatibleFinalLogStore {
                    expected: FinalLogStore::LocalFiles,
                    actual: FinalLogStore::ObjectStore,
                },
            ),
            (
                FormationSelection {
                    writer_topology: Some(WriterTopology::Distributed),
                    ..FormationSelection::tickr_lite()
                },
                FormationAdmissionError::IncompatibleWriterTopology {
                    expected: WriterTopology::ConductorOwned,
                    actual: WriterTopology::Distributed,
                },
            ),
            (
                FormationSelection {
                    executor_count: Some(2),
                    ..FormationSelection::tickr_lite()
                },
                FormationAdmissionError::InvalidExecutorCount {
                    expected: 1,
                    actual: 2,
                },
            ),
        ];

        for (selection, expected) in cases {
            assert_eq!(resolve_formation(&selection), Err(expected));
        }
    }

    #[test]
    fn tickr_lite_rejects_nats_redis_mixed_roles_and_event_ingress() {
        let cases = [
            (
                CoordinationRole::TaskDispatch,
                RoleImplementation::NatsJetStream,
                FormationAdmissionError::IncompatibleRole {
                    role: CoordinationRole::TaskDispatch,
                    expected: RoleImplementation::LocalSqlite,
                    actual: RoleImplementation::NatsJetStream,
                },
            ),
            (
                CoordinationRole::ScopeStore,
                RoleImplementation::Redis,
                FormationAdmissionError::IncompatibleRole {
                    role: CoordinationRole::ScopeStore,
                    expected: RoleImplementation::LocalSqlite,
                    actual: RoleImplementation::Redis,
                },
            ),
            (
                CoordinationRole::EventIngress,
                RoleImplementation::NatsJetStream,
                FormationAdmissionError::ExternalEventIngressEnabled {
                    actual: RoleImplementation::NatsJetStream,
                },
            ),
        ];

        for (role, implementation, expected) in cases {
            let selection = FormationSelection::tickr_lite().with_role_override(
                role,
                RoleOverride {
                    implementation: Some(implementation),
                    ..RoleOverride::default()
                },
            );
            assert_eq!(resolve_formation(&selection), Err(expected));
        }
    }

    #[test]
    fn formation_rejects_missing_and_unknown_protocol_identities() {
        let missing = FormationSelection::tickr_lite().with_role_override(
            CoordinationRole::TaskEvents,
            RoleOverride {
                protocol: ProtocolOverride::Missing,
                ..RoleOverride::default()
            },
        );
        assert_eq!(
            resolve_formation(&missing),
            Err(FormationAdmissionError::MissingProtocolIdentity {
                role: CoordinationRole::TaskEvents,
            })
        );

        let unknown = ProtocolIdentity::new("tickr.task-events.sqlite", 2);
        let selection = FormationSelection::tickr_lite().with_role_override(
            CoordinationRole::TaskEvents,
            RoleOverride {
                protocol: ProtocolOverride::Identity(unknown),
                ..RoleOverride::default()
            },
        );
        assert!(matches!(
            resolve_formation(&selection),
            Err(FormationAdmissionError::UnsupportedProtocolIdentity {
                role: CoordinationRole::TaskEvents,
                actual,
                ..
            }) if actual == unknown
        ));
    }

    #[test]
    fn formation_rejects_each_missing_choreography_proof() {
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
                ..FormationSelection::tickr_lite()
            };
            assert_eq!(
                resolve_formation(&selection),
                Err(FormationAdmissionError::MissingCapability { capability })
            );
        }
    }
}
