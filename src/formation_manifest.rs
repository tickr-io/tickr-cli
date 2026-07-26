use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::data_directory::{
    AdmittedFilesystem, AdmittedPlatform, DataDirectory, DataDirectoryError,
    DurableReplaceBoundary, FormationPath, RootRelativePath,
};
use crate::formation::{
    CoordinationRole, ExecutorTopology, FinalLogStore, FormationProfile, HttpCommandIngress,
    ResolvedFormationDescriptor, RoleImplementation, SqlImplementation, Topology, WriterTopology,
};

const MANIFEST_VERSION: u16 = 1;
const CHECKSUM_ALGORITHM: &str = "sha256";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The verified logical migration set represented by the SQLite schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlMigrationSetIdentity {
    protocol_name: String,
    protocol_version: u16,
    latest_logical_name: String,
    latest_logical_version: i64,
}

impl SqlMigrationSetIdentity {
    pub fn new(
        protocol_name: impl Into<String>,
        protocol_version: u16,
        latest_logical_name: impl Into<String>,
        latest_logical_version: i64,
    ) -> Self {
        Self {
            protocol_name: protocol_name.into(),
            protocol_version,
            latest_logical_name: latest_logical_name.into(),
            latest_logical_version,
        }
    }
}

/// Canonical inputs whose behavior must not change across a restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormationManifestSpec {
    descriptor: DescriptorRecord,
    normalized_configuration: BTreeMap<String, String>,
    sql_migration: SqlMigrationSetIdentity,
    file_formats: BTreeMap<String, u16>,
    namespaces: BTreeMap<String, String>,
    required_files: Vec<String>,
}

impl FormationManifestSpec {
    pub fn new(
        descriptor: &ResolvedFormationDescriptor,
        sql_migration: SqlMigrationSetIdentity,
        normalized_configuration: BTreeMap<String, String>,
        file_formats: BTreeMap<String, u16>,
        namespaces: BTreeMap<String, String>,
        required_files: Vec<RootRelativePath>,
    ) -> Result<Self, FormationManifestError> {
        let expected_sql = &descriptor.sql_migration_identity;
        if sql_migration.protocol_name != expected_sql.name
            || sql_migration.protocol_version != expected_sql.version
        {
            return Err(FormationManifestError::SchemaDisagreement {
                expected: format!("{}@{}", expected_sql.name, expected_sql.version),
                actual: format!(
                    "{}@{}",
                    sql_migration.protocol_name, sql_migration.protocol_version
                ),
            });
        }
        validate_string_map("configuration", &normalized_configuration)?;
        validate_version_map(&file_formats)?;
        validate_string_map("namespace", &namespaces)?;
        if sql_migration.latest_logical_name.is_empty() || sql_migration.latest_logical_version <= 0
        {
            return Err(FormationManifestError::InvalidSpec(
                "SQL logical migration identity must have a non-empty name and positive version"
                    .to_owned(),
            ));
        }

        let manifest_path = FormationPath::FormationManifest.relative_path();
        let mut required_files = required_files
            .into_iter()
            .map(|path| {
                if path.as_path() == manifest_path {
                    return Err(FormationManifestError::InvalidSpec(
                        "the manifest cannot require itself".to_owned(),
                    ));
                }
                path.as_path().to_str().map(str::to_owned).ok_or_else(|| {
                    FormationManifestError::InvalidSpec(
                        "required file paths must be valid UTF-8".to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        required_files.sort();
        required_files.dedup();
        if required_files.is_empty() {
            return Err(FormationManifestError::InvalidSpec(
                "at least one durable state file must be required".to_owned(),
            ));
        }

        Ok(Self {
            descriptor: DescriptorRecord::from(descriptor),
            normalized_configuration,
            sql_migration,
            file_formats,
            namespaces,
            required_files,
        })
    }
}

/// Evidence supplied by the durable-state owner, not inferred by the manifest layer.
pub trait EmptyReconstructibleFrontier {
    fn prove_empty_and_reconstructible(&self) -> Result<(), String>;
}

/// The only contexts allowed to install a missing or changed manifest.
pub enum ManifestAdmission<'a> {
    Runtime,
    OfflineMigration,
    EmptyReconstructible(&'a dyn EmptyReconstructibleFrontier),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestStatus {
    Installed,
    Verified,
    Replaced,
}

#[derive(Debug)]
pub enum FormationManifestError {
    Storage(DataDirectoryError),
    Io {
        operation: &'static str,
        source: io::Error,
    },
    InvalidSpec(String),
    MissingManifest,
    ManifestTooLarge(u64),
    InvalidEncoding(String),
    UnknownManifestVersion(u16),
    UnknownChecksumAlgorithm(String),
    ChecksumFailure,
    FingerprintFailure,
    DataDirectoryIdentityMismatch,
    SchemaDisagreement {
        expected: String,
        actual: String,
    },
    ProtocolIdentityMismatch(String),
    FileFormatIdentityMismatch,
    NamespaceIdentityMismatch,
    RequiredFileSetMismatch,
    ChangedFingerprint,
    FrontierNotEmpty(String),
}

impl fmt::Display for FormationManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "formation manifest storage: {error}"),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::InvalidSpec(message) => write!(formatter, "invalid formation manifest spec: {message}"),
            Self::MissingManifest => formatter.write_str(
                "formation manifest is missing; run an offline migration or prove an empty reconstructible frontier",
            ),
            Self::ManifestTooLarge(size) => {
                write!(formatter, "formation manifest is {size} bytes; maximum is {MAX_MANIFEST_BYTES}")
            }
            Self::InvalidEncoding(message) => write!(formatter, "invalid formation manifest: {message}"),
            Self::UnknownManifestVersion(version) => {
                write!(formatter, "unknown formation manifest version {version}")
            }
            Self::UnknownChecksumAlgorithm(algorithm) => {
                write!(formatter, "unknown formation manifest checksum algorithm `{algorithm}`")
            }
            Self::ChecksumFailure => formatter.write_str("formation manifest checksum verification failed"),
            Self::FingerprintFailure => {
                formatter.write_str("formation manifest fingerprint verification failed")
            }
            Self::DataDirectoryIdentityMismatch => {
                formatter.write_str("formation manifest belongs to a different data directory")
            }
            Self::SchemaDisagreement { expected, actual } => write!(
                formatter,
                "SQLite migration identity disagrees: expected {expected}, found {actual}"
            ),
            Self::ProtocolIdentityMismatch(role) => {
                write!(formatter, "unknown or changed protocol identity for role `{role}`")
            }
            Self::FileFormatIdentityMismatch => {
                formatter.write_str("unknown or changed formation file-format identity")
            }
            Self::NamespaceIdentityMismatch => {
                formatter.write_str("unknown or changed formation namespace identity")
            }
            Self::RequiredFileSetMismatch => {
                formatter.write_str("formation required-file set disagrees with the resolved formation")
            }
            Self::ChangedFingerprint => formatter.write_str(
                "formation fingerprint changed; run an offline migration or prove an empty reconstructible frontier",
            ),
            Self::FrontierNotEmpty(message) => {
                write!(formatter, "durable frontier is not empty and reconstructible: {message}")
            }
        }
    }
}

impl Error for FormationManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<DataDirectoryError> for FormationManifestError {
    fn from(value: DataDirectoryError) -> Self {
        Self::Storage(value)
    }
}

/// Installs the first manifest or verifies an existing one before runtime side effects.
pub fn install_or_verify_formation_manifest(
    data_directory: &DataDirectory,
    spec: &FormationManifestSpec,
    admission: ManifestAdmission<'_>,
) -> Result<ManifestStatus, FormationManifestError> {
    validate_required_files(data_directory, &spec.required_files)?;
    let expected = expected_payload(data_directory, spec)?;
    let Some(stored) = read_manifest(data_directory)? else {
        match admission {
            ManifestAdmission::Runtime => return Err(FormationManifestError::MissingManifest),
            other => authorize_change(other)?,
        }
        install_manifest(data_directory, &expected)?;
        return Ok(ManifestStatus::Installed);
    };

    validate_stored_manifest(data_directory, spec, &stored)?;
    if stored.payload.fingerprint == expected.fingerprint {
        return Ok(ManifestStatus::Verified);
    }

    authorize_change(admission)?;
    install_manifest(data_directory, &expected)?;
    Ok(ManifestStatus::Replaced)
}

fn authorize_change(admission: ManifestAdmission<'_>) -> Result<(), FormationManifestError> {
    match admission {
        ManifestAdmission::Runtime => Err(FormationManifestError::ChangedFingerprint),
        ManifestAdmission::OfflineMigration => Ok(()),
        ManifestAdmission::EmptyReconstructible(proof) => proof
            .prove_empty_and_reconstructible()
            .map_err(FormationManifestError::FrontierNotEmpty),
    }
}

fn validate_stored_manifest(
    data_directory: &DataDirectory,
    spec: &FormationManifestSpec,
    stored: &StoredManifest,
) -> Result<(), FormationManifestError> {
    if stored.version != MANIFEST_VERSION {
        return Err(FormationManifestError::UnknownManifestVersion(
            stored.version,
        ));
    }
    if stored.checksum_algorithm != CHECKSUM_ALGORITHM {
        return Err(FormationManifestError::UnknownChecksumAlgorithm(
            stored.checksum_algorithm.clone(),
        ));
    }
    let payload_bytes = serde_json::to_vec(&stored.payload)
        .map_err(|error| FormationManifestError::InvalidEncoding(error.to_string()))?;
    if stored.checksum != checksum(&payload_bytes) {
        return Err(FormationManifestError::ChecksumFailure);
    }
    if stored.payload.fingerprint != fingerprint(&stored.payload)? {
        return Err(FormationManifestError::FingerprintFailure);
    }

    let expected = expected_payload(data_directory, spec)?;
    if stored.payload.data_directory != expected.data_directory {
        return Err(FormationManifestError::DataDirectoryIdentityMismatch);
    }
    if stored.payload.sql_migration != expected.sql_migration {
        return Err(FormationManifestError::SchemaDisagreement {
            expected: sql_identity_label(&expected.sql_migration),
            actual: sql_identity_label(&stored.payload.sql_migration),
        });
    }
    validate_role_protocols(&stored.payload.descriptor, &expected.descriptor)?;
    if stored.payload.file_formats != expected.file_formats {
        return Err(FormationManifestError::FileFormatIdentityMismatch);
    }
    if stored.payload.namespaces != expected.namespaces {
        return Err(FormationManifestError::NamespaceIdentityMismatch);
    }
    if stored.payload.required_files != expected.required_files {
        return Err(FormationManifestError::RequiredFileSetMismatch);
    }
    validate_required_files(data_directory, &stored.payload.required_files)
}

fn validate_role_protocols(
    stored: &DescriptorRecord,
    expected: &DescriptorRecord,
) -> Result<(), FormationManifestError> {
    if stored.profile != expected.profile || stored.roles.len() != expected.roles.len() {
        return Err(FormationManifestError::ProtocolIdentityMismatch(
            "formation-profile".to_owned(),
        ));
    }
    for expected_role in &expected.roles {
        let Some(stored_role) = stored
            .roles
            .iter()
            .find(|role| role.role == expected_role.role)
        else {
            return Err(FormationManifestError::ProtocolIdentityMismatch(
                expected_role.role.clone(),
            ));
        };
        if stored_role.protocol != expected_role.protocol {
            return Err(FormationManifestError::ProtocolIdentityMismatch(
                expected_role.role.clone(),
            ));
        }
    }
    Ok(())
}

fn expected_payload(
    data_directory: &DataDirectory,
    spec: &FormationManifestSpec,
) -> Result<ManifestPayload, FormationManifestError> {
    let identity = data_directory.identity()?;
    let mut payload = ManifestPayload {
        fingerprint: String::new(),
        descriptor: spec.descriptor.clone(),
        normalized_configuration: spec.normalized_configuration.clone(),
        sql_migration: spec.sql_migration.clone(),
        file_formats: spec.file_formats.clone(),
        namespaces: spec.namespaces.clone(),
        required_files: spec.required_files.clone(),
        data_directory: DataDirectoryRecord {
            device: identity.device,
            inode: identity.inode,
            platform: platform_name(data_directory.admission().platform).to_owned(),
            filesystem: filesystem_name(&data_directory.admission().filesystem).to_owned(),
        },
    };
    payload.fingerprint = fingerprint(&payload)?;
    Ok(payload)
}

fn fingerprint(payload: &ManifestPayload) -> Result<String, FormationManifestError> {
    let input = FingerprintInput {
        descriptor: &payload.descriptor,
        normalized_configuration: &payload.normalized_configuration,
        sql_migration: &payload.sql_migration,
        file_formats: &payload.file_formats,
        namespaces: &payload.namespaces,
        required_files: &payload.required_files,
    };
    let bytes = serde_json::to_vec(&input)
        .map_err(|error| FormationManifestError::InvalidEncoding(error.to_string()))?;
    Ok(checksum(&bytes))
}

fn read_manifest(
    data_directory: &DataDirectory,
) -> Result<Option<StoredManifest>, FormationManifestError> {
    let path = RootRelativePath::try_from(FormationPath::FormationManifest)?;
    let mut file = match data_directory.open_existing_file(&path, false) {
        Ok(file) => file,
        Err(DataDirectoryError::Operation { source, .. })
            if source.kind() == io::ErrorKind::NotFound =>
        {
            return Ok(None)
        }
        Err(error) => return Err(error.into()),
    };
    let size = file
        .metadata()
        .map_err(|source| FormationManifestError::Io {
            operation: "inspect formation manifest size",
            source,
        })?
        .len();
    if size > MAX_MANIFEST_BYTES {
        return Err(FormationManifestError::ManifestTooLarge(size));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    file.read_to_end(&mut bytes)
        .map_err(|source| FormationManifestError::Io {
            operation: "read formation manifest",
            source,
        })?;
    let manifest = serde_json::from_slice(&bytes)
        .map_err(|error| FormationManifestError::InvalidEncoding(error.to_string()))?;
    Ok(Some(manifest))
}

fn install_manifest(
    data_directory: &DataDirectory,
    payload: &ManifestPayload,
) -> Result<(), FormationManifestError> {
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|error| FormationManifestError::InvalidEncoding(error.to_string()))?;
    let stored = StoredManifest {
        version: MANIFEST_VERSION,
        checksum_algorithm: CHECKSUM_ALGORITHM.to_owned(),
        checksum: checksum(&payload_bytes),
        payload: payload.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&stored)
        .map_err(|error| FormationManifestError::InvalidEncoding(error.to_string()))?;
    let temporary = temporary_manifest_path()?;
    let destination = RootRelativePath::try_from(FormationPath::FormationManifest)?;
    let mut file = data_directory.create_new_file(&temporary)?;
    file.write_all(&bytes)
        .map_err(|source| FormationManifestError::Io {
            operation: "write temporary formation manifest",
            source,
        })?;
    file.sync_all()
        .map_err(|source| FormationManifestError::Io {
            operation: "sync temporary formation manifest",
            source,
        })?;
    drop(file);
    data_directory.durable_replace_observed(&temporary, &destination, observe_install_boundary)?;

    let installed =
        read_manifest(data_directory)?.ok_or(FormationManifestError::MissingManifest)?;
    if installed != stored {
        return Err(FormationManifestError::ChecksumFailure);
    }
    Ok(())
}

fn temporary_manifest_path() -> Result<RootRelativePath, FormationManifestError> {
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    RootRelativePath::new(format!(
        "tmp/formation-manifest.{}.{}.{}",
        std::process::id(),
        nanos,
        sequence
    ))
    .map_err(Into::into)
}

#[cfg(not(test))]
fn observe_install_boundary(_: DurableReplaceBoundary) {}

#[cfg(test)]
fn observe_install_boundary(boundary: DurableReplaceBoundary) {
    let Ok(requested) = std::env::var("TICKR_MANIFEST_CRASH_AT") else {
        return;
    };
    let actual = match boundary {
        DurableReplaceBoundary::TemporaryFileSynced => "temporary-file-synced",
        DurableReplaceBoundary::DestinationInstalled => "destination-installed",
        DurableReplaceBoundary::ParentDirectorySynced => "parent-directory-synced",
    };
    if requested == actual {
        std::process::exit(86);
    }
}

fn validate_required_files(
    data_directory: &DataDirectory,
    required_files: &[String],
) -> Result<(), FormationManifestError> {
    for required in required_files {
        let path = RootRelativePath::new(required)?;
        data_directory.open_existing_file(&path, false)?;
    }
    Ok(())
}

fn validate_string_map(
    label: &str,
    values: &BTreeMap<String, String>,
) -> Result<(), FormationManifestError> {
    if values.is_empty()
        || values
            .iter()
            .any(|(key, value)| key.is_empty() || value.is_empty())
    {
        return Err(FormationManifestError::InvalidSpec(format!(
            "{label} identities must contain non-empty keys and values"
        )));
    }
    Ok(())
}

fn validate_version_map(values: &BTreeMap<String, u16>) -> Result<(), FormationManifestError> {
    if values.is_empty()
        || values
            .iter()
            .any(|(key, version)| key.is_empty() || *version == 0)
    {
        return Err(FormationManifestError::InvalidSpec(
            "file-format identities must contain non-empty names and positive versions".to_owned(),
        ));
    }
    Ok(())
}

fn checksum(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn sql_identity_label(identity: &SqlMigrationSetIdentity) -> String {
    format!(
        "{}@{} logical {}@{}",
        identity.protocol_name,
        identity.protocol_version,
        identity.latest_logical_name,
        identity.latest_logical_version
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredManifest {
    version: u16,
    checksum_algorithm: String,
    checksum: String,
    payload: ManifestPayload,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPayload {
    fingerprint: String,
    descriptor: DescriptorRecord,
    normalized_configuration: BTreeMap<String, String>,
    sql_migration: SqlMigrationSetIdentity,
    file_formats: BTreeMap<String, u16>,
    namespaces: BTreeMap<String, String>,
    required_files: Vec<String>,
    data_directory: DataDirectoryRecord,
}

#[derive(Serialize)]
struct FingerprintInput<'a> {
    descriptor: &'a DescriptorRecord,
    normalized_configuration: &'a BTreeMap<String, String>,
    sql_migration: &'a SqlMigrationSetIdentity,
    file_formats: &'a BTreeMap<String, u16>,
    namespaces: &'a BTreeMap<String, String>,
    required_files: &'a [String],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataDirectoryRecord {
    device: u64,
    inode: u64,
    platform: String,
    filesystem: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorRecord {
    profile: String,
    topology: String,
    sql: String,
    sql_migration_protocol: IdentityRecord,
    final_logs: String,
    writer_topology: String,
    executors: String,
    http_commands: String,
    roles: Vec<RoleRecord>,
    safe_pickup_handoff: bool,
    safe_attempt_outcome_handoff: bool,
    safe_cancellation_fence: bool,
}

impl From<&ResolvedFormationDescriptor> for DescriptorRecord {
    fn from(value: &ResolvedFormationDescriptor) -> Self {
        Self {
            profile: profile_name(value.profile).to_owned(),
            topology: topology_name(value.topology).to_owned(),
            sql: sql_name(value.sql).to_owned(),
            sql_migration_protocol: IdentityRecord::new(
                value.sql_migration_identity.name,
                value.sql_migration_identity.version,
            ),
            final_logs: final_log_store_name(value.final_logs).to_owned(),
            writer_topology: writer_topology_name(value.writer_topology).to_owned(),
            executors: executor_topology_name(value.executors),
            http_commands: http_command_ingress_name(value.http_commands).to_owned(),
            roles: value
                .roles
                .iter()
                .map(|role| RoleRecord {
                    role: role_name(role.role).to_owned(),
                    implementation: role_implementation_name(role.implementation).to_owned(),
                    protocol: IdentityRecord::new(role.protocol.name, role.protocol.version),
                })
                .collect(),
            safe_pickup_handoff: value.choreography.safe_pickup_handoff,
            safe_attempt_outcome_handoff: value.choreography.safe_attempt_outcome_handoff,
            safe_cancellation_fence: value.choreography.safe_cancellation_fence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleRecord {
    role: String,
    implementation: String,
    protocol: IdentityRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRecord {
    name: String,
    version: u16,
}

impl IdentityRecord {
    fn new(name: impl Into<String>, version: u16) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

fn profile_name(value: FormationProfile) -> &'static str {
    value.name()
}

fn topology_name(value: Topology) -> &'static str {
    match value {
        Topology::Distributed => "distributed",
        Topology::SingleNode => "single-node",
    }
}

fn sql_name(value: SqlImplementation) -> &'static str {
    match value {
        SqlImplementation::Postgres => "postgres",
        SqlImplementation::Sqlite => "sqlite",
    }
}

fn final_log_store_name(value: FinalLogStore) -> &'static str {
    match value {
        FinalLogStore::ObjectStore => "object-store",
        FinalLogStore::LocalFiles => "local-files",
    }
}

fn writer_topology_name(value: WriterTopology) -> &'static str {
    match value {
        WriterTopology::Distributed => "distributed",
        WriterTopology::ConductorOwned => "conductor-owned",
    }
}

fn executor_topology_name(value: ExecutorTopology) -> String {
    match value {
        ExecutorTopology::DistributedFleet => "distributed-fleet".to_owned(),
        ExecutorTopology::Exactly(count) => format!("exactly-{count}"),
    }
}

fn http_command_ingress_name(value: HttpCommandIngress) -> &'static str {
    match value {
        HttpCommandIngress::Enabled => "enabled",
        HttpCommandIngress::Disabled => "disabled",
    }
}

fn role_name(value: CoordinationRole) -> &'static str {
    match value {
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

fn role_implementation_name(value: RoleImplementation) -> &'static str {
    match value {
        RoleImplementation::NatsJetStream => "nats-jetstream",
        RoleImplementation::Redis => "redis",
        RoleImplementation::LocalRequestReply => "local-request-reply",
        RoleImplementation::LocalSqlite => "local-sqlite",
        RoleImplementation::LocalJournal => "local-journal",
        RoleImplementation::LocalNotification => "local-notification",
        RoleImplementation::LocalObservation => "local-observation",
        RoleImplementation::Disabled => "disabled",
    }
}

fn platform_name(value: AdmittedPlatform) -> &'static str {
    match value {
        AdmittedPlatform::MacOs => "macos",
        AdmittedPlatform::Linux => "linux",
    }
}

fn filesystem_name(value: &AdmittedFilesystem) -> &'static str {
    match value {
        AdmittedFilesystem::Apfs => "apfs",
        AdmittedFilesystem::Hfs => "hfs",
        AdmittedFilesystem::Ext => "ext",
        AdmittedFilesystem::Xfs => "xfs",
        AdmittedFilesystem::Btrfs => "btrfs",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};

    use crate::formation::{resolve_formation, FormationSelection};

    fn admitted_tempdir() -> Option<tempfile::TempDir> {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        match DataDirectory::admit(directory.path()) {
            Ok(_) => Some(directory),
            Err(DataDirectoryError::UnsupportedFilesystem(_)) => None,
            Err(error) => panic!("unexpected admission failure: {error}"),
        }
    }

    fn prepare_directory(directory: &tempfile::TempDir) {
        let lease = DataDirectory::admit(directory.path()).unwrap();
        let path = RootRelativePath::try_from(FormationPath::SqliteState).unwrap();
        let mut file = lease.create_new_file(&path).unwrap();
        file.write_all(b"durable-sqlite-state").unwrap();
        file.sync_all().unwrap();
    }

    fn spec(configuration_value: &str) -> FormationManifestSpec {
        let descriptor = resolve_formation(&FormationSelection::lite_local()).unwrap();
        FormationManifestSpec::new(
            &descriptor,
            SqlMigrationSetIdentity::new(
                "tickr.sqlite-migrations",
                1,
                "current_conductor_schema",
                1,
            ),
            BTreeMap::from([
                ("data-plane.sql.backend".to_owned(), "sqlite".to_owned()),
                (
                    "data-plane.sql.file".to_owned(),
                    configuration_value.to_owned(),
                ),
            ]),
            BTreeMap::from([
                ("formation-manifest".to_owned(), 1),
                ("sqlite-schema".to_owned(), 1),
            ]),
            BTreeMap::from([
                ("data-directory".to_owned(), "tickr-lite".to_owned()),
                ("sqlite".to_owned(), "conductor".to_owned()),
            ]),
            vec![RootRelativePath::try_from(FormationPath::SqliteState).unwrap()],
        )
        .unwrap()
    }

    #[test]
    fn first_install_and_identical_restart_preserve_durable_state() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        prepare_directory(&directory);
        let expected = spec("tickr.db");
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert_eq!(
            install_or_verify_formation_manifest(
                &lease,
                &expected,
                ManifestAdmission::OfflineMigration,
            )
            .unwrap(),
            ManifestStatus::Installed
        );
        drop(lease);

        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert_eq!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime)
                .unwrap(),
            ManifestStatus::Verified
        );
        assert_eq!(
            fs::read(directory.path().join("tickr.db")).unwrap(),
            b"durable-sqlite-state"
        );
        let metadata = fs::metadata(directory.path().join("formation-manifest.json")).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn changed_fingerprint_requires_explicit_authorization() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        prepare_directory(&directory);
        let old = spec("tickr.db");
        let changed = spec("renamed.db");
        let lease = DataDirectory::admit(directory.path()).unwrap();
        install_or_verify_formation_manifest(&lease, &old, ManifestAdmission::OfflineMigration)
            .unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &changed, ManifestAdmission::Runtime),
            Err(FormationManifestError::ChangedFingerprint)
        ));
        assert_eq!(
            install_or_verify_formation_manifest(
                &lease,
                &changed,
                ManifestAdmission::OfflineMigration,
            )
            .unwrap(),
            ManifestStatus::Replaced
        );
        assert_eq!(
            install_or_verify_formation_manifest(&lease, &changed, ManifestAdmission::Runtime)
                .unwrap(),
            ManifestStatus::Verified
        );
    }

    struct FrontierProof(Result<(), &'static str>);

    impl EmptyReconstructibleFrontier for FrontierProof {
        fn prove_empty_and_reconstructible(&self) -> Result<(), String> {
            self.0.map_err(str::to_owned)
        }
    }

    #[test]
    fn reconstructible_frontier_must_be_proved() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        prepare_directory(&directory);
        let old = spec("tickr.db");
        let changed = spec("reconstructed.db");
        let lease = DataDirectory::admit(directory.path()).unwrap();
        install_or_verify_formation_manifest(&lease, &old, ManifestAdmission::OfflineMigration)
            .unwrap();
        let refused = FrontierProof(Err("rows remain"));
        assert!(matches!(
            install_or_verify_formation_manifest(
                &lease,
                &changed,
                ManifestAdmission::EmptyReconstructible(&refused),
            ),
            Err(FormationManifestError::FrontierNotEmpty(_))
        ));
        let accepted = FrontierProof(Ok(()));
        assert_eq!(
            install_or_verify_formation_manifest(
                &lease,
                &changed,
                ManifestAdmission::EmptyReconstructible(&accepted),
            )
            .unwrap(),
            ManifestStatus::Replaced
        );
    }

    #[test]
    fn corrupt_unknown_missing_and_unsafe_records_fail_closed() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        prepare_directory(&directory);
        let expected = spec("tickr.db");
        let lease = DataDirectory::admit(directory.path()).unwrap();
        install_or_verify_formation_manifest(
            &lease,
            &expected,
            ManifestAdmission::OfflineMigration,
        )
        .unwrap();
        drop(lease);

        let manifest_path = directory.path().join("formation-manifest.json");
        let original = fs::read(&manifest_path).unwrap();
        let mut corrupt = original.clone();
        let last = corrupt.last_mut().unwrap();
        *last ^= 1;
        fs::write(&manifest_path, &corrupt).unwrap();
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime),
            Err(FormationManifestError::InvalidEncoding(_))
                | Err(FormationManifestError::ChecksumFailure)
        ));
        drop(lease);

        fs::write(&manifest_path, &original).unwrap();
        let mut stored: StoredManifest = serde_json::from_slice(&original).unwrap();
        stored.version = 999;
        fs::write(&manifest_path, serde_json::to_vec_pretty(&stored).unwrap()).unwrap();
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime),
            Err(FormationManifestError::UnknownManifestVersion(999))
        ));
        drop(lease);

        fs::write(&manifest_path, &original).unwrap();
        fs::remove_file(directory.path().join("tickr.db")).unwrap();
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime),
            Err(FormationManifestError::Storage(
                DataDirectoryError::Operation { .. }
            ))
        ));
        drop(lease);

        fs::write(directory.path().join("tickr.db"), b"state").unwrap();
        fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o644)).unwrap();
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime),
            Err(FormationManifestError::Storage(
                DataDirectoryError::UnsafePermissions { .. }
            ))
        ));
    }

    #[test]
    fn unknown_protocol_and_schema_identities_are_refused() {
        let descriptor = resolve_formation(&FormationSelection::lite_local()).unwrap();
        let error = FormationManifestSpec::new(
            &descriptor,
            SqlMigrationSetIdentity::new(
                "tickr.sqlite-migrations",
                2,
                "current_conductor_schema",
                1,
            ),
            BTreeMap::from([("sql".to_owned(), "sqlite".to_owned())]),
            BTreeMap::from([("manifest".to_owned(), 1)]),
            BTreeMap::from([("root".to_owned(), "tickr-lite".to_owned())]),
            vec![RootRelativePath::try_from(FormationPath::SqliteState).unwrap()],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            FormationManifestError::SchemaDisagreement { .. }
        ));

        let Some(directory) = admitted_tempdir() else {
            return;
        };
        prepare_directory(&directory);
        let expected = spec("tickr.db");
        let lease = DataDirectory::admit(directory.path()).unwrap();
        install_or_verify_formation_manifest(
            &lease,
            &expected,
            ManifestAdmission::OfflineMigration,
        )
        .unwrap();
        drop(lease);

        let manifest_path = directory.path().join("formation-manifest.json");
        let mut stored: StoredManifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        stored.payload.descriptor.roles[0].protocol.version = 999;
        stored.payload.fingerprint = fingerprint(&stored.payload).unwrap();
        stored.checksum = checksum(&serde_json::to_vec(&stored.payload).unwrap());
        fs::write(&manifest_path, serde_json::to_vec_pretty(&stored).unwrap()).unwrap();
        let lease = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            install_or_verify_formation_manifest(&lease, &expected, ManifestAdmission::Runtime),
            Err(FormationManifestError::ProtocolIdentityMismatch(_))
        ));
    }

    #[test]
    fn child_manifest_install() {
        let Ok(root) = std::env::var("TICKR_MANIFEST_CRASH_ROOT") else {
            return;
        };
        let lease = DataDirectory::admit(root).unwrap();
        install_or_verify_formation_manifest(
            &lease,
            &spec("new.db"),
            ManifestAdmission::OfflineMigration,
        )
        .unwrap();
    }

    #[test]
    fn install_crashes_converge_to_the_old_or_new_valid_manifest() {
        for boundary in [
            "temporary-file-synced",
            "destination-installed",
            "parent-directory-synced",
        ] {
            let Some(directory) = admitted_tempdir() else {
                return;
            };
            prepare_directory(&directory);
            let old = spec("old.db");
            let new = spec("new.db");
            let lease = DataDirectory::admit(directory.path()).unwrap();
            install_or_verify_formation_manifest(&lease, &old, ManifestAdmission::OfflineMigration)
                .unwrap();
            drop(lease);

            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "formation_manifest::tests::child_manifest_install",
                    "--nocapture",
                ])
                .env("TICKR_MANIFEST_CRASH_ROOT", directory.path())
                .env("TICKR_MANIFEST_CRASH_AT", boundary)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86));

            let lease = DataDirectory::admit(directory.path()).unwrap();
            let old_valid =
                install_or_verify_formation_manifest(&lease, &old, ManifestAdmission::Runtime)
                    .is_ok();
            let new_valid =
                install_or_verify_formation_manifest(&lease, &new, ManifestAdmission::Runtime)
                    .is_ok();
            assert_ne!(
                old_valid, new_valid,
                "boundary {boundary} left no unique valid manifest"
            );
        }
    }
}
