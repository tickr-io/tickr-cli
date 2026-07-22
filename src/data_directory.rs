#[cfg(target_os = "linux")]
use std::ffi::c_long;
#[cfg(target_os = "macos")]
use std::ffi::{c_char, CStr};
use std::ffi::{c_int, CString, OsStr};
use std::fmt;
use std::fs::{File, Metadata};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const LOCK_EXCLUSIVE: c_int = 2;
const LOCK_NONBLOCKING: c_int = 4;
const FCNTL_SET_FD: c_int = 2;
#[cfg(target_os = "macos")]
type ModeT = u16;
#[cfg(not(target_os = "macos"))]
type ModeT = u32;
const FD_CLOEXEC: c_int = 1;
const AT_REMOVEDIR: c_int = 0x80;

#[cfg(target_os = "macos")]
const OPEN_NO_FOLLOW: c_int = 0x0000_0100;
#[cfg(target_os = "macos")]
const OPEN_CREATE: c_int = 0x0000_0200;
#[cfg(target_os = "macos")]
const OPEN_EXCLUSIVE: c_int = 0x0000_0800;
#[cfg(target_os = "macos")]
const OPEN_DIRECTORY: c_int = 0x0010_0000;
#[cfg(target_os = "macos")]
const OPEN_CLOEXEC: c_int = 0x0100_0000;

#[cfg(target_os = "linux")]
const OPEN_NO_FOLLOW: c_int = 0x0002_0000;
#[cfg(target_os = "linux")]
const OPEN_CREATE: c_int = 0x0000_0040;
#[cfg(target_os = "linux")]
const OPEN_EXCLUSIVE: c_int = 0x0000_0080;
#[cfg(target_os = "linux")]
const OPEN_DIRECTORY: c_int = 0x0001_0000;
#[cfg(target_os = "linux")]
const OPEN_CLOEXEC: c_int = 0x0008_0000;

const OPEN_READ_ONLY: c_int = 0;
const OPEN_READ_WRITE: c_int = 2;

static PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmittedPlatform {
    MacOs,
    Linux,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmittedFilesystem {
    Apfs,
    Hfs,
    Ext,
    Xfs,
    Btrfs,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredCapability {
    RootRelativeNoFollow,
    OwnershipAndPermissions,
    SameDeviceTemporaryPlacement,
    ExclusiveLocking,
    FileSync,
    ParentDirectorySync,
    AtomicReplacement,
    ProcessContainment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionCapabilities {
    pub root_relative_no_follow: bool,
    pub ownership_and_permissions: bool,
    pub same_device_temporary_placement: bool,
    pub exclusive_locking: bool,
    pub file_sync: bool,
    pub parent_directory_sync: bool,
    pub atomic_replacement: bool,
    pub process_containment: bool,
}

impl AdmissionCapabilities {
    const PROVEN: Self = Self {
        root_relative_no_follow: true,
        ownership_and_permissions: true,
        same_device_temporary_placement: true,
        exclusive_locking: true,
        file_sync: true,
        parent_directory_sync: true,
        atomic_replacement: true,
        process_containment: true,
    };

    fn require_all(self) -> Result<(), DataDirectoryError> {
        for (capability, proven) in [
            (
                RequiredCapability::RootRelativeNoFollow,
                self.root_relative_no_follow,
            ),
            (
                RequiredCapability::OwnershipAndPermissions,
                self.ownership_and_permissions,
            ),
            (
                RequiredCapability::SameDeviceTemporaryPlacement,
                self.same_device_temporary_placement,
            ),
            (RequiredCapability::ExclusiveLocking, self.exclusive_locking),
            (RequiredCapability::FileSync, self.file_sync),
            (
                RequiredCapability::ParentDirectorySync,
                self.parent_directory_sync,
            ),
            (
                RequiredCapability::AtomicReplacement,
                self.atomic_replacement,
            ),
            (
                RequiredCapability::ProcessContainment,
                self.process_containment,
            ),
        ] {
            if !proven {
                return Err(DataDirectoryError::MissingCapability(capability));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataDirectoryAdmission {
    pub platform: AdmittedPlatform,
    pub filesystem: AdmittedFilesystem,
    pub capabilities: AdmissionCapabilities,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormationPath {
    SqliteState,
    FormationManifest,
    Journals,
    StagedLogs,
    FinalLogs,
    TemporaryFiles,
    Quarantine,
}

impl FormationPath {
    pub const ALL: [Self; 7] = [
        Self::SqliteState,
        Self::FormationManifest,
        Self::Journals,
        Self::StagedLogs,
        Self::FinalLogs,
        Self::TemporaryFiles,
        Self::Quarantine,
    ];

    pub fn relative_path(self) -> &'static Path {
        match self {
            Self::SqliteState => Path::new("tickr.db"),
            Self::FormationManifest => Path::new("formation-manifest.json"),
            Self::Journals => Path::new("journals"),
            Self::StagedLogs => Path::new("logs/staged"),
            Self::FinalLogs => Path::new("logs/final"),
            Self::TemporaryFiles => Path::new("tmp"),
            Self::Quarantine => Path::new("quarantine"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootRelativePath(PathBuf);

impl RootRelativePath {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DataDirectoryError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path.as_os_str().as_bytes().contains(&0) {
            return Err(DataDirectoryError::InvalidRelativePath(path.to_path_buf()));
        }
        if path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(DataDirectoryError::InvalidRelativePath(path.to_path_buf()));
        }
        Ok(Self(path.to_path_buf()))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<FormationPath> for RootRelativePath {
    type Error = DataDirectoryError;

    fn try_from(value: FormationPath) -> Result<Self, Self::Error> {
        Self::new(value.relative_path())
    }
}

#[derive(Debug)]
pub enum DataDirectoryError {
    UnsupportedPlatform,
    NetworkFilesystem(String),
    UnsupportedFilesystem(String),
    InvalidRoot(PathBuf),
    InvalidRelativePath(PathBuf),
    WrongOwnership {
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
    UnsafePermissions {
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
    DifferentDevice(PathBuf),
    AlreadyLocked(PathBuf),
    MissingCapability(RequiredCapability),
    UnsupportedSqliteUrl(String),
    Operation {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for DataDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => formatter
                .write_str("Tickr Lite data-directory admission is unsupported on this platform"),
            Self::NetworkFilesystem(filesystem) => {
                write!(
                    formatter,
                    "network filesystem `{filesystem}` is not admissible"
                )
            }
            Self::UnsupportedFilesystem(filesystem) => write!(
                formatter,
                "filesystem `{filesystem}` has no admitted Tickr Lite durability contract"
            ),
            Self::InvalidRoot(path) => write!(
                formatter,
                "data-directory root {} must be a real directory, not a symlink",
                path.display()
            ),
            Self::InvalidRelativePath(path) => write!(
                formatter,
                "formation path {} is not a non-empty root-relative path",
                path.display()
            ),
            Self::WrongOwnership {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "{} is owned by uid {actual}; required uid is {expected}",
                path.display()
            ),
            Self::UnsafePermissions {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "{} has mode {actual:#o}; required mode is {expected:#o}",
                path.display()
            ),
            Self::DifferentDevice(path) => write!(
                formatter,
                "{} is not on the data-directory device",
                path.display()
            ),
            Self::AlreadyLocked(path) => write!(
                formatter,
                "data directory {} is already exclusively locked",
                path.display()
            ),
            Self::MissingCapability(capability) => {
                write!(formatter, "data directory does not prove {capability:?}")
            }
            Self::UnsupportedSqliteUrl(url) => write!(
                formatter,
                "SQLite URL `{url}` must name an absolute on-disk database"
            ),
            Self::Operation {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for DataDirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DataDirectoryIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableReplaceBoundary {
    TemporaryFileSynced,
    DestinationInstalled,
    ParentDirectorySynced,
}

/// The exclusive lease and already-open authority for all Tickr Lite files.
/// Dropping this value releases the operating-system lock.
pub struct DataDirectory {
    root: File,
    configured_path: PathBuf,
    root_device: u64,
    admission: DataDirectoryAdmission,
}

impl DataDirectory {
    pub fn admit(path: impl AsRef<Path>) -> Result<Self, DataDirectoryError> {
        let configured_path = path.as_ref().to_path_buf();
        let platform = admitted_platform()?;
        let root = open_root(&configured_path)?;
        let metadata = root
            .metadata()
            .map_err(|source| operation("inspect data-directory root", &configured_path, source))?;
        if !metadata.is_dir() {
            return Err(DataDirectoryError::InvalidRoot(configured_path));
        }
        validate_owner_and_mode(&configured_path, &metadata, DIRECTORY_MODE)?;
        let root_device = metadata.dev();
        let filesystem = admit_filesystem(root.as_raw_fd())?;
        probe_process_containment()?;
        lock_root(&root, &configured_path)?;
        probe_durability(&root, &configured_path, root_device)?;

        let capabilities = AdmissionCapabilities::PROVEN;
        capabilities.require_all()?;
        Ok(Self {
            root,
            configured_path,
            root_device,
            admission: DataDirectoryAdmission {
                platform,
                filesystem,
                capabilities,
            },
        })
    }

    pub fn admission(&self) -> &DataDirectoryAdmission {
        &self.admission
    }

    pub fn configured_path(&self) -> &Path {
        &self.configured_path
    }
    pub(crate) fn identity(&self) -> Result<DataDirectoryIdentity, DataDirectoryError> {
        let metadata = self.root.metadata().map_err(|source| {
            operation(
                "inspect data-directory identity",
                &self.configured_path,
                source,
            )
        })?;
        Ok(DataDirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub fn ensure_directory(&self, path: &RootRelativePath) -> Result<(), DataDirectoryError> {
        let mut directory = self.root.try_clone().map_err(|source| {
            operation("clone data-directory handle", &self.configured_path, source)
        })?;
        let mut traversed = PathBuf::new();
        for component in normal_components(path.as_path()) {
            traversed.push(component);
            let opened = match open_at(
                directory.as_raw_fd(),
                component,
                OPEN_READ_ONLY | OPEN_DIRECTORY | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
                DIRECTORY_MODE,
            ) {
                Ok(opened) => opened,
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    mkdir_at(directory.as_raw_fd(), component).map_err(|source| {
                        operation("create formation directory", &traversed, source)
                    })?;
                    directory.sync_all().map_err(|source| {
                        operation("sync formation parent directory", &traversed, source)
                    })?;
                    open_at(
                        directory.as_raw_fd(),
                        component,
                        OPEN_READ_ONLY | OPEN_DIRECTORY | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
                        DIRECTORY_MODE,
                    )
                    .map_err(|source| {
                        operation("open created formation directory", &traversed, source)
                    })?
                }
                Err(source) => {
                    return Err(operation(
                        "open formation directory without following links",
                        &traversed,
                        source,
                    ))
                }
            };
            self.validate_formation_entry(&traversed, &opened, DIRECTORY_MODE)?;
            directory = opened;
        }
        Ok(())
    }

    pub fn create_new_file(&self, path: &RootRelativePath) -> Result<File, DataDirectoryError> {
        let (parent, name) = self.open_parent(path, true)?;
        let file = open_at(
            parent.as_raw_fd(),
            name,
            OPEN_READ_WRITE | OPEN_CREATE | OPEN_EXCLUSIVE | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
            FILE_MODE,
        )
        .map_err(|source| operation("create formation file", path.as_path(), source))?;
        self.validate_formation_entry(path.as_path(), &file, FILE_MODE)?;
        file.sync_all()
            .map_err(|source| operation("sync new formation file", path.as_path(), source))?;
        parent.sync_all().map_err(|source| {
            operation("sync new formation file parent", path.as_path(), source)
        })?;
        Ok(file)
    }

    pub fn open_existing_file(
        &self,
        path: &RootRelativePath,
        writable: bool,
    ) -> Result<File, DataDirectoryError> {
        let (parent, name) = self.open_parent(path, false)?;
        let access = if writable {
            OPEN_READ_WRITE
        } else {
            OPEN_READ_ONLY
        };
        let file = open_at(
            parent.as_raw_fd(),
            name,
            access | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
            FILE_MODE,
        )
        .map_err(|source| {
            operation(
                "open formation file without following links",
                path.as_path(),
                source,
            )
        })?;
        self.validate_formation_entry(path.as_path(), &file, FILE_MODE)?;
        Ok(file)
    }

    pub fn open_or_create_file(&self, path: &RootRelativePath) -> Result<File, DataDirectoryError> {
        match self.create_new_file(path) {
            Ok(file) => Ok(file),
            Err(DataDirectoryError::Operation { source, .. })
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                self.open_existing_file(path, true)
            }
            Err(error) => Err(error),
        }
    }

    /// Remove one regular formation file through its already-open parent and
    /// sync the parent directory before reporting completion.
    pub fn remove_file(&self, path: &RootRelativePath) -> Result<(), DataDirectoryError> {
        let (parent, name) = self.open_parent(path, false)?;
        let file = self.open_existing_file(path, false)?;
        self.validate_formation_entry(path.as_path(), &file, FILE_MODE)?;
        unlink_at(parent.as_raw_fd(), name, false)
            .map_err(|source| operation("remove formation file", path.as_path(), source))?;
        parent
            .sync_all()
            .map_err(|source| operation("sync formation file parent", path.as_path(), source))
    }

    pub fn prepare_unix_socket_path(
        &self,
        path: &RootRelativePath,
    ) -> Result<PathBuf, DataDirectoryError> {
        let (parent, name) = self.open_parent(path, true)?;
        let socket_path = self.configured_path.join(path.as_path());
        match std::fs::symlink_metadata(&socket_path) {
            Ok(metadata) => {
                if !metadata.file_type().is_socket() {
                    return Err(operation(
                        "refuse non-socket endpoint entry",
                        path.as_path(),
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "existing endpoint entry is not a Unix-domain socket",
                        ),
                    ));
                }
                validate_owner_and_mode(path.as_path(), &metadata, FILE_MODE)?;
                if metadata.dev() != self.root_device {
                    return Err(DataDirectoryError::DifferentDevice(
                        path.as_path().to_path_buf(),
                    ));
                }
                unlink_at(parent.as_raw_fd(), name, false).map_err(|source| {
                    operation("remove stale Unix-domain endpoint", path.as_path(), source)
                })?;
                parent
                    .sync_all()
                    .map_err(|source| operation("sync endpoint parent", path.as_path(), source))?;
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(operation(
                    "inspect Unix-domain endpoint without following links",
                    path.as_path(),
                    source,
                ))
            }
        }
        Ok(socket_path)
    }

    pub fn validate_unix_socket_path(
        &self,
        path: &RootRelativePath,
    ) -> Result<(), DataDirectoryError> {
        let socket_path = self.configured_path.join(path.as_path());
        let metadata = std::fs::symlink_metadata(&socket_path).map_err(|source| {
            operation(
                "inspect bound Unix-domain endpoint without following links",
                path.as_path(),
                source,
            )
        })?;
        if !metadata.file_type().is_socket() {
            return Err(operation(
                "validate Unix-domain endpoint",
                path.as_path(),
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bound endpoint is not a Unix-domain socket",
                ),
            ));
        }
        validate_owner_and_mode(path.as_path(), &metadata, FILE_MODE)?;
        if metadata.dev() != self.root_device {
            return Err(DataDirectoryError::DifferentDevice(
                path.as_path().to_path_buf(),
            ));
        }
        Ok(())
    }

    pub fn secure_unix_socket_permissions(
        &self,
        path: &RootRelativePath,
    ) -> Result<(), DataDirectoryError> {
        let socket_path = self.configured_path.join(path.as_path());
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(FILE_MODE))
            .map_err(|source| operation("secure Unix-domain endpoint", path.as_path(), source))?;
        self.validate_unix_socket_path(path)
    }

    pub fn remove_unix_socket(&self, path: &RootRelativePath) -> Result<(), DataDirectoryError> {
        let (parent, name) = self.open_parent(path, false)?;
        let socket_path = self.configured_path.join(path.as_path());
        match std::fs::symlink_metadata(&socket_path) {
            Ok(metadata) => {
                if !metadata.file_type().is_socket() {
                    return Err(operation(
                        "refuse non-socket endpoint removal",
                        path.as_path(),
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "endpoint entry is not a Unix-domain socket",
                        ),
                    ));
                }
                validate_owner_and_mode(path.as_path(), &metadata, FILE_MODE)?;
                unlink_at(parent.as_raw_fd(), name, false).map_err(|source| {
                    operation("remove Unix-domain endpoint", path.as_path(), source)
                })?;
                parent
                    .sync_all()
                    .map_err(|source| operation("sync endpoint parent", path.as_path(), source))
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(operation(
                "inspect Unix-domain endpoint before removal",
                path.as_path(),
                source,
            )),
        }
    }

    pub fn durable_replace(
        &self,
        temporary: &RootRelativePath,
        destination: &RootRelativePath,
    ) -> Result<(), DataDirectoryError> {
        self.durable_replace_observed(temporary, destination, |_| {})
    }

    pub(crate) fn durable_replace_observed(
        &self,
        temporary: &RootRelativePath,
        destination: &RootRelativePath,
        mut observe: impl FnMut(DurableReplaceBoundary),
    ) -> Result<(), DataDirectoryError> {
        let temporary_file = self.open_existing_file(temporary, true)?;
        temporary_file.sync_all().map_err(|source| {
            operation(
                "sync replacement temporary file",
                temporary.as_path(),
                source,
            )
        })?;
        validate_same_device(temporary.as_path(), self.root_device, &temporary_file)?;
        observe(DurableReplaceBoundary::TemporaryFileSynced);

        let (temporary_parent, temporary_name) = self.open_parent(temporary, false)?;
        let (destination_parent, destination_name) = self.open_parent(destination, true)?;
        rename_at(
            temporary_parent.as_raw_fd(),
            temporary_name,
            destination_parent.as_raw_fd(),
            destination_name,
        )
        .map_err(|source| {
            operation(
                "atomically replace formation file",
                destination.as_path(),
                source,
            )
        })?;
        observe(DurableReplaceBoundary::DestinationInstalled);
        destination_parent.sync_all().map_err(|source| {
            operation(
                "sync replacement parent directory",
                destination.as_path(),
                source,
            )
        })?;
        observe(DurableReplaceBoundary::ParentDirectorySynced);
        Ok(())
    }

    /// Sync a validated file's parent after recovery observes an already
    /// installed durable replacement.
    pub fn sync_parent(&self, path: &RootRelativePath) -> Result<(), DataDirectoryError> {
        let (parent, _) = self.open_parent(path, false)?;
        parent
            .sync_all()
            .map_err(|source| operation("sync formation file parent", path.as_path(), source))
    }

    fn open_parent<'a>(
        &self,
        path: &'a RootRelativePath,
        create: bool,
    ) -> Result<(File, &'a OsStr), DataDirectoryError> {
        let parent_path = path.as_path().parent().unwrap_or_else(|| Path::new(""));
        if create && !parent_path.as_os_str().is_empty() {
            self.ensure_directory(&RootRelativePath::new(parent_path)?)?;
        }
        let mut parent = self.root.try_clone().map_err(|source| {
            operation("clone data-directory handle", &self.configured_path, source)
        })?;
        let mut traversed = PathBuf::new();
        for component in normal_components(parent_path) {
            traversed.push(component);
            let opened = open_at(
                parent.as_raw_fd(),
                component,
                OPEN_READ_ONLY | OPEN_DIRECTORY | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
                DIRECTORY_MODE,
            )
            .map_err(|source| operation("open formation file parent", &traversed, source))?;
            self.validate_formation_entry(&traversed, &opened, DIRECTORY_MODE)?;
            parent = opened;
        }
        let name = path
            .as_path()
            .file_name()
            .ok_or_else(|| DataDirectoryError::InvalidRelativePath(path.0.clone()))?;
        Ok((parent, name))
    }

    fn validate_formation_entry(
        &self,
        path: &Path,
        file: &File,
        expected_mode: u32,
    ) -> Result<(), DataDirectoryError> {
        let metadata = file
            .metadata()
            .map_err(|source| operation("inspect formation entry", path, source))?;
        validate_owner_and_mode(path, &metadata, expected_mode)?;
        validate_same_device(path, self.root_device, file)
    }
}

pub fn sqlite_path_from_url(url: &str) -> Result<PathBuf, DataDirectoryError> {
    let without_query = url.split_once('?').map_or(url, |(path, _)| path);
    let raw_path = without_query
        .strip_prefix("sqlite://")
        .or_else(|| without_query.strip_prefix("sqlite:"))
        .ok_or_else(|| DataDirectoryError::UnsupportedSqliteUrl(url.to_owned()))?;
    let path = PathBuf::from(raw_path);
    if !path.is_absolute()
        || path.file_name().is_none()
        || raw_path.contains('%')
        || raw_path == ":memory:"
    {
        return Err(DataDirectoryError::UnsupportedSqliteUrl(url.to_owned()));
    }
    Ok(path)
}

fn admitted_platform() -> Result<AdmittedPlatform, DataDirectoryError> {
    #[cfg(target_os = "macos")]
    {
        Ok(AdmittedPlatform::MacOs)
    }
    #[cfg(target_os = "linux")]
    {
        Ok(AdmittedPlatform::Linux)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err(DataDirectoryError::UnsupportedPlatform)
    }
}

fn open_root(path: &Path) -> Result<File, DataDirectoryError> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let path_string = c_string(path.as_os_str())
            .map_err(|source| operation("encode data-directory root", path, source))?;
        let descriptor = unsafe {
            c_open(
                path_string.as_ptr(),
                OPEN_READ_ONLY | OPEN_DIRECTORY | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
                DIRECTORY_MODE as c_int,
            )
        };
        if descriptor < 0 {
            let source = io::Error::last_os_error();
            if source.raw_os_error() == Some(40) || source.raw_os_error() == Some(62) {
                return Err(DataDirectoryError::InvalidRoot(path.to_path_buf()));
            }
            return Err(operation(
                "open data-directory root without following links",
                path,
                source,
            ));
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = path;
        Err(DataDirectoryError::UnsupportedPlatform)
    }
}

fn lock_root(root: &File, path: &Path) -> Result<(), DataDirectoryError> {
    let result = unsafe { c_flock(root.as_raw_fd(), LOCK_EXCLUSIVE | LOCK_NONBLOCKING) };
    if result == 0 {
        return Ok(());
    }
    let source = io::Error::last_os_error();
    if source.kind() == io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(11) | Some(35))
    {
        Err(DataDirectoryError::AlreadyLocked(path.to_path_buf()))
    } else {
        Err(operation(
            "acquire exclusive data-directory lock",
            path,
            source,
        ))
    }
}

fn validate_owner_and_mode(
    path: &Path,
    metadata: &Metadata,
    expected_mode: u32,
) -> Result<(), DataDirectoryError> {
    let expected_owner = unsafe { c_geteuid() };
    if metadata.uid() != expected_owner {
        return Err(DataDirectoryError::WrongOwnership {
            path: path.to_path_buf(),
            expected: expected_owner,
            actual: metadata.uid(),
        });
    }
    let actual_mode = metadata.mode() & 0o777;
    if actual_mode != expected_mode {
        return Err(DataDirectoryError::UnsafePermissions {
            path: path.to_path_buf(),
            expected: expected_mode,
            actual: actual_mode,
        });
    }
    Ok(())
}

fn validate_same_device(
    path: &Path,
    root_device: u64,
    file: &File,
) -> Result<(), DataDirectoryError> {
    let device = file
        .metadata()
        .map_err(|source| operation("inspect formation entry device", path, source))?
        .dev();
    if device != root_device {
        return Err(DataDirectoryError::DifferentDevice(path.to_path_buf()));
    }
    Ok(())
}

fn probe_process_containment() -> Result<(), DataDirectoryError> {
    let mut descriptors = [-1; 2];
    if unsafe { c_pipe(descriptors.as_mut_ptr()) } != 0 {
        return Err(operation(
            "create parent-liveness pipe",
            Path::new("<platform>"),
            io::Error::last_os_error(),
        ));
    }
    let mut reader = unsafe { File::from_raw_fd(descriptors[0]) };
    let mut writer = unsafe { File::from_raw_fd(descriptors[1]) };
    for descriptor in descriptors {
        if unsafe { c_fcntl(descriptor, FCNTL_SET_FD, FD_CLOEXEC) } != 0 {
            return Err(operation(
                "protect parent-liveness descriptor inheritance",
                Path::new("<platform>"),
                io::Error::last_os_error(),
            ));
        }
    }
    writer.write_all(&[0x54]).map_err(|source| {
        operation(
            "write parent-liveness pipe",
            Path::new("<platform>"),
            source,
        )
    })?;
    let mut byte = [0_u8; 1];
    reader.read_exact(&mut byte).map_err(|source| {
        operation("read parent-liveness pipe", Path::new("<platform>"), source)
    })?;
    drop(writer);
    let mut eof = [0_u8; 1];
    let count = reader.read(&mut eof).map_err(|source| {
        operation(
            "observe parent-liveness EOF",
            Path::new("<platform>"),
            source,
        )
    })?;
    if byte != [0x54] || count != 0 {
        return Err(DataDirectoryError::MissingCapability(
            RequiredCapability::ProcessContainment,
        ));
    }
    Ok(())
}

fn probe_durability(
    root: &File,
    configured_path: &Path,
    root_device: u64,
) -> Result<(), DataDirectoryError> {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let source_name = format!(".tickr-admission-{}-{sequence}.tmp", std::process::id());
    let destination_name = format!(".tickr-admission-{}-{sequence}.dst", std::process::id());
    let source = OsStr::new(&source_name);
    let destination = OsStr::new(&destination_name);
    let cleanup = || {
        let _ = unlink_at(root.as_raw_fd(), source, false);
        let _ = unlink_at(root.as_raw_fd(), destination, false);
    };

    let result = (|| {
        let mut source_file = open_at(
            root.as_raw_fd(),
            source,
            OPEN_READ_WRITE | OPEN_CREATE | OPEN_EXCLUSIVE | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
            FILE_MODE,
        )
        .map_err(|source_error| {
            operation(
                "create same-device temporary probe",
                configured_path,
                source_error,
            )
        })?;
        validate_same_device(Path::new(&source_name), root_device, &source_file)?;
        validate_owner_and_mode(
            Path::new(&source_name),
            &source_file.metadata().map_err(|source_error| {
                operation("inspect durability probe", configured_path, source_error)
            })?,
            FILE_MODE,
        )?;
        source_file.write_all(b"new").map_err(|source_error| {
            operation("write durability probe", configured_path, source_error)
        })?;
        source_file.sync_all().map_err(|source_error| {
            operation("sync durability probe file", configured_path, source_error)
        })?;

        let mut destination_file = open_at(
            root.as_raw_fd(),
            destination,
            OPEN_READ_WRITE | OPEN_CREATE | OPEN_EXCLUSIVE | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
            FILE_MODE,
        )
        .map_err(|source_error| {
            operation("create replacement probe", configured_path, source_error)
        })?;
        validate_owner_and_mode(
            Path::new(&destination_name),
            &destination_file.metadata().map_err(|source_error| {
                operation("inspect replacement probe", configured_path, source_error)
            })?,
            FILE_MODE,
        )?;
        destination_file.write_all(b"old").map_err(|source_error| {
            operation("write replacement probe", configured_path, source_error)
        })?;
        destination_file.sync_all().map_err(|source_error| {
            operation("sync replacement probe", configured_path, source_error)
        })?;
        drop(destination_file);

        rename_at(root.as_raw_fd(), source, root.as_raw_fd(), destination).map_err(
            |source_error| operation("prove atomic replacement", configured_path, source_error),
        )?;
        root.sync_all().map_err(|source_error| {
            operation(
                "sync data-directory after replacement",
                configured_path,
                source_error,
            )
        })?;

        let mut installed = open_at(
            root.as_raw_fd(),
            destination,
            OPEN_READ_ONLY | OPEN_NO_FOLLOW | OPEN_CLOEXEC,
            FILE_MODE,
        )
        .map_err(|source_error| {
            operation("open installed replacement", configured_path, source_error)
        })?;
        let mut contents = String::new();
        installed
            .read_to_string(&mut contents)
            .map_err(|source_error| {
                operation(
                    "verify installed replacement",
                    configured_path,
                    source_error,
                )
            })?;
        if contents != "new" {
            return Err(DataDirectoryError::MissingCapability(
                RequiredCapability::AtomicReplacement,
            ));
        }
        Ok(())
    })();
    cleanup();
    result?;
    root.sync_all().map_err(|source| {
        operation(
            "sync data-directory after admission probes",
            configured_path,
            source,
        )
    })?;
    Ok(())
}

fn normal_components(path: &Path) -> impl Iterator<Item = &OsStr> {
    path.components().map(|component| match component {
        Component::Normal(component) => component,
        _ => unreachable!("RootRelativePath already validated every component"),
    })
}

fn operation(
    operation: &'static str,
    path: impl AsRef<Path>,
    source: io::Error,
) -> DataDirectoryError {
    DataDirectoryError::Operation {
        operation,
        path: path.as_ref().to_path_buf(),
        source,
    }
}

fn c_string(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains an interior NUL byte",
        )
    })
}

fn open_at(directory: RawFd, name: &OsStr, flags: c_int, mode: u32) -> io::Result<File> {
    let name = c_string(name)?;
    let descriptor = unsafe { c_openat(directory, name.as_ptr(), flags, mode as c_int) };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

fn mkdir_at(directory: RawFd, name: &OsStr) -> io::Result<()> {
    let name = c_string(name)?;
    if unsafe { c_mkdirat(directory, name.as_ptr(), DIRECTORY_MODE as ModeT) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn rename_at(
    source_directory: RawFd,
    source: &OsStr,
    destination_directory: RawFd,
    destination: &OsStr,
) -> io::Result<()> {
    let source = c_string(source)?;
    let destination = c_string(destination)?;
    if unsafe {
        c_renameat(
            source_directory,
            source.as_ptr(),
            destination_directory,
            destination.as_ptr(),
        )
    } == 0
    {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn unlink_at(directory: RawFd, name: &OsStr, directory_entry: bool) -> io::Result<()> {
    let name = c_string(name)?;
    let flags = if directory_entry { AT_REMOVEDIR } else { 0 };
    if unsafe { c_unlinkat(directory, name.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn admit_filesystem(descriptor: RawFd) -> Result<AdmittedFilesystem, DataDirectoryError> {
    const MOUNT_LOCAL: u32 = 0x0000_1000;
    let mut status: DarwinStatFs = unsafe { std::mem::zeroed() };
    if unsafe { c_fstatfs(descriptor, &mut status) } != 0 {
        return Err(operation(
            "inspect data-directory filesystem",
            Path::new("<root>"),
            io::Error::last_os_error(),
        ));
    }
    let filesystem = unsafe { CStr::from_ptr(status.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    classify_macos_filesystem(&filesystem, status.f_flags & MOUNT_LOCAL != 0)
}

#[cfg(target_os = "macos")]
fn classify_macos_filesystem(
    filesystem: &str,
    is_local: bool,
) -> Result<AdmittedFilesystem, DataDirectoryError> {
    if !is_local {
        return Err(DataDirectoryError::NetworkFilesystem(filesystem.to_owned()));
    }
    match filesystem {
        "apfs" => Ok(AdmittedFilesystem::Apfs),
        "hfs" => Ok(AdmittedFilesystem::Hfs),
        _ => Err(DataDirectoryError::UnsupportedFilesystem(
            filesystem.to_owned(),
        )),
    }
}

#[cfg(target_os = "linux")]
fn admit_filesystem(descriptor: RawFd) -> Result<AdmittedFilesystem, DataDirectoryError> {
    let mut status: LinuxStatFs = unsafe { std::mem::zeroed() };
    if unsafe { c_fstatfs(descriptor, &mut status) } != 0 {
        return Err(operation(
            "inspect data-directory filesystem",
            Path::new("<root>"),
            io::Error::last_os_error(),
        ));
    }
    classify_linux_filesystem(status.f_type as u64)
}

#[cfg(target_os = "linux")]
fn classify_linux_filesystem(kind: u64) -> Result<AdmittedFilesystem, DataDirectoryError> {
    match kind {
        0x0000_EF53 => Ok(AdmittedFilesystem::Ext),
        0x5846_5342 => Ok(AdmittedFilesystem::Xfs),
        0x9123_683E => Ok(AdmittedFilesystem::Btrfs),
        0x0000_6969 => Err(DataDirectoryError::NetworkFilesystem("nfs".to_owned())),
        0xFF53_4D42 | 0xFE53_4D42 => {
            Err(DataDirectoryError::NetworkFilesystem("smb/cifs".to_owned()))
        }
        0x0102_1997 => Err(DataDirectoryError::NetworkFilesystem("9p".to_owned())),
        value => Err(DataDirectoryError::UnsupportedFilesystem(format!(
            "magic-{value:#x}"
        ))),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn admit_filesystem(_: RawFd) -> Result<AdmittedFilesystem, DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatform)
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct DarwinStatFs {
    f_bsize: u32,
    f_iosize: i32,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_fsid: [i32; 2],
    f_owner: u32,
    f_type: u32,
    f_flags: u32,
    f_fssubtype: u32,
    f_fstypename: [c_char; 16],
    f_mntonname: [c_char; 1024],
    f_mntfromname: [c_char; 1024],
    f_flags_ext: u32,
    f_reserved: [u32; 7],
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct LinuxStatFs {
    f_type: c_long,
    f_bsize: c_long,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_fsid: [i32; 2],
    f_namelen: c_long,
    f_frsize: c_long,
    f_flags: c_long,
    f_spare: [c_long; 4],
}

unsafe extern "C" {
    #[link_name = "open"]
    fn c_open(path: *const c_char, flags: c_int, ...) -> c_int;
    #[link_name = "openat"]
    fn c_openat(directory: c_int, path: *const c_char, flags: c_int, ...) -> c_int;
    #[link_name = "mkdirat"]
    fn c_mkdirat(directory: c_int, path: *const c_char, mode: ModeT) -> c_int;
    #[link_name = "renameat"]
    fn c_renameat(
        source_directory: c_int,
        source: *const c_char,
        destination_directory: c_int,
        destination: *const c_char,
    ) -> c_int;
    #[link_name = "unlinkat"]
    fn c_unlinkat(directory: c_int, path: *const c_char, flags: c_int) -> c_int;
    #[link_name = "flock"]
    fn c_flock(descriptor: c_int, operation: c_int) -> c_int;
    #[link_name = "geteuid"]
    fn c_geteuid() -> u32;
    #[link_name = "pipe"]
    fn c_pipe(descriptors: *mut c_int) -> c_int;
    #[link_name = "fcntl"]
    fn c_fcntl(descriptor: c_int, command: c_int, argument: c_int) -> c_int;
    #[link_name = "fstatfs"]
    fn c_fstatfs(descriptor: c_int, status: *mut PlatformStatFs) -> c_int;
}

#[cfg(target_os = "macos")]
type PlatformStatFs = DarwinStatFs;
#[cfg(target_os = "linux")]
type PlatformStatFs = LinuxStatFs;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
#[repr(C)]
struct PlatformStatFs;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{Seek, SeekFrom};
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    fn admitted_tempdir() -> Option<tempfile::TempDir> {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(DIRECTORY_MODE)).unwrap();
        match DataDirectory::admit(directory.path()) {
            Ok(_) => Some(directory),
            Err(DataDirectoryError::UnsupportedFilesystem(_)) => None,
            Err(error) => panic!("unexpected admission failure: {error}"),
        }
    }

    #[test]
    fn formation_paths_are_root_relative() {
        for formation_path in FormationPath::ALL {
            let path = RootRelativePath::try_from(formation_path).unwrap();
            assert!(!path.as_path().is_absolute());
        }
        for invalid in ["", "/outside", "../outside", "inside/../../outside"] {
            assert!(matches!(
                RootRelativePath::new(invalid),
                Err(DataDirectoryError::InvalidRelativePath(_))
            ));
        }
    }

    #[test]
    fn every_required_capability_is_fail_closed() {
        let mut capabilities = AdmissionCapabilities::PROVEN;
        let cases: [(
            RequiredCapability,
            fn(&mut AdmissionCapabilities) -> &mut bool,
        ); 8] = [
            (RequiredCapability::RootRelativeNoFollow, |value| {
                &mut value.root_relative_no_follow
            }),
            (RequiredCapability::OwnershipAndPermissions, |value| {
                &mut value.ownership_and_permissions
            }),
            (RequiredCapability::SameDeviceTemporaryPlacement, |value| {
                &mut value.same_device_temporary_placement
            }),
            (RequiredCapability::ExclusiveLocking, |value| {
                &mut value.exclusive_locking
            }),
            (RequiredCapability::FileSync, |value| &mut value.file_sync),
            (RequiredCapability::ParentDirectorySync, |value| {
                &mut value.parent_directory_sync
            }),
            (RequiredCapability::AtomicReplacement, |value| {
                &mut value.atomic_replacement
            }),
            (RequiredCapability::ProcessContainment, |value| {
                &mut value.process_containment
            }),
        ];
        for (expected, disable) in cases {
            capabilities = AdmissionCapabilities::PROVEN;
            *disable(&mut capabilities) = false;
            assert_eq!(
                capabilities.require_all().unwrap_err().to_string(),
                DataDirectoryError::MissingCapability(expected).to_string()
            );
        }
    }

    #[test]
    fn wrong_permissions_fail_before_probe_files_exist() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            DataDirectory::admit(directory.path()),
            Err(DataDirectoryError::UnsafePermissions { .. })
        ));
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn symlink_root_is_never_followed() {
        let parent = tempfile::tempdir().unwrap();
        let real_root = parent.path().join("real");
        fs::create_dir(&real_root).unwrap();
        fs::set_permissions(&real_root, fs::Permissions::from_mode(DIRECTORY_MODE)).unwrap();
        let linked_root = parent.path().join("linked");
        symlink(&real_root, &linked_root).unwrap();
        assert!(matches!(
            DataDirectory::admit(&linked_root),
            Err(DataDirectoryError::InvalidRoot(_)) | Err(DataDirectoryError::Operation { .. })
        ));
        assert_eq!(fs::read_dir(&real_root).unwrap().count(), 0);
    }

    #[test]
    fn admitted_root_rejects_symlink_components_and_replaces_durably() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        let lease = DataDirectory::admit(directory.path()).unwrap();
        for directory_path in [
            FormationPath::Journals,
            FormationPath::StagedLogs,
            FormationPath::FinalLogs,
            FormationPath::TemporaryFiles,
            FormationPath::Quarantine,
        ] {
            lease
                .ensure_directory(&RootRelativePath::try_from(directory_path).unwrap())
                .unwrap();
        }
        for file_path in [FormationPath::SqliteState, FormationPath::FormationManifest] {
            lease
                .create_new_file(&RootRelativePath::try_from(file_path).unwrap())
                .unwrap();
        }
        for formation_path in FormationPath::ALL {
            assert!(directory
                .path()
                .join(formation_path.relative_path())
                .exists());
        }
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), directory.path().join("escape")).unwrap();
        let escaped = RootRelativePath::new("escape/file").unwrap();
        assert!(lease.open_existing_file(&escaped, false).is_err());
        assert!(!outside.path().join("file").exists());

        let temporary = RootRelativePath::new("tmp/installing").unwrap();
        let destination = RootRelativePath::new("logs/final/installed").unwrap();
        let mut file = lease.create_new_file(&temporary).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"durable").unwrap();
        lease.durable_replace(&temporary, &destination).unwrap();
        let mut installed = lease.open_existing_file(&destination, false).unwrap();
        let mut contents = String::new();
        installed.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "durable");
    }

    #[test]
    fn unsafe_file_mode_and_cross_device_entries_are_refused() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        let lease = DataDirectory::admit(directory.path()).unwrap();
        let unsafe_file = directory.path().join("unsafe");
        fs::write(&unsafe_file, b"unsafe").unwrap();
        fs::set_permissions(&unsafe_file, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            lease.open_existing_file(&RootRelativePath::new("unsafe").unwrap(), false),
            Err(DataDirectoryError::UnsafePermissions { .. })
        ));
        let other_device = File::open("/dev/null").unwrap();
        assert!(matches!(
            validate_same_device(Path::new("/dev/null"), lease.root_device, &other_device),
            Err(DataDirectoryError::DifferentDevice(_))
        ));
    }

    #[test]
    fn a_second_handle_in_the_same_process_contends() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        let first = DataDirectory::admit(directory.path()).unwrap();
        assert!(matches!(
            DataDirectory::admit(directory.path()),
            Err(DataDirectoryError::AlreadyLocked(_))
        ));
        drop(first);
        DataDirectory::admit(directory.path()).unwrap();
    }

    #[test]
    fn child_process_lock_holder() {
        let Ok(path) = std::env::var("TICKR_DATA_DIRECTORY_LOCK_HELPER") else {
            return;
        };
        let lease = DataDirectory::admit(&path).unwrap();
        fs::write(Path::new(&path).join("holder-ready"), b"ready").unwrap();
        while !Path::new(&path).join("holder-release").exists() {
            thread::sleep(Duration::from_millis(10));
        }
        drop(lease);
    }

    #[test]
    fn independent_processes_contend_on_the_directory_lock() {
        let Some(directory) = admitted_tempdir() else {
            return;
        };
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "data_directory::tests::child_process_lock_holder",
            ])
            .env("TICKR_DATA_DIRECTORY_LOCK_HELPER", directory.path())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let ready = directory.path().join("holder-ready");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "child lock holder did not become ready");
        assert!(matches!(
            DataDirectory::admit(directory.path()),
            Err(DataDirectoryError::AlreadyLocked(_))
        ));
        fs::write(directory.path().join("holder-release"), b"release").unwrap();
        assert!(child.wait().unwrap().success());
        DataDirectory::admit(directory.path()).unwrap();
    }

    #[test]
    fn sqlite_urls_are_restricted_to_absolute_disk_paths() {
        assert_eq!(
            sqlite_path_from_url("sqlite:///var/lib/tickr/tickr.db?mode=rwc").unwrap(),
            PathBuf::from("/var/lib/tickr/tickr.db")
        );
        for invalid in [
            "sqlite::memory:",
            "sqlite://relative.db",
            "postgres:///var/lib/tickr/tickr.db",
            "sqlite:///tmp/tickr%20lite.db",
        ] {
            assert!(matches!(
                sqlite_path_from_url(invalid),
                Err(DataDirectoryError::UnsupportedSqliteUrl(_))
            ));
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_admits_known_local_filesystems_and_refuses_network_or_unknown_mounts() {
        assert_eq!(
            classify_macos_filesystem("apfs", true).unwrap(),
            AdmittedFilesystem::Apfs
        );
        assert_eq!(
            classify_macos_filesystem("hfs", true).unwrap(),
            AdmittedFilesystem::Hfs
        );
        assert!(matches!(
            classify_macos_filesystem("nfs", false),
            Err(DataDirectoryError::NetworkFilesystem(_))
        ));
        assert!(matches!(
            classify_macos_filesystem("mysteryfs", true),
            Err(DataDirectoryError::UnsupportedFilesystem(_))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_network_and_unknown_filesystems_are_refused() {
        assert_eq!(
            classify_linux_filesystem(0xEF53).unwrap(),
            AdmittedFilesystem::Ext
        );
        assert_eq!(
            classify_linux_filesystem(0x5846_5342).unwrap(),
            AdmittedFilesystem::Xfs
        );
        assert_eq!(
            classify_linux_filesystem(0x9123_683E).unwrap(),
            AdmittedFilesystem::Btrfs
        );
        for kind in [0x6969_u64, 0xFF53_4D42, 0x0102_1997] {
            assert!(matches!(
                classify_linux_filesystem(kind),
                Err(DataDirectoryError::NetworkFilesystem(_))
            ));
        }
        assert!(matches!(
            classify_linux_filesystem(0xDEAD_BEEF),
            Err(DataDirectoryError::UnsupportedFilesystem(_))
        ));
    }
}
