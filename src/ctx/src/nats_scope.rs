use async_nats::jetstream::kv;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_SCOPE_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_SCOPE_ROWS: usize = 4096;
pub const MAX_SCOPE_BYTES: usize = 64 * 1024 * 1024;

const MAX_NAMESPACE_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 512;
const METADATA_PREFIX: &str = "__tickr_scope_meta.";
const SNAPSHOT_MAGIC: &[u8] = b"TICKR_CTX_SCOPE\0\x01";
const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum NatsScopeError {
    #[error("scope namespace must contain 1 to {MAX_NAMESPACE_BYTES} bytes")]
    InvalidNamespace,
    #[error("scope owner must contain 1 to {MAX_OWNER_BYTES} bytes")]
    InvalidOwner,
    #[error("scope key must contain an owner prefix and at most {MAX_KEY_BYTES} bytes")]
    InvalidKey,
    #[error("scope value `{key}` contains {actual} bytes; limit is {limit}")]
    ValueLimit {
        key: String,
        actual: usize,
        limit: usize,
    },
    #[error("scope contains {actual} values; limit is {limit}")]
    RowLimit { actual: usize, limit: usize },
    #[error("scope contains {actual} value bytes; limit is {limit}")]
    ByteLimit { actual: usize, limit: usize },
    #[error("tickr-ctx scope `{0}` is missing")]
    Missing(String),
    #[error("tickr-ctx scope `{0}` is sealed for Compaction")]
    Sealed(String),
    #[error("tickr-ctx scope `{owner}` is cleaned after snapshot `{digest}`")]
    Cleaned { owner: String, digest: String },
    #[error("tickr-ctx scope is corrupt: {0}")]
    Corrupt(String),
    #[error("NATS ScopeStore operation failed: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsScopeEntry {
    pub key: String,
    pub envelope: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsScopeSnapshot {
    pub owner: String,
    pub entries: Vec<NatsScopeEntry>,
    pub bytes: Vec<u8>,
    pub digest: String,
    pub value_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ScopeState {
    Active,
    Mutating,
    Snapshotted,
    ArchiveCommitted,
    Cleaning,
    Cleaned,
    Corrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopeMetadata {
    protocol_version: u8,
    namespace: String,
    owner: String,
    state: ScopeState,
    row_count: usize,
    value_bytes: usize,
    snapshot_digest: Option<String>,
    diagnostic: Option<String>,
}

#[derive(Debug)]
struct VersionedMetadata {
    value: ScopeMetadata,
    revision: u64,
}

#[derive(Clone)]
pub struct NatsScopeStore {
    store: kv::Store,
    namespace: String,
}

impl NatsScopeStore {
    pub fn new(store: kv::Store, namespace: &str) -> Result<Self, NatsScopeError> {
        if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_BYTES {
            return Err(NatsScopeError::InvalidNamespace);
        }
        Ok(Self {
            store,
            namespace: namespace.to_owned(),
        })
    }

    pub fn raw_store(&self) -> &kv::Store {
        &self.store
    }

    pub async fn ensure_scope(&self, owner: &str) -> Result<(), NatsScopeError> {
        validate_owner(owner)?;
        let key = metadata_key(owner);
        if let Some(metadata) = self.load_metadata(owner).await? {
            self.require_identity(&metadata.value, owner)?;
            return match metadata.value.state {
                ScopeState::Active | ScopeState::Mutating => Ok(()),
                ScopeState::Snapshotted
                | ScopeState::ArchiveCommitted
                | ScopeState::Cleaning
                | ScopeState::Cleaned => Err(NatsScopeError::Sealed(owner.to_owned())),
                ScopeState::Corrupt => Err(NatsScopeError::Corrupt(
                    metadata
                        .value
                        .diagnostic
                        .unwrap_or_else(|| format!("scope `{owner}` is quarantined")),
                )),
            };
        }

        let metadata = ScopeMetadata {
            protocol_version: PROTOCOL_VERSION,
            namespace: self.namespace.clone(),
            owner: owner.to_owned(),
            state: ScopeState::Active,
            row_count: 0,
            value_bytes: 0,
            snapshot_digest: None,
            diagnostic: None,
        };
        match self
            .store
            .create(&key, encode_metadata(&metadata)?.into())
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => {
                let metadata = self.load_metadata(owner).await?.ok_or_else(|| {
                    NatsScopeError::Backend("scope identity creation was ambiguous".to_owned())
                })?;
                self.require_identity(&metadata.value, owner)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, NatsScopeError> {
        validate_key(key)?;
        self.store
            .get(key)
            .await
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(backend)
    }

    pub async fn keys(&self, prefix: &str) -> Result<Vec<String>, NatsScopeError> {
        let mut keys = match self.store.keys().await {
            Ok(keys) => keys,
            Err(error) if no_keys(&error.to_string()) => return Ok(Vec::new()),
            Err(error) => return Err(backend(error)),
        };
        let mut collected = Vec::new();
        while let Some(key) = keys.next().await {
            let key = key.map_err(backend)?;
            if !is_metadata_key(&key) && key.starts_with(prefix) {
                collected.push(key);
            }
        }
        collected.sort();
        Ok(collected)
    }

    pub async fn put(&self, key: String, envelope: Vec<u8>) -> Result<(), NatsScopeError> {
        validate_key(&key)?;
        if envelope.len() > MAX_SCOPE_VALUE_BYTES {
            return Err(NatsScopeError::ValueLimit {
                key,
                actual: envelope.len(),
                limit: MAX_SCOPE_VALUE_BYTES,
            });
        }
        validate_envelope(&key, &envelope)?;
        let owner = owner_from_key(&key)?.to_owned();
        let locked = self.lock_active(&owner).await?;
        let values = self.collect_owner(&owner).await?;
        let existing_bytes = values
            .iter()
            .find(|value| value.key == key)
            .map_or(0, |value| value.envelope.len());
        let row_count = values.len() + usize::from(existing_bytes == 0);
        let value_bytes = values
            .iter()
            .map(|value| value.envelope.len())
            .sum::<usize>()
            .saturating_sub(existing_bytes)
            .saturating_add(envelope.len());
        if let Err(error) = validate_bounds(row_count, value_bytes) {
            self.unlock_active(
                locked,
                values.len(),
                values.iter().map(|v| v.envelope.len()).sum(),
            )
            .await?;
            return Err(error);
        }

        if let Err(error) = self.store.put(&key, envelope.into()).await {
            self.unlock_active(
                locked,
                values.len(),
                values.iter().map(|v| v.envelope.len()).sum(),
            )
            .await?;
            return Err(backend(error));
        }
        self.unlock_active(locked, row_count, value_bytes).await
    }

    pub async fn delete(&self, key: &str) -> Result<bool, NatsScopeError> {
        validate_key(key)?;
        let owner = owner_from_key(key)?.to_owned();
        let locked = self.lock_active(&owner).await?;
        let values = self.collect_owner(&owner).await?;
        let existing = values.iter().find(|value| value.key == key);
        let Some(existing) = existing else {
            self.unlock_active(
                locked,
                values.len(),
                values.iter().map(|value| value.envelope.len()).sum(),
            )
            .await?;
            return Ok(false);
        };
        let value_bytes = values
            .iter()
            .map(|value| value.envelope.len())
            .sum::<usize>()
            .saturating_sub(existing.envelope.len());
        self.store.delete(key).await.map_err(backend)?;
        self.unlock_active(locked, values.len() - 1, value_bytes)
            .await?;
        Ok(true)
    }

    pub async fn snapshot(&self, owner: &str) -> Result<NatsScopeSnapshot, NatsScopeError> {
        validate_owner(owner)?;
        let mut metadata = self
            .load_metadata(owner)
            .await?
            .ok_or_else(|| NatsScopeError::Missing(owner.to_owned()))?;
        self.require_identity(&metadata.value, owner)?;
        if metadata.value.state == ScopeState::Mutating {
            metadata = self.recover_mutation(metadata).await?;
        }
        match metadata.value.state {
            ScopeState::Active => {
                metadata.value.state = ScopeState::Snapshotted;
                let entries = self.collect_owner(owner).await?;
                let snapshot = snapshot_from_entries(owner, entries)?;
                metadata.value.row_count = snapshot.entries.len();
                metadata.value.value_bytes = snapshot.value_bytes;
                metadata.value.snapshot_digest = Some(snapshot.digest.clone());
                self.update_metadata(&metadata).await?;
                Ok(snapshot)
            }
            ScopeState::Snapshotted | ScopeState::ArchiveCommitted => {
                let snapshot = snapshot_from_entries(owner, self.collect_owner(owner).await?)?;
                let expected = metadata.value.snapshot_digest.as_deref().ok_or_else(|| {
                    NatsScopeError::Corrupt(format!("scope `{owner}` has no snapshot digest"))
                })?;
                if expected != snapshot.digest
                    || metadata.value.row_count != snapshot.entries.len()
                    || metadata.value.value_bytes != snapshot.value_bytes
                {
                    return Err(NatsScopeError::Corrupt(format!(
                        "scope `{owner}` changed after its Compaction snapshot was sealed"
                    )));
                }
                Ok(snapshot)
            }
            ScopeState::Cleaning => Err(NatsScopeError::Corrupt(format!(
                "scope `{owner}` cleanup is incomplete"
            ))),
            ScopeState::Cleaned => Err(NatsScopeError::Cleaned {
                owner: owner.to_owned(),
                digest: metadata.value.snapshot_digest.ok_or_else(|| {
                    NatsScopeError::Corrupt(format!(
                        "cleaned scope `{owner}` has no snapshot digest"
                    ))
                })?,
            }),
            ScopeState::Corrupt => Err(NatsScopeError::Corrupt(
                metadata
                    .value
                    .diagnostic
                    .unwrap_or_else(|| format!("scope `{owner}` is quarantined")),
            )),
            ScopeState::Mutating => unreachable!("mutation recovery returned a mutating state"),
        }
    }

    pub async fn mark_archive_committed(
        &self,
        owner: &str,
        digest: &str,
    ) -> Result<(), NatsScopeError> {
        let mut metadata = self
            .load_metadata(owner)
            .await?
            .ok_or_else(|| NatsScopeError::Missing(owner.to_owned()))?;
        self.require_identity(&metadata.value, owner)?;
        if metadata.value.snapshot_digest.as_deref() != Some(digest) {
            return Err(NatsScopeError::Corrupt(format!(
                "scope `{owner}` archive digest does not match its sealed snapshot"
            )));
        }
        match metadata.value.state {
            ScopeState::Snapshotted => {
                metadata.value.state = ScopeState::ArchiveCommitted;
                self.update_metadata(&metadata).await?;
                Ok(())
            }
            ScopeState::ArchiveCommitted | ScopeState::Cleaning | ScopeState::Cleaned => Ok(()),
            other => Err(NatsScopeError::Corrupt(format!(
                "scope `{owner}` cannot record archive commit from state {other:?}"
            ))),
        }
    }

    pub async fn cleanup_archived(&self, owner: &str) -> Result<(), NatsScopeError> {
        let mut metadata = self
            .load_metadata(owner)
            .await?
            .ok_or_else(|| NatsScopeError::Missing(owner.to_owned()))?;
        self.require_identity(&metadata.value, owner)?;
        match metadata.value.state {
            ScopeState::Cleaned => return Ok(()),
            ScopeState::ArchiveCommitted => {
                metadata.value.state = ScopeState::Cleaning;
                let revision = self.update_metadata(&metadata).await?;
                metadata.revision = revision;
            }
            ScopeState::Cleaning => {}
            other => {
                return Err(NatsScopeError::Corrupt(format!(
                    "scope `{owner}` cleanup requires committed archive state, found {other:?}"
                )))
            }
        }

        for entry in self.collect_owner(owner).await? {
            self.store.delete(&entry.key).await.map_err(backend)?;
        }
        metadata.value.state = ScopeState::Cleaned;
        metadata.value.row_count = 0;
        metadata.value.value_bytes = 0;
        self.update_metadata(&metadata).await?;
        Ok(())
    }

    async fn lock_active(&self, owner: &str) -> Result<VersionedMetadata, NatsScopeError> {
        self.ensure_scope(owner).await?;
        let mut metadata = self
            .load_metadata(owner)
            .await?
            .ok_or_else(|| NatsScopeError::Missing(owner.to_owned()))?;
        self.require_identity(&metadata.value, owner)?;
        if metadata.value.state == ScopeState::Mutating {
            metadata = self.recover_mutation(metadata).await?;
        }
        match metadata.value.state {
            ScopeState::Active => {
                metadata.value.state = ScopeState::Mutating;
                let revision = self.update_metadata(&metadata).await?;
                metadata.revision = revision;
                Ok(metadata)
            }
            ScopeState::Snapshotted
            | ScopeState::ArchiveCommitted
            | ScopeState::Cleaning
            | ScopeState::Cleaned => Err(NatsScopeError::Sealed(owner.to_owned())),
            ScopeState::Corrupt => Err(NatsScopeError::Corrupt(
                metadata
                    .value
                    .diagnostic
                    .unwrap_or_else(|| format!("scope `{owner}` is quarantined")),
            )),
            ScopeState::Mutating => unreachable!("mutation recovery returned a mutating state"),
        }
    }

    async fn unlock_active(
        &self,
        mut metadata: VersionedMetadata,
        row_count: usize,
        value_bytes: usize,
    ) -> Result<(), NatsScopeError> {
        metadata.value.state = ScopeState::Active;
        metadata.value.row_count = row_count;
        metadata.value.value_bytes = value_bytes;
        self.update_metadata(&metadata).await?;
        Ok(())
    }

    async fn recover_mutation(
        &self,
        mut metadata: VersionedMetadata,
    ) -> Result<VersionedMetadata, NatsScopeError> {
        let entries = self.collect_owner(&metadata.value.owner).await?;
        let value_bytes = entries.iter().map(|entry| entry.envelope.len()).sum();
        if let Err(error) = validate_bounds(entries.len(), value_bytes) {
            metadata.value.state = ScopeState::Corrupt;
            metadata.value.diagnostic = Some(error.to_string());
        } else if let Some(error) = entries
            .iter()
            .find_map(|entry| validate_envelope(&entry.key, &entry.envelope).err())
        {
            metadata.value.state = ScopeState::Corrupt;
            metadata.value.diagnostic = Some(error.to_string());
        } else {
            metadata.value.state = ScopeState::Active;
            metadata.value.row_count = entries.len();
            metadata.value.value_bytes = value_bytes;
        }
        metadata.revision = self.update_metadata(&metadata).await?;
        if metadata.value.state == ScopeState::Corrupt {
            return Err(NatsScopeError::Corrupt(
                metadata.value.diagnostic.clone().unwrap_or_default(),
            ));
        }
        Ok(metadata)
    }

    async fn collect_owner(&self, owner: &str) -> Result<Vec<NatsScopeEntry>, NatsScopeError> {
        let prefix = format!("{owner}/");
        let keys = self.keys(&prefix).await?;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let entry = self
                .store
                .entry(&key)
                .await
                .map_err(backend)?
                .ok_or_else(|| {
                    NatsScopeError::Corrupt(format!("scope value `{key}` disappeared during read"))
                })?;
            if entry.operation != kv::Operation::Put {
                return Err(NatsScopeError::Corrupt(format!(
                    "scope value `{key}` is not readable"
                )));
            }
            let envelope = entry.value.to_vec();
            validate_envelope(&key, &envelope)?;
            values.push(NatsScopeEntry { key, envelope });
        }
        values.sort_by(|left, right| left.key.cmp(&right.key));
        validate_bounds(
            values.len(),
            values.iter().map(|value| value.envelope.len()).sum(),
        )?;
        Ok(values)
    }

    async fn load_metadata(
        &self,
        owner: &str,
    ) -> Result<Option<VersionedMetadata>, NatsScopeError> {
        let key = metadata_key(owner);
        let Some(entry) = self.store.entry(&key).await.map_err(backend)? else {
            return Ok(None);
        };
        if entry.operation != kv::Operation::Put {
            return Ok(None);
        }
        let value: ScopeMetadata = serde_json::from_slice(&entry.value)
            .map_err(|error| NatsScopeError::Corrupt(format!("metadata `{key}`: {error}")))?;
        Ok(Some(VersionedMetadata {
            value,
            revision: entry.revision,
        }))
    }

    async fn update_metadata(&self, metadata: &VersionedMetadata) -> Result<u64, NatsScopeError> {
        self.store
            .update(
                metadata_key(&metadata.value.owner),
                encode_metadata(&metadata.value)?.into(),
                metadata.revision,
            )
            .await
            .map_err(backend)
    }

    fn require_identity(
        &self,
        metadata: &ScopeMetadata,
        owner: &str,
    ) -> Result<(), NatsScopeError> {
        if metadata.protocol_version == PROTOCOL_VERSION
            && metadata.namespace == self.namespace
            && metadata.owner == owner
        {
            Ok(())
        } else {
            Err(NatsScopeError::Corrupt(format!(
                "scope `{owner}` metadata identity does not match its namespace and protocol"
            )))
        }
    }
}

pub fn is_metadata_key(key: &str) -> bool {
    key.starts_with(METADATA_PREFIX)
}

fn metadata_key(owner: &str) -> String {
    format!("{METADATA_PREFIX}{owner}")
}

fn owner_from_key(key: &str) -> Result<&str, NatsScopeError> {
    let (owner, _) = key.split_once('/').ok_or(NatsScopeError::InvalidKey)?;
    validate_owner(owner)?;
    Ok(owner)
}

fn validate_owner(owner: &str) -> Result<(), NatsScopeError> {
    validate_segment(owner, MAX_OWNER_BYTES).map_err(|_| NatsScopeError::InvalidOwner)
}

fn validate_key(key: &str) -> Result<(), NatsScopeError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES || is_metadata_key(key) {
        return Err(NatsScopeError::InvalidKey);
    }
    owner_from_key(key).map(|_| ())
}

fn validate_segment(value: &str, limit: usize) -> Result<(), ()> {
    if value.is_empty() || value.len() > limit {
        return Err(());
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'=' | b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_envelope(key: &str, envelope: &[u8]) -> Result<(), NatsScopeError> {
    let value: serde_json::Value = serde_json::from_slice(envelope).map_err(|error| {
        NatsScopeError::Corrupt(format!("scope value `{key}` is not an envelope: {error}"))
    })?;
    match value.get("v").and_then(serde_json::Value::as_u64) {
        Some(1 | 2) => Ok(()),
        Some(version) => Err(NatsScopeError::Corrupt(format!(
            "scope value `{key}` uses unknown envelope version {version}"
        ))),
        None => Err(NatsScopeError::Corrupt(format!(
            "scope value `{key}` has no envelope version"
        ))),
    }
}

fn validate_bounds(row_count: usize, value_bytes: usize) -> Result<(), NatsScopeError> {
    if row_count > MAX_SCOPE_ROWS {
        return Err(NatsScopeError::RowLimit {
            actual: row_count,
            limit: MAX_SCOPE_ROWS,
        });
    }
    if value_bytes > MAX_SCOPE_BYTES {
        return Err(NatsScopeError::ByteLimit {
            actual: value_bytes,
            limit: MAX_SCOPE_BYTES,
        });
    }
    Ok(())
}

pub fn snapshot_from_entries(
    owner: &str,
    mut entries: Vec<NatsScopeEntry>,
) -> Result<NatsScopeSnapshot, NatsScopeError> {
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let value_bytes = entries.iter().map(|entry| entry.envelope.len()).sum();
    validate_bounds(entries.len(), value_bytes)?;
    let capacity = SNAPSHOT_MAGIC.len()
        + 4
        + entries
            .iter()
            .map(|entry| 8 + entry.key.len() + entry.envelope.len())
            .sum::<usize>();
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(entries.len())
            .expect("scope row bound fits u32")
            .to_be_bytes(),
    );
    for entry in &entries {
        append_len_prefixed(&mut bytes, entry.key.as_bytes());
        append_len_prefixed(&mut bytes, &entry.envelope);
    }
    let digest = hex::encode(Sha256::digest(&bytes));
    Ok(NatsScopeSnapshot {
        owner: owner.to_owned(),
        entries,
        bytes,
        digest,
        value_bytes,
    })
}

fn append_len_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .expect("scope bounds fit u32")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

fn encode_metadata(metadata: &ScopeMetadata) -> Result<Vec<u8>, NatsScopeError> {
    serde_json::to_vec(metadata)
        .map_err(|error| NatsScopeError::Backend(format!("encoding scope metadata: {error}")))
}

fn no_keys(message: &str) -> bool {
    message.to_ascii_lowercase().contains("no keys")
}

fn backend(error: impl std::fmt::Display) -> NatsScopeError {
    NatsScopeError::Backend(error.to_string())
}
