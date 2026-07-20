//! Tenant identity for the data plane.
//!
//! A [`TenantId`] has one rendering everywhere — the full 36-char hyphenated
//! UUID string — so the same value serves unchanged as identity-seed segment,
//! storage key prefix, wire field, and Postgres column. One rendering means zero
//! conversion sites.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Environment variable naming the tenant slug the data plane derives its
/// [`TenantId`] from. Mandatory — an absent or blank value is a boot error
/// (there is no default tenant to fall back to).
pub const TENANT_SLUG_ENV: &str = "TICKR_TENANT_SLUG";

/// Namespace OID for deriving a [`TenantId`] from a tenant slug via UUIDv5.
/// Distinct from `Uuid::NAMESPACE_OID` (which seeds workflow identity) so
/// tenant derivation never shares a preimage space with workflow identity.
/// The 16 ASCII bytes of `tickr-tenant-oid` are a fixed, self-documenting seed.
const TENANT_NAMESPACE_OID: Uuid = Uuid::from_bytes(*b"tickr-tenant-oid");

/// Tenant identity. The tenant slug is distinct from a workflow `Slug`: this is
/// the tenant-identity input, keyed apart from the per-workflow identity
/// segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(Uuid);

impl TenantId {
    /// Derive a `TenantId` deterministically from a tenant slug:
    /// `UUIDv5(tenant-namespace-oid, slug)`. The same slug always yields the
    /// same id — across restarts and across every conductor that shares the
    /// slug — so the slug alone propagates identity across a multi-conductor
    /// tenant fleet.
    pub fn from_slug(slug: &str) -> Self {
        TenantId(Uuid::new_v5(&TENANT_NAMESPACE_OID, slug.as_bytes()))
    }

    /// Resolve the data plane's `TenantId` from the environment.
    /// [`TENANT_SLUG_ENV`] is **mandatory** — an absent or blank value panics
    /// (the component refuses to boot without a tenant, matching the fail-fast
    /// boot convention). There is no default tenant to fall back to.
    pub fn from_env() -> Self {
        Self::from_slug(&required_slug_from_env())
    }

    /// The resolved tenant slug from the environment — the human-readable name
    /// the id derives from (the one-way UUIDv5 in [`from_slug`] can't be
    /// reversed back into it). Panics on an absent/blank slug, same as
    /// [`from_env`]. Used by the API component's tenant endpoint to show the
    /// operator which tenant it serves.
    pub fn slug_from_env() -> String {
        required_slug_from_env()
    }

    /// Borrow the underlying UUID for the few callers that need the raw value
    /// (e.g. seeding a derived id). Every user-facing rendering goes through
    /// [`Display`](fmt::Display).
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Derive a workflow id from its identity seed `tenant.namespace.slug` via
/// UUIDv5. This is the single source of truth for workflow identity, shared by
/// both planes: the conductor's parser stamps a workflow's id with it at
/// registration, and the server's admission gate re-derives with it and rejects
/// on mismatch. Because both sides call this one function, the recompute is
/// byte-identical to the stamp by construction — it can never drift from the
/// derivation it validates.
///
/// `namespace` must already be normalized by the caller (empty → `default`),
/// exactly as the stamping site normalizes it, so recompute and stamp agree.
/// The tenant renders dot-free (hyphenated UUID) and namespace/slug forbid `.`
/// by the kebab grammar, so the dot separators stay injective across segments.
///
/// Seeds over `Uuid::NAMESPACE_OID`, distinct from [`TenantId::from_slug`]'s
/// [`TENANT_NAMESPACE_OID`], so workflow identity and tenant identity never
/// share a preimage space.
pub fn derive_workflow_id(tenant: TenantId, namespace: &str, slug: &str) -> Uuid {
    let identity_seed = format!("{tenant}.{namespace}.{slug}");
    Uuid::new_v5(&Uuid::NAMESPACE_OID, identity_seed.as_bytes())
}

/// Derive the deterministic id for an ordinary scheduled workflow instance.
///
/// The first eight bytes come from `workflow_id`; the final eight come from
/// the SHA-256 digest of the scheduled timestamp's RFC3339 rendering. The
/// result carries UUIDv4 version and variant bits to preserve the established
/// regular-run id format and keep it distinct from replay's UUIDv5 ids.
pub fn derive_scheduled_workflow_instance_id(
    workflow_id: Uuid,
    scheduled_at: DateTime<Utc>,
) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&workflow_id.as_bytes()[..8]);
    let hash = Sha256::digest(scheduled_at.to_rfc3339().as_bytes());
    bytes[8..].copy_from_slice(&hash[..8]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Read the **mandatory** tenant slug from the environment: the trimmed,
/// non-blank [`TENANT_SLUG_ENV`] value. Panics naming the variable when it is
/// absent or blank — tenant is mandatory and there is no default to fall back
/// to, so a missing slug is a deploy misconfiguration caught at boot.
fn required_slug_from_env() -> String {
    let raw = std::env::var(TENANT_SLUG_ENV).unwrap_or_default();
    let slug = raw.trim();
    assert!(
        !slug.is_empty(),
        "{TENANT_SLUG_ENV} is required (a non-blank tenant slug) — set it before starting this component; there is no default tenant"
    );
    slug.to_string()
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hyphenated is uuid's own Display default; spelling it out pins the
        // single rendering so it can't silently drift to simple/urn form.
        write!(f, "{}", self.0.as_hyphenated())
    }
}

impl FromStr for TenantId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(TenantId)
    }
}

impl From<Uuid> for TenantId {
    fn from(id: Uuid) -> Self {
        TenantId(id)
    }
}

impl From<TenantId> for Uuid {
    fn from(tenant: TenantId) -> Self {
        tenant.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_slug_is_deterministic() {
        // Same slug -> same id, every time. This is what lets every conductor
        // of a tenant agree by reading the same slug.
        assert_eq!(TenantId::from_slug("acme"), TenantId::from_slug("acme"));
    }

    #[test]
    fn distinct_slugs_produce_distinct_ids() {
        assert_ne!(TenantId::from_slug("acme"), TenantId::from_slug("globex"));
    }

    #[test]
    fn renders_as_hyphenated_uuid_and_round_trips_with_no_conversion() {
        let id = TenantId::from_slug("acme");
        let rendered = id.to_string();
        assert_eq!(rendered.len(), 36);
        assert_eq!(rendered.matches('-').count(), 4);
        // The single rendering parses straight back to the same value.
        assert_eq!(rendered.parse::<TenantId>().unwrap(), id);
    }

    #[test]
    fn uuid_round_trips_without_a_conversion_step() {
        let id = TenantId::from_slug("acme");
        assert_eq!(TenantId::from(id.as_uuid()), id);
    }

    #[test]
    fn from_env_reads_the_named_variable_trimmed() {
        // nextest runs each test in its own process, so mutating the real
        // environment here is isolated from other tests.
        std::env::set_var(TENANT_SLUG_ENV, "  acme  ");
        // Trimmed, so a padded value resolves the same as its bare form.
        assert_eq!(TenantId::from_env(), TenantId::from_slug("acme"));
        assert_eq!(TenantId::slug_from_env(), "acme");
        // The rendered slug always derives the rendered id — they can't disagree.
        assert_eq!(
            TenantId::from_env(),
            TenantId::from_slug(&TenantId::slug_from_env())
        );
        std::env::remove_var(TENANT_SLUG_ENV);
    }

    #[test]
    fn derive_workflow_id_pins_the_uuidv5_vector() {
        // Pin the derivation against a hand-computed vector so a future change
        // to the OID namespace or the `tenant.namespace.slug` seed format fails
        // loudly here, at unit level, rather than only surfacing as rejected
        // registrations. Both planes must derive the same id — the admission
        // gate re-derives and rejects on mismatch.
        let tenant = TenantId::from_slug("acme");
        // from_slug is deterministic, so its rendering is a fixed vector too.
        assert_eq!(tenant.to_string(), "8f51db61-785a-5bad-b6c9-e92abfcf5ad7");
        // UUIDv5(NAMESPACE_OID, "8f51db61-...-e92abfcf5ad7.reporting.daily-sync").
        let id = derive_workflow_id(tenant, "reporting", "daily-sync");
        assert_eq!(id.to_string(), "62e7d2fa-5996-5458-868d-77eb2e46fb1f");
        // It is a UUIDv5, not a v4 — the derivation is deterministic.
        assert_eq!(id.get_version_num(), 5);
    }

    #[test]
    fn derive_workflow_id_is_deterministic_and_segment_sensitive() {
        let tenant = TenantId::from_slug("acme");
        let base = derive_workflow_id(tenant, "reporting", "daily-sync");
        // Same inputs -> same id, so the conductor's stamp and the server's
        // recompute agree by construction.
        assert_eq!(base, derive_workflow_id(tenant, "reporting", "daily-sync"));
        // Any changed segment reforges the id — the admission gate's basis for
        // rejecting a wrong-tenant / wrong-namespace / wrong-slug stamp.
        assert_ne!(
            base,
            derive_workflow_id(TenantId::from_slug("globex"), "reporting", "daily-sync")
        );
        assert_ne!(base, derive_workflow_id(tenant, "billing", "daily-sync"));
        assert_ne!(
            base,
            derive_workflow_id(tenant, "reporting", "nightly-sync")
        );
    }

    #[test]
    fn derive_scheduled_workflow_instance_id_pins_known_vectors() {
        let first_workflow_id = Uuid::parse_str("7c2d9f7e-8a4b-4c3d-b2a1-0f9e8d7c6b5a").unwrap();
        let first_scheduled_at = "2024-01-02T03:04:05Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            derive_scheduled_workflow_instance_id(first_workflow_id, first_scheduled_at),
            Uuid::parse_str("7c2d9f7e-8a4b-4c3d-8940-d0ac18b0aebd").unwrap()
        );

        let second_workflow_id = Uuid::parse_str("00000000-1111-2222-3333-444444444444").unwrap();
        let second_scheduled_at = "2030-12-31T23:59:59.123456789Z"
            .parse::<DateTime<Utc>>()
            .unwrap();
        assert_eq!(
            derive_scheduled_workflow_instance_id(second_workflow_id, second_scheduled_at),
            Uuid::parse_str("00000000-1111-4222-9ce5-d5769cebee00").unwrap()
        );
    }

    #[test]
    #[should_panic(expected = "TICKR_TENANT_SLUG is required")]
    fn from_env_panics_when_the_slug_is_absent() {
        // Tenant is mandatory — no default to fall back to; an unconfigured
        // deploy fails fast rather than resolving to a stand-in tenant.
        std::env::remove_var(TENANT_SLUG_ENV);
        let _ = TenantId::from_env();
    }

    #[test]
    #[should_panic(expected = "TICKR_TENANT_SLUG is required")]
    fn from_env_panics_when_the_slug_is_blank() {
        std::env::set_var(TENANT_SLUG_ENV, "   ");
        let _ = TenantId::from_env();
    }
}
