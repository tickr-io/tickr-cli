use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serializer};

use crate::tenant::TenantId;

pub const INSTALLATION_BOOTSTRAP_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapValidationError {
    UnsupportedSchema(u32),
    UnsafeTenantSlug,
    MalformedTenantId,
    InconsistentTenantId,
    InvalidTenantTier,
    MissingControlPlaneEndpoints,
    InvalidFormationProfile,
    MissingCompatibility,
    InvalidAuthenticationMode,
    InvalidTimestamp(&'static str),
    ExpiredBootstrap,
    MissingVariant,
    InvalidVariantCombination,
    MissingCredential,
    InvalidCredential,
    InvalidCredentialLifecycle,
    ExpiredCredential,
    MissingLease,
    InvalidLease,
    InvalidLeaseLifecycle,
    ExpiredLease,
    GuestCredentialLeaseMismatch,
}

impl fmt::Display for BootstrapValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported InstallationBootstrap schema {version}"
                )
            }
            Self::UnsafeTenantSlug => formatter.write_str("unsafe Tenant slug"),
            Self::MalformedTenantId => formatter.write_str("malformed TenantId"),
            Self::InconsistentTenantId => {
                formatter.write_str("Tenant slug and TenantId are inconsistent")
            }
            Self::InvalidTenantTier => formatter.write_str("invalid Tenant tier"),
            Self::MissingControlPlaneEndpoints => {
                formatter.write_str("missing Control-plane endpoints")
            }
            Self::InvalidFormationProfile => formatter.write_str("invalid Formation profile"),
            Self::MissingCompatibility => {
                formatter.write_str("missing Tickr Lite compatibility requirement")
            }
            Self::InvalidAuthenticationMode => {
                formatter.write_str("invalid Installation authentication mode")
            }
            Self::InvalidTimestamp(field) => write!(formatter, "invalid {field} timestamp"),
            Self::ExpiredBootstrap => formatter.write_str("InstallationBootstrap has expired"),
            Self::MissingVariant => formatter.write_str("missing InstallationBootstrap variant"),
            Self::InvalidVariantCombination => {
                formatter.write_str("invalid InstallationBootstrap variant combination")
            }
            Self::MissingCredential => formatter.write_str("missing Installation credential"),
            Self::InvalidCredential => formatter.write_str("invalid Installation credential"),
            Self::InvalidCredentialLifecycle => {
                formatter.write_str("invalid Installation credential lifecycle")
            }
            Self::ExpiredCredential => formatter.write_str("Installation credential has expired"),
            Self::MissingLease => formatter.write_str("missing Guest lease"),
            Self::InvalidLease => formatter.write_str("invalid Guest lease"),
            Self::InvalidLeaseLifecycle => formatter.write_str("invalid Guest lease lifecycle"),
            Self::ExpiredLease => formatter.write_str("Guest lease has expired"),
            Self::GuestCredentialLeaseMismatch => {
                formatter.write_str("Guest credential and lease expiry are inconsistent")
            }
        }
    }
}

impl std::error::Error for BootstrapValidationError {}

impl InstallationBootstrap {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), BootstrapValidationError> {
        if self.schema_version != INSTALLATION_BOOTSTRAP_SCHEMA_VERSION {
            return Err(BootstrapValidationError::UnsupportedSchema(
                self.schema_version,
            ));
        }
        if !is_safe_tenant_slug(&self.tenant_slug) {
            return Err(BootstrapValidationError::UnsafeTenantSlug);
        }
        let tenant_id = self
            .tenant_id
            .parse::<TenantId>()
            .map_err(|_| BootstrapValidationError::MalformedTenantId)?;
        if tenant_id != TenantId::from_slug(&self.tenant_slug) {
            return Err(BootstrapValidationError::InconsistentTenantId);
        }

        let tier = TenantTier::try_from(self.tenant_tier)
            .map_err(|_| BootstrapValidationError::InvalidTenantTier)?;
        if tier == TenantTier::Unspecified {
            return Err(BootstrapValidationError::InvalidTenantTier);
        }
        let endpoints = self
            .control_plane
            .as_ref()
            .filter(|value| !value.http.trim().is_empty() && !value.relay.trim().is_empty())
            .ok_or(BootstrapValidationError::MissingControlPlaneEndpoints)?;
        let _ = endpoints;

        let formation = FormationProfile::try_from(self.formation_profile)
            .map_err(|_| BootstrapValidationError::InvalidFormationProfile)?;
        if formation == FormationProfile::Unspecified {
            return Err(BootstrapValidationError::InvalidFormationProfile);
        }
        self.compatibility
            .as_ref()
            .filter(|value| !value.version_requirement.trim().is_empty())
            .ok_or(BootstrapValidationError::MissingCompatibility)?;
        let authentication = AuthenticationMode::try_from(self.authentication)
            .map_err(|_| BootstrapValidationError::InvalidAuthenticationMode)?;
        if authentication == AuthenticationMode::Unspecified {
            return Err(BootstrapValidationError::InvalidAuthenticationMode);
        }

        let bootstrap_expires_at = self
            .bootstrap_expires_at
            .as_deref()
            .map(|value| parse_timestamp(value, "bootstrap expiry"))
            .transpose()?;
        if bootstrap_expires_at.is_some_and(|expires_at| expires_at <= now) {
            return Err(BootstrapValidationError::ExpiredBootstrap);
        }

        match self.variant.as_ref() {
            Some(installation_bootstrap::Variant::Authenticated(variant)) => {
                if !matches!(tier, TenantTier::Solo | TenantTier::Enterprise)
                    || authentication != AuthenticationMode::Required
                {
                    return Err(BootstrapValidationError::InvalidVariantCombination);
                }
                validate_credential(variant.credential.as_ref(), now)?;
            }
            Some(installation_bootstrap::Variant::Guest(variant)) => {
                if tier != TenantTier::Guest || authentication != AuthenticationMode::Required {
                    return Err(BootstrapValidationError::InvalidVariantCombination);
                }
                let credential_expiry = validate_credential(variant.credential.as_ref(), now)?;
                let lease_expiry = validate_lease(variant.lease.as_ref(), now)?;
                if credential_expiry != lease_expiry {
                    return Err(BootstrapValidationError::GuestCredentialLeaseMismatch);
                }
                if bootstrap_expires_at.is_some_and(|expires_at| expires_at > lease_expiry) {
                    return Err(BootstrapValidationError::InvalidVariantCombination);
                }
            }
            Some(installation_bootstrap::Variant::Disabled(_)) => {
                if tier != TenantTier::Development
                    || authentication != AuthenticationMode::None
                    || bootstrap_expires_at.is_some()
                {
                    return Err(BootstrapValidationError::InvalidVariantCombination);
                }
            }
            None => return Err(BootstrapValidationError::MissingVariant),
        }

        Ok(())
    }
}

fn validate_credential(
    credential: Option<&InstallationCredential>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, BootstrapValidationError> {
    let credential = credential.ok_or(BootstrapValidationError::MissingCredential)?;
    if credential.credential_id.trim().is_empty()
        || !is_canonical_credential(&credential.credential)
    {
        return Err(BootstrapValidationError::InvalidCredential);
    }
    let created_at = parse_timestamp(&credential.created_at, "credential creation")?;
    let expires_at = parse_timestamp(&credential.expires_at, "credential expiry")?;
    if created_at >= expires_at {
        return Err(BootstrapValidationError::InvalidCredentialLifecycle);
    }
    if expires_at <= now {
        return Err(BootstrapValidationError::ExpiredCredential);
    }
    Ok(expires_at)
}

fn validate_lease(
    lease: Option<&GuestLease>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, BootstrapValidationError> {
    let lease = lease.ok_or(BootstrapValidationError::MissingLease)?;
    if lease.lease_id.trim().is_empty() {
        return Err(BootstrapValidationError::InvalidLease);
    }
    let created_at = parse_timestamp(&lease.created_at, "lease creation")?;
    let expires_at = parse_timestamp(&lease.expires_at, "lease expiry")?;
    if created_at >= expires_at {
        return Err(BootstrapValidationError::InvalidLeaseLifecycle);
    }
    if expires_at <= now {
        return Err(BootstrapValidationError::ExpiredLease);
    }
    Ok(expires_at)
}

fn parse_timestamp(
    value: &str,
    field: &'static str,
) -> Result<DateTime<Utc>, BootstrapValidationError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| BootstrapValidationError::InvalidTimestamp(field))
}

fn is_safe_tenant_slug(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn is_canonical_credential(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

macro_rules! string_enum_serde {
    ($module:ident, $type:ty, {$($wire:literal => $variant:path),+ $(,)?}) => {
        pub(crate) mod $module {
            use super::*;

            pub fn serialize<S>(value: &i32, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let value = <$type>::try_from(*value)
                    .map_err(serde::ser::Error::custom)?;
                let name = match value {
                    $($variant => $wire,)+
                };
                serializer.serialize_str(name)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                match value.as_str() {
                    $($wire => Ok($variant as i32),)+
                    _ => Err(serde::de::Error::unknown_variant(
                        &value,
                        &[$($wire),+],
                    )),
                }
            }
        }
    };
}

string_enum_serde!(serde_tenant_tier, TenantTier, {
    "unspecified" => TenantTier::Unspecified,
    "guest" => TenantTier::Guest,
    "solo" => TenantTier::Solo,
    "enterprise" => TenantTier::Enterprise,
    "development" => TenantTier::Development,
});

string_enum_serde!(serde_formation_profile, FormationProfile, {
    "unspecified" => FormationProfile::Unspecified,
    "lite-local" => FormationProfile::LiteLocal,
    "all-nats" => FormationProfile::AllNats,
    "all-redis" => FormationProfile::AllRedis,
});

string_enum_serde!(serde_authentication_mode, AuthenticationMode, {
    "unspecified" => AuthenticationMode::Unspecified,
    "required" => AuthenticationMode::Required,
    "none" => AuthenticationMode::None,
});
