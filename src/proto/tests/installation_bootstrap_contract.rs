use chrono::{DateTime, Utc};
use prost::Message;
use tickr_proto::installation::installation_bootstrap::Variant;
use tickr_proto::installation::{
    AuthenticatedBootstrap, AuthenticationMode, BootstrapValidationError, ControlPlaneEndpoints,
    DisabledBootstrap, FormationProfile, GuestBootstrap, GuestLease, InstallationBootstrap,
    InstallationCredential, TenantTier, TickrLiteCompatibility,
    INSTALLATION_BOOTSTRAP_SCHEMA_VERSION,
};
use tickr_proto::TenantId;

const NOW: &str = "2026-01-01T00:00:00Z";
const CREATED_AT: &str = "2025-12-01T00:00:00Z";
const BOOTSTRAP_EXPIRES_AT: &str = "2026-06-01T00:00:00Z";
const CREDENTIAL_EXPIRES_AT: &str = "2027-01-01T00:00:00Z";
const RAW_CREDENTIAL: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn now() -> DateTime<Utc> {
    NOW.parse().unwrap()
}

fn credential() -> InstallationCredential {
    InstallationCredential {
        credential_id: "credential-01".to_owned(),
        credential: RAW_CREDENTIAL.to_owned(),
        created_at: CREATED_AT.to_owned(),
        expires_at: CREDENTIAL_EXPIRES_AT.to_owned(),
    }
}

fn bootstrap(
    tier: TenantTier,
    authentication: AuthenticationMode,
    variant: Variant,
) -> InstallationBootstrap {
    let tenant_slug = "steady-orbit".to_owned();
    InstallationBootstrap {
        schema_version: INSTALLATION_BOOTSTRAP_SCHEMA_VERSION,
        tenant_id: TenantId::from_slug(&tenant_slug).to_string(),
        tenant_slug,
        tenant_tier: tier as i32,
        control_plane: Some(ControlPlaneEndpoints {
            http: "https://control.example.test".to_owned(),
            relay: "https://relay.example.test".to_owned(),
        }),
        formation_profile: FormationProfile::LiteLocal as i32,
        compatibility: Some(TickrLiteCompatibility {
            version_requirement: ">=0.1.5,<0.2.0".to_owned(),
        }),
        authentication: authentication as i32,
        bootstrap_expires_at: (!matches!(variant, Variant::Disabled(_)))
            .then(|| BOOTSTRAP_EXPIRES_AT.to_owned()),
        variant: Some(variant),
    }
}

fn authenticated() -> InstallationBootstrap {
    bootstrap(
        TenantTier::Solo,
        AuthenticationMode::Required,
        Variant::Authenticated(AuthenticatedBootstrap {
            credential: Some(credential()),
        }),
    )
}

fn guest() -> InstallationBootstrap {
    bootstrap(
        TenantTier::Guest,
        AuthenticationMode::Required,
        Variant::Guest(GuestBootstrap {
            credential: Some(credential()),
            lease: Some(GuestLease {
                lease_id: "lease-01".to_owned(),
                created_at: CREATED_AT.to_owned(),
                expires_at: CREDENTIAL_EXPIRES_AT.to_owned(),
            }),
        }),
    )
}

fn disabled() -> InstallationBootstrap {
    bootstrap(
        TenantTier::Development,
        AuthenticationMode::None,
        Variant::Disabled(DisabledBootstrap {}),
    )
}

#[test]
fn every_bootstrap_variant_round_trips_protobuf_and_json() {
    for expected in [authenticated(), guest(), disabled()] {
        expected.validate_at(now()).unwrap();

        let protobuf = InstallationBootstrap::decode(expected.encode_to_vec().as_slice()).unwrap();
        assert_eq!(protobuf, expected);
        protobuf.validate_at(now()).unwrap();

        let json = serde_json::to_string(&expected).unwrap();
        assert!(json.contains("\"tenant_tier\":\""));
        assert!(json.contains("\"formation_profile\":\"lite-local\""));
        let projection: InstallationBootstrap = serde_json::from_str(&json).unwrap();
        assert_eq!(projection, expected);
        projection.validate_at(now()).unwrap();
    }
}

#[test]
fn unsupported_schema_and_invalid_variant_combinations_fail_closed() {
    let mut unsupported = authenticated();
    unsupported.schema_version += 1;
    assert_eq!(
        unsupported.validate_at(now()),
        Err(BootstrapValidationError::UnsupportedSchema(2))
    );

    let mut guest_tier_authenticated = authenticated();
    guest_tier_authenticated.tenant_tier = TenantTier::Guest as i32;
    assert_eq!(
        guest_tier_authenticated.validate_at(now()),
        Err(BootstrapValidationError::InvalidVariantCombination)
    );

    let mut disabled_with_required_authentication = disabled();
    disabled_with_required_authentication.authentication = AuthenticationMode::Required as i32;
    assert_eq!(
        disabled_with_required_authentication.validate_at(now()),
        Err(BootstrapValidationError::InvalidVariantCombination)
    );

    let mut disabled_with_expiry = disabled();
    disabled_with_expiry.bootstrap_expires_at = Some(BOOTSTRAP_EXPIRES_AT.to_owned());
    assert_eq!(
        disabled_with_expiry.validate_at(now()),
        Err(BootstrapValidationError::InvalidVariantCombination)
    );

    let mut no_variant = authenticated();
    no_variant.variant = None;
    assert_eq!(
        no_variant.validate_at(now()),
        Err(BootstrapValidationError::MissingVariant)
    );

    let mut missing_credential = authenticated();
    let Some(Variant::Authenticated(variant)) = missing_credential.variant.as_mut() else {
        unreachable!();
    };
    variant.credential = None;
    assert_eq!(
        missing_credential.validate_at(now()),
        Err(BootstrapValidationError::MissingCredential)
    );
}

#[test]
fn tenant_identity_expiry_and_guest_lease_are_validated() {
    let mut unsafe_slug = authenticated();
    unsafe_slug.tenant_slug = "../steady-orbit".to_owned();
    assert_eq!(
        unsafe_slug.validate_at(now()),
        Err(BootstrapValidationError::UnsafeTenantSlug)
    );

    let mut inconsistent_id = authenticated();
    inconsistent_id.tenant_id = TenantId::from_slug("different-orbit").to_string();
    assert_eq!(
        inconsistent_id.validate_at(now()),
        Err(BootstrapValidationError::InconsistentTenantId)
    );

    let mut expired = authenticated();
    expired.bootstrap_expires_at = Some(NOW.to_owned());
    assert_eq!(
        expired.validate_at(now()),
        Err(BootstrapValidationError::ExpiredBootstrap)
    );

    let mut inconsistent_guest = guest();
    let Some(Variant::Guest(variant)) = inconsistent_guest.variant.as_mut() else {
        unreachable!();
    };
    variant.lease.as_mut().unwrap().expires_at = "2026-12-01T00:00:00Z".to_owned();
    assert_eq!(
        inconsistent_guest.validate_at(now()),
        Err(BootstrapValidationError::GuestCredentialLeaseMismatch)
    );
}

#[test]
fn validation_errors_do_not_expose_the_one_time_credential() {
    let mut invalid = authenticated();
    let Some(Variant::Authenticated(variant)) = invalid.variant.as_mut() else {
        unreachable!();
    };
    variant.credential.as_mut().unwrap().expires_at = "not-a-time".to_owned();

    let error = invalid.validate_at(now()).unwrap_err().to_string();
    assert!(!error.contains(RAW_CREDENTIAL));
    assert_eq!(error, "invalid credential expiry timestamp");
}

#[test]
fn unknown_additive_fields_remain_compatible() {
    let expected = authenticated();

    let mut future_binary = expected.encode_to_vec();
    // Future length-delimited field 100. Proto3 readers discard it safely.
    future_binary.extend_from_slice(&[0xa2, 0x06, 0x03, b'n', b'e', b'w']);
    let decoded = InstallationBootstrap::decode(future_binary.as_slice()).unwrap();
    assert_eq!(decoded, expected);
    decoded.validate_at(now()).unwrap();

    let mut future_json = serde_json::to_value(&expected).unwrap();
    future_json.as_object_mut().unwrap().insert(
        "future_additive_field".to_owned(),
        serde_json::json!({"v": 2}),
    );
    let decoded: InstallationBootstrap = serde_json::from_value(future_json).unwrap();
    assert_eq!(decoded, expected);
    decoded.validate_at(now()).unwrap();
}
