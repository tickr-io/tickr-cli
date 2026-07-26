use anyhow::{anyhow, Result};
use chrono::Utc;
use sha2::{Digest, Sha256};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeStore, ScopeValueInput, ScopeWriteOutcome,
    WriteTickrCtxScopeInput,
};
use uuid::Uuid;

use crate::captures_extractor::NamedEnvelope;

/// Persist extracted Event-variable envelopes through the selected ScopeStore.
/// The envelope bytes are serialized once and remain opaque below this boundary.
pub async fn write_event_captures(
    store: &dyn ScopeStore,
    namespace: &str,
    scope_id: Uuid,
    run_id: &str,
    operation_owner: Uuid,
    captures: &[NamedEnvelope],
) -> Result<()> {
    if captures.is_empty() {
        return Ok(());
    }

    let owner = tickr_ctx::scope::sanitize_segment(run_id);
    let mut values = captures
        .iter()
        .map(|capture| {
            let key = format!(
                "{}/{}",
                owner,
                tickr_ctx::scope::sanitize_segment(&capture.name)
            );
            let envelope = serde_json::to_vec(&capture.envelope)
                .map_err(|error| anyhow!("serialize Event-variable envelope: {error}"))?;
            Ok((key, envelope))
        })
        .collect::<Result<Vec<_>>>()?;
    values.sort_by(|left, right| left.0.cmp(&right.0));

    let claim_id = capture_claim_id(operation_owner, values.iter().map(|(key, _)| key.as_str()));
    let inputs = values
        .iter()
        .map(|(key, envelope)| ScopeValueInput { key, envelope })
        .collect::<Vec<_>>();
    let now = Utc::now();

    match store
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id,
            namespace,
            run_id,
            claim_id,
            values: &inputs,
            now,
        })
        .await
        .map_err(|error| anyhow!("create Event-variable capture scope: {error}"))?
    {
        ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent => Ok(()),
        ScopeCreationOutcome::Collision { existing_scope_id } if existing_scope_id == scope_id => {
            match store
                .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                    scope_id,
                    claim_id,
                    values: &inputs,
                    now,
                })
                .await
                .map_err(|error| anyhow!("write Event-variable captures: {error}"))?
            {
                ScopeWriteOutcome::Applied { .. } | ScopeWriteOutcome::Idempotent => Ok(()),
                outcome => Err(anyhow!("write Event-variable captures: {outcome:?}")),
            }
        }
        outcome => Err(anyhow!("create Event-variable capture scope: {outcome:?}")),
    }
}

fn capture_claim_id<'a>(operation_owner: Uuid, keys: impl Iterator<Item = &'a str>) -> Uuid {
    let mut identity = Sha256::new();
    identity.update(b"tickr-scope-event-capture-v1\0");
    for key in keys {
        identity.update((key.len() as u64).to_be_bytes());
        identity.update(key.as_bytes());
    }
    Uuid::new_v5(&operation_owner, identity.finalize().as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_identity_depends_on_owner_and_ordered_keys_not_payload() {
        let owner = Uuid::new_v4();
        let first = capture_claim_id(owner, ["run/a", "run/b"].into_iter());
        let retry = capture_claim_id(owner, ["run/a", "run/b"].into_iter());
        let changed_keys = capture_claim_id(owner, ["run/a", "run/c"].into_iter());
        assert_eq!(first, retry);
        assert_ne!(first, changed_keys);
    }
}
