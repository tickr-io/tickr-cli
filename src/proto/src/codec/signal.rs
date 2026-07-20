//! Pure Signal-family relay codec.
//!
//! The published Signal messages are the complete relay wire format. These
//! helpers encode, decode, and inspect that contract.

use anyhow::{anyhow, Context, Result};
use prost::Message;

use crate::signal as sp;

/// Encode a published Signal for the relay payload.
pub fn encode_signal(signal: &sp::Signal) -> Vec<u8> {
    signal.encode_to_vec()
}

/// Decode a published Signal from a relay payload.
pub fn decode_signal(bytes: &[u8]) -> Result<sp::Signal> {
    sp::Signal::decode(bytes).context("decode tickr.signal.Signal")
}

/// Encode a published SignalApplied relay-back payload.
pub fn encode_signal_applied(applied: &sp::SignalApplied) -> Vec<u8> {
    applied.encode_to_vec()
}

/// Decode a published SignalApplied relay-back payload.
pub fn decode_signal_applied(bytes: &[u8]) -> Result<sp::SignalApplied> {
    sp::SignalApplied::decode(bytes).context("decode tickr.signal.SignalApplied")
}

/// Return the instance target from a Cancel Signal, rejecting every other
/// published Signal or target shape with a diagnostic suitable for fixtures.
pub fn cancel_instance_target(signal: &sp::Signal) -> Result<&sp::target::Instance> {
    let cancel = match signal.variant.as_ref() {
        Some(sp::signal::Variant::Cancel(cancel)) => cancel,
        Some(_) => return Err(anyhow!("signal is not Cancel")),
        None => return Err(anyhow!("signal carries no variant")),
    };
    let target = cancel
        .target
        .as_ref()
        .ok_or_else(|| anyhow!("cancel carries no target"))?;
    match target.addressing.as_ref() {
        Some(sp::target::Addressing::Instance(instance)) => Ok(instance),
        Some(_) => Err(anyhow!("cancel target is not Instance")),
        None => Err(anyhow!("cancel target carries no addressing")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_applied_round_trips() {
        let applied = sp::SignalApplied {
            signal_id: "3dc8e434-9261-4b4e-8769-67f68057d356".to_string(),
            matched_count: 3,
        };
        assert_eq!(
            decode_signal_applied(&encode_signal_applied(&applied)).unwrap(),
            applied
        );
    }

    #[test]
    fn published_cancel_reasons_keep_user_and_external_variants() {
        let reasons = [
            sp::cancel_reason::Reason::UserRequested(sp::cancel_reason::UserRequested {
                actor: Some("operator".to_string()),
            }),
            sp::cancel_reason::Reason::External(sp::cancel_reason::External {
                source: "pager".to_string(),
            }),
        ];

        for reason in reasons {
            let signal = sp::Signal {
                signal_id: "3dc8e434-9261-4b4e-8769-67f68057d356".to_string(),
                idempotency_key: None,
                variant: Some(sp::signal::Variant::Cancel(sp::Cancel {
                    target: Some(sp::Target {
                        addressing: Some(sp::target::Addressing::Instance(sp::target::Instance {
                            workflow_instance_id: "16e72ba5-f65d-425d-a50e-4ccfeb1d72e1"
                                .to_string(),
                            node_id: None,
                        })),
                    }),
                    reason: Some(sp::CancelReason {
                        reason: Some(reason),
                    }),
                    note: None,
                })),
            };
            let decoded = decode_signal(&encode_signal(&signal)).unwrap();
            assert_eq!(decoded, signal);
            assert!(cancel_instance_target(&decoded).is_ok());
        }
    }
}
