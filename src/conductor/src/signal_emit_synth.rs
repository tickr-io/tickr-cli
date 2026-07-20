//! Pure synthesizer for `mkSignalEmit` / `mkSignalEmitOnFailure`.
//! Takes the completing task's emit declarations + the relevant
//! grounded-outcome context (routing variables for success,
//! lineage for failure) and produces a list of `SynthesizedWakeup`
//! envelopes the conductor's task-completion path will publish on
//! `tickr.external.signals`.
//!
//! Pure module: no I/O, no NATS, no relay. Tests unit-test the
//! shape against synthetic inputs.

use std::collections::BTreeMap;
use thiserror::Error;
use tickr_proto::workflow::task_signal_emit::Emit;
use tickr_proto::workflow::{routing_value, RoutingValue, TaskSignalEmit};
use uuid::Uuid;

/// One synthesized wakeup envelope. Mirrors the shape of an
/// externally-arriving wakeup so the downstream wakeup-translator
/// can't tell synthetic from external. `payload` is opaque JSON
/// here; the wakeup ingress accepts arbitrary payload shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynthesizedWakeup {
    pub signal_name: String,
    pub payload: serde_json::Value,
    /// Locally-minted `signal_id` the conductor mints per
    /// synthesized wakeup. Distinct from any wakeup-translator-
    /// minted ids so downstream consumers see a fresh signal_id
    /// per emit.
    pub signal_id: Uuid,
}

/// Failure modes returned by the synthesizer. The only fatal case
/// is a declared `from_routing_var` whose value never landed —
/// i.e., the producing task didn't emit the value despite
/// declaring `mkRoutingVar`. The parser's single-producer rule +
/// task-completion enforcement should catch this upstream; if it
/// reaches the synthesizer, surface it as a defense-in-depth
/// error rather than silently dropping the emit.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SynthError {
    #[error(
        "task declared `mkSignalEmit {{ from_routing_var = \"{routing_var}\" }}` but \
         no value for that variable was produced at completion"
    )]
    MissingRoutingValue { routing_var: String },
}

/// Lineage carried on `mkSignalEmitOnFailure` payloads. Auto-
/// populated from the failing task's identifiers; authors can't
/// override.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FailureContext {
    pub task_instance_id: Uuid,
    pub task_id: Uuid,
    pub run_id: Uuid,
}

/// Synthesize the on-success wakeups for a completing task. For
/// each `TaskSignalEmit::OnSuccess` declaration, looks up the
/// named routing variable in `routing_vars` and builds a wakeup
/// envelope with that value as payload. `OnFailure` declarations
/// are skipped here — the failure path's synthesizer lands in a
/// separate function.
pub fn synthesize_on_success(
    emits: &[TaskSignalEmit],
    routing_vars: &BTreeMap<String, RoutingValue>,
) -> Result<Vec<SynthesizedWakeup>, SynthError> {
    let mut out = Vec::new();
    for emit in emits {
        let Some(Emit::OnSuccess(on_success)) = &emit.emit else {
            continue;
        };
        let Some(value) = routing_vars.get(&on_success.from_routing_var) else {
            return Err(SynthError::MissingRoutingValue {
                routing_var: on_success.from_routing_var.clone(),
            });
        };
        out.push(SynthesizedWakeup {
            signal_name: on_success.signal_name.clone(),
            payload: routing_value_to_json(value),
            signal_id: Uuid::new_v4(),
        });
    }
    Ok(out)
}

/// Synthesize the on-failure wakeups for a failing task. For each
/// `TaskSignalEmit::OnFailure` declaration, builds a wakeup
/// envelope whose payload is the auto-populated
/// `FailureContext`. Routing variables are not consulted —
/// failure-grounded tasks emit nothing on the routing-variable
/// channel.
pub fn synthesize_on_failure(
    emits: &[TaskSignalEmit],
    ctx: FailureContext,
) -> Vec<SynthesizedWakeup> {
    let mut out = Vec::new();
    for emit in emits {
        let Some(Emit::OnFailure(on_failure)) = &emit.emit else {
            continue;
        };
        out.push(SynthesizedWakeup {
            signal_name: on_failure.signal_name.clone(),
            payload: serde_json::to_value(&ctx).unwrap_or(serde_json::Value::Null),
            signal_id: Uuid::new_v4(),
        });
    }
    out
}

fn routing_value_to_json(v: &RoutingValue) -> serde_json::Value {
    match v.value.as_ref() {
        Some(routing_value::Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(routing_value::Value::IntValue(n)) => serde_json::Value::Number((*n).into()),
        Some(routing_value::Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(routing_value::Value::BytesValue(bytes)) => serde_json::Value::Array(
            bytes
                .iter()
                .map(|b| serde_json::Value::Number((*b as u64).into()))
                .collect(),
        ),
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tickr_proto::workflow::task_signal_emit::{OnFailure, OnSuccess};

    fn rv(s: &str) -> RoutingValue {
        RoutingValue {
            value: Some(routing_value::Value::StringValue(s.to_string())),
        }
    }

    fn on_success(signal_name: &str, from_routing_var: &str) -> TaskSignalEmit {
        TaskSignalEmit {
            emit: Some(Emit::OnSuccess(OnSuccess {
                signal_name: signal_name.to_string(),
                from_routing_var: from_routing_var.to_string(),
            })),
        }
    }

    fn on_failure(signal_name: &str) -> TaskSignalEmit {
        TaskSignalEmit {
            emit: Some(Emit::OnFailure(OnFailure {
                signal_name: signal_name.to_string(),
            })),
        }
    }

    #[test]
    fn zero_emits_produces_empty_output() {
        let out = synthesize_on_success(&[], &BTreeMap::new()).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn one_on_success_emit_produces_one_wakeup_with_routing_variable_payload() {
        let emits = vec![on_success("deployment-complete", "deploy_id")];
        let mut vars = BTreeMap::new();
        vars.insert("deploy_id".to_string(), rv("rel-42"));
        let out = synthesize_on_success(&emits, &vars).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].signal_name, "deployment-complete");
        assert_eq!(out[0].payload, serde_json::json!("rel-42"));
        assert!(
            !out[0].signal_id.is_nil(),
            "signal_id must be freshly minted"
        );
    }

    #[test]
    fn multiple_emits_each_produce_a_wakeup_with_correct_routing_var() {
        let emits = vec![on_success("a", "x"), on_success("b", "y")];
        let mut vars = BTreeMap::new();
        vars.insert(
            "x".to_string(),
            RoutingValue {
                value: Some(routing_value::Value::IntValue(1)),
            },
        );
        vars.insert(
            "y".to_string(),
            RoutingValue {
                value: Some(routing_value::Value::IntValue(2)),
            },
        );
        let out = synthesize_on_success(&emits, &vars).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].signal_name, "a");
        assert_eq!(out[0].payload, serde_json::json!(1));
        assert_eq!(out[1].signal_name, "b");
        assert_eq!(out[1].payload, serde_json::json!(2));
    }

    #[test]
    fn missing_routing_variable_returns_a_typed_error() {
        let emits = vec![on_success("x", "missing")];
        let err = synthesize_on_success(&emits, &BTreeMap::new()).unwrap_err();
        match err {
            SynthError::MissingRoutingValue { routing_var } => {
                assert_eq!(routing_var, "missing");
            }
        }
    }

    #[test]
    fn on_failure_emits_use_failure_context_payload() {
        let emits = vec![on_failure("deployment-failed")];
        let ctx = FailureContext {
            task_instance_id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
        };
        let out = synthesize_on_failure(&emits, ctx.clone());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].signal_name, "deployment-failed");
        // Payload deserializes back to the same lineage.
        let decoded: FailureContext = serde_json::from_value(out[0].payload.clone()).unwrap();
        assert_eq!(decoded, ctx);
    }

    #[test]
    fn on_success_skips_on_failure_emits() {
        // Mixed list: `synthesize_on_success` ignores OnFailure;
        // `synthesize_on_failure` ignores OnSuccess.
        let emits = vec![on_success("s", "v"), on_failure("f")];
        let mut vars = BTreeMap::new();
        vars.insert("v".to_string(), rv("y"));
        let ok = synthesize_on_success(&emits, &vars).unwrap();
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].signal_name, "s");
        let fail = synthesize_on_failure(
            &emits,
            FailureContext {
                task_instance_id: Uuid::new_v4(),
                task_id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
            },
        );
        assert_eq!(fail.len(), 1);
        assert_eq!(fail[0].signal_name, "f");
    }
}
