//! Pure splitter for the routing-variable seam. Takes the
//! completing task's declared `mkRoutingVar` names and the raw
//! emitted outputs (the bag of `(name, serde_json::Value)` pairs
//! the task wrote via `tickr-ctx publish`), and partitions them
//! into:
//!
//! - `routing_vars_for_TaskUpdate`: declared values cast onto
//!   `RoutingValue`. These ride on `TaskUpdate.routing_variables`
//!   over the relay and merge into the workflow instance's
//!   `routing_variables` map server-side.
//! - `ctx_only_values`: undeclared outputs. These stay in
//!   `ctx-<ns>` NATS KV — they're either operational data the
//!   server doesn't need to see, or `from.task` consumer-side
//!   reads.
//!
//! Pure module — no I/O. Tests cover declared/emitted,
//! emitted-undeclared, declared-missing, declared-with-wrong-type
//! and empty-input cases.

use std::collections::BTreeMap;
use thiserror::Error;
use tickr_proto::workflow::{self as wf, RoutingValue, RoutingVarDecl};

/// Failure modes returned by the splitter. Today the only
/// fatal case is a type mismatch between the declared
/// `mkRoutingVar.type` and the emitted value's JSON shape.
/// Declared-but-missing is non-fatal — the splitter omits the
/// entry from the routing-vars bucket and the upstream
/// completion path handles the rest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SplitError {
    #[error(
        "routing variable {name:?}: declared type `{declared_type}` does not match \
         emitted value's actual type `{actual_type}`"
    )]
    TypeMismatch {
        name: String,
        declared_type: String,
        actual_type: String,
    },
    #[error(
        "routing variable {name:?}: emitted value is not a supported scalar (string / int / \
         bool / bytes): {message}"
    )]
    UnsupportedShape { name: String, message: String },
}

/// Outcome of the split: declared values projected onto
/// `RoutingValue` plus the remainder that stays in `ctx-<ns>`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SplitOutput {
    pub routing_vars: BTreeMap<String, RoutingValue>,
    pub ctx_only: BTreeMap<String, serde_json::Value>,
}

/// Pure-function splitter. Iterates the emitted outputs and
/// partitions them by whether each name appears in `declared`.
/// Declared but missing from `outputs` is silently omitted from
/// the routing-vars bucket — the upstream completion-grounded
/// check enforces "all declared outputs must be present".
pub fn split(
    declared: &[RoutingVarDecl],
    outputs: BTreeMap<String, serde_json::Value>,
) -> Result<SplitOutput, SplitError> {
    let mut by_name: std::collections::HashMap<&str, &RoutingVarDecl> =
        std::collections::HashMap::new();
    for spec in declared {
        by_name.insert(spec.name.as_str(), spec);
    }
    let mut routing_vars: BTreeMap<String, RoutingValue> = BTreeMap::new();
    let mut ctx_only: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (name, value) in outputs {
        match by_name.get(name.as_str()) {
            Some(spec) => {
                let rv = json_to_routing_value(&name, &value)?;
                if let Some(declared_type) = spec.var_type.as_deref() {
                    if routing_value_type_tag(&rv) != declared_type {
                        return Err(SplitError::TypeMismatch {
                            name,
                            declared_type: declared_type.to_string(),
                            actual_type: routing_value_type_tag(&rv).to_string(),
                        });
                    }
                }
                routing_vars.insert(name, rv);
            }
            None => {
                ctx_only.insert(name, value);
            }
        }
    }
    Ok(SplitOutput {
        routing_vars,
        ctx_only,
    })
}

fn json_to_routing_value(
    name: &str,
    value: &serde_json::Value,
) -> Result<RoutingValue, SplitError> {
    match value {
        serde_json::Value::String(s) => Ok(RoutingValue {
            value: Some(wf::routing_value::Value::StringValue(s.clone())),
        }),
        serde_json::Value::Bool(b) => Ok(RoutingValue {
            value: Some(wf::routing_value::Value::BoolValue(*b)),
        }),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|value| RoutingValue {
                value: Some(wf::routing_value::Value::IntValue(value)),
            })
            .ok_or_else(|| SplitError::UnsupportedShape {
                name: name.to_string(),
                message: "non-integer numbers are not supported (floats break Eq + Hash)"
                    .to_string(),
            }),
        _ => Err(SplitError::UnsupportedShape {
            name: name.to_string(),
            message: "supported scalars are strings, integers, and booleans".to_string(),
        }),
    }
}

/// Return the closed scalar type tag used by Nickel declarations.
pub fn routing_value_type_tag(value: &RoutingValue) -> &'static str {
    match value.value.as_ref() {
        Some(wf::routing_value::Value::StringValue(_)) => "string",
        Some(wf::routing_value::Value::IntValue(_)) => "int",
        Some(wf::routing_value::Value::BoolValue(_)) => "bool",
        Some(wf::routing_value::Value::BytesValue(_)) => "bytes",
        None => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, ty: Option<&str>) -> RoutingVarDecl {
        RoutingVarDecl {
            name: name.to_string(),
            var_type: ty.map(String::from),
        }
    }

    fn outputs(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn declared_and_emitted_lands_in_routing_vars_bucket() {
        let declared = vec![spec("coverage", None)];
        let out = split(
            &declared,
            outputs(&[("coverage", serde_json::Value::Number(80.into()))]),
        )
        .unwrap();
        assert_eq!(
            out.routing_vars.get("coverage"),
            Some(&RoutingValue {
                value: Some(wf::routing_value::Value::IntValue(80)),
            })
        );
        assert!(out.ctx_only.is_empty());
    }

    #[test]
    fn emitted_undeclared_stays_in_ctx_only_bucket() {
        let declared: Vec<RoutingVarDecl> = vec![];
        let out = split(
            &declared,
            outputs(&[(
                "image_digest",
                serde_json::Value::String("sha256:abc".into()),
            )]),
        )
        .unwrap();
        assert!(out.routing_vars.is_empty());
        assert_eq!(out.ctx_only.len(), 1);
        assert!(out.ctx_only.contains_key("image_digest"));
    }

    #[test]
    fn declared_but_missing_emission_is_omitted_silently() {
        // Upstream completion check enforces presence of declared
        // outputs; the splitter just omits unfilled slots so the
        // routing-vars bucket reflects what was actually emitted.
        let declared = vec![spec("coverage", None), spec("approver", None)];
        let out = split(
            &declared,
            outputs(&[("coverage", serde_json::Value::Number(85.into()))]),
        )
        .unwrap();
        assert_eq!(out.routing_vars.len(), 1);
        assert!(!out.routing_vars.contains_key("approver"));
        assert!(out.ctx_only.is_empty());
    }

    #[test]
    fn declared_type_mismatch_errors_loudly() {
        let declared = vec![spec("coverage", Some("int"))];
        let err = split(
            &declared,
            outputs(&[("coverage", serde_json::Value::String("eighty".into()))]),
        )
        .unwrap_err();
        match err {
            SplitError::TypeMismatch {
                name,
                declared_type,
                actual_type,
            } => {
                assert_eq!(name, "coverage");
                assert_eq!(declared_type, "int");
                assert_eq!(actual_type, "string");
            }
            other => panic!("expected TypeMismatch, got {:?}", other),
        }
    }

    #[test]
    fn empty_inputs_produce_empty_outputs() {
        let out = split(&[], BTreeMap::new()).unwrap();
        assert!(out.routing_vars.is_empty());
        assert!(out.ctx_only.is_empty());
    }

    #[test]
    fn mixed_inputs_partition_correctly() {
        let declared = vec![spec("decision", None), spec("count", Some("int"))];
        let out = split(
            &declared,
            outputs(&[
                ("decision", serde_json::Value::String("approve".into())),
                ("count", serde_json::Value::Number(7.into())),
                ("log_path", serde_json::Value::String("/var/log/x".into())),
            ]),
        )
        .unwrap();
        assert_eq!(out.routing_vars.len(), 2);
        assert_eq!(
            out.routing_vars.get("decision"),
            Some(&RoutingValue {
                value: Some(wf::routing_value::Value::StringValue("approve".to_string())),
            })
        );
        assert_eq!(
            out.routing_vars.get("count"),
            Some(&RoutingValue {
                value: Some(wf::routing_value::Value::IntValue(7)),
            })
        );
        assert_eq!(out.ctx_only.len(), 1);
        assert!(out.ctx_only.contains_key("log_path"));
    }
}
