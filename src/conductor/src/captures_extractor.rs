//! Pure JSONPath capture extractor.
//!
//! Runs at the conductor's HTTP `/trigger` ingress against the inbound
//! `inputs` JSON. Workflow definitions declare a list of named captures, each
//! with a singular-query JSONPath (RFC 9535); this module applies each query
//! to the payload and packages the result as a versioned tickr-ctx envelope
//! keyed by the capture name. Zero-match resolves to a JSON `null` with
//! `present: false` on the envelope — an explicit value-absent state the
//! consumer can branch on — not an error.
//!
//! The function performs no I/O. SQL repository + NATS writes are the
//! caller's responsibility once this layer has produced the envelopes.

use serde_json::Value;
use serde_json_path::JsonPath;
use thiserror::Error;
use tickr_ctx::envelope::{Envelope, Producer, SignalSource};
use tickr_proto::workflow::{capture_source, CaptureDeclaration};
use uuid::Uuid;

/// A capture-name paired with the envelope the extractor built for it.
/// The ingress adapter writes these into the SQL Event-variable archive and
/// NATS KV `ctx-<ns>/<signal_id>/<name>` verbatim.
#[derive(Debug, Clone)]
pub struct NamedEnvelope {
    pub name: String,
    pub envelope: Envelope,
}

/// Failure modes for capture extraction. `JsonPathParseError` is structurally
/// preventable by registration-time validation (the captures-declarations
/// validator runs the same parse), but the extractor revalidates defensively
/// so a corrupted persisted workflow definition surfaces a clear 4xx rather
/// than panicking.
#[derive(Debug, Error)]
pub enum ExtractionError {
    #[error("capture `{name}` has an unparseable JSONPath `{jsonpath}`: {message}")]
    JsonPathParseError {
        name: String,
        jsonpath: String,
        message: String,
    },
}

/// Apply every declaration in `captures` against `payload` and return the
/// resulting envelopes. Stops at the first parse failure; structural
/// validation of singular-query shape happened at registration, so a parse
/// failure here would only be reachable through a persisted-state corruption
/// path.
pub fn extract_captures(
    payload: &Value,
    captures: &[CaptureDeclaration],
    signal_id: Uuid,
    source: SignalSource,
) -> Result<Vec<NamedEnvelope>, ExtractionError> {
    let mut out = Vec::with_capacity(captures.len());

    for decl in captures {
        let jsonpath = match decl.from.as_ref().and_then(|source| source.source.as_ref()) {
            Some(capture_source::Source::Trigger(trigger)) => &trigger.jsonpath,
            None => {
                return Err(ExtractionError::JsonPathParseError {
                    name: decl.name.clone(),
                    jsonpath: String::new(),
                    message: "capture declaration is missing its trigger source".to_string(),
                });
            }
        };
        let path = JsonPath::parse(jsonpath).map_err(|e| ExtractionError::JsonPathParseError {
            name: decl.name.clone(),
            jsonpath: jsonpath.clone(),
            message: e.to_string(),
        })?;

        let nodes = path.query(payload);
        let extracted = nodes.first();
        let envelope = build_envelope(extracted, signal_id, source.clone());

        out.push(NamedEnvelope {
            name: decl.name.clone(),
            envelope,
        });
    }

    Ok(out)
}

/// Build an envelope for a JSONPath query result. `Some(v)` produces a typed
/// envelope whose `kind` reflects the JSON shape; `None` (zero-match)
/// produces a `present: false` envelope carrying JSON `null` so the
/// downstream reader can branch on absence without a runtime miss.
fn build_envelope(value: Option<&Value>, signal_id: Uuid, source: SignalSource) -> Envelope {
    let producer = Producer::Signal { signal_id, source };
    match value {
        Some(v) => {
            let (kind, jv) = classify(v);
            Envelope::new(kind, jv, false, producer)
        }
        None => {
            // Absence is a captured value, not a missing one — the
            // distinction lets a task author branch on whether the trigger
            // payload carried the field at all.
            let mut env = Envelope::new("json", Value::Null, false, producer);
            env.present = false;
            env
        }
    }
}

/// Map a JSON value's shape onto an envelope `kind` so renderers downstream
/// reproduce the original scalar. Composite shapes (object, array) round-trip
/// as `json`.
fn classify(v: &Value) -> (&'static str, Value) {
    match v {
        Value::String(_) => ("string", v.clone()),
        Value::Bool(_) => ("bool", v.clone()),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                ("int", v.clone())
            } else {
                ("float", v.clone())
            }
        }
        Value::Null | Value::Object(_) | Value::Array(_) => ("json", v.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tickr_proto::workflow::{capture_source, CaptureDeclaration, CaptureSource};

    fn trigger(name: &str, jsonpath: &str) -> CaptureDeclaration {
        CaptureDeclaration {
            name: name.into(),
            from: Some(CaptureSource {
                source: Some(capture_source::Source::Trigger(capture_source::Trigger {
                    jsonpath: jsonpath.into(),
                })),
            }),
        }
    }

    fn manual_source() -> SignalSource {
        SignalSource::Manual
    }

    #[test]
    fn extracts_string_capture() {
        let payload = json!({"user": {"email": "alice@example.com"}});
        let caps = vec![trigger("user_email", "$.user.email")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "user_email");
        assert_eq!(out[0].envelope.kind, "string");
        assert_eq!(out[0].envelope.value, json!("alice@example.com"));
        assert!(out[0].envelope.present);
        match &out[0].envelope.producer {
            Producer::Signal {
                signal_id: sid,
                source: SignalSource::Manual,
            } => {
                assert_eq!(*sid, signal_id);
            }
            other => panic!("expected Signal producer with Manual source, got {other:?}"),
        }
    }

    #[test]
    fn zero_match_yields_present_false_envelope() {
        let payload = json!({"user": {"name": "alice"}});
        let caps = vec![trigger("user_email", "$.user.email")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out.len(), 1);
        assert!(
            !out[0].envelope.present,
            "absent capture must carry present:false"
        );
        assert_eq!(out[0].envelope.value, Value::Null);
    }

    #[test]
    fn extracts_int_capture() {
        let payload = json!({"order": {"amount": 4200}});
        let caps = vec![trigger("amount", "$.order.amount")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out[0].envelope.kind, "int");
        assert_eq!(out[0].envelope.value, json!(4200));
    }

    #[test]
    fn extracts_bool_capture() {
        let payload = json!({"flag": true});
        let caps = vec![trigger("flag", "$.flag")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out[0].envelope.kind, "bool");
        assert_eq!(out[0].envelope.value, json!(true));
    }

    #[test]
    fn extracts_float_capture() {
        let payload = json!({"price": 19.99});
        let caps = vec![trigger("price", "$.price")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out[0].envelope.kind, "float");
        assert_eq!(out[0].envelope.value, json!(19.99));
    }

    #[test]
    fn extracts_object_as_json() {
        let payload = json!({"address": {"city": "Sea", "zip": "98101"}});
        let caps = vec![trigger("address", "$.address")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert_eq!(out[0].envelope.kind, "json");
        assert_eq!(
            out[0].envelope.value,
            json!({"city": "Sea", "zip": "98101"})
        );
    }

    #[test]
    fn malformed_jsonpath_yields_parse_error() {
        let payload = json!({"x": 1});
        let caps = vec![trigger("x", "$.[")];
        let signal_id = Uuid::new_v4();

        let err = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap_err();
        assert!(
            matches!(err, ExtractionError::JsonPathParseError { ref name, .. } if name == "x"),
            "expected JsonPathParseError for `x`, got {err:?}"
        );
    }

    #[test]
    fn signal_id_threads_into_every_envelope() {
        let payload = json!({"a": 1, "b": 2});
        let caps = vec![trigger("a", "$.a"), trigger("b", "$.b")];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        for env in &out {
            match &env.envelope.producer {
                Producer::Signal { signal_id: sid, .. } => assert_eq!(*sid, signal_id),
                other => panic!("expected Signal producer, got {other:?}"),
            }
        }
    }

    #[test]
    fn empty_declarations_yields_empty_output() {
        let payload = json!({"anything": "here"});
        let caps: Vec<CaptureDeclaration> = vec![];
        let signal_id = Uuid::new_v4();

        let out = extract_captures(&payload, &caps, signal_id, manual_source()).unwrap();
        assert!(out.is_empty());
    }
}
