//! JSON value envelope persisted in NATS KV.
//!
//! Every captured value carries metadata so consumers and operators can
//! introspect lineage, type, and integrity. The envelope is versioned (`v`)
//! so future features (Object Store spill, audit cross-references) can extend
//! it without breaking older readers.
//!
//! v=2 carries a typed `producer` discriminator so the data plane can tell
//! task-produced values apart from signal-derived captures. v=1 envelopes
//! (flat `producer_task` / `producer_task_name` strings) are still accepted
//! on read and rewritten as `Producer::Task` for backward compatibility.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Lineage discriminator for the producer that wrote this envelope. Tasks
/// stamp the executor-injected task id/name; signal-derived captures stamp the
/// originating signal id plus the source channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Producer {
    Task {
        task_id: String,
        task_name: String,
    },
    Signal {
        signal_id: Uuid,
        source: SignalSource,
    },
    /// A tickr system component — not a task or a signal — wrote this value.
    /// Used for reserved, engine-written ctx keys such as the Conductor's
    /// `tickr_graph` mirror; `component` names the writer for lineage.
    System {
        component: String,
    },
}

/// Channel a signal arrived through. Mirrors the conductor's external-event
/// ingress shapes — manual UI, wakeup timer, or a NATS subject.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignalSource {
    Manual,
    Wakeup { name: String },
    ExternalNats { subject: String },
}

#[derive(Debug, Clone)]
pub struct Envelope {
    /// Schema version observed at deserialize time. The Serialize impl
    /// always emits v=2, so a freshly-constructed envelope reports 2; v=1
    /// shows up only when reading legacy entries from KV. Exposed for
    /// operator/debug tooling; the CLI doesn't branch on it today.
    #[allow(dead_code)]
    pub v: u8,
    /// One of: "string", "json", "int", "float", "bool".
    /// "bytes" / Object-Store spill is reserved for a later phase.
    pub kind: String,
    pub value: serde_json::Value,
    pub secret: bool,
    pub producer: Producer,
    /// `true` for an extracted scalar; `false` when a declared capture
    /// resolved to zero matches at extraction time. Carrying absence as a
    /// distinct envelope flag (rather than just omitting the entry) lets
    /// downstream consumers branch on "field was missing from the trigger
    /// payload" without confusing it with "field hasn't been written yet."
    pub present: bool,
    pub created_at: String,
    pub sha256: String,
}

impl Envelope {
    pub fn new(kind: &str, value: serde_json::Value, secret: bool, producer: Producer) -> Self {
        let raw = match &value {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let mut hasher = Sha256::new();
        hasher.update(raw.as_bytes());
        let sha = hex::encode(hasher.finalize());

        Envelope {
            v: 2,
            kind: kind.to_string(),
            value,
            secret,
            producer,
            present: true,
            created_at: chrono::Utc::now().to_rfc3339(),
            sha256: sha,
        }
    }

    /// Bytes a consumer would print on `tickr-ctx get`. For string/bytes
    /// types we hand back the raw text; for numerics/bool we use the json
    /// scalar repr; for `json` we emit the canonical serialization.
    pub fn render(&self) -> Result<Vec<u8>> {
        match self.kind.as_str() {
            "string" => match &self.value {
                serde_json::Value::String(s) => Ok(s.as_bytes().to_vec()),
                _ => Err(anyhow!(
                    "envelope marked as string but value is not a JSON string"
                )),
            },
            "json" => Ok(serde_json::to_vec(&self.value)?),
            "int" | "float" | "bool" => Ok(self.value.to_string().into_bytes()),
            other => Err(anyhow!("unsupported envelope type: {}", other)),
        }
    }
}

/// On the wire we always emit the v=2 shape (typed `producer`). The custom
/// impl exists so the v=1 read path below has a matching write path that
/// won't regress to flat strings.
impl Serialize for Envelope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            v: u8,
            #[serde(rename = "type")]
            kind: &'a str,
            value: &'a serde_json::Value,
            secret: bool,
            producer: &'a Producer,
            #[serde(skip_serializing_if = "is_true")]
            present: bool,
            created_at: &'a str,
            sha256: &'a str,
        }
        Wire {
            v: 2,
            kind: &self.kind,
            value: &self.value,
            secret: self.secret,
            producer: &self.producer,
            present: self.present,
            created_at: &self.created_at,
            sha256: &self.sha256,
        }
        .serialize(serializer)
    }
}

/// Wire-shape helper: omit `present` when it's the default `true` so
/// envelopes written for present scalars round-trip identically with
/// pre-`present` v=2 entries already at rest in NATS KV.
fn is_true(b: &bool) -> bool {
    *b
}

/// Version-aware read path: v=2 reads the typed `producer`, v=1 reads the
/// flat strings and lifts them into `Producer::Task`. Any other version is a
/// hard error so we surface unknown writers explicitly instead of silently
/// dropping fields.
impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            v: u8,
            #[serde(rename = "type")]
            kind: String,
            value: serde_json::Value,
            #[serde(default)]
            secret: bool,
            // v=2 shape
            #[serde(default)]
            producer: Option<Producer>,
            // v=1 shape
            #[serde(default)]
            producer_task: Option<String>,
            #[serde(default)]
            producer_task_name: Option<String>,
            // Optional on the wire — absent means `true` so legacy v=1 reads
            // and pre-`present` v=2 entries deserialize as present scalars.
            #[serde(default = "default_present")]
            present: bool,
            created_at: String,
            sha256: String,
        }

        fn default_present() -> bool {
            true
        }

        let raw = Raw::deserialize(deserializer)?;
        let producer = match raw.v {
            2 => raw.producer.ok_or_else(|| {
                serde::de::Error::custom("v=2 envelope missing required `producer` field")
            })?,
            1 => Producer::Task {
                task_id: raw.producer_task.unwrap_or_default(),
                task_name: raw.producer_task_name.unwrap_or_default(),
            },
            other => {
                return Err(serde::de::Error::custom(format!(
                    "unsupported envelope version: {} (this build understands v=1, v=2)",
                    other
                )));
            }
        };

        Ok(Envelope {
            v: raw.v,
            kind: raw.kind,
            value: raw.value,
            secret: raw.secret,
            producer,
            present: raw.present,
            created_at: raw.created_at,
            sha256: raw.sha256,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_producer() -> Producer {
        Producer::Task {
            task_id: "task-uuid".into(),
            task_name: "build".into(),
        }
    }

    fn bare_task(id: &str) -> Producer {
        Producer::Task {
            task_id: id.into(),
            task_name: String::new(),
        }
    }

    #[test]
    fn round_trip_string() {
        let env = Envelope::new(
            "string",
            serde_json::Value::String("sha256:abc".into()),
            false,
            task_producer(),
        );
        let bytes = serde_json::to_vec(&env).unwrap();
        let parsed: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.kind, "string");
        assert_eq!(parsed.render().unwrap(), b"sha256:abc");
    }

    #[test]
    fn round_trip_task_producer_preserves_lineage() {
        let env = Envelope::new(
            "string",
            serde_json::Value::String("v".into()),
            false,
            Producer::Task {
                task_id: "t-123".into(),
                task_name: "compile".into(),
            },
        );
        let bytes = serde_json::to_vec(&env).unwrap();
        let parsed: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.v, 2);
        match parsed.producer {
            Producer::Task { task_id, task_name } => {
                assert_eq!(task_id, "t-123");
                assert_eq!(task_name, "compile");
            }
            other => panic!("expected Producer::Task, got {:?}", other),
        }
    }

    #[test]
    fn round_trip_signal_manual_producer() {
        let sig = Uuid::new_v4();
        let env = Envelope::new(
            "string",
            serde_json::Value::String("payload".into()),
            false,
            Producer::Signal {
                signal_id: sig,
                source: SignalSource::Manual,
            },
        );
        let bytes = serde_json::to_vec(&env).unwrap();
        let parsed: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.v, 2);
        match parsed.producer {
            Producer::Signal { signal_id, source } => {
                assert_eq!(signal_id, sig);
                assert_eq!(source, SignalSource::Manual);
            }
            other => panic!("expected Producer::Signal, got {:?}", other),
        }
    }

    #[test]
    fn round_trip_system_producer_preserves_component() {
        // The Conductor stamps its reserved-key writes (the `tickr_graph`
        // mirror) with a System producer; lineage must survive the round-trip.
        let env = Envelope::new(
            "json",
            serde_json::json!({"version": 0}),
            false,
            Producer::System {
                component: "conductor".into(),
            },
        );
        let bytes = serde_json::to_vec(&env).unwrap();
        let parsed: Envelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.v, 2);
        match parsed.producer {
            Producer::System { component } => assert_eq!(component, "conductor"),
            other => panic!("expected Producer::System, got {:?}", other),
        }
    }

    #[test]
    fn system_written_json_envelope_renders_the_stored_value() {
        // A task reading the ctx graph via `tickr-ctx get tickr_graph`
        // deserializes the stored bytes as an Envelope and calls `render()`.
        // A System-produced json envelope must render back the exact document
        // it wrapped — the read-path contract the graph mirror relies on.
        let graph = serde_json::json!({"version": 0, "graph": {"nodes": [{"code": "0ABC"}]}});
        let env = Envelope::new(
            "json",
            graph.clone(),
            false,
            Producer::System {
                component: "conductor".into(),
            },
        );
        let bytes = serde_json::to_vec(&env).unwrap();
        let read_back: Envelope = serde_json::from_slice(&bytes).unwrap();
        let rendered: serde_json::Value =
            serde_json::from_slice(&read_back.render().unwrap()).unwrap();
        assert_eq!(rendered, graph);
    }

    #[test]
    fn v1_envelope_deserialises_as_task_producer() {
        // Older writers (pre-substrate) emit the flat string pair. New readers
        // must lift them into Producer::Task without dropping data.
        let v1 = serde_json::json!({
            "v": 1,
            "type": "string",
            "value": "hello",
            "secret": false,
            "producer_task": "legacy-task-id",
            "producer_task_name": "legacy-name",
            "created_at": "2026-05-13T00:00:00Z",
            "sha256": "deadbeef",
        });
        let parsed: Envelope = serde_json::from_value(v1).unwrap();
        assert_eq!(parsed.v, 1);
        match parsed.producer {
            Producer::Task { task_id, task_name } => {
                assert_eq!(task_id, "legacy-task-id");
                assert_eq!(task_name, "legacy-name");
            }
            other => panic!("expected Producer::Task, got {:?}", other),
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let v99 = serde_json::json!({
            "v": 99,
            "type": "string",
            "value": "hello",
            "secret": false,
            "created_at": "2026-05-13T00:00:00Z",
            "sha256": "deadbeef",
        });
        let err = serde_json::from_value::<Envelope>(v99).expect_err("v=99 must reject");
        assert!(
            err.to_string().contains("unsupported envelope version"),
            "got: {}",
            err
        );
    }

    #[test]
    fn render_int() {
        let env = Envelope::new(
            "int",
            serde_json::Value::Number(42.into()),
            false,
            bare_task("task-uuid"),
        );
        assert_eq!(env.render().unwrap(), b"42");
    }

    #[test]
    fn sha256_is_stable_for_equal_strings() {
        let a = Envelope::new(
            "string",
            serde_json::Value::String("x".into()),
            false,
            bare_task("t"),
        );
        let b = Envelope::new(
            "string",
            serde_json::Value::String("x".into()),
            false,
            bare_task("t"),
        );
        assert_eq!(a.sha256, b.sha256);
    }

    #[test]
    fn sha256_differs_for_different_strings() {
        let a = Envelope::new(
            "string",
            serde_json::Value::String("x".into()),
            false,
            bare_task("t"),
        );
        let b = Envelope::new(
            "string",
            serde_json::Value::String("y".into()),
            false,
            bare_task("t"),
        );
        assert_ne!(
            a.sha256, b.sha256,
            "differing payloads must produce differing digests"
        );
    }

    #[test]
    fn render_string_kind_with_non_string_value_errors() {
        // The DSL contract is "kind must match the JSON shape". A producer
        // claiming kind=string but actually sending a number is malformed and
        // must surface a clear error, not silently coerce.
        let env = Envelope::new(
            "string",
            serde_json::Value::Number(42.into()),
            false,
            bare_task("t"),
        );
        let err = env.render().expect_err("string-kind w/ number must error");
        assert!(
            err.to_string().contains("not a JSON string"),
            "got: {}",
            err
        );
    }

    #[test]
    fn render_unknown_kind_errors() {
        // "bytes" is reserved (Object Store spill) and must reject until that
        // lands; any unrecognized kind likewise errors.
        let env = Envelope::new(
            "bytes",
            serde_json::Value::String("ignored".into()),
            false,
            bare_task("t"),
        );
        let err = env.render().expect_err("unknown kind must error");
        assert!(
            err.to_string().contains("unsupported envelope type: bytes"),
            "got: {}",
            err
        );
    }

    #[test]
    fn render_json_kind_emits_canonical_serialization() {
        let env = Envelope::new(
            "json",
            serde_json::json!({"k": "v", "n": 1}),
            false,
            bare_task("t"),
        );
        let bytes = env.render().expect("json render");
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, serde_json::json!({"k": "v", "n": 1}));
    }

    #[test]
    fn render_bool_kind_emits_lowercase() {
        let env = Envelope::new("bool", serde_json::Value::Bool(true), false, bare_task("t"));
        assert_eq!(env.render().unwrap(), b"true");
    }
}
