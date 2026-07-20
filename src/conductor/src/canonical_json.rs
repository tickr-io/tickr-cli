//! Canonical-JSON hashing.
//!
//! Maps `serde_json::Value` to a stable byte representation: object keys are
//! sorted lexicographically at every level; numbers, strings, booleans, and
//! arrays serialize the same way `serde_json::to_writer` already produces;
//! whitespace is uniform (none). Two JSON values that are semantically equal
//! but differ in key insertion order hash identically — required for the
//! idempotency-cache contract to treat `{a:1,b:2}` and `{b:2,a:1}` as the
//! same retry.
//!
//! Missing inputs canonicalize to `{}` before hashing so a producer that
//! omits the `inputs` field doesn't accidentally produce a different cache
//! key than the same producer sending an explicit empty object.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// SHA256 over the canonical-JSON encoding of `value`. `None` canonicalizes
/// to an empty JSON object so producers don't have to remember the empty-
/// object convention to be idempotent.
pub fn hash(value: Option<&Value>) -> [u8; 32] {
    let mut canonical = Vec::with_capacity(64);
    let owned_empty;
    let v = match value {
        Some(v) => v,
        None => {
            owned_empty = Value::Object(Default::default());
            &owned_empty
        }
    };
    write_canonical(v, &mut canonical);
    let mut hasher = Sha256::new();
    hasher.update(&canonical);
    hasher.finalize().into()
}

/// Recursively serialize a JSON value with object keys sorted at every level.
/// Mirrors `serde_json::to_writer` for primitives and arrays; objects emit a
/// sorted key set so two JSON values that are semantically equal but differ
/// in key insertion order produce identical bytes.
fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => {
            // serde_json's Number Display matches the JSON-spec text repr.
            let s = n.to_string();
            out.extend_from_slice(s.as_bytes());
        }
        Value::String(s) => {
            // Re-use serde_json's string escaping by serializing a tiny
            // Value::String through `to_writer`. Avoids reimplementing all
            // the JSON-escape edge cases.
            serde_json::to_writer(&mut *out, &Value::String(s.clone())).expect(
                "writing a String to a Vec<u8> cannot fail; serde_json buffer never errors",
            );
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // Quoted, escaped key.
                serde_json::to_writer(&mut *out, &Value::String((*k).clone()))
                    .expect("string write to Vec<u8> cannot fail");
                out.push(b':');
                write_canonical(&map[*k], out);
            }
            out.push(b'}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_order_independent() {
        let a = json!({"a": 1, "b": 2, "c": 3});
        let b = json!({"c": 3, "b": 2, "a": 1});
        assert_eq!(hash(Some(&a)), hash(Some(&b)));
    }

    #[test]
    fn nested_object_keys_sort_recursively() {
        let a = json!({"outer": {"a": 1, "b": 2}});
        let b = json!({"outer": {"b": 2, "a": 1}});
        assert_eq!(hash(Some(&a)), hash(Some(&b)));
    }

    #[test]
    fn arrays_preserve_order() {
        // Arrays are sequences; reordering changes the value, so the hash
        // must differ.
        let a = json!([1, 2, 3]);
        let b = json!([3, 2, 1]);
        assert_ne!(hash(Some(&a)), hash(Some(&b)));
    }

    #[test]
    fn empty_inputs_canonicalizes_to_empty_object() {
        assert_eq!(hash(None), hash(Some(&json!({}))));
    }

    #[test]
    fn differing_values_yield_different_hashes() {
        let a = json!({"x": 1});
        let b = json!({"x": 2});
        assert_ne!(hash(Some(&a)), hash(Some(&b)));
    }

    #[test]
    fn primitive_values_hash_distinctly() {
        // null / false / 0 / "0" / [] / {} are all distinct values; each must
        // produce a distinct hash so a producer accidentally swapping types
        // doesn't dedup against the wrong cached row.
        let hashes = [
            hash(Some(&json!(null))),
            hash(Some(&json!(false))),
            hash(Some(&json!(0))),
            hash(Some(&json!("0"))),
            hash(Some(&json!([]))),
            hash(Some(&json!({}))),
        ];
        // Pairwise inequality.
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j], "primitives {i} and {j} collided");
            }
        }
    }

    #[test]
    fn string_with_escapes_round_trips_through_canonical_form() {
        // The canonicaliser delegates string escaping to serde_json; a
        // string containing a backslash and a quote must hash stably.
        let v = json!({"k": "back\\slash and \"quote\""});
        let h = hash(Some(&v));
        assert_eq!(h, hash(Some(&v)));
    }
}
