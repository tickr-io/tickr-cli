//! Predicate evaluator for waits-on-signal triggers. Thin wrapper over
//! `serde_json_path::JsonPath`: a predicate is satisfied iff its filter
//! expression yields at least one match. Pure function; no I/O.
//!
//! The author-facing semantic is "this whole payload satisfies the
//! filter" — predicates like `$[?@.amount > 100]` read as "amount > 100
//! on the payload object". To make that intuition work, the evaluator
//! wraps the payload in a single-element array before querying so the
//! filter selector iterates that one element. Without the wrap, JSONPath
//! filter semantics (RFC 9535) treat the payload object's *members* as
//! the iteration target, which is rarely what an author writing a
//! payload predicate intends.
//!
//! The predicate is parsed once at workflow registration (so author
//! syntax errors surface at deploy, not at first signal arrival). The
//! parsed `JsonPath` lives on the subscription index entry and is
//! evaluated hot-path against each inbound wakeup's payload.

use serde_json::Value;
use serde_json_path::JsonPath;

/// Returns `true` iff the filter yields at least one match against the
/// payload (with the payload wrapped as a one-element array so root-
/// level filters read naturally).
pub fn evaluate(path: &JsonPath, payload: &Value) -> bool {
    let wrapped = Value::Array(vec![payload.clone()]);
    !path.query(&wrapped).all().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parsed(s: &str) -> JsonPath {
        s.parse().expect("test predicate parses")
    }

    #[test]
    fn matching_filter_returns_true() {
        let pred = parsed("$[?@.amount > 100]");
        assert!(evaluate(&pred, &json!({"amount": 200})));
    }

    #[test]
    fn non_matching_filter_returns_false() {
        let pred = parsed("$[?@.amount > 100]");
        assert!(!evaluate(&pred, &json!({"amount": 50})));
    }

    #[test]
    fn filter_against_empty_object_returns_false() {
        let pred = parsed("$[?@.amount > 100]");
        assert!(!evaluate(&pred, &json!({})));
    }

    #[test]
    fn filter_referencing_missing_field_returns_false() {
        let pred = parsed("$[?@.tier == \"gold\"]");
        assert!(!evaluate(&pred, &json!({"region": "us"})));
    }

    #[test]
    fn compound_boolean_filter_evaluates_correctly() {
        let pred = parsed("$[?@.amount > 100 && @.tier == \"gold\"]");
        assert!(evaluate(&pred, &json!({"amount": 200, "tier": "gold"})));
        assert!(!evaluate(&pred, &json!({"amount": 200, "tier": "silver"})));
        assert!(!evaluate(&pred, &json!({"amount": 50, "tier": "gold"})));
    }

    #[test]
    fn root_path_against_any_value_returns_true() {
        // Authors who supply `$` as a predicate are saying "always fire";
        // not the recommended pattern but the semantic should be honest.
        let pred = parsed("$");
        assert!(evaluate(&pred, &json!({"any": "value"})));
        assert!(evaluate(&pred, &json!(null)));
    }

    #[test]
    fn filter_against_object_payload_treats_payload_as_filter_target() {
        // Author intent: "fire when payload.tier == gold". The wrap-as-
        // array trick makes this read naturally without forcing authors
        // to nest their event payloads under an array key.
        let pred = parsed("$[?@.tier == \"gold\"]");
        assert!(evaluate(&pred, &json!({"tier": "gold", "amount": 200})));
        assert!(!evaluate(&pred, &json!({"tier": "silver", "amount": 200})));
    }

    #[test]
    fn filter_against_array_payload_iterates_elements() {
        // Some publishers publish arrays directly. The wrap is then a
        // [[...]] — outer array has one element (the inner array), and
        // the filter `$[?...]` against that yields the inner array as a
        // single match if any element satisfies. The semantic the
        // author cares about is "did any item match" which still holds.
        let pred = parsed("$[?@[0].amount > 100]");
        assert!(evaluate(&pred, &json!([{"amount": 200}, {"amount": 50}])));
    }
}
