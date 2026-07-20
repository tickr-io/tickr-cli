//! Captures-merge logic for waits-on-signal: `mkWorkflow.captures` and
//! `triggerOn.captures` are conceptually two declaration lists scoped to
//! the same `signal_id` namespace. At runtime the translator extracts
//! values from the wakeup payload against the merged list.
//!
//! Merge rule: `triggerOn.captures` wins on name collision. The more-
//! specific declaration (scoped to this particular trigger source) takes
//! precedence over the workflow-level default. Pure function; no I/O.

use std::collections::HashMap;
use tickr_proto::workflow::CaptureDeclaration;

/// Merge `workflow_captures` with `trigger_captures`. Returns a flat
/// `Vec` preserving stable iteration order: workflow-level captures
/// first, then trigger-level captures, with any name collisions
/// resolved in favour of the trigger-level entry (so the workflow-level
/// version is dropped from the result).
pub fn merge(
    workflow_captures: &[CaptureDeclaration],
    trigger_captures: &[CaptureDeclaration],
) -> Vec<CaptureDeclaration> {
    let mut by_name: HashMap<String, CaptureDeclaration> = HashMap::new();
    let mut order: Vec<String> =
        Vec::with_capacity(workflow_captures.len() + trigger_captures.len());
    for cap in workflow_captures {
        if !by_name.contains_key(&cap.name) {
            order.push(cap.name.clone());
        }
        by_name.insert(cap.name.clone(), cap.clone());
    }
    for cap in trigger_captures {
        if !by_name.contains_key(&cap.name) {
            order.push(cap.name.clone());
        }
        by_name.insert(cap.name.clone(), cap.clone());
    }
    order
        .into_iter()
        .filter_map(|n| by_name.remove(&n))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickr_proto::workflow::{capture_source, CaptureSource};

    fn cap(name: &str, jsonpath: &str) -> CaptureDeclaration {
        CaptureDeclaration {
            name: name.to_string(),
            from: Some(CaptureSource {
                source: Some(capture_source::Source::Trigger(capture_source::Trigger {
                    jsonpath: jsonpath.to_string(),
                })),
            }),
        }
    }

    fn jsonpath_of(cap: &CaptureDeclaration) -> &str {
        match cap.from.as_ref().and_then(|source| source.source.as_ref()) {
            Some(capture_source::Source::Trigger(trigger)) => &trigger.jsonpath,
            None => panic!("capture declaration is missing trigger source"),
        }
    }

    #[test]
    fn empty_inputs_yield_empty_output() {
        assert!(merge(&[], &[]).is_empty());
    }

    #[test]
    fn workflow_only_passes_through_unchanged() {
        let inputs = vec![cap("a", "$.a"), cap("b", "$.b")];
        let merged = merge(&inputs, &[]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].name, "a");
        assert_eq!(merged[1].name, "b");
    }

    #[test]
    fn trigger_only_passes_through_unchanged() {
        let inputs = vec![cap("a", "$.a"), cap("b", "$.b")];
        let merged = merge(&[], &inputs);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].name, "a");
        assert_eq!(merged[1].name, "b");
    }

    #[test]
    fn disjoint_keys_are_unioned() {
        let workflow_caps = vec![cap("a", "$.a"), cap("b", "$.b")];
        let trigger_caps = vec![cap("c", "$.c"), cap("d", "$.d")];
        let merged = merge(&workflow_caps, &trigger_caps);
        assert_eq!(merged.len(), 4);
        // workflow order first, then trigger order
        assert_eq!(merged[0].name, "a");
        assert_eq!(merged[1].name, "b");
        assert_eq!(merged[2].name, "c");
        assert_eq!(merged[3].name, "d");
    }

    #[test]
    fn key_collision_resolves_in_favour_of_trigger() {
        let workflow_caps = vec![cap("email", "$.user.email")];
        let trigger_caps = vec![cap("email", "$.contact.primary_email")];
        let merged = merge(&workflow_caps, &trigger_caps);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].name, "email");
        assert_eq!(jsonpath_of(&merged[0]), "$.contact.primary_email");
    }

    #[test]
    fn collision_position_follows_first_declaration() {
        // Workflow declares `email` at position 0; trigger overrides it.
        // The result keeps the workflow-order position (email first)
        // because that's the first place the name was seen.
        let workflow_caps = vec![cap("email", "$.user.email"), cap("amount", "$.amount")];
        let trigger_caps = vec![cap("region", "$.region"), cap("email", "$.alt_email")];
        let merged = merge(&workflow_caps, &trigger_caps);
        assert_eq!(
            merged.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["email", "amount", "region"]
        );
        let email = merged.iter().find(|c| c.name == "email").unwrap();
        assert_eq!(jsonpath_of(email), "$.alt_email");
    }
}
