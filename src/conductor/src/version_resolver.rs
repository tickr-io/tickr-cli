//! Pure register-decision resolver.
//!
//! Given the incoming content hash and a snapshot of the latest stored version
//! row for a `workflow_id`, decide what registration should do. This is the
//! heart of system-assigned versioning, deliberately separated from the SQL
//! that effects it so it is exhaustively unit-testable without a database.
//!
//! The four register outcomes are all resolved here. When the incoming content
//! hash differs from the latest row (or there is no row) the decision is
//! [`RegisterDecision::Insert`]. When it *matches*, the decision branches on the
//! latest row's build status and whether the cosmetic fields changed:
//! [`NoOp`](RegisterDecision::NoOp), [`Refreshed`](RegisterDecision::Refreshed),
//! or [`BuildRequeued`](RegisterDecision::BuildRequeued).

/// Snapshot of the latest (highest-version) stored row for a `workflow_id`.
/// `version` is therefore `MAX(version)` for that id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestRow {
    pub version: i64,
    pub content_hash: String,
    /// The latest row's cosmetic-field hash (display `name` + `tags`). Compared
    /// only when the content hash matches, to split Refreshed from NoOp.
    pub cosmetic_hash: String,
    /// The latest row's build lifecycle status: `Building` | `Ready` |
    /// `Submitted` | `BuildFailed`. Selects the hash-match branch.
    pub status: String,
}

/// What registration should do, given the incoming content vs. the latest row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterDecision {
    /// Persist a new version row at `version`. Covers the first registration
    /// (`version = 1`), a substantive change (`MAX + 1`), and rollback-by-
    /// resubmit (incoming content matches an *older* version, but differs from
    /// the latest, so it still mints a new `MAX + 1` row).
    Insert { version: i64 },
    /// Content matches the latest version and the latest built successfully, but
    /// a cosmetic field (display `name` / `tags`) changed. Update the latest
    /// row's cosmetic columns + archived source in place; no version bump.
    Refreshed { version: i64 },
    /// Content matches the latest version whose build previously failed.
    /// Re-enqueue that version's failed task builds on the same row; no bump.
    BuildRequeued { version: i64 },
    /// Content matches the latest version byte-for-byte (or a build is still in
    /// flight); nothing is persisted. `version` echoes the matched version.
    NoOp { version: i64 },
}

/// Resolve the registration outcome from the incoming content + cosmetic hashes
/// and the latest-row snapshot (or `None` when the `workflow_id` has no rows).
///
/// - No prior row → `Insert { version: 1 }`.
/// - Content hash != latest → `Insert { MAX + 1 }` (includes rollback: the
///   incoming content matches an older version but differs from the latest).
/// - Content hash == latest, branching on the latest row's status:
///   - `BuildFailed` → `BuildRequeued` (re-run the flaky build, no bump).
///   - `Building` → `NoOp` (a build is already in flight; don't double-enqueue).
///   - terminal-success (`Ready`/`Submitted`):
///     - cosmetic hash differs → `Refreshed` (update cosmetics in place).
///     - cosmetic hash matches → `NoOp` (byte-identical re-submit).
pub fn resolve(
    incoming_content_hash: &str,
    incoming_cosmetic_hash: &str,
    latest: Option<&LatestRow>,
) -> RegisterDecision {
    let Some(row) = latest else {
        return RegisterDecision::Insert { version: 1 };
    };
    if row.content_hash != incoming_content_hash {
        return RegisterDecision::Insert {
            version: row.version + 1,
        };
    }
    // Content matches the latest version — branch on its build status.
    match row.status.as_str() {
        "BuildFailed" => RegisterDecision::BuildRequeued {
            version: row.version,
        },
        "Building" => RegisterDecision::NoOp {
            version: row.version,
        },
        // Terminal-success (Ready / Submitted) and anything else treated as
        // settled: a cosmetic-only change refreshes in place; otherwise no-op.
        _ if row.cosmetic_hash != incoming_cosmetic_hash => RegisterDecision::Refreshed {
            version: row.version,
        },
        _ => RegisterDecision::NoOp {
            version: row.version,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A latest-row snapshot with explicit status + cosmetic hash. Most tests
    /// use `Ready` (terminal-success) and a fixed cosmetic hash `"cos"`.
    fn row_full(version: i64, content: &str, cosmetic: &str, status: &str) -> LatestRow {
        LatestRow {
            version,
            content_hash: content.to_string(),
            cosmetic_hash: cosmetic.to_string(),
            status: status.to_string(),
        }
    }

    fn ready(version: i64, content: &str) -> LatestRow {
        row_full(version, content, "cos", "Ready")
    }

    #[test]
    fn no_prior_row_inserts_version_one() {
        assert_eq!(
            resolve("abc", "cos", None),
            RegisterDecision::Insert { version: 1 }
        );
    }

    #[test]
    fn identical_content_and_cosmetics_is_a_noop() {
        let latest = ready(3, "abc");
        assert_eq!(
            resolve("abc", "cos", Some(&latest)),
            RegisterDecision::NoOp { version: 3 }
        );
    }

    #[test]
    fn changed_content_inserts_at_max_plus_one() {
        let latest = ready(3, "abc");
        assert_eq!(
            resolve("xyz", "cos", Some(&latest)),
            RegisterDecision::Insert { version: 4 }
        );
    }

    #[test]
    fn rollback_to_older_content_still_inserts_a_new_version() {
        // Incoming content matches an *older* version's hash ("v1hash"), but the
        // latest row's hash differs — so it is a change relative to latest and
        // mints a new MAX+1 row rather than reactivating the old version.
        let latest = ready(5, "v5hash");
        assert_eq!(
            resolve("v1hash", "cos", Some(&latest)),
            RegisterDecision::Insert { version: 6 }
        );
    }

    #[test]
    fn content_match_with_cosmetic_change_on_ready_is_refreshed() {
        let latest = row_full(3, "abc", "old-cos", "Ready");
        assert_eq!(
            resolve("abc", "new-cos", Some(&latest)),
            RegisterDecision::Refreshed { version: 3 }
        );
    }

    #[test]
    fn submitted_is_terminal_success_too_so_cosmetic_change_refreshes() {
        let latest = row_full(3, "abc", "old-cos", "Submitted");
        assert_eq!(
            resolve("abc", "new-cos", Some(&latest)),
            RegisterDecision::Refreshed { version: 3 }
        );
    }

    #[test]
    fn content_match_on_build_failed_requeues() {
        let latest = row_full(3, "abc", "cos", "BuildFailed");
        // Even if cosmetics also differ, a failed latest re-runs the build.
        assert_eq!(
            resolve("abc", "different", Some(&latest)),
            RegisterDecision::BuildRequeued { version: 3 }
        );
    }

    #[test]
    fn content_match_while_building_is_a_noop() {
        let latest = row_full(3, "abc", "cos", "Building");
        assert_eq!(
            resolve("abc", "different", Some(&latest)),
            RegisterDecision::NoOp { version: 3 }
        );
    }

    #[test]
    fn changed_content_on_build_failed_still_inserts_a_new_version() {
        // A genuine content change supersedes the failed version with a new one,
        // rather than requeueing the old failed build.
        let latest = row_full(3, "abc", "cos", "BuildFailed");
        assert_eq!(
            resolve("xyz", "cos", Some(&latest)),
            RegisterDecision::Insert { version: 4 }
        );
    }
}
