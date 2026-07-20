//! Duration-string parser for workflow / task / gate timeout fields.
//!
//! Accepts the same grammar the `tickr-ctx` CLI's `--wait` flag accepts —
//! one consistent duration shape across the system — but lives as an
//! independent implementation because the two crates don't share a
//! dependency edge. The grammar is small enough that two copies don't
//! realistically drift: `<n>s | <n>m | <n>h | <n>` (bare number = seconds).
//!
//! Structured error variants per failure mode let registration-time
//! rejection surface a precise reason to the workflow author rather than a
//! generic anyhow string.

use std::time::Duration;
use thiserror::Error;

/// Failure modes for `parse_duration`. Each variant names the specific
/// defect so registration callers can render an actionable error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseDurationError {
    #[error("duration string is empty")]
    Empty,
    #[error("duration `{raw}` has no numeric component")]
    NotANumber { raw: String },
    #[error("duration `{raw}` overflows u64 seconds")]
    Overflow { raw: String },
    #[error("duration must be positive: `{raw}`")]
    NonPositive { raw: String },
    #[error("duration `{raw}` has unknown unit `{unit}` (expected `s`, `m`, or `h`)")]
    UnknownUnit { raw: String, unit: String },
}

/// Parse a duration string into `Duration`. Accepts `30s` / `5m` / `1h` /
/// `42` (bare number = seconds). Rejects empty input, non-numeric prefix,
/// non-positive values, unknown units, and u64 overflow.
pub fn parse_duration(s: &str) -> Result<Duration, ParseDurationError> {
    let raw = s.to_string();
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(ParseDurationError::Empty);
    }

    // Walk the string splitting at the first non-digit. ASCII digits only
    // — multi-byte numerals fall into the unit half and surface as
    // UnknownUnit, which is the right rejection for a paste-from-some-
    // weird-keyboard case.
    let split = trimmed
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit())
        .map(|(i, _)| i)
        .unwrap_or(trimmed.len());

    let (num_part, unit_part) = trimmed.split_at(split);
    if num_part.is_empty() {
        return Err(ParseDurationError::NotANumber { raw });
    }

    let value: u64 = num_part
        .parse()
        .map_err(|_| ParseDurationError::NotANumber { raw: raw.clone() })?;
    if value == 0 {
        return Err(ParseDurationError::NonPositive { raw });
    }

    let seconds = match unit_part {
        "" | "s" => value,
        "m" => value
            .checked_mul(60)
            .ok_or_else(|| ParseDurationError::Overflow { raw: raw.clone() })?,
        "h" => value
            .checked_mul(3600)
            .ok_or_else(|| ParseDurationError::Overflow { raw: raw.clone() })?,
        other => {
            return Err(ParseDurationError::UnknownUnit {
                raw,
                unit: other.to_string(),
            });
        }
    };

    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_unit_explicit() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn minutes_unit() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn hours_unit() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn bare_number_defaults_to_seconds() {
        // Same shape the tickr-ctx CLI's --wait flag accepts; an author
        // who writes `42` gets 42 seconds, not 42 of-some-other-unit.
        assert_eq!(parse_duration("42").unwrap(), Duration::from_secs(42));
    }

    #[test]
    fn empty_string_rejected() {
        assert_eq!(parse_duration("").unwrap_err(), ParseDurationError::Empty);
        assert_eq!(
            parse_duration("   ").unwrap_err(),
            ParseDurationError::Empty
        );
    }

    #[test]
    fn non_numeric_prefix_rejected() {
        assert!(matches!(
            parse_duration("abc").unwrap_err(),
            ParseDurationError::NotANumber { .. }
        ));
    }

    #[test]
    fn zero_value_rejected() {
        assert!(matches!(
            parse_duration("0s").unwrap_err(),
            ParseDurationError::NonPositive { .. }
        ));
        assert!(matches!(
            parse_duration("0").unwrap_err(),
            ParseDurationError::NonPositive { .. }
        ));
    }

    #[test]
    fn unknown_unit_rejected() {
        let err = parse_duration("30q").unwrap_err();
        match err {
            ParseDurationError::UnknownUnit { unit, .. } => assert_eq!(unit, "q"),
            other => panic!("expected UnknownUnit, got {:?}", other),
        }
    }

    #[test]
    fn whitespace_mid_token_rejected_as_unknown_unit() {
        // `30 s` splits as ("30", " s") at the space — " s" is the unit
        // part and isn't recognized. Acceptable rejection shape; the
        // important property is that the value isn't silently coerced to
        // 30 seconds.
        let err = parse_duration("30 s").unwrap_err();
        assert!(matches!(err, ParseDurationError::UnknownUnit { .. }));
    }

    #[test]
    fn negative_value_rejected_as_not_a_number() {
        // `-5s` has `-` as the first non-digit, so the numeric component
        // is empty. Surfaces as NotANumber — the right shape (no negative
        // durations exist), even if the variant name is slightly indirect.
        assert!(matches!(
            parse_duration("-5s").unwrap_err(),
            ParseDurationError::NotANumber { .. }
        ));
    }

    #[test]
    fn massive_hours_overflow_rejected() {
        // u64::MAX hours overflows when multiplied by 3600.
        let raw = format!("{}h", u64::MAX);
        let err = parse_duration(&raw).unwrap_err();
        assert!(matches!(err, ParseDurationError::Overflow { .. }));
    }
}
