//! Backend-neutral Run-calendar timezone bucketing.

use chrono::{DateTime, Days, NaiveDate, TimeDelta, Utc};
use chrono_tz::Tz;

/// A conservative half-open UTC candidate window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcEnvelope {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Map the authoritative UTC `scheduled_at` instant to the viewer's local date.
pub fn scheduled_local_date(scheduled_at: DateTime<Utc>, timezone: Tz) -> NaiveDate {
    scheduled_at.with_timezone(&timezone).date_naive()
}

/// Cover every UTC instant that can map into `year` under an IANA timezone.
pub fn year_envelope(year: i32) -> Option<UtcEnvelope> {
    let start = NaiveDate::from_ymd_opt(year, 1, 1)?;
    let end = NaiveDate::from_ymd_opt(year.checked_add(1)?, 1, 1)?;
    conservative_envelope(start, end)
}

/// Cover every UTC instant that can map into one viewer-relative local date.
pub fn date_envelope(date: NaiveDate) -> Option<UtcEnvelope> {
    conservative_envelope(date, date.checked_add_days(Days::new(1))?)
}

fn conservative_envelope(start: NaiveDate, end: NaiveDate) -> Option<UtcEnvelope> {
    // IANA civil offsets stay inside one day of UTC. Expanding both local
    // boundaries by a full day remains conservative across historical jumps.
    let margin = TimeDelta::days(1);
    Some(UtcEnvelope {
        start: start
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .checked_sub_signed(margin)?,
        end: end
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .checked_add_signed(margin)?,
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    fn instant(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn date(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()
    }

    fn timezone(value: &str) -> Tz {
        value.parse().unwrap()
    }

    #[test]
    fn buckets_utc_and_positive_and_negative_non_hour_offsets() {
        assert_eq!(
            scheduled_local_date(instant("2026-06-15T00:00:00Z"), timezone("UTC")),
            date("2026-06-15")
        );
        assert_eq!(
            scheduled_local_date(instant("2026-06-14T18:30:00Z"), timezone("Asia/Kathmandu")),
            date("2026-06-15")
        );
        assert_eq!(
            scheduled_local_date(
                instant("2026-06-15T02:45:00Z"),
                timezone("America/St_Johns")
            ),
            date("2026-06-15")
        );
    }

    #[test]
    fn buckets_both_sides_of_dst_transitions_by_absolute_instant() {
        let new_york = timezone("America/New_York");
        for value in [
            "2024-03-10T06:59:59.999999Z",
            "2024-03-10T07:00:00Z",
            "2024-03-10T07:00:00.000001Z",
        ] {
            assert_eq!(
                scheduled_local_date(instant(value), new_york),
                date("2024-03-10")
            );
        }

        // Both occurrences of the repeated 01:30 local time remain November 3.
        for value in ["2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"] {
            assert_eq!(
                scheduled_local_date(instant(value), new_york),
                date("2024-11-03")
            );
        }
    }

    #[test]
    fn preserves_microsecond_midnight_and_year_boundaries() {
        let new_york = timezone("America/New_York");
        assert_eq!(
            scheduled_local_date(instant("2026-06-15T03:59:59.999999Z"), new_york),
            date("2026-06-14")
        );
        assert_eq!(
            scheduled_local_date(instant("2026-06-15T04:00:00Z"), new_york),
            date("2026-06-15")
        );
        assert_eq!(
            scheduled_local_date(instant("2026-06-15T04:00:00.000001Z"), new_york),
            date("2026-06-15")
        );
        assert_eq!(
            scheduled_local_date(instant("2025-12-31T15:00:00Z"), timezone("Asia/Tokyo")),
            date("2026-01-01")
        );
    }

    #[test]
    fn uses_historical_rules_and_skips_nonexistent_civil_dates() {
        // Kathmandu used +05:30 in 1985, rather than its current +05:45.
        assert_eq!(
            scheduled_local_date(instant("1985-06-01T18:20:00Z"), timezone("Asia/Kathmandu")),
            date("1985-06-01")
        );

        let apia = timezone("Pacific/Apia");
        assert_eq!(
            scheduled_local_date(instant("2011-12-30T09:59:59.999999Z"), apia),
            date("2011-12-29")
        );
        assert_eq!(
            scheduled_local_date(instant("2011-12-30T10:00:00Z"), apia),
            date("2011-12-31")
        );
    }

    #[test]
    fn recomputes_placement_for_each_viewer_timezone() {
        let scheduled_at = instant("2026-01-01T00:30:00Z");
        assert_eq!(
            scheduled_local_date(scheduled_at, timezone("UTC")),
            date("2026-01-01")
        );
        assert_eq!(
            scheduled_local_date(scheduled_at, timezone("Pacific/Honolulu")),
            date("2025-12-31")
        );
    }

    #[test]
    fn candidate_envelopes_include_boundary_offsets() {
        assert_eq!(
            year_envelope(2026),
            Some(UtcEnvelope {
                start: instant("2025-12-31T00:00:00Z"),
                end: instant("2027-01-02T00:00:00Z"),
            })
        );
        assert_eq!(
            date_envelope(date("2026-06-15")),
            Some(UtcEnvelope {
                start: instant("2026-06-14T00:00:00Z"),
                end: instant("2026-06-17T00:00:00Z"),
            })
        );
    }
}
