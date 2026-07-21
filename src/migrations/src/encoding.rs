use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("stored UUID is not canonical lowercase text: {0}")]
    NonCanonicalUuid(String),
    #[error("stored timestamp is outside chrono's supported range: {0}")]
    TimestampOutOfRange(i64),
    #[error("stored JSON is invalid: {0}")]
    InvalidJson(String),
    #[error("stored boolean must be 0 or 1, got {0}")]
    InvalidBoolean(i64),
    #[error("stored enum value `{value}` is not one of {allowed:?}")]
    InvalidEnum {
        value: String,
        allowed: &'static [&'static str],
    },
}

pub fn encode_uuid(value: Uuid) -> String {
    value.hyphenated().to_string()
}

pub fn decode_uuid(value: &str) -> Result<Uuid, DecodeError> {
    let parsed =
        Uuid::parse_str(value).map_err(|_| DecodeError::NonCanonicalUuid(value.to_owned()))?;
    if encode_uuid(parsed) != value {
        return Err(DecodeError::NonCanonicalUuid(value.to_owned()));
    }
    Ok(parsed)
}

pub fn encode_timestamp(value: DateTime<Utc>) -> i64 {
    value.timestamp_micros()
}

pub fn decode_timestamp(value: i64) -> Result<DateTime<Utc>, DecodeError> {
    DateTime::from_timestamp_micros(value).ok_or(DecodeError::TimestampOutOfRange(value))
}

pub fn encode_json(value: &Value) -> String {
    serde_json::to_string(&canonical_json(value)).expect("JSON values are always serializable")
}

pub fn decode_json(value: &str) -> Result<Value, DecodeError> {
    serde_json::from_str(value).map_err(|source| DecodeError::InvalidJson(source.to_string()))
}

pub const fn encode_boolean(value: bool) -> i64 {
    value as i64
}

pub fn decode_boolean(value: i64) -> Result<bool, DecodeError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(DecodeError::InvalidBoolean(other)),
    }
}

pub fn encode_enum(value: &str, allowed: &'static [&'static str]) -> Result<String, DecodeError> {
    decode_enum(value, allowed).map(str::to_owned)
}

pub fn decode_enum<'a>(
    value: &'a str,
    allowed: &'static [&'static str],
) -> Result<&'a str, DecodeError> {
    if allowed.contains(&value) {
        Ok(value)
    } else {
        Err(DecodeError::InvalidEnum {
            value: value.to_owned(),
            allowed,
        })
    }
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            let values = keys
                .into_iter()
                .map(|key| (key.clone(), canonical_json(&values[key])))
                .collect::<Map<_, _>>();
            Value::Object(values)
        }
        scalar => scalar.clone(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike};
    use serde_json::json;

    use super::*;

    #[test]
    fn timestamp_encoding_uses_utc_microseconds() {
        let instant = Utc
            .with_ymd_and_hms(2026, 7, 21, 12, 34, 56)
            .single()
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        let decoded = decode_timestamp(encode_timestamp(instant)).unwrap();
        assert_eq!(decoded.timestamp_subsec_nanos(), 123_456_000);
        assert_eq!(decoded, instant.with_nanosecond(123_456_000).unwrap());
    }

    #[test]
    fn json_encoding_is_recursive_and_canonical() {
        let value = json!({"z": [{"b": 2, "a": 1}], "a": true});
        assert_eq!(encode_json(&value), r#"{"a":true,"z":[{"a":1,"b":2}]}"#);
    }
}
