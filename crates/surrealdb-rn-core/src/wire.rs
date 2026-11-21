use std::collections::BTreeMap;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, json};
use surrealdb::types::{
    Array, Bytes, Datetime, Decimal, Number, Object, RecordId, RecordIdKey, Set, ToSql, Uuid, Value,
};

use crate::SurrealRnError;

const TAG: &str = "$surreal";
const MAX_WIRE_DEPTH: usize = 128;

pub fn encode_value(value: Value) -> Result<String, SurrealRnError> {
    serde_json::to_string(&WireValue(&value)).map_err(|error| SurrealRnError::Codec {
        message: error.to_string(),
    })
}

pub fn encode_value_tree(value: Value) -> Result<String, SurrealRnError> {
    serde_json::to_string(&to_wire_value(value)).map_err(|error| SurrealRnError::Codec {
        message: error.to_string(),
    })
}

struct WireValue<'a>(&'a Value);

impl Serialize for WireValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::None => tagged(serializer, "none", None),
            Value::Null => serializer.serialize_none(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(Number::Int(value)) => tagged(serializer, "int", Some(value.to_string())),
            Value::Number(Number::Float(value)) if value.is_finite() => {
                serializer.serialize_f64(*value)
            }
            Value::Number(Number::Float(value)) => {
                tagged(serializer, "float", Some(value.to_string()))
            }
            Value::Number(Number::Decimal(value)) => {
                tagged(serializer, "decimal", Some(value.to_string()))
            }
            Value::String(value) => serializer.serialize_str(value),
            Value::Bytes(value) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TAG, "bytes")?;
                map.serialize_entry("base64", &BASE64.encode(value.as_ref()))?;
                map.end()
            }
            Value::Duration(value) => tagged(
                serializer,
                "duration",
                Some(Value::Duration(*value).to_sql()),
            ),
            Value::Datetime(value) => tagged(
                serializer,
                "datetime",
                Some(Value::Datetime(*value).to_sql()),
            ),
            Value::Uuid(value) => tagged(serializer, "uuid", Some(value.to_string())),
            Value::Geometry(value) => tagged(
                serializer,
                "geometry",
                Some(Value::Geometry(value.clone()).to_sql()),
            ),
            Value::Table(value) => tagged(serializer, "table", Some(value.as_str().to_owned())),
            Value::RecordId(value) => tagged(serializer, "record", Some(value.to_sql())),
            Value::File(value) => tagged(
                serializer,
                "file",
                Some(Value::File(value.clone()).to_sql()),
            ),
            Value::Range(value) => tagged(
                serializer,
                "range",
                Some(Value::Range(value.clone()).to_sql()),
            ),
            Value::Regex(value) => tagged(
                serializer,
                "regex",
                Some(Value::Regex(value.clone()).to_sql()),
            ),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values.iter() {
                    sequence.serialize_element(&WireValue(value))?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values.iter() {
                    map.serialize_entry(key, &WireValue(value))?;
                }
                map.end()
            }
            Value::Set(values) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TAG, "set")?;
                map.serialize_entry("values", &WireValues(values.iter()))?;
                map.end()
            }
        }
    }
}

struct WireValues<'a, I>(I)
where
    I: Iterator<Item = &'a Value> + Clone;

impl<'a, I> Serialize for WireValues<'a, I>
where
    I: Iterator<Item = &'a Value> + Clone,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let iterator = self.0.clone();
        let mut sequence = serializer.serialize_seq(iterator.size_hint().1)?;
        for value in iterator {
            sequence.serialize_element(&WireValue(value))?;
        }
        sequence.end()
    }
}

fn tagged<S>(serializer: S, kind: &str, value: Option<String>) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(if value.is_some() { 2 } else { 1 }))?;
    map.serialize_entry(TAG, kind)?;
    if let Some(value) = value {
        map.serialize_entry("value", &value)?;
    }
    map.end()
}

pub fn decode_variables(input: Option<&str>) -> Result<Object, SurrealRnError> {
    let Some(input) = input else {
        return Ok(Object::new());
    };

    let json: JsonValue =
        serde_json::from_str(input).map_err(|error| SurrealRnError::InvalidVariables {
            message: error.to_string(),
        })?;
    let value = from_wire_value(json)?;
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(SurrealRnError::InvalidVariables {
            message: "query variables must be a JSON object".into(),
        }),
    }
}

fn to_wire_value(value: Value) -> JsonValue {
    match value {
        Value::None => json!({ TAG: "none" }),
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::Bool(value),
        Value::Number(Number::Int(value)) => json!({ TAG: "int", "value": value.to_string() }),
        Value::Number(Number::Float(value)) if value.is_finite() => JsonNumber::from_f64(value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Number(Number::Float(value)) => {
            json!({ TAG: "float", "value": value.to_string() })
        }
        Value::Number(Number::Decimal(value)) => {
            json!({ TAG: "decimal", "value": value.to_string() })
        }
        Value::String(value) => JsonValue::String(value),
        Value::Bytes(value) => json!({ TAG: "bytes", "base64": BASE64.encode(value.as_ref()) }),
        Value::Duration(value) => {
            json!({ TAG: "duration", "value": Value::Duration(value).to_sql() })
        }
        Value::Datetime(value) => {
            json!({ TAG: "datetime", "value": Value::Datetime(value).to_sql() })
        }
        Value::Uuid(value) => json!({ TAG: "uuid", "value": value.to_string() }),
        Value::Geometry(value) => {
            json!({ TAG: "geometry", "value": Value::Geometry(value).to_sql() })
        }
        Value::Table(value) => json!({ TAG: "table", "value": value.as_str() }),
        Value::RecordId(value) => json!({ TAG: "record", "value": value.to_sql() }),
        Value::File(value) => json!({ TAG: "file", "value": Value::File(value).to_sql() }),
        Value::Range(value) => json!({ TAG: "range", "value": Value::Range(value).to_sql() }),
        Value::Regex(value) => json!({ TAG: "regex", "value": Value::Regex(value).to_sql() }),
        Value::Array(value) => JsonValue::Array(value.into_iter().map(to_wire_value).collect()),
        Value::Object(value) => JsonValue::Object(
            value
                .into_iter()
                .map(|(key, value)| (key, to_wire_value(value)))
                .collect(),
        ),
        Value::Set(value) => json!({
            TAG: "set",
            "values": value.into_iter().map(to_wire_value).collect::<Vec<_>>()
        }),
    }
}

pub(crate) fn from_wire_value(value: JsonValue) -> Result<Value, SurrealRnError> {
    from_wire_value_at_depth(value, 0)
}

fn from_wire_value_at_depth(value: JsonValue, depth: usize) -> Result<Value, SurrealRnError> {
    if depth > MAX_WIRE_DEPTH {
        return Err(invalid_variables(
            "value nesting exceeds the supported depth",
        ));
    }

    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Ok(Value::Number(Number::Int(integer)))
            } else if let Some(float) = value.as_f64() {
                Ok(Value::Number(Number::Float(float)))
            } else {
                Err(invalid_variables("number is outside the supported range"))
            }
        }
        JsonValue::String(value) => Ok(Value::String(value)),
        JsonValue::Array(values) => values
            .into_iter()
            .map(|value| from_wire_value_at_depth(value, depth + 1))
            .collect::<Result<Vec<_>, _>>()
            .map(Array::from)
            .map(Value::Array),
        JsonValue::Object(mut object) => {
            if let Some(tag) = object.remove(TAG) {
                return decode_tagged_value(tag, object, depth);
            }

            object
                .into_iter()
                .map(|(key, value)| {
                    from_wire_value_at_depth(value, depth + 1).map(|value| (key, value))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Object::from)
                .map(Value::Object)
        }
    }
}

fn decode_tagged_value(
    tag: JsonValue,
    mut object: JsonMap<String, JsonValue>,
    depth: usize,
) -> Result<Value, SurrealRnError> {
    let tag = tag
        .as_str()
        .ok_or_else(|| invalid_variables("$surreal tag must be a string"))?;

    match tag {
        "none" => Ok(Value::None),
        "int" => required_string(&mut object, "value")?
            .parse::<i64>()
            .map(Number::Int)
            .map(Value::Number)
            .map_err(|error| invalid_variables(error.to_string())),
        "float" => required_string(&mut object, "value")?
            .parse::<f64>()
            .map(Number::Float)
            .map(Value::Number)
            .map_err(|error| invalid_variables(error.to_string())),
        "decimal" => Decimal::from_str(&required_string(&mut object, "value")?)
            .map(Number::Decimal)
            .map(Value::Number)
            .map_err(|error| invalid_variables(error.to_string())),
        "datetime" => {
            let literal = required_string(&mut object, "value")?;
            if !object.is_empty() {
                return Err(invalid_variables("datetime tag has unsupported fields"));
            }
            let timestamp = literal
                .strip_prefix("d'")
                .and_then(|value| value.strip_suffix('\''))
                .ok_or_else(|| invalid_variables("datetime must be a quoted literal"))?;
            let datetime = Datetime::from_str(timestamp)
                .map_err(|_| invalid_variables("invalid datetime literal"))?;
            if Value::Datetime(datetime).to_sql() != literal {
                return Err(invalid_variables("noncanonical datetime literal"));
            }
            Ok(Value::Datetime(datetime))
        }
        "bytes" => BASE64
            .decode(required_string(&mut object, "base64")?)
            .map(Bytes::from)
            .map(Value::Bytes)
            .map_err(|error| invalid_variables(error.to_string())),
        "uuid" => Uuid::from_str(&required_string(&mut object, "value")?)
            .map(Value::Uuid)
            .map_err(|error| invalid_variables(error.to_string())),
        "record" => {
            let literal = required_string(&mut object, "value")?;
            decode_record_id_literal(&literal).map(Value::RecordId)
        }
        "set" => {
            let values = object
                .remove("values")
                .and_then(|value| value.as_array().cloned())
                .ok_or_else(|| invalid_variables("set requires a values array"))?;
            values
                .into_iter()
                .map(|value| from_wire_value_at_depth(value, depth + 1))
                .collect::<Result<Vec<_>, _>>()
                .map(Set::from)
                .map(Value::Set)
        }
        other => Err(invalid_variables(format!(
            "unsupported $surreal tag '{other}'"
        ))),
    }
}

// Decode only canonical scalar record keys. The SDK exposes no typed parser for
// record literals; parse_simple treats a quoted/numeric key as a plain string.
// Construct the typed key and require SurrealDB's serializer to reproduce the
// exact input, so unsupported key shapes cannot silently change identity.
fn decode_record_id_literal(literal: &str) -> Result<RecordId, SurrealRnError> {
    let (table, key) = literal
        .split_once(':')
        .ok_or_else(|| invalid_variables("record id must contain a table and key"))?;
    let key = if let Some(quoted) = key.strip_prefix('`').and_then(|key| key.strip_suffix('`')) {
        if quoted.is_empty() || quoted.contains('`') || quoted.contains('\\') {
            return Err(invalid_variables("unsupported quoted record id key"));
        }
        RecordIdKey::String(quoted.to_owned())
    } else if key.as_bytes().iter().all(u8::is_ascii_digit) {
        RecordIdKey::Number(
            key.parse::<i64>()
                .map_err(|_| invalid_variables("record id number is outside i64"))?,
        )
    } else {
        RecordIdKey::String(key.to_owned())
    };
    let record = RecordId::new(table, key);
    if record.to_sql() != literal {
        return Err(invalid_variables("unsupported record id key shape"));
    }
    Ok(record)
}

fn required_string(
    object: &mut JsonMap<String, JsonValue>,
    key: &str,
) -> Result<String, SurrealRnError> {
    object
        .remove(key)
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| invalid_variables(format!("tagged value requires string field '{key}'")))
}

fn invalid_variables(message: impl Into<String>) -> SurrealRnError {
    SurrealRnError::InvalidVariables {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_large_integers_without_javascript_precision_loss() {
        let encoded = encode_value(Value::Number(Number::Int(i64::MAX))).unwrap();
        assert_eq!(
            encoded,
            r#"{"$surreal":"int","value":"9223372036854775807"}"#
        );
    }

    #[test]
    fn streaming_and_tree_encoders_produce_equivalent_wire_values() {
        let value = Value::Object(
            decode_variables(Some(
                r#"{
                    "bytes":{"$surreal":"bytes","base64":"AAH+/w=="},
                    "count":{"$surreal":"int","value":"42"},
                    "decimal":{"$surreal":"decimal","value":"1234567890.0000000001"},
                    "finiteFloat":1.25,
                    "missing":{"$surreal":"none"},
                    "name":"answer",
                    "nested":[true,null],
                    "nonFiniteFloat":{"$surreal":"float","value":"NaN"},
                    "record":{"$surreal":"record","value":"person:ada"},
                    "set":{"$surreal":"set","values":["x",{"$surreal":"int","value":"7"}]},
                    "uuid":{"$surreal":"uuid","value":"2f1b0ff8-0c2d-4b2b-bdce-497784869c2f"}
                }"#,
            ))
            .unwrap(),
        );
        let streaming: JsonValue =
            serde_json::from_str(&encode_value(value.clone()).unwrap()).unwrap();
        let tree: JsonValue = serde_json::from_str(&encode_value_tree(value).unwrap()).unwrap();

        assert_eq!(streaming, tree);
    }

    #[test]
    fn decodes_tagged_variables() {
        let variables = decode_variables(Some(
			r#"{"limit":{"$surreal":"int","value":"9007199254740993"},"missing":{"$surreal":"none"}}"#,
		))
		.unwrap();
        assert_eq!(
            variables.get("limit"),
            Some(&Value::Number(Number::Int(9_007_199_254_740_993)))
        );
        assert_eq!(variables.get("missing"), Some(&Value::None));
    }

    #[test]
    fn datetime_round_trips_at_nanosecond_precision_with_nested_values() {
        let input = r#"{
            "when":{"$surreal":"datetime","value":"d'2026-01-01T00:00:00.123456789Z'"},
            "nested":{"integer":{"$surreal":"int","value":"9007199254740993"},
                "decimal":{"$surreal":"decimal","value":"1234567890.0000000001"},
                "bytes":{"$surreal":"bytes","base64":"AAH+/w=="},
                "record":{"$surreal":"record","value":"person:ada"},
                "missing":{"$surreal":"none"},"null":null}
        }"#;
        let decoded = Value::Object(decode_variables(Some(input)).unwrap());
        let streaming: JsonValue =
            serde_json::from_str(&encode_value(decoded.clone()).unwrap()).unwrap();
        let tree: JsonValue = serde_json::from_str(&encode_value_tree(decoded).unwrap()).unwrap();
        assert_eq!(streaming, serde_json::from_str::<JsonValue>(input).unwrap());
        assert_eq!(tree, streaming);
        assert_ne!(streaming["nested"]["missing"], streaming["nested"]["null"]);
    }

    #[test]
    fn datetime_decoder_rejects_noncanonical_and_injected_sql() {
        for literal in [
            "2026-01-01T00:00:00Z",
            "d'2026-01-01T00:00:00Z'; DELETE note",
            "d'not-a-date'",
            "d'2026-01-01T01:00:00+01:00'",
            "d'2026-01-01T00:00:00.1234567890Z'",
        ] {
            let input = json!({"when": {TAG: "datetime", "value": literal}}).to_string();
            assert!(
                matches!(
                    decode_variables(Some(&input)),
                    Err(SurrealRnError::InvalidVariables { .. })
                ),
                "accepted {literal}"
            );
        }
        assert!(
            decode_variables(Some(
                r#"{"when":{"$surreal":"datetime","value":"d'2026-01-01T00:00:00Z'","extra":true}}"#
            ))
            .is_err()
        );
        assert!(
            decode_variables(Some(
                r#"{"when":{"$surreal":"geometry","value":"point(1,2)"}}"#
            ))
            .is_err()
        );
    }

    #[test]
    fn record_decoder_rejects_unhandled_key_syntax() {
        assert!(
            decode_variables(Some(r#"{"id":{"$surreal":"record","value":"person:ada"}}"#)).is_ok()
        );
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let app_id = RecordId::new("note", uuid);
        let encoded = encode_value(Value::RecordId(app_id.clone())).unwrap();
        assert_eq!(
            encoded,
            format!(r#"{{"$surreal":"record","value":"note:`{uuid}`"}}"#)
        );
        let decoded = decode_variables(Some(&format!(r#"{{"id":{encoded}}}"#))).unwrap();
        assert_eq!(decoded.get("id"), Some(&Value::RecordId(app_id)));
        let numeric = decode_record_id_literal("person:123").unwrap();
        let numeric_string = decode_record_id_literal("person:`123`").unwrap();
        assert_eq!(numeric.key, RecordIdKey::Number(123));
        assert_eq!(numeric_string.key, RecordIdKey::String("123".into()));
        assert_ne!(numeric, numeric_string);
        for record in ["person:[1,2]", "person:{key:'one'}", "person:stable-1"] {
            let input = json!({"id": {TAG: "record", "value": record}}).to_string();
            assert!(decode_variables(Some(&input)).is_err(), "accepted {record}");
        }
    }

    #[test]
    fn rejects_excessively_nested_values_before_recursive_decoding() {
        let mut value = JsonValue::Null;
        for _ in 0..=MAX_WIRE_DEPTH {
            value = JsonValue::Array(vec![value]);
        }
        let error = from_wire_value(value).unwrap_err();
        assert!(matches!(error, SurrealRnError::InvalidVariables { .. }));
        assert!(
            error
                .to_string()
                .contains("nesting exceeds the supported depth")
        );
    }
}
