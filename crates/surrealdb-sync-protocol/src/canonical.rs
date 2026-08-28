//! The owned, bounded canonical value and commit fingerprint profile for `surrealdb-sync/1`.

use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::io::{self, Cursor, Write as IoWrite};

use crate::{
    BaseVersion, ClientCommitId, ClientId, Fingerprint, Operation, PartitionId, RecordId, V1_NAME,
};
use ciborium::value::Value as CborValue;
use sha2::{Digest, Sha256};

const NONE_TAG: u64 = 6;
const RECORD_ID_TAG: u64 = 8;
const FINGERPRINT_DOMAIN: &str = "surrealdb-sync/1/commit-fingerprint/sha256";

pub const MAX_DEPTH: usize = 32;
pub const MAX_CONTAINER_ITEMS: usize = 1_024;
pub const MAX_TOTAL_ITEMS: usize = 4_096;
pub const MAX_STRING_BYTES: usize = 1_048_576;
pub const MAX_ENCODED_BYTES: usize = 4_194_304;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalValue {
    None,
    Null,
    Bool(bool),
    Int(i64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<CanonicalValue>),
    Object(BTreeMap<String, CanonicalValue>),
    RecordId {
        table: String,
        key: Box<CanonicalValue>,
    },
}

type Value = CanonicalValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    Decode,
    Encode,
    TrailingBytes,
    NonCanonical,
    Unsupported(&'static str),
    InvalidRecordId,
    DuplicateObjectKey,
    InvalidIdentifier,
    InvalidFingerprint,
    InvalidEnvelope,
    DepthLimit,
    ContainerLimit,
    StringLimit,
    EncodedLimit,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode => formatter.write_str("invalid CBOR"),
            Self::Encode => formatter.write_str("CBOR encoding failed"),
            Self::TrailingBytes => formatter.write_str("trailing bytes after the CBOR value"),
            Self::NonCanonical => formatter.write_str("CBOR input is not canonical"),
            Self::Unsupported(kind) => write!(formatter, "unsupported value kind: {kind}"),
            Self::InvalidRecordId => formatter.write_str("invalid record ID"),
            Self::DuplicateObjectKey => formatter.write_str("duplicate object key"),
            Self::InvalidIdentifier => formatter.write_str("invalid or oversized identifier"),
            Self::InvalidFingerprint => formatter.write_str("invalid commit fingerprint"),
            Self::InvalidEnvelope => formatter.write_str("invalid protocol envelope"),
            Self::DepthLimit => formatter.write_str("value exceeds the depth limit"),
            Self::ContainerLimit => formatter.write_str("container exceeds the item limit"),
            Self::StringLimit => formatter.write_str("string or byte value exceeds the size limit"),
            Self::EncodedLimit => formatter.write_str("encoded value exceeds the size limit"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Encode one value with the deterministic v1 CBOR profile.
pub fn canonical_cbor(value: &CanonicalValue) -> Result<Vec<u8>, CodecError> {
    validate(value)?;
    let bytes = encode_cbor_value(to_cbor(value)?)?;
    preflight_canonical_cbor(&bytes)?;
    Ok(bytes)
}

/// Decode one bounded, canonical v1 CBOR value.
pub fn decode_canonical_cbor(bytes: &[u8]) -> Result<CanonicalValue, CodecError> {
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::EncodedLimit);
    }
    preflight_canonical_cbor(bytes)?;

    let mut reader = Cursor::new(bytes);
    let decoded: CborValue =
        ciborium::de::from_reader(&mut reader).map_err(|_| CodecError::Decode)?;
    if reader.position() != bytes.len() as u64 {
        return Err(CodecError::TrailingBytes);
    }

    let value = from_cbor(decoded)?;
    validate(&value)?;
    if canonical_cbor(&value)? != bytes {
        return Err(CodecError::NonCanonical);
    }
    Ok(value)
}

/// Compute the domain-separated SHA-256 fingerprint for an ordered commit.
pub fn fingerprint_commit(
    partition_id: &PartitionId,
    client_id: &ClientId,
    client_commit_id: &ClientCommitId,
    operations: &[Operation<Value>],
) -> Result<Fingerprint, CodecError> {
    if operations.len() > MAX_CONTAINER_ITEMS {
        return Err(CodecError::ContainerLimit);
    }
    validate_identifier(&partition_id.0)?;
    validate_identifier(&client_id.0)?;
    validate_identifier(&client_commit_id.0)?;

    let mut remaining_items = MAX_TOTAL_ITEMS;
    let mut remaining_bytes = MAX_ENCODED_BYTES;
    take_bytes(&mut remaining_bytes, FINGERPRINT_DOMAIN.len())?;
    take_bytes(&mut remaining_bytes, V1_NAME.len())?;
    take_bytes(&mut remaining_bytes, partition_id.0.len())?;
    take_bytes(&mut remaining_bytes, client_id.0.len())?;
    take_bytes(&mut remaining_bytes, client_commit_id.0.len())?;
    let operations = operations
        .iter()
        .map(|operation| operation_to_cbor(operation, &mut remaining_items, &mut remaining_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let envelope = CborValue::Array(vec![
        CborValue::Text(FINGERPRINT_DOMAIN.to_owned()),
        CborValue::Text(V1_NAME.to_owned()),
        CborValue::Text(partition_id.0.clone()),
        CborValue::Text(client_id.0.clone()),
        CborValue::Text(client_commit_id.0.clone()),
        CborValue::Array(operations),
    ]);
    let bytes = encode_cbor_value(envelope)?;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(7 + digest.len() * 2);
    encoded.push_str("sha256:");
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(Fingerprint(encoded))
}

fn validate(value: &Value) -> Result<(), CodecError> {
    let mut remaining_items = MAX_TOTAL_ITEMS;
    let mut remaining_bytes = MAX_ENCODED_BYTES;
    validate_with_budget(value, 0, &mut remaining_items, &mut remaining_bytes)
}

pub(crate) fn validate_with_budget(
    value: &Value,
    depth: usize,
    remaining_items: &mut usize,
    remaining_bytes: &mut usize,
) -> Result<(), CodecError> {
    if depth > MAX_DEPTH {
        return Err(CodecError::DepthLimit);
    }
    *remaining_items = remaining_items
        .checked_sub(1)
        .ok_or(CodecError::ContainerLimit)?;
    match value {
        Value::String(value) if value.len() > MAX_STRING_BYTES => Err(CodecError::StringLimit),
        Value::String(value) => take_bytes(remaining_bytes, value.len()),
        Value::Bytes(value) if value.len() > MAX_STRING_BYTES => Err(CodecError::StringLimit),
        Value::Bytes(value) => take_bytes(remaining_bytes, value.len()),
        Value::Array(values) if values.len() > MAX_CONTAINER_ITEMS => {
            Err(CodecError::ContainerLimit)
        }
        Value::Array(values) => values.iter().try_for_each(|value| {
            validate_with_budget(value, depth + 1, remaining_items, remaining_bytes)
        }),
        Value::Object(values) if values.len() > MAX_CONTAINER_ITEMS => {
            Err(CodecError::ContainerLimit)
        }
        Value::Object(values) => {
            *remaining_items = remaining_items
                .checked_sub(values.len())
                .ok_or(CodecError::ContainerLimit)?;
            values.iter().try_for_each(|(key, value)| {
                if key.len() > MAX_STRING_BYTES {
                    Err(CodecError::StringLimit)
                } else {
                    take_bytes(remaining_bytes, key.len())?;
                    validate_with_budget(value, depth + 1, remaining_items, remaining_bytes)
                }
            })
        }
        Value::RecordId { table, key } => {
            if table.is_empty() {
                return Err(CodecError::InvalidRecordId);
            }
            if table.len() > MAX_STRING_BYTES {
                return Err(CodecError::StringLimit);
            }
            take_bytes(remaining_bytes, table.len())?;
            if !matches!(
                key.as_ref(),
                Value::Int(_) | Value::String(_) | Value::Array(_) | Value::Object(_)
            ) {
                return Err(CodecError::InvalidRecordId);
            }
            *remaining_items = remaining_items
                .checked_sub(2)
                .ok_or(CodecError::ContainerLimit)?;
            validate_with_budget(key, depth + 1, remaining_items, remaining_bytes)
        }
        Value::None => {
            *remaining_items = remaining_items
                .checked_sub(1)
                .ok_or(CodecError::ContainerLimit)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn to_cbor(value: &Value) -> Result<CborValue, CodecError> {
    Ok(match value {
        Value::None => CborValue::Tag(NONE_TAG, Box::new(CborValue::Null)),
        Value::Null => CborValue::Null,
        Value::Bool(value) => CborValue::Bool(*value),
        Value::Int(value) => CborValue::Integer((*value).into()),
        Value::String(value) => CborValue::Text(value.clone()),
        Value::Bytes(value) => CborValue::Bytes(value.clone()),
        Value::Array(values) => {
            CborValue::Array(values.iter().map(to_cbor).collect::<Result<_, _>>()?)
        }
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| {
                left.len()
                    .cmp(&right.len())
                    .then_with(|| left.as_bytes().cmp(right.as_bytes()))
            });
            CborValue::Map(
                entries
                    .into_iter()
                    .map(|(key, value)| Ok((CborValue::Text(key.clone()), to_cbor(value)?)))
                    .collect::<Result<_, CodecError>>()?,
            )
        }
        Value::RecordId { table, key } => CborValue::Tag(
            RECORD_ID_TAG,
            Box::new(CborValue::Array(vec![
                CborValue::Text(table.clone()),
                to_cbor(key)?,
            ])),
        ),
    })
}

pub(crate) fn from_cbor(value: CborValue) -> Result<Value, CodecError> {
    match value {
        CborValue::Integer(value) => i64::try_from(value)
            .map(Value::Int)
            .map_err(|_| CodecError::Unsupported("integer outside i64")),
        CborValue::Bytes(value) => Ok(Value::Bytes(value)),
        CborValue::Float(_) => Err(CodecError::Unsupported("float")),
        CborValue::Text(value) => Ok(Value::String(value)),
        CborValue::Bool(value) => Ok(Value::Bool(value)),
        CborValue::Null => Ok(Value::Null),
        CborValue::Tag(NONE_TAG, value) if matches!(value.as_ref(), CborValue::Null) => {
            Ok(Value::None)
        }
        CborValue::Tag(RECORD_ID_TAG, value) => record_id_from_cbor(*value),
        CborValue::Tag(_, _) => Err(CodecError::Unsupported("CBOR tag")),
        CborValue::Array(values) => values
            .into_iter()
            .map(from_cbor)
            .collect::<Result<_, _>>()
            .map(Value::Array),
        CborValue::Map(entries) => {
            let mut values = BTreeMap::new();
            for (key, value) in entries {
                let CborValue::Text(key) = key else {
                    return Err(CodecError::Unsupported("non-text object key"));
                };
                if values.insert(key, from_cbor(value)?).is_some() {
                    return Err(CodecError::DuplicateObjectKey);
                }
            }
            Ok(Value::Object(values))
        }
        _ => Err(CodecError::Unsupported("unknown CBOR value")),
    }
}

fn record_id_from_cbor(value: CborValue) -> Result<Value, CodecError> {
    let CborValue::Array(mut parts) = value else {
        return Err(CodecError::InvalidRecordId);
    };
    if parts.len() != 2 {
        return Err(CodecError::InvalidRecordId);
    }
    let key = from_cbor(parts.pop().expect("length checked"))?;
    let CborValue::Text(table) = parts.pop().expect("length checked") else {
        return Err(CodecError::InvalidRecordId);
    };
    let value = Value::RecordId {
        table,
        key: Box::new(key),
    };
    validate(&value)?;
    Ok(value)
}

fn operation_to_cbor(
    operation: &Operation<Value>,
    remaining_items: &mut usize,
    remaining_bytes: &mut usize,
) -> Result<CborValue, CodecError> {
    match operation {
        Operation::Upsert {
            record_id,
            base_version,
            value,
            reference,
        } => {
            validate_identifier(&record_id.0)?;
            take_bytes(remaining_bytes, record_id.0.len())?;
            if let Some(reference) = reference {
                validate_identifier(&reference.0)?;
                take_bytes(remaining_bytes, reference.0.len())?;
            }
            validate_with_budget(value, 0, remaining_items, remaining_bytes)?;
            Ok(CborValue::Array(vec![
                CborValue::Integer(0.into()),
                CborValue::Text(record_id.0.clone()),
                base_version_to_cbor(*base_version),
                to_cbor(value)?,
                optional_record_id_to_cbor(reference.as_ref()),
            ]))
        }
        Operation::Delete {
            record_id,
            base_version,
        } => {
            validate_identifier(&record_id.0)?;
            take_bytes(remaining_bytes, record_id.0.len())?;
            Ok(CborValue::Array(vec![
                CborValue::Integer(1.into()),
                CborValue::Text(record_id.0.clone()),
                CborValue::Integer((*base_version).into()),
            ]))
        }
    }
}

fn base_version_to_cbor(version: BaseVersion) -> CborValue {
    match version {
        BaseVersion::Absent => CborValue::Array(vec![CborValue::Integer(0.into())]),
        BaseVersion::Exact(value) => CborValue::Array(vec![
            CborValue::Integer(1.into()),
            CborValue::Integer(value.into()),
        ]),
    }
}

fn optional_record_id_to_cbor(record_id: Option<&RecordId>) -> CborValue {
    record_id.map_or(CborValue::Null, |record_id| {
        CborValue::Text(record_id.0.clone())
    })
}

pub(crate) fn encode_cbor_value(value: CborValue) -> Result<Vec<u8>, CodecError> {
    let mut writer = LimitedWriter::new(MAX_ENCODED_BYTES);
    let result = ciborium::ser::into_writer(&value, &mut writer);
    if writer.exceeded {
        return Err(CodecError::EncodedLimit);
    }
    result.map_err(|_| CodecError::Encode)?;
    Ok(writer.bytes)
}

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl LimitedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }
}

impl IoWrite for LimitedWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(input.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("canonical CBOR limit exceeded"));
        };
        if next_len > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("canonical CBOR limit exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn take_bytes(remaining: &mut usize, length: usize) -> Result<(), CodecError> {
    *remaining = remaining
        .checked_sub(length)
        .ok_or(CodecError::EncodedLimit)?;
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), CodecError> {
    if value.is_empty() || value.len() > MAX_STRING_BYTES {
        Err(CodecError::InvalidIdentifier)
    } else {
        Ok(())
    }
}

pub(crate) fn preflight_canonical_cbor(bytes: &[u8]) -> Result<(), CodecError> {
    let mut cursor = 0;
    let mut remaining_items = MAX_TOTAL_ITEMS;
    scan_cbor_item(bytes, &mut cursor, 0, &mut remaining_items)?;
    if cursor != bytes.len() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(())
}

fn scan_cbor_item(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    remaining_items: &mut usize,
) -> Result<(), CodecError> {
    if depth > MAX_DEPTH {
        return Err(CodecError::DepthLimit);
    }
    *remaining_items = remaining_items
        .checked_sub(1)
        .ok_or(CodecError::ContainerLimit)?;

    let initial = *bytes.get(*cursor).ok_or(CodecError::Decode)?;
    *cursor += 1;
    let major = initial >> 5;
    let additional = initial & 0x1f;
    let argument = read_cbor_argument(bytes, cursor, additional)?;

    match major {
        0 | 1 => Ok(()),
        2 | 3 => {
            let length = usize::try_from(argument).map_err(|_| CodecError::StringLimit)?;
            if length > MAX_STRING_BYTES {
                return Err(CodecError::StringLimit);
            }
            *cursor = cursor.checked_add(length).ok_or(CodecError::Decode)?;
            if *cursor > bytes.len() {
                return Err(CodecError::Decode);
            }
            Ok(())
        }
        4 | 5 => {
            let length = usize::try_from(argument).map_err(|_| CodecError::ContainerLimit)?;
            if length > MAX_CONTAINER_ITEMS {
                return Err(CodecError::ContainerLimit);
            }
            let entries = if major == 5 {
                length.checked_mul(2).ok_or(CodecError::ContainerLimit)?
            } else {
                length
            };
            for _ in 0..entries {
                scan_cbor_item(bytes, cursor, depth + 1, remaining_items)?;
            }
            Ok(())
        }
        6 => scan_cbor_item(bytes, cursor, depth + 1, remaining_items),
        7 => Ok(()),
        _ => Err(CodecError::Decode),
    }
}

fn read_cbor_argument(bytes: &[u8], cursor: &mut usize, additional: u8) -> Result<u64, CodecError> {
    let width = match additional {
        0..=23 => return Ok(u64::from(additional)),
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return Err(CodecError::Decode),
    };
    let end = cursor.checked_add(width).ok_or(CodecError::Decode)?;
    let payload = bytes.get(*cursor..end).ok_or(CodecError::Decode)?;
    *cursor = end;
    Ok(payload
        .iter()
        .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    fn record(table: &str, key: Value) -> Value {
        Value::RecordId {
            table: table.to_owned(),
            key: Box::new(key),
        }
    }

    fn sample_operations() -> Vec<Operation<Value>> {
        vec![
            Operation::Upsert {
                record_id: RecordId("edge:one".into()),
                base_version: BaseVersion::Absent,
                value: Value::Object(BTreeMap::from([
                    ("in".into(), record("person", Value::String("alice".into()))),
                    ("out".into(), record("person", Value::String("bob".into()))),
                    (
                        "tags".into(),
                        Value::Array(vec![Value::String("friend".into())]),
                    ),
                ])),
                reference: Some(RecordId("person:alice".into())),
            },
            Operation::Delete {
                record_id: RecordId("note:old".into()),
                base_version: 7,
            },
        ]
    }

    #[test]
    fn canonical_value_vectors_are_stable() {
        assert_eq!(hex(&canonical_cbor(&Value::None).unwrap()), "c6f6");
        assert_eq!(
            hex(&canonical_cbor(&record("person", Value::String("alice".into()))).unwrap()),
            "c88266706572736f6e65616c696365"
        );

        let object = Value::Object(BTreeMap::from([
            ("aa".into(), Value::Int(1)),
            ("b".into(), Value::Int(2)),
        ]));
        assert_eq!(hex(&canonical_cbor(&object).unwrap()), "a261620262616101");
        assert_eq!(
            decode_canonical_cbor(&canonical_cbor(&object).unwrap()).unwrap(),
            object
        );

        let wide = Value::Object(
            (0..MAX_CONTAINER_ITEMS)
                .map(|index| (format!("key-{index:04}"), Value::Array(vec![Value::Null])))
                .collect(),
        );
        assert_eq!(
            decode_canonical_cbor(&canonical_cbor(&wide).unwrap()).unwrap(),
            wide
        );
    }

    #[test]
    fn decoder_rejects_noncanonical_duplicate_and_trailing_inputs() {
        assert_eq!(
            decode_canonical_cbor(&[0xa2, 0x62, b'a', b'a', 0x01, 0x61, b'b', 0x02]),
            Err(CodecError::NonCanonical)
        );
        assert_eq!(
            decode_canonical_cbor(&[0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02]),
            Err(CodecError::DuplicateObjectKey)
        );
        assert_eq!(
            decode_canonical_cbor(&[0xf6, 0xf6]),
            Err(CodecError::TrailingBytes)
        );
    }

    #[test]
    fn unsupported_and_oversized_values_fail_closed() {
        assert_eq!(
            canonical_cbor(&Value::Array(vec![Value::Null; MAX_CONTAINER_ITEMS + 1])),
            Err(CodecError::ContainerLimit)
        );
        assert_eq!(
            decode_canonical_cbor(&vec![0; MAX_ENCODED_BYTES + 1]),
            Err(CodecError::EncodedLimit)
        );
        assert_eq!(
            canonical_cbor(&Value::Array(vec![
                Value::String(
                    "x".repeat(MAX_STRING_BYTES)
                );
                5
            ])),
            Err(CodecError::EncodedLimit)
        );
    }

    #[test]
    fn decoder_preflight_rejects_hostile_shapes_before_deserialization() {
        let mut too_deep = vec![0x81; MAX_DEPTH + 2];
        too_deep.push(0xf6);
        assert_eq!(
            decode_canonical_cbor(&too_deep),
            Err(CodecError::DepthLimit)
        );
        assert_eq!(
            decode_canonical_cbor(&[0x9a, 0x00, 0x00, 0x04, 0x01]),
            Err(CodecError::ContainerLimit)
        );
        assert_eq!(
            decode_canonical_cbor(&[0x7a, 0x00, 0x10, 0x00, 0x01]),
            Err(CodecError::StringLimit)
        );
        assert_eq!(
            decode_canonical_cbor(&[0x18, 0x00]),
            Err(CodecError::NonCanonical)
        );
        assert_eq!(
            decode_canonical_cbor(&[0xc8, 0x82, 0x66, b'p', b'e', b'r', b's', b'o', b'n']),
            Err(CodecError::Decode)
        );
    }

    #[test]
    fn commit_identity_fields_share_codec_resource_limits() {
        let operations = sample_operations();
        assert_eq!(
            fingerprint_commit(
                &PartitionId(String::new()),
                &ClientId("client-a".into()),
                &ClientCommitId("commit-1".into()),
                &operations,
            ),
            Err(CodecError::InvalidIdentifier)
        );
        assert_eq!(
            fingerprint_commit(
                &PartitionId("partition".into()),
                &ClientId("client-a".into()),
                &ClientCommitId("x".repeat(MAX_STRING_BYTES + 1)),
                &operations,
            ),
            Err(CodecError::InvalidIdentifier)
        );
    }

    #[test]
    fn fingerprint_is_content_bound_and_order_sensitive() {
        let partition = PartitionId("primary".into());
        let client = ClientId("client-a".into());
        let commit = ClientCommitId("commit-7".into());
        let operations = sample_operations();
        let fingerprint = fingerprint_commit(&partition, &client, &commit, &operations).unwrap();
        assert_eq!(
            fingerprint.0,
            "sha256:0167e7a657c1697643410c575a0ad1ffa6015f94e4ba70ed17f08320339bd769"
        );

        let mut reordered = operations.clone();
        reordered.reverse();
        assert_ne!(
            fingerprint_commit(&partition, &client, &commit, &reordered).unwrap(),
            fingerprint
        );
        assert_ne!(
            fingerprint_commit(&PartitionId("other".into()), &client, &commit, &operations)
                .unwrap(),
            fingerprint
        );

        let mut altered_type = operations.clone();
        insert_rank(&mut altered_type, Value::Int(1));
        let integer_fingerprint =
            fingerprint_commit(&partition, &client, &commit, &altered_type).unwrap();
        insert_rank(&mut altered_type, Value::String("1".into()));
        assert_ne!(
            fingerprint_commit(&partition, &client, &commit, &altered_type).unwrap(),
            integer_fingerprint
        );

        let mut same_object_different_insertion = sample_operations();
        let Operation::Upsert { value, .. } = &mut same_object_different_insertion[0] else {
            unreachable!();
        };
        let Value::Object(object) = value else {
            unreachable!();
        };
        *object = object
            .iter()
            .rev()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert_eq!(
            fingerprint_commit(
                &partition,
                &client,
                &commit,
                &same_object_different_insertion
            )
            .unwrap(),
            fingerprint
        );
    }

    fn insert_rank(operations: &mut [Operation<Value>], rank: Value) {
        let Operation::Upsert { value, .. } = &mut operations[0] else {
            unreachable!();
        };
        let Value::Object(object) = value else {
            unreachable!();
        };
        object.insert("rank".into(), rank);
    }
}
