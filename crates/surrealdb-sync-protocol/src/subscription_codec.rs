//! Separately versioned deterministic CBOR; values use the existing canonical profile.
use crate::{
    CanonicalValue, CodecError,
    canonical::{encode_cbor_value, from_cbor, preflight_canonical_cbor, to_cbor},
    subscription::*,
};
use ciborium::value::Value;
use serde::{Serialize, de::DeserializeOwned};
use std::io::Cursor;

fn sorted(value: Value) -> Result<Value, CodecError> {
    Ok(match value {
        Value::Array(values) => {
            Value::Array(values.into_iter().map(sorted).collect::<Result<_, _>>()?)
        }
        Value::Map(values) => {
            let mut entries = values
                .into_iter()
                .map(|(key, value)| Ok((encode_cbor_value(key.clone())?, key, sorted(value)?)))
                .collect::<Result<Vec<_>, CodecError>>()?;
            entries.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then(a.0.cmp(&b.0)));
            Value::Map(
                entries
                    .into_iter()
                    .map(|(_, key, value)| (key, value))
                    .collect(),
            )
        }
        value => value,
    })
}
fn encode<T: Serialize>(kind: &str, value: &T) -> Result<Vec<u8>, CodecError> {
    let value = Value::serialized(value).map_err(|_| CodecError::Encode)?;
    let bytes = encode_cbor_value(sorted(Value::Array(vec![
        Value::Text(SUBSCRIPTION_PROTOCOL.into()),
        Value::Text(kind.into()),
        value,
    ]))?)?;
    if bytes.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(CodecError::EncodedLimit);
    }
    preflight_canonical_cbor(&bytes)?;
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(kind: &str, bytes: &[u8]) -> Result<T, CodecError> {
    if bytes.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(CodecError::EncodedLimit);
    }
    preflight_canonical_cbor(bytes)?;
    let mut cursor = Cursor::new(bytes);
    let value: Value = ciborium::de::from_reader(&mut cursor).map_err(|_| CodecError::Decode)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(CodecError::TrailingBytes);
    }
    let Value::Array(mut values) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    if values.len() != 3
        || values[0] != Value::Text(SUBSCRIPTION_PROTOCOL.into())
        || values[1] != Value::Text(kind.into())
    {
        return Err(CodecError::InvalidEnvelope);
    }
    values
        .pop()
        .ok_or(CodecError::InvalidEnvelope)?
        .deserialized()
        .map_err(|_| CodecError::InvalidEnvelope)
}
pub fn encode_subscription_request(request: &SubscriptionRequest) -> Result<Vec<u8>, CodecError> {
    if !validate_subscription_request(request) {
        return Err(CodecError::InvalidEnvelope);
    }
    encode("request", request)
}
pub fn decode_subscription_request(bytes: &[u8]) -> Result<SubscriptionRequest, CodecError> {
    let request = decode("request", bytes)?;
    if encode_subscription_request(&request)? != bytes {
        return Err(CodecError::NonCanonical);
    }
    Ok(request)
}
fn map_response<A, B>(
    response: SubscriptionResponse<A>,
    mut map: impl FnMut(A) -> Result<B, CodecError>,
) -> Result<SubscriptionResponse<B>, CodecError> {
    let changes = response
        .changes
        .into_iter()
        .map(|change| {
            Ok(match change {
                MembershipChange::Upsert { row } => MembershipChange::Upsert {
                    row: SubscriptionRow {
                        record_id: row.record_id,
                        value: map(row.value)?,
                        version: row.version,
                        sort_key: row.sort_key,
                    },
                },
                MembershipChange::Leave { record_id } => MembershipChange::Leave { record_id },
                MembershipChange::Delete { record_id, version } => {
                    MembershipChange::Delete { record_id, version }
                }
            })
        })
        .collect::<Result<_, CodecError>>()?;
    Ok(SubscriptionResponse {
        partition_id: response.partition_id,
        client_id: response.client_id,
        subscription_id: response.subscription_id,
        generation: response.generation,
        plan_fingerprint: response.plan_fingerprint,
        scope: response.scope,
        frontier: response.frontier,
        view_revision: response.view_revision,
        coverage: response.coverage,
        response: response.response,
        token: response.token,
        ordered_ids: response.ordered_ids,
        changes,
    })
}
pub fn encode_subscription_response(
    response: &SubscriptionResponse<CanonicalValue>,
) -> Result<Vec<u8>, CodecError> {
    if !validate_subscription_response(response) {
        return Err(CodecError::ContainerLimit);
    }
    encode(
        "response",
        &map_response(response.clone(), |value| to_cbor(&value))?,
    )
}
pub fn decode_subscription_response(
    bytes: &[u8],
) -> Result<SubscriptionResponse<CanonicalValue>, CodecError> {
    let response: SubscriptionResponse<Value> = decode("response", bytes)?;
    let response = map_response(response, from_cbor)?;
    if encode_subscription_response(&response)? != bytes {
        return Err(CodecError::NonCanonical);
    }
    Ok(response)
}
/// Domain separation prevents a subscription plan digest from being a commit fingerprint.
pub fn subscription_plan_fingerprint(
    request: &SubscriptionRequest,
    definition: &TrustedQueryDefinition,
) -> Result<String, CodecError> {
    use sha2::{Digest, Sha256};
    let bytes = encode("plan", &(request, definition))?;
    let mut hash = Sha256::new();
    hash.update(b"surrealdb-subscription-plan/1\0");
    hash.update(bytes);
    Ok(format!("sha256:{:x}", hash.finalize()))
}
