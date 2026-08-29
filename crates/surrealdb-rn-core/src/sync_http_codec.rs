//! UniFFI bridge for the protocol-owned canonical HTTP codec.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use surrealdb::types::{
    Array as SurrealArray, Number as SurrealNumber, Object as SurrealObject,
    RecordId as SurrealRecordId, RecordIdKey, ToSql, Value as SurrealValue,
};
use surrealdb_sync_protocol::{
    AppliedRecord, CanonicalValue, ClientCommit, ClientId, DurableOutcome, OpaqueCheckpoint,
    PartitionId, PullBatch, PullCommit, PullFrame, PullRequest, PullResponse, PushRequest,
    PushResponse, RecordState, ResetSnapshot, SchemaVersion, ScopeIdentity, SnapshotRecord,
    decode_pull_response, decode_push_response, encode_pull_request, encode_push_request,
};

use crate::sync_client::NativeSyncError;
use crate::sync_codec::{canonical_json, canonical_operation};

const TAG: &str = "$surreal";

#[uniffi::export(async_runtime = "tokio")]
pub async fn encode_sync_push_request(
    partition_id: String,
    client_id: String,
    commit_json: String,
) -> Result<Vec<u8>, NativeSyncError> {
    let commit: ClientCommit<JsonValue> = decode_json(&commit_json)?;
    let operations = commit
        .operations
        .into_iter()
        .map(|operation| canonical_operation(&operation))
        .collect::<Result<_, _>>()?;
    let request = PushRequest {
        schema_version: SchemaVersion::V1,
        partition_id: PartitionId(partition_id),
        client_id: ClientId(client_id),
        commit: ClientCommit {
            identity: commit.identity,
            operations,
        },
    };
    encode_push_request(&request).map_err(|_| NativeSyncError::InvalidInput)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn decode_sync_push_response(bytes: Vec<u8>) -> Result<String, NativeSyncError> {
    let response = decode_push_response(&bytes).map_err(|_| NativeSyncError::Protocol)?;
    encode_json(&push_response_to_json(response)?)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn encode_sync_pull_request(
    partition_id: String,
    client_id: String,
    checkpoint: Option<String>,
    requested_scope: String,
    subscription_revision: u64,
) -> Result<Vec<u8>, NativeSyncError> {
    encode_pull_request(&PullRequest {
        schema_version: SchemaVersion::V1,
        partition_id: PartitionId(partition_id),
        client_id: ClientId(client_id),
        checkpoint: checkpoint.map(OpaqueCheckpoint),
        requested_scope: ScopeIdentity(requested_scope),
        subscription_revision,
    })
    .map_err(|_| NativeSyncError::InvalidInput)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn decode_sync_pull_response(bytes: Vec<u8>) -> Result<String, NativeSyncError> {
    let response = decode_pull_response(&bytes).map_err(|_| NativeSyncError::Protocol)?;
    encode_json(&pull_response_to_json(response)?)
}

fn decode_json<T: serde::de::DeserializeOwned>(input: &str) -> Result<T, NativeSyncError> {
    serde_json::from_str(input).map_err(|_| NativeSyncError::InvalidInput)
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<String, NativeSyncError> {
    serde_json::to_string(value).map_err(|_| NativeSyncError::Internal)
}

fn push_response_to_json(
    response: PushResponse<CanonicalValue>,
) -> Result<PushResponse<JsonValue>, NativeSyncError> {
    Ok(PushResponse {
        schema_version: response.schema_version,
        partition_id: response.partition_id,
        client_id: response.client_id,
        outcome: match response.outcome {
            DurableOutcome::Accepted {
                identity,
                sequence,
                id_mappings,
            } => DurableOutcome::Accepted {
                identity,
                sequence,
                id_mappings,
            },
            DurableOutcome::Conflict {
                identity,
                record_id,
                authoritative,
            } => DurableOutcome::Conflict {
                identity,
                record_id,
                authoritative: record_state_to_json(authoritative)?,
            },
            DurableOutcome::Rejected { identity, reason } => {
                DurableOutcome::Rejected { identity, reason }
            }
        },
    })
}

fn pull_response_to_json(
    response: PullResponse<CanonicalValue>,
) -> Result<PullResponse<JsonValue>, NativeSyncError> {
    Ok(match response {
        PullResponse::Batch(batch) => PullResponse::Batch(PullBatch {
            frames: batch
                .frames
                .into_iter()
                .map(pull_frame_to_json)
                .collect::<Result<_, _>>()?,
        }),
        PullResponse::Reset(reset) => PullResponse::Reset(ResetSnapshot {
            reason: reset.reason,
            checkpoint: reset.checkpoint,
            records: reset
                .records
                .into_iter()
                .map(|record| {
                    Ok(SnapshotRecord {
                        record_id: record.record_id,
                        state: record_state_to_json(record.state)?,
                    })
                })
                .collect::<Result<_, NativeSyncError>>()?,
        }),
    })
}

fn pull_frame_to_json(
    frame: PullFrame<CanonicalValue>,
) -> Result<PullFrame<JsonValue>, NativeSyncError> {
    Ok(match frame {
        PullFrame::Start { checkpoint } => PullFrame::Start { checkpoint },
        PullFrame::Commit { checkpoint, commit } => PullFrame::Commit {
            checkpoint,
            commit: PullCommit {
                sequence: commit.sequence,
                source: commit.source,
                records: commit
                    .records
                    .into_iter()
                    .map(|record| {
                        Ok(AppliedRecord {
                            record_id: record.record_id,
                            state: record_state_to_json(record.state)?,
                        })
                    })
                    .collect::<Result<_, NativeSyncError>>()?,
            },
        },
        PullFrame::End { checkpoint } => PullFrame::End { checkpoint },
    })
}

fn record_state_to_json(
    state: RecordState<CanonicalValue>,
) -> Result<RecordState<JsonValue>, NativeSyncError> {
    Ok(match state {
        RecordState::Absent => RecordState::Absent,
        RecordState::Present {
            value,
            version,
            reference,
        } => RecordState::Present {
            value: canonical_to_json(value)?,
            version,
            reference,
        },
        RecordState::Tombstone { version } => RecordState::Tombstone { version },
    })
}

fn canonical_to_json(value: CanonicalValue) -> Result<JsonValue, NativeSyncError> {
    Ok(match value {
        CanonicalValue::None => json!({ TAG: "none" }),
        CanonicalValue::Null => JsonValue::Null,
        CanonicalValue::Bool(value) => JsonValue::Bool(value),
        CanonicalValue::Int(value) => json!({ TAG: "int", "value": value.to_string() }),
        CanonicalValue::String(value) => JsonValue::String(value),
        CanonicalValue::Bytes(value) => json!({ TAG: "bytes", "base64": BASE64.encode(value) }),
        CanonicalValue::Array(values) => JsonValue::Array(
            values
                .into_iter()
                .map(canonical_to_json)
                .collect::<Result<_, _>>()?,
        ),
        CanonicalValue::Object(values) => {
            if values.contains_key(TAG) {
                return Err(NativeSyncError::Protocol);
            }
            JsonValue::Object(
                values
                    .into_iter()
                    .map(|(key, value)| Ok((key, canonical_to_json(value)?)))
                    .collect::<Result<JsonMap<_, _>, NativeSyncError>>()?,
            )
        }
        CanonicalValue::RecordId { table, key } => {
            let canonical = CanonicalValue::RecordId {
                table: table.clone(),
                key: key.clone(),
            };
            let record_id = SurrealRecordId::new(table, canonical_record_key(*key)?).to_sql();
            let tagged = json!({ TAG: "record", "value": record_id });
            if canonical_json(&tagged).map_err(|_| NativeSyncError::Protocol)? != canonical {
                return Err(NativeSyncError::Protocol);
            }
            tagged
        }
    })
}

fn canonical_record_key(value: CanonicalValue) -> Result<RecordIdKey, NativeSyncError> {
    match value {
        CanonicalValue::Int(value) => Ok(RecordIdKey::Number(value)),
        CanonicalValue::String(value) => Ok(RecordIdKey::String(value)),
        CanonicalValue::Array(values) => values
            .into_iter()
            .map(canonical_to_surreal)
            .collect::<Result<Vec<_>, _>>()
            .map(SurrealArray::from)
            .map(RecordIdKey::Array),
        CanonicalValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, canonical_to_surreal(value)?)))
            .collect::<Result<BTreeMap<_, _>, NativeSyncError>>()
            .map(SurrealObject::from)
            .map(RecordIdKey::Object),
        _ => Err(NativeSyncError::Protocol),
    }
}

fn canonical_to_surreal(value: CanonicalValue) -> Result<SurrealValue, NativeSyncError> {
    Ok(match value {
        CanonicalValue::None => SurrealValue::None,
        CanonicalValue::Null => SurrealValue::Null,
        CanonicalValue::Bool(value) => SurrealValue::Bool(value),
        CanonicalValue::Int(value) => SurrealValue::Number(SurrealNumber::Int(value)),
        CanonicalValue::String(value) => SurrealValue::String(value),
        CanonicalValue::Bytes(value) => SurrealValue::Bytes(value.into()),
        CanonicalValue::Array(values) => SurrealValue::Array(SurrealArray::from(
            values
                .into_iter()
                .map(canonical_to_surreal)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        CanonicalValue::Object(values) => SurrealValue::Object(SurrealObject::from(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, canonical_to_surreal(value)?)))
                .collect::<Result<BTreeMap<_, _>, NativeSyncError>>()?,
        )),
        CanonicalValue::RecordId { table, key } => {
            SurrealValue::RecordId(SurrealRecordId::new(table, canonical_record_key(*key)?))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb_sync_protocol::{
        ClientCommitId, CommitIdentity, Fingerprint, PullResponse, RecordId, RejectReason,
    };

    const FINGERPRINT: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn bytes(encoded: &str) -> Vec<u8> {
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn request_bridge_replays_private_golden_vectors() {
        let push = encode_sync_push_request(
            "p".into(),
            "c".into(),
            format!(
                r#"{{"identity":{{"clientCommitId":"i","fingerprint":"{FINGERPRINT}"}},"operations":[]}}"#
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            hex(&push),
            "84707375727265616c64622d73796e632f310000836170616383616978477368613235363a3031323334353637383961626364656630313233343536373839616263646566303132333435363738396162636465663031323334353637383961626364656680"
        );

        let pull = encode_sync_pull_request("p".into(), "c".into(), None, "s".into(), 0)
            .await
            .unwrap();
        assert_eq!(
            hex(&pull),
            "84707375727265616c64622d73796e632f3100028561706163f6617300"
        );
    }

    #[tokio::test]
    async fn response_bridge_replays_private_golden_vectors() {
        let push = bytes(
            "84707375727265616c64622d73796e632f3100018361706163830282616978477368613235363a3031323334353637383961626364656630313233343536373839616263646566303132333435363738396162636465663031323334353637383961626364656602",
        );
        let decoded = decode_sync_push_response(push).await.unwrap();
        let response: PushResponse<JsonValue> = serde_json::from_str(&decoded).unwrap();
        assert!(matches!(
            response.outcome,
            DurableOutcome::Rejected {
                reason: RejectReason::Unauthorized,
                ..
            }
        ));

        let pull = bytes("84707375727265616c64622d73796e632f310003820080");
        let decoded = decode_sync_pull_response(pull).await.unwrap();
        let response: PullResponse<JsonValue> = serde_json::from_str(&decoded).unwrap();
        assert_eq!(response, PullResponse::Batch(PullBatch { frames: vec![] }));
    }

    #[tokio::test]
    async fn response_bridge_preserves_every_canonical_value_variant() {
        let value = CanonicalValue::Object(BTreeMap::from([
            (
                "array".into(),
                CanonicalValue::Array(vec![CanonicalValue::Null]),
            ),
            ("bool".into(), CanonicalValue::Bool(true)),
            ("bytes".into(), CanonicalValue::Bytes(vec![0, 1, 2])),
            ("int".into(), CanonicalValue::Int(i64::MAX)),
            ("none".into(), CanonicalValue::None),
            ("null".into(), CanonicalValue::Null),
            (
                "record".into(),
                CanonicalValue::RecordId {
                    table: "person".into(),
                    key: Box::new(CanonicalValue::String("ada".into())),
                },
            ),
            ("string".into(), CanonicalValue::String("value".into())),
        ]));
        let response = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            outcome: DurableOutcome::Conflict {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("i".into()),
                    fingerprint: Fingerprint(FINGERPRINT.into()),
                },
                record_id: RecordId("person:one".into()),
                authoritative: RecordState::Present {
                    value: value.clone(),
                    version: u64::MAX,
                    reference: None,
                },
            },
        };

        let decoded = decode_sync_push_response(
            surrealdb_sync_protocol::encode_push_response(&response).unwrap(),
        )
        .await
        .unwrap();
        let response: PushResponse<JsonValue> = serde_json::from_str(&decoded).unwrap();
        let DurableOutcome::Conflict {
            authoritative: RecordState::Present { value: decoded, .. },
            ..
        } = response.outcome
        else {
            panic!("expected present conflict")
        };
        assert_eq!(canonical_json(&decoded).unwrap(), value);
    }

    #[tokio::test]
    async fn malformed_and_unrepresentable_values_fail_closed() {
        assert!(matches!(
            decode_sync_pull_response(vec![0x9f, 0xff]).await,
            Err(NativeSyncError::Protocol)
        ));

        let response = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            outcome: DurableOutcome::Conflict {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("i".into()),
                    fingerprint: Fingerprint(FINGERPRINT.into()),
                },
                record_id: RecordId("person:one".into()),
                authoritative: RecordState::Present {
                    value: CanonicalValue::Object(BTreeMap::from([(
                        TAG.into(),
                        CanonicalValue::String("ordinary-user-key".into()),
                    )])),
                    version: 1,
                    reference: None,
                },
            },
        };
        let bytes = surrealdb_sync_protocol::encode_push_response(&response).unwrap();
        assert!(matches!(
            decode_sync_push_response(bytes).await,
            Err(NativeSyncError::Protocol)
        ));

        let response = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            outcome: DurableOutcome::Conflict {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("i".into()),
                    fingerprint: Fingerprint(FINGERPRINT.into()),
                },
                record_id: RecordId("person:one".into()),
                authoritative: RecordState::Present {
                    value: CanonicalValue::RecordId {
                        table: "person".into(),
                        key: Box::new(CanonicalValue::Object(BTreeMap::from([(
                            "name".into(),
                            CanonicalValue::String("Ada".into()),
                        )]))),
                    },
                    version: 1,
                    reference: None,
                },
            },
        };
        let bytes = surrealdb_sync_protocol::encode_push_response(&response).unwrap();
        assert!(matches!(
            decode_sync_push_response(bytes).await,
            Err(NativeSyncError::Protocol)
        ));
    }
}
