//! Native validation bridge for the protocol-owned canonical value profile.

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use surrealdb::types::{Number as SurrealNumber, RecordIdKey, Value as SurrealValue};
use surrealdb_sync_client::DurableClientState;
use surrealdb_sync_protocol::{
    CanonicalFloat, CanonicalValue, ClientCommit, DurableOutcome, Fingerprint, Operation,
    RecordState, canonical_cbor, fingerprint_commit,
};

use crate::sync_client::NativeSyncError;
use crate::wire::from_wire_value;

pub(crate) fn canonical_fingerprint(
    state: &DurableClientState<JsonValue>,
    commit: &ClientCommit<JsonValue>,
) -> Result<Fingerprint, NativeSyncError> {
    let operations = commit
        .operations
        .iter()
        .map(canonical_operation)
        .collect::<Result<Vec<_>, _>>()?;
    fingerprint_commit(
        &state.partition_id,
        &state.client_id,
        &commit.identity.client_commit_id,
        &operations,
    )
    .map_err(|_| NativeSyncError::InvalidInput)
}

pub(crate) fn validate_state_codec(
    state: &DurableClientState<JsonValue>,
) -> Result<(), NativeSyncError> {
    for record in state.confirmed.values() {
        validate_record_state(record)?;
    }
    for commit in &state.outbox {
        validate_commit_identity(state, commit)?;
    }
    for resolved in &state.outcomes {
        // Accepted ID mappings rewrite recoverable local intent while the durable
        // identity remains bound to the exact pre-remap commit sent to authority.
        // Revalidate the remapped content and bounds, but do not derive a new identity.
        canonical_fingerprint(state, &resolved.local_commit)?;
        if let DurableOutcome::Conflict { authoritative, .. } = &resolved.outcome {
            validate_record_state(authoritative)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_push_response_values(
    outcome: &DurableOutcome<JsonValue>,
) -> Result<(), NativeSyncError> {
    if let DurableOutcome::Conflict { authoritative, .. } = outcome {
        validate_record_state(authoritative)?;
    }
    Ok(())
}

pub(crate) fn validate_pull_response_values(
    response: &surrealdb_sync_protocol::PullResponse<JsonValue>,
) -> Result<(), NativeSyncError> {
    use surrealdb_sync_protocol::{PullFrame, PullResponse};

    match response {
        PullResponse::Batch(batch) => {
            for frame in &batch.frames {
                if let PullFrame::Commit { commit, .. } = frame {
                    for record in &commit.records {
                        validate_record_state(&record.state)?;
                    }
                }
            }
        }
        PullResponse::Reset(reset) => {
            for record in &reset.records {
                validate_record_state(&record.state)?;
            }
        }
    }
    Ok(())
}

fn validate_commit_identity(
    state: &DurableClientState<JsonValue>,
    commit: &ClientCommit<JsonValue>,
) -> Result<(), NativeSyncError> {
    if canonical_fingerprint(state, commit)? != commit.identity.fingerprint {
        return Err(NativeSyncError::Protocol);
    }
    Ok(())
}

pub(crate) fn canonical_operation(
    operation: &Operation<JsonValue>,
) -> Result<Operation<CanonicalValue>, NativeSyncError> {
    Ok(match operation {
        Operation::Upsert {
            record_id,
            base_version,
            value,
            reference,
        } => Operation::Upsert {
            record_id: record_id.clone(),
            base_version: *base_version,
            value: canonical_json(value)?,
            reference: reference.clone(),
        },
        Operation::Delete {
            record_id,
            base_version,
        } => Operation::Delete {
            record_id: record_id.clone(),
            base_version: *base_version,
        },
    })
}

fn validate_record_state(state: &RecordState<JsonValue>) -> Result<(), NativeSyncError> {
    if let RecordState::Present { value, .. } = state {
        let canonical = canonical_json(value)?;
        canonical_cbor(&canonical).map_err(|_| NativeSyncError::InvalidInput)?;
    }
    Ok(())
}

pub(crate) fn canonical_json(value: &JsonValue) -> Result<CanonicalValue, NativeSyncError> {
    let value = from_wire_value(value.clone()).map_err(|_| NativeSyncError::InvalidInput)?;
    from_surreal(value).map_err(|_| NativeSyncError::InvalidInput)
}

fn from_surreal(value: SurrealValue) -> Result<CanonicalValue, ()> {
    match value {
        SurrealValue::None => Ok(CanonicalValue::None),
        SurrealValue::Null => Ok(CanonicalValue::Null),
        SurrealValue::Bool(value) => Ok(CanonicalValue::Bool(value)),
        SurrealValue::Number(SurrealNumber::Int(value)) => Ok(CanonicalValue::Int(value)),
        SurrealValue::Number(SurrealNumber::Float(value)) => CanonicalFloat::new(value)
            .map(CanonicalValue::Float)
            .map_err(|_| ()),
        SurrealValue::Number(SurrealNumber::Decimal(_)) => Err(()),
        SurrealValue::String(value) => Ok(CanonicalValue::String(value)),
        SurrealValue::Bytes(value) => Ok(CanonicalValue::Bytes(value.to_vec())),
        SurrealValue::Array(value) => value
            .into_vec()
            .into_iter()
            .map(from_surreal)
            .collect::<Result<_, _>>()
            .map(CanonicalValue::Array),
        SurrealValue::Object(value) => value
            .into_inner()
            .into_iter()
            .map(|(key, value)| Ok((key, from_surreal(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(CanonicalValue::Object),
        SurrealValue::RecordId(value) => Ok(CanonicalValue::RecordId {
            table: value.table.into_string(),
            key: Box::new(record_key(value.key)?),
        }),
        SurrealValue::Duration(_)
        | SurrealValue::Datetime(_)
        | SurrealValue::Uuid(_)
        | SurrealValue::Geometry(_)
        | SurrealValue::Table(_)
        | SurrealValue::File(_)
        | SurrealValue::Range(_)
        | SurrealValue::Regex(_)
        | SurrealValue::Set(_) => Err(()),
    }
}

fn record_key(key: RecordIdKey) -> Result<CanonicalValue, ()> {
    match key {
        RecordIdKey::Number(value) => Ok(CanonicalValue::Int(value)),
        RecordIdKey::String(value) => Ok(CanonicalValue::String(value)),
        RecordIdKey::Array(value) => from_surreal(SurrealValue::Array(value)),
        RecordIdKey::Object(value) => from_surreal(SurrealValue::Object(value)),
        RecordIdKey::Uuid(_) | RecordIdKey::Range(_) => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use surrealdb_sync_client::DurableClientState;
    use surrealdb_sync_protocol::{
        BaseVersion, ClientCommitId, ClientId, CommitIdentity, PartitionId, RecordId, ScopeIdentity,
    };

    use super::*;

    fn state() -> DurableClientState<JsonValue> {
        DurableClientState::empty(
            PartitionId("partition".into()),
            ClientId("client".into()),
            ScopeIdentity("all".into()),
            1,
        )
    }

    fn commit(value: JsonValue) -> ClientCommit<JsonValue> {
        ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("commit-1".into()),
                fingerprint: Fingerprint("untrusted".into()),
            },
            operations: vec![Operation::Upsert {
                record_id: RecordId("person:ada".into()),
                base_version: BaseVersion::Absent,
                value,
                reference: None,
            }],
        }
    }

    #[test]
    fn lossless_wire_tags_feed_the_canonical_fingerprint() {
        let commit = commit(json!({
            "bytes": {"$surreal": "bytes", "base64": "AQID"},
            "friend": {"$surreal": "record", "value": "person:bob"},
            "maximum": {"$surreal": "int", "value": i64::MAX.to_string()},
            "missing": {"$surreal": "none"},
            "servings": 2.5
        }));

        let fingerprint = canonical_fingerprint(&state(), &commit).unwrap();
        assert!(fingerprint.0.starts_with("sha256:"));
        assert_eq!(fingerprint.0.len(), 71);
    }

    #[test]
    fn unsupported_and_hostile_values_fail_closed() {
        let fractional = canonical_json(&json!(1.5)).unwrap();
        let CanonicalValue::Float(value) = fractional else {
            panic!("expected canonical float");
        };
        assert_eq!(value.get().to_bits(), 1.5_f64.to_bits());

        let mut nested = JsonValue::Null;
        for _ in 0..=surrealdb_sync_protocol::MAX_DEPTH {
            nested = JsonValue::Array(vec![nested]);
        }
        assert!(canonical_fingerprint(&state(), &commit(nested)).is_err());
    }
}
