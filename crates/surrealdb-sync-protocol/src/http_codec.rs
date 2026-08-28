//! Deterministic, bounded CBOR envelopes for the HTTP correctness boundary.

use std::io::Cursor;

use ciborium::value::Value as Cbor;

use crate::canonical::{
    encode_cbor_value, from_cbor, preflight_canonical_cbor, take_bytes, to_cbor,
    validate_with_budget,
};
use crate::{
    AppliedRecord, BaseVersion, CanonicalValue, Checkpoint, ClientCommit, ClientCommitId, ClientId,
    CodecError, CommitIdentity, Cursor as ProtocolCursor, DurableOutcome, Fingerprint, IdMapping,
    MAX_CONTAINER_ITEMS, MAX_ENCODED_BYTES, MAX_TOTAL_ITEMS, OpaqueCheckpoint, Operation,
    PartitionId, PullBatch, PullCommit, PullFrame, PullRequest, PullResponse, PushRequest,
    PushResponse, RecordId, RecordState, RejectReason, ResetReason, ResetSnapshot, SchemaVersion,
    ScopeIdentity, ScopeSnapshot, SnapshotRecord, V1_NAME,
};

pub const HTTP_CONTENT_TYPE: &str = "application/vnd.surrealdb-sync.v1+cbor";
pub const MAX_IDENTITY_BYTES: usize = 1_024;
pub const MAX_CHECKPOINT_BYTES: usize = 4_096;

const SCHEMA_V1: u64 = 0;
const PUSH_REQUEST: u64 = 0;
const PUSH_RESPONSE: u64 = 1;
const PULL_REQUEST: u64 = 2;
const PULL_RESPONSE: u64 = 3;

pub fn encode_push_request(request: &PushRequest<CanonicalValue>) -> Result<Vec<u8>, CodecError> {
    encode_message(PUSH_REQUEST, |encoder| encoder.push_request(request))
}

pub fn decode_push_request(bytes: &[u8]) -> Result<PushRequest<CanonicalValue>, CodecError> {
    let payload = decode_message(bytes, PUSH_REQUEST)?;
    let message = Decoder::push_request(payload)?;
    require_typed_canonical(bytes, encode_push_request(&message)?)?;
    Ok(message)
}

pub fn encode_push_response(
    response: &PushResponse<CanonicalValue>,
) -> Result<Vec<u8>, CodecError> {
    encode_message(PUSH_RESPONSE, |encoder| encoder.push_response(response))
}

pub fn decode_push_response(bytes: &[u8]) -> Result<PushResponse<CanonicalValue>, CodecError> {
    let payload = decode_message(bytes, PUSH_RESPONSE)?;
    let message = Decoder::push_response(payload)?;
    require_typed_canonical(bytes, encode_push_response(&message)?)?;
    Ok(message)
}

pub fn encode_pull_request(request: &PullRequest) -> Result<Vec<u8>, CodecError> {
    encode_message(PULL_REQUEST, |encoder| encoder.pull_request(request))
}

pub fn decode_pull_request(bytes: &[u8]) -> Result<PullRequest, CodecError> {
    let payload = decode_message(bytes, PULL_REQUEST)?;
    let message = Decoder::pull_request(payload)?;
    require_typed_canonical(bytes, encode_pull_request(&message)?)?;
    Ok(message)
}

pub fn encode_pull_response(
    response: &PullResponse<CanonicalValue>,
) -> Result<Vec<u8>, CodecError> {
    encode_message(PULL_RESPONSE, |encoder| encoder.pull_response(response))
}

pub fn decode_pull_response(bytes: &[u8]) -> Result<PullResponse<CanonicalValue>, CodecError> {
    let payload = decode_message(bytes, PULL_RESPONSE)?;
    let message = Decoder::pull_response(payload)?;
    require_typed_canonical(bytes, encode_pull_response(&message)?)?;
    Ok(message)
}

fn require_typed_canonical(input: &[u8], encoded: Vec<u8>) -> Result<(), CodecError> {
    if input == encoded {
        Ok(())
    } else {
        Err(CodecError::NonCanonical)
    }
}

fn encode_message(
    kind: u64,
    payload: impl FnOnce(&mut Encoder) -> Result<Cbor, CodecError>,
) -> Result<Vec<u8>, CodecError> {
    let mut encoder = Encoder::new();
    encoder.item(4)?;
    take_bytes(&mut encoder.remaining_bytes, V1_NAME.len())?;
    let value = Cbor::Array(vec![
        Cbor::Text(V1_NAME.to_owned()),
        integer(SCHEMA_V1),
        integer(kind),
        payload(&mut encoder)?,
    ]);
    encode_cbor_value(value)
}

fn decode_message(bytes: &[u8], expected_kind: u64) -> Result<Cbor, CodecError> {
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::EncodedLimit);
    }
    preflight_canonical_cbor(bytes)?;
    let mut reader = Cursor::new(bytes);
    let decoded: Cbor = ciborium::de::from_reader(&mut reader).map_err(|_| CodecError::Decode)?;
    if reader.position() != bytes.len() as u64 {
        return Err(CodecError::TrailingBytes);
    }
    let [name, schema, kind, payload] = array(decoded)?;
    if text(name)? != V1_NAME || unsigned(schema)? != SCHEMA_V1 || unsigned(kind)? != expected_kind
    {
        return Err(CodecError::InvalidEnvelope);
    }
    Ok(payload)
}

struct Encoder {
    remaining_items: usize,
    remaining_bytes: usize,
}

impl Encoder {
    fn new() -> Self {
        Self {
            remaining_items: MAX_TOTAL_ITEMS,
            remaining_bytes: MAX_ENCODED_BYTES,
        }
    }

    fn item(&mut self, count: usize) -> Result<(), CodecError> {
        self.remaining_items = self
            .remaining_items
            .checked_sub(count)
            .ok_or(CodecError::ContainerLimit)?;
        Ok(())
    }

    fn vec(&mut self, length: usize) -> Result<(), CodecError> {
        if length > MAX_CONTAINER_ITEMS {
            return Err(CodecError::ContainerLimit);
        }
        self.item(length + 1)
    }

    fn identity_text(&mut self, value: &str) -> Result<Cbor, CodecError> {
        if value.is_empty() || value.len() > MAX_IDENTITY_BYTES {
            return Err(CodecError::InvalidIdentifier);
        }
        take_bytes(&mut self.remaining_bytes, value.len())?;
        self.item(1)?;
        Ok(Cbor::Text(value.to_owned()))
    }

    fn checkpoint_text(&mut self, value: &str) -> Result<Cbor, CodecError> {
        if value.is_empty() || value.len() > MAX_CHECKPOINT_BYTES {
            return Err(CodecError::InvalidIdentifier);
        }
        take_bytes(&mut self.remaining_bytes, value.len())?;
        self.item(1)?;
        Ok(Cbor::Text(value.to_owned()))
    }

    fn fingerprint(&mut self, value: &Fingerprint) -> Result<Cbor, CodecError> {
        validate_fingerprint(&value.0)?;
        take_bytes(&mut self.remaining_bytes, value.0.len())?;
        self.item(1)?;
        Ok(Cbor::Text(value.0.clone()))
    }

    fn canonical_value(&mut self, value: &CanonicalValue) -> Result<Cbor, CodecError> {
        validate_with_budget(
            value,
            0,
            &mut self.remaining_items,
            &mut self.remaining_bytes,
        )?;
        to_cbor(value)
    }

    fn push_request(&mut self, request: &PushRequest<CanonicalValue>) -> Result<Cbor, CodecError> {
        ensure_v1(request.schema_version)?;
        self.item(5)?;
        Ok(Cbor::Array(vec![
            self.identity_text(&request.partition_id.0)?,
            self.identity_text(&request.client_id.0)?,
            self.commit(&request.commit)?,
        ]))
    }

    fn commit(&mut self, commit: &ClientCommit<CanonicalValue>) -> Result<Cbor, CodecError> {
        self.item(4)?;
        self.vec(commit.operations.len())?;
        let operations = commit
            .operations
            .iter()
            .map(|operation| self.operation(operation))
            .collect::<Result<_, _>>()?;
        Ok(Cbor::Array(vec![
            self.identity_text(&commit.identity.client_commit_id.0)?,
            self.fingerprint(&commit.identity.fingerprint)?,
            Cbor::Array(operations),
        ]))
    }

    fn operation(&mut self, operation: &Operation<CanonicalValue>) -> Result<Cbor, CodecError> {
        match operation {
            Operation::Upsert {
                record_id,
                base_version,
                value,
                reference,
            } => {
                self.item(6)?;
                Ok(Cbor::Array(vec![
                    integer(0),
                    self.identity_text(&record_id.0)?,
                    encode_base_version(*base_version),
                    self.canonical_value(value)?,
                    self.optional_record_id(reference.as_ref())?,
                ]))
            }
            Operation::Delete {
                record_id,
                base_version,
            } => {
                self.item(4)?;
                Ok(Cbor::Array(vec![
                    integer(1),
                    self.identity_text(&record_id.0)?,
                    integer(*base_version),
                ]))
            }
        }
    }

    fn optional_record_id(&mut self, value: Option<&RecordId>) -> Result<Cbor, CodecError> {
        match value {
            Some(value) => self.identity_text(&value.0),
            None => {
                self.item(1)?;
                Ok(Cbor::Null)
            }
        }
    }

    fn identity(&mut self, identity: &CommitIdentity) -> Result<Cbor, CodecError> {
        self.item(3)?;
        Ok(Cbor::Array(vec![
            self.identity_text(&identity.client_commit_id.0)?,
            self.fingerprint(&identity.fingerprint)?,
        ]))
    }

    fn push_response(
        &mut self,
        response: &PushResponse<CanonicalValue>,
    ) -> Result<Cbor, CodecError> {
        ensure_v1(response.schema_version)?;
        self.item(5)?;
        Ok(Cbor::Array(vec![
            self.identity_text(&response.partition_id.0)?,
            self.identity_text(&response.client_id.0)?,
            self.outcome(&response.outcome)?,
        ]))
    }

    fn outcome(&mut self, outcome: &DurableOutcome<CanonicalValue>) -> Result<Cbor, CodecError> {
        match outcome {
            DurableOutcome::Accepted {
                identity,
                sequence,
                id_mappings,
            } => {
                self.item(5)?;
                self.vec(id_mappings.len())?;
                let mappings = id_mappings
                    .iter()
                    .map(|mapping| {
                        self.item(3)?;
                        Ok(Cbor::Array(vec![
                            self.identity_text(&mapping.local_id.0)?,
                            self.identity_text(&mapping.canonical_id.0)?,
                        ]))
                    })
                    .collect::<Result<_, CodecError>>()?;
                Ok(Cbor::Array(vec![
                    integer(0),
                    self.identity(identity)?,
                    integer(*sequence),
                    Cbor::Array(mappings),
                ]))
            }
            DurableOutcome::Conflict {
                identity,
                record_id,
                authoritative,
            } => {
                self.item(5)?;
                Ok(Cbor::Array(vec![
                    integer(1),
                    self.identity(identity)?,
                    self.identity_text(&record_id.0)?,
                    self.record_state(authoritative)?,
                ]))
            }
            DurableOutcome::Rejected { identity, reason } => {
                self.item(4)?;
                Ok(Cbor::Array(vec![
                    integer(2),
                    self.identity(identity)?,
                    integer(reject_reason(*reason)),
                ]))
            }
        }
    }

    fn record_state(&mut self, state: &RecordState<CanonicalValue>) -> Result<Cbor, CodecError> {
        match state {
            RecordState::Absent => {
                self.item(2)?;
                Ok(Cbor::Array(vec![integer(0)]))
            }
            RecordState::Present {
                value,
                version,
                reference,
            } => {
                self.item(5)?;
                Ok(Cbor::Array(vec![
                    integer(1),
                    self.canonical_value(value)?,
                    integer(*version),
                    self.optional_record_id(reference.as_ref())?,
                ]))
            }
            RecordState::Tombstone { version } => {
                self.item(3)?;
                Ok(Cbor::Array(vec![integer(2), integer(*version)]))
            }
        }
    }

    fn pull_request(&mut self, request: &PullRequest) -> Result<Cbor, CodecError> {
        ensure_v1(request.schema_version)?;
        self.item(8)?;
        let checkpoint = match &request.checkpoint {
            Some(checkpoint) => self.checkpoint_text(&checkpoint.0)?,
            None => Cbor::Null,
        };
        Ok(Cbor::Array(vec![
            self.identity_text(&request.partition_id.0)?,
            self.identity_text(&request.client_id.0)?,
            checkpoint,
            self.identity_text(&request.requested_scope.0)?,
            integer(request.subscription_revision),
        ]))
    }

    fn pull_response(
        &mut self,
        response: &PullResponse<CanonicalValue>,
    ) -> Result<Cbor, CodecError> {
        match response {
            PullResponse::Batch(batch) => {
                self.item(3)?;
                self.vec(batch.frames.len())?;
                let frames = batch
                    .frames
                    .iter()
                    .map(|frame| self.pull_frame(frame))
                    .collect::<Result<_, _>>()?;
                Ok(Cbor::Array(vec![integer(0), Cbor::Array(frames)]))
            }
            PullResponse::Reset(reset) => {
                self.item(5)?;
                self.vec(reset.records.len())?;
                let records = reset
                    .records
                    .iter()
                    .map(|record| self.snapshot_record(record))
                    .collect::<Result<_, _>>()?;
                Ok(Cbor::Array(vec![
                    integer(1),
                    integer(reset_reason(reset.reason)),
                    self.checkpoint(&reset.checkpoint)?,
                    Cbor::Array(records),
                ]))
            }
        }
    }

    fn pull_frame(&mut self, frame: &PullFrame<CanonicalValue>) -> Result<Cbor, CodecError> {
        match frame {
            PullFrame::Start { checkpoint } => {
                self.item(3)?;
                Ok(Cbor::Array(vec![integer(0), self.checkpoint(checkpoint)?]))
            }
            PullFrame::Commit { checkpoint, commit } => {
                self.item(4)?;
                Ok(Cbor::Array(vec![
                    integer(1),
                    self.checkpoint_text(&checkpoint.0)?,
                    self.pull_commit(commit)?,
                ]))
            }
            PullFrame::End { checkpoint } => {
                self.item(3)?;
                Ok(Cbor::Array(vec![integer(2), self.checkpoint(checkpoint)?]))
            }
        }
    }

    fn pull_commit(&mut self, commit: &PullCommit<CanonicalValue>) -> Result<Cbor, CodecError> {
        self.item(3)?;
        self.vec(commit.records.len())?;
        let records = commit
            .records
            .iter()
            .map(|record| self.applied_record(record))
            .collect::<Result<_, _>>()?;
        // Source identity is authority-private and deliberately omitted from pull bytes.
        Ok(Cbor::Array(vec![
            integer(commit.sequence),
            Cbor::Array(records),
        ]))
    }

    fn applied_record(
        &mut self,
        record: &AppliedRecord<CanonicalValue>,
    ) -> Result<Cbor, CodecError> {
        self.item(3)?;
        Ok(Cbor::Array(vec![
            self.identity_text(&record.record_id.0)?,
            self.record_state(&record.state)?,
        ]))
    }

    fn snapshot_record(
        &mut self,
        record: &SnapshotRecord<CanonicalValue>,
    ) -> Result<Cbor, CodecError> {
        self.item(3)?;
        Ok(Cbor::Array(vec![
            self.identity_text(&record.record_id.0)?,
            self.record_state(&record.state)?,
        ]))
    }

    fn checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<Cbor, CodecError> {
        self.item(4)?;
        Ok(Cbor::Array(vec![
            self.checkpoint_text(&checkpoint.token.0)?,
            Cbor::Array(vec![
                integer(checkpoint.cursor.epoch),
                integer(checkpoint.cursor.sequence),
            ]),
            Cbor::Array(vec![
                self.identity_text(&checkpoint.scope.identity.0)?,
                integer(checkpoint.scope.authorization_revision),
                integer(checkpoint.scope.subscription_revision),
            ]),
        ]))
    }
}

struct Decoder;

impl Decoder {
    fn push_request(value: Cbor) -> Result<PushRequest<CanonicalValue>, CodecError> {
        let [partition, client, commit] = array(value)?;
        Ok(PushRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId(identity(partition)?),
            client_id: ClientId(identity(client)?),
            commit: Self::commit(commit)?,
        })
    }

    fn commit(value: Cbor) -> Result<ClientCommit<CanonicalValue>, CodecError> {
        let [commit_id, fingerprint, operations] = array(value)?;
        Ok(ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId(identity(commit_id)?),
                fingerprint: Fingerprint(fingerprint_text(fingerprint)?),
            },
            operations: vec_decode(operations, Self::operation)?,
        })
    }

    fn operation(value: Cbor) -> Result<Operation<CanonicalValue>, CodecError> {
        let mut values = array_vec(value)?;
        let kind = values.first().ok_or(CodecError::InvalidEnvelope)?;
        match unsigned(kind.clone())? {
            0 if values.len() == 5 => Ok(Operation::Upsert {
                record_id: RecordId(identity(values.remove(1))?),
                base_version: decode_base_version(values.remove(1))?,
                value: from_cbor(values.remove(1))?,
                reference: optional_record_id(values.remove(1))?,
            }),
            1 if values.len() == 3 => Ok(Operation::Delete {
                record_id: RecordId(identity(values.remove(1))?),
                base_version: unsigned(values.remove(1))?,
            }),
            _ => Err(CodecError::InvalidEnvelope),
        }
    }

    fn push_response(value: Cbor) -> Result<PushResponse<CanonicalValue>, CodecError> {
        let [partition, client, outcome] = array(value)?;
        Ok(PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId(identity(partition)?),
            client_id: ClientId(identity(client)?),
            outcome: Self::outcome(outcome)?,
        })
    }

    fn outcome(value: Cbor) -> Result<DurableOutcome<CanonicalValue>, CodecError> {
        let mut values = array_vec(value)?;
        let kind = values.first().cloned().ok_or(CodecError::InvalidEnvelope)?;
        match unsigned(kind)? {
            0 if values.len() == 4 => Ok(DurableOutcome::Accepted {
                identity: decode_identity(values.remove(1))?,
                sequence: unsigned(values.remove(1))?,
                id_mappings: vec_decode(values.remove(1), |mapping| {
                    let [local, canonical] = array(mapping)?;
                    Ok(IdMapping {
                        local_id: RecordId(identity(local)?),
                        canonical_id: RecordId(identity(canonical)?),
                    })
                })?,
            }),
            1 if values.len() == 4 => Ok(DurableOutcome::Conflict {
                identity: decode_identity(values.remove(1))?,
                record_id: RecordId(identity(values.remove(1))?),
                authoritative: Self::record_state(values.remove(1))?,
            }),
            2 if values.len() == 3 => Ok(DurableOutcome::Rejected {
                identity: decode_identity(values.remove(1))?,
                reason: decode_reject_reason(unsigned(values.remove(1))?)?,
            }),
            _ => Err(CodecError::InvalidEnvelope),
        }
    }

    fn record_state(value: Cbor) -> Result<RecordState<CanonicalValue>, CodecError> {
        let mut values = array_vec(value)?;
        let kind = values.first().cloned().ok_or(CodecError::InvalidEnvelope)?;
        match unsigned(kind)? {
            0 if values.len() == 1 => Ok(RecordState::Absent),
            1 if values.len() == 4 => Ok(RecordState::Present {
                value: from_cbor(values.remove(1))?,
                version: unsigned(values.remove(1))?,
                reference: optional_record_id(values.remove(1))?,
            }),
            2 if values.len() == 2 => Ok(RecordState::Tombstone {
                version: unsigned(values.remove(1))?,
            }),
            _ => Err(CodecError::InvalidEnvelope),
        }
    }

    fn pull_request(value: Cbor) -> Result<PullRequest, CodecError> {
        let [partition, client, checkpoint, scope, subscription] = array(value)?;
        Ok(PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId(identity(partition)?),
            client_id: ClientId(identity(client)?),
            checkpoint: optional_checkpoint(checkpoint)?,
            requested_scope: ScopeIdentity(identity(scope)?),
            subscription_revision: unsigned(subscription)?,
        })
    }

    fn pull_response(value: Cbor) -> Result<PullResponse<CanonicalValue>, CodecError> {
        let mut values = array_vec(value)?;
        let kind = values.first().cloned().ok_or(CodecError::InvalidEnvelope)?;
        match unsigned(kind)? {
            0 if values.len() == 2 => Ok(PullResponse::Batch(PullBatch {
                frames: vec_decode(values.remove(1), Self::pull_frame)?,
            })),
            1 if values.len() == 4 => Ok(PullResponse::Reset(ResetSnapshot {
                reason: decode_reset_reason(unsigned(values.remove(1))?)?,
                checkpoint: decode_checkpoint(values.remove(1))?,
                records: vec_decode(values.remove(1), |record| {
                    let [record_id, state] = array(record)?;
                    Ok(SnapshotRecord {
                        record_id: RecordId(identity(record_id)?),
                        state: Self::record_state(state)?,
                    })
                })?,
            })),
            _ => Err(CodecError::InvalidEnvelope),
        }
    }

    fn pull_frame(value: Cbor) -> Result<PullFrame<CanonicalValue>, CodecError> {
        let mut values = array_vec(value)?;
        let kind = values.first().cloned().ok_or(CodecError::InvalidEnvelope)?;
        match unsigned(kind)? {
            0 if values.len() == 2 => Ok(PullFrame::Start {
                checkpoint: decode_checkpoint(values.remove(1))?,
            }),
            1 if values.len() == 3 => Ok(PullFrame::Commit {
                checkpoint: OpaqueCheckpoint(checkpoint_text(values.remove(1))?),
                commit: Self::pull_commit(values.remove(1))?,
            }),
            2 if values.len() == 2 => Ok(PullFrame::End {
                checkpoint: decode_checkpoint(values.remove(1))?,
            }),
            _ => Err(CodecError::InvalidEnvelope),
        }
    }

    fn pull_commit(value: Cbor) -> Result<PullCommit<CanonicalValue>, CodecError> {
        let [sequence, records] = array(value)?;
        Ok(PullCommit {
            sequence: unsigned(sequence)?,
            source: None,
            records: vec_decode(records, |record| {
                let [record_id, state] = array(record)?;
                Ok(AppliedRecord {
                    record_id: RecordId(identity(record_id)?),
                    state: Self::record_state(state)?,
                })
            })?,
        })
    }
}

fn integer(value: u64) -> Cbor {
    Cbor::Integer(value.into())
}

fn array<const N: usize>(value: Cbor) -> Result<[Cbor; N], CodecError> {
    let Cbor::Array(values) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    values.try_into().map_err(|_| CodecError::InvalidEnvelope)
}

fn array_vec(value: Cbor) -> Result<Vec<Cbor>, CodecError> {
    let Cbor::Array(values) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    if values.len() > MAX_CONTAINER_ITEMS {
        return Err(CodecError::ContainerLimit);
    }
    Ok(values)
}

fn vec_decode<T>(
    value: Cbor,
    decode: impl FnMut(Cbor) -> Result<T, CodecError>,
) -> Result<Vec<T>, CodecError> {
    array_vec(value)?.into_iter().map(decode).collect()
}

fn unsigned(value: Cbor) -> Result<u64, CodecError> {
    let Cbor::Integer(value) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    u64::try_from(value).map_err(|_| CodecError::InvalidEnvelope)
}

fn text(value: Cbor) -> Result<String, CodecError> {
    let Cbor::Text(value) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    Ok(value)
}

fn identity(value: Cbor) -> Result<String, CodecError> {
    let value = text(value)?;
    if value.is_empty() || value.len() > MAX_IDENTITY_BYTES {
        return Err(CodecError::InvalidIdentifier);
    }
    Ok(value)
}

fn checkpoint_text(value: Cbor) -> Result<String, CodecError> {
    let value = text(value)?;
    if value.is_empty() || value.len() > MAX_CHECKPOINT_BYTES {
        return Err(CodecError::InvalidIdentifier);
    }
    Ok(value)
}

fn fingerprint_text(value: Cbor) -> Result<String, CodecError> {
    let value = text(value)?;
    validate_fingerprint(&value)?;
    Ok(value)
}

fn validate_fingerprint(value: &str) -> Result<(), CodecError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CodecError::InvalidFingerprint);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CodecError::InvalidFingerprint);
    }
    Ok(())
}

fn ensure_v1(version: SchemaVersion) -> Result<(), CodecError> {
    match version {
        SchemaVersion::V1 => Ok(()),
    }
}

fn encode_base_version(version: BaseVersion) -> Cbor {
    match version {
        BaseVersion::Absent => Cbor::Array(vec![integer(0)]),
        BaseVersion::Exact(version) => Cbor::Array(vec![integer(1), integer(version)]),
    }
}

fn decode_base_version(value: Cbor) -> Result<BaseVersion, CodecError> {
    let values = array_vec(value)?;
    match values.as_slice() {
        [kind] if unsigned(kind.clone())? == 0 => Ok(BaseVersion::Absent),
        [kind, version] if unsigned(kind.clone())? == 1 => {
            Ok(BaseVersion::Exact(unsigned(version.clone())?))
        }
        _ => Err(CodecError::InvalidEnvelope),
    }
}

fn optional_record_id(value: Cbor) -> Result<Option<RecordId>, CodecError> {
    match value {
        Cbor::Null => Ok(None),
        value => Ok(Some(RecordId(identity(value)?))),
    }
}

fn optional_checkpoint(value: Cbor) -> Result<Option<OpaqueCheckpoint>, CodecError> {
    match value {
        Cbor::Null => Ok(None),
        value => Ok(Some(OpaqueCheckpoint(checkpoint_text(value)?))),
    }
}

fn decode_identity(value: Cbor) -> Result<CommitIdentity, CodecError> {
    let [commit_id, fingerprint] = array(value)?;
    Ok(CommitIdentity {
        client_commit_id: ClientCommitId(identity(commit_id)?),
        fingerprint: Fingerprint(fingerprint_text(fingerprint)?),
    })
}

fn decode_checkpoint(value: Cbor) -> Result<Checkpoint, CodecError> {
    let [token, cursor, scope] = array(value)?;
    let [epoch, sequence] = array(cursor)?;
    let [
        scope_identity,
        authorization_revision,
        subscription_revision,
    ] = array(scope)?;
    Ok(Checkpoint {
        token: OpaqueCheckpoint(checkpoint_text(token)?),
        cursor: ProtocolCursor {
            epoch: unsigned(epoch)?,
            sequence: unsigned(sequence)?,
        },
        scope: ScopeSnapshot {
            identity: ScopeIdentity(identity(scope_identity)?),
            authorization_revision: unsigned(authorization_revision)?,
            subscription_revision: unsigned(subscription_revision)?,
        },
    })
}

fn reject_reason(reason: RejectReason) -> u64 {
    match reason {
        RejectReason::InvalidOperation => 0,
        RejectReason::CommitIdentityMismatch => 1,
        RejectReason::Unauthorized => 2,
        RejectReason::UnsupportedSchemaVersion => 3,
    }
}

fn decode_reject_reason(value: u64) -> Result<RejectReason, CodecError> {
    match value {
        0 => Ok(RejectReason::InvalidOperation),
        1 => Ok(RejectReason::CommitIdentityMismatch),
        2 => Ok(RejectReason::Unauthorized),
        3 => Ok(RejectReason::UnsupportedSchemaVersion),
        _ => Err(CodecError::InvalidEnvelope),
    }
}

fn reset_reason(reason: ResetReason) -> u64 {
    match reason {
        ResetReason::CheckpointExpired => 0,
        ResetReason::EpochChanged => 1,
        ResetReason::AuthorizationChanged => 2,
        ResetReason::SubscriptionChanged => 3,
        ResetReason::ScopeChanged => 4,
        ResetReason::ExternalWriteQuarantined => 5,
    }
}

fn decode_reset_reason(value: u64) -> Result<ResetReason, CodecError> {
    match value {
        0 => Ok(ResetReason::CheckpointExpired),
        1 => Ok(ResetReason::EpochChanged),
        2 => Ok(ResetReason::AuthorizationChanged),
        3 => Ok(ResetReason::SubscriptionChanged),
        4 => Ok(ResetReason::ScopeChanged),
        5 => Ok(ResetReason::ExternalWriteQuarantined),
        _ => Err(CodecError::InvalidEnvelope),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn identity() -> CommitIdentity {
        CommitIdentity {
            client_commit_id: ClientCommitId("commit-1".into()),
            fingerprint: Fingerprint(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            ),
        }
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            token: OpaqueCheckpoint("opaque-token".into()),
            cursor: ProtocolCursor {
                epoch: 3,
                sequence: 9,
            },
            scope: ScopeSnapshot {
                identity: ScopeIdentity("scope-a".into()),
                authorization_revision: 4,
                subscription_revision: 5,
            },
        }
    }

    fn value() -> CanonicalValue {
        CanonicalValue::Object(BTreeMap::from([
            ("none".into(), CanonicalValue::None),
            ("number".into(), CanonicalValue::Int(-7)),
            (
                "record".into(),
                CanonicalValue::RecordId {
                    table: "person".into(),
                    key: Box::new(CanonicalValue::String("alice".into())),
                },
            ),
        ]))
    }

    #[test]
    fn all_http_messages_round_trip() {
        let push = PushRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("primary".into()),
            client_id: ClientId("client-a".into()),
            commit: ClientCommit {
                identity: identity(),
                operations: vec![
                    Operation::Upsert {
                        record_id: RecordId("local:1".into()),
                        base_version: BaseVersion::Absent,
                        value: value(),
                        reference: Some(RecordId("person:bob".into())),
                    },
                    Operation::Upsert {
                        record_id: RecordId("note:existing".into()),
                        base_version: BaseVersion::Exact(4),
                        value: CanonicalValue::Bytes(vec![0, 1, 2]),
                        reference: None,
                    },
                    Operation::Delete {
                        record_id: RecordId("old:1".into()),
                        base_version: 2,
                    },
                ],
            },
        };
        assert_eq!(
            decode_push_request(&encode_push_request(&push).unwrap()).unwrap(),
            push
        );

        let mut outcomes = vec![
            DurableOutcome::Accepted {
                identity: identity(),
                sequence: 10,
                id_mappings: vec![IdMapping {
                    local_id: RecordId("local:1".into()),
                    canonical_id: RecordId("person:1".into()),
                }],
            },
            DurableOutcome::Conflict {
                identity: identity(),
                record_id: RecordId("person:1".into()),
                authoritative: RecordState::Present {
                    value: value(),
                    version: 11,
                    reference: None,
                },
            },
            DurableOutcome::Rejected {
                identity: identity(),
                reason: RejectReason::InvalidOperation,
            },
        ];
        outcomes.extend(
            [
                RejectReason::CommitIdentityMismatch,
                RejectReason::Unauthorized,
                RejectReason::UnsupportedSchemaVersion,
            ]
            .map(|reason| DurableOutcome::Rejected {
                identity: identity(),
                reason,
            }),
        );
        for outcome in outcomes {
            let response = PushResponse {
                schema_version: SchemaVersion::V1,
                partition_id: PartitionId("primary".into()),
                client_id: ClientId("client-a".into()),
                outcome,
            };
            assert_eq!(
                decode_push_response(&encode_push_response(&response).unwrap()).unwrap(),
                response
            );
        }

        let pull = PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("primary".into()),
            client_id: ClientId("client-a".into()),
            checkpoint: Some(OpaqueCheckpoint("opaque-token".into())),
            requested_scope: ScopeIdentity("scope-a".into()),
            subscription_revision: 5,
        };
        assert_eq!(
            decode_pull_request(&encode_pull_request(&pull).unwrap()).unwrap(),
            pull
        );

        let commit = PullCommit {
            sequence: 10,
            source: Some(identity()),
            records: vec![AppliedRecord {
                record_id: RecordId("person:1".into()),
                state: RecordState::Tombstone { version: 10 },
            }],
        };
        let batch = PullResponse::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: checkpoint(),
                },
                PullFrame::Commit {
                    checkpoint: checkpoint().token,
                    commit,
                },
                PullFrame::End {
                    checkpoint: checkpoint(),
                },
            ],
        });
        let decoded = decode_pull_response(&encode_pull_response(&batch).unwrap()).unwrap();
        let PullResponse::Batch(decoded_batch) = &decoded else {
            panic!("expected batch")
        };
        let PullFrame::Commit { commit, .. } = &decoded_batch.frames[1] else {
            panic!("expected commit")
        };
        assert_eq!(commit.source, None);

        for reason in [
            ResetReason::CheckpointExpired,
            ResetReason::EpochChanged,
            ResetReason::AuthorizationChanged,
            ResetReason::SubscriptionChanged,
            ResetReason::ScopeChanged,
            ResetReason::ExternalWriteQuarantined,
        ] {
            let reset = PullResponse::Reset(ResetSnapshot {
                reason,
                checkpoint: checkpoint(),
                records: vec![SnapshotRecord {
                    record_id: RecordId("person:1".into()),
                    state: RecordState::Absent,
                }],
            });
            assert_eq!(
                decode_pull_response(&encode_pull_response(&reset).unwrap()).unwrap(),
                reset
            );
        }
    }

    #[test]
    fn malformed_noncanonical_and_type_confused_envelopes_fail_closed() {
        assert_eq!(decode_pull_request(&[0x9f, 0xff]), Err(CodecError::Decode));
        assert_eq!(
            decode_pull_request(&[0x83, 0x18, 0x00, 0x02, 0x80]),
            Err(CodecError::InvalidEnvelope)
        );
        assert_eq!(
            decode_pull_request(&[0x83, 0x00, 0x00, 0x80]),
            Err(CodecError::InvalidEnvelope)
        );
        let mut valid = encode_pull_request(&PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            checkpoint: None,
            requested_scope: ScopeIdentity("s".into()),
            subscription_revision: 0,
        })
        .unwrap();
        valid.push(0);
        assert_eq!(decode_pull_request(&valid), Err(CodecError::TrailingBytes));

        let canonical_pull = encode_pull_request(&PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            checkpoint: None,
            requested_scope: ScopeIdentity("s".into()),
            subscription_revision: 0,
        })
        .unwrap();
        let mut decoded: Cbor = ciborium::de::from_reader(canonical_pull.as_slice()).unwrap();
        let Cbor::Array(root) = &mut decoded else {
            unreachable!()
        };
        root[0] = Cbor::Text("another-protocol/1".into());
        assert_eq!(
            decode_pull_request(&encode_cbor_value(decoded).unwrap()),
            Err(CodecError::InvalidEnvelope)
        );

        let mut decoded: Cbor = ciborium::de::from_reader(canonical_pull.as_slice()).unwrap();
        let Cbor::Array(root) = &mut decoded else {
            unreachable!()
        };
        let Cbor::Array(payload) = &mut root[3] else {
            unreachable!()
        };
        payload[4] = Cbor::Integer((-1).into());
        assert_eq!(
            decode_pull_request(&encode_cbor_value(decoded).unwrap()),
            Err(CodecError::InvalidEnvelope)
        );

        let noncanonical_object = Cbor::Map(vec![
            (Cbor::Text("aa".into()), integer(1)),
            (Cbor::Text("b".into()), integer(2)),
        ]);
        let noncanonical_request = Cbor::Array(vec![
            Cbor::Text(V1_NAME.into()),
            integer(SCHEMA_V1),
            integer(PUSH_REQUEST),
            Cbor::Array(vec![
                Cbor::Text("p".into()),
                Cbor::Text("c".into()),
                Cbor::Array(vec![
                    Cbor::Text("commit".into()),
                    Cbor::Text(identity().fingerprint.0),
                    Cbor::Array(vec![Cbor::Array(vec![
                        integer(0),
                        Cbor::Text("record:1".into()),
                        Cbor::Array(vec![integer(0)]),
                        noncanonical_object,
                        Cbor::Null,
                    ])]),
                ]),
            ]),
        ]);
        let bytes = encode_cbor_value(noncanonical_request).unwrap();
        assert_eq!(decode_push_request(&bytes), Err(CodecError::NonCanonical));

        let valid_push = PushRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            commit: ClientCommit {
                identity: identity(),
                operations: vec![],
            },
        };
        let mut decoded: Cbor =
            ciborium::de::from_reader(encode_push_request(&valid_push).unwrap().as_slice())
                .unwrap();
        let Cbor::Array(root) = &mut decoded else {
            unreachable!()
        };
        let Cbor::Array(payload) = &mut root[3] else {
            unreachable!()
        };
        let Cbor::Array(commit) = &mut payload[2] else {
            unreachable!()
        };
        commit[1] = Cbor::Text(
            "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        );
        assert_eq!(
            decode_push_request(&encode_cbor_value(decoded).unwrap()),
            Err(CodecError::InvalidFingerprint)
        );
    }

    #[test]
    fn limits_and_fingerprints_are_strict() {
        let request = PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            checkpoint: Some(OpaqueCheckpoint("x".repeat(MAX_CHECKPOINT_BYTES + 1))),
            requested_scope: ScopeIdentity("s".into()),
            subscription_revision: 0,
        };
        assert_eq!(
            encode_pull_request(&request),
            Err(CodecError::InvalidIdentifier)
        );

        let mut bad = identity();
        bad.fingerprint.0 = "sha256:ABCDEF".into();
        let response = PushResponse::<CanonicalValue> {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            outcome: DurableOutcome::Rejected {
                identity: bad,
                reason: RejectReason::InvalidOperation,
            },
        };
        assert_eq!(
            encode_push_response(&response),
            Err(CodecError::InvalidFingerprint)
        );

        let response = PullResponse::Batch(PullBatch::<CanonicalValue> {
            frames: vec![
                PullFrame::Start {
                    checkpoint: checkpoint()
                };
                MAX_CONTAINER_ITEMS + 1
            ],
        });
        assert_eq!(
            encode_pull_response(&response),
            Err(CodecError::ContainerLimit)
        );
    }

    #[test]
    fn http_message_golden_vectors_are_stable() {
        let fingerprint = identity().fingerprint;
        let push_request = PushRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            commit: ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("i".into()),
                    fingerprint: fingerprint.clone(),
                },
                operations: vec![],
            },
        };
        let push_response = PushResponse::<CanonicalValue> {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            outcome: DurableOutcome::Rejected {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("i".into()),
                    fingerprint,
                },
                reason: RejectReason::Unauthorized,
            },
        };
        let pull_request = PullRequest {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            checkpoint: None,
            requested_scope: ScopeIdentity("s".into()),
            subscription_revision: 0,
        };
        let pull_response = PullResponse::Batch(PullBatch::<CanonicalValue> { frames: vec![] });
        let vectors = [
            encode_push_request(&push_request).unwrap(),
            encode_push_response(&push_response).unwrap(),
            encode_pull_request(&pull_request).unwrap(),
            encode_pull_response(&pull_response).unwrap(),
        ]
        .map(|bytes| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        });
        assert_eq!(
            vectors,
            [
                "84707375727265616c64622d73796e632f310000836170616383616978477368613235363a3031323334353637383961626364656630313233343536373839616263646566303132333435363738396162636465663031323334353637383961626364656680",
                "84707375727265616c64622d73796e632f3100018361706163830282616978477368613235363a3031323334353637383961626364656630313233343536373839616263646566303132333435363738396162636465663031323334353637383961626364656602",
                "84707375727265616c64622d73796e632f3100028561706163f6617300",
                "84707375727265616c64622d73796e632f310003820080",
            ]
        );
    }
}
