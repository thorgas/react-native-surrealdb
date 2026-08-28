//! Versioned, transport-neutral messages for the sync correctness boundary.
//!
//! Authentication, serialization, compression, HTTP framing, and the internal
//! SurrealDB changefeed resume position are intentionally outside this crate.

use serde::{Deserialize, Serialize};

pub mod canonical;
pub use canonical::{
    CanonicalValue, CodecError, MAX_CONTAINER_ITEMS, MAX_DEPTH, MAX_ENCODED_BYTES,
    MAX_STRING_BYTES, MAX_TOTAL_ITEMS, canonical_cbor, decode_canonical_cbor, fingerprint_commit,
};

pub const V1_NAME: &str = "surrealdb-sync/1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaVersion {
    V1,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PartitionId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ClientId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ClientCommitId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Fingerprint(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RecordId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ScopeIdentity(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OpaqueCheckpoint(pub String);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitIdentity {
    pub client_commit_id: ClientCommitId,
    pub fingerprint: Fingerprint,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseVersion {
    Absent,
    Exact(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Operation<V> {
    Upsert {
        record_id: RecordId,
        base_version: BaseVersion,
        value: V,
        reference: Option<RecordId>,
    },
    Delete {
        record_id: RecordId,
        base_version: u64,
    },
}

impl<V> Operation<V> {
    pub fn record_id(&self) -> &RecordId {
        match self {
            Self::Upsert { record_id, .. } | Self::Delete { record_id, .. } => record_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCommit<V> {
    pub identity: CommitIdentity,
    pub operations: Vec<Operation<V>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushRequest<V> {
    pub schema_version: SchemaVersion,
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub commit: ClientCommit<V>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdMapping {
    pub local_id: RecordId,
    pub canonical_id: RecordId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecordState<V> {
    Absent,
    Present {
        value: V,
        version: u64,
        reference: Option<RecordId>,
    },
    Tombstone {
        version: u64,
    },
}

impl<V> RecordState<V> {
    pub fn version(&self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Present { version, .. } | Self::Tombstone { version } => Some(*version),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    InvalidOperation,
    CommitIdentityMismatch,
    Unauthorized,
    UnsupportedSchemaVersion,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DurableOutcome<V> {
    Accepted {
        identity: CommitIdentity,
        sequence: u64,
        id_mappings: Vec<IdMapping>,
    },
    Conflict {
        identity: CommitIdentity,
        record_id: RecordId,
        authoritative: RecordState<V>,
    },
    Rejected {
        identity: CommitIdentity,
        reason: RejectReason,
    },
}

impl<V> DurableOutcome<V> {
    pub fn identity(&self) -> &CommitIdentity {
        match self {
            Self::Accepted { identity, .. }
            | Self::Conflict { identity, .. }
            | Self::Rejected { identity, .. } => identity,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushResponse<V> {
    pub schema_version: SchemaVersion,
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub outcome: DurableOutcome<V>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cursor {
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSnapshot {
    pub identity: ScopeIdentity,
    pub authorization_revision: u64,
    pub subscription_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub token: OpaqueCheckpoint,
    pub cursor: Cursor,
    pub scope: ScopeSnapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequest {
    pub schema_version: SchemaVersion,
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub checkpoint: Option<OpaqueCheckpoint>,
    pub requested_scope: ScopeIdentity,
    pub subscription_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppliedRecord<V> {
    pub record_id: RecordId,
    pub state: RecordState<V>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullCommit<V> {
    pub sequence: u64,
    pub source: Option<CommitIdentity>,
    pub records: Vec<AppliedRecord<V>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum PullFrame<V> {
    Start {
        checkpoint: Checkpoint,
    },
    Commit {
        checkpoint: OpaqueCheckpoint,
        commit: PullCommit<V>,
    },
    End {
        checkpoint: Checkpoint,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullBatch<V> {
    pub frames: Vec<PullFrame<V>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetReason {
    CheckpointExpired,
    EpochChanged,
    AuthorizationChanged,
    SubscriptionChanged,
    ScopeChanged,
    ExternalWriteQuarantined,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotRecord<V> {
    pub record_id: RecordId,
    pub state: RecordState<V>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetSnapshot<V> {
    pub reason: ResetReason,
    pub checkpoint: Checkpoint,
    pub records: Vec<SnapshotRecord<V>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "response", rename_all = "snake_case")]
pub enum PullResponse<V> {
    Batch(PullBatch<V>),
    Reset(ResetSnapshot<V>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_and_version_are_stable() {
        assert_eq!(V1_NAME, "surrealdb-sync/1");
        assert_eq!(SchemaVersion::V1, SchemaVersion::V1);
    }

    #[test]
    fn tombstones_and_absence_are_distinct() {
        let absent = RecordState::<String>::Absent;
        let tombstone = RecordState::<String>::Tombstone { version: 3 };
        assert_eq!(absent.version(), None);
        assert_eq!(tombstone.version(), Some(3));
        assert_ne!(absent, tombstone);
    }
}
