//! Additive local read protocol. This is separate from the authority wire protocol.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use surrealdb_sync_protocol::{
    CommitIdentity, Cursor, PartitionId, RecordId, RecordState, ScopeIdentity, ScopeSnapshot,
};

use crate::{ConflictResolution, OptimisticRecord};

pub const READ_PROTOCOL_VERSION: u8 = 1;

/// Persisted proof that a complete authorized reset replaced this scope's projection.
/// It does not contain the opaque checkpoint token or claim universal partition access.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetCoverage {
    pub cursor: Cursor,
    pub scope: ScopeSnapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadScopeV1 {
    pub partition_id: PartitionId,
    pub scope_identity: ScopeIdentity,
    pub subscription_revision: u64,
    pub authorization_revision: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadReadinessV1 {
    LocalAvailableAwaitingCoverage,
    AuthorizedScopeAsOf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReadCoverageV1 {
    Unknown,
    AuthorizedReset { cursor: Cursor },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadMetadataV1 {
    pub revision: u64,
    pub scope: ReadScopeV1,
    pub as_of: Option<Cursor>,
    pub readiness: ReadReadinessV1,
    pub coverage: ReadCoverageV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConfirmedAbsenceV1 {
    NotAbsent,
    Unknown,
    AbsentWithinAuthorizedScope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadConflictV1 {
    pub identity: CommitIdentity,
    pub resolution: Option<ConflictResolution>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadAcceptedStatusV1 {
    CoveredByAuthorizedReset,
    AwaitingPullEpochUnknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadAcceptedV1 {
    pub identity: CommitIdentity,
    /// The original v1 receipt sequence, never an inferred acceptance epoch.
    pub sequence: u64,
    pub status: ReadAcceptedStatusV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRowV1<V> {
    pub record_id: RecordId,
    pub confirmed: RecordState<V>,
    pub confirmed_absence: ReadConfirmedAbsenceV1,
    pub optimistic: Option<OptimisticRecord<V>>,
    pub pending: Vec<CommitIdentity>,
    pub conflicts: Vec<ReadConflictV1>,
    pub accepted: Vec<ReadAcceptedV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSnapshotV1<V> {
    pub protocol_version: u8,
    pub metadata: ReadMetadataV1,
    pub rows: BTreeMap<RecordId, ReadRowV1<V>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ReadRowChangeV1<V> {
    Upsert { row: ReadRowV1<V> },
    Remove { record_id: RecordId },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommittedReadBatchV1<V> {
    pub protocol_version: u8,
    pub before_revision: u64,
    pub revision: u64,
    pub metadata: ReadMetadataV1,
    pub changes: Vec<ReadRowChangeV1<V>>,
}

/// Diagnostic only. The old journal is preserved; authorization and a new
/// journal's atomic lifecycle belong to the native/storage adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeTransitionDecisionV1 {
    Unchanged,
    NeedsNewScopeJournal,
    PendingQuarantineRequired,
}

use crate::{ClientError, ClientRuntime, DurableClientState};
use std::collections::BTreeSet;
use surrealdb_sync_protocol::DurableOutcome;

pub(crate) fn validate_coverage<V>(state: &DurableClientState<V>) -> Result<(), ClientError> {
    if let Some(coverage) = &state.reset_coverage {
        let checkpoint = state
            .checkpoint
            .as_ref()
            .ok_or(ClientError::InvalidReadCoverage)?;
        if coverage.cursor.epoch == 0
            || coverage.cursor.epoch != checkpoint.cursor.epoch
            || coverage.cursor.sequence > checkpoint.cursor.sequence
            || coverage.scope != checkpoint.scope
        {
            return Err(ClientError::InvalidReadCoverage);
        }
    }
    Ok(())
}

fn metadata<V>(state: &DurableClientState<V>) -> ReadMetadataV1 {
    ReadMetadataV1 {
        revision: state.revision,
        scope: ReadScopeV1 {
            partition_id: state.partition_id.clone(),
            scope_identity: state.requested_scope.clone(),
            subscription_revision: state.subscription_revision,
            authorization_revision: state
                .checkpoint
                .as_ref()
                .map(|c| c.scope.authorization_revision),
        },
        as_of: state.checkpoint.as_ref().map(|c| c.cursor),
        readiness: if state.reset_coverage.is_some() {
            ReadReadinessV1::AuthorizedScopeAsOf
        } else {
            ReadReadinessV1::LocalAvailableAwaitingCoverage
        },
        coverage: state
            .reset_coverage
            .as_ref()
            .map_or(ReadCoverageV1::Unknown, |coverage| {
                ReadCoverageV1::AuthorizedReset {
                    cursor: coverage.cursor,
                }
            }),
    }
}

fn empty_row<V>(record_id: RecordId, complete: bool) -> ReadRowV1<V> {
    ReadRowV1 {
        record_id,
        confirmed: RecordState::Absent,
        confirmed_absence: if complete {
            ReadConfirmedAbsenceV1::AbsentWithinAuthorizedScope
        } else {
            ReadConfirmedAbsenceV1::Unknown
        },
        optimistic: None,
        pending: vec![],
        conflicts: vec![],
        accepted: vec![],
    }
}

fn snapshot<V: Clone + Eq>(state: &DurableClientState<V>) -> ReadSnapshotV1<V> {
    let complete = state.reset_coverage.is_some();
    let mut rows = BTreeMap::new();
    for (id, confirmed) in &state.confirmed {
        let mut row = empty_row(id.clone(), complete);
        row.confirmed = confirmed.clone();
        row.confirmed_absence = ReadConfirmedAbsenceV1::NotAbsent;
        rows.insert(id.clone(), row);
    }
    for (id, optimistic) in crate::optimistic_projection(state) {
        rows.entry(id.clone())
            .or_insert_with(|| empty_row(id, complete))
            .optimistic = Some(optimistic);
    }
    for commit in &state.outbox {
        for operation in &commit.operations {
            let id = operation.record_id().clone();
            rows.entry(id.clone())
                .or_insert_with(|| empty_row(id, complete))
                .pending
                .push(commit.identity.clone());
        }
    }
    for resolved in &state.outcomes {
        for operation in &resolved.local_commit.operations {
            let id = operation.record_id().clone();
            match &resolved.outcome {
                DurableOutcome::Accepted { sequence, .. } => rows
                    .entry(id.clone())
                    .or_insert_with(|| empty_row(id, complete))
                    .accepted
                    .push(ReadAcceptedV1 {
                        identity: resolved.local_commit.identity.clone(),
                        sequence: *sequence,
                        status: if resolved.covered_by_reset {
                            ReadAcceptedStatusV1::CoveredByAuthorizedReset
                        } else {
                            ReadAcceptedStatusV1::AwaitingPullEpochUnknown
                        },
                    }),
                DurableOutcome::Conflict { .. } => rows
                    .entry(id.clone())
                    .or_insert_with(|| empty_row(id, complete))
                    .conflicts
                    .push(ReadConflictV1 {
                        identity: resolved.local_commit.identity.clone(),
                        resolution: resolved.resolution.clone(),
                    }),
                DurableOutcome::Rejected { .. } => {}
            }
        }
    }
    ReadSnapshotV1 {
        protocol_version: READ_PROTOCOL_VERSION,
        metadata: metadata(state),
        rows,
    }
}

impl<V: Clone + Eq> ReadSnapshotV1<V> {
    /// Missing means absent only within this snapshot's complete authorized scope.
    /// Unknown coverage never turns a missing local row into authoritative absence.
    pub fn row(&self, record_id: &RecordId) -> ReadRowV1<V> {
        self.rows.get(record_id).cloned().unwrap_or_else(|| {
            empty_row(
                record_id.clone(),
                matches!(
                    self.metadata.coverage,
                    ReadCoverageV1::AuthorizedReset { .. }
                ),
            )
        })
    }
}

impl<V: Clone + Eq> ClientRuntime<V> {
    /// Metadata of the installed local view without materializing record payloads.
    pub fn read_metadata(&self) -> ReadMetadataV1 {
        metadata(&self.state)
    }

    /// One pure read of one installed durable revision; no database/network read.
    pub fn read_snapshot(&self) -> ReadSnapshotV1<V> {
        snapshot(&self.state)
    }

    /// This diagnostic never reassigns or clears the existing scope's journal.
    pub fn scope_transition_decision(
        &self,
        requested_scope: &ScopeIdentity,
        subscription_revision: u64,
    ) -> ScopeTransitionDecisionV1 {
        if *requested_scope == self.state.requested_scope
            && subscription_revision == self.state.subscription_revision
        {
            return ScopeTransitionDecisionV1::Unchanged;
        }
        if !self.state.outbox.is_empty()
            || self.state.outcomes.iter().any(|outcome| {
                matches!(outcome.outcome, DurableOutcome::Conflict { .. })
                    && outcome.resolution.is_none()
            })
        {
            ScopeTransitionDecisionV1::PendingQuarantineRequired
        } else {
            ScopeTransitionDecisionV1::NeedsNewScopeJournal
        }
    }
}

pub(crate) fn committed_batch<V: Clone + Eq>(
    before: &DurableClientState<V>,
    after: &DurableClientState<V>,
) -> Option<CommittedReadBatchV1<V>> {
    if before.revision == after.revision {
        return None;
    }
    let before = snapshot(before);
    let after = snapshot(after);
    let ids = before
        .rows
        .keys()
        .chain(after.rows.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut changes = Vec::new();
    for id in ids {
        if before.rows.get(&id) != after.rows.get(&id) {
            match after.rows.get(&id) {
                Some(row) => changes.push(ReadRowChangeV1::Upsert { row: row.clone() }),
                None => changes.push(ReadRowChangeV1::Remove { record_id: id }),
            }
        }
    }
    Some(CommittedReadBatchV1 {
        protocol_version: READ_PROTOCOL_VERSION,
        before_revision: before.metadata.revision,
        revision: after.metadata.revision,
        metadata: after.metadata,
        changes,
    })
}
