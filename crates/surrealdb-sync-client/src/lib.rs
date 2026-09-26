//! Storage-neutral client transitions for `surrealdb-sync/1`.
//!
//! The runtime prepares complete durable replacements but performs no I/O. An
//! adapter persists [`PreparedTransition::state`] atomically, then calls
//! [`ClientRuntime::install`] only after persistence succeeds. This keeps the
//! correctness boundary compatible with asynchronous transactional stores
//! without selecting an async runtime or persistence dependency.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};
use surrealdb_sync_protocol::{
    BaseVersion, Checkpoint, ClientCommit, ClientCommitId, ClientId, CommitIdentity,
    DurableOutcome, IdMapping, Operation, PartitionId, PullBatch, PullFrame, PullResponse,
    PushResponse, RecordId, RecordState, ResetSnapshot, SchemaVersion, ScopeIdentity,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolvedCommit<V> {
    pub local_commit: ClientCommit<V>,
    pub outcome: DurableOutcome<V>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ConflictResolution>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ConflictResolution {
    KeepServer,
    KeepLocal { replacement: CommitIdentity },
    Merge { replacement: CommitIdentity },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictResolutionRequest<V> {
    KeepServer,
    KeepLocal { replacement: ClientCommit<V> },
    Merge { replacement: ClientCommit<V> },
}

/// Complete durable state owned by one partition/client/scope tuple.
///
/// Optimistic state is deliberately derived from `confirmed` plus `outbox` so
/// persistence never has to keep a mirrored projection synchronized.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableClientState<V> {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub requested_scope: ScopeIdentity,
    pub subscription_revision: u64,
    pub revision: u64,
    pub confirmed: BTreeMap<RecordId, RecordState<V>>,
    pub outbox: Vec<ClientCommit<V>>,
    pub outcomes: Vec<ResolvedCommit<V>>,
    pub checkpoint: Option<Checkpoint>,
    pub id_map: BTreeMap<RecordId, RecordId>,
}

impl<V> DurableClientState<V> {
    pub fn empty(
        partition_id: PartitionId,
        client_id: ClientId,
        requested_scope: ScopeIdentity,
        subscription_revision: u64,
    ) -> Self {
        Self {
            partition_id,
            client_id,
            requested_scope,
            subscription_revision,
            revision: 0,
            confirmed: BTreeMap::new(),
            outbox: Vec::new(),
            outcomes: Vec::new(),
            checkpoint: None,
            id_map: BTreeMap::new(),
        }
    }
}

impl<V: Eq> DurableClientState<V> {
    pub fn validate(&self) -> Result<(), ClientError> {
        let mut identities = BTreeSet::new();
        for commit in &self.outbox {
            validate_commit(commit)?;
            if !identities.insert(commit.identity.client_commit_id.clone()) {
                return Err(ClientError::DuplicateCommitId(
                    commit.identity.client_commit_id.clone(),
                ));
            }
        }
        for resolved in &self.outcomes {
            validate_commit(&resolved.local_commit)?;
            if resolved.outcome.identity() != &resolved.local_commit.identity {
                return Err(ClientError::OutcomeIdentityMismatch);
            }
            let commit_id = resolved.local_commit.identity.client_commit_id.clone();
            if !identities.insert(commit_id.clone()) {
                return Err(ClientError::DuplicateCommitId(commit_id));
            }
        }
        validate_conflict_resolutions(self)?;
        validate_checkpoint(self)?;
        validate_id_map(&self.id_map)?;
        validate_no_mapped_local_ids(self)
    }
}

fn validate_conflict_resolutions<V: Eq>(state: &DurableClientState<V>) -> Result<(), ClientError> {
    for resolved in &state.outcomes {
        let Some(resolution) = &resolved.resolution else {
            continue;
        };
        if !matches!(&resolved.outcome, DurableOutcome::Conflict { .. }) {
            return Err(ClientError::InvalidConflictResolution);
        }
        let replacement = match resolution {
            ConflictResolution::KeepServer => continue,
            ConflictResolution::KeepLocal { replacement }
            | ConflictResolution::Merge { replacement } => replacement,
        };
        if replacement == &resolved.local_commit.identity
            || (!state
                .outbox
                .iter()
                .any(|commit| &commit.identity == replacement)
                && !state
                    .outcomes
                    .iter()
                    .any(|outcome| &outcome.local_commit.identity == replacement))
        {
            return Err(ClientError::InvalidConflictResolution);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimisticRecord<V> {
    Present {
        value: V,
        base_version: BaseVersion,
        reference: Option<RecordId>,
    },
    Deleted {
        base_version: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTransition<V> {
    base_revision: u64,
    state: DurableClientState<V>,
}

impl<V> PreparedTransition<V> {
    /// The complete state an adapter must atomically persist before install.
    pub fn state(&self) -> &DurableClientState<V> {
        &self.state
    }

    pub fn into_state(self) -> DurableClientState<V> {
        self.state
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    EmptyCommit,
    DuplicateOperation(RecordId),
    DuplicateCommitId(ClientCommitId),
    UnknownCommit(ClientCommitId),
    UnknownConflict(ClientCommitId),
    ConflictAlreadyResolved(ClientCommitId),
    InvalidConflictResolution,
    OutcomeIdentityMismatch,
    EnvelopeMismatch,
    InvalidPullFraming,
    CheckpointMismatch,
    ScopeMismatch,
    CursorEpochMismatch,
    CursorWouldMoveBackward,
    CommitSequenceGap,
    DuplicateSnapshotRecord(RecordId),
    AbsentConfirmedRecord(RecordId),
    InvalidIdMapping(IdMapping),
    IdMappingCollision(RecordId),
    MappedLocalIdRemains(RecordId),
    RevisionExhausted,
    StaleTransition { expected: u64, actual: u64 },
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyCommit => "commit has no operations",
            Self::DuplicateOperation(_) => "commit repeats a record operation",
            Self::DuplicateCommitId(_) => "commit identity is already durable",
            Self::UnknownCommit(_) => "push outcome has no matching pending commit",
            Self::UnknownConflict(_) => "conflict resolution has no matching durable conflict",
            Self::ConflictAlreadyResolved(_) => "durable conflict is already resolved",
            Self::InvalidConflictResolution => "conflict resolution is invalid",
            Self::OutcomeIdentityMismatch => "push outcome identity does not match",
            Self::EnvelopeMismatch => "protocol envelope does not match this client",
            Self::InvalidPullFraming => "pull response is not a complete batch",
            Self::CheckpointMismatch => "pull checkpoint does not match",
            Self::ScopeMismatch => "pull scope does not match this client",
            Self::CursorEpochMismatch => "pull cursor epoch does not match",
            Self::CursorWouldMoveBackward => "pull cursor would move backward",
            Self::CommitSequenceGap => "pull commit sequence is incomplete",
            Self::DuplicateSnapshotRecord(_) => "reset snapshot repeats a record",
            Self::AbsentConfirmedRecord(_) => "confirmed state contains an absent record",
            Self::InvalidIdMapping(_) => "record ID mapping is invalid",
            Self::IdMappingCollision(_) => "record ID mapping collides with canonical state",
            Self::MappedLocalIdRemains(_) => "mapped local record ID remains in durable state",
            Self::RevisionExhausted => "client state revision is exhausted",
            Self::StaleTransition { .. } => "prepared client transition is stale",
        };
        formatter.write_str(message)
    }
}

impl Error for ClientError {}

pub struct ClientRuntime<V> {
    state: DurableClientState<V>,
}

impl<V: Clone + Eq> ClientRuntime<V> {
    pub fn open(state: DurableClientState<V>) -> Result<Self, ClientError> {
        state.validate()?;
        Ok(Self { state })
    }

    pub fn state(&self) -> &DurableClientState<V> {
        &self.state
    }

    pub fn pending_commits(&self) -> impl Iterator<Item = &ClientCommit<V>> {
        self.state.outbox.iter()
    }

    pub fn optimistic(&self) -> BTreeMap<RecordId, OptimisticRecord<V>> {
        optimistic_projection(&self.state)
    }

    pub fn prepare_enqueue(
        &self,
        commit: ClientCommit<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        validate_commit(&commit)?;
        let commit_id = &commit.identity.client_commit_id;
        if identity_exists(&self.state, commit_id) {
            return Err(ClientError::DuplicateCommitId(commit_id.clone()));
        }
        let mut next = self.state.clone();
        next.outbox.push(commit);
        self.prepare(next)
    }

    pub fn prepare_push_response(
        &self,
        response: PushResponse<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if response.schema_version != SchemaVersion::V1
            || response.partition_id != self.state.partition_id
            || response.client_id != self.state.client_id
        {
            return Err(ClientError::EnvelopeMismatch);
        }
        let identity = response.outcome.identity();
        let commit_id = &identity.client_commit_id;
        if let Some(resolved) = self
            .state
            .outcomes
            .iter()
            .find(|resolved| resolved.local_commit.identity.client_commit_id == *commit_id)
        {
            if resolved.outcome == response.outcome {
                return self.prepare(self.state.clone());
            }
            return Err(ClientError::OutcomeIdentityMismatch);
        }
        let Some(index) = self
            .state
            .outbox
            .iter()
            .position(|commit| commit.identity == *identity)
        else {
            return Err(ClientError::UnknownCommit(commit_id.clone()));
        };

        let mut next = self.state.clone();
        let local_commit = next.outbox.remove(index);
        if let DurableOutcome::Accepted { id_mappings, .. } = &response.outcome {
            for mapping in id_mappings {
                apply_id_mapping(&mut next, mapping)?;
            }
        }
        let mut local_commit = local_commit;
        for mapping in accepted_mappings(&response.outcome) {
            remap_commit(&mut local_commit, mapping);
        }
        next.outcomes.push(ResolvedCommit {
            local_commit,
            outcome: response.outcome,
            resolution: None,
        });
        self.prepare(next)
    }

    pub fn prepare_resolve_conflict(
        &self,
        conflicted_commit_id: &ClientCommitId,
        request: ConflictResolutionRequest<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        let Some(index) = self.state.outcomes.iter().position(|resolved| {
            resolved.local_commit.identity.client_commit_id == *conflicted_commit_id
                && matches!(&resolved.outcome, DurableOutcome::Conflict { .. })
        }) else {
            return Err(ClientError::UnknownConflict(conflicted_commit_id.clone()));
        };
        if let Some(resolution) = &self.state.outcomes[index].resolution {
            if resolution_request_matches(self.state(), resolution, &request) {
                return self.prepare(self.state.clone());
            }
            return Err(ClientError::ConflictAlreadyResolved(
                conflicted_commit_id.clone(),
            ));
        }

        let (record_id, authoritative) = match &self.state.outcomes[index].outcome {
            DurableOutcome::Conflict {
                record_id,
                authoritative,
                ..
            } => (record_id, authoritative),
            DurableOutcome::Accepted { .. } | DurableOutcome::Rejected { .. } => {
                unreachable!("the conflict lookup matched only conflict outcomes")
            }
        };

        let mut next = self.state.clone();
        match request {
            ConflictResolutionRequest::KeepServer => {
                next.outcomes[index].resolution = Some(ConflictResolution::KeepServer);
            }
            ConflictResolutionRequest::KeepLocal { replacement } => {
                validate_replacement_identity(&self.state, &replacement)?;
                if !same_intent(&self.state.outcomes[index].local_commit, &replacement)
                    || !commit_matches_authoritative_base(&replacement, record_id, authoritative)
                {
                    return Err(ClientError::InvalidConflictResolution);
                }
                next.outcomes[index].resolution = Some(ConflictResolution::KeepLocal {
                    replacement: replacement.identity.clone(),
                });
                next.outbox.push(replacement);
            }
            ConflictResolutionRequest::Merge { replacement } => {
                validate_replacement_identity(&self.state, &replacement)?;
                if !commit_matches_authoritative_base(&replacement, record_id, authoritative) {
                    return Err(ClientError::InvalidConflictResolution);
                }
                next.outcomes[index].resolution = Some(ConflictResolution::Merge {
                    replacement: replacement.identity.clone(),
                });
                next.outbox.push(replacement);
            }
        }
        self.prepare(next)
    }

    pub fn prepare_pull_response(
        &self,
        response: PullResponse<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        match response {
            PullResponse::Batch(batch) => self.prepare_pull_batch(batch),
            PullResponse::Reset(snapshot) => self.prepare_reset(snapshot),
        }
    }

    /// Installs a state only after its complete replacement was persisted.
    pub fn install(&mut self, prepared: PreparedTransition<V>) -> Result<(), ClientError> {
        if prepared.base_revision != self.state.revision {
            return Err(ClientError::StaleTransition {
                expected: prepared.base_revision,
                actual: self.state.revision,
            });
        }
        self.state = prepared.state;
        Ok(())
    }

    fn prepare_pull_batch(
        &self,
        batch: PullBatch<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        let (checkpoint, commits) = validate_pull_batch(&self.state, &batch)?;
        if let Some(current) = &self.state.checkpoint
            && checkpoint.cursor.sequence <= current.cursor.sequence
        {
            return self.prepare(self.state.clone());
        }
        let mut next = self.state.clone();
        for commit in commits {
            for record in &commit.records {
                match &record.state {
                    RecordState::Absent => {
                        next.confirmed.remove(&record.record_id);
                    }
                    state => {
                        next.confirmed
                            .insert(record.record_id.clone(), state.clone());
                    }
                }
            }
        }
        next.checkpoint = Some(checkpoint);
        self.prepare(next)
    }

    fn prepare_reset(
        &self,
        snapshot: ResetSnapshot<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        validate_scope(&self.state, &snapshot.checkpoint)?;
        let mut confirmed = BTreeMap::new();
        let mut record_ids = BTreeSet::new();
        for record in snapshot.records {
            if !record_ids.insert(record.record_id.clone()) {
                return Err(ClientError::DuplicateSnapshotRecord(record.record_id));
            }
            if record.state != RecordState::Absent {
                confirmed.insert(record.record_id, record.state);
            }
        }
        let mut next = self.state.clone();
        next.confirmed = confirmed;
        next.checkpoint = Some(snapshot.checkpoint);
        self.prepare(next)
    }

    fn prepare(
        &self,
        mut next: DurableClientState<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if next != self.state {
            next.revision = self
                .state
                .revision
                .checked_add(1)
                .ok_or(ClientError::RevisionExhausted)?;
        }
        next.validate()?;
        Ok(PreparedTransition {
            base_revision: self.state.revision,
            state: next,
        })
    }
}

fn validate_commit<V>(commit: &ClientCommit<V>) -> Result<(), ClientError> {
    if commit.operations.is_empty() {
        return Err(ClientError::EmptyCommit);
    }
    let mut records = BTreeSet::new();
    for operation in &commit.operations {
        if !records.insert(operation.record_id().clone()) {
            return Err(ClientError::DuplicateOperation(
                operation.record_id().clone(),
            ));
        }
    }
    Ok(())
}

fn validate_replacement_identity<V: Eq>(
    state: &DurableClientState<V>,
    replacement: &ClientCommit<V>,
) -> Result<(), ClientError> {
    validate_commit(replacement)?;
    let commit_id = &replacement.identity.client_commit_id;
    if identity_exists(state, commit_id) {
        return Err(ClientError::DuplicateCommitId(commit_id.clone()));
    }
    Ok(())
}

fn resolution_request_matches<V: Eq>(
    state: &DurableClientState<V>,
    resolution: &ConflictResolution,
    request: &ConflictResolutionRequest<V>,
) -> bool {
    match (resolution, request) {
        (ConflictResolution::KeepServer, ConflictResolutionRequest::KeepServer) => true,
        (
            ConflictResolution::KeepLocal {
                replacement: identity,
            },
            ConflictResolutionRequest::KeepLocal { replacement },
        )
        | (
            ConflictResolution::Merge {
                replacement: identity,
            },
            ConflictResolutionRequest::Merge { replacement },
        ) => {
            identity == &replacement.identity
                && state
                    .outbox
                    .iter()
                    .chain(state.outcomes.iter().map(|resolved| &resolved.local_commit))
                    .any(|commit| commit == replacement)
        }
        (
            ConflictResolution::KeepServer
            | ConflictResolution::KeepLocal { .. }
            | ConflictResolution::Merge { .. },
            ConflictResolutionRequest::KeepServer
            | ConflictResolutionRequest::KeepLocal { .. }
            | ConflictResolutionRequest::Merge { .. },
        ) => false,
    }
}

fn same_intent<V: Eq>(original: &ClientCommit<V>, replacement: &ClientCommit<V>) -> bool {
    original.operations.len() == replacement.operations.len()
        && original
            .operations
            .iter()
            .zip(&replacement.operations)
            .all(|(original, replacement)| match (original, replacement) {
                (
                    Operation::Upsert {
                        record_id: original_id,
                        value: original_value,
                        reference: original_reference,
                        ..
                    },
                    Operation::Upsert {
                        record_id: replacement_id,
                        value: replacement_value,
                        reference: replacement_reference,
                        ..
                    },
                ) => {
                    original_id == replacement_id
                        && original_value == replacement_value
                        && original_reference == replacement_reference
                }
                (
                    Operation::Delete {
                        record_id: original_id,
                        ..
                    },
                    Operation::Delete {
                        record_id: replacement_id,
                        ..
                    },
                ) => original_id == replacement_id,
                (Operation::Upsert { .. }, Operation::Delete { .. })
                | (Operation::Delete { .. }, Operation::Upsert { .. }) => false,
            })
}

fn commit_matches_authoritative_base<V>(
    commit: &ClientCommit<V>,
    conflict_record_id: &RecordId,
    authoritative: &RecordState<V>,
) -> bool {
    commit
        .operations
        .iter()
        .find(|operation| operation.record_id() == conflict_record_id)
        .is_some_and(|operation| match (operation, authoritative) {
            (
                Operation::Upsert {
                    base_version: BaseVersion::Absent,
                    ..
                },
                RecordState::Absent,
            ) => true,
            (
                Operation::Upsert {
                    base_version: BaseVersion::Exact(base),
                    ..
                },
                RecordState::Present { version, .. } | RecordState::Tombstone { version },
            ) => base == version,
            (Operation::Delete { base_version, .. }, RecordState::Present { version, .. }) => {
                base_version == version
            }
            (
                Operation::Upsert { .. },
                RecordState::Absent | RecordState::Present { .. } | RecordState::Tombstone { .. },
            )
            | (Operation::Delete { .. }, RecordState::Absent | RecordState::Tombstone { .. }) => {
                false
            }
        })
}

fn validate_checkpoint<V: Eq>(state: &DurableClientState<V>) -> Result<(), ClientError> {
    for (record_id, record) in &state.confirmed {
        if record == &RecordState::Absent {
            return Err(ClientError::AbsentConfirmedRecord(record_id.clone()));
        }
    }
    if let Some(checkpoint) = &state.checkpoint {
        if checkpoint.token.0.is_empty() || checkpoint.cursor.epoch == 0 {
            return Err(ClientError::CheckpointMismatch);
        }
        validate_scope(state, checkpoint)?;
    }
    Ok(())
}

fn validate_scope<V>(
    state: &DurableClientState<V>,
    checkpoint: &Checkpoint,
) -> Result<(), ClientError> {
    if checkpoint.scope.identity != state.requested_scope
        || checkpoint.scope.subscription_revision != state.subscription_revision
    {
        return Err(ClientError::ScopeMismatch);
    }
    Ok(())
}

fn validate_id_map(id_map: &BTreeMap<RecordId, RecordId>) -> Result<(), ClientError> {
    for (local, canonical) in id_map {
        if local == canonical || id_map.contains_key(canonical) {
            return Err(ClientError::InvalidIdMapping(IdMapping {
                local_id: local.clone(),
                canonical_id: canonical.clone(),
            }));
        }
    }
    Ok(())
}

fn validate_no_mapped_local_ids<V>(state: &DurableClientState<V>) -> Result<(), ClientError> {
    for local_id in state.id_map.keys() {
        let confirmed_contains = state.confirmed.iter().any(|(record_id, record)| {
            record_id == local_id
                || matches!(record, RecordState::Present { reference: Some(reference), .. } if reference == local_id)
        });
        let outbox_contains = state
            .outbox
            .iter()
            .any(|commit| commit_contains_id(commit, local_id));
        let outcome_contains = state.outcomes.iter().any(|resolved| {
            commit_contains_id(&resolved.local_commit, local_id)
                || outcome_contains_id(&resolved.outcome, local_id)
        });
        if confirmed_contains || outbox_contains || outcome_contains {
            return Err(ClientError::MappedLocalIdRemains(local_id.clone()));
        }
    }
    Ok(())
}

fn commit_contains_id<V>(commit: &ClientCommit<V>, record_id: &RecordId) -> bool {
    commit.operations.iter().any(|operation| match operation {
        Operation::Upsert {
            record_id: operation_id,
            reference,
            ..
        } => operation_id == record_id || reference.as_ref() == Some(record_id),
        Operation::Delete {
            record_id: operation_id,
            ..
        } => operation_id == record_id,
    })
}

fn outcome_contains_id<V>(outcome: &DurableOutcome<V>, record_id: &RecordId) -> bool {
    match outcome {
        DurableOutcome::Conflict {
            record_id: conflict_id,
            authoritative,
            ..
        } => {
            conflict_id == record_id
                || matches!(authoritative, RecordState::Present { reference: Some(reference), .. } if reference == record_id)
        }
        DurableOutcome::Accepted { .. } | DurableOutcome::Rejected { .. } => false,
    }
}

fn validate_pull_batch<'a, V: Eq>(
    state: &DurableClientState<V>,
    batch: &'a PullBatch<V>,
) -> Result<(Checkpoint, Vec<&'a surrealdb_sync_protocol::PullCommit<V>>), ClientError> {
    let Some(PullFrame::Start { checkpoint: start }) = batch.frames.first() else {
        return Err(ClientError::InvalidPullFraming);
    };
    let Some(PullFrame::End { checkpoint: end }) = batch.frames.last() else {
        return Err(ClientError::InvalidPullFraming);
    };
    if start != end {
        return Err(ClientError::CheckpointMismatch);
    }
    validate_scope(state, end)?;
    if let Some(current) = &state.checkpoint {
        if end.cursor.epoch != current.cursor.epoch {
            return Err(ClientError::CursorEpochMismatch);
        }
        if end.cursor.sequence < current.cursor.sequence {
            return Err(ClientError::CursorWouldMoveBackward);
        }
    }

    let mut commits = Vec::new();
    for frame in &batch.frames[1..batch.frames.len() - 1] {
        let PullFrame::Commit { checkpoint, commit } = frame else {
            return Err(ClientError::InvalidPullFraming);
        };
        if checkpoint != &end.token {
            return Err(ClientError::CheckpointMismatch);
        }
        commits.push(commit);
    }

    for pair in commits.windows(2) {
        if pair[1].sequence != pair[0].sequence.saturating_add(1) {
            return Err(ClientError::CommitSequenceGap);
        }
    }
    if commits
        .last()
        .is_some_and(|commit| commit.sequence != end.cursor.sequence)
    {
        return Err(ClientError::CommitSequenceGap);
    }

    let current_sequence = state
        .checkpoint
        .as_ref()
        .filter(|current| current.cursor.epoch == end.cursor.epoch)
        .map_or(0, |current| current.cursor.sequence);
    if end.cursor.sequence > current_sequence {
        let mut expected = current_sequence + 1;
        for commit in &commits {
            if commit.sequence != expected {
                return Err(ClientError::CommitSequenceGap);
            }
            expected += 1;
        }
        if expected - 1 != end.cursor.sequence {
            return Err(ClientError::CommitSequenceGap);
        }
    }
    Ok((end.clone(), commits))
}

fn identity_exists<V>(state: &DurableClientState<V>, commit_id: &ClientCommitId) -> bool {
    state
        .outbox
        .iter()
        .any(|commit| commit.identity.client_commit_id == *commit_id)
        || state
            .outcomes
            .iter()
            .any(|resolved| resolved.local_commit.identity.client_commit_id == *commit_id)
}

pub fn optimistic_projection<V: Clone>(
    state: &DurableClientState<V>,
) -> BTreeMap<RecordId, OptimisticRecord<V>> {
    let mut projection = state
        .confirmed
        .iter()
        .filter_map(|(record_id, state)| match state {
            RecordState::Absent => None,
            RecordState::Present {
                value,
                version,
                reference,
            } => Some((
                record_id.clone(),
                OptimisticRecord::Present {
                    value: value.clone(),
                    base_version: BaseVersion::Exact(*version),
                    reference: reference.clone(),
                },
            )),
            RecordState::Tombstone { version } => Some((
                record_id.clone(),
                OptimisticRecord::Deleted {
                    base_version: *version,
                },
            )),
        })
        .collect::<BTreeMap<_, _>>();
    let applied_sequence = state
        .checkpoint
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.cursor.sequence);
    let accepted_not_pulled =
        state
            .outcomes
            .iter()
            .filter_map(|resolved| match &resolved.outcome {
                DurableOutcome::Accepted { sequence, .. } if *sequence > applied_sequence => {
                    Some(&resolved.local_commit)
                }
                DurableOutcome::Accepted { .. }
                | DurableOutcome::Conflict { .. }
                | DurableOutcome::Rejected { .. } => None,
            });
    for operation in accepted_not_pulled
        .chain(&state.outbox)
        .flat_map(|commit| &commit.operations)
    {
        match operation {
            Operation::Upsert {
                record_id,
                base_version,
                value,
                reference,
            } => {
                projection.insert(
                    record_id.clone(),
                    OptimisticRecord::Present {
                        value: value.clone(),
                        base_version: *base_version,
                        reference: reference.clone(),
                    },
                );
            }
            Operation::Delete {
                record_id,
                base_version,
            } => {
                projection.insert(
                    record_id.clone(),
                    OptimisticRecord::Deleted {
                        base_version: *base_version,
                    },
                );
            }
        }
    }
    projection
}

fn accepted_mappings<V>(outcome: &DurableOutcome<V>) -> &[IdMapping] {
    match outcome {
        DurableOutcome::Accepted { id_mappings, .. } => id_mappings,
        DurableOutcome::Conflict { .. } | DurableOutcome::Rejected { .. } => &[],
    }
}

fn apply_id_mapping<V: Clone + Eq>(
    state: &mut DurableClientState<V>,
    mapping: &IdMapping,
) -> Result<(), ClientError> {
    if mapping.local_id == mapping.canonical_id || state.id_map.contains_key(&mapping.canonical_id)
    {
        return Err(ClientError::InvalidIdMapping(mapping.clone()));
    }
    if let Some(existing) = state.id_map.get(&mapping.local_id) {
        if existing == &mapping.canonical_id {
            return Ok(());
        }
        return Err(ClientError::InvalidIdMapping(mapping.clone()));
    }
    if let Some(local_state) = state.confirmed.remove(&mapping.local_id) {
        if let Some(canonical_state) = state.confirmed.get(&mapping.canonical_id) {
            if canonical_state != &local_state {
                return Err(ClientError::IdMappingCollision(
                    mapping.canonical_id.clone(),
                ));
            }
        } else {
            state
                .confirmed
                .insert(mapping.canonical_id.clone(), local_state);
        }
    }
    remap_state_references(state, mapping);
    for commit in &mut state.outbox {
        remap_commit(commit, mapping);
    }
    for resolved in &mut state.outcomes {
        remap_commit(&mut resolved.local_commit, mapping);
        remap_outcome(&mut resolved.outcome, mapping);
    }
    for canonical in state.id_map.values_mut() {
        if canonical == &mapping.local_id {
            *canonical = mapping.canonical_id.clone();
        }
    }
    state
        .id_map
        .insert(mapping.local_id.clone(), mapping.canonical_id.clone());
    Ok(())
}

fn remap_state_references<V>(state: &mut DurableClientState<V>, mapping: &IdMapping) {
    for record in state.confirmed.values_mut() {
        if let RecordState::Present {
            reference: Some(reference),
            ..
        } = record
        {
            remap_record_id(reference, mapping);
        }
    }
}

fn remap_commit<V>(commit: &mut ClientCommit<V>, mapping: &IdMapping) {
    for operation in &mut commit.operations {
        match operation {
            Operation::Upsert {
                record_id,
                reference,
                ..
            } => {
                remap_record_id(record_id, mapping);
                if let Some(reference) = reference {
                    remap_record_id(reference, mapping);
                }
            }
            Operation::Delete { record_id, .. } => remap_record_id(record_id, mapping),
        }
    }
}

fn remap_outcome<V>(outcome: &mut DurableOutcome<V>, mapping: &IdMapping) {
    if let DurableOutcome::Conflict {
        record_id,
        authoritative,
        ..
    } = outcome
    {
        remap_record_id(record_id, mapping);
        if let RecordState::Present {
            reference: Some(reference),
            ..
        } = authoritative
        {
            remap_record_id(reference, mapping);
        }
    }
}

fn remap_record_id(record_id: &mut RecordId, mapping: &IdMapping) {
    if record_id == &mapping.local_id {
        *record_id = mapping.canonical_id.clone();
    }
}
