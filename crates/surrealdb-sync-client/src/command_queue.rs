//! Opt-in local-first command admission. Aggregates are indivisible; transmission is durable.
use crate::{ClientError, ClientRuntime, DurableClientState, PreparedTransition};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use surrealdb_sync_protocol::*;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandQueue<V> {
    pub transmitted: BTreeSet<ClientCommitId>,
    pub deferred: Vec<ClientCommit<V>>,
}
impl<V> Default for CommandQueue<V> {
    fn default() -> Self {
        Self {
            transmitted: BTreeSet::new(),
            deferred: vec![],
        }
    }
}
fn single_upsert<V>(c: &ClientCommit<V>) -> Option<&RecordId> {
    match c.operations.as_slice() {
        [Operation::Upsert { record_id, .. }] => Some(record_id),
        _ => None,
    }
}
fn touches<V>(c: &ClientCommit<V>, id: &RecordId) -> bool {
    c.operations.iter().any(|op| op.record_id() == id)
}
fn blocked<V>(s: &DurableClientState<V>, id: &RecordId) -> bool {
    s.outbox.iter().any(|c| touches(c, id))
        || s.outcomes.iter().any(|r| {
            touches(&r.local_commit, id)
                && match &r.outcome {
                    DurableOutcome::Accepted { .. } => {
                        !r.covered_by_reset && !crate::receipt::covered(s, &r.local_commit.identity)
                    }
                    DurableOutcome::Conflict { .. } => r.resolution.is_none(),
                    DurableOutcome::Rejected { .. } => true,
                }
        })
}
fn overlaps<V>(left: &ClientCommit<V>, right: &ClientCommit<V>) -> bool {
    left.operations
        .iter()
        .any(|op| touches(right, op.record_id()))
}
fn waiting<V>(s: &DurableClientState<V>, commit: &ClientCommit<V>) -> bool {
    commit
        .operations
        .iter()
        .any(|op| blocked(s, op.record_id()))
}
pub(crate) fn validate<V: Eq>(s: &DurableClientState<V>) -> Result<(), ClientError> {
    if s.command_queue.deferred.len() > 64 {
        return Err(ClientError::InvalidReadCoverage);
    }
    let mut ids = s
        .outbox
        .iter()
        .map(|c| c.identity.client_commit_id.clone())
        .chain(
            s.outcomes
                .iter()
                .map(|c| c.local_commit.identity.client_commit_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    for c in &s.command_queue.deferred {
        crate::validate_commit(c)?;
        if !ids.insert(c.identity.client_commit_id.clone())
            || c.operations.iter().any(|op| {
                s.command_queue
                    .deferred
                    .iter()
                    .filter(|other| touches(other, op.record_id()))
                    .count()
                    > 16
            })
        {
            return Err(ClientError::InvalidReadCoverage);
        }
    }
    if s.command_queue.transmitted.iter().any(|id| {
        !s.outbox.iter().any(|c| c.identity.client_commit_id == *id)
            && !s
                .outcomes
                .iter()
                .any(|r| r.local_commit.identity.client_commit_id == *id)
    }) {
        return Err(ClientError::InvalidReadCoverage);
    }
    Ok(())
}
impl<V: Clone + Eq> ClientRuntime<V> {
    pub fn prepare_with_successors(
        &self,
        prepared: PreparedTransition<V>,
        fingerprint: impl Fn(
            &DurableClientState<V>,
            &ClientCommit<V>,
        ) -> Result<Fingerprint, ClientError>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        let temporary = ClientRuntime::open(prepared.state)?;
        let mut next = temporary.prepare_admit_successors(fingerprint)?.state;
        next.revision = self.state.revision;
        self.prepare(next)
    }

    pub fn prepare_mark_transmitted(
        &self,
        identity: &CommitIdentity,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if !self.state.outbox.iter().any(|c| c.identity == *identity) {
            return Err(ClientError::OutcomeIdentityMismatch);
        }
        let mut next = self.state.clone();
        next.command_queue
            .transmitted
            .insert(identity.client_commit_id.clone());
        self.prepare(next)
    }
    pub fn prepare_enqueue_latest(
        &self,
        mut commit: ClientCommit<V>,
        fingerprint: impl Fn(
            &DurableClientState<V>,
            &ClientCommit<V>,
        ) -> Result<Fingerprint, ClientError>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        crate::validate_commit(&commit)?;
        if crate::identity_exists(&self.state, &commit.identity.client_commit_id)
            || self
                .state
                .command_queue
                .deferred
                .iter()
                .any(|c| c.identity.client_commit_id == commit.identity.client_commit_id)
        {
            return Err(ClientError::DuplicateCommitId(
                commit.identity.client_commit_id,
            ));
        }
        let mut next = self.state.clone();
        // Only unsent one-record upserts may coalesce. Never merge or partially replace aggregates.
        let coalescible = single_upsert(&commit).and_then(|id| {
            next.outbox.iter().position(|c| {
                single_upsert(c) == Some(id)
                    && !next.command_queue.deferred.iter().any(|d| touches(d, id))
                    && !next
                        .command_queue
                        .transmitted
                        .contains(&c.identity.client_commit_id)
            })
        });
        if let Some(index) = coalescible {
            let previous = &next.outbox[index];
            commit.identity.client_commit_id = previous.identity.client_commit_id.clone();
            if let (
                Operation::Upsert {
                    base_version: old, ..
                },
                Operation::Upsert { base_version, .. },
            ) = (&previous.operations[0], &mut commit.operations[0])
            {
                *base_version = *old;
            }
            commit.identity.fingerprint = fingerprint(&next, &commit)?;
            next.outbox[index] = commit;
        } else if waiting(&next, &commit)
            || next
                .command_queue
                .deferred
                .iter()
                .any(|c| overlaps(c, &commit))
        {
            if next.command_queue.deferred.len() >= 64
                || commit.operations.iter().any(|op| {
                    next.command_queue
                        .deferred
                        .iter()
                        .filter(|c| touches(c, op.record_id()))
                        .count()
                        >= 16
                })
            {
                return Err(ClientError::InvalidReadCoverage);
            }
            commit.identity.fingerprint = fingerprint(&next, &commit)?;
            next.command_queue.deferred.push(commit);
        } else {
            commit.identity.fingerprint = fingerprint(&next, &commit)?;
            next.outbox.push(commit);
        }
        self.prepare(next)
    }
    /// Rebase only a never-transmitted successor, after its predecessor is covered.
    pub fn prepare_admit_successors(
        &self,
        fingerprint: impl Fn(
            &DurableClientState<V>,
            &ClientCommit<V>,
        ) -> Result<Fingerprint, ClientError>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        let mut next = self.state.clone();
        let mut index = 0;
        while index < next.command_queue.deferred.len() {
            let candidate = &next.command_queue.deferred[index];
            if waiting(&next, candidate)
                || next.command_queue.deferred[..index]
                    .iter()
                    .any(|c| overlaps(c, candidate))
            {
                index += 1;
                continue;
            }
            let mut commit = next.command_queue.deferred.remove(index);
            if !rebase(&next, &mut commit) {
                next.command_queue.deferred.insert(index, commit);
                index += 1;
                continue;
            }
            commit.identity.fingerprint = fingerprint(&next, &commit)?;
            next.outbox.push(commit);
        }
        self.prepare(next)
    }
}

/// Rebase a private clone only. A failed child leaves the whole durable aggregate deferred.
/// Preserve explicit new-record absence; updates/deletes may never resurrect tombstones.
fn rebase<V>(state: &DurableClientState<V>, commit: &mut ClientCommit<V>) -> bool {
    let legacy_single_upsert = single_upsert(commit).is_some();
    for operation in &mut commit.operations {
        let confirmed = state.confirmed.get(operation.record_id());
        match operation {
            Operation::Upsert {
                base_version: BaseVersion::Absent,
                ..
            } if !legacy_single_upsert => {
                if !matches!(confirmed, None | Some(RecordState::Absent)) {
                    return false;
                }
            }
            Operation::Upsert { base_version, .. } => match confirmed {
                Some(RecordState::Present { version, .. }) => {
                    *base_version = BaseVersion::Exact(*version)
                }
                _ => return false,
            },
            Operation::Delete { base_version, .. } => match confirmed {
                Some(RecordState::Present { version, .. }) => *base_version = *version,
                _ => return false,
            },
        }
    }
    true
}
