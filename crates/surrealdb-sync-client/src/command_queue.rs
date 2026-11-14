//! Opt-in local-first single-record command admission. Transmission is a durable boundary.
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
        if single_upsert(c).is_none() || !ids.insert(c.identity.client_commit_id.clone()) {
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
        let Some(id) = single_upsert(&commit).cloned() else {
            return self.prepare_enqueue(commit);
        };
        let mut next = self.state.clone();
        if let Some(index) = next.outbox.iter().position(|c| {
            single_upsert(c) == Some(&id)
                && !next.command_queue.deferred.iter().any(|d| touches(d, &id))
                && !next
                    .command_queue
                    .transmitted
                    .contains(&c.identity.client_commit_id)
        }) {
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
        } else if blocked(&next, &id) || next.command_queue.deferred.iter().any(|c| touches(c, &id))
        {
            if next.command_queue.deferred.len() >= 64
                || next
                    .command_queue
                    .deferred
                    .iter()
                    .filter(|c| touches(c, &id))
                    .count()
                    >= 16
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
            let id = single_upsert(&next.command_queue.deferred[index])
                .ok_or(ClientError::InvalidReadCoverage)?
                .clone();
            if blocked(&next, &id)
                || next.command_queue.deferred[..index]
                    .iter()
                    .any(|c| touches(c, &id))
            {
                index += 1;
                continue;
            }
            let mut commit = next.command_queue.deferred.remove(index);
            let base = match next.confirmed.get(&id) {
                Some(RecordState::Present { version, .. }) => BaseVersion::Exact(*version),
                _ => {
                    next.command_queue.deferred.insert(index, commit);
                    index += 1;
                    continue;
                }
            };
            if let Operation::Upsert { base_version, .. } = &mut commit.operations[0] {
                *base_version = base;
            }
            commit.identity.fingerprint = fingerprint(&next, &commit)?;
            next.outbox.push(commit);
        }
        self.prepare(next)
    }
}
