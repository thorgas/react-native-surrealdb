//! Receipt coverage is installed in the same durable replacement as the validated pull.
use crate::{
    ClientError, ClientRuntime, DurableClientState, PreparedTransition, ReadAcceptedStatusV1,
    ReadSnapshotV1,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use surrealdb_sync_protocol::*;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptState {
    pub receipts: BTreeMap<ClientCommitId, StoredReceipt>,
    pub frontier: Option<VerifiedFrontier>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoredReceipt {
    pub receipt: AcceptanceReceipt,
    pub covered_at: Option<Cursor>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedFrontier {
    pub context: ReceiptContext,
    pub cursor: Cursor,
}
fn valid_context<V>(state: &DurableClientState<V>, context: &ReceiptContext) -> bool {
    !context.authority_incarnation.is_empty()
        && !context.dataset_incarnation.is_empty()
        && context.partition_id == state.partition_id
        && context.client_id == state.client_id
        && context.scope.identity == state.requested_scope
        && context.scope.subscription_revision == state.subscription_revision
}
pub(crate) fn validate<V>(state: &DurableClientState<V>) -> Result<(), ClientError> {
    if let Some(frontier) = &state.receipt_state.frontier
        && (!valid_context(state, &frontier.context)
            || !state
                .checkpoint
                .as_ref()
                .is_some_and(|c| c.cursor == frontier.cursor && c.scope == frontier.context.scope))
    {
        return Err(ClientError::InvalidReadCoverage);
    }
    for (id, stored) in &state.receipt_state.receipts {
        let receipt = &stored.receipt;
        let outcome = state
            .outcomes
            .iter()
            .find(|r| r.local_commit.identity == receipt.identity)
            .ok_or(ClientError::InvalidReadCoverage)?;
        if id != &receipt.identity.client_commit_id
            || !valid_context(state, &receipt.context)
            || receipt.cursor.epoch == 0
            || !matches!(&outcome.outcome, DurableOutcome::Accepted { sequence, .. } if *sequence == receipt.cursor.sequence && *sequence > 0)
            || stored.covered_at.is_some_and(|c| {
                c.epoch != receipt.cursor.epoch || c.sequence < receipt.cursor.sequence
            })
        {
            return Err(ClientError::InvalidReadCoverage);
        }
    }
    Ok(())
}
fn cover<V>(state: &mut DurableClientState<V>) {
    if let Some(frontier) = &state.receipt_state.frontier {
        for stored in state.receipt_state.receipts.values_mut() {
            if stored.covered_at.is_none()
                && stored.receipt.context == frontier.context
                && stored.receipt.cursor.epoch == frontier.cursor.epoch
                && stored.receipt.cursor.sequence <= frontier.cursor.sequence
            {
                stored.covered_at = Some(frontier.cursor);
            }
        }
    }
}
pub fn covered<V>(state: &DurableClientState<V>, identity: &CommitIdentity) -> bool {
    state
        .receipt_state
        .receipts
        .get(&identity.client_commit_id)
        .is_some_and(|r| r.receipt.identity == *identity && r.covered_at.is_some())
}
impl<V: Clone + Eq> ClientRuntime<V> {
    pub fn prepare_push_response_v2(
        &self,
        response: PushResponseV2<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        validate_receipt(&response).map_err(|_| ClientError::OutcomeIdentityMismatch)?;
        if let Some(receipt) = &response.receipt {
            if !valid_context(&self.state, &receipt.context) {
                return Err(ClientError::ScopeMismatch);
            }
            if let Some(stored) = self
                .state
                .receipt_state
                .receipts
                .get(&receipt.identity.client_commit_id)
                && stored.receipt != *receipt
            {
                return Err(ClientError::OutcomeIdentityMismatch);
            }
        }
        let mut next = self.prepare_push_response(response.response)?.state;
        if let Some(receipt) = response.receipt {
            next.receipt_state
                .receipts
                .entry(receipt.identity.client_commit_id.clone())
                .or_insert(StoredReceipt {
                    receipt,
                    covered_at: None,
                });
        }
        cover(&mut next);
        // prepare_push_response may already increment; prepare owns the single revision boundary.
        next.revision = self.state.revision;
        self.prepare(next)
    }
    pub fn prepare_pull_response_v2(
        &self,
        response: PullResponseV2<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if !valid_context(&self.state, &response.context) {
            return Err(ClientError::ScopeMismatch);
        }
        let mut next = self.prepare_pull_response(response.response)?.state;
        let checkpoint = next
            .checkpoint
            .as_ref()
            .ok_or(ClientError::InvalidReadCoverage)?;
        if checkpoint.scope != response.context.scope {
            return Err(ClientError::ScopeMismatch);
        }
        if self.state.receipt_state.frontier.as_ref().is_some_and(|f| {
            f.context.authority_incarnation != response.context.authority_incarnation
                || f.context.dataset_incarnation != response.context.dataset_incarnation
        }) {
            return Err(ClientError::NeedsNewScopeJournal);
        }
        next.receipt_state.frontier = Some(VerifiedFrontier {
            context: response.context,
            cursor: checkpoint.cursor,
        });
        cover(&mut next);
        next.revision = self.state.revision;
        self.prepare(next)
    }
    /// V1 snapshots never emit the new status. Callers must select this contract explicitly.
    pub fn read_snapshot_v2(&self) -> ReadSnapshotV1<V> {
        snapshot_v2(&self.state)
    }
    pub fn prepared_read_batch_v2(
        &self,
        prepared: &PreparedTransition<V>,
    ) -> Option<crate::CommittedReadBatchV1<V>> {
        crate::read::committed_batch_version(&self.state, prepared.state(), true)
    }
}
pub(crate) fn snapshot_v2<V: Clone + Eq>(state: &DurableClientState<V>) -> ReadSnapshotV1<V> {
    let mut snapshot = crate::read::snapshot(state);
    snapshot.protocol_version = 2;
    for row in snapshot.rows.values_mut() {
        for accepted in &mut row.accepted {
            if covered(state, &accepted.identity) {
                accepted.status = ReadAcceptedStatusV1::CoveredByVerifiedPull;
            } else if state
                .receipt_state
                .receipts
                .contains_key(&accepted.identity.client_commit_id)
                && accepted.status != ReadAcceptedStatusV1::CoveredByAuthorizedReset
            {
                accepted.status = ReadAcceptedStatusV1::AwaitingVerifiedPull;
            }
        }
    }
    snapshot
}
