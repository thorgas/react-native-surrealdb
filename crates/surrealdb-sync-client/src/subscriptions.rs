//! Durable membership is independent from v1 confirmed rows and full-scope coverage.
use crate::{ClientError, ClientRuntime, DurableClientState, PreparedTransition};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use surrealdb_sync_protocol::{Cursor, OpaqueCheckpoint, RecordId, ScopeSnapshot, subscription::*};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionSlot {
    pub generation: u64,
    pub plan_fingerprint: String,
    pub scope: ScopeSnapshot,
    pub frontier: Cursor,
    pub view_revision: u64,
    pub coverage: WindowCoverage,
    pub ordered_ids: Vec<RecordId>,
    pub token: OpaqueCheckpoint,
    /// References include epoch and record version, keeping overlapping frontiers consistent.
    pub members: BTreeMap<RecordId, String>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableSubscriptions<V> {
    pub requests: BTreeMap<String, SubscriptionRequest>,
    pub slots: BTreeMap<String, SubscriptionSlot>,
    pub records: BTreeMap<String, SubscriptionRow<V>>,
    pub cancelled: BTreeMap<String, u64>,
    #[serde(default)]
    pub authorization_scope: Option<ScopeSnapshot>,
}
impl<V> Default for DurableSubscriptions<V> {
    fn default() -> Self {
        Self {
            requests: BTreeMap::new(),
            slots: BTreeMap::new(),
            records: BTreeMap::new(),
            cancelled: BTreeMap::new(),
            authorization_scope: None,
        }
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionViewMetadata {
    pub revision: u64,
    pub subscription_id: String,
    pub generation: u64,
    pub plan_fingerprint: String,
    pub scope: ScopeSnapshot,
    pub frontier: Cursor,
    pub view_revision: u64,
    pub coverage: WindowCoverage,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstalledSubscription<V> {
    pub metadata: SubscriptionViewMetadata,
    pub rows: Vec<SubscriptionRow<V>>,
    /// None means authorization/coverage provenance is insufficient to enumerate intent.
    pub pending_outside_window: Option<Vec<RecordId>>,
}
fn invalid() -> ClientError {
    ClientError::InvalidSubscription
}
fn cache_key(epoch: u64, row: &SubscriptionRow<impl Sized>) -> String {
    format!(
        "{epoch}:{}:{}:{}",
        row.version,
        row.record_id.0.len(),
        row.record_id.0
    )
}
fn collect_records<V>(state: &mut DurableSubscriptions<V>) {
    let used = state
        .slots
        .values()
        .flat_map(|slot| slot.members.values().cloned())
        .collect::<BTreeSet<_>>();
    state.records.retain(|key, _| used.contains(key));
}
fn ordered<V>(
    rows: &BTreeMap<RecordId, SubscriptionRow<V>>,
    coverage: &WindowCoverage,
) -> Vec<RecordId> {
    let mut rows = rows.values().collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let comparison = a.sort_key.cmp(&b.sort_key);
        let comparison = match coverage.direction {
            SortDirection::Ascending => comparison,
            SortDirection::Descending => comparison.reverse(),
        };
        comparison.then(a.record_id.cmp(&b.record_id))
    });
    rows.into_iter().map(|row| row.record_id.clone()).collect()
}
pub(crate) fn guard_subscription_scope<V>(
    state: &DurableClientState<V>,
    scope: &ScopeSnapshot,
) -> Result<(), ClientError> {
    let changed = state
        .subscriptions
        .as_ref()
        .and_then(|subscriptions| subscriptions.authorization_scope.as_ref())
        .is_some_and(|previous| previous != scope);
    if changed {
        if !state.outbox.is_empty()
            || state.outcomes.iter().any(|outcome| {
                matches!(
                    outcome.outcome,
                    surrealdb_sync_protocol::DurableOutcome::Conflict { .. }
                ) && outcome.resolution.is_none()
            })
        {
            return Err(ClientError::PendingQuarantineRequired);
        }
        return Err(ClientError::NeedsNewScopeJournal);
    }
    Ok(())
}
impl<V: Eq> DurableSubscriptions<V> {
    pub(crate) fn validate(&self, owner: &DurableClientState<V>) -> Result<(), ClientError> {
        if self.requests.len() > MAX_SUBSCRIPTIONS
            || self.cancelled.len() > MAX_SUBSCRIPTIONS
            || self.slots.len() > MAX_SUBSCRIPTIONS
            || self.records.len() > MAX_SUBSCRIPTIONS * MAX_WINDOW_ROWS
        {
            return Err(invalid());
        }
        for (id, request) in &self.requests {
            if !validate_subscription_request(request)
                || !matches!(request.command, SubscriptionCommand::Start { .. })
                || request.command.identity().0 != id
                || request.partition_id != owner.partition_id
                || request.client_id != owner.client_id
                || request.requested_scope != owner.requested_scope
                || request.subscription_revision != owner.subscription_revision
            {
                return Err(invalid());
            }
        }
        for (id, barrier) in &self.cancelled {
            if !valid_subscription_text(id) || *barrier == 0 {
                return Err(invalid());
            }
        }
        let distinct_ids = self
            .requests
            .keys()
            .chain(self.cancelled.keys())
            .collect::<BTreeSet<_>>();
        if distinct_ids.len() > MAX_SUBSCRIPTIONS {
            return Err(invalid());
        }
        let mut used = BTreeSet::new();
        if let Some(scope) = &self.authorization_scope {
            if scope.identity != owner.requested_scope
                || scope.subscription_revision != owner.subscription_revision
                || owner
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| checkpoint.scope != *scope)
            {
                return Err(invalid());
            }
        } else if !self.slots.is_empty() {
            return Err(invalid());
        }
        for (id, slot) in &self.slots {
            let request = self.requests.get(id).ok_or_else(invalid)?;
            if self.authorization_scope.as_ref() != Some(&slot.scope) {
                return Err(invalid());
            }
            if request.command.identity().1 != slot.generation
                || self
                    .cancelled
                    .get(id)
                    .is_some_and(|barrier| *barrier >= slot.generation)
                || slot.frontier.epoch == 0
                || slot.view_revision == 0
                || slot.scope.identity != owner.requested_scope
                || slot.scope.subscription_revision != owner.subscription_revision
                || !valid_subscription_text(&slot.plan_fingerprint)
                || !valid_subscription_text(&slot.token.0)
                || slot.coverage.limit == 0
                || usize::from(slot.coverage.limit) > MAX_WINDOW_ROWS
                || slot.members.len() > usize::from(slot.coverage.limit)
            {
                return Err(invalid());
            }
            if let SubscriptionCommand::Start {
                definition_id,
                limit,
                ..
            } = &request.command
                && (*definition_id != slot.coverage.definition_id || *limit != slot.coverage.limit)
            {
                return Err(invalid());
            }
            if !valid_subscription_text(&slot.coverage.order_field) {
                return Err(invalid());
            }
            let mut rows = BTreeMap::new();
            for (record_id, key) in &slot.members {
                let row = self.records.get(key).ok_or_else(invalid)?;
                if !valid_subscription_text(&record_id.0)
                    || matches!(&row.sort_key, QueryScalar::Text(text) if !valid_subscription_text(text))
                    || row.record_id != *record_id
                    || cache_key(slot.frontier.epoch, row) != *key
                    || row.sort_key.kind() != slot.coverage.order_kind
                    || row.version == 0
                {
                    return Err(invalid());
                }
                used.insert(key.clone());
                rows.insert(record_id.clone(), row);
            }
            if rows.len() != slot.ordered_ids.len()
                || slot.ordered_ids.iter().collect::<BTreeSet<_>>().len() != rows.len()
                || slot.ordered_ids.iter().any(|id| !rows.contains_key(id))
            {
                return Err(invalid());
            }
            for ids in slot.ordered_ids.windows(2) {
                let a = rows.get(&ids[0]).ok_or_else(invalid)?;
                let b = rows.get(&ids[1]).ok_or_else(invalid)?;
                let mut cmp = a.sort_key.cmp(&b.sort_key);
                if slot.coverage.direction == SortDirection::Descending {
                    cmp = cmp.reverse();
                }
                if cmp.then(a.record_id.cmp(&b.record_id)).is_ge() {
                    return Err(invalid());
                }
            }
        }
        if self.records.keys().any(|key| !used.contains(key)) {
            return Err(invalid());
        }
        Ok(())
    }
}
impl<V: Clone + Eq> ClientRuntime<V> {
    pub fn prepare_start_subscription(
        &self,
        request: SubscriptionRequest,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if !validate_subscription_request(&request)
            || !matches!(request.command, SubscriptionCommand::Start { .. })
            || request.partition_id != self.state.partition_id
            || request.client_id != self.state.client_id
            || request.requested_scope != self.state.requested_scope
            || request.subscription_revision != self.state.subscription_revision
        {
            return Err(invalid());
        }
        let (id, generation) = request.command.identity();
        let mut next = self.state.clone();
        let subscriptions = next
            .subscriptions
            .get_or_insert_with(DurableSubscriptions::default);
        if subscriptions
            .cancelled
            .get(id)
            .is_some_and(|barrier| *barrier >= generation)
        {
            return Err(ClientError::SubscriptionCancelled);
        }
        if let Some(previous) = subscriptions.requests.get(id) {
            if previous == &request {
                return self.prepare(next);
            }
            if previous.command.identity().1 >= generation {
                return Err(invalid());
            }
        }
        if !subscriptions.requests.contains_key(id)
            && !subscriptions.cancelled.contains_key(id)
            && subscriptions
                .requests
                .keys()
                .chain(subscriptions.cancelled.keys())
                .collect::<BTreeSet<_>>()
                .len()
                >= MAX_SUBSCRIPTIONS
        {
            return Err(invalid());
        }
        subscriptions.slots.remove(id);
        subscriptions.requests.insert(id.to_owned(), request);
        collect_records(subscriptions);
        self.prepare(next)
    }
    /// Persist this barrier before withdrawing a view or dispatching server cancellation.
    /// Also use it on authorization failure; v1 outbox/outcomes remain untouched.
    pub fn prepare_cancel_subscription(
        &self,
        id: &str,
        generation: u64,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if !valid_subscription_text(id) || generation == 0 {
            return Err(invalid());
        }
        let mut next = self.state.clone();
        let subscriptions = next
            .subscriptions
            .get_or_insert_with(DurableSubscriptions::default);
        if !subscriptions.cancelled.contains_key(id)
            && subscriptions.cancelled.len() >= MAX_SUBSCRIPTIONS
        {
            return Err(invalid());
        }
        if subscriptions
            .requests
            .get(id)
            .is_some_and(|request| request.command.identity().1 > generation)
        {
            return Err(invalid());
        }
        let barrier = subscriptions
            .cancelled
            .entry(id.into())
            .or_insert(generation);
        *barrier = (*barrier).max(generation);
        subscriptions.slots.remove(id);
        subscriptions.requests.remove(id);
        collect_records(subscriptions);
        self.prepare(next)
    }
    pub fn prepare_subscription_response(
        &self,
        response: SubscriptionResponse<V>,
    ) -> Result<PreparedTransition<V>, ClientError> {
        if !validate_subscription_response(&response) {
            return Err(invalid());
        }
        let id = &response.subscription_id;
        let Some(current) = &self.state.subscriptions else {
            return Err(invalid());
        };
        if current
            .cancelled
            .get(id)
            .is_some_and(|barrier| *barrier >= response.generation)
        {
            return Err(ClientError::SubscriptionCancelled);
        }
        let request = current.requests.get(id).ok_or_else(invalid)?;
        let SubscriptionCommand::Start {
            definition_id,
            limit,
            ..
        } = &request.command
        else {
            return Err(invalid());
        };
        if response.partition_id != self.state.partition_id
            || response.client_id != self.state.client_id
            || response.generation != request.command.identity().1
            || response.scope.identity != self.state.requested_scope
            || response.scope.subscription_revision != self.state.subscription_revision
            || response.coverage.definition_id != *definition_id
            || response.coverage.limit != *limit
            || response.frontier.epoch == 0
            || response.view_revision == 0
            || response.changes.len() > MAX_WINDOW_ROWS * 2
            || response.ordered_ids.len() > usize::from(*limit)
            || !valid_subscription_text(&response.plan_fingerprint)
        {
            return Err(invalid());
        }
        if matches!(response.response, WindowResponseKind::Cancelled) {
            return self.prepare_cancel_subscription(id, response.generation);
        }
        let token = response
            .token
            .clone()
            .filter(|token| valid_subscription_text(&token.0))
            .ok_or_else(invalid)?;
        guard_subscription_scope(&self.state, &response.scope)?;
        if self
            .state
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.scope != response.scope)
        {
            if !self.state.outbox.is_empty()
                || self.state.outcomes.iter().any(|outcome| {
                    matches!(
                        outcome.outcome,
                        surrealdb_sync_protocol::DurableOutcome::Conflict { .. }
                    ) && outcome.resolution.is_none()
                })
            {
                return Err(ClientError::PendingQuarantineRequired);
            }
            return Err(ClientError::NeedsNewScopeJournal);
        }
        let previous = current.slots.get(id);
        if let Some(previous) = previous {
            if response.scope != previous.scope {
                if !self.state.outbox.is_empty()
                    || self.state.outcomes.iter().any(|outcome| {
                        matches!(
                            outcome.outcome,
                            surrealdb_sync_protocol::DurableOutcome::Conflict { .. }
                        ) && outcome.resolution.is_none()
                    })
                {
                    return Err(ClientError::PendingQuarantineRequired);
                }
                return Err(ClientError::NeedsNewScopeJournal);
            }
            if response.frontier.epoch < previous.frontier.epoch
                || (response.frontier.epoch == previous.frontier.epoch
                    && response.frontier.sequence < previous.frontier.sequence)
                || response.view_revision < previous.view_revision
            {
                return Err(invalid());
            }
        }
        if let Some(previous) = previous
            && response.view_revision == previous.view_revision
        {
            let same = response.scope == previous.scope
                && response.frontier == previous.frontier
                && response.plan_fingerprint == previous.plan_fingerprint
                && response.coverage == previous.coverage
                && response.ordered_ids == previous.ordered_ids
                && response.token.as_ref() == Some(&previous.token)
                && response.changes.iter().all(|change| match change {
                    MembershipChange::Upsert { row } => {
                        previous
                            .members
                            .get(&row.record_id)
                            .and_then(|key| current.records.get(key))
                            == Some(row)
                    }
                    MembershipChange::Leave { record_id }
                    | MembershipChange::Delete { record_id, .. } => {
                        !previous.members.contains_key(record_id)
                    }
                });
            if same {
                return self.prepare(self.state.clone());
            }
            return Err(invalid());
        }
        let mut rows = BTreeMap::new();
        match &response.response {
            WindowResponseKind::Delta { before_revision } => {
                let previous = previous.ok_or_else(invalid)?;
                if *before_revision != previous.view_revision
                    || response.view_revision
                        != before_revision.checked_add(1).ok_or_else(invalid)?
                    || response.plan_fingerprint != previous.plan_fingerprint
                    || response.frontier.epoch != previous.frontier.epoch
                    || response.coverage != previous.coverage
                {
                    return Err(invalid());
                }
                for (id, key) in &previous.members {
                    rows.insert(
                        id.clone(),
                        current.records.get(key).ok_or_else(invalid)?.clone(),
                    );
                }
            }
            WindowResponseKind::Snapshot { .. } => {}
            WindowResponseKind::Cancelled => return Err(invalid()),
        }
        let mut touched = BTreeSet::new();
        for change in &response.changes {
            let id = match change {
                MembershipChange::Upsert { row } => &row.record_id,
                MembershipChange::Leave { record_id }
                | MembershipChange::Delete { record_id, .. } => record_id,
            };
            if !valid_subscription_text(&id.0) || !touched.insert(id.clone()) {
                return Err(invalid());
            }
            match change {
                MembershipChange::Upsert { row } => {
                    if row.version == 0 || row.sort_key.kind() != response.coverage.order_kind {
                        return Err(invalid());
                    }
                    if rows
                        .get(id)
                        .is_some_and(|previous: &SubscriptionRow<V>| row.version < previous.version)
                    {
                        return Err(invalid());
                    }
                    rows.insert(id.clone(), row.clone());
                }
                MembershipChange::Leave { .. } | MembershipChange::Delete { .. } => {
                    if let MembershipChange::Delete { version, .. } = change
                        && rows
                            .get(id)
                            .is_some_and(|previous| *version <= previous.version)
                    {
                        return Err(invalid());
                    }
                    if !matches!(response.response, WindowResponseKind::Delta { .. })
                        || rows.remove(id).is_none()
                    {
                        return Err(invalid());
                    }
                }
            }
        }
        if rows.len() > usize::from(*limit)
            || ordered(&rows, &response.coverage) != response.ordered_ids
        {
            return Err(invalid());
        }
        let mut next = self.state.clone();
        let subscriptions = next.subscriptions.as_mut().ok_or_else(invalid)?;
        subscriptions.authorization_scope = Some(response.scope.clone());
        let mut members = BTreeMap::new();
        for (id, row) in rows {
            let key = cache_key(response.frontier.epoch, &row);
            if subscriptions
                .records
                .get(&key)
                .is_some_and(|stored| stored != &row)
            {
                return Err(invalid());
            }
            subscriptions.records.insert(key.clone(), row);
            members.insert(id, key);
        }
        subscriptions.slots.insert(
            response.subscription_id,
            SubscriptionSlot {
                generation: response.generation,
                plan_fingerprint: response.plan_fingerprint,
                scope: response.scope,
                frontier: response.frontier,
                view_revision: response.view_revision,
                coverage: response.coverage,
                ordered_ids: response.ordered_ids,
                token,
                members,
            },
        );
        collect_records(subscriptions);
        self.prepare(next)
    }
    /// Produces the currently installed resume request; tokens stay outside read metadata.
    pub fn subscription_request(&self, id: &str) -> Option<SubscriptionRequest> {
        let state = self.state.subscriptions.as_ref()?;
        let mut request = if let Some(request) = state.requests.get(id) {
            request.clone()
        } else {
            return state
                .cancelled
                .get(id)
                .map(|generation| SubscriptionRequest {
                    partition_id: self.state.partition_id.clone(),
                    client_id: self.state.client_id.clone(),
                    requested_scope: self.state.requested_scope.clone(),
                    subscription_revision: self.state.subscription_revision,
                    command: SubscriptionCommand::Cancel {
                        subscription_id: id.into(),
                        generation: *generation,
                    },
                });
        };
        if let Some(slot) = state.slots.get(id) {
            request.command = SubscriptionCommand::Resume {
                subscription_id: id.into(),
                generation: slot.generation,
                token: slot.token.clone(),
            };
        }
        Some(request)
    }
    pub fn subscription_snapshot(&self, id: &str) -> Option<InstalledSubscription<V>> {
        let subscriptions = self.state.subscriptions.as_ref()?;
        let slot = subscriptions.slots.get(id)?;
        let rows = slot
            .ordered_ids
            .iter()
            .filter_map(|id| {
                slot.members
                    .get(id)
                    .and_then(|key| subscriptions.records.get(key))
                    .cloned()
            })
            .collect();
        let pending_outside_window = self
            .state
            .outbox
            .iter()
            .flat_map(|commit| {
                commit
                    .operations
                    .iter()
                    .map(|operation| operation.record_id().clone())
            })
            .filter(|id| !slot.members.contains_key(id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let pending_outside_window = if crate::read::validate_coverage(&self.state).is_ok()
            && self
                .state
                .reset_coverage
                .as_ref()
                .is_some_and(|coverage| coverage.scope == slot.scope)
            && self
                .state
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.scope == slot.scope)
        {
            Some(pending_outside_window)
        } else {
            None
        };
        Some(InstalledSubscription {
            metadata: SubscriptionViewMetadata {
                revision: self.state.revision,
                subscription_id: id.into(),
                generation: slot.generation,
                plan_fingerprint: slot.plan_fingerprint.clone(),
                scope: slot.scope.clone(),
                frontier: slot.frontier,
                view_revision: slot.view_revision,
                coverage: slot.coverage.clone(),
            },
            rows,
            pending_outside_window,
        })
    }
}
