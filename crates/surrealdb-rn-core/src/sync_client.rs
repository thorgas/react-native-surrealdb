//! Experimental UniFFI facade over the durable sync client state machine.
//!
//! The facade performs no networking. Callers provide serialized protocol
//! messages and remain responsible for transport, authentication, and retry.
//! Runtime ownership is per database/session/key, shared across wrapper handles.
//! Lock order is database lifecycle -> shared runtime -> DB transaction. Registry
//! locks never span await. Raw SQL session/private-journal writes are trusted misuse.

use crate::sync_diagnostics::{NativeSyncDiagnostics, SyncDiagnostics};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use surrealdb_sync_client::{
    AutomaticConflictPolicy, ClientError, ClientRuntime, ConflictResolutionRequest,
    PreparedTransition,
};
use surrealdb_sync_protocol::{
    BaseVersion, ClientCommit, ClientCommitId, ClientId, CommitIdentity, DurableOutcome,
    Fingerprint, Operation, PartitionId, PullResponse, PushResponse, RecordId, RecordState,
    ScopeIdentity,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::SurrealDatabase;
use crate::sync_codec::{
    canonical_fingerprint, validate_pull_response_values, validate_push_response_values,
    validate_state_codec,
};
use crate::sync_state::{
    MAX_IDENTITY_BYTES, MAX_SYNC_STATE_BYTES, ScopeBarrier, StoredSyncState, SyncStateError,
    SyncStateKey, SyncStateStore,
};

#[derive(Clone, Debug, uniffi::Record)]
pub struct NativeSyncOpenOptions {
    pub partition_id: String,
    pub client_id: String,
    pub requested_scope: String,
    pub subscription_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, uniffi::Record)]
pub struct NativeSyncStatus {
    pub revision: u64,
    pub pending_count: u32,
    pub outcome_count: u32,
    pub conflict_count: u32,
    pub cursor_epoch: Option<u64>,
    pub cursor_sequence: Option<u64>,
}

/// Counts remain available when a persisted scope barrier blocks ordinary reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, uniffi::Enum)]
pub enum NativeSyncInspectionBarrier {
    None,
    NeedsNewScopeJournal,
    PendingQuarantineRequired,
}

#[derive(Clone, Debug, Eq, PartialEq, uniffi::Enum)]
pub enum NativeSyncInspection {
    Missing,
    Present {
        revision: u64,
        pending_count: u32,
        outcome_count: u32,
        conflict_count: u32,
        barrier: NativeSyncInspectionBarrier,
    },
}

/// Read existing durable state only; never initialize, migrate, or clear a barrier.
#[uniffi::export(async_runtime = "tokio")]
pub async fn inspect_sync_state(
    database: Arc<SurrealDatabase>,
    options: NativeSyncOpenOptions,
) -> Result<NativeSyncInspection, NativeSyncError> {
    if !database.embedded {
        return Err(NativeSyncError::RequiresEmbeddedDatabase);
    }
    let _lifecycle = database.journal_lifecycle.read().await;
    if database.is_closed() {
        return Err(NativeSyncError::Closed);
    }
    let session_generation = database.session_generation.load(Ordering::Acquire);
    if options.requested_scope.len() > MAX_IDENTITY_BYTES {
        return Err(NativeSyncError::InvalidInput);
    }
    let partition = PartitionId(options.partition_id);
    let client = ClientId(options.client_id);
    let (state, barrier) = SyncStateStore::new(&database)
        .inspect(SyncStateKey::new(&partition, &client))
        .await
        .map_err(map_persistence_error)?;
    if database.is_closed()
        || database.session_generation.load(Ordering::Acquire) != session_generation
    {
        return Err(NativeSyncError::RecoveryRequired);
    }
    let Some(state) = state else {
        return Ok(NativeSyncInspection::Missing);
    };
    if state.requested_scope.0 != options.requested_scope
        || state.subscription_revision != options.subscription_revision
    {
        return Err(NativeSyncError::Protocol);
    }
    validate_state_codec(&state)?;
    let runtime = ClientRuntime::open(state).map_err(map_protocol_error)?;
    let counts = status(&runtime)?;
    Ok(NativeSyncInspection::Present {
        revision: counts.revision,
        pending_count: counts.pending_count,
        outcome_count: counts.outcome_count,
        conflict_count: counts.conflict_count,
        barrier: match barrier.as_ref().map(|value| value.reason.as_str()) {
            None => NativeSyncInspectionBarrier::None,
            Some("needs_new_scope_journal") => NativeSyncInspectionBarrier::NeedsNewScopeJournal,
            Some("pending_quarantine_required") => {
                NativeSyncInspectionBarrier::PendingQuarantineRequired
            }
            _ => return Err(NativeSyncError::RecoveryRequired),
        },
    })
}

#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum NativeConflictPolicy {
    Manual,
    PreferServerV1,
}

impl From<NativeConflictPolicy> for AutomaticConflictPolicy {
    fn from(policy: NativeConflictPolicy) -> Self {
        match policy {
            NativeConflictPolicy::Manual => Self::Manual,
            NativeConflictPolicy::PreferServerV1 => Self::PreferServerV1,
        }
    }
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum NativeSyncError {
    #[error("sync client is closed")]
    Closed,

    #[error("sync input is invalid")]
    InvalidInput,

    #[error("sync protocol transition is invalid")]
    Protocol,

    #[error("sync persistence operation failed")]
    Persistence,

    #[error("sync persistence outcome is unknown; reopen before continuing")]
    RecoveryRequired,

    #[error("sync client requires an embedded database")]
    RequiresEmbeddedDatabase,

    #[error("sync read output exceeds the bounded bridge limit")]
    ReadOutputTooLarge,

    #[error("scope change requires a new authorized journal")]
    NeedsNewScopeJournal,
    #[error("scope change requires explicit pending intent quarantine")]
    PendingQuarantineRequired,

    #[error("subscription generation is durably cancelled")]
    SubscriptionCancelled,
    #[error("invalid subscription transition")]
    InvalidSubscription,

    #[error("sync client internal conversion failed")]
    Internal,
}

type RuntimeKey = (u64, String, String);
#[derive(Default)]
pub(crate) struct RuntimeRegistry(Mutex<BTreeMap<RuntimeKey, Weak<AsyncMutex<RuntimeState>>>>);
impl RuntimeRegistry {
    fn acquire(&self, key: RuntimeKey) -> Result<Arc<AsyncMutex<RuntimeState>>, NativeSyncError> {
        let mut entries = self.0.lock().map_err(|_| NativeSyncError::Internal)?;
        entries.retain(|_, value| value.strong_count() > 0);
        if let Some(existing) = entries.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        let runtime = Arc::new(AsyncMutex::new(RuntimeState {
            runtime: None,
            journal: Default::default(),
            subscription_journal: Default::default(),
            recovery_required: false,
            scope_blocked: None,
            bootstrap_binding: None,
            bootstrap_binding_ticket: 0,
        }));
        entries.insert(key, Arc::downgrade(&runtime));
        Ok(runtime)
    }
}

#[derive(uniffi::Object)]
pub struct NativeSyncClient {
    database: Arc<SurrealDatabase>,
    runtime: Arc<AsyncMutex<RuntimeState>>,
    local_closed: AtomicBool,
    session_generation: u64,
    diagnostics: SyncDiagnostics,
    #[cfg(test)]
    lose_next_persist_ack: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    completion_pause: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn open_sync_client(
    database: Arc<SurrealDatabase>,
    options: NativeSyncOpenOptions,
) -> Result<Arc<NativeSyncClient>, NativeSyncError> {
    if !database.embedded {
        return Err(NativeSyncError::RequiresEmbeddedDatabase);
    }

    let lifecycle = database.journal_lifecycle.read().await;
    if database.is_closed() {
        return Err(NativeSyncError::Closed);
    }
    let session_generation = database.session_generation.load(Ordering::Acquire);
    let partition_id = PartitionId(options.partition_id);
    let client_id = ClientId(options.client_id);
    let requested_scope = ScopeIdentity(options.requested_scope);
    let key = SyncStateKey::new(&partition_id, &client_id);
    crate::sync_state::validate_key(key).map_err(map_persistence_error)?;
    let shared = database.sync_runtimes.acquire((
        session_generation,
        partition_id.0.clone(),
        client_id.0.clone(),
    ))?;
    let mut shared_slot = shared.lock().await;
    let store = SyncStateStore::new(&database).with_session_generation(session_generation);
    if shared_slot.runtime.is_none() {
        store.define_schema().await.map_err(map_persistence_error)?;

        let state = match store.load(key).await.map_err(map_persistence_error)? {
            Some(state) => state,
            None => {
                let initial = StoredSyncState::empty(
                    partition_id.clone(),
                    client_id.clone(),
                    requested_scope.clone(),
                    options.subscription_revision,
                );
                match store.save_atomic(None, &initial).await {
                    Ok(_) => initial,
                    Err(SyncStateError::RevisionConflict) => store
                        .load(key)
                        .await
                        .map_err(map_persistence_error)?
                        .ok_or(NativeSyncError::Persistence)?,
                    Err(error) => return Err(map_persistence_error(error)),
                }
            }
        };

        if state.partition_id != partition_id
            || state.client_id != client_id
            || state.requested_scope != requested_scope
            || state.subscription_revision != options.subscription_revision
        {
            return Err(NativeSyncError::Protocol);
        }
        validate_state_codec(&state)?;
        let barrier = store
            .load_scope_barrier(key)
            .await
            .map_err(|_| NativeSyncError::RecoveryRequired)?;
        let scope_blocked = barrier.map(|value| ScopeBlock::from_reason(&value.reason));
        let runtime = ClientRuntime::open(state).map_err(map_protocol_error)?;
        if shared_slot.recovery_required
            && (shared_slot.journal.rotate().is_err()
                || shared_slot.subscription_journal.rotate().is_err())
        {
            invalidate_runtime(&mut shared_slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        // Preserve each random prefix/checked counter; recovery never creates Default journals.
        shared_slot.runtime = Some(runtime);
        shared_slot.recovery_required = false;
        shared_slot.scope_blocked = scope_blocked;
    }
    let state = shared_slot.client()?.state();
    if state.requested_scope != requested_scope
        || state.subscription_revision != options.subscription_revision
    {
        return Err(NativeSyncError::Protocol);
    }
    if database.session_generation.load(Ordering::Acquire) != session_generation
        || database.is_closed()
    {
        shared_slot.runtime.take();
        shared_slot.recovery_required = true;
        return Err(NativeSyncError::RecoveryRequired);
    }
    drop(shared_slot);
    drop(lifecycle);
    Ok(Arc::new(NativeSyncClient {
        database,
        runtime: shared,
        local_closed: AtomicBool::new(false),
        session_generation,
        diagnostics: SyncDiagnostics::default(),
        #[cfg(test)]
        lose_next_persist_ack: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        completion_pause: Mutex::new(None),
    }))
}

impl NativeSyncClient {
    fn ensure_session(&self) -> Result<(), NativeSyncError> {
        if self.local_closed.load(Ordering::Acquire) {
            return Err(NativeSyncError::Closed);
        }
        if self.database.is_closed()
            || self.database.session_generation.load(Ordering::Acquire) != self.session_generation
        {
            return Err(NativeSyncError::RecoveryRequired);
        }
        Ok(())
    }
    async fn refresh_scope_barrier(&self, slot: &mut RuntimeState) -> Result<(), NativeSyncError> {
        self.ensure_session()?;
        let state = slot.client()?.state();
        match SyncStateStore::new(&self.database)
            .with_session_generation(self.session_generation)
            .load_scope_barrier(SyncStateKey::new(&state.partition_id, &state.client_id))
            .await
        {
            Ok(Some(barrier)) => {
                slot.scope_blocked = Some(ScopeBlock::from_reason(&barrier.reason))
            }
            Ok(None) => {}
            Err(_) => {
                slot.runtime.take();
                slot.recovery_required = true;
                return Err(NativeSyncError::RecoveryRequired);
            }
        }
        Ok(())
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl NativeSyncClient {
    pub async fn enqueue(&self, commit_json: String) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut commit = decode_input::<ClientCommit<JsonValue>>(&commit_json)?;
        let mut slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        commit.identity.fingerprint = canonical_fingerprint(runtime.state(), &commit)?;
        let prepared = slot
            .client()?
            .prepare_enqueue(commit)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn record_push_response(
        &self,
        response_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        self.record_push_response_with_policy(response_json, NativeConflictPolicy::Manual)
            .await
    }

    /// The caller selects a local policy; the server outcome remains untrusted
    /// and is validated before any durable state is replaced.
    pub async fn record_push_response_with_policy(
        &self,
        response_json: String,
        policy: NativeConflictPolicy,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let response = decode_input::<PushResponse<JsonValue>>(&response_json)?;
        validate_push_response_values(&response.outcome)?;
        let mut slot = self.lock_runtime().await;
        let prepared = slot
            .client()?
            .prepare_push_response_with_policy(response, policy.into())
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn apply_pull_response(
        &self,
        response_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let response = decode_input::<PullResponse<JsonValue>>(&response_json)?;
        validate_pull_response_values(&response)?;
        let mut slot = self.lock_runtime().await;
        let required_scope = match &response {
            PullResponse::Reset(snapshot) => Some(snapshot.checkpoint.scope.clone()),
            PullResponse::Batch(batch) => batch.frames.iter().find_map(|frame| match frame {
                surrealdb_sync_protocol::PullFrame::End { checkpoint } => {
                    Some(checkpoint.scope.clone())
                }
                _ => None,
            }),
        };
        let prepared = match slot.client()?.prepare_pull_response(response) {
            Ok(prepared) => prepared,
            Err(error) => {
                match error {
                    ClientError::PendingQuarantineRequired => {
                        slot.scope_blocked = Some(ScopeBlock::PendingQuarantine)
                    }
                    ClientError::NeedsNewScopeJournal => {
                        slot.scope_blocked = Some(ScopeBlock::NewJournal)
                    }
                    _ => {}
                }
                if let (Some(block), Some(scope)) = (slot.scope_blocked, required_scope) {
                    let state = slot.client()?.state();
                    let result = SyncStateStore::new(&self.database)
                        .with_session_generation(self.session_generation)
                        .save_scope_barrier(
                            SyncStateKey::new(&state.partition_id, &state.client_id),
                            ScopeBarrier {
                                reason: block.reason().into(),
                                required_scope: scope.identity.0,
                                required_subscription_revision: scope
                                    .subscription_revision
                                    .to_string(),
                                required_authorization_revision: scope
                                    .authorization_revision
                                    .to_string(),
                            },
                        )
                        .await;
                    if result.is_err() {
                        slot.runtime.take();
                        slot.recovery_required = true;
                        return Err(NativeSyncError::RecoveryRequired);
                    }
                }
                return Err(map_protocol_error(error));
            }
        };
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn resolve_conflict_keep_server(
        &self,
        conflicted_commit_id: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        let prepared = slot
            .client()?
            .prepare_resolve_conflict(
                &ClientCommitId(conflicted_commit_id),
                ConflictResolutionRequest::KeepServer,
            )
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn resolve_conflict_keep_local(
        &self,
        conflicted_commit_id: String,
        replacement_commit_id: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        let conflict_id = ClientCommitId(conflicted_commit_id);
        let (mut replacement, record_id, authoritative) = conflict_context(runtime, &conflict_id)?;
        replacement.identity = CommitIdentity {
            client_commit_id: ClientCommitId(replacement_commit_id),
            fingerprint: Fingerprint(String::new()),
        };
        rebase_conflict_operation(&mut replacement, &record_id, &authoritative)?;
        replacement.identity.fingerprint = canonical_fingerprint(runtime.state(), &replacement)?;
        let prepared = runtime
            .prepare_resolve_conflict(
                &conflict_id,
                ConflictResolutionRequest::KeepLocal { replacement },
            )
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn resolve_conflict_merge(
        &self,
        conflicted_commit_id: String,
        replacement_commit_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut replacement = decode_input::<ClientCommit<JsonValue>>(&replacement_commit_json)?;
        let mut slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        replacement.identity.fingerprint = canonical_fingerprint(runtime.state(), &replacement)?;
        let prepared = runtime
            .prepare_resolve_conflict(
                &ClientCommitId(conflicted_commit_id),
                ConflictResolutionRequest::Merge { replacement },
            )
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn pending_json(&self) -> Result<Vec<String>, NativeSyncError> {
        let slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        runtime
            .pending_commits()
            .map(encode_output)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn conflicts_json(&self) -> Result<Vec<String>, NativeSyncError> {
        let slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        runtime
            .state()
            .outcomes
            .iter()
            .filter(|resolved| {
                resolved.resolution.is_none()
                    && matches!(&resolved.outcome, DurableOutcome::Conflict { .. })
            })
            .map(encode_output)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Returns the token from the last completely and atomically applied pull.
    pub async fn checkpoint_token(&self) -> Result<Option<String>, NativeSyncError> {
        let slot = self.lock_runtime().await;
        let runtime = slot.client()?;
        Ok(runtime
            .state()
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.token.0.clone()))
    }

    pub async fn status(&self) -> Result<NativeSyncStatus, NativeSyncError> {
        let slot = self.lock_runtime().await;
        status(slot.client()?)
    }

    pub async fn close(&self) {
        let _slot = self.lock_runtime().await;
        self.local_closed.store(true, Ordering::Release);
    }

    pub fn subscription_capabilities_json(&self) -> String {
        crate::sync_subscription::capabilities()
    }

    pub async fn start_subscription_json(
        &self,
        request_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let request = crate::sync_subscription::parse_request(&request_json)?;
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        let prepared = slot
            .read_client()?
            .prepare_start_subscription(request)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn cancel_subscription(
        &self,
        subscription_id: String,
        generation: u64,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        // Local cancellation remains available after quarantine and never exposes old rows.
        let prepared = slot
            .client()?
            .prepare_cancel_subscription(&subscription_id, generation)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn apply_subscription_response_json(
        &self,
        response_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let response = crate::sync_subscription::parse_response(&response_json)?;
        let required_scope = response.scope.clone();
        let subscription_id = response.subscription_id.clone();
        let generation = response.generation;
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        let prepared = match slot.read_client()?.prepare_subscription_response(response) {
            Ok(prepared) => prepared,
            Err(
                error
                @ (ClientError::PendingQuarantineRequired | ClientError::NeedsNewScopeJournal),
            ) => {
                let block = if error == ClientError::PendingQuarantineRequired {
                    ScopeBlock::PendingQuarantine
                } else {
                    ScopeBlock::NewJournal
                };
                slot.scope_blocked = Some(block);
                let state = slot.client()?.state();
                let persisted = SyncStateStore::new(&self.database)
                    .with_session_generation(self.session_generation)
                    .save_scope_barrier(
                        SyncStateKey::new(&state.partition_id, &state.client_id),
                        ScopeBarrier {
                            reason: block.reason().into(),
                            required_scope: required_scope.identity.0,
                            required_subscription_revision: required_scope
                                .subscription_revision
                                .to_string(),
                            required_authorization_revision: required_scope
                                .authorization_revision
                                .to_string(),
                        },
                    )
                    .await;
                if persisted.is_err() {
                    slot.runtime.take();
                    slot.recovery_required = true;
                    return Err(NativeSyncError::RecoveryRequired);
                }
                let cancel = slot
                    .client()?
                    .prepare_cancel_subscription(&subscription_id, generation)
                    .map_err(map_protocol_error)?;
                self.persist_and_install(&mut slot, cancel).await?;
                return Err(map_protocol_error(error));
            }
            Err(error) => return Err(map_protocol_error(error)),
        };
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn subscription_request_json(
        &self,
        subscription_id: String,
    ) -> Result<String, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        crate::sync_subscription::output(&crate::sync_subscription::request(
            slot.read_client()?,
            &subscription_id,
        )?)
    }

    pub async fn subscription_snapshot_json(
        &self,
        subscription_id: String,
    ) -> Result<String, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        crate::sync_subscription::output(
            &slot
                .subscription_journal
                .bootstrap(slot.read_client()?, &subscription_id)?,
        )
    }

    pub async fn subscription_changes_json(
        &self,
        subscription_id: String,
        cursor_json: String,
        max_batches: u32,
    ) -> Result<String, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        crate::sync_subscription::output(&slot.subscription_journal.changes(
            slot.read_client()?,
            &subscription_id,
            &cursor_json,
            max_batches,
        )?)
    }

    pub fn read_capabilities_json(&self) -> String {
        crate::sync_read::capabilities()
    }

    pub async fn read_snapshot_json(&self) -> Result<String, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        crate::sync_read::output(&slot.journal.bootstrap(slot.read_client()?)?)
    }

    pub async fn read_changes_json(
        &self,
        cursor_json: String,
        max_batches: u32,
    ) -> Result<String, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        crate::sync_read::output(&slot.journal.changes(
            slot.read_client()?,
            &cursor_json,
            max_batches,
        )?)
    }

    pub async fn requires_authorized_reset(&self) -> Result<bool, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        self.refresh_scope_barrier(&mut slot).await?;
        Ok(slot.read_client()?.state().reset_coverage.is_none())
    }

    pub async fn scope_transition_decision(
        &self,
        scope: String,
        subscription_revision: u64,
    ) -> Result<String, NativeSyncError> {
        let slot = self.lock_runtime().await;
        let decision = slot
            .client()?
            .scope_transition_decision(&ScopeIdentity(scope), subscription_revision);
        serde_json::to_value(decision)
            .map_err(|_| NativeSyncError::Internal)?
            .as_str()
            .map(str::to_owned)
            .ok_or(NativeSyncError::Internal)
    }

    /// Metadata-only cumulative measurements, not an atomic snapshot of application rows.
    /// Storage duration includes validation/encoding and the embedded transaction;
    /// it is not server authority processing time. Counters include failed attempts.
    pub fn diagnostics(&self) -> NativeSyncDiagnostics {
        self.diagnostics.snapshot()
    }

    pub fn is_closed(&self) -> bool {
        self.local_closed.load(Ordering::Acquire)
            || self.ensure_session().is_err()
            || self.runtime.try_lock().is_ok_and(|slot| slot.is_none())
    }
}

#[derive(Clone, Copy)]
enum ScopeBlock {
    PendingQuarantine,
    NewJournal,
}

impl ScopeBlock {
    fn reason(self) -> &'static str {
        match self {
            Self::PendingQuarantine => "pending_quarantine_required",
            Self::NewJournal => "needs_new_scope_journal",
        }
    }
    fn from_reason(reason: &str) -> Self {
        if reason == "pending_quarantine_required" {
            Self::PendingQuarantine
        } else {
            Self::NewJournal
        }
    }
}

fn invalidate_runtime(slot: &mut RuntimeState) {
    slot.runtime.take();
    slot.recovery_required = true;
    slot.journal.rotate().ok();
    slot.subscription_journal.rotate().ok();
}

struct RuntimeState {
    runtime: Option<ClientRuntime<JsonValue>>,
    journal: crate::sync_read::ReadJournal,
    subscription_journal: crate::sync_subscription::SubscriptionJournal,
    recovery_required: bool,
    scope_blocked: Option<ScopeBlock>,
    bootstrap_binding: Option<surrealdb_sync_protocol::bootstrap::BootstrapContext>,
    // Never reset on shared-cell recovery: old Session objects remain stale.
    bootstrap_binding_ticket: u64,
}
impl RuntimeState {
    fn read_client(&self) -> Result<&ClientRuntime<JsonValue>, NativeSyncError> {
        let client = self.client()?;
        match self.scope_blocked {
            Some(ScopeBlock::PendingQuarantine) => Err(NativeSyncError::PendingQuarantineRequired),
            Some(ScopeBlock::NewJournal) => Err(NativeSyncError::NeedsNewScopeJournal),
            None => Ok(client),
        }
    }

    fn client(&self) -> Result<&ClientRuntime<JsonValue>, NativeSyncError> {
        self.runtime.as_ref().ok_or(if self.recovery_required {
            NativeSyncError::RecoveryRequired
        } else {
            NativeSyncError::Closed
        })
    }
}
impl Deref for RuntimeState {
    type Target = Option<ClientRuntime<JsonValue>>;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}
impl DerefMut for RuntimeState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

pub(crate) struct BootstrapAccess<'a> {
    slot: RuntimeGuard<'a>,
}
impl BootstrapAccess<'_> {
    pub(crate) fn store(&self) -> crate::sync_bootstrap_store::BootstrapStore<'_> {
        crate::sync_bootstrap_store::BootstrapStore::new(&self.slot.owner.database)
            .with_session_generation(self.slot.owner.session_generation)
    }
    pub(crate) fn bind(
        &mut self,
        expected: &surrealdb_sync_protocol::bootstrap::BootstrapContext,
    ) -> Result<u64, crate::sync_bootstrap::NativeBootstrapError> {
        if self.slot.bootstrap_binding.as_ref() != Some(expected) {
            let next = self
                .slot
                .bootstrap_binding_ticket
                .checked_add(1)
                .ok_or(crate::sync_bootstrap::NativeBootstrapError::RecoveryRequired)?;
            self.slot.bootstrap_binding = Some(expected.clone());
            self.slot.bootstrap_binding_ticket = next;
        }
        Ok(self.slot.bootstrap_binding_ticket)
    }
    pub(crate) async fn check_writable(
        &mut self,
    ) -> Result<(), crate::sync_bootstrap::NativeBootstrapError> {
        let owner = self.slot.owner;
        owner
            .refresh_scope_barrier(&mut self.slot)
            .await
            .map_err(crate::sync_bootstrap::NativeBootstrapError::from_native)?;
        self.slot
            .read_client()
            .map_err(crate::sync_bootstrap::NativeBootstrapError::from_native)?;
        Ok(())
    }
    pub(crate) fn finish<T>(
        &mut self,
        result: Result<T, SyncStateError>,
        mutating: bool,
    ) -> Result<T, crate::sync_bootstrap::NativeBootstrapError> {
        use crate::sync_bootstrap::NativeBootstrapError;
        if matches!(
            &result,
            Err(SyncStateError::CommitUnknown(_) | SyncStateError::RollbackFailed { .. })
        ) || self.slot.owner.ensure_session().is_err()
        {
            invalidate_runtime(&mut self.slot);
            return Err(NativeBootstrapError::RecoveryRequired);
        }
        #[cfg(test)]
        if mutating
            && result.is_ok()
            && self
                .slot
                .owner
                .lose_next_persist_ack
                .swap(false, Ordering::SeqCst)
        {
            invalidate_runtime(&mut self.slot);
            return Err(NativeBootstrapError::RecoveryRequired);
        }
        #[cfg(not(test))]
        let _ = mutating;
        result.map_err(NativeBootstrapError::from_store)
    }
}

struct RuntimeGuard<'a> {
    owner: &'a NativeSyncClient,
    _lifecycle: tokio::sync::RwLockReadGuard<'a, ()>,
    guard: tokio::sync::MutexGuard<'a, RuntimeState>,
    diagnostics: &'a SyncDiagnostics,
    started: Instant,
    ordinal: u64,
    wait_ns: u64,
}
impl RuntimeGuard<'_> {
    fn client(&self) -> Result<&ClientRuntime<JsonValue>, NativeSyncError> {
        self.owner.ensure_session()?;
        self.guard.client()
    }
    fn read_client(&self) -> Result<&ClientRuntime<JsonValue>, NativeSyncError> {
        self.owner.ensure_session()?;
        self.guard.read_client()
    }
}
impl Deref for RuntimeGuard<'_> {
    type Target = RuntimeState;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}
impl DerefMut for RuntimeGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
impl Drop for RuntimeGuard<'_> {
    fn drop(&mut self) {
        self.diagnostics
            .complete_operation(self.ordinal, self.wait_ns, self.started);
        self.diagnostics.runtime_hold.record(self.started);
    }
}

impl NativeSyncClient {
    async fn lock_runtime(&self) -> RuntimeGuard<'_> {
        let lifecycle = self.database.journal_lifecycle.read().await;
        let started = Instant::now();
        let guard = self.runtime.lock().await;
        let wait_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.diagnostics.runtime_wait.record(started);
        let ordinal = self.diagnostics.begin_operation();
        RuntimeGuard {
            owner: self,
            _lifecycle: lifecycle,
            guard,
            diagnostics: &self.diagnostics,
            started: Instant::now(),
            ordinal,
            wait_ns,
        }
    }

    pub(crate) async fn bootstrap_access(
        &self,
        expected: &surrealdb_sync_protocol::bootstrap::BootstrapContext,
        ticket: Option<u64>,
    ) -> Result<BootstrapAccess<'_>, crate::sync_bootstrap::NativeBootstrapError> {
        use crate::sync_bootstrap::NativeBootstrapError;
        let slot = self.lock_runtime().await;
        let state = slot
            .client()
            .map_err(NativeBootstrapError::from_native)?
            .state();
        if state.partition_id != expected.partition_id
            || state.client_id != expected.client_id
            || state.requested_scope != expected.scope.identity
            || state.subscription_revision != expected.scope.subscription_revision
        {
            return Err(NativeBootstrapError::ContextMismatch);
        }
        if state.checkpoint.as_ref().is_some_and(|checkpoint| {
            expected.scope.authorization_revision < checkpoint.scope.authorization_revision
        }) || slot.bootstrap_binding.as_ref().is_some_and(|bound| {
            !crate::sync_bootstrap_store::same_owner(bound, expected)
                || expected.scope.authorization_revision < bound.scope.authorization_revision
        }) || ticket.is_some_and(|ticket| {
            ticket != slot.bootstrap_binding_ticket
                || slot.bootstrap_binding.as_ref() != Some(expected)
        }) {
            return Err(NativeBootstrapError::ContextMismatch);
        }
        Ok(BootstrapAccess { slot })
    }
    #[cfg(test)]
    pub(crate) fn lose_next_bootstrap_ack_for_test(&self) {
        self.lose_next_persist_ack.store(true, Ordering::SeqCst);
    }

    /// Private composition only. Shared slot is held from latest durable prepare
    /// through commit/readback/publication; the lifecycle lease fences typed session changes.
    #[cfg(test)]
    pub(crate) async fn complete_bootstrap(
        &self,
        receipt: &[u8],
        expected: &surrealdb_sync_protocol::bootstrap::BootstrapContext,
        incarnation: u64,
        clock: &crate::sync_bootstrap_store::CompletionClock<'_>,
    ) -> Result<bool, NativeSyncError> {
        self.complete_bootstrap_bound(receipt, expected, incarnation, clock, None)
            .await
    }
    pub(crate) async fn complete_bootstrap_bound(
        &self,
        receipt: &[u8],
        expected: &surrealdb_sync_protocol::bootstrap::BootstrapContext,
        incarnation: u64,
        clock: &crate::sync_bootstrap_store::CompletionClock<'_>,
        ticket: Option<u64>,
    ) -> Result<bool, NativeSyncError> {
        let mut slot = self.lock_runtime().await;
        if ticket.is_some_and(|ticket| {
            ticket != slot.bootstrap_binding_ticket
                || slot.bootstrap_binding.as_ref() != Some(expected)
        }) {
            return Err(NativeSyncError::Protocol);
        }
        let state = slot.client()?.state();
        if state.partition_id != expected.partition_id
            || state.client_id != expected.client_id
            || state.requested_scope != expected.scope.identity
            || state.subscription_revision != expected.scope.subscription_revision
        {
            return Err(NativeSyncError::Protocol);
        }
        let checked_clock = || {
            self.ensure_session()
                .map_err(|_| SyncStateError::RevisionConflict)?;
            let now = (clock.utc_seconds)()?;
            self.ensure_session()
                .map_err(|_| SyncStateError::RevisionConflict)?;
            Ok(now)
        };
        let clock = crate::sync_bootstrap_store::CompletionClock {
            utc_seconds: &checked_clock,
            deadline: clock.deadline,
        };
        let result = crate::sync_bootstrap_store::BootstrapStore::new(&self.database)
            .with_session_generation(self.session_generation)
            .complete(receipt, expected, incarnation, &clock)
            .await;
        #[cfg(test)]
        {
            let pause = self.completion_pause.lock().unwrap().take();
            if let Some((arrived, release)) = pause {
                arrived.notify_one();
                release.notified().await;
            }
        }
        if matches!(
            &result,
            Err(SyncStateError::CommitUnknown(_) | SyncStateError::RollbackFailed { .. })
        ) || self.ensure_session().is_err()
        {
            invalidate_runtime(&mut slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.refresh_scope_barrier(&mut slot).await?;
                match slot.scope_blocked {
                    Some(ScopeBlock::NewJournal) => {
                        return Err(NativeSyncError::NeedsNewScopeJournal);
                    }
                    Some(ScopeBlock::PendingQuarantine) => {
                        return Err(NativeSyncError::PendingQuarantineRequired);
                    }
                    None => {
                        return Err(match error {
                            SyncStateError::InvalidState(error) => map_protocol_error(error),
                            error => map_persistence_error(error),
                        });
                    }
                }
            }
        };
        #[cfg(test)]
        if self.lose_next_persist_ack.swap(false, Ordering::SeqCst) {
            invalidate_runtime(&mut slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        let inspected = SyncStateStore::new(&self.database)
            .with_session_generation(self.session_generation)
            .inspect(key)
            .await;
        let Ok((Some(latest), barrier)) = inspected else {
            invalidate_runtime(&mut slot);
            return Err(NativeSyncError::RecoveryRequired);
        };
        if self.ensure_session().is_err() {
            invalidate_runtime(&mut slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        match result {
            crate::sync_bootstrap_store::Completion::HistoricalSuccess => {
                if !slot
                    .runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.state() == &latest)
                {
                    invalidate_runtime(&mut slot);
                    return Err(NativeSyncError::RecoveryRequired);
                }
                if let Some(barrier) = barrier {
                    slot.scope_blocked = Some(ScopeBlock::from_reason(&barrier.reason));
                }
                Ok(false)
            }
            crate::sync_bootstrap_store::Completion::Applied(applied) => {
                if latest != *applied || barrier.is_some() {
                    invalidate_runtime(&mut slot);
                    return Err(NativeSyncError::RecoveryRequired);
                }
                let Ok(runtime) = ClientRuntime::open(latest) else {
                    invalidate_runtime(&mut slot);
                    return Err(NativeSyncError::RecoveryRequired);
                };
                if slot.journal.rotate().is_err() || slot.subscription_journal.rotate().is_err() {
                    invalidate_runtime(&mut slot);
                    return Err(NativeSyncError::RecoveryRequired);
                }
                slot.runtime = Some(runtime);
                slot.recovery_required = false;
                Ok(true)
            }
        }
    }

    async fn persist_and_install(
        &self,
        slot: &mut RuntimeState,
        prepared: PreparedTransition<JsonValue>,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        self.ensure_session()?;
        let current = slot.client()?.state();
        let before_revision = current.revision;
        let next = prepared.state();
        validate_state_codec(next)?;
        let storage_started = Instant::now();
        let persisted = SyncStateStore::new(&self.database)
            .with_session_generation(self.session_generation)
            .with_diagnostics(&self.diagnostics)
            .apply_transition_atomic(current, next)
            .await;
        self.diagnostics.storage.record(storage_started);
        if persisted.is_err() {
            self.diagnostics
                .storage_errors
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |v| Some(v.saturating_add(1)),
                )
                .ok();
        }
        #[cfg(test)]
        if persisted.is_ok()
            && self
                .lose_next_persist_ack
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            invalidate_runtime(slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        if matches!(
            &persisted,
            Err(SyncStateError::CommitUnknown(_) | SyncStateError::RollbackFailed { .. })
        ) {
            invalidate_runtime(slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        if self.ensure_session().is_err() {
            invalidate_runtime(slot);
            return Err(NativeSyncError::RecoveryRequired);
        }
        persisted.map_err(map_persistence_error)?;
        let Some(runtime) = slot.as_mut() else {
            invalidate_runtime(slot);
            return Err(NativeSyncError::RecoveryRequired);
        };
        let batch = match runtime.install_with_read_batch(prepared) {
            Ok(batch) => batch,
            Err(_) => {
                invalidate_runtime(slot);
                return Err(NativeSyncError::RecoveryRequired);
            }
        };
        let status = match status(runtime) {
            Ok(status) => status,
            Err(_) => {
                invalidate_runtime(slot);
                return Err(NativeSyncError::RecoveryRequired);
            }
        };
        slot.journal.record(batch);
        // Query replay is installed only after the same durable transaction and runtime install.
        let RuntimeState {
            runtime,
            subscription_journal,
            ..
        } = slot;
        let Some(runtime) = runtime.as_ref() else {
            invalidate_runtime(slot);
            return Err(NativeSyncError::RecoveryRequired);
        };
        subscription_journal.record(runtime, before_revision);
        Ok(status)
    }
}

fn conflict_context(
    runtime: &ClientRuntime<JsonValue>,
    conflicted_commit_id: &ClientCommitId,
) -> Result<(ClientCommit<JsonValue>, RecordId, RecordState<JsonValue>), NativeSyncError> {
    runtime
        .state()
        .outcomes
        .iter()
        .find_map(|resolved| {
            if resolved.local_commit.identity.client_commit_id != *conflicted_commit_id {
                return None;
            }
            match &resolved.outcome {
                DurableOutcome::Conflict {
                    record_id,
                    authoritative,
                    ..
                } => Some((
                    resolved.local_commit.clone(),
                    record_id.clone(),
                    authoritative.clone(),
                )),
                DurableOutcome::Accepted { .. } | DurableOutcome::Rejected { .. } => None,
            }
        })
        .ok_or(NativeSyncError::Protocol)
}

fn rebase_conflict_operation(
    commit: &mut ClientCommit<JsonValue>,
    conflict_record_id: &RecordId,
    authoritative: &RecordState<JsonValue>,
) -> Result<(), NativeSyncError> {
    let operation = commit
        .operations
        .iter_mut()
        .find(|operation| operation.record_id() == conflict_record_id)
        .ok_or(NativeSyncError::Protocol)?;
    match (operation, authoritative) {
        (Operation::Upsert { base_version, .. }, RecordState::Absent) => {
            *base_version = BaseVersion::Absent;
        }
        (
            Operation::Upsert { base_version, .. },
            RecordState::Present { version, .. } | RecordState::Tombstone { version },
        ) => *base_version = BaseVersion::Exact(*version),
        (Operation::Delete { base_version, .. }, RecordState::Present { version, .. }) => {
            *base_version = *version;
        }
        (Operation::Delete { .. }, RecordState::Absent | RecordState::Tombstone { .. }) => {
            return Err(NativeSyncError::Protocol);
        }
    }
    Ok(())
}

fn decode_input<T: DeserializeOwned>(input: &str) -> Result<T, NativeSyncError> {
    if input.len() > MAX_SYNC_STATE_BYTES {
        return Err(NativeSyncError::InvalidInput);
    }
    serde_json::from_str(input).map_err(|_| NativeSyncError::InvalidInput)
}

fn encode_output<T: serde::Serialize>(value: &T) -> Result<String, NativeSyncError> {
    serde_json::to_string(value).map_err(|_| NativeSyncError::Internal)
}

fn status(runtime: &ClientRuntime<JsonValue>) -> Result<NativeSyncStatus, NativeSyncError> {
    let state = runtime.state();
    let pending_count = u32::try_from(state.outbox.len()).map_err(|_| NativeSyncError::Internal)?;
    let outcome_count =
        u32::try_from(state.outcomes.len()).map_err(|_| NativeSyncError::Internal)?;
    let conflict_count = u32::try_from(
        state
            .outcomes
            .iter()
            .filter(|resolved| {
                resolved.resolution.is_none()
                    && matches!(&resolved.outcome, DurableOutcome::Conflict { .. })
            })
            .count(),
    )
    .map_err(|_| NativeSyncError::Internal)?;
    let cursor = state
        .checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.cursor);
    Ok(NativeSyncStatus {
        revision: state.revision,
        pending_count,
        outcome_count,
        conflict_count,
        cursor_epoch: cursor.map(|cursor| cursor.epoch),
        cursor_sequence: cursor.map(|cursor| cursor.sequence),
    })
}

fn map_protocol_error(error: ClientError) -> NativeSyncError {
    match error {
        ClientError::SubscriptionCancelled => NativeSyncError::SubscriptionCancelled,
        ClientError::InvalidSubscription => NativeSyncError::InvalidSubscription,
        ClientError::NeedsNewScopeJournal => NativeSyncError::NeedsNewScopeJournal,
        ClientError::PendingQuarantineRequired => NativeSyncError::PendingQuarantineRequired,
        _ => NativeSyncError::Protocol,
    }
}

fn map_persistence_error(error: SyncStateError) -> NativeSyncError {
    match error {
        SyncStateError::IdentityTooLarge
        | SyncStateError::StateTooLarge
        | SyncStateError::UnsupportedRecordId
        | SyncStateError::RecordValueMustBeObject
        | SyncStateError::ValueCodec(_) => NativeSyncError::InvalidInput,
        SyncStateError::InvalidState(_) => NativeSyncError::Protocol,
        SyncStateError::RevisionMismatch
        | SyncStateError::RevisionConflict
        | SyncStateError::CorruptState
        | SyncStateError::Encode(_)
        | SyncStateError::Decode(_)
        | SyncStateError::Database(_)
        | SyncStateError::Core(_)
        | SyncStateError::RollbackFailed { .. } => NativeSyncError::Persistence,
        SyncStateError::CommitUnknown(_) => NativeSyncError::RecoveryRequired,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use surrealdb::types::{Number as SurrealNumber, RecordId as NativeRecordId, Value};
    use surrealdb_sync_protocol::{
        AppliedRecord, BaseVersion, Checkpoint, ClientCommitId, CommitIdentity, Cursor,
        Fingerprint, IdMapping, OpaqueCheckpoint, Operation, PullBatch, PullCommit, PullFrame,
        RecordId, RecordState, SchemaVersion, ScopeSnapshot,
    };

    use super::*;
    use crate::{ConnectOptions, connect};

    fn options(scope: &str) -> NativeSyncOpenOptions {
        NativeSyncOpenOptions {
            partition_id: "partition".into(),
            client_id: "client".into(),
            requested_scope: scope.into(),
            subscription_revision: 1,
        }
    }

    fn commit() -> ClientCommit<JsonValue> {
        ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("commit-1".into()),
                fingerprint: Fingerprint("fingerprint-1".into()),
            },
            operations: vec![Operation::Upsert {
                record_id: RecordId("person:ada".into()),
                base_version: BaseVersion::Absent,
                value: json!({
                    "name": "Ada",
                    "friend": {"$surreal": "record", "value": "person:bob"}
                }),
                reference: Some(RecordId("person:bob".into())),
            }],
        }
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            token: OpaqueCheckpoint("checkpoint-1".into()),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            scope: ScopeSnapshot {
                identity: ScopeIdentity("all".into()),
                authorization_revision: 1,
                subscription_revision: 1,
            },
        }
    }

    async fn database() -> Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("native_sync_client".into()),
            database: Some("native_sync_client".into()),
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn inspection_rejects_unavailable_and_nonembedded_database() {
        let closed = database().await;
        closed.close().await.unwrap();
        assert!(inspect_sync_state(closed, options("all")).await.is_err());
        let mut database = Arc::try_unwrap(database().await).ok().unwrap();
        database.embedded = false;
        let database = Arc::new(database);
        assert!(matches!(
            inspect_sync_state(database.clone(), options("all")).await,
            Err(NativeSyncError::RequiresEmbeddedDatabase)
        ));
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn inspection_counts_survive_reopen_and_scope_barrier_without_mutation() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        client
            .enqueue(serde_json::to_string(&commit()).unwrap())
            .await
            .unwrap();
        let pending: ClientCommit<JsonValue> =
            serde_json::from_str(&client.pending_json().await.unwrap()[0]).unwrap();
        client
            .record_push_response(
                serde_json::to_string(&PushResponse::<JsonValue> {
                    schema_version: SchemaVersion::V1,
                    partition_id: PartitionId("partition".into()),
                    client_id: ClientId("client".into()),
                    outcome: DurableOutcome::Conflict {
                        identity: pending.identity,
                        record_id: RecordId("person:ada".into()),
                        authoritative: RecordState::Absent,
                    },
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let first = inspect_sync_state(database.clone(), options("all"))
            .await
            .unwrap();
        assert!(matches!(
            first,
            NativeSyncInspection::Present {
                pending_count: 0,
                outcome_count: 1,
                conflict_count: 1,
                ..
            }
        ));
        client
            .resolve_conflict_keep_local("commit-1".into(), "commit-2".into())
            .await
            .unwrap();
        let store = SyncStateStore::new(&database);
        let partition = PartitionId("partition".into());
        let account = ClientId("client".into());
        let key = SyncStateKey::new(&partition, &account);
        store
            .save_scope_barrier(
                key,
                ScopeBarrier {
                    reason: "pending_quarantine_required".into(),
                    required_scope: "new".into(),
                    required_subscription_revision: "2".into(),
                    required_authorization_revision: "3".into(),
                },
            )
            .await
            .unwrap();
        let before = store.inspect(key).await.unwrap().0.unwrap();
        let expected = NativeSyncInspection::Present {
            revision: before.revision,
            pending_count: 1,
            outcome_count: 1,
            conflict_count: 0,
            barrier: NativeSyncInspectionBarrier::PendingQuarantineRequired,
        };
        assert_eq!(
            inspect_sync_state(database.clone(), options("all"))
                .await
                .unwrap(),
            expected
        );
        assert_eq!(store.inspect(key).await.unwrap().0.unwrap(), before);
        assert_eq!(
            store.load_scope_barrier(key).await.unwrap().unwrap().reason,
            "pending_quarantine_required"
        );
        for changed in [
            NativeSyncOpenOptions {
                requested_scope: "other".into(),
                ..options("all")
            },
            NativeSyncOpenOptions {
                subscription_revision: 2,
                ..options("all")
            },
        ] {
            assert!(matches!(
                inspect_sync_state(database.clone(), changed).await,
                Err(NativeSyncError::Protocol)
            ));
        }
        for changed in [
            NativeSyncOpenOptions {
                partition_id: "other".into(),
                ..options("all")
            },
            NativeSyncOpenOptions {
                client_id: "other-account".into(),
                ..options("all")
            },
        ] {
            assert_eq!(
                inspect_sync_state(database.clone(), changed).await.unwrap(),
                NativeSyncInspection::Missing
            );
        }
        client.close().await;
        database.close().await.unwrap();
        drop(client);
        drop(database);
        let reopened = surrealkv_database(directory.path()).await;
        assert_eq!(
            inspect_sync_state(reopened.clone(), options("all"))
                .await
                .unwrap(),
            expected
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_native_atomic_cancel_reopen_and_scope_barriers() {
        let directory = temp_dir::TempDir::new().unwrap();
        let db = surrealkv_database(directory.path()).await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        client
            .enqueue(serde_json::to_string(&commit()).unwrap())
            .await
            .unwrap();
        let v1_before = client
            .lock_runtime()
            .await
            .client()
            .unwrap()
            .state()
            .clone();
        let initial: JsonValue = serde_json::from_str(
            &client
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        let v1_cursor: JsonValue =
            serde_json::from_str(&client.read_snapshot_json().await.unwrap()).unwrap();
        client
            .start_subscription_json(crate::sync_subscription::test_request().to_string())
            .await
            .unwrap();
        client
            .apply_subscription_response_json(crate::sync_subscription::test_response().to_string())
            .await
            .unwrap();
        let v1_events: JsonValue = serde_json::from_str(
            &client
                .read_changes_json(v1_cursor["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v1_events["batches"].as_array().unwrap().len(), 2);
        assert!(
            v1_events["batches"]
                .as_array()
                .unwrap()
                .iter()
                .all(|batch| batch["changes"].as_array().unwrap().is_empty())
        );
        let installed: JsonValue = serde_json::from_str(
            &client
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(installed["snapshot"]["rows"].as_array().unwrap().len(), 2);
        assert_eq!(
            installed["snapshot"]["metadata"]["revision"],
            installed["cursor"]["revision"]
        );
        assert!(installed["snapshot"]["pendingOutsideWindow"].is_null());
        let changes: JsonValue = serde_json::from_str(
            &client
                .subscription_changes_json("window".into(), initial["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(changes["batches"].as_array().unwrap().len(), 2);
        let state = client
            .lock_runtime()
            .await
            .client()
            .unwrap()
            .state()
            .clone();
        assert_eq!(state.confirmed, v1_before.confirmed);
        assert_eq!(state.outbox, v1_before.outbox);
        assert_eq!(state.reset_coverage, v1_before.reset_coverage);
        client.close().await;
        db.close().await.unwrap();
        drop(client);
        drop(db);
        let db = surrealkv_database(directory.path()).await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        let recovered: JsonValue = serde_json::from_str(
            &client
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(recovered["snapshot"], installed["snapshot"]);
        let delayed = crate::sync_subscription::test_response().to_string();
        let (response, cancel) = tokio::join!(
            client.apply_subscription_response_json(delayed.clone()),
            client.cancel_subscription("window".into(), 1)
        );
        assert!(
            response.is_ok() || matches!(response, Err(NativeSyncError::SubscriptionCancelled))
        );
        cancel.unwrap();
        assert!(matches!(
            client.apply_subscription_response_json(delayed).await,
            Err(NativeSyncError::SubscriptionCancelled)
        ));
        client.close().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        let cancelled: JsonValue = serde_json::from_str(
            &client
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(cancelled["snapshot"].is_null());
        assert_eq!(cancelled["cancelledThrough"], "1");
        if std::env::var_os("SURREALDB_EXPORT_WINDOW_FIXTURES").is_some() {
            println!(
                "WINDOW_FIXTURE_JSON={}",
                json!({"installed":installed,"changes":changes,"cancelled":cancelled})
            );
        }
        let mut next = crate::sync_subscription::test_request();
        next["command"]["generation"] = json!("2");
        client
            .start_subscription_json(next.to_string())
            .await
            .unwrap();
        let mut response = crate::sync_subscription::test_response();
        response["generation"] = json!("2");
        client
            .apply_subscription_response_json(response.to_string())
            .await
            .unwrap();
        let second = open_sync_client(db.clone(), options("all")).await.unwrap();
        response["scope"]["authorizationRevision"] = json!("2");
        response["viewRevision"] = json!("2");
        assert!(matches!(
            client
                .apply_subscription_response_json(response.to_string())
                .await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert!(matches!(
            second.subscription_snapshot_json("window".into()).await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert!(matches!(
            second.subscription_request_json("window".into()).await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert_eq!(client.pending_json().await.unwrap().len(), 1);
        client.close().await;
        second.close().await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn subscription_lost_ack_has_no_emission_and_recovers_durable_cancel() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        client
            .start_subscription_json(crate::sync_subscription::test_request().to_string())
            .await
            .unwrap();
        client
            .apply_subscription_response_json(crate::sync_subscription::test_response().to_string())
            .await
            .unwrap();
        let before: JsonValue = serde_json::from_str(
            &client
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        client
            .lose_next_persist_ack
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            client.cancel_subscription("window".into(), 1).await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            client
                .subscription_changes_json("window".into(), before["cursor"].to_string(), 128)
                .await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        let durable = SyncStateStore::new(&db)
            .load(SyncStateKey::new(
                &PartitionId("partition".into()),
                &ClientId("client".into()),
            ))
            .await
            .unwrap()
            .unwrap();
        let recovered = ClientRuntime::open(durable).unwrap();
        let slot = client.lock_runtime().await;
        let replay = slot
            .subscription_journal
            .changes(&recovered, "window", &before["cursor"].to_string(), 128)
            .unwrap();
        assert_eq!(replay["kind"], "reset_required");
        drop(slot);
        let reopened = open_sync_client(db, options("all")).await.unwrap();
        let frame: JsonValue = serde_json::from_str(
            &reopened
                .subscription_snapshot_json("window".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(frame["snapshot"].is_null());
        assert_eq!(frame["cancelledThrough"], "1");
    }

    #[tokio::test]
    async fn lost_persist_ack_emits_nothing_and_reopen_recovers_commit() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        let initial: JsonValue =
            serde_json::from_str(&client.read_snapshot_json().await.unwrap()).unwrap();
        client
            .lose_next_persist_ack
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            client
                .enqueue(serde_json::to_string(&commit()).unwrap())
                .await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            client.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        let slot = client.lock_runtime().await;
        let durable = SyncStateStore::new(&db)
            .load(SyncStateKey::new(
                &PartitionId("partition".into()),
                &ClientId("client".into()),
            ))
            .await
            .unwrap()
            .unwrap();
        let recovered = ClientRuntime::open(durable).unwrap();
        let replay = slot
            .journal
            .changes(&recovered, &initial["cursor"].to_string(), 128)
            .unwrap();
        assert_eq!(replay["kind"], "reset_required");
        assert!(replay.get("batches").is_none());
        drop(slot);
        let reopened = open_sync_client(db, options("all")).await.unwrap();
        assert_eq!(reopened.status().await.unwrap().pending_count, 1);
        let snapshot: JsonValue =
            serde_json::from_str(&reopened.read_snapshot_json().await.unwrap()).unwrap();
        assert_eq!(snapshot["cursor"]["revision"], "1");
    }

    #[tokio::test]
    async fn racing_bootstrap_and_multirow_commit_never_exposes_half_a_batch() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        let start: JsonValue =
            serde_json::from_str(&client.read_snapshot_json().await.unwrap()).unwrap();
        let mut multi = commit();
        let mut other = multi.operations[0].clone();
        if let Operation::Upsert { record_id, .. } = &mut other {
            *record_id = RecordId("person:grace".into());
        }
        multi.operations.push(other);
        let encoded = serde_json::to_string(&multi).unwrap();
        let (snapshot, queued) = tokio::join!(client.read_snapshot_json(), client.enqueue(encoded));
        queued.unwrap();
        let snapshot: JsonValue = serde_json::from_str(&snapshot.unwrap()).unwrap();
        let row_count = snapshot["snapshot"]["rows"].as_object().unwrap().len();
        assert!(row_count == 0 || row_count == 2);
        assert_eq!(
            snapshot["cursor"]["revision"],
            if row_count == 0 { "0" } else { "1" }
        );
        let changes: JsonValue = serde_json::from_str(
            &client
                .read_changes_json(start["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(changes["batches"].as_array().unwrap().len(), 1);
        assert_eq!(
            changes["batches"][0]["changes"].as_array().unwrap().len(),
            2
        );
        let mut next = commit();
        next.identity.client_commit_id = ClientCommitId("failed-storage".into());
        db.close().await.unwrap();
        assert!(
            client
                .enqueue(serde_json::to_string(&next).unwrap())
                .await
                .is_err()
        );
        let slot = client.lock_runtime().await;
        let after = slot
            .journal
            .changes(
                slot.guard.client().unwrap(),
                &changes["cursor"].to_string(),
                128,
            )
            .unwrap();
        assert!(after["batches"].as_array().unwrap().is_empty());
        assert_eq!(after["cursor"], changes["cursor"]);
    }

    #[tokio::test]
    async fn barrier_storage_failure_invalidates_read_handle() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        db.close().await.unwrap();
        assert!(matches!(
            client.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            client.read_changes_json("{}".into(), 1).await,
            Err(NativeSyncError::RecoveryRequired)
        ));
    }

    async fn surrealkv_database(path: &std::path::Path) -> Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: format!("surrealkv://{}", path.display()),
            namespace: Some("native_sync_authority".into()),
            database: Some("native_sync_authority".into()),
        })
        .await
        .unwrap()
    }

    fn decode_hex(encoded: &str) -> Vec<u8> {
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
    async fn native_read_cursor_is_atomic_durable_and_explicit_about_terminal_states() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert!(client.requires_authorized_reset().await.unwrap());
        let bootstrap: JsonValue =
            serde_json::from_str(&client.read_snapshot_json().await.unwrap()).unwrap();
        let cursor = bootstrap["cursor"].to_string();
        let mut one = commit();
        let mut two = commit();
        one.identity.client_commit_id = ClientCommitId("read-one".into());
        two.identity.client_commit_id = ClientCommitId("read-two".into());
        let (a, b) = tokio::join!(
            client.enqueue(serde_json::to_string(&one).unwrap()),
            client.enqueue(serde_json::to_string(&two).unwrap())
        );
        a.unwrap();
        b.unwrap();
        let first: JsonValue =
            serde_json::from_str(&client.read_changes_json(cursor.clone(), 1).await.unwrap())
                .unwrap();
        assert_eq!(first["batches"].as_array().unwrap().len(), 1);
        assert_eq!(first["batches"][0]["beforeRevision"], "0");
        assert_eq!(first["batches"][0]["revision"], "1");
        let second: JsonValue = serde_json::from_str(
            &client
                .read_changes_json(first["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second["batches"].as_array().unwrap().len(), 1);
        assert_eq!(second["batches"][0]["revision"], "2");
        let before = client.read_snapshot_json().await.unwrap();
        let mut invalid_projection = commit();
        invalid_projection.identity.client_commit_id = ClientCommitId("invalid-projection".into());
        if let Operation::Upsert { value, .. } = &mut invalid_projection.operations[0] {
            *value = json!(["not a database object"]);
        }
        assert!(matches!(
            client
                .enqueue(serde_json::to_string(&invalid_projection).unwrap())
                .await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert_eq!(client.read_snapshot_json().await.unwrap(), before);
        let unchanged: JsonValue = serde_json::from_str(
            &client
                .read_changes_json(second["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(unchanged["batches"].as_array().unwrap().is_empty());
        assert!(client.apply_pull_response("{}".into()).await.is_err());
        assert_eq!(client.read_snapshot_json().await.unwrap(), before);
        assert_eq!(
            client
                .scope_transition_decision("other".into(), 1)
                .await
                .unwrap(),
            "pending_quarantine_required"
        );
        let reset = surrealdb_sync_protocol::PullResponse::<JsonValue>::Reset(
            surrealdb_sync_protocol::ResetSnapshot {
                reason: surrealdb_sync_protocol::ResetReason::EpochChanged,
                checkpoint: checkpoint(),
                records: vec![],
            },
        );
        client
            .apply_pull_response(serde_json::to_string(&reset).unwrap())
            .await
            .unwrap();
        assert!(!client.requires_authorized_reset().await.unwrap());
        let reset_snapshot: JsonValue =
            serde_json::from_str(&client.read_snapshot_json().await.unwrap()).unwrap();
        client
            .apply_pull_response(serde_json::to_string(&reset).unwrap())
            .await
            .unwrap();
        let no_op: JsonValue = serde_json::from_str(
            &client
                .read_changes_json(reset_snapshot["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(no_op["batches"].as_array().unwrap().is_empty());
        assert!(
            !client
                .read_snapshot_json()
                .await
                .unwrap()
                .contains("checkpoint-1")
        );
        let second = open_sync_client(db.clone(), options("all")).await.unwrap();
        let cross_handle: JsonValue =
            serde_json::from_str(&second.read_changes_json(cursor.clone(), 128).await.unwrap())
                .unwrap();
        assert!(Arc::ptr_eq(&client.runtime, &second.runtime));
        assert_eq!(cross_handle["kind"], "batches");
        let pending_before = client.pending_json().await.unwrap();
        let mut blocked_reset = reset.clone();
        if let surrealdb_sync_protocol::PullResponse::Reset(snapshot) = &mut blocked_reset {
            snapshot.checkpoint.scope.authorization_revision += 1;
        }
        let failing_db = database().await;
        let failing = open_sync_client(failing_db.clone(), options("all"))
            .await
            .unwrap();
        {
            let state = client
                .lock_runtime()
                .await
                .client()
                .unwrap()
                .state()
                .clone();
            failing.lock_runtime().await.runtime = Some(ClientRuntime::open(state).unwrap());
        }
        failing_db.close().await.unwrap();
        assert!(matches!(
            failing
                .apply_pull_response(serde_json::to_string(&blocked_reset).unwrap())
                .await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            failing.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            client
                .apply_pull_response(serde_json::to_string(&blocked_reset).unwrap())
                .await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert!(matches!(
            client.read_snapshot_json().await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert!(matches!(
            client.read_changes_json(cursor.clone(), 128).await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert_eq!(client.pending_json().await.unwrap(), pending_before);
        assert!(matches!(
            second.read_snapshot_json().await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        client.close().await;
        assert!(matches!(
            client.read_snapshot_json().await,
            Err(NativeSyncError::Closed)
        ));
        let reopened = open_sync_client(db, options("all")).await.unwrap();
        assert!(matches!(
            reopened.requires_authorized_reset().await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        assert_eq!(reopened.status().await.unwrap().pending_count, 2);
        assert!(matches!(
            reopened.read_changes_json(cursor, 128).await,
            Err(NativeSyncError::PendingQuarantineRequired)
        ));
        {
            let mut slot = reopened.lock_runtime().await;
            slot.runtime.take();
            slot.recovery_required = true;
        }
        assert!(matches!(
            reopened.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        reopened.close().await;
        assert!(matches!(
            reopened.read_snapshot_json().await,
            Err(NativeSyncError::Closed)
        ));
        assert!(matches!(
            map_protocol_error(ClientError::PendingQuarantineRequired),
            NativeSyncError::PendingQuarantineRequired
        ));
        assert!(matches!(
            map_protocol_error(ClientError::NeedsNewScopeJournal),
            NativeSyncError::NeedsNewScopeJournal
        ));
    }

    #[tokio::test]
    async fn native_diagnostics_are_metadata_only_and_do_not_wait_for_runtime_lock() {
        let database = database().await;
        let client = open_sync_client(
            database,
            NativeSyncOpenOptions {
                partition_id: "private-partition".into(),
                client_id: "private-user".into(),
                requested_scope: "private-scope".into(),
                subscription_revision: 1,
            },
        )
        .await
        .unwrap();
        let guard = client.lock_runtime().await;
        let snapshot = client.diagnostics();
        assert_eq!(snapshot.schema_version, 1);
        assert_eq!(snapshot.runtime_lock_acquisitions, 1);
        assert_eq!(snapshot.storage_attempts, 0);
        let debug = format!("{snapshot:?}");
        assert!(!debug.contains("private"));
        drop(guard);
        let before = client.status().await.unwrap();
        for _ in 0..10_000 {
            let _ = client.diagnostics();
        }
        assert_eq!(before, client.status().await.unwrap());
        assert!(client.diagnostics().runtime_lock_hold_ns > 0);
        client.close().await;
        assert_eq!(client.diagnostics().schema_version, 1);
    }

    #[tokio::test]
    async fn native_facade_persists_enqueue_reopen_and_conflict_outcome() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(client.checkpoint_token().await.unwrap(), None);
        assert_eq!(
            client.status().await.unwrap(),
            NativeSyncStatus {
                revision: 0,
                pending_count: 0,
                outcome_count: 0,
                conflict_count: 0,
                cursor_epoch: None,
                cursor_sequence: None,
            }
        );

        let commit = commit();
        let status = client
            .enqueue(serde_json::to_string(&commit).unwrap())
            .await
            .unwrap();
        assert_eq!(status.revision, 1);
        assert_eq!(status.pending_count, 1);
        let diagnostics = client.diagnostics();
        assert_eq!(diagnostics.storage_attempts, 1);
        assert_eq!(diagnostics.storage_errors, 0);
        assert_eq!(diagnostics.state_encoding_attempts, 1);
        assert!(diagnostics.storage_ns > 0);
        assert!(diagnostics.state_encoding_ns > 0);
        let span = diagnostics.latest_completed_operation.unwrap();
        assert!(span.operation_ordinal > 0);
        assert!(span.storage_ns > 0);
        assert!(span.state_encoding_ns > 0);
        let pending = client.pending_json().await.unwrap();
        assert_eq!(pending.len(), 1);
        let pending_commit: ClientCommit<JsonValue> = serde_json::from_str(&pending[0]).unwrap();
        assert!(pending_commit.identity.fingerprint.0.starts_with("sha256:"));
        assert_ne!(
            pending_commit.identity.fingerprint,
            commit.identity.fingerprint
        );
        let database_client = database.client().await.unwrap();
        let record: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert!(matches!(record, Some(Value::Object(_))));
        drop(database_client);

        client.close().await;
        assert!(client.is_closed());
        assert!(matches!(
            client.status().await,
            Err(NativeSyncError::Closed)
        ));

        let reopened = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(reopened.status().await.unwrap().pending_count, 1);
        let response: PushResponse<JsonValue> = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            outcome: DurableOutcome::Conflict {
                identity: pending_commit.identity,
                record_id: RecordId("person:ada".into()),
                authoritative: RecordState::Absent,
            },
        };
        let status = reopened
            .record_push_response(serde_json::to_string(&response).unwrap())
            .await
            .unwrap();
        assert_eq!(status.pending_count, 0);
        assert_eq!(status.outcome_count, 1);
        assert_eq!(status.conflict_count, 1);
        assert_eq!(reopened.conflicts_json().await.unwrap().len(), 1);

        let database_client = database.client().await.unwrap();
        let record: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert_eq!(record, None);
    }

    #[tokio::test]
    async fn opt_in_prefer_server_is_durable_and_idempotent() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let mut local = commit();
        local.identity.client_commit_id = ClientCommitId("stale-upsert".into());
        let Operation::Upsert { base_version, .. } = &mut local.operations[0] else {
            unreachable!();
        };
        *base_version = BaseVersion::Exact(1);
        client
            .enqueue(serde_json::to_string(&local).unwrap())
            .await
            .unwrap();
        let pending: ClientCommit<JsonValue> =
            serde_json::from_str(&client.pending_json().await.unwrap()[0]).unwrap();
        let conflict = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            outcome: DurableOutcome::Conflict {
                identity: pending.identity,
                record_id: RecordId("person:ada".into()),
                authoritative: RecordState::Present {
                    value: json!({"name": "Server"}),
                    version: 2,
                    reference: None,
                },
            },
        };
        let response = serde_json::to_string(&conflict).unwrap();
        let status = client
            .record_push_response_with_policy(
                response.clone(),
                NativeConflictPolicy::PreferServerV1,
            )
            .await
            .unwrap();
        assert_eq!(status.conflict_count, 0);
        assert_eq!(status.outcome_count, 1);
        client.close().await;
        let reopened = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(reopened.status().await.unwrap().conflict_count, 0);
        let duplicate = reopened.record_push_response(response).await.unwrap();
        assert_eq!(duplicate.outcome_count, 1);
        assert_eq!(duplicate.conflict_count, 0);
    }

    #[tokio::test]
    async fn projection_uses_native_record_id_even_when_payload_has_a_logical_id() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let commit = ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("logical-id-1".into()),
                fingerprint: Fingerprint("computed-by-native".into()),
            },
            operations: vec![Operation::Upsert {
                record_id: RecordId("item:stable-1".into()),
                base_version: BaseVersion::Absent,
                value: json!({
                    "id": "stable-1",
                    "owner": "tenant-1",
                    "label": "Example"
                }),
                reference: None,
            }],
        };
        client
            .enqueue(serde_json::to_string(&commit).unwrap())
            .await
            .unwrap();
        let database_client = database.client().await.unwrap();
        let record: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("item:stable-1").unwrap())
            .await
            .unwrap();
        let Some(Value::Object(row)) = record else {
            panic!("optimistic row must be queryable");
        };
        assert!(matches!(
            row.get("id"),
            Some(Value::RecordId(id))
                if id == &NativeRecordId::parse_simple("item:stable-1").unwrap()
        ));
        assert!(
            matches!(row.get("owner"), Some(Value::String(owner)) if owner.as_str() == "tenant-1")
        );
    }

    #[tokio::test]
    async fn explicit_conflict_resolutions_persist_and_only_unresolved_conflicts_are_reported() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let original = commit();
        client
            .enqueue(serde_json::to_string(&original).unwrap())
            .await
            .unwrap();
        let pending: ClientCommit<JsonValue> =
            serde_json::from_str(&client.pending_json().await.unwrap()[0]).unwrap();
        let response: PushResponse<JsonValue> = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            outcome: DurableOutcome::Conflict {
                identity: pending.identity,
                record_id: RecordId("person:ada".into()),
                authoritative: RecordState::Absent,
            },
        };
        client
            .record_push_response(serde_json::to_string(&response).unwrap())
            .await
            .unwrap();

        let status = client
            .resolve_conflict_keep_local("commit-1".into(), "commit-2".into())
            .await
            .unwrap();
        assert_eq!(status.pending_count, 1);
        assert_eq!(status.outcome_count, 1);
        assert_eq!(status.conflict_count, 0);
        assert!(client.conflicts_json().await.unwrap().is_empty());

        let retry = client
            .resolve_conflict_keep_local("commit-1".into(), "commit-2".into())
            .await
            .unwrap();
        assert_eq!(retry.pending_count, 1);
        let retry_commit: ClientCommit<JsonValue> =
            serde_json::from_str(&client.pending_json().await.unwrap()[0]).unwrap();
        assert_eq!(retry_commit.identity.client_commit_id.0, "commit-2");
        assert!(retry_commit.identity.fingerprint.0.starts_with("sha256:"));
        assert!(matches!(
            retry_commit.operations[0],
            Operation::Upsert {
                base_version: BaseVersion::Absent,
                ..
            }
        ));

        client.close().await;
        let reopened = open_sync_client(database, options("all")).await.unwrap();
        let status = reopened.status().await.unwrap();
        assert_eq!(status.pending_count, 1);
        assert_eq!(status.conflict_count, 0);
    }

    #[tokio::test]
    async fn keep_server_and_merge_are_explicit_durable_native_actions() {
        async fn conflicted_client() -> Arc<NativeSyncClient> {
            let client = open_sync_client(database().await, options("all"))
                .await
                .unwrap();
            client
                .enqueue(serde_json::to_string(&commit()).unwrap())
                .await
                .unwrap();
            let pending: ClientCommit<JsonValue> =
                serde_json::from_str(&client.pending_json().await.unwrap()[0]).unwrap();
            client
                .record_push_response(
                    serde_json::to_string(&PushResponse::<JsonValue> {
                        schema_version: SchemaVersion::V1,
                        partition_id: PartitionId("partition".into()),
                        client_id: ClientId("client".into()),
                        outcome: DurableOutcome::Conflict {
                            identity: pending.identity,
                            record_id: RecordId("person:ada".into()),
                            authoritative: RecordState::Absent,
                        },
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            client
        }

        let keep_server = conflicted_client().await;
        let status = keep_server
            .resolve_conflict_keep_server("commit-1".into())
            .await
            .unwrap();
        assert_eq!(status.pending_count, 0);
        assert_eq!(status.conflict_count, 0);
        keep_server
            .resolve_conflict_keep_server("commit-1".into())
            .await
            .expect("an identical keep-server retry is idempotent");

        let merge = conflicted_client().await;
        let mut replacement = commit();
        replacement.identity.client_commit_id = ClientCommitId("merge-1".into());
        if let Operation::Upsert { value, .. } = &mut replacement.operations[0] {
            *value = json!({"name": "merged"});
        }
        let status = merge
            .resolve_conflict_merge(
                "commit-1".into(),
                serde_json::to_string(&replacement).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.pending_count, 1);
        assert_eq!(status.conflict_count, 0);
        let queued: ClientCommit<JsonValue> =
            serde_json::from_str(&merge.pending_json().await.unwrap()[0]).unwrap();
        assert_eq!(queued.identity.client_commit_id.0, "merge-1");
        assert_eq!(queued.operations[0], replacement.operations[0]);
        assert_ne!(
            queued.identity.fingerprint,
            replacement.identity.fingerprint
        );
    }

    #[tokio::test]
    async fn fractional_recipe_value_survives_surrealkv_reopen() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let mut fractional = commit();
        if let Operation::Upsert {
            record_id, value, ..
        } = &mut fractional.operations[0]
        {
            *record_id = RecordId("recipe:oatmeal".into());
            *value = json!({"title": "Oatmeal", "servings": 2.5, "proteinGrams": 13.75});
        }

        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        client
            .enqueue(serde_json::to_string(&fractional).unwrap())
            .await
            .unwrap();
        client.close().await;
        database.close().await.unwrap();
        drop(database);

        let reopened_database = surrealkv_database(directory.path()).await;
        let reopened = open_sync_client(reopened_database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(reopened.status().await.unwrap().pending_count, 1);
        let record: Option<Value> = reopened_database
            .client()
            .await
            .unwrap()
            .select(NativeRecordId::parse_simple("recipe:oatmeal").unwrap())
            .await
            .unwrap();
        let Value::Object(record) = record.unwrap() else {
            panic!("expected recipe object");
        };
        assert_eq!(
            record.get("servings"),
            Some(&Value::Number(SurrealNumber::Float(2.5)))
        );
        assert_eq!(
            record.get("proteinGrams"),
            Some(&Value::Number(SurrealNumber::Float(13.75)))
        );
    }

    #[tokio::test]
    async fn accepted_id_mapping_preserves_original_identity_across_reopen() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let mut local = commit();
        local.identity.client_commit_id = ClientCommitId("commit-mapped".into());
        let Operation::Upsert { record_id, .. } = &mut local.operations[0] else {
            unreachable!();
        };
        *record_id = RecordId("temp:ada".into());

        client
            .enqueue(serde_json::to_string(&local).unwrap())
            .await
            .unwrap();
        let pending = client.pending_json().await.unwrap();
        let pending: ClientCommit<JsonValue> = serde_json::from_str(&pending[0]).unwrap();
        let response: PushResponse<JsonValue> = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            outcome: DurableOutcome::Accepted {
                identity: pending.identity,
                sequence: 1,
                id_mappings: vec![IdMapping {
                    local_id: RecordId("temp:ada".into()),
                    canonical_id: RecordId("person:server-ada".into()),
                }],
            },
        };

        let status = client
            .record_push_response(serde_json::to_string(&response).unwrap())
            .await
            .unwrap();
        assert_eq!(status.pending_count, 0);
        assert_eq!(status.outcome_count, 1);
        client.close().await;

        let reopened = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(reopened.status().await.unwrap().outcome_count, 1);
        let database_client = database.client().await.unwrap();
        let local: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("temp:ada").unwrap())
            .await
            .unwrap();
        let canonical: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:server-ada").unwrap())
            .await
            .unwrap();
        assert_eq!(local, None);
        assert!(matches!(canonical, Some(Value::Object(_))));
    }

    #[tokio::test]
    async fn authority_adapter_response_persists_across_surrealkv_reopen() {
        const ACCEPTED_RESPONSE: &str = "84707375727265616c64622d73796e632f310001836e666978747572652d676f6c64656e66636c69656e7484008277636f6d6d69742d617574686f726974792d676f6c64656e78477368613235363a666531633736356461616435356432646536323036353836373431353134346338393066656163316463353330326366306164306263356262636637663536300180";
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let open_options = NativeSyncOpenOptions {
            partition_id: "fixture-golden".into(),
            client_id: "client".into(),
            requested_scope: "all".into(),
            subscription_revision: 1,
        };
        let client = open_sync_client(database.clone(), open_options.clone())
            .await
            .unwrap();
        let commit = ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("commit-authority-golden".into()),
                fingerprint: Fingerprint("recomputed-by-native".into()),
            },
            operations: vec![Operation::Upsert {
                record_id: RecordId("person:ada".into()),
                base_version: BaseVersion::Absent,
                value: json!({"name": "Ada"}),
                reference: None,
            }],
        };
        client
            .enqueue(serde_json::to_string(&commit).unwrap())
            .await
            .unwrap();
        let pending = client.pending_json().await.unwrap();
        let pending: ClientCommit<JsonValue> = serde_json::from_str(&pending[0]).unwrap();
        assert_eq!(
            pending.identity.fingerprint.0,
            "sha256:fe1c765daad55d2de62065867415144c890feac1dc5302cf0ad0bc5bbcf7f560"
        );

        let response =
            crate::sync_http_codec::decode_sync_push_response(decode_hex(ACCEPTED_RESPONSE))
                .await
                .unwrap();
        let status = client.record_push_response(response).await.unwrap();
        assert_eq!(status.pending_count, 0);
        assert_eq!(status.outcome_count, 1);
        client.close().await;
        database.close().await.unwrap();
        drop(database);

        let reopened_database = surrealkv_database(directory.path()).await;
        let reopened = open_sync_client(reopened_database.clone(), open_options)
            .await
            .unwrap();
        assert_eq!(reopened.status().await.unwrap().pending_count, 0);
        assert_eq!(reopened.status().await.unwrap().outcome_count, 1);
        let record: Option<Value> = reopened_database
            .client()
            .await
            .unwrap()
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert!(matches!(record, Some(Value::Object(_))));
    }

    #[tokio::test]
    async fn native_facade_rejects_scope_drift_and_malformed_input_without_mutation() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert!(matches!(
            client.enqueue("{not-json".into()).await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert_eq!(client.status().await.unwrap().revision, 0);

        let mut unsupported = commit();
        if let Operation::Upsert { value, .. } = &mut unsupported.operations[0] {
            *value = json!({"unsupportedDecimal": {"$surreal": "decimal", "value": "1.50"}});
        }
        assert!(matches!(
            client
                .enqueue(serde_json::to_string(&unsupported).unwrap())
                .await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert_eq!(client.status().await.unwrap().revision, 0);

        let hostile_push = PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            outcome: DurableOutcome::Conflict {
                identity: commit().identity,
                record_id: RecordId("person:ada".into()),
                authoritative: RecordState::Present {
                    value: json!({"$surreal": "decimal", "value": "1.50"}),
                    version: 1,
                    reference: None,
                },
            },
        };
        assert!(matches!(
            client
                .record_push_response(serde_json::to_string(&hostile_push).unwrap())
                .await,
            Err(NativeSyncError::InvalidInput)
        ));

        let checkpoint = checkpoint();
        let hostile_pull = PullResponse::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: checkpoint.clone(),
                },
                PullFrame::Commit {
                    checkpoint: checkpoint.token.clone(),
                    commit: PullCommit {
                        sequence: 1,
                        source: None,
                        records: vec![AppliedRecord {
                            record_id: RecordId("person:lin".into()),
                            state: RecordState::Present {
                                value: json!({"$surreal": "decimal", "value": "1.50"}),
                                version: 1,
                                reference: None,
                            },
                        }],
                    },
                },
                PullFrame::End { checkpoint },
            ],
        });
        assert!(matches!(
            client
                .apply_pull_response(serde_json::to_string(&hostile_pull).unwrap())
                .await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert_eq!(client.status().await.unwrap().revision, 0);

        assert!(matches!(
            open_sync_client(database, options("different")).await,
            Err(NativeSyncError::Protocol)
        ));
    }

    #[tokio::test]
    async fn native_facade_applies_complete_pull_and_advances_cursor_atomically() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let checkpoint = checkpoint();
        let response = PullResponse::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: checkpoint.clone(),
                },
                PullFrame::Commit {
                    checkpoint: checkpoint.token.clone(),
                    commit: PullCommit {
                        sequence: 1,
                        source: None,
                        records: vec![AppliedRecord {
                            record_id: RecordId("person:lin".into()),
                            state: RecordState::Present {
                                value: json!({"name": "Lin"}),
                                version: 1,
                                reference: None,
                            },
                        }],
                    },
                },
                PullFrame::End { checkpoint },
            ],
        });

        let status = client
            .apply_pull_response(serde_json::to_string(&response).unwrap())
            .await
            .unwrap();
        assert_eq!(status.cursor_epoch, Some(1));
        assert_eq!(status.cursor_sequence, Some(1));
        assert_eq!(
            client.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-1")
        );
        let database_client = database.client().await.unwrap();
        let record: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:lin").unwrap())
            .await
            .unwrap();
        assert!(matches!(record, Some(Value::Object(_))));
        drop(database_client);

        client.close().await;
        let reopened = open_sync_client(database, options("all")).await.unwrap();
        assert_eq!(
            reopened.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-1")
        );
    }

    #[tokio::test]
    async fn empty_pull_rotates_durable_native_checkpoint_before_next_commit() {
        let database = database().await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let initial = checkpoint();
        let first = PullResponse::<JsonValue>::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: initial.clone(),
                },
                PullFrame::Commit {
                    checkpoint: initial.token.clone(),
                    commit: PullCommit {
                        sequence: 1,
                        source: None,
                        records: Vec::new(),
                    },
                },
                PullFrame::End {
                    checkpoint: initial,
                },
            ],
        });
        client
            .apply_pull_response(serde_json::to_string(&first).unwrap())
            .await
            .unwrap();

        let mut rotated = checkpoint();
        rotated.token = OpaqueCheckpoint("checkpoint-2".into());
        let empty = PullResponse::<JsonValue>::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: rotated.clone(),
                },
                PullFrame::End {
                    checkpoint: rotated.clone(),
                },
            ],
        });
        let status = client
            .apply_pull_response(serde_json::to_string(&empty).unwrap())
            .await
            .unwrap();
        assert_eq!(status.cursor_sequence, Some(1));
        assert_eq!(
            client.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-2")
        );
        client.close().await;

        let reopened = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(
            reopened.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-2")
        );
        let mut next = checkpoint();
        next.token = OpaqueCheckpoint("checkpoint-3".into());
        next.cursor.sequence = 2;
        let second = PullResponse::Batch(PullBatch {
            frames: vec![
                PullFrame::Start {
                    checkpoint: next.clone(),
                },
                PullFrame::Commit {
                    checkpoint: next.token.clone(),
                    commit: PullCommit {
                        sequence: 2,
                        source: None,
                        records: vec![AppliedRecord {
                            record_id: RecordId("recipe:one".into()),
                            state: RecordState::Present {
                                value: json!({"recipeName": "newest"}),
                                version: 2,
                                reference: None,
                            },
                        }],
                    },
                },
                PullFrame::End { checkpoint: next },
            ],
        });
        let status = reopened
            .apply_pull_response(serde_json::to_string(&second).unwrap())
            .await
            .unwrap();
        assert_eq!(status.cursor_sequence, Some(2));
        let record: Option<Value> = database
            .client()
            .await
            .unwrap()
            .select(NativeRecordId::parse_simple("recipe:one").unwrap())
            .await
            .unwrap();
        assert!(matches!(record, Some(Value::Object(_))));
    }

    #[tokio::test]
    async fn adapter_pull_cbor_reopens_with_durable_batch_and_reset_state() {
        const BATCH: &str = "84707375727265616c64622d73796e632f3100038200838200837820636865636b706f696e742d617574686f726974792d70756c6c2d676f6c64656e8201018363616c6c010183017820636865636b706f696e742d617574686f726974792d70756c6c2d676f6c64656e820181826a706572736f6e3a6c696e8401a1646e616d65634c696e01f68202837820636865636b706f696e742d617574686f726974792d70756c6c2d676f6c64656e8201018363616c6c0101";
        const RESET: &str = "84707375727265616c64622d73796e632f310003840100837821636865636b706f696e742d617574686f726974792d72657365742d676f6c64656e8201018363616c6c010181826a706572736f6e3a6c696e8401a1646e616d65634c696e01f6";
        let directory = temp_dir::TempDir::new().unwrap();

        let database = surrealkv_database(directory.path()).await;
        let client = open_sync_client(database.clone(), options("all"))
            .await
            .unwrap();
        let batch_json = crate::sync_http_codec::decode_sync_pull_response(decode_hex(BATCH))
            .await
            .unwrap();
        let status = client.apply_pull_response(batch_json).await.unwrap();
        assert_eq!(status.cursor_epoch, Some(1));
        assert_eq!(status.cursor_sequence, Some(1));
        assert_eq!(
            client.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-authority-pull-golden")
        );
        client.close().await;
        database.close().await.unwrap();
        drop(client);
        drop(database);

        let reopened_database = surrealkv_database(directory.path()).await;
        let reopened = open_sync_client(reopened_database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(
            reopened.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-authority-pull-golden")
        );
        assert_eq!(reopened.status().await.unwrap().cursor_sequence, Some(1));
        let confirmed: Option<Value> = reopened_database
            .client()
            .await
            .unwrap()
            .select(NativeRecordId::parse_simple("person:lin").unwrap())
            .await
            .unwrap();
        assert!(matches!(confirmed, Some(Value::Object(_))));
        reopened.close().await;
        reopened_database.close().await.unwrap();
        drop(reopened);
        drop(reopened_database);

        let reset_database = surrealkv_database(directory.path()).await;
        let reset_client = open_sync_client(reset_database.clone(), options("all"))
            .await
            .unwrap();
        let pending = serde_json::to_string(&commit()).unwrap();
        reset_client.enqueue(pending).await.unwrap();
        let reset_json = crate::sync_http_codec::decode_sync_pull_response(decode_hex(RESET))
            .await
            .unwrap();
        let reset_status = reset_client.apply_pull_response(reset_json).await.unwrap();
        assert_eq!(reset_status.cursor_sequence, Some(1));
        assert_eq!(reset_client.pending_json().await.unwrap().len(), 1);
        reset_client.close().await;
        reset_database.close().await.unwrap();
        drop(reset_client);
        drop(reset_database);

        let final_database = surrealkv_database(directory.path()).await;
        let final_client = open_sync_client(final_database.clone(), options("all"))
            .await
            .unwrap();
        assert_eq!(
            final_client.checkpoint_token().await.unwrap().as_deref(),
            Some("checkpoint-authority-reset-golden")
        );
        assert_eq!(final_client.status().await.unwrap().pending_count, 1);
        let database_client = final_database.client().await.unwrap();
        let confirmed: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:lin").unwrap())
            .await
            .unwrap();
        let optimistic: Option<Value> = database_client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert!(matches!(confirmed, Some(Value::Object(_))));
        assert!(matches!(optimistic, Some(Value::Object(_))));
        drop(database_client);
        final_client.close().await;
        final_database.close().await.unwrap();
    }
    #[tokio::test]
    async fn shared_runtime_siblings_see_commits_local_close_and_independent_keys() {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert!(Arc::ptr_eq(&a.runtime, &b.runtime));
        a.enqueue(serde_json::to_string(&commit()).unwrap())
            .await
            .unwrap();
        assert_eq!(b.status().await.unwrap().pending_count, 1);
        assert_eq!(
            a.read_snapshot_json().await.unwrap(),
            b.read_snapshot_json().await.unwrap()
        );
        a.close().await;
        assert!(matches!(a.status().await, Err(NativeSyncError::Closed)));
        assert_eq!(b.pending_json().await.unwrap().len(), 1);
        let mut other = options("all");
        other.client_id = "other".into();
        let c = open_sync_client(db.clone(), other).await.unwrap();
        let hold = b.lock_runtime().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), c.status())
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), a.status())
                .await
                .is_err(),
            "same-key operation bypassed shared slot"
        );
        drop(hold);
        drop(a);
        drop(b);
        drop(c);
        let next = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert_eq!(
            db.sync_runtimes.0.lock().unwrap().len(),
            1,
            "dead weak entries retained"
        );
        assert_eq!(next.status().await.unwrap().pending_count, 1);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn lifecycle_intent_fences_helpers_without_inner_reader_deadlock_and_never_revives_old_handles()
     {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let hold = a.lock_runtime().await;
        let generation = a.session_generation;
        let change = tokio::spawn({
            let db = db.clone();
            async move {
                db.use_namespace_database("other".into(), "other".into())
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while db.session_generation.load(Ordering::Acquire) == generation {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            hold.client(),
            Err(NativeSyncError::RecoveryRequired)
        ));
        // Writer queues on lifecycle BEFORE inner.write; helper's short client read cannot recurse-deadlock.
        let state = hold.guard.client().unwrap().state().clone();
        let store = SyncStateStore::new(&db);
        let loaded = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            store.load(SyncStateKey::new(&state.partition_id, &state.client_id)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded, Some(state.clone()));
        let prepared = ClientRuntime::open(state.clone())
            .unwrap()
            .prepare_enqueue(commit())
            .unwrap()
            .into_state();
        assert!(matches!(
            store
                .with_session_generation(generation)
                .apply_transition_atomic(&state, &prepared)
                .await,
            Err(SyncStateError::RevisionConflict)
        ));
        drop(hold);
        change.await.unwrap().unwrap();
        assert!(matches!(
            a.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            b.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        db.use_namespace_database("test".into(), "test".into())
            .await
            .unwrap();
        assert!(
            matches!(a.status().await, Err(NativeSyncError::RecoveryRequired)),
            "session ABA revived old wrapper"
        );
        let fresh = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert_eq!(fresh.status().await.unwrap().pending_count, 0);
        assert!(db.authenticate("not-a-token".into()).await.is_err());
        assert!(
            matches!(fresh.status().await, Err(NativeSyncError::RecoveryRequired)),
            "failed auth did not invalidate journal"
        );
        let fresh = open_sync_client(db.clone(), options("all")).await.unwrap();
        db.invalidate().await.unwrap();
        assert!(matches!(
            fresh.pending_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        let fresh = open_sync_client(db.clone(), options("all")).await.unwrap();
        db.close().await.unwrap();
        assert!(matches!(
            fresh.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
    }

    async fn staged_native_completion(
        db: &SurrealDatabase,
        session: &str,
    ) -> (
        Vec<u8>,
        surrealdb_sync_protocol::bootstrap::BootstrapContext,
        u64,
    ) {
        use surrealdb_sync_protocol::bootstrap::*;
        use surrealdb_sync_protocol::bootstrap_codec::*;
        let context = BootstrapContext {
            authority_id: "authority".into(),
            principal_id: "principal".into(),
            partition_id: PartitionId("partition".into()),
            client_id: ClientId("client".into()),
            journal_generation: "generation".into(),
            scope: ScopeSnapshot {
                identity: ScopeIdentity("all".into()),
                subscription_revision: 1,
                authorization_revision: 1,
            },
        };
        let start = BootstrapStartRequest {
            partition_id: context.partition_id.clone(),
            client_id: context.client_id.clone(),
            request_id: session.into(),
            requested_scope: context.scope.identity.clone(),
            subscription_revision: 1,
            journal_generation: context.journal_generation.clone(),
        };
        let mut page = BootstrapPage {
            session_id: session.into(),
            manifest_digest: BootstrapDigest([0; 32]),
            index: 0,
            records: vec![surrealdb_sync_protocol::SnapshotRecord {
                record_id: RecordId("person:server".into()),
                state: RecordState::Present {
                    value: surrealdb_sync_protocol::CanonicalValue::Object(
                        std::collections::BTreeMap::from([(
                            "name".into(),
                            surrealdb_sync_protocol::CanonicalValue::String("server".into()),
                        )]),
                    ),
                    version: 1,
                    reference: None,
                },
            }],
        };
        let mut hasher = BootstrapContentHasher::new(session.into()).unwrap();
        hasher.push(&page).unwrap();
        let summary = hasher.summary();
        let manifest = BootstrapManifest {
            session_id: session.into(),
            start_request_id: session.into(),
            start_fingerprint: bootstrap_start_fingerprint(&start).unwrap(),
            context: context.clone(),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            limits: BootstrapLimits::default(),
            page_count: summary.page_count,
            record_count: summary.record_count,
            content_bytes: summary.content_bytes,
            root: summary.root,
            expires_at: u64::MAX,
        };
        page.manifest_digest = bootstrap_manifest_digest(&manifest).unwrap();
        let store = crate::sync_bootstrap_store::BootstrapStore::new(db);
        let handle = store
            .begin(
                &encode_bootstrap_start(&start).unwrap(),
                &encode_bootstrap_manifest(&manifest).unwrap(),
                &context,
            )
            .await
            .unwrap();
        store
            .stage_page(&encode_bootstrap_page(&page).unwrap(), &context, handle)
            .await
            .unwrap();
        let receipt = BootstrapReceipt {
            session_id: session.into(),
            manifest_digest: page.manifest_digest,
            checkpoint: Checkpoint {
                token: OpaqueCheckpoint("complete".into()),
                cursor: manifest.cursor,
                scope: context.scope.clone(),
            },
            expires_at: manifest.expires_at,
        };
        (encode_bootstrap_receipt(&receipt).unwrap(), context, handle)
    }

    #[tokio::test]
    async fn private_completion_publishes_once_to_siblings_rotates_both_cursors_and_historical_retry_is_noop()
     {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let before: JsonValue =
            serde_json::from_str(&a.read_snapshot_json().await.unwrap()).unwrap();
        let subscription: JsonValue =
            serde_json::from_str(&b.subscription_snapshot_json("window".into()).await.unwrap())
                .unwrap();
        let (receipt, context, handle) = staged_native_completion(&db, "publish").await;
        let clock = crate::sync_bootstrap_store::CompletionClock {
            utc_seconds: &|| Ok(1),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
        };
        assert!(
            a.complete_bootstrap(&receipt, &context, handle, &clock)
                .await
                .unwrap()
        );
        assert_eq!(b.status().await.unwrap().cursor_sequence, Some(1));
        let read: JsonValue = serde_json::from_str(
            &b.read_changes_json(before["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(read["kind"], "reset_required");
        assert_ne!(
            read["cursor"]["processEpoch"],
            before["cursor"]["processEpoch"]
        );
        let mut coincident = before["cursor"].clone();
        coincident["revision"] = read["cursor"]["revision"].clone();
        let coincident: JsonValue = serde_json::from_str(
            &b.read_changes_json(coincident.to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            coincident["kind"], "reset_required",
            "old process epoch accepted a coincident numeric revision"
        );
        let changes: JsonValue = serde_json::from_str(
            &b.subscription_changes_json("window".into(), subscription["cursor"].to_string(), 128)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(changes["kind"], "reset_required");
        assert_ne!(
            changes["cursor"]["processEpoch"],
            subscription["cursor"]["processEpoch"]
        );
        b.enqueue(serde_json::to_string(&commit()).unwrap())
            .await
            .unwrap();
        let latest = b.read_snapshot_json().await.unwrap();
        let status = b.status().await.unwrap();
        assert!(
            !a.complete_bootstrap(&receipt, &context, handle, &clock)
                .await
                .unwrap()
        );
        assert_eq!(b.status().await.unwrap(), status);
        assert_eq!(b.read_snapshot_json().await.unwrap(), latest);
        a.close().await;
        assert!(b.status().await.is_ok());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn private_completion_lost_ack_invalidates_all_siblings_and_reopen_preserves_durable_result()
     {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let (receipt, context, handle) = staged_native_completion(&db, "lost").await;
        let clock = crate::sync_bootstrap_store::CompletionClock {
            utc_seconds: &|| Ok(1),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
        };
        a.lose_next_persist_ack.store(true, Ordering::SeqCst);
        assert!(matches!(
            a.complete_bootstrap(&receipt, &context, handle, &clock)
                .await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            b.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(matches!(
            b.read_snapshot_json().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        let reopened = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert_eq!(reopened.status().await.unwrap().cursor_sequence, Some(1));
        reopened
            .enqueue(serde_json::to_string(&commit()).unwrap())
            .await
            .unwrap();
        let current = reopened.read_snapshot_json().await.unwrap();
        assert!(
            !reopened
                .complete_bootstrap(&receipt, &context, handle, &clock)
                .await
                .unwrap()
        );
        assert_eq!(reopened.read_snapshot_json().await.unwrap(), current);
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn completion_postcommit_gate_blocks_sibling_but_not_other_key() {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let mut other = options("all");
        other.client_id = "independent".into();
        let c = open_sync_client(db.clone(), other).await.unwrap();
        let (receipt, context, handle) = staged_native_completion(&db, "postcommit").await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *a.completion_pause.lock().unwrap() = Some((arrived.clone(), release.clone()));
        let clock = crate::sync_bootstrap_store::CompletionClock {
            utc_seconds: &|| Ok(1),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
        };
        let complete = a.complete_bootstrap(&receipt, &context, handle, &clock);
        let competitor = async {
            arrived.notified().await;
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), b.status())
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(200), c.status())
                    .await
                    .unwrap()
                    .is_ok()
            );
            release.notify_one();
        };
        let (result, ()) = tokio::join!(complete, competitor);
        assert!(result.unwrap());
        assert_eq!(b.status().await.unwrap().cursor_sequence, Some(1));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn session_or_close_intent_after_commit_never_publishes_old_runtime() {
        for close in [false, true] {
            let db = database().await;
            let a = open_sync_client(db.clone(), options("all")).await.unwrap();
            let b = open_sync_client(db.clone(), options("all")).await.unwrap();
            let before: JsonValue =
                serde_json::from_str(&b.read_snapshot_json().await.unwrap()).unwrap();
            let old_state = b.lock_runtime().await.client().unwrap().state().clone();
            let (receipt, context, handle) =
                staged_native_completion(&db, "session-publication").await;
            let arrived = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            *a.completion_pause.lock().unwrap() = Some((arrived.clone(), release.clone()));
            let clock = crate::sync_bootstrap_store::CompletionClock {
                utc_seconds: &|| Ok(1),
                deadline: Instant::now() + std::time::Duration::from_secs(10),
            };
            let complete = a.complete_bootstrap(&receipt, &context, handle, &clock);
            let change = async {
                arrived.notified().await;
                let writer = tokio::spawn({
                    let db = db.clone();
                    async move {
                        if close {
                            db.close().await
                        } else {
                            db.use_namespace_database("after".into(), "after".into())
                                .await
                        }
                    }
                });
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    while db.session_generation.load(Ordering::Acquire) == a.session_generation {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                release.notify_one();
                writer.await.unwrap().unwrap();
            };
            let (result, ()) = tokio::join!(complete, change);
            assert!(matches!(result, Err(NativeSyncError::RecoveryRequired)));
            assert!(matches!(
                b.read_snapshot_json().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            let slot = a.lock_runtime().await;
            assert!(slot.guard.runtime.is_none());
            assert!(slot.guard.recovery_required);
            // No old-epoch read batches can represent the unconfirmed publication.
            let invalidated = slot
                .guard
                .journal
                .bootstrap(&ClientRuntime::open(old_state).unwrap())
                .unwrap();
            assert_ne!(
                invalidated["cursor"]["processEpoch"],
                before["cursor"]["processEpoch"]
            );
            drop(slot);
            if !close {
                db.close().await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn every_typed_session_mutator_invalidates_existing_generation_and_exhaustion_fails_closed()
     {
        for mutation in 0..5 {
            let db = database().await;
            let client = open_sync_client(db.clone(), options("all")).await.unwrap();
            let previous = client.session_generation;
            match mutation {
                0 => {
                    db.use_namespace_database("test".into(), "test".into())
                        .await
                        .unwrap();
                }
                1 => {
                    assert!(db.authenticate("invalid-token".into()).await.is_err());
                }
                2 => {
                    let _ = db
                        .sign_in_root("nonexistent".into(), "fixture-only".into())
                        .await;
                }
                3 => {
                    let _ = db
                        .sign_in_database(
                            "test".into(),
                            "test".into(),
                            "nonexistent".into(),
                            "fixture-only".into(),
                        )
                        .await;
                }
                _ => {
                    db.invalidate().await.unwrap();
                }
            }
            assert_eq!(db.session_generation.load(Ordering::Acquire), previous + 2);
            assert!(matches!(
                client.status().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client
                    .enqueue(serde_json::to_string(&commit()).unwrap())
                    .await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.pending_json().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.conflicts_json().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.checkpoint_token().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.requires_authorized_reset().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.scope_transition_decision("all".into(), 1).await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                client.subscription_snapshot_json("window".into()).await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            db.close().await.unwrap();
        }
        let db = database().await;
        db.session_generation.store(u64::MAX, Ordering::Release);
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert!(db.invalidate().await.is_err());
        assert!(db.is_closed());
        assert!(matches!(
            client.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert!(db.close().await.is_err());
        assert!(
            db.inner.read().await.is_none(),
            "exhausted close did not drain native client"
        );
    }
    #[tokio::test]
    async fn queued_open_between_two_reserved_session_mutations_is_invalidated_at_second_boundary()
    {
        let db = database().await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *db.session_pause.lock().unwrap() = Some((arrived, release.clone()));
        let a = db.use_namespace_database("a".into(), "a".into());
        tokio::pin!(a);
        assert!(futures::poll!(a.as_mut()).is_pending()); // A owns lifecycle write and pauses before session side effects.
        let open = open_sync_client(db.clone(), options("all"));
        tokio::pin!(open);
        assert!(futures::poll!(open.as_mut()).is_pending()); // Read is now queued behind A.
        let b = db.use_namespace_database("b".into(), "b".into());
        tokio::pin!(b);
        assert!(futures::poll!(b.as_mut()).is_pending()); // B reserves intent, then queues behind open.
        release.notify_one();
        let (a, opened, b) = tokio::join!(a, open, b);
        a.unwrap();
        b.unwrap();
        let opened = opened.unwrap();
        assert!(opened.session_generation < db.session_generation.load(Ordering::Acquire));
        assert!(
            matches!(
                opened.status().await,
                Err(NativeSyncError::RecoveryRequired)
            ),
            "open adopted B's pending intent generation as if B had already mutated session"
        );
        let fresh = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert!(fresh.status().await.is_ok());
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn unexpected_durable_write_in_postcommit_gap_invalidates_instead_of_publishing_stale_state()
     {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let (receipt, context, handle) = staged_native_completion(&db, "readback-mismatch").await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *a.completion_pause.lock().unwrap() = Some((arrived.clone(), release.clone()));
        let clock = crate::sync_bootstrap_store::CompletionClock {
            utc_seconds: &|| Ok(1),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
        };
        let complete = a.complete_bootstrap(&receipt, &context, handle, &clock);
        let bypass = async {
            arrived.notified().await;
            // Deliberately bypass supported shared ownership through a PRIVATE store helper.
            // This tests mismatch recovery, not general raw-writer coherence after readback.
            let key = SyncStateKey::new(&context.partition_id, &context.client_id);
            let store = SyncStateStore::new(&db);
            let latest = store.load(key).await.unwrap().unwrap();
            let mut local = commit();
            local.identity.fingerprint = canonical_fingerprint(&latest, &local).unwrap();
            let next = ClientRuntime::open(latest.clone())
                .unwrap()
                .prepare_enqueue(local)
                .unwrap()
                .into_state();
            store.apply_transition_atomic(&latest, &next).await.unwrap();
            release.notify_one();
            next
        };
        let (result, later) = tokio::join!(complete, bypass);
        assert!(matches!(result, Err(NativeSyncError::RecoveryRequired)));
        assert!(matches!(
            b.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        let recovered = open_sync_client(db.clone(), options("all")).await.unwrap();
        assert_eq!(recovered.status().await.unwrap().pending_count, 1);
        assert_eq!(
            recovered.lock_runtime().await.client().unwrap().state(),
            &later
        );
        assert!(
            !recovered
                .complete_bootstrap(&receipt, &context, handle, &clock)
                .await
                .unwrap()
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn repeated_recovery_preserves_prefix_and_rejects_every_old_epoch_even_at_equal_revision()
    {
        let db = database().await;
        let a = open_sync_client(db.clone(), options("all")).await.unwrap();
        let b = open_sync_client(db.clone(), options("all")).await.unwrap();
        let mut read_cursors = Vec::new();
        let mut subscription_cursors = Vec::new();
        let mut prefix = None;
        let mut subscription_prefix = None;
        let mut last_generation = 0;
        for round in 0..4 {
            let before: JsonValue =
                serde_json::from_str(&b.read_snapshot_json().await.unwrap()).unwrap();
            read_cursors.push(before["cursor"].clone());
            let subscription: JsonValue =
                serde_json::from_str(&b.subscription_snapshot_json("window".into()).await.unwrap())
                    .unwrap();
            subscription_cursors.push(subscription["cursor"].clone());
            let epoch = before["cursor"]["processEpoch"].as_str().unwrap();
            let (current_prefix, _) = epoch.rsplit_once(':').unwrap();
            assert_eq!(
                prefix.get_or_insert_with(|| current_prefix.to_owned()),
                current_prefix
            );
            let mut local = commit();
            local.identity.client_commit_id = ClientCommitId(format!("recovery-{round}"));
            a.lose_next_persist_ack.store(true, Ordering::SeqCst);
            assert!(matches!(
                a.enqueue(serde_json::to_string(&local).unwrap()).await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            let recovered = open_sync_client(db.clone(), options("all")).await.unwrap();
            assert!(Arc::ptr_eq(&a.runtime, &recovered.runtime));
            let after: JsonValue =
                serde_json::from_str(&recovered.read_snapshot_json().await.unwrap()).unwrap();
            let epoch = after["cursor"]["processEpoch"].as_str().unwrap();
            let (current_prefix, generation) = epoch.rsplit_once(':').unwrap();
            assert_eq!(prefix.as_deref(), Some(current_prefix));
            let generation = generation.parse::<u64>().unwrap();
            assert!(generation > last_generation);
            last_generation = generation;
            for old in &read_cursors {
                let mut coincident = old.clone();
                coincident["revision"] = after["cursor"]["revision"].clone();
                let changes: JsonValue = serde_json::from_str(
                    &b.read_changes_json(coincident.to_string(), 128)
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(changes["kind"], "reset_required");
            }
            for old in &subscription_cursors {
                let changes: JsonValue = serde_json::from_str(
                    &b.subscription_changes_json("window".into(), old.to_string(), 128)
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(changes["kind"], "reset_required");
                let epoch = changes["cursor"]["processEpoch"].as_str().unwrap();
                let (current_prefix, _) = epoch.rsplit_once(':').unwrap();
                assert_eq!(
                    subscription_prefix.get_or_insert_with(|| current_prefix.to_owned()),
                    current_prefix
                );
            }
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn reader_generation_exhaustion_never_defaults_or_revives_a_recovery_cell() {
        for subscription in [false, true] {
            let db = database().await;
            let a = open_sync_client(db.clone(), options("all")).await.unwrap();
            let b = open_sync_client(db.clone(), options("all")).await.unwrap();
            let mut slot = a.lock_runtime().await;
            let state = slot.client().unwrap().state().clone();
            if subscription {
                slot.subscription_journal.exhaust_epoch_for_test();
            } else {
                slot.journal.exhaust_epoch_for_test();
            }
            let runtime = ClientRuntime::open(state).unwrap();
            let cursor = if subscription {
                slot.subscription_journal
                    .bootstrap(&runtime, "window")
                    .unwrap()["cursor"]
                    .clone()
            } else {
                slot.journal.bootstrap(&runtime).unwrap()["cursor"].clone()
            };
            invalidate_runtime(&mut slot);
            drop(slot);
            assert!(matches!(
                open_sync_client(db.clone(), options("all")).await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            assert!(matches!(
                b.status().await,
                Err(NativeSyncError::RecoveryRequired)
            ));
            let slot = a.lock_runtime().await;
            let after = if subscription {
                slot.subscription_journal
                    .bootstrap(&runtime, "window")
                    .unwrap()["cursor"]
                    .clone()
            } else {
                slot.journal.bootstrap(&runtime).unwrap()["cursor"].clone()
            };
            assert_eq!(after, cursor, "exhausted counter/prefix was silently reset");
            assert!(slot.guard.runtime.is_none());
            drop(slot);
            db.close().await.unwrap();
        }
    }
    #[tokio::test]
    async fn bootstrap_binding_ticket_exhaustion_preserves_binding_across_recovery() {
        let db = database().await;
        let client = open_sync_client(db.clone(), options("all")).await.unwrap();
        let (_, context, _) = staged_native_completion(&db, "ticket").await;
        {
            let mut access = client.bootstrap_access(&context, None).await.unwrap();
            assert_eq!(access.bind(&context).unwrap(), 1);
            access.slot.bootstrap_binding_ticket = u64::MAX;
            invalidate_runtime(&mut access.slot);
        }
        let reopened = open_sync_client(db.clone(), options("all")).await.unwrap();
        let mut newer = context.clone();
        newer.scope.authorization_revision += 1;
        let mut access = reopened.bootstrap_access(&newer, None).await.unwrap();
        assert_eq!(
            access.bind(&newer).unwrap_err(),
            crate::sync_bootstrap::NativeBootstrapError::RecoveryRequired
        );
        assert_eq!(access.slot.bootstrap_binding.as_ref(), Some(&context));
        assert_eq!(access.slot.bootstrap_binding_ticket, u64::MAX);
    }
}
