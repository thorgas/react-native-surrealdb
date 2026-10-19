//! Additive Rust/UniFFI bootstrap facade; generated bindings/native artifacts are separate gates.
//!
//! Expected context is supplied independently by trusted authenticated application
//! composition. This transport-free SDK does NOT authenticate JS or infer owner,
//! generation or initially unknown authorization revision from received wire bytes.
//! OS UTC accuracy is assumed; the authority must enforce expiry on fresh Complete.
//! UniFFI copies input bytes before Rust validation; limits do not protect against
//! hostile-JS bridge preallocation. No passwords/tokens are persisted or logged here.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use surrealdb_sync_protocol::bootstrap::BootstrapContext;
use surrealdb_sync_protocol::bootstrap_codec::{
    bootstrap_manifest_digest, decode_bootstrap_manifest, decode_bootstrap_page,
    decode_bootstrap_receipt, decode_bootstrap_start, validate_bootstrap_manifest_context,
    validate_bootstrap_receipt,
};
use surrealdb_sync_protocol::{ClientId, PartitionId, ScopeIdentity, ScopeSnapshot};

use crate::sync_bootstrap_store::{CompletionClock, StageState};
use crate::sync_client::{NativeSyncClient, NativeSyncError};
use crate::sync_state::{MAX_SYNC_STATE_BYTES, SyncStateError};

#[derive(Clone, Debug, uniffi::Record)]
pub struct NativeBootstrapContext {
    pub authority_id: String,
    pub principal_id: String,
    pub partition_id: String,
    pub client_id: String,
    pub journal_generation: String,
    pub requested_scope: String,
    pub subscription_revision: u64,
    /// Independently obtained from authenticated authority; never inferred from Manifest.
    pub authorization_revision: u64,
}
impl NativeBootstrapContext {
    fn validated(self) -> Result<BootstrapContext, NativeBootstrapError> {
        for key in [&self.partition_id, &self.client_id, &self.requested_scope] {
            if key.is_empty() || key.len() > crate::sync_state::MAX_IDENTITY_BYTES {
                return Err(NativeBootstrapError::InvalidInput);
            }
        }
        for key in [
            &self.authority_id,
            &self.principal_id,
            &self.journal_generation,
        ] {
            if key.is_empty() || key.len() > surrealdb_sync_protocol::MAX_IDENTITY_BYTES {
                return Err(NativeBootstrapError::InvalidInput);
            }
        }
        Ok(BootstrapContext {
            authority_id: self.authority_id,
            principal_id: self.principal_id,
            partition_id: PartitionId(self.partition_id),
            client_id: ClientId(self.client_id),
            journal_generation: self.journal_generation,
            scope: ScopeSnapshot {
                identity: ScopeIdentity(self.requested_scope),
                subscription_revision: self.subscription_revision,
                authorization_revision: self.authorization_revision,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, uniffi::Enum)]
pub enum NativeBootstrapStageState {
    Missing,
    Staging,
    Cancelled,
    Consumed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, uniffi::Enum)]
pub enum NativeBootstrapCompletion {
    Applied,
    HistoricalSuccess,
}
#[derive(uniffi::Record)]
pub struct NativeBootstrapProgress {
    pub state: NativeBootstrapStageState,
    pub stage: Option<Arc<NativeBootstrapStage>>,
    pub received_indices: Option<Vec<u64>>,
    pub expected_pages: Option<u64>,
    pub successful_revision: Option<u64>,
}
#[derive(uniffi::Record)]
pub struct NativeBootstrapCapabilities {
    pub protocol: String,
    pub requires_matching_native_abi: bool,
    pub max_frame_bytes: u64,
    pub max_persistence_bytes: u64,
    pub max_components: u32,
    pub native_key_bytes: u32,
    pub completion_deadline_seconds: u32,
}

#[derive(Debug, Eq, PartialEq, thiserror::Error, uniffi::Error)]
pub enum NativeBootstrapError {
    #[error("bootstrap input is outside native bounds")]
    InvalidInput,
    #[error("bootstrap frame is not canonical or correctly bound")]
    InvalidFrame,
    #[error("bootstrap context does not match the independently bound journal")]
    ContextMismatch,
    #[error("bootstrap stage identity changed")]
    StageMismatch,
    #[error("bootstrap capacity is exhausted")]
    CapacityExceeded,
    #[error("bootstrap stage is corrupt or incomplete")]
    CorruptStage,
    #[error("bootstrap completion expired")]
    Expired,
    #[error("native bootstrap UTC clock is unavailable")]
    ClockUnavailable,
    #[error("bootstrap parent handle is closed")]
    Closed,
    #[error("bootstrap scope requires a new journal")]
    NeedsNewScopeJournal,
    #[error("bootstrap scope requires pending quarantine")]
    PendingQuarantineRequired,
    #[error("bootstrap outcome requires durable recovery")]
    RecoveryRequired,
    #[error("bootstrap storage operation failed")]
    Storage,
}
impl NativeBootstrapError {
    pub(crate) fn from_native(error: NativeSyncError) -> Self {
        match error {
            NativeSyncError::Closed => Self::Closed,
            NativeSyncError::RecoveryRequired => Self::RecoveryRequired,
            NativeSyncError::NeedsNewScopeJournal => Self::NeedsNewScopeJournal,
            NativeSyncError::PendingQuarantineRequired => Self::PendingQuarantineRequired,
            NativeSyncError::InvalidInput | NativeSyncError::ReadOutputTooLarge => {
                Self::InvalidInput
            }
            NativeSyncError::Protocol | NativeSyncError::InvalidSubscription => Self::InvalidFrame,
            _ => Self::Storage,
        }
    }
    pub(crate) fn from_store(error: SyncStateError) -> Self {
        match error {
            SyncStateError::IdentityTooLarge => Self::InvalidInput,
            SyncStateError::StateTooLarge => Self::CapacityExceeded,
            SyncStateError::RevisionConflict | SyncStateError::RevisionMismatch => {
                Self::StageMismatch
            }
            SyncStateError::CommitUnknown(_) | SyncStateError::RollbackFailed { .. } => {
                Self::RecoveryRequired
            }
            SyncStateError::CorruptState | SyncStateError::Decode(_) => Self::CorruptStage,
            SyncStateError::InvalidState(
                surrealdb_sync_client::ClientError::NeedsNewScopeJournal,
            ) => Self::NeedsNewScopeJournal,
            SyncStateError::InvalidState(
                surrealdb_sync_client::ClientError::PendingQuarantineRequired,
            ) => Self::PendingQuarantineRequired,
            SyncStateError::UnsupportedRecordId
            | SyncStateError::RecordValueMustBeObject
            | SyncStateError::ValueCodec(_)
            | SyncStateError::InvalidState(_) => Self::InvalidFrame,
            _ => Self::Storage,
        }
    }
}

/// Source capability only until regenerated bindings and matching native binaries exist.
#[uniffi::export]
pub fn bootstrap_capabilities() -> NativeBootstrapCapabilities {
    NativeBootstrapCapabilities {
        protocol: surrealdb_sync_protocol::bootstrap::BOOTSTRAP_NAME.into(),
        requires_matching_native_abi: true,
        max_frame_bytes: MAX_SYNC_STATE_BYTES as u64,
        max_persistence_bytes: crate::sync_state::MAX_AGGREGATE_STATE_BYTES as u64,
        max_components: crate::sync_state::MAX_COMPONENT_ITEMS as u32,
        native_key_bytes: crate::sync_state::MAX_IDENTITY_BYTES as u32,
        completion_deadline_seconds: 30,
    }
}

#[derive(uniffi::Object)]
pub struct NativeBootstrapSession {
    client: Arc<NativeSyncClient>,
    expected: BootstrapContext,
    binding: Arc<()>,
    ticket: u64,
}
/// Opaque fixed incarnation. Fresh sessions recover a new handle through read-only progress.
#[derive(uniffi::Object)]
pub struct NativeBootstrapStage {
    binding: Arc<()>,
    manifest: Vec<u8>,
    incarnation: u64,
}

/// Trusted caller supplies authenticated context independently from any received body.
/// A generation-bearing client identity is an application concern, never parsed here.
/// Factory binding is read-only/process-local. A newer authorization snapshot may
/// supersede a terminal stage; its first successful Begin establishes the durable
/// floor. Existing sessions become stale on promotion. Active stages remain exact,
/// and Complete always applies the canonical pending/history/subscription fences.
#[uniffi::export(async_runtime = "tokio")]
pub async fn bind_bootstrap_session(
    client: Arc<NativeSyncClient>,
    expected: NativeBootstrapContext,
) -> Result<Arc<NativeBootstrapSession>, NativeBootstrapError> {
    let expected = expected.validated()?;
    let mut access = client.bootstrap_access(&expected, None).await?;
    // Validate any existing durable stage owner BEFORE publishing an in-memory binding.
    let result = access.store().resume(None, &expected, None).await;
    access.finish(result, false).map_err(|error| {
        if error == NativeBootstrapError::StageMismatch {
            NativeBootstrapError::ContextMismatch
        } else {
            error
        }
    })?;
    let ticket = access.bind(&expected)?;
    drop(access);
    Ok(Arc::new(NativeBootstrapSession {
        client,
        expected,
        binding: Arc::new(()),
        ticket,
    }))
}

fn bounded(raw: &[u8]) -> Result<(), NativeBootstrapError> {
    if raw.len() > MAX_SYNC_STATE_BYTES {
        return Err(NativeBootstrapError::InvalidInput);
    }
    Ok(())
}
impl NativeBootstrapSession {
    fn stage(&self, manifest: Vec<u8>, incarnation: u64) -> Arc<NativeBootstrapStage> {
        Arc::new(NativeBootstrapStage {
            binding: self.binding.clone(),
            manifest,
            incarnation,
        })
    }
    fn validate_stage(&self, stage: &NativeBootstrapStage) -> Result<(), NativeBootstrapError> {
        if !Arc::ptr_eq(&self.binding, &stage.binding) {
            return Err(NativeBootstrapError::StageMismatch);
        }
        Ok(())
    }
    fn manifest(
        &self,
        raw: &[u8],
    ) -> Result<surrealdb_sync_protocol::bootstrap::BootstrapManifest, NativeBootstrapError> {
        bounded(raw)?;
        let manifest =
            decode_bootstrap_manifest(raw).map_err(|_| NativeBootstrapError::InvalidFrame)?;
        if manifest.context != self.expected {
            return Err(NativeBootstrapError::ContextMismatch);
        }
        Ok(manifest)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl NativeBootstrapSession {
    pub async fn begin(
        self: Arc<Self>,
        start: Vec<u8>,
        manifest: Vec<u8>,
    ) -> Result<Arc<NativeBootstrapStage>, NativeBootstrapError> {
        let client = self.client.clone();
        client
            .run_mutation(move |_| async move { self.begin_owned(start, manifest).await })
            .await
    }
    pub async fn page(
        self: Arc<Self>,
        stage: Arc<NativeBootstrapStage>,
        bytes: Vec<u8>,
    ) -> Result<(), NativeBootstrapError> {
        let client = self.client.clone();
        client
            .run_mutation(move |_| async move { self.page_owned(stage, bytes).await })
            .await
    }
    /// Read-only resume: Missing never invokes Begin or initializes schema/state.
    pub async fn progress(
        &self,
        manifest: Vec<u8>,
    ) -> Result<NativeBootstrapProgress, NativeBootstrapError> {
        self.manifest(&manifest)?;
        let mut access = self
            .client
            .bootstrap_access(&self.expected, Some(self.ticket))
            .await?;
        let result = access
            .store()
            .resume(Some(&manifest), &self.expected, None)
            .await;
        let Some(inventory) = access.finish(result, false)? else {
            return Ok(NativeBootstrapProgress {
                state: NativeBootstrapStageState::Missing,
                stage: None,
                received_indices: None,
                expected_pages: None,
                successful_revision: None,
            });
        };
        let (state, successful_revision) = match inventory.state {
            StageState::Staging => (NativeBootstrapStageState::Staging, None),
            StageState::Cancelled => (NativeBootstrapStageState::Cancelled, None),
            StageState::Consumed { revision } => {
                (NativeBootstrapStageState::Consumed, Some(revision))
            }
        };
        Ok(NativeBootstrapProgress {
            state,
            stage: Some(self.stage(manifest, inventory.incarnation)),
            received_indices: Some(inventory.indices),
            expected_pages: Some(inventory.expected_pages),
            successful_revision,
        })
    }
    pub async fn cancel(
        self: Arc<Self>,
        stage: Arc<NativeBootstrapStage>,
    ) -> Result<(), NativeBootstrapError> {
        let client = self.client.clone();
        client
            .run_mutation(move |_| async move { self.cancel_owned(stage).await })
            .await
    }
    /// New installation requires fresh authenticated transport receipt from trusted caller.
    /// Clock/deadline are native-owned; consumed historical identity may survive expiry.
    pub async fn complete(
        self: Arc<Self>,
        stage: Arc<NativeBootstrapStage>,
        receipt: Vec<u8>,
    ) -> Result<NativeBootstrapCompletion, NativeBootstrapError> {
        let client = self.client.clone();
        client
            .run_mutation(move |_| async move { self.complete_owned(stage, receipt).await })
            .await
    }
}

#[cfg(test)]
#[path = "sync_bootstrap_tests.rs"]
mod tests;

impl From<NativeSyncError> for NativeBootstrapError {
    fn from(error: NativeSyncError) -> Self {
        Self::from_native(error)
    }
}

impl NativeBootstrapSession {
    async fn begin_owned(
        &self,
        start: Vec<u8>,
        manifest: Vec<u8>,
    ) -> Result<Arc<NativeBootstrapStage>, NativeBootstrapError> {
        bounded(&start)?;
        let decoded = self.manifest(&manifest)?;
        let request =
            decode_bootstrap_start(&start).map_err(|_| NativeBootstrapError::InvalidFrame)?;
        validate_bootstrap_manifest_context(&decoded, &request, &self.expected)
            .map_err(|_| NativeBootstrapError::InvalidFrame)?;
        let mut access = self
            .client
            .bootstrap_access(&self.expected, Some(self.ticket))
            .await?;
        access.check_writable().await?;
        access.arm_mutation();
        let result = access
            .store()
            .begin(&start, &manifest, &self.expected)
            .await;
        let incarnation = access.finish(result, true)?;
        Ok(self.stage(manifest, incarnation))
    }

    async fn page_owned(
        &self,
        stage: Arc<NativeBootstrapStage>,
        bytes: Vec<u8>,
    ) -> Result<(), NativeBootstrapError> {
        self.validate_stage(&stage)?;
        bounded(&bytes)?;
        let page = decode_bootstrap_page(&bytes).map_err(|_| NativeBootstrapError::InvalidFrame)?;
        let manifest = self.manifest(&stage.manifest)?;
        if page.session_id != manifest.session_id
            || page.manifest_digest
                != bootstrap_manifest_digest(&manifest)
                    .map_err(|_| NativeBootstrapError::InvalidFrame)?
        {
            return Err(NativeBootstrapError::StageMismatch);
        }
        let mut access = self
            .client
            .bootstrap_access(&self.expected, Some(self.ticket))
            .await?;
        access.check_writable().await?;
        access.arm_mutation();
        let result = access
            .store()
            .stage_page(&bytes, &self.expected, stage.incarnation)
            .await;
        access.finish(result, true)
    }

    async fn cancel_owned(
        &self,
        stage: Arc<NativeBootstrapStage>,
    ) -> Result<(), NativeBootstrapError> {
        self.validate_stage(&stage)?;
        let mut access = self
            .client
            .bootstrap_access(&self.expected, Some(self.ticket))
            .await?;
        access.arm_mutation();
        let result = access
            .store()
            .cancel(&stage.manifest, &self.expected, stage.incarnation)
            .await;
        access.finish(result, true)
    }

    async fn complete_owned(
        &self,
        stage: Arc<NativeBootstrapStage>,
        receipt: Vec<u8>,
    ) -> Result<NativeBootstrapCompletion, NativeBootstrapError> {
        self.validate_stage(&stage)?;
        bounded(&receipt)?;
        let decoded =
            decode_bootstrap_receipt(&receipt).map_err(|_| NativeBootstrapError::InvalidFrame)?;
        let manifest = self.manifest(&stage.manifest)?;
        validate_bootstrap_receipt(&manifest, &decoded)
            .map_err(|_| NativeBootstrapError::StageMismatch)?;
        let unavailable = AtomicBool::new(false);
        let expired = AtomicBool::new(false);
        let utc = || match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(now) => {
                expired.store(now.as_secs() >= decoded.expires_at, Ordering::Relaxed);
                Ok(now.as_secs())
            }
            Err(_) => {
                unavailable.store(true, Ordering::Relaxed);
                Err(SyncStateError::CorruptState)
            }
        };
        let clock = CompletionClock {
            utc_seconds: &utc,
            deadline: Instant::now() + Duration::from_secs(30),
        };
        match self
            .client
            .complete_bootstrap_bound(
                &receipt,
                &self.expected,
                stage.incarnation,
                &clock,
                Some(self.ticket),
            )
            .await
        {
            Ok(true) => Ok(NativeBootstrapCompletion::Applied),
            Ok(false) => Ok(NativeBootstrapCompletion::HistoricalSuccess),
            Err(NativeSyncError::RecoveryRequired) => Err(NativeBootstrapError::RecoveryRequired),
            Err(_) if unavailable.load(Ordering::Relaxed) => {
                Err(NativeBootstrapError::ClockUnavailable)
            }
            Err(_) if expired.load(Ordering::Relaxed) => Err(NativeBootstrapError::Expired),
            Err(error) => Err(NativeBootstrapError::from_native(error)),
        }
    }
}
