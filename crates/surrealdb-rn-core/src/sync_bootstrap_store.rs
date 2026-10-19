//! Dormant private bootstrap storage. Only verified complete publishes durable projection;
//! no public/native runtime activation or observer publication is exposed here.
//! Encoded reservations are deliberately conservative; they do not bound RSS or DB scans.
use std::collections::{BTreeMap, BTreeSet};

use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::types::{Array, Bytes, RecordId, SurrealValue, Value};
use surrealdb_sync_protocol::RecordState;
use surrealdb_sync_protocol::bootstrap::{BootstrapContext, BootstrapManifest, BootstrapPage};
use surrealdb_sync_protocol::bootstrap_codec::{
    BootstrapVerifier, bootstrap_manifest_digest, decode_bootstrap_manifest, decode_bootstrap_page,
    decode_bootstrap_receipt, decode_bootstrap_start, validate_bootstrap_manifest_context,
    validate_bootstrap_receipt,
};

use crate::SurrealDatabase;
use crate::sync_state::{
    MAX_AGGREGATE_STATE_BYTES, MAX_COMPONENT_ITEMS, MAX_SYNC_STATE_BYTES, SyncStateError,
    SyncStateKey, encode_tagged, finish_transaction, meta_record_id, state_budget,
};

const HEADER: &str = "_sync_bootstrap_header";
const PAGE: &str = "_sync_bootstrap_page";
// Final metadata and the future consumed marker each have the existing per-component cap.
// Old state is reserved again for retained pending/outcomes. This can reject early safely.
const FINAL_OVERHEAD: usize = 3 * MAX_SYNC_STATE_BYTES;
const SCHEMA: &str = r#"
DEFINE FIELD IF NOT EXISTS bootstrap_fence ON _sync_client_meta TYPE option<string>;
DEFINE TABLE IF NOT EXISTS _sync_bootstrap_header SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS format_version ON _sync_bootstrap_header TYPE int;
DEFINE FIELD IF NOT EXISTS start ON _sync_bootstrap_header TYPE bytes;
DEFINE FIELD IF NOT EXISTS manifest ON _sync_bootstrap_header TYPE bytes;
DEFINE FIELD IF NOT EXISTS cancelled ON _sync_bootstrap_header TYPE bool;
DEFINE FIELD IF NOT EXISTS incarnation ON _sync_bootstrap_header TYPE string;
DEFINE FIELD IF NOT EXISTS consumed_receipt ON _sync_bootstrap_header TYPE option<bytes>;
DEFINE FIELD IF NOT EXISTS consumed_revision ON _sync_bootstrap_header TYPE option<string>;
DEFINE TABLE IF NOT EXISTS _sync_bootstrap_page SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS format_version ON _sync_bootstrap_page TYPE int;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS index ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_bootstrap_page TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_bootstrap_page FIELDS partition_id, client_id;
"#;

#[derive(SurrealValue)]
struct Header {
    format_version: i64,
    cancelled: bool,
    incarnation: String,
    start: Bytes,
    manifest: Bytes,
    consumed_receipt: Option<Bytes>,
    consumed_revision: Option<String>,
}
#[derive(SurrealValue)]
struct PageRow {
    format_version: i64,
    partition_id: String,
    client_id: String,
    index: String,
    payload: Bytes,
}
#[derive(SurrealValue)]
struct Fence {
    bootstrap_fence: Option<String>,
}
#[derive(SurrealValue)]
struct Schema {
    tables: BTreeMap<String, String>,
}
#[derive(SurrealValue)]
struct Lengths {
    bytes: u64,
    key_bytes: u64,
    canonical: bool,
}
struct Stage {
    header: Header,
    manifest: BootstrapManifest,
    pages: BTreeMap<u64, Vec<u8>>,
}

// This proof cannot outlive or switch its read/write transaction. Construction
// performs combined scalar admission before any stored stage/current blob load.
struct InspectedStage<'a> {
    transaction: &'a Transaction<Any>,
    key: SyncStateKey<'a>,
    current_budget: (usize, usize),
    stage: Option<Stage>,
}
impl InspectedStage<'_> {
    async fn current(&self) -> Result<crate::sync_state::StoredSyncState, SyncStateError> {
        crate::sync_state::transaction_state_after_preflight(self.transaction, self.key).await
    }
}

/// Supplied by authenticated composition on EACH completion attempt (including restart).
/// UTC must be independently trusted; monotonic deadline bounds verification stalls.
pub(crate) struct CompletionClock<'a> {
    pub(crate) utc_seconds: &'a (dyn Fn() -> Result<u64, SyncStateError> + Send + Sync),
    pub(crate) deadline: std::time::Instant,
}
impl CompletionClock<'_> {
    fn check(&self, expires_at: u64) -> Result<(), SyncStateError> {
        let now = std::time::Instant::now();
        if now >= self.deadline
            || self.deadline.duration_since(now) > std::time::Duration::from_secs(30)
            || (self.utc_seconds)()? >= expires_at
        {
            return Err(SyncStateError::RevisionConflict);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub(crate) enum Completion {
    Applied(Box<crate::sync_state::StoredSyncState>),
    /// Historical success identity ONLY: never publish a prepared historical runtime.
    HistoricalSuccess,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StageState {
    Staging,
    Cancelled,
    Consumed { revision: u64 },
}
pub(crate) struct StageInventory {
    pub(crate) incarnation: u64,
    pub(crate) state: StageState,
    pub(crate) indices: Vec<u64>,
    pub(crate) expected_pages: u64,
}
pub(crate) struct BootstrapStore<'a> {
    database: &'a SurrealDatabase,
    session_generation: Option<u64>,
}

impl<'a> BootstrapStore<'a> {
    pub(crate) fn new(database: &'a SurrealDatabase) -> Self {
        Self {
            database,
            session_generation: None,
        }
    }

    pub(crate) fn with_session_generation(mut self, generation: u64) -> Self {
        self.session_generation = Some(generation);
        self
    }
    async fn finish<T>(
        &self,
        transaction: Transaction<Any>,
        operation: Result<T, SyncStateError>,
    ) -> Result<T, SyncStateError> {
        let operation = operation.and_then(|value| {
            if self.session_generation.is_some_and(|generation| {
                self.database.is_closed()
                    || self
                        .database
                        .session_generation
                        .load(std::sync::atomic::Ordering::Acquire)
                        != generation
            }) {
                return Err(SyncStateError::RevisionConflict);
            }
            Ok(value)
        });
        finish_transaction(transaction, operation).await
    }

    /// Inspect ONLY. Missing schema/header is never initialized. A surviving fence,
    /// partial schema or orphan page is corruption, not an empty ready stage.
    pub(crate) async fn resume(
        &self,
        manifest: Option<&[u8]>,
        expected: &BootstrapContext,
        incarnation: Option<u64>,
    ) -> Result<Option<StageInventory>, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        if let Some(manifest) = manifest {
            if manifest.len() > MAX_SYNC_STATE_BYTES {
                return Err(SyncStateError::StateTooLarge);
            }
            let incoming =
                decode_bootstrap_manifest(manifest).map_err(|_| SyncStateError::CorruptState)?;
            if incoming.context != *expected {
                return Err(SyncStateError::CorruptState);
            }
        }
        let transaction = self
            .database
            .client()
            .await
            .map_err(SyncStateError::Core)?
            .begin()
            .await
            .map_err(SyncStateError::Database)?;
        let operation = async {
            let response = transaction
                .query("INFO FOR DB")
                .await
                .map_err(SyncStateError::Database)?;
            let mut response = match response.check() {
                Ok(response) => response,
                Err(error)
                    if matches!(
                        error.not_found_details(),
                        Some(
                            surrealdb::types::NotFoundError::Namespace { .. }
                                | surrealdb::types::NotFoundError::Database { .. }
                        )
                    ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(SyncStateError::Database(error)),
            };
            let schema: Option<Schema> = response.take(0).map_err(SyncStateError::Database)?;
            let tables = schema.ok_or(SyncStateError::CorruptState)?.tables;
            if !tables.contains_key(HEADER) && !tables.contains_key(PAGE) {
                if tables.contains_key("_sync_client_meta")
                    && read_fence(&transaction, key).await?.is_some()
                {
                    return Err(SyncStateError::CorruptState);
                }
                return Ok(None);
            }
            let mut inspected = inspect_stage(&transaction, key).await?;
            let Some(stage) = inspected.stage.take() else {
                return if read_fence(&transaction, key).await?.is_some() {
                    Err(SyncStateError::CorruptState)
                } else {
                    Ok(None)
                };
            };
            let persisted = &stage.manifest.context;
            // Factory inspection can advance a terminal authorization snapshot;
            // explicit resume and all stage operations retain exact context.
            admit_context(
                persisted,
                expected,
                manifest.is_none() && stage.header.cancelled,
            )?;
            validate_header(&stage.header.start, &stage.manifest, persisted)?;
            let current_incarnation = parse_fence(&stage.header.incarnation)?;
            if manifest.is_some_and(|expected| &stage.header.manifest[..] != expected)
                || incarnation.is_some_and(|n| n != current_incarnation)
            {
                return Err(SyncStateError::RevisionConflict);
            }
            let current = inspected.current().await?;
            check_budget(key, state_budget(&current)?, &stage)?;
            let state = if let Some(revision) = stage.header.consumed_revision {
                StageState::Consumed {
                    revision: parse_fence(&revision)?,
                }
            } else if stage.header.cancelled {
                StageState::Cancelled
            } else {
                StageState::Staging
            };
            Ok(Some(StageInventory {
                incarnation: current_incarnation,
                state,
                indices: stage.pages.keys().copied().collect(),
                expected_pages: stage.manifest.page_count,
            }))
        }
        .await;
        transaction
            .cancel()
            .await
            .map_err(SyncStateError::Database)?;
        operation
    }

    /// Only this explicit entry installs stage schema/the optional metadata fence.
    /// Expected context is independently supplied by authenticated composition, not the body.
    pub(crate) async fn begin(
        &self,
        start: &[u8],
        manifest: &[u8],
        expected: &BootstrapContext,
    ) -> Result<u64, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let decoded =
            decode_bootstrap_manifest(manifest).map_err(|_| SyncStateError::CorruptState)?;
        validate_header(start, &decoded, expected)?;
        if start
            .len()
            .checked_add(manifest.len())
            .is_none_or(|n| n > MAX_SYNC_STATE_BYTES)
        {
            return Err(SyncStateError::StateTooLarge);
        }
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            transaction
                .query(SCHEMA)
                .await
                .map_err(SyncStateError::Database)?
                .check()
                .map_err(SyncStateError::Database)?;
            // Combined persisted stage/current scalar admission MUST precede any
            // current-state payload decoding, including on exact begin retry.
            let inspection = inspect_stage(&transaction, key).await?;
            if inspection.stage.is_none() {
                let current = inspection.current_budget;
                let incoming = start.len() + manifest.len();
                if current
                    .0
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(FINAL_OVERHEAD))
                    .and_then(|n| n.checked_add(incoming))
                    .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
                    || current
                        .1
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(4))
                        .is_none_or(|n| n > MAX_COMPONENT_ITEMS)
                {
                    return Err(SyncStateError::StateTooLarge);
                }
            }
            let state = inspection.current().await?;
            let existing = inspection.stage;
            check_barrier(&transaction, key).await?;
            if read_fence(&transaction, key).await?.is_none() {
                fence_first_barrier(&transaction, key, &decoded.context).await?;
            }
            if state.requested_scope != expected.scope.identity
                || state.subscription_revision != expected.scope.subscription_revision
            {
                return Err(SyncStateError::CorruptState);
            }
            if state.checkpoint.as_ref().is_some_and(|checkpoint| {
                expected.scope.authorization_revision < checkpoint.scope.authorization_revision
            }) {
                return Err(SyncStateError::RevisionConflict);
            }
            if let Some(existing) = existing {
                // A terminal stage permits a newer authorization snapshot, never
                // another owner/origin/generation or a downgrade.
                admit_context(
                    &existing.manifest.context,
                    expected,
                    existing.header.cancelled,
                )?;
                validate_header(
                    &existing.header.start,
                    &existing.manifest,
                    &existing.manifest.context,
                )?;
                check_budget(key, state_budget(&state)?, &existing)?;
                if existing.header.cancelled {
                    if &existing.header.manifest[..] == manifest {
                        return Err(SyncStateError::RevisionConflict);
                    }
                } else {
                    validate_header(&existing.header.start, &existing.manifest, expected)?;
                    return if &existing.header.start[..] == start
                        && &existing.header.manifest[..] == manifest
                    {
                        parse_fence(&existing.header.incarnation)
                    } else {
                        Err(SyncStateError::RevisionConflict)
                    };
                }
            }
            let next = next_fence(read_fence(&transaction, key).await?)?;
            let incarnation = parse_metadata_fence(&next)?.0;
            let next = format!("{incarnation}:a");
            let stage = Stage {
                header: Header {
                    format_version: 1,
                    consumed_receipt: None,
                    consumed_revision: None,
                    cancelled: false,
                    incarnation: incarnation.to_string(),
                    start: start.to_vec().into(),
                    manifest: manifest.to_vec().into(),
                },
                manifest: decoded,
                pages: BTreeMap::new(),
            };
            check_budget(key, state_budget(&state)?, &stage)?;
            let _: Option<Value> = transaction
                .upsert(header_id(key))
                .content(stage.header)
                .await
                .map_err(SyncStateError::Database)?;
            write_fence(&transaction, key, next).await?;
            Ok(incarnation)
        }
        .await;
        self.finish(transaction, operation).await
    }

    pub(crate) async fn stage_page(
        &self,
        raw: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<(), SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let page = decode_bootstrap_page(raw).map_err(|_| SyncStateError::CorruptState)?;
        // Validate native admissibility/encoded reservation before any persistence.
        page_reservation(&page)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            let fence = read_fence(&transaction, key)
                .await?
                .ok_or(SyncStateError::CorruptState)?;
            let mut inspection = inspect_stage(&transaction, key).await?;
            let mut stage = inspection
                .stage
                .take()
                .ok_or(SyncStateError::CorruptState)?;
            validate_header(&stage.header.start, &stage.manifest, expected)?;
            if parse_fence(&stage.header.incarnation)? != incarnation {
                return Err(SyncStateError::RevisionConflict);
            }
            if stage.header.cancelled {
                return Err(SyncStateError::RevisionConflict);
            }
            check_barrier(&transaction, key).await?;
            validate_page(&stage.manifest, &page)?;
            if let Some(old) = stage.pages.get(&page.index) {
                return if old == raw {
                    Ok(())
                } else {
                    Err(SyncStateError::RevisionConflict)
                };
            }
            stage.pages.insert(page.index, raw.to_vec());
            let state = inspection.current().await?;
            check_budget(key, state_budget(&state)?, &stage)?;
            let row = PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: page.index.to_string(),
                payload: raw.to_vec().into(),
            };
            let _: Option<Value> = transaction
                .upsert(page_id(key, page.index))
                .content(row)
                .await
                .map_err(SyncStateError::Database)?;
            write_fence(&transaction, key, next_fence(Some(fence))?).await
        }
        .await;
        self.finish(transaction, operation).await
    }

    /// Trusted context/clock are independent of the decoded receipt. No authorization is
    /// inferred from wire bytes. Fresh authenticated completion is required after restart.
    pub(crate) async fn complete(
        &self,
        receipt_bytes: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
        clock: &CompletionClock<'_>,
    ) -> Result<Completion, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        if receipt_bytes.len() > MAX_SYNC_STATE_BYTES {
            return Err(SyncStateError::StateTooLarge);
        }
        let receipt =
            decode_bootstrap_receipt(receipt_bytes).map_err(|_| SyncStateError::CorruptState)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = self
            .complete_in_transaction(
                &transaction,
                receipt_bytes,
                &receipt,
                expected,
                incarnation,
                clock,
            )
            .await;
        self.finish(transaction, operation).await
    }

    async fn complete_in_transaction(
        &self,
        transaction: &Transaction<Any>,
        receipt_bytes: &[u8],
        receipt: &surrealdb_sync_protocol::bootstrap::BootstrapReceipt,
        expected: &BootstrapContext,
        incarnation: u64,
        clock: &CompletionClock<'_>,
    ) -> Result<Completion, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        let mut inspection = inspect_stage(transaction, key).await?;
        let stage = inspection
            .stage
            .take()
            .ok_or(SyncStateError::CorruptState)?;
        validate_header(&stage.header.start, &stage.manifest, expected)?;
        if parse_fence(&stage.header.incarnation)? != incarnation {
            return Err(SyncStateError::RevisionConflict);
        }
        validate_bootstrap_receipt(&stage.manifest, receipt)
            .map_err(|_| SyncStateError::CorruptState)?;
        if let Some(consumed) = &stage.header.consumed_receipt {
            if &consumed[..] != receipt_bytes {
                return Err(SyncStateError::RevisionConflict);
            }
            let current = inspection.current().await?;
            if current.revision
                < parse_fence(
                    stage
                        .header
                        .consumed_revision
                        .as_ref()
                        .ok_or(SyncStateError::CorruptState)?,
                )?
            {
                return Err(SyncStateError::CorruptState);
            }
            // Readback validates the latest journal, but never clears/rewrites its barrier.
            return Ok(Completion::HistoricalSuccess);
        }
        if stage.header.cancelled {
            return Err(SyncStateError::RevisionConflict);
        }
        check_barrier(transaction, key).await?;
        clock.check(stage.manifest.expires_at)?;
        let current = inspection.current().await?;
        check_budget(key, state_budget(&current)?, &stage)?;
        let mut verifier = BootstrapVerifier::new(stage.manifest.clone())
            .map_err(|_| SyncStateError::CorruptState)?;
        let mut records = Vec::new();
        for raw in stage.pages.values() {
            clock.check(stage.manifest.expires_at)?;
            let page = decode_bootstrap_page(raw).map_err(|_| SyncStateError::CorruptState)?;
            verifier
                .push(&page)
                .map_err(|_| SyncStateError::CorruptState)?;
            page_reservation(&page)?;
            for record in page.records {
                let state = match record.state {
                    RecordState::Present {
                        value,
                        version,
                        reference,
                    } => RecordState::Present {
                        value: crate::sync_http_codec::canonical_to_json(value)
                            .map_err(|_| SyncStateError::CorruptState)?,
                        version,
                        reference,
                    },
                    RecordState::Tombstone { version } => RecordState::Tombstone { version },
                    RecordState::Absent => return Err(SyncStateError::CorruptState),
                };
                records.push(surrealdb_sync_protocol::SnapshotRecord {
                    record_id: record.record_id,
                    state,
                });
            }
        }
        verifier
            .finish()
            .map_err(|_| SyncStateError::CorruptState)?;
        clock.check(stage.manifest.expires_at)?;
        let runtime = surrealdb_sync_client::ClientRuntime::open(current.clone())
            .map_err(SyncStateError::InvalidState)?;
        let next = runtime
            .prepare_bootstrap_snapshot(receipt.checkpoint.clone(), records)
            .map_err(SyncStateError::InvalidState)?
            .into_state();
        crate::sync_codec::validate_state_codec(&next).map_err(|_| SyncStateError::CorruptState)?;
        final_budget(key, &current, &next, &stage, receipt_bytes)?;
        // Prior staging schemas predate terminal marker fields. Only explicit successful
        // completion installs these definitions, atomically with the final marker.
        transaction.query("DEFINE FIELD IF NOT EXISTS consumed_receipt ON _sync_bootstrap_header TYPE option<bytes>; DEFINE FIELD IF NOT EXISTS consumed_revision ON _sync_bootstrap_header TYPE option<string>;").await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
        crate::sync_state::apply_transition_in_transaction(transaction, &current, &next).await?;
        for index in stage.pages.keys() {
            let _: Option<Value> = transaction
                .delete(page_id(key, *index))
                .await
                .map_err(SyncStateError::Database)?;
        }
        let marker: Option<Header> = transaction
            .upsert(header_id(key))
            .content(Header {
                cancelled: true,
                consumed_receipt: Some(receipt_bytes.to_vec().into()),
                consumed_revision: Some(next.revision.to_string()),
                ..stage.header
            })
            .await
            .map_err(SyncStateError::Database)?;
        let marker = marker.ok_or(SyncStateError::CorruptState)?;
        if marker.consumed_receipt.as_ref().map(|bytes| &bytes[..]) != Some(receipt_bytes)
            || marker.consumed_revision.as_deref() != Some(next.revision.to_string().as_str())
            || !marker.cancelled
        {
            return Err(SyncStateError::CorruptState);
        }
        let fence = next_fence(read_fence(transaction, key).await?)?;
        write_fence(
            transaction,
            key,
            format!("{}:i", parse_metadata_fence(&fence)?.0),
        )
        .await?;
        // All tentative writes roll back if verification/preparation exhausted the deadline.
        clock.check(stage.manifest.expires_at)?;
        Ok(Completion::Applied(Box::new(next)))
    }

    /// Progress grants no coverage/read revision or completion. It performs bounded
    /// full stage/current integrity validation, NOT a metadata-only/constant-time read.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) async fn progress(
        &self,
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<Option<Vec<u64>>, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            if read_fence(&transaction, key).await?.is_none() {
                return Ok(None);
            }
            let mut inspection = inspect_stage(&transaction, key).await?;
            let Some(stage) = inspection.stage.take() else {
                return Ok(None);
            };
            validate_header(&stage.header.start, &stage.manifest, expected)?;
            if parse_fence(&stage.header.incarnation)? != incarnation {
                return Err(SyncStateError::RevisionConflict);
            }
            if stage.header.cancelled {
                return Ok(None);
            }
            let state = inspection.current().await?;
            check_budget(key, state_budget(&state)?, &stage)?;
            Ok(Some(stage.pages.keys().copied().collect()))
        }
        .await;
        transaction
            .cancel()
            .await
            .map_err(SyncStateError::Database)?;
        operation
    }

    /// Full context AND manifest bytes bind cancellation; delayed callers cannot erase a successor.
    pub(crate) async fn cancel(
        &self,
        manifest: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<(), SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = self
            .cancel_in_transaction(&transaction, manifest, expected, incarnation)
            .await;
        self.finish(transaction, operation).await
    }
    async fn cancel_in_transaction(
        &self,
        transaction: &Transaction<Any>,
        manifest: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<(), SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        let Some(fence) = read_fence(transaction, key).await? else {
            return Ok(());
        };
        let Some(stage) = load_stage(transaction, key).await? else {
            return Ok(());
        };
        validate_header(&stage.header.start, &stage.manifest, expected)?;
        if parse_fence(&stage.header.incarnation)? != incarnation {
            return Err(SyncStateError::RevisionConflict);
        }
        if &stage.header.manifest[..] != manifest {
            return Err(SyncStateError::RevisionConflict);
        }
        if stage.header.consumed_receipt.is_some() {
            return Ok(());
        }
        for index in stage.pages.keys() {
            let _: Option<Value> = transaction
                .delete(page_id(key, *index))
                .await
                .map_err(SyncStateError::Database)?;
        }
        let _: Option<Value> = transaction
            .upsert(header_id(key))
            .content(Header {
                cancelled: true,
                ..stage.header
            })
            .await
            .map_err(SyncStateError::Database)?;
        let next = next_fence(Some(fence))?;
        write_fence(
            transaction,
            key,
            format!("{}:i", parse_metadata_fence(&next)?.0),
        )
        .await
    }
}

/// Ownership is fixed for the journal; authorization snapshots are stage-local.
pub(crate) fn same_owner(a: &BootstrapContext, b: &BootstrapContext) -> bool {
    a.authority_id == b.authority_id
        && a.principal_id == b.principal_id
        && a.partition_id == b.partition_id
        && a.client_id == b.client_id
        && a.journal_generation == b.journal_generation
        && a.scope.identity == b.scope.identity
        && a.scope.subscription_revision == b.scope.subscription_revision
}
fn admit_context(
    previous: &BootstrapContext,
    incoming: &BootstrapContext,
    terminal: bool,
) -> Result<(), SyncStateError> {
    if !same_owner(previous, incoming)
        || incoming.scope.authorization_revision < previous.scope.authorization_revision
        || (!terminal && previous != incoming)
    {
        return Err(SyncStateError::RevisionConflict);
    }
    Ok(())
}

fn validate_header(
    start: &[u8],
    manifest: &BootstrapManifest,
    expected: &BootstrapContext,
) -> Result<(), SyncStateError> {
    let start = decode_bootstrap_start(start).map_err(|_| SyncStateError::CorruptState)?;
    validate_bootstrap_manifest_context(manifest, &start, expected)
        .map_err(|_| SyncStateError::CorruptState)
}
fn validate_page(manifest: &BootstrapManifest, page: &BootstrapPage) -> Result<(), SyncStateError> {
    if page.session_id != manifest.session_id
        || page.manifest_digest
            != bootstrap_manifest_digest(manifest).map_err(|_| SyncStateError::CorruptState)?
        || page.index >= manifest.page_count
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}
fn page_reservation(page: &BootstrapPage) -> Result<usize, SyncStateError> {
    page.records.iter().try_fold(0usize, |sum, record| {
        let native_id = crate::sync_state::native_record_id(&record.record_id)?;
        if native_id.table.as_str().starts_with("_sync_") {
            return Err(SyncStateError::UnsupportedRecordId);
        }
        let state = match &record.state {
            RecordState::Present {
                value,
                version,
                reference,
            } => {
                if !matches!(value, surrealdb_sync_protocol::CanonicalValue::Object(_)) {
                    return Err(SyncStateError::RecordValueMustBeObject);
                }
                let value = crate::sync_http_codec::canonical_to_json(value.clone())
                    .map_err(|_| SyncStateError::CorruptState)?;
                if !matches!(
                    crate::wire::from_wire_value(value.clone())
                        .map_err(SyncStateError::ValueCodec)?,
                    Value::Object(_)
                ) {
                    return Err(SyncStateError::RecordValueMustBeObject);
                }
                RecordState::Present {
                    value,
                    version: *version,
                    reference: reference.clone(),
                }
            }
            RecordState::Tombstone { version } => RecordState::Tombstone { version: *version },
            RecordState::Absent => return Err(SyncStateError::CorruptState),
        };
        // Both the actual tagged payload and its final metadata key are charged.
        let bytes = encode_tagged(&state)?
            .len()
            .checked_add(encode_tagged(&record.record_id)?.len())
            .ok_or(SyncStateError::StateTooLarge)?;
        sum.checked_add(bytes).ok_or(SyncStateError::StateTooLarge)
    })
}
fn check_budget(
    key: SyncStateKey<'_>,
    current: (usize, usize),
    stage: &Stage,
) -> Result<(), SyncStateError> {
    if stage.header.cancelled {
        if !stage.pages.is_empty() {
            return Err(SyncStateError::CorruptState);
        }
        return check_inactive_budget(current);
    }
    let mut bytes = current
        .0
        .checked_mul(2)
        .and_then(|n| n.checked_add(FINAL_OVERHEAD))
        .and_then(|n| n.checked_add(stage.header.start.len() + stage.header.manifest.len()))
        .ok_or(SyncStateError::StateTooLarge)?;
    let mut count = current
        .1
        .checked_mul(2)
        .and_then(|n| n.checked_add(4))
        .ok_or(SyncStateError::StateTooLarge)?;
    let mut records = 0u64;
    let mut ids = BTreeSet::new();
    let mut last_id = None;
    for (index, raw) in &stage.pages {
        let page = decode_bootstrap_page(raw).map_err(|_| SyncStateError::CorruptState)?;
        validate_page(&stage.manifest, &page)?;
        if page.index != *index {
            return Err(SyncStateError::CorruptState);
        }
        for record in &page.records {
            if !ids.insert(record.record_id.clone())
                || last_id
                    .as_ref()
                    .is_some_and(|last| last >= &record.record_id)
            {
                return Err(SyncStateError::CorruptState);
            }
            last_id = Some(record.record_id.clone());
        }
        records = records
            .checked_add(page.records.len() as u64)
            .ok_or(SyncStateError::StateTooLarge)?;
        let key_bytes = encode_tagged(&(
            key.partition_id.0.as_str(),
            key.client_id.0.as_str(),
            index.to_string(),
        ))?
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(128))
        .ok_or(SyncStateError::StateTooLarge)?;
        let reservation = page_reservation(&page)?;
        bytes = bytes
            .checked_add(raw.len())
            .and_then(|n| n.checked_add(reservation))
            .and_then(|n| n.checked_add(key_bytes))
            .ok_or(SyncStateError::StateTooLarge)?;
        count = count
            .checked_add(page.records.len() + 1)
            .ok_or(SyncStateError::StateTooLarge)?;
    }
    // Header key and rotating fence are real encoded overhead too.
    bytes = bytes
        .checked_add(
            encode_tagged(&(
                key.partition_id.0.as_str(),
                key.client_id.0.as_str(),
                u64::MAX.to_string(),
            ))?
            .len(),
        )
        .ok_or(SyncStateError::StateTooLarge)?;
    if bytes > MAX_AGGREGATE_STATE_BYTES
        || count > MAX_COMPONENT_ITEMS
        || records > stage.manifest.record_count
    {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}

fn final_budget(
    key: SyncStateKey<'_>,
    current: &crate::sync_state::StoredSyncState,
    next: &crate::sync_state::StoredSyncState,
    stage: &Stage,
    receipt: &[u8],
) -> Result<(), SyncStateError> {
    let current = state_budget(current)?;
    let next = state_budget(next)?;
    let header = stage.header.start.len() + stage.header.manifest.len() + receipt.len();
    if header > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    // Include both old and final encoded state, ALL raw staged pages, terminal header,
    // original header, exact key framing and bounded fence/revision/marker overhead.
    let mut bytes = current
        .0
        .checked_add(next.0)
        .and_then(|n| n.checked_add(FINAL_OVERHEAD))
        .and_then(|n| n.checked_add(header))
        .ok_or(SyncStateError::StateTooLarge)?;
    let mut count = current
        .1
        .checked_add(next.1)
        .and_then(|n| n.checked_add(4))
        .ok_or(SyncStateError::StateTooLarge)?;
    for (index, raw) in &stage.pages {
        bytes = bytes
            .checked_add(raw.len())
            .and_then(|n| {
                n.checked_add(
                    encode_tagged(&(
                        key.partition_id.0.as_str(),
                        key.client_id.0.as_str(),
                        index.to_string(),
                    ))
                    .ok()?
                    .len()
                        * 2
                        + 128,
                )
            })
            .ok_or(SyncStateError::StateTooLarge)?;
        count = count.checked_add(1).ok_or(SyncStateError::StateTooLarge)?;
    }
    if bytes > MAX_AGGREGATE_STATE_BYTES || count > MAX_COMPONENT_ITEMS {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}

async fn read_fence(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<Option<String>, SyncStateError> {
    // Meta exists on every initialized v1/v2 journal. This never touches stage schema.
    let mut response = transaction
        .query("SELECT IF bootstrap_fence = NONE THEN NONE ELSE string::slice(bootstrap_fence,0,24) END AS bootstrap_fence FROM $id")
        .bind(("id", meta_record_id(key)))
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    let rows: Vec<Fence> = response.take(0).map_err(SyncStateError::Database)?;
    let fence = rows.into_iter().next().and_then(|r| r.bootstrap_fence);
    if let Some(value) = &fence {
        parse_metadata_fence(value)?;
    }
    Ok(fence)
}
fn parse_fence(raw: &str) -> Result<u64, SyncStateError> {
    let number = raw
        .parse::<u64>()
        .map_err(|_| SyncStateError::CorruptState)?;
    if number.to_string() != raw {
        return Err(SyncStateError::CorruptState);
    }
    Ok(number)
}
fn parse_metadata_fence(raw: &str) -> Result<(u64, bool), SyncStateError> {
    let (counter, active) = raw.split_once(':').ok_or(SyncStateError::CorruptState)?;
    let active = match active {
        "a" => true,
        "i" => false,
        _ => return Err(SyncStateError::CorruptState),
    };
    Ok((parse_fence(counter)?, active))
}
fn next_fence(raw: Option<String>) -> Result<String, SyncStateError> {
    let (counter, active) = raw.map_or(Ok((0, true)), |v| parse_metadata_fence(&v))?;
    let next = counter
        .checked_add(1)
        .ok_or(SyncStateError::StateTooLarge)?;
    Ok(format!("{next}:{}", if active { "a" } else { "i" }))
}
fn check_inactive_budget(budget: (usize, usize)) -> Result<(), SyncStateError> {
    // One bounded closed marker remains; no download/final-state reservation survives cancel.
    if budget
        .0
        .checked_add(MAX_SYNC_STATE_BYTES + 65_536)
        .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
        || budget
            .1
            .checked_add(4)
            .is_none_or(|n| n > MAX_COMPONENT_ITEMS)
    {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}
pub(crate) async fn write_fence(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    fence: String,
) -> Result<(), SyncStateError> {
    transaction
        .query("UPDATE $id SET bootstrap_fence = $fence")
        .bind(("id", meta_record_id(key)))
        .bind(("fence", fence))
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    Ok(())
}
pub(crate) async fn guard_state_write(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    budget: (usize, usize),
) -> Result<Option<String>, SyncStateError> {
    let Some(fence) = read_fence(transaction, key).await? else {
        return Ok(None);
    };
    if parse_metadata_fence(&fence)?.1 {
        let stage = load_stage(transaction, key)
            .await?
            .ok_or(SyncStateError::CorruptState)?;
        if stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_budget(key, budget, &stage)?;
    } else {
        let stage = load_stage(transaction, key)
            .await?
            .ok_or(SyncStateError::CorruptState)?;
        if !stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_inactive_budget(budget)?;
    }
    Ok(Some(next_fence(Some(fence))?))
}
pub(crate) async fn guard_barrier_write(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<(), SyncStateError> {
    let Some(fence) = read_fence(transaction, key).await? else {
        // First begin itself writes the barrier-key intent, so the ordinary
        // pre-bootstrap barrier path needs no legacy metadata mutation.
        return Ok(());
    };
    let inspection = inspect_stage(transaction, key).await?;
    let stage = inspection
        .stage
        .as_ref()
        .ok_or(SyncStateError::CorruptState)?;
    let state = inspection.current().await?;
    if parse_metadata_fence(&fence)?.1 {
        check_budget(key, state_budget(&state)?, stage)?;
    } else {
        if !stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_inactive_budget(state_budget(&state)?)?;
    }
    write_fence(transaction, key, next_fence(Some(fence))?).await
}
/// A first begin shares a real write intent with an otherwise metadata-independent
/// barrier writer. The valid temporary row is removed before commit; no barrier
/// schema/decoder changes or ordinary v1 metadata rewrites are needed.
/// Pinned memory AND SurrealKV conflict/absence tests are an upgrade prerequisite.
async fn fence_first_barrier(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    context: &BootstrapContext,
) -> Result<(), SyncStateError> {
    use crate::sync_state::ScopeBarrier;
    check_barrier(transaction, key).await?;
    let id = barrier_id(key);
    let temporary = ScopeBarrier {
        reason: "needs_new_scope_journal".into(),
        required_scope: context.scope.identity.0.clone(),
        required_subscription_revision: context.scope.subscription_revision.to_string(),
        required_authorization_revision: context.scope.authorization_revision.to_string(),
    };
    let created: Option<ScopeBarrier> = transaction
        .create(id.clone())
        .content(temporary.clone())
        .await
        .map_err(SyncStateError::Database)?;
    let deleted: Option<ScopeBarrier> = transaction
        .delete(id.clone())
        .await
        .map_err(SyncStateError::Database)?;
    let absent: Option<ScopeBarrier> = transaction
        .select(id)
        .await
        .map_err(SyncStateError::Database)?;
    let matches = |row: &ScopeBarrier| {
        row.reason == temporary.reason
            && row.required_scope == temporary.required_scope
            && row.required_subscription_revision == temporary.required_subscription_revision
            && row.required_authorization_revision == temporary.required_authorization_revision
    };
    if !created.as_ref().is_some_and(matches)
        || !deleted.as_ref().is_some_and(matches)
        || absent.is_some()
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}
fn barrier_id(key: SyncStateKey<'_>) -> RecordId {
    RecordId::new(
        "_sync_client_scope_barrier",
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}
async fn check_barrier(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<(), SyncStateError> {
    let id = barrier_id(key);
    let barrier: Option<Value> = transaction
        .select(id)
        .await
        .map_err(SyncStateError::Database)?;
    if barrier.is_some() {
        return Err(SyncStateError::RevisionConflict);
    }
    Ok(())
}
fn header_id(key: SyncStateKey<'_>) -> RecordId {
    RecordId::new(
        HEADER,
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}
fn page_id(key: SyncStateKey<'_>, index: u64) -> RecordId {
    RecordId::new(
        PAGE,
        Array::from(vec![
            key.partition_id.0.clone(),
            key.client_id.0.clone(),
            index.to_string(),
        ]),
    )
}

/// Whole-physical-store no-init bootstrap ownership and codec admission.
/// Terminal header authorization may predate current journal authorization.
pub(crate) async fn inspect_all_owners(
    transaction: &Transaction<Any>,
    tables: &BTreeSet<String>,
    key: SyncStateKey<'_>,
) -> Result<bool, SyncStateError> {
    if tables.contains(HEADER) != tables.contains(PAGE) {
        return Err(SyncStateError::CorruptState);
    }
    let mut occupied = false;
    for table in [HEADER, PAGE] {
        if !tables.contains(table) {
            continue;
        }
        let predicate = if table == HEADER {
            "record::id(id)=[$partition,$client]"
        } else {
            "type::is_string(partition_id) AND type::is_string(client_id) AND partition_id=$partition AND client_id=$client AND type::is_string(index) AND string::len(index) <= 20 AND record::id(id)=[$partition,$client,index]"
        };
        let mut response = transaction.query(format!("SELECT VALUE true FROM {table} WHERE NOT ({predicate}) LIMIT 1; SELECT VALUE true FROM {table} LIMIT 1"))
            .bind(("partition", key.partition_id.0.clone())).bind(("client", key.client_id.0.clone()))
            .await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
        let foreign: Vec<bool> = response.take(0).map_err(SyncStateError::Database)?;
        let present: Vec<bool> = response.take(1).map_err(SyncStateError::Database)?;
        if !foreign.is_empty() {
            return Err(SyncStateError::CorruptState);
        }
        occupied |= !present.is_empty();
    }
    if !occupied
        && tables.contains("_sync_client_meta")
        && read_fence(transaction, key).await?.is_some()
    {
        return Err(SyncStateError::CorruptState);
    }
    if occupied {
        if !tables.contains(HEADER) || !tables.contains(PAGE) {
            return Err(SyncStateError::CorruptState);
        }
        let inspected = inspect_stage_with_tables(transaction, key, tables).await?;
        let stage = inspected.stage.ok_or(SyncStateError::CorruptState)?;
        for (index, bytes) in &stage.pages {
            let page = decode_bootstrap_page(bytes).map_err(|_| SyncStateError::CorruptState)?;
            if page.index != *index {
                return Err(SyncStateError::CorruptState);
            }
            validate_page(&stage.manifest, &page)?;
        }
    }
    Ok(occupied)
}

async fn load_stage(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<Option<Stage>, SyncStateError> {
    Ok(inspect_stage(transaction, key).await?.stage)
}

async fn inspect_stage<'a>(
    transaction: &'a Transaction<Any>,
    key: SyncStateKey<'a>,
) -> Result<InspectedStage<'a>, SyncStateError> {
    let mut response = transaction
        .query("INFO FOR DB")
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    let schema: Option<Schema> = response.take(0).map_err(SyncStateError::Database)?;
    let schema = schema.ok_or(SyncStateError::CorruptState)?;
    inspect_stage_with_tables(transaction, key, &schema.tables.into_keys().collect()).await
}
async fn inspect_stage_with_tables<'a>(
    transaction: &'a Transaction<Any>,
    key: SyncStateKey<'a>,
    tables: &BTreeSet<String>,
) -> Result<InspectedStage<'a>, SyncStateError> {
    if !tables.contains(HEADER) || !tables.contains(PAGE) {
        return Err(SyncStateError::CorruptState);
    }
    // Scalars first, including header bytes. No unbounded blob is fetched before this guard.
    let mut lengths = transaction.query("SELECT bytes::len(start) + bytes::len(manifest) + (IF consumed_receipt = NONE { 0 } ELSE { bytes::len(consumed_receipt) }) AS bytes, bytes::len(<bytes>incarnation) + (IF consumed_revision = NONE { 0 } ELSE { bytes::len(<bytes>consumed_revision) }) AS key_bytes, (format_version = 1 AND array::len(array::complement(object::keys($this), [\"id\",\"format_version\",\"start\",\"manifest\",\"cancelled\",\"incarnation\",\"consumed_receipt\",\"consumed_revision\"])) = 0 AND ((consumed_receipt = NONE AND consumed_revision = NONE) OR (type::is_bytes(consumed_receipt) AND type::is_string(consumed_revision) AND cancelled = true))) AS canonical FROM $header; SELECT bytes::len(payload) AS bytes, bytes::len(<bytes>encoding::json::encode([partition_id,client_id,index])) AS key_bytes, (format_version = 1 AND record::id(id) = [partition_id,client_id,index]) AS canonical FROM _sync_bootstrap_page WHERE partition_id = $partition AND client_id = $client LIMIT 16385")
        .bind(("header", header_id(key))).bind(("partition", key.partition_id.0.clone())).bind(("client", key.client_id.0.clone())).await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
    let headers: Vec<Lengths> = lengths.take(0).map_err(SyncStateError::Database)?;
    let pages: Vec<Lengths> = lengths.take(1).map_err(SyncStateError::Database)?;
    let current = crate::sync_state::preflight_inspection(transaction, tables, key).await?;
    let mut aggregate = 0usize;
    for row in headers.iter().chain(&pages) {
        if !row.canonical {
            return Err(SyncStateError::CorruptState);
        }
        if row.bytes > MAX_SYNC_STATE_BYTES as u64 || row.key_bytes > (12 * 512 + 128) {
            return Err(SyncStateError::StateTooLarge);
        }
        aggregate = aggregate
            .checked_add(row.bytes as usize)
            .and_then(|n| n.checked_add(row.key_bytes as usize))
            .ok_or(SyncStateError::StateTooLarge)?;
    }
    if aggregate
        .checked_add(current.0)
        .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
        || pages.len() + headers.len() + current.1 > MAX_COMPONENT_ITEMS
    {
        return Err(SyncStateError::StateTooLarge);
    }
    let header: Option<Header> = transaction
        .select(header_id(key))
        .await
        .map_err(SyncStateError::Database)?;
    let Some(header) = header else {
        return if pages.is_empty() {
            Ok(InspectedStage {
                transaction,
                key,
                current_budget: current,
                stage: None,
            })
        } else {
            Err(SyncStateError::CorruptState)
        };
    };
    if header.consumed_receipt.is_some() != header.consumed_revision.is_some() {
        return Err(SyncStateError::CorruptState);
    }
    if let Some(revision) = &header.consumed_revision {
        if parse_fence(revision)? == 0 {
            return Err(SyncStateError::CorruptState);
        }
        if !header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
    }
    let incarnation = parse_fence(&header.incarnation)?;
    let fence = read_fence(transaction, key)
        .await?
        .ok_or(SyncStateError::CorruptState)?;
    let (counter, active) = parse_metadata_fence(&fence)?;
    if header.cancelled == active || incarnation > counter {
        return Err(SyncStateError::CorruptState);
    }
    let manifest =
        decode_bootstrap_manifest(&header.manifest).map_err(|_| SyncStateError::CorruptState)?;
    validate_header(&header.start, &manifest, &manifest.context)?;
    if let Some(bytes) = &header.consumed_receipt {
        let receipt = decode_bootstrap_receipt(bytes).map_err(|_| SyncStateError::CorruptState)?;
        validate_bootstrap_receipt(&manifest, &receipt)
            .map_err(|_| SyncStateError::CorruptState)?;
    }
    if manifest.context.partition_id != *key.partition_id
        || manifest.context.client_id != *key.client_id
        || (header.cancelled && !pages.is_empty())
    {
        return Err(SyncStateError::CorruptState);
    }
    let mut response = transaction.query("SELECT * FROM _sync_bootstrap_page WHERE partition_id = $partition AND client_id = $client LIMIT 16385").bind(("partition", key.partition_id.0.clone())).bind(("client", key.client_id.0.clone())).await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
    let rows: Vec<PageRow> = response.take(0).map_err(SyncStateError::Database)?;
    let mut pages = BTreeMap::new();
    for row in rows {
        let index = parse_fence(&row.index)?;
        if row.partition_id != key.partition_id.0
            || row.client_id != key.client_id.0
            || pages.insert(index, row.payload.to_vec()).is_some()
        {
            return Err(SyncStateError::CorruptState);
        }
    }
    Ok(InspectedStage {
        transaction,
        key,
        current_budget: current,
        stage: Some(Stage {
            header,
            manifest,
            pages,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_state::{ScopeBarrier, StoredSyncState, SyncStateStore};
    use crate::{ConnectOptions, connect};
    use surrealdb_sync_client::ClientRuntime;
    use surrealdb_sync_protocol::bootstrap::{
        BootstrapDigest, BootstrapLimits, BootstrapStartRequest,
    };
    use surrealdb_sync_protocol::bootstrap_codec::{
        BootstrapContentHasher, bootstrap_start_fingerprint, encode_bootstrap_manifest,
        encode_bootstrap_page, encode_bootstrap_start,
    };
    use surrealdb_sync_protocol::{
        CanonicalValue, ClientCommit, ClientCommitId, ClientId, CommitIdentity, Cursor,
        Fingerprint, Operation, PartitionId, RecordId as ProtocolId, ScopeIdentity, ScopeSnapshot,
        SnapshotRecord,
    };

    async fn database() -> std::sync::Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("bootstrap".into()),
            database: Some("bootstrap".into()),
        })
        .await
        .unwrap()
    }
    fn state() -> StoredSyncState {
        StoredSyncState::empty(
            PartitionId("partition".into()),
            ClientId("client".into()),
            ScopeIdentity("all".into()),
            1,
        )
    }
    async fn initialized(database: &SurrealDatabase) -> StoredSyncState {
        let state = state();
        database
            .client()
            .await
            .unwrap()
            .query("DEFINE TABLE item SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let store = SyncStateStore::new(database);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        state
    }
    fn pending(state: StoredSyncState, id: &str) -> StoredSyncState {
        ClientRuntime::open(state)
            .unwrap()
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId(id.into()),
                    fingerprint: Fingerprint(id.into()),
                },
                operations: vec![Operation::Delete {
                    record_id: ProtocolId("item:pending".into()),
                    base_version: 0,
                }],
            })
            .unwrap()
            .into_state()
    }
    fn wire(session: &str) -> (Vec<u8>, Vec<u8>, BootstrapContext, Vec<Vec<u8>>) {
        let state = state();
        let context = BootstrapContext {
            authority_id: "authority".into(),
            principal_id: "owner".into(),
            partition_id: state.partition_id,
            client_id: state.client_id,
            journal_generation: "generation".into(),
            scope: ScopeSnapshot {
                identity: state.requested_scope,
                authorization_revision: 1,
                subscription_revision: 1,
            },
        };
        let start = BootstrapStartRequest {
            partition_id: context.partition_id.clone(),
            client_id: context.client_id.clone(),
            request_id: format!("request-{session}"),
            requested_scope: context.scope.identity.clone(),
            subscription_revision: 1,
            journal_generation: context.journal_generation.clone(),
        };
        let mut pages = (0..2)
            .map(|index| BootstrapPage {
                session_id: session.into(),
                manifest_digest: BootstrapDigest([0; 32]),
                index,
                records: vec![SnapshotRecord {
                    record_id: ProtocolId(format!("item:{index}")),
                    state: RecordState::Present {
                        value: CanonicalValue::Object(BTreeMap::new()),
                        version: 1,
                        reference: None,
                    },
                }],
            })
            .collect::<Vec<_>>();
        let mut hasher = BootstrapContentHasher::new(session.into()).unwrap();
        for page in &pages {
            hasher.push(page).unwrap();
        }
        let content = hasher.summary();
        let manifest = BootstrapManifest {
            session_id: session.into(),
            start_request_id: start.request_id.clone(),
            start_fingerprint: bootstrap_start_fingerprint(&start).unwrap(),
            context: context.clone(),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            limits: BootstrapLimits::default(),
            page_count: content.page_count,
            record_count: content.record_count,
            content_bytes: content.content_bytes,
            root: content.root,
            expires_at: u64::MAX,
        };
        let digest = bootstrap_manifest_digest(&manifest).unwrap();
        for page in &mut pages {
            page.manifest_digest = digest;
        }
        (
            encode_bootstrap_start(&start).unwrap(),
            encode_bootstrap_manifest(&manifest).unwrap(),
            context,
            pages
                .iter()
                .map(|p| encode_bootstrap_page(p).unwrap())
                .collect(),
        )
    }
    async fn schema(database: &SurrealDatabase) -> BTreeSet<String> {
        let client = database.client().await.unwrap();
        let mut response = client.query("INFO FOR DB").await.unwrap();
        response
            .take::<Option<Schema>>(0)
            .unwrap()
            .unwrap()
            .tables
            .into_keys()
            .collect()
    }
    fn barrier() -> ScopeBarrier {
        ScopeBarrier {
            reason: "needs_new_scope_journal".into(),
            required_scope: "new".into(),
            required_subscription_revision: "2".into(),
            required_authorization_revision: "2".into(),
        }
    }

    #[tokio::test]
    async fn no_stage_writes_leave_stage_schema_and_fence_absent() {
        let db = database().await;
        let state = initialized(&db).await;
        let store = SyncStateStore::new(&db);
        let before = schema(&db).await;
        let next = pending(state.clone(), "pending");
        store.apply_transition_atomic(&state, &next).await.unwrap();
        assert_eq!(
            store
                .load(SyncStateKey::new(&next.partition_id, &next.client_id))
                .await
                .unwrap(),
            Some(next.clone())
        );
        let raw_before: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(meta_record_id(SyncStateKey::new(
                &next.partition_id,
                &next.client_id,
            )))
            .await
            .unwrap();
        store
            .save_scope_barrier(
                SyncStateKey::new(&next.partition_id, &next.client_id),
                barrier(),
            )
            .await
            .unwrap();
        let raw_after: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(meta_record_id(SyncStateKey::new(
                &next.partition_id,
                &next.client_id,
            )))
            .await
            .unwrap();
        if let (Some(Value::Object(before)), Some(Value::Object(after))) = (&raw_before, &raw_after)
        {
            let changed = before
                .iter()
                .filter(|(key, value)| after.get(*key) != Some(*value))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            assert!(
                raw_before == raw_after,
                "barrier changed metadata fields: {changed:?}"
            );
        } else {
            assert!(
                raw_before == raw_after,
                "metadata presence/type changed before={:?} after={:?}",
                raw_before.as_ref().map(std::mem::discriminant),
                raw_after.as_ref().map(std::mem::discriminant)
            );
        }
        assert_eq!(schema(&db).await, before);
        assert!(!before.contains(HEADER));
        let client = db.client().await.unwrap();
        let transaction = client.clone().begin().await.unwrap();
        assert_eq!(
            read_fence(
                &transaction,
                SyncStateKey::new(&next.partition_id, &next.client_id)
            )
            .await
            .unwrap(),
            None
        );
        transaction.cancel().await.unwrap();
        assert_eq!(
            store
                .load(SyncStateKey::new(&next.partition_id, &next.client_id))
                .await
                .unwrap(),
            Some(next)
        );
    }

    #[tokio::test]
    async fn pages_are_separate_idempotent_out_of_order_and_survive_store_reopen() {
        let db = database().await;
        let initial = initialized(&db).await;
        let initial = pending(initial, "pending");
        SyncStateStore::new(&db)
            .save_atomic(Some(0), &initial)
            .await
            .unwrap();
        let (start, manifest, context, pages) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        assert_eq!(
            store.begin(&start, &manifest, &context).await.unwrap(),
            handle
        );
        store.stage_page(&pages[1], &context, handle).await.unwrap();
        store.stage_page(&pages[1], &context, handle).await.unwrap();
        let reopened = BootstrapStore::new(&db);
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![1])
        );
        reopened
            .stage_page(&pages[0], &context, handle)
            .await
            .unwrap();
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&initial.partition_id, &initial.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        let mut foreign = context.clone();
        foreign.principal_id = "other".into();
        assert!(reopened.cancel(&manifest, &foreign, handle).await.is_err());
        let mut altered = decode_bootstrap_page(&pages[0]).unwrap();
        altered.records[0].state = RecordState::Tombstone { version: 2 };
        assert!(
            reopened
                .stage_page(&encode_bootstrap_page(&altered).unwrap(), &context, handle)
                .await
                .is_err()
        );
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
    }

    #[tokio::test]
    async fn cancelled_incarnation_never_repeats_even_after_aba_and_reopen() {
        let db = database().await;
        initialized(&db).await;
        let (start_a, manifest_a, context, pages) = wire("a");
        let (start_b, manifest_b, _, _) = wire("b");
        let store = BootstrapStore::new(&db);
        let a = store.begin(&start_a, &manifest_a, &context).await.unwrap();
        store.cancel(&manifest_a, &context, a).await.unwrap();
        assert!(store.begin(&start_a, &manifest_a, &context).await.is_err());
        let b = store.begin(&start_b, &manifest_b, &context).await.unwrap();
        assert!(store.cancel(&manifest_a, &context, a).await.is_err());
        store.cancel(&manifest_b, &context, b).await.unwrap();
        let reopened = BootstrapStore::new(&db);
        let a2 = reopened
            .begin(&start_a, &manifest_a, &context)
            .await
            .unwrap();
        assert!(a2 > b && b > a);
        assert!(reopened.cancel(&manifest_a, &context, a).await.is_err());
        assert!(reopened.stage_page(&pages[0], &context, a).await.is_err());
        assert_eq!(reopened.progress(&context, a2).await.unwrap(), Some(vec![]));
    }

    #[tokio::test]
    async fn barrier_blocks_pages_but_cancel_keeps_barrier_and_pending() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        store.stage_page(&pages[0], &context, handle).await.unwrap();
        SyncStateStore::new(&db)
            .save_scope_barrier(key, barrier())
            .await
            .unwrap();
        assert!(store.stage_page(&pages[1], &context, handle).await.is_err());
        store.cancel(&manifest, &context, handle).await.unwrap();
        assert!(
            SyncStateStore::new(&db)
                .load_scope_barrier(key)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }

    #[tokio::test]
    async fn scalar_preflight_rejects_invalid_oversize_blob_before_codec() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        let _: Option<Value> = client
            .upsert(page_id(key, 0))
            .content(PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: "0".into(),
                payload: vec![0xff; MAX_SYNC_STATE_BYTES + 1].into(),
            })
            .await
            .unwrap();
        assert!(matches!(
            store.progress(&context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }

    #[test]
    fn reservations_use_actual_tagged_encoding_and_reject_scalar_projection() {
        let (_, _, _, pages) = wire("session");
        let mut page = decode_bootstrap_page(&pages[0]).unwrap();
        let initial = page_reservation(&page).unwrap();
        page.records[0].state = RecordState::Present {
            value: CanonicalValue::Object(BTreeMap::from([(
                "bytes".into(),
                CanonicalValue::Bytes(vec![0xff; 4096]),
            )])),
            version: 1,
            reference: None,
        };
        let encoded = page_reservation(&page).unwrap();
        assert!(encoded > initial + 4096); // native base64 + tagged JSON expansion, not CBOR estimate
        page.records[0].state = RecordState::Present {
            value: CanonicalValue::String("scalar".into()),
            version: 1,
            reference: None,
        };
        assert!(matches!(
            page_reservation(&page),
            Err(SyncStateError::RecordValueMustBeObject)
        ));
    }

    // Explicit begin/barrier and ordinary writers MUST conflict on the same metadata
    // record even when their snapshots predate stage schema. No sleeps/scheduling oracle.
    #[tokio::test]
    async fn metadata_fence_conflicts_in_both_orders_and_unrelated_keys_do_not() {
        for persistent in [false, true] {
            for stage_first in [false, true] {
                let directory = temp_dir::TempDir::new().unwrap();
                let db = if persistent {
                    connect(ConnectOptions {
                        endpoint: format!("surrealkv://{}", directory.path().display()),
                        namespace: Some("bootstrap".into()),
                        database: Some("bootstrap".into()),
                    })
                    .await
                    .unwrap()
                } else {
                    database().await
                };

                let state = initialized(&db).await;
                let key = SyncStateKey::new(&state.partition_id, &state.client_id);
                let raw_before: Option<Value> = db
                    .client()
                    .await
                    .unwrap()
                    .select(meta_record_id(key))
                    .await
                    .unwrap();
                let client = db.client().await.unwrap();
                let a = client.clone().begin().await.unwrap();
                let b = client.begin().await.unwrap();
                assert_eq!(read_fence(&a, key).await.unwrap(), None);
                assert_eq!(read_fence(&b, key).await.unwrap(), None);
                // Snapshot/absence is captured BEFORE explicit begin defines stage schema.
                a.query(SCHEMA).await.unwrap().check().unwrap();
                let (_, _, context, _) = wire("fence");
                fence_first_barrier(&a, key, &context).await.unwrap();
                write_fence(&a, key, "1:a".into()).await.unwrap();
                guard_barrier_write(&b, key).await.unwrap();
                let _: Option<ScopeBarrier> =
                    b.upsert(barrier_id(key)).content(barrier()).await.unwrap();
                if stage_first {
                    a.commit().await.unwrap();
                    assert!(b.commit().await.is_err());
                } else {
                    b.commit().await.unwrap();
                    assert!(a.commit().await.is_err());
                }
                let actual_barrier = SyncStateStore::new(&db)
                    .load_scope_barrier(key)
                    .await
                    .unwrap();
                assert_eq!(actual_barrier.is_none(), stage_first);
                assert_eq!(
                    SyncStateStore::new(&db).load(key).await.unwrap(),
                    Some(state.clone())
                );
                let raw_after: Option<Value> = db
                    .client()
                    .await
                    .unwrap()
                    .select(meta_record_id(key))
                    .await
                    .unwrap();
                if let (Some(Value::Object(before)), Some(Value::Object(mut after))) =
                    (raw_before, raw_after)
                {
                    after.remove("bootstrap_fence");
                    assert!(
                        before == after,
                        "begin/barrier changed preexisting metadata fields/bytes"
                    );
                } else {
                    panic!("metadata presence/type changed");
                }
                db.close().await.unwrap();
            }
        }
        let db = database().await;
        let state = initialized(&db).await;
        let mut other = state.clone();
        other.client_id = ClientId("unrelated".into());
        SyncStateStore::new(&db)
            .save_atomic(None, &other)
            .await
            .unwrap();
        let client = db.client().await.unwrap();
        let a = client.clone().begin().await.unwrap();
        let b = client.begin().await.unwrap();
        guard_barrier_write(&a, SyncStateKey::new(&state.partition_id, &state.client_id))
            .await
            .unwrap();
        guard_barrier_write(&b, SyncStateKey::new(&other.partition_id, &other.client_id))
            .await
            .unwrap();
        let _: Option<ScopeBarrier> = a
            .upsert(barrier_id(SyncStateKey::new(
                &state.partition_id,
                &state.client_id,
            )))
            .content(barrier())
            .await
            .unwrap();
        let _: Option<ScopeBarrier> = b
            .upsert(barrier_id(SyncStateKey::new(
                &other.partition_id,
                &other.client_id,
            )))
            .content(barrier())
            .await
            .unwrap();
        a.commit().await.unwrap();
        b.commit().await.unwrap();
    }
    #[tokio::test]
    async fn journal_growth_capacity_failure_rolls_back_every_component() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (start, manifest, context, _) = wire("capacity");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let mut next = initial.clone();
        next.revision = 1;
        for index in 0..26 {
            next.confirmed.insert(
                ProtocolId(format!("item:large{index}")),
                RecordState::Present {
                    value: serde_json::json!({"value": "x".repeat(1024*1024)}),
                    version: 1,
                    reference: None,
                },
            );
        }
        // Current-only state is valid. Combined current+stage+final reservation is not.
        assert!(state_budget(&next).unwrap().0 < MAX_AGGREGATE_STATE_BYTES);
        assert!(matches!(
            SyncStateStore::new(&db).save_atomic(Some(0), &next).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&initial.partition_id, &initial.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
    }

    #[tokio::test]
    async fn cancelled_page_transaction_does_not_change_stage_or_accounting() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("rollback");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        let transaction = client.clone().begin().await.unwrap();
        let _: Option<Value> = transaction
            .upsert(page_id(key, 0))
            .content(PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: "0".into(),
                payload: pages[0].clone().into(),
            })
            .await
            .unwrap();
        write_fence(&transaction, key, "999:a".into())
            .await
            .unwrap();
        transaction.cancel().await.unwrap();
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
        store.stage_page(&pages[0], &context, handle).await.unwrap();
        // Treat this successful commit's response as lost: reopening/exact retry cannot double charge.
        let reopened = BootstrapStore::new(&db);
        reopened
            .stage_page(&pages[0], &context, handle)
            .await
            .unwrap();
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
    }
    #[tokio::test]
    async fn surrealkv_restart_preserves_stage_counter_and_old_pending_without_migration() {
        let directory = temp_dir::TempDir::new().unwrap();
        let options = || ConnectOptions {
            endpoint: format!("surrealkv://{}", directory.path().display()),
            namespace: Some("bootstrap".into()),
            database: Some("bootstrap".into()),
        };
        let db = connect(options()).await.unwrap();
        let initial = initialized(&db).await;
        let pending = pending(initial, "preserved");
        let key = SyncStateKey::new(&pending.partition_id, &pending.client_id);
        SyncStateStore::new(&db)
            .save_atomic(Some(0), &pending)
            .await
            .unwrap();
        let (start, manifest, context, pages) = wire("durable");
        let handle = BootstrapStore::new(&db)
            .begin(&start, &manifest, &context)
            .await
            .unwrap();
        BootstrapStore::new(&db)
            .stage_page(&pages[1], &context, handle)
            .await
            .unwrap();
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(pending.clone())
        );
        BootstrapStore::new(&db)
            .cancel(&manifest, &context, handle)
            .await
            .unwrap();
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            None
        );
        assert!(
            BootstrapStore::new(&db)
                .begin(&start, &manifest, &context)
                .await
                .is_err()
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(pending)
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn normal_growth_preserves_stage_then_cancel_keeps_counter_monotonic() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("growth");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        stage.stage_page(&pages[0], &context, handle).await.unwrap();
        let next = pending(state.clone(), "native-pending");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&state, &next)
            .await
            .unwrap();
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
        stage.cancel(&manifest, &context, handle).await.unwrap();
        let second = pending(next.clone(), "inactive-pending");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&next, &second)
            .await
            .unwrap();
        let client = db.client().await.unwrap();
        let transaction = client.clone().begin().await.unwrap();
        let fence = read_fence(&transaction, key).await.unwrap().unwrap();
        let (counter, active) = parse_metadata_fence(&fence).unwrap();
        assert!(counter > handle);
        assert!(!active);
        transaction.cancel().await.unwrap();
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(second)
        );
    }

    #[tokio::test]
    async fn missing_stage_table_and_malformed_counter_never_recreate_or_zero_inventory() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("corrupt");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        client
            .query("REMOVE TABLE _sync_bootstrap_page")
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.progress(&context, handle).await,
            Err(SyncStateError::CorruptState)
        ));
        let next = pending(state.clone(), "no-recreate");
        assert!(
            SyncStateStore::new(&db)
                .save_atomic(Some(0), &next)
                .await
                .is_err()
        );
        assert!(!schema(&db).await.contains(PAGE));
        client
            .query("UPDATE $id SET bootstrap_fence='01:a'")
            .bind(("id", meta_record_id(key)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.progress(&context, handle).await,
            Err(SyncStateError::CorruptState)
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn existing_blocked_barrier_is_never_removed_by_first_begin() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        SyncStateStore::new(&db)
            .save_scope_barrier(key, barrier())
            .await
            .unwrap();
        let before: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(barrier_id(key))
            .await
            .unwrap();
        let before_schema = schema(&db).await;
        let (start, manifest, context, _) = wire("blocked");
        assert!(
            BootstrapStore::new(&db)
                .begin(&start, &manifest, &context)
                .await
                .is_err()
        );
        let after: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(barrier_id(key))
            .await
            .unwrap();
        assert!(before == after, "existing barrier changed");
        assert_eq!(schema(&db).await, before_schema);
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn page_reservation_and_real_journal_transition_conflict_before_either_can_overwrite() {
        for persistent in [false, true] {
            for page_first in [false, true] {
                let directory = temp_dir::TempDir::new().unwrap();
                let db = if persistent {
                    connect(ConnectOptions {
                        endpoint: format!("surrealkv://{}", directory.path().display()),
                        namespace: Some("bootstrap".into()),
                        database: Some("bootstrap".into()),
                    })
                    .await
                    .unwrap()
                } else {
                    database().await
                };
                let state = initialized(&db).await;
                let key = SyncStateKey::new(&state.partition_id, &state.client_id);
                let (start, manifest, context, pages) = wire("race");
                let stage = BootstrapStore::new(&db);
                let handle = stage.begin(&start, &manifest, &context).await.unwrap();
                let client = db.client().await.unwrap();
                let a = client.clone().begin().await.unwrap();
                let b = client.begin().await.unwrap();
                let old = read_fence(&a, key).await.unwrap();
                assert_eq!(read_fence(&b, key).await.unwrap(), old);
                let next = pending(state.clone(), "racing-pending");
                crate::sync_state::test_transition_in_transaction(&b, &state, &next)
                    .await
                    .unwrap();
                let _: Option<PageRow> = a
                    .upsert(page_id(key, 0))
                    .content(PageRow {
                        format_version: 1,
                        partition_id: key.partition_id.0.clone(),
                        client_id: key.client_id.0.clone(),
                        index: "0".into(),
                        payload: pages[0].clone().into(),
                    })
                    .await
                    .unwrap();
                write_fence(&a, key, next_fence(old).unwrap())
                    .await
                    .unwrap();
                if page_first {
                    a.commit().await.unwrap();
                    assert!(b.commit().await.is_err());
                } else {
                    b.commit().await.unwrap();
                    assert!(a.commit().await.is_err());
                }
                assert_eq!(
                    stage.progress(&context, handle).await.unwrap(),
                    Some(if page_first { vec![0] } else { vec![] })
                );
                assert_eq!(
                    SyncStateStore::new(&db).load(key).await.unwrap(),
                    Some(if page_first { state.clone() } else { next })
                );
                db.close().await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn exhausted_counter_rolls_back_page_cancel_and_journal_growth() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("overflow");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        db.client()
            .await
            .unwrap()
            .query("UPDATE $id SET bootstrap_fence = $fence")
            .bind(("id", meta_record_id(key)))
            .bind(("fence", format!("{}:a", u64::MAX)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.stage_page(&pages[0], &context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert!(matches!(
            stage.cancel(&manifest, &context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert!(matches!(
            SyncStateStore::new(&db)
                .save_atomic(Some(0), &pending(state.clone(), "overflow-pending"))
                .await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn combined_scalar_rejection_precedes_current_decode_in_begin_and_barrier() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("predecode");
        let stage = BootstrapStore::new(&db);
        stage.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        // Malformed JSON of a valid small length would produce Decode if the
        // current-state decoder is entered. Aggregate stage inventory must win.
        client
            .query("UPDATE $id SET payload = $payload")
            .bind(("id", meta_record_id(key)))
            .bind(("payload", Bytes::from(vec![0xff])))
            .await
            .unwrap()
            .check()
            .unwrap();
        for index in 0..16 {
            let _: Option<PageRow> = client
                .upsert(page_id(key, index))
                .content(PageRow {
                    format_version: 1,
                    partition_id: key.partition_id.0.clone(),
                    client_id: key.client_id.0.clone(),
                    index: index.to_string(),
                    payload: vec![0xff; MAX_SYNC_STATE_BYTES].into(),
                })
                .await
                .unwrap();
        }
        assert!(
            matches!(
                stage.begin(&start, &manifest, &context).await,
                Err(SyncStateError::StateTooLarge)
            ),
            "current decoder ran before combined scalar guard"
        );
        assert!(
            matches!(
                SyncStateStore::new(&db)
                    .save_scope_barrier(key, barrier())
                    .await,
                Err(SyncStateError::StateTooLarge)
            ),
            "barrier current decoder ran before combined scalar guard"
        );
        let existing: Option<ScopeBarrier> = client.select(barrier_id(key)).await.unwrap();
        assert!(existing.is_none());
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn native_key_boundary_is_enforced_at_every_entry_before_database_access() {
        let db = database().await;
        let (start, manifest, context, pages) = wire("key-limit");
        db.close().await.unwrap();
        for partition in [false, true] {
            let mut oversized = context.clone();
            if partition {
                oversized.partition_id = PartitionId("é".repeat(257));
            } else {
                oversized.client_id = ClientId("c".repeat(513));
            }
            let store = BootstrapStore::new(&db);
            assert!(matches!(
                store.begin(&start, &manifest, &oversized).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.stage_page(&pages[0], &oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.progress(&oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.cancel(&manifest, &oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store
                    .complete(b"invalid", &oversized, 1, &live_clock(&|| Ok(1)))
                    .await,
                Err(SyncStateError::IdentityTooLarge)
            ));
        }
    }

    #[tokio::test]
    async fn exact_512_utf8_byte_keys_support_begin_page_progress_and_cancel() {
        let db = database().await;
        db.client()
            .await
            .unwrap()
            .query("DEFINE TABLE item SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let mut state = state();
        state.partition_id = PartitionId("é".repeat(256));
        state.client_id = ClientId("c".repeat(512));
        let store = SyncStateStore::new(&db);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let (start, manifest, mut context, pages) = wire("exact-key");
        context.partition_id = state.partition_id.clone();
        context.client_id = state.client_id.clone();
        let mut start = decode_bootstrap_start(&start).unwrap();
        start.partition_id = context.partition_id.clone();
        start.client_id = context.client_id.clone();
        let mut manifest = decode_bootstrap_manifest(&manifest).unwrap();
        manifest.context = context.clone();
        manifest.start_fingerprint = bootstrap_start_fingerprint(&start).unwrap();
        let digest = bootstrap_manifest_digest(&manifest).unwrap();
        let manifest = encode_bootstrap_manifest(&manifest).unwrap();
        let start = encode_bootstrap_start(&start).unwrap();
        let mut page = decode_bootstrap_page(&pages[0]).unwrap();
        page.manifest_digest = digest;
        let page = encode_bootstrap_page(&page).unwrap();
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        stage.stage_page(&page, &context, handle).await.unwrap();
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
        stage.cancel(&manifest, &context, handle).await.unwrap();
        assert_eq!(stage.progress(&context, handle).await.unwrap(), None);
    }
    fn completion_receipt(manifest: &[u8]) -> Vec<u8> {
        let manifest = decode_bootstrap_manifest(manifest).unwrap();
        surrealdb_sync_protocol::bootstrap_codec::encode_bootstrap_receipt(
            &surrealdb_sync_protocol::bootstrap::BootstrapReceipt {
                session_id: manifest.session_id.clone(),
                manifest_digest: bootstrap_manifest_digest(&manifest).unwrap(),
                checkpoint: surrealdb_sync_protocol::Checkpoint {
                    token: surrealdb_sync_protocol::OpaqueCheckpoint("bootstrap-receipt".into()),
                    cursor: manifest.cursor,
                    scope: manifest.context.scope,
                },
                expires_at: manifest.expires_at,
            },
        )
        .unwrap()
    }
    fn live_clock(
        utc: &(dyn Fn() -> Result<u64, SyncStateError> + Send + Sync),
    ) -> CompletionClock<'_> {
        CompletionClock {
            utc_seconds: utc,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(20),
        }
    }
    async fn ready_stage(db: &SurrealDatabase, session: &str) -> (Vec<u8>, BootstrapContext, u64) {
        let (start, manifest, context, pages) = wire(session);
        let store = BootstrapStore::new(db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        for page in pages {
            store.stage_page(&page, &context, handle).await.unwrap();
        }
        (manifest, context, handle)
    }
    fn valid_pending(current: StoredSyncState, id: &str) -> StoredSyncState {
        let mut commit = ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId(id.into()),
                fingerprint: Fingerprint(String::new()),
            },
            operations: vec![Operation::Delete {
                record_id: ProtocolId("item:pending".into()),
                base_version: 0,
            }],
        };
        commit.identity.fingerprint =
            crate::sync_codec::canonical_fingerprint(&current, &commit).unwrap();
        ClientRuntime::open(current)
            .unwrap()
            .prepare_enqueue(commit)
            .unwrap()
            .into_state()
    }

    #[tokio::test]
    async fn completion_installs_root_projection_cursor_preserves_pending_and_consumed_retry_is_historical()
     {
        let db = database().await;
        let initial = initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "complete").await;
        let pending = valid_pending(initial.clone(), "later-local");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&initial, &pending)
            .await
            .unwrap();
        let receipt = completion_receipt(&manifest);
        let Completion::Applied(installed) = BootstrapStore::new(&db)
            .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
            .await
            .unwrap()
        else {
            panic!("not installed")
        };
        let installed = *installed;
        assert_eq!(installed.outbox, pending.outbox);
        assert_eq!(installed.confirmed.len(), 2);
        assert_eq!(installed.checkpoint.as_ref().unwrap().cursor.sequence, 1);
        assert!(installed.reset_coverage.is_some());
        let row: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(("item", "0"))
            .await
            .unwrap();
        assert!(row.is_some());
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            None
        );
        let later = valid_pending(installed.clone(), "post-completion");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&installed, &later)
            .await
            .unwrap();
        let key = SyncStateKey::new(&context.partition_id, &context.client_id);
        SyncStateStore::new(&db)
            .save_scope_barrier(key, barrier())
            .await
            .unwrap();
        assert!(matches!(
            BootstrapStore::new(&db)
                .complete(
                    &receipt,
                    &context,
                    handle,
                    &CompletionClock {
                        utc_seconds: &|| Ok(u64::MAX),
                        deadline: std::time::Instant::now()
                    }
                )
                .await
                .unwrap(),
            Completion::HistoricalSuccess
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(later)
        );
        assert!(
            SyncStateStore::new(&db)
                .load_scope_barrier(key)
                .await
                .unwrap()
                .is_some()
        );
        BootstrapStore::new(&db)
            .cancel(&manifest, &context, handle)
            .await
            .unwrap();
        assert!(matches!(
            BootstrapStore::new(&db)
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await
                .unwrap(),
            Completion::HistoricalSuccess
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn completion_failures_roll_back_and_clock_is_rechecked_after_verification() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (start, manifest, context, pages) = wire("failures");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let receipt = completion_receipt(&manifest);
        assert!(
            store
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await
                .is_err()
        );
        for page in &pages {
            store.stage_page(page, &context, handle).await.unwrap();
        }
        let mut foreign = context.clone();
        foreign.principal_id = "other".into();
        assert!(
            store
                .complete(&receipt, &foreign, handle, &live_clock(&|| Ok(1)))
                .await
                .is_err()
        );
        assert!(
            store
                .complete(&receipt, &context, handle + 1, &live_clock(&|| Ok(1)))
                .await
                .is_err()
        );
        assert!(
            store
                .complete(b"forged", &context, handle, &live_clock(&|| Ok(1)))
                .await
                .is_err()
        );
        let count = std::sync::atomic::AtomicUsize::new(0);
        let clock = || {
            let n = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(if n < 4 { 1 } else { u64::MAX })
        };
        assert!(
            store
                .complete(&receipt, &context, handle, &live_clock(&clock))
                .await
                .is_err()
        );
        assert!(count.load(std::sync::atomic::Ordering::Relaxed) >= 5);
        let key = SyncStateKey::new(&context.partition_id, &context.client_id);
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(initial)
        );
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        let row: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(("item", "0"))
            .await
            .unwrap();
        assert!(row.is_none());
        assert!(matches!(
            store
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await
                .unwrap(),
            Completion::Applied(_)
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn completion_surrealkv_restart_and_lost_ack_never_reapplies_over_later_writes() {
        let dir = temp_dir::TempDir::new().unwrap();
        let options = || ConnectOptions {
            endpoint: format!("surrealkv://{}", dir.path().display()),
            namespace: Some("complete".into()),
            database: Some("complete".into()),
        };
        let db = connect(options()).await.unwrap();
        initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "restart").await;
        let receipt = completion_receipt(&manifest);
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        let store = BootstrapStore::new(&db);
        let client = db.client().await.unwrap();
        let transaction = client.clone().begin().await.unwrap();
        let decoded = decode_bootstrap_receipt(&receipt).unwrap();
        assert!(matches!(
            store
                .complete_in_transaction(
                    &transaction,
                    &receipt,
                    &decoded,
                    &context,
                    handle,
                    &live_clock(&|| Ok(1))
                )
                .await
                .unwrap(),
            Completion::Applied(_)
        ));
        transaction.cancel().await.unwrap();
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        let transaction = client.clone().begin().await.unwrap();
        let Completion::Applied(installed) = store
            .complete_in_transaction(
                &transaction,
                &receipt,
                &decoded,
                &context,
                handle,
                &live_clock(&|| Ok(1)),
            )
            .await
            .unwrap()
        else {
            panic!()
        };
        let installed = *installed;
        transaction.commit().await.unwrap(); // Deliberately discard the successful result (lost ACK).
        let later = valid_pending(installed.clone(), "post-ack-loss");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&installed, &later)
            .await
            .unwrap();
        drop(client);
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        assert!(matches!(
            BootstrapStore::new(&db)
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(u64::MAX)))
                .await
                .unwrap(),
            Completion::HistoricalSuccess
        ));
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&context.partition_id, &context.client_id))
                .await
                .unwrap(),
            Some(later)
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn complete_conflicts_with_enqueue_barrier_and_cancel_in_both_orders_memory_and_kv() {
        for persistent in [false, true] {
            for competitor in ["enqueue", "barrier", "cancel"] {
                for complete_first in [false, true] {
                    let directory = temp_dir::TempDir::new().unwrap();
                    let db = if persistent {
                        connect(ConnectOptions {
                            endpoint: format!("surrealkv://{}", directory.path().display()),
                            namespace: Some("race".into()),
                            database: Some("race".into()),
                        })
                        .await
                        .unwrap()
                    } else {
                        database().await
                    };
                    let initial = initialized(&db).await;
                    let (manifest, context, handle) = ready_stage(&db, "race-complete").await;
                    let receipt = completion_receipt(&manifest);
                    let decoded = decode_bootstrap_receipt(&receipt).unwrap();
                    let store = BootstrapStore::new(&db);
                    let key = SyncStateKey::new(&context.partition_id, &context.client_id);
                    let client = db.client().await.unwrap();
                    let a = client.clone().begin().await.unwrap();
                    let b = client.begin().await.unwrap();
                    let old = read_fence(&a, key).await.unwrap();
                    assert_eq!(read_fence(&b, key).await.unwrap(), old);
                    let Completion::Applied(installed) = store
                        .complete_in_transaction(
                            &a,
                            &receipt,
                            &decoded,
                            &context,
                            handle,
                            &live_clock(&|| Ok(1)),
                        )
                        .await
                        .unwrap()
                    else {
                        panic!()
                    };
                    let queued = valid_pending(initial.clone(), "racing");
                    match competitor {
                        "enqueue" => {
                            crate::sync_state::test_transition_in_transaction(
                                &b, &initial, &queued,
                            )
                            .await
                            .unwrap();
                        }
                        "barrier" => {
                            guard_barrier_write(&b, key).await.unwrap();
                            let _: Option<Value> =
                                b.upsert(barrier_id(key)).content(barrier()).await.unwrap();
                        }
                        _ => {
                            store
                                .cancel_in_transaction(&b, &manifest, &context, handle)
                                .await
                                .unwrap();
                        }
                    }
                    if complete_first {
                        a.commit().await.unwrap();
                        assert!(b.commit().await.is_err());
                    } else {
                        b.commit().await.unwrap();
                        assert!(a.commit().await.is_err());
                    }
                    let current = SyncStateStore::new(&db).load(key).await.unwrap().unwrap();
                    if complete_first {
                        assert_eq!(current, *installed);
                        assert!(matches!(
                            store
                                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                                .await
                                .unwrap(),
                            Completion::HistoricalSuccess
                        ));
                    } else {
                        assert_eq!(
                            current,
                            if competitor == "enqueue" {
                                queued.clone()
                            } else {
                                initial.clone()
                            }
                        );
                        if competitor == "enqueue" {
                            let Completion::Applied(next) = store
                                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                                .await
                                .unwrap()
                            else {
                                panic!()
                            };
                            assert_eq!(next.outbox, queued.outbox);
                        } else {
                            assert!(
                                store
                                    .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                                    .await
                                    .is_err()
                            );
                        }
                    }
                    db.close().await.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn preexisting_header_compatibility_marker_pair_and_unknown_fields_fail_closed() {
        let db = database().await;
        initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "headers").await;
        let key = SyncStateKey::new(&context.partition_id, &context.client_id);
        let client = db.client().await.unwrap();
        // Old staging v3 schemas/rows have neither optional consumed field.
        client.query("REMOVE FIELD consumed_receipt ON _sync_bootstrap_header; REMOVE FIELD consumed_revision ON _sync_bootstrap_header;").await.unwrap().check().unwrap();
        let receipt = completion_receipt(&manifest);
        assert!(matches!(
            BootstrapStore::new(&db)
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await
                .unwrap(),
            Completion::Applied(_)
        ));
        client
            .query("UPDATE $id UNSET consumed_revision")
            .bind(("id", header_id(key)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .is_err()
        );
        client
            .query("UPDATE $id SET consumed_revision='1', consumed_receipt=$bad")
            .bind(("id", header_id(key)))
            .bind(("bad", Bytes::from(vec![0xff])))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .is_err()
        );
        client
            .query("UPDATE $id SET consumed_receipt=$receipt")
            .bind(("id", header_id(key)))
            .bind(("receipt", Bytes::from(receipt.clone())))
            .await
            .unwrap()
            .check()
            .unwrap();
        client.query("UPDATE $id SET consumed_revision='1'; DEFINE FIELD unexpected ON _sync_bootstrap_header TYPE string; UPDATE $id SET unexpected='invalid';").bind(("id",header_id(key))).await.unwrap().check().unwrap();
        assert!(
            BootstrapStore::new(&db)
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await
                .is_err()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn forged_root_and_late_projection_error_preserve_old_journal_and_stage() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (start, manifest, context, pages) = wire("root");
        let mut bad = decode_bootstrap_manifest(&manifest).unwrap();
        bad.root.0[0] ^= 1;
        let bad = encode_bootstrap_manifest(&bad).unwrap();
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &bad, &context).await.unwrap();
        let digest = bootstrap_manifest_digest(&decode_bootstrap_manifest(&bad).unwrap()).unwrap();
        for raw in pages {
            let mut page = decode_bootstrap_page(&raw).unwrap();
            page.manifest_digest = digest;
            store
                .stage_page(&encode_bootstrap_page(&page).unwrap(), &context, handle)
                .await
                .unwrap();
        }
        assert!(
            store
                .complete(
                    &completion_receipt(&bad),
                    &context,
                    handle,
                    &live_clock(&|| Ok(1))
                )
                .await
                .is_err()
        );
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&context.partition_id, &context.client_id))
                .await
                .unwrap(),
            Some(initial.clone())
        );
        store.cancel(&bad, &context, handle).await.unwrap();
        let (manifest, context, handle) = ready_stage(&db, "projection").await;
        db.client()
            .await
            .unwrap()
            .query("DEFINE FIELD fail ON item VALUE true ASSERT false")
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(
            store
                .complete(
                    &completion_receipt(&manifest),
                    &context,
                    handle,
                    &live_clock(&|| Ok(1))
                )
                .await
                .is_err()
        );
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&context.partition_id, &context.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn complete_retains_same_epoch_accepted_and_conflict_but_rejects_unknown_or_other_epoch()
    {
        for epoch in [None, Some(1), Some(2)] {
            let db = database().await;
            let initial = initialized(&db).await;
            let mut current = valid_pending(initial.clone(), "accepted");
            let commit = current.outbox.remove(0);
            current
                .outcomes
                .push(surrealdb_sync_client::ResolvedCommit {
                    outcome: surrealdb_sync_protocol::DurableOutcome::Accepted {
                        identity: commit.identity.clone(),
                        sequence: 2,
                        id_mappings: vec![],
                    },
                    local_commit: commit,
                    resolution: None,
                    covered_by_reset: false,
                });
            if let Some(epoch) = epoch {
                current.checkpoint = Some(surrealdb_sync_protocol::Checkpoint {
                    token: surrealdb_sync_protocol::OpaqueCheckpoint("old".into()),
                    cursor: Cursor { epoch, sequence: 0 },
                    scope: wire("x").2.scope,
                });
            }
            let mut conflict = valid_pending(initial, "conflict").outbox.remove(0);
            conflict.identity.fingerprint =
                crate::sync_codec::canonical_fingerprint(&current, &conflict).unwrap();
            current
                .outcomes
                .push(surrealdb_sync_client::ResolvedCommit {
                    outcome: surrealdb_sync_protocol::DurableOutcome::Conflict {
                        identity: conflict.identity.clone(),
                        record_id: ProtocolId("item:pending".into()),
                        authoritative: RecordState::Tombstone { version: 1 },
                    },
                    local_commit: conflict,
                    resolution: None,
                    covered_by_reset: false,
                });
            SyncStateStore::new(&db)
                .save_atomic(Some(0), &current)
                .await
                .unwrap();
            let (manifest, context, handle) = ready_stage(&db, "accepted").await;
            let result = BootstrapStore::new(&db)
                .complete(
                    &completion_receipt(&manifest),
                    &context,
                    handle,
                    &live_clock(&|| Ok(1)),
                )
                .await;
            if epoch == Some(1) {
                let Completion::Applied(next) = result.unwrap() else {
                    panic!()
                };
                assert_eq!(next.outcomes, current.outcomes);
                assert!(matches!(
                    ClientRuntime::open(*next)
                        .unwrap()
                        .optimistic()
                        .get(&ProtocolId("item:pending".into())),
                    Some(surrealdb_sync_client::OptimisticRecord::Deleted { .. })
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(SyncStateError::InvalidState(
                        surrealdb_sync_client::ClientError::NeedsNewScopeJournal
                    ))
                ));
                assert_eq!(
                    SyncStateStore::new(&db)
                        .load(SyncStateKey::new(&context.partition_id, &context.client_id))
                        .await
                        .unwrap(),
                    Some(current)
                );
                assert_eq!(
                    BootstrapStore::new(&db)
                        .progress(&context, handle)
                        .await
                        .unwrap(),
                    Some(vec![0, 1])
                );
            }
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn consumed_marker_scalar_admission_precedes_current_payload_decode() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "oversize-marker").await;
        let key = SyncStateKey::new(&initial.partition_id, &initial.client_id);
        db.client().await.unwrap().query("UPDATE $meta SET payload=$bad; UPDATE $header SET consumed_receipt=$large, consumed_revision='1', cancelled=true;")
            .bind(("meta",meta_record_id(key))).bind(("header",header_id(key))).bind(("bad",Bytes::from(vec![0xff]))).bind(("large",Bytes::from(vec![0xff;MAX_SYNC_STATE_BYTES+1]))).await.unwrap().check().unwrap();
        assert!(matches!(
            BootstrapStore::new(&db)
                .complete(
                    &completion_receipt(&manifest),
                    &context,
                    handle,
                    &live_clock(&|| Ok(1))
                )
                .await,
            Err(SyncStateError::StateTooLarge)
        ));
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn final_candidate_budget_counts_old_pages_marker_and_final_components_together() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (_manifest, context, handle) = ready_stage(&db, "combined-final").await;
        let key = SyncStateKey::new(&context.partition_id, &context.client_id);
        let transaction = db.client().await.unwrap().begin().await.unwrap();
        let stage = load_stage(&transaction, key).await.unwrap().unwrap();
        let mut candidate = initial.clone();
        candidate.revision = 1;
        for index in 0..(MAX_COMPONENT_ITEMS - 4) {
            candidate.confirmed.insert(
                ProtocolId(format!("item:{index}")),
                RecordState::Tombstone { version: 1 },
            );
        }
        assert!(
            state_budget(&candidate).is_ok(),
            "candidate alone fits native component budget"
        );
        assert!(
            matches!(
                final_budget(key, &initial, &candidate, &stage, b"receipt"),
                Err(SyncStateError::StateTooLarge)
            ),
            "old+stage+marker+final must fit together"
        );
        transaction.cancel().await.unwrap();
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(initial)
        );
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            Some(vec![0, 1])
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn completion_rejects_expired_unbounded_or_failed_trusted_clock_without_mutation() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "clocks").await;
        let receipt = completion_receipt(&manifest);
        let store = BootstrapStore::new(&db);
        let now = || Ok(1);
        for deadline in [
            std::time::Instant::now(),
            std::time::Instant::now() + std::time::Duration::from_secs(31),
        ] {
            assert!(
                store
                    .complete(
                        &receipt,
                        &context,
                        handle,
                        &CompletionClock {
                            utc_seconds: &now,
                            deadline
                        }
                    )
                    .await
                    .is_err()
            );
        }
        assert!(
            store
                .complete(
                    &receipt,
                    &context,
                    handle,
                    &live_clock(&|| Err(SyncStateError::RevisionConflict))
                )
                .await
                .is_err()
        );
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&context.partition_id, &context.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn zero_consumed_revision_blocks_historical_success_and_inactive_journal_writes() {
        let db = database().await;
        initialized(&db).await;
        let (manifest, context, handle) = ready_stage(&db, "zero-revision").await;
        let receipt = completion_receipt(&manifest);
        let store = BootstrapStore::new(&db);
        let Completion::Applied(installed) = store
            .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
            .await
            .unwrap()
        else {
            panic!()
        };
        let key = SyncStateKey::new(&context.partition_id, &context.client_id);
        db.client()
            .await
            .unwrap()
            .query("UPDATE $id SET consumed_revision='0'")
            .bind(("id", header_id(key)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            store.progress(&context, handle).await,
            Err(SyncStateError::CorruptState)
        ));
        assert!(matches!(
            store
                .complete(&receipt, &context, handle, &live_clock(&|| Ok(1)))
                .await,
            Err(SyncStateError::CorruptState)
        ));
        let next = valid_pending((*installed).clone(), "must-not-write");
        assert!(matches!(
            SyncStateStore::new(&db)
                .apply_transition_atomic(&installed, &next)
                .await,
            Err(SyncStateError::CorruptState)
        ));
        assert!(matches!(
            SyncStateStore::new(&db)
                .save_scope_barrier(key, barrier())
                .await,
            Err(SyncStateError::CorruptState)
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(*installed)
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn whole_storage_accepts_staging_and_terminal_header_with_older_authorization() {
        use crate::sync_storage_inspection::{NativeStorageInspectionOptions, inspect_storage};
        let db = database().await;
        let initial = initialized(&db).await;
        let (start, manifest, context, pages) = wire("whole-storage");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        stage.stage_page(&pages[0], &context, handle).await.unwrap();
        let options = || NativeStorageInspectionOptions {
            namespace: "bootstrap".into(),
            database: "bootstrap".into(),
            expected_journal: crate::sync_client::NativeSyncOpenOptions {
                partition_id: "partition".into(),
                client_id: "client".into(),
                requested_scope: "all".into(),
                subscription_revision: 1,
            },
            application_tables: vec!["item".into()],
        };
        assert!(inspect_storage(&db, options()).await.is_ok());
        stage.cancel(&manifest, &context, handle).await.unwrap();
        let mut current = initial;
        current.revision += 1;
        let mut scope = context.scope.clone();
        scope.authorization_revision += 1;
        current.checkpoint = Some(surrealdb_sync_protocol::Checkpoint {
            token: surrealdb_sync_protocol::OpaqueCheckpoint("new-current".into()),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            scope,
        });
        SyncStateStore::new(&db)
            .save_atomic(Some(0), &current)
            .await
            .unwrap();
        assert!(inspect_storage(&db, options()).await.is_ok());
    }
    #[tokio::test]
    async fn whole_storage_rejects_orphan_bootstrap_fence_even_without_stage_tables() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        db.client()
            .await
            .unwrap()
            .query("DEFINE FIELD bootstrap_fence ON _sync_client_meta TYPE option<string>; UPDATE $id SET bootstrap_fence='1:i'")
            .bind(("id", meta_record_id(key)))
            .await
            .unwrap()
            .check()
            .unwrap();
        let options = crate::sync_storage_inspection::NativeStorageInspectionOptions {
            namespace: "bootstrap".into(),
            database: "bootstrap".into(),
            expected_journal: crate::sync_client::NativeSyncOpenOptions {
                partition_id: "partition".into(),
                client_id: "client".into(),
                requested_scope: "all".into(),
                subscription_revision: 1,
            },
            application_tables: vec!["item".into()],
        };
        assert!(
            crate::sync_storage_inspection::inspect_storage(&db, options)
                .await
                .is_err()
        );
    }
}
