//! Transactional persistence for the experimental sync client state.
//!
//! This module is Rust-only. It deliberately exposes no UniFFI or TypeScript API
//! until the durable lifecycle has been proven on mobile.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::types::{Array, Bytes, RecordId as NativeRecordId, SurrealValue, Value};
use surrealdb_sync_client::{
    ClientError, DurableClientState, OptimisticRecord, ResolvedCommit, optimistic_projection,
};
use surrealdb_sync_protocol::{
    Checkpoint, ClientCommit, ClientId, PartitionId, RecordId as ProtocolRecordId, RecordState,
    ScopeIdentity,
};

use crate::wire::from_wire_value;
use crate::{SurrealDatabase, SurrealRnError};

const LEGACY_FORMAT_VERSION: i64 = 1;
const FORMAT_VERSION: i64 = 2;
const SUBSCRIPTION_FORMAT_VERSION: i64 = 3;
const STATE_TABLE: &str = "_sync_client_state";
const META_TABLE: &str = "_sync_client_meta";
const CONFIRMED_TABLE: &str = "_sync_client_confirmed";
const OUTBOX_TABLE: &str = "_sync_client_outbox";
const OUTCOME_TABLE: &str = "_sync_client_outcome";
const SUBSCRIPTION_RECORD_TABLE: &str = "_sync_client_subscription_record";
const ID_MAP_TABLE: &str = "_sync_client_id_map";
const MIGRATED_SENTINEL: &[u8] = b"migrated-to-v2";
pub(crate) const MAX_IDENTITY_BYTES: usize = 512;
const MAX_RECORD_ID_BYTES: usize = 1024;
const MAX_COMPONENT_KEY_BYTES: usize = 1024;
pub const MAX_SYNC_STATE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_COMPONENT_ITEMS: usize = 16_384;
pub(crate) const MAX_AGGREGATE_STATE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, SurrealValue)]
pub(crate) struct ScopeBarrier {
    pub reason: String,
    pub required_scope: String,
    pub required_subscription_revision: String,
    pub required_authorization_revision: String,
}

const DEFINE_SCHEMA: &str = r#"
DEFINE TABLE IF NOT EXISTS _sync_client_scope_barrier SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS reason ON _sync_client_scope_barrier TYPE string ASSERT $value IN ["pending_quarantine_required", "needs_new_scope_journal"];
DEFINE FIELD IF NOT EXISTS required_scope ON _sync_client_scope_barrier TYPE string;
DEFINE FIELD IF NOT EXISTS required_subscription_revision ON _sync_client_scope_barrier TYPE string;
DEFINE FIELD IF NOT EXISTS required_authorization_revision ON _sync_client_scope_barrier TYPE string;

DEFINE TABLE IF NOT EXISTS _sync_client_state SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_state TYPE int
    ASSERT $value = 1 OR $value = 2;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS revision ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_state TYPE bytes;

DEFINE TABLE IF NOT EXISTS _sync_client_meta SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_meta TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_meta TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_meta TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_meta TYPE bytes;

DEFINE TABLE IF NOT EXISTS _sync_client_confirmed SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_confirmed TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_confirmed TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_confirmed TYPE string;
DEFINE FIELD IF NOT EXISTS component_key ON _sync_client_confirmed TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_confirmed TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_client_confirmed FIELDS partition_id, client_id;

DEFINE TABLE IF NOT EXISTS _sync_client_outbox SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_outbox TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_outbox TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_outbox TYPE string;
DEFINE FIELD IF NOT EXISTS component_key ON _sync_client_outbox TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_outbox TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_client_outbox FIELDS partition_id, client_id;

DEFINE TABLE IF NOT EXISTS _sync_client_outcome SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_outcome TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_outcome TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_outcome TYPE string;
DEFINE FIELD IF NOT EXISTS component_key ON _sync_client_outcome TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_outcome TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_client_outcome FIELDS partition_id, client_id;

DEFINE TABLE IF NOT EXISTS _sync_client_id_map SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_id_map TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_id_map TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_id_map TYPE string;
DEFINE FIELD IF NOT EXISTS component_key ON _sync_client_id_map TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_id_map TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_client_id_map FIELDS partition_id, client_id;
DEFINE TABLE IF NOT EXISTS _sync_client_subscription_record SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE format_version ON _sync_client_subscription_record TYPE int ASSERT $value = 2 OR $value = 3;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_subscription_record TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_subscription_record TYPE string;
DEFINE FIELD IF NOT EXISTS component_key ON _sync_client_subscription_record TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_subscription_record TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_client_subscription_record FIELDS partition_id, client_id;
"#;

pub type StoredSyncState = DurableClientState<JsonValue>;

#[derive(Clone, Copy)]
pub struct SyncStateKey<'a> {
    pub partition_id: &'a PartitionId,
    pub client_id: &'a ClientId,
}

impl<'a> SyncStateKey<'a> {
    pub fn new(partition_id: &'a PartitionId, client_id: &'a ClientId) -> Self {
        Self {
            partition_id,
            client_id,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum SyncStateWrite {
    Applied,
    AlreadyCurrent,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncStateError {
    #[error("sync state identity exceeds the supported size")]
    IdentityTooLarge,

    #[error("sync state payload exceeds the supported size")]
    StateTooLarge,

    #[error("sync state revision does not follow the expected revision")]
    RevisionMismatch,

    #[error("sync state storage revision changed")]
    RevisionConflict,

    #[error("stored sync state is corrupt")]
    CorruptState,

    #[error("sync record ID is not supported by the native adapter")]
    UnsupportedRecordId,

    #[error("sync record value must be a SurrealDB object")]
    RecordValueMustBeObject,

    #[error("sync record value decoding failed")]
    ValueCodec(#[source] SurrealRnError),

    #[error("sync state is invalid")]
    InvalidState(#[source] ClientError),

    #[error("sync state encoding failed")]
    Encode(#[source] serde_json::Error),

    #[error("sync state decoding failed")]
    Decode(#[source] serde_json::Error),

    #[error("sync state database operation failed")]
    Database(#[source] surrealdb::Error),

    #[error("sync state database handle is unavailable")]
    Core(#[source] SurrealRnError),

    #[error("sync state commit outcome is unknown")]
    CommitUnknown(#[source] surrealdb::Error),

    #[error("sync state rollback failed after an operation error")]
    RollbackFailed {
        operation: Box<SyncStateError>,
        #[source]
        rollback: surrealdb::Error,
    },
}

#[derive(Debug, SurrealValue)]
struct StoredRecord {
    format_version: i64,
    partition_id: String,
    client_id: String,
    revision: String,
    payload: Bytes,
}

#[derive(Debug, SurrealValue)]
struct StoredMetaRow {
    format_version: i64,
    partition_id: String,
    client_id: String,
    payload: Bytes,
}

#[derive(Debug, SurrealValue)]
struct StoredComponentRow {
    format_version: i64,
    partition_id: String,
    client_id: String,
    component_key: String,
    payload: Bytes,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StateMeta {
    partition_id: String,
    client_id: String,
    requested_scope: String,
    subscription_revision: String,
    revision: String,
    checkpoint: Option<Checkpoint>,
    #[serde(default)]
    reset_coverage: Option<surrealdb_sync_client::ResetCoverage>,
    #[serde(default)]
    subscriptions: Option<surrealdb_sync_client::DurableSubscriptions<JsonValue>>,
    #[serde(default)]
    subscription_record_keys: Vec<String>,
    confirmed_keys: Vec<ProtocolRecordId>,
    outbox_keys: Vec<String>,
    outcome_keys: Vec<String>,
    id_map_keys: Vec<ProtocolRecordId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum TaggedJson {
    Null,
    Bool(bool),
    String(String),
    I64(String),
    U64(String),
    Float(String),
    Array(Vec<TaggedJson>),
    Object(BTreeMap<String, TaggedJson>),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ComponentAddress {
    table: &'static str,
    key: String,
}

struct NormalizedRows {
    format_version: i64,
    meta: Vec<u8>,
    components: BTreeMap<ComponentAddress, Vec<u8>>,
}

pub struct SyncStateStore<'a> {
    database: &'a SurrealDatabase,
    diagnostics: Option<&'a crate::sync_diagnostics::SyncDiagnostics>,
}

impl<'a> SyncStateStore<'a> {
    pub fn new(database: &'a SurrealDatabase) -> Self {
        Self {
            database,
            diagnostics: None,
        }
    }

    pub(crate) fn with_diagnostics(
        mut self,
        diagnostics: &'a crate::sync_diagnostics::SyncDiagnostics,
    ) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    /// Install the private local table used by the adapter.
    ///
    /// Applications should call this once while opening the embedded database,
    /// before loading or persisting sync state.
    pub async fn define_schema(&self) -> Result<(), SyncStateError> {
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        client
            .query(DEFINE_SCHEMA)
            .await
            .map_err(SyncStateError::Database)?
            .check()
            .map_err(SyncStateError::Database)?;
        Ok(())
    }

    pub(crate) async fn load_scope_barrier(
        &self,
        key: SyncStateKey<'_>,
    ) -> Result<Option<ScopeBarrier>, SyncStateError> {
        validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let barrier: Option<ScopeBarrier> = client
            .select(scope_barrier_record_id(key))
            .await
            .map_err(SyncStateError::Database)?;
        validate_scope_barrier(barrier.as_ref())?;
        Ok(barrier)
    }

    // Independent of ordinary journal saves: never removed or weakened by them.
    pub(crate) async fn save_scope_barrier(
        &self,
        key: SyncStateKey<'_>,
        barrier: ScopeBarrier,
    ) -> Result<(), SyncStateError> {
        validate_key(key)?;
        validate_scope_barrier(Some(&barrier))?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
        crate::sync_bootstrap_store::guard_barrier_write(&transaction, key).await?;
        transaction.query("UPSERT $id SET reason = IF reason = 'pending_quarantine_required' OR $reason = 'pending_quarantine_required' THEN 'pending_quarantine_required' ELSE 'needs_new_scope_journal' END, required_scope = $scope, required_subscription_revision = $subscription, required_authorization_revision = $authorization;")
            .bind(("id", scope_barrier_record_id(key)))
            .bind(("reason", barrier.reason)).bind(("scope", barrier.required_scope))
            .bind(("subscription", barrier.required_subscription_revision)).bind(("authorization", barrier.required_authorization_revision))
            .await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
        Ok(())
        }.await;
        finish_transaction(transaction, operation).await
    }

    /// Inspect a journal without installing schema, initializing, or migrating it.
    /// The cancelled read transaction keeps all components and the barrier coherent.
    pub(crate) async fn inspect(
        &self,
        key: SyncStateKey<'_>,
    ) -> Result<(Option<StoredSyncState>, Option<ScopeBarrier>), SyncStateError> {
        validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let result = async {
            #[derive(SurrealValue)]
            struct Schema {
                tables: BTreeMap<String, String>,
            }
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
                    return Ok((None, None));
                }
                Err(error) => return Err(SyncStateError::Database(error)),
            };
            let schema: Option<Schema> = response.take(0).map_err(SyncStateError::Database)?;
            let tables: BTreeSet<String> = schema
                .ok_or(SyncStateError::CorruptState)?
                .tables
                .into_keys()
                .collect();
            preflight_inspection(&transaction, &tables, key).await?;
            let state = load_v2(Connection::Inspection(&transaction, &tables), key).await?;
            let legacy: Option<StoredRecord> = if tables.contains(STATE_TABLE) {
                transaction
                    .select(record_id(key))
                    .await
                    .map_err(SyncStateError::Database)?
            } else {
                None
            };
            let state = match (state, legacy) {
                (Some(state), Some(sentinel)) => {
                    if sentinel.format_version != FORMAT_VERSION
                        || sentinel.partition_id != key.partition_id.0
                        || sentinel.client_id != key.client_id.0
                        || sentinel.payload.as_ref() != MIGRATED_SENTINEL
                        || parse_revision(&sentinel.revision)? > state.revision
                    {
                        return Err(SyncStateError::CorruptState);
                    }
                    Some(state)
                }
                (Some(state), None) => Some(state),
                (None, Some(legacy)) => Some(decode_record(key, legacy)?),
                (None, None) => None,
            };
            let barrier: Option<ScopeBarrier> = if tables.contains("_sync_client_scope_barrier") {
                transaction
                    .select(scope_barrier_record_id(key))
                    .await
                    .map_err(SyncStateError::Database)?
            } else {
                None
            };
            validate_scope_barrier(barrier.as_ref())?;
            if state.is_none() && barrier.is_some() {
                return Err(SyncStateError::CorruptState);
            }
            Ok((state, barrier))
        }
        .await;
        // Cancellation failure is an error even if the reads succeeded.
        transaction
            .cancel()
            .await
            .map_err(SyncStateError::Database)?;
        result
    }

    pub async fn load(
        &self,
        key: SyncStateKey<'_>,
    ) -> Result<Option<StoredSyncState>, SyncStateError> {
        validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        if let Some(state) = load_v2(Connection::Client(&client), key).await? {
            return Ok(Some(state));
        }
        let stored: Option<StoredRecord> = client
            .select(record_id(key))
            .await
            .map_err(SyncStateError::Database)?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.format_version == FORMAT_VERSION {
            return Err(SyncStateError::CorruptState);
        }
        let state = decode_record(key, stored)?;
        self.migrate_v1(key, &state).await?;
        Ok(Some(state))
    }

    /// Atomically replace one client's complete durable state.
    ///
    /// `expected_revision = None` initializes an absent row at revision zero.
    /// Existing state requires the exact prior revision. Repeating an already
    /// committed byte-identical write succeeds without changing the record.
    pub async fn save_atomic(
        &self,
        expected_revision: Option<u64>,
        state: &StoredSyncState,
    ) -> Result<SyncStateWrite, SyncStateError> {
        let (key, rows) = prepare_state_write(expected_revision, state)?;
        let _ = self.load(key).await?;

        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation =
            replace_v2_in_transaction(&transaction, key, expected_revision, None, state, &rows)
                .await;

        finish_transaction(transaction, operation).await
    }

    /// Apply a prepared optimistic projection and its durable state together.
    ///
    /// This is still a Rust-only prototype. A successful return means both the
    /// changed SurrealDB records and the sync metadata committed atomically.
    pub async fn apply_transition_atomic(
        &self,
        current: &StoredSyncState,
        next: &StoredSyncState,
    ) -> Result<SyncStateWrite, SyncStateError> {
        if current.partition_id != next.partition_id || current.client_id != next.client_id {
            return Err(SyncStateError::CorruptState);
        }
        current.validate().map_err(SyncStateError::InvalidState)?;
        if current == next {
            let key = SyncStateKey::new(&current.partition_id, &current.client_id);
            return match self.load(key).await? {
                Some(stored) if stored == *current => Ok(SyncStateWrite::AlreadyCurrent),
                Some(_) | None => Err(SyncStateError::RevisionConflict),
            };
        }

        let encoding_started = std::time::Instant::now();
        let encoded = prepare_state_write(Some(current.revision), next);
        if let Some(diagnostics) = self.diagnostics {
            diagnostics.state_encoding.record(encoding_started);
        }
        let (key, rows) = encoded?;
        let current_projection = optimistic_projection(current);
        let next_projection = optimistic_projection(next);

        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            let disposition = replace_v2_in_transaction(
                &transaction,
                key,
                Some(current.revision),
                Some(current),
                next,
                &rows,
            )
            .await?;
            if disposition == SyncStateWrite::Applied {
                apply_projection_diff(&transaction, &current_projection, &next_projection).await?;
            }
            Ok(disposition)
        }
        .await;

        finish_transaction(transaction, operation).await
    }

    async fn migrate_v1(
        &self,
        key: SyncStateKey<'_>,
        state: &StoredSyncState,
    ) -> Result<(), SyncStateError> {
        let rows = normalize_state(state)?;
        let projection = optimistic_projection(state);
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            if let Some(existing) = load_v2(Connection::Transaction(&transaction), key).await? {
                return if existing == *state {
                    Ok(SyncStateWrite::AlreadyCurrent)
                } else {
                    Err(SyncStateError::RevisionConflict)
                };
            }
            let legacy: Option<StoredRecord> = transaction
                .select(record_id(key))
                .await
                .map_err(SyncStateError::Database)?;
            let Some(legacy) = legacy else {
                return Err(SyncStateError::RevisionConflict);
            };
            if decode_record(key, legacy)? != *state {
                return Err(SyncStateError::RevisionConflict);
            }
            write_row_diff(&transaction, key, None, &rows).await?;
            apply_projection_diff(&transaction, &BTreeMap::new(), &projection).await?;
            let sentinel = StoredRecord {
                format_version: FORMAT_VERSION,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                revision: state.revision.to_string(),
                payload: MIGRATED_SENTINEL.to_vec().into(),
            };
            let _: Option<Value> = transaction
                .upsert(record_id(key))
                .content(sentinel)
                .await
                .map_err(SyncStateError::Database)?;
            Ok(SyncStateWrite::Applied)
        }
        .await;
        finish_transaction(transaction, operation).await.map(|_| ())
    }
}

pub(crate) async fn finish_transaction<T>(
    transaction: Transaction<Any>,
    operation: Result<T, SyncStateError>,
) -> Result<T, SyncStateError> {
    match operation {
        Ok(disposition) => transaction
            .commit()
            .await
            .map(|_| disposition)
            .map_err(SyncStateError::CommitUnknown),
        Err(operation) => match transaction.cancel().await {
            Ok(_) => Err(operation),
            Err(rollback) => Err(SyncStateError::RollbackFailed {
                operation: Box::new(operation),
                rollback,
            }),
        },
    }
}

fn prepare_state_write<'a>(
    expected_revision: Option<u64>,
    state: &'a StoredSyncState,
) -> Result<(SyncStateKey<'a>, NormalizedRows), SyncStateError> {
    let key = SyncStateKey::new(&state.partition_id, &state.client_id);
    validate_key(key)?;
    validate_next_revision(expected_revision, state.revision)?;
    state.validate().map_err(SyncStateError::InvalidState)?;
    Ok((key, normalize_state(state)?))
}

fn normalize_state(state: &StoredSyncState) -> Result<NormalizedRows, SyncStateError> {
    let count = state
        .confirmed
        .len()
        .checked_add(
            state
                .subscriptions
                .as_ref()
                .map_or(0, |value| value.records.len()),
        )
        .and_then(|value| value.checked_add(state.outbox.len()))
        .and_then(|value| value.checked_add(state.outcomes.len()))
        .and_then(|value| value.checked_add(state.id_map.len()))
        .ok_or(SyncStateError::StateTooLarge)?;
    if count > MAX_COMPONENT_ITEMS {
        return Err(SyncStateError::StateTooLarge);
    }
    for record_id in state.confirmed.keys() {
        validate_component_key(&record_id.0)?;
    }
    for commit in &state.outbox {
        validate_component_key(&commit.identity.client_commit_id.0)?;
    }
    for outcome in &state.outcomes {
        validate_component_key(&outcome.local_commit.identity.client_commit_id.0)?;
    }
    for (local_id, canonical_id) in &state.id_map {
        validate_component_key(&local_id.0)?;
        validate_component_key(&canonical_id.0)?;
    }
    let mut subscription_meta = state.subscriptions.clone();
    let subscription_records = subscription_meta
        .as_mut()
        .map(|value| std::mem::take(&mut value.records))
        .unwrap_or_default();
    for key in subscription_records.keys() {
        validate_component_key(key)?;
    }
    let meta = encode_tagged(&StateMeta {
        partition_id: state.partition_id.0.clone(),
        client_id: state.client_id.0.clone(),
        requested_scope: state.requested_scope.0.clone(),
        subscription_revision: state.subscription_revision.to_string(),
        revision: state.revision.to_string(),
        checkpoint: state.checkpoint.clone(),
        reset_coverage: state.reset_coverage.clone(),
        subscriptions: subscription_meta,
        subscription_record_keys: subscription_records.keys().cloned().collect(),
        confirmed_keys: state.confirmed.keys().cloned().collect(),
        outbox_keys: state
            .outbox
            .iter()
            .map(|commit| commit.identity.client_commit_id.0.clone())
            .collect(),
        outcome_keys: state
            .outcomes
            .iter()
            .map(|outcome| outcome.local_commit.identity.client_commit_id.0.clone())
            .collect(),
        id_map_keys: state.id_map.keys().cloned().collect(),
    })?;
    let mut components = BTreeMap::new();
    for (key, value) in subscription_records {
        components.insert(
            address(SUBSCRIPTION_RECORD_TABLE, key),
            encode_tagged(&value)?,
        );
    }
    for (key, value) in &state.confirmed {
        components.insert(
            address(CONFIRMED_TABLE, key.0.clone()),
            encode_tagged(value)?,
        );
    }
    for value in &state.outbox {
        components.insert(
            address(OUTBOX_TABLE, value.identity.client_commit_id.0.clone()),
            encode_tagged(value)?,
        );
    }
    for value in &state.outcomes {
        components.insert(
            address(
                OUTCOME_TABLE,
                value.local_commit.identity.client_commit_id.0.clone(),
            ),
            encode_tagged(value)?,
        );
    }
    for (key, value) in &state.id_map {
        components.insert(address(ID_MAP_TABLE, key.0.clone()), encode_tagged(value)?);
    }
    let total = components.values().try_fold(meta.len(), |total, payload| {
        total
            .checked_add(payload.len())
            .ok_or(SyncStateError::StateTooLarge)
    })?;
    if total > MAX_AGGREGATE_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(NormalizedRows {
        format_version: if state.subscriptions.is_some() {
            SUBSCRIPTION_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        },
        meta,
        components,
    })
}

/// Exact normalized payload accounting, including the metadata component.
pub(crate) fn state_budget(state: &StoredSyncState) -> Result<(usize, usize), SyncStateError> {
    normalized_budget(&normalize_state(state)?)
}

fn normalized_budget(rows: &NormalizedRows) -> Result<(usize, usize), SyncStateError> {
    let bytes = rows
        .components
        .values()
        .try_fold(rows.meta.len(), |sum, bytes| {
            sum.checked_add(bytes.len())
                .ok_or(SyncStateError::StateTooLarge)
        })?;
    Ok((bytes, rows.components.len() + 1))
}

/// Private composition: caller owns the transaction and has scalar-admitted its journal.
/// Does not begin/commit; projection failure rolls back with every other caller write.
pub(crate) async fn apply_transition_in_transaction(
    transaction: &Transaction<Any>,
    current: &StoredSyncState,
    next: &StoredSyncState,
) -> Result<SyncStateWrite, SyncStateError> {
    let (key, rows) = prepare_state_write(Some(current.revision), next)?;
    let disposition = replace_v2_in_transaction(
        transaction,
        key,
        Some(current.revision),
        Some(current),
        next,
        &rows,
    )
    .await?;
    if disposition == SyncStateWrite::Applied {
        apply_projection_diff(
            transaction,
            &optimistic_projection(current),
            &optimistic_projection(next),
        )
        .await?;
    }
    Ok(disposition)
}

/// Deterministic tests drive the same inner transaction as apply_transition_atomic.
#[cfg(test)]
pub(crate) async fn test_transition_in_transaction(
    transaction: &Transaction<Any>,
    current: &StoredSyncState,
    next: &StoredSyncState,
) -> Result<SyncStateWrite, SyncStateError> {
    apply_transition_in_transaction(transaction, current, next).await
}

/// Caller holds the same transaction-local combined scalar inspection proof.
/// This still performs the complete existing payload/canonical/orphan validation.
pub(crate) async fn transaction_state_after_preflight(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<StoredSyncState, SyncStateError> {
    load_v2(Connection::Transaction(transaction), key)
        .await?
        .ok_or(SyncStateError::CorruptState)
}

fn address(table: &'static str, key: String) -> ComponentAddress {
    ComponentAddress { table, key }
}

async fn replace_v2_in_transaction(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    expected_revision: Option<u64>,
    expected_state: Option<&StoredSyncState>,
    state: &StoredSyncState,
    rows: &NormalizedRows,
) -> Result<SyncStateWrite, SyncStateError> {
    let current = load_v2(Connection::Transaction(transaction), key).await?;
    if let Some(expected_state) = expected_state
        && current.as_ref() != Some(expected_state)
    {
        return Err(SyncStateError::RevisionConflict);
    }
    match current {
        Some(ref current) if current.revision == state.revision => {
            return if current == state {
                Ok(SyncStateWrite::AlreadyCurrent)
            } else {
                Err(SyncStateError::RevisionConflict)
            };
        }
        Some(ref current) if Some(current.revision) != expected_revision => {
            return Err(SyncStateError::RevisionConflict);
        }
        None if expected_revision.is_some() => return Err(SyncStateError::RevisionConflict),
        _ => {}
    }
    // A normal save must never erase a feature's durable cancellation/auth barriers.
    if current
        .as_ref()
        .is_some_and(|current| current.subscriptions.is_some())
        && state.subscriptions.is_none()
    {
        return Err(SyncStateError::CorruptState);
    }
    let current_rows = current.as_ref().map(normalize_state).transpose()?;
    write_row_diff(transaction, key, current_rows.as_ref(), rows).await?;
    Ok(SyncStateWrite::Applied)
}

async fn write_row_diff(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    current: Option<&NormalizedRows>,
    next: &NormalizedRows,
) -> Result<(), SyncStateError> {
    let bootstrap_fence =
        crate::sync_bootstrap_store::guard_state_write(transaction, key, normalized_budget(next)?)
            .await?;
    if let Some(current) = current {
        for item in current.components.keys() {
            if !next.components.contains_key(item) {
                let _: Option<Value> = transaction
                    .delete(component_record_id(key, item))
                    .await
                    .map_err(SyncStateError::Database)?;
            }
        }
    }
    for (item, payload) in &next.components {
        if current.and_then(|value| value.components.get(item)) == Some(payload) {
            continue;
        }
        let row = StoredComponentRow {
            format_version: next.format_version,
            partition_id: key.partition_id.0.clone(),
            client_id: key.client_id.0.clone(),
            component_key: item.key.clone(),
            payload: payload.clone().into(),
        };
        let _: Option<Value> = transaction
            .upsert(component_record_id(key, item))
            .content(row)
            .await
            .map_err(SyncStateError::Database)?;
    }
    let row = StoredMetaRow {
        format_version: next.format_version,
        partition_id: key.partition_id.0.clone(),
        client_id: key.client_id.0.clone(),
        payload: next.meta.clone().into(),
    };
    let _: Option<Value> = transaction
        .upsert(meta_record_id(key))
        .content(row)
        .await
        .map_err(SyncStateError::Database)?;
    if let Some(fence) = bootstrap_fence {
        crate::sync_bootstrap_store::write_fence(transaction, key, fence).await?;
    }
    Ok(())
}

/// Only scalar lengths cross into Rust until the entire journal passes its budgets.
/// LIMIT bounds returned metadata rows, not database scans or server-side CPU.
#[derive(SurrealValue)]
struct InspectionLengths {
    payload_bytes: u64,
    partition_bytes: u64,
    client_bytes: u64,
    key_bytes: u64,
}

fn inspection_length_budget(
    row: &InspectionLengths,
    component: bool,
    aggregate: &mut u64,
    count: &mut usize,
) -> Result<(), SyncStateError> {
    // JSON encoding can expand each UTF-8 byte by up to six bytes plus quotes.
    // Exact decoded identity/key limits are still checked by the normal loader.
    if row.payload_bytes > MAX_SYNC_STATE_BYTES as u64
        || row.partition_bytes > (6 * MAX_IDENTITY_BYTES + 2) as u64
        || row.client_bytes > (6 * MAX_IDENTITY_BYTES + 2) as u64
        || row.key_bytes > (6 * MAX_COMPONENT_KEY_BYTES + 2) as u64
    {
        return Err(SyncStateError::StateTooLarge);
    }
    *aggregate = aggregate
        .checked_add(row.payload_bytes)
        .ok_or(SyncStateError::StateTooLarge)?;
    *count = count
        .checked_add(usize::from(component))
        .ok_or(SyncStateError::StateTooLarge)?;
    if *aggregate > MAX_AGGREGATE_STATE_BYTES as u64 || *count > MAX_COMPONENT_ITEMS {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}

pub(crate) async fn preflight_inspection(
    transaction: &Transaction<Any>,
    tables: &BTreeSet<String>,
    key: SyncStateKey<'_>,
) -> Result<(usize, usize), SyncStateError> {
    let mut statements = Vec::new();
    let mut selected = Vec::new();
    for table in [
        META_TABLE,
        STATE_TABLE,
        CONFIRMED_TABLE,
        OUTBOX_TABLE,
        OUTCOME_TABLE,
        ID_MAP_TABLE,
        SUBSCRIPTION_RECORD_TABLE,
    ] {
        if !tables.contains(table) {
            continue;
        }
        let component = !matches!(table, META_TABLE | STATE_TABLE);
        let key_expression = if component {
            "bytes::len(<bytes>encoding::json::encode(component_key))"
        } else if table == STATE_TABLE {
            "bytes::len(<bytes>encoding::json::encode(revision))"
        } else {
            "0"
        };
        let projection = format!(
            "SELECT bytes::len(payload) AS payload_bytes, bytes::len(<bytes>encoding::json::encode(partition_id)) AS partition_bytes, bytes::len(<bytes>encoding::json::encode(client_id)) AS client_bytes, {key_expression} AS key_bytes"
        );
        statements.push(if component { format!("{projection} FROM {table} WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385") } else if table == STATE_TABLE { format!("{projection} FROM $legacy_id") } else { format!("{projection} FROM $meta_id") });
        selected.push((table, component));
    }
    let has_barrier = tables.contains("_sync_client_scope_barrier");
    if has_barrier {
        statements.push("SELECT bytes::len(<bytes>encoding::json::encode(reason)) AS reason_bytes, bytes::len(<bytes>encoding::json::encode(required_scope)) AS scope_bytes, bytes::len(<bytes>encoding::json::encode(required_subscription_revision)) AS subscription_bytes, bytes::len(<bytes>encoding::json::encode(required_authorization_revision)) AS authorization_bytes FROM $barrier_id".into());
    }
    if statements.is_empty() {
        return Ok((0, 1));
    }
    // Only scalar metadata is batched. Each table retains its prior returned-row
    // cap; returned metadata is bounded by the fixed table count times that cap,
    // not by a claim about scanned rows, CPU, or constant RSS. No blobs are loaded.
    let mut response = transaction
        .query(statements.join(";"))
        .bind(("partition_id", key.partition_id.0.clone()))
        .bind(("client_id", key.client_id.0.clone()))
        .bind(("meta_id", meta_record_id(key)))
        .bind(("legacy_id", record_id(key)))
        .bind(("barrier_id", scope_barrier_record_id(key)))
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    let mut aggregate = 0;
    let mut count = 0;
    for (index, (table, component)) in selected.iter().enumerate() {
        let rows: Vec<InspectionLengths> =
            response.take(index).map_err(SyncStateError::Database)?;
        for row in rows {
            if *table == STATE_TABLE {
                if row.key_bytes > 6 * 20 + 2 {
                    return Err(SyncStateError::StateTooLarge);
                }
                inspection_length_budget(&row, false, &mut 0, &mut 0)?;
            } else {
                inspection_length_budget(&row, *component, &mut aggregate, &mut count)?;
            }
        }
    }
    if has_barrier {
        #[derive(SurrealValue)]
        struct BarrierLengths {
            reason_bytes: u64,
            scope_bytes: u64,
            subscription_bytes: u64,
            authorization_bytes: u64,
        }
        let rows: Vec<BarrierLengths> = response
            .take(selected.len())
            .map_err(SyncStateError::Database)?;
        if rows.iter().any(|row| {
            row.reason_bytes > 6 * 32 + 2
                || row.scope_bytes > (6 * MAX_IDENTITY_BYTES + 2) as u64
                || row.subscription_bytes > 6 * 20 + 2
                || row.authorization_bytes > 6 * 20 + 2
        }) {
            return Err(SyncStateError::StateTooLarge);
        }
    }
    Ok((aggregate as usize, count + 1))
}

fn validate_scope_barrier(barrier: Option<&ScopeBarrier>) -> Result<(), SyncStateError> {
    if let Some(value) = barrier
        && (!matches!(
            value.reason.as_str(),
            "pending_quarantine_required" | "needs_new_scope_journal"
        ) || value.required_scope.len() > MAX_IDENTITY_BYTES
            || [
                &value.required_subscription_revision,
                &value.required_authorization_revision,
            ]
            .iter()
            .any(|s| match s.parse::<u64>() {
                Ok(value) => value.to_string() != **s,
                Err(_) => true,
            }))
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}

enum Connection<'a> {
    Client(&'a surrealdb::Surreal<Any>),
    Transaction(&'a Transaction<Any>),
    Inspection(&'a Transaction<Any>, &'a BTreeSet<String>),
}

impl Connection<'_> {
    async fn meta(&self, id: NativeRecordId) -> Result<Option<StoredMetaRow>, SyncStateError> {
        if let Self::Inspection(_, tables) = self
            && !tables.contains(META_TABLE)
        {
            return Ok(None);
        }
        match self {
            Self::Client(value) => value.select(id).await.map_err(SyncStateError::Database),
            Self::Transaction(value) | Self::Inspection(value, _) => {
                value.select(id).await.map_err(SyncStateError::Database)
            }
        }
    }

    async fn components(
        &self,
        table: &'static str,
        key: SyncStateKey<'_>,
    ) -> Result<Vec<StoredComponentRow>, SyncStateError> {
        if let Self::Inspection(_, tables) = self
            && !tables.contains(table)
        {
            return Ok(Vec::new());
        }
        let statement = match table {
            SUBSCRIPTION_RECORD_TABLE => {
                "SELECT * FROM _sync_client_subscription_record WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385"
            }
            CONFIRMED_TABLE => {
                "SELECT * FROM _sync_client_confirmed WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385"
            }
            OUTBOX_TABLE => {
                "SELECT * FROM _sync_client_outbox WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385"
            }
            OUTCOME_TABLE => {
                "SELECT * FROM _sync_client_outcome WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385"
            }
            ID_MAP_TABLE => {
                "SELECT * FROM _sync_client_id_map WHERE partition_id = $partition_id AND client_id = $client_id LIMIT 16385"
            }
            _ => return Err(SyncStateError::CorruptState),
        };
        match self {
            Self::Client(value) => {
                let mut response = value
                    .query(statement)
                    .bind(("partition_id", key.partition_id.0.clone()))
                    .bind(("client_id", key.client_id.0.clone()))
                    .await
                    .map_err(SyncStateError::Database)?
                    .check()
                    .map_err(SyncStateError::Database)?;
                response.take(0).map_err(SyncStateError::Database)
            }
            Self::Transaction(value) | Self::Inspection(value, _) => {
                let mut response = value
                    .query(statement)
                    .bind(("partition_id", key.partition_id.0.clone()))
                    .bind(("client_id", key.client_id.0.clone()))
                    .await
                    .map_err(SyncStateError::Database)?
                    .check()
                    .map_err(SyncStateError::Database)?;
                response.take(0).map_err(SyncStateError::Database)
            }
        }
    }
}

async fn load_v2(
    connection: Connection<'_>,
    key: SyncStateKey<'_>,
) -> Result<Option<StoredSyncState>, SyncStateError> {
    let mut inventory = load_inventory(&connection, key).await?;
    let Some(row) = connection.meta(meta_record_id(key)).await? else {
        return if inventory.is_empty() {
            Ok(None)
        } else {
            Err(SyncStateError::CorruptState)
        };
    };
    validate_meta_row(key, &row)?;
    let mut meta: StateMeta = decode_tagged(&row.payload)?;
    if (row.format_version == SUBSCRIPTION_FORMAT_VERSION) != meta.subscriptions.is_some() {
        return Err(SyncStateError::CorruptState);
    }
    if meta.partition_id != key.partition_id.0
        || meta.client_id != key.client_id.0
        || !strictly_sorted(&meta.confirmed_keys)
        || !strictly_sorted(&meta.subscription_record_keys)
        || !strictly_sorted(&meta.id_map_keys)
    {
        return Err(SyncStateError::CorruptState);
    }
    let count = meta
        .confirmed_keys
        .len()
        .checked_add(meta.subscription_record_keys.len())
        .and_then(|value| value.checked_add(meta.outbox_keys.len()))
        .and_then(|value| value.checked_add(meta.outcome_keys.len()))
        .and_then(|value| value.checked_add(meta.id_map_keys.len()))
        .ok_or(SyncStateError::StateTooLarge)?;
    if count > MAX_COMPONENT_ITEMS {
        return Err(SyncStateError::StateTooLarge);
    }
    let aggregate = inventory
        .values()
        .try_fold(row.payload.len(), |total, payload| {
            total
                .checked_add(payload.len())
                .ok_or(SyncStateError::StateTooLarge)
        })?;
    if aggregate > MAX_AGGREGATE_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    let mut confirmed = BTreeMap::new();
    for record_id in &meta.confirmed_keys {
        let bytes = take_component(
            &mut inventory,
            &address(CONFIRMED_TABLE, record_id.0.clone()),
        )?;
        confirmed.insert(
            record_id.clone(),
            decode_tagged::<RecordState<JsonValue>>(&bytes)?,
        );
    }
    let mut outbox = Vec::with_capacity(meta.outbox_keys.len());
    for commit_id in &meta.outbox_keys {
        let bytes = take_component(&mut inventory, &address(OUTBOX_TABLE, commit_id.clone()))?;
        let commit = decode_tagged::<ClientCommit<JsonValue>>(&bytes)?;
        if commit.identity.client_commit_id.0 != *commit_id {
            return Err(SyncStateError::CorruptState);
        }
        outbox.push(commit);
    }
    let mut outcomes = Vec::with_capacity(meta.outcome_keys.len());
    for commit_id in &meta.outcome_keys {
        let bytes = take_component(&mut inventory, &address(OUTCOME_TABLE, commit_id.clone()))?;
        let outcome = decode_tagged::<ResolvedCommit<JsonValue>>(&bytes)?;
        if outcome.local_commit.identity.client_commit_id.0 != *commit_id {
            return Err(SyncStateError::CorruptState);
        }
        outcomes.push(outcome);
    }
    let mut id_map = BTreeMap::new();
    for local in &meta.id_map_keys {
        let bytes = take_component(&mut inventory, &address(ID_MAP_TABLE, local.0.clone()))?;
        id_map.insert(local.clone(), decode_tagged::<ProtocolRecordId>(&bytes)?);
    }
    if meta
        .subscriptions
        .as_ref()
        .is_some_and(|value| !value.records.is_empty())
    {
        return Err(SyncStateError::CorruptState);
    }
    for key in &meta.subscription_record_keys {
        let bytes = take_component(
            &mut inventory,
            &address(SUBSCRIPTION_RECORD_TABLE, key.clone()),
        )?;
        let records = &mut meta
            .subscriptions
            .as_mut()
            .ok_or(SyncStateError::CorruptState)?
            .records;
        records.insert(key.clone(), decode_tagged(&bytes)?);
    }
    if !inventory.is_empty() {
        return Err(SyncStateError::CorruptState);
    }
    let state = StoredSyncState {
        partition_id: PartitionId(meta.partition_id),
        client_id: ClientId(meta.client_id),
        requested_scope: ScopeIdentity(meta.requested_scope),
        subscription_revision: parse_revision(&meta.subscription_revision)?,
        revision: parse_revision(&meta.revision)?,
        confirmed,
        outbox,
        outcomes,
        checkpoint: meta.checkpoint,
        reset_coverage: meta.reset_coverage,
        subscriptions: meta.subscriptions,
        id_map,
    };
    state.validate().map_err(SyncStateError::InvalidState)?;
    Ok(Some(state))
}

async fn load_inventory(
    connection: &Connection<'_>,
    key: SyncStateKey<'_>,
) -> Result<BTreeMap<ComponentAddress, Bytes>, SyncStateError> {
    let mut inventory = BTreeMap::new();
    for table in [
        CONFIRMED_TABLE,
        OUTBOX_TABLE,
        OUTCOME_TABLE,
        ID_MAP_TABLE,
        SUBSCRIPTION_RECORD_TABLE,
    ] {
        let rows = connection.components(table, key).await?;
        if rows.len() > MAX_COMPONENT_ITEMS {
            return Err(SyncStateError::StateTooLarge);
        }
        for row in rows {
            if !matches!(
                row.format_version,
                FORMAT_VERSION | SUBSCRIPTION_FORMAT_VERSION
            ) || row.partition_id != key.partition_id.0
                || row.client_id != key.client_id.0
                || row.payload.len() > MAX_SYNC_STATE_BYTES
            {
                return Err(SyncStateError::CorruptState);
            }
            validate_component_key(&row.component_key)?;
            let item = address(table, row.component_key);
            if inventory.insert(item, row.payload).is_some() {
                return Err(SyncStateError::CorruptState);
            }
            if inventory.len() > MAX_COMPONENT_ITEMS {
                return Err(SyncStateError::StateTooLarge);
            }
        }
    }
    Ok(inventory)
}

fn take_component(
    inventory: &mut BTreeMap<ComponentAddress, Bytes>,
    item: &ComponentAddress,
) -> Result<Bytes, SyncStateError> {
    inventory.remove(item).ok_or(SyncStateError::CorruptState)
}

fn validate_meta_row(key: SyncStateKey<'_>, row: &StoredMetaRow) -> Result<(), SyncStateError> {
    validate_meta_row_version(key, row, SUBSCRIPTION_FORMAT_VERSION)
}

fn validate_meta_row_version(
    key: SyncStateKey<'_>,
    row: &StoredMetaRow,
    max_version: i64,
) -> Result<(), SyncStateError> {
    if row.payload.len() > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    if !(FORMAT_VERSION..=max_version).contains(&row.format_version)
        || row.partition_id != key.partition_id.0
        || row.client_id != key.client_id.0
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

pub(crate) fn encode_tagged<T: Serialize>(value: &T) -> Result<Vec<u8>, SyncStateError> {
    let value = serde_json::to_value(value).map_err(SyncStateError::Encode)?;
    let bytes = serde_json::to_vec(&tag_json(value)).map_err(SyncStateError::Encode)?;
    if bytes.len() > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(bytes)
}

fn decode_tagged<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, SyncStateError> {
    if bytes.len() > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    let tagged: TaggedJson = serde_json::from_slice(bytes).map_err(SyncStateError::Decode)?;
    let canonical = serde_json::to_vec(&tagged).map_err(SyncStateError::Encode)?;
    if canonical != bytes {
        return Err(SyncStateError::CorruptState);
    }
    serde_json::from_value(untag_json(tagged)?).map_err(SyncStateError::Decode)
}

fn tag_json(value: JsonValue) -> TaggedJson {
    match value {
        JsonValue::Null => TaggedJson::Null,
        JsonValue::Bool(value) => TaggedJson::Bool(value),
        JsonValue::String(value) => TaggedJson::String(value),
        JsonValue::Number(value) => {
            if value.is_i64() {
                TaggedJson::I64(value.to_string())
            } else if value.is_u64() {
                TaggedJson::U64(value.to_string())
            } else {
                TaggedJson::Float(value.to_string())
            }
        }
        JsonValue::Array(value) => TaggedJson::Array(value.into_iter().map(tag_json).collect()),
        JsonValue::Object(value) => TaggedJson::Object(
            value
                .into_iter()
                .map(|(key, value)| (key, tag_json(value)))
                .collect(),
        ),
    }
}

fn untag_json(value: TaggedJson) -> Result<JsonValue, SyncStateError> {
    Ok(match value {
        TaggedJson::Null => JsonValue::Null,
        TaggedJson::Bool(value) => JsonValue::Bool(value),
        TaggedJson::String(value) => JsonValue::String(value),
        TaggedJson::I64(value) => JsonValue::Number(
            value
                .parse::<i64>()
                .map(JsonNumber::from)
                .map_err(|_| SyncStateError::CorruptState)?,
        ),
        TaggedJson::U64(value) => JsonValue::Number(
            value
                .parse::<u64>()
                .map(JsonNumber::from)
                .map_err(|_| SyncStateError::CorruptState)?,
        ),
        TaggedJson::Float(value) => JsonValue::Number(
            value
                .parse::<JsonNumber>()
                .map_err(|_| SyncStateError::CorruptState)?,
        ),
        TaggedJson::Array(value) => JsonValue::Array(
            value
                .into_iter()
                .map(untag_json)
                .collect::<Result<_, _>>()?,
        ),
        TaggedJson::Object(value) => JsonValue::Object(
            value
                .into_iter()
                .map(|(key, value)| Ok((key, untag_json(value)?)))
                .collect::<Result<_, SyncStateError>>()?,
        ),
    })
}

async fn apply_projection_diff(
    transaction: &Transaction<Any>,
    current: &BTreeMap<ProtocolRecordId, OptimisticRecord<JsonValue>>,
    next: &BTreeMap<ProtocolRecordId, OptimisticRecord<JsonValue>>,
) -> Result<(), SyncStateError> {
    let record_ids = current
        .keys()
        .chain(next.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    for record_id in record_ids {
        let previous = current.get(&record_id);
        let updated = next.get(&record_id);
        if previous == updated {
            continue;
        }

        let native_id = native_record_id(&record_id)?;
        match updated {
            Some(OptimisticRecord::Present { value, .. }) => {
                let Value::Object(content) =
                    from_wire_value(value.clone()).map_err(SyncStateError::ValueCodec)?
                else {
                    return Err(SyncStateError::RecordValueMustBeObject);
                };
                let _: Option<Value> = transaction
                    .upsert(native_id)
                    .content(content)
                    .await
                    .map_err(SyncStateError::Database)?;
            }
            Some(OptimisticRecord::Deleted { .. }) | None => {
                let _: Option<Value> = transaction
                    .delete(native_id)
                    .await
                    .map_err(SyncStateError::Database)?;
            }
        }
    }
    Ok(())
}

fn decode_record(
    key: SyncStateKey<'_>,
    stored: StoredRecord,
) -> Result<StoredSyncState, SyncStateError> {
    validate_stored_envelope(key, &stored)?;
    if stored.payload.len() > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    let revision = parse_revision(&stored.revision)?;
    let state: StoredSyncState =
        serde_json::from_slice(&stored.payload).map_err(SyncStateError::Decode)?;
    if state.partition_id != *key.partition_id
        || state.client_id != *key.client_id
        || state.revision != revision
    {
        return Err(SyncStateError::CorruptState);
    }
    state.validate().map_err(SyncStateError::InvalidState)?;
    Ok(state)
}

pub(crate) fn validate_key(key: SyncStateKey<'_>) -> Result<(), SyncStateError> {
    if key.partition_id.0.len() > MAX_IDENTITY_BYTES || key.client_id.0.len() > MAX_IDENTITY_BYTES {
        return Err(SyncStateError::IdentityTooLarge);
    }
    Ok(())
}

fn validate_component_key(value: &str) -> Result<(), SyncStateError> {
    if value.is_empty() || value.len() > MAX_COMPONENT_KEY_BYTES {
        return Err(SyncStateError::IdentityTooLarge);
    }
    Ok(())
}

fn validate_next_revision(
    expected_revision: Option<u64>,
    revision: u64,
) -> Result<(), SyncStateError> {
    let expected_next = match expected_revision {
        Some(expected) => expected
            .checked_add(1)
            .ok_or(SyncStateError::RevisionMismatch)?,
        None => 0,
    };
    if revision != expected_next {
        return Err(SyncStateError::RevisionMismatch);
    }
    Ok(())
}

fn validate_stored_envelope(
    key: SyncStateKey<'_>,
    stored: &StoredRecord,
) -> Result<(), SyncStateError> {
    if stored.format_version != LEGACY_FORMAT_VERSION
        || stored.partition_id != key.partition_id.0
        || stored.client_id != key.client_id.0
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}

fn parse_revision(revision: &str) -> Result<u64, SyncStateError> {
    let parsed = revision
        .parse::<u64>()
        .map_err(|_| SyncStateError::CorruptState)?;
    if parsed.to_string() != revision {
        return Err(SyncStateError::CorruptState);
    }
    Ok(parsed)
}

fn record_id(key: SyncStateKey<'_>) -> NativeRecordId {
    NativeRecordId::new(
        STATE_TABLE,
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}

pub(crate) fn meta_record_id(key: SyncStateKey<'_>) -> NativeRecordId {
    NativeRecordId::new(
        META_TABLE,
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}

fn component_record_id(key: SyncStateKey<'_>, item: &ComponentAddress) -> NativeRecordId {
    NativeRecordId::new(
        item.table,
        Array::from(vec![
            key.partition_id.0.clone(),
            key.client_id.0.clone(),
            item.key.clone(),
        ]),
    )
}

pub(crate) fn native_record_id(
    record_id: &ProtocolRecordId,
) -> Result<NativeRecordId, SyncStateError> {
    if record_id.0.len() > MAX_RECORD_ID_BYTES {
        return Err(SyncStateError::UnsupportedRecordId);
    }
    let record_id = NativeRecordId::parse_simple(&record_id.0)
        .map_err(|_| SyncStateError::UnsupportedRecordId)?;
    if [
        STATE_TABLE,
        META_TABLE,
        CONFIRMED_TABLE,
        OUTBOX_TABLE,
        OUTCOME_TABLE,
        ID_MAP_TABLE,
    ]
    .contains(&record_id.table.as_str())
    {
        return Err(SyncStateError::UnsupportedRecordId);
    }
    Ok(record_id)
}

fn scope_barrier_record_id(key: SyncStateKey<'_>) -> NativeRecordId {
    NativeRecordId::new(
        "_sync_client_scope_barrier",
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use surrealdb_sync_client::{ClientRuntime, OptimisticRecord};
    use surrealdb_sync_protocol::{
        BaseVersion, Checkpoint, ClientCommit, ClientCommitId, CommitIdentity, Cursor,
        DurableOutcome, Fingerprint, OpaqueCheckpoint, Operation, RecordId, RecordState,
        RejectReason, ScopeIdentity, ScopeSnapshot,
    };

    use super::*;
    use crate::{ConnectOptions, connect};

    fn empty_state(partition: &str, client: &str) -> StoredSyncState {
        DurableClientState::empty(
            PartitionId(partition.into()),
            ClientId(client.into()),
            ScopeIdentity("all".into()),
            1,
        )
    }

    fn enqueue(
        state: StoredSyncState,
        commit_id: &str,
        record_id: &str,
        value: JsonValue,
    ) -> StoredSyncState {
        let runtime = ClientRuntime::open(state).unwrap();
        runtime
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId(commit_id.into()),
                    fingerprint: Fingerprint(format!("fingerprint-{commit_id}")),
                },
                operations: vec![Operation::Upsert {
                    record_id: RecordId(record_id.into()),
                    base_version: BaseVersion::Absent,
                    value,
                    reference: None,
                }],
            })
            .unwrap()
            .into_state()
    }

    fn enqueue_delete(state: StoredSyncState, commit_id: &str, record_id: &str) -> StoredSyncState {
        let runtime = ClientRuntime::open(state).unwrap();
        runtime
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId(commit_id.into()),
                    fingerprint: Fingerprint(format!("fingerprint-{commit_id}")),
                },
                operations: vec![Operation::Delete {
                    record_id: RecordId(record_id.into()),
                    base_version: 0,
                }],
            })
            .unwrap()
            .into_state()
    }

    async fn memory_database() -> std::sync::Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("sync_state_tests".into()),
            database: Some("sync_state_tests".into()),
        })
        .await
        .unwrap()
    }

    async fn surrealkv_database(path: &std::path::Path) -> std::sync::Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: format!("surrealkv://{}", path.display()),
            namespace: Some("sync_state_tests".into()),
            database: Some("sync_state_tests".into()),
        })
        .await
        .unwrap()
    }

    fn structural_root(value: Option<Value>) -> Option<Value> {
        value.map(|mut value| {
            if let Value::Object(root) = &mut value {
                // Exclude only the two observed volatile INFO counters, preserving
                // every other field (including unknown/new system metadata).
                if let Some(Value::Object(system)) = root.get_mut("system") {
                    system.remove("cpu_usage");
                    system.remove("memory_usage");
                }
            }
            value
        })
    }

    #[tokio::test]
    async fn inspection_missing_and_legacy_never_install_or_migrate() {
        let database = memory_database().await;
        let client = database.client().await.unwrap();
        let state = enqueue(
            empty_state("partition", "legacy"),
            "one",
            "person:local",
            json!("private"),
        );
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let store = SyncStateStore::new(&database);
        let before = client
            .query("INFO FOR ROOT")
            .await
            .unwrap()
            .take::<Option<Value>>(0)
            .unwrap();
        assert!(store.inspect(key).await.unwrap().0.is_none());
        let after = client
            .query("INFO FOR ROOT")
            .await
            .unwrap()
            .take::<Option<Value>>(0)
            .unwrap();
        assert_eq!(structural_root(before.clone()), structural_root(after));
        client
            .query("DEFINE NAMESPACE unexpected_schema_mutation")
            .await
            .unwrap()
            .check()
            .unwrap();
        let changed = client
            .query("INFO FOR ROOT")
            .await
            .unwrap()
            .take::<Option<Value>>(0)
            .unwrap();
        assert_ne!(
            structural_root(before),
            structural_root(changed),
            "structural namespace changes must remain visible"
        );
        // A genuine v1-only schema: no normalized tables are defined.
        client
            .query("DEFINE TABLE _sync_client_state SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let _: Option<Value> = client
            .upsert(record_id(key))
            .content(StoredRecord {
                format_version: LEGACY_FORMAT_VERSION,
                partition_id: state.partition_id.0.clone(),
                client_id: state.client_id.0.clone(),
                revision: state.revision.to_string(),
                payload: serde_json::to_vec(&state).unwrap().into(),
            })
            .await
            .unwrap();
        let before = client
            .query("INFO FOR DB; SELECT * FROM _sync_client_state")
            .await
            .unwrap();
        let mut before = before;
        let schema: Option<Value> = before.take(0).unwrap();
        let rows: Vec<Value> = before.take(1).unwrap();
        assert_eq!(store.inspect(key).await.unwrap().0, Some(state));
        let mut after = client
            .query("INFO FOR DB; SELECT * FROM _sync_client_state")
            .await
            .unwrap();
        assert_eq!(schema, after.take::<Option<Value>>(0).unwrap());
        assert_eq!(rows, after.take::<Vec<Value>>(1).unwrap());
    }

    #[tokio::test]
    async fn inspection_rejects_barrier_only_orphan_and_corrupt_legacy() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let state = empty_state("partition", "client");
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        store
            .save_scope_barrier(
                key,
                ScopeBarrier {
                    reason: "needs_new_scope_journal".into(),
                    required_scope: "new".into(),
                    required_subscription_revision: "2".into(),
                    required_authorization_revision: "3".into(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            store.inspect(key).await,
            Err(SyncStateError::CorruptState)
        ));
        let client = database.client().await.unwrap();
        let _: Option<Value> = client.delete(scope_barrier_record_id(key)).await.unwrap();
        let _: Option<Value> = client
            .upsert(record_id(key))
            .content(StoredRecord {
                format_version: LEGACY_FORMAT_VERSION,
                partition_id: "wrong-owner".into(),
                client_id: "client".into(),
                revision: "0".into(),
                payload: b"invalid".to_vec().into(),
            })
            .await
            .unwrap();
        assert!(store.inspect(key).await.is_err());
        let _: Option<Value> = client.delete(record_id(key)).await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let queued = enqueue(state.clone(), "one", "person:local", json!("private"));
        store.save_atomic(Some(0), &queued).await.unwrap();
        let _: Option<Value> = client.delete(meta_record_id(key)).await.unwrap();
        assert!(matches!(
            store.inspect(key).await,
            Err(SyncStateError::CorruptState)
        ));
    }

    #[tokio::test]
    async fn inspection_rejects_missing_components_and_foreign_metadata() {
        for foreign_metadata in [false, true] {
            let database = memory_database().await;
            let store = SyncStateStore::new(&database);
            store.define_schema().await.unwrap();
            let state = enqueue(
                empty_state("partition", "client"),
                "one",
                "person:local",
                json!("private"),
            );
            let key = SyncStateKey::new(&state.partition_id, &state.client_id);
            let initial = empty_state("partition", "client");
            store.save_atomic(None, &initial).await.unwrap();
            store.save_atomic(Some(0), &state).await.unwrap();
            let client = database.client().await.unwrap();
            client
                .query(if foreign_metadata {
                    "UPDATE _sync_client_meta SET partition_id = 'foreign'"
                } else {
                    "DELETE _sync_client_outbox"
                })
                .await
                .unwrap()
                .check()
                .unwrap();
            assert!(matches!(
                store.inspect(key).await,
                Err(SyncStateError::CorruptState)
            ));
        }
    }

    #[tokio::test]
    async fn inspection_preflights_oversize_and_aggregate_before_decoding_payloads() {
        for aggregate_overflow in [false, true] {
            let database = memory_database().await;
            let store = SyncStateStore::new(&database);
            store.define_schema().await.unwrap();
            let state = empty_state("partition", "client");
            let key = SyncStateKey::new(&state.partition_id, &state.client_id);
            let client = database.client().await.unwrap();
            // Deliberately malformed metadata: any payload decoding would fail
            // differently, before reaching the aggregate validation in load_v2.
            let _: Option<Value> = client
                .upsert(meta_record_id(key))
                .content(StoredMetaRow {
                    format_version: FORMAT_VERSION,
                    partition_id: "partition".into(),
                    client_id: "client".into(),
                    payload: b"not-json".to_vec().into(),
                })
                .await
                .unwrap();
            let rows = if aggregate_overflow { 17 } else { 1 };
            let payload = vec![0; MAX_SYNC_STATE_BYTES + usize::from(!aggregate_overflow)];
            for index in 0..rows {
                let component_key = format!("commit-{index}");
                let _: Option<Value> = client
                    .upsert(component_record_id(
                        key,
                        &address(OUTBOX_TABLE, component_key.clone()),
                    ))
                    .content(StoredComponentRow {
                        format_version: FORMAT_VERSION,
                        partition_id: "partition".into(),
                        client_id: "client".into(),
                        component_key,
                        payload: payload.clone().into(),
                    })
                    .await
                    .unwrap();
            }
            assert!(matches!(
                store.inspect(key).await,
                Err(SyncStateError::StateTooLarge)
            ));
        }
    }

    #[test]
    fn inspection_preflight_metadata_budgets_preserve_exact_limits() {
        let row = InspectionLengths {
            payload_bytes: MAX_SYNC_STATE_BYTES as u64,
            partition_bytes: 6 * MAX_IDENTITY_BYTES as u64 + 2,
            client_bytes: 6 * MAX_IDENTITY_BYTES as u64 + 2,
            key_bytes: 6 * MAX_COMPONENT_KEY_BYTES as u64 + 2,
        };
        let mut aggregate = 0;
        let mut count = 0;
        for _ in 0..16 {
            inspection_length_budget(&row, true, &mut aggregate, &mut count).unwrap();
        }
        assert_eq!(aggregate, MAX_AGGREGATE_STATE_BYTES as u64);
        assert!(matches!(
            inspection_length_budget(&row, true, &mut aggregate, &mut count),
            Err(SyncStateError::StateTooLarge)
        ));
        let row = InspectionLengths {
            payload_bytes: 0,
            partition_bytes: 0,
            client_bytes: 0,
            key_bytes: 0,
        };
        let mut count = MAX_COMPONENT_ITEMS;
        assert!(matches!(
            inspection_length_budget(&row, true, &mut 0, &mut count),
            Err(SyncStateError::StateTooLarge)
        ));
    }

    #[tokio::test]
    async fn atomic_save_is_revision_checked_and_idempotent() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();

        let initial = empty_state("partition", "client");
        assert_eq!(
            store.save_atomic(None, &initial).await.unwrap(),
            SyncStateWrite::Applied
        );
        assert_eq!(
            store.save_atomic(None, &initial).await.unwrap(),
            SyncStateWrite::AlreadyCurrent
        );

        let queued = enqueue(initial.clone(), "commit-1", "person:local", json!("Ada"));
        assert_eq!(
            store.save_atomic(Some(0), &queued).await.unwrap(),
            SyncStateWrite::Applied
        );
        assert_eq!(
            store.save_atomic(Some(0), &queued).await.unwrap(),
            SyncStateWrite::AlreadyCurrent
        );

        let competing = enqueue(initial, "commit-2", "person:other", json!("Grace"));
        assert!(matches!(
            store.save_atomic(Some(0), &competing).await,
            Err(SyncStateError::RevisionConflict)
        ));

        let loaded = store
            .load(SyncStateKey::new(&queued.partition_id, &queued.client_id))
            .await
            .unwrap()
            .unwrap();
        let runtime = ClientRuntime::open(loaded).unwrap();
        assert_eq!(runtime.pending_commits().count(), 1);
        assert_eq!(
            runtime.optimistic().get(&RecordId("person:local".into())),
            Some(&OptimisticRecord::Present {
                value: json!("Ada"),
                base_version: BaseVersion::Absent,
                reference: None,
            })
        );
    }

    #[tokio::test]
    async fn cancelled_replacement_leaves_previous_state_visible() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let queued = enqueue(initial.clone(), "commit-1", "person:local", json!("Ada"));
        let rows = normalize_state(&queued).unwrap();
        let key = SyncStateKey::new(&queued.partition_id, &queued.client_id);

        let client = database.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        assert_eq!(
            replace_v2_in_transaction(&transaction, key, Some(0), None, &queued, &rows)
                .await
                .unwrap(),
            SyncStateWrite::Applied
        );
        let client = transaction.cancel().await.unwrap();
        assert_eq!(store.load(key).await.unwrap(), Some(initial));

        let transaction = client.begin().await.unwrap();
        replace_v2_in_transaction(&transaction, key, Some(0), None, &queued, &rows)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(store.load(key).await.unwrap(), Some(queued));
    }

    #[tokio::test]
    async fn subscription_format_upgrade_preserves_pending_and_refuses_v2_downgrade() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let queued = enqueue(
            initial,
            "pending-before-upgrade",
            "probe:local",
            json!({"name":"pending"}),
        );
        store.save_atomic(Some(0), &queued).await.unwrap();
        let key = SyncStateKey::new(&queued.partition_id, &queued.client_id);
        let before: StoredMetaRow = database
            .client()
            .await
            .unwrap()
            .select(meta_record_id(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.format_version, FORMAT_VERSION);
        let mut runtime = surrealdb_sync_client::ClientRuntime::open(queued.clone()).unwrap();
        let start = runtime
            .prepare_start_subscription(
                crate::sync_subscription::parse_request(
                    &crate::sync_subscription::test_request().to_string(),
                )
                .unwrap(),
            )
            .unwrap();
        store
            .apply_transition_atomic(runtime.state(), start.state())
            .await
            .unwrap();
        runtime.install_with_read_batch(start).unwrap();
        let response = runtime
            .prepare_subscription_response(
                crate::sync_subscription::parse_response(
                    &crate::sync_subscription::test_response().to_string(),
                )
                .unwrap(),
            )
            .unwrap();
        store
            .apply_transition_atomic(runtime.state(), response.state())
            .await
            .unwrap();
        runtime.install_with_read_batch(response).unwrap();
        let cancel = runtime.prepare_cancel_subscription("window", 1).unwrap();
        store
            .apply_transition_atomic(runtime.state(), cancel.state())
            .await
            .unwrap();
        runtime.install_with_read_batch(cancel).unwrap();
        let metadata: StoredMetaRow = database
            .client()
            .await
            .unwrap()
            .select(meta_record_id(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.format_version, SUBSCRIPTION_FORMAT_VERSION);
        // Models the prior V2 reader's exact version check, before decode or mutation.
        assert!(matches!(
            validate_meta_row_version(key, &metadata, FORMAT_VERSION),
            Err(SyncStateError::CorruptState)
        ));
        let mut downgrade = runtime.state().clone();
        downgrade.subscriptions = None;
        downgrade.revision += 1;
        assert!(matches!(
            store
                .save_atomic(Some(runtime.state().revision), &downgrade)
                .await,
            Err(SyncStateError::CorruptState)
        ));
        assert_eq!(
            store.load(key).await.unwrap().as_ref(),
            Some(runtime.state())
        );
        assert_eq!(runtime.state().outbox, queued.outbox);
        database.close().await.unwrap();
        let reopened = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&reopened);
        store.define_schema().await.unwrap();
        let recovered = store.load(key).await.unwrap().unwrap();
        assert_eq!(recovered.outbox, queued.outbox);
        let subscriptions = recovered.subscriptions.unwrap();
        assert_eq!(subscriptions.cancelled.get("window"), Some(&1));
        assert!(subscriptions.authorization_scope.is_some());
        assert!(subscriptions.slots.is_empty());
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn scope_barrier_survives_disk_reopen_and_ordinary_journal_saves() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let state = empty_state("partition", "client");
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let barrier = ScopeBarrier {
            reason: "pending_quarantine_required".into(),
            required_scope: "all".into(),
            required_subscription_revision: "1".into(),
            required_authorization_revision: "2".into(),
        };
        store
            .save_scope_barrier(key, barrier.clone())
            .await
            .unwrap();
        let mut weaker = barrier.clone();
        weaker.reason = "needs_new_scope_journal".into();
        store.save_scope_barrier(key, weaker).await.unwrap();
        let queued = enqueue(
            state.clone(),
            "barrier-preserved",
            "person:local",
            json!("Ada"),
        );
        store.save_atomic(Some(0), &queued).await.unwrap();
        database.close().await.unwrap();
        drop(database);
        let reopened = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&reopened);
        store.define_schema().await.unwrap();
        assert_eq!(
            store.load_scope_barrier(key).await.unwrap().unwrap().reason,
            barrier.reason
        );
        assert_eq!(store.load(key).await.unwrap().unwrap(), queued);
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn surrealkv_reopen_discards_uncommitted_state_and_rebuilds_optimistic_state() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();

        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let queued = enqueue(initial, "commit-1", "person:local", json!("Ada"));
        store.save_atomic(Some(0), &queued).await.unwrap();
        let uncommitted = enqueue(
            queued.clone(),
            "commit-2",
            "person:uncommitted",
            json!("Grace"),
        );
        let key = SyncStateKey::new(&queued.partition_id, &queued.client_id);

        let client = database.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        let rows = normalize_state(&uncommitted).unwrap();
        replace_v2_in_transaction(&transaction, key, Some(1), None, &uncommitted, &rows)
            .await
            .unwrap();
        drop(transaction);
        database.close().await.unwrap();
        drop(database);

        let reopened = surrealkv_database(directory.path()).await;
        let reopened_store = SyncStateStore::new(&reopened);
        let loaded = reopened_store.load(key).await.unwrap().unwrap();
        assert_eq!(loaded, queued);
        let runtime = ClientRuntime::open(loaded).unwrap();
        assert_eq!(runtime.pending_commits().count(), 1);
        assert!(
            !runtime
                .optimistic()
                .contains_key(&RecordId("person:uncommitted".into()))
        );
    }

    #[tokio::test]
    async fn optimistic_record_and_state_commit_together_and_survive_reopen() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let queued = enqueue(
            initial.clone(),
            "commit-1",
            "person:ada",
            json!({
                "name": "Ada",
                "friend": {"$surreal": "record", "value": "person:bob"},
                "links": [{"$surreal": "record", "value": "person:grace"}],
                "profile": {"age": {"$surreal": "int", "value": "42"}}
            }),
        );

        assert_eq!(
            store
                .apply_transition_atomic(&initial, &queued)
                .await
                .unwrap(),
            SyncStateWrite::Applied
        );
        assert_eq!(
            store
                .apply_transition_atomic(&queued, &queued)
                .await
                .unwrap(),
            SyncStateWrite::AlreadyCurrent
        );
        let client = database.client().await.unwrap();
        let record: Option<Value> = client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        let Value::Object(record) = record.unwrap() else {
            panic!("expected object record");
        };
        assert_eq!(record.get("name"), Some(&Value::String("Ada".into())));
        assert_eq!(
            record.get("friend"),
            Some(&Value::RecordId(
                NativeRecordId::parse_simple("person:bob").unwrap()
            ))
        );
        drop(client);
        database.close().await.unwrap();
        drop(database);

        let reopened = surrealkv_database(directory.path()).await;
        let reopened_store = SyncStateStore::new(&reopened);
        let key = SyncStateKey::new(&queued.partition_id, &queued.client_id);
        assert_eq!(reopened_store.load(key).await.unwrap(), Some(queued));
        let client = reopened.client().await.unwrap();
        let record: Option<Value> = client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert!(matches!(record, Some(Value::Object(_))));
    }

    #[tokio::test]
    async fn invalid_record_projection_rolls_back_state_and_domain_changes() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        database
            .client()
            .await
            .unwrap()
            .query("DEFINE TABLE person SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let invalid = enqueue(
            initial.clone(),
            "commit-1",
            "person:ada",
            json!("records require object content"),
        );

        assert!(matches!(
            store.apply_transition_atomic(&initial, &invalid).await,
            Err(SyncStateError::RecordValueMustBeObject)
        ));
        let key = SyncStateKey::new(&initial.partition_id, &initial.client_id);
        assert_eq!(store.load(key).await.unwrap(), Some(initial));
        let client = database.client().await.unwrap();
        let record: Option<Value> = client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert_eq!(record, None);
    }

    #[tokio::test]
    async fn optimistic_delete_and_state_commit_together() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let created = enqueue(
            initial.clone(),
            "commit-1",
            "person:ada",
            json!({"name": "Ada"}),
        );
        store
            .apply_transition_atomic(&initial, &created)
            .await
            .unwrap();
        let deleted = enqueue_delete(created.clone(), "commit-2", "person:ada");
        store
            .apply_transition_atomic(&created, &deleted)
            .await
            .unwrap();

        let client = database.client().await.unwrap();
        let record: Option<Value> = client
            .select(NativeRecordId::parse_simple("person:ada").unwrap())
            .await
            .unwrap();
        assert_eq!(record, None);
        let key = SyncStateKey::new(&deleted.partition_id, &deleted.client_id);
        assert_eq!(store.load(key).await.unwrap(), Some(deleted));
    }

    #[tokio::test]
    async fn corrupt_or_oversized_payload_is_never_treated_as_empty_state() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let key = SyncStateKey::new(&initial.partition_id, &initial.client_id);
        let client = database.client().await.unwrap();

        let malformed = StoredMetaRow {
            format_version: FORMAT_VERSION,
            partition_id: initial.partition_id.0.clone(),
            client_id: initial.client_id.0.clone(),
            payload: Vec::from(b"{".as_slice()).into(),
        };
        let _: Option<Value> = client
            .upsert(meta_record_id(key))
            .content(malformed)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::Decode(_))
        ));

        let semantic_corruption = StoredMetaRow {
            format_version: FORMAT_VERSION,
            partition_id: "wrong".into(),
            client_id: initial.client_id.0.clone(),
            payload: normalize_state(&initial).unwrap().meta.into(),
        };
        let _: Option<Value> = client
            .upsert(meta_record_id(key))
            .content(semantic_corruption)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));

        let oversized = StoredMetaRow {
            format_version: FORMAT_VERSION,
            partition_id: initial.partition_id.0.clone(),
            client_id: initial.client_id.0.clone(),
            payload: vec![0; MAX_SYNC_STATE_BYTES + 1].into(),
        };
        let _: Option<Value> = client
            .upsert(meta_record_id(key))
            .content(oversized)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::StateTooLarge)
        ));

        let mut oversized_before_write = initial;
        oversized_before_write.requested_scope =
            ScopeIdentity("x".repeat(MAX_SYNC_STATE_BYTES + 1));
        assert!(matches!(
            store.save_atomic(None, &oversized_before_write).await,
            Err(SyncStateError::StateTooLarge)
        ));
    }

    #[tokio::test]
    async fn normalized_rows_roundtrip_every_component_in_stable_order() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let mut state = empty_state("partition", "client");
        state.confirmed.insert(
            RecordId("recipe:server".into()),
            RecordState::Present {
                value: json!({"servings": 2, "ratio": 1.5}),
                version: 7,
                reference: None,
            },
        );
        state.outbox.push(ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("pending".into()),
                fingerprint: Fingerprint("fingerprint-pending".into()),
            },
            operations: vec![Operation::Delete {
                record_id: RecordId("recipe:old".into()),
                base_version: 3,
            }],
        });
        let resolved = ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("resolved".into()),
                fingerprint: Fingerprint("fingerprint-resolved".into()),
            },
            operations: vec![Operation::Delete {
                record_id: RecordId("recipe:gone".into()),
                base_version: 1,
            }],
        };
        state.outcomes.push(ResolvedCommit {
            local_commit: resolved.clone(),
            outcome: DurableOutcome::Rejected {
                identity: resolved.identity,
                reason: RejectReason::InvalidOperation,
            },
            resolution: None,
            covered_by_reset: false,
        });
        state.id_map.insert(
            RecordId("recipe:temporary-unused".into()),
            RecordId("recipe:server".into()),
        );
        state.checkpoint = Some(Checkpoint {
            token: OpaqueCheckpoint("checkpoint".into()),
            cursor: Cursor {
                epoch: 4,
                sequence: 9,
            },
            scope: ScopeSnapshot {
                identity: ScopeIdentity("all".into()),
                authorization_revision: 8,
                subscription_revision: 1,
            },
        });

        assert_eq!(
            store.save_atomic(None, &state).await.unwrap(),
            SyncStateWrite::Applied
        );
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        assert_eq!(store.load(key).await.unwrap(), Some(state.clone()));
    }

    #[tokio::test]
    async fn aggregate_larger_than_legacy_blob_limit_roundtrips() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let mut state = empty_state("partition", "large-client");
        for index in 0..7 {
            state.confirmed.insert(
                RecordId(format!("recipe:{index}")),
                RecordState::Present {
                    value: json!({"content": "x".repeat(700_000)}),
                    version: index + 1,
                    reference: None,
                },
            );
        }
        assert!(serde_json::to_vec(&state).unwrap().len() > MAX_SYNC_STATE_BYTES);
        store.save_atomic(None, &state).await.unwrap();
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        assert_eq!(store.load(key).await.unwrap(), Some(state));
    }

    #[tokio::test]
    async fn v1_migration_is_exact_idempotent_and_poisoned_for_downgrade() {
        let directory = temp_dir::TempDir::new().unwrap();
        let database = surrealkv_database(directory.path()).await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let state = enqueue(
            empty_state("partition", "legacy-client"),
            "commit-1",
            "recipe:local",
            json!({"name": "redacted"}),
        );
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let legacy = StoredRecord {
            format_version: LEGACY_FORMAT_VERSION,
            partition_id: state.partition_id.0.clone(),
            client_id: state.client_id.0.clone(),
            revision: state.revision.to_string(),
            payload: serde_json::to_vec(&state).unwrap().into(),
        };
        let client = database.client().await.unwrap();
        let _: Option<Value> = client.upsert(record_id(key)).content(legacy).await.unwrap();

        assert_eq!(store.load(key).await.unwrap(), Some(state.clone()));
        assert_eq!(store.load(key).await.unwrap(), Some(state.clone()));
        let sentinel: StoredRecord = client.select(record_id(key)).await.unwrap().unwrap();
        assert_eq!(sentinel.format_version, FORMAT_VERSION);
        assert_eq!(sentinel.payload.as_ref(), MIGRATED_SENTINEL);
        assert!(matches!(
            decode_record(key, sentinel),
            Err(SyncStateError::CorruptState)
        ));
        drop(client);
        database.close().await.unwrap();
        drop(database);

        let reopened = surrealkv_database(directory.path()).await;
        let reopened_store = SyncStateStore::new(&reopened);
        assert_eq!(reopened_store.load(key).await.unwrap(), Some(state));
    }

    #[tokio::test]
    async fn legacy_blob_and_normalized_metadata_default_to_unknown_then_persist_reset_coverage() {
        let db = memory_database().await;
        let store = SyncStateStore::new(&db);
        store.define_schema().await.unwrap();
        let mut state = enqueue(
            empty_state("partition", "coverage-client"),
            "intent",
            "recipe:local",
            json!({"name":"local"}),
        );
        state.checkpoint = Some(Checkpoint {
            token: OpaqueCheckpoint("private-token".into()),
            cursor: surrealdb_sync_protocol::Cursor {
                epoch: 1,
                sequence: 0,
            },
            scope: ScopeSnapshot {
                identity: ScopeIdentity("all".into()),
                authorization_revision: 1,
                subscription_revision: 1,
            },
        });
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let mut legacy = serde_json::to_value(&state).unwrap();
        legacy.as_object_mut().unwrap().remove("reset_coverage");
        legacy.as_object_mut().unwrap().remove("subscriptions");
        let client = db.client().await.unwrap();
        let _: Option<Value> = client
            .upsert(record_id(key))
            .content(StoredRecord {
                format_version: LEGACY_FORMAT_VERSION,
                partition_id: state.partition_id.0.clone(),
                client_id: state.client_id.0.clone(),
                revision: state.revision.to_string(),
                payload: serde_json::to_vec(&legacy).unwrap().into(),
            })
            .await
            .unwrap();
        let loaded = store.load(key).await.unwrap().unwrap();
        assert_eq!(loaded, state);
        assert!(loaded.reset_coverage.is_none());
        assert!(loaded.subscriptions.is_none());
        assert_eq!(loaded.outbox.len(), 1);
        let mut row: StoredMetaRow = client.select(meta_record_id(key)).await.unwrap().unwrap();
        let mut payload: JsonValue = decode_tagged(&row.payload).unwrap();
        payload.as_object_mut().unwrap().remove("reset_coverage");
        payload.as_object_mut().unwrap().remove("subscriptions");
        payload
            .as_object_mut()
            .unwrap()
            .remove("subscription_record_keys");
        row.payload = encode_tagged(&payload).unwrap().into();
        let _: Option<Value> = client
            .upsert(meta_record_id(key))
            .content(row)
            .await
            .unwrap();
        assert!(
            store
                .load(key)
                .await
                .unwrap()
                .unwrap()
                .reset_coverage
                .is_none()
        );
        state.revision += 1;
        state.reset_coverage = Some(surrealdb_sync_client::ResetCoverage {
            cursor: state.checkpoint.as_ref().unwrap().cursor,
            scope: state.checkpoint.as_ref().unwrap().scope.clone(),
        });
        store
            .save_atomic(Some(state.revision - 1), &state)
            .await
            .unwrap();
        assert_eq!(store.load(key).await.unwrap(), Some(state));
    }

    #[tokio::test]
    async fn missing_normalized_component_fails_closed() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let state = enqueue(
            initial,
            "commit-1",
            "recipe:local",
            json!({"name": "redacted"}),
        );
        store.save_atomic(Some(0), &state).await.unwrap();
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let item = address(OUTBOX_TABLE, "commit-1".into());
        let _: Option<Value> = database
            .client()
            .await
            .unwrap()
            .delete(component_record_id(key, &item))
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));
    }

    #[tokio::test]
    async fn same_revision_different_current_never_mutates_state_or_projection() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let initial = empty_state("partition", "client");
        store.save_atomic(None, &initial).await.unwrap();
        let persisted = enqueue(
            initial.clone(),
            "commit-1",
            "recipe:persisted",
            json!({"name": "persisted"}),
        );
        store
            .apply_transition_atomic(&initial, &persisted)
            .await
            .unwrap();

        let mut supplied_current = persisted.clone();
        supplied_current.requested_scope = ScopeIdentity("different-scope".into());
        let next = enqueue(
            supplied_current.clone(),
            "commit-2",
            "recipe:must-not-exist",
            json!({"name": "must-not-exist"}),
        );
        assert!(matches!(
            store
                .apply_transition_atomic(&supplied_current, &next)
                .await,
            Err(SyncStateError::RevisionConflict)
        ));
        let key = SyncStateKey::new(&persisted.partition_id, &persisted.client_id);
        assert_eq!(store.load(key).await.unwrap(), Some(persisted));
        let projected: Option<Value> = database
            .client()
            .await
            .unwrap()
            .select(NativeRecordId::parse_simple("recipe:must-not-exist").unwrap())
            .await
            .unwrap();
        assert_eq!(projected, None);
    }

    #[tokio::test]
    async fn oversized_component_keys_are_rejected_before_database_ids() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let mut confirmed = empty_state("partition", "confirmed-client");
        confirmed.confirmed.insert(
            RecordId("x".repeat(MAX_COMPONENT_KEY_BYTES + 1)),
            RecordState::Tombstone { version: 1 },
        );
        assert!(matches!(
            store.save_atomic(None, &confirmed).await,
            Err(SyncStateError::IdentityTooLarge)
        ));

        let mut outbox = empty_state("partition", "outbox-client");
        outbox.outbox.push(ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("x".repeat(MAX_COMPONENT_KEY_BYTES + 1)),
                fingerprint: Fingerprint("fingerprint".into()),
            },
            operations: vec![Operation::Delete {
                record_id: RecordId("recipe:one".into()),
                base_version: 1,
            }],
        });
        assert!(matches!(
            store.save_atomic(None, &outbox).await,
            Err(SyncStateError::IdentityTooLarge)
        ));
    }

    #[tokio::test]
    async fn orphan_rows_and_components_without_meta_fail_closed() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let state = empty_state("partition", "client");
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let orphan_address = address(CONFIRMED_TABLE, "recipe:orphan".into());
        let orphan = StoredComponentRow {
            format_version: FORMAT_VERSION,
            partition_id: state.partition_id.0.clone(),
            client_id: state.client_id.0.clone(),
            component_key: orphan_address.key.clone(),
            payload: encode_tagged(&RecordState::<JsonValue>::Tombstone { version: 1 })
                .unwrap()
                .into(),
        };
        let client = database.client().await.unwrap();
        let _: Option<Value> = client
            .upsert(component_record_id(key, &orphan_address))
            .content(orphan)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));

        store.save_atomic(None, &state).await.unwrap_err();
        let _: Option<Value> = client
            .delete(component_record_id(key, &orphan_address))
            .await
            .unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let extra_address = address(ID_MAP_TABLE, "recipe:unused".into());
        let extra = StoredComponentRow {
            format_version: FORMAT_VERSION,
            partition_id: state.partition_id.0.clone(),
            client_id: state.client_id.0.clone(),
            component_key: extra_address.key.clone(),
            payload: encode_tagged(&RecordId("recipe:canonical".into()))
                .unwrap()
                .into(),
        };
        let _: Option<Value> = client
            .upsert(component_record_id(key, &extra_address))
            .content(extra)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));
    }

    #[tokio::test]
    async fn noncanonical_tagged_row_bytes_fail_closed() {
        let database = memory_database().await;
        let store = SyncStateStore::new(&database);
        store.define_schema().await.unwrap();
        let state = empty_state("partition", "client");
        store.save_atomic(None, &state).await.unwrap();
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let client = database.client().await.unwrap();
        let mut row: StoredMetaRow = client.select(meta_record_id(key)).await.unwrap().unwrap();
        let mut noncanonical = b" ".to_vec();
        noncanonical.extend_from_slice(&row.payload);
        row.payload = noncanonical.into();
        let _: Option<Value> = client
            .upsert(meta_record_id(key))
            .content(row)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));
    }

    #[test]
    fn structured_keys_do_not_alias_and_identity_lengths_are_bounded() {
        let partition_a = PartitionId("a".into());
        let client_a = ClientId("b.c".into());
        let partition_b = PartitionId("a.b".into());
        let client_b = ClientId("c".into());
        assert_ne!(
            record_id(SyncStateKey::new(&partition_a, &client_a)),
            record_id(SyncStateKey::new(&partition_b, &client_b))
        );

        let oversized = PartitionId("x".repeat(MAX_IDENTITY_BYTES + 1));
        assert!(matches!(
            validate_key(SyncStateKey::new(&oversized, &client_b)),
            Err(SyncStateError::IdentityTooLarge)
        ));

        for unsupported in [
            RecordId("missing_table_separator".into()),
            RecordId(format!("{STATE_TABLE}:reserved")),
            RecordId(format!("{META_TABLE}:reserved")),
            RecordId(format!("{CONFIRMED_TABLE}:reserved")),
            RecordId(format!("{OUTBOX_TABLE}:reserved")),
            RecordId(format!("{OUTCOME_TABLE}:reserved")),
            RecordId(format!("{ID_MAP_TABLE}:reserved")),
            RecordId(format!("person:{}", "x".repeat(MAX_RECORD_ID_BYTES))),
        ] {
            assert!(matches!(
                native_record_id(&unsupported),
                Err(SyncStateError::UnsupportedRecordId)
            ));
        }
    }
}
