//! Transactional persistence for the experimental sync client state.
//!
//! This module is Rust-only. It deliberately exposes no UniFFI or TypeScript API
//! until the durable lifecycle has been proven on mobile.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::types::{Array, Bytes, RecordId as NativeRecordId, SurrealValue, Value};
use surrealdb_sync_client::{ClientError, ClientRuntime, DurableClientState, OptimisticRecord};
use surrealdb_sync_protocol::{ClientId, PartitionId, RecordId as ProtocolRecordId};

use crate::wire::from_wire_value;
use crate::{SurrealDatabase, SurrealRnError};

const FORMAT_VERSION: i64 = 1;
const STATE_TABLE: &str = "_sync_client_state";
const MAX_IDENTITY_BYTES: usize = 512;
const MAX_RECORD_ID_BYTES: usize = 1024;
pub const MAX_SYNC_STATE_BYTES: usize = 4 * 1024 * 1024;

const DEFINE_SCHEMA: &str = r#"
DEFINE TABLE IF NOT EXISTS _sync_client_state SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS format_version ON _sync_client_state TYPE int
    ASSERT $value = 1;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS revision ON _sync_client_state TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_client_state TYPE bytes;
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

pub struct SyncStateStore<'a> {
    database: &'a SurrealDatabase,
}

impl<'a> SyncStateStore<'a> {
    pub fn new(database: &'a SurrealDatabase) -> Self {
        Self { database }
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

    pub async fn load(
        &self,
        key: SyncStateKey<'_>,
    ) -> Result<Option<StoredSyncState>, SyncStateError> {
        validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let stored: Option<StoredRecord> = client
            .select(record_id(key))
            .await
            .map_err(SyncStateError::Database)?;
        stored.map(|record| decode_record(key, record)).transpose()
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
        let (key, payload) = prepare_state_write(expected_revision, state)?;

        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = replace_in_transaction(
            &transaction,
            key,
            expected_revision,
            state.revision,
            payload,
        )
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

        let (key, payload) = prepare_state_write(Some(current.revision), next)?;
        let current_projection = ClientRuntime::open(current.clone())
            .map_err(SyncStateError::InvalidState)?
            .optimistic();
        let next_projection = ClientRuntime::open(next.clone())
            .map_err(SyncStateError::InvalidState)?
            .optimistic();

        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            let disposition = replace_in_transaction(
                &transaction,
                key,
                Some(current.revision),
                next.revision,
                payload,
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
}

async fn finish_transaction(
    transaction: Transaction<Any>,
    operation: Result<SyncStateWrite, SyncStateError>,
) -> Result<SyncStateWrite, SyncStateError> {
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
) -> Result<(SyncStateKey<'a>, Vec<u8>), SyncStateError> {
    let key = SyncStateKey::new(&state.partition_id, &state.client_id);
    validate_key(key)?;
    validate_next_revision(expected_revision, state.revision)?;
    state.validate().map_err(SyncStateError::InvalidState)?;
    let payload = serde_json::to_vec(state).map_err(SyncStateError::Encode)?;
    if payload.len() > MAX_SYNC_STATE_BYTES {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok((key, payload))
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

async fn replace_in_transaction(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    expected_revision: Option<u64>,
    revision: u64,
    payload: Vec<u8>,
) -> Result<SyncStateWrite, SyncStateError> {
    let id = record_id(key);
    let current: Option<StoredRecord> = transaction
        .select(id.clone())
        .await
        .map_err(SyncStateError::Database)?;

    if let Some(current) = current {
        validate_stored_envelope(key, &current)?;
        let current_revision = parse_revision(&current.revision)?;
        if current_revision == revision {
            if current.payload.as_ref() == payload.as_slice() {
                return Ok(SyncStateWrite::AlreadyCurrent);
            }
            return Err(SyncStateError::RevisionConflict);
        }
        if Some(current_revision) != expected_revision {
            return Err(SyncStateError::RevisionConflict);
        }
    } else if expected_revision.is_some() {
        return Err(SyncStateError::RevisionConflict);
    }

    let stored = StoredRecord {
        format_version: FORMAT_VERSION,
        partition_id: key.partition_id.0.clone(),
        client_id: key.client_id.0.clone(),
        revision: revision.to_string(),
        payload: payload.into(),
    };
    let _: Option<Value> = transaction
        .upsert(id)
        .content(stored)
        .await
        .map_err(SyncStateError::Database)?;
    Ok(SyncStateWrite::Applied)
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

fn validate_key(key: SyncStateKey<'_>) -> Result<(), SyncStateError> {
    if key.partition_id.0.len() > MAX_IDENTITY_BYTES || key.client_id.0.len() > MAX_IDENTITY_BYTES {
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
    if stored.format_version != FORMAT_VERSION
        || stored.partition_id != key.partition_id.0
        || stored.client_id != key.client_id.0
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}

fn parse_revision(revision: &str) -> Result<u64, SyncStateError> {
    revision.parse().map_err(|_| SyncStateError::CorruptState)
}

fn record_id(key: SyncStateKey<'_>) -> NativeRecordId {
    NativeRecordId::new(
        STATE_TABLE,
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}

fn native_record_id(record_id: &ProtocolRecordId) -> Result<NativeRecordId, SyncStateError> {
    if record_id.0.len() > MAX_RECORD_ID_BYTES {
        return Err(SyncStateError::UnsupportedRecordId);
    }
    let record_id = NativeRecordId::parse_simple(&record_id.0)
        .map_err(|_| SyncStateError::UnsupportedRecordId)?;
    if record_id.table.as_str() == STATE_TABLE {
        return Err(SyncStateError::UnsupportedRecordId);
    }
    Ok(record_id)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use surrealdb_sync_client::{ClientRuntime, OptimisticRecord};
    use surrealdb_sync_protocol::{
        BaseVersion, ClientCommit, ClientCommitId, CommitIdentity, Fingerprint, Operation,
        RecordId, ScopeIdentity,
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
        let payload = serde_json::to_vec(&queued).unwrap();
        let key = SyncStateKey::new(&queued.partition_id, &queued.client_id);

        let client = database.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        assert_eq!(
            replace_in_transaction(&transaction, key, Some(0), 1, payload.clone())
                .await
                .unwrap(),
            SyncStateWrite::Applied
        );
        let client = transaction.cancel().await.unwrap();
        assert_eq!(store.load(key).await.unwrap(), Some(initial));

        let transaction = client.begin().await.unwrap();
        replace_in_transaction(&transaction, key, Some(0), 1, payload)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(store.load(key).await.unwrap(), Some(queued));
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
        replace_in_transaction(
            &transaction,
            key,
            Some(1),
            2,
            serde_json::to_vec(&uncommitted).unwrap(),
        )
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

        let malformed = StoredRecord {
            format_version: FORMAT_VERSION,
            partition_id: initial.partition_id.0.clone(),
            client_id: initial.client_id.0.clone(),
            revision: "0".into(),
            payload: Vec::from(b"{".as_slice()).into(),
        };
        let _: Option<Value> = client
            .upsert(record_id(key))
            .content(malformed)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::Decode(_))
        ));

        let mut mismatched = initial.clone();
        mismatched.revision = 1;
        let semantic_corruption = StoredRecord {
            format_version: FORMAT_VERSION,
            partition_id: initial.partition_id.0.clone(),
            client_id: initial.client_id.0.clone(),
            revision: "0".into(),
            payload: serde_json::to_vec(&mismatched).unwrap().into(),
        };
        let _: Option<Value> = client
            .upsert(record_id(key))
            .content(semantic_corruption)
            .await
            .unwrap();
        assert!(matches!(
            store.load(key).await,
            Err(SyncStateError::CorruptState)
        ));

        let oversized = StoredRecord {
            format_version: FORMAT_VERSION,
            partition_id: initial.partition_id.0.clone(),
            client_id: initial.client_id.0.clone(),
            revision: "0".into(),
            payload: vec![0; MAX_SYNC_STATE_BYTES + 1].into(),
        };
        let _: Option<Value> = client
            .upsert(record_id(key))
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
            RecordId(format!("person:{}", "x".repeat(MAX_RECORD_ID_BYTES))),
        ] {
            assert!(matches!(
                native_record_id(&unsupported),
                Err(SyncStateError::UnsupportedRecordId)
            ));
        }
    }
}
