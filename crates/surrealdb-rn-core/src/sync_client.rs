//! Experimental UniFFI facade over the durable sync client state machine.
//!
//! The facade performs no networking. Callers provide serialized protocol
//! messages and remain responsible for transport, authentication, and retry.

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use surrealdb_sync_client::{ClientError, ClientRuntime, PreparedTransition};
use surrealdb_sync_protocol::{
    ClientCommit, ClientId, DurableOutcome, PartitionId, PullResponse, PushResponse, ScopeIdentity,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::SurrealDatabase;
use crate::sync_codec::{
    canonical_fingerprint, validate_pull_response_values, validate_push_response_values,
    validate_state_codec,
};
use crate::sync_state::{
    MAX_SYNC_STATE_BYTES, StoredSyncState, SyncStateError, SyncStateKey, SyncStateStore,
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

    #[error("sync client internal conversion failed")]
    Internal,
}

#[derive(uniffi::Object)]
pub struct NativeSyncClient {
    database: Arc<SurrealDatabase>,
    runtime: AsyncMutex<Option<ClientRuntime<JsonValue>>>,
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn open_sync_client(
    database: Arc<SurrealDatabase>,
    options: NativeSyncOpenOptions,
) -> Result<Arc<NativeSyncClient>, NativeSyncError> {
    if !database.embedded {
        return Err(NativeSyncError::RequiresEmbeddedDatabase);
    }

    let partition_id = PartitionId(options.partition_id);
    let client_id = ClientId(options.client_id);
    let requested_scope = ScopeIdentity(options.requested_scope);
    let key = SyncStateKey::new(&partition_id, &client_id);
    let store = SyncStateStore::new(&database);
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
    let runtime = ClientRuntime::open(state).map_err(map_protocol_error)?;
    Ok(Arc::new(NativeSyncClient {
        database,
        runtime: AsyncMutex::new(Some(runtime)),
    }))
}

#[uniffi::export(async_runtime = "tokio")]
impl NativeSyncClient {
    pub async fn enqueue(&self, commit_json: String) -> Result<NativeSyncStatus, NativeSyncError> {
        let mut commit = decode_input::<ClientCommit<JsonValue>>(&commit_json)?;
        let mut slot = self.runtime.lock().await;
        let runtime = slot.as_ref().ok_or(NativeSyncError::Closed)?;
        commit.identity.fingerprint = canonical_fingerprint(runtime.state(), &commit)?;
        let prepared = slot
            .as_ref()
            .ok_or(NativeSyncError::Closed)?
            .prepare_enqueue(commit)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn record_push_response(
        &self,
        response_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let response = decode_input::<PushResponse<JsonValue>>(&response_json)?;
        validate_push_response_values(&response.outcome)?;
        let mut slot = self.runtime.lock().await;
        let prepared = slot
            .as_ref()
            .ok_or(NativeSyncError::Closed)?
            .prepare_push_response(response)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn apply_pull_response(
        &self,
        response_json: String,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let response = decode_input::<PullResponse<JsonValue>>(&response_json)?;
        validate_pull_response_values(&response)?;
        let mut slot = self.runtime.lock().await;
        let prepared = slot
            .as_ref()
            .ok_or(NativeSyncError::Closed)?
            .prepare_pull_response(response)
            .map_err(map_protocol_error)?;
        self.persist_and_install(&mut slot, prepared).await
    }

    pub async fn pending_json(&self) -> Result<Vec<String>, NativeSyncError> {
        let slot = self.runtime.lock().await;
        let runtime = slot.as_ref().ok_or(NativeSyncError::Closed)?;
        runtime
            .pending_commits()
            .map(encode_output)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn conflicts_json(&self) -> Result<Vec<String>, NativeSyncError> {
        let slot = self.runtime.lock().await;
        let runtime = slot.as_ref().ok_or(NativeSyncError::Closed)?;
        runtime
            .state()
            .outcomes
            .iter()
            .filter(|resolved| matches!(&resolved.outcome, DurableOutcome::Conflict { .. }))
            .map(encode_output)
            .collect::<Result<Vec<_>, _>>()
    }

    /// Returns the token from the last completely and atomically applied pull.
    pub async fn checkpoint_token(&self) -> Result<Option<String>, NativeSyncError> {
        let slot = self.runtime.lock().await;
        let runtime = slot.as_ref().ok_or(NativeSyncError::Closed)?;
        Ok(runtime
            .state()
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.token.0.clone()))
    }

    pub async fn status(&self) -> Result<NativeSyncStatus, NativeSyncError> {
        let slot = self.runtime.lock().await;
        status(slot.as_ref().ok_or(NativeSyncError::Closed)?)
    }

    pub async fn close(&self) {
        self.runtime.lock().await.take();
    }

    pub fn is_closed(&self) -> bool {
        self.runtime.try_lock().is_ok_and(|slot| slot.is_none())
    }
}

impl NativeSyncClient {
    async fn persist_and_install(
        &self,
        slot: &mut Option<ClientRuntime<JsonValue>>,
        prepared: PreparedTransition<JsonValue>,
    ) -> Result<NativeSyncStatus, NativeSyncError> {
        let current = slot
            .as_ref()
            .ok_or(NativeSyncError::Closed)?
            .state()
            .clone();
        let next = prepared.state().clone();
        validate_state_codec(&next)?;
        let persisted = SyncStateStore::new(&self.database)
            .apply_transition_atomic(&current, &next)
            .await;
        if matches!(&persisted, Err(SyncStateError::CommitUnknown(_))) {
            slot.take();
            return Err(NativeSyncError::RecoveryRequired);
        }
        persisted.map_err(map_persistence_error)?;
        let runtime = slot.as_mut().ok_or(NativeSyncError::Closed)?;
        runtime.install(prepared).map_err(map_protocol_error)?;
        status(runtime)
    }
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
            .filter(|resolved| matches!(&resolved.outcome, DurableOutcome::Conflict { .. }))
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

fn map_protocol_error(_: ClientError) -> NativeSyncError {
    NativeSyncError::Protocol
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
}
