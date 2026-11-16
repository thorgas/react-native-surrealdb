//! Regression for atomic aggregate admission and durable offline successors on embedded 3.2.4.
use super::*;
use crate::{ConnectOptions, connect};
use serde_json::json;
use surrealdb_sync_protocol::*;

fn options() -> NativeSyncOpenOptions {
    NativeSyncOpenOptions {
        partition_id: "partition".into(),
        client_id: "client".into(),
        requested_scope: "all".into(),
        subscription_revision: 1,
    }
}
async fn database(endpoint: &str) -> Arc<SurrealDatabase> {
    connect(ConnectOptions {
        endpoint: endpoint.into(),
        namespace: Some("aggregate_queue".into()),
        database: Some("aggregate_queue".into()),
    })
    .await
    .unwrap()
}
fn context() -> ReceiptContext {
    ReceiptContext {
        authority_incarnation: "a".into(),
        dataset_incarnation: "d".into(),
        partition_id: PartitionId("partition".into()),
        client_id: ClientId("client".into()),
        scope: ScopeSnapshot {
            identity: ScopeIdentity("all".into()),
            authorization_revision: 1,
            subscription_revision: 1,
        },
    }
}
fn commit(id: &str, base: BaseVersion, delete: bool) -> ClientCommit<JsonValue> {
    ClientCommit {
        identity: CommitIdentity {
            client_commit_id: ClientCommitId(id.into()),
            fingerprint: Fingerprint("computed-by-native".into()),
        },
        operations: vec![
            Operation::Upsert {
                record_id: RecordId("plan:one".into()),
                base_version: base,
                value: json!({"name":"parent"}),
                reference: None,
            },
            if delete {
                Operation::Delete {
                    record_id: RecordId("entry:one".into()),
                    base_version: 0,
                }
            } else {
                Operation::Upsert {
                    record_id: RecordId("entry:one".into()),
                    base_version: base,
                    value: json!({"name":id}),
                    reference: Some(RecordId("plan:one".into())),
                }
            },
        ],
    }
}
fn ack(c: &ClientCommit<JsonValue>, sequence: u64) -> String {
    serde_json::to_string(&PushResponseV2::<JsonValue> {
        response: PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: context().partition_id,
            client_id: context().client_id,
            outcome: DurableOutcome::Accepted {
                identity: c.identity.clone(),
                sequence,
                id_mappings: vec![],
            },
        },
        receipt: Some(AcceptanceReceipt {
            context: context(),
            identity: c.identity.clone(),
            cursor: Cursor { epoch: 1, sequence },
        }),
    })
    .unwrap()
}
fn pull(sequence: u64, version: u64) -> String {
    serde_json::to_string(&PullResponseV2 {
        context: context(),
        response: PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::CheckpointExpired,
            checkpoint: Checkpoint {
                token: OpaqueCheckpoint(format!("cp-{sequence}")),
                cursor: Cursor { epoch: 1, sequence },
                scope: context().scope,
            },
            records: vec![
                SnapshotRecord {
                    record_id: RecordId("plan:one".into()),
                    state: RecordState::Present {
                        version,
                        value: json!({"name":"parent"}),
                        reference: None,
                    },
                },
                SnapshotRecord {
                    record_id: RecordId("entry:one".into()),
                    state: RecordState::Present {
                        version,
                        value: json!({"name":"entry"}),
                        reference: Some(RecordId("plan:one".into())),
                    },
                },
            ],
        }),
    })
    .unwrap()
}
async fn state(client: &NativeSyncClient) -> StoredSyncState {
    client
        .lock_runtime()
        .await
        .client()
        .unwrap()
        .state()
        .clone()
}
async fn restart(
    db: Arc<SurrealDatabase>,
    client: Arc<NativeSyncClient>,
    endpoint: &str,
) -> (Arc<SurrealDatabase>, Arc<NativeSyncClient>) {
    client.close().await;
    db.close().await.unwrap();
    let db = database(endpoint).await;
    let client = open_existing_sync_client(db.clone(), options())
        .await
        .unwrap();
    (db, client)
}
#[tokio::test]
async fn aggregate_native_fingerprints_and_offline_successors_persist_across_physical_reopens() {
    for pull_first in [false, true] {
        let dir = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", dir.path().display());
        let mut db = database(&endpoint).await;
        let mut client = open_sync_client(db.clone(), options()).await.unwrap();
        client
            .clone()
            .enqueue_latest_bounded(
                serde_json::to_string(&commit("add", BaseVersion::Absent, false)).unwrap(),
                64,
                4 * 1024 * 1024,
            )
            .await
            .unwrap();
        let add = state(&client).await.outbox[0].clone();
        assert!(add.identity.fingerprint.0.starts_with("sha256:"));
        client
            .clone()
            .mark_transmitted(serde_json::to_string(&add).unwrap())
            .await
            .unwrap();
        for (id, delete) in [("move", false), ("remove", true)] {
            client
                .clone()
                .enqueue_latest_bounded(
                    serde_json::to_string(&commit(id, BaseVersion::Exact(0), delete)).unwrap(),
                    64,
                    4 * 1024 * 1024,
                )
                .await
                .unwrap();
            (db, client) = restart(db, client, &endpoint).await;
        }
        let before = state(&client).await;
        assert_eq!(before.outbox, vec![add.clone()]);
        assert_eq!(before.command_queue.deferred.len(), 2);
        assert!(matches!(
            surrealdb_sync_client::optimistic_projection(&before)[&RecordId("entry:one".into())],
            surrealdb_sync_client::OptimisticRecord::Deleted { .. }
        ));
        if pull_first {
            client
                .clone()
                .apply_pull_response(pull(1, 1))
                .await
                .unwrap();
        } else {
            client
                .clone()
                .record_push_response(ack(&add, 1))
                .await
                .unwrap();
        }
        (db, client) = restart(db, client, &endpoint).await;
        assert_eq!(state(&client).await.command_queue.deferred.len(), 2);
        if pull_first {
            client
                .clone()
                .record_push_response(ack(&add, 1))
                .await
                .unwrap();
        } else {
            client
                .clone()
                .apply_pull_response(pull(1, 1))
                .await
                .unwrap();
        }
        (db, client) = restart(db, client, &endpoint).await;
        let moved = state(&client).await.outbox[0].clone();
        assert_eq!(moved.identity.client_commit_id.0, "move");
        assert!(moved.operations.iter().all(|op| matches!(
            op,
            Operation::Upsert {
                base_version: BaseVersion::Exact(1),
                ..
            }
        )));
        assert_eq!(
            canonical_fingerprint(&state(&client).await, &moved).unwrap(),
            moved.identity.fingerprint
        );
        client
            .clone()
            .record_push_response(ack(&moved, 2))
            .await
            .unwrap();
        assert!(state(&client).await.outbox.is_empty());
        client
            .clone()
            .apply_pull_response(pull(2, 2))
            .await
            .unwrap();
        (db, client) = restart(db, client, &endpoint).await;
        let after = state(&client).await;
        let removed = &after.outbox[0];
        assert_eq!(removed.identity.client_commit_id.0, "remove");
        assert!(matches!(
            removed.operations[0],
            Operation::Upsert {
                base_version: BaseVersion::Exact(2),
                ..
            }
        ));
        assert!(matches!(
            removed.operations[1],
            Operation::Delete {
                base_version: 2,
                ..
            }
        ));
        assert_eq!(
            canonical_fingerprint(&after, removed).unwrap(),
            removed.identity.fingerprint
        );
        assert!(after.command_queue.deferred.is_empty());
        client.close().await;
        db.close().await.unwrap();
    }
}
#[tokio::test]
async fn aggregate_capacity_refusal_leaves_native_store_and_queue_unchanged() {
    let db = database("mem://").await;
    let client = open_sync_client(db.clone(), options()).await.unwrap();
    client
        .clone()
        .enqueue_latest_bounded(
            serde_json::to_string(&commit("add", BaseVersion::Absent, false)).unwrap(),
            1,
            65536,
        )
        .await
        .unwrap();
    let before = state(&client).await;
    assert!(matches!(
        client
            .clone()
            .enqueue_latest_bounded(
                serde_json::to_string(&commit("move", BaseVersion::Exact(0), false)).unwrap(),
                1,
                65536
            )
            .await,
        Err(NativeSyncError::QueueCapacity)
    ));
    assert_eq!(state(&client).await, before);
    client.close().await;
    let reopened = open_existing_sync_client(db.clone(), options())
        .await
        .unwrap();
    assert_eq!(state(&reopened).await, before);
    reopened.close().await;
    db.close().await.unwrap();
}
