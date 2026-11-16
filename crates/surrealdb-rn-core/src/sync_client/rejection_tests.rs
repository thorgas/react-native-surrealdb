//! The fixture uses the old journal shape (Rejected + no resolution) and real SurrealKV.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

async fn fixture(endpoint: &str) -> (Arc<SurrealDatabase>, Arc<NativeSyncClient>) {
    let db = database(endpoint).await;
    let client = open_sync_client(db.clone(), options()).await.unwrap();
    client
        .clone()
        .apply_pull_response(pull(1, 7))
        .await
        .unwrap();
    for name in ["refused", "next", "last"] {
        client
            .clone()
            .enqueue_latest_bounded(
                serde_json::to_string(&commit(name, BaseVersion::Exact(7), false)).unwrap(),
                64,
                4 * 1024 * 1024,
            )
            .await
            .unwrap();
        if name == "refused" {
            let first = state(&client).await.outbox[0].clone();
            client
                .clone()
                .mark_transmitted(serde_json::to_string(&first).unwrap())
                .await
                .unwrap();
        }
    }
    let first = state(&client).await.outbox[0].clone();
    let response = PushResponse::<JsonValue> {
        schema_version: SchemaVersion::V1,
        partition_id: context().partition_id,
        client_id: context().client_id,
        outcome: DurableOutcome::Rejected {
            identity: first.identity,
            reason: RejectReason::Unauthorized,
        },
    };
    client
        .clone()
        .record_push_response(serde_json::to_string(&response).unwrap())
        .await
        .unwrap();
    (db, client)
}

#[tokio::test]
async fn rejection_upgrade_metadata_pagination_and_duplicate_resolution_survive_reopen() {
    let dir = temp_dir::TempDir::new().unwrap();
    let endpoint = format!("surrealkv://{}", dir.path().display());
    let (db, client) = fixture(&endpoint).await;
    let (db, client) = restart(db, client, &endpoint).await;
    let entries = client.rejected_entries(None, 1).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].commit_id, "refused");
    assert_eq!(entries[0].rejection_code, NativeRejectionCode::Unauthorized);
    assert_eq!(
        entries[0].records,
        vec![
            NativeRejectedRecord {
                record_id: "plan:one".into(),
                operation: NativeRejectedOperation::Upsert
            },
            NativeRejectedRecord {
                record_id: "entry:one".into(),
                operation: NativeRejectedOperation::Upsert
            },
        ]
    );
    for limit in [0, 65] {
        assert!(matches!(
            client.rejected_entries(None, limit).await,
            Err(NativeSyncError::InvalidInput)
        ));
    }
    assert!(matches!(
        client.rejected_entries(Some("unknown".into()), 1).await,
        Err(NativeSyncError::InvalidInput)
    ));
    assert!(
        client
            .rejected_entries(Some("refused".into()), 1)
            .await
            .unwrap()
            .is_empty()
    );
    let read_v1 = client.read_snapshot_json().await.unwrap();
    let read_v2 = client.read_snapshot_v2_json().await.unwrap();
    assert!(read_v1.contains("\"protocolVersion\":1"));
    assert!(read_v2.contains("\"protocolVersion\":2"));
    client
        .clone()
        .resolve_rejected_keep_server("refused".into())
        .await
        .unwrap();
    let (db, client) = restart(db, client, &endpoint).await;
    let before = state(&client).await;
    assert_eq!(before.outbox[0].identity.client_commit_id.0, "next");
    assert_eq!(
        before.command_queue.deferred[0].identity.client_commit_id.0,
        "last"
    );
    client
        .clone()
        .resolve_rejected_keep_server("refused".into())
        .await
        .unwrap();
    assert_eq!(state(&client).await, before);
    assert!(client.rejected_entries(None, 64).await.unwrap().is_empty());
    assert!(
        client
            .rejected_entries(Some("refused".into()), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .clone()
            .resolve_rejected_keep_server("next".into())
            .await
            .is_err()
    );
    client.close().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn rejection_resolution_races_pull_and_cancelled_waiter_without_partial_projection() {
    for after_dispatch in [false, true] {
        let dir = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", dir.path().display());
        let (db, client) = fixture(&endpoint).await;
        let arrived = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *client.persistence_pause.lock().unwrap() = Some(crate::sync_state::PersistencePause {
            after_dispatch,
            arrived: arrived.clone(),
            release: release.clone(),
            first_poll_pending: Arc::new(AtomicBool::new(false)),
        });
        let waiter = tokio::spawn(
            client
                .clone()
                .resolve_rejected_keep_server("refused".into()),
        );
        arrived.notified().await;
        waiter.abort(); // native worker owns settlement even after JS drops the promise
        let pulling = tokio::spawn(client.clone().apply_pull_response(pull(2, 9)));
        release.notify_one();
        pulling.await.unwrap().unwrap();
        let (db, client) = restart(db, client, &endpoint).await;
        let recovered = state(&client).await;
        assert!(recovered.outcomes[0].resolution.is_some());
        assert_eq!(recovered.outbox.len(), 1);
        assert_eq!(recovered.command_queue.deferred.len(), 1);
        for op in &recovered.outbox[0].operations {
            assert!(matches!(
                op,
                Operation::Upsert {
                    base_version: BaseVersion::Exact(7),
                    ..
                }
            ));
        }
        for row in recovered.confirmed.values() {
            assert!(matches!(row, RecordState::Present { version: 9, .. }));
        }
        assert!(client.rejected_entries(None, 64).await.unwrap().is_empty());
        client.close().await;
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rejection_resolution_abrupt_exit_at_every_transition_boundary_is_atomic() {
    const ENDPOINT: &str = "SDK_REJECTION_CRASH_ENDPOINT";
    const PHASE: &str = "SDK_REJECTION_CRASH_PHASE";
    if let Ok(endpoint) = std::env::var(ENDPOINT) {
        let (_db, client) = fixture(&endpoint).await;
        std::fs::write(
            std::env::var("SDK_REJECTION_EXPECTED").unwrap(),
            serde_json::to_vec(&state(&client).await).unwrap(),
        )
        .unwrap();
        let phase = std::env::var(PHASE).unwrap();
        if phase == "closed" || phase == "promoted" {
            let slot = client.lock_runtime().await;
            let r = slot.client().unwrap();
            let t = r
                .prepare_resolve_rejected_keep_server(&ClientCommitId("refused".into()))
                .unwrap();
            if phase == "promoted" {
                r.prepare_with_successors(t, queue_fingerprint).unwrap();
            }
        } else if phase == "dispatched" {
            let arrived = Arc::new(tokio::sync::Notify::new());
            *client.persistence_pause.lock().unwrap() = Some(crate::sync_state::PersistencePause {
                after_dispatch: true,
                arrived: arrived.clone(),
                release: Arc::new(tokio::sync::Notify::new()),
                first_poll_pending: Arc::new(AtomicBool::new(false)),
            });
            tokio::spawn(
                client
                    .clone()
                    .resolve_rejected_keep_server("refused".into()),
            );
            arrived.notified().await;
        } else if phase == "persisted" {
            client.lose_next_persist_ack.store(true, Ordering::SeqCst);
            assert!(matches!(
                client
                    .clone()
                    .resolve_rejected_keep_server("refused".into())
                    .await,
                Err(NativeSyncError::RecoveryRequired)
            ));
        } else if phase == "installed" {
            client
                .clone()
                .resolve_rejected_keep_server("refused".into())
                .await
                .unwrap();
        } else {
            assert_eq!(phase, "before");
        }
        // No destructor, database close, runtime install, or ACK is assumed to run after this.
        std::process::exit(23);
    }
    for phase in [
        "before",
        "closed",
        "promoted",
        "dispatched",
        "persisted",
        "installed",
    ] {
        let dir = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", dir.path().join("db").display());
        let expected = dir.path().join("expected.json");
        let result = std::process::Command::new(std::env::current_exe().unwrap()).args([
            "--exact", "sync_client::aggregate_tests::rejection_tests::rejection_resolution_abrupt_exit_at_every_transition_boundary_is_atomic", "--nocapture",
        ]).env(ENDPOINT, &endpoint).env(PHASE, phase).env("SDK_REJECTION_EXPECTED", &expected).output().unwrap();
        assert_eq!(
            result.status.code(),
            Some(23),
            "phase {phase}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let original: StoredSyncState =
            serde_json::from_slice(&std::fs::read(&expected).unwrap()).unwrap();
        let db = database(&endpoint).await;
        let client = open_existing_sync_client(db.clone(), options())
            .await
            .unwrap();
        let recovered = state(&client).await;
        // Verify the application projection, not only the queue's serialized metadata.
        let native = db.client().await.unwrap();
        let row: Option<surrealdb::types::Value> = native
            .select(surrealdb::types::RecordId::parse_simple("entry:one").unwrap())
            .await
            .unwrap();
        let Some(surrealdb::types::Value::Object(row)) = row else {
            panic!("projection missing")
        };
        assert!(
            matches!(row.get("name"), Some(surrealdb::types::Value::String(name)) if name.as_str() == "last")
        );
        drop(native);
        let resolved = recovered.outcomes[0].resolution.is_some();
        if ["before", "closed", "promoted"].contains(&phase) {
            assert!(!resolved);
        }
        if ["persisted", "installed"].contains(&phase) {
            assert!(resolved);
        }
        assert_eq!(recovered.outcomes[0].outcome, original.outcomes[0].outcome);
        assert_eq!(
            recovered.outcomes[0].local_commit,
            original.outcomes[0].local_commit
        );
        assert_eq!(recovered.outbox.len(), usize::from(resolved));
        assert_eq!(
            recovered.command_queue.deferred.len(),
            if resolved { 1 } else { 2 }
        );
        if resolved {
            assert_eq!(recovered.outbox[0].identity.client_commit_id.0, "next");
            assert_eq!(
                recovered.command_queue.deferred[0],
                original.command_queue.deferred[1]
            );
        } else {
            assert_eq!(recovered, original);
        }
        // Both an old and a committed recovery are safe for the same idempotent app action.
        client
            .clone()
            .resolve_rejected_keep_server("refused".into())
            .await
            .unwrap();
        assert_eq!(
            state(&client).await.outbox[0].identity.client_commit_id.0,
            "next"
        );
        client.close().await;
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rejection_without_successors_materializes_authority_for_both_wire_versions() {
    for v2 in [false, true] {
        let dir = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", dir.path().display());
        let db = database(&endpoint).await;
        let client = open_sync_client(db.clone(), options()).await.unwrap();
        client
            .clone()
            .apply_pull_response(pull(1, 7))
            .await
            .unwrap();
        client
            .clone()
            .enqueue_latest_bounded(
                serde_json::to_string(&commit("denied-change", BaseVersion::Exact(7), false))
                    .unwrap(),
                64,
                4 * 1024 * 1024,
            )
            .await
            .unwrap();
        let pending = state(&client).await.outbox[0].clone();
        let response = PushResponse::<JsonValue> {
            schema_version: SchemaVersion::V1,
            partition_id: context().partition_id,
            client_id: context().client_id,
            outcome: DurableOutcome::Rejected {
                identity: pending.identity,
                reason: RejectReason::InvalidOperation,
            },
        };
        let encoded = if v2 {
            serde_json::to_string(&PushResponseV2 {
                response,
                receipt: None,
            })
            .unwrap()
        } else {
            serde_json::to_string(&response).unwrap()
        };
        client.clone().record_push_response(encoded).await.unwrap();
        client
            .clone()
            .resolve_rejected_keep_server("denied-change".into())
            .await
            .unwrap();
        let (db, client) = restart(db, client, &endpoint).await;
        let native = db.client().await.unwrap();
        let row: Option<surrealdb::types::Value> = native
            .select(surrealdb::types::RecordId::parse_simple("entry:one").unwrap())
            .await
            .unwrap();
        let Some(surrealdb::types::Value::Object(row)) = row else {
            panic!("authority projection missing")
        };
        assert!(
            matches!(row.get("name"), Some(surrealdb::types::Value::String(name)) if name.as_str() == "entry")
        );
        assert!(client.rejected_entries(None, 64).await.unwrap().is_empty());
        drop(native);
        client.close().await;
        db.close().await.unwrap();
    }
}
