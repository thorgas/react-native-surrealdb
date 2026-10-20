use super::*;
use crate::sync_client::{NativeSyncOpenOptions, open_sync_client};
use crate::{ConnectOptions, SurrealDatabase, connect};
use std::collections::BTreeMap;
use surrealdb_sync_protocol::bootstrap::*;
use surrealdb_sync_protocol::bootstrap_codec::*;
use surrealdb_sync_protocol::{
    CanonicalValue, Checkpoint, Cursor, OpaqueCheckpoint, RecordId, RecordState, SnapshotRecord,
};

fn context() -> NativeBootstrapContext {
    NativeBootstrapContext {
        authority_id: "authority".into(),
        principal_id: "owner".into(),
        partition_id: "partition".into(),
        client_id: "account.00000000-0000-0000-0000-000000000001-state-v2".into(),
        journal_generation: "00000000-0000-0000-0000-000000000001".into(),
        requested_scope: "household-state-v2".into(),
        subscription_revision: 1,
        authorization_revision: 7,
    }
}
fn options(context: &NativeBootstrapContext) -> NativeSyncOpenOptions {
    NativeSyncOpenOptions {
        partition_id: context.partition_id.clone(),
        client_id: context.client_id.clone(),
        requested_scope: context.requested_scope.clone(),
        subscription_revision: context.subscription_revision,
    }
}
struct Wire {
    start: Vec<u8>,
    manifest: Vec<u8>,
    page: Vec<u8>,
    receipt: Vec<u8>,
}
fn wire(expected: &NativeBootstrapContext, session: &str, expires_at: u64) -> Wire {
    let context = expected.clone().validated().unwrap();
    let start = BootstrapStartRequest {
        partition_id: context.partition_id.clone(),
        client_id: context.client_id.clone(),
        request_id: session.into(),
        requested_scope: context.scope.identity.clone(),
        subscription_revision: context.scope.subscription_revision,
        journal_generation: context.journal_generation.clone(),
    };
    let mut page = BootstrapPage {
        session_id: session.into(),
        manifest_digest: BootstrapDigest([0; 32]),
        index: 0,
        records: vec![SnapshotRecord {
            record_id: RecordId("person:server".into()),
            state: RecordState::Present {
                value: CanonicalValue::Object(BTreeMap::from([(
                    "name".into(),
                    CanonicalValue::String("server".into()),
                )])),
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
        expires_at,
    };
    page.manifest_digest = bootstrap_manifest_digest(&manifest).unwrap();
    let receipt = BootstrapReceipt {
        session_id: session.into(),
        manifest_digest: page.manifest_digest,
        checkpoint: Checkpoint {
            token: OpaqueCheckpoint("opaque-checkpoint".into()),
            cursor: manifest.cursor,
            scope: context.scope,
        },
        expires_at,
    };
    Wire {
        start: encode_bootstrap_start(&start).unwrap(),
        manifest: encode_bootstrap_manifest(&manifest).unwrap(),
        page: encode_bootstrap_page(&page).unwrap(),
        receipt: encode_bootstrap_receipt(&receipt).unwrap(),
    }
}
async fn database() -> Arc<SurrealDatabase> {
    let db = connect(ConnectOptions {
        endpoint: "mem://".into(),
        namespace: Some("facade".into()),
        database: Some("facade".into()),
    })
    .await
    .unwrap();
    db.query("DEFINE TABLE person SCHEMALESS".into(), None)
        .await
        .unwrap();
    db
}
async fn schema(db: &SurrealDatabase) -> String {
    db.query("INFO FOR DB".into(), None).await.unwrap()[0]
        .value_json
        .clone()
}

#[tokio::test]
async fn exported_factory_binds_independent_context_and_missing_progress_is_read_only() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    assert_eq!(
        client.status().await.unwrap().cursor_epoch,
        None,
        "parent authorization initially unknown"
    );
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "missing", u64::MAX);
    let before = schema(&db).await;
    let progress = session.progress(input.manifest.clone()).await.unwrap();
    assert_eq!(progress.state, NativeBootstrapStageState::Missing);
    assert!(progress.stage.is_none());
    assert!(progress.received_indices.is_none());
    assert_eq!(schema(&db).await, before);
    assert_eq!(client.status().await.unwrap().revision, 0);
    let mut foreign = expected.clone();
    foreign.principal_id = "different".into();
    assert!(matches!(
        bind_bootstrap_session(client.clone(), foreign.clone()).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    let foreign = wire(&foreign, "foreign", u64::MAX);
    assert!(matches!(
        session.clone().begin(foreign.start, foreign.manifest).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    assert!(matches!(
        session
            .clone()
            .begin(vec![0xff], input.manifest.clone())
            .await,
        Err(NativeBootstrapError::InvalidFrame)
    ));
    assert!(matches!(
        session.progress(vec![0; MAX_SYNC_STATE_BYTES + 1]).await,
        Err(NativeBootstrapError::InvalidInput)
    ));
    assert_eq!(schema(&db).await, before);
    let mut oversized = expected.clone();
    oversized.client_id = "x".repeat(513);
    assert!(matches!(
        bind_bootstrap_session(client, oversized).await,
        Err(NativeBootstrapError::InvalidInput)
    ));
    let capability = bootstrap_capabilities();
    assert!(capability.requires_matching_native_abi);
    assert_eq!(capability.native_key_bytes, 512);
    assert_eq!(capability.max_frame_bytes, 4 * 1024 * 1024);
    assert_eq!(capability.completion_deadline_seconds, 30);
    db.close().await.unwrap();
}

#[tokio::test]
async fn facade_stages_opaque_bytes_resumes_new_object_and_publishes_only_complete_root() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let sibling = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "complete", u64::MAX);
    let stage = session
        .clone()
        .begin(input.start.clone(), input.manifest.clone())
        .await
        .unwrap();
    session
        .clone()
        .page(stage.clone(), input.page.clone())
        .await
        .unwrap();
    session
        .clone()
        .page(stage.clone(), input.page.clone())
        .await
        .unwrap();
    assert_eq!(client.status().await.unwrap().revision, 0);
    assert_eq!(
        db.query("RETURN count(SELECT * FROM person)".into(), None)
            .await
            .unwrap()[0]
            .value_json,
        r#"{"$surreal":"int","value":"0"}"#
    );
    let resumed = bind_bootstrap_session(sibling.clone(), expected)
        .await
        .unwrap();
    let inventory = resumed.progress(input.manifest.clone()).await.unwrap();
    assert_eq!(inventory.state, NativeBootstrapStageState::Staging);
    assert_eq!(inventory.received_indices, Some(vec![0]));
    assert!(
        matches!(
            resumed.clone().page(stage.clone(), input.page).await,
            Err(NativeBootstrapError::StageMismatch)
        ),
        "foreign factory stage object accepted"
    );
    let recovered = inventory.stage.unwrap();
    assert_eq!(
        resumed
            .clone()
            .complete(recovered.clone(), input.receipt.clone())
            .await
            .unwrap(),
        NativeBootstrapCompletion::Applied
    );
    assert_eq!(client.status().await.unwrap().cursor_sequence, Some(1));
    assert_eq!(
        db.query(
            "RETURN (SELECT VALUE name FROM person:server)[0]".into(),
            None
        )
        .await
        .unwrap()[0]
            .value_json,
        r#""server""#
    );
    let revision = client.status().await.unwrap().revision;
    assert_eq!(
        session
            .clone()
            .complete(stage.clone(), input.receipt)
            .await
            .unwrap(),
        NativeBootstrapCompletion::HistoricalSuccess
    );
    session.clone().cancel(stage).await.unwrap();
    let inventory = resumed.progress(input.manifest).await.unwrap();
    assert_eq!(inventory.state, NativeBootstrapStageState::Consumed);
    assert_eq!(inventory.successful_revision, Some(revision));
    assert_eq!(client.status().await.unwrap().revision, revision);
    db.close().await.unwrap();
}

#[tokio::test]
async fn native_clock_rejects_expired_install_without_extending_fixed_expiry() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "expired", 1);
    let stage = session
        .clone()
        .begin(input.start, input.manifest.clone())
        .await
        .unwrap();
    session
        .clone()
        .page(stage.clone(), input.page)
        .await
        .unwrap();
    assert_eq!(
        session
            .clone()
            .complete(stage, input.receipt)
            .await
            .unwrap_err(),
        NativeBootstrapError::Expired
    );
    assert_eq!(
        session.progress(input.manifest).await.unwrap().state,
        NativeBootstrapStageState::Staging
    );
    assert_eq!(client.status().await.unwrap().revision, 0);
    assert!(client.requires_authorized_reset().await.unwrap());
    assert_eq!(
        NativeBootstrapError::Expired.to_string(),
        "bootstrap completion expired"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unknown_begin_page_cancel_ack_invalidates_every_sibling_until_read_only_resume() {
    for operation in ["begin", "page", "cancel"] {
        let db = database().await;
        let expected = context();
        let client = open_sync_client(db.clone(), options(&expected))
            .await
            .unwrap();
        let sibling = open_sync_client(db.clone(), options(&expected))
            .await
            .unwrap();
        let session = bind_bootstrap_session(client.clone(), expected.clone())
            .await
            .unwrap();
        let input = wire(&expected, operation, u64::MAX);
        let mut stage = None;
        if operation != "begin" {
            stage = Some(
                session
                    .clone()
                    .begin(input.start.clone(), input.manifest.clone())
                    .await
                    .unwrap(),
            );
        }
        if operation == "cancel" {
            session
                .clone()
                .page(stage.clone().unwrap(), input.page.clone())
                .await
                .unwrap();
        }
        client.lose_next_bootstrap_ack_for_test();
        let result = match operation {
            "begin" => session
                .clone()
                .begin(input.start.clone(), input.manifest.clone())
                .await
                .map(|_| ()),
            "page" => session.clone().page(stage.unwrap(), input.page).await,
            _ => session.clone().cancel(stage.unwrap()).await,
        };
        assert_eq!(result.unwrap_err(), NativeBootstrapError::RecoveryRequired);
        assert!(matches!(
            sibling.status().await,
            Err(NativeSyncError::RecoveryRequired)
        ));
        assert_eq!(
            session
                .progress(input.manifest.clone())
                .await
                .err()
                .unwrap(),
            NativeBootstrapError::RecoveryRequired
        );
        let recovered = open_sync_client(db.clone(), options(&expected))
            .await
            .unwrap();
        let recovered = bind_bootstrap_session(recovered, expected.clone())
            .await
            .unwrap();
        let inventory = recovered.progress(input.manifest.clone()).await.unwrap();
        assert_eq!(
            inventory.state,
            if operation == "cancel" {
                NativeBootstrapStageState::Cancelled
            } else {
                NativeBootstrapStageState::Staging
            }
        );
        assert_eq!(
            inventory.received_indices,
            Some(if operation == "page" { vec![0] } else { vec![] })
        );
        let old = inventory.stage.unwrap();
        recovered.clone().cancel(old.clone()).await.unwrap();
        let next = wire(&expected, "next", u64::MAX);
        recovered
            .clone()
            .begin(next.start, next.manifest.clone())
            .await
            .unwrap();
        assert_eq!(
            recovered.clone().cancel(old).await.unwrap_err(),
            NativeBootstrapError::StageMismatch
        );
        assert_eq!(
            recovered.progress(next.manifest).await.unwrap().state,
            NativeBootstrapStageState::Staging
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn closed_or_changed_parent_cannot_mutate_through_retained_stage() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let sibling = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let other = bind_bootstrap_session(sibling, expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "close", u64::MAX);
    let stage = session
        .clone()
        .begin(input.start, input.manifest.clone())
        .await
        .unwrap();
    client.close().await;
    assert_eq!(
        session.clone().cancel(stage.clone()).await.unwrap_err(),
        NativeBootstrapError::Closed
    );
    assert_eq!(
        session
            .clone()
            .page(stage.clone(), input.page.clone())
            .await
            .unwrap_err(),
        NativeBootstrapError::Closed
    );
    assert_eq!(
        session
            .clone()
            .complete(stage, input.receipt)
            .await
            .unwrap_err(),
        NativeBootstrapError::Closed
    );
    let resumed = other
        .progress(input.manifest.clone())
        .await
        .unwrap()
        .stage
        .unwrap();
    other
        .clone()
        .page(resumed.clone(), input.page)
        .await
        .unwrap();
    db.use_namespace_database("other".into(), "other".into())
        .await
        .unwrap();
    assert_eq!(
        other.clone().cancel(resumed).await.unwrap_err(),
        NativeBootstrapError::RecoveryRequired
    );
    assert_eq!(
        other.progress(input.manifest).await.err().unwrap(),
        NativeBootstrapError::RecoveryRequired
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn persisted_stage_resume_and_terminal_owner_binding_survive_surrealkv_restart() {
    let directory = temp_dir::TempDir::new().unwrap();
    let options_db = || ConnectOptions {
        endpoint: format!("surrealkv://{}", directory.path().display()),
        namespace: Some("facade".into()),
        database: Some("facade".into()),
    };
    let expected = context();
    let input = wire(&expected, "restart", u64::MAX);
    let db = connect(options_db()).await.unwrap();
    db.query("DEFINE TABLE person SCHEMALESS".into(), None)
        .await
        .unwrap();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client, expected.clone())
        .await
        .unwrap();
    let stage = session
        .clone()
        .begin(input.start.clone(), input.manifest.clone())
        .await
        .unwrap();
    session
        .clone()
        .page(stage, input.page.clone())
        .await
        .unwrap();
    drop(session);
    db.close().await.unwrap();
    drop(db);
    let db = connect(options_db()).await.unwrap();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client, expected.clone())
        .await
        .unwrap();
    let progress = session.progress(input.manifest.clone()).await.unwrap();
    assert_eq!(progress.received_indices, Some(vec![0]));
    let stage = progress.stage.unwrap();
    session.clone().cancel(stage).await.unwrap();
    drop(session);
    db.close().await.unwrap();
    drop(db);
    let db = connect(options_db()).await.unwrap();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let mut foreign = expected.clone();
    foreign.principal_id = "other-owner".into();
    let foreign_wire = wire(&foreign, "owner-rebind", u64::MAX);
    assert!(matches!(
        bind_bootstrap_session(client.clone(), foreign).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    let legitimate = bind_bootstrap_session(client, expected).await.unwrap();
    assert_eq!(
        legitimate.progress(input.manifest).await.unwrap().state,
        NativeBootstrapStageState::Cancelled
    );
    assert!(matches!(
        legitimate
            .begin(foreign_wire.start, foreign_wire.manifest)
            .await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn resume_partial_schema_and_orphans_fail_closed_without_recreating_tables() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client, expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "orphan", u64::MAX);
    session
        .clone()
        .begin(input.start, input.manifest.clone())
        .await
        .unwrap();
    db.query("REMOVE TABLE _sync_bootstrap_header".into(), None)
        .await
        .unwrap();
    let before = schema(&db).await;
    assert_eq!(
        session.progress(input.manifest).await.err().unwrap(),
        NativeBootstrapError::CorruptStage
    );
    assert_eq!(schema(&db).await, before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn scope_barrier_blocks_new_mutations_but_allows_cancel_and_historical_identity() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "barrier", u64::MAX);
    let stage = session
        .clone()
        .begin(input.start.clone(), input.manifest.clone())
        .await
        .unwrap();
    session
        .clone()
        .page(stage.clone(), input.page.clone())
        .await
        .unwrap();
    assert_eq!(
        session
            .clone()
            .complete(stage.clone(), input.receipt.clone())
            .await
            .unwrap(),
        NativeBootstrapCompletion::Applied
    );
    let partition = PartitionId(expected.partition_id.clone());
    let owner = ClientId(expected.client_id.clone());
    let key = crate::sync_state::SyncStateKey::new(&partition, &owner);
    crate::sync_state::SyncStateStore::new(&db)
        .save_scope_barrier(
            key,
            crate::sync_state::ScopeBarrier {
                reason: "pending_quarantine_required".into(),
                required_scope: "next".into(),
                required_subscription_revision: "2".into(),
                required_authorization_revision: "8".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .clone()
            .begin(input.start, input.manifest.clone())
            .await
            .err()
            .unwrap(),
        NativeBootstrapError::PendingQuarantineRequired
    );
    assert_eq!(
        session
            .clone()
            .page(stage.clone(), input.page)
            .await
            .unwrap_err(),
        NativeBootstrapError::PendingQuarantineRequired
    );
    assert_eq!(
        session
            .clone()
            .complete(stage.clone(), input.receipt)
            .await
            .unwrap(),
        NativeBootstrapCompletion::HistoricalSuccess
    );
    session.clone().cancel(stage).await.unwrap();
    assert_eq!(
        session.progress(input.manifest).await.unwrap().state,
        NativeBootstrapStageState::Consumed
    );
    assert!(matches!(
        client.read_snapshot_json().await,
        Err(NativeSyncError::PendingQuarantineRequired)
    ));
    assert!(
        crate::sync_state::SyncStateStore::new(&db)
            .load_scope_barrier(key)
            .await
            .unwrap()
            .is_some()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn missing_namespace_and_fence_only_inventory_never_initialize_or_zero_corruption() {
    let expected = context();
    let input = wire(&expected, "absent", u64::MAX);
    let db = connect(ConnectOptions {
        endpoint: "mem://".into(),
        namespace: Some("uncreated".into()),
        database: Some("uncreated".into()),
    })
    .await
    .unwrap();
    let before: serde_json::Value =
        serde_json::from_str(&db.query("INFO FOR ROOT".into(), None).await.unwrap()[0].value_json)
            .unwrap();
    assert!(
        crate::sync_bootstrap_store::BootstrapStore::new(&db)
            .resume(
                Some(&input.manifest),
                &expected.clone().validated().unwrap(),
                None
            )
            .await
            .unwrap()
            .is_none()
    );
    let after: serde_json::Value =
        serde_json::from_str(&db.query("INFO FOR ROOT".into(), None).await.unwrap()[0].value_json)
            .unwrap();
    assert_eq!(before["namespaces"], after["namespaces"]);
    db.close().await.unwrap();
    for corruption in ["fence-only", "orphan-page"] {
        let db = database().await;
        let client = open_sync_client(db.clone(), options(&expected))
            .await
            .unwrap();
        let session = bind_bootstrap_session(client, expected.clone())
            .await
            .unwrap();
        let stage = session
            .clone()
            .begin(input.start.clone(), input.manifest.clone())
            .await
            .unwrap();
        if corruption == "orphan-page" {
            session
                .clone()
                .page(stage, input.page.clone())
                .await
                .unwrap();
            db.query("DELETE _sync_bootstrap_header".into(), None)
                .await
                .unwrap();
        } else {
            db.query(
                "REMOVE TABLE _sync_bootstrap_header; REMOVE TABLE _sync_bootstrap_page".into(),
                None,
            )
            .await
            .unwrap();
        }
        let before = schema(&db).await;
        assert_eq!(
            session
                .progress(input.manifest.clone())
                .await
                .err()
                .unwrap(),
            NativeBootstrapError::CorruptStage
        );
        assert_eq!(schema(&db).await, before);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn clean_authorization_advance_invalidates_old_sessions_and_survives_reopen() {
    let db = database().await;
    let mut old = context();
    old.authorization_revision = 5;
    let client = open_sync_client(db.clone(), options(&old)).await.unwrap();
    let session5 = bind_bootstrap_session(client.clone(), old.clone())
        .await
        .unwrap();
    let input5 = wire(&old, "auth-five", u64::MAX);
    let stage5 = session5
        .clone()
        .begin(input5.start, input5.manifest.clone())
        .await
        .unwrap();
    session5
        .clone()
        .page(stage5.clone(), input5.page)
        .await
        .unwrap();
    session5
        .clone()
        .complete(stage5.clone(), input5.receipt.clone())
        .await
        .unwrap();
    let mut next = old.clone();
    next.authorization_revision = 6;
    let session6 = bind_bootstrap_session(client.clone(), next.clone())
        .await
        .unwrap();
    assert!(session5.progress(input5.manifest).await.is_err());
    assert!(
        session5
            .clone()
            .complete(stage5, input5.receipt)
            .await
            .is_err()
    );
    let input6 = wire(&next, "auth-six", u64::MAX);
    let stage6 = session6
        .clone()
        .begin(input6.start, input6.manifest.clone())
        .await
        .unwrap();
    client.close().await;
    drop(session5);
    drop(session6);
    drop(client);
    let reopened = open_sync_client(db.clone(), options(&next)).await.unwrap();
    assert!(matches!(
        bind_bootstrap_session(reopened.clone(), old).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    let resumed = bind_bootstrap_session(reopened.clone(), next)
        .await
        .unwrap();
    let progress = resumed.progress(input6.manifest).await.unwrap();
    assert_eq!(progress.state, NativeBootstrapStageState::Staging);
    assert!(
        resumed
            .clone()
            .page(stage6, input6.page.clone())
            .await
            .is_err(),
        "old object is never adopted"
    );
    let stage = progress.stage.unwrap();
    resumed
        .clone()
        .page(stage.clone(), input6.page)
        .await
        .unwrap();
    assert_eq!(
        resumed
            .clone()
            .complete(stage, input6.receipt)
            .await
            .unwrap(),
        NativeBootstrapCompletion::Applied
    );
    assert_eq!(reopened.status().await.unwrap().cursor_sequence, Some(1));
}

#[tokio::test]
async fn active_context_cannot_advance_but_cancelled_context_can_and_unknown_begin_recovers_floor()
{
    let db = database().await;
    let old = context();
    let mut next = old.clone();
    next.authorization_revision += 1;
    let client = open_sync_client(db.clone(), options(&old)).await.unwrap();
    let session = bind_bootstrap_session(client.clone(), old.clone())
        .await
        .unwrap();
    let input = wire(&old, "active-old", u64::MAX);
    let stage = session
        .clone()
        .begin(input.start, input.manifest.clone())
        .await
        .unwrap();
    assert!(matches!(
        bind_bootstrap_session(client.clone(), next.clone()).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    assert_eq!(
        session.progress(input.manifest).await.unwrap().state,
        NativeBootstrapStageState::Staging
    );
    session.clone().cancel(stage.clone()).await.unwrap();
    let newer = bind_bootstrap_session(client.clone(), next.clone())
        .await
        .unwrap();
    assert!(session.clone().cancel(stage).await.is_err());
    let input = wire(&next, "newer-unknown-ack", u64::MAX);
    client.lose_next_bootstrap_ack_for_test();
    assert_eq!(
        newer
            .clone()
            .begin(input.start, input.manifest.clone())
            .await
            .err()
            .unwrap(),
        NativeBootstrapError::RecoveryRequired
    );
    let reopened = open_sync_client(db.clone(), options(&next)).await.unwrap();
    assert!(matches!(
        bind_bootstrap_session(reopened.clone(), old).await,
        Err(NativeBootstrapError::ContextMismatch)
    ));
    let resumed = bind_bootstrap_session(reopened, next).await.unwrap();
    assert_eq!(
        resumed.progress(input.manifest).await.unwrap().state,
        NativeBootstrapStageState::Staging
    );
}

#[tokio::test]
async fn authorization_advance_complete_keeps_canonical_pending_outcome_and_subscription_guards() {
    use surrealdb_sync_client::{DurableSubscriptions, ResolvedCommit};
    use surrealdb_sync_protocol::{
        BaseVersion, ClientCommit, ClientCommitId, CommitIdentity, DurableOutcome, Fingerprint,
        Operation,
    };
    for case in ["pending", "accepted", "conflict", "subscriptions"] {
        let db = database().await;
        let mut old = context();
        old.authorization_revision = 5;
        let old_context = old.clone().validated().unwrap();
        let mut state = crate::sync_state::StoredSyncState::empty(
            old_context.partition_id.clone(),
            old_context.client_id.clone(),
            old_context.scope.identity.clone(),
            1,
        );
        state.checkpoint = Some(Checkpoint {
            token: OpaqueCheckpoint("old".into()),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            scope: old_context.scope.clone(),
        });
        let mut local = ClientCommit {
            identity: CommitIdentity {
                client_commit_id: ClientCommitId("local".into()),
                fingerprint: Fingerprint("local-fingerprint".into()),
            },
            operations: vec![Operation::Upsert {
                record_id: RecordId("person:local".into()),
                base_version: BaseVersion::Absent,
                value: serde_json::json!({"name":"local"}),
                reference: None,
            }],
        };
        local.identity.fingerprint =
            crate::sync_codec::canonical_fingerprint(&state, &local).unwrap();
        match case {
            "pending" => state.outbox.push(local),
            "accepted" | "conflict" => {
                let outcome = if case == "accepted" {
                    DurableOutcome::Accepted {
                        identity: local.identity.clone(),
                        sequence: 1,
                        id_mappings: vec![],
                    }
                } else {
                    DurableOutcome::Conflict {
                        identity: local.identity.clone(),
                        record_id: RecordId("person:local".into()),
                        authoritative: RecordState::Absent,
                    }
                };
                state.outcomes.push(ResolvedCommit {
                    local_commit: local,
                    outcome,
                    resolution: None,
                    covered_by_reset: false,
                });
            }
            _ => {
                state.subscriptions = Some(DurableSubscriptions {
                    authorization_scope: Some(old_context.scope),
                    ..Default::default()
                })
            }
        }
        let store = crate::sync_state::SyncStateStore::new(&db);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let client = open_sync_client(db.clone(), options(&old)).await.unwrap();
        let mut next = old;
        next.authorization_revision = 6;
        let session = bind_bootstrap_session(client.clone(), next.clone())
            .await
            .unwrap();
        let input = wire(&next, case, u64::MAX);
        let stage = session
            .clone()
            .begin(input.start, input.manifest)
            .await
            .unwrap();
        session
            .clone()
            .page(stage.clone(), input.page)
            .await
            .unwrap();
        let before = client.status().await.unwrap();
        let error = session
            .clone()
            .complete(stage, input.receipt)
            .await
            .unwrap_err();
        let expected = if case == "pending" || case == "conflict" {
            NativeBootstrapError::PendingQuarantineRequired
        } else {
            NativeBootstrapError::NeedsNewScopeJournal
        };
        assert_eq!(error, expected, "{case}");
        let after = client.status().await.unwrap();
        assert_eq!(after.revision, before.revision, "{case}");
        let partition = PartitionId(next.partition_id);
        let owner = ClientId(next.client_id);
        assert_eq!(
            store
                .load(crate::sync_state::SyncStateKey::new(&partition, &owner))
                .await
                .unwrap()
                .unwrap(),
            state,
            "{case}: canonical data changed"
        );
        let rows = db
            .query("SELECT * FROM person:server".into(), None)
            .await
            .unwrap();
        assert!(
            rows[0].value_json.contains("[]"),
            "{case}: projection published"
        );
    }
}

#[tokio::test]
async fn newer_durable_stage_and_checkpoint_authorization_floors_survive_physical_restart() {
    for terminal in ["active", "cancelled", "consumed"] {
        let directory = temp_dir::TempDir::new().unwrap();
        let db_options = || ConnectOptions {
            endpoint: format!("surrealkv://{}", directory.path().display()),
            namespace: Some("facade".into()),
            database: Some("facade".into()),
        };
        let db = connect(db_options()).await.unwrap();
        db.query("DEFINE TABLE person SCHEMALESS".into(), None)
            .await
            .unwrap();
        let mut old = context();
        old.authorization_revision = 5;
        let mut newer = old.clone();
        newer.authorization_revision = 6;
        let client = open_sync_client(db.clone(), options(&old)).await.unwrap();
        let session = bind_bootstrap_session(client.clone(), old.clone())
            .await
            .unwrap();
        let input = wire(&old, "initial-terminal", u64::MAX);
        let stage = session
            .clone()
            .begin(input.start, input.manifest)
            .await
            .unwrap();
        session.clone().cancel(stage).await.unwrap();
        let next = bind_bootstrap_session(client.clone(), newer.clone())
            .await
            .unwrap();
        let input = wire(&newer, "durable-six", u64::MAX);
        let stage = next
            .clone()
            .begin(input.start, input.manifest.clone())
            .await
            .unwrap();
        match terminal {
            "cancelled" => next.clone().cancel(stage).await.unwrap(),
            "consumed" => {
                next.clone().page(stage.clone(), input.page).await.unwrap();
                next.clone().complete(stage, input.receipt).await.unwrap();
            }
            _ => (),
        }
        drop(next);
        drop(session);
        drop(client);
        db.close().await.unwrap();
        drop(db);
        let db = connect(db_options()).await.unwrap();
        let client = open_sync_client(db.clone(), options(&newer)).await.unwrap();
        assert!(
            matches!(
                bind_bootstrap_session(client.clone(), old).await,
                Err(NativeBootstrapError::ContextMismatch)
            ),
            "{terminal}"
        );
        let session = bind_bootstrap_session(client, newer).await.unwrap();
        let progress = session.progress(input.manifest).await.unwrap();
        assert_eq!(
            progress.state,
            match terminal {
                "cancelled" => NativeBootstrapStageState::Cancelled,
                "consumed" => NativeBootstrapStageState::Consumed,
                _ => NativeBootstrapStageState::Staging,
            }
        );
        drop(session);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn native_request_builders_preserve_bound_context_and_manifest_without_mutation() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db.clone(), options(&expected))
        .await
        .unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    let input = wire(&expected, "request-codec", u64::MAX);
    let before = schema(&db).await;
    assert_eq!(
        session.request_start("request-codec".into()).await.unwrap(),
        input.start
    );
    assert_eq!(client.status().await.unwrap().revision, 0);
    assert_eq!(schema(&db).await, before);
    let stage = session
        .clone()
        .begin(input.start, input.manifest.clone())
        .await
        .unwrap();
    let before = client.status().await.unwrap().revision;
    let request =
        decode_bootstrap_page_request(&session.request_page(stage.clone(), 0).await.unwrap())
            .unwrap();
    assert_eq!(request.partition_id.0, expected.partition_id);
    assert_eq!(request.client_id.0, expected.client_id);
    assert_eq!(request.session_id, "request-codec");
    assert_eq!(request.index, 0);
    let complete = session.request_complete(stage.clone()).await.unwrap();
    let decoded = decode_bootstrap_complete(&complete).unwrap();
    assert_eq!(
        decoded.manifest_digest,
        bootstrap_manifest_digest(&decode_bootstrap_manifest(&input.manifest).unwrap()).unwrap()
    );
    assert_eq!(decoded.session_id, "request-codec");
    assert_eq!(decoded.partition_id.0, expected.partition_id);
    assert_eq!(decoded.client_id.0, expected.client_id);
    assert_eq!(client.status().await.unwrap().revision, before);
    session
        .clone()
        .page(stage.clone(), input.page)
        .await
        .unwrap();
    session
        .clone()
        .complete(stage.clone(), input.receipt)
        .await
        .unwrap();
    let after = client.status().await.unwrap().revision;
    assert_eq!(session.request_complete(stage).await.unwrap(), complete);
    assert_eq!(client.status().await.unwrap().revision, after);
    client.close().await;
    assert!(matches!(
        session.request_start("request-codec".into()).await,
        Err(NativeBootstrapError::Closed)
    ));
}

#[tokio::test]
async fn request_builders_refuse_bad_indices_cross_binding_cancelled_and_replaced_stages() {
    let db = database().await;
    let expected = context();
    let client = open_sync_client(db, options(&expected)).await.unwrap();
    let session = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    for id in [String::new(), "x".repeat(513), "é".repeat(257)] {
        assert!(matches!(
            session.request_start(id).await,
            Err(NativeBootstrapError::InvalidInput)
        ));
    }
    let input = wire(&expected, "request-errors", u64::MAX);
    let stage = session
        .clone()
        .begin(input.start.clone(), input.manifest.clone())
        .await
        .unwrap();
    for index in [1, u64::MAX] {
        assert!(matches!(
            session.request_page(stage.clone(), index).await,
            Err(NativeBootstrapError::InvalidInput)
        ));
    }
    let other = bind_bootstrap_session(client.clone(), expected.clone())
        .await
        .unwrap();
    assert!(matches!(
        other.request_page(stage.clone(), 0).await,
        Err(NativeBootstrapError::StageMismatch)
    ));
    assert!(matches!(
        other.request_complete(stage.clone()).await,
        Err(NativeBootstrapError::StageMismatch)
    ));
    session.clone().cancel(stage.clone()).await.unwrap();
    assert!(matches!(
        session.request_complete(stage.clone()).await,
        Err(NativeBootstrapError::StageMismatch)
    ));
    assert!(matches!(
        session.request_page(stage.clone(), 0).await,
        Err(NativeBootstrapError::StageMismatch)
    ));
    let replacement_input = wire(&expected, "request-replacement", u64::MAX);
    let replacement = session
        .clone()
        .begin(replacement_input.start, replacement_input.manifest)
        .await
        .unwrap();
    assert!(session.request_complete(replacement).await.is_ok());
    assert!(matches!(
        session.request_complete(stage).await,
        Err(NativeBootstrapError::StageMismatch)
    ));
}
