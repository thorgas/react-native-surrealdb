use super::*;
use crate::{ConnectOptions, connect};
use serde_json::json;
use surrealdb_sync_protocol::{Operation, RecordId, SchemaVersion};

fn fixtures() -> Vec<JsonValue> {
    serde_json::from_str(include_str!(
        "../../../../packages/react-native-surrealdb/test/fixtures/bigint-cas.json"
    ))
    .unwrap()
}
fn options() -> NativeSyncOpenOptions {
    NativeSyncOpenOptions {
        partition_id: "partition".into(),
        client_id: "client".into(),
        requested_scope: "all".into(),
        subscription_revision: 1,
    }
}
async fn database(endpoint: String) -> Arc<SurrealDatabase> {
    connect(ConnectOptions {
        endpoint,
        namespace: Some("bigint_cas".into()),
        database: Some("bigint_cas".into()),
    })
    .await
    .unwrap()
}
async fn seed_record(db: &SurrealDatabase) {
    db.query("UPSERT person:ada CONTENT { name: 'before' }".into(), None)
        .await
        .unwrap();
}
fn queued(raw: &str) -> ClientCommit<JsonValue> {
    serde_json::from_str(raw).unwrap()
}

#[tokio::test]
async fn real_js_bigint_wire_enqueue_and_bounded_preserve_exact_versions_payload_and_retry() {
    for fixture in fixtures() {
        for bounded in [false, true] {
            let db = database("mem://".into()).await;
            seed_record(&db).await;
            let client = open_sync_client(db, options()).await.unwrap();
            let raw = fixture["commit"].to_string();
            if bounded {
                client
                    .clone()
                    .enqueue_bounded(raw.clone(), 10, 65536)
                    .await
                    .unwrap();
            } else {
                client.clone().enqueue(raw.clone()).await.unwrap();
            }
            let pending = client.pending_json().await.unwrap();
            let parsed = queued(&pending[0]);
            let expected: u64 = fixture["version"].as_str().unwrap().parse().unwrap();
            match &parsed.operations[0] {
                Operation::Upsert {
                    base_version,
                    value,
                    ..
                } => {
                    assert_eq!(*base_version, BaseVersion::Exact(expected));
                    assert_eq!(value, &fixture["commit"]["operations"][0]["value"]);
                }
                Operation::Delete { base_version, .. } => assert_eq!(*base_version, expected),
            }
            assert!(parsed.identity.fingerprint.0.starts_with("sha256:"));
            // Numeric native representation must have the same canonical identity.
            assert!(matches!(
                client
                    .clone()
                    .enqueue(serde_json::to_string(&parsed).unwrap())
                    .await,
                Err(NativeSyncError::Protocol)
            ));
            client
                .clone()
                .enqueue_bounded(raw.clone(), 10, 65536)
                .await
                .unwrap();
            assert!(matches!(
                client.clone().enqueue(raw).await,
                Err(NativeSyncError::Protocol)
            ));
            client
                .clone()
                .enqueue_bounded(serde_json::to_string(&parsed).unwrap(), 10, 65536)
                .await
                .unwrap();
            assert_eq!(client.pending_json().await.unwrap(), pending);
            let before = client.status().await.unwrap();
            let mut changed = parsed.clone();
            match &mut changed.operations[0] {
                Operation::Upsert { base_version, .. } => {
                    *base_version = BaseVersion::Exact(if expected == 0 { 5 } else { 0 })
                }
                Operation::Delete { base_version, .. } => {
                    *base_version = if expected == 0 { 5 } else { 0 }
                }
            }
            let changed = serde_json::to_string(&changed).unwrap();
            assert!(client.clone().enqueue(changed.clone()).await.is_err());
            assert!(
                client
                    .clone()
                    .enqueue_bounded(changed, 10, 65536)
                    .await
                    .is_err()
            );
            assert_eq!(client.status().await.unwrap(), before);
            assert_eq!(client.pending_json().await.unwrap(), pending);
            client.close().await;
        }
    }
}

#[tokio::test]
async fn invalid_bigint_versions_do_not_mutate_native_queue() {
    let invalid = vec![
        json!({"$surreal":"int","value":"-1"}),
        json!({"$surreal":"int","value":"1.5"}),
        json!({"$surreal":"int","value":"18446744073709551616"}),
        json!({"$surreal":"decimal","value":"5"}),
        json!({"$surreal":"int","value":5}),
        json!({"$surreal":"int","value":"5","extra":true}),
        json!({"$surreal":"int"}),
        json!({"$surreal":"int","value":""}),
        json!({"$surreal":"int","value":"1e3"}),
        json!(-1),
        json!(1.5),
    ];
    for fixture in fixtures().into_iter().take(2) {
        let db = database("mem://".into()).await;
        seed_record(&db).await;
        let client = open_sync_client(db, options()).await.unwrap();
        let mut seed = fixture["commit"].clone();
        seed["identity"]["clientCommitId"] = json!("seed");
        client.clone().enqueue(seed.to_string()).await.unwrap();
        let pending = client.pending_json().await.unwrap();
        let before = client.status().await.unwrap();
        for bad in &invalid {
            let mut raw = fixture["commit"].clone();
            raw["operations"][0]["base_version"] = if raw["operations"][0]["kind"] == "upsert" {
                json!({"exact":bad})
            } else {
                bad.clone()
            };
            assert!(
                matches!(
                    client.clone().enqueue(raw.to_string()).await,
                    Err(NativeSyncError::InvalidInput)
                ),
                "{bad}"
            );
            assert!(
                matches!(
                    client
                        .clone()
                        .enqueue_bounded(raw.to_string(), 10, 65536)
                        .await,
                    Err(NativeSyncError::InvalidInput)
                ),
                "{bad}"
            );
            assert_eq!(client.status().await.unwrap(), before);
            assert_eq!(client.pending_json().await.unwrap(), pending);
        }
        let raw = fixture["commit"].to_string().replace(
            r#""$surreal":"int","value":"0""#,
            r#""$surreal":"int","value":"0","value":"5""#,
        );
        assert!(matches!(
            client.clone().enqueue(raw.clone()).await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert!(matches!(
            client.clone().enqueue_bounded(raw, 10, 65536).await,
            Err(NativeSyncError::InvalidInput)
        ));
        assert_eq!(client.status().await.unwrap(), before);
    }
}

#[tokio::test]
async fn bigint_versions_survive_disk_reopen_and_conflict_merge() {
    for fixture in fixtures() {
        let directory = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", directory.path().display());
        let db = database(endpoint.clone()).await;
        seed_record(&db).await;
        let client = open_sync_client(db.clone(), options()).await.unwrap();
        client
            .clone()
            .enqueue(fixture["commit"].to_string())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "enqueue fixture {}: {error:?}",
                    fixture["commit"]["identity"]["clientCommitId"]
                )
            });
        let pending = client.pending_json().await.unwrap();
        client.close().await;
        db.close().await.unwrap();
        drop(client);
        drop(db);

        let db = database(endpoint).await;
        let client = open_sync_client(db.clone(), options()).await.unwrap();
        assert_eq!(client.pending_json().await.unwrap(), pending);
        let original = queued(&pending[0]);
        let expected: u64 = fixture["version"].as_str().unwrap().parse().unwrap();
        client
            .clone()
            .record_push_response(
                serde_json::to_string(&PushResponse::<JsonValue> {
                    schema_version: SchemaVersion::V1,
                    partition_id: PartitionId("partition".into()),
                    client_id: ClientId("client".into()),
                    outcome: DurableOutcome::Conflict {
                        identity: original.identity.clone(),
                        record_id: RecordId("person:ada".into()),
                        authoritative: RecordState::Present {
                            value: json!({"name":"server"}),
                            version: expected,
                            reference: None,
                        },
                    },
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let mut replacement = fixture["commit"].clone();
        replacement["identity"]["clientCommitId"] = json!("merged");
        let mut invalid = replacement.clone();
        let bad = json!({"$surreal":"int","value":"18446744073709551616"});
        invalid["operations"][0]["base_version"] = if invalid["operations"][0]["kind"] == "upsert" {
            json!({"exact":bad})
        } else {
            bad
        };
        let before = client.status().await.unwrap();
        assert!(
            client
                .clone()
                .resolve_conflict_merge(
                    original.identity.client_commit_id.0.clone(),
                    invalid.to_string()
                )
                .await
                .is_err()
        );
        assert_eq!(client.status().await.unwrap(), before);
        client
            .clone()
            .resolve_conflict_merge(
                original.identity.client_commit_id.0.clone(),
                replacement.to_string(),
            )
            .await
            .unwrap();
        let merged = queued(&client.pending_json().await.unwrap()[0]);
        assert_eq!(merged.operations, original.operations);
        let status = client.status().await.unwrap();
        client
            .clone()
            .resolve_conflict_merge(
                original.identity.client_commit_id.0,
                serde_json::to_string(&merged).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            client.status().await.unwrap().pending_count,
            status.pending_count
        );
        client.close().await;
        db.close().await.unwrap();
    }
}
