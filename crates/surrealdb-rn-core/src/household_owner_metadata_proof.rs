//! Test-only conformance for the app's private pre-native ROOT/NS admission.
//! No API, ABI, initializer, owner-marker policy or runtime behavior is added.
use crate::sync_client::{NativeSyncInspection, NativeSyncOpenOptions};
use crate::{
    ConnectOptions, NativeStorageInspection, NativeStorageInspectionOptions, SurrealDatabase,
    connect,
};
use std::sync::{Arc, atomic::Ordering};
use surrealdb::types::Value;
const NAME: &str = "household-local";
async fn fresh(endpoint: String) -> Arc<SurrealDatabase> {
    connect(ConnectOptions {
        endpoint,
        namespace: None,
        database: None,
    })
    .await
    .unwrap()
}
async fn value(db: &SurrealDatabase, sql: &str) -> Value {
    let mut response = db
        .client()
        .await
        .unwrap()
        .query(sql)
        .await
        .unwrap()
        .check()
        .unwrap();
    let last = response.num_statements() - 1;
    response.take(last).unwrap()
}
fn root_admitted(root: &Value) -> Result<bool, ()> {
    let Value::Object(root) = root else {
        return Err(());
    };
    let keys = [
        "accesses",
        "defaults",
        "namespaces",
        "nodes",
        "system",
        "users",
        "config",
    ];
    if root.len() != keys.len() || root.keys().any(|name| !keys.contains(&name.as_str())) {
        return Err(());
    }
    for field in ["accesses", "defaults", "users"] {
        if !matches!(root.get(field),Some(Value::Object(map)) if map.is_empty()) {
            return Err(());
        }
    }
    if !matches!(root.get("config"),Some(Value::Object(map)) if map.len()==1 && map.get("QUERY_TIMEOUT")==Some(&Value::None))
    {
        return Err(());
    }
    let Some(Value::Object(names)) = root.get("namespaces") else {
        return Err(());
    };
    if names.is_empty() {
        return Ok(false);
    }
    if names.len() != 1 || !names.contains_key(NAME) {
        return Err(());
    }
    Ok(true)
}
fn ns_admitted(ns: &Value) -> bool {
    let Value::Object(ns) = ns else { return false };
    ns.len() == 3
        && ["accesses", "users"]
            .iter()
            .all(|field| matches!(ns.get(*field),Some(Value::Object(map)) if map.is_empty()))
        && matches!(ns.get("databases"),Some(Value::Object(map)) if map.len()==1 && map.contains_key(NAME))
}
fn durable_root(mut root: Value) -> Value {
    let Value::Object(map) = &mut root else {
        panic!("root object")
    };
    map.remove("nodes");
    map.remove("system");
    root
}
async fn setup(db: &SurrealDatabase) {
    value(db,"DEFINE NAMESPACE `household-local`; USE NS `household-local`; DEFINE DATABASE `household-local`; USE DB `household-local`; CREATE app_household_storage_owner:fixture SET phase='synthetic'; CREATE recipe:fixture SET value=1;").await;
}
fn options() -> NativeStorageInspectionOptions {
    NativeStorageInspectionOptions {
        namespace: NAME.into(),
        database: NAME.into(),
        expected_journal: NativeSyncOpenOptions {
            partition_id: "partition".into(),
            client_id: "client".into(),
            requested_scope: "household-state-v2".into(),
            subscription_revision: 1,
        },
        application_tables: vec!["app_household_storage_owner".into(), "recipe".into()],
    }
}
/// Exactly the proposed app sequence. Raw USE scope is confined to this batch;
/// the later typed use is explicit and is reached only after existing DB proof.
async fn admit_existing(db: &SurrealDatabase, trace: &mut Vec<&'static str>) -> Result<bool, ()> {
    trace.push("root");
    if !root_admitted(&value(db, "INFO FOR ROOT").await)? {
        return Err(());
    }
    trace.push("use-ns-info");
    let generation = db.session_generation.load(Ordering::Acquire);
    let mut response = db
        .client()
        .await
        .unwrap()
        .query("USE NS `household-local`; INFO FOR NS")
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(response.num_statements(), 2);
    let ns: Value = response.take(1).unwrap();
    if !ns_admitted(&ns) {
        return Err(());
    }
    assert_eq!(db.session_generation.load(Ordering::Acquire), generation);
    // Record actual cross-query raw selection behavior, without depending on it.
    let next = db
        .client()
        .await
        .unwrap()
        .query("INFO FOR NS")
        .await
        .and_then(|response| response.check());
    let persisted = match next {
        Ok(mut result) => {
            let v: Value = result.take(0).unwrap();
            assert!(ns_admitted(&v));
            true
        }
        Err(error) => {
            assert!(format!("{error}").contains("namespace"));
            false
        }
    };
    eprintln!("PHYSICAL_OWNER_METADATA_PROOF raw_use_persists={persisted} ns_statement_index=1");
    trace.push("typed-use");
    db.use_namespace_database(NAME.into(), NAME.into())
        .await
        .unwrap();
    assert!(db.session_generation.load(Ordering::Acquire) > generation);
    Ok(persisted)
}
#[tokio::test]
async fn pristine_root_admission_and_native_inspection_do_not_select_or_initialize() {
    let db = fresh("mem://".into()).await;
    let before = durable_root(value(&db, "INFO FOR ROOT").await);
    assert_eq!(root_admitted(&value(&db, "INFO FOR ROOT").await), Ok(false));
    assert_eq!(
        crate::inspect_storage(db.clone(), options()).await.unwrap(),
        NativeStorageInspection::PristineUnselected
    );
    assert_eq!(durable_root(value(&db, "INFO FOR ROOT").await), before);
    assert_eq!(db.session_generation.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn same_request_namespace_info_then_typed_use_preserves_existing_metadata_and_occupancy() {
    let db = fresh("mem://".into()).await;
    setup(&db).await;
    let before_root = durable_root(value(&db, "INFO FOR ROOT").await);
    let before_db = value(
        &db,
        "USE NS `household-local` DB `household-local`; INFO FOR DB",
    )
    .await;
    let mut trace = Vec::new();
    admit_existing(&db, &mut trace).await.unwrap();
    assert_eq!(trace, vec!["root", "use-ns-info", "typed-use"]);
    let NativeStorageInspection::Selected {
        native,
        application_tables,
    } = crate::inspect_storage(db.clone(), options()).await.unwrap()
    else {
        panic!("selected")
    };
    assert_eq!(native, NativeSyncInspection::Missing);
    assert!(
        application_tables
            .iter()
            .all(|table| table.schema_present && table.rows_present)
    );
    assert_eq!(durable_root(value(&db, "INFO FOR ROOT").await), before_root);
    assert_eq!(value(&db, "INFO FOR DB").await, before_db);
}
#[tokio::test]
async fn foreign_namespace_or_database_is_rejected_before_any_new_metadata_or_typed_selection() {
    let db = fresh("mem://".into()).await;
    value(&db, "DEFINE NAMESPACE foreign").await;
    let before = durable_root(value(&db, "INFO FOR ROOT").await);
    let mut trace = Vec::new();
    assert!(admit_existing(&db, &mut trace).await.is_err());
    assert_eq!(trace, vec!["root"]);
    assert_eq!(durable_root(value(&db, "INFO FOR ROOT").await), before);
    let db = fresh("mem://".into()).await;
    value(
        &db,
        "DEFINE NAMESPACE `household-local`; USE NS `household-local`; DEFINE DATABASE foreign",
    )
    .await;
    let before_root = durable_root(value(&db, "INFO FOR ROOT").await);
    let before_ns = value(&db, "USE NS `household-local`; INFO FOR NS").await;
    let mut trace = Vec::new();
    assert!(admit_existing(&db, &mut trace).await.is_err());
    assert_eq!(trace, vec!["root", "use-ns-info"]);
    assert_eq!(db.session_generation.load(Ordering::Acquire), 0);
    assert_eq!(durable_root(value(&db, "INFO FOR ROOT").await), before_root);
    assert_eq!(
        value(&db, "USE NS `household-local`; INFO FOR NS").await,
        before_ns
    );
}
#[tokio::test]
#[ignore = "explicit owned persistent fixture; preserve its files"]
async fn surrealkv_reopen_is_initially_unselected_and_exact_admission_preserves_store() {
    let path = std::env::var("HOUSEHOLD_OWNER_METADATA_FIXTURE").expect("owned fixture path");
    let endpoint = format!("surrealkv://{path}");
    let db = fresh(endpoint.clone()).await;
    setup(&db).await;
    let before_root = durable_root(value(&db, "INFO FOR ROOT").await);
    let before_db = value(
        &db,
        "USE NS `household-local` DB `household-local`; INFO FOR DB",
    )
    .await;
    db.close().await.unwrap();
    drop(db);
    let db = fresh(endpoint).await;
    assert!(
        db.client()
            .await
            .unwrap()
            .query("INFO FOR NS")
            .await
            .and_then(|response| response.check())
            .is_err()
    );
    let mut trace = Vec::new();
    admit_existing(&db, &mut trace).await.unwrap();
    assert_eq!(trace, vec!["root", "use-ns-info", "typed-use"]);
    let NativeStorageInspection::Selected {
        native,
        application_tables,
    } = crate::inspect_storage(db.clone(), options()).await.unwrap()
    else {
        panic!("selected")
    };
    assert_eq!(native, NativeSyncInspection::Missing);
    assert!(
        application_tables
            .iter()
            .all(|table| table.schema_present && table.rows_present)
    );
    assert_eq!(durable_root(value(&db, "INFO FOR ROOT").await), before_root);
    assert_eq!(value(&db, "INFO FOR DB").await, before_db);
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_encodes_root_none_and_exact_namespace_statement_indices() {
    let db = fresh("memory".into()).await;
    setup(&db).await;
    let root = db.query("INFO FOR ROOT".into(), None).await.unwrap();
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].statement_index, 0);
    let root: serde_json::Value = serde_json::from_str(&root[0].value_json).unwrap();
    assert_eq!(
        root["config"]["QUERY_TIMEOUT"],
        serde_json::json!({"$surreal":"none"})
    );
    assert!(root["namespaces"].as_object().unwrap().contains_key(NAME));
    let ns = db
        .query("USE NS `household-local`; INFO FOR NS".into(), None)
        .await
        .unwrap();
    assert_eq!(ns.len(), 2);
    assert_eq!(ns[0].statement_index, 0);
    assert_eq!(ns[1].statement_index, 1);
    let use_result: serde_json::Value = serde_json::from_str(&ns[0].value_json).unwrap();
    assert_eq!(
        use_result,
        serde_json::json!({"database":{"$surreal":"none"},"namespace":NAME})
    );
    let ns: serde_json::Value = serde_json::from_str(&ns[1].value_json).unwrap();
    assert_eq!(ns.as_object().unwrap().len(), 3);
    assert_eq!(ns["accesses"], serde_json::json!({}));
    assert_eq!(ns["users"], serde_json::json!({}));
    let databases = ns["databases"].as_object().unwrap();
    assert_eq!(databases.len(), 1);
    assert!(databases.contains_key(NAME));
    db.close().await.unwrap();
}
