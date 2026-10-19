//! No-init whole-storage ownership observation. App marker policy remains separate.
use crate::SurrealDatabase;
use crate::sync_client::{
    NativeSyncInspection, NativeSyncInspectionBarrier, NativeSyncOpenOptions,
};
use crate::sync_state::{self, SyncStateError, SyncStateKey};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use surrealdb::engine::any::Any;
use surrealdb::types::{SurrealValue, Value};
use surrealdb_sync_client::ClientRuntime;
use surrealdb_sync_protocol::{ClientId, DurableOutcome, PartitionId};

#[derive(Clone, Debug, uniffi::Record)]
pub struct NativeStorageInspectionOptions {
    pub namespace: String,
    pub database: String,
    pub expected_journal: NativeSyncOpenOptions,
    pub application_tables: Vec<String>,
}
#[derive(Debug, PartialEq, Eq, uniffi::Enum)]
pub enum NativeStorageInspection {
    PristineUnselected,
    Selected {
        native: NativeSyncInspection,
        application_tables: Vec<NativeTableOccupancy>,
    },
}
#[derive(Debug, PartialEq, Eq, uniffi::Record)]
pub struct NativeTableOccupancy {
    pub table: String,
    pub schema_present: bool,
    pub rows_present: bool,
}
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum NativeStorageInspectionError {
    #[error("invalid inspection options")]
    InvalidOptions,
    #[error("storage is closed or unsettled")]
    Unsettled,
    #[error("foreign or unsupported storage layout")]
    UnsupportedLayout,
    #[error("storage metadata exceeds inspection limits")]
    MetadataLimit,
    #[error("storage journal is corrupt")]
    CorruptState,
    #[error("storage read or cancellation failed")]
    Persistence,
}
fn settled(db: &SurrealDatabase, generation: u64) -> Result<(), NativeStorageInspectionError> {
    if db.is_closed()
        || db.sync_runtimes.is_poisoned()
        || db.session_generation.load(Ordering::Acquire) != generation
    {
        return Err(NativeStorageInspectionError::Unsettled);
    }
    Ok(())
}
// Namespace/database names are bound or compared, never interpolated as SQL.
fn storage_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
}
fn persistence(_: impl std::fmt::Debug) -> NativeStorageInspectionError {
    NativeStorageInspectionError::Persistence
}
fn state_error(error: SyncStateError) -> NativeStorageInspectionError {
    match error {
        SyncStateError::CorruptState => NativeStorageInspectionError::CorruptState,
        SyncStateError::StateTooLarge => NativeStorageInspectionError::MetadataLimit,
        _ => NativeStorageInspectionError::Persistence,
    }
}
#[derive(SurrealValue)]
struct Metadata {
    count: u64,
    valid: bool,
}
async fn metadata(
    tx: &surrealdb::Surreal<Any>,
    sql: &str,
) -> Result<Metadata, NativeStorageInspectionError> {
    let mut r = tx
        .query(sql)
        .await
        .map_err(persistence)?
        .check()
        .map_err(persistence)?;
    r.take::<Option<Metadata>>(r.num_statements() - 1)
        .map_err(persistence)?
        .ok_or(NativeStorageInspectionError::UnsupportedLayout)
}

/// Observation only: caller retains its lifecycle/owner fence after this returns.
/// No native initializer, schema writer or session-selection operation is invoked.
pub(crate) async fn inspect_storage(
    db: &SurrealDatabase,
    options: NativeStorageInspectionOptions,
) -> Result<NativeStorageInspection, NativeStorageInspectionError> {
    if !db.embedded
        || !storage_name(&options.namespace)
        || !storage_name(&options.database)
        || options.application_tables.len() > 16
        || options.expected_journal.requested_scope.is_empty()
        || options.expected_journal.requested_scope.len() > sync_state::MAX_IDENTITY_BYTES
    {
        return Err(NativeStorageInspectionError::InvalidOptions);
    }
    let app: BTreeSet<_> = options.application_tables.iter().cloned().collect();
    if app.len() != options.application_tables.len()
        || app
            .iter()
            .any(|s| !identifier(s) || s.starts_with("_sync_"))
    {
        return Err(NativeStorageInspectionError::InvalidOptions);
    }
    let partition = PartitionId(options.expected_journal.partition_id.clone());
    let client = ClientId(options.expected_journal.client_id.clone());
    let key = SyncStateKey::new(&partition, &client);
    sync_state::validate_key(key).map_err(|_| NativeStorageInspectionError::InvalidOptions)?;
    let generation = db.session_generation.load(Ordering::Acquire);
    settled(db, generation)?;
    let _guard = db.journal_lifecycle.write().await;
    settled(db, generation)?;
    let client = db.client().await.map_err(persistence)?;
    settled(db, generation)?;
    let result = inspect_transaction(db, generation, &client, &options, &app, key).await;
    settled(db, generation)?;
    result
}

async fn inspect_transaction(
    db: &SurrealDatabase,
    generation: u64,
    tx: &surrealdb::Surreal<Any>,
    options: &NativeStorageInspectionOptions,
    app: &BTreeSet<String>,
    key: SyncStateKey<'_>,
) -> Result<NativeStorageInspection, NativeStorageInspectionError> {
    // Pinned unselected scalar projections require a namespace. Direct INFO is
    // the reviewed metadata-only exception: engine allocation precedes admission.
    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), tx.query("INFO FOR ROOT"))
            .await
            .map_err(|_| NativeStorageInspectionError::Persistence)?;
    let mut response = response
        .map_err(persistence)?
        .check()
        .map_err(persistence)?;
    settled(db, generation)?;
    let root: Value = response.take(0).map_err(persistence)?;
    let Value::Object(root) = &root else {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    };
    let allowed = [
        "accesses",
        "defaults",
        "namespaces",
        "nodes",
        "system",
        "users",
        "config",
    ];
    if root.len() != allowed.len() || root.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    for field in ["accesses", "defaults", "users"] {
        if !matches!(root.get(field),Some(Value::Object(value)) if value.is_empty()) {
            return Err(NativeStorageInspectionError::UnsupportedLayout);
        }
    }
    if !matches!(root.get("config"),Some(Value::Object(value)) if value.len()==1 && value.get("QUERY_TIMEOUT")==Some(&Value::None))
    {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    let Some(Value::Object(namespaces)) = root.get("namespaces") else {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    };
    if namespaces.is_empty() {
        return Ok(NativeStorageInspection::PristineUnselected);
    }
    if namespaces.len() != 1 || !namespaces.contains_key(&options.namespace) {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    let ns = metadata(tx,"LET $s=(INFO FOR NS); RETURN {count:array::len(object::keys($s.databases)),valid:array::len(array::complement(object::keys($s),['accesses','databases','users']))=0 AND $s.accesses={} AND $s.users={}}").await?;
    settled(db, generation)?;
    if !ns.valid || ns.count != 1 {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    let mut r = tx
        .query("LET $s=(INFO FOR NS); RETURN object::keys($s.databases)=[$expected]")
        .bind(("expected", options.database.clone()))
        .await
        .map_err(persistence)?
        .check()
        .map_err(persistence)?;
    let matches: Option<bool> = r.take(r.num_statements() - 1).map_err(persistence)?;
    settled(db, generation)?;
    if matches != Some(true) {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    let schema = metadata(tx,"LET $s=(INFO FOR DB); RETURN {count:array::len(object::keys($s.tables)),valid:array::len(array::complement(object::keys($s),['accesses','apis','analyzers','buckets','functions','modules','models','params','tables','users','configs','sequences']))=0 AND $s.accesses={} AND $s.apis={} AND $s.analyzers={} AND $s.buckets={} AND $s.functions={} AND $s.modules={} AND $s.models={} AND $s.params={} AND $s.users={} AND $s.configs={} AND $s.sequences={}}").await?;
    settled(db, generation)?;
    if !schema.valid {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    if schema.count > 10 + app.len() as u64 {
        return Err(NativeStorageInspectionError::MetadataLimit);
    }
    // Reject giant names inside the engine before returning even a bounded name list.
    let mut r = tx.query("LET $s=(INFO FOR DB); RETURN array::len(object::keys($s.tables)[WHERE bytes::len(<bytes>$this)>64])=0").await.map_err(persistence)?.check().map_err(persistence)?;
    let names_valid: Option<bool> = r.take(r.num_statements() - 1).map_err(persistence)?;
    settled(db, generation)?;
    if names_valid != Some(true) {
        return Err(NativeStorageInspectionError::MetadataLimit);
    }
    let mut r = tx
        .query("LET $s=(INFO FOR DB); RETURN object::keys($s.tables)")
        .await
        .map_err(persistence)?
        .check()
        .map_err(persistence)?;
    let names: Vec<String> = r.take(r.num_statements() - 1).map_err(persistence)?;
    settled(db, generation)?;
    let tables: BTreeSet<String> = names.into_iter().collect();
    if tables.iter().any(|t| {
        !sync_state::JOURNAL_TABLES.contains(&t.as_str())
            && !["_sync_bootstrap_header", "_sync_bootstrap_page"].contains(&t.as_str())
            && !app.contains(t)
    }) {
        return Err(NativeStorageInspectionError::UnsupportedLayout);
    }
    let transaction = tx.clone().begin().await.map_err(persistence)?;
    let tx = &transaction;
    let result = async {
        let journal_rows = sync_state::inspect_all_owners(tx, &tables, key)
            .await
            .map_err(state_error)?;
        settled(db, generation)?;
        let bootstrap_rows = crate::sync_bootstrap_store::inspect_all_owners(tx, &tables, key)
            .await
            .map_err(state_error)?;
        settled(db, generation)?;
        let (state, barrier) = sync_state::inspect_known_tables(tx, &tables, key)
            .await
            .map_err(state_error)?;
        settled(db, generation)?;
        let native = match state {
            None if journal_rows || bootstrap_rows => {
                return Err(NativeStorageInspectionError::CorruptState);
            }
            None => NativeSyncInspection::Missing,
            Some(state) => {
                if state.requested_scope.0 != options.expected_journal.requested_scope
                    || state.subscription_revision != options.expected_journal.subscription_revision
                {
                    return Err(NativeStorageInspectionError::CorruptState);
                }
                crate::sync_codec::validate_state_codec(&state)
                    .map_err(|_| NativeStorageInspectionError::CorruptState)?;
                let runtime = ClientRuntime::open(state)
                    .map_err(|_| NativeStorageInspectionError::CorruptState)?;
                let state = runtime.state();
                NativeSyncInspection::Present {
                    revision: state.revision,
                    pending_count: u32::try_from(state.outbox.len()).map_err(persistence)?,
                    outcome_count: u32::try_from(state.outcomes.len()).map_err(persistence)?,
                    conflict_count: u32::try_from(
                        state
                            .outcomes
                            .iter()
                            .filter(|r| {
                                r.resolution.is_none()
                                    && matches!(r.outcome, DurableOutcome::Conflict { .. })
                            })
                            .count(),
                    )
                    .map_err(persistence)?,
                    barrier: match barrier.as_ref().map(|b| b.reason.as_str()) {
                        None => NativeSyncInspectionBarrier::None,
                        Some("needs_new_scope_journal") => {
                            NativeSyncInspectionBarrier::NeedsNewScopeJournal
                        }
                        Some("pending_quarantine_required") => {
                            NativeSyncInspectionBarrier::PendingQuarantineRequired
                        }
                        _ => return Err(NativeStorageInspectionError::CorruptState),
                    },
                }
            }
        };
        let mut application_tables = Vec::new();
        for table in app {
            let schema_present = tables.contains(table);
            let rows_present = if schema_present {
                let mut r = tx
                    .query(format!("SELECT VALUE true FROM {table} LIMIT 1"))
                    .await
                    .map_err(persistence)?
                    .check()
                    .map_err(persistence)?;
                let rows: Vec<bool> = r.take(0).map_err(persistence)?;
                settled(db, generation)?;
                !rows.is_empty()
            } else {
                false
            };
            application_tables.push(NativeTableOccupancy {
                table: table.clone(),
                schema_present,
                rows_present,
            });
        }
        Ok(NativeStorageInspection::Selected {
            native,
            application_tables,
        })
    }
    .await;
    let cancelled = transaction.cancel().await.map_err(persistence);
    settled(db, generation)?;
    cancelled?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_client::open_sync_client;
    use crate::{ConnectOptions, connect};
    use std::sync::Arc;
    fn options() -> NativeStorageInspectionOptions {
        NativeStorageInspectionOptions {
            namespace: "inspection".into(),
            database: "inspection".into(),
            expected_journal: NativeSyncOpenOptions {
                partition_id: "partition".into(),
                client_id: "client".into(),
                requested_scope: "scope".into(),
                subscription_revision: 1,
            },
            application_tables: vec!["owner_marker".into(), "projection".into()],
        }
    }
    async fn database(selected: bool) -> Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: selected.then(|| "inspection".into()),
            database: selected.then(|| "inspection".into()),
        })
        .await
        .unwrap()
    }
    async fn sql(db: &SurrealDatabase, query: &str) {
        db.query(query.into(), None).await.unwrap();
    }
    #[tokio::test]
    async fn unselected_pristine_and_selected_empty_are_distinct_no_init_observations() {
        let db = database(false).await;

        let generation = db.session_generation.load(Ordering::Acquire);
        assert_eq!(
            inspect_storage(&db, options()).await.unwrap(),
            NativeStorageInspection::PristineUnselected
        );
        assert_eq!(generation, db.session_generation.load(Ordering::Acquire));
        db.use_namespace_database("inspection".into(), "inspection".into())
            .await
            .unwrap();
        assert_eq!(
            inspect_storage(&db, options()).await.unwrap(),
            NativeStorageInspection::Selected {
                native: NativeSyncInspection::Missing,
                application_tables: vec![
                    NativeTableOccupancy {
                        table: "owner_marker".into(),
                        schema_present: false,
                        rows_present: false
                    },
                    NativeTableOccupancy {
                        table: "projection".into(),
                        schema_present: false,
                        rows_present: false
                    }
                ]
            }
        );
        let result = db
            .query(
                "LET $s=(INFO FOR DB); RETURN array::len(object::keys($s.tables))".into(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result.last().unwrap().value_json).unwrap(),
            serde_json::json!({"$surreal":"int","value":"0"})
        );
    }
    #[tokio::test]
    async fn projection_occupancy_does_not_initialize_or_hide_native_state() {
        let db = database(true).await;
        sql(&db, "CREATE projection:one SET value=1").await;
        let NativeStorageInspection::Selected {
            native,
            application_tables,
        } = inspect_storage(&db, options()).await.unwrap()
        else {
            panic!("selected")
        };
        assert_eq!(native, NativeSyncInspection::Missing);
        assert_eq!(
            application_tables[1],
            NativeTableOccupancy {
                table: "projection".into(),
                schema_present: true,
                rows_present: true
            }
        );
    }
    #[tokio::test]
    async fn valid_journal_then_dual_and_third_keys_fail_closed() {
        let db = database(true).await;
        let _owned = open_sync_client(db.clone(), options().expected_journal)
            .await
            .unwrap();
        assert!(matches!(
            inspect_storage(&db, options()).await.unwrap(),
            NativeStorageInspection::Selected {
                native: NativeSyncInspection::Present { .. },
                ..
            }
        ));
        let mut other = options().expected_journal;
        other.client_id = "legacy".into();
        let _legacy = open_sync_client(db.clone(), other).await.unwrap();
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::CorruptState)
        ));
        let mut other = options().expected_journal;
        other.client_id = "third".into();
        let _third = open_sync_client(db.clone(), other).await.unwrap();
        assert!(inspect_storage(&db, options()).await.is_err());
    }
    #[tokio::test]
    async fn every_native_table_rejects_foreign_or_alias_rows_before_blob_decode() {
        for table in sync_state::JOURNAL_TABLES
            .iter()
            .chain(["_sync_bootstrap_header", "_sync_bootstrap_page"].iter())
        {
            let db = database(true).await;
            sql(&db,&format!("CREATE {table}:alias SET partition_id='partition',client_id='client',component_key='x',index='0',payload=NONE")).await;
            assert!(inspect_storage(&db, options()).await.is_err(), "{table}");
        }
    }
    #[tokio::test]
    async fn unsupported_metadata_and_configuration_are_rejected() {
        let db = database(true).await;
        sql(&db, "DEFINE FUNCTION fn::foreign(){RETURN true}").await;
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::UnsupportedLayout)
        ));
        let db = database(true).await;
        sql(&db, "CREATE foreign:one").await;
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::UnsupportedLayout)
        ));
        let db = database(false).await;
        let mut o = options();
        o.application_tables = vec!["_sync_client_meta".into()];
        assert!(matches!(
            inspect_storage(&db, o).await,
            Err(NativeStorageInspectionError::InvalidOptions)
        ));
        let mut o = options();
        o.application_tables = vec!["injected;DELETE".into()];
        assert!(matches!(
            inspect_storage(&db, o).await,
            Err(NativeStorageInspectionError::InvalidOptions)
        ));
    }
    #[tokio::test]
    async fn metadata_count_and_name_budgets_are_checked_before_names_are_returned() {
        let db = database(true).await;
        for n in 0..27 {
            sql(&db, &format!("DEFINE TABLE foreign{n}")).await;
        }
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::MetadataLimit)
        ));
        let db = database(true).await;
        sql(&db, &format!("DEFINE TABLE {}", "x".repeat(65))).await;
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::MetadataLimit)
        ));
    }
    #[tokio::test]
    async fn lifecycle_write_waits_for_workers_and_session_reservation_withdraws_waiter() {
        let db = database(true).await;
        let worker = db.journal_lifecycle.read().await;
        let future = inspect_storage(&db, options());
        tokio::pin!(future);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut future)
                .await
                .is_err()
        );
        db.session_generation.fetch_add(1, Ordering::AcqRel);
        drop(worker);
        assert!(matches!(
            future.await,
            Err(NativeStorageInspectionError::Unsettled)
        ));
    }
    #[tokio::test]
    async fn wrong_selection_or_closed_storage_never_returns_known_state() {
        let db = database(true).await;
        let mut o = options();
        o.namespace = "other".into();
        assert!(inspect_storage(&db, o).await.is_err());
        db.close().await.unwrap();
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::Unsettled)
        ));
    }
    #[tokio::test]
    async fn sticky_poison_before_or_during_lifecycle_wait_is_never_known_storage() {
        let db = database(true).await;
        db.sync_runtimes.poison_for_test();
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::Unsettled)
        ));
        let db = database(true).await;
        let worker = db.journal_lifecycle.read().await;
        let future = inspect_storage(&db, options());
        tokio::pin!(future);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut future)
                .await
                .is_err()
        );
        db.sync_runtimes.poison_for_test();
        drop(worker);
        assert!(matches!(
            future.await,
            Err(NativeStorageInspectionError::Unsettled)
        ));
    }
    #[tokio::test]
    async fn null_none_wrong_owner_and_structured_alias_all_fail_closed() {
        for owner in ["NONE", "NULL", "123", "'foreign'"] {
            let db = database(true).await;
            sql(&db,&format!("CREATE _sync_client_meta:['partition','client'] SET partition_id={owner},client_id='client'")).await;
            assert!(inspect_storage(&db, options()).await.is_err());
        }
    }
    #[tokio::test]
    async fn root_users_and_extra_namespaces_never_look_pristine() {
        let db = database(false).await;
        sql(
            &db,
            "DEFINE USER hidden ON ROOT PASSWORD 'synthetic-only' ROLES OWNER",
        )
        .await;
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::UnsupportedLayout)
        ));
        let db = database(true).await;
        sql(&db, "DEFINE NAMESPACE foreign").await;
        assert!(matches!(
            inspect_storage(&db, options()).await,
            Err(NativeStorageInspectionError::UnsupportedLayout)
        ));
    }
    #[tokio::test]
    async fn expected_component_without_state_and_partial_bootstrap_schema_are_not_absent() {
        let db = database(true).await;
        sql(&db,"CREATE _sync_client_outbox:['partition','client','commit'] SET partition_id='partition',client_id='client',component_key='commit',payload=NONE").await;
        assert!(inspect_storage(&db, options()).await.is_err());
        let db = database(true).await;
        sql(&db, "DEFINE TABLE _sync_bootstrap_header").await;
        assert!(inspect_storage(&db, options()).await.is_err());
    }
    #[tokio::test]
    #[ignore = "explicit persistent reopen conformance gate"]
    async fn surrealkv_reopen_preserves_occupancy_and_never_creates_tables() {
        let path =
            std::env::var("STORAGE_INSPECTION_FIXTURE").expect("owned preserved fixture path");
        let endpoint = format!("surrealkv://{path}");
        let db = connect(ConnectOptions {
            endpoint: endpoint.clone(),
            namespace: Some("inspection".into()),
            database: Some("inspection".into()),
        })
        .await
        .unwrap();
        sql(&db, "CREATE owner_marker:one SET phase='preparing'").await;
        let before = inspect_storage(&db, options()).await.unwrap();
        db.close().await.unwrap();
        drop(db);
        let db = connect(ConnectOptions {
            endpoint,
            namespace: Some("inspection".into()),
            database: Some("inspection".into()),
        })
        .await
        .unwrap();
        assert_eq!(inspect_storage(&db, options()).await.unwrap(), before);
        let schema = db.query("INFO FOR DB".into(), None).await.unwrap();
        let value: serde_json::Value = serde_json::from_str(&schema[0].value_json).unwrap();
        assert_eq!(
            value["tables"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["owner_marker"]
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn hyphenated_storage_names_are_bound_and_inspection_never_initializes() {
        let db = database(false).await;
        let mut expected = options();
        expected.namespace = "household-local".into();
        expected.database = "household-local".into();
        assert_eq!(
            inspect_storage(&db, expected).await.unwrap(),
            NativeStorageInspection::PristineUnselected
        );
        db.use_namespace_database("household-local".into(), "household-local".into())
            .await
            .unwrap();
        let mut expected = options();
        expected.namespace = "household-local".into();
        expected.database = "household-local".into();
        assert!(matches!(
            inspect_storage(&db, expected).await.unwrap(),
            NativeStorageInspection::Selected {
                native: NativeSyncInspection::Missing,
                ..
            }
        ));
        let result = db.query("INFO FOR DB".into(), None).await.unwrap();
        let value: serde_json::Value = serde_json::from_str(&result[0].value_json).unwrap();
        assert!(value["tables"].as_object().unwrap().is_empty());
    }
}
