//! One-time export/import migration of SurrealKV stores written by SurrealDB 3.x.
//!
//! SurrealDB 4 refuses to open 3.x SurrealKV directories (the upstream in-place
//! upgrade is experimental and one-way). Instead we read the old directory with a
//! pinned 3.x engine, export every database as SurrealQL, and import it into a
//! fresh 4.x store at the same path.
//!
//! Crash safety (all renames are atomic on one filesystem):
//! 1. `<store>.v3-export/` holds the exports; it is recreated on every attempt.
//! 2. `<store>` is renamed to `<store>.v3-backup` before the new store is created.
//! 3. Only after the import has been verified is the backup renamed to
//!    `<store>.v3-obsolete` and deleted.
//!
//! If a previous attempt died after step 2, the partial 4.x store is discarded
//! and the backup is restored before retrying. Anything unexpected fails closed
//! and leaves the 3.x data in place.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::SurrealRnError;

const REFUSAL_MARKER: &str = "SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X";

/// True when a 4.x open failed only because the directory holds 3.x data.
pub(crate) fn is_v3_refusal(error: &surrealdb::Error) -> bool {
    error.to_string().contains(REFUSAL_MARKER)
}

pub(crate) fn store_path(endpoint: &str) -> Option<PathBuf> {
    endpoint
        .strip_prefix("surrealkv://")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

/// Dropped SurrealKV handles release their file lock asynchronously, so opens
/// retry briefly on the lock error (same policy as `connect_with_shutdown_retry`).
async fn with_lock_retry<T, E: std::fmt::Display, F: std::future::Future<Output = Result<T, E>>>(
    mut open: impl FnMut() -> F,
) -> Result<T, E> {
    let mut attempt = 0;
    loop {
        match open().await {
            Err(error) if attempt < 40 && error.to_string().contains("already locked") => {
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            result => return result,
        }
    }
}

/// SurrealKV shuts a dropped handle down on a background task and only then
/// releases `LOCK`. Wait until the lock can be taken so no late write lands in
/// a directory we are about to move.
async fn wait_until_released(store: &Path) -> Result<(), SurrealRnError> {
    let lock = store.join("LOCK");
    for _ in 0..400 {
        let file = match std::fs::File::open(&lock) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(failed("waiting for the store to close", error)),
        };
        // fs2 issues the same flock as SurrealKV; std's File::try_lock
        // reports Unsupported on Android, so it never succeeded there.
        if fs2::FileExt::try_lock_exclusive(&file).is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Err(failed(
        "waiting for the store to close",
        "the store lock was not released",
    ))
}

fn sibling(store: &Path, suffix: &str) -> PathBuf {
    let mut name = store.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn failed(stage: &str, error: impl std::fmt::Display) -> SurrealRnError {
    SurrealRnError::Database {
        message: format!("SurrealDB 3.x store migration failed while {stage}: {error}"),
    }
}

fn unsupported(reason: &str) -> SurrealRnError {
    SurrealRnError::Database {
        message: format!(
            "SurrealDB 3.x store cannot be migrated automatically: {reason}; the 3.x data was left unchanged"
        ),
    }
}

/// Undo a migration attempt that stopped after the store was moved aside.
pub(crate) fn recover_interrupted(store: &Path) -> Result<(), SurrealRnError> {
    let obsolete = sibling(store, ".v3-obsolete");
    if obsolete.exists() {
        std::fs::remove_dir_all(&obsolete).map_err(|e| failed("removing an obsolete backup", e))?;
    }
    let backup = sibling(store, ".v3-backup");
    if backup.exists() {
        if store.exists() {
            std::fs::remove_dir_all(store)
                .map_err(|e| failed("discarding a partial 4.x store", e))?;
        }
        std::fs::rename(&backup, store).map_err(|e| failed("restoring the 3.x backup", e))?;
    }
    Ok(())
}

struct Exported {
    namespace: String,
    database: String,
    file: PathBuf,
    counts: BTreeMap<String, i64>,
}

fn object_keys(value: &serde_json::Value, field: &str) -> Result<Vec<String>, SurrealRnError> {
    match value.get(field) {
        Some(serde_json::Value::Object(map)) => Ok(map.keys().cloned().collect()),
        _ => Err(unsupported(&format!("INFO output has no '{field}' object"))),
    }
}

fn require_empty(
    value: &serde_json::Value,
    fields: &[&str],
    scope: &str,
) -> Result<(), SurrealRnError> {
    for field in fields {
        if let Some(serde_json::Value::Object(map)) = value.get(*field)
            && !map.is_empty()
        {
            return Err(unsupported(&format!(
                "{scope} defines {field}, which an export cannot carry"
            )));
        }
    }
    Ok(())
}

type DatastoreV3 = surrealdb_core_v3::kvs::Datastore;
type SessionV3 = surrealdb_core_v3::dbs::Session;

async fn query_v3(
    ds: &DatastoreV3,
    session: &SessionV3,
    sql: &str,
    vars: Option<surrealdb_v3::types::Variables>,
    stage: &str,
) -> Result<surrealdb_v3::types::Value, SurrealRnError> {
    let mut results = ds
        .execute(sql, session, vars)
        .await
        .map_err(|e| failed(stage, e))?;
    if results.len() != 1 {
        return Err(failed(stage, "unexpected number of statement results"));
    }
    results.remove(0).result.map_err(|e| failed(stage, e))
}

async fn info_v3(
    ds: &DatastoreV3,
    session: &SessionV3,
    sql: &str,
) -> Result<serde_json::Value, SurrealRnError> {
    Ok(query_v3(ds, session, sql, None, "reading 3.x metadata")
        .await?
        .into_json_value())
}

async fn count_v3(
    ds: &DatastoreV3,
    session: &SessionV3,
    table: &str,
) -> Result<i64, SurrealRnError> {
    let mut vars = surrealdb_v3::types::Variables::new();
    vars.insert("table", table.to_owned());
    let value = query_v3(
        ds,
        session,
        "RETURN count(SELECT VALUE id FROM type::table($table))",
        Some(vars),
        "counting 3.x rows",
    )
    .await?;
    match value.into_json_value() {
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| failed("counting 3.x rows", "count is not an integer")),
        serde_json::Value::Null => Ok(0),
        other => Err(failed("counting 3.x rows", format!("unexpected count {other}"))),
    }
}

async fn export_v3(
    ds: &DatastoreV3,
    session: &SessionV3,
    file: &Path,
) -> Result<(), SurrealRnError> {
    use std::io::Write;
    let (sender, receiver) = surrealdb_core_v3::channel::bounded::<Vec<u8>>(32);
    let job = ds
        .export(session, sender)
        .await
        .map_err(|e| failed("exporting a 3.x database", e))?;
    let mut output = std::io::BufWriter::new(
        std::fs::File::create(file).map_err(|e| failed("creating an export file", e))?,
    );
    let drain = async {
        while let Ok(chunk) = receiver.recv().await {
            output
                .write_all(&chunk)
                .map_err(|e| failed("writing an export file", e))?;
        }
        Ok::<(), SurrealRnError>(())
    };
    let (exported, written) = tokio::join!(job, drain);
    exported.map_err(|e| failed("exporting a 3.x database", e))?;
    written?;
    output
        .flush()
        .map_err(|e| failed("writing an export file", e))?;
    output
        .get_ref()
        .sync_all()
        .map_err(|e| failed("writing an export file", e))
}

async fn count_v4(
    db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table: &str,
) -> Result<i64, SurrealRnError> {
    let mut response = db
        .query("RETURN count(SELECT VALUE id FROM type::table($table))")
        .bind(("table", table.to_owned()))
        .await
        .map_err(|e| failed("verifying imported rows", e))?;
    let count: Option<i64> = response
        .take(0)
        .map_err(|e| failed("verifying imported rows", e))?;
    Ok(count.unwrap_or(0))
}

async fn export_all(store: &Path, export_dir: &Path) -> Result<Vec<Exported>, SurrealRnError> {
    let endpoint = format!("surrealkv://{}", store.display());
    let ds = with_lock_retry(|| DatastoreV3::new(&endpoint))
        .await
        .map_err(|e| failed("opening the 3.x store", e))?;
    let exported = export_open(&ds, export_dir).await;
    // Shut the 3.x engine down explicitly and wait for it, so its LOCK is free
    // before the store is moved. A dropped SDK handle only shuts down on a
    // background task, which never finished on Android.
    let shutdown = ds.shutdown().await;
    drop(ds);
    let exported = exported?;
    shutdown.map_err(|e| failed("closing the 3.x store", e))?;
    Ok(exported)
}

async fn export_open(ds: &DatastoreV3, export_dir: &Path) -> Result<Vec<Exported>, SurrealRnError> {
    let root = info_v3(ds, &SessionV3::owner(), "INFO FOR ROOT").await?;
    require_empty(&root, &["accesses", "users"], "the root")?;
    let mut exported = Vec::new();
    for namespace in object_keys(&root, "namespaces")? {
        let ns_session = SessionV3::owner().with_ns(&namespace);
        let ns = info_v3(ds, &ns_session, "INFO FOR NS").await?;
        require_empty(&ns, &["accesses", "users"], "a namespace")?;
        for database in object_keys(&ns, "databases")? {
            let session = SessionV3::owner().with_ns(&namespace).with_db(&database);
            let info = info_v3(ds, &session, "INFO FOR DB").await?;
            require_empty(&info, &["accesses", "users"], "a database")?;
            let mut counts = BTreeMap::new();
            for table in object_keys(&info, "tables")? {
                let count = count_v3(ds, &session, &table).await?;
                counts.insert(table, count);
            }
            let file = export_dir.join(format!("{}.surql", exported.len()));
            export_v3(ds, &session, &file).await?;
            exported.push(Exported {
                namespace: namespace.clone(),
                database,
                file,
                counts,
            });
        }
    }
    Ok(exported)
}

async fn import_all(store: &Path, exported: &[Exported]) -> Result<(), SurrealRnError> {
    let endpoint = format!("surrealkv://{}", store.display());
    let db = with_lock_retry(|| {
        let endpoint = endpoint.clone();
        async move { surrealdb::engine::any::connect(endpoint).await }
    })
    .await
    .map_err(|e| failed("creating the 4.x store", e))?;
    for item in exported {
        db.use_ns(&item.namespace)
            .use_db(&item.database)
            .await
            .map_err(|e| failed("selecting a 4.x database", e))?;
        db.import(&item.file)
            .await
            .map_err(|e| failed("importing into 4.x", e))?;
        for (table, expected) in &item.counts {
            let actual = count_v4(&db, table).await?;
            if actual != *expected {
                return Err(failed(
                    "verifying imported rows",
                    format!("table '{table}' has {actual} rows, expected {expected}"),
                ));
            }
        }
    }
    drop(db);
    wait_until_released(store).await
}

/// Convert a 3.x SurrealKV store at `store` into a 4.x store in place.
pub(crate) async fn migrate(store: &Path) -> Result<(), SurrealRnError> {
    let export_dir = sibling(store, ".v3-export");
    let backup = sibling(store, ".v3-backup");
    let obsolete = sibling(store, ".v3-obsolete");
    if export_dir.exists() {
        std::fs::remove_dir_all(&export_dir).map_err(|e| failed("clearing stale exports", e))?;
    }
    std::fs::create_dir_all(&export_dir).map_err(|e| failed("creating the export folder", e))?;

    let exported = export_all(store, &export_dir).await;
    // Release the 3.x store before returning on any path, so a refused store can
    // be reopened (or moved aside below) immediately.
    wait_until_released(store).await?;
    let exported = match exported {
        Ok(exported) => exported,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&export_dir);
            return Err(error);
        }
    };

    std::fs::rename(store, &backup).map_err(|e| failed("moving the 3.x store aside", e))?;
    if let Err(error) = import_all(store, &exported).await {
        recover_interrupted(store)?;
        let _ = std::fs::remove_dir_all(&export_dir);
        return Err(error);
    }
    std::fs::rename(&backup, &obsolete).map_err(|e| failed("retiring the 3.x backup", e))?;
    std::fs::remove_dir_all(&obsolete).map_err(|e| failed("removing the 3.x backup", e))?;
    let _ = std::fs::remove_dir_all(&export_dir);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectOptions, connect};

    async fn seed_v3(store: &Path, statements: &str) {
        let db = surrealdb_v3::engine::any::connect(format!("surrealkv://{}", store.display()))
            .await
            .unwrap();
        db.use_ns("app").use_db("app").await.unwrap();
        db.query(statements).await.unwrap().check().unwrap();
    }

    async fn open_v4(
        store: &Path,
    ) -> Result<std::sync::Arc<crate::SurrealDatabase>, SurrealRnError> {
        connect(ConnectOptions {
            endpoint: format!("surrealkv://{}", store.display()),
            namespace: Some("app".into()),
            database: Some("app".into()),
        })
        .await
    }

    const SEED: &str = "
        DEFINE TABLE item SCHEMAFULL;
        DEFINE FIELD name ON item TYPE string;
        DEFINE FIELD blob ON item TYPE bytes;
        DEFINE FIELD owner ON item TYPE option<record<person>>;
        DEFINE FIELD big ON item TYPE int;
        CREATE person:ada SET name = 'Ada';
        CREATE item:one SET name = 'one', blob = <bytes>'AAH+', owner = person:ada, big = 9223372036854775807;
        CREATE item:two SET name = 'two', blob = <bytes>'', big = -1;
    ";

    #[tokio::test]
    async fn v3_store_is_exported_and_reimported_on_first_open() {
        let directory = temp_dir::TempDir::new().unwrap();
        let store = directory.path().join("app.db");
        seed_v3(&store, SEED).await;

        let db = open_v4(&store).await.unwrap();
        let rows = db
            .query(
                "SELECT name, big, owner.name AS owner FROM item ORDER BY name; INFO FOR DB".into(),
                None,
            )
            .await
            .unwrap();
        let items: serde_json::Value = serde_json::from_str(&rows[0].value_json).unwrap();
        assert_eq!(
            items,
            serde_json::json!([
                {"name":"one","big":{"$surreal":"int","value":"9223372036854775807"},"owner":"Ada"},
                {"name":"two","big":{"$surreal":"int","value":"-1"},"owner":{"$surreal":"none"}}
            ])
        );
        let info: serde_json::Value = serde_json::from_str(&rows[1].value_json).unwrap();
        assert_eq!(
            info["compatibility"],
            serde_json::json!({"$surreal":"int","value":"4"})
        );
        for leftover in [".v3-backup", ".v3-obsolete", ".v3-export"] {
            assert!(
                !sibling(&store, leftover).exists(),
                "{leftover} was left behind"
            );
        }
        db.close().await.unwrap();
        // A second open is a plain 4.x open and keeps the data.
        let db = open_v4(&store).await.unwrap();
        let rows = db
            .query("RETURN count(SELECT * FROM item)".into(), None)
            .await
            .unwrap();
        assert_eq!(rows[0].value_json, r#"{"$surreal":"int","value":"2"}"#);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn stores_with_credentials_are_refused_and_left_untouched() {
        let directory = temp_dir::TempDir::new().unwrap();
        let store = directory.path().join("app.db");
        seed_v3(
            &store,
            "DEFINE USER hidden ON ROOT PASSWORD 'synthetic-only' ROLES OWNER; CREATE item:one SET name = 'kept';",
        )
        .await;

        let error = open_v4(&store).await.err().unwrap();
        assert!(
            error
                .to_string()
                .contains("cannot be migrated automatically"),
            "{error}"
        );
        assert!(!sibling(&store, ".v3-backup").exists());

        let db = surrealdb_v3::engine::any::connect(format!("surrealkv://{}", store.display()))
            .await
            .unwrap();
        db.use_ns("app").use_db("app").await.unwrap();
        let name: Option<String> = db
            .query("RETURN item:one.name")
            .await
            .unwrap()
            .take(0)
            .unwrap();
        assert_eq!(name.as_deref(), Some("kept"));
    }

    #[tokio::test]
    async fn interrupted_migration_restores_the_backup_and_retries() {
        let directory = temp_dir::TempDir::new().unwrap();
        let store = directory.path().join("app.db");
        seed_v3(&store, SEED).await;
        // Simulate a crash after the 3.x store was moved aside and a partial 4.x
        // store had been created.
        std::fs::rename(&store, sibling(&store, ".v3-backup")).unwrap();
        std::fs::create_dir_all(store.join("partial")).unwrap();

        let db = open_v4(&store).await.unwrap();
        let rows = db
            .query("RETURN count(SELECT * FROM item)".into(), None)
            .await
            .unwrap();
        assert_eq!(rows[0].value_json, r#"{"$surreal":"int","value":"2"}"#);
        assert!(!sibling(&store, ".v3-backup").exists());
        db.close().await.unwrap();
    }
}
