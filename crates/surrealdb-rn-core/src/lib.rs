//! Native database core for `react-native-surrealdb`.
//!
//! The exported surface is intentionally small. TypeScript owns the ergonomic
//! SDK while Rust owns engine selection, connection lifetime, authentication,
//! query execution, and lossless value transport.

mod wire;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect as connect_any};
use surrealdb::method::Transaction as NativeTransaction;
use surrealdb::opt::auth::{Database as DatabaseCredentials, Root, Token};
use surrealdb::types::{Action, Object, Value};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock, watch};

use futures::StreamExt;

use crate::wire::{decode_variables, encode_value};

uniffi::setup_scaffolding!();

#[derive(Debug, Clone, uniffi::Record)]
pub struct ConnectOptions {
    pub endpoint: String,
    pub namespace: Option<String>,
    pub database: Option<String>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct QueryStatementResult {
    pub statement_index: u32,
    pub value_json: String,
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum LiveAction {
    Create,
    Update,
    Delete,
    Error,
    Unknown,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct LiveNotification {
    pub query_id: String,
    pub action: LiveAction,
    pub value_json: String,
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SurrealRnError {
    #[error("invalid endpoint: {message}")]
    InvalidEndpoint { message: String },

    #[error("invalid variables: {message}")]
    InvalidVariables { message: String },

    #[error("database is closed")]
    Closed,

    #[error("database error: {message}")]
    Database { message: String },

    #[error("value codec error: {message}")]
    Codec { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

impl From<surrealdb::Error> for SurrealRnError {
    fn from(error: surrealdb::Error) -> Self {
        Self::Database {
            message: error.to_string(),
        }
    }
}

#[derive(uniffi::Object)]
pub struct SurrealDatabase {
    // Reads only hold the lock long enough to clone the cheap SDK handle.
    // Session mutations use the write side across their await point, preventing
    // concurrent sign-in/use calls from silently overwriting one another.
    inner: AsyncRwLock<Option<Surreal<Any>>>,
    live_queries: Mutex<Vec<Weak<LiveQuery>>>,
    transactions: Mutex<Vec<Weak<SurrealTransaction>>>,
    closed: AtomicBool,
}

#[derive(uniffi::Object)]
pub struct SurrealTransaction {
    inner: AsyncMutex<Option<NativeTransaction<Any>>>,
    closed: AtomicBool,
}

#[derive(uniffi::Object)]
pub struct LiveQuery {
    stream: AsyncMutex<Option<surrealdb::method::QueryStream<Value>>>,
    response: Mutex<Option<surrealdb::IndexedResults>>,
    client: Mutex<Option<Surreal<Any>>>,
    closed: AtomicBool,
    close_signal: watch::Sender<bool>,
}

impl SurrealTransaction {
    async fn cancel_inner(&self) -> Result<(), SurrealRnError> {
        self.closed.store(true, Ordering::Release);
        if let Some(transaction) = self.inner.lock().await.take() {
            transaction.cancel().await?;
        }
        Ok(())
    }
}

impl Drop for SurrealTransaction {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let Some(transaction) = self.inner.get_mut().take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                transaction.cancel().await.ok();
            });
        }
    }
}

impl LiveQuery {
    fn request_close(&self) -> bool {
        if self.closed.swap(true, Ordering::AcqRel) {
            false
        } else {
            self.close_signal.send(true).ok();
            true
        }
    }

    async fn close_inner(&self) {
        self.request_close();
        self.stream.lock().await.take();
        if let Ok(mut response) = self.response.lock() {
            response.take();
        }
        if let Ok(mut client) = self.client.lock() {
            client.take();
        }
    }
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        self.request_close();
        self.stream.get_mut().take();
        if let Ok(response) = self.response.get_mut() {
            response.take();
        }
        if let Ok(client) = self.client.get_mut() {
            client.take();
        }
    }
}

impl SurrealDatabase {
    async fn client(&self) -> Result<Surreal<Any>, SurrealRnError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SurrealRnError::Closed);
        }
        self.inner
            .read()
            .await
            .clone()
            .ok_or(SurrealRnError::Closed)
    }
}

// UniFFI futures are polled by the foreign runtime. The Tokio compatibility
// adapter supplies a reactor even when Hermes invokes us from a native thread.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect(options: ConnectOptions) -> Result<Arc<SurrealDatabase>, SurrealRnError> {
    validate_endpoint(&options.endpoint)?;

    let client = connect_with_shutdown_retry(&options.endpoint).await?;

    match (options.namespace, options.database) {
        (Some(namespace), Some(database)) => {
            client.use_ns(namespace).use_db(database).await?;
        }
        (Some(namespace), None) => {
            client.use_ns(namespace).await?;
        }
        (None, Some(_)) => {
            return Err(SurrealRnError::InvalidEndpoint {
                message: "database requires a namespace".into(),
            });
        }
        (None, None) => {}
    }

    Ok(Arc::new(SurrealDatabase {
        inner: AsyncRwLock::new(Some(client)),
        live_queries: Mutex::new(Vec::new()),
        transactions: Mutex::new(Vec::new()),
        closed: AtomicBool::new(false),
    }))
}

#[uniffi::export(async_runtime = "tokio")]
impl SurrealTransaction {
    /// Execute one or more statements inside this transaction.
    pub async fn query(
        &self,
        surql: String,
        variables_json: Option<String>,
    ) -> Result<Vec<QueryStatementResult>, SurrealRnError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SurrealRnError::Closed);
        }
        let variables: Object = decode_variables(variables_json.as_deref())?;
        let slot = self.inner.lock().await;
        let transaction = slot.as_ref().ok_or(SurrealRnError::Closed)?;
        let response = transaction.query(surql).bind(variables).await?.check()?;
        query_results(response)
    }

    /// Commit once, persisting all queries executed through this handle.
    pub async fn commit(&self) -> Result<(), SurrealRnError> {
        self.closed.store(true, Ordering::Release);
        if let Some(transaction) = self.inner.lock().await.take() {
            transaction.commit().await?;
        }
        Ok(())
    }

    /// Idempotently roll back all queries executed through this handle.
    pub async fn cancel(&self) -> Result<(), SurrealRnError> {
        self.cancel_inner().await
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl LiveQuery {
    pub async fn next(&self) -> Result<Option<LiveNotification>, SurrealRnError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }

        let mut close_signal = self.close_signal.subscribe();
        let mut slot = self.stream.lock().await;
        if self.closed.load(Ordering::Acquire) || *close_signal.borrow() {
            return Ok(None);
        }
        let Some(stream) = slot.as_mut() else {
            return Ok(None);
        };

        tokio::select! {
            changed = close_signal.changed() => {
                changed.ok();
                Ok(None)
            }
            notification = stream.next() => {
                match notification {
                    Some(Ok(notification)) => Ok(Some(LiveNotification {
                        query_id: notification.query_id.to_string(),
                        action: live_action(notification.action),
                        value_json: encode_value(notification.data)?,
                    })),
                    Some(Err(error)) => Err(error.into()),
                    None => {
                        self.closed.store(true, Ordering::Release);
                        slot.take();
                        Ok(None)
                    }
                }
            }
        }
    }

    pub async fn close(&self) -> Result<(), SurrealRnError> {
        self.close_inner().await;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl SurrealDatabase {
    /// Begin a transaction whose queries can be issued individually.
    pub async fn begin_transaction(&self) -> Result<Arc<SurrealTransaction>, SurrealRnError> {
        let client = self.client().await?;
        let native_transaction = client.begin().await?;
        let transaction = Arc::new(SurrealTransaction {
            inner: AsyncMutex::new(Some(native_transaction)),
            closed: AtomicBool::new(false),
        });

        {
            let mut transactions =
                self.transactions
                    .lock()
                    .map_err(|_| SurrealRnError::Internal {
                        message: "transaction registry lock poisoned".into(),
                    })?;
            if self.closed.load(Ordering::Acquire) {
                drop(transactions);
                transaction.cancel_inner().await?;
                return Err(SurrealRnError::Closed);
            }
            transactions.retain(|transaction| transaction.strong_count() > 0);
            transactions.push(Arc::downgrade(&transaction));
        }

        Ok(transaction)
    }

    pub async fn use_namespace_database(
        &self,
        namespace: String,
        database: String,
    ) -> Result<(), SurrealRnError> {
        let mut slot = self.inner.write().await;
        let client = slot.clone().ok_or(SurrealRnError::Closed)?;
        client.use_ns(namespace).use_db(database).await?;
        *slot = Some(client);
        Ok(())
    }

    /// Execute one or more SurrealQL statements.
    ///
    /// `variables_json` uses the versioned wire representation documented in
    /// `wire.rs`. Results contain one encoded value per statement.
    pub async fn query(
        &self,
        surql: String,
        variables_json: Option<String>,
    ) -> Result<Vec<QueryStatementResult>, SurrealRnError> {
        let client = self.client().await?;
        let variables: Object = decode_variables(variables_json.as_deref())?;
        let response = client.query(surql).bind(variables).await?.check()?;
        query_results(response)
    }

    /// Start a single live SurrealQL statement and return a pull-based stream.
    ///
    /// Pulling avoids invoking JavaScript from a Rust worker thread, provides
    /// natural backpressure, and maps cleanly to an async iterator in TypeScript.
    pub async fn live_query(
        &self,
        surql: String,
        variables_json: Option<String>,
    ) -> Result<Arc<LiveQuery>, SurrealRnError> {
        let client = self.client().await?;
        let variables: Object = decode_variables(variables_json.as_deref())?;
        let mut response = client.query(surql).bind(variables).await?;
        let stream = response.stream::<Value>(0)?;
        let (close_signal, _) = watch::channel(false);
        let live_query = Arc::new(LiveQuery {
            stream: AsyncMutex::new(Some(stream)),
            response: Mutex::new(Some(response)),
            client: Mutex::new(Some(client)),
            closed: AtomicBool::new(false),
            close_signal,
        });

        let mut subscriptions = self
            .live_queries
            .lock()
            .map_err(|_| SurrealRnError::Internal {
                message: "live query registry lock poisoned".into(),
            })?;
        subscriptions.retain(|query| query.strong_count() > 0);
        subscriptions.push(Arc::downgrade(&live_query));

        Ok(live_query)
    }

    pub async fn authenticate(&self, access_token: String) -> Result<(), SurrealRnError> {
        let mut slot = self.inner.write().await;
        let client = slot.clone().ok_or(SurrealRnError::Closed)?;
        client.authenticate(Token::from(access_token)).await?;
        *slot = Some(client);
        Ok(())
    }

    pub async fn sign_in_root(
        &self,
        username: String,
        password: String,
    ) -> Result<String, SurrealRnError> {
        let mut slot = self.inner.write().await;
        let client = slot.clone().ok_or(SurrealRnError::Closed)?;
        let token = client.signin(Root { username, password }).await?;
        let access_token = token.access.as_insecure_token().to_owned();
        client.authenticate(token).await?;
        *slot = Some(client);
        Ok(access_token)
    }

    pub async fn sign_in_database(
        &self,
        namespace: String,
        database: String,
        username: String,
        password: String,
    ) -> Result<String, SurrealRnError> {
        let mut slot = self.inner.write().await;
        let client = slot.clone().ok_or(SurrealRnError::Closed)?;
        let token = client
            .signin(DatabaseCredentials {
                namespace,
                database,
                username,
                password,
            })
            .await?;
        let access_token = token.access.as_insecure_token().to_owned();
        client.authenticate(token).await?;
        *slot = Some(client);
        Ok(access_token)
    }

    pub async fn invalidate(&self) -> Result<(), SurrealRnError> {
        let mut slot = self.inner.write().await;
        let client = slot.clone().ok_or(SurrealRnError::Closed)?;
        client.invalidate().await?;
        *slot = Some(client);
        Ok(())
    }

    /// Idempotently prevent new operations and release this handle's client.
    pub async fn close(&self) -> Result<(), SurrealRnError> {
        self.closed.store(true, Ordering::Release);
        self.inner.write().await.take();
        let live_queries = {
            let mut subscriptions =
                self.live_queries
                    .lock()
                    .map_err(|_| SurrealRnError::Internal {
                        message: "live query registry lock poisoned".into(),
                    })?;
            let live_queries = subscriptions
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            subscriptions.clear();
            live_queries
        };
        for live_query in live_queries {
            live_query.close_inner().await;
        }
        let transactions = {
            let mut transaction_registry =
                self.transactions
                    .lock()
                    .map_err(|_| SurrealRnError::Internal {
                        message: "transaction registry lock poisoned".into(),
                    })?;
            let transactions = transaction_registry
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            transaction_registry.clear();
            transactions
        };
        for transaction in transactions {
            transaction.cancel_inner().await?;
        }
        // The SDK owns its embedded router task. Dropping the last client closes
        // its route channel; yield so that task can begin datastore shutdown.
        tokio::task::yield_now().await;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

fn query_results(
    mut response: surrealdb::IndexedResults,
) -> Result<Vec<QueryStatementResult>, SurrealRnError> {
    let statement_count = response.num_statements();
    let mut results = Vec::with_capacity(statement_count);

    for index in 0..statement_count {
        let value: Value = response.take(index)?;
        results.push(QueryStatementResult {
            statement_index: index as u32,
            value_json: encode_value(value)?,
        });
    }

    Ok(results)
}

fn live_action(action: Action) -> LiveAction {
    match action {
        Action::Create => LiveAction::Create,
        Action::Update => LiveAction::Update,
        Action::Delete => LiveAction::Delete,
        Action::Error => LiveAction::Error,
        _ => LiveAction::Unknown,
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), SurrealRnError> {
    let supported = endpoint == "memory"
        || endpoint.starts_with("mem://")
        || endpoint.starts_with("surrealkv://")
        || endpoint.starts_with("ws://")
        || endpoint.starts_with("wss://");

    if supported {
        Ok(())
    } else {
        Err(SurrealRnError::InvalidEndpoint {
            message: format!(
                "unsupported endpoint '{endpoint}'; expected mem://, surrealkv://, ws://, or wss://"
            ),
        })
    }
}

async fn connect_with_shutdown_retry(endpoint: &str) -> Result<Surreal<Any>, SurrealRnError> {
    const MAX_ATTEMPTS: usize = 21;
    const RETRY_DELAY: Duration = Duration::from_millis(25);

    for attempt in 0..MAX_ATTEMPTS {
        match connect_any(endpoint).await {
            Ok(client) => return Ok(client),
            Err(error)
                if endpoint.starts_with("surrealkv://")
                    && error.to_string().contains("already locked")
                    && attempt + 1 < MAX_ATTEMPTS =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error.into()),
        }
    }

    Err(SurrealRnError::Internal {
        message: "unreachable connection retry state".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_allowlist_rejects_http_and_unknown_schemes() {
        assert!(validate_endpoint("mem://").is_ok());
        assert!(validate_endpoint("surrealkv:///tmp/test").is_ok());
        assert!(validate_endpoint("wss://example.test").is_ok());
        assert!(validate_endpoint("https://example.test").is_err());
        assert!(validate_endpoint("file:///tmp/test").is_err());
    }

    #[tokio::test]
    async fn memory_engine_executes_queries_and_closes_idempotently() {
        let database = connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("test".into()),
            database: Some("test".into()),
        })
        .await
        .unwrap();

        let results = database
            .query(
                "RETURN $large; RETURN { id: person:one, active: true };".into(),
                Some(r#"{"large":{"$surreal":"int","value":"9007199254740993"}}"#.into()),
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].value_json,
            r#"{"$surreal":"int","value":"9007199254740993"}"#
        );
        assert!(results[1].value_json.contains(r#""$surreal":"record""#));

        database.close().await.unwrap();
        database.close().await.unwrap();
        assert!(database.is_closed());
        assert!(matches!(
            database.query("RETURN 1".into(), None).await,
            Err(SurrealRnError::Closed)
        ));
    }

    #[tokio::test]
    async fn transaction_handle_commits_and_cancels_individual_queries() {
        let database = connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("transactions".into()),
            database: Some("transactions".into()),
        })
        .await
        .unwrap();
        database
            .query("DEFINE TABLE person SCHEMALESS".into(), None)
            .await
            .unwrap();

        let transaction = database.begin_transaction().await.unwrap();
        transaction
            .query(
                "CREATE person:ada SET name = 'Ada' RETURN NONE".into(),
                None,
            )
            .await
            .unwrap();
        transaction
            .query(
                "CREATE person:lin SET name = 'Lin' RETURN NONE".into(),
                None,
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        transaction.commit().await.unwrap();
        assert!(transaction.is_closed());

        let result = database
            .query("SELECT VALUE name FROM person ORDER BY name".into(), None)
            .await
            .unwrap();
        assert_eq!(result[0].value_json, r#"["Ada","Lin"]"#);

        let cancelled = database.begin_transaction().await.unwrap();
        cancelled
            .query(
                "CREATE person:grace SET name = 'Grace' RETURN NONE".into(),
                None,
            )
            .await
            .unwrap();
        cancelled.cancel().await.unwrap();
        cancelled.cancel().await.unwrap();
        assert!(cancelled.is_closed());

        let result = database
            .query("RETURN record::exists(person:grace)".into(), None)
            .await
            .unwrap();
        assert_eq!(result[0].value_json, "false");
    }

    #[tokio::test]
    async fn database_close_cancels_open_transactions() {
        let database = connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("transaction_close".into()),
            database: Some("transaction_close".into()),
        })
        .await
        .unwrap();
        let transaction = database.begin_transaction().await.unwrap();
        transaction
            .query("CREATE event:uncommitted RETURN NONE".into(), None)
            .await
            .unwrap();

        database.close().await.unwrap();

        assert!(transaction.is_closed());
        assert!(matches!(
            transaction.query("RETURN 1".into(), None).await,
            Err(SurrealRnError::Closed)
        ));
    }

    #[tokio::test]
    async fn live_query_delivers_notifications_and_database_close_cancels_next() {
        let database = connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("live".into()),
            database: Some("live".into()),
        })
        .await
        .unwrap();

        database
            .query("DEFINE TABLE event SCHEMALESS".into(), None)
            .await
            .unwrap();
        let live_query = database
            .live_query("LIVE SELECT * FROM event".into(), None)
            .await
            .unwrap();
        database
            .query("CREATE event:ada SET name = 'Ada'".into(), None)
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(2), live_query.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(notification.action, LiveAction::Create));
        assert!(notification.value_json.contains("Ada"));

        let pending_query = Arc::clone(&live_query);
        let pending_next = tokio::spawn(async move { pending_query.next().await });
        tokio::task::yield_now().await;
        database.close().await.unwrap();

        let pending_result = tokio::time::timeout(Duration::from_secs(2), pending_next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(pending_result.is_none());
        assert!(live_query.is_closed());
    }

    /// Run explicitly against a local authenticated SurrealDB server:
    ///
    /// `SURREAL_TEST_WS_ENDPOINT=ws://127.0.0.1:18080 cargo test \
    ///   -p surrealdb-rn-core authenticated_websocket_live_query -- --ignored`
    #[tokio::test]
    #[ignore = "requires an authenticated SurrealDB server"]
    async fn authenticated_websocket_live_query() {
        let endpoint = std::env::var("SURREAL_TEST_WS_ENDPOINT")
            .unwrap_or_else(|_| "ws://127.0.0.1:18080".into());
        let username = std::env::var("SURREAL_TEST_WS_USERNAME").unwrap_or_else(|_| "root".into());
        let password = std::env::var("SURREAL_TEST_WS_PASSWORD").unwrap_or_else(|_| "root".into());
        let database = connect(ConnectOptions {
            endpoint,
            namespace: None,
            database: None,
        })
        .await
        .unwrap();

        let token = database.sign_in_root(username, password).await.unwrap();
        assert!(!token.is_empty());
        database
            .use_namespace_database("rn_remote".into(), "rn_remote".into())
            .await
            .unwrap();
        database
            .query("DEFINE TABLE event SCHEMALESS".into(), None)
            .await
            .unwrap();

        let live_query = database
            .live_query("LIVE SELECT * FROM event".into(), None)
            .await
            .unwrap();
        database
            .query(
                "CREATE event:remote SET transport = 'websocket'".into(),
                None,
            )
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(5), live_query.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(notification.action, LiveAction::Create));
        assert!(notification.value_json.contains("websocket"));

        live_query.close().await.unwrap();
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn surrealkv_persists_across_handles() {
        let directory = temp_dir::TempDir::new().unwrap();
        let endpoint = format!("surrealkv://{}", directory.path().display());
        let options = ConnectOptions {
            endpoint,
            namespace: Some("test".into()),
            database: Some("test".into()),
        };

        let first = connect(options.clone()).await.unwrap();
        first
            .query("CREATE person:one SET name = 'Ada'".into(), None)
            .await
            .unwrap();
        first.close().await.unwrap();
        drop(first);

        let second = connect(options).await.unwrap();
        let result = second
            .query("RETURN (SELECT VALUE name FROM person:one)[0]".into(), None)
            .await
            .unwrap();
        assert_eq!(result[0].value_json, r#""Ada""#);
    }
}
