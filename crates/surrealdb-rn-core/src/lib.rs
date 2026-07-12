//! Native database core for `react-native-surrealdb`.
//!
//! The exported surface is intentionally small. TypeScript owns the ergonomic
//! SDK while Rust owns engine selection, connection lifetime, authentication,
//! query execution, and lossless value transport.

mod wire;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect as connect_any};
use surrealdb::opt::auth::{Database as DatabaseCredentials, Root, Token};
use surrealdb::types::{Object, Value};

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
    // Surreal is cheaply cloneable. Taking a clone while holding this mutex lets
    // async work proceed without retaining a lock across an await point.
    inner: Mutex<Option<Surreal<Any>>>,
}

impl SurrealDatabase {
    fn client(&self) -> Result<Surreal<Any>, SurrealRnError> {
        self.inner
            .lock()
            .map_err(|_| SurrealRnError::Internal {
                message: "database handle lock poisoned".into(),
            })?
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
        inner: Mutex::new(Some(client)),
    }))
}

#[uniffi::export(async_runtime = "tokio")]
impl SurrealDatabase {
    pub async fn use_namespace_database(
        &self,
        namespace: String,
        database: String,
    ) -> Result<(), SurrealRnError> {
        self.client()?.use_ns(namespace).use_db(database).await?;
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
        let client = self.client()?;
        let variables: Object = decode_variables(variables_json.as_deref())?;
        let mut response = client.query(surql).bind(variables).await?;
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

    pub async fn authenticate(&self, access_token: String) -> Result<(), SurrealRnError> {
        self.client()?
            .authenticate(Token::from(access_token))
            .await?;
        Ok(())
    }

    pub async fn sign_in_root(
        &self,
        username: String,
        password: String,
    ) -> Result<String, SurrealRnError> {
        let token = self.client()?.signin(Root { username, password }).await?;
        Ok(token.access.as_insecure_token().to_owned())
    }

    pub async fn sign_in_database(
        &self,
        namespace: String,
        database: String,
        username: String,
        password: String,
    ) -> Result<String, SurrealRnError> {
        let token = self
            .client()?
            .signin(DatabaseCredentials {
                namespace,
                database,
                username,
                password,
            })
            .await?;
        Ok(token.access.as_insecure_token().to_owned())
    }

    pub async fn invalidate(&self) -> Result<(), SurrealRnError> {
        self.client()?.invalidate().await?;
        Ok(())
    }

    /// Idempotently prevent new operations and release this handle's client.
    pub async fn close(&self) -> Result<(), SurrealRnError> {
        {
            let mut slot = self.inner.lock().map_err(|_| SurrealRnError::Internal {
                message: "database handle lock poisoned".into(),
            })?;
            slot.take();
        }
        // The SDK owns its embedded router task. Dropping the last client closes
        // its route channel; yield so that task can begin datastore shutdown.
        tokio::task::yield_now().await;
        Ok(())
    }

    pub fn is_closed(&self) -> Result<bool, SurrealRnError> {
        let slot = self.inner.lock().map_err(|_| SurrealRnError::Internal {
            message: "database handle lock poisoned".into(),
        })?;
        Ok(slot.is_none())
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
        assert!(database.is_closed().unwrap());
        assert!(matches!(
            database.query("RETURN 1".into(), None).await,
            Err(SurrealRnError::Closed)
        ));
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
