//! The main Prisma client.
//!
//! Ties together the query compiler, executor, and database adapter into
//! a single entry point for executing Prisma operations. Supports middleware,
//! query logging, and result extensions.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use prisma_compiler::QueryCompiler;
use prisma_compiler::quaint::connector::ConnectionInfo;
use prisma_compiler::quaint::prelude::{ExternalConnectionInfo, SqlFamily};
use prisma_driver_core::{Provider, SqlComment, SqlDriverAdapter, SqlDriverAdapterFactory};
use prisma_query_executor::QueryExecutor;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::error::ClientError;
use crate::extensions::{self, ResultExtension};
use crate::logging::{LogConfig, QueryEvent};
use crate::middleware::{MiddlewareExecutor, MiddlewareParams, MiddlewareStack};
use crate::query::QueryBuilder;
use crate::transaction::TransactionClient;

/// The main entry point for Prisma database operations.
///
/// Created via `PrismaClient::new()` for basic usage or
/// `PrismaClientBuilder` for advanced configuration (middleware,
/// logging, extensions).
pub struct PrismaClient {
    compiler: QueryCompiler,
    adapter: Mutex<Box<dyn SqlDriverAdapter>>,
    middleware: MiddlewareStack,
    log_config: LogConfig,
    result_extensions: Vec<Box<dyn ResultExtension>>,
    sql_comment: Option<SqlComment>,
    disposed: bool,
}

/// Builder for configuring a `PrismaClient` with middleware, logging,
/// extensions, and SQL commenter tags.
pub struct PrismaClientBuilder<'a> {
    schema: &'a str,
    factory: &'a dyn SqlDriverAdapterFactory,
    middleware: MiddlewareStack,
    log_config: LogConfig,
    result_extensions: Vec<Box<dyn ResultExtension>>,
    sql_comment: Option<SqlComment>,
}

impl<'a> PrismaClientBuilder<'a> {
    /// Create a new builder.
    pub fn new(schema: &'a str, factory: &'a dyn SqlDriverAdapterFactory) -> Self {
        Self {
            schema,
            factory,
            middleware: MiddlewareStack::new(),
            log_config: LogConfig::new(),
            result_extensions: Vec::new(),
            sql_comment: None,
        }
    }

    /// Add middleware to the execution pipeline.
    pub fn middleware(mut self, mw: impl crate::middleware::Middleware + 'static) -> Self {
        self.middleware.push(Arc::new(mw));
        self
    }

    /// Set the logging configuration.
    pub fn log(mut self, config: LogConfig) -> Self {
        self.log_config = config;
        self
    }

    /// Add a result extension for computed fields.
    pub fn result_extension(mut self, ext: impl ResultExtension + 'static) -> Self {
        self.result_extensions.push(Box::new(ext));
        self
    }

    /// Set SQL commenter tags for query tagging.
    pub fn sql_comment(mut self, comment: SqlComment) -> Self {
        self.sql_comment = Some(comment);
        self
    }

    /// Build the client, connecting to the database.
    pub async fn build(self) -> Result<PrismaClient, ClientError> {
        let adapter = self.factory.connect().await?;

        let sql_family = match self.factory.provider() {
            Provider::Postgres => SqlFamily::Postgres,
            Provider::Mysql => SqlFamily::Mysql,
            Provider::Sqlite => SqlFamily::Sqlite,
            Provider::DuckDb => SqlFamily::Postgres,
        };

        let conn_info = ConnectionInfo::External(ExternalConnectionInfo::new(
            sql_family,
            None,
            None,
            sql_family != SqlFamily::Sqlite,
        ));

        let compiler = QueryCompiler::new(self.schema, conn_info);

        Ok(PrismaClient {
            compiler,
            adapter: Mutex::new(adapter),
            middleware: self.middleware,
            log_config: self.log_config,
            result_extensions: self.result_extensions,
            sql_comment: self.sql_comment,
            disposed: false,
        })
    }
}

impl PrismaClient {
    /// Create a new Prisma client with default configuration.
    ///
    /// `schema` is the Prisma schema string (without `url` in datasource).
    /// `factory` is a database adapter factory that provides connections.
    pub async fn new(schema: &str, factory: &dyn SqlDriverAdapterFactory) -> Result<Self, ClientError> {
        PrismaClientBuilder::new(schema, factory).build().await
    }

    /// Execute a query built with `QueryBuilder`.
    pub async fn execute(&self, query: &QueryBuilder) -> Result<Value, ClientError> {
        let request = query.build();
        self.execute_json(&request).await
    }

    /// Execute a raw JSON protocol request.
    ///
    /// Runs the request through the middleware pipeline, compiles it,
    /// executes against the database, applies result extensions, and
    /// emits query events.
    #[tracing::instrument(skip_all, fields(
        prisma.model = %request.get("modelName").and_then(|v| v.as_str()).unwrap_or(""),
        prisma.action = %request.get("action").and_then(|v| v.as_str()).unwrap_or(""),
    ))]
    pub async fn execute_json(&self, request: &Value) -> Result<Value, ClientError> {
        let model = request.get("modelName").and_then(|v| v.as_str()).map(String::from);
        let action = request
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ClientError::InvalidQuery("missing required field: action".into()))?
            .to_string();
        let args = request
            .get("query")
            .and_then(|q| q.get("arguments"))
            .cloned()
            .unwrap_or(Value::Object(Default::default()));

        if self.middleware.is_empty() {
            self.execute_inner(request, model.as_deref(), &action).await
        } else {
            let params = MiddlewareParams { model, action, args };
            let executor = ClientExecutor {
                client: self,
                request: request.clone(),
            };
            self.middleware.execute(params, &executor).await
        }
    }

    /// Internal execution without middleware.
    async fn execute_inner(&self, request: &Value, model: Option<&str>, action: &str) -> Result<Value, ClientError> {
        let start = Instant::now();

        let request_str = serde_json::to_string(request)?;
        let expr = self.compiler.compile_to_ir(&request_str)?;

        let mut adapter = self.adapter.lock().await;
        let result = QueryExecutor::execute(&expr, &mut **adapter).await?;
        drop(adapter);

        let mut json = result.to_json();

        // Apply result extensions
        if let Some(model_name) = model {
            if !self.result_extensions.is_empty() {
                extensions::apply_result_extensions(&mut json, model_name, &self.result_extensions);
            }
        }

        // Emit query event
        if self.log_config.is_query_enabled() {
            let event = QueryEvent::new(
                model.map(String::from),
                action.to_string(),
                request_str,
                "[]".into(),
                start.elapsed(),
            );
            self.log_config.emit_query_event(event);
        }

        Ok(json)
    }

    /// Start an interactive transaction.
    ///
    /// Returns a `TransactionClient` that executes all queries within
    /// a database transaction. Call `.commit()` or `.rollback()` to finalize.
    pub async fn transaction(&self) -> Result<TransactionClient<'_>, ClientError> {
        let mut adapter = self.adapter.lock().await;
        let tx = (**adapter).start_transaction(None).await?;
        drop(adapter);
        Ok(TransactionClient::new(&self.compiler, tx))
    }

    /// Get a reference to the SQL comment configuration.
    pub fn sql_comment(&self) -> Option<&SqlComment> {
        self.sql_comment.as_ref()
    }

    /// Disconnect from the database and release resources.
    pub async fn disconnect(self) -> Result<(), ClientError> {
        let mut adapter = self.adapter.lock().await;
        (**adapter).dispose().await?;
        Ok(())
    }
}

impl Drop for PrismaClient {
    fn drop(&mut self) {
        if !self.disposed {
            // Check if adapter is still held (not yet disposed).
            // Since we use Mutex, we can't easily check, so we skip the warning
            // to avoid false positives when disconnect() was called.
        }
    }
}

/// Middleware executor that compiles and runs the actual query.
struct ClientExecutor<'a> {
    client: &'a PrismaClient,
    request: Value,
}

#[async_trait]
impl MiddlewareExecutor for ClientExecutor<'_> {
    async fn execute(&self, params: MiddlewareParams) -> Result<Value, ClientError> {
        // Rebuild the request from (possibly modified) middleware params
        let mut request = self.request.clone();
        if let Value::Object(ref mut map) = request {
            if let Some(ref model) = params.model {
                map.insert("modelName".into(), Value::String(model.clone()));
            }
            map.insert("action".into(), Value::String(params.action.clone()));
            if let Some(Value::Object(query)) = map.get_mut("query") {
                query.insert("arguments".into(), params.args.clone());
            }
        }

        self.client
            .execute_inner(&request, params.model.as_deref(), &params.action)
            .await
    }
}
