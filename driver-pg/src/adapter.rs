use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::NoTls;

use prisma_driver_core::{
    ConnectionInfo, DatabaseUrl, DriverError, IsolationLevel, MappedError, Provider, QueryValue, SafeMessage,
    SqlDriverAdapter, SqlDriverAdapterFactory, SqlMigrationAwareDriverAdapterFactory, SqlQuery, SqlQueryable,
    SqlResultSet, Transaction, TransactionOptions, static_sql,
};

use crate::conversion::{pg_row_value, pg_type_to_column_type, query_value_to_pg_param_typed};
use crate::error::convert_pg_error;

/// TLS mode for PostgreSQL connections.
///
/// Mirrors PostgreSQL's `sslmode` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// No TLS. Connections are unencrypted.
    Disable,
    /// Try TLS first, fall back to plaintext if the server doesn't support it.
    #[default]
    Prefer,
    /// Require TLS. Fail if the server doesn't support it.
    Require,
}

/// Connection pool configuration.
#[derive(Debug, Clone, Default)]
pub struct PoolOptions {
    /// Maximum number of connections in the pool.
    ///
    /// Default: `cpu_count * 2` (deadpool default).
    pub max_size: Option<usize>,

    /// Timeout waiting for a connection to become available.
    ///
    /// Default: no timeout (waits indefinitely).
    pub wait_timeout: Option<Duration>,

    /// Timeout for creating a new connection.
    ///
    /// Default: no timeout.
    pub create_timeout: Option<Duration>,

    /// Timeout for recycling (health-checking) an idle connection.
    ///
    /// Default: no timeout.
    pub recycle_timeout: Option<Duration>,
}

/// PostgreSQL driver adapter options.
#[derive(Debug, Clone, Default)]
pub struct PgOptions {
    pub schema: Option<String>,
    pub pool: PoolOptions,
    pub ssl_mode: SslMode,
}

/// PostgreSQL driver adapter backed by `deadpool-postgres`.
pub struct PgDriverAdapter {
    pool: Pool,
    options: PgOptions,
}

impl PgDriverAdapter {
    pub fn new(pool: Pool, options: PgOptions) -> Self {
        Self { pool, options }
    }
}

#[async_trait]
impl SqlQueryable for PgDriverAdapter {
    fn provider(&self) -> Provider {
        Provider::Postgres
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-pg"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| DriverError::new(pool_error_to_mapped(e)))?;
        execute_query(&client, query).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| DriverError::new(pool_error_to_mapped(e)))?;
        execute_mutation(&client, query).await
    }

    async fn start_transaction(
        &mut self,
        isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| DriverError::new(pool_error_to_mapped(e)))?;

        let begin_sql = match isolation_level {
            Some(level) => static_sql!("BEGIN ISOLATION LEVEL ", level.as_sql()),
            None => static_sql!("BEGIN"),
        };
        client
            .batch_execute(begin_sql.as_str())
            .await
            .map_err(|e| convert_pg_error(&e))?;

        Ok(Box::new(PgTransaction {
            client,
            options: TransactionOptions::default(),
            closed: false,
        }))
    }
}

#[async_trait]
impl SqlDriverAdapter for PgDriverAdapter {
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| DriverError::new(pool_error_to_mapped(e)))?;
        client.batch_execute(script).await.map_err(|e| convert_pg_error(&e))?;
        Ok(())
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: self.options.schema.clone(),
            max_bind_values: Provider::Postgres.max_bind_values(),
            supports_relation_joins: true,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        self.pool.close();
        Ok(())
    }
}

/// An active PostgreSQL transaction.
struct PgTransaction {
    client: deadpool_postgres::Object,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl SqlQueryable for PgTransaction {
    fn provider(&self) -> Provider {
        Provider::Postgres
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-pg"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        execute_query(&self.client, query).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        execute_mutation(&self.client, query).await
    }
}

#[async_trait]
impl Transaction for PgTransaction {
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.client
                .batch_execute("COMMIT")
                .await
                .map_err(|e| convert_pg_error(&e))?;
            self.closed = true;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.client
                .batch_execute("ROLLBACK")
                .await
                .map_err(|e| convert_pg_error(&e))?;
            self.closed = true;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("SAVEPOINT ", name);
        self.client
            .batch_execute(sql.as_str())
            .await
            .map_err(|e| convert_pg_error(&e))?;
        Ok(())
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("ROLLBACK TO SAVEPOINT ", name);
        self.client
            .batch_execute(sql.as_str())
            .await
            .map_err(|e| convert_pg_error(&e))?;
        Ok(())
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("RELEASE SAVEPOINT ", name);
        self.client
            .batch_execute(sql.as_str())
            .await
            .map_err(|e| convert_pg_error(&e))?;
        Ok(())
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-pg] WARNING: Transaction dropped without commit/rollback, \
                 connection returned to pool with implicit rollback"
            );
            // When the deadpool Object is dropped, the connection returns to the pool.
            // The pool resets connection state, which implicitly rolls back any open
            // transaction. The warning is the best we can do here since we cannot
            // perform async operations in Drop.
        }
    }
}

/// Factory for creating PostgreSQL driver adapters.
///
/// Accepts a [`DatabaseUrl`] which parses the connection string on construction
/// and redacts the password in all `Display`/`Debug` output, preventing
/// credential leakage through error messages or logs.
pub struct PgDriverAdapterFactory {
    url: DatabaseUrl,
    options: PgOptions,
}

impl PgDriverAdapterFactory {
    pub fn new(url: DatabaseUrl) -> Self {
        Self {
            url,
            options: PgOptions::default(),
        }
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.options.schema = Some(schema.into());
        self
    }

    pub fn with_pool_options(mut self, pool: PoolOptions) -> Self {
        self.options.pool = pool;
        self
    }

    pub fn with_max_pool_size(mut self, max_size: usize) -> Self {
        self.options.pool.max_size = Some(max_size);
        self
    }

    pub fn with_wait_timeout(mut self, timeout: Duration) -> Self {
        self.options.pool.wait_timeout = Some(timeout);
        self
    }

    pub fn with_create_timeout(mut self, timeout: Duration) -> Self {
        self.options.pool.create_timeout = Some(timeout);
        self
    }

    pub fn with_ssl_mode(mut self, ssl_mode: SslMode) -> Self {
        self.options.ssl_mode = ssl_mode;
        self
    }

    fn build_pool(&self) -> Result<Pool, DriverError> {
        let mut cfg = Config::new();
        // expose_url() is the only place the real password is used
        cfg.url = Some(self.url.expose_url());

        let pool_opts = &self.options.pool;
        if pool_opts.max_size.is_some()
            || pool_opts.wait_timeout.is_some()
            || pool_opts.create_timeout.is_some()
            || pool_opts.recycle_timeout.is_some()
        {
            let mut pool_cfg = deadpool_postgres::PoolConfig::default();
            if let Some(max_size) = pool_opts.max_size {
                pool_cfg.max_size = max_size;
            }
            pool_cfg.timeouts.wait = pool_opts.wait_timeout;
            pool_cfg.timeouts.create = pool_opts.create_timeout;
            pool_cfg.timeouts.recycle = pool_opts.recycle_timeout;
            cfg.pool = Some(pool_cfg);
        }

        let make_error = |_| {
            let msg = SafeMessage::new("Failed to create pool for {0}").secret(self.url.expose_url());
            DriverError::new(MappedError::DatabaseNotReachable {
                host: self.url.host().map(|h| h.to_string()),
                port: self.url.port(),
            })
            .with_safe_message("POOL_CREATE", msg)
        };

        match self.options.ssl_mode {
            SslMode::Disable => cfg.create_pool(Some(Runtime::Tokio1), NoTls).map_err(make_error),
            SslMode::Prefer | SslMode::Require => {
                let tls_config = build_rustls_config().map_err(|e| {
                    DriverError::new(MappedError::TlsConnectionError {
                        reason: format!("Failed to initialize TLS: {e}"),
                    })
                })?;
                let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
                cfg.create_pool(Some(Runtime::Tokio1), tls).map_err(make_error)
            }
        }
    }
}

#[async_trait]
impl SqlDriverAdapterFactory for PgDriverAdapterFactory {
    fn provider(&self) -> Provider {
        Provider::Postgres
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-pg"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let pool = self.build_pool()?;
        Ok(Box::new(PgDriverAdapter::new(pool, self.options.clone())))
    }
}

#[async_trait]
impl SqlMigrationAwareDriverAdapterFactory for PgDriverAdapterFactory {
    async fn connect_to_shadow_db(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        // For shadow DB, create a separate connection to the same server but
        // with a temporary database. For now, just connect to the same DB.
        // A full implementation would CREATE a temp DB and return an adapter to it.
        self.connect().await
    }
}

// -- Internal helpers --

/// Convert query values to PG params, using the prepared statement's parameter
/// types to pick the correct Rust type (e.g. i32 for INT4 vs i64 for INT8).
fn build_pg_params(
    args: &[QueryValue],
    stmt_param_types: &[tokio_postgres::types::Type],
) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
    args.iter()
        .enumerate()
        .map(|(i, v)| {
            let pg_type = stmt_param_types.get(i);
            query_value_to_pg_param_typed(v, pg_type)
        })
        .collect()
}

async fn execute_query(client: &tokio_postgres::Client, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
    query.validate()?;
    let stmt = client.prepare(&query.sql).await.map_err(|e| convert_pg_error(&e))?;

    let params = build_pg_params(&query.args, stmt.params());

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let column_types: Vec<_> = stmt
        .columns()
        .iter()
        .map(|c| pg_type_to_column_type(c.type_()))
        .collect();

    let column_names: Vec<_> = stmt.columns().iter().map(|c| c.name().to_string()).collect();

    let rows = client
        .query(&stmt, &param_refs)
        .await
        .map_err(|e| convert_pg_error(&e))?;

    let result_rows: Vec<Vec<_>> = rows
        .iter()
        .map(|row| {
            column_types
                .iter()
                .enumerate()
                .map(|(i, ct)| pg_row_value(row, i, *ct))
                .collect()
        })
        .collect();

    Ok(SqlResultSet {
        column_names,
        column_types,
        rows: result_rows,
        last_insert_id: None,
    })
}

async fn execute_mutation(client: &tokio_postgres::Client, query: SqlQuery) -> Result<u64, DriverError> {
    query.validate()?;
    let stmt = client.prepare(&query.sql).await.map_err(|e| convert_pg_error(&e))?;

    let params = build_pg_params(&query.args, stmt.params());

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let rows_affected = client
        .execute(&stmt, &param_refs)
        .await
        .map_err(|e| convert_pg_error(&e))?;

    Ok(rows_affected)
}

fn pool_error_to_mapped(err: deadpool_postgres::PoolError) -> MappedError {
    let message = err.to_string();
    if message.contains("timed out") {
        MappedError::SocketTimeout
    } else if message.contains("authentication") {
        MappedError::DatabaseAccessDenied { db: None }
    } else if message.contains("refused") || message.contains("No such file") {
        MappedError::DatabaseNotReachable { host: None, port: None }
    } else {
        MappedError::TooManyConnections {
            cause: "connection pool exhausted".to_string(),
        }
    }
}

/// Build a rustls `ClientConfig` using the system's native certificate store.
fn build_rustls_config() -> Result<rustls::ClientConfig, Box<dyn std::error::Error>> {
    let mut root_store = rustls::RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    for cert in result.certs {
        root_store.add(cert)?;
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(config)
}
