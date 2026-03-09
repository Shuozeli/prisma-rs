use std::sync::{Arc, Mutex};

use adbc_core::Connection as AdbcConnection;
use adbc_core::Statement as AdbcStatement;
use async_trait::async_trait;
use prisma_driver_core::{
    ConnectionInfo, DriverError, IsolationLevel, MappedError, Provider, SqlDriverAdapter, SqlQuery, SqlQueryable,
    SqlResultSet, Transaction, TransactionOptions, static_sql,
};

use crate::arrow::{query_values_to_record_batch, record_batches_to_result_set};
use crate::error::convert_adbc_error;

/// Generic ADBC adapter that wraps any `adbc_core::Connection` implementation.
///
/// This bridges the ADBC synchronous API to our async `SqlDriverAdapter` trait.
/// The ADBC connection is shared via `Arc<Mutex<C>>` so transactions can
/// reference the same underlying connection.
pub struct AdbcDriverAdapter<C: AdbcConnection> {
    conn: Arc<Mutex<C>>,
    provider: Provider,
}

impl<C: AdbcConnection + Send + 'static> AdbcDriverAdapter<C> {
    pub fn new(conn: C, provider: Provider) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            provider,
        }
    }
}

fn adbc_query<C: AdbcConnection>(
    conn: &Mutex<C>,
    sql: &str,
    args: &[prisma_driver_core::QueryValue],
) -> Result<SqlResultSet, DriverError> {
    let mut guard = conn.lock().map_err(|e| {
        DriverError::new(MappedError::DuckDb {
            message: format!("mutex poisoned: {e}"),
        })
    })?;
    let mut stmt = guard.new_statement().map_err(|e| convert_adbc_error(&e))?;
    stmt.set_sql_query(sql).map_err(|e| convert_adbc_error(&e))?;

    if !args.is_empty() {
        let batch = query_values_to_record_batch(args);
        stmt.bind(batch).map_err(|e| convert_adbc_error(&e))?;
    }

    let reader = stmt.execute().map_err(|e| convert_adbc_error(&e))?;
    let batches: Vec<_> = reader.into_iter().collect::<Result<Vec<_>, _>>().map_err(|e| {
        DriverError::new(MappedError::DuckDb {
            message: format!("arrow error: {e}"),
        })
    })?;

    Ok(record_batches_to_result_set(&batches))
}

fn adbc_execute<C: AdbcConnection>(
    conn: &Mutex<C>,
    sql: &str,
    args: &[prisma_driver_core::QueryValue],
) -> Result<u64, DriverError> {
    let mut guard = conn.lock().map_err(|e| {
        DriverError::new(MappedError::DuckDb {
            message: format!("mutex poisoned: {e}"),
        })
    })?;
    let mut stmt = guard.new_statement().map_err(|e| convert_adbc_error(&e))?;
    stmt.set_sql_query(sql).map_err(|e| convert_adbc_error(&e))?;

    if !args.is_empty() {
        let batch = query_values_to_record_batch(args);
        stmt.bind(batch).map_err(|e| convert_adbc_error(&e))?;
    }

    let affected = stmt.execute_update().map_err(|e| convert_adbc_error(&e))?;
    Ok(affected.unwrap_or(0) as u64)
}

fn adbc_exec_sql<C: AdbcConnection>(conn: &Mutex<C>, sql: &str) -> Result<(), DriverError> {
    let mut guard = conn.lock().map_err(|e| {
        DriverError::new(MappedError::DuckDb {
            message: format!("mutex poisoned: {e}"),
        })
    })?;
    let mut stmt = guard.new_statement().map_err(|e| convert_adbc_error(&e))?;
    stmt.set_sql_query(sql).map_err(|e| convert_adbc_error(&e))?;
    let _ = stmt.execute_update().map_err(|e| convert_adbc_error(&e))?;
    Ok(())
}

#[async_trait]
impl<C: AdbcConnection + Send + 'static> SqlQueryable for AdbcDriverAdapter<C>
where
    C::StatementType: Send,
{
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-adbc"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        query.validate()?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || adbc_query(&conn, &query.sql, &query.args))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        query.validate()?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || adbc_execute(&conn, &query.sql, &query.args))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn start_transaction(
        &mut self,
        _isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            adbc_exec_sql(&conn, "BEGIN TRANSACTION")?;
            Ok(Box::new(AdbcTransaction {
                conn,
                provider: Provider::DuckDb,
                options: TransactionOptions::default(),
                closed: false,
            }) as Box<dyn Transaction + Send>)
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }
}

#[async_trait]
impl<C: AdbcConnection + Send + 'static> SqlDriverAdapter for AdbcDriverAdapter<C>
where
    C::StatementType: Send,
{
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let script = script.to_string();
        tokio::task::spawn_blocking(move || {
            for sql in script.split(';') {
                let trimmed = sql.trim();
                if trimmed.is_empty() {
                    continue;
                }
                adbc_exec_sql(&conn, trimmed)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: Some("main".into()),
            max_bind_values: self.provider.max_bind_values(),
            supports_relation_joins: false,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        Ok(())
    }
}

struct AdbcTransaction<C: AdbcConnection> {
    conn: Arc<Mutex<C>>,
    provider: Provider,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl<C: AdbcConnection + Send + 'static> SqlQueryable for AdbcTransaction<C>
where
    C::StatementType: Send,
{
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-adbc"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        query.validate()?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || adbc_query(&conn, &query.sql, &query.args))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        query.validate()?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || adbc_execute(&conn, &query.sql, &query.args))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }
}

#[async_trait]
impl<C: AdbcConnection + Send + 'static> Transaction for AdbcTransaction<C>
where
    C::StatementType: Send,
{
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || adbc_exec_sql(&conn, "COMMIT"))
                .await
                .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || adbc_exec_sql(&conn, "ROLLBACK"))
                .await
                .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let sql = static_sql!("SAVEPOINT ", name);
        tokio::task::spawn_blocking(move || adbc_exec_sql(&conn, sql.as_str()))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let sql = static_sql!("ROLLBACK TO SAVEPOINT ", name);
        tokio::task::spawn_blocking(move || adbc_exec_sql(&conn, sql.as_str()))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let sql = static_sql!("RELEASE SAVEPOINT ", name);
        tokio::task::spawn_blocking(move || adbc_exec_sql(&conn, sql.as_str()))
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }
}

impl<C: AdbcConnection> Drop for AdbcTransaction<C> {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-adbc] WARNING: Transaction dropped without commit/rollback, \
                 attempting synchronous rollback"
            );
            // Cannot use spawn_blocking here because Drop bounds cannot be
            // stricter than the struct bounds (which lack Send + 'static).
            // Attempt a synchronous rollback directly.
            let _ = adbc_exec_sql(&self.conn, "ROLLBACK");
        }
    }
}

/// Factory for creating ADBC-based adapters.
///
/// `F` is a closure that creates a new ADBC connection.
pub struct AdbcDriverAdapterFactory<C, F>
where
    C: AdbcConnection + Send + 'static,
    F: Fn() -> Result<C, DriverError> + Send + Sync,
{
    connect_fn: F,
    provider: Provider,
    _phantom: std::marker::PhantomData<C>,
}

impl<C, F> AdbcDriverAdapterFactory<C, F>
where
    C: AdbcConnection + Send + 'static,
    C::StatementType: Send,
    F: Fn() -> Result<C, DriverError> + Send + Sync,
{
    pub fn new(provider: Provider, connect_fn: F) -> Self {
        Self {
            connect_fn,
            provider,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<C, F> prisma_driver_core::SqlDriverAdapterFactory for AdbcDriverAdapterFactory<C, F>
where
    C: AdbcConnection + Send + Sync + 'static,
    C::StatementType: Send,
    F: Fn() -> Result<C, DriverError> + Send + Sync,
{
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-adbc"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let conn = (self.connect_fn)()?;
        Ok(Box::new(AdbcDriverAdapter::new(conn, self.provider)))
    }
}
