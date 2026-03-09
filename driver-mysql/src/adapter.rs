use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool, Row};

use prisma_driver_core::{
    ConnectionInfo, DatabaseUrl, DriverError, IsolationLevel, MappedError, Provider, SafeMessage, SqlDriverAdapter,
    SqlDriverAdapterFactory, SqlQuery, SqlQueryable, SqlResultSet, Transaction, TransactionOptions, static_sql,
};

use crate::conversion::{mysql_row_value, mysql_type_to_column_type, query_value_to_mysql, supports_relation_joins};
use crate::error::convert_mysql_error;

/// MySQL/MariaDB driver adapter options.
#[derive(Debug, Clone, Default)]
pub struct MySqlOptions {
    pub database: Option<String>,
}

/// MySQL/MariaDB driver adapter backed by `mysql_async`.
pub struct MySqlDriverAdapter {
    pool: Pool,
    options: MySqlOptions,
    supports_joins: bool,
}

impl MySqlDriverAdapter {
    pub fn new(pool: Pool, options: MySqlOptions, supports_joins: bool) -> Self {
        Self {
            pool,
            options,
            supports_joins,
        }
    }
}

#[async_trait]
impl SqlQueryable for MySqlDriverAdapter {
    fn provider(&self) -> Provider {
        Provider::Mysql
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-mysql"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| convert_mysql_error(&e))?;
        execute_query(&mut conn, query).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| convert_mysql_error(&e))?;
        execute_mutation(&mut conn, query).await
    }

    async fn start_transaction(
        &mut self,
        isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| convert_mysql_error(&e))?;

        if let Some(level) = isolation_level {
            let sql = static_sql!("SET TRANSACTION ISOLATION LEVEL ", level.as_sql());
            conn.query_drop(sql.as_str())
                .await
                .map_err(|e| convert_mysql_error(&e))?;
        }

        conn.query_drop("BEGIN").await.map_err(|e| convert_mysql_error(&e))?;

        Ok(Box::new(MySqlTransaction {
            conn,
            options: TransactionOptions::default(),
            closed: false,
        }))
    }
}

#[async_trait]
impl SqlDriverAdapter for MySqlDriverAdapter {
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        let mut conn = self.pool.get_conn().await.map_err(|e| convert_mysql_error(&e))?;
        conn.query_drop(script).await.map_err(|e| convert_mysql_error(&e))?;
        Ok(())
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: self.options.database.clone(),
            max_bind_values: Provider::Mysql.max_bind_values(),
            supports_relation_joins: self.supports_joins,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        self.pool
            .clone()
            .disconnect()
            .await
            .map_err(|e| convert_mysql_error(&e))?;
        Ok(())
    }
}

/// An active MySQL/MariaDB transaction.
///
/// Holds a `Conn` directly -- no Mutex needed since the trait takes `&mut self`.
struct MySqlTransaction {
    conn: Conn,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl SqlQueryable for MySqlTransaction {
    fn provider(&self) -> Provider {
        Provider::Mysql
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-mysql"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        execute_query(&mut self.conn, query).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        execute_mutation(&mut self.conn, query).await
    }
}

#[async_trait]
impl Transaction for MySqlTransaction {
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.conn
                .query_drop("COMMIT")
                .await
                .map_err(|e| convert_mysql_error(&e))?;
            self.closed = true;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.conn
                .query_drop("ROLLBACK")
                .await
                .map_err(|e| convert_mysql_error(&e))?;
            self.closed = true;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("SAVEPOINT ", name);
        self.conn
            .query_drop(sql.as_str())
            .await
            .map_err(|e| convert_mysql_error(&e))?;
        Ok(())
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("ROLLBACK TO ", name);
        self.conn
            .query_drop(sql.as_str())
            .await
            .map_err(|e| convert_mysql_error(&e))?;
        Ok(())
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("RELEASE SAVEPOINT ", name);
        self.conn
            .query_drop(sql.as_str())
            .await
            .map_err(|e| convert_mysql_error(&e))?;
        Ok(())
    }
}

impl Drop for MySqlTransaction {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-mysql] WARNING: Transaction dropped without commit/rollback, \
                 connection returned to pool with implicit rollback"
            );
        }
    }
}

/// Factory for creating MySQL/MariaDB driver adapters.
///
/// Accepts a [`DatabaseUrl`] which parses the connection string on construction
/// and redacts the password in all `Display`/`Debug` output.
pub struct MySqlDriverAdapterFactory {
    url: DatabaseUrl,
    options: MySqlOptions,
}

impl MySqlDriverAdapterFactory {
    pub fn new(url: DatabaseUrl) -> Self {
        Self {
            url,
            options: MySqlOptions::default(),
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.options.database = Some(database.into());
        self
    }
}

#[async_trait]
impl SqlDriverAdapterFactory for MySqlDriverAdapterFactory {
    fn provider(&self) -> Provider {
        Provider::Mysql
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-mysql"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        // expose_url() is the only place the real password is used
        let opts = Opts::from_url(&self.url.expose_url()).map_err(|_| {
            let msg = SafeMessage::new("Failed to parse MySQL URL: {0}").secret(self.url.expose_url());
            DriverError::new(MappedError::DatabaseNotReachable {
                host: self.url.host().map(|h| h.to_string()),
                port: self.url.port(),
            })
            .with_safe_message("URL_PARSE", msg)
        })?;

        let database = self
            .options
            .database
            .clone()
            .or_else(|| opts.db_name().map(|s| s.to_string()));

        let pool = Pool::new(opts);

        // Detect version for capability detection
        let mut conn = pool.get_conn().await.map_err(|e| convert_mysql_error(&e))?;
        let version: Option<String> = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| convert_mysql_error(&e))?;
        drop(conn);

        let supports_joins = version.as_deref().map(supports_relation_joins).unwrap_or(false);

        let options = MySqlOptions { database };

        Ok(Box::new(MySqlDriverAdapter::new(pool, options, supports_joins)))
    }
}

// -- Internal helpers --

async fn execute_query(conn: &mut Conn, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
    query.validate()?;
    let params: Vec<mysql_async::Value> = query.args.iter().map(query_value_to_mysql).collect();

    let mut result = conn
        .exec_iter(&query.sql, params)
        .await
        .map_err(|e| convert_mysql_error(&e))?;

    let columns = result.columns_ref();
    let column_names: Vec<String> = columns.iter().map(|c| c.name_str().to_string()).collect();
    let column_types: Vec<_> = columns
        .iter()
        .map(|c| mysql_type_to_column_type(c.column_type(), c.flags()))
        .collect();

    let last_id = result.last_insert_id();

    let rows: Vec<Row> = result.collect().await.map_err(|e| convert_mysql_error(&e))?;

    let result_rows: Vec<Vec<_>> = rows
        .iter()
        .map(|row| {
            column_types
                .iter()
                .enumerate()
                .map(|(i, ct)| mysql_row_value(row, i, *ct))
                .collect()
        })
        .collect();

    Ok(SqlResultSet {
        column_names,
        column_types,
        rows: result_rows,
        last_insert_id: last_id.map(|id| id.to_string()),
    })
}

async fn execute_mutation(conn: &mut Conn, query: SqlQuery) -> Result<u64, DriverError> {
    query.validate()?;
    let params: Vec<mysql_async::Value> = query.args.iter().map(query_value_to_mysql).collect();

    conn.exec_drop(&query.sql, params)
        .await
        .map_err(|e| convert_mysql_error(&e))?;

    Ok(conn.affected_rows())
}
