use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::Connection;
use prisma_driver_core::{
    ColumnType, ConnectionInfo, DriverError, IsolationLevel, MappedError, Provider, ResultValue, SqlDriverAdapter,
    SqlDriverAdapterFactory, SqlQuery, SqlQueryable, SqlResultSet, Transaction, TransactionOptions, static_sql,
};

use crate::conversion::{decl_type_to_column_type, duckdb_owned_value_to_result, query_value_to_duckdb};
use crate::error::convert_duckdb_error;

pub struct DuckDbDriverAdapter {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDbDriverAdapter {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }
}

#[async_trait]
impl SqlQueryable for DuckDbDriverAdapter {
    fn provider(&self) -> Provider {
        Provider::DuckDb
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-duckdb"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| {
                DriverError::new(MappedError::DuckDb {
                    message: "Mutex poisoned".into(),
                })
            })?;
            execute_query_sync(&guard, query)
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| {
                DriverError::new(MappedError::DuckDb {
                    message: "Mutex poisoned".into(),
                })
            })?;
            execute_mutation_sync(&guard, query)
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn start_transaction(
        &mut self,
        _isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .map_err(|_| {
                    DriverError::new(MappedError::DuckDb {
                        message: "Mutex poisoned".into(),
                    })
                })?
                .execute_batch("BEGIN TRANSACTION")
                .map_err(|e| convert_duckdb_error(&e))
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;

        Ok(Box::new(DuckDbTransaction {
            conn: self.conn.clone(),
            options: TransactionOptions::default(),
            closed: false,
        }))
    }
}

#[async_trait]
impl SqlDriverAdapter for DuckDbDriverAdapter {
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let script = script.to_string();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .map_err(|_| {
                    DriverError::new(MappedError::DuckDb {
                        message: "Mutex poisoned".into(),
                    })
                })?
                .execute_batch(&script)
                .map_err(|e| convert_duckdb_error(&e))
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: Some("main".into()),
            max_bind_values: Provider::DuckDb.max_bind_values(),
            supports_relation_joins: false,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        Ok(())
    }
}

struct DuckDbTransaction {
    conn: Arc<Mutex<Connection>>,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl SqlQueryable for DuckDbTransaction {
    fn provider(&self) -> Provider {
        Provider::DuckDb
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-duckdb"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| {
                DriverError::new(MappedError::DuckDb {
                    message: "Mutex poisoned".into(),
                })
            })?;
            execute_query_sync(&guard, query)
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| {
                DriverError::new(MappedError::DuckDb {
                    message: "Mutex poisoned".into(),
                })
            })?;
            execute_mutation_sync(&guard, query)
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
    }
}

#[async_trait]
impl Transaction for DuckDbTransaction {
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                conn.lock()
                    .map_err(|_| {
                        DriverError::new(MappedError::DuckDb {
                            message: "Mutex poisoned".into(),
                        })
                    })?
                    .execute_batch("COMMIT")
                    .map_err(|e| convert_duckdb_error(&e))
            })
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
            self.closed = true;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                conn.lock()
                    .map_err(|_| {
                        DriverError::new(MappedError::DuckDb {
                            message: "Mutex poisoned".into(),
                        })
                    })?
                    .execute_batch("ROLLBACK")
                    .map_err(|e| convert_duckdb_error(&e))
            })
            .await
            .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
            self.closed = true;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("SAVEPOINT ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .map_err(|_| {
                    DriverError::new(MappedError::DuckDb {
                        message: "Mutex poisoned".into(),
                    })
                })?
                .execute_batch(sql.as_str())
                .map_err(|e| convert_duckdb_error(&e))
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
        Ok(())
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("ROLLBACK TO SAVEPOINT ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .map_err(|_| {
                    DriverError::new(MappedError::DuckDb {
                        message: "Mutex poisoned".into(),
                    })
                })?
                .execute_batch(sql.as_str())
                .map_err(|e| convert_duckdb_error(&e))
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
        Ok(())
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("RELEASE SAVEPOINT ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.lock()
                .map_err(|_| {
                    DriverError::new(MappedError::DuckDb {
                        message: "Mutex poisoned".into(),
                    })
                })?
                .execute_batch(sql.as_str())
                .map_err(|e| convert_duckdb_error(&e))
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))??;
        Ok(())
    }
}

impl Drop for DuckDbTransaction {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-duckdb] WARNING: Transaction dropped without commit/rollback, \
                 auto-rolling back"
            );
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                let _ = conn.lock().map(|c| c.execute_batch("ROLLBACK"));
            });
        }
    }
}

// -- Factory --

pub struct DuckDbDriverAdapterFactory {
    path: String,
}

impl DuckDbDriverAdapterFactory {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn in_memory() -> Self {
        Self {
            path: ":memory:".into(),
        }
    }
}

#[async_trait]
impl SqlDriverAdapterFactory for DuckDbDriverAdapterFactory {
    fn provider(&self) -> Provider {
        Provider::DuckDb
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-duckdb"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let path = self.path.clone();
        let conn = tokio::task::spawn_blocking(move || {
            if path == ":memory:" {
                Connection::open_in_memory()
            } else {
                Connection::open(&path)
            }
        })
        .await
        .map_err(|e| DriverError::new(MappedError::DuckDb { message: e.to_string() }))?
        .map_err(|e| convert_duckdb_error(&e))?;

        Ok(Box::new(DuckDbDriverAdapter::new(conn)))
    }
}

// -- Synchronous helpers (run inside spawn_blocking) --

fn execute_query_sync(conn: &Connection, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
    query.validate()?;
    let mut stmt = conn.prepare(&query.sql).map_err(|e| convert_duckdb_error(&e))?;

    let params: Vec<duckdb::types::Value> = query.args.iter().map(query_value_to_duckdb).collect();
    let params_refs: Vec<&dyn duckdb::types::ToSql> = params.iter().map(|v| v as &dyn duckdb::types::ToSql).collect();

    // DuckDB requires execution before column metadata is available.
    // Use Rows::as_ref() to access stmt metadata after query() executes.
    let mut rows = stmt
        .query(params_refs.as_slice())
        .map_err(|e| convert_duckdb_error(&e))?;

    let inner_stmt = rows.as_ref().ok_or_else(|| {
        DriverError::new(MappedError::DuckDb {
            message: "Query result is in an error state".into(),
        })
    })?;
    let col_count = inner_stmt.column_count();
    let column_names: Vec<String> = (0..col_count)
        .map(|i| inner_stmt.column_name(i).map_or("?".to_string(), |v| v.to_string()))
        .collect();
    let column_types: Vec<ColumnType> = (0..col_count)
        .map(|i| {
            let type_name = inner_stmt.column_type(i).to_string();
            decl_type_to_column_type(&type_name)
        })
        .collect();

    let mut result_rows = Vec::new();
    while let Some(row) = rows.next().map_err(|e| convert_duckdb_error(&e))? {
        let result_row: Vec<ResultValue> = (0..col_count)
            .map(|i| {
                let val = row
                    .get::<usize, duckdb::types::Value>(i)
                    .unwrap_or(duckdb::types::Value::Null);
                duckdb_owned_value_to_result(val, column_types[i])
            })
            .collect();
        result_rows.push(result_row);
    }

    Ok(SqlResultSet {
        column_names,
        column_types,
        rows: result_rows,
        last_insert_id: None,
    })
}

fn execute_mutation_sync(conn: &Connection, query: SqlQuery) -> Result<u64, DriverError> {
    query.validate()?;
    let params: Vec<duckdb::types::Value> = query.args.iter().map(query_value_to_duckdb).collect();
    let params_refs: Vec<&dyn duckdb::types::ToSql> = params.iter().map(|v| v as &dyn duckdb::types::ToSql).collect();

    let changes = conn
        .execute(&query.sql, params_refs.as_slice())
        .map_err(|e| convert_duckdb_error(&e))?;

    Ok(changes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prisma_driver_core::ResultValue;

    #[tokio::test]
    async fn duckdb_in_memory_connection() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let adapter = factory.connect().await.unwrap();
        assert_eq!(adapter.provider(), Provider::DuckDb);
        assert_eq!(adapter.adapter_name(), "prisma-driver-duckdb");
    }

    #[tokio::test]
    async fn duckdb_create_and_query() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR, active BOOLEAN)")
            .await
            .unwrap();

        adapter
            .execute_raw(SqlQuery {
                sql: "INSERT INTO test VALUES (1, 'Alice', true)".into(),
                args: vec![],
                arg_types: vec![],
            })
            .await
            .unwrap();

        adapter
            .execute_raw(SqlQuery {
                sql: "INSERT INTO test VALUES (2, 'Bob', false)".into(),
                args: vec![],
                arg_types: vec![],
            })
            .await
            .unwrap();

        let rs = adapter
            .query_raw(SqlQuery {
                sql: "SELECT id, name, active FROM test ORDER BY id".into(),
                args: vec![],
                arg_types: vec![],
            })
            .await
            .unwrap();

        assert_eq!(rs.column_names, vec!["id", "name", "active"]);
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][0], ResultValue::Int32(1));
        assert_eq!(rs.rows[0][1], ResultValue::Text("Alice".into()));
        assert_eq!(rs.rows[0][2], ResultValue::Boolean(true));
        assert_eq!(rs.rows[1][0], ResultValue::Int32(2));
    }

    #[tokio::test]
    async fn duckdb_parameterized_query() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL)")
            .await
            .unwrap();

        adapter
            .execute_raw(SqlQuery {
                sql: "INSERT INTO users VALUES ($1, $2)".into(),
                args: vec![
                    prisma_driver_core::QueryValue::Int32(1),
                    prisma_driver_core::QueryValue::Text("test@example.com".into()),
                ],
                arg_types: vec![],
            })
            .await
            .unwrap();

        let rs = adapter
            .query_raw(SqlQuery {
                sql: "SELECT * FROM users WHERE email = $1".into(),
                args: vec![prisma_driver_core::QueryValue::Text("test@example.com".into())],
                arg_types: vec![],
            })
            .await
            .unwrap();

        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], ResultValue::Text("test@example.com".into()));
    }

    #[tokio::test]
    async fn duckdb_transaction_commit() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let mut adapter = factory.connect().await.unwrap();

        adapter.execute_script("CREATE TABLE t (id INTEGER)").await.unwrap();

        let mut tx = adapter.start_transaction(None).await.unwrap();
        tx.execute_raw(SqlQuery {
            sql: "INSERT INTO t VALUES (1)".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let rs = adapter
            .query_raw(SqlQuery {
                sql: "SELECT COUNT(*) FROM t".into(),
                args: vec![],
                arg_types: vec![],
            })
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0], ResultValue::Int64(1));
    }

    #[tokio::test]
    async fn duckdb_transaction_rollback() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let mut adapter = factory.connect().await.unwrap();

        adapter.execute_script("CREATE TABLE t (id INTEGER)").await.unwrap();

        let mut tx = adapter.start_transaction(None).await.unwrap();
        tx.execute_raw(SqlQuery {
            sql: "INSERT INTO t VALUES (1)".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
        tx.rollback().await.unwrap();

        let rs = adapter
            .query_raw(SqlQuery {
                sql: "SELECT COUNT(*) FROM t".into(),
                args: vec![],
                arg_types: vec![],
            })
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0], ResultValue::Int64(0));
    }

    #[tokio::test]
    async fn duckdb_execute_returns_affected_rows() {
        let factory = DuckDbDriverAdapterFactory::in_memory();
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)")
            .await
            .unwrap();

        let affected = adapter
            .execute_raw(SqlQuery {
                sql: "DELETE FROM t WHERE id > $1".into(),
                args: vec![prisma_driver_core::QueryValue::Int32(1)],
                arg_types: vec![],
            })
            .await
            .unwrap();

        assert_eq!(affected, 2);
    }
}
