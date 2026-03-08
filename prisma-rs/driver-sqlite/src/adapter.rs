use std::sync::Arc;

use async_trait::async_trait;
use rusqlite::Connection;
use tokio::sync::Mutex;

use prisma_driver_core::{
    ColumnType, ConnectionInfo, DriverError, IsolationLevel, MappedError, Provider, SqlDriverAdapter,
    SqlDriverAdapterFactory, SqlMigrationAwareDriverAdapterFactory, SqlQuery, SqlQueryable, SqlResultSet, Transaction,
    TransactionOptions, static_sql,
};

use crate::conversion::{decl_type_to_column_type, infer_column_type, query_value_to_sqlite, sqlite_value_to_result};
use crate::error::convert_sqlite_error;

/// SQLite driver adapter options.
#[derive(Debug, Clone, Default)]
pub struct SqliteOptions {
    pub shadow_database_url: Option<String>,
}

/// SQLite driver adapter backed by `rusqlite` with async via `spawn_blocking`.
///
/// SQLite is single-writer, so all operations are serialized through a `Mutex`.
pub struct SqliteDriverAdapter {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDriverAdapter {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }
}

#[async_trait]
impl SqlQueryable for SqliteDriverAdapter {
    fn provider(&self) -> Provider {
        Provider::Sqlite
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-sqlite"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || execute_query_sync(&conn.blocking_lock(), query))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || execute_mutation_sync(&conn.blocking_lock(), query))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })?
    }

    async fn start_transaction(
        &mut self,
        isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        if let Some(level) = isolation_level {
            if level != IsolationLevel::Serializable {
                return Err(DriverError::new(MappedError::InvalidIsolationLevel {
                    level: format!("SQLite only supports SERIALIZABLE isolation, got {}", level.as_sql()),
                }));
            }
        }

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch("BEGIN")
                .map_err(|e| convert_sqlite_error(&e))
        })
        .await
        .map_err(|e| {
            DriverError::new(MappedError::Sqlite {
                extended_code: 0,
                message: e.to_string(),
            })
        })??;

        Ok(Box::new(SqliteTransaction {
            conn: self.conn.clone(),
            options: TransactionOptions::default(),
            closed: false,
        }))
    }
}

#[async_trait]
impl SqlDriverAdapter for SqliteDriverAdapter {
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        let conn = self.conn.clone();
        let script = script.to_string();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch(&script)
                .map_err(|e| convert_sqlite_error(&e))
        })
        .await
        .map_err(|e| {
            DriverError::new(MappedError::Sqlite {
                extended_code: 0,
                message: e.to_string(),
            })
        })?
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: None,
            max_bind_values: Provider::Sqlite.max_bind_values(),
            supports_relation_joins: false,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        Ok(())
    }
}

/// An active SQLite transaction.
struct SqliteTransaction {
    conn: Arc<Mutex<Connection>>,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl SqlQueryable for SqliteTransaction {
    fn provider(&self) -> Provider {
        Provider::Sqlite
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-sqlite"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || execute_query_sync(&conn.blocking_lock(), query))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })?
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || execute_mutation_sync(&conn.blocking_lock(), query))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })?
    }
}

#[async_trait]
impl Transaction for SqliteTransaction {
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                conn.blocking_lock()
                    .execute_batch("COMMIT")
                    .map_err(|e| convert_sqlite_error(&e))
            })
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })??;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                conn.blocking_lock()
                    .execute_batch("ROLLBACK")
                    .map_err(|e| convert_sqlite_error(&e))
            })
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })??;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("SAVEPOINT ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch(sql.as_str())
                .map_err(|e| convert_sqlite_error(&e))
        })
        .await
        .map_err(|e| {
            DriverError::new(MappedError::Sqlite {
                extended_code: 0,
                message: e.to_string(),
            })
        })??;
        Ok(())
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("ROLLBACK TO ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch(sql.as_str())
                .map_err(|e| convert_sqlite_error(&e))
        })
        .await
        .map_err(|e| {
            DriverError::new(MappedError::Sqlite {
                extended_code: 0,
                message: e.to_string(),
            })
        })??;
        Ok(())
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        let sql = static_sql!("RELEASE SAVEPOINT ", name);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch(sql.as_str())
                .map_err(|e| convert_sqlite_error(&e))
        })
        .await
        .map_err(|e| {
            DriverError::new(MappedError::Sqlite {
                extended_code: 0,
                message: e.to_string(),
            })
        })??;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-sqlite] WARNING: Transaction dropped without commit/rollback, \
                 auto-rolling back"
            );
            let conn = self.conn.clone();
            tokio::task::spawn_blocking(move || {
                let _ = conn.blocking_lock().execute_batch("ROLLBACK");
            });
        }
    }
}

/// Factory for creating SQLite driver adapters.
pub struct SqliteDriverAdapterFactory {
    path: String,
    options: SqliteOptions,
}

impl SqliteDriverAdapterFactory {
    /// Create a factory for a file-based or `:memory:` SQLite database.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            options: SqliteOptions::default(),
        }
    }

    pub fn with_shadow_database(mut self, url: impl Into<String>) -> Self {
        self.options.shadow_database_url = Some(url.into());
        self
    }

    fn open_connection(path: &str) -> Result<Connection, DriverError> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        };

        let conn = conn.map_err(|e| convert_sqlite_error(&e))?;

        // Enable WAL mode for better concurrency on file-based databases
        if path != ":memory:" {
            conn.execute_batch("PRAGMA journal_mode=WAL;")
                .map_err(|e| convert_sqlite_error(&e))?;
        }

        // Enable foreign keys
        conn.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| convert_sqlite_error(&e))?;

        Ok(conn)
    }
}

#[async_trait]
impl SqlDriverAdapterFactory for SqliteDriverAdapterFactory {
    fn provider(&self) -> Provider {
        Provider::Sqlite
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-sqlite"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let path = self.path.clone();
        let conn = tokio::task::spawn_blocking(move || Self::open_connection(&path))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })??;

        Ok(Box::new(SqliteDriverAdapter::new(conn)))
    }
}

#[async_trait]
impl SqlMigrationAwareDriverAdapterFactory for SqliteDriverAdapterFactory {
    async fn connect_to_shadow_db(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let path = self
            .options
            .shadow_database_url
            .clone()
            .unwrap_or_else(|| ":memory:".to_string());

        let conn = tokio::task::spawn_blocking(move || Self::open_connection(&path))
            .await
            .map_err(|e| {
                DriverError::new(MappedError::Sqlite {
                    extended_code: 0,
                    message: e.to_string(),
                })
            })??;

        Ok(Box::new(SqliteDriverAdapter::new(conn)))
    }
}

// -- Synchronous helpers (run inside spawn_blocking) --

fn execute_query_sync(conn: &Connection, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
    query.validate()?;
    let mut stmt = conn.prepare(&query.sql).map_err(|e| convert_sqlite_error(&e))?;

    // Extract column metadata before borrowing stmt for query.
    let col_count = stmt.column_count();
    let column_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    // Collect declared types as owned strings so we don't hold a borrow on stmt.
    let decl_types: Vec<Option<String>> = {
        let columns = stmt.columns();
        columns.iter().map(|c| c.decl_type().map(|s| s.to_string())).collect()
    };

    let params: Vec<rusqlite::types::Value> = query.args.iter().map(query_value_to_sqlite).collect();
    let params_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();

    let mut rows_result = stmt
        .query(params_refs.as_slice())
        .map_err(|e| convert_sqlite_error(&e))?;

    let mut result_rows = Vec::new();
    let mut column_types: Option<Vec<ColumnType>> = None;

    while let Some(row) = rows_result.next().map_err(|e| convert_sqlite_error(&e))? {
        if column_types.is_none() {
            let types: Vec<ColumnType> = (0..col_count)
                .map(|i| {
                    if let Some(ct) = decl_type_to_column_type(decl_types[i].as_deref()) {
                        ct
                    } else if let Ok(val) = row.get_ref(i) {
                        infer_column_type(val)
                    } else {
                        ColumnType::Int32
                    }
                })
                .collect();
            column_types = Some(types);
        }

        let types = column_types.as_ref().unwrap();
        let result_row: Vec<_> = (0..col_count)
            .map(|i| {
                let val = row.get_ref(i).unwrap_or(rusqlite::types::ValueRef::Null);
                sqlite_value_to_result(val, types[i])
            })
            .collect();
        result_rows.push(result_row);
    }

    let column_types = column_types.unwrap_or_else(|| {
        decl_types
            .iter()
            .map(|dt| decl_type_to_column_type(dt.as_deref()).unwrap_or(ColumnType::Int32))
            .collect()
    });

    Ok(SqlResultSet {
        column_names,
        column_types,
        rows: result_rows,
        last_insert_id: None,
    })
}

fn execute_mutation_sync(conn: &Connection, query: SqlQuery) -> Result<u64, DriverError> {
    query.validate()?;
    let params: Vec<rusqlite::types::Value> = query.args.iter().map(query_value_to_sqlite).collect();
    let params_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();

    let changes = conn
        .execute(&query.sql, params_refs.as_slice())
        .map_err(|e| convert_sqlite_error(&e))?;

    Ok(changes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prisma_driver_core::ResultValue;

    #[tokio::test]
    async fn sqlite_memory_basic_query() {
        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();

        let insert = SqlQuery {
            sql: "INSERT INTO test (id, name) VALUES (?1, ?2)".to_string(),
            args: vec![
                prisma_driver_core::QueryValue::Int32(1),
                prisma_driver_core::QueryValue::Text("hello".to_string()),
            ],
            arg_types: vec![],
        };
        let affected = adapter.execute_raw(insert).await.unwrap();
        assert_eq!(affected, 1);

        let select = SqlQuery {
            sql: "SELECT id, name FROM test".to_string(),
            args: vec![],
            arg_types: vec![],
        };
        let result = adapter.query_raw(select).await.unwrap();
        assert_eq!(result.column_names, vec!["id", "name"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], ResultValue::Int32(1));
        assert_eq!(result.rows[0][1], ResultValue::Text("hello".to_string()));
    }

    #[tokio::test]
    async fn sqlite_transaction_commit() {
        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();

        let mut tx = adapter.start_transaction(None).await.unwrap();

        let insert = SqlQuery {
            sql: "INSERT INTO test (id) VALUES (?1)".to_string(),
            args: vec![prisma_driver_core::QueryValue::Int32(1)],
            arg_types: vec![],
        };
        tx.execute_raw(insert).await.unwrap();
        tx.commit().await.unwrap();

        let select = SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM test".to_string(),
            args: vec![],
            arg_types: vec![],
        };
        let result = adapter.query_raw(select).await.unwrap();
        assert_eq!(result.rows[0][0], ResultValue::Int64(1));
    }

    #[tokio::test]
    async fn sqlite_transaction_rollback() {
        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();

        let mut tx = adapter.start_transaction(None).await.unwrap();
        let insert = SqlQuery {
            sql: "INSERT INTO test (id) VALUES (?1)".to_string(),
            args: vec![prisma_driver_core::QueryValue::Int32(1)],
            arg_types: vec![],
        };
        tx.execute_raw(insert).await.unwrap();
        tx.rollback().await.unwrap();

        let select = SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM test".to_string(),
            args: vec![],
            arg_types: vec![],
        };
        let result = adapter.query_raw(select).await.unwrap();
        assert_eq!(result.rows[0][0], ResultValue::Int64(0));
    }

    #[tokio::test]
    async fn sqlite_savepoints() {
        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();

        let mut tx = adapter.start_transaction(None).await.unwrap();

        let insert1 = SqlQuery {
            sql: "INSERT INTO test (id) VALUES (?1)".to_string(),
            args: vec![prisma_driver_core::QueryValue::Int32(1)],
            arg_types: vec![],
        };
        tx.execute_raw(insert1).await.unwrap();

        tx.create_savepoint("sp1").await.unwrap();

        let insert2 = SqlQuery {
            sql: "INSERT INTO test (id) VALUES (?1)".to_string(),
            args: vec![prisma_driver_core::QueryValue::Int32(2)],
            arg_types: vec![],
        };
        tx.execute_raw(insert2).await.unwrap();

        tx.rollback_to_savepoint("sp1").await.unwrap();
        tx.release_savepoint("sp1").await.unwrap();

        tx.commit().await.unwrap();

        let select = SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM test".to_string(),
            args: vec![],
            arg_types: vec![],
        };
        let result = adapter.query_raw(select).await.unwrap();
        // Only the first insert should survive (savepoint rolled back the second).
        // COUNT(*) has no declared type, so SQLite infers Int64 from the integer value.
        assert_eq!(result.rows[0][0], ResultValue::Int64(1));
    }

    #[tokio::test]
    async fn sqlite_error_unique_constraint() {
        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script("CREATE TABLE test (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)")
            .await
            .unwrap();

        let insert = SqlQuery {
            sql: "INSERT INTO test (id, email) VALUES (?1, ?2)".to_string(),
            args: vec![
                prisma_driver_core::QueryValue::Int32(1),
                prisma_driver_core::QueryValue::Text("foo@bar.com".to_string()),
            ],
            arg_types: vec![],
        };
        adapter.execute_raw(insert.clone()).await.unwrap();

        let insert2 = SqlQuery {
            sql: "INSERT INTO test (id, email) VALUES (?1, ?2)".to_string(),
            args: vec![
                prisma_driver_core::QueryValue::Int32(2),
                prisma_driver_core::QueryValue::Text("foo@bar.com".to_string()),
            ],
            arg_types: vec![],
        };
        let err = adapter.execute_raw(insert2).await.unwrap_err();
        assert!(
            matches!(err.mapped, MappedError::UniqueConstraintViolation { .. }),
            "Expected UniqueConstraintViolation, got {:?}",
            err.mapped
        );
    }

    #[tokio::test]
    async fn sqlite_data_types() {
        use prisma_driver_core::ColumnType;

        let factory = SqliteDriverAdapterFactory::new(":memory:");
        let mut adapter = factory.connect().await.unwrap();

        adapter
            .execute_script(
                "CREATE TABLE type_test (
                    id INTEGER PRIMARY KEY,
                    bool_col BOOLEAN,
                    int_col INTEGER,
                    bigint_col BIGINT,
                    real_col REAL,
                    double_col DOUBLE PRECISION,
                    text_col TEXT,
                    varchar_col VARCHAR(100),
                    date_col DATE,
                    datetime_col DATETIME,
                    blob_col BLOB
                )",
            )
            .await
            .unwrap();

        let insert = SqlQuery {
            sql: "INSERT INTO type_test (id, bool_col, int_col, bigint_col, real_col, double_col,
                  text_col, varchar_col, date_col, datetime_col, blob_col)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
                .to_string(),
            args: vec![
                prisma_driver_core::QueryValue::Int32(1),
                prisma_driver_core::QueryValue::Boolean(true),
                prisma_driver_core::QueryValue::Int32(42),
                prisma_driver_core::QueryValue::Int64(9_000_000_000),
                prisma_driver_core::QueryValue::Float(1.23),
                prisma_driver_core::QueryValue::Double(5.6789),
                prisma_driver_core::QueryValue::Text("hello world".into()),
                prisma_driver_core::QueryValue::Text("short".into()),
                prisma_driver_core::QueryValue::Text("2025-06-15".into()),
                prisma_driver_core::QueryValue::Text("2025-06-15 14:30:00".into()),
                prisma_driver_core::QueryValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ],
            arg_types: vec![],
        };
        adapter.execute_raw(insert).await.unwrap();

        let select = SqlQuery {
            sql: "SELECT bool_col, int_col, bigint_col, real_col, double_col,
                  text_col, varchar_col, date_col, datetime_col, blob_col
                  FROM type_test"
                .to_string(),
            args: vec![],
            arg_types: vec![],
        };
        let result = adapter.query_raw(select).await.unwrap();
        let row = &result.rows[0];

        // Verify column types from declared types
        assert_eq!(result.column_types[0], ColumnType::Boolean);
        assert_eq!(result.column_types[1], ColumnType::Int32); // INTEGER -> Int32 (declared type)
        assert_eq!(result.column_types[2], ColumnType::Int64); // BIGINT
        assert_eq!(result.column_types[3], ColumnType::Double); // REAL
        assert_eq!(result.column_types[4], ColumnType::Double); // DOUBLE PRECISION
        assert_eq!(result.column_types[5], ColumnType::Text);
        assert_eq!(result.column_types[6], ColumnType::Text); // VARCHAR
        assert_eq!(result.column_types[7], ColumnType::Date);
        assert_eq!(result.column_types[8], ColumnType::DateTime);
        assert_eq!(result.column_types[9], ColumnType::Bytes); // BLOB

        // Verify values
        assert_eq!(row[0], ResultValue::Boolean(true));
        assert_eq!(row[1], ResultValue::Int32(42)); // INTEGER declared type -> Int32
        assert_eq!(row[2], ResultValue::Int64(9_000_000_000));
        assert_eq!(row[5], ResultValue::Text("hello world".into()));
        assert_eq!(row[6], ResultValue::Text("short".into()));
        assert_eq!(row[7], ResultValue::Date("2025-06-15".into()));
        assert_eq!(row[8], ResultValue::DateTime("2025-06-15 14:30:00".into()));
        assert_eq!(row[9], ResultValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }
}
