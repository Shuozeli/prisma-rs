use prisma_driver_core::{
    ColumnType, MappedError, QueryValue, ResultValue, SqlDriverAdapter, SqlDriverAdapterFactory, SqlQuery,
};
use prisma_driver_sqlite::SqliteDriverAdapterFactory;

async fn setup(table: &str) -> Box<dyn SqlDriverAdapter> {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!(
            "CREATE TABLE {table} (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL,
                 email TEXT UNIQUE NOT NULL,
                 age INTEGER,
                 score REAL,
                 active BOOLEAN NOT NULL DEFAULT 1
             )"
        ))
        .await
        .unwrap();

    adapter
}

#[tokio::test]
async fn sqlite_basic_insert_and_select() {
    let table = "basic_test";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email, age, score) VALUES (?, ?, ?, ?)"),
        args: vec![
            QueryValue::Text("Alice".into()),
            QueryValue::Text("alice@example.com".into()),
            QueryValue::Int32(30),
            QueryValue::Double(95.5),
        ],
        arg_types: vec![],
    };
    let affected = adapter.execute_raw(insert).await.unwrap();
    assert_eq!(affected, 1);

    let select = SqlQuery {
        sql: format!("SELECT id, name, email, age, score, active FROM {table} WHERE name = ?"),
        args: vec![QueryValue::Text("Alice".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();

    assert_eq!(
        result.column_names,
        vec!["id", "name", "email", "age", "score", "active"]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], ResultValue::Text("Alice".into()));
    assert_eq!(result.rows[0][2], ResultValue::Text("alice@example.com".into()));
    assert_eq!(result.rows[0][3], ResultValue::Int32(30));
}

#[tokio::test]
async fn sqlite_null_handling() {
    let table = "null_test";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email, age) VALUES (?, ?, ?)"),
        args: vec![
            QueryValue::Text("Bob".into()),
            QueryValue::Text("bob@example.com".into()),
            QueryValue::Null,
        ],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: format!("SELECT age, score FROM {table} WHERE name = ?"),
        args: vec![QueryValue::Text("Bob".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Null);
    assert_eq!(result.rows[0][1], ResultValue::Null);
}

#[tokio::test]
async fn sqlite_transaction_commit() {
    let table = "tx_commit_test";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("TxCommit".into()),
            QueryValue::Text("tx-commit@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert).await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table} WHERE name = ?"),
        args: vec![QueryValue::Text("TxCommit".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(1));
}

#[tokio::test]
async fn sqlite_transaction_rollback() {
    let table = "tx_rollback_test";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("TxRollback".into()),
            QueryValue::Text("tx-rollback@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert).await.unwrap();
    tx.rollback().await.unwrap();
    drop(tx);

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table} WHERE name = ?"),
        args: vec![QueryValue::Text("TxRollback".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(0));
}

#[tokio::test]
async fn sqlite_savepoints() {
    let table = "savepoint_test";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert1 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("SP-Keep".into()),
            QueryValue::Text("sp-keep@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert1).await.unwrap();

    tx.create_savepoint("sp1").await.unwrap();

    let insert2 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("SP-Discard".into()),
            QueryValue::Text("sp-discard@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert2).await.unwrap();

    tx.rollback_to_savepoint("sp1").await.unwrap();
    tx.release_savepoint("sp1").await.unwrap();
    tx.commit().await.unwrap();
    drop(tx);

    let select = SqlQuery {
        sql: format!("SELECT name FROM {table} ORDER BY id"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ResultValue::Text("SP-Keep".into()));
}

#[tokio::test]
async fn sqlite_unique_constraint_error() {
    let table = "unique_err_test";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("Dup".into()),
            QueryValue::Text("dup@example.com".into()),
        ],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let insert2 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("Dup2".into()),
            QueryValue::Text("dup@example.com".into()),
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
async fn sqlite_multiple_rows() {
    let table = "multi_rows_test";
    let mut adapter = setup(table).await;

    for i in 0..5 {
        let insert = SqlQuery {
            sql: format!("INSERT INTO {table} (name, email, age) VALUES (?, ?, ?)"),
            args: vec![
                QueryValue::Text(format!("User{i}")),
                QueryValue::Text(format!("user{i}@example.com")),
                QueryValue::Int32(20 + i),
            ],
            arg_types: vec![],
        };
        adapter.execute_raw(insert).await.unwrap();
    }

    let select = SqlQuery {
        sql: format!("SELECT name, age FROM {table} ORDER BY age"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][0], ResultValue::Text("User0".into()));
    assert_eq!(result.rows[4][0], ResultValue::Text("User4".into()));
    assert_eq!(result.rows[0][1], ResultValue::Int32(20));
    assert_eq!(result.rows[4][1], ResultValue::Int32(24));
}

#[tokio::test]
async fn sqlite_transaction_query_within_tx() {
    let table = "tx_query_test";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("InTx".into()),
            QueryValue::Text("in-tx@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert).await.unwrap();

    // Query within the same transaction should see the uncommitted row
    let select = SqlQuery {
        sql: format!("SELECT name FROM {table} WHERE email = ?"),
        args: vec![QueryValue::Text("in-tx@example.com".into())],
        arg_types: vec![],
    };
    let result = tx.query_raw(select).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ResultValue::Text("InTx".into()));

    tx.rollback().await.unwrap();
    drop(tx);

    // After rollback, the row should be gone
    let select2 = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table} WHERE email = ?"),
        args: vec![QueryValue::Text("in-tx@example.com".into())],
        arg_types: vec![],
    };
    let result2 = adapter.query_raw(select2).await.unwrap();
    assert_eq!(result2.rows[0][0], ResultValue::Int64(0));
}

#[tokio::test]
async fn sqlite_data_types() {
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
                 text_col TEXT,
                 varchar_col VARCHAR(100),
                 date_col DATE,
                 datetime_col DATETIME,
                 blob_col BLOB
             )",
        )
        .await
        .unwrap();

    adapter
        .execute_script(
            "INSERT INTO type_test (bool_col, int_col, bigint_col, real_col,
             text_col, varchar_col, date_col, datetime_col, blob_col)
             VALUES (1, 42, 9000000000, 1.23,
             'hello world', 'short', '2025-06-15', '2025-06-15 14:30:00', X'DEADBEEF')",
        )
        .await
        .unwrap();

    let select = SqlQuery {
        sql: "SELECT bool_col, int_col, bigint_col, real_col,
              text_col, varchar_col, date_col, datetime_col, blob_col
              FROM type_test"
            .to_string(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    let row = &result.rows[0];

    // Column types from declared types
    assert_eq!(result.column_types[0], ColumnType::Boolean);
    assert_eq!(result.column_types[1], ColumnType::Int32); // INTEGER
    assert_eq!(result.column_types[2], ColumnType::Int64); // BIGINT
    assert_eq!(result.column_types[3], ColumnType::Double); // REAL
    assert_eq!(result.column_types[4], ColumnType::Text);
    assert_eq!(result.column_types[5], ColumnType::Text); // VARCHAR
    assert_eq!(result.column_types[6], ColumnType::Date);
    assert_eq!(result.column_types[7], ColumnType::DateTime);
    assert_eq!(result.column_types[8], ColumnType::Bytes); // BLOB

    // Values
    assert_eq!(row[0], ResultValue::Boolean(true));
    assert_eq!(row[1], ResultValue::Int32(42));
    assert_eq!(row[2], ResultValue::Int64(9_000_000_000));
    assert!(matches!(&row[3], ResultValue::Double(v) if (*v - 1.23).abs() < 0.001));
    assert_eq!(row[4], ResultValue::Text("hello world".into()));
    assert_eq!(row[5], ResultValue::Text("short".into()));
    assert_eq!(row[6], ResultValue::Date("2025-06-15".into()));
    assert_eq!(row[7], ResultValue::DateTime("2025-06-15 14:30:00".into()));
    assert_eq!(row[8], ResultValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[tokio::test]
async fn sqlite_factory_connect() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    // Verify foreign keys are enabled
    let select = SqlQuery {
        sql: "PRAGMA foreign_keys".to_string(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(1));
}

// --- Raw query parameterization with all types ---

#[tokio::test]
async fn sqlite_raw_param_types() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(
            "CREATE TABLE param_test (
                 id INTEGER PRIMARY KEY,
                 txt TEXT,
                 int_val INTEGER,
                 real_val REAL,
                 bool_val BOOLEAN,
                 blob_val BLOB
             )",
        )
        .await
        .unwrap();

    // Insert with all param types
    let insert = SqlQuery {
        sql: "INSERT INTO param_test (txt, int_val, real_val, bool_val, blob_val) VALUES (?, ?, ?, ?, ?)".into(),
        args: vec![
            QueryValue::Text("hello".into()),
            QueryValue::Int32(42),
            QueryValue::Double(3.14159),
            QueryValue::Boolean(true),
            QueryValue::Bytes(vec![0xCA, 0xFE]),
        ],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: "SELECT txt, int_val, real_val, bool_val, blob_val FROM param_test".into(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    let row = &result.rows[0];
    assert_eq!(row[0], ResultValue::Text("hello".into()));
    assert_eq!(row[1], ResultValue::Int32(42));
    assert!(matches!(&row[2], ResultValue::Double(v) if (*v - 3.14159).abs() < 0.0001));
    assert_eq!(row[3], ResultValue::Boolean(true));
    assert_eq!(row[4], ResultValue::Bytes(vec![0xCA, 0xFE]));
}

#[tokio::test]
async fn sqlite_raw_null_param() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script("CREATE TABLE null_param_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .unwrap();

    let insert = SqlQuery {
        sql: "INSERT INTO null_param_test (val) VALUES (?)".into(),
        args: vec![QueryValue::Null],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: "SELECT val FROM null_param_test".into(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Null);
}

#[tokio::test]
async fn sqlite_raw_uuid_param() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script("CREATE TABLE uuid_test (id INTEGER PRIMARY KEY, uid TEXT)")
        .await
        .unwrap();

    let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let insert = SqlQuery {
        sql: "INSERT INTO uuid_test (uid) VALUES (?)".into(),
        args: vec![QueryValue::Uuid(u)],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: "SELECT uid FROM uuid_test".into(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(
        result.rows[0][0],
        ResultValue::Text("550e8400-e29b-41d4-a716-446655440000".into())
    );
}

#[tokio::test]
async fn sqlite_raw_datetime_param() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script("CREATE TABLE dt_test (id INTEGER PRIMARY KEY, ts DATETIME)")
        .await
        .unwrap();

    let dt = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(10, 30, 0)
        .unwrap();
    let insert = SqlQuery {
        sql: "INSERT INTO dt_test (ts) VALUES (?)".into(),
        args: vec![QueryValue::DateTime(dt)],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: "SELECT ts FROM dt_test".into(),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    if let ResultValue::DateTime(s) = &result.rows[0][0] {
        assert!(s.starts_with("2024-06-15 10:30:00"), "Got: {s}");
    } else {
        panic!("Expected DateTime, got {:?}", result.rows[0][0]);
    }
}

// --- Transaction edge cases ---

#[tokio::test]
async fn sqlite_nested_savepoints() {
    let table = "nested_sp_test";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Insert base row
    tx.execute_raw(SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![
            QueryValue::Text("Base".into()),
            QueryValue::Text("base@test.com".into()),
        ],
        arg_types: vec![],
    })
    .await
    .unwrap();

    // Create savepoint sp1, insert, then create sp2, insert, rollback sp2, release sp1
    tx.create_savepoint("sp1").await.unwrap();
    tx.execute_raw(SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![QueryValue::Text("SP1".into()), QueryValue::Text("sp1@test.com".into())],
        arg_types: vec![],
    })
    .await
    .unwrap();

    tx.create_savepoint("sp2").await.unwrap();
    tx.execute_raw(SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![QueryValue::Text("SP2".into()), QueryValue::Text("sp2@test.com".into())],
        arg_types: vec![],
    })
    .await
    .unwrap();

    // Rollback sp2 (discards SP2 row)
    tx.rollback_to_savepoint("sp2").await.unwrap();
    // Release sp1 (keeps SP1 row)
    tx.release_savepoint("sp1").await.unwrap();

    tx.commit().await.unwrap();

    // Should have Base + SP1 = 2 rows
    let result = adapter
        .query_raw(SqlQuery {
            sql: format!("SELECT name FROM {table} ORDER BY id"),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], ResultValue::Text("Base".into()));
    assert_eq!(result.rows[1][0], ResultValue::Text("SP1".into()));
}

#[tokio::test]
async fn sqlite_not_null_constraint_error() {
    let table = "notnull_err_test";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
        args: vec![QueryValue::Null, QueryValue::Text("test@test.com".into())],
        arg_types: vec![],
    };
    let err = adapter.execute_raw(insert).await.unwrap_err();
    assert!(
        matches!(err.mapped, MappedError::NullConstraintViolation { .. }),
        "Expected NullConstraintViolation, got {:?}",
        err.mapped
    );
}

#[tokio::test]
async fn sqlite_table_not_found_error() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    let select = SqlQuery {
        sql: "SELECT * FROM nonexistent_table".into(),
        args: vec![],
        arg_types: vec![],
    };
    let err = adapter.query_raw(select).await.unwrap_err();
    assert!(
        matches!(err.mapped, MappedError::TableDoesNotExist { .. }),
        "Expected TableDoesNotExist, got {:?}",
        err.mapped
    );
}

#[tokio::test]
async fn sqlite_execute_raw_returns_affected_count() {
    let table = "affected_test";
    let mut adapter = setup(table).await;

    // Insert 3 rows
    for i in 0..3 {
        adapter
            .execute_raw(SqlQuery {
                sql: format!("INSERT INTO {table} (name, email) VALUES (?, ?)"),
                args: vec![
                    QueryValue::Text("Same".into()),
                    QueryValue::Text(format!("same{i}@test.com")),
                ],
                arg_types: vec![],
            })
            .await
            .unwrap();
    }

    // Update all 3
    let affected = adapter
        .execute_raw(SqlQuery {
            sql: format!("UPDATE {table} SET name = ? WHERE name = ?"),
            args: vec![QueryValue::Text("Updated".into()), QueryValue::Text("Same".into())],
            arg_types: vec![],
        })
        .await
        .unwrap();
    assert_eq!(affected, 3);

    // Delete 1
    let affected = adapter
        .execute_raw(SqlQuery {
            sql: format!("DELETE FROM {table} WHERE email = ?"),
            args: vec![QueryValue::Text("same0@test.com".into())],
            arg_types: vec![],
        })
        .await
        .unwrap();
    assert_eq!(affected, 1);
}

#[tokio::test]
async fn sqlite_connection_info() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let adapter = factory.connect().await.unwrap();
    let info = adapter.connection_info();
    assert_eq!(info.max_bind_values, Some(999));
    assert!(!info.supports_relation_joins);
}

#[tokio::test]
async fn sqlite_execute_script_multiple_statements() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);
             CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);
             INSERT INTO t1 (val) VALUES ('a');
             INSERT INTO t2 (val) VALUES ('b');",
        )
        .await
        .unwrap();

    let r1 = adapter
        .query_raw(SqlQuery {
            sql: "SELECT val FROM t1".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
    assert_eq!(r1.rows[0][0], ResultValue::Text("a".into()));

    let r2 = adapter
        .query_raw(SqlQuery {
            sql: "SELECT val FROM t2".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
    assert_eq!(r2.rows[0][0], ResultValue::Text("b".into()));
}

#[tokio::test]
async fn sqlite_last_insert_id() {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script("CREATE TABLE lid_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .await
        .unwrap();

    let result = adapter
        .query_raw(SqlQuery {
            sql: "INSERT INTO lid_test (val) VALUES (?) RETURNING id".into(),
            args: vec![QueryValue::Text("first".into())],
            arg_types: vec![],
        })
        .await
        .unwrap();
    // RETURNING should give us the id (INTEGER maps to Int32 in SQLite)
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ResultValue::Int32(1));
}
