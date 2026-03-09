use prisma_driver_core::{
    ColumnType, DatabaseUrl, MappedError, QueryValue, ResultValue, SqlDriverAdapter, SqlDriverAdapterFactory, SqlQuery,
};
use prisma_driver_mysql::MySqlDriverAdapterFactory;

fn connection_url() -> DatabaseUrl {
    let raw =
        std::env::var("MYSQL_TEST_URL").unwrap_or_else(|_| "mysql://prisma:prisma@127.0.0.1:13306/prisma_test".into());
    DatabaseUrl::parse(&raw).expect("invalid MYSQL_TEST_URL")
}

async fn setup(table: &str) -> Box<dyn SqlDriverAdapter> {
    let factory = MySqlDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    adapter
        .execute_script(&format!(
            "CREATE TABLE {table} (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 name VARCHAR(255) NOT NULL,
                 email VARCHAR(255) UNIQUE NOT NULL,
                 age INT,
                 score DOUBLE,
                 active BOOLEAN NOT NULL DEFAULT TRUE
             )"
        ))
        .await
        .unwrap();

    adapter
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_basic_insert_and_select() {
    let table = "mysql_test_basic";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_null_handling() {
    let table = "mysql_test_null";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_transaction_commit() {
    let table = "mysql_test_tx_commit";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_transaction_rollback() {
    let table = "mysql_test_tx_rollback";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_savepoints() {
    let table = "mysql_test_savepoints";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_unique_constraint_error() {
    let table = "mysql_test_unique_err";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_multiple_rows() {
    let table = "mysql_test_multi_rows";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_transaction_query_within_tx() {
    let table = "mysql_test_tx_query";
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_data_types() {
    let table = "mysql_test_types";
    let factory = MySqlDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    adapter
        .execute_script(&format!(
            "CREATE TABLE {table} (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 bool_col BOOLEAN,
                 tinyint_col TINYINT,
                 int_col INT,
                 bigint_col BIGINT,
                 float_col FLOAT,
                 double_col DOUBLE,
                 decimal_col DECIMAL(10, 2),
                 text_col TEXT,
                 varchar_col VARCHAR(100),
                 date_col DATE,
                 time_col TIME,
                 datetime_col DATETIME,
                 timestamp_col TIMESTAMP NULL,
                 json_col JSON,
                 blob_col BLOB,
                 enum_col ENUM('a', 'b', 'c')
             )"
        ))
        .await
        .unwrap();

    adapter
        .execute_script(&format!(
            "INSERT INTO {table} (bool_col, tinyint_col, int_col, bigint_col, float_col, double_col,
             decimal_col, text_col, varchar_col, date_col, time_col, datetime_col, timestamp_col,
             json_col, blob_col, enum_col)
             VALUES (TRUE, 42, 100000, 9000000000, 1.23, 5.6789,
             12345.67, 'hello world', 'short', '2025-06-15', '14:30:00', '2025-06-15 14:30:00',
             '2025-06-15 14:30:00', '{{\"key\": \"value\"}}', X'DEADBEEF', 'b')"
        ))
        .await
        .unwrap();

    let select = SqlQuery {
        sql: format!(
            "SELECT bool_col, tinyint_col, int_col, bigint_col, float_col, double_col,
             decimal_col, text_col, varchar_col, date_col, time_col, datetime_col,
             json_col, blob_col, enum_col
             FROM {table} WHERE id = 1"
        ),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    let row = &result.rows[0];

    // Column types
    assert_eq!(result.column_types[0], ColumnType::Int32); // BOOLEAN is TINYINT(1) in MySQL
    assert_eq!(result.column_types[1], ColumnType::Int32); // TINYINT
    assert_eq!(result.column_types[2], ColumnType::Int32); // INT
    assert_eq!(result.column_types[3], ColumnType::Int64); // BIGINT
    assert_eq!(result.column_types[4], ColumnType::Float);
    assert_eq!(result.column_types[5], ColumnType::Double);
    assert_eq!(result.column_types[6], ColumnType::Numeric); // DECIMAL
    assert_eq!(result.column_types[7], ColumnType::Text);
    assert_eq!(result.column_types[8], ColumnType::Text); // VARCHAR
    assert_eq!(result.column_types[9], ColumnType::Date);
    assert_eq!(result.column_types[10], ColumnType::Time);
    assert_eq!(result.column_types[11], ColumnType::DateTime);
    assert_eq!(result.column_types[12], ColumnType::Json);
    assert_eq!(result.column_types[13], ColumnType::Bytes); // BLOB with BINARY flag
    // MySQL reports ENUM as STRING type at the wire protocol level
    assert!(
        result.column_types[14] == ColumnType::Enum || result.column_types[14] == ColumnType::Text,
        "Expected Enum or Text for ENUM column, got {:?}",
        result.column_types[14]
    );

    // Values
    assert_eq!(row[0], ResultValue::Int32(1)); // TRUE -> 1
    assert_eq!(row[1], ResultValue::Int32(42));
    assert_eq!(row[2], ResultValue::Int32(100000));
    assert_eq!(row[3], ResultValue::Int64(9_000_000_000));
    assert!(matches!(&row[4], ResultValue::Float(v) if (*v - 1.23).abs() < 0.01));
    assert!(matches!(&row[5], ResultValue::Double(v) if (*v - 5.6789).abs() < 0.0001));
    assert_eq!(row[6], ResultValue::Numeric("12345.67".into()));
    assert_eq!(row[7], ResultValue::Text("hello world".into()));
    assert_eq!(row[8], ResultValue::Text("short".into()));
    assert_eq!(row[9], ResultValue::Date("2025-06-15".into()));
    assert_eq!(row[10], ResultValue::Time("14:30:00".into()));
    assert_eq!(row[13], ResultValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    // ENUM value rendered as Text regardless of column type detection
    assert!(
        row[14] == ResultValue::Text("b".into()) || row[14] == ResultValue::Enum("b".into()),
        "Expected Text or Enum 'b', got {:?}",
        row[14]
    );
}
