use prisma_driver_core::{
    ColumnType, DatabaseUrl, MappedError, QueryValue, ResultValue, SqlDriverAdapter, SqlDriverAdapterFactory, SqlQuery,
};
use prisma_driver_pg::PgDriverAdapterFactory;

fn connection_url() -> DatabaseUrl {
    let raw = std::env::var("PG_TEST_URL")
        .unwrap_or_else(|_| "postgresql://prisma:prisma@127.0.0.1:15432/prisma_test".into());
    DatabaseUrl::parse(&raw).expect("invalid PG_TEST_URL")
}

async fn setup(table: &str) -> Box<dyn SqlDriverAdapter> {
    let factory = PgDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             CREATE TABLE {table} (
                 id SERIAL PRIMARY KEY,
                 name TEXT NOT NULL,
                 email TEXT UNIQUE NOT NULL,
                 age INTEGER,
                 score DOUBLE PRECISION,
                 active BOOLEAN NOT NULL DEFAULT TRUE
             );"
        ))
        .await
        .unwrap();

    adapter
}

#[tokio::test]
async fn pg_basic_insert_and_select() {
    let table = "pg_test_basic";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email, age, score) VALUES ($1, $2, $3, $4)"),
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
        sql: format!("SELECT id, name, email, age, score, active FROM {table} WHERE name = $1"),
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
    assert_eq!(result.rows[0][5], ResultValue::Boolean(true));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_null_handling() {
    let table = "pg_test_null";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email, age) VALUES ($1, $2, $3)"),
        args: vec![
            QueryValue::Text("Bob".into()),
            QueryValue::Text("bob@example.com".into()),
            QueryValue::Null,
        ],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let select = SqlQuery {
        sql: format!("SELECT age, score FROM {table} WHERE name = $1"),
        args: vec![QueryValue::Text("Bob".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Null);
    assert_eq!(result.rows[0][1], ResultValue::Null);

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_transaction_commit() {
    let table = "pg_test_tx_commit";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
        args: vec![
            QueryValue::Text("TxCommit".into()),
            QueryValue::Text("tx-commit@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert).await.unwrap();
    tx.commit().await.unwrap();

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table} WHERE name = $1"),
        args: vec![QueryValue::Text("TxCommit".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(1));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_transaction_rollback() {
    let table = "pg_test_tx_rollback";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
        args: vec![
            QueryValue::Text("TxRollback".into()),
            QueryValue::Text("tx-rollback@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert).await.unwrap();
    tx.rollback().await.unwrap();

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table} WHERE name = $1"),
        args: vec![QueryValue::Text("TxRollback".into())],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(0));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_savepoints() {
    let table = "pg_test_savepoints";
    let mut adapter = setup(table).await;

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let insert1 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
        args: vec![
            QueryValue::Text("SP-Keep".into()),
            QueryValue::Text("sp-keep@example.com".into()),
        ],
        arg_types: vec![],
    };
    tx.execute_raw(insert1).await.unwrap();

    tx.create_savepoint("sp1").await.unwrap();

    let insert2 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
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

    let select = SqlQuery {
        sql: format!("SELECT name FROM {table} ORDER BY id"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ResultValue::Text("SP-Keep".into()));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_unique_constraint_error() {
    let table = "pg_test_unique_err";
    let mut adapter = setup(table).await;

    let insert = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
        args: vec![
            QueryValue::Text("Dup".into()),
            QueryValue::Text("dup@example.com".into()),
        ],
        arg_types: vec![],
    };
    adapter.execute_raw(insert).await.unwrap();

    let insert2 = SqlQuery {
        sql: format!("INSERT INTO {table} (name, email) VALUES ($1, $2)"),
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

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_multiple_rows() {
    let table = "pg_test_multi_rows";
    let mut adapter = setup(table).await;

    for i in 0..5 {
        let insert = SqlQuery {
            sql: format!("INSERT INTO {table} (name, email, age) VALUES ($1, $2, $3)"),
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

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_execute_script() {
    let table = "pg_test_script";
    let mut adapter = setup(table).await;

    adapter
        .execute_script(&format!(
            "INSERT INTO {table} (name, email) VALUES ('Script1', 'script1@example.com');
             INSERT INTO {table} (name, email) VALUES ('Script2', 'script2@example.com');"
        ))
        .await
        .unwrap();

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table}"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(2));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_data_types() {
    let table = "pg_test_types";
    let factory = PgDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             CREATE TABLE {table} (
                 id SERIAL PRIMARY KEY,
                 bool_col BOOLEAN,
                 int2_col SMALLINT,
                 int4_col INTEGER,
                 int8_col BIGINT,
                 float4_col REAL,
                 float8_col DOUBLE PRECISION,
                 text_col TEXT,
                 varchar_col VARCHAR(100),
                 date_col DATE,
                 time_col TIME,
                 ts_col TIMESTAMP,
                 json_col JSON,
                 jsonb_col JSONB,
                 uuid_col UUID,
                 bytes_col BYTEA
             );"
        ))
        .await
        .unwrap();

    // Insert using execute_script with literals (avoids parameter type inference issues)
    adapter
        .execute_script(&format!(
            "INSERT INTO {table} (bool_col, int2_col, int4_col, int8_col, float4_col, float8_col,
             text_col, varchar_col, date_col, time_col, ts_col,
             json_col, jsonb_col, uuid_col, bytes_col)
             VALUES (TRUE, 42, 100000, 9000000000, 1.23, 5.6789,
             'hello world', 'short', '2025-06-15', '14:30:00', '2025-06-15 14:30:00',
             '{{\"key\": \"value\"}}', '{{\"nested\": {{\"a\": 1}}}}',
             '550e8400-e29b-41d4-a716-446655440000', E'\\\\xDEADBEEF')"
        ))
        .await
        .unwrap();

    let select = SqlQuery {
        sql: format!(
            "SELECT bool_col, int2_col, int4_col, int8_col, float4_col, float8_col,
             text_col, varchar_col, date_col, time_col, ts_col,
             json_col, jsonb_col, uuid_col, bytes_col
             FROM {table} WHERE id = 1"
        ),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    let row = &result.rows[0];

    // Verify column types
    assert_eq!(result.column_types[0], ColumnType::Boolean);
    assert_eq!(result.column_types[1], ColumnType::Int32); // smallint
    assert_eq!(result.column_types[2], ColumnType::Int32); // integer
    assert_eq!(result.column_types[3], ColumnType::Int64); // bigint
    assert_eq!(result.column_types[4], ColumnType::Float);
    assert_eq!(result.column_types[5], ColumnType::Double);
    assert_eq!(result.column_types[6], ColumnType::Text);
    assert_eq!(result.column_types[7], ColumnType::Text); // varchar
    assert_eq!(result.column_types[8], ColumnType::Date);
    assert_eq!(result.column_types[9], ColumnType::Time);
    assert_eq!(result.column_types[10], ColumnType::DateTime);
    assert_eq!(result.column_types[11], ColumnType::Json);
    assert_eq!(result.column_types[12], ColumnType::Json); // jsonb
    assert_eq!(result.column_types[13], ColumnType::Uuid);
    assert_eq!(result.column_types[14], ColumnType::Bytes);

    // Verify values
    assert_eq!(row[0], ResultValue::Boolean(true));
    assert_eq!(row[1], ResultValue::Int32(42));
    assert_eq!(row[2], ResultValue::Int32(100000));
    assert_eq!(row[3], ResultValue::Int64(9_000_000_000));
    assert!(matches!(&row[4], ResultValue::Float(v) if (*v - 1.23).abs() < 0.01));
    assert!(matches!(&row[5], ResultValue::Double(v) if (*v - 5.6789).abs() < 0.0001));
    assert_eq!(row[6], ResultValue::Text("hello world".into()));
    assert_eq!(row[7], ResultValue::Text("short".into()));
    assert!(matches!(&row[8], ResultValue::Date(d) if d == "2025-06-15"));
    assert!(matches!(&row[13], ResultValue::Uuid(u) if u == "550e8400-e29b-41d4-a716-446655440000"));
    assert_eq!(row[14], ResultValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_parameterized_types() {
    let table = "pg_test_params";
    let factory = PgDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             CREATE TABLE {table} (
                 id SERIAL PRIMARY KEY,
                 val TEXT NOT NULL,
                 kind TEXT NOT NULL
             );"
        ))
        .await
        .unwrap();

    // Test each param type individually to verify serialization
    let test_cases: Vec<(&str, QueryValue)> = vec![
        ("bool", QueryValue::Boolean(true)),
        ("int32", QueryValue::Int32(42)),
        ("int64", QueryValue::Int64(9_000_000_000)),
        ("float", QueryValue::Float(1.23)),
        ("double", QueryValue::Double(5.67)),
        ("text", QueryValue::Text("hello".into())),
        (
            "uuid",
            QueryValue::Uuid(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
        ),
        ("bytes", QueryValue::Bytes(vec![0xDE, 0xAD])),
        ("json", QueryValue::Json(serde_json::json!({"k": "v"}))),
        (
            "date",
            QueryValue::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
        ),
        (
            "time",
            QueryValue::Time(chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        ),
        (
            "datetime",
            QueryValue::DateTime(
                chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
            ),
        ),
        ("null", QueryValue::Null),
    ];

    for (kind, val) in &test_cases {
        let insert = SqlQuery {
            sql: format!("INSERT INTO {table} (val, kind) VALUES ($1::text, $2)"),
            args: vec![
                // Cast to text so PG accepts any input type
                match val {
                    QueryValue::Null => QueryValue::Text("NULL".into()),
                    QueryValue::Boolean(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Int32(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Int64(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Float(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Double(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Text(v) => QueryValue::Text(v.clone()),
                    QueryValue::Uuid(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Bytes(v) => QueryValue::Text(format!("{:?}", v)),
                    QueryValue::Json(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Date(v) => QueryValue::Text(v.to_string()),
                    QueryValue::Time(v) => QueryValue::Text(v.to_string()),
                    QueryValue::DateTime(v) => QueryValue::Text(v.to_string()),
                    _ => QueryValue::Text("unknown".into()),
                },
                QueryValue::Text(kind.to_string()),
            ],
            arg_types: vec![],
        };
        adapter
            .execute_raw(insert)
            .await
            .unwrap_or_else(|e| panic!("Failed to insert {kind}: {e:?}"));
    }

    let select = SqlQuery {
        sql: format!("SELECT COUNT(*) FROM {table}"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    assert_eq!(result.rows[0][0], ResultValue::Int64(test_cases.len() as i64));

    adapter.dispose().await.unwrap();
}

#[tokio::test]
async fn pg_array_types() {
    let table = "pg_test_arrays";
    let factory = PgDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();

    adapter
        .execute_script(&format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             CREATE TABLE {table} (
                 id SERIAL PRIMARY KEY,
                 int_arr INTEGER[],
                 text_arr TEXT[],
                 bool_arr BOOLEAN[]
             );"
        ))
        .await
        .unwrap();

    adapter
        .execute_script(&format!(
            "INSERT INTO {table} (int_arr, text_arr, bool_arr)
             VALUES (ARRAY[1,2,3], ARRAY['a','b','c'], ARRAY[true,false,true])"
        ))
        .await
        .unwrap();

    let select = SqlQuery {
        sql: format!("SELECT int_arr, text_arr, bool_arr FROM {table}"),
        args: vec![],
        arg_types: vec![],
    };
    let result = adapter.query_raw(select).await.unwrap();
    let row = &result.rows[0];

    assert_eq!(result.column_types[0], ColumnType::Int32Array);
    assert_eq!(result.column_types[1], ColumnType::TextArray);
    assert_eq!(result.column_types[2], ColumnType::BooleanArray);

    assert_eq!(
        row[0],
        ResultValue::Array(vec![
            ResultValue::Int32(1),
            ResultValue::Int32(2),
            ResultValue::Int32(3),
        ])
    );
    assert_eq!(
        row[1],
        ResultValue::Array(vec![
            ResultValue::Text("a".into()),
            ResultValue::Text("b".into()),
            ResultValue::Text("c".into()),
        ])
    );
    assert_eq!(
        row[2],
        ResultValue::Array(vec![
            ResultValue::Boolean(true),
            ResultValue::Boolean(false),
            ResultValue::Boolean(true),
        ])
    );

    adapter.dispose().await.unwrap();
}
