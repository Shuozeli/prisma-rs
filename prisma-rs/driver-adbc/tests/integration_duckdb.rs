//! Integration tests for Arrow conversion using DuckDB's native Arrow output.
//!
//! These tests verify that our Arrow -> Prisma conversion handles real database
//! output correctly, using DuckDB as the Arrow data source.
//!
//! DuckDB re-exports arrow v56, the same version our driver uses, so
//! `duckdb::arrow::record_batch::RecordBatch` == `arrow_array::RecordBatch`.

use duckdb::Connection;
use prisma_driver_adbc::arrow::record_batches_to_result_set;
use prisma_driver_core::ResultValue;

fn query_arrow(conn: &Connection, sql: &str) -> Vec<arrow_array::RecordBatch> {
    let mut stmt = conn.prepare(sql).unwrap();
    // DuckDB's query_arrow returns its own Arrow type, but since duckdb re-exports
    // arrow v56 (same as our dependency), the types are identical.
    stmt.query_arrow([]).unwrap().collect()
}

#[test]
fn duckdb_arrow_basic_types() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE test (id INTEGER, name VARCHAR, active BOOLEAN);
         INSERT INTO test VALUES (1, 'Alice', true);
         INSERT INTO test VALUES (2, 'Bob', false);",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT id, name, active FROM test ORDER BY id");
    let result = record_batches_to_result_set(&batches);

    assert_eq!(result.column_names, vec!["id", "name", "active"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], ResultValue::Int32(1));
    assert_eq!(result.rows[0][1], ResultValue::Text("Alice".into()));
    assert_eq!(result.rows[0][2], ResultValue::Boolean(true));
    assert_eq!(result.rows[1][0], ResultValue::Int32(2));
    assert_eq!(result.rows[1][1], ResultValue::Text("Bob".into()));
    assert_eq!(result.rows[1][2], ResultValue::Boolean(false));
}

#[test]
fn duckdb_arrow_numeric_types() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE nums (i BIGINT, f FLOAT, d DOUBLE);
         INSERT INTO nums VALUES (9999999999, 1.5, 2.718281828);",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT i, f, d FROM nums");
    let result = record_batches_to_result_set(&batches);

    assert_eq!(result.rows[0][0], ResultValue::Int64(9999999999));
    assert_eq!(result.rows[0][1], ResultValue::Float(1.5));
    if let ResultValue::Double(v) = result.rows[0][2] {
        assert!((v - std::f64::consts::E).abs() < 1e-6);
    } else {
        panic!("Expected Double");
    }
}

#[test]
fn duckdb_arrow_null_values() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE nullable (val INTEGER);
         INSERT INTO nullable VALUES (1);
         INSERT INTO nullable VALUES (NULL);
         INSERT INTO nullable VALUES (3);",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT val FROM nullable ORDER BY rowid");
    let result = record_batches_to_result_set(&batches);

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], ResultValue::Int32(1));
    assert_eq!(result.rows[1][0], ResultValue::Null);
    assert_eq!(result.rows[2][0], ResultValue::Int32(3));
}

#[test]
fn duckdb_arrow_timestamp() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE ts (t TIMESTAMP);
         INSERT INTO ts VALUES ('2024-06-15 10:30:00');",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT t FROM ts");
    let result = record_batches_to_result_set(&batches);

    if let ResultValue::DateTime(s) = &result.rows[0][0] {
        assert!(s.starts_with("2024-06-15 10:30:00"), "Unexpected timestamp: {s}");
    } else {
        panic!("Expected DateTime, got {:?}", result.rows[0][0]);
    }
}

#[test]
fn duckdb_arrow_date_and_time() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE dt (d DATE, t TIME);
         INSERT INTO dt VALUES ('2024-03-14', '13:45:30');",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT d, t FROM dt");
    let result = record_batches_to_result_set(&batches);

    if let ResultValue::Date(s) = &result.rows[0][0] {
        assert_eq!(s, "2024-03-14");
    } else {
        panic!("Expected Date, got {:?}", result.rows[0][0]);
    }

    if let ResultValue::Time(s) = &result.rows[0][1] {
        assert!(s.starts_with("13:45:30"), "Unexpected time: {s}");
    } else {
        panic!("Expected Time, got {:?}", result.rows[0][1]);
    }
}

#[test]
fn duckdb_arrow_boolean_and_blob() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE bb (b BOOLEAN, data BLOB);
         INSERT INTO bb VALUES (true, '\\x48454C4C4F'::BLOB);",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT b, data FROM bb");
    let result = record_batches_to_result_set(&batches);

    assert_eq!(result.rows[0][0], ResultValue::Boolean(true));
    if let ResultValue::Bytes(bytes) = &result.rows[0][1] {
        assert!(!bytes.is_empty());
    } else {
        panic!("Expected Bytes, got {:?}", result.rows[0][1]);
    }
}

#[test]
fn duckdb_arrow_count_aggregate() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE items (id INTEGER);
         INSERT INTO items VALUES (1);
         INSERT INTO items VALUES (2);
         INSERT INTO items VALUES (3);",
    )
    .unwrap();

    let batches = query_arrow(&conn, "SELECT COUNT(*) as cnt FROM items");
    let result = record_batches_to_result_set(&batches);

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ResultValue::Int64(3));
}

#[test]
fn duckdb_arrow_empty_result() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE empty (id INTEGER, name VARCHAR)")
        .unwrap();

    let batches = query_arrow(&conn, "SELECT id, name FROM empty");
    let result = record_batches_to_result_set(&batches);

    // DuckDB may return zero batches for empty results (no schema preserved).
    // In that case column_names will be empty. When batches exist, schema is available.
    assert_eq!(result.rows.len(), 0);
}
