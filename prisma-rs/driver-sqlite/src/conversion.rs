use prisma_driver_core::{ColumnType, QueryValue, ResultValue};
use rusqlite::types::ValueRef;

/// Map a SQLite declared column type string to a Prisma `ColumnType`.
///
/// SQLite is dynamically typed, so we infer from the declared type name.
/// This mirrors the TS `mapDeclType()` function.
pub fn decl_type_to_column_type(decl_type: Option<&str>) -> Option<ColumnType> {
    let dt = decl_type?.to_uppercase();

    if dt.contains("INT") {
        // INTEGER, INT, BIGINT, SMALLINT, TINYINT, etc.
        if dt.contains("BIG") {
            return Some(ColumnType::Int64);
        }
        return Some(ColumnType::Int32);
    }
    if dt.contains("CHAR") || dt.contains("CLOB") || dt.contains("TEXT") {
        return Some(ColumnType::Text);
    }
    if dt.contains("BLOB") || dt == "BINARY" || dt == "VARBINARY" {
        return Some(ColumnType::Bytes);
    }
    if dt.contains("REAL") || dt.contains("FLOA") || dt.contains("DOUB") {
        return Some(ColumnType::Double);
    }
    if dt.contains("BOOL") {
        return Some(ColumnType::Boolean);
    }
    if dt.contains("DATETIME") || dt.contains("TIMESTAMP") {
        return Some(ColumnType::DateTime);
    }
    if dt.contains("DATE") {
        return Some(ColumnType::Date);
    }
    if dt.contains("TIME") {
        return Some(ColumnType::Time);
    }
    if dt.contains("DECIMAL") || dt.contains("NUMERIC") {
        return Some(ColumnType::Numeric);
    }
    if dt.contains("JSON") {
        return Some(ColumnType::Json);
    }
    if dt.contains("UUID") {
        return Some(ColumnType::Uuid);
    }

    None
}

/// Infer column type from the actual SQLite value when declared type is unknown.
pub fn infer_column_type(value: ValueRef<'_>) -> ColumnType {
    match value {
        ValueRef::Null => ColumnType::Int32,
        ValueRef::Integer(_) => ColumnType::Int64,
        ValueRef::Real(_) => ColumnType::Double,
        ValueRef::Text(_) => ColumnType::Text,
        ValueRef::Blob(_) => ColumnType::Bytes,
    }
}

/// Convert a `QueryValue` to a `rusqlite::types::Value`.
pub fn query_value_to_sqlite(value: &QueryValue) -> rusqlite::types::Value {
    match value {
        QueryValue::Null => rusqlite::types::Value::Null,
        QueryValue::Boolean(v) => rusqlite::types::Value::Integer(if *v { 1 } else { 0 }),
        QueryValue::Int32(v) => rusqlite::types::Value::Integer(*v as i64),
        QueryValue::Int64(v) => rusqlite::types::Value::Integer(*v),
        QueryValue::Float(v) => rusqlite::types::Value::Real(*v as f64),
        QueryValue::Double(v) => rusqlite::types::Value::Real(*v),
        QueryValue::Numeric(v) => rusqlite::types::Value::Text(v.to_string()),
        QueryValue::Text(v) => rusqlite::types::Value::Text(v.clone()),
        QueryValue::Bytes(v) => rusqlite::types::Value::Blob(v.clone()),
        QueryValue::Uuid(v) => rusqlite::types::Value::Text(v.to_string()),
        QueryValue::DateTime(v) => rusqlite::types::Value::Text(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        QueryValue::Date(v) => rusqlite::types::Value::Text(v.format("%Y-%m-%d").to_string()),
        QueryValue::Time(v) => rusqlite::types::Value::Text(v.format("%H:%M:%S%.f").to_string()),
        QueryValue::Json(v) => rusqlite::types::Value::Text(v.to_string()),
        QueryValue::Array(_) => rusqlite::types::Value::Null,
    }
}

/// Extract a `ResultValue` from a SQLite row value.
pub fn sqlite_value_to_result(value: ValueRef<'_>, col_type: ColumnType) -> ResultValue {
    match value {
        ValueRef::Null => ResultValue::Null,
        ValueRef::Integer(v) => match col_type {
            ColumnType::Boolean => ResultValue::Boolean(v != 0),
            ColumnType::Int32 => ResultValue::Int32(v as i32),
            ColumnType::Int64 => ResultValue::Int64(v),
            ColumnType::Double | ColumnType::Float => ResultValue::Double(v as f64),
            _ => ResultValue::Int64(v),
        },
        ValueRef::Real(v) => match col_type {
            ColumnType::Float => ResultValue::Float(v as f32),
            ColumnType::Double => ResultValue::Double(v),
            ColumnType::Int32 => ResultValue::Int32(v as i32),
            ColumnType::Int64 => ResultValue::Int64(v as i64),
            _ => ResultValue::Double(v),
        },
        ValueRef::Text(bytes) => {
            let s = String::from_utf8_lossy(bytes).to_string();
            match col_type {
                ColumnType::DateTime => ResultValue::DateTime(s),
                ColumnType::Date => ResultValue::Date(s),
                ColumnType::Time => ResultValue::Time(s),
                ColumnType::Json => ResultValue::Json(s),
                ColumnType::Uuid => ResultValue::Uuid(s),
                ColumnType::Enum => ResultValue::Enum(s),
                ColumnType::Numeric => ResultValue::Numeric(s),
                _ => ResultValue::Text(s),
            }
        }
        ValueRef::Blob(bytes) => ResultValue::Bytes(bytes.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decl_type_mapping() {
        assert_eq!(decl_type_to_column_type(Some("INTEGER")), Some(ColumnType::Int32));
        assert_eq!(decl_type_to_column_type(Some("BIGINT")), Some(ColumnType::Int64));
        assert_eq!(decl_type_to_column_type(Some("TEXT")), Some(ColumnType::Text));
        assert_eq!(decl_type_to_column_type(Some("VARCHAR(255)")), Some(ColumnType::Text));
        assert_eq!(decl_type_to_column_type(Some("BLOB")), Some(ColumnType::Bytes));
        assert_eq!(decl_type_to_column_type(Some("REAL")), Some(ColumnType::Double));
        assert_eq!(decl_type_to_column_type(Some("BOOLEAN")), Some(ColumnType::Boolean));
        assert_eq!(decl_type_to_column_type(Some("DATETIME")), Some(ColumnType::DateTime));
        assert_eq!(decl_type_to_column_type(Some("DATE")), Some(ColumnType::Date));
        assert_eq!(decl_type_to_column_type(Some("TIME")), Some(ColumnType::Time));
        assert_eq!(decl_type_to_column_type(Some("DECIMAL")), Some(ColumnType::Numeric));
        assert_eq!(decl_type_to_column_type(Some("JSON")), Some(ColumnType::Json));
        assert_eq!(decl_type_to_column_type(None), None);
    }

    #[test]
    fn boolean_conversion() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(1), ColumnType::Boolean),
            ResultValue::Boolean(true)
        );
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(0), ColumnType::Boolean),
            ResultValue::Boolean(false)
        );
    }

    // --- decl_type_to_column_type edge cases ---

    #[test]
    fn decl_type_uuid() {
        assert_eq!(decl_type_to_column_type(Some("UUID")), Some(ColumnType::Uuid));
    }

    #[test]
    fn decl_type_timestamp() {
        assert_eq!(decl_type_to_column_type(Some("TIMESTAMP")), Some(ColumnType::DateTime));
    }

    #[test]
    fn decl_type_case_insensitive() {
        assert_eq!(decl_type_to_column_type(Some("integer")), Some(ColumnType::Int32));
        assert_eq!(decl_type_to_column_type(Some("Text")), Some(ColumnType::Text));
        assert_eq!(decl_type_to_column_type(Some("boolean")), Some(ColumnType::Boolean));
    }

    #[test]
    fn decl_type_smallint() {
        assert_eq!(decl_type_to_column_type(Some("SMALLINT")), Some(ColumnType::Int32));
    }

    #[test]
    fn decl_type_tinyint() {
        assert_eq!(decl_type_to_column_type(Some("TINYINT")), Some(ColumnType::Int32));
    }

    #[test]
    fn decl_type_double() {
        assert_eq!(decl_type_to_column_type(Some("DOUBLE")), Some(ColumnType::Double));
        assert_eq!(
            decl_type_to_column_type(Some("DOUBLE PRECISION")),
            Some(ColumnType::Double)
        );
    }

    #[test]
    fn decl_type_float() {
        assert_eq!(decl_type_to_column_type(Some("FLOAT")), Some(ColumnType::Double));
    }

    #[test]
    fn decl_type_clob() {
        assert_eq!(decl_type_to_column_type(Some("CLOB")), Some(ColumnType::Text));
    }

    #[test]
    fn decl_type_binary() {
        assert_eq!(decl_type_to_column_type(Some("BINARY")), Some(ColumnType::Bytes));
        assert_eq!(decl_type_to_column_type(Some("VARBINARY")), Some(ColumnType::Bytes));
    }

    #[test]
    fn decl_type_numeric() {
        assert_eq!(decl_type_to_column_type(Some("NUMERIC")), Some(ColumnType::Numeric));
        assert_eq!(
            decl_type_to_column_type(Some("DECIMAL(10,2)")),
            Some(ColumnType::Numeric)
        );
    }

    #[test]
    fn decl_type_unknown() {
        assert_eq!(decl_type_to_column_type(Some("FOOBAR")), None);
    }

    // --- infer_column_type ---

    #[test]
    fn infer_null() {
        assert_eq!(infer_column_type(ValueRef::Null), ColumnType::Int32);
    }

    #[test]
    fn infer_integer() {
        assert_eq!(infer_column_type(ValueRef::Integer(42)), ColumnType::Int64);
    }

    #[test]
    fn infer_real() {
        assert_eq!(infer_column_type(ValueRef::Real(3.14)), ColumnType::Double);
    }

    #[test]
    fn infer_text() {
        assert_eq!(infer_column_type(ValueRef::Text(b"hello")), ColumnType::Text);
    }

    #[test]
    fn infer_blob() {
        assert_eq!(infer_column_type(ValueRef::Blob(&[0, 1, 2])), ColumnType::Bytes);
    }

    // --- query_value_to_sqlite ---

    #[test]
    fn query_value_null() {
        assert_eq!(query_value_to_sqlite(&QueryValue::Null), rusqlite::types::Value::Null);
    }

    #[test]
    fn query_value_boolean_true() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Boolean(true)),
            rusqlite::types::Value::Integer(1)
        );
    }

    #[test]
    fn query_value_boolean_false() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Boolean(false)),
            rusqlite::types::Value::Integer(0)
        );
    }

    #[test]
    fn query_value_int32() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Int32(42)),
            rusqlite::types::Value::Integer(42)
        );
    }

    #[test]
    fn query_value_int64() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Int64(9_999_999_999)),
            rusqlite::types::Value::Integer(9_999_999_999)
        );
    }

    #[test]
    fn query_value_float() {
        if let rusqlite::types::Value::Real(v) = query_value_to_sqlite(&QueryValue::Float(3.14)) {
            assert!((v - 3.14f32 as f64).abs() < 0.001);
        } else {
            panic!("Expected Real");
        }
    }

    #[test]
    fn query_value_double() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Double(2.718281828)),
            rusqlite::types::Value::Real(2.718281828)
        );
    }

    #[test]
    fn query_value_numeric() {
        let dec = rust_decimal::Decimal::new(12345, 2); // 123.45
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Numeric(dec)),
            rusqlite::types::Value::Text("123.45".into())
        );
    }

    #[test]
    fn query_value_text() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Text("hello".into())),
            rusqlite::types::Value::Text("hello".into())
        );
    }

    #[test]
    fn query_value_bytes() {
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Bytes(vec![0xDE, 0xAD])),
            rusqlite::types::Value::Blob(vec![0xDE, 0xAD])
        );
    }

    #[test]
    fn query_value_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Uuid(u)),
            rusqlite::types::Value::Text("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn query_value_datetime() {
        let dt = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let result = query_value_to_sqlite(&QueryValue::DateTime(dt));
        if let rusqlite::types::Value::Text(s) = result {
            assert!(s.starts_with("2024-01-15 10:30:00"));
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn query_value_date() {
        let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            query_value_to_sqlite(&QueryValue::Date(d)),
            rusqlite::types::Value::Text("2024-06-15".into())
        );
    }

    #[test]
    fn query_value_time() {
        let t = chrono::NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        let result = query_value_to_sqlite(&QueryValue::Time(t));
        if let rusqlite::types::Value::Text(s) = result {
            assert!(s.starts_with("14:30:45"));
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn query_value_json() {
        let j = serde_json::json!({"key": "value"});
        let result = query_value_to_sqlite(&QueryValue::Json(j));
        if let rusqlite::types::Value::Text(s) = result {
            assert!(s.contains("key"));
            assert!(s.contains("value"));
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn query_value_array_becomes_null() {
        let arr = QueryValue::Array(vec![QueryValue::Int32(1), QueryValue::Int32(2)]);
        assert_eq!(query_value_to_sqlite(&arr), rusqlite::types::Value::Null);
    }

    // --- sqlite_value_to_result comprehensive ---

    #[test]
    fn result_null() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Null, ColumnType::Text),
            ResultValue::Null
        );
    }

    #[test]
    fn result_integer_as_int32() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(42), ColumnType::Int32),
            ResultValue::Int32(42)
        );
    }

    #[test]
    fn result_integer_as_int64() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(9_999_999_999), ColumnType::Int64),
            ResultValue::Int64(9_999_999_999)
        );
    }

    #[test]
    fn result_integer_as_double() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(5), ColumnType::Double),
            ResultValue::Double(5.0)
        );
    }

    #[test]
    fn result_integer_fallback() {
        // Unknown column type falls through to Int64
        assert_eq!(
            sqlite_value_to_result(ValueRef::Integer(7), ColumnType::Text),
            ResultValue::Int64(7)
        );
    }

    #[test]
    fn result_real_as_float() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Real(1.5), ColumnType::Float),
            ResultValue::Float(1.5)
        );
    }

    #[test]
    fn result_real_as_double() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Real(3.14), ColumnType::Double),
            ResultValue::Double(3.14)
        );
    }

    #[test]
    fn result_real_as_int32() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Real(10.0), ColumnType::Int32),
            ResultValue::Int32(10)
        );
    }

    #[test]
    fn result_real_fallback() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Real(2.5), ColumnType::Text),
            ResultValue::Double(2.5)
        );
    }

    #[test]
    fn result_text_as_datetime() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"2024-01-15 10:30:00"), ColumnType::DateTime),
            ResultValue::DateTime("2024-01-15 10:30:00".into())
        );
    }

    #[test]
    fn result_text_as_date() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"2024-01-15"), ColumnType::Date),
            ResultValue::Date("2024-01-15".into())
        );
    }

    #[test]
    fn result_text_as_time() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"14:30:00"), ColumnType::Time),
            ResultValue::Time("14:30:00".into())
        );
    }

    #[test]
    fn result_text_as_json() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"{\"a\":1}"), ColumnType::Json),
            ResultValue::Json("{\"a\":1}".into())
        );
    }

    #[test]
    fn result_text_as_uuid() {
        assert_eq!(
            sqlite_value_to_result(
                ValueRef::Text(b"550e8400-e29b-41d4-a716-446655440000"),
                ColumnType::Uuid
            ),
            ResultValue::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
    }

    #[test]
    fn result_text_as_enum() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"ACTIVE"), ColumnType::Enum),
            ResultValue::Enum("ACTIVE".into())
        );
    }

    #[test]
    fn result_text_as_numeric() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"123.45"), ColumnType::Numeric),
            ResultValue::Numeric("123.45".into())
        );
    }

    #[test]
    fn result_text_fallback() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Text(b"hello"), ColumnType::Text),
            ResultValue::Text("hello".into())
        );
    }

    #[test]
    fn result_blob() {
        assert_eq!(
            sqlite_value_to_result(ValueRef::Blob(&[0xDE, 0xAD]), ColumnType::Bytes),
            ResultValue::Bytes(vec![0xDE, 0xAD])
        );
    }

    #[test]
    fn result_blob_ignores_column_type() {
        // Blob always returns Bytes regardless of column type
        assert_eq!(
            sqlite_value_to_result(ValueRef::Blob(&[1, 2, 3]), ColumnType::Text),
            ResultValue::Bytes(vec![1, 2, 3])
        );
    }
}
