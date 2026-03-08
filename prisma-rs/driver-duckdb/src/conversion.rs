use prisma_driver_core::{ColumnType, QueryValue, ResultValue};

/// Map a DuckDB column type name to a Prisma `ColumnType`.
pub fn decl_type_to_column_type(type_name: &str) -> ColumnType {
    let upper = type_name.to_uppercase();

    if upper == "BOOLEAN" || upper == "BOOL" {
        return ColumnType::Boolean;
    }
    if upper == "TINYINT" || upper == "SMALLINT" || upper == "INTEGER" || upper == "INT" {
        return ColumnType::Int32;
    }
    if upper == "BIGINT" || upper == "HUGEINT" || upper == "UBIGINT" || upper == "UINTEGER" {
        return ColumnType::Int64;
    }
    if upper == "FLOAT" || upper == "REAL" {
        return ColumnType::Float;
    }
    if upper == "DOUBLE" || upper == "DOUBLE PRECISION" {
        return ColumnType::Double;
    }
    if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
        return ColumnType::Numeric;
    }
    if upper.starts_with("VARCHAR") || upper == "TEXT" || upper.starts_with("CHAR") || upper == "STRING" {
        return ColumnType::Text;
    }
    if upper == "BLOB" || upper == "BYTEA" || upper == "BINARY" || upper == "VARBINARY" {
        return ColumnType::Bytes;
    }
    if upper == "DATE" {
        return ColumnType::Date;
    }
    if upper == "TIME" || upper.starts_with("TIME ") {
        return ColumnType::Time;
    }
    if upper == "TIMESTAMP" || upper.starts_with("TIMESTAMP") || upper == "DATETIME" || upper.starts_with("TIMESTAMPTZ")
    {
        return ColumnType::DateTime;
    }
    if upper == "JSON" || upper == "JSONB" {
        return ColumnType::Json;
    }
    if upper == "UUID" {
        return ColumnType::Uuid;
    }

    // Fallback
    ColumnType::Text
}

/// Convert a `QueryValue` to a DuckDB parameter value.
pub fn query_value_to_duckdb(value: &QueryValue) -> duckdb::types::Value {
    match value {
        QueryValue::Null => duckdb::types::Value::Null,
        QueryValue::Boolean(v) => duckdb::types::Value::Boolean(*v),
        QueryValue::Int32(v) => duckdb::types::Value::Int(*v),
        QueryValue::Int64(v) => duckdb::types::Value::BigInt(*v),
        QueryValue::Float(v) => duckdb::types::Value::Float(*v),
        QueryValue::Double(v) => duckdb::types::Value::Double(*v),
        QueryValue::Numeric(v) => duckdb::types::Value::Text(v.to_string()),
        QueryValue::Text(v) => duckdb::types::Value::Text(v.clone()),
        QueryValue::Bytes(v) => duckdb::types::Value::Blob(v.clone()),
        QueryValue::Uuid(v) => duckdb::types::Value::Text(v.to_string()),
        QueryValue::DateTime(v) => duckdb::types::Value::Text(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        QueryValue::Date(v) => duckdb::types::Value::Text(v.format("%Y-%m-%d").to_string()),
        QueryValue::Time(v) => duckdb::types::Value::Text(v.format("%H:%M:%S%.f").to_string()),
        QueryValue::Json(v) => duckdb::types::Value::Text(v.to_string()),
        QueryValue::Array(_) => duckdb::types::Value::Null,
    }
}

/// Convert an owned DuckDB `Value` to a Prisma `ResultValue`.
/// Used when we must collect rows before reading column metadata (DuckDB
/// requires execution before `column_type()` is available).
pub fn duckdb_owned_value_to_result(value: duckdb::types::Value, col_type: ColumnType) -> ResultValue {
    match value {
        duckdb::types::Value::Null => ResultValue::Null,
        duckdb::types::Value::Boolean(v) => ResultValue::Boolean(v),
        duckdb::types::Value::TinyInt(v) => match col_type {
            ColumnType::Boolean => ResultValue::Boolean(v != 0),
            _ => ResultValue::Int32(v as i32),
        },
        duckdb::types::Value::SmallInt(v) => ResultValue::Int32(v as i32),
        duckdb::types::Value::Int(v) => match col_type {
            ColumnType::Boolean => ResultValue::Boolean(v != 0),
            ColumnType::Int64 => ResultValue::Int64(v as i64),
            _ => ResultValue::Int32(v),
        },
        duckdb::types::Value::BigInt(v) => match col_type {
            ColumnType::Int32 => ResultValue::Int32(v as i32),
            ColumnType::Boolean => ResultValue::Boolean(v != 0),
            _ => ResultValue::Int64(v),
        },
        duckdb::types::Value::HugeInt(v) => ResultValue::Int64(v as i64),
        duckdb::types::Value::UTinyInt(v) => ResultValue::Int32(v as i32),
        duckdb::types::Value::USmallInt(v) => ResultValue::Int32(v as i32),
        duckdb::types::Value::UInt(v) => ResultValue::Int64(v as i64),
        duckdb::types::Value::UBigInt(v) => ResultValue::Int64(v as i64),
        duckdb::types::Value::Float(v) => ResultValue::Float(v),
        duckdb::types::Value::Double(v) => match col_type {
            ColumnType::Float => ResultValue::Float(v as f32),
            _ => ResultValue::Double(v),
        },
        duckdb::types::Value::Text(s) => match col_type {
            ColumnType::DateTime => ResultValue::DateTime(s),
            ColumnType::Date => ResultValue::Date(s),
            ColumnType::Time => ResultValue::Time(s),
            ColumnType::Json => ResultValue::Json(s),
            ColumnType::Uuid => ResultValue::Uuid(s),
            ColumnType::Enum => ResultValue::Enum(s),
            ColumnType::Numeric => ResultValue::Numeric(s),
            _ => ResultValue::Text(s),
        },
        duckdb::types::Value::Blob(bytes) => ResultValue::Bytes(bytes),
        duckdb::types::Value::Timestamp(unit, val) => {
            let s = timestamp_to_string(unit, val);
            ResultValue::DateTime(s)
        }
        duckdb::types::Value::Date32(days) => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(days as i64);
            ResultValue::Date(date.format("%Y-%m-%d").to_string())
        }
        duckdb::types::Value::Time64(unit, val) => {
            let micros = match unit {
                duckdb::types::TimeUnit::Microsecond => val,
                duckdb::types::TimeUnit::Nanosecond => val / 1000,
                duckdb::types::TimeUnit::Millisecond => val * 1000,
                duckdb::types::TimeUnit::Second => val * 1_000_000,
            };
            let secs = (micros / 1_000_000) as u32;
            let nanos = ((micros % 1_000_000) * 1000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap_or_default();
            ResultValue::Time(time.format("%H:%M:%S%.f").to_string())
        }
        other => ResultValue::Text(format!("{other:?}")),
    }
}

fn timestamp_to_string(unit: duckdb::types::TimeUnit, val: i64) -> String {
    let micros = match unit {
        duckdb::types::TimeUnit::Microsecond => val,
        duckdb::types::TimeUnit::Nanosecond => val / 1000,
        duckdb::types::TimeUnit::Millisecond => val * 1000,
        duckdb::types::TimeUnit::Second => val * 1_000_000,
    };
    let secs = micros / 1_000_000;
    let remaining_micros = (micros % 1_000_000).unsigned_abs() as u32;
    let dt = chrono::DateTime::from_timestamp(secs, remaining_micros * 1000)
        .unwrap_or_default()
        .naive_utc();
    dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decl_type_integer() {
        assert_eq!(decl_type_to_column_type("INTEGER"), ColumnType::Int32);
        assert_eq!(decl_type_to_column_type("INT"), ColumnType::Int32);
        assert_eq!(decl_type_to_column_type("SMALLINT"), ColumnType::Int32);
    }

    #[test]
    fn decl_type_bigint() {
        assert_eq!(decl_type_to_column_type("BIGINT"), ColumnType::Int64);
        assert_eq!(decl_type_to_column_type("HUGEINT"), ColumnType::Int64);
    }

    #[test]
    fn decl_type_text() {
        assert_eq!(decl_type_to_column_type("VARCHAR"), ColumnType::Text);
        assert_eq!(decl_type_to_column_type("VARCHAR(255)"), ColumnType::Text);
        assert_eq!(decl_type_to_column_type("TEXT"), ColumnType::Text);
    }

    #[test]
    fn decl_type_boolean() {
        assert_eq!(decl_type_to_column_type("BOOLEAN"), ColumnType::Boolean);
        assert_eq!(decl_type_to_column_type("BOOL"), ColumnType::Boolean);
    }

    #[test]
    fn decl_type_float_double() {
        assert_eq!(decl_type_to_column_type("FLOAT"), ColumnType::Float);
        assert_eq!(decl_type_to_column_type("DOUBLE"), ColumnType::Double);
    }

    #[test]
    fn decl_type_timestamp() {
        assert_eq!(decl_type_to_column_type("TIMESTAMP"), ColumnType::DateTime);
        assert_eq!(decl_type_to_column_type("DATETIME"), ColumnType::DateTime);
    }

    #[test]
    fn decl_type_json() {
        assert_eq!(decl_type_to_column_type("JSON"), ColumnType::Json);
    }

    #[test]
    fn decl_type_uuid() {
        assert_eq!(decl_type_to_column_type("UUID"), ColumnType::Uuid);
    }

    #[test]
    fn decl_type_blob() {
        assert_eq!(decl_type_to_column_type("BLOB"), ColumnType::Bytes);
        assert_eq!(decl_type_to_column_type("BYTEA"), ColumnType::Bytes);
    }

    #[test]
    fn query_value_conversions() {
        assert!(matches!(
            query_value_to_duckdb(&QueryValue::Null),
            duckdb::types::Value::Null
        ));
        assert!(matches!(
            query_value_to_duckdb(&QueryValue::Boolean(true)),
            duckdb::types::Value::Boolean(true)
        ));
        assert!(matches!(
            query_value_to_duckdb(&QueryValue::Int32(42)),
            duckdb::types::Value::Int(42)
        ));
        assert!(matches!(
            query_value_to_duckdb(&QueryValue::Int64(999)),
            duckdb::types::Value::BigInt(999)
        ));
    }
}
