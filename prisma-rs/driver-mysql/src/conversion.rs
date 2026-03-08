use mysql_async::consts::ColumnFlags;
use mysql_async::consts::ColumnType as MysqlColumnType;
use mysql_async::{Row, Value as MysqlValue};
use prisma_driver_core::{ColumnType, QueryValue, ResultValue};

/// Map a mysql_async column type to a Prisma `ColumnType`.
pub fn mysql_type_to_column_type(col_type: MysqlColumnType, flags: ColumnFlags) -> ColumnType {
    match col_type {
        MysqlColumnType::MYSQL_TYPE_TINY
        | MysqlColumnType::MYSQL_TYPE_SHORT
        | MysqlColumnType::MYSQL_TYPE_INT24
        | MysqlColumnType::MYSQL_TYPE_YEAR => ColumnType::Int32,

        MysqlColumnType::MYSQL_TYPE_LONG => {
            if flags.contains(ColumnFlags::UNSIGNED_FLAG) {
                ColumnType::Int64
            } else {
                ColumnType::Int32
            }
        }

        MysqlColumnType::MYSQL_TYPE_LONGLONG => ColumnType::Int64,

        MysqlColumnType::MYSQL_TYPE_FLOAT => ColumnType::Float,
        MysqlColumnType::MYSQL_TYPE_DOUBLE => ColumnType::Double,

        MysqlColumnType::MYSQL_TYPE_TIMESTAMP | MysqlColumnType::MYSQL_TYPE_DATETIME => ColumnType::DateTime,

        MysqlColumnType::MYSQL_TYPE_DATE | MysqlColumnType::MYSQL_TYPE_NEWDATE => ColumnType::Date,
        MysqlColumnType::MYSQL_TYPE_TIME => ColumnType::Time,

        MysqlColumnType::MYSQL_TYPE_DECIMAL | MysqlColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::Numeric,

        MysqlColumnType::MYSQL_TYPE_JSON => ColumnType::Json,

        MysqlColumnType::MYSQL_TYPE_ENUM => ColumnType::Enum,

        MysqlColumnType::MYSQL_TYPE_BIT | MysqlColumnType::MYSQL_TYPE_GEOMETRY => ColumnType::Bytes,

        MysqlColumnType::MYSQL_TYPE_NULL => ColumnType::Int32,

        // Text-like types: VARCHAR, VAR_STRING, STRING, BLOB, etc.
        // In the TS adapter, these are further classified by collation (binary -> Bytes)
        // and dataTypeFormat (json -> Json). We default to Text here and can refine later
        // when we have access to column metadata.
        MysqlColumnType::MYSQL_TYPE_VARCHAR
        | MysqlColumnType::MYSQL_TYPE_VAR_STRING
        | MysqlColumnType::MYSQL_TYPE_STRING
        | MysqlColumnType::MYSQL_TYPE_BLOB
        | MysqlColumnType::MYSQL_TYPE_TINY_BLOB
        | MysqlColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | MysqlColumnType::MYSQL_TYPE_LONG_BLOB => {
            if flags.contains(ColumnFlags::BINARY_FLAG) {
                ColumnType::Bytes
            } else {
                ColumnType::Text
            }
        }

        _ => ColumnType::Text,
    }
}

/// Convert a `QueryValue` into a `mysql_async::Value`.
pub fn query_value_to_mysql(value: &QueryValue) -> MysqlValue {
    match value {
        QueryValue::Null => MysqlValue::NULL,
        QueryValue::Boolean(v) => MysqlValue::from(*v),
        QueryValue::Int32(v) => MysqlValue::from(*v),
        QueryValue::Int64(v) => MysqlValue::from(*v),
        QueryValue::Float(v) => MysqlValue::from(*v),
        QueryValue::Double(v) => MysqlValue::from(*v),
        QueryValue::Numeric(v) => MysqlValue::from(v.to_string()),
        QueryValue::Text(v) => MysqlValue::from(v.as_str()),
        QueryValue::Bytes(v) => MysqlValue::from(v.as_slice()),
        QueryValue::Uuid(v) => MysqlValue::from(v.to_string()),
        QueryValue::DateTime(v) => MysqlValue::from(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        QueryValue::Date(v) => MysqlValue::from(v.format("%Y-%m-%d").to_string()),
        QueryValue::Time(v) => MysqlValue::from(v.format("%H:%M:%S%.f").to_string()),
        QueryValue::Json(v) => MysqlValue::from(v.to_string()),
        QueryValue::Array(_) => MysqlValue::NULL,
    }
}

/// Extract a `ResultValue` from a mysql_async `Row` at the given column index.
pub fn mysql_row_value(row: &Row, col_idx: usize, col_type: ColumnType) -> ResultValue {
    let val: MysqlValue = match row.get(col_idx) {
        Some(v) => v,
        None => {
            eprintln!("[prisma-driver-mysql] WARNING: Column index {col_idx} out of bounds, returning NULL");
            return ResultValue::Null;
        }
    };
    if val == MysqlValue::NULL {
        return ResultValue::Null;
    }

    match col_type {
        ColumnType::Boolean => match &val {
            MysqlValue::Int(v) => ResultValue::Boolean(*v != 0),
            MysqlValue::UInt(v) => ResultValue::Boolean(*v != 0),
            _ => ResultValue::Null,
        },
        ColumnType::Int32 => match &val {
            MysqlValue::Int(v) => ResultValue::Int32(*v as i32),
            MysqlValue::UInt(v) => ResultValue::Int32(*v as i32),
            _ => ResultValue::Null,
        },
        ColumnType::Int64 => match &val {
            MysqlValue::Int(v) => ResultValue::Int64(*v),
            MysqlValue::UInt(v) => ResultValue::Int64(*v as i64),
            _ => ResultValue::Null,
        },
        ColumnType::Float => match &val {
            MysqlValue::Float(v) => ResultValue::Float(*v),
            MysqlValue::Double(v) => ResultValue::Float(*v as f32),
            _ => ResultValue::Null,
        },
        ColumnType::Double => match &val {
            MysqlValue::Double(v) => ResultValue::Double(*v),
            MysqlValue::Float(v) => ResultValue::Double(*v as f64),
            _ => ResultValue::Null,
        },
        ColumnType::Numeric => ResultValue::Numeric(mysql_value_to_string(&val)),
        ColumnType::Text | ColumnType::Character | ColumnType::Enum => ResultValue::Text(mysql_value_to_string(&val)),
        ColumnType::Date => ResultValue::Date(mysql_value_to_string(&val)),
        ColumnType::Time => ResultValue::Time(mysql_value_to_string(&val)),
        ColumnType::DateTime => {
            let s = mysql_value_to_string(&val);
            // MySQL TIMESTAMP/DATETIME -> ISO format with timezone
            if !s.contains('+') && !s.ends_with('Z') {
                ResultValue::DateTime(format!("{s}+00:00"))
            } else {
                ResultValue::DateTime(s)
            }
        }
        ColumnType::Json => ResultValue::Json(mysql_value_to_string(&val)),
        ColumnType::Uuid => ResultValue::Uuid(mysql_value_to_string(&val)),
        ColumnType::Bytes => match &val {
            MysqlValue::Bytes(v) => ResultValue::Bytes(v.clone()),
            _ => ResultValue::Null,
        },
        _ => ResultValue::Text(mysql_value_to_string(&val)),
    }
}

fn mysql_value_to_string(val: &MysqlValue) -> String {
    match val {
        MysqlValue::NULL => String::new(),
        MysqlValue::Bytes(v) => String::from_utf8_lossy(v).to_string(),
        MysqlValue::Int(v) => v.to_string(),
        MysqlValue::UInt(v) => v.to_string(),
        MysqlValue::Float(v) => v.to_string(),
        MysqlValue::Double(v) => v.to_string(),
        MysqlValue::Date(y, m, d, h, min, s, us) => {
            if *h == 0 && *min == 0 && *s == 0 && *us == 0 {
                format!("{y:04}-{m:02}-{d:02}")
            } else if *us > 0 {
                format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}.{us:06}")
            } else {
                format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}")
            }
        }
        MysqlValue::Time(neg, days, h, min, s, us) => {
            let sign = if *neg { "-" } else { "" };
            let total_hours = *days * 24 + (*h as u32);
            if *us > 0 {
                format!("{sign}{total_hours:02}:{min:02}:{s:02}.{us:06}")
            } else {
                format!("{sign}{total_hours:02}:{min:02}:{s:02}")
            }
        }
    }
}

/// Detect if a MySQL version supports relation joins.
/// MySQL >= 8.0.14 supports lateral joins; MariaDB does not.
pub fn supports_relation_joins(version: &str) -> bool {
    // MariaDB version strings contain "MariaDB"
    if version.contains("MariaDB") {
        return false;
    }
    // Parse MySQL version: "8.0.33" or similar
    let parts: Vec<u32> = version
        .split('.')
        .filter_map(|p| {
            p.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .ok()
        })
        .collect();
    match parts.as_slice() {
        [major, minor, patch, ..] => (*major, *minor, *patch) >= (8, 0, 14),
        [major, minor, ..] => (*major, *minor) >= (8, 1),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_integers() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_TINY, ColumnFlags::empty()),
            ColumnType::Int32
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_LONG, ColumnFlags::empty()),
            ColumnType::Int32
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_LONG, ColumnFlags::UNSIGNED_FLAG),
            ColumnType::Int64
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_LONGLONG, ColumnFlags::empty()),
            ColumnType::Int64
        );
    }

    #[test]
    fn type_mapping_text_vs_binary() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_BLOB, ColumnFlags::empty()),
            ColumnType::Text
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_BLOB, ColumnFlags::BINARY_FLAG),
            ColumnType::Bytes
        );
    }

    #[test]
    fn version_detection() {
        assert!(supports_relation_joins("8.0.33"));
        assert!(supports_relation_joins("8.0.14"));
        assert!(!supports_relation_joins("8.0.13"));
        assert!(!supports_relation_joins("5.7.44"));
        assert!(!supports_relation_joins("10.11.6-MariaDB"));
    }

    #[test]
    fn version_detection_edge_cases() {
        assert!(supports_relation_joins("8.1.0"));
        assert!(supports_relation_joins("9.0.0"));
        assert!(!supports_relation_joins("7.99.99"));
        assert!(!supports_relation_joins(""));
        assert!(!supports_relation_joins("not-a-version"));
        assert!(!supports_relation_joins("5.5.5-10.11.6-MariaDB"));
    }

    // --- mysql_type_to_column_type additional ---

    #[test]
    fn type_mapping_floats() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_FLOAT, ColumnFlags::empty()),
            ColumnType::Float
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_DOUBLE, ColumnFlags::empty()),
            ColumnType::Double
        );
    }

    #[test]
    fn type_mapping_datetime() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_TIMESTAMP, ColumnFlags::empty()),
            ColumnType::DateTime
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_DATETIME, ColumnFlags::empty()),
            ColumnType::DateTime
        );
    }

    #[test]
    fn type_mapping_date_time() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_DATE, ColumnFlags::empty()),
            ColumnType::Date
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_NEWDATE, ColumnFlags::empty()),
            ColumnType::Date
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_TIME, ColumnFlags::empty()),
            ColumnType::Time
        );
    }

    #[test]
    fn type_mapping_decimal() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_DECIMAL, ColumnFlags::empty()),
            ColumnType::Numeric
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_NEWDECIMAL, ColumnFlags::empty()),
            ColumnType::Numeric
        );
    }

    #[test]
    fn type_mapping_json() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_JSON, ColumnFlags::empty()),
            ColumnType::Json
        );
    }

    #[test]
    fn type_mapping_enum() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_ENUM, ColumnFlags::empty()),
            ColumnType::Enum
        );
    }

    #[test]
    fn type_mapping_bit_geometry() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_BIT, ColumnFlags::empty()),
            ColumnType::Bytes
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_GEOMETRY, ColumnFlags::empty()),
            ColumnType::Bytes
        );
    }

    #[test]
    fn type_mapping_null() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_NULL, ColumnFlags::empty()),
            ColumnType::Int32
        );
    }

    #[test]
    fn type_mapping_year() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_YEAR, ColumnFlags::empty()),
            ColumnType::Int32
        );
    }

    #[test]
    fn type_mapping_string_types() {
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_VARCHAR, ColumnFlags::empty()),
            ColumnType::Text
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_VAR_STRING, ColumnFlags::empty()),
            ColumnType::Text
        );
        assert_eq!(
            mysql_type_to_column_type(MysqlColumnType::MYSQL_TYPE_STRING, ColumnFlags::empty()),
            ColumnType::Text
        );
    }

    #[test]
    fn type_mapping_blob_sizes() {
        for t in [
            MysqlColumnType::MYSQL_TYPE_TINY_BLOB,
            MysqlColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            MysqlColumnType::MYSQL_TYPE_LONG_BLOB,
        ] {
            assert_eq!(mysql_type_to_column_type(t, ColumnFlags::empty()), ColumnType::Text);
            assert_eq!(
                mysql_type_to_column_type(t, ColumnFlags::BINARY_FLAG),
                ColumnType::Bytes
            );
        }
    }

    // --- query_value_to_mysql ---

    #[test]
    fn mysql_param_null() {
        assert_eq!(query_value_to_mysql(&QueryValue::Null), MysqlValue::NULL);
    }

    #[test]
    fn mysql_param_boolean() {
        assert_eq!(query_value_to_mysql(&QueryValue::Boolean(true)), MysqlValue::from(true));
        assert_eq!(
            query_value_to_mysql(&QueryValue::Boolean(false)),
            MysqlValue::from(false)
        );
    }

    #[test]
    fn mysql_param_int32() {
        assert_eq!(query_value_to_mysql(&QueryValue::Int32(42)), MysqlValue::from(42i32));
    }

    #[test]
    fn mysql_param_int64() {
        assert_eq!(
            query_value_to_mysql(&QueryValue::Int64(9_999_999_999)),
            MysqlValue::from(9_999_999_999i64)
        );
    }

    #[test]
    fn mysql_param_float() {
        assert_eq!(
            query_value_to_mysql(&QueryValue::Float(3.14)),
            MysqlValue::from(3.14f32)
        );
    }

    #[test]
    fn mysql_param_double() {
        assert_eq!(
            query_value_to_mysql(&QueryValue::Double(2.718)),
            MysqlValue::from(2.718f64)
        );
    }

    #[test]
    fn mysql_param_numeric() {
        let dec = rust_decimal::Decimal::new(12345, 2);
        assert_eq!(
            query_value_to_mysql(&QueryValue::Numeric(dec)),
            MysqlValue::from("123.45")
        );
    }

    #[test]
    fn mysql_param_text() {
        assert_eq!(
            query_value_to_mysql(&QueryValue::Text("hello".into())),
            MysqlValue::from("hello")
        );
    }

    #[test]
    fn mysql_param_bytes() {
        assert_eq!(
            query_value_to_mysql(&QueryValue::Bytes(vec![0xDE, 0xAD])),
            MysqlValue::from(vec![0xDE, 0xAD].as_slice())
        );
    }

    #[test]
    fn mysql_param_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            query_value_to_mysql(&QueryValue::Uuid(u)),
            MysqlValue::from("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn mysql_param_datetime() {
        let dt = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let result = query_value_to_mysql(&QueryValue::DateTime(dt));
        if let MysqlValue::Bytes(bytes) = result {
            let s = String::from_utf8(bytes).unwrap();
            assert!(s.starts_with("2024-01-15 10:30:00"));
        } else {
            panic!("Expected Bytes (string), got {:?}", result);
        }
    }

    #[test]
    fn mysql_param_date() {
        let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let result = query_value_to_mysql(&QueryValue::Date(d));
        if let MysqlValue::Bytes(bytes) = result {
            assert_eq!(String::from_utf8(bytes).unwrap(), "2024-06-15");
        } else {
            panic!("Expected Bytes (string), got {:?}", result);
        }
    }

    #[test]
    fn mysql_param_time() {
        let t = chrono::NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        let result = query_value_to_mysql(&QueryValue::Time(t));
        if let MysqlValue::Bytes(bytes) = result {
            let s = String::from_utf8(bytes).unwrap();
            assert!(s.starts_with("14:30:45"));
        } else {
            panic!("Expected Bytes (string), got {:?}", result);
        }
    }

    #[test]
    fn mysql_param_json() {
        let j = serde_json::json!({"key": "value"});
        let result = query_value_to_mysql(&QueryValue::Json(j));
        if let MysqlValue::Bytes(bytes) = result {
            let s = String::from_utf8(bytes).unwrap();
            assert!(s.contains("key"));
        } else {
            panic!("Expected Bytes (string), got {:?}", result);
        }
    }

    #[test]
    fn mysql_param_array_becomes_null() {
        let arr = QueryValue::Array(vec![QueryValue::Int32(1)]);
        assert_eq!(query_value_to_mysql(&arr), MysqlValue::NULL);
    }

    // --- mysql_value_to_string ---

    #[test]
    fn mysql_value_to_string_null() {
        assert_eq!(mysql_value_to_string(&MysqlValue::NULL), "");
    }

    #[test]
    fn mysql_value_to_string_int() {
        assert_eq!(mysql_value_to_string(&MysqlValue::Int(42)), "42");
        assert_eq!(mysql_value_to_string(&MysqlValue::Int(-1)), "-1");
    }

    #[test]
    fn mysql_value_to_string_uint() {
        assert_eq!(mysql_value_to_string(&MysqlValue::UInt(100)), "100");
    }

    #[test]
    fn mysql_value_to_string_float() {
        let s = mysql_value_to_string(&MysqlValue::Float(3.14));
        assert!(s.starts_with("3.14"));
    }

    #[test]
    fn mysql_value_to_string_double() {
        let s = mysql_value_to_string(&MysqlValue::Double(2.718));
        assert!(s.starts_with("2.718"));
    }

    #[test]
    fn mysql_value_to_string_bytes() {
        assert_eq!(mysql_value_to_string(&MysqlValue::Bytes(b"hello".to_vec())), "hello");
    }

    #[test]
    fn mysql_value_to_string_date_only() {
        let val = MysqlValue::Date(2024, 1, 15, 0, 0, 0, 0);
        assert_eq!(mysql_value_to_string(&val), "2024-01-15");
    }

    #[test]
    fn mysql_value_to_string_datetime() {
        let val = MysqlValue::Date(2024, 1, 15, 10, 30, 45, 0);
        assert_eq!(mysql_value_to_string(&val), "2024-01-15 10:30:45");
    }

    #[test]
    fn mysql_value_to_string_datetime_with_microseconds() {
        let val = MysqlValue::Date(2024, 1, 15, 10, 30, 45, 123456);
        assert_eq!(mysql_value_to_string(&val), "2024-01-15 10:30:45.123456");
    }

    #[test]
    fn mysql_value_to_string_time() {
        let val = MysqlValue::Time(false, 0, 14, 30, 45, 0);
        assert_eq!(mysql_value_to_string(&val), "14:30:45");
    }

    #[test]
    fn mysql_value_to_string_time_negative() {
        let val = MysqlValue::Time(true, 0, 2, 30, 0, 0);
        assert_eq!(mysql_value_to_string(&val), "-02:30:00");
    }

    #[test]
    fn mysql_value_to_string_time_with_days() {
        let val = MysqlValue::Time(false, 1, 2, 30, 0, 0);
        // 1 day * 24 + 2 = 26 hours
        assert_eq!(mysql_value_to_string(&val), "26:30:00");
    }

    #[test]
    fn mysql_value_to_string_time_with_microseconds() {
        let val = MysqlValue::Time(false, 0, 10, 20, 30, 500000);
        assert_eq!(mysql_value_to_string(&val), "10:20:30.500000");
    }
}
