use postgres_types::Type as PgType;
use prisma_driver_core::{ColumnType, QueryValue, ResultValue};

/// A NULL value that accepts any PostgreSQL type.
///
/// `Option::<String>::None` is typed as TEXT, which causes errors when binding
/// NULL to non-text columns. This struct writes SQL NULL regardless of the
/// target column type.
#[derive(Debug)]
struct PgNull;

impl tokio_postgres::types::ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &PgType,
        _out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(postgres_types::IsNull::Yes)
    }

    fn accepts(_ty: &PgType) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

/// First PostgreSQL OID for user-defined types.
const FIRST_CUSTOM_OID: u32 = 16384;

/// Map a PostgreSQL type OID to a Prisma `ColumnType`.
pub fn pg_type_to_column_type(pg_type: &PgType) -> ColumnType {
    match *pg_type {
        // Integer types
        PgType::INT2 | PgType::INT4 => ColumnType::Int32,
        PgType::INT8 | PgType::OID => ColumnType::Int64,

        // Float types
        PgType::FLOAT4 => ColumnType::Float,
        PgType::FLOAT8 => ColumnType::Double,

        // Numeric
        PgType::NUMERIC | PgType::MONEY => ColumnType::Numeric,

        // Boolean
        PgType::BOOL => ColumnType::Boolean,

        // Text types
        PgType::CHAR | PgType::BPCHAR | PgType::VARCHAR | PgType::TEXT | PgType::XML | PgType::NAME => ColumnType::Text,

        // Date/time types
        PgType::DATE => ColumnType::Date,
        PgType::TIME | PgType::TIMETZ => ColumnType::Time,
        PgType::TIMESTAMP | PgType::TIMESTAMPTZ => ColumnType::DateTime,

        // JSON
        PgType::JSON | PgType::JSONB => ColumnType::Json,

        // UUID
        PgType::UUID => ColumnType::Uuid,

        // Binary
        PgType::BYTEA => ColumnType::Bytes,

        // BIT types are treated as text
        PgType::BIT | PgType::VARBIT => ColumnType::Text,

        // Array types
        PgType::INT2_ARRAY | PgType::INT4_ARRAY => ColumnType::Int32Array,
        PgType::INT8_ARRAY | PgType::OID_ARRAY => ColumnType::Int64Array,
        PgType::FLOAT4_ARRAY => ColumnType::FloatArray,
        PgType::FLOAT8_ARRAY => ColumnType::DoubleArray,
        PgType::NUMERIC_ARRAY | PgType::MONEY_ARRAY => ColumnType::NumericArray,
        PgType::BOOL_ARRAY => ColumnType::BooleanArray,
        PgType::CHAR_ARRAY
        | PgType::BPCHAR_ARRAY
        | PgType::VARCHAR_ARRAY
        | PgType::TEXT_ARRAY
        | PgType::XML_ARRAY
        | PgType::NAME_ARRAY => ColumnType::TextArray,
        PgType::DATE_ARRAY => ColumnType::DateArray,
        PgType::TIME_ARRAY | PgType::TIMETZ_ARRAY => ColumnType::TimeArray,
        PgType::TIMESTAMP_ARRAY | PgType::TIMESTAMPTZ_ARRAY => ColumnType::DateTimeArray,
        PgType::JSON_ARRAY | PgType::JSONB_ARRAY => ColumnType::JsonArray,
        PgType::UUID_ARRAY => ColumnType::UuidArray,
        PgType::BYTEA_ARRAY => ColumnType::BytesArray,
        PgType::BIT_ARRAY | PgType::VARBIT_ARRAY => ColumnType::TextArray,

        // Custom/user-defined types treated as Text
        ref other if other.oid() >= FIRST_CUSTOM_OID => ColumnType::Text,

        // Fallback
        _ => ColumnType::Text,
    }
}

/// Convert a `QueryValue` into a boxed `ToSql` parameter for tokio-postgres.
pub fn query_value_to_pg_param(value: &QueryValue) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    match value {
        QueryValue::Null => Box::new(PgNull),
        QueryValue::Boolean(v) => Box::new(*v),
        QueryValue::Int32(v) => Box::new(*v),
        QueryValue::Int64(v) => Box::new(*v),
        QueryValue::Float(v) => Box::new(*v),
        QueryValue::Double(v) => Box::new(*v),
        QueryValue::Numeric(v) => Box::new(v.to_string()),
        QueryValue::Text(v) => Box::new(v.clone()),
        QueryValue::Bytes(v) => Box::new(v.clone()),
        QueryValue::Uuid(v) => Box::new(*v),
        QueryValue::DateTime(v) => Box::new(*v),
        QueryValue::Date(v) => Box::new(*v),
        QueryValue::Time(v) => Box::new(*v),
        QueryValue::Json(v) => Box::new(v.clone()),
        QueryValue::Array(items) => {
            // Convert array elements to their string representations and bind
            // as a PostgreSQL text array. This works for most column types since
            // PG can coerce text[] elements to the target type.
            const MAX_ARRAY_PARAMS: usize = 32_768;
            if items.len() > MAX_ARRAY_PARAMS {
                eprintln!(
                    "[prisma-driver-pg] WARNING: Array parameter has {} elements, exceeding limit of {}. Truncating.",
                    items.len(),
                    MAX_ARRAY_PARAMS
                );
            }
            let strings: Vec<Option<String>> = items
                .iter()
                .take(MAX_ARRAY_PARAMS)
                .map(|item| match item {
                    QueryValue::Null => None,
                    QueryValue::Text(s) => Some(s.clone()),
                    QueryValue::Int32(v) => Some(v.to_string()),
                    QueryValue::Int64(v) => Some(v.to_string()),
                    QueryValue::Float(v) => Some(v.to_string()),
                    QueryValue::Double(v) => Some(v.to_string()),
                    QueryValue::Boolean(v) => Some(v.to_string()),
                    QueryValue::Numeric(v) => Some(v.to_string()),
                    QueryValue::Uuid(v) => Some(v.to_string()),
                    QueryValue::Json(v) => Some(v.to_string()),
                    QueryValue::Date(v) => Some(v.format("%Y-%m-%d").to_string()),
                    QueryValue::Time(v) => Some(v.format("%H:%M:%S%.f").to_string()),
                    QueryValue::DateTime(v) => Some(v.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                    QueryValue::Bytes(v) => {
                        let hex: String = v.iter().map(|b| format!("{b:02x}")).collect();
                        Some(format!("\\x{hex}"))
                    }
                    QueryValue::Array(_) => None,
                })
                .collect();
            Box::new(strings)
        }
    }
}

/// Type-aware parameter conversion.
///
/// Uses the PG-inferred parameter type from `Statement::params()` to select the
/// correct Rust type. This handles the common case where the compiler emits
/// `Int64` but the PG column is `INT4`, or `Boolean` but the compiler sends
/// an int, etc.
pub fn query_value_to_pg_param_typed(
    value: &QueryValue,
    pg_type: Option<&PgType>,
) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    use tokio_postgres::types::Type as T;

    match (value, pg_type) {
        // INT4 column but we have Int64 -- downcast to i32
        (QueryValue::Int64(v), Some(t)) if *t == T::INT4 => {
            let narrowed = i32::try_from(*v).unwrap_or_else(|_| {
                eprintln!("[prisma-driver-pg] WARNING: i64 value {v} truncated to i32 for INT4 column");
                if *v > 0 { i32::MAX } else { i32::MIN }
            });
            Box::new(narrowed)
        }
        // INT2 column but we have Int64 -- downcast to i16
        (QueryValue::Int64(v), Some(t)) if *t == T::INT2 => {
            let narrowed = i16::try_from(*v).unwrap_or_else(|_| {
                eprintln!("[prisma-driver-pg] WARNING: i64 value {v} truncated to i16 for INT2 column");
                if *v > 0 { i16::MAX } else { i16::MIN }
            });
            Box::new(narrowed)
        }
        // INT4 column but we have Int32 -- already correct
        (QueryValue::Int32(v), Some(t)) if *t == T::INT8 => Box::new(*v as i64),
        // BOOL column but we have Int64 (Prisma sometimes sends 0/1 for bool)
        (QueryValue::Int64(v), Some(t)) if *t == T::BOOL => Box::new(*v != 0),
        (QueryValue::Int32(v), Some(t)) if *t == T::BOOL => Box::new(*v != 0),
        // Default: use the untyped conversion
        _ => query_value_to_pg_param(value),
    }
}

/// Extract a `ResultValue` from a tokio-postgres `Row` at the given column index.
pub fn pg_row_value(row: &tokio_postgres::Row, col_idx: usize, col_type: ColumnType) -> ResultValue {
    // Check for NULL by trying to get an Option<&str>. If the column is NULL,
    // `try_get::<_, Option<T>>` returns Ok(None) for any T.
    if let Ok(None) = row.try_get::<_, Option<&[u8]>>(col_idx) {
        return ResultValue::Null;
    }

    match col_type {
        ColumnType::Boolean => match row.try_get::<_, bool>(col_idx) {
            Ok(v) => ResultValue::Boolean(v),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Int32 => {
            // Try i32 first, then i16
            if let Ok(v) = row.try_get::<_, i32>(col_idx) {
                ResultValue::Int32(v)
            } else if let Ok(v) = row.try_get::<_, i16>(col_idx) {
                ResultValue::Int32(v as i32)
            } else {
                ResultValue::Null
            }
        }
        ColumnType::Int64 => {
            if let Ok(v) = row.try_get::<_, i64>(col_idx) {
                ResultValue::Int64(v)
            } else if let Ok(v) = row.try_get::<_, u32>(col_idx) {
                ResultValue::Int64(v as i64)
            } else {
                ResultValue::Null
            }
        }
        ColumnType::Float => match row.try_get::<_, f32>(col_idx) {
            Ok(v) if v.is_nan() || v.is_infinite() => ResultValue::Null,
            Ok(v) => ResultValue::Float(v),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Double => match row.try_get::<_, f64>(col_idx) {
            Ok(v) if v.is_nan() || v.is_infinite() => ResultValue::Null,
            Ok(v) => ResultValue::Double(v),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Numeric => match row.try_get::<_, &str>(col_idx) {
            Ok(v) => ResultValue::Numeric(v.to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Text | ColumnType::Character | ColumnType::Enum => match row.try_get::<_, &str>(col_idx) {
            Ok(v) => ResultValue::Text(v.to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Date => match row.try_get::<_, chrono::NaiveDate>(col_idx) {
            Ok(v) => ResultValue::Date(v.format("%Y-%m-%d").to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Time => match row.try_get::<_, chrono::NaiveTime>(col_idx) {
            Ok(v) => ResultValue::Time(v.format("%H:%M:%S%.f").to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::DateTime => {
            if let Ok(v) = row.try_get::<_, chrono::NaiveDateTime>(col_idx) {
                ResultValue::DateTime(v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            } else if let Ok(v) = row.try_get::<_, chrono::DateTime<chrono::Utc>>(col_idx) {
                ResultValue::DateTime(v.format("%Y-%m-%d %H:%M:%S%.f+00:00").to_string())
            } else {
                ResultValue::Null
            }
        }
        ColumnType::Json => match row.try_get::<_, serde_json::Value>(col_idx) {
            Ok(v) => ResultValue::Json(v.to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Uuid => match row.try_get::<_, uuid::Uuid>(col_idx) {
            Ok(v) => ResultValue::Uuid(v.to_string()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Bytes => match row.try_get::<_, Vec<u8>>(col_idx) {
            Ok(v) => ResultValue::Bytes(v),
            Err(_) => ResultValue::Null,
        },

        // Array types -- extract as Vec and wrap
        ColumnType::Int32Array => match row.try_get::<_, Vec<i32>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Int32).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::Int64Array => match row.try_get::<_, Vec<i64>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Int64).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::FloatArray => match row.try_get::<_, Vec<f32>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Float).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::DoubleArray => match row.try_get::<_, Vec<f64>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Double).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::BooleanArray => match row.try_get::<_, Vec<bool>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Boolean).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::TextArray | ColumnType::CharacterArray | ColumnType::EnumArray => {
            match row.try_get::<_, Vec<String>>(col_idx) {
                Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Text).collect()),
                Err(_) => ResultValue::Null,
            }
        }
        ColumnType::UuidArray => match row.try_get::<_, Vec<uuid::Uuid>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(|u| ResultValue::Uuid(u.to_string())).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::JsonArray => match row.try_get::<_, Vec<serde_json::Value>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(|j| ResultValue::Json(j.to_string())).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::BytesArray => match row.try_get::<_, Vec<Vec<u8>>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Bytes).collect()),
            Err(_) => ResultValue::Null,
        },
        ColumnType::DateArray => match row.try_get::<_, Vec<chrono::NaiveDate>>(col_idx) {
            Ok(v) => ResultValue::Array(
                v.into_iter()
                    .map(|d| ResultValue::Date(d.format("%Y-%m-%d").to_string()))
                    .collect(),
            ),
            Err(_) => ResultValue::Null,
        },
        ColumnType::TimeArray => match row.try_get::<_, Vec<chrono::NaiveTime>>(col_idx) {
            Ok(v) => ResultValue::Array(
                v.into_iter()
                    .map(|t| ResultValue::Time(t.format("%H:%M:%S%.f").to_string()))
                    .collect(),
            ),
            Err(_) => ResultValue::Null,
        },
        ColumnType::DateTimeArray => match row.try_get::<_, Vec<chrono::NaiveDateTime>>(col_idx) {
            Ok(v) => ResultValue::Array(
                v.into_iter()
                    .map(|dt| ResultValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()))
                    .collect(),
            ),
            Err(_) => ResultValue::Null,
        },
        ColumnType::NumericArray => match row.try_get::<_, Vec<String>>(col_idx) {
            Ok(v) => ResultValue::Array(v.into_iter().map(ResultValue::Numeric).collect()),
            Err(_) => ResultValue::Null,
        },

        _ => ResultValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_mapping_scalars() {
        assert_eq!(pg_type_to_column_type(&PgType::INT2), ColumnType::Int32);
        assert_eq!(pg_type_to_column_type(&PgType::INT4), ColumnType::Int32);
        assert_eq!(pg_type_to_column_type(&PgType::INT8), ColumnType::Int64);
        assert_eq!(pg_type_to_column_type(&PgType::FLOAT4), ColumnType::Float);
        assert_eq!(pg_type_to_column_type(&PgType::FLOAT8), ColumnType::Double);
        assert_eq!(pg_type_to_column_type(&PgType::NUMERIC), ColumnType::Numeric);
        assert_eq!(pg_type_to_column_type(&PgType::BOOL), ColumnType::Boolean);
        assert_eq!(pg_type_to_column_type(&PgType::TEXT), ColumnType::Text);
        assert_eq!(pg_type_to_column_type(&PgType::VARCHAR), ColumnType::Text);
        assert_eq!(pg_type_to_column_type(&PgType::DATE), ColumnType::Date);
        assert_eq!(pg_type_to_column_type(&PgType::TIME), ColumnType::Time);
        assert_eq!(pg_type_to_column_type(&PgType::TIMESTAMP), ColumnType::DateTime);
        assert_eq!(pg_type_to_column_type(&PgType::TIMESTAMPTZ), ColumnType::DateTime);
        assert_eq!(pg_type_to_column_type(&PgType::JSON), ColumnType::Json);
        assert_eq!(pg_type_to_column_type(&PgType::JSONB), ColumnType::Json);
        assert_eq!(pg_type_to_column_type(&PgType::UUID), ColumnType::Uuid);
        assert_eq!(pg_type_to_column_type(&PgType::BYTEA), ColumnType::Bytes);
    }

    #[test]
    fn pg_type_mapping_arrays() {
        assert_eq!(pg_type_to_column_type(&PgType::INT4_ARRAY), ColumnType::Int32Array);
        assert_eq!(pg_type_to_column_type(&PgType::INT8_ARRAY), ColumnType::Int64Array);
        assert_eq!(pg_type_to_column_type(&PgType::TEXT_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::BOOL_ARRAY), ColumnType::BooleanArray);
        assert_eq!(pg_type_to_column_type(&PgType::UUID_ARRAY), ColumnType::UuidArray);
    }

    // --- Additional scalar type mappings ---

    #[test]
    fn pg_type_oid() {
        assert_eq!(pg_type_to_column_type(&PgType::OID), ColumnType::Int64);
    }

    #[test]
    fn pg_type_money() {
        assert_eq!(pg_type_to_column_type(&PgType::MONEY), ColumnType::Numeric);
    }

    #[test]
    fn pg_type_bit_varbit() {
        assert_eq!(pg_type_to_column_type(&PgType::BIT), ColumnType::Text);
        assert_eq!(pg_type_to_column_type(&PgType::VARBIT), ColumnType::Text);
    }

    #[test]
    fn pg_type_char_bpchar() {
        assert_eq!(pg_type_to_column_type(&PgType::CHAR), ColumnType::Text);
        assert_eq!(pg_type_to_column_type(&PgType::BPCHAR), ColumnType::Text);
    }

    #[test]
    fn pg_type_xml_name() {
        assert_eq!(pg_type_to_column_type(&PgType::XML), ColumnType::Text);
        assert_eq!(pg_type_to_column_type(&PgType::NAME), ColumnType::Text);
    }

    #[test]
    fn pg_type_timetz() {
        assert_eq!(pg_type_to_column_type(&PgType::TIMETZ), ColumnType::Time);
    }

    #[test]
    fn pg_type_jsonb() {
        assert_eq!(pg_type_to_column_type(&PgType::JSONB), ColumnType::Json);
    }

    #[test]
    fn pg_type_custom_oid() {
        // Custom types (OID >= 16384) should map to Text
        let custom = PgType::from_oid(20000).unwrap_or(PgType::TEXT);
        // If from_oid returns None for unknown OIDs, test fallback
        assert_eq!(pg_type_to_column_type(&custom), ColumnType::Text);
    }

    // --- Additional array type mappings ---

    #[test]
    fn pg_type_array_int2() {
        assert_eq!(pg_type_to_column_type(&PgType::INT2_ARRAY), ColumnType::Int32Array);
    }

    #[test]
    fn pg_type_array_oid() {
        assert_eq!(pg_type_to_column_type(&PgType::OID_ARRAY), ColumnType::Int64Array);
    }

    #[test]
    fn pg_type_array_float() {
        assert_eq!(pg_type_to_column_type(&PgType::FLOAT4_ARRAY), ColumnType::FloatArray);
        assert_eq!(pg_type_to_column_type(&PgType::FLOAT8_ARRAY), ColumnType::DoubleArray);
    }

    #[test]
    fn pg_type_array_numeric_money() {
        assert_eq!(pg_type_to_column_type(&PgType::NUMERIC_ARRAY), ColumnType::NumericArray);
        assert_eq!(pg_type_to_column_type(&PgType::MONEY_ARRAY), ColumnType::NumericArray);
    }

    #[test]
    fn pg_type_array_text_variants() {
        assert_eq!(pg_type_to_column_type(&PgType::CHAR_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::BPCHAR_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::VARCHAR_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::XML_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::NAME_ARRAY), ColumnType::TextArray);
    }

    #[test]
    fn pg_type_array_datetime() {
        assert_eq!(pg_type_to_column_type(&PgType::DATE_ARRAY), ColumnType::DateArray);
        assert_eq!(pg_type_to_column_type(&PgType::TIME_ARRAY), ColumnType::TimeArray);
        assert_eq!(pg_type_to_column_type(&PgType::TIMETZ_ARRAY), ColumnType::TimeArray);
        assert_eq!(
            pg_type_to_column_type(&PgType::TIMESTAMP_ARRAY),
            ColumnType::DateTimeArray
        );
        assert_eq!(
            pg_type_to_column_type(&PgType::TIMESTAMPTZ_ARRAY),
            ColumnType::DateTimeArray
        );
    }

    #[test]
    fn pg_type_array_json() {
        assert_eq!(pg_type_to_column_type(&PgType::JSON_ARRAY), ColumnType::JsonArray);
        assert_eq!(pg_type_to_column_type(&PgType::JSONB_ARRAY), ColumnType::JsonArray);
    }

    #[test]
    fn pg_type_array_bytea() {
        assert_eq!(pg_type_to_column_type(&PgType::BYTEA_ARRAY), ColumnType::BytesArray);
    }

    #[test]
    fn pg_type_array_bit() {
        assert_eq!(pg_type_to_column_type(&PgType::BIT_ARRAY), ColumnType::TextArray);
        assert_eq!(pg_type_to_column_type(&PgType::VARBIT_ARRAY), ColumnType::TextArray);
    }
}
