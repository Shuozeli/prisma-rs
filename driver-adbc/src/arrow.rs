//! Arrow RecordBatch <-> Prisma type conversions.
//!
//! This module is the core value of the ADBC driver: it converts Arrow
//! `RecordBatch` results into Prisma `SqlResultSet` and converts Prisma
//! `QueryValue` parameters into Arrow `RecordBatch` for parameter binding.
//!
//! These conversions are reusable for any Arrow-native driver (ADBC, Flight SQL, etc.).

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use prisma_driver_core::{ColumnType, QueryValue, ResultValue, SqlResultSet};
use std::sync::Arc;

/// Convert an Arrow `DataType` to a Prisma `ColumnType`.
pub fn arrow_type_to_column_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => ColumnType::Int32,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ColumnType::Int64,
        DataType::Float16 | DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => ColumnType::Numeric,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ColumnType::Text,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            ColumnType::Bytes
        }
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::Time,
        DataType::Timestamp(_, _) => ColumnType::DateTime,
        DataType::Null => ColumnType::Text,
        _ => ColumnType::Text,
    }
}

/// Convert Arrow `RecordBatch`es into a Prisma `SqlResultSet`.
pub fn record_batches_to_result_set(batches: &[RecordBatch]) -> SqlResultSet {
    if batches.is_empty() {
        return SqlResultSet {
            column_names: vec![],
            column_types: vec![],
            rows: vec![],
            last_insert_id: None,
        };
    }

    let schema = batches[0].schema();
    let col_count = schema.fields().len();

    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let column_types: Vec<ColumnType> = schema
        .fields()
        .iter()
        .map(|f| arrow_type_to_column_type(f.data_type()))
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<ResultValue> = (0..col_count)
                .map(|col_idx| arrow_value_to_result(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }

    SqlResultSet {
        column_names,
        column_types,
        rows,
        last_insert_id: None,
    }
}

/// Extract a single value from an Arrow array at the given row index.
fn arrow_value_to_result(array: &dyn Array, row: usize) -> ResultValue {
    if array.is_null(row) {
        return ResultValue::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            ResultValue::Boolean(arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            ResultValue::Int32(arr.value(row) as i32)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            ResultValue::Int32(arr.value(row) as i32)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            ResultValue::Int32(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            ResultValue::Int64(arr.value(row))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            ResultValue::Int32(arr.value(row) as i32)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            ResultValue::Int32(arr.value(row) as i32)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            ResultValue::Int64(arr.value(row) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            ResultValue::Int64(arr.value(row) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            ResultValue::Float(arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            ResultValue::Double(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            ResultValue::Text(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            ResultValue::Text(arr.value(row).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            ResultValue::Bytes(arr.value(row).to_vec())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            ResultValue::Bytes(arr.value(row).to_vec())
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Date32Array>().unwrap();
            let days = arr.value(row);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(days as i64);
            ResultValue::Date(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Date64Array>().unwrap();
            let ms = arr.value(row);
            let secs = ms / 1000;
            let dt = chrono::DateTime::from_timestamp(secs, ((ms % 1000) * 1_000_000) as u32)
                .unwrap_or_default()
                .naive_utc();
            ResultValue::Date(dt.format("%Y-%m-%d").to_string())
        }
        DataType::Time32(TimeUnit::Second) => {
            let arr = array.as_any().downcast_ref::<arrow_array::Time32SecondArray>().unwrap();
            let secs = arr.value(row) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap_or_default();
            ResultValue::Time(time.format("%H:%M:%S").to_string())
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Time32MillisecondArray>()
                .unwrap();
            let ms = arr.value(row);
            let secs = (ms / 1000) as u32;
            let nanos = ((ms % 1000) * 1_000_000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap_or_default();
            ResultValue::Time(time.format("%H:%M:%S%.f").to_string())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .unwrap();
            let us = arr.value(row);
            let secs = (us / 1_000_000) as u32;
            let nanos = ((us % 1_000_000) * 1000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap_or_default();
            ResultValue::Time(time.format("%H:%M:%S%.f").to_string())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::Time64NanosecondArray>()
                .unwrap();
            let ns = arr.value(row);
            let secs = (ns / 1_000_000_000) as u32;
            let nanos = (ns % 1_000_000_000) as u32;
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap_or_default();
            ResultValue::Time(time.format("%H:%M:%S%.f").to_string())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let secs = arr.value(row);
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .unwrap_or_default()
                .naive_utc();
            ResultValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            let ms = arr.value(row);
            let secs = ms / 1000;
            let nanos = ((ms % 1000).unsigned_abs() as u32) * 1_000_000;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .unwrap_or_default()
                .naive_utc();
            ResultValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let us = arr.value(row);
            let secs = us / 1_000_000;
            let nanos = ((us % 1_000_000).unsigned_abs() as u32) * 1000;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .unwrap_or_default()
                .naive_utc();
            ResultValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let ns = arr.value(row);
            let secs = ns / 1_000_000_000;
            let nanos = (ns % 1_000_000_000).unsigned_abs() as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                .unwrap_or_default()
                .naive_utc();
            ResultValue::DateTime(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<arrow_array::Decimal128Array>().unwrap();
            let val = arr.value(row);
            let s = format_decimal128(val, *scale as u32);
            ResultValue::Numeric(s)
        }
        _ => ResultValue::Text(format!("{:?}", array.data_type())),
    }
}

fn format_decimal128(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10i128.pow(scale);
    let integer = value / divisor;
    let frac = (value % divisor).unsigned_abs();
    format!("{integer}.{frac:0>width$}", width = scale as usize)
}

/// Convert Prisma `QueryValue` parameters into an Arrow `RecordBatch` for ADBC parameter binding.
pub fn query_values_to_record_batch(args: &[QueryValue]) -> RecordBatch {
    if args.is_empty() {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }

    let mut fields = Vec::with_capacity(args.len());
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(args.len());

    for (i, arg) in args.iter().enumerate() {
        let name = format!("{}", i + 1);
        match arg {
            QueryValue::Null => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![None::<&str>])));
            }
            QueryValue::Boolean(v) => {
                fields.push(Field::new(name, DataType::Boolean, true));
                columns.push(Arc::new(BooleanArray::from(vec![Some(*v)])));
            }
            QueryValue::Int32(v) => {
                fields.push(Field::new(name, DataType::Int32, true));
                columns.push(Arc::new(Int32Array::from(vec![Some(*v)])));
            }
            QueryValue::Int64(v) => {
                fields.push(Field::new(name, DataType::Int64, true));
                columns.push(Arc::new(Int64Array::from(vec![Some(*v)])));
            }
            QueryValue::Float(v) => {
                fields.push(Field::new(name, DataType::Float32, true));
                columns.push(Arc::new(Float32Array::from(vec![Some(*v)])));
            }
            QueryValue::Double(v) => {
                fields.push(Field::new(name, DataType::Float64, true));
                columns.push(Arc::new(Float64Array::from(vec![Some(*v)])));
            }
            QueryValue::Text(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(v.as_str())])));
            }
            QueryValue::Bytes(v) => {
                fields.push(Field::new(name, DataType::Binary, true));
                columns.push(Arc::new(BinaryArray::from_vec(vec![v.as_slice()])));
            }
            QueryValue::Numeric(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(v.to_string().as_str())])));
            }
            QueryValue::Uuid(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(v.to_string().as_str())])));
            }
            QueryValue::DateTime(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(
                    v.format("%Y-%m-%d %H:%M:%S%.f").to_string().as_str(),
                )])));
            }
            QueryValue::Date(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(
                    v.format("%Y-%m-%d").to_string().as_str(),
                )])));
            }
            QueryValue::Time(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(
                    v.format("%H:%M:%S%.f").to_string().as_str(),
                )])));
            }
            QueryValue::Json(v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![Some(v.to_string().as_str())])));
            }
            QueryValue::Array(_) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vec![None::<&str>])));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_type_mappings() {
        assert_eq!(arrow_type_to_column_type(&DataType::Boolean), ColumnType::Boolean);
        assert_eq!(arrow_type_to_column_type(&DataType::Int32), ColumnType::Int32);
        assert_eq!(arrow_type_to_column_type(&DataType::Int64), ColumnType::Int64);
        assert_eq!(arrow_type_to_column_type(&DataType::Float32), ColumnType::Float);
        assert_eq!(arrow_type_to_column_type(&DataType::Float64), ColumnType::Double);
        assert_eq!(arrow_type_to_column_type(&DataType::Utf8), ColumnType::Text);
        assert_eq!(arrow_type_to_column_type(&DataType::Binary), ColumnType::Bytes);
        assert_eq!(
            arrow_type_to_column_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            ColumnType::DateTime
        );
        assert_eq!(arrow_type_to_column_type(&DataType::Date32), ColumnType::Date);
        assert_eq!(
            arrow_type_to_column_type(&DataType::Decimal128(10, 2)),
            ColumnType::Numeric
        );
    }

    #[test]
    fn empty_record_batches() {
        let result = record_batches_to_result_set(&[]);
        assert_eq!(result.column_names.len(), 0);
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn record_batch_to_result_set_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            ],
        )
        .unwrap();

        let result = record_batches_to_result_set(&[batch]);
        assert_eq!(result.column_names, vec!["id", "name", "active"]);
        assert_eq!(
            result.column_types,
            vec![ColumnType::Int32, ColumnType::Text, ColumnType::Boolean]
        );
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], ResultValue::Int32(1));
        assert_eq!(result.rows[0][1], ResultValue::Text("Alice".into()));
        assert_eq!(result.rows[0][2], ResultValue::Boolean(true));
        assert_eq!(result.rows[1][0], ResultValue::Int32(2));
        assert_eq!(result.rows[1][1], ResultValue::Text("Bob".into()));
        assert_eq!(result.rows[1][2], ResultValue::Boolean(false));
    }

    #[test]
    fn record_batch_with_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(42), None, Some(99)]))]).unwrap();

        let result = record_batches_to_result_set(&[batch]);
        assert_eq!(result.rows[0][0], ResultValue::Int64(42));
        assert_eq!(result.rows[1][0], ResultValue::Null);
        assert_eq!(result.rows[2][0], ResultValue::Int64(99));
    }

    #[test]
    fn record_batch_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let b2 = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![3]))]).unwrap();

        let result = record_batches_to_result_set(&[b1, b2]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[2][0], ResultValue::Int32(3));
    }

    #[test]
    fn record_batch_float_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float32Array::from(vec![1.5f32])),
                Arc::new(Float64Array::from(vec![2.5f64])),
            ],
        )
        .unwrap();

        let result = record_batches_to_result_set(&[batch]);
        assert_eq!(result.rows[0][0], ResultValue::Float(1.5));
        assert_eq!(result.rows[0][1], ResultValue::Double(2.5));
    }

    #[test]
    fn record_batch_timestamp() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        // 2024-01-15 11:30:00 UTC in microseconds since epoch
        let us = 1_705_318_200_000_000i64;
        let batch = RecordBatch::try_new(schema, vec![Arc::new(TimestampMicrosecondArray::from(vec![us]))]).unwrap();

        let result = record_batches_to_result_set(&[batch]);
        if let ResultValue::DateTime(s) = &result.rows[0][0] {
            assert!(s.starts_with("2024-01-15 11:30:00"));
        } else {
            panic!("Expected DateTime, got {:?}", result.rows[0][0]);
        }
    }

    #[test]
    fn query_values_to_batch_empty() {
        let batch = query_values_to_record_batch(&[]);
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn query_values_to_batch_mixed() {
        let args = vec![
            QueryValue::Int32(42),
            QueryValue::Text("hello".into()),
            QueryValue::Boolean(true),
        ];
        let batch = query_values_to_record_batch(&args);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Boolean);
    }

    #[test]
    fn decimal128_formatting() {
        assert_eq!(format_decimal128(12345, 2), "123.45");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(42, 0), "42");
        assert_eq!(format_decimal128(-12345, 2), "-123.45");
    }
}
