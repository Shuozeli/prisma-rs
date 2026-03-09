//! Intermediate value type used during expression interpretation.
//!
//! Maps between prisma-ir PrismaValue and our driver-core types.

use prisma_driver_core::{QueryValue, ResultValue, SqlResultSet};
use prisma_ir::PrismaValue;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// The result of evaluating an expression node.
#[derive(Debug, Clone)]
pub struct IntermediateValue {
    pub value: IValue,
    pub last_insert_id: Option<String>,
}

impl IntermediateValue {
    pub fn new(value: IValue) -> Self {
        Self {
            value,
            last_insert_id: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_last_insert_id(value: IValue, id: String) -> Self {
        Self {
            value,
            last_insert_id: Some(id),
        }
    }

    pub fn unit() -> Self {
        Self::new(IValue::Null)
    }
}

/// Internal value representation during interpretation.
///
/// This bridges the gap between PrismaValue (from the expression tree),
/// SqlResultSet (from drivers), and the final JSON output.
#[derive(Debug, Clone)]
pub enum IValue {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<IValue>),
    Record(BTreeMap<String, IValue>),
    Json(JsonValue),
}

impl IValue {
    pub fn is_null(&self) -> bool {
        matches!(self, IValue::Null)
    }

    pub fn is_list(&self) -> bool {
        matches!(self, IValue::List(_))
    }

    pub fn as_list(&self) -> Option<&[IValue]> {
        match self {
            IValue::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_list(self) -> Vec<IValue> {
        match self {
            IValue::List(v) => v,
            other => vec![other],
        }
    }

    pub fn as_record(&self) -> Option<&BTreeMap<String, IValue>> {
        match self {
            IValue::Record(m) => Some(m),
            _ => None,
        }
    }

    pub fn into_record(self) -> Option<BTreeMap<String, IValue>> {
        match self {
            IValue::Record(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            IValue::Int(n) => Some(*n),
            IValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            IValue::Float(f) => Some(*f),
            IValue::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Count records in a list (or 1 for non-list, 0 for null).
    pub fn row_count(&self) -> usize {
        match self {
            IValue::List(v) => v.len(),
            IValue::Null => 0,
            _ => 1,
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            IValue::Null => JsonValue::Null,
            IValue::Boolean(b) => JsonValue::Bool(*b),
            IValue::Int(n) => serde_json::json!(*n),
            IValue::Float(f) => serde_json::json!(*f),
            IValue::String(s) => JsonValue::String(s.clone()),
            IValue::Bytes(b) => {
                JsonValue::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b))
            }
            IValue::List(items) => JsonValue::Array(items.iter().map(IValue::to_json).collect()),
            IValue::Record(map) => {
                let obj: serde_json::Map<String, JsonValue> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                JsonValue::Object(obj)
            }
            IValue::Json(v) => v.clone(),
        }
    }
}

/// Convert a driver ResultValue to an IValue.
pub fn result_value_to_ivalue(rv: &ResultValue) -> IValue {
    match rv {
        ResultValue::Null => IValue::Null,
        ResultValue::Boolean(b) => IValue::Boolean(*b),
        ResultValue::Int32(n) => IValue::Int(*n as i64),
        ResultValue::Int64(n) => IValue::Int(*n),
        ResultValue::Float(f) => IValue::Float(*f as f64),
        ResultValue::Double(f) => IValue::Float(*f),
        ResultValue::Numeric(s) => IValue::String(s.clone()),
        ResultValue::Text(s) => IValue::String(s.clone()),
        ResultValue::Date(s) => IValue::String(s.clone()),
        ResultValue::Time(s) => IValue::String(s.clone()),
        ResultValue::DateTime(s) => IValue::String(s.clone()),
        ResultValue::Json(s) => serde_json::from_str(s)
            .map(IValue::Json)
            .unwrap_or_else(|_| IValue::String(s.clone())),
        ResultValue::Enum(s) => IValue::String(s.clone()),
        ResultValue::Uuid(s) => IValue::String(s.clone()),
        ResultValue::Bytes(b) => IValue::Bytes(b.clone()),
        ResultValue::Array(arr) => IValue::List(arr.iter().map(result_value_to_ivalue).collect()),
    }
}

/// Convert an SqlResultSet into a list of record IValues.
pub fn result_set_to_records(rs: &SqlResultSet) -> Vec<IValue> {
    rs.rows
        .iter()
        .map(|row| {
            let mut record = BTreeMap::new();
            for (i, col_name) in rs.column_names.iter().enumerate() {
                let val = row.get(i).map(result_value_to_ivalue).unwrap_or(IValue::Null);
                record.insert(col_name.clone(), val);
            }
            IValue::Record(record)
        })
        .collect()
}

/// Convert a PrismaValue (from IR) to a driver QueryValue.
pub fn prisma_value_to_query_value(pv: &PrismaValue) -> QueryValue {
    match pv {
        PrismaValue::Null => QueryValue::Null,
        PrismaValue::Boolean(b) => QueryValue::Boolean(*b),
        PrismaValue::Int(n) => QueryValue::Int64(*n),
        PrismaValue::BigInt(n) => QueryValue::Int64(*n),
        PrismaValue::Float(s) => {
            if let Ok(f) = s.parse::<f64>() {
                QueryValue::Double(f)
            } else {
                QueryValue::Text(s.clone())
            }
        }
        PrismaValue::String(s) | PrismaValue::Enum(s) => QueryValue::Text(s.clone()),
        PrismaValue::DateTime(s) => QueryValue::Text(s.clone()),
        PrismaValue::Uuid(s) => {
            if let Ok(u) = uuid::Uuid::parse_str(s) {
                QueryValue::Uuid(u)
            } else {
                QueryValue::Text(s.clone())
            }
        }
        PrismaValue::Bytes(b) => QueryValue::Bytes(b.clone()),
        PrismaValue::Json(s) => QueryValue::Json(serde_json::from_str(s).unwrap_or(JsonValue::String(s.clone()))),
        PrismaValue::List(items) => QueryValue::Array(items.iter().map(prisma_value_to_query_value).collect()),
        PrismaValue::GeneratorCall { name, .. } => match name.as_str() {
            "now" => QueryValue::Text(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            other => {
                tracing::warn!(generator = other, "Unknown generator call, defaulting to NULL");
                QueryValue::Null
            }
        },
        PrismaValue::Object(_) | PrismaValue::Placeholder(_) => QueryValue::Null,
    }
}

/// Convert a PrismaValue to an IValue.
pub fn prisma_value_to_ivalue(pv: &PrismaValue) -> IValue {
    match pv {
        PrismaValue::Null => IValue::Null,
        PrismaValue::Boolean(b) => IValue::Boolean(*b),
        PrismaValue::Int(n) => IValue::Int(*n),
        PrismaValue::BigInt(n) => IValue::Int(*n),
        PrismaValue::Float(s) => IValue::Float(s.parse::<f64>().unwrap_or_else(|_| {
            tracing::error!(decimal = %s, "Failed to parse Decimal as f64, defaulting to 0.0");
            0.0
        })),
        PrismaValue::String(s) | PrismaValue::Enum(s) => IValue::String(s.clone()),
        PrismaValue::DateTime(s) => IValue::String(s.clone()),
        PrismaValue::Uuid(s) => IValue::String(s.clone()),
        PrismaValue::Bytes(b) => IValue::Bytes(b.clone()),
        PrismaValue::Json(s) => serde_json::from_str(s)
            .map(IValue::Json)
            .unwrap_or_else(|_| IValue::String(s.clone())),
        PrismaValue::List(items) => IValue::List(items.iter().map(prisma_value_to_ivalue).collect()),
        PrismaValue::Object(fields) => {
            let map = fields
                .iter()
                .map(|(k, v)| (k.clone(), prisma_value_to_ivalue(v)))
                .collect();
            IValue::Record(map)
        }
        PrismaValue::GeneratorCall { name, .. } => match name.as_str() {
            "now" => IValue::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            other => {
                tracing::warn!(generator = other, "Unknown generator call, defaulting to Null");
                IValue::Null
            }
        },
        PrismaValue::Placeholder(_) => IValue::Null,
    }
}

/// Resolve placeholders in a PrismaValue from a scope.
pub fn resolve_prisma_value(pv: &PrismaValue, scope: &crate::scope::Scope) -> PrismaValue {
    match pv {
        PrismaValue::Placeholder(placeholder) => {
            if let Some(iv) = scope.get(&placeholder.name) {
                ivalue_to_prisma_value(iv)
            } else {
                PrismaValue::Null
            }
        }
        PrismaValue::GeneratorCall { name, .. } => match name.as_str() {
            "now" => PrismaValue::DateTime(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            other => {
                tracing::warn!(generator = other, "Unknown generator call, defaulting to Null");
                PrismaValue::Null
            }
        },
        PrismaValue::List(items) => PrismaValue::List(items.iter().map(|v| resolve_prisma_value(v, scope)).collect()),
        PrismaValue::Object(fields) => PrismaValue::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), resolve_prisma_value(v, scope)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn ivalue_to_prisma_value(iv: &IValue) -> PrismaValue {
    match iv {
        IValue::Null => PrismaValue::Null,
        IValue::Boolean(b) => PrismaValue::Boolean(*b),
        IValue::Int(n) => PrismaValue::Int(*n),
        IValue::Float(f) => PrismaValue::Float(f.to_string()),
        IValue::String(s) => PrismaValue::String(s.clone()),
        IValue::Bytes(b) => PrismaValue::Bytes(b.clone()),
        IValue::List(items) => PrismaValue::List(items.iter().map(ivalue_to_prisma_value).collect()),
        IValue::Record(fields) => PrismaValue::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), ivalue_to_prisma_value(v)))
                .collect(),
        ),
        IValue::Json(v) => PrismaValue::Json(v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prisma_driver_core::{ColumnType, ResultValue, SqlResultSet};
    use serde_json::json;

    // --- IValue ---

    #[test]
    fn ivalue_null_to_json() {
        assert_eq!(IValue::Null.to_json(), JsonValue::Null);
    }

    #[test]
    fn ivalue_boolean_to_json() {
        assert_eq!(IValue::Boolean(true).to_json(), json!(true));
        assert_eq!(IValue::Boolean(false).to_json(), json!(false));
    }

    #[test]
    fn ivalue_int_to_json() {
        assert_eq!(IValue::Int(42).to_json(), json!(42));
        assert_eq!(IValue::Int(-1).to_json(), json!(-1));
        assert_eq!(IValue::Int(0).to_json(), json!(0));
        assert_eq!(IValue::Int(i64::MAX).to_json(), json!(i64::MAX));
    }

    #[test]
    fn ivalue_float_to_json() {
        assert_eq!(IValue::Float(3.14).to_json(), json!(3.14));
        assert_eq!(IValue::Float(0.0).to_json(), json!(0.0));
        assert_eq!(IValue::Float(-1.5).to_json(), json!(-1.5));
    }

    #[test]
    fn ivalue_string_to_json() {
        assert_eq!(IValue::String("hello".into()).to_json(), json!("hello"));
        assert_eq!(IValue::String("".into()).to_json(), json!(""));
    }

    #[test]
    fn ivalue_bytes_to_json_base64() {
        let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let val = IValue::Bytes(bytes).to_json();
        assert_eq!(val, json!("SGVsbG8=")); // base64 of "Hello"
    }

    #[test]
    fn ivalue_list_to_json() {
        let list = IValue::List(vec![IValue::Int(1), IValue::Int(2), IValue::Int(3)]);
        assert_eq!(list.to_json(), json!([1, 2, 3]));
    }

    #[test]
    fn ivalue_empty_list_to_json() {
        let list = IValue::List(vec![]);
        assert_eq!(list.to_json(), json!([]));
    }

    #[test]
    fn ivalue_nested_list_to_json() {
        let nested = IValue::List(vec![
            IValue::List(vec![IValue::Int(1), IValue::Int(2)]),
            IValue::List(vec![IValue::Int(3)]),
        ]);
        assert_eq!(nested.to_json(), json!([[1, 2], [3]]));
    }

    #[test]
    fn ivalue_record_to_json() {
        let mut map = BTreeMap::new();
        map.insert("id".into(), IValue::Int(1));
        map.insert("name".into(), IValue::String("Alice".into()));
        let record = IValue::Record(map);
        assert_eq!(record.to_json(), json!({"id": 1, "name": "Alice"}));
    }

    #[test]
    fn ivalue_nested_record_to_json() {
        let mut inner = BTreeMap::new();
        inner.insert("city".into(), IValue::String("NYC".into()));
        let mut outer = BTreeMap::new();
        outer.insert("id".into(), IValue::Int(1));
        outer.insert("address".into(), IValue::Record(inner));
        let record = IValue::Record(outer);
        assert_eq!(record.to_json(), json!({"address": {"city": "NYC"}, "id": 1}));
    }

    #[test]
    fn ivalue_json_passthrough() {
        let val = json!({"key": [1, 2, 3]});
        assert_eq!(IValue::Json(val.clone()).to_json(), val);
    }

    #[test]
    fn ivalue_mixed_list_to_json() {
        let list = IValue::List(vec![
            IValue::Int(1),
            IValue::String("two".into()),
            IValue::Boolean(true),
            IValue::Null,
        ]);
        assert_eq!(list.to_json(), json!([1, "two", true, null]));
    }

    // --- IValue helpers ---

    #[test]
    fn ivalue_is_null() {
        assert!(IValue::Null.is_null());
        assert!(!IValue::Int(0).is_null());
    }

    #[test]
    fn ivalue_is_list() {
        assert!(IValue::List(vec![]).is_list());
        assert!(!IValue::Int(0).is_list());
    }

    #[test]
    fn ivalue_as_list() {
        let list = IValue::List(vec![IValue::Int(1)]);
        assert_eq!(list.as_list().unwrap().len(), 1);
        assert!(IValue::Int(0).as_list().is_none());
    }

    #[test]
    fn ivalue_into_list() {
        let list = IValue::List(vec![IValue::Int(1), IValue::Int(2)]);
        assert_eq!(list.into_list().len(), 2);
        // Non-list wraps in a vec
        let single = IValue::Int(42);
        assert_eq!(single.into_list().len(), 1);
    }

    #[test]
    fn ivalue_as_record() {
        let mut map = BTreeMap::new();
        map.insert("x".into(), IValue::Int(1));
        let record = IValue::Record(map);
        assert!(record.as_record().is_some());
        assert!(IValue::Int(0).as_record().is_none());
    }

    #[test]
    fn ivalue_as_i64() {
        assert_eq!(IValue::Int(42).as_i64(), Some(42));
        assert_eq!(IValue::Float(3.7).as_i64(), Some(3));
        assert_eq!(IValue::String("x".into()).as_i64(), None);
    }

    #[test]
    fn ivalue_as_f64() {
        assert_eq!(IValue::Float(3.14).as_f64(), Some(3.14));
        assert_eq!(IValue::Int(42).as_f64(), Some(42.0));
        assert_eq!(IValue::String("x".into()).as_f64(), None);
    }

    #[test]
    fn ivalue_row_count() {
        assert_eq!(IValue::Null.row_count(), 0);
        assert_eq!(IValue::Int(1).row_count(), 1);
        assert_eq!(IValue::List(vec![IValue::Int(1), IValue::Int(2)]).row_count(), 2);
        assert_eq!(IValue::List(vec![]).row_count(), 0);
    }

    // --- result_value_to_ivalue ---

    #[test]
    fn result_value_null() {
        assert!(result_value_to_ivalue(&ResultValue::Null).is_null());
    }

    #[test]
    fn result_value_boolean() {
        match result_value_to_ivalue(&ResultValue::Boolean(true)) {
            IValue::Boolean(true) => {}
            other => panic!("expected Boolean(true), got {other:?}"),
        }
    }

    #[test]
    fn result_value_int32() {
        match result_value_to_ivalue(&ResultValue::Int32(42)) {
            IValue::Int(42) => {}
            other => panic!("expected Int(42), got {other:?}"),
        }
    }

    #[test]
    fn result_value_int64() {
        match result_value_to_ivalue(&ResultValue::Int64(i64::MAX)) {
            IValue::Int(n) if n == i64::MAX => {}
            other => panic!("expected Int(MAX), got {other:?}"),
        }
    }

    #[test]
    fn result_value_float() {
        match result_value_to_ivalue(&ResultValue::Float(1.5)) {
            IValue::Float(f) if (f - 1.5).abs() < f64::EPSILON => {}
            other => panic!("expected Float(1.5), got {other:?}"),
        }
    }

    #[test]
    fn result_value_double() {
        match result_value_to_ivalue(&ResultValue::Double(3.14)) {
            IValue::Float(f) if (f - 3.14).abs() < f64::EPSILON => {}
            other => panic!("expected Float(3.14), got {other:?}"),
        }
    }

    #[test]
    fn result_value_text() {
        match result_value_to_ivalue(&ResultValue::Text("hello".into())) {
            IValue::String(s) if s == "hello" => {}
            other => panic!("expected String(hello), got {other:?}"),
        }
    }

    #[test]
    fn result_value_numeric() {
        match result_value_to_ivalue(&ResultValue::Numeric("123.456".into())) {
            IValue::String(s) if s == "123.456" => {}
            other => panic!("expected String(123.456), got {other:?}"),
        }
    }

    #[test]
    fn result_value_date() {
        match result_value_to_ivalue(&ResultValue::Date("2024-01-15".into())) {
            IValue::String(s) if s == "2024-01-15" => {}
            other => panic!("expected String(2024-01-15), got {other:?}"),
        }
    }

    #[test]
    fn result_value_datetime() {
        match result_value_to_ivalue(&ResultValue::DateTime("2024-01-15T10:30:00Z".into())) {
            IValue::String(s) if s == "2024-01-15T10:30:00Z" => {}
            other => panic!("expected DateTime string, got {other:?}"),
        }
    }

    #[test]
    fn result_value_json_valid() {
        match result_value_to_ivalue(&ResultValue::Json("{\"key\":42}".into())) {
            IValue::Json(v) => assert_eq!(v, json!({"key": 42})),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn result_value_json_invalid() {
        match result_value_to_ivalue(&ResultValue::Json("not json".into())) {
            IValue::String(s) if s == "not json" => {}
            other => panic!("expected String fallback, got {other:?}"),
        }
    }

    #[test]
    fn result_value_uuid() {
        match result_value_to_ivalue(&ResultValue::Uuid("550e8400-e29b-41d4-a716-446655440000".into())) {
            IValue::String(s) if s.contains("550e8400") => {}
            other => panic!("expected UUID string, got {other:?}"),
        }
    }

    #[test]
    fn result_value_bytes() {
        match result_value_to_ivalue(&ResultValue::Bytes(vec![1, 2, 3])) {
            IValue::Bytes(b) if b == vec![1, 2, 3] => {}
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn result_value_enum() {
        match result_value_to_ivalue(&ResultValue::Enum("ACTIVE".into())) {
            IValue::String(s) if s == "ACTIVE" => {}
            other => panic!("expected String(ACTIVE), got {other:?}"),
        }
    }

    #[test]
    fn result_value_array() {
        let arr = ResultValue::Array(vec![ResultValue::Int32(1), ResultValue::Int32(2)]);
        match result_value_to_ivalue(&arr) {
            IValue::List(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], IValue::Int(1)));
                assert!(matches!(items[1], IValue::Int(2)));
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    // --- result_set_to_records ---

    #[test]
    fn result_set_empty() {
        let rs = SqlResultSet {
            column_names: vec!["id".into(), "name".into()],
            column_types: vec![ColumnType::Int32, ColumnType::Text],
            rows: vec![],
            last_insert_id: None,
        };
        let records = result_set_to_records(&rs);
        assert!(records.is_empty());
    }

    #[test]
    fn result_set_single_row() {
        let rs = SqlResultSet {
            column_names: vec!["id".into(), "name".into()],
            column_types: vec![ColumnType::Int32, ColumnType::Text],
            rows: vec![vec![ResultValue::Int32(1), ResultValue::Text("Alice".into())]],
            last_insert_id: None,
        };
        let records = result_set_to_records(&rs);
        assert_eq!(records.len(), 1);
        let json = records[0].to_json();
        assert_eq!(json, json!({"id": 1, "name": "Alice"}));
    }

    #[test]
    fn result_set_multiple_rows() {
        let rs = SqlResultSet {
            column_names: vec!["id".into(), "email".into()],
            column_types: vec![ColumnType::Int32, ColumnType::Text],
            rows: vec![
                vec![ResultValue::Int32(1), ResultValue::Text("a@b.com".into())],
                vec![ResultValue::Int32(2), ResultValue::Text("c@d.com".into())],
                vec![ResultValue::Int32(3), ResultValue::Null],
            ],
            last_insert_id: None,
        };
        let records = result_set_to_records(&rs);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].to_json(), json!({"email": "a@b.com", "id": 1}));
        assert_eq!(records[2].to_json(), json!({"email": null, "id": 3}));
    }

    #[test]
    fn result_set_with_all_types() {
        let rs = SqlResultSet {
            column_names: vec![
                "b".into(),
                "i".into(),
                "f".into(),
                "s".into(),
                "dt".into(),
                "j".into(),
                "u".into(),
            ],
            column_types: vec![
                ColumnType::Boolean,
                ColumnType::Int64,
                ColumnType::Double,
                ColumnType::Text,
                ColumnType::DateTime,
                ColumnType::Json,
                ColumnType::Uuid,
            ],
            rows: vec![vec![
                ResultValue::Boolean(true),
                ResultValue::Int64(9999999999),
                ResultValue::Double(2.718),
                ResultValue::Text("test".into()),
                ResultValue::DateTime("2024-01-01T00:00:00Z".into()),
                ResultValue::Json("{\"x\":1}".into()),
                ResultValue::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ]],
            last_insert_id: None,
        };
        let records = result_set_to_records(&rs);
        let json = records[0].to_json();
        assert_eq!(json["b"], json!(true));
        assert_eq!(json["i"], json!(9999999999_i64));
        assert_eq!(json["s"], json!("test"));
        assert_eq!(json["u"], json!("550e8400-e29b-41d4-a716-446655440000"));
    }

    // --- IntermediateValue ---

    #[test]
    fn intermediate_value_unit() {
        let iv = IntermediateValue::unit();
        assert!(iv.value.is_null());
        assert!(iv.last_insert_id.is_none());
    }

    #[test]
    fn intermediate_value_with_id() {
        let iv = IntermediateValue::with_last_insert_id(IValue::Int(1), "42".into());
        assert_eq!(iv.last_insert_id, Some("42".into()));
    }
}
