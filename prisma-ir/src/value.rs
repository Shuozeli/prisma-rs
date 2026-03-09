//! Owned PrismaValue mirror.
//!
//! Mirrors the serialization format of `prisma_value::PrismaValue`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Mirrors prisma-engines `PrismaValue`.
///
/// The upstream type uses `#[serde(untagged)]` with custom serializers
/// for each variant. Our IR deserializes the same JSON shape.
///
/// PrismaValue serializes as:
/// - Null -> `{ "prisma__type": "null", "prisma__value": null }`
/// - Boolean -> `true` / `false`
/// - Int -> `42`
/// - BigInt -> `{ "prisma__type": "bigint", "prisma__value": "42" }`
/// - Float -> `{ "prisma__type": "decimal", "prisma__value": "3.14" }`
/// - String -> `"hello"`
/// - Enum -> `"ACTIVE"` (same as String in JSON)
/// - DateTime -> `{ "prisma__type": "date", "prisma__value": "2024-01-01T00:00:00Z" }`
/// - Uuid -> UUID string
/// - Bytes -> `{ "prisma__type": "bytes", "prisma__value": "base64..." }`
/// - Json -> string containing JSON
/// - List -> array
/// - Object -> `{ "prisma__type": "object", "prisma__value": { ... } }`
/// - Placeholder -> `{ "prisma__type": "param", "prisma__value": { "name": "x", ... } }`
/// - GeneratorCall -> `{ "prisma__type": "generatorCall", "prisma__value": { ... } }`
///
/// Because the upstream uses `#[serde(untagged)]` with custom serializers
/// that produce typed wrappers (`prisma__type`/`prisma__value`), we use
/// a custom deserializer to handle all variants.
#[derive(Debug, Clone, PartialEq)]
pub enum PrismaValue {
    Null,
    Boolean(bool),
    Int(i64),
    BigInt(i64),
    Float(String),
    String(String),
    Enum(String),
    DateTime(String),
    Uuid(String),
    Bytes(Vec<u8>),
    Json(String),
    List(Vec<PrismaValue>),
    Object(Vec<(String, PrismaValue)>),
    Placeholder(Placeholder),
    GeneratorCall { name: String, args: Vec<PrismaValue> },
}

/// A placeholder reference in an expression tree.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Placeholder {
    pub name: String,
    #[serde(flatten)]
    pub r#type: PrismaValueType,
}

/// Type tag for PrismaValue placeholders.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", content = "inner")]
pub enum PrismaValueType {
    String,
    Boolean,
    Enum,
    Int,
    Uuid,
    List(Box<PrismaValueType>),
    Json,
    Object,
    DateTime,
    Float,
    BigInt,
    Bytes,
    Any,
}

// Custom serialization: match the prisma-engines wire format
impl Serialize for PrismaValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            PrismaValue::Null => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "null")?;
                map.serialize_entry("prisma__value", &())?;
                map.end()
            }
            PrismaValue::Boolean(b) => serializer.serialize_bool(*b),
            PrismaValue::Int(n) => serializer.serialize_i64(*n),
            PrismaValue::BigInt(n) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "bigint")?;
                map.serialize_entry("prisma__value", &n.to_string())?;
                map.end()
            }
            PrismaValue::Float(s) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "decimal")?;
                map.serialize_entry("prisma__value", s)?;
                map.end()
            }
            PrismaValue::String(s) | PrismaValue::Enum(s) => serializer.serialize_str(s),
            PrismaValue::DateTime(s) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "date")?;
                map.serialize_entry("prisma__value", s)?;
                map.end()
            }
            PrismaValue::Uuid(s) => serializer.serialize_str(s),
            PrismaValue::Bytes(b) => {
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "bytes")?;
                map.serialize_entry("prisma__value", &encoded)?;
                map.end()
            }
            PrismaValue::Json(s) => serializer.serialize_str(s),
            PrismaValue::List(items) => items.serialize(serializer),
            PrismaValue::Object(fields) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "object")?;
                let obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::to_value(v).unwrap_or_default()))
                    .collect();
                map.serialize_entry("prisma__value", &obj)?;
                map.end()
            }
            PrismaValue::Placeholder(p) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "param")?;
                // Serialize placeholder as a JSON value to avoid Serialize bound
                let pv = serde_json::json!({ "name": p.name });
                map.serialize_entry("prisma__value", &pv)?;
                map.end()
            }
            PrismaValue::GeneratorCall { name, args } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("prisma__type", "generatorCall")?;
                #[derive(Serialize)]
                struct GC<'a> {
                    name: &'a str,
                    args: &'a [PrismaValue],
                }
                map.serialize_entry("prisma__value", &GC { name, args })?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for PrismaValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(deserializer)?;
        Ok(prisma_value_from_json(v))
    }
}

fn prisma_value_from_json(v: serde_json::Value) -> PrismaValue {
    match &v {
        serde_json::Value::Null => PrismaValue::Null,
        serde_json::Value::Bool(b) => PrismaValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PrismaValue::Int(i)
            } else {
                PrismaValue::Float(n.to_string())
            }
        }
        serde_json::Value::String(s) => PrismaValue::String(s.clone()),
        serde_json::Value::Array(arr) => PrismaValue::List(arr.iter().cloned().map(prisma_value_from_json).collect()),
        serde_json::Value::Object(map) => {
            if let Some(ty) = map.get("prisma__type").and_then(|v| v.as_str()) {
                let val = map.get("prisma__value").cloned().unwrap_or(serde_json::Value::Null);
                match ty {
                    "null" => PrismaValue::Null,
                    "bigint" => {
                        let s = val.as_str().unwrap_or("0");
                        PrismaValue::BigInt(s.parse().unwrap_or(0))
                    }
                    "decimal" => {
                        let s = val.as_str().unwrap_or("0");
                        PrismaValue::Float(s.to_string())
                    }
                    "date" => {
                        let s = val.as_str().unwrap_or("");
                        PrismaValue::DateTime(s.to_string())
                    }
                    "bytes" => {
                        use base64::Engine;
                        let s = val.as_str().unwrap_or("");
                        let bytes = base64::engine::general_purpose::STANDARD.decode(s).unwrap_or_default();
                        PrismaValue::Bytes(bytes)
                    }
                    "object" => {
                        if let serde_json::Value::Object(obj) = val {
                            let fields: Vec<(String, PrismaValue)> =
                                obj.into_iter().map(|(k, v)| (k, prisma_value_from_json(v))).collect();
                            PrismaValue::Object(fields)
                        } else {
                            PrismaValue::Null
                        }
                    }
                    "placeholder" | "param" => {
                        if let Ok(p) = serde_json::from_value::<Placeholder>(val) {
                            PrismaValue::Placeholder(p)
                        } else {
                            PrismaValue::Null
                        }
                    }
                    "generatorCall" => {
                        #[derive(Deserialize)]
                        struct GC {
                            name: String,
                            #[serde(default)]
                            args: Vec<PrismaValue>,
                        }
                        if let Ok(gc) = serde_json::from_value::<GC>(val) {
                            PrismaValue::GeneratorCall {
                                name: gc.name,
                                args: gc.args,
                            }
                        } else {
                            PrismaValue::Null
                        }
                    }
                    _ => PrismaValue::String(format!("{v}")),
                }
            } else {
                // Plain object without prisma__type tag
                let fields: Vec<(String, PrismaValue)> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), prisma_value_from_json(v.clone())))
                    .collect();
                PrismaValue::Object(fields)
            }
        }
    }
}

impl fmt::Display for PrismaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrismaValue::Null => write!(f, "null"),
            PrismaValue::Boolean(b) => write!(f, "{b}"),
            PrismaValue::Int(n) => write!(f, "{n}"),
            PrismaValue::BigInt(n) => write!(f, "{n}"),
            PrismaValue::Float(s) => write!(f, "{s}"),
            PrismaValue::String(s) | PrismaValue::Enum(s) => write!(f, "\"{s}\""),
            PrismaValue::DateTime(s) => write!(f, "{s}"),
            PrismaValue::Uuid(s) => write!(f, "{s}"),
            PrismaValue::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            PrismaValue::Json(s) => write!(f, "{s}"),
            PrismaValue::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            PrismaValue::Object(fields) => {
                write!(f, "{{")?;
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            PrismaValue::Placeholder(p) => write!(f, "${}", p.name),
            PrismaValue::GeneratorCall { name, .. } => write!(f, "{name}()"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_null() {
        let v = PrismaValue::Null;
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::Null);
    }

    #[test]
    fn roundtrip_int() {
        let v = PrismaValue::Int(42);
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::Int(42));
    }

    #[test]
    fn roundtrip_bigint() {
        let v = PrismaValue::BigInt(9999999999);
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::BigInt(9999999999));
    }

    #[test]
    fn roundtrip_string() {
        let v = PrismaValue::String("hello".into());
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::String("hello".into()));
    }

    #[test]
    fn roundtrip_list() {
        let v = PrismaValue::List(vec![PrismaValue::Int(1), PrismaValue::Int(2)]);
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn roundtrip_datetime() {
        let v = PrismaValue::DateTime("2024-01-01T00:00:00Z".into());
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::DateTime("2024-01-01T00:00:00Z".into()));
    }

    #[test]
    fn roundtrip_bytes() {
        let v = PrismaValue::Bytes(vec![1, 2, 3]);
        let json = serde_json::to_value(&v).unwrap();
        let back: PrismaValue = serde_json::from_value(json).unwrap();
        assert_eq!(back, PrismaValue::Bytes(vec![1, 2, 3]));
    }
}
