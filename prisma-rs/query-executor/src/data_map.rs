//! Data mapping: transform raw SQL results to match the Prisma schema structure.

use crate::value::IValue;
use prisma_ir::{EnumsMap, ResultNode, ResultObject};
use std::collections::BTreeMap;

/// Apply the data map transformation to a raw value according to the result structure.
///
/// This converts flat SQL result rows into nested Prisma response objects,
/// applying enum mappings and field type coercions.
pub fn apply_data_map(
    value: &IValue,
    structure: &ResultNode,
    enums: &EnumsMap,
) -> Result<IValue, crate::ExecutorError> {
    Ok(apply_data_map_inner(value, structure, enums))
}

fn apply_data_map_inner(value: &IValue, structure: &ResultNode, enums: &EnumsMap) -> IValue {
    match structure {
        ResultNode::AffectedRows => {
            let mut map = BTreeMap::new();
            map.insert("count".to_string(), value.clone());
            IValue::Record(map)
        }
        ResultNode::Object(obj) => match value {
            IValue::List(items) => IValue::List(items.iter().map(|item| map_object(item, obj, enums)).collect()),
            _ => map_object(value, obj, enums),
        },
        ResultNode::Field { db_name: _, field_type } => {
            let ft_str = field_type.to_string();
            map_field_value(value, &ft_str, enums)
        }
    }
}

fn map_object(value: &IValue, obj: &ResultObject, enums: &EnumsMap) -> IValue {
    let record = match value {
        IValue::Record(r) => r,
        _ => return value.clone(),
    };

    let mut result = BTreeMap::new();

    for (key, node) in obj.fields() {
        let field_key = key.as_str();
        match node {
            ResultNode::Field { db_name, field_type } => {
                let raw_val = record.get(db_name.as_str()).cloned().unwrap_or(IValue::Null);
                let ft_str = field_type.to_string();
                let mapped = map_field_value(&raw_val, &ft_str, enums);
                result.insert(field_key.to_string(), mapped);
            }
            ResultNode::Object(nested_obj) => {
                if nested_obj.serialized_name().is_none() {
                    let nested = map_object(value, nested_obj, enums);
                    result.insert(field_key.to_string(), nested);
                } else {
                    let lookup_key = nested_obj.serialized_name().unwrap_or(field_key);
                    let nested_val = record
                        .get(lookup_key)
                        .or_else(|| record.get(field_key))
                        .cloned()
                        .unwrap_or(IValue::Null);
                    match &nested_val {
                        IValue::List(items) => {
                            let mapped: Vec<IValue> =
                                items.iter().map(|item| map_object(item, nested_obj, enums)).collect();
                            result.insert(field_key.to_string(), IValue::List(mapped));
                        }
                        _ => {
                            result.insert(field_key.to_string(), map_object(&nested_val, nested_obj, enums));
                        }
                    }
                }
            }
            ResultNode::AffectedRows => {
                let raw_val = record.get(field_key).cloned().unwrap_or(IValue::Null);
                let mut map = BTreeMap::new();
                map.insert("count".to_string(), raw_val);
                result.insert(field_key.to_string(), IValue::Record(map));
            }
        }
    }

    IValue::Record(result)
}

/// Extract enum name from a `FieldType`'s Display output.
///
/// `FieldType::Display` outputs `Enum<Name>`, `Enum<Name>?`, or `Enum<Name>[]`.
/// Returns `Some("Name")` for enum types, `None` otherwise.
fn extract_enum_name(field_type_str: &str) -> Option<&str> {
    let s = field_type_str.strip_suffix("[]").unwrap_or(field_type_str);
    let s = s.strip_suffix('?').unwrap_or(s);
    let s = s.strip_prefix("Enum<")?;
    let name = s.strip_suffix('>')?;
    Some(name)
}

/// Map a single field value, applying enum db_value -> app_value mapping
/// when the field type is an enum.
fn map_field_value(value: &IValue, field_type_str: &str, enums: &EnumsMap) -> IValue {
    let Some(enum_name) = extract_enum_name(field_type_str) else {
        return value.clone();
    };

    let Some(enum_mapping) = enums.0.get(enum_name) else {
        return value.clone();
    };

    match value {
        IValue::String(db_value) => {
            match enum_mapping.get(db_value.as_str()) {
                Some(app_value) => IValue::String(app_value.clone()),
                // No mapping found means db_value == app_value
                None => value.clone(),
            }
        }
        IValue::Null => IValue::Null,
        IValue::List(items) => {
            // Enum arrays (e.g. PostgreSQL enum[])
            IValue::List(
                items
                    .iter()
                    .map(|item| map_field_value(item, field_type_str, enums))
                    .collect(),
            )
        }
        // Non-string values pass through unchanged
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_enum_name_required() {
        assert_eq!(extract_enum_name("Enum<Role>"), Some("Role"));
    }

    #[test]
    fn extract_enum_name_optional() {
        assert_eq!(extract_enum_name("Enum<Status>?"), Some("Status"));
    }

    #[test]
    fn extract_enum_name_list() {
        assert_eq!(extract_enum_name("Enum<Color>[]"), Some("Color"));
    }

    #[test]
    fn extract_enum_name_non_enum() {
        assert_eq!(extract_enum_name("String"), None);
        assert_eq!(extract_enum_name("Int"), None);
        assert_eq!(extract_enum_name("Boolean?"), None);
    }
}
