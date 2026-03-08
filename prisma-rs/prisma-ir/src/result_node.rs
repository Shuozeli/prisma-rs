//! Result structure types.
//!
//! Mirrors `query_compiler::result_node::ResultNode` and related types.

use std::fmt;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// The structure of a query result.
///
/// Mirrors `query_compiler::result_node::ResultNode`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResultNode {
    AffectedRows,
    Object(ResultObject),
    #[serde(rename_all = "camelCase")]
    Field {
        db_name: String,
        field_type: FieldType,
    },
}

/// An object node in the result structure.
///
/// Mirrors `query_compiler::result_node::Object`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultObject {
    serialized_name: Option<String>,
    fields: IndexMap<String, ResultNode>,
    skip_nulls: bool,
}

impl ResultObject {
    pub fn serialized_name(&self) -> Option<&str> {
        self.serialized_name.as_deref()
    }

    pub fn fields(&self) -> &IndexMap<String, ResultNode> {
        &self.fields
    }
}

/// Field type describing the scalar type and arity of a result field.
///
/// Mirrors `query_compiler::data_mapper::FieldType`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldType {
    pub arity: FieldArity,
    #[serde(flatten)]
    pub r#type: FieldScalarType,
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.arity {
            FieldArity::Required => write!(f, "{}", self.r#type),
            FieldArity::List => write!(f, "{}[]", self.r#type),
            FieldArity::Optional => write!(f, "{}?", self.r#type),
        }
    }
}

/// Scalar type for result fields.
///
/// Mirrors `query_compiler::data_mapper::FieldScalarType`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum FieldScalarType {
    String,
    Int,
    #[serde(rename = "bigint")]
    BigInt,
    Float,
    Decimal,
    Boolean,
    Enum {
        name: String,
    },
    Extension {
        name: String,
    },
    Json,
    Object,
    #[serde(rename = "datetime")]
    DateTime,
    Bytes {
        encoding: ByteArrayEncoding,
    },
    Unsupported,
}

impl fmt::Display for FieldScalarType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::Int => write!(f, "Int"),
            Self::BigInt => write!(f, "BigInt"),
            Self::Float => write!(f, "Float"),
            Self::Decimal => write!(f, "Decimal"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Enum { name } => write!(f, "Enum<{name}>"),
            Self::Extension { name } => write!(f, "{name}"),
            Self::Json => write!(f, "Json"),
            Self::Object => write!(f, "Object"),
            Self::DateTime => write!(f, "DateTime"),
            Self::Bytes { .. } => write!(f, "Bytes"),
            Self::Unsupported => write!(f, "Unsupported"),
        }
    }
}

/// Byte array encoding format.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ByteArrayEncoding {
    #[default]
    Array,
    Base64,
    Hex,
}

/// Field arity (required, optional, or list).
///
/// Mirrors `query_compiler::data_mapper::Arity`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FieldArity {
    Required,
    Optional,
    List,
}
