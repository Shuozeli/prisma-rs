//! Database query types.
//!
//! Mirrors `query_builder::DbQuery`, `query_template::Fragment`,
//! and related types from prisma-engines.

use serde::{Deserialize, Serialize};

use crate::PrismaValue;

/// A database query, either raw SQL or a template.
///
/// Mirrors `query_builder::DbQuery`.
/// Serialized with `#[serde(tag = "type", rename_all = "camelCase")]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DbQuery {
    #[serde(rename_all = "camelCase")]
    RawSql {
        sql: String,
        args: Vec<PrismaValue>,
        arg_types: Vec<ArgType>,
    },
    #[serde(rename_all = "camelCase")]
    TemplateSql {
        fragments: Vec<Fragment>,
        args: Vec<PrismaValue>,
        arg_types: Vec<DynamicArgType>,
        placeholder_format: PlaceholderFormat,
        chunkable: bool,
    },
}

/// A SQL template fragment.
///
/// Mirrors `query_template::Fragment`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Fragment {
    StringChunk {
        chunk: String,
    },
    Parameter,
    #[serde(rename_all = "camelCase")]
    ParameterTuple {
        item_prefix: String,
        item_separator: String,
        item_suffix: String,
    },
    #[serde(rename_all = "camelCase")]
    ParameterTupleList {
        item_prefix: String,
        item_separator: String,
        item_suffix: String,
        group_separator: String,
    },
}

/// Placeholder format for SQL parameters.
///
/// Mirrors `query_template::PlaceholderFormat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceholderFormat {
    pub prefix: String,
    pub has_numbering: bool,
}

/// Argument type for SQL parameters.
///
/// Mirrors `query_builder::ArgType`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgType {
    pub arity: QueryArity,
    pub scalar_type: ArgScalarType,
    pub db_type: Option<String>,
}

/// Dynamic argument type (single or tuple).
///
/// Mirrors `query_builder::DynamicArgType`.
/// Uses `#[serde(tag = "arity", rename_all = "camelCase")]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "arity", rename_all = "camelCase")]
pub enum DynamicArgType {
    Tuple {
        elements: Vec<ArgType>,
    },
    #[serde(untagged)]
    Single {
        #[serde(flatten)]
        r#type: ArgType,
    },
}

/// Scalar type for SQL arguments.
///
/// Mirrors `query_builder::ArgScalarType`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArgScalarType {
    String,
    Int,
    #[serde(rename = "bigint")]
    BigInt,
    Float,
    Decimal,
    Boolean,
    Enum,
    Uuid,
    Json,
    #[serde(rename = "datetime")]
    DateTime,
    Bytes,
    Unknown,
}

/// Arity for query builder arguments (scalar vs list).
///
/// Mirrors `query_builder::Arity` (distinct from `data_mapper::Arity`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryArity {
    Scalar,
    List,
}
