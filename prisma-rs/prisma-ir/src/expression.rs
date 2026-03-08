//! Expression tree types.
//!
//! Mirrors `query_compiler::expression::Expression` and related types.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::{DataRule, DbQuery, PrismaValue, ResultNode};

/// Enum map: enum_name -> (db_value -> app_value).
///
/// Mirrors `query_compiler::expression::EnumsMap`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnumsMap(pub BTreeMap<String, BTreeMap<String, String>>);

impl EnumsMap {
    pub fn map_db_value(&self, enum_name: &str, db_value: &str) -> Option<&str> {
        self.0
            .get(enum_name)
            .and_then(|mapping| mapping.get(db_value))
            .map(|s| s.as_str())
    }
}

/// A variable binding in a Let expression.
#[derive(Debug, Clone, Deserialize)]
pub struct Binding {
    pub name: String,
    pub expr: Expression,
}

/// A join expression for application-level joins.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinExpression {
    pub child: Expression,
    pub on: Vec<(String, String)>,
    pub parent_field: String,
    pub is_relation_unique: bool,
}

/// The compiled query plan expression tree.
///
/// Mirrors `query_compiler::expression::Expression`.
/// Uses `#[serde(tag = "type", content = "args")]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "args", rename_all = "camelCase")]
pub enum Expression {
    Value(PrismaValue),

    Seq(Vec<Expression>),

    Get {
        name: String,
    },

    Let {
        bindings: Vec<Binding>,
        expr: Box<Expression>,
    },

    GetFirstNonEmpty {
        names: Vec<String>,
    },

    Query(DbQuery),

    Execute(DbQuery),

    Sum(Vec<Expression>),

    Concat(Vec<Expression>),

    Unique(Box<Expression>),

    Required(Box<Expression>),

    #[serde(rename_all = "camelCase")]
    Join {
        parent: Box<Expression>,
        children: Vec<JoinExpression>,
        can_assume_strict_equality: bool,
    },

    MapField {
        field: String,
        records: Box<Expression>,
    },

    Transaction(Box<Expression>),

    DataMap {
        expr: Box<Expression>,
        structure: ResultNode,
        enums: EnumsMap,
    },

    #[serde(rename_all = "camelCase")]
    Validate {
        expr: Box<Expression>,
        rules: Vec<DataRule>,
        error_identifier: String,
        context: serde_json::Value,
    },

    If {
        value: Box<Expression>,
        rule: DataRule,
        then: Box<Expression>,
        r#else: Box<Expression>,
    },

    Unit,

    Diff {
        from: Box<Expression>,
        to: Box<Expression>,
        fields: Vec<String>,
    },

    InitializeRecord {
        expr: Box<Expression>,
        fields: BTreeMap<String, FieldInitializer>,
    },

    MapRecord {
        expr: Box<Expression>,
        fields: BTreeMap<String, FieldOperation>,
    },

    Process {
        expr: Box<Expression>,
        operations: InMemoryOps,
    },
}

/// Field initializer for InitializeRecord.
///
/// Mirrors `query_compiler::expression::FieldInitializer`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum FieldInitializer {
    LastInsertId,
    Value(PrismaValue),
}

/// Field operation for MapRecord.
///
/// Mirrors `query_compiler::expression::FieldOperation`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum FieldOperation {
    Set(PrismaValue),
    Add(PrismaValue),
    Subtract(PrismaValue),
    Multiply(PrismaValue),
    Divide(PrismaValue),
}

/// In-memory operations applied to query results.
///
/// Mirrors `query_compiler::expression::InMemoryOps`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InMemoryOps {
    #[serde(default)]
    pub pagination: Option<Pagination>,
    #[serde(default)]
    pub distinct: Option<Vec<String>>,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub nested: BTreeMap<String, InMemoryOps>,
    #[serde(default)]
    pub linking_fields: Option<Vec<String>>,
}

/// Pagination parameters for in-memory processing.
///
/// Mirrors `query_compiler::expression::Pagination`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub cursor: Option<std::collections::HashMap<String, PrismaValue>>,
    pub take: Option<i64>,
    pub skip: Option<i64>,
}
