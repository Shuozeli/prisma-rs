//! Data validation rules.
//!
//! Mirrors `query_core::DataRule` from prisma-engines.

use serde::{Deserialize, Serialize};

/// A rule for validating data shape in expressions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "args", rename_all = "camelCase")]
pub enum DataRule {
    RowCountEq(usize),
    RowCountNeq(usize),
    AffectedRowCountEq(usize),
    Never,
}
