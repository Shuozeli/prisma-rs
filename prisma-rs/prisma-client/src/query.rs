//! Query builder for constructing Prisma JSON protocol requests.
//!
//! Builds the request body that the compiler expects:
//! ```json
//! {
//!   "modelName": "User",
//!   "action": "findMany",
//!   "query": {
//!     "arguments": { ... },
//!     "selection": { "$scalars": true }
//!   }
//! }
//! ```

use serde_json::{Map, Value};

use crate::selection::Selection;

/// A Prisma client operation (maps to the JSON protocol action).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    FindMany,
    FindUnique,
    FindFirst,
    CreateOne,
    CreateMany,
    CreateManyAndReturn,
    UpdateOne,
    UpdateMany,
    UpdateManyAndReturn,
    DeleteOne,
    DeleteMany,
    UpsertOne,
    Aggregate,
    GroupBy,
    Count,
    FindFirstOrThrow,
    FindUniqueOrThrow,
    ExecuteRaw,
    QueryRaw,
}

impl Operation {
    fn as_str(&self) -> &'static str {
        match self {
            Operation::FindMany => "findMany",
            Operation::FindUnique => "findUnique",
            Operation::FindFirst => "findFirst",
            Operation::CreateOne => "createOne",
            Operation::CreateMany => "createMany",
            Operation::CreateManyAndReturn => "createManyAndReturn",
            Operation::UpdateOne => "updateOne",
            Operation::UpdateMany => "updateMany",
            Operation::UpdateManyAndReturn => "updateManyAndReturn",
            Operation::DeleteOne => "deleteOne",
            Operation::DeleteMany => "deleteMany",
            Operation::UpsertOne => "upsertOne",
            Operation::Aggregate => "aggregate",
            Operation::GroupBy => "groupBy",
            Operation::Count => "aggregate",
            Operation::FindFirstOrThrow => "findFirstOrThrow",
            Operation::FindUniqueOrThrow => "findUniqueOrThrow",
            Operation::ExecuteRaw => "executeRaw",
            Operation::QueryRaw => "queryRaw",
        }
    }
}

/// Builds a Prisma JSON protocol request for a single model operation.
///
/// Generated client code creates one of these per method call, fills in
/// the arguments, and calls `PrismaClient::execute` with the built request.
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    model: Option<String>,
    action: Operation,
    arguments: Map<String, Value>,
    selection: Selection,
}

impl QueryBuilder {
    /// Create a new query builder for a model operation.
    pub fn new(model: impl Into<String>, action: Operation) -> Self {
        Self {
            model: Some(model.into()),
            action,
            arguments: Map::new(),
            selection: Selection::scalars(),
        }
    }

    /// Create a query builder for a raw SQL operation (no model).
    pub fn raw(action: Operation) -> Self {
        Self {
            model: None,
            action,
            arguments: Map::new(),
            selection: Selection::scalars(),
        }
    }

    /// Set the `where` argument.
    pub fn where_arg(mut self, value: Value) -> Self {
        self.arguments.insert("where".into(), value);
        self
    }

    /// Set the `data` argument.
    pub fn data(mut self, value: Value) -> Self {
        self.arguments.insert("data".into(), value);
        self
    }

    /// Set the `orderBy` argument.
    pub fn order_by(mut self, value: Value) -> Self {
        self.arguments.insert("orderBy".into(), value);
        self
    }

    /// Set the `take` argument (limit).
    pub fn take(mut self, n: i64) -> Self {
        self.arguments.insert("take".into(), Value::Number(n.into()));
        self
    }

    /// Set the `skip` argument (offset).
    pub fn skip(mut self, n: i64) -> Self {
        self.arguments.insert("skip".into(), Value::Number(n.into()));
        self
    }

    /// Set the `cursor` argument.
    pub fn cursor(mut self, value: Value) -> Self {
        self.arguments.insert("cursor".into(), value);
        self
    }

    /// Set the `distinct` argument.
    pub fn distinct(mut self, fields: Vec<String>) -> Self {
        let arr: Vec<Value> = fields.into_iter().map(Value::String).collect();
        self.arguments.insert("distinct".into(), Value::Array(arr));
        self
    }

    /// Set a custom argument by key.
    pub fn arg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.arguments.insert(key.into(), value);
        self
    }

    /// Set the selection (overrides the default scalars selection).
    pub fn selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Build the JSON protocol request body.
    pub fn build(&self) -> Value {
        let mut query = Map::new();

        if !self.arguments.is_empty() {
            query.insert("arguments".into(), Value::Object(self.arguments.clone()));
        }

        query.insert("selection".into(), self.selection.build());

        let mut request = Map::new();
        if let Some(ref model) = self.model {
            request.insert("modelName".into(), Value::String(model.clone()));
        }
        request.insert("action".into(), Value::String(self.action.as_str().into()));
        request.insert("query".into(), Value::Object(query));

        Value::Object(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_find_many() {
        let qb = QueryBuilder::new("User", Operation::FindMany);
        let json = qb.build();
        assert_eq!(json["modelName"], "User");
        assert_eq!(json["action"], "findMany");
        assert_eq!(json["query"]["selection"]["$scalars"], true);
    }

    #[test]
    fn build_find_unique_with_where() {
        let qb = QueryBuilder::new("User", Operation::FindUnique).where_arg(serde_json::json!({ "id": 1 }));
        let json = qb.build();
        assert_eq!(json["action"], "findUnique");
        assert_eq!(json["query"]["arguments"]["where"]["id"], 1);
    }

    #[test]
    fn build_create_with_data() {
        let qb = QueryBuilder::new("User", Operation::CreateOne)
            .data(serde_json::json!({ "email": "a@b.com", "name": "Alice" }));
        let json = qb.build();
        assert_eq!(json["action"], "createOne");
        assert_eq!(json["query"]["arguments"]["data"]["email"], "a@b.com");
    }

    #[test]
    fn build_with_pagination() {
        let qb = QueryBuilder::new("User", Operation::FindMany)
            .skip(10)
            .take(5)
            .order_by(serde_json::json!([{ "id": "asc" }]));
        let json = qb.build();
        assert_eq!(json["query"]["arguments"]["skip"], 10);
        assert_eq!(json["query"]["arguments"]["take"], 5);
    }

    #[test]
    fn build_with_select() {
        let sel = Selection::select().field("id").field("email");
        let qb = QueryBuilder::new("User", Operation::FindMany).selection(sel);
        let json = qb.build();
        assert_eq!(json["query"]["selection"]["id"], true);
        assert_eq!(json["query"]["selection"]["email"], true);
        assert!(json["query"]["selection"]["$scalars"].is_null());
    }

    #[test]
    fn build_delete_many_with_where() {
        let qb = QueryBuilder::new("User", Operation::DeleteMany)
            .where_arg(serde_json::json!({ "email": { "contains": "@test.com" } }))
            .selection(Selection::select().field("count"));
        let json = qb.build();
        assert_eq!(json["action"], "deleteMany");
        assert_eq!(json["query"]["selection"]["count"], true);
    }

    #[test]
    fn build_raw_query() {
        let qb = QueryBuilder::raw(Operation::QueryRaw)
            .arg("query", serde_json::json!("SELECT 1"))
            .arg("parameters", serde_json::json!([]));
        let json = qb.build();
        assert!(json["modelName"].is_null());
        assert_eq!(json["action"], "queryRaw");
    }
}
