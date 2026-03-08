//! The main query plan interpreter.
//!
//! Walks the `Expression` tree produced by the query compiler,
//! dispatching database operations to the driver adapter and performing
//! data transformation, joins, and validation in Rust.

use std::collections::BTreeMap;

use prisma_driver_core::SqlQueryable;
use prisma_ir::{DataRule, Expression, FieldInitializer, FieldOperation, InMemoryOps, JoinExpression};

use crate::data_map::apply_data_map;
use crate::error::ExecutorError;
use crate::render::render_query;
use crate::scope::Scope;
use crate::value::{IValue, IntermediateValue, prisma_value_to_ivalue, result_set_to_records};

/// Maximum recursion depth for expression interpretation.
///
/// Prevents stack overflow from deeply nested expressions. A depth of 128
/// is generous for any realistic Prisma query (typical depth is 2-10).
const MAX_INTERPRET_DEPTH: u32 = 128;

/// Executes compiled Prisma query plans against a database.
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute a compiled expression against a database adapter.
    ///
    /// The `queryable` can be a `SqlDriverAdapter` (for top-level execution)
    /// or a `Transaction` (for execution within a transaction context).
    pub async fn execute(expr: &Expression, queryable: &mut dyn SqlQueryable) -> Result<IValue, ExecutorError> {
        let scope = Scope::new();
        let result = Self::interpret(expr, queryable, &scope, 0).await?;
        Ok(result.value)
    }

    /// Interpret a single expression node.
    ///
    /// Uses `Box::pin` to enable recursion in async context.
    fn interpret<'a>(
        expr: &'a Expression,
        queryable: &'a mut dyn SqlQueryable,
        scope: &'a Scope,
        depth: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<IntermediateValue, ExecutorError>> + Send + 'a>>
    {
        Box::pin(async move {
            if depth > MAX_INTERPRET_DEPTH {
                return Err(ExecutorError::Validation {
                    message: format!("Maximum expression depth ({MAX_INTERPRET_DEPTH}) exceeded"),
                });
            }
            match expr {
                Expression::Value(pv) => {
                    let resolved = crate::value::resolve_prisma_value(pv, scope);
                    Ok(IntermediateValue::new(prisma_value_to_ivalue(&resolved)))
                }

                Expression::Seq(exprs) => {
                    let mut last = IntermediateValue::unit();
                    for e in exprs {
                        last = Self::interpret(e, queryable, scope, depth + 1).await?;
                    }
                    Ok(last)
                }

                Expression::Get { name } => scope
                    .get_intermediate(name)
                    .cloned()
                    .ok_or_else(|| ExecutorError::VariableNotFound(name.to_string())),

                Expression::Let { bindings, expr } => {
                    let mut child_scope = scope.child();
                    for binding in bindings {
                        let val = Self::interpret(&binding.expr, queryable, &child_scope, depth + 1).await?;
                        child_scope.set(binding.name.to_string(), val);
                    }
                    Self::interpret(expr, queryable, &child_scope, depth + 1).await
                }

                Expression::GetFirstNonEmpty { names } => {
                    let val = scope.get_first_non_empty(names);
                    Ok(IntermediateValue::new(val))
                }

                Expression::Query(db_query) => {
                    let max_bind = queryable.provider().max_bind_values();
                    let queries = render_query(db_query, scope, max_bind)?;

                    let mut all_records = Vec::new();
                    let mut last_insert_id = None;

                    for q in queries {
                        tracing::debug!(
                            target: "prisma:engine:query",
                            sql = %q.sql,
                            params = q.args.len(),
                            "query_raw"
                        );
                        let rs = queryable.query_raw(q).await?;
                        if last_insert_id.is_none() {
                            last_insert_id.clone_from(&rs.last_insert_id);
                        }
                        all_records.extend(result_set_to_records(&rs));
                    }

                    let mut iv = IntermediateValue::new(IValue::List(all_records));
                    iv.last_insert_id = last_insert_id;
                    Ok(iv)
                }

                Expression::Execute(db_query) => {
                    let max_bind = queryable.provider().max_bind_values();
                    let queries = render_query(db_query, scope, max_bind)?;

                    let mut total_affected = 0u64;
                    for q in queries {
                        tracing::debug!(
                            target: "prisma:engine:query",
                            sql = %q.sql,
                            params = q.args.len(),
                            "execute_raw"
                        );
                        total_affected =
                            total_affected
                                .checked_add(queryable.execute_raw(q).await?)
                                .ok_or_else(|| ExecutorError::Validation {
                                    message: "Affected row count overflow (exceeded u64::MAX)".into(),
                                })?;
                    }

                    let count = i64::try_from(total_affected).unwrap_or_else(|_| {
                        tracing::error!(total_affected, "Affected row count exceeds i64::MAX, clamping");
                        i64::MAX
                    });
                    Ok(IntermediateValue::new(IValue::Int(count)))
                }

                Expression::Sum(exprs) => {
                    let mut total: f64 = 0.0;
                    for e in exprs {
                        let val = Self::interpret(e, queryable, scope, depth + 1).await?;
                        match val.value.as_f64() {
                            Some(n) => total += n,
                            None => {
                                tracing::warn!(value = ?val.value, "Non-numeric value in Sum, treating as 0.0");
                            }
                        }
                    }
                    Ok(IntermediateValue::new(IValue::Float(total)))
                }

                Expression::Concat(exprs) => {
                    let mut result = Vec::new();
                    for e in exprs {
                        let val = Self::interpret(e, queryable, scope, depth + 1).await?;
                        result.extend(val.value.into_list());
                    }
                    Ok(IntermediateValue::new(IValue::List(result)))
                }

                Expression::Unique(inner) => {
                    let val = Self::interpret(inner, queryable, scope, depth + 1).await?;
                    match &val.value {
                        IValue::List(items) if items.len() > 1 => Err(ExecutorError::UniqueViolation(items.len())),
                        IValue::List(items) => {
                            let single = items.first().cloned().unwrap_or(IValue::Null);
                            Ok(IntermediateValue {
                                value: single,
                                last_insert_id: val.last_insert_id,
                            })
                        }
                        _ => Ok(val),
                    }
                }

                Expression::Required(inner) => {
                    let val = Self::interpret(inner, queryable, scope, depth + 1).await?;
                    match &val.value {
                        IValue::Null => Err(ExecutorError::RequiredNotFound {
                            context: "Required record not found".into(),
                        }),
                        IValue::List(items) if items.is_empty() => Err(ExecutorError::RequiredNotFound {
                            context: "Required record not found (empty list)".into(),
                        }),
                        _ => Ok(val),
                    }
                }

                Expression::Join {
                    parent,
                    children,
                    can_assume_strict_equality: _,
                } => {
                    let parent_val = Self::interpret(parent, queryable, scope, depth + 1).await?;
                    let mut parent_records = match parent_val.value {
                        IValue::List(items) => items,
                        other => vec![other],
                    };

                    for join_expr in children {
                        parent_records =
                            Self::perform_join(parent_records, join_expr, queryable, scope, depth + 1).await?;
                    }

                    Ok(IntermediateValue::new(IValue::List(parent_records)))
                }

                Expression::MapField { field, records } => {
                    let val = Self::interpret(records, queryable, scope, depth + 1).await?;
                    let mapped = map_field(&val.value, field);
                    Ok(IntermediateValue::new(mapped))
                }

                Expression::Transaction(inner) => {
                    if queryable.is_transaction() {
                        // Already inside a transaction -- execute directly without
                        // starting a new one (which would use a different connection).
                        Self::interpret(inner, queryable, scope, depth + 1).await
                    } else {
                        let mut tx = queryable.start_transaction(None).await?;
                        let result = Self::interpret(inner, tx.as_mut(), scope, depth + 1).await;
                        match result {
                            Ok(val) => {
                                tx.commit().await?;
                                Ok(val)
                            }
                            Err(e) => {
                                if let Err(rollback_err) = tx.rollback().await {
                                    tracing::error!(
                                        error = %rollback_err,
                                        "Failed to rollback transaction after error"
                                    );
                                }
                                Err(e)
                            }
                        }
                    }
                }

                Expression::DataMap { expr, structure, enums } => {
                    let val = Self::interpret(expr, queryable, scope, depth + 1).await?;
                    let mapped = apply_data_map(&val.value, structure, enums)?;
                    Ok(IntermediateValue {
                        value: mapped,
                        last_insert_id: val.last_insert_id,
                    })
                }

                Expression::Validate {
                    expr,
                    rules,
                    error_identifier,
                    context,
                } => {
                    let val = Self::interpret(expr, queryable, scope, depth + 1).await?;
                    validate_rules(&val.value, rules, error_identifier, context)?;
                    Ok(val)
                }

                Expression::If {
                    value,
                    rule,
                    then,
                    r#else,
                } => {
                    let val = Self::interpret(value, queryable, scope, depth + 1).await?;
                    if satisfies_rule(&val.value, rule) {
                        Self::interpret(then, queryable, scope, depth + 1).await
                    } else {
                        Self::interpret(r#else, queryable, scope, depth + 1).await
                    }
                }

                Expression::Unit => Ok(IntermediateValue::unit()),

                Expression::Diff { from, to, fields } => {
                    let from_val = Self::interpret(from, queryable, scope, depth + 1).await?;
                    let to_val = Self::interpret(to, queryable, scope, depth + 1).await?;
                    let diff = compute_diff(&from_val.value, &to_val.value, fields);
                    Ok(IntermediateValue::new(diff))
                }

                Expression::InitializeRecord { expr, fields } => {
                    let val = Self::interpret(expr, queryable, scope, depth + 1).await?;
                    let record = initialize_record(&val, fields, scope);
                    Ok(IntermediateValue::new(record))
                }

                Expression::MapRecord { expr, fields } => {
                    let val = Self::interpret(expr, queryable, scope, depth + 1).await?;
                    let record = apply_field_operations(&val.value, fields, scope);
                    Ok(IntermediateValue::new(record))
                }

                Expression::Process { expr, operations } => {
                    let val = Self::interpret(expr, queryable, scope, depth + 1).await?;
                    let processed = process_records(val.value, operations);
                    Ok(IntermediateValue::new(processed))
                }
            }
        })
    }

    async fn perform_join(
        parent_records: Vec<IValue>,
        join_expr: &JoinExpression,
        queryable: &mut dyn SqlQueryable,
        scope: &Scope,
        depth: u32,
    ) -> Result<Vec<IValue>, ExecutorError> {
        // Execute the child query
        let child_val = Self::interpret(&join_expr.child, queryable, scope, depth).await?;
        let child_records = child_val.value.into_list();

        // Build an index from child key -> child records
        let on_fields = &join_expr.on;
        let mut child_index: BTreeMap<String, Vec<IValue>> = BTreeMap::new();

        for child in &child_records {
            if let IValue::Record(rec) = child {
                let key = build_join_key(rec, on_fields, JoinSide::Child);
                child_index.entry(key).or_default().push(child.clone());
            }
        }

        // Attach children to parents
        let parent_field = &join_expr.parent_field;
        let is_unique = join_expr.is_relation_unique;

        let result: Vec<IValue> = parent_records
            .into_iter()
            .map(|parent| {
                if let IValue::Record(mut rec) = parent {
                    let key = build_join_key(&rec, on_fields, JoinSide::Parent);
                    let children = child_index.get(&key).cloned().unwrap_or_default();

                    if is_unique {
                        let val = children.into_iter().next().unwrap_or(IValue::Null);
                        rec.insert(parent_field.clone(), val);
                    } else {
                        rec.insert(parent_field.clone(), IValue::List(children));
                    }
                    IValue::Record(rec)
                } else {
                    parent
                }
            })
            .collect();

        Ok(result)
    }
}

#[derive(Clone, Copy)]
enum JoinSide {
    Parent,
    Child,
}

fn build_join_key(record: &BTreeMap<String, IValue>, on_fields: &[(String, String)], side: JoinSide) -> String {
    // Use length-prefixed fields with \0 separator to avoid collisions.
    // A value like "a|b" won't collide with two separate values "a" and "b"
    // because the length prefix disambiguates them.
    let mut key = String::new();
    for (parent_field, child_field) in on_fields {
        let field = match side {
            JoinSide::Parent => parent_field,
            JoinSide::Child => child_field,
        };
        let val_str = record.get(field).map(|v| format!("{v:?}")).unwrap_or_default();
        key.push_str(&val_str.len().to_string());
        key.push('\0');
        key.push_str(&val_str);
        key.push('\0');
    }
    key
}

/// Build a composite key from a record's fields using length-prefixed `\0`
/// separators to prevent key collisions.
fn build_composite_key(rec: &BTreeMap<String, IValue>, fields: &[impl AsRef<str>]) -> String {
    let mut key = String::new();
    for f in fields {
        let val_str = format!("{:?}", rec.get(f.as_ref()).unwrap_or(&IValue::Null));
        key.push_str(&val_str.len().to_string());
        key.push('\0');
        key.push_str(&val_str);
        key.push('\0');
    }
    key
}

fn map_field(value: &IValue, field: &str) -> IValue {
    match value {
        IValue::List(items) => IValue::List(items.iter().map(|item| map_field(item, field)).collect()),
        IValue::Record(rec) => rec.get(field).cloned().unwrap_or(IValue::Null),
        _ => IValue::Null,
    }
}

fn satisfies_rule(value: &IValue, rule: &DataRule) -> bool {
    match rule {
        DataRule::RowCountEq(expected) => value.row_count() == *expected,
        DataRule::RowCountNeq(expected) => value.row_count() != *expected,
        DataRule::AffectedRowCountEq(expected) => value.as_i64().map(|n| n as usize) == Some(*expected),
        DataRule::Never => false,
    }
}

fn validate_rules(
    value: &IValue,
    rules: &[DataRule],
    error_identifier: &str,
    _context: &serde_json::Value,
) -> Result<(), ExecutorError> {
    for rule in rules {
        if !satisfies_rule(value, rule) {
            return Err(ExecutorError::Validation {
                message: format!(
                    "Validation failed for '{}': rule {:?} not satisfied",
                    error_identifier, rule
                ),
            });
        }
    }
    Ok(())
}

fn compute_diff(from: &IValue, to: &IValue, fields: &[String]) -> IValue {
    let from_list = match from {
        IValue::List(v) => v.as_slice(),
        _ => return from.clone(),
    };
    let to_list = match to {
        IValue::List(v) => v.as_slice(),
        _ => return from.clone(),
    };

    // Build set of keys from `to`
    let to_keys: std::collections::HashSet<String> = to_list
        .iter()
        .filter_map(|item| {
            if let IValue::Record(rec) = item {
                Some(build_composite_key(rec, fields))
            } else {
                None
            }
        })
        .collect();

    let result: Vec<IValue> = from_list
        .iter()
        .filter(|item| {
            if let IValue::Record(rec) = item {
                let key = build_composite_key(rec, fields);
                !to_keys.contains(&key)
            } else {
                true
            }
        })
        .cloned()
        .collect();

    IValue::List(result)
}

fn initialize_record(val: &IntermediateValue, fields: &BTreeMap<String, FieldInitializer>, scope: &Scope) -> IValue {
    let mut record = match &val.value {
        IValue::Record(r) => r.clone(),
        _ => BTreeMap::new(),
    };

    for (name, init) in fields {
        match init {
            FieldInitializer::LastInsertId => {
                if let Some(ref id) = val.last_insert_id {
                    record.insert(name.clone(), IValue::String(id.clone()));
                }
            }
            FieldInitializer::Value(pv) => {
                let resolved = crate::value::resolve_prisma_value(pv, scope);
                record.insert(name.clone(), prisma_value_to_ivalue(&resolved));
            }
        }
    }

    IValue::Record(record)
}

fn apply_field_operations(value: &IValue, fields: &BTreeMap<String, FieldOperation>, scope: &Scope) -> IValue {
    let mut record = match value {
        IValue::Record(r) => r.clone(),
        _ => return value.clone(),
    };

    for (name, op) in fields {
        let current = record.get(name).cloned().unwrap_or(IValue::Null);
        let resolve = |pv: &prisma_ir::PrismaValue| {
            let resolved = crate::value::resolve_prisma_value(pv, scope);
            prisma_value_to_ivalue(&resolved)
        };
        let new_val = match op {
            FieldOperation::Set(pv) => resolve(pv),
            FieldOperation::Add(pv) => numeric_op(&current, &resolve(pv), |a, b| a + b),
            FieldOperation::Subtract(pv) => numeric_op(&current, &resolve(pv), |a, b| a - b),
            FieldOperation::Multiply(pv) => numeric_op(&current, &resolve(pv), |a, b| a * b),
            FieldOperation::Divide(pv) => {
                let divisor = resolve(pv);
                let divisor_f64 = divisor.as_f64().unwrap_or(0.0);
                if divisor_f64 == 0.0 {
                    tracing::error!(field = %name, "Division by zero in field operation, setting to Null");
                    IValue::Null
                } else {
                    numeric_op(&current, &divisor, |a, b| a / b)
                }
            }
        };
        record.insert(name.clone(), new_val);
    }

    IValue::Record(record)
}

fn numeric_op(a: &IValue, b: &IValue, op: impl Fn(f64, f64) -> f64) -> IValue {
    let a_num = a.as_f64().unwrap_or(0.0);
    let b_num = b.as_f64().unwrap_or(0.0);
    let result = op(a_num, b_num);
    // If both operands were integers, try to return an integer
    if matches!(a, IValue::Int(_)) && matches!(b, IValue::Int(_)) && result.fract() == 0.0 {
        if result >= i64::MIN as f64 && result <= i64::MAX as f64 {
            IValue::Int(result as i64)
        } else {
            tracing::error!(result, "Numeric operation result overflows i64, returning as float");
            IValue::Float(result)
        }
    } else {
        IValue::Float(result)
    }
}

fn process_records(value: IValue, operations: &InMemoryOps) -> IValue {
    process_records_inner(value, operations)
}

fn process_records_inner(value: IValue, ops: &InMemoryOps) -> IValue {
    let mut items = match value {
        IValue::List(v) => v,
        other => return other,
    };

    // Apply distinct
    if let Some(ref distinct_fields) = ops.distinct {
        let mut seen = std::collections::HashSet::new();
        items.retain(|item| {
            if let IValue::Record(rec) = item {
                let key = build_composite_key(rec, distinct_fields);
                seen.insert(key)
            } else {
                true
            }
        });
    }

    // Apply reverse
    if ops.reverse {
        items.reverse();
    }

    // Apply pagination
    const MAX_PAGINATION_LIMIT: usize = 100_000;
    if let Some(ref pagination) = ops.pagination {
        // Apply skip
        if let Some(skip) = pagination.skip {
            let skip = (skip.max(0) as usize).min(MAX_PAGINATION_LIMIT);
            if skip < items.len() {
                items = items.split_off(skip);
            } else {
                items.clear();
            }
        }

        // Apply take
        if let Some(take) = pagination.take {
            let take = (take.unsigned_abs() as usize).min(MAX_PAGINATION_LIMIT);
            if take < items.len() {
                items.truncate(take);
            }
        }
    }

    // Apply nested operations
    for (field_name, nested_ops) in &ops.nested {
        items = items
            .into_iter()
            .map(|item| {
                if let IValue::Record(mut rec) = item {
                    if let Some(nested_val) = rec.remove(field_name) {
                        let processed = process_records_inner(nested_val, nested_ops);
                        rec.insert(field_name.clone(), processed);
                    }
                    IValue::Record(rec)
                } else {
                    item
                }
            })
            .collect();
    }

    IValue::List(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn satisfies_rule_row_count_eq() {
        let list = IValue::List(vec![IValue::Int(1), IValue::Int(2)]);
        assert!(satisfies_rule(&list, &DataRule::RowCountEq(2)));
        assert!(!satisfies_rule(&list, &DataRule::RowCountEq(1)));
    }

    #[test]
    fn satisfies_rule_row_count_neq() {
        let list = IValue::List(vec![IValue::Int(1)]);
        assert!(satisfies_rule(&list, &DataRule::RowCountNeq(0)));
        assert!(!satisfies_rule(&list, &DataRule::RowCountNeq(1)));
    }

    #[test]
    fn satisfies_rule_affected_row_count() {
        let val = IValue::Int(3);
        assert!(satisfies_rule(&val, &DataRule::AffectedRowCountEq(3)));
        assert!(!satisfies_rule(&val, &DataRule::AffectedRowCountEq(2)));
    }

    #[test]
    fn satisfies_rule_never() {
        assert!(!satisfies_rule(&IValue::Null, &DataRule::Never));
    }

    #[test]
    fn map_field_from_record() {
        let mut rec = BTreeMap::new();
        rec.insert("id".to_string(), IValue::Int(42));
        rec.insert("name".to_string(), IValue::String("Alice".into()));

        let result = map_field(&IValue::Record(rec), "id");
        assert!(matches!(result, IValue::Int(42)));
    }

    #[test]
    fn map_field_from_list() {
        let rec1 = {
            let mut r = BTreeMap::new();
            r.insert("id".to_string(), IValue::Int(1));
            IValue::Record(r)
        };
        let rec2 = {
            let mut r = BTreeMap::new();
            r.insert("id".to_string(), IValue::Int(2));
            IValue::Record(r)
        };

        let result = map_field(&IValue::List(vec![rec1, rec2]), "id");
        match result {
            IValue::List(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], IValue::Int(1)));
                assert!(matches!(items[1], IValue::Int(2)));
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn compute_diff_basic() {
        let from = IValue::List(vec![
            make_record(&[("id", IValue::Int(1))]),
            make_record(&[("id", IValue::Int(2))]),
            make_record(&[("id", IValue::Int(3))]),
        ]);
        let to = IValue::List(vec![make_record(&[("id", IValue::Int(2))])]);

        let result = compute_diff(&from, &to, &["id".to_string()]);
        match result {
            IValue::List(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn process_records_distinct() {
        let items = IValue::List(vec![
            make_record(&[("name", IValue::String("Alice".into()))]),
            make_record(&[("name", IValue::String("Bob".into()))]),
            make_record(&[("name", IValue::String("Alice".into()))]),
        ]);
        let ops = InMemoryOps {
            distinct: Some(vec!["name".to_string()]),
            pagination: None,
            reverse: false,
            nested: BTreeMap::new(),
            linking_fields: None,
        };
        let result = process_records_inner(items, &ops);
        match result {
            IValue::List(items) => assert_eq!(items.len(), 2),
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn process_records_reverse() {
        let items = IValue::List(vec![IValue::Int(1), IValue::Int(2), IValue::Int(3)]);
        let ops = InMemoryOps {
            distinct: None,
            pagination: None,
            reverse: true,
            nested: BTreeMap::new(),
            linking_fields: None,
        };
        let result = process_records_inner(items, &ops);
        match result {
            IValue::List(items) => {
                assert!(matches!(items[0], IValue::Int(3)));
                assert!(matches!(items[2], IValue::Int(1)));
            }
            _ => panic!("Expected list"),
        }
    }

    #[test]
    fn numeric_operations() {
        let a = IValue::Int(10);
        let b = IValue::Int(3);
        assert!(matches!(numeric_op(&a, &b, |a, b| a + b), IValue::Int(13)));
        assert!(matches!(numeric_op(&a, &b, |a, b| a / b), IValue::Float(_)));
    }

    fn make_record(fields: &[(&str, IValue)]) -> IValue {
        let mut rec = BTreeMap::new();
        for (k, v) in fields {
            rec.insert(k.to_string(), v.clone());
        }
        IValue::Record(rec)
    }
}
