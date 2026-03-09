//! Render a `DbQuery` template into a concrete `SqlQuery` for execution.

use prisma_driver_core::{ArgScalarType, ArgType, Arity, QueryValue, SqlQuery};
use prisma_ir::{DbQuery, Fragment, PlaceholderFormat, PrismaValue};

use crate::error::ExecutorError;
use crate::scope::Scope;
use crate::value::{prisma_value_to_query_value, resolve_prisma_value};

/// Render a DbQuery into one or more concrete SqlQueries ready for execution.
///
/// Handles placeholder expansion, parameter tuple expansion, and chunking
/// for providers with bind-parameter limits.
pub fn render_query(
    db_query: &DbQuery,
    scope: &Scope,
    max_bind_values: Option<u32>,
) -> Result<Vec<SqlQuery>, ExecutorError> {
    match db_query {
        DbQuery::RawSql { sql, args, arg_types } => {
            let resolved_args: Vec<QueryValue> = args
                .iter()
                .map(|a| {
                    let resolved = resolve_prisma_value(a, scope);
                    prisma_value_to_query_value(&resolved)
                })
                .collect();

            let converted_types: Vec<ArgType> = arg_types.iter().map(convert_arg_type).collect();

            Ok(vec![SqlQuery {
                sql: sql.clone(),
                args: resolved_args,
                arg_types: converted_types,
            }])
        }
        DbQuery::TemplateSql {
            fragments,
            args,
            arg_types,
            placeholder_format,
            chunkable,
        } => {
            let resolved_args: Vec<PrismaValue> = args.iter().map(|a| resolve_prisma_value(a, scope)).collect();

            // Build the concrete queries from fragments
            let queries = render_template(
                fragments,
                &resolved_args,
                arg_types,
                placeholder_format,
                *chunkable,
                max_bind_values,
            )?;

            Ok(queries)
        }
    }
}

fn render_template(
    fragments: &[Fragment],
    args: &[PrismaValue],
    arg_types: &[prisma_ir::DynamicArgType],
    placeholder_format: &PlaceholderFormat,
    _chunkable: bool,
    _max_bind_values: Option<u32>,
) -> Result<Vec<SqlQuery>, ExecutorError> {
    // For now, render as a single query (chunking can be added later for large IN clauses)
    let mut sql = String::new();
    let mut query_args: Vec<QueryValue> = Vec::new();
    let mut query_arg_types: Vec<ArgType> = Vec::new();
    let mut param_idx = 0;
    let mut placeholder_num: usize = 1;

    for fragment in fragments {
        match fragment {
            Fragment::StringChunk { chunk } => {
                sql.push_str(chunk);
            }
            Fragment::Parameter => {
                write_placeholder(&mut sql, placeholder_format, &mut placeholder_num);
                if param_idx < args.len() {
                    query_args.push(prisma_value_to_query_value(&args[param_idx]));
                    if param_idx < arg_types.len() {
                        query_arg_types.push(convert_dynamic_arg_type(&arg_types[param_idx]));
                    }
                }
                param_idx += 1;
            }
            Fragment::ParameterTuple {
                item_prefix,
                item_separator,
                item_suffix,
            } => {
                if param_idx < args.len() {
                    // Normalize to a list: if the resolved value is a single value
                    // (e.g. from Unique extraction), wrap it in a single-element list.
                    let items: Vec<&PrismaValue> = if let PrismaValue::List(items) = &args[param_idx] {
                        items.iter().collect()
                    } else {
                        vec![&args[param_idx]]
                    };
                    sql.push('(');
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(item_separator);
                        }
                        sql.push_str(item_prefix);
                        write_placeholder(&mut sql, placeholder_format, &mut placeholder_num);
                        sql.push_str(item_suffix);
                        query_args.push(prisma_value_to_query_value(item));
                        if param_idx < arg_types.len() {
                            query_arg_types.push(convert_dynamic_arg_type(&arg_types[param_idx]));
                        }
                    }
                    sql.push(')');
                }
                param_idx += 1;
            }
            Fragment::ParameterTupleList {
                item_prefix,
                item_separator,
                item_suffix,
                group_separator,
            } => {
                if param_idx < args.len() {
                    if let PrismaValue::List(groups) = &args[param_idx] {
                        // For tuple lists, the arg type at param_idx describes the tuple.
                        // Extract element types if available, falling back to Unknown.
                        let element_types: Option<Vec<ArgType>> = if param_idx < arg_types.len() {
                            match &arg_types[param_idx] {
                                prisma_ir::DynamicArgType::Tuple { elements } => {
                                    Some(elements.iter().map(convert_arg_type).collect())
                                }
                                prisma_ir::DynamicArgType::Single { r#type } => Some(vec![convert_arg_type(r#type)]),
                            }
                        } else {
                            None
                        };

                        for (gi, group) in groups.iter().enumerate() {
                            if gi > 0 {
                                sql.push_str(group_separator);
                            }
                            if let PrismaValue::List(items) = group {
                                sql.push('(');
                                for (i, item) in items.iter().enumerate() {
                                    if i > 0 {
                                        sql.push_str(item_separator);
                                    }
                                    sql.push_str(item_prefix);
                                    write_placeholder(&mut sql, placeholder_format, &mut placeholder_num);
                                    sql.push_str(item_suffix);
                                    query_args.push(prisma_value_to_query_value(item));
                                    if let Some(ref types) = element_types {
                                        // Use the corresponding element type, or the last one
                                        let type_idx = i.min(types.len().saturating_sub(1));
                                        if let Some(t) = types.get(type_idx) {
                                            query_arg_types.push(t.clone());
                                        }
                                    }
                                }
                                sql.push(')');
                            }
                        }
                    }
                }
                param_idx += 1;
            }
        }
    }

    Ok(vec![SqlQuery {
        sql,
        args: query_args,
        arg_types: query_arg_types,
    }])
}

fn write_placeholder(sql: &mut String, format: &PlaceholderFormat, number: &mut usize) {
    sql.push_str(&format.prefix);
    if format.has_numbering {
        sql.push_str(&number.to_string());
        *number += 1;
    }
}

fn convert_arg_type(t: &prisma_ir::ArgType) -> ArgType {
    ArgType {
        scalar_type: convert_scalar_type(&t.scalar_type),
        db_type: t.db_type.clone(),
        arity: match t.arity {
            prisma_ir::QueryArity::Scalar => Arity::Scalar,
            prisma_ir::QueryArity::List => Arity::List,
        },
    }
}

fn convert_dynamic_arg_type(t: &prisma_ir::DynamicArgType) -> ArgType {
    match t {
        prisma_ir::DynamicArgType::Single { r#type } => convert_arg_type(r#type),
        prisma_ir::DynamicArgType::Tuple { elements } => {
            // For tuples, use the first element type as representative
            elements.first().map(convert_arg_type).unwrap_or(ArgType {
                scalar_type: ArgScalarType::Unknown,
                db_type: None,
                arity: Arity::Scalar,
            })
        }
    }
}

fn convert_scalar_type(t: &prisma_ir::ArgScalarType) -> ArgScalarType {
    match t {
        prisma_ir::ArgScalarType::String => ArgScalarType::String,
        prisma_ir::ArgScalarType::Int => ArgScalarType::Int,
        prisma_ir::ArgScalarType::BigInt => ArgScalarType::BigInt,
        prisma_ir::ArgScalarType::Float => ArgScalarType::Float,
        prisma_ir::ArgScalarType::Decimal => ArgScalarType::Decimal,
        prisma_ir::ArgScalarType::Boolean => ArgScalarType::Boolean,
        prisma_ir::ArgScalarType::Enum => ArgScalarType::Enum,
        prisma_ir::ArgScalarType::Uuid => ArgScalarType::Uuid,
        prisma_ir::ArgScalarType::Json => ArgScalarType::Json,
        prisma_ir::ArgScalarType::DateTime => ArgScalarType::DateTime,
        prisma_ir::ArgScalarType::Bytes => ArgScalarType::Bytes,
        prisma_ir::ArgScalarType::Unknown => ArgScalarType::Unknown,
    }
}
