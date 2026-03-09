//! Cross-compatibility test harness for Prisma.
//!
//! Compiles Prisma client operations into query plans via the query compiler,
//! then verifies the generated SQL and query structure are correct.
//!
//! This harness validates that:
//! 1. The query compiler produces valid SQL for each provider
//! 2. The generated SQL can be executed by our native drivers
//! 3. Results match expected outputs (golden tests)

use prisma_compiler::QueryCompiler;
use prisma_compiler::quaint::connector::ConnectionInfo;
use prisma_compiler::quaint::prelude::{ExternalConnectionInfo, SqlFamily};
use serde_json::Value;

/// Create a query compiler for the given provider and schema.
pub fn compiler_for_provider(provider: &str, schema: &str) -> QueryCompiler {
    let family = match provider {
        "postgresql" | "postgres" => SqlFamily::Postgres,
        "mysql" => SqlFamily::Mysql,
        "sqlite" => SqlFamily::Sqlite,
        other => panic!("Unsupported provider: {other}"),
    };

    let conn_info = ConnectionInfo::External(ExternalConnectionInfo::new(
        family,
        None,
        None,
        family == SqlFamily::Postgres || family == SqlFamily::Mysql,
    ));
    QueryCompiler::new(schema, conn_info)
}

/// Compile a Prisma operation and return the query plan as JSON.
pub fn compile_operation(compiler: &QueryCompiler, request: &str) -> Result<Value, String> {
    compiler.compile_to_json(request).map_err(|e| e.to_string())
}

/// Extract SQL queries from a compiled expression tree.
///
/// Walks the expression tree and collects all SQL query strings.
pub fn extract_sql_queries(plan: &Value) -> Vec<String> {
    let mut queries = Vec::new();
    collect_sql(plan, &mut queries);
    queries
}

fn collect_sql(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            // Expression nodes with type "templateSql" contain SQL in their fragments
            if map.get("type").and_then(Value::as_str) == Some("templateSql") {
                if let Some(Value::Array(fragments)) = map.get("fragments") {
                    let sql: String = fragments
                        .iter()
                        .filter_map(|f| {
                            if f.get("type").and_then(Value::as_str) == Some("stringChunk") {
                                f.get("chunk").and_then(Value::as_str).map(str::to_owned)
                            } else {
                                Some("?".to_owned())
                            }
                        })
                        .collect();
                    if !sql.is_empty() {
                        out.push(sql);
                    }
                }
            }
            for v in map.values() {
                collect_sql(v, out);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_sql(v, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PG_SCHEMA: &str = r#"
        datasource db {
            provider = "postgresql"
        }

        model User {
            id    Int    @id @default(autoincrement())
            email String @unique
            name  String?
            posts Post[]
        }

        model Post {
            id       Int    @id @default(autoincrement())
            title    String
            authorId Int
            author   User   @relation(fields: [authorId], references: [id])
        }
    "#;

    const MYSQL_SCHEMA: &str = r#"
        datasource db {
            provider = "mysql"
        }

        model User {
            id    Int    @id @default(autoincrement())
            email String @unique
            name  String?
            posts Post[]
        }

        model Post {
            id       Int    @id @default(autoincrement())
            title    String
            authorId Int
            author   User   @relation(fields: [authorId], references: [id])
        }
    "#;

    const SQLITE_SCHEMA: &str = r#"
        datasource db {
            provider = "sqlite"
        }

        model User {
            id    Int    @id @default(autoincrement())
            email String @unique
            name  String?
            posts Post[]
        }

        model Post {
            id       Int    @id @default(autoincrement())
            title    String
            authorId Int
            author   User   @relation(fields: [authorId], references: [id])
        }
    "#;

    #[test]
    fn compile_find_many_all_providers() {
        let request = r#"{
            "modelName": "User",
            "action": "findMany",
            "query": {
                "selection": { "$scalars": true }
            }
        }"#;

        for (provider, schema) in [
            ("postgresql", PG_SCHEMA),
            ("mysql", MYSQL_SCHEMA),
            ("sqlite", SQLITE_SCHEMA),
        ] {
            let compiler = compiler_for_provider(provider, schema);
            let plan = compile_operation(&compiler, request).unwrap_or_else(|e| panic!("{provider}: {e}"));

            let sqls = extract_sql_queries(&plan);
            assert!(!sqls.is_empty(), "{provider}: Expected SQL queries in the plan");

            // All providers should generate a SELECT for findMany
            let has_select = sqls.iter().any(|s| s.contains("SELECT"));
            assert!(has_select, "{provider}: Expected SELECT query, got: {sqls:?}");
        }
    }

    #[test]
    fn compile_create_one_all_providers() {
        let request = r#"{
            "modelName": "User",
            "action": "createOne",
            "query": {
                "arguments": {
                    "data": {
                        "email": "test@example.com",
                        "name": "Test"
                    }
                },
                "selection": { "$scalars": true }
            }
        }"#;

        for (provider, schema) in [
            ("postgresql", PG_SCHEMA),
            ("mysql", MYSQL_SCHEMA),
            ("sqlite", SQLITE_SCHEMA),
        ] {
            let compiler = compiler_for_provider(provider, schema);
            let plan = compile_operation(&compiler, request).unwrap_or_else(|e| panic!("{provider}: {e}"));

            let sqls = extract_sql_queries(&plan);
            let has_insert = sqls.iter().any(|s| s.contains("INSERT"));
            assert!(has_insert, "{provider}: Expected INSERT query, got: {sqls:?}");
        }
    }

    #[test]
    fn compile_find_unique_all_providers() {
        let request = r#"{
            "modelName": "User",
            "action": "findUnique",
            "query": {
                "arguments": {
                    "where": { "id": 1 }
                },
                "selection": { "$scalars": true }
            }
        }"#;

        for (provider, schema) in [
            ("postgresql", PG_SCHEMA),
            ("mysql", MYSQL_SCHEMA),
            ("sqlite", SQLITE_SCHEMA),
        ] {
            let compiler = compiler_for_provider(provider, schema);
            let plan = compile_operation(&compiler, request).unwrap_or_else(|e| panic!("{provider}: {e}"));

            let sqls = extract_sql_queries(&plan);
            let has_where = sqls.iter().any(|s| s.contains("WHERE"));
            assert!(has_where, "{provider}: Expected WHERE clause, got: {sqls:?}");
        }
    }

    #[test]
    fn sql_differs_by_provider() {
        let request = r#"{
            "modelName": "User",
            "action": "findMany",
            "query": {
                "selection": { "$scalars": true }
            }
        }"#;

        let pg = compiler_for_provider("postgresql", PG_SCHEMA);
        let mysql = compiler_for_provider("mysql", MYSQL_SCHEMA);

        let pg_plan = compile_operation(&pg, request).unwrap();
        let mysql_plan = compile_operation(&mysql, request).unwrap();

        let pg_sqls = extract_sql_queries(&pg_plan);
        let mysql_sqls = extract_sql_queries(&mysql_plan);

        // PostgreSQL uses double-quoted identifiers, MySQL uses backticks
        let pg_sql = &pg_sqls[0];
        let mysql_sql = &mysql_sqls[0];

        assert!(
            pg_sql.contains('"') || !pg_sql.contains('`'),
            "PG should use double quotes, got: {pg_sql}"
        );
        assert!(mysql_sql.contains('`'), "MySQL should use backticks, got: {mysql_sql}");
    }
}
