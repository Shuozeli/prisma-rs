use crate::SchemaError;

/// Validate a Prisma schema. Returns `Ok(())` if valid.
///
/// Input: JSON `{ "prismaSchema": [["file.prisma", "datasource db { ... }"]] }`
pub fn validate(schema_files: &str) -> Result<(), SchemaError> {
    prisma_fmt::validate(schema_files.to_string()).map_err(SchemaError::Validation)
}

/// Format a Prisma schema.
///
/// `schema_input` is a JSON `SchemaFileInput` (single string or `[["file.prisma", "..."]]`).
/// `params` is a JSON `DocumentFormattingParams` (e.g. `{"options":{"tabSize":2}}`).
pub fn format(schema_input: &str, params: &str) -> String {
    prisma_fmt::format(schema_input.to_string(), params)
}

/// Generate the DMMF (Data Model Meta Format) from a Prisma schema.
///
/// Input: JSON `{ "prismaSchema": [["file.prisma", "..."]], "noColor": true }`
/// Returns: JSON string of the DMMF document.
pub fn get_dmmf(params: &str) -> Result<String, SchemaError> {
    prisma_fmt::get_dmmf(params.to_string()).map_err(SchemaError::Dmmf)
}

/// Extract configuration (datasources, generators) from a Prisma schema.
///
/// Input: JSON `{ "prismaSchema": [["file.prisma", "..."]] }`
/// Returns: JSON string of the configuration.
pub fn get_config(params: &str) -> String {
    prisma_fmt::get_config(params.to_string())
}

/// Lint a Prisma schema and return diagnostics.
///
/// `schema_input` is a JSON `SchemaFileInput`.
/// Returns: JSON array of lint diagnostics.
pub fn lint(schema_input: &str) -> String {
    prisma_fmt::lint(schema_input.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema_input(schema: &str) -> String {
        serde_json::json!({
            "prismaSchema": [["schema.prisma", schema]]
        })
        .to_string()
    }

    #[test]
    fn validate_valid_schema() {
        let schema = r#"
            datasource db {
                provider = "postgresql"
            }

            model User {
                id    Int    @id @default(autoincrement())
                email String @unique
                name  String?
            }
        "#;
        let input = make_schema_input(schema);
        assert!(validate(&input).is_ok());
    }

    #[test]
    fn validate_invalid_schema_syntax_error() {
        let schema = r#"
            datasource db {
                provider = "postgresql"
            }

            mode User {
                id Int @id
            }
        "#;
        let input = make_schema_input(schema);
        let err = validate(&input);
        assert!(err.is_err(), "Expected validation error for syntax error");
    }

    #[test]
    fn format_schema() {
        let schema = "datasource db {\nprovider=\"postgresql\"\n}\nmodel User {\nid Int @id\n}";
        let input = serde_json::json!(schema).to_string();
        let params = serde_json::json!({
            "textDocument": { "uri": "file:///schema.prisma" },
            "options": { "tabSize": 2, "insertSpaces": true }
        })
        .to_string();
        let formatted = format(&input, &params);
        // format returns the formatted schema as a JSON string for Single input
        let result: String = serde_json::from_str(&formatted).unwrap_or(formatted.clone());
        assert!(
            result.contains("provider = \"postgresql\""),
            "Formatted result: {result}"
        );
    }

    #[test]
    fn get_dmmf_valid_schema() {
        let schema = r#"
            datasource db {
                provider = "postgresql"
            }

            model User {
                id    Int    @id @default(autoincrement())
                email String @unique
                name  String?
            }
        "#;
        let params = serde_json::json!({
            "prismaSchema": [["schema.prisma", schema]],
            "noColor": true
        })
        .to_string();
        let dmmf = get_dmmf(&params).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&dmmf).unwrap();
        assert!(parsed["datamodel"]["models"].is_array());

        let models = parsed["datamodel"]["models"].as_array().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0]["name"], "User");
    }

    #[test]
    fn lint_schema() {
        let schema = r#"
            datasource db {
                provider = "postgresql"
            }

            model User {
                id    Int    @id @default(autoincrement())
                name  String
            }
        "#;
        let input = serde_json::to_string(schema).unwrap();
        let result = lint(&input);
        // Lint returns a JSON array of diagnostics
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
    }
}
