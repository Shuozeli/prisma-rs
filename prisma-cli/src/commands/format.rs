use crate::config;
use crate::error::CliError;

pub fn run(schema_path: &str, check: bool) -> Result<(), CliError> {
    config::validate_path_within_cwd(schema_path)?;
    let content = config::load_schema(schema_path)?;

    let schema_input = serde_json::to_string(&content)?;
    let params = serde_json::json!({
        "textDocument": { "uri": format!("file:///{schema_path}") },
        "options": { "tabSize": 2, "insertSpaces": true }
    })
    .to_string();

    let formatted_json = prisma_schema::format(&schema_input, &params);
    let formatted: String = serde_json::from_str(&formatted_json)
        .map_err(|e| CliError::Config(format!("Failed to parse formatted schema output: {e}")))?;

    if check {
        if formatted.trim() != content.trim() {
            eprintln!("The schema at {schema_path} is not formatted.");
            return Err(CliError::Config(
                "Schema is not formatted. Run `prisma format` to fix.".to_string(),
            ));
        }
        println!("The schema at {schema_path} is already formatted.");
    } else {
        std::fs::write(schema_path, &formatted)?;
        println!("Formatted {schema_path}.");
    }

    Ok(())
}
