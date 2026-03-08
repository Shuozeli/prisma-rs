use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::IntrospectParams;

pub async fn run(schema_path: &str, url: Option<&str>) -> Result<(), CliError> {
    config::validate_path_within_cwd(schema_path)?;
    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content.clone()), Some(db_url), None)?;

    let schema_dir = std::path::Path::new(schema_path)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_string_lossy()
        .to_string();

    let input = IntrospectParams {
        schema: config::schemas_container(schema_path, &content),
        base_directory_path: schema_dir,
        force: false,
        composite_type_depth: -1,
        namespaces: None,
    };

    let result = engine
        .introspect(input)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    // Write the introspected schema back
    if let Some(file) = result.schema.files.first() {
        if file.content.trim().is_empty() {
            return Err(CliError::Config(
                "Introspection returned an empty schema. Verify the database URL and that the database contains tables.".to_string(),
            ));
        }
        std::fs::write(schema_path, &file.content)?;
        println!("Introspected schema written to {schema_path}.");
    } else {
        return Err(CliError::Config(
            "Introspection returned no schema files. Verify the database URL and that the database contains tables."
                .to_string(),
        ));
    }

    if let Some(warnings) = &result.warnings {
        if !warnings.is_empty() {
            eprintln!("Warnings:");
            eprintln!("  {warnings}");
        }
    }

    Ok(())
}
