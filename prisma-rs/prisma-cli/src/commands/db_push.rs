use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::{SchemaFilter, SchemaPushInput};

pub async fn run(schema_path: &str, url: Option<&str>, accept_data_loss: bool) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content.clone()), Some(db_url), None)?;

    let input = SchemaPushInput {
        schema: config::schemas_container(schema_path, &content),
        force: accept_data_loss,
        filters: SchemaFilter {
            external_tables: vec![],
            external_enums: vec![],
        },
    };

    let output = engine
        .schema_push(input)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    if !output.unexecutable.is_empty() {
        eprintln!("Unexecutable steps:");
        for step in &output.unexecutable {
            eprintln!("  - {step}");
        }
        if !accept_data_loss {
            return Err(CliError::Config(
                "Some steps cannot be executed. Use --accept-data-loss to force.".to_string(),
            ));
        }
    }

    if !output.warnings.is_empty() {
        eprintln!("Warnings:");
        for warning in &output.warnings {
            eprintln!("  - {warning}");
        }
    }

    println!(
        "Database schema pushed successfully ({} steps executed).",
        output.executed_steps
    );
    Ok(())
}
