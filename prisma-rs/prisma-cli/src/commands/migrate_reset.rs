use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::SchemaFilter;

pub async fn run(schema_path: &str, force: bool, url: Option<&str>) -> Result<(), CliError> {
    if !force {
        return Err(CliError::Config(
            "Are you sure you want to reset the database? Use --force to confirm.".to_string(),
        ));
    }

    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content), Some(db_url), None)?;

    engine
        .reset(prisma_migrate::rpc_types::ResetInput {
            filter: SchemaFilter::default(),
        })
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    println!("Database reset successfully.");
    Ok(())
}
