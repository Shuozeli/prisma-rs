use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::SchemaFilter;

pub async fn run(schema_path: &str, url: Option<&str>) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content), Some(db_url), None)?;

    let migrations_dir = std::path::Path::new(schema_path)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("migrations");

    let migrations_list = super::load_migrations_from_disk(&migrations_dir)?;

    let input = prisma_migrate::rpc_types::ApplyMigrationsInput {
        migrations_list,
        filters: SchemaFilter::default(),
    };

    let output = engine
        .apply_migrations(input)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    if output.applied_migration_names.is_empty() {
        println!("No pending migrations to apply.");
    } else {
        println!("Applied {} migration(s):", output.applied_migration_names.len());
        for name in &output.applied_migration_names {
            println!("  - {name}");
        }
    }

    Ok(())
}
