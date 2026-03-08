use crate::config;
use crate::error::CliError;

pub async fn run(
    schema_path: &str,
    applied: Option<&str>,
    rolled_back: Option<&str>,
    url: Option<&str>,
) -> Result<(), CliError> {
    if applied.is_none() && rolled_back.is_none() {
        return Err(CliError::Config("Provide --applied or --rolled-back.".to_string()));
    }

    // Validate migration names to prevent injection
    for name in [applied, rolled_back].into_iter().flatten() {
        if !super::is_valid_migration_dir_name(name) {
            return Err(CliError::Config(format!(
                "Invalid migration name: '{}'. Must contain only alphanumeric characters, underscores, and hyphens.",
                name
            )));
        }
    }

    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content), Some(db_url), None)?;

    let migrations_dir = std::path::Path::new(schema_path)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("migrations");

    if let Some(migration_name) = applied {
        let migrations_list = super::load_migrations_from_disk(&migrations_dir)?;
        let input = prisma_migrate::rpc_types::MarkMigrationAppliedInput {
            migration_name: migration_name.to_string(),
            migrations_list,
        };
        engine
            .mark_migration_applied(input)
            .await
            .map_err(prisma_migrate::MigrateError::from)?;
        println!("Marked migration '{migration_name}' as applied.");
    }

    if let Some(migration_name) = rolled_back {
        let input = prisma_migrate::rpc_types::MarkMigrationRolledBackInput {
            migration_name: migration_name.to_string(),
        };
        engine
            .mark_migration_rolled_back(input)
            .await
            .map_err(prisma_migrate::MigrateError::from)?;
        println!("Marked migration '{migration_name}' as rolled back.");
    }

    Ok(())
}
