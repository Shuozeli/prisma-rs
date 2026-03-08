use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::{CreateMigrationInput, SchemaFilter};

pub async fn run(schema_path: &str, name: &str, create_only: bool, url: Option<&str>) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let db_url = config::resolve_url(url)?;

    let engine = prisma_migrate::create_engine(Some(content.clone()), Some(db_url), None)?;

    let migrations_dir = std::path::Path::new(schema_path)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("migrations");
    std::fs::create_dir_all(&migrations_dir)?;

    // First run dev diagnostic to check if we need to reset
    let diag_input = prisma_migrate::rpc_types::DevDiagnosticInput {
        migrations_list: super::load_migrations_from_disk(&migrations_dir)?,
        filters: SchemaFilter::default(),
    };

    let diag = engine
        .dev_diagnostic(diag_input)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;
    match &diag.action {
        prisma_migrate::rpc_types::DevAction::Reset(reset) => {
            println!("Database needs reset: {}", reset.reason);
            println!("Resetting database...");
            engine
                .reset(prisma_migrate::rpc_types::ResetInput {
                    filter: SchemaFilter::default(),
                })
                .await
                .map_err(prisma_migrate::MigrateError::from)?;
            println!("Database reset.");
        }
        prisma_migrate::rpc_types::DevAction::CreateMigration => {}
    }

    // Create the migration
    let input = CreateMigrationInput {
        draft: create_only,
        migration_name: name.to_string(),
        schema: config::schemas_container(schema_path, &content),
        migrations_list: super::load_migrations_from_disk(&migrations_dir)?,
        filters: SchemaFilter::default(),
    };

    let output = engine
        .create_migration(input)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    if output.migration_script.is_some() {
        let dir_name = &output.generated_migration_name;
        // Write the migration file to disk
        let migration_dir = migrations_dir.join(dir_name);
        std::fs::create_dir_all(&migration_dir)?;
        if let Some(script) = &output.migration_script {
            std::fs::write(migration_dir.join("migration.sql"), script)?;
        }
        println!("Created migration: {dir_name}");

        if !create_only {
            // Apply all pending migrations
            let updated_list = super::load_migrations_from_disk(&migrations_dir)?;
            let apply_input = prisma_migrate::rpc_types::ApplyMigrationsInput {
                migrations_list: updated_list,
                filters: SchemaFilter::default(),
            };
            let apply_output = engine
                .apply_migrations(apply_input)
                .await
                .map_err(prisma_migrate::MigrateError::from)?;
            println!("Applied {} migration(s).", apply_output.applied_migration_names.len());
        }
    } else {
        println!("No migration needed - schema is up to date.");
    }

    Ok(())
}
