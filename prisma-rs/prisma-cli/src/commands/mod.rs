pub mod db_execute;
pub mod db_pull;
pub mod db_push;
pub mod format;
pub mod generate;
pub mod migrate_deploy;
pub mod migrate_dev;
pub mod migrate_diff;
pub mod migrate_reset;
pub mod migrate_resolve;
pub mod validate;

use prisma_migrate::rpc_types::{JsResult, MigrationDirectory, MigrationFile, MigrationList, MigrationLockfile};

/// Validate that a migration directory name matches the expected format.
///
/// Prisma migration directories are named `<timestamp>_<name>` where timestamp
/// is a 14-digit number and name is alphanumeric with underscores.
/// Rejects names containing path separators, `..`, or other suspicious characters.
fn is_valid_migration_dir_name(name: &str) -> bool {
    if name.is_empty() || name.contains("..") || name.contains('/') || name.contains('\\') {
        return false;
    }
    // Must match: digits followed by underscore followed by alphanumeric/underscore
    // e.g. "20240301120000_initial_migration"
    name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

/// Load migrations from the filesystem into a MigrationList.
fn load_migrations_from_disk(migrations_dir: &std::path::Path) -> Result<MigrationList, crate::error::CliError> {
    let base_dir = migrations_dir.to_string_lossy().to_string();

    // Read lockfile if it exists
    let lockfile_path = migrations_dir.join("migration_lock.toml");
    let lockfile = MigrationLockfile {
        path: "./migration_lock.toml".to_string(),
        content: std::fs::read_to_string(&lockfile_path).ok(),
    };

    // Read migration directories (sorted by name = chronological order)
    let mut migration_directories = Vec::new();
    if migrations_dir.exists() {
        let mut entries: Vec<_> = std::fs::read_dir(migrations_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let path = e.path();
                // Only real directories (not symlinks to directories)
                path.is_dir() && path.read_link().is_err()
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let dir_name = entry.file_name().to_string_lossy().to_string();
            if !is_valid_migration_dir_name(&dir_name) {
                eprintln!("[prisma-cli] WARNING: Skipping migration directory with invalid name: {dir_name}");
                continue;
            }
            let migration_file_path = entry.path().join("migration.sql");
            let content = match std::fs::read_to_string(&migration_file_path) {
                Ok(c) => JsResult::Ok(c),
                Err(e) => JsResult::Err(e.to_string()),
            };

            migration_directories.push(MigrationDirectory {
                path: dir_name,
                migration_file: MigrationFile {
                    path: "migration.sql".to_string(),
                    content,
                },
            });
        }
    }

    Ok(MigrationList {
        base_dir,
        lockfile,
        shadow_db_init_script: String::new(),
        migration_directories,
    })
}
