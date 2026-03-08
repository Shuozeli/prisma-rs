use std::path::Path;

use crate::error::CliError;
use prisma_migrate::rpc_types::{SchemaContainer, SchemasContainer};

/// Validate that a file path does not escape the current working directory.
///
/// Prevents path traversal attacks (e.g. `../../etc/passwd`) by resolving the
/// canonical path and checking it starts with the current working directory.
pub fn validate_path_within_cwd(path: &str) -> Result<std::path::PathBuf, CliError> {
    let cwd =
        std::env::current_dir().map_err(|e| CliError::Config(format!("Failed to determine current directory: {e}")))?;

    // Resolve relative to cwd for consistency
    let target = if Path::new(path).is_absolute() {
        std::path::PathBuf::from(path)
    } else {
        cwd.join(path)
    };

    // For existing files, canonicalize to resolve symlinks
    // For new files, canonicalize the parent directory
    let canonical = if target.exists() {
        target
            .canonicalize()
            .map_err(|e| CliError::Config(format!("Failed to resolve path '{}': {e}", target.display())))?
    } else {
        let parent = target.parent().unwrap_or(Path::new("."));
        let parent_canonical = if parent.exists() {
            parent.canonicalize().map_err(|e| {
                CliError::Config(format!(
                    "Failed to resolve parent directory '{}': {e}",
                    parent.display()
                ))
            })?
        } else {
            return Err(CliError::Config(format!(
                "Parent directory does not exist: {}",
                parent.display()
            )));
        };
        parent_canonical.join(target.file_name().unwrap_or_default())
    };

    let cwd_canonical = cwd
        .canonicalize()
        .map_err(|e| CliError::Config(format!("Failed to canonicalize cwd: {e}")))?;

    if !canonical.starts_with(&cwd_canonical) {
        return Err(CliError::Config(format!(
            "Path '{}' escapes the project directory. Resolved to: {}",
            path,
            canonical.display()
        )));
    }

    Ok(canonical)
}

/// Load a Prisma schema file from disk.
pub fn load_schema(path: &str) -> Result<String, CliError> {
    validate_path_within_cwd(path)?;
    std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CliError::Config(format!("Schema file not found: {path}"))
        } else {
            CliError::Io(e)
        }
    })
}

/// Build the JSON input that prisma-fmt functions expect:
/// `{ "prismaSchema": [["file.prisma", "content"]] }`
pub fn schema_to_validate_input(path: &str, content: &str) -> String {
    serde_json::json!({
        "prismaSchema": [[path, content]]
    })
    .to_string()
}

/// Build a SchemasContainer from a schema path and content.
pub fn schemas_container(path: &str, content: &str) -> SchemasContainer {
    SchemasContainer {
        files: vec![SchemaContainer {
            path: path.to_string(),
            content: content.to_string(),
        }],
    }
}

/// Resolve the database URL: CLI flag overrides, then env var DATABASE_URL.
///
/// Validates that the URL uses a known database scheme.
pub fn resolve_url(cli_url: Option<&str>) -> Result<String, CliError> {
    let url = if let Some(url) = cli_url {
        url.to_string()
    } else {
        std::env::var("DATABASE_URL").map_err(|_| {
            CliError::Config(
                "No database URL provided. Use --url or set DATABASE_URL environment variable.".to_string(),
            )
        })?
    };

    validate_database_url_scheme(&url)?;
    Ok(url)
}

/// Validate that a database URL uses a known scheme.
fn validate_database_url_scheme(url: &str) -> Result<(), CliError> {
    const ALLOWED_SCHEMES: &[&str] = &[
        "postgresql://",
        "postgres://",
        "mysql://",
        "file:",
        "sqlite:",
        "sqlserver://",
        "mongodb://",
        "mongodb+srv://",
    ];
    if !ALLOWED_SCHEMES.iter().any(|s| url.starts_with(s)) {
        return Err(CliError::Config(format!(
            "Unrecognized database URL scheme. Expected one of: postgresql://, postgres://, mysql://, sqlite:, sqlserver://, mongodb://. Got: {}",
            url.split("://").next().unwrap_or("<empty>")
        )));
    }
    Ok(())
}
