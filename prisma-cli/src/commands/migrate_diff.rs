use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::{DiffParams, DiffTarget, SchemaContainer, SchemaFilter, SchemasContainer};

pub async fn run(schema_path: &str, from: &str, to: &str, script: bool) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let url = config::resolve_url(None).ok();

    let engine = prisma_migrate::create_engine(Some(content.clone()), url, None)?;

    let from_target = parse_diff_target(from, schema_path, &content)?;
    let to_target = parse_diff_target(to, schema_path, &content)?;

    let params = DiffParams {
        from: from_target,
        to: to_target,
        script,
        shadow_database_url: None,
        exit_code: None,
        filters: SchemaFilter::default(),
    };

    let result = engine.diff(params).await.map_err(prisma_migrate::MigrateError::from)?;

    if let Some(content) = &result.stdout {
        if content.is_empty() {
            println!("No difference.");
        } else {
            println!("{content}");
        }
    } else {
        println!("No difference.");
    }

    Ok(())
}

fn parse_diff_target(spec: &str, schema_path: &str, schema_content: &str) -> Result<DiffTarget, CliError> {
    match spec {
        "empty" => Ok(DiffTarget::Empty),
        s if s.ends_with(".prisma") => {
            let content = if s == schema_path {
                schema_content.to_string()
            } else {
                config::load_schema(s)?
            };
            Ok(DiffTarget::SchemaDatamodel(SchemasContainer {
                files: vec![SchemaContainer {
                    path: s.to_string(),
                    content,
                }],
            }))
        }
        _ => Ok(DiffTarget::SchemaDatamodel(SchemasContainer {
            files: vec![SchemaContainer {
                path: schema_path.to_string(),
                content: schema_content.to_string(),
            }],
        })),
    }
}
