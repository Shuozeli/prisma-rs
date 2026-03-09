use crate::config;
use crate::error::CliError;
use prisma_migrate::rpc_types::{DbExecuteDatasourceType, DbExecuteParams, UrlContainer};

pub async fn run(schema_path: &str, url: Option<&str>, stdin: bool, file: Option<&str>) -> Result<(), CliError> {
    let script = if stdin {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
        buf
    } else if let Some(file_path) = file {
        config::validate_path_within_cwd(file_path)?;
        std::fs::read_to_string(file_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                CliError::Config(format!("SQL file not found: {file_path}"))
            } else {
                CliError::Io(e)
            }
        })?
    } else {
        return Err(CliError::Config("Provide SQL via --stdin or --file.".to_string()));
    };

    let db_url = config::resolve_url(url)?;
    let content = config::load_schema(schema_path)?;

    let engine = prisma_migrate::create_engine(Some(content), Some(db_url.clone()), None)?;

    let params = DbExecuteParams {
        datasource_type: DbExecuteDatasourceType::Url(UrlContainer { url: db_url }),
        script,
    };

    engine
        .db_execute(params)
        .await
        .map_err(prisma_migrate::MigrateError::from)?;

    println!("SQL executed successfully.");
    Ok(())
}
