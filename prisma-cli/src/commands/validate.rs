use crate::config;
use crate::error::CliError;

pub fn run(schema_path: &str) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let input = config::schema_to_validate_input(schema_path, &content);
    prisma_schema::validate(&input)?;
    println!("The schema at {schema_path} is valid.");
    Ok(())
}
