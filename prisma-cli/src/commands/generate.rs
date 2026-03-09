use crate::config;
use crate::error::CliError;
use prisma_codegen::{RustGenerator, SchemaIR, TypeScriptGenerator};

pub fn run(schema_path: &str, output_dir: &str, language: &str) -> Result<(), CliError> {
    let content = config::load_schema(schema_path)?;
    let ir = SchemaIR::from_schema(&content)?;

    std::fs::create_dir_all(output_dir)?;

    match language {
        "typescript" | "ts" => {
            let code = TypeScriptGenerator::generate(&ir)?;
            let out_path = format!("{output_dir}/index.ts");
            std::fs::write(&out_path, &code)?;
            println!("Generated TypeScript client at {out_path}");
        }
        "rust" | "rs" => {
            let code = RustGenerator::generate(&ir)?;
            let out_path = format!("{output_dir}/client.rs");
            std::fs::write(&out_path, &code)?;
            println!("Generated Rust client at {out_path}");
        }
        _ => {
            return Err(CliError::Config(format!(
                "Unsupported language: {language}. Use 'typescript' or 'rust'."
            )));
        }
    }

    Ok(())
}
