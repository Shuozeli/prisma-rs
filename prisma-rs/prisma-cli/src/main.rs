mod commands;
mod config;
mod error;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "prisma",
    version,
    about = "Prisma CLI - schema management, code generation, and migrations"
)]
struct Cli {
    /// Path to the Prisma schema file
    #[arg(long, global = true, default_value = "prisma/schema.prisma")]
    schema: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate the Prisma schema
    Validate,

    /// Format the Prisma schema
    Format {
        /// Check formatting without modifying the file (exit code 1 if unformatted)
        #[arg(long)]
        check: bool,
    },

    /// Generate client code from the Prisma schema
    Generate {
        /// Output directory for generated code
        #[arg(long, short, default_value = "generated")]
        output: String,

        /// Target language: "typescript" or "rust"
        #[arg(long, short, default_value = "typescript")]
        language: String,
    },

    /// Database commands
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },

    /// Migration commands
    Migrate {
        #[command(subcommand)]
        command: MigrateCommand,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// Push the schema to the database without migrations
    Push {
        /// Database connection URL (overrides schema datasource)
        #[arg(long)]
        url: Option<String>,

        /// Accept data loss warnings
        #[arg(long)]
        accept_data_loss: bool,
    },

    /// Introspect the database and update the schema
    Pull {
        /// Database connection URL (overrides schema datasource)
        #[arg(long)]
        url: Option<String>,
    },

    /// Execute a SQL script against the database
    Execute {
        /// Database connection URL (overrides schema datasource)
        #[arg(long)]
        url: Option<String>,

        /// SQL script to execute
        #[arg(long)]
        stdin: bool,

        /// SQL file to execute
        #[arg(long)]
        file: Option<String>,
    },
}

#[derive(Subcommand)]
enum MigrateCommand {
    /// Create and apply migrations in development
    Dev {
        /// Migration name
        #[arg(long)]
        name: String,

        /// Create migration without applying
        #[arg(long)]
        create_only: bool,

        /// Database connection URL
        #[arg(long)]
        url: Option<String>,
    },

    /// Apply pending migrations in production
    Deploy {
        /// Database connection URL
        #[arg(long)]
        url: Option<String>,
    },

    /// Reset the database
    Reset {
        /// Skip confirmation prompt
        #[arg(long)]
        force: bool,

        /// Database connection URL
        #[arg(long)]
        url: Option<String>,
    },

    /// Resolve a failed migration
    Resolve {
        /// Mark migration as applied
        #[arg(long)]
        applied: Option<String>,

        /// Mark migration as rolled back
        #[arg(long)]
        rolled_back: Option<String>,

        /// Database connection URL
        #[arg(long)]
        url: Option<String>,
    },

    /// Show differences between schemas
    Diff {
        /// From source (e.g. "schema.prisma", "empty", or a database URL)
        #[arg(long)]
        from: String,

        /// To target (e.g. "schema.prisma", "empty", or a database URL)
        #[arg(long)]
        to: String,

        /// Output as SQL script instead of human-readable summary
        #[arg(long)]
        script: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Validate => commands::validate::run(&cli.schema),
        Command::Format { check } => commands::format::run(&cli.schema, check),
        Command::Generate { output, language } => commands::generate::run(&cli.schema, &output, &language),
        Command::Db { command } => match command {
            DbCommand::Push { url, accept_data_loss } => {
                commands::db_push::run(&cli.schema, url.as_deref(), accept_data_loss).await
            }
            DbCommand::Pull { url } => commands::db_pull::run(&cli.schema, url.as_deref()).await,
            DbCommand::Execute { url, stdin, file } => {
                commands::db_execute::run(&cli.schema, url.as_deref(), stdin, file.as_deref()).await
            }
        },
        Command::Migrate { command } => match command {
            MigrateCommand::Dev { name, create_only, url } => {
                commands::migrate_dev::run(&cli.schema, &name, create_only, url.as_deref()).await
            }
            MigrateCommand::Deploy { url } => commands::migrate_deploy::run(&cli.schema, url.as_deref()).await,
            MigrateCommand::Reset { force, url } => {
                commands::migrate_reset::run(&cli.schema, force, url.as_deref()).await
            }
            MigrateCommand::Resolve {
                applied,
                rolled_back,
                url,
            } => {
                commands::migrate_resolve::run(&cli.schema, applied.as_deref(), rolled_back.as_deref(), url.as_deref())
                    .await
            }
            MigrateCommand::Diff { from, to, script } => {
                commands::migrate_diff::run(&cli.schema, &from, &to, script).await
            }
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
