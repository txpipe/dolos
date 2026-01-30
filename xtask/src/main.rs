use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::{cmd, Shell};

mod bootstrap;
mod config;
mod create_test_instance;
mod delete_test_instance;
mod ground_truth;
mod util;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run external tests
    ExternalTest,

    /// Bootstrap a local Mithril snapshot into an instance
    BootstrapMithrilLocal(bootstrap::BootstrapArgs),

    /// Generate ground-truth fixtures from cardano-db-sync
    CardanoGroundTruth(ground_truth::GroundTruthArgs),

    /// Create a test instance and ground-truth fixtures
    CreateTestInstance(create_test_instance::CreateTestInstanceArgs),

    /// Delete a test instance directory
    DeleteTestInstance(delete_test_instance::DeleteTestInstanceArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse_from(normalize_args());
    let sh = Shell::new()?;

    match cli.command {
        Commands::ExternalTest => {
            println!("Running smoke tests...");
            cmd!(sh, "cargo test --test smoke -- --ignored --nocapture").run()?;
        }
        Commands::BootstrapMithrilLocal(args) => bootstrap::run(&sh, &args)?,
        Commands::CardanoGroundTruth(args) => ground_truth::run(&args)?,
        Commands::CreateTestInstance(args) => create_test_instance::run(&sh, &args)?,
        Commands::DeleteTestInstance(args) => delete_test_instance::run(&args)?,
    }

    Ok(())
}

fn normalize_args() -> Vec<String> {
    let mut args: Vec<String> = std::env::args().collect();

    if args.get(1).is_some_and(|arg| arg == "xtask") {
        args.remove(1);
    }

    args
}
