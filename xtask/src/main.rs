use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::{cmd, Shell};

mod bootstrap;
mod config;
mod ground_truth;
mod test_instance;
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

    /// Ground-truth fixture commands (generate, compare, query)
    #[command(subcommand)]
    GroundTruth(ground_truth::GroundTruthCmd),

    /// Test instance management commands (create, delete)
    #[command(subcommand)]
    TestInstance(test_instance::TestInstanceCmd),
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
        Commands::GroundTruth(cmd) => ground_truth::run(cmd)?,
        Commands::TestInstance(cmd) => test_instance::run(&sh, cmd)?,
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
