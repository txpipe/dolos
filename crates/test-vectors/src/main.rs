use clap::{Parser, Subcommand};

pub mod build;
pub mod queries;
pub mod utils;

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the node configuration
    Build(build::Args),
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,
}

fn main() -> miette::Result<()> {
    let args = Cli::parse();

    match args.command {
        Command::Build(args) => build::run(&args),
    }
}
