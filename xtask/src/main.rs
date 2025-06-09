use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::{cmd, Shell};

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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = Shell::new()?;

    match cli.command {
        Commands::ExternalTest => {
            println!("Running smoke tests...");
            cmd!(sh, "cargo test --test smoke -- --ignored --nocapture").run()?;
        }
    }

    Ok(())
}
