use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::{cmd, Shell};
use xtask::perf;
use xtask::perf::measure::PeakAlloc;

mod bootstrap;
mod config;
mod ground_truth;
mod test_instance;
mod util;

// The benchmark harness reads transient heap peaks off the global allocator.
#[global_allocator]
static ALLOC: PeakAlloc = PeakAlloc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run e2e tests
    E2eTest,

    /// Storage and minibf performance experiments with shared measurement and
    /// reporting
    #[command(subcommand)]
    Perf(perf::Cmd),

    /// Compatibility entry point for storage benchmarks; prefer perf storage
    #[command(subcommand)]
    ArchiveBench(perf::storage::Cmd),

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
        Commands::E2eTest => {
            println!("Running smoke tests...");
            cmd!(sh, "cargo test --test smoke -- --ignored --nocapture").run()?;

            println!("Running sync tests...");
            cmd!(sh, "cargo test --test sync -- --ignored --nocapture").run()?;
        }
        Commands::Perf(cmd) => perf::run(cmd)?,
        Commands::ArchiveBench(cmd) => perf::storage::run(cmd)?,
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
