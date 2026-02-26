use clap::{Parser, Subcommand};
use miette::{Context, IntoDiagnostic, Result};

mod common;
mod daemon;
mod doctor;
mod eval;
mod feedback;
mod serve;
mod sync;

#[cfg(feature = "utils")]
mod init;

#[cfg(feature = "utils")]
mod data;

#[cfg(feature = "mithril")]
mod bootstrap;

#[cfg(feature = "minibf")]
mod minibf;

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the node configuration
    #[cfg(feature = "utils")]
    Init(init::Args),

    /// Run the node in all its glory
    Daemon(daemon::Args),

    /// Just sync from upstream peer
    Sync(sync::Args),

    /// Just serve data through apis
    Serve(serve::Args),

    /// Commands to interact with data
    #[cfg(feature = "utils")]
    Data(data::Args),

    /// Evaluate txs using current ledger
    Eval(eval::Args),

    /// Commands to fix problems
    Doctor(doctor::Args),

    /// Bootstrap the node using Mithril
    #[cfg(feature = "mithril")]
    Bootstrap(bootstrap::Args),

    /// runs a minibf query in-process
    #[cfg(feature = "minibf")]
    Minibf(minibf::Args),
}

#[derive(Debug, Parser)]
#[clap(name = "Dolos")]
#[clap(bin_name = "dolos")]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, global = true)]
    config: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let config = crate::common::load_config(&args.config)
        .into_diagnostic()
        .context("parsing configuration");

    let feedback = crate::feedback::Feedback::default();

    match (config, args.command) {
        (Ok(config), Command::Daemon(args)) => daemon::run(config, &args),
        (Ok(config), Command::Sync(args)) => sync::run(&config, &args),
        (Ok(config), Command::Serve(args)) => serve::run(config, &args),
        (Ok(config), Command::Eval(args)) => eval::run(&config, &args),
        (Ok(config), Command::Doctor(args)) => doctor::run(&config, &args, &feedback),

        // the init command is special because it knows how to execute with or without a valid
        // configuration, that is why we pass the whole result and let the command logic decide what
        // to do with it.
        #[cfg(feature = "utils")]
        (config, Command::Init(args)) => init::run(config, &args, &feedback),

        #[cfg(feature = "utils")]
        (Ok(config), Command::Data(args)) => data::run(&config, &args, &feedback),

        #[cfg(feature = "mithril")]
        (Ok(config), Command::Bootstrap(args)) => bootstrap::run(&config, &args, &feedback),

        #[cfg(feature = "minibf")]
        (Ok(config), Command::Minibf(x)) => minibf::run(&config, &x),

        (Err(x), _) => Err(x),
    }
}
