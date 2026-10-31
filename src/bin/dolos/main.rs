use clap::{Parser, Subcommand};
use miette::Result;

use crate::output::ExitCode;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod banner;
mod bootstrap;
mod cache;
mod common;
mod config;
mod daemon;
mod doctor;
mod eval;
mod feedback;
mod output;
mod registry;
mod serve;
mod service;
mod snapshot;
mod sync;

#[cfg(feature = "utils")]
mod init;

#[cfg(feature = "utils")]
mod data;

#[cfg(feature = "minibf")]
mod minibf;

#[cfg(feature = "minikupo")]
mod minikupo;

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the node configuration
    #[cfg(feature = "utils")]
    Init(init::Args),

    /// Run the node in all its glory
    Daemon(daemon::Args),

    /// Sync from the upstream peer, and inspect, recover or roll back the WAL
    #[command(arg_required_else_help = true)]
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

    /// Commands over the configuration file
    Config(config::Args),

    /// Inspect and reproduce Stelae snapshots for this node
    Snapshot(snapshot::Args),

    /// Fill, clear, inspect and check the cache, with the node stopped
    Cache(cache::Args),

    /// Install the node as an OS service, or uninstall it
    Service(service::Args),

    /// Fill the node's stores from a Stelae snapshot, Mithril or a local
    /// ImmutableDB
    Bootstrap(bootstrap::Args),

    /// runs a minibf query in-process
    #[cfg(feature = "minibf")]
    Minibf(minibf::Args),

    /// runs a minikupo query in-process
    #[cfg(feature = "minikupo")]
    Minikupo(minikupo::Args),
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

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => ExitCode::Ok.into(),
        Err(report) => {
            // What returning the error from `main` prints.
            eprintln!("Error: {report:?}");
            ExitCode::of(&report).into()
        }
    }
}

fn run() -> Result<()> {
    // reqwest 0.13 (via mithril-client) uses `rustls-no-provider`: it reads the
    // process-default provider and panics if none is installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Cli::parse();
    let explicit_config = args.config.as_deref();
    let config_file = crate::common::config_file(explicit_config);

    let feedback = crate::feedback::Feedback::default();

    // Each command loads the configuration it runs on, so `config
    // write-genesis --network` runs without one.
    let load = || crate::common::load_and_check_config(config_file);

    match args.command {
        Command::Daemon(args) => daemon::run(load()?, &args),
        Command::Sync(args) => sync::run(&load()?, &args, &feedback),
        Command::Serve(args) => serve::run(load()?, &args),
        Command::Eval(args) => eval::run(&load()?, &args),
        Command::Doctor(args) => doctor::run(&load()?, &args, &feedback),
        Command::Snapshot(args) => snapshot::run(&load()?, &args),
        Command::Cache(args) => cache::run(&load()?, &args),
        Command::Config(args) => config::run(config_file, &args),
        Command::Service(args) => service::run(explicit_config, &args),

        // the init command is special because it knows how to execute with or without a valid
        // configuration, that is why we pass the whole result and let the command logic decide what
        // to do with it.
        #[cfg(feature = "utils")]
        Command::Init(args) => init::run(load(), config_file, &args),

        #[cfg(feature = "utils")]
        Command::Data(args) => data::run(&load()?, &args, &feedback),

        Command::Bootstrap(args) => bootstrap::run(&load()?, &args, &feedback),

        #[cfg(feature = "minibf")]
        Command::Minibf(x) => minibf::run(&load()?, &x),

        #[cfg(feature = "minikupo")]
        Command::Minikupo(x) => minikupo::run(&load()?, &x),
    }
}
