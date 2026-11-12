// Keeps a `TracingGuard` from being discarded with `let _ =`, which shuts
// telemetry down at startup; `#[must_use]` alone does not catch that.
#![warn(clippy::let_underscore_must_use)]

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
mod feedback;
mod limits;
mod output;
mod registry;
mod serve;
mod service;
mod sync;

#[cfg(feature = "utils")]
mod init;

#[cfg(feature = "utils")]
mod data;

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

    /// Commands over the configuration file
    Config(config::Args),

    /// Fill, clear, inspect and check the cache, with the node stopped
    Cache(cache::Args),

    /// Install the node as an OS service, or uninstall it
    Service(service::Args),

    /// Fill the node's stores from a Stelae snapshot, Mithril or a local
    /// ImmutableDB
    Bootstrap(bootstrap::Args),
}

impl Command {
    fn uses_storage(&self) -> bool {
        match self {
            Self::Daemon(_)
            | Self::Sync(_)
            | Self::Serve(_)
            | Self::Cache(_)
            | Self::Bootstrap(_) => true,
            #[cfg(feature = "utils")]
            Self::Data(_) => true,
            Self::Config(_) | Self::Service(_) => false,
            #[cfg(feature = "utils")]
            Self::Init(_) => false,
        }
    }
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
    #[allow(clippy::let_underscore_must_use)]
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Cli::parse();
    let explicit_config = args.config.as_deref();
    let config_file = crate::common::config_file(explicit_config);

    let feedback = crate::feedback::Feedback::default();

    // Each command loads the configuration it runs on, so `config
    // write-genesis --network` runs without one.
    let uses_storage = args.command.uses_storage();
    let load = || -> Result<_> {
        let config = crate::common::load_and_check_config(config_file)?;
        if uses_storage {
            crate::limits::prepare()?;
        }
        Ok(config)
    };

    match args.command {
        Command::Daemon(args) => daemon::run(load()?, &args),
        Command::Sync(args) => sync::run(&load()?, &args, &feedback),
        Command::Serve(args) => serve::run(load()?, &args),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_commands_require_resource_preflight() {
        for args in [
            &["daemon"][..],
            &["serve"],
            &["sync", "catchup"],
            &["sync", "status"],
            &["sync", "recover", "--dry-run"],
            #[cfg(feature = "utils")]
            &["sync", "dump"],
            &["cache", "stats"],
            &["bootstrap", "immutabledb", "--source", "db"],
        ] {
            let cli =
                Cli::try_parse_from(std::iter::once("dolos").chain(args.iter().copied())).unwrap();
            assert!(cli.command.uses_storage(), "{args:?}");
        }

        #[cfg(feature = "utils")]
        assert!(Cli::try_parse_from(["dolos", "data", "stats"])
            .unwrap()
            .command
            .uses_storage());
    }

    #[test]
    fn configuration_and_service_commands_need_no_resource_preflight() {
        for args in [
            &["config", "check"][..],
            &["service", "install"],
            &["service", "uninstall", "example"],
        ] {
            let cli =
                Cli::try_parse_from(std::iter::once("dolos").chain(args.iter().copied())).unwrap();
            assert!(!cli.command.uses_storage(), "{args:?}");
        }

        #[cfg(feature = "utils")]
        assert!(!Cli::try_parse_from(["dolos", "init"])
            .unwrap()
            .command
            .uses_storage());
    }
}
