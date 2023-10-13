use std::{path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};
use miette::{IntoDiagnostic, Result};
use serde::Deserialize;

mod daemon;
mod read;
mod serve;
mod sync;

#[derive(Debug, Subcommand)]
enum Command {
    Daemon(daemon::Args),
    Sync(sync::Args),
    Read(read::Args),
    Serve(serve::Args),
}

#[derive(Debug, Parser)]
#[clap(name = "Dolos")]
#[clap(bin_name = "dolos")]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    config: Option<std::path::PathBuf>,
}

#[derive(Deserialize)]
pub struct RolldbConfig {
    path: Option<std::path::PathBuf>,
    k_param: Option<u64>,
    k_param_buffer: Option<u64>,
}

#[derive(Deserialize)]
pub struct GenesisFileRef {
    path: PathBuf,
    // TODO: add hash of genesis for runtime verification
    // hash: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub rolldb: RolldbConfig,
    pub upstream: dolos::sync::Config,
    pub serve: dolos::serve::Config,
    pub retries: Option<gasket::retries::Policy>,
    pub byron: GenesisFileRef,
}

impl Config {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // our base config will always be in /etc/dolos
        s = s.add_source(config::File::with_name("/etc/dolos/daemon.toml").required(false));

        // but we can override it by having a file in the working dir
        s = s.add_source(config::File::with_name("dolos.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(config::Environment::with_prefix("DOLOS").separator("_"));

        s.build()?.try_deserialize()
    }
}

fn define_gasket_policy(config: Option<&gasket::retries::Policy>) -> gasket::runtime::Policy {
    let default_policy = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    gasket::runtime::Policy {
        tick_timeout: std::time::Duration::from_secs(120).into(),
        bootstrap_retry: config.cloned().unwrap_or(default_policy.clone()),
        work_retry: config.cloned().unwrap_or(default_policy.clone()),
        teardown_retry: config.cloned().unwrap_or(default_policy.clone()),
    }
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let config = Config::new(&args.config).into_diagnostic()?;

    let retries = define_gasket_policy(config.retries.as_ref());

    match args.command {
        Command::Daemon(x) => daemon::run(config, &retries, &x).into_diagnostic()?,
        Command::Sync(x) => sync::run(&config, &retries, &x).into_diagnostic()?,
        Command::Read(x) => read::run(&config, &x).into_diagnostic()?,
        Command::Serve(x) => serve::run(config, &x).into_diagnostic()?,
    };

    Ok(())
}
