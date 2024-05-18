use clap::{Parser, Subcommand};
use miette::{Context, IntoDiagnostic, Result};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::path::PathBuf;

mod common;
mod daemon;
mod data;
mod doctor;
mod eval;
mod serve;
mod sync;

#[cfg(feature = "mithril")]
mod bootstrap;

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the node in all its glory
    Daemon(daemon::Args),

    /// Just sync from upstream peer
    Sync(sync::Args),

    /// Just serve data through apis
    Serve(serve::Args),

    /// Commands to interact with data
    Data(data::Args),

    /// Evaluate txs using current ledger
    Eval(eval::Args),

    /// Commands to fix problems
    Doctor(doctor::Args),

    /// Bootstrap the node using Mithril
    #[cfg(feature = "mithril")]
    Bootstrap(bootstrap::Args),
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

#[derive(Deserialize)]
pub struct StorageConfig {
    path: Option<std::path::PathBuf>,
    wal_size: Option<u64>,
    immutable_overlap: Option<u64>,
}

#[derive(Deserialize)]
pub struct GenesisConfig {
    byron_path: PathBuf,
    shelley_path: PathBuf,
    alonzo_path: PathBuf,
    // TODO: add hash of genesis for runtime verification
    // hash: String,
}

#[derive(Deserialize)]
pub struct MithrilConfig {
    aggregator: String,
    genesis_key: String,
}

#[serde_as]
#[derive(Deserialize, Default, Debug)]
pub struct LoggingConfig {
    #[serde_as(as = "Option<DisplayFromStr>")]
    max_level: Option<tracing::Level>,

    #[serde(default)]
    include_pallas: bool,

    #[serde(default)]
    include_grpc: bool,
}

#[derive(Deserialize)]
pub struct Config {
    pub upstream: dolos::model::UpstreamConfig,
    pub storage: StorageConfig,
    pub genesis: GenesisConfig,
    pub sync: dolos::sync::Config,
    pub serve: dolos::serve::Config,
    pub submit: dolos::submit::Config,
    pub retries: Option<gasket::retries::Policy>,
    pub mithril: Option<MithrilConfig>,
    #[serde(default)]
    pub logging: LoggingConfig,
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

fn main() -> Result<()> {
    let args = Cli::parse();
    let config = Config::new(&args.config)
        .into_diagnostic()
        .context("parsing configuration")?;

    match args.command {
        Command::Daemon(x) => daemon::run(config, &x)?,
        Command::Sync(x) => sync::run(&config, &x)?,
        Command::Data(x) => data::run(&config, &x)?,
        Command::Serve(x) => serve::run(config, &x)?,
        Command::Eval(x) => eval::run(&config, &x)?,
        Command::Doctor(x) => doctor::run(&config, &x)?,

        #[cfg(feature = "mithril")]
        Command::Bootstrap(x) => bootstrap::run(&config, &x)?,
    };

    Ok(())
}
