use clap::{Parser, Subcommand};
use miette::{Context, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::path::PathBuf;

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

#[derive(Serialize, Deserialize)]
pub struct GenesisConfig {
    byron_path: PathBuf,
    shelley_path: PathBuf,
    alonzo_path: PathBuf,
    conway_path: PathBuf,
    force_protocol: Option<usize>,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            byron_path: PathBuf::from("byron.json"),
            shelley_path: PathBuf::from("shelley.json"),
            alonzo_path: PathBuf::from("alonzo.json"),
            conway_path: PathBuf::from("conway.json"),
            force_protocol: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MithrilConfig {
    aggregator: String,
    genesis_key: String,
    ancillary_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SnapshotConfig {
    download_url: String,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct LoggingConfig {
    #[serde_as(as = "DisplayFromStr")]
    max_level: tracing::Level,

    #[serde(default)]
    include_tokio: bool,

    #[serde(default)]
    include_pallas: bool,

    #[serde(default)]
    include_grpc: bool,

    #[serde(default)]
    include_trp: bool,

    #[serde(default)]
    include_minibf: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_level: tracing::Level::INFO,
            include_tokio: Default::default(),
            include_pallas: Default::default(),
            include_grpc: Default::default(),
            include_trp: Default::default(),
            include_minibf: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub upstream: dolos::core::UpstreamConfig,
    pub storage: dolos::core::StorageConfig,
    pub genesis: GenesisConfig,
    pub sync: dolos::sync::Config,
    pub submit: dolos::core::SubmitConfig,
    pub serve: dolos::serve::Config,
    pub relay: Option<dolos::relay::Config>,
    pub retries: Option<gasket::retries::Policy>,
    pub mithril: Option<MithrilConfig>,
    pub snapshot: Option<SnapshotConfig>,

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

        // finally, we use env vars to make some last-step overrides.
        // DEPRECATED: Use double underscore instead, config values include _ in the name
        s = s.add_source(config::Environment::with_prefix("DOLOS").separator("_"));
        s = s.add_source(config::Environment::with_prefix("DOLOS").separator("__"));

        s.build()?.try_deserialize()
    }
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let config = Config::new(&args.config)
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

        (Err(x), _) => Err(x),
    }
}
