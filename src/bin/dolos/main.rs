use clap::{Parser, Subcommand};
use miette::{IntoDiagnostic, Result};
use serde::Deserialize;
mod read;
mod sync;

#[derive(Debug, Subcommand)]
enum Command {
    Sync(sync::Args),
    Read(read::Args),
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
pub struct UpstreamConfig {
    peer_address: String,
    network_magic: u64,
}

#[derive(Deserialize)]
pub struct RolldbConfig {
    path: Option<std::path::PathBuf>,
}

#[derive(Deserialize)]
pub struct Config {
    upstream: UpstreamConfig,
    rolldb: RolldbConfig,
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
    let config = Config::new(&args.config).into_diagnostic()?;

    match args.command {
        Command::Sync(x) => sync::run(&config, &x).into_diagnostic()?,
        Command::Read(x) => read::run(&config, &x).into_diagnostic()?,
    };

    Ok(())
}
