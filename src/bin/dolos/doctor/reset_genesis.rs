use std::path::Path;

use dolos_cardano::networks::KnownNetwork;
use dolos_core::config::{GenesisConfig, RootConfig};
use miette::{Context as _, IntoDiagnostic as _};

use crate::init::parse_known_network;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, value_parser = parse_known_network)]
    pub network: Option<&'static KnownNetwork>,
}

/// A known network is itself; a custom one is the known network its magic is.
fn infer_network(config: &RootConfig) -> Option<&'static KnownNetwork> {
    match &config.chain.cardano {
        Some(cardano) => KnownNetwork::by_magic(cardano.magic.into()),
        None => KnownNetwork::by_name(&config.chain.network),
    }
}

/// A custom network's configured genesis files, which resolve against the
/// configuration file's directory; a known network reads none, so its files
/// go to the working directory.
fn target(config: &RootConfig) -> GenesisConfig {
    match &config.chain.cardano {
        Some(cardano) => cardano.genesis.clone(),
        None => GenesisConfig::in_dir(Path::new("./")),
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let network = match (args.network, infer_network(config)) {
        (Some(network), _) => network,
        (None, Some(infered)) => infered,
        (None, None) => {
            return Err(miette::Error::msg(
                "no network specified and can't infer it from the config",
            ))
        }
    };

    network
        .save_genesis_to(&target(config))
        .into_diagnostic()
        .context("saving genesis")?;

    Ok(())
}
