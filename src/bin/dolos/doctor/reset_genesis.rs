use std::path::PathBuf;

use crate::init::KnownNetwork;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long)]
    pub network: Option<KnownNetwork>,
}

fn infer_network(config: &crate::Config) -> Option<KnownNetwork> {
    let magic = config.upstream.network_magic()?;
    KnownNetwork::from_magic(magic)
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    let infered = infer_network(config);

    let network = match (&args.network, &infered) {
        (Some(network), _) => network,
        (None, Some(infered)) => infered,
        (None, None) => {
            return Err(miette::Error::msg(
                "no network specified and can't infer it from the config",
            ))
        }
    };

    network.save_included_genesis(&PathBuf::from("./"))?;

    Ok(())
}
