use dolos_core::config::RootConfig;
use dolos_core::Domain;
use miette::bail;

use crate::common::setup_domain;

#[derive(Debug, clap::Args)]
/// Run housekeeping procedure on all stores.
pub struct Args {
    /// maximum amount of housekeeping rounds to execute.
    #[arg(long)]
    max_rounds: Option<u64>,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let domain = setup_domain(config)?;

    match domain.drain_housekeeping(args.max_rounds) {
        Ok(rounds) => tracing::info!(rounds, "housekeeping complete"),
        Err(err) => {
            tracing::error!(err =? err, "running housekeeping");
            bail!("got error while running housekeeping");
        }
    }

    Ok(())
}
