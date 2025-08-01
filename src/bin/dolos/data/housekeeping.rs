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

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(config)?;

    let domain = setup_domain(config)?;

    let mut done = false;
    let mut rounds = 0;

    while !done {
        tracing::info!(round = rounds, max =? args.max_rounds, "starting housekeeping round");
        done = match domain.housekeeping() {
            Ok(done) => done,
            Err(err) => {
                tracing::error!(err =? err, "running housekeeping");
                bail!("got error while running housekeeping");
            }
        };
        rounds += 1;

        if let Some(max) = args.max_rounds {
            done = rounds >= max;
        }
    }

    Ok(())
}
