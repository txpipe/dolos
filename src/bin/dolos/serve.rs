use futures_util::stream::FuturesUnordered;
use log::warn;
use tracing::error;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config)?;

    let domain = crate::common::setup_domain(&config)?;

    let exit = crate::common::hook_exit_token();

    let drivers = FuturesUnordered::new();

    dolos::serve::load_drivers(&drivers, config.serve, domain.clone(), exit.clone());

    for result in drivers {
        if let Err(e) = result.await.unwrap() {
            error!("driver error: {}", e);

            warn!("cancelling remaining drivers");
            exit.cancel();
        }
    }

    warn!("shutdown complete");

    Ok(())
}
