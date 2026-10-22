use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use log::warn;
use miette::{Context as _, IntoDiagnostic as _};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain(&config)?;

    let exit = crate::common::hook_exit_token();

    let drivers = FuturesUnordered::new();
    let network_magic = crate::common::resolve_network(&config)?.magic();

    dolos::serve::load_drivers(
        &drivers,
        config.serve,
        network_magic,
        domain.clone(),
        exit.clone(),
    );

    let outcome = crate::common::monitor_drivers(drivers, exit.clone()).await;

    warn!("shutdown complete");

    outcome.into_diagnostic().context("serving clients")
}
