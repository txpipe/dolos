use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use log::warn;
use miette::{Context as _, IntoDiagnostic as _};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: RootConfig, _args: &Args) -> miette::Result<()> {
    let _tracing = crate::common::setup_tracing(&config)?;

    let domain = crate::common::setup_domain(&config)?;
    let _limiter_gauges =
        dolos_core::telemetry::queries::LimiterGauges::register(&domain.query_limiter);

    let _mempool_metrics = crate::common::mempool_metrics(&domain);

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
