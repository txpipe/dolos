use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use miette::{Context, IntoDiagnostic};
use tracing::warn;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    stop: crate::common::StopArgs,
}

#[tokio::main]
pub async fn run(config: RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain_with_stop_epoch(&config, args.stop.stop_epoch)?;

    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(&config, args.stop.sync_limit(), domain.clone())
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    let sync = tokio::spawn(crate::common::run_pipeline(
        gasket::daemon::Daemon::new(sync),
        exit.clone(),
    ));

    let drivers = FuturesUnordered::new();
    let network_magic = crate::common::resolve_network(&config)?.magic();

    dolos::serve::load_drivers(
        &drivers,
        config.serve,
        network_magic,
        domain.clone(),
        exit.clone(),
    );
    dolos::relay::load_drivers(
        &drivers,
        config.relay,
        network_magic,
        domain.clone(),
        exit.clone(),
    );

    let outcome = crate::common::monitor_drivers(drivers, exit.clone()).await;

    sync.await.unwrap();

    warn!("shutdown complete");

    outcome.into_diagnostic().context("serving clients")
}
