use dolos::sync::SyncLimit;
use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use miette::{Context, IntoDiagnostic};
use tracing::warn;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain(&config)?;

    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(&config, SyncLimit::Follow, domain.clone())
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    let progress = sync.progress;

    let sync = tokio::spawn(crate::common::run_pipeline(
        gasket::daemon::Daemon::new(sync.tethers),
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

    let ended = sync.await.unwrap();

    warn!("shutdown complete");

    // The daemon has no stop condition: a sync that ends on its own failed,
    // which a supervisor reads from the exit status.
    if let Some(ended) = ended {
        return Err(crate::common::sync_failure(
            "the sync stopped",
            &progress,
            &ended,
        ));
    }

    outcome.into_diagnostic().context("serving clients")
}
