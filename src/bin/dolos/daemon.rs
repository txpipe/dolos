use dolos::health::{Command, Pipeline};
use dolos::sync::{metrics::ExportedDaemon, SyncLimit};
use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use miette::{Context, IntoDiagnostic};
use tracing::warn;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::refuse_interrupted_restore(&config)?;

    let telemetry = crate::common::setup_tracing(&config)?;

    let domain = crate::common::setup_domain(&config)?;
    let status_lock = crate::common::lock_status(&config)?;

    let _limiter_gauges =
        dolos_core::telemetry::queries::LimiterGauges::register(&domain.query_limiter);

    let _mempool_metrics = crate::common::mempool_metrics(&domain);

    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(
        &config,
        SyncLimit::Follow,
        domain.clone(),
        telemetry.metrics_flush(),
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let progress = sync.progress;
    let _sync_gauges = sync.gauges;

    let _status = crate::common::keep_status(
        &config,
        status_lock,
        Command::Daemon,
        Some(Box::new(Pipeline::new(&progress, &domain))),
    )?;

    let sync = tokio::spawn(crate::common::run_pipeline(
        ExportedDaemon::new(gasket::daemon::Daemon::new(sync.tethers)),
        telemetry.metrics_flush(),
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
            progress.upstream_failure(),
            &ended,
        ));
    }

    outcome.into_diagnostic().context("serving clients")
}
