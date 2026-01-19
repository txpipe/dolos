use dolos_core::config::RootConfig;
use futures_util::stream::FuturesUnordered;
use miette::{Context, IntoDiagnostic};
use tracing::{error, warn};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let domain = crate::common::setup_domain(&config).await?;

    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        domain.clone(),
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let sync = tokio::spawn(crate::common::run_pipeline(
        gasket::daemon::Daemon::new(sync),
        exit.clone(),
    ));

    let drivers = FuturesUnordered::new();

    dolos::serve::load_drivers(&drivers, config.serve, domain.clone(), exit.clone());
    dolos::relay::load_drivers(&drivers, config.relay, domain.clone(), exit.clone());

    for result in drivers {
        if let Err(e) = result.await.unwrap() {
            error!("driver error: {}", e);

            warn!("cancelling remaining drivers");
            exit.cancel();
        }
    }

    let stop_reason = sync.await.unwrap();

    warn!("shutdown complete");

    // Return appropriate exit code based on why the pipeline stopped
    let exit_code = match stop_reason {
        crate::common::PipelineStopReason::Signal => 0,
        crate::common::PipelineStopReason::QuotaReached => 1,
        crate::common::PipelineStopReason::StopEpochReached => 0,
        crate::common::PipelineStopReason::Other => 2,
    };

    if exit_code == 0 {
        Ok(())
    } else {
        std::process::exit(exit_code);
    }
}
