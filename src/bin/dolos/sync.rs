use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    stop: crate::common::StopArgs,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain_with_stop_epoch(config, args.stop.stop_epoch)?;

    let sync = dolos::sync::pipeline(config, args.stop.sync_limit(), domain)
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
