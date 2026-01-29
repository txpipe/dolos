use dolos_core::config::RootConfig;
use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let domain = crate::common::setup_domain(config)?;

    let sync = dolos::sync::pipeline(&config.sync, &config.upstream, domain, &config.retries)
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
