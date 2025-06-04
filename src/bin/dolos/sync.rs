use std::sync::Arc;

use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    quit_on_tip: bool,
}

pub fn run(config: &super::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let domain = crate::common::setup_domain(config)?;

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        domain,
        &config.retries,
        args.quit_on_tip,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
