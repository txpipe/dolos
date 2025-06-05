use log::warn;
use miette::Context;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let domain = crate::common::setup_domain(&config)?;

    let exit = crate::common::hook_exit_token();

    dolos::serve::serve(config.serve, domain, exit)
        .await
        .context("serving clients")?;

    warn!("shutdown complete");

    Ok(())
}
