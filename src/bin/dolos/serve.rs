use std::sync::Arc;

use log::warn;
use miette::Context;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger, chain) = crate::common::open_data_stores(&config)?;
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);
    let mempool = dolos::mempool::Mempool::new(genesis.clone(), ledger.clone());
    let exit = crate::common::hook_exit_token();

    dolos::serve::serve(config.serve, genesis, wal, ledger, chain, mempool, exit)
        .await
        .context("serving clients")?;

    warn!("shutdown complete");

    Ok(())
}
