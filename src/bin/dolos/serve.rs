use log::warn;
use miette::Context;
use std::sync::Arc;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger) = crate::common::open_data_stores(&config)?;
    let (byron, shelley, alonzo) = crate::common::open_genesis_files(&config.genesis)?;
    let (txs_out, _txs_in) = gasket::messaging::tokio::mpsc_channel(64);
    let mempool = Arc::new(dolos::submit::MempoolState::default());
    let exit = crate::common::hook_exit_token();

    dolos::serve::serve(
        config.serve,
        (alonzo, byron, shelley),
        wal,
        ledger,
        mempool,
        txs_out,
        exit,
    )
    .await
    .context("serving clients")?;

    warn!("shutdown complete");

    Ok(())
}
