use miette::{Context, IntoDiagnostic};
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger) = crate::common::open_data_stores(&config)?;
    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;
    let (txs_out, _) = gasket::messaging::tokio::mpsc_channel(64);
    let mempool = Arc::new(dolos::submit::MempoolState::default());
    let offchain = crate::common::load_offchain_runtime(&config)?;
    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        wal.clone(),
        ledger.clone(),
        offchain.clone(),
        byron,
        shelley,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let sync = crate::common::spawn_pipeline(gasket::daemon::Daemon::new(sync), exit.clone());

    // TODO: spawn submit pipeline. Skipping for now since it's giving more trouble
    // that benefits

    let serve = tokio::spawn(dolos::serve::serve(
        config.serve,
        wal.clone(),
        ledger.clone(),
        mempool.clone(),
        txs_out,
        offchain.clone(),
        exit.clone(),
    ));

    let relay = tokio::spawn(dolos::relay::serve(config.relay, wal.clone(), exit.clone()));

    let (_, serve, relay) = tokio::try_join!(sync, serve, relay)
        .into_diagnostic()
        .context("joining threads")?;

    serve.context("serve thread")?;
    relay.into_diagnostic().context("relay thread")?;

    warn!("shutdown complete");

    Ok(())
}
