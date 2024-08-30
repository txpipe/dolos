use miette::{Context, IntoDiagnostic};
use tracing::warn;

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger) = crate::common::open_data_stores(&config)?;
    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;
    let mempool = dolos::mempool::Mempool::new();
    let exit = crate::common::hook_exit_token();

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        wal.clone(),
        ledger.clone(),
        byron,
        shelley,
        mempool.clone(),
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let sync = crate::common::spawn_pipeline(gasket::daemon::Daemon::new(sync), exit.clone());

    // TODO: spawn submit pipeline. Skipping for now since it's giving more trouble
    // that benefits

    // We need new file handled for the separate process.
    let (byron, shelley, alonzo) = crate::common::open_genesis_files(&config.genesis)?;
    let serve = tokio::spawn(dolos::serve::serve(
        config.serve,
        (alonzo, byron, shelley),
        wal.clone(),
        ledger.clone(),
        mempool.clone(),
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
