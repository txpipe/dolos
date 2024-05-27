use std::sync::Arc;

use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger) = crate::common::open_data_stores(&config)?;
    let (byron, shelley, _) = crate::common::open_genesis_files(&config.genesis)?;
    let (txs_out, txs_in) = gasket::messaging::tokio::mpsc_channel(64);
    let mempool = Arc::new(dolos::submit::MempoolState::default());

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        wal.clone(),
        ledger.clone(),
        byron,
        shelley,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let submit = dolos::submit::pipeline(
        &config.submit,
        &config.upstream,
        wal.clone(),
        mempool.clone(),
        txs_in,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping submit pipeline")?;

    let pipelines = gasket::daemon::Daemon::new(sync.into_iter().chain(submit).collect());

    let serve = tokio::spawn(dolos::serve::serve(
        config.serve,
        wal.clone(),
        ledger.clone(),
        mempool.clone(),
        txs_out,
    ));

    let relay = config
        .relay
        .map(|relay| tokio::spawn(dolos::relay::serve(relay, wal.clone())));

    pipelines.block();
    relay.inspect(|x| x.abort());
    serve.abort();

    Ok(())
}
