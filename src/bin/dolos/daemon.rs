use miette::{Context, IntoDiagnostic};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, clap::Args)]
pub struct Args {}

#[tokio::main]
pub async fn run(config: super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, chain, ledger) = crate::common::open_data_stores(&config)?;
    let (byron, _, _) = crate::common::open_genesis_files(&config.genesis)?;

    let (txs_out, txs_in) = gasket::messaging::tokio::mpsc_channel(64);

    let mempool = Arc::new(dolos::submit::MempoolState::default());
    let cancellation_token = CancellationToken::new();

    let server = tokio::spawn(dolos::serve::serve(
        config.serve,
        wal.clone(),
        chain.clone(),
        ledger.clone(),
        mempool.clone(),
        txs_out,
        cancellation_token.clone(),
    ));

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        wal.clone(),
        chain,
        ledger.clone(),
        byron,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    let submit = dolos::submit::pipeline(
        &config.submit,
        &config.upstream,
        wal,
        mempool.clone(),
        txs_in,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping submit pipeline")?;

    let mut sigint =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

    let sync_future = tokio::task::spawn_blocking(move || {
        gasket::daemon::Daemon(sync.into_iter().chain(submit).collect()).block()
    });

    tokio::select! {
        _ = sigint.recv() => {
            warn!("SIGINT received, shutting down...");

            cancellation_token.cancel();
            server.abort();

            if let Err(e) = ledger.close() {
                error!("Failed to close ledger: {:?}", e);
            }

            info!("Cleanup completed, exiting now.");
        }
        _ = sync_future => {
            info!("Daemon completed.");
        }
    }

    Ok(())
}
