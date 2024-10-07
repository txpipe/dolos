use miette::{Context, IntoDiagnostic};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, ledger) = crate::common::open_data_stores(config)?;

    let mempool = dolos::mempool::Mempool::new();

    let (byron, shelley, _, _) = crate::common::open_genesis_files(&config.genesis)?;

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        wal,
        ledger,
        byron,
        shelley,
        mempool,
        &config.retries,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
