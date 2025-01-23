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

    let (wal, ledger) = crate::common::open_data_stores(config)?;
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);
    let mempool = dolos::mempool::Mempool::new(genesis.clone(), ledger.clone());

    let sync = dolos::sync::pipeline(
        &config.sync,
        &config.upstream,
        &config.storage,
        wal,
        ledger,
        genesis,
        mempool,
        &config.retries,
        args.quit_on_tip,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
