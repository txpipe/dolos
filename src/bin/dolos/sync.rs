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
        args.quit_on_tip,
        config.storage.max_slots_before_prune,
    )
    .into_diagnostic()
    .context("bootstrapping sync pipeline")?;

    gasket::daemon::Daemon::new(sync).block();

    Ok(())
}
