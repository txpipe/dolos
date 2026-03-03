use dolos::storage::ArchiveStoreBackend;
use dolos_core::config::RootConfig;
use dolos_core::ArchiveStore as _;
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// the maximum number of slots to keep in the Chain
    #[arg(long)]
    max_slots: Option<u64>,

    /// the maximum number of slots to prune in a single operation
    #[arg(long)]
    max_prune: Option<u64>,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let mut stores = crate::common::open_data_stores(config).context("opening data stores")?;

    let max_slots = match args.max_slots {
        Some(x) => x,
        None => match config.storage.archive.max_history() {
            Some(x) => x,
            None => bail!("neither args or config provided for max_slots"),
        },
    };

    info!(max_slots, "prunning to max slots");

    stores
        .archive
        .prune_history(max_slots, args.max_prune)
        .into_diagnostic()
        .context("removing range from chain")?;

    // Compaction requires direct redb access
    match &mut stores.archive {
        ArchiveStoreBackend::Redb(s) => {
            let db = s.db_mut();

            while db.compact().into_diagnostic()? {
                info!("chain compaction round");
            }

            info!("chain segment trimmed");
        }
        ArchiveStoreBackend::NoOp(_) => {
            // No compaction needed for noop
            info!("noop archive, skipping compaction");
        }
    }

    Ok(())
}
