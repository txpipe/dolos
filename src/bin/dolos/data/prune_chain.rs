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

    /// the maximum number of rows to remove per call; the command repeats
    /// calls until the chain is pruned
    #[arg(long)]
    max_prune_rows: Option<u64>,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging, &config.telemetry)?;

    let mut stores = crate::common::open_data_stores(config).context("opening data stores")?;

    let max_slots = match args.max_slots {
        Some(x) => x,
        None => match config.sync.max_history {
            Some(x) => x,
            None => bail!("neither args or config provided for max_slots"),
        },
    };

    info!(max_slots, "prunning to max slots");

    // Each call runs to its budget and reports whether work is left. Calls
    // repeat within this process because the index sweep resumes from a
    // cursor that does not outlive it.
    let mut rounds = 1u64;
    while !stores
        .archive
        .prune_history(max_slots, args.max_prune_rows)
        .into_diagnostic()
        .context("removing range from chain")?
    {
        rounds += 1;
    }

    info!(rounds, "chain pruned");

    // Compaction requires direct backend access
    match &mut stores.archive {
        ArchiveStoreBackend::LogsOnly(_) => {
            bail!("chain compaction needs exclusive access to the archive database")
        }
        ArchiveStoreBackend::Fjall(s) => {
            // Background compaction would reclaim the space eventually; a
            // major compaction makes the prune's effect immediate, which is
            // what the command promises.
            s.compact().into_diagnostic()?;

            info!("chain segment trimmed");
        }
        ArchiveStoreBackend::Memory(_) | ArchiveStoreBackend::NoOp(_) => {
            // Nothing on disk to reclaim.
            info!("ephemeral archive, skipping compaction");
        }
    }

    Ok(())
}
