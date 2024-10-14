use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// the maximum number of slots to keep in the WAL
    #[arg(long)]
    max_slots: Option<u64>,

    /// the maximum number of slots to prune in a single operation
    #[arg(long)]
    max_prune: Option<u64>,
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (mut wal, _) = crate::common::open_data_stores(config).context("opening data stores")?;

    let max_slots = match args.max_slots {
        Some(x) => x,
        None => match config.storage.max_wal_history {
            Some(x) => x,
            None => bail!("neither args or config provided for max_slots"),
        },
    };

    info!(max_slots, "prunning to max slots");

    wal.prune_history(max_slots, args.max_prune)
        .into_diagnostic()
        .context("removing range from WAL")?;

    let db = wal.db_mut().unwrap();

    while db.compact().into_diagnostic()? {
        info!("wal compaction round");
    }

    info!("wal segment trimmed");

    Ok(())
}
