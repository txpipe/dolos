use dolos::wal::redb::WalStore;
use miette::{bail, Context, IntoDiagnostic};
use tracing::info;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args, Default)]
pub struct Args {
    /// Skip the bootstrap if there's already data in the stores
    #[arg(long, action)]
    skip_if_not_empty: bool,
}

fn open_empty_wal(config: &crate::Config, args: &Args) -> miette::Result<Option<WalStore>> {
    let wal = crate::common::open_wal(config)?;

    let is_empty = wal.is_empty().into_diagnostic()?;

    if !is_empty {
        if args.skip_if_not_empty {
            return Ok(None);
        } else {
            bail!("can't continue with data already available");
        }
    }

    Ok(Some(wal))
}

pub fn run(config: &crate::Config, args: &Args, _feedback: &Feedback) -> miette::Result<()> {
    match open_empty_wal(config, args).context("opening WAL")? {
        Some(mut wal) => {
            wal.initialize_from_origin()
                .into_diagnostic()
                .context("initializing WAL")?;

            Ok(())
        }
        None => {
            info!("Skipping bootstrap, data already present.");
            Ok(())
        }
    }
}
