use dolos_core::{StateError, StateStore};
use miette::{Context, IntoDiagnostic};
use std::path::PathBuf;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// path to the new WAL db to create
    #[arg(long)]
    output: PathBuf,
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let progress = feedback.slot_progress_bar();
    progress.set_message("copying ledger ledger");

    let source = crate::common::open_ledger_store(config)?;

    let target = dolos_redb::state::LedgerStore::open(&args.output, None)
        .map_err(StateError::from)
        .into_diagnostic()
        .context("opening target ledger")?;
    let target = dolos::adapters::StateAdapter::Redb(target);

    let pb = feedback.indeterminate_progress_bar();
    pb.set_message("copying memory ledger into disc");

    source
        .copy(&target)
        .into_diagnostic()
        .context("copying into target")?;

    pb.abandon_with_message("ledger copy to disk finished");

    Ok(())
}
