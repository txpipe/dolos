use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use tracing::debug;

use dolos::prelude::*;

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(short, long, default_value_t = 500)]
    pub chunk: usize,
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding stores");

    let domain = crate::common::setup_domain(config)?;

    let (tip, _) = domain
        .wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    progress.set_length(tip.slot());

    let remaining = domain
        .wal
        .iter_blocks(None, None)
        .into_diagnostic()
        .context("iterating over wal blocks")?;

    for chunk in remaining.chunks(args.chunk).into_iter() {
        let collected = chunk.into_iter().map(|(_, x)| x).collect_vec();

        let Ok(cursor) = dolos_core::catchup::import_batch(&domain, collected) else {
            miette::bail!("failed to apply block chunk");
        };

        progress.set_position(cursor);
    }

    Ok(())
}
