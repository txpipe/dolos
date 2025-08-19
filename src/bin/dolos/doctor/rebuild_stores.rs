use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use std::sync::Arc;
use tracing::debug;

use dolos::cardano::mutable_slots;
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

    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);

    if domain
        .state
        .is_empty()
        .into_diagnostic()
        .context("checking empty state")?
    {
        debug!("importing genesis");

        let delta = dolos::cardano::compute_origin_delta(&genesis);

        domain
            .state
            .apply(&[delta])
            .into_diagnostic()
            .context("applying origin utxos")?;
    }

    let (_, tip) = domain
        .wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    match tip {
        ChainPoint::Origin => progress.set_length(0),
        ChainPoint::Specific(slot, _) => progress.set_length(slot),
    }

    // Amount of slots until unmutability is guaranteed.
    let lookahead = mutable_slots(&genesis);
    let remaining = WalBlockReader::try_new(&domain.wal, None, lookahead)
        .into_diagnostic()
        .context("creating wal block reader")?;

    for chunk in remaining.chunks(args.chunk).into_iter() {
        let collected = chunk.collect_vec();
        if let Err(err) = domain.apply_blocks(&collected) {
            miette::bail!("failed to apply block chunk: {}", err);
        }
        collected.last().inspect(|b| progress.set_position(b.slot));
    }

    Ok(())
}
