use itertools::Itertools;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;

use dolos::{adapters::ChainAdapter, prelude::*};

use crate::feedback::Feedback;

#[derive(Debug, clap::Args)]
pub struct Args {
    #[arg(long, default_value_t = 100)]
    pub chunk: usize,

    #[arg(short, long, action)]
    pub stdout: bool,
}

pub fn run(config: &crate::Config, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    let progress = feedback.slot_progress_bar();
    progress.set_message("rebuilding ledger");

    let archive = crate::common::open_chain_store(config)?;
    let blocks = archive
        .get_range(None, None)
        .into_diagnostic()
        .context("getting range")?;

    let schema = dolos_cardano::model::build_schema();
    let root = crate::common::ensure_storage_path(config)?;
    let state3_path = root.join("state");
    let state3 = dolos_redb3::StateStore::open(schema, state3_path, Some(5000))
        .into_diagnostic()
        .context("opening state3 db")?;

    let (tip, _) = archive
        .get_tip()
        .into_diagnostic()
        .context("finding archive tip")?
        .ok_or(miette::miette!("no archive tip found"))?;
    progress.set_length(tip);

    let mut slot = 0;
    let chain: ChainAdapter = config.chain.clone().unwrap_or_default().into();

    for chunk in blocks.chunks(args.chunk).into_iter() {
        let collected = chunk.collect_vec();
        let blocks: Vec<_> = collected
            .iter()
            .map(|(_, body)| MultiEraBlock::decode(body))
            .try_collect()
            .into_diagnostic()
            .context("decoding blocks")?;

        for block in blocks.iter() {
            let delta = chain
                .compute_apply_delta3(&state3, block)
                .into_diagnostic()
                .context("calculating state3 deltas.")?;

            state3
                .apply_delta(delta)
                .into_diagnostic()
                .context("applying delta")?;

            slot = block.slot();
        }
        progress.set_position(slot);

        if args.stdout {
            println!("progress: {} / {}, {:2}%", slot, tip, slot / tip * 100)
        }
    }

    Ok(())
}
