use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use miette::{Context, IntoDiagnostic};
use pallas::ledger::traverse::MultiEraBlock;

use dolos::prelude::*;

struct Feedback {
    _multi: MultiProgress,
    global_pb: ProgressBar,
}

impl Default for Feedback {
    fn default() -> Self {
        let multi = MultiProgress::new();

        let global_pb = ProgressBar::new_spinner();
        let global_pb = multi.add(global_pb);
        global_pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} (eta: {eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        Self {
            _multi: multi,
            global_pb,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    //crate::common::setup_tracing(&config.logging)?;

    let feedback = Feedback::default();

    let wal = crate::common::open_wal_store(config)?;

    let (tip, _) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    feedback.global_pb.set_length(tip.slot());

    let remaining = wal
        .iter_blocks(None, None)
        .into_diagnostic()
        .context("crawling wal")?;

    let mut last_hash = None;

    for (point, block) in remaining {
        let slot = point.slot();
        let hash = point.hash();
        let body = block;

        feedback
            .global_pb
            .set_message(format!("checking block {hash:?}"));

        let blockd = MultiEraBlock::decode(&body)
            .into_diagnostic()
            .context("decoding blocks")?;

        if let Some(last) = last_hash {
            let previous = blockd.header().previous_hash();
            assert_eq!(previous, last);
        }

        last_hash = Some(hash);

        feedback.global_pb.set_position(slot);
    }

    println!("no integrity issues found in wal");

    Ok(())
}
