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

    let (_, tip) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    match tip {
        ChainPoint::Origin => feedback.global_pb.set_length(0),
        ChainPoint::Specific(slot, _) => feedback.global_pb.set_length(slot),
    }

    let remaining = wal
        .crawl_from(None)
        .into_diagnostic()
        .context("crawling wal")?
        .filter_forward()
        .into_blocks()
        .flatten();

    let mut last_hash = None;

    for block in remaining {
        let RawBlock {
            slot, hash, body, ..
        } = block;

        feedback
            .global_pb
            .set_message(format!("checking block {hash}"));

        let blockd = MultiEraBlock::decode(&body)
            .into_diagnostic()
            .context("decoding blocks")?;

        if let Some(last) = last_hash {
            if let Some(previous) = blockd.header().previous_hash() {
                assert_eq!(previous, last);
            }
        }

        last_hash = Some(hash);

        feedback.global_pb.set_position(slot);
    }

    println!("no integrity issues found in wal");

    Ok(())
}
