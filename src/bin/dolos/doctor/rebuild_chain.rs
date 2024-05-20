use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use miette::{Context, IntoDiagnostic};
use tracing::debug;

use crate::common::open_data_stores;

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
    let feedback = Feedback::default();

    let (wal, mut chain, _) = open_data_stores(config).context("opening data stores")?;

    let (tip, _) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding wal tip")?
        .ok_or(miette::miette!("wal is empty"))?;

    feedback.global_pb.set_length(tip);

    let remaining = wal.crawl_after(None);

    for block in remaining {
        let (slot, log) = block.into_diagnostic()?;

        debug!(slot, "filling up chain");

        if let pallas::storage::rolldb::wal::Log::Apply(slot, hash, body) = log {
            chain
                .roll_forward(slot, hash, body)
                .into_diagnostic()
                .context("rolling chain forward")?;

            feedback.global_pb.set_position(slot);
        };
    }

    Ok(())
}
