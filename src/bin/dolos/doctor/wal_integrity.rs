use dolos_core::{
    config::RootConfig,
    wal::{verify_integrity_with, IntegrityFailure},
    WalStore,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use miette::{Context, IntoDiagnostic};
use pallas::crypto::hash::Hash;

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

fn fmt_hash(h: Option<Hash<32>>) -> String {
    match h {
        Some(h) => hex::encode(h),
        None => "none".to_string(),
    }
}

pub fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    let feedback = Feedback::default();

    let wal = crate::common::open_wal_store(config)?;

    let (tip, _) = wal
        .find_tip()
        .into_diagnostic()
        .context("finding WAL tip")?
        .ok_or(miette::miette!("no WAL tip found"))?;

    feedback.global_pb.set_length(tip.slot());

    let pb = feedback.global_pb.clone();
    let report = verify_integrity_with(&wal, |slot| {
        pb.set_position(slot);
        pb.set_message(format!("checking slot {slot}"));
    })
    .into_diagnostic()
    .context("crawling wal")?;

    let mut discontinuities = 0;

    for failure in &report.failures {
        match failure {
            IntegrityFailure::Discontinuity {
                slot,
                block_hash,
                expected_prev,
                got_prev,
            } => {
                discontinuities += 1;
                eprintln!(
                    "discontinuity at slot {slot}: block {}, expected prev {}, got prev {}",
                    fmt_hash(*block_hash),
                    fmt_hash(*expected_prev),
                    fmt_hash(*got_prev),
                );
            }
            IntegrityFailure::Undecodable { slot, error } => {
                return Err(miette::miette!("decoding block at slot {slot}: {error}"));
            }
        }
    }

    if discontinuities == 0 {
        println!("no integrity issues found in wal");
        return Ok(());
    }

    Err(miette::miette!(
        "found {discontinuities} integrity issue(s) in wal"
    ))
}
