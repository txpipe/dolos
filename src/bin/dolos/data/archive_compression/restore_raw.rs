//! Restore compressed segments to raw in place, through the same verified
//! transition sealing uses, in the other direction.

use dolos_core::config::RootConfig;
use dolos_fjall::flatfiles::{Representation, SegmentInfo};
use miette::{IntoDiagnostic, WrapErr};
use serde::Serialize;

use super::{bytes, ensure_headroom, open_archive, scratch_for, setup_tracing, SegmentRange};

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    range: SegmentRange,

    /// report what would be restored without changing anything
    #[arg(long)]
    dry_run: bool,

    /// print the report as JSON
    #[arg(long)]
    json: bool,

    /// enable verbose logging
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Serialize)]
struct Outcome {
    segment: u32,
    action: &'static str,
    logical_len: u64,
    physical_len_before: u64,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    setup_tracing(config, args.verbose)?;
    let segments = args.range.segments()?;

    let archive = open_archive(config, true)?;
    let infos: std::collections::BTreeMap<u32, SegmentInfo> = archive
        .segments()
        .into_diagnostic()
        .wrap_err("listing the segments")?
        .into_iter()
        .map(|info| (info.segment_id, info))
        .collect();
    let dir = archive.segments_dir().to_path_buf();

    let mut outcomes = Vec::new();
    for segment in segments {
        let outcome = match infos.get(&segment) {
            None => Outcome {
                segment,
                action: "absent",
                logical_len: 0,
                physical_len_before: 0,
            },
            Some(info) if info.representation == Representation::Raw => Outcome {
                segment,
                action: "already-raw",
                logical_len: info.logical_len,
                physical_len_before: info.physical_len,
            },
            Some(info) => {
                ensure_headroom(
                    &dir,
                    scratch_for(info.logical_len),
                    &format!("restoring segment {segment:06}"),
                )?;
                if args.dry_run {
                    Outcome {
                        segment,
                        action: "would-restore",
                        logical_len: info.logical_len,
                        physical_len_before: info.physical_len,
                    }
                } else {
                    archive
                        .thaw_segment(segment)
                        .into_diagnostic()
                        .wrap_err_with(|| format!("restoring segment {segment:06} to raw"))?;
                    Outcome {
                        segment,
                        action: "restored",
                        logical_len: info.logical_len,
                        physical_len_before: info.physical_len,
                    }
                }
            }
        };
        if !args.json {
            let line = match outcome.action {
                "absent" => "no such segment in the archive".to_string(),
                "already-raw" => {
                    format!("already raw ({}), left as is", bytes(outcome.logical_len))
                }
                "would-restore" => format!(
                    "dry run: would restore {} on disk to {} raw",
                    bytes(outcome.physical_len_before),
                    bytes(outcome.logical_len)
                ),
                _ => format!(
                    "restored to raw: {} on disk -> {}",
                    bytes(outcome.physical_len_before),
                    bytes(outcome.logical_len)
                ),
            };
            println!("segment {segment:06}: {line}");
        }
        outcomes.push(outcome);
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&outcomes).into_diagnostic()?
        );
    }
    Ok(())
}
