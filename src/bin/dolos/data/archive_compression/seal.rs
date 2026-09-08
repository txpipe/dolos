//! Compress completed raw segments in place, one at a time, through the
//! store's verified transition.

use std::collections::HashSet;

use clap::ValueEnum;
use dolos_core::config::{CompressionProfile, RootConfig};
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::compressed::{FrameMode, Metadata, WriterOptions};
use dolos_fjall::flatfiles::{Representation, SegmentInfo};
use miette::{bail, IntoDiagnostic, WrapErr};
use serde::Serialize;

use super::dictionary;
use super::walk;
use super::{
    bytes, ensure_headroom, open_archive, scratch_for, segments_dir, setup_tracing, tip_segment,
    SegmentRange,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Profile {
    PerBlock,
    Chunked,
}

impl From<Profile> for CompressionProfile {
    fn from(profile: Profile) -> Self {
        match profile {
            Profile::PerBlock => CompressionProfile::PerBlock,
            Profile::Chunked => CompressionProfile::Chunked,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    range: SegmentRange,

    /// profile to seal with; defaults to
    /// storage.archive.block_compression.profile
    #[arg(long, value_enum)]
    profile: Option<Profile>,

    /// identity of the installed dictionary the per-block profile compresses
    /// with; defaults to storage.archive.block_compression.dictionary
    #[arg(long)]
    dictionary: Option<String>,

    /// frame target of the chunked profile, in bytes; defaults to
    /// storage.archive.block_compression.chunk_target
    #[arg(long)]
    chunk_target: Option<u32>,

    /// report what would be sealed without changing anything
    #[arg(long)]
    dry_run: bool,

    /// print the report as JSON
    #[arg(long)]
    json: bool,

    /// enable verbose logging
    #[arg(long)]
    verbose: bool,
}

/// What the run decided for one segment.
#[derive(Debug, Serialize)]
struct Outcome {
    segment: u32,
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bodies: Option<usize>,
    logical_len: u64,
    physical_len: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    frames: Option<u32>,
}

/// The profile a run seals with, resolved from the command line over the
/// configuration and loaded before any segment is touched.
struct Selection {
    profile: CompressionProfile,
    options: WriterOptions,
    dictionary: Option<String>,
}

fn describe(metadata: &Metadata) -> (String, Option<String>) {
    let profile = match metadata.mode {
        FrameMode::PerBlock => "per-block".to_string(),
        FrameMode::Chunked { target } => format!("chunked ({} frames)", bytes(target as u64)),
    };
    (profile, metadata.dictionary.map(|id| id.to_string()))
}

fn select(config: &RootConfig, args: &Args) -> miette::Result<Selection> {
    let compression = match &config.storage.archive {
        dolos_core::config::ArchiveStoreConfig::Fjall(cfg) => {
            cfg.block_compression.clone().map(|c| *c)
        }
        _ => None,
    }
    .unwrap_or_default();

    let profile = match args.profile {
        Some(profile) => profile.into(),
        None => match compression.profile() {
            Some(profile) => profile,
            None => bail!(
                "no sealing profile selected: pass --profile per-block|chunked or set \
                 storage.archive.block_compression.profile"
            ),
        },
    };

    match profile {
        CompressionProfile::PerBlock => {
            if args.chunk_target.is_some() {
                bail!("--chunk-target applies to the chunked profile only");
            }
            let id = match args
                .dictionary
                .as_deref()
                .or(compression.dictionary.as_deref())
            {
                Some(id) => dictionary::parse_id(id)?,
                None => bail!(
                    "the per-block profile needs a dictionary: pass --dictionary <identity> or \
                     set storage.archive.block_compression.dictionary (train one with `dolos data \
                     archive-compression train-dictionary`)"
                ),
            };
            let dictionaries = dictionary::dir(&segments_dir(config)?);
            let dictionary = dictionary::resolve(&dictionaries, &id, config.chain.magic())?;
            Ok(Selection {
                profile,
                options: WriterOptions::per_block().with_dictionary(dictionary),
                dictionary: Some(id.to_string()),
            })
        }
        CompressionProfile::Chunked => {
            if args.dictionary.is_some() {
                bail!("--dictionary applies to the per-block profile only");
            }
            let target = args
                .chunk_target
                .unwrap_or_else(|| compression.chunk_target());
            if target == 0 {
                bail!("--chunk-target must be greater than zero");
            }
            Ok(Selection {
                profile,
                options: WriterOptions::chunked(target),
                dictionary: None,
            })
        }
    }
}

/// The boundaries a seal cuts at: every body of the stream, checked against
/// the index so a location the index holds is always a whole body.
fn boundaries(archive: &ArchiveStore, info: &SegmentInfo) -> miette::Result<Vec<walk::Body>> {
    let segment = info.segment_id;
    let bodies = walk::walk(archive, segment, info.logical_len)?;
    let covered: HashSet<(u64, u32)> = bodies
        .iter()
        .map(|b| (b.location.offset, b.location.length))
        .collect();
    let indexed = archive
        .segment_locations(segment)
        .into_diagnostic()
        .wrap_err_with(|| format!("listing the indexed blocks of segment {segment:06}"))?;
    if let Some(stray) = indexed
        .iter()
        .find(|loc| !covered.contains(&(loc.offset, loc.length)))
    {
        bail!(
            "segment {segment:06}: the archive index points at offset {} length {}, which is \
             not a body of the segment stream; the index and the segment disagree, refusing \
             to seal",
            stray.offset,
            stray.length
        );
    }
    Ok(bodies)
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    setup_tracing(config, args.verbose)?;
    let segments = args.range.segments()?;

    let archive = open_archive(config, true)?;
    let selection = select(config, args)?;

    if let Some((tip_segment, tip_slot)) = tip_segment(&archive)? {
        if *segments.end() >= tip_segment {
            bail!(
                "segment {tip_segment:06} holds the archive tip (slot {tip_slot}) and is still \
                 being appended to; sealing stops at segment {:06}",
                tip_segment.saturating_sub(1)
            );
        }
    } else {
        bail!("the archive is empty; there is nothing to seal");
    }

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
        let Some(info) = infos.get(&segment) else {
            outcomes.push(report(
                args.json,
                Outcome {
                    segment,
                    action: "absent",
                    profile: None,
                    dictionary: None,
                    bodies: None,
                    logical_len: 0,
                    physical_len: 0,
                    frames: None,
                },
            ));
            continue;
        };

        if info.representation == Representation::Compressed {
            let (profile, dictionary) = info.metadata.as_ref().map(describe).unwrap_or_default();
            outcomes.push(report(
                args.json,
                Outcome {
                    segment,
                    action: "already-compressed",
                    profile: Some(profile),
                    dictionary,
                    bodies: None,
                    logical_len: info.logical_len,
                    physical_len: info.physical_len,
                    frames: None,
                },
            ));
            continue;
        }

        if info.logical_len == 0 {
            outcomes.push(report(
                args.json,
                Outcome {
                    segment,
                    action: "empty",
                    profile: None,
                    dictionary: None,
                    bodies: Some(0),
                    logical_len: 0,
                    physical_len: 0,
                    frames: None,
                },
            ));
            continue;
        }

        let bodies = boundaries(&archive, info)?;
        ensure_headroom(
            &dir,
            scratch_for(info.logical_len),
            &format!("sealing segment {segment:06}"),
        )?;

        if args.dry_run {
            outcomes.push(report(
                args.json,
                Outcome {
                    segment,
                    action: "would-seal",
                    profile: Some(selection.profile.to_string()),
                    dictionary: selection.dictionary.clone(),
                    bodies: Some(bodies.len()),
                    logical_len: info.logical_len,
                    physical_len: info.physical_len,
                    frames: None,
                },
            ));
            continue;
        }

        let locations: Vec<_> = bodies.iter().map(|b| b.location).collect();
        let summary = archive
            .seal_segment_at(segment, &locations, &selection.options)
            .into_diagnostic()
            .wrap_err_with(|| format!("sealing segment {segment:06}"))?;
        outcomes.push(report(
            args.json,
            Outcome {
                segment,
                action: "sealed",
                profile: Some(selection.profile.to_string()),
                dictionary: selection.dictionary.clone(),
                bodies: Some(bodies.len()),
                logical_len: summary.logical_len,
                physical_len: summary.physical_len,
                frames: Some(summary.frames),
            },
        ));
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&outcomes).into_diagnostic()?
        );
    }
    Ok(())
}

/// Print the outcome as a line unless the run reports JSON at the end.
fn report(json: bool, outcome: Outcome) -> Outcome {
    if !json {
        let segment = outcome.segment;
        let line = match outcome.action {
            "absent" => "no such segment in the archive".to_string(),
            "empty" => "empty, nothing to seal".to_string(),
            "already-compressed" => format!(
                "already compressed as {}{}: {} on disk for {}, left as is",
                outcome.profile.as_deref().unwrap_or("?"),
                with_dictionary(&outcome),
                bytes(outcome.physical_len),
                bytes(outcome.logical_len),
            ),
            "would-seal" => format!(
                "dry run: would seal {} bodies ({}) as {}{}",
                outcome.bodies.unwrap_or(0),
                bytes(outcome.logical_len),
                outcome.profile.as_deref().unwrap_or("?"),
                with_dictionary(&outcome),
            ),
            _ => format!(
                "sealed {} bodies as {}{}: {} -> {} ({:.1}%), {} frames",
                outcome.bodies.unwrap_or(0),
                outcome.profile.as_deref().unwrap_or("?"),
                with_dictionary(&outcome),
                bytes(outcome.logical_len),
                bytes(outcome.physical_len),
                100.0 * outcome.physical_len as f64 / outcome.logical_len.max(1) as f64,
                outcome.frames.unwrap_or(0),
            ),
        };
        println!("segment {segment:06}: {line}");
    }
    outcome
}

fn with_dictionary(outcome: &Outcome) -> String {
    match &outcome.dictionary {
        Some(id) => format!(" with dictionary {id}"),
        None => String::new(),
    }
}
