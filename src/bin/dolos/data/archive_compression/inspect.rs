//! Describe a segments directory from its files alone: which representation
//! each segment has, what a compressed one was sealed with and whether its
//! dictionary is installed, and what an interrupted transition left behind
//! — without opening a store, so nothing is recovered or rewritten and a
//! running node is not disturbed.

use std::fs::File;
use std::io;

use dolos_core::config::{ArchiveCompressionConfig, ArchiveStoreConfig, RootConfig};
use dolos_fjall::flatfiles::compressed::{
    DictionaryDir, DictionarySource as _, FrameMode, SegmentIndex,
};
use dolos_fjall::flatfiles::{
    Access, FlatFileStore, Found, Lease, Representation, SegmentPaths, Transition,
};
use miette::{IntoDiagnostic, WrapErr};
use serde::Serialize;

use super::dictionary::{self, Installed};
use super::{bytes, segments_dir};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// first segment number to describe (inclusive)
    #[arg(long)]
    from: Option<u32>,

    /// last segment number to describe (inclusive)
    #[arg(long)]
    to: Option<u32>,

    /// print the report as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    segments_dir: String,
    /// Whether another process holds the directory exclusively right now.
    maintenance_in_progress: bool,
    config: ArchiveCompressionConfig,
    dictionaries: Vec<Installed>,
    segments: Vec<Segment>,
    pending: Vec<Pending>,
    totals: Totals,
}

#[derive(Debug, Serialize)]
struct Segment {
    segment: u32,
    representation: String,
    logical_len: u64,
    physical_len: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frames: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<String>,
}

#[derive(Debug, Serialize)]
struct Pending {
    segment: u32,
    description: String,
}

#[derive(Debug, Default, Serialize)]
struct Totals {
    segments: usize,
    raw: usize,
    compressed: usize,
    logical_len: u64,
    physical_len: u64,
}

/// The recovery table, read without acting on it.
fn authority(found: &Found) -> Result<Option<Representation>, String> {
    match found.transition {
        None => match (found.raw, found.compressed) {
            (true, true) => Err(
                "both a raw and a compressed file and no transition record; the store refuses \
                 to open until one is removed"
                    .to_string(),
            ),
            (true, false) => Ok(Some(Representation::Raw)),
            (false, true) => Ok(Some(Representation::Compressed)),
            (false, false) => Ok(None),
        },
        Some(transition) => {
            let has = |r: Representation| match r {
                Representation::Raw => found.raw,
                Representation::Compressed => found.compressed,
            };
            if has(transition.to()) {
                Ok(Some(transition.to()))
            } else if has(transition.from()) {
                Ok(Some(transition.from()))
            } else {
                Err(format!(
                    "a {transition} record and neither representation file; the segment is lost"
                ))
            }
        }
    }
}

fn pending(segment: u32, found: &Found) -> Option<Pending> {
    let staged = match (found.raw_staging, found.compressed_staging) {
        (true, true) => Some("staged raw and compressed outputs"),
        (true, false) => Some("a staged raw output"),
        (false, true) => Some("a staged compressed output"),
        (false, false) => None,
    };
    let description = match (found.transition, staged) {
        (Some(transition), staged) => {
            let published = match transition {
                Transition::Seal => found.compressed,
                Transition::Thaw => found.raw,
            };
            let outcome = if published {
                format!(
                    "the {} file is published and authoritative; the next open retires the {} \
                     file and the record",
                    transition.to(),
                    transition.from()
                )
            } else {
                format!(
                    "the {} file stays authoritative; the next open discards {} and the record",
                    transition.from(),
                    staged.unwrap_or("nothing")
                )
            };
            format!("interrupted {transition}: {outcome}")
        }
        (None, Some(staged)) => {
            format!("{staged} without a transition record; the next open discards it")
        }
        (None, None) if found.transition_staging => {
            "a staged transition record; the next open discards it".to_string()
        }
        (None, None) => return None,
    };
    Some(Pending {
        segment,
        description,
    })
}

fn describe(
    id: u32,
    found: &Found,
    paths: &SegmentPaths,
    dictionaries: &DictionaryDir,
) -> Option<Segment> {
    let representation = match authority(found) {
        Ok(Some(representation)) => representation,
        Ok(None) => return None,
        Err(problem) => {
            return Some(Segment {
                segment: id,
                representation: "unresolved".to_string(),
                logical_len: 0,
                physical_len: 0,
                profile: None,
                level: None,
                frames: None,
                dictionary: None,
                dictionary_status: None,
                problem: Some(problem),
            })
        }
    };
    let path = paths.representation(representation);
    let mut segment = Segment {
        segment: id,
        representation: representation.to_string(),
        logical_len: 0,
        physical_len: 0,
        profile: None,
        level: None,
        frames: None,
        dictionary: None,
        dictionary_status: None,
        problem: None,
    };
    let parsed: io::Result<()> = (|| {
        let file = File::open(&path)?;
        let physical_len = file.metadata()?.len();
        segment.physical_len = physical_len;
        match representation {
            Representation::Raw => segment.logical_len = physical_len,
            Representation::Compressed => {
                let index = SegmentIndex::parse(&file)?;
                let metadata = index.metadata();
                segment.logical_len = index.logical_len();
                segment.profile = Some(match metadata.mode {
                    FrameMode::PerBlock => "per-block".to_string(),
                    FrameMode::Chunked { target } => format!("chunked ({})", bytes(target as u64)),
                });
                segment.level = Some(metadata.level);
                segment.frames = Some(index.frames().len().saturating_sub(1));
                if let Some(dictionary) = metadata.dictionary {
                    segment.dictionary = Some(dictionary.to_string());
                    segment.dictionary_status = Some(match dictionaries.dictionary(&dictionary) {
                        Ok(Some(_)) => "installed".to_string(),
                        Ok(None) => "missing: the store refuses to open".to_string(),
                        Err(e) => format!("unusable: {e}"),
                    });
                }
            }
        }
        Ok(())
    })();
    if let Err(e) = parsed {
        segment.problem = Some(format!("{}: {e}", path.display()));
    }
    Some(segment)
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let dir = segments_dir(config)?;
    let compression = match &config.storage.archive {
        ArchiveStoreConfig::Fjall(cfg) => cfg.compression.clone().map(|c| *c),
        _ => None,
    }
    .unwrap_or_default();

    let lease = match Lease::acquire(&dir, Access::Shared) {
        Ok(lease) => Some(lease),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => None,
        Err(e) => {
            return Err(e)
                .into_diagnostic()
                .wrap_err_with(|| format!("taking the lease on {}", dir.display()))
        }
    };

    let dictionaries = dictionary::dir(&dir);
    let scanned = FlatFileStore::scan(&dir)
        .into_diagnostic()
        .wrap_err_with(|| format!("scanning {}", dir.display()))?;

    let mut segments = Vec::new();
    let mut pending_work = Vec::new();
    let mut totals = Totals::default();
    for (id, found) in &scanned {
        if args.from.is_some_and(|from| *id < from) || args.to.is_some_and(|to| *id > to) {
            continue;
        }
        if let Some(pending) = pending(*id, found) {
            pending_work.push(pending);
        }
        let paths = SegmentPaths::new(&dir, *id);
        if let Some(segment) = describe(*id, found, &paths, &dictionaries) {
            totals.segments += 1;
            match segment.representation.as_str() {
                "raw" => totals.raw += 1,
                "compressed" => totals.compressed += 1,
                _ => {}
            }
            totals.logical_len += segment.logical_len;
            totals.physical_len += segment.physical_len;
            segments.push(segment);
        }
    }

    let report = Report {
        segments_dir: dir.display().to_string(),
        maintenance_in_progress: lease.is_none(),
        config: compression,
        dictionaries: dictionary::installed(&dictionaries)?,
        segments,
        pending: pending_work,
        totals,
    };
    drop(lease);

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).into_diagnostic()?
        );
        return Ok(());
    }

    println!("segments directory: {}", report.segments_dir);
    if report.maintenance_in_progress {
        println!("  held exclusively by another process: files may be in motion");
    }
    println!(
        "config: profile {}, dictionary {}, chunk target {}",
        report
            .config
            .profile()
            .map(|p| p.to_string())
            .unwrap_or_else(|| "none".to_string()),
        report.config.dictionary.as_deref().unwrap_or("none"),
        bytes(report.config.chunk_target() as u64)
    );

    println!("dictionaries ({}):", report.dictionaries.len());
    for d in &report.dictionaries {
        let mut line = format!("  {}  {}", d.id, bytes(d.bytes));
        match &d.manifest {
            Some(m) => line.push_str(&format!(
                "  network {}  segments {:06}-{:06}, {} samples, seed {}",
                m.network_magic, m.from_segment, m.to_segment, m.samples, m.seed
            )),
            None => line.push_str("  no manifest"),
        }
        if let Some(problem) = &d.problem {
            line.push_str(&format!("  PROBLEM: {problem}"));
        }
        println!("{line}");
    }

    println!("segments ({}):", report.segments.len());
    for s in &report.segments {
        let mut line = format!("  {:06}  {:<10}", s.segment, s.representation);
        match s.representation.as_str() {
            "raw" => line.push_str(&format!("  {}", bytes(s.logical_len))),
            "compressed" => line.push_str(&format!(
                "  {}  {} -> {} ({:.1}%)  {} frames",
                s.profile.as_deref().unwrap_or("?"),
                bytes(s.logical_len),
                bytes(s.physical_len),
                100.0 * s.physical_len as f64 / s.logical_len.max(1) as f64,
                s.frames.unwrap_or(0)
            )),
            _ => {}
        }
        if let Some(d) = &s.dictionary {
            line.push_str(&format!(
                "  dictionary {d} ({})",
                s.dictionary_status.as_deref().unwrap_or("?")
            ));
        }
        if let Some(problem) = &s.problem {
            line.push_str(&format!("  PROBLEM: {problem}"));
        }
        println!("{line}");
    }

    if !report.pending.is_empty() {
        println!("pending ({}):", report.pending.len());
        for p in &report.pending {
            println!("  {:06}  {}", p.segment, p.description);
        }
    }

    println!(
        "totals: {} segments ({} raw, {} compressed), {} of blocks on {} of disk",
        report.totals.segments,
        report.totals.raw,
        report.totals.compressed,
        bytes(report.totals.logical_len),
        bytes(report.totals.physical_len)
    );
    Ok(())
}
