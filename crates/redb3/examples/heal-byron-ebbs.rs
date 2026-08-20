//! One-time recovery scan for Byron epoch-boundary blocks lost to slot-key
//! overwrite.
//!
//! Every archive written before a slot could index more than one block kept
//! one location per slot. A Byron EBB shares its absolute slot with the first
//! main block of the epoch it opens, so the main block overwrote the EBB's
//! index row on every Byron epoch — 208 of them on mainnet, 4 on preprod, none
//! on preview. The EBB's *bytes* were never lost: segment files are
//! append-only and the EBB was appended first, so each orphaned block sits in
//! the offset gap between two indexed blocks. This scan finds those gaps and
//! puts the missing locations back into their slots.
//!
//! **This is not a product surface.** The shipped `dolos` binary offers no
//! repair command, no startup hook and no lazy heal: an operator on a pre-fix
//! archive re-syncs from genesis or restores from a stele cut on a healed
//! archive. This program exists for exactly one archive — the publisher's own
//! local snapshot, which the published steles are generated from and which
//! therefore has to be healed before the next stele is cut. It is an example
//! target, built by `cargo build --examples` and reachable no other way.
//!
//! # Guard
//!
//! Restore's redo path deliberately leaves superseded bodies behind in segment
//! files, so a gap is not automatically an orphaned EBB. A gap is accepted
//! only when it holds exactly one CBOR item, the era probe calls that item an
//! epoch-boundary block, and it chains: its `prev_block` is the hash of the
//! indexed block before the gap, and the indexed block after the gap names the
//! recovered block as its parent. A gap that fails any of these is **refused**
//! — reported, and the whole run leaves the archive untouched. Nothing here
//! skips a gap quietly: a wrong insert would be baked into every stele cut
//! from this archive and would reach every operator who bootstraps from one.
//!
//! # Usage
//!
//!     cargo run -p dolos-redb3 --example heal-byron-ebbs -- \
//!         <archive-dir> [options]
//!
//!     --blocks-dir <dir>   segment files live here (config `blocks_path`);
//!                          defaults to <archive-dir>
//!     --commit             write the recovered rows; without it the scan is
//!                          read-only and only reports what it found
//!
//! Run it twice: the second run finds nothing left to recover, because the
//! gaps it healed are no longer gaps.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pallas::ledger::traverse::{probe, MultiEraBlock};
use redb::{Database, ReadableDatabase as _, ReadableTable as _};

use dolos_redb3::archive::flatfiles::{
    decode_locations, encode_locations, BlockLocation, FlatFileStore,
};
use dolos_redb3::archive::tables::BlocksTable;

type BoxError = Box<dyn std::error::Error>;

/// An indexed block, as the scan needs to see it: where its bytes are, and
/// what it says about the chain around it.
struct Indexed {
    location: BlockLocation,
    slot: u64,
    hash: pallas::crypto::hash::Hash<32>,
    prev: Option<pallas::crypto::hash::Hash<32>>,
    shares_slot: bool,
}

/// A stretch of segment bytes no index row points at.
struct Gap {
    segment_id: u32,
    start: u64,
    end: u64,
}

/// A gap the guard accepted: an orphaned epoch-boundary block, ready to index.
struct Recovered {
    slot: u64,
    location: BlockLocation,
    hash: pallas::crypto::hash::Hash<32>,
}

/// What one run of the scan found.
struct Report {
    recovered: Vec<Recovered>,
    refused: Vec<(Gap, String)>,
    indexed: usize,
    already_shared: usize,
    gaps: usize,
    committed: bool,
}

fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);

    let mut archive_dir: Option<PathBuf> = None;
    let mut blocks_dir: Option<PathBuf> = None;
    let mut commit = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--commit" => commit = true,
            "--blocks-dir" => {
                blocks_dir = Some(PathBuf::from(
                    args.next().ok_or("--blocks-dir needs a directory")?,
                ))
            }
            "-h" | "--help" => {
                eprintln!("usage: heal-byron-ebbs <archive-dir> [--blocks-dir <dir>] [--commit]");
                return Ok(());
            }
            other if archive_dir.is_none() => archive_dir = Some(PathBuf::from(other)),
            other => return Err(format!("unexpected argument: {other}").into()),
        }
    }

    let archive_dir = archive_dir.ok_or("usage: heal-byron-ebbs <archive-dir> [--commit]")?;
    let blocks_dir = blocks_dir.unwrap_or_else(|| archive_dir.clone());

    let report = heal(&archive_dir, &blocks_dir, commit)?;

    println!(
        "indexed blocks: {} ({} sharing a slot)",
        report.indexed, report.already_shared
    );
    println!("segment gaps: {}", report.gaps);

    for (gap, reason) in &report.refused {
        eprintln!(
            "REFUSED segment {} bytes {}..{} ({} bytes): {reason}",
            gap.segment_id,
            gap.start,
            gap.end,
            gap.end - gap.start
        );
    }

    for block in &report.recovered {
        println!(
            "recovered epoch-boundary block {} at slot {} (segment {}, offset {}, {} bytes)",
            block.hash,
            block.slot,
            block.location.segment_id,
            block.location.offset,
            block.location.length
        );
    }

    if !report.refused.is_empty() {
        return Err(format!(
            "{} gap(s) could not be classified as orphaned epoch-boundary blocks; \
             the archive was left untouched",
            report.refused.len()
        )
        .into());
    }

    if report.recovered.is_empty() {
        println!("nothing to recover; the archive already holds every epoch-boundary block");
    } else if report.committed {
        println!("indexed {} epoch-boundary block(s)", report.recovered.len());
    } else {
        println!(
            "{} block(s) would be indexed; re-run with --commit to write them",
            report.recovered.len()
        );
    }

    Ok(())
}

/// Scan `archive_dir` for orphaned epoch-boundary blocks, and index them when
/// `commit` is set and every gap was classified.
fn heal(archive_dir: &Path, blocks_dir: &Path, commit: bool) -> Result<Report, BoxError> {
    let db = Database::builder().open(archive_dir.join("index"))?;
    let flatfiles = FlatFileStore::new(blocks_dir)?;

    let indexed = read_index(&db, &flatfiles)?;
    let already_shared = indexed.values().filter(|block| block.shares_slot).count();
    let gaps = find_gaps(blocks_dir, &indexed)?;

    let mut recovered = Vec::new();
    let mut refused = Vec::new();

    for gap in gaps {
        match classify(&flatfiles, &indexed, &gap) {
            Ok(block) => recovered.push(block),
            Err(reason) => refused.push((gap, reason)),
        }
    }

    let mut report = Report {
        indexed: indexed.len(),
        already_shared,
        gaps: recovered.len() + refused.len(),
        recovered,
        refused,
        committed: false,
    };

    // One bad gap stops the whole run: the point of the guard is that this
    // archive feeds every published stele, so a partial heal under an
    // unexplained gap is worse than no heal at all.
    if commit && report.refused.is_empty() && !report.recovered.is_empty() {
        let wx = db.begin_write()?;
        {
            let mut table = wx.open_table(BlocksTable::DEF)?;

            for block in &report.recovered {
                // The list a slot holds is newest first, and a recovered EBB
                // is the oldest block at its slot — it is what the epoch's
                // first main block was written over — so it goes last.
                let stored = table.get(block.slot)?.map(|value| value.value().to_vec());
                let mut locations: Vec<BlockLocation> = stored
                    .as_deref()
                    .map(|bytes| decode_locations(bytes).collect())
                    .unwrap_or_default();

                locations.push(block.location);

                table.insert(block.slot, encode_locations(&locations).as_slice())?;
            }
        }
        wx.commit()?;

        report.committed = true;
    }

    Ok(report)
}

/// Read every indexed block and decode what it says about its neighbours.
///
/// Keyed by segment position rather than by slot, because what the scan needs
/// is the order the bytes were written in — which is the order the chain was
/// applied in, and the only thing that makes a gap's neighbours meaningful.
/// A slot that already holds more than one block contributes each of them.
fn read_index(
    db: &Database,
    flatfiles: &FlatFileStore,
) -> Result<BTreeMap<(u32, u64), Indexed>, BoxError> {
    let rx = db.begin_read()?;
    let mut out = BTreeMap::new();

    let table = rx.open_table(BlocksTable::DEF)?;

    for entry in table.iter()? {
        let (slot, value) = entry?;
        let locations: Vec<BlockLocation> = decode_locations(value.value()).collect();
        let shares_slot = locations.len() > 1;

        for location in locations {
            let body = flatfiles.read(&location)?;
            let decoded = MultiEraBlock::decode(&body)?;

            out.insert(
                (location.segment_id, location.offset),
                Indexed {
                    location,
                    slot: slot.value(),
                    hash: decoded.hash(),
                    prev: decoded.header().previous_hash(),
                    shares_slot,
                },
            );
        }
    }

    Ok(out)
}

/// Walk every segment file and report the byte ranges no index row covers.
fn find_gaps(
    blocks_dir: &Path,
    indexed: &BTreeMap<(u32, u64), Indexed>,
) -> Result<Vec<Gap>, BoxError> {
    let mut segments: BTreeMap<u32, u64> = BTreeMap::new();

    for entry in std::fs::read_dir(blocks_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(id) = name.strip_suffix(".segment") else {
            continue;
        };
        let Ok(id) = id.parse::<u32>() else { continue };

        segments.insert(id, entry.metadata()?.len());
    }

    let mut gaps = Vec::new();

    for (&segment_id, &file_len) in &segments {
        let mut cursor = 0u64;

        for (_, block) in indexed.range((segment_id, 0)..(segment_id + 1, 0)) {
            if block.location.offset > cursor {
                gaps.push(Gap {
                    segment_id,
                    start: cursor,
                    end: block.location.offset,
                });
            }

            cursor = block.location.offset + block.location.length as u64;
        }

        if file_len > cursor {
            gaps.push(Gap {
                segment_id,
                start: cursor,
                end: file_len,
            });
        }
    }

    Ok(gaps)
}

/// Decide whether a gap holds one orphaned epoch-boundary block, or refuse it.
fn classify(
    flatfiles: &FlatFileStore,
    indexed: &BTreeMap<(u32, u64), Indexed>,
    gap: &Gap,
) -> Result<Recovered, String> {
    let length = gap.end - gap.start;

    let length: u32 = length
        .try_into()
        .map_err(|_| format!("{length} bytes is larger than any block"))?;

    let location = BlockLocation {
        segment_id: gap.segment_id,
        offset: gap.start,
        length,
    };

    let body = flatfiles
        .read(&location)
        .map_err(|err| format!("unreadable: {err}"))?;

    // The gap must be exactly one CBOR item. More than one, or a trailing
    // fragment, means these bytes are not a single orphaned block.
    let mut decoder = pallas::codec::minicbor::Decoder::new(&body);
    decoder
        .skip()
        .map_err(|err| format!("does not decode as CBOR: {err}"))?;

    if decoder.position() != body.len() {
        return Err(format!(
            "holds {} bytes beyond the first CBOR item",
            body.len() - decoder.position()
        ));
    }

    if !matches!(probe::block_era(&body), probe::Outcome::EpochBoundary) {
        return Err("is not an epoch-boundary block".to_string());
    }

    let decoded =
        MultiEraBlock::decode(&body).map_err(|err| format!("does not decode as a block: {err}"))?;

    // The gap's neighbours in write order. The predecessor is absent only for
    // the very first bytes of the archive, which is where the genesis EBB
    // sits; the successor is what the chain check needs above all, since it is
    // the block whose parent the recovered block claims to be.
    let predecessor = indexed
        .range(..(gap.segment_id, gap.start))
        .next_back()
        .map(|(_, block)| block);

    let successor = indexed
        .range((gap.segment_id, gap.end)..)
        .next()
        .map(|(_, block)| block);

    let Some(successor) = successor else {
        return Err("has no indexed block after it to chain onto".to_string());
    };

    if let Some(predecessor) = predecessor {
        if decoded.header().previous_hash() != Some(predecessor.hash) {
            return Err(format!(
                "names {:?} as its parent, but the block before it is {}",
                decoded.header().previous_hash(),
                predecessor.hash
            ));
        }
    }

    if successor.prev != Some(decoded.hash()) {
        return Err(format!(
            "is not the parent of the block after it: slot {} names {:?}",
            successor.slot, successor.prev
        ));
    }

    if successor.slot != decoded.slot() {
        return Err(format!(
            "sits at slot {} but the block after it is at slot {}; an \
             epoch-boundary block shares its slot with the block that follows it",
            decoded.slot(),
            successor.slot
        ));
    }

    Ok(Recovered {
        slot: decoded.slot(),
        location,
        hash: decoded.hash(),
    })
}

#[cfg(test)]
mod tests {
    use dolos_core::{ArchiveWriter as _, ChainPoint, RawBlock, StateSchema};
    use dolos_redb3::archive::ArchiveStore;
    use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev};

    use super::*;

    fn config() -> dolos_core::config::RedbArchiveConfig {
        dolos_core::config::RedbArchiveConfig::default()
    }

    /// Build an archive the way the pre-fix writer did: both blocks appended
    /// to the segment file, but only one index row per slot — so the epoch's
    /// first main block overwrites the EBB that shares its slot, and the EBB's
    /// bytes are left orphaned in the segment.
    fn broken_archive(dir: &Path, epoch: u64) -> ((ChainPoint, RawBlock), (ChainPoint, RawBlock)) {
        let head = make_conway_block_with_prev(byron_ebb_slot(epoch) - 10, None, 0);
        let ebb = make_byron_ebb(epoch, head.0.hash().unwrap());
        let main = make_conway_block_with_prev(byron_ebb_slot(epoch), ebb.0.hash(), 1);

        let flatfiles = FlatFileStore::new(dir).unwrap();
        let locations = flatfiles
            .append_batch(&[
                (
                    BlockLocation::segment_for_slot(head.0.slot()),
                    head.1.as_slice(),
                ),
                (
                    BlockLocation::segment_for_slot(ebb.0.slot()),
                    ebb.1.as_slice(),
                ),
                (
                    BlockLocation::segment_for_slot(main.0.slot()),
                    main.1.as_slice(),
                ),
            ])
            .unwrap();

        let db = Database::builder().create(dir.join("index")).unwrap();
        let wx = db.begin_write().unwrap();
        {
            let mut table = wx.open_table(BlocksTable::DEF).unwrap();
            table
                .insert(head.0.slot(), locations[0].to_bytes().as_slice())
                .unwrap();
            // Both of these carry the boundary slot; the second wins, which is
            // the whole defect.
            table
                .insert(ebb.0.slot(), locations[1].to_bytes().as_slice())
                .unwrap();
            table
                .insert(main.0.slot(), locations[2].to_bytes().as_slice())
                .unwrap();
        }
        wx.commit().unwrap();
        drop(db);

        (ebb, main)
    }

    fn archived_bodies(dir: &Path) -> Vec<Vec<u8>> {
        let store = ArchiveStore::open(StateSchema::default(), dir, &config()).unwrap();
        let bodies = store
            .get_range(None, None)
            .unwrap()
            .map(|(_, body)| body)
            .collect();

        store.shutdown().unwrap();
        bodies
    }

    #[test]
    fn an_orphaned_ebb_is_recovered_and_a_second_run_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let (ebb, main) = broken_archive(dir.path(), 1);

        // Before the scan, the archive skips straight past the EBB.
        let before = archived_bodies(dir.path());
        assert_eq!(before.len(), 2);
        assert!(!before.contains(&ebb.1.as_ref().clone()));

        let report = heal(dir.path(), dir.path(), true).unwrap();
        assert!(report.refused.is_empty(), "{:?}", report.refused.len());
        assert_eq!(report.recovered.len(), 1);
        assert_eq!(report.recovered[0].slot, ebb.0.slot());
        assert!(report.committed);

        // The archive now yields both blocks of the boundary, in chain order,
        // with no re-import.
        let after = archived_bodies(dir.path());
        assert_eq!(
            after[1..],
            [ebb.1.as_ref().clone(), main.1.as_ref().clone()]
        );

        // Running it again finds nothing: the gap it healed is no longer one.
        let report = heal(dir.path(), dir.path(), true).unwrap();
        assert_eq!(report.gaps, 0);
        assert!(report.recovered.is_empty());
        assert!(!report.committed);

        assert_eq!(archived_bodies(dir.path()), after);
    }

    #[test]
    fn a_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (ebb, _) = broken_archive(dir.path(), 1);

        let report = heal(dir.path(), dir.path(), false).unwrap();
        assert_eq!(report.recovered.len(), 1);
        assert!(!report.committed);

        assert!(!archived_bodies(dir.path()).contains(&ebb.1.as_ref().clone()));
    }

    /// Restore's redo path leaves superseded bodies in segment files, so a gap
    /// is not proof of an orphaned EBB. A gap holding an ordinary block is
    /// refused rather than indexed, and the run writes nothing at all.
    #[test]
    fn a_gap_that_is_not_an_ebb_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let head = make_conway_block_with_prev(100, None, 0);
        let garbage = make_conway_block_with_prev(101, head.0.hash(), 1);
        let tail = make_conway_block_with_prev(102, garbage.0.hash(), 2);

        let flatfiles = FlatFileStore::new(dir.path()).unwrap();
        let locations = flatfiles
            .append_batch(&[
                (0, head.1.as_slice()),
                (0, garbage.1.as_slice()),
                (0, tail.1.as_slice()),
            ])
            .unwrap();

        let db = Database::builder()
            .create(dir.path().join("index"))
            .unwrap();
        let wx = db.begin_write().unwrap();
        {
            let mut table = wx.open_table(BlocksTable::DEF).unwrap();
            for i in [0usize, 2] {
                let point = if i == 0 { &head.0 } else { &tail.0 };
                table
                    .insert(point.slot(), locations[i].to_bytes().as_slice())
                    .unwrap();
            }
        }
        wx.commit().unwrap();
        drop(db);

        let report = heal(dir.path(), dir.path(), true).unwrap();
        assert_eq!(report.refused.len(), 1);
        assert!(report.recovered.is_empty());
        assert!(!report.committed);
        assert!(report.refused[0].1.contains("not an epoch-boundary block"));
    }

    /// The chain guard, not just the era probe: an epoch-boundary block that
    /// no indexed block claims as its parent is refused.
    #[test]
    fn an_ebb_that_does_not_chain_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        let head = make_conway_block_with_prev(byron_ebb_slot(1) - 10, None, 0);
        let stray = make_byron_ebb(1, head.0.hash().unwrap());
        // The block after the gap points at the head, not at the EBB — so the
        // EBB is a leftover body, not a lost link.
        let tail = make_conway_block_with_prev(byron_ebb_slot(1), head.0.hash(), 1);

        let flatfiles = FlatFileStore::new(dir.path()).unwrap();
        let locations = flatfiles
            .append_batch(&[
                (0, head.1.as_slice()),
                (0, stray.1.as_slice()),
                (0, tail.1.as_slice()),
            ])
            .unwrap();

        let db = Database::builder()
            .create(dir.path().join("index"))
            .unwrap();
        let wx = db.begin_write().unwrap();
        {
            let mut table = wx.open_table(BlocksTable::DEF).unwrap();
            table
                .insert(head.0.slot(), locations[0].to_bytes().as_slice())
                .unwrap();
            table
                .insert(tail.0.slot(), locations[2].to_bytes().as_slice())
                .unwrap();
        }
        wx.commit().unwrap();
        drop(db);

        let report = heal(dir.path(), dir.path(), true).unwrap();
        assert_eq!(report.refused.len(), 1);
        assert!(report.recovered.is_empty());
        assert!(!report.committed);
        assert!(report.refused[0].1.contains("is not the parent"));
    }

    /// A writer that already routes epoch-boundary blocks leaves no gaps, so
    /// the scan has nothing to say about an archive written by the fixed code.
    #[test]
    fn a_healthy_archive_has_no_gaps() {
        let dir = tempfile::tempdir().unwrap();

        let ebb = make_byron_ebb(1, pallas::crypto::hash::Hash::new([9u8; 32]));
        let main = make_conway_block_with_prev(byron_ebb_slot(1), ebb.0.hash(), 1);

        let store = ArchiveStore::open(StateSchema::default(), dir.path(), &config()).unwrap();
        let writer = store.start_writer().unwrap();
        writer.apply(&ebb.0, &ebb.1).unwrap();
        writer.apply(&main.0, &main.1).unwrap();
        writer.commit().unwrap();
        store.shutdown().unwrap();
        drop(store);

        let report = heal(dir.path(), dir.path(), true).unwrap();
        assert_eq!(report.gaps, 0);
        assert!(report.recovered.is_empty());
    }
}
