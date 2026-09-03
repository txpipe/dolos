//! Archive block continuity — the same walk `doctor wal-integrity` does, but
//! over the store that actually feeds queries.
//!
//! The rule itself is not restated here. [`dolos_cardano::consensus::
//! check_extension`] is what the pull stage and the apply stage's batch guard
//! both call to decide whether a block extends the chain, and it is what this
//! check calls too. That matters most for era history: Byron epoch-boundary
//! blocks share a slot with the block that follows them, and `check_extension`
//! already tolerates a non-advancing slot for exactly that reason. A check
//! that re-derived "slots must ascend" would flag every Byron epoch on
//! mainnet as corruption.

use dolos_cardano::consensus::check_extension;
use dolos_core::{ArchiveStore, BlockBody, BlockSlot, ChainPoint};
use indicatif::ProgressBar;
use miette::{Context as _, IntoDiagnostic as _};
use pallas::ledger::traverse::MultiEraBlock;

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::ArchiveContinuity;

/// Walk the archive's blocks in slot order and verify each one extends its
/// predecessor.
///
/// A block that fails to decode is an issue in its own right and ends the
/// walk: without its header there is no hash to chain the next block onto,
/// and every following block would be reported against a stale parent.
pub fn check_continuity(
    blocks: impl Iterator<Item = (BlockSlot, BlockBody)>,
    mut on_progress: impl FnMut(BlockSlot),
) -> Vec<Issue> {
    let mut issues = Vec::new();
    let mut tip = ChainPoint::Origin;

    for (slot, body) in blocks {
        on_progress(slot);

        let decoded = match MultiEraBlock::decode(&body) {
            Ok(x) => x,
            Err(err) => {
                issues.push(Issue::new(
                    CHECK,
                    format!(
                        "block at slot {slot} does not decode ({err}); the walk cannot continue \
                         past it because the following block has no parent to chain onto"
                    ),
                ));
                break;
            }
        };

        let header = decoded.header();

        if let Err(err) = check_extension(&tip, slot, header.previous_hash()) {
            issues.push(Issue::new(
                CHECK,
                format!("archive block {} at slot {slot}: {err}", decoded.hash()),
            ));
        }

        tip = ChainPoint::Specific(slot, decoded.hash());
    }

    issues
}

pub fn run(stores: &crate::common::Stores, progress: &ProgressBar) -> miette::Result<Vec<Issue>> {
    let blocks = stores
        .archive
        .get_range(None, None)
        .into_diagnostic()
        .context("iterating archive blocks")?;

    let mut seen = 0u64;

    Ok(check_continuity(blocks, |slot| {
        seen += 1;

        // Formatting a message per block would cost more than the check
        // itself; one update every few thousand blocks is enough to show the
        // walk is alive.
        if seen.is_multiple_of(10_000) {
            progress.set_message(format!("archive-continuity: slot {slot}"));
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_testing::blocks::make_conway_block_with_prev;
    use pallas::crypto::hash::Hash;

    /// Build a chain of `count` blocks starting at `first_slot`, each
    /// pointing at its predecessor, in the shape `get_range` yields.
    fn chain(first_slot: BlockSlot, count: u64) -> Vec<(BlockSlot, BlockBody)> {
        let mut out = Vec::new();
        let mut prev = None;

        for i in 0..count {
            let slot = first_slot + i;
            let (point, body) = make_conway_block_with_prev(slot, prev, i);
            prev = point.hash();
            out.push((slot, body.as_ref().clone()));
        }

        out
    }

    #[test]
    fn a_chained_archive_passes() {
        let issues = check_continuity(chain(100, 5).into_iter(), |_| {});
        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A Byron epoch boundary as the archive now yields it: the EBB, then the
    /// epoch's first main block on the same slot, pointing back at it. The
    /// walk has to accept both — the slot does not advance and the parent is
    /// the EBB, not the block before it.
    #[test]
    fn a_byron_boundary_passes_when_the_ebb_is_there() {
        use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb};

        let mut blocks = chain(byron_ebb_slot(1) - 2, 2);
        let last_hash = MultiEraBlock::decode(&blocks[1].1).unwrap().hash();

        let (ebb_point, ebb_body) = make_byron_ebb(1, last_hash);
        let slot = ebb_point.slot();
        blocks.push((slot, ebb_body.as_ref().clone()));

        let (_, first_of_epoch) = make_conway_block_with_prev(slot, ebb_point.hash(), 2);
        blocks.push((slot, first_of_epoch.as_ref().clone()));

        let issues = check_continuity(blocks.into_iter(), |_| {});
        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The defect this check exists to catch: the same boundary with the EBB
    /// missing, which is what every pre-fix archive holds. The epoch's first
    /// block then claims a parent the archive never yields.
    #[test]
    fn a_byron_boundary_is_reported_when_the_ebb_is_missing() {
        use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb};

        let mut blocks = chain(byron_ebb_slot(1) - 2, 2);
        let last_hash = MultiEraBlock::decode(&blocks[1].1).unwrap().hash();

        let (ebb_point, _) = make_byron_ebb(1, last_hash);
        let slot = ebb_point.slot();

        let (_, first_of_epoch) = make_conway_block_with_prev(slot, ebb_point.hash(), 2);
        blocks.push((slot, first_of_epoch.as_ref().clone()));

        let issues = check_continuity(blocks.into_iter(), |_| {});
        assert_eq!(issues.len(), 1, "{issues:?}");
    }

    #[test]
    fn an_empty_archive_passes() {
        let issues = check_continuity(std::iter::empty(), |_| {});
        assert!(issues.is_empty());
    }

    /// The corruption fixture: one block's `previous_hash` is doctored. The
    /// walk reports it and keeps going, so the blocks after it are still
    /// checked against their real parent.
    #[test]
    fn a_broken_prev_hash_is_reported_and_the_walk_continues() {
        let mut blocks = chain(100, 4);

        // Rebuild block #2 with a parent nothing produced, then rechain the
        // rest onto it so the only defect in the run is the doctored link.
        let wrong = Hash::from([0xEE; 32]);
        let (broken_point, broken_body) = make_conway_block_with_prev(102, Some(wrong), 2);
        blocks[2] = (102, broken_body.as_ref().clone());

        let (tail_point, tail_body) = make_conway_block_with_prev(103, broken_point.hash(), 3);
        blocks[3] = (103, tail_body.as_ref().clone());
        let _ = tail_point;

        let issues = check_continuity(blocks.into_iter(), |_| {});

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].check, CheckKind::ArchiveContinuity);
        assert!(issues[0].detail.contains("slot 102"));
        assert!(issues[0].detail.contains(&hex::encode(wrong)));
    }

    /// Byron epoch-boundary blocks share a slot with the block that follows.
    /// `check_extension` allows a non-advancing slot, so era history is not
    /// flagged as corruption.
    #[test]
    fn same_slot_blocks_are_tolerated() {
        let (first, first_body) = make_conway_block_with_prev(100, None, 0);
        let (_, second_body) = make_conway_block_with_prev(100, first.hash(), 1);

        let blocks = vec![
            (100, first_body.as_ref().clone()),
            (100, second_body.as_ref().clone()),
        ];

        let issues = check_continuity(blocks.into_iter(), |_| {});

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A block whose slot goes backwards is a real discontinuity, and the
    /// same rule the apply stage enforces is what names it.
    #[test]
    fn a_backwards_slot_is_reported() {
        let (first, first_body) = make_conway_block_with_prev(200, None, 0);
        let (_, second_body) = make_conway_block_with_prev(100, first.hash(), 1);

        let blocks = vec![
            (200, first_body.as_ref().clone()),
            (100, second_body.as_ref().clone()),
        ];

        let issues = check_continuity(blocks.into_iter(), |_| {});

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("does not advance"));
    }

    #[test]
    fn an_undecodable_block_is_reported() {
        let blocks = vec![(100, vec![0xFF, 0xFF, 0xFF])];

        let issues = check_continuity(blocks.into_iter(), |_| {});

        assert_eq!(issues.len(), 1);
        assert!(issues[0].detail.contains("does not decode"));
    }
}
