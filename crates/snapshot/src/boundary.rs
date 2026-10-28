//! Which store sets this profile publishes from.
//!
//! A stele captures the state immediately after the first block of its
//! sequence epoch: Ewrap has closed epoch E-1, Estart has opened epoch E, and
//! E's first block has been applied (decision 0025). [`check`] is the one place
//! that rule is decided, and every path that builds layers — directory and
//! registry publication, dry runs, digest and reproduction — asks it before a
//! layer is written.
//!
//! ## The evidence
//!
//! The archive has to start at the chain's origin: its first block is the
//! chain's first, by the test `doctor rebuild-state` applies (block number at
//! most 1). A pruned archive's sweep has taken the entity index entries of its
//! pruned blocks with them, so its node stages a re-carried script or asset
//! again as a first sighting and seals it into its period logs. That is a
//! pruned node's accepted posture as a query node (decision 0064, item 4) and
//! exactly what a stele must never carry, so a publisher keeps full history
//! (item 3).
//!
//! The state cursor has to be the plan's point, and the archive has to hold the
//! block at that point together with the block before it in chain order. That
//! predecessor must be the block the cursor's header names as its parent, and
//! it must sit before `epoch_start(E)`. The parent link is what proves nothing
//! lies between the two, so empty slots after the boundary are accepted and a
//! Byron epoch-boundary block, which shares its slot with the first main block
//! after it, is the first block of its epoch while that main block is not.
//!
//! The archive must end at the cursor's block. Export reads logs and indexes by
//! slot alone, and blocks applied after the cursor write rows inside the
//! stele's windows: Ewrap keys an epoch's closing rows at its start, and a
//! Byron main block shares the boundary block's slot. A sync stopped at the
//! epoch stores nothing past its first block; an archive that ran on past a
//! rebuilt state is refused.
//!
//! Neither the configured `stop_epoch` nor the cursor's slot is evidence: a
//! node can be configured one way and stand somewhere else, and an epoch's
//! first block rarely sits on its first slot. An archive that does not reach
//! back to the parent — pruned, or started at the cursor by a bootstrap —
//! cannot show where the epoch opened, and is refused rather than trusted.
//!
//! Epoch 0 is never eligible. No boundary opens it, so there is no transition
//! for a stele at sequence 0 to follow, and the first stele a chain can carry
//! is `epoch-1`.
//!
//! ## What the check does not do
//!
//! It reads the stores once, before the walk. It does not keep them still: a
//! caller exporting from stores something else is writing to gets an
//! inconsistent stele whatever the cursor said at the start. The storage
//! backends lock their directories against a second process, and the headless
//! replay API lends its snapshot only while no import runs.

use dolos_cardano::{eras::ChainSummary, pallas::ledger::traverse::MultiEraBlock};
use dolos_core::{ArchiveStore, BlockBody, BlockSlot, ChainPoint, StateStore};

use crate::{export::Plan, Error};

/// What the stores showed about the boundary a stele is cut at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boundary {
    /// The epoch the boundary opens: the plan's sequence.
    pub epoch: u64,
    /// The first block of `epoch`, where the state cursor stands.
    pub first: ChainPoint,
    /// That block's parent, the last block before the boundary.
    pub parent: ChainPoint,
}

/// Why a store set is not an epoch-boundary cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ineligible {
    /// The archive's first block is not the chain's first: it was pruned, or
    /// bootstrapped somewhere after origin.
    PrunedArchive { first: ChainPoint, number: u64 },
    /// The plan stands at a different point than the state store's cursor.
    PlanMismatch { stored: Option<ChainPoint> },
    /// The plan's sequence is not the epoch its cursor stands in.
    SequenceMismatch { epoch: u64 },
    /// The cursor stands in epoch 0, which no boundary opens.
    InitialEpoch,
    /// The archive holds no block with the cursor's hash at its slot.
    MissingCursorBlock,
    /// The archive holds nothing before the cursor's block.
    MissingParent,
    /// The block before the cursor's in the archive is not its parent.
    BrokenChain { predecessor: ChainPoint },
    /// The cursor's parent is already in the sequence epoch.
    LaterBlock { parent: ChainPoint },
    /// The archive holds blocks after the cursor's, up to `tip`.
    ArchivePastCursor { tip: ChainPoint },
}

impl std::fmt::Display for Ineligible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrunedArchive { first, number } => write!(
                f,
                "the archive starts at block #{number}, slot {}, not at the chain's first block, \
                 so its entity index lacks the first sightings of the history it no longer holds; \
                 publish from a node that keeps full history (history.mode = \"full\") and was \
                 synced or restored from origin",
                first.slot()
            ),
            Self::PlanMismatch {
                stored: Some(stored),
            } => write!(
                f,
                "the state store's cursor is {stored}; plan from the stores being exported"
            ),
            Self::PlanMismatch { stored: None } => write!(
                f,
                "the state store has no cursor; plan from the stores being exported"
            ),
            Self::SequenceMismatch { epoch } => write!(
                f,
                "the cursor stands in epoch {epoch}; the sequence is the cursor's epoch"
            ),
            Self::InitialEpoch => write!(
                f,
                "no boundary opens epoch 0, so the first stele a chain can carry is epoch 1; \
                 run dolos sync --stop-epoch 1"
            ),
            Self::MissingCursorBlock => write!(
                f,
                "the archive holds no block with the cursor's hash at its slot, so nothing shows \
                 which block the state stands on"
            ),
            Self::MissingParent => write!(
                f,
                "the archive holds no block before the cursor's, so nothing shows that the cursor \
                 is the epoch's first block; publish from a node whose archive reaches the \
                 previous epoch"
            ),
            Self::BrokenChain { predecessor } => write!(
                f,
                "the archive's block before the cursor, {predecessor}, is not the cursor block's \
                 parent, so the archive has a gap there; publish from a node whose archive is \
                 contiguous across the boundary"
            ),
            Self::LaterBlock { parent } => write!(
                f,
                "the cursor block's parent {parent} is in the same epoch, so the state is past \
                 the epoch's first block and the boundary state is no longer in these stores; \
                 restore the stele for this epoch, or cut the next one with \
                 dolos sync --stop-epoch set to the following epoch"
            ),
            Self::ArchivePastCursor { tip } => write!(
                f,
                "the archive runs on past the cursor to {tip}, and the blocks after the cursor \
                 wrote log and index records the stele cannot tell from the boundary's; publish \
                 from a node whose archive ends at the cursor, as dolos sync --stop-epoch leaves it"
            ),
        }
    }
}

/// Establish that `plan` stands on the first block of its sequence epoch, from
/// what `archive` and `state` hold.
pub fn check<A: ArchiveStore, S: StateStore>(
    plan: &Plan,
    archive: &A,
    state: &S,
) -> Result<Boundary, Error> {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(state)?;

    evidence(&summary, plan, state.read_cursor()?, archive)
}

fn evidence<A: ArchiveStore>(
    summary: &ChainSummary,
    plan: &Plan,
    stored: Option<ChainPoint>,
    archive: &A,
) -> Result<Boundary, Error> {
    let refuse = |reason| Error::NotABoundary {
        cursor: plan.cursor.to_string(),
        sequence: plan.sequence,
        reason,
    };

    let hash = plan
        .cursor
        .hash()
        .ok_or_else(|| Error::UnanchoredPoint(plan.cursor.to_string()))?;
    let slot = plan.cursor.slot();

    // `ChainPoint`'s equality lets a hashed point equal a bare slot, so the two
    // halves are held against each other explicitly.
    if stored.as_ref().map(|point| (point.slot(), point.hash())) != Some((slot, Some(hash))) {
        return Err(refuse(Ineligible::PlanMismatch { stored }));
    }

    let (epoch, _) = summary.slot_epoch(slot);

    if epoch != plan.sequence {
        return Err(refuse(Ineligible::SequenceMismatch { epoch }));
    }

    if epoch == 0 {
        return Err(refuse(Ineligible::InitialEpoch));
    }

    // An empty archive holds no cursor block either, which is the refusal
    // that names it.
    if let Some((first_slot, first_body)) = archive.get_range(None, None)?.next() {
        let first = decode(first_slot, &first_body)?;

        if first.number() > 1 {
            return Err(refuse(Ineligible::PrunedArchive {
                first: ChainPoint::Specific(first_slot, first.hash()),
                number: first.number(),
            }));
        }
    }

    let at_slot = archive.get_blocks_by_slot(&slot)?;

    let mut position = None;

    for (index, body) in at_slot.iter().enumerate() {
        if decode(slot, body)?.hash() == hash {
            position = Some(index);
            break;
        }
    }

    let Some(position) = position else {
        return Err(refuse(Ineligible::MissingCursorBlock));
    };

    let parent_hash = decode(slot, &at_slot[position])?.header().previous_hash();

    let predecessor = match position.checked_sub(1) {
        Some(earlier) => Some((slot, at_slot[earlier].clone())),
        None => archive.get_range(None, Some(slot))?.next_back(),
    };

    let Some((parent_slot, parent_body)) = predecessor else {
        return Err(refuse(Ineligible::MissingParent));
    };

    let parent = ChainPoint::Specific(parent_slot, decode(parent_slot, &parent_body)?.hash());

    if parent.hash() != parent_hash {
        return Err(refuse(Ineligible::BrokenChain {
            predecessor: parent,
        }));
    }

    if parent_slot >= summary.epoch_start(epoch) {
        return Err(refuse(Ineligible::LaterBlock { parent }));
    }

    let beyond = match archive.get_tip()? {
        Some((tip_slot, tip_body)) if tip_slot > slot => Some((tip_slot, tip_body)),
        _ => at_slot[position + 1..]
            .last()
            .map(|body| (slot, body.clone())),
    };

    if let Some((tip_slot, tip_body)) = beyond {
        let tip = ChainPoint::Specific(tip_slot, decode(tip_slot, &tip_body)?.hash());

        return Err(refuse(Ineligible::ArchivePastCursor { tip }));
    }

    Ok(Boundary {
        epoch,
        first: plan.cursor.clone(),
        parent,
    })
}

fn decode(slot: BlockSlot, body: &BlockBody) -> Result<MultiEraBlock<'_>, Error> {
    MultiEraBlock::decode(body).map_err(|e| Error::UndecodableBlock {
        slot,
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use dolos_core::{builtin::MemoryArchiveStore, ArchiveStore as _, ArchiveWriter as _};
    use dolos_testing::blocks::{
        byron_ebb_slot, make_byron_ebb, make_conway_block_with_prev, BYRON_EPOCH_LENGTH,
    };

    use super::*;
    use crate::{Network, RetainedEpochs};

    /// One era of `length`-slot epochs from slot zero.
    fn summary(length: u64) -> ChainSummary {
        use dolos_cardano::{model::EraState, EraBoundary};

        let mut chain = ChainSummary::default();

        chain.append_era(
            6,
            EraState {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: length,
                slot_length: 1,
                protocol: 6,
            },
        );

        chain
    }

    /// An archive holding `blocks` in the order given.
    fn archive(blocks: &[(ChainPoint, dolos_core::RawBlock)]) -> MemoryArchiveStore {
        let archive = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
        let writer = archive.start_writer().unwrap();

        for (point, body) in blocks {
            writer.apply(point, body).unwrap();
        }

        writer.commit().unwrap();

        archive
    }

    /// A Conway chain at `slots`, each block chained onto the one before.
    fn chain(slots: &[BlockSlot]) -> Vec<(ChainPoint, dolos_core::RawBlock)> {
        let mut blocks: Vec<(ChainPoint, dolos_core::RawBlock)> = Vec::new();

        for (number, slot) in slots.iter().enumerate() {
            let prev = blocks.last().and_then(|(point, _)| point.hash());
            blocks.push(make_conway_block_with_prev(*slot, prev, number as u64));
        }

        blocks
    }

    fn plan(summary: &ChainSummary, at: &ChainPoint) -> Plan {
        Plan::new(
            summary,
            Network::for_magic(2),
            at.clone(),
            RetainedEpochs::default(),
        )
        .unwrap()
    }

    fn judge(
        summary: &ChainSummary,
        blocks: &[(ChainPoint, dolos_core::RawBlock)],
        at: &ChainPoint,
    ) -> Result<Boundary, Error> {
        evidence(
            summary,
            &plan(summary, at),
            Some(at.clone()),
            &archive(blocks),
        )
    }

    fn reason(result: Result<Boundary, Error>) -> Ineligible {
        match result {
            Err(Error::NotABoundary { reason, .. }) => reason,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_first_block_after_empty_slots_is_the_boundary() {
        let summary = summary(100);
        let blocks = chain(&[150, 180, 237]);

        let boundary = judge(&summary, &blocks, &blocks[2].0).unwrap();

        assert_eq!(boundary.epoch, 2);
        assert_eq!(boundary.first, blocks[2].0);
        assert_eq!(boundary.parent, blocks[1].0);
    }

    #[test]
    fn a_first_block_on_the_epochs_first_slot_is_the_boundary() {
        let summary = summary(100);
        let blocks = chain(&[199, 200]);

        assert_eq!(judge(&summary, &blocks, &blocks[1].0).unwrap().epoch, 2);
    }

    #[test]
    fn the_second_block_of_an_epoch_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205, 210]);

        assert_eq!(
            reason(judge(&summary, &blocks, &blocks[2].0)),
            Ineligible::LaterBlock {
                parent: blocks[1].0.clone()
            }
        );
    }

    #[test]
    fn a_hashless_cursor_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205]);
        let anchored = plan(&summary, &blocks[1].0);
        let mut plan = anchored.clone();
        plan.cursor = ChainPoint::Slot(205);

        let err = evidence(
            &summary,
            &plan,
            Some(plan.cursor.clone()),
            &archive(&blocks),
        );

        assert!(matches!(err, Err(Error::UnanchoredPoint(_))), "{err:?}");
    }

    #[test]
    fn a_plan_that_is_not_the_stores_cursor_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205, 210]);
        let plan = plan(&summary, &blocks[1].0);
        let archive = archive(&blocks);

        let ahead = evidence(&summary, &plan, Some(blocks[2].0.clone()), &archive);
        assert_eq!(
            reason(ahead),
            Ineligible::PlanMismatch {
                stored: Some(blocks[2].0.clone())
            }
        );

        let bare = evidence(&summary, &plan, Some(ChainPoint::Slot(205)), &archive);
        assert!(matches!(reason(bare), Ineligible::PlanMismatch { .. }));

        let none = evidence(&summary, &plan, None, &archive);
        assert_eq!(reason(none), Ineligible::PlanMismatch { stored: None });
    }

    /// A plan's fields are public, so a hand-built one cannot claim another
    /// epoch than its cursor's.
    #[test]
    fn a_hand_built_sequence_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205]);
        let mut plan = plan(&summary, &blocks[1].0);
        plan.sequence = 3;

        let err = evidence(
            &summary,
            &plan,
            Some(blocks[1].0.clone()),
            &archive(&blocks),
        );

        assert_eq!(reason(err), Ineligible::SequenceMismatch { epoch: 2 });
    }

    #[test]
    fn epoch_zero_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[3, 7]);

        assert_eq!(
            reason(judge(&summary, &blocks, &blocks[0].0)),
            Ineligible::InitialEpoch
        );
    }

    #[test]
    fn a_cursor_block_the_archive_lacks_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205]);

        assert_eq!(
            reason(judge(&summary, &blocks[..1], &blocks[1].0)),
            Ineligible::MissingCursorBlock
        );
    }

    /// A publisher keeps full history: an archive whose first block is past
    /// the chain's first is refused, naming that block's slot, before
    /// anything else about the boundary is read.
    #[test]
    fn an_archive_that_starts_after_origin_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[150, 160, 180, 205]);

        let refusal = reason(judge(&summary, &blocks[2..], &blocks[3].0));

        assert_eq!(
            refusal,
            Ineligible::PrunedArchive {
                first: blocks[2].0.clone(),
                number: 2,
            }
        );

        let message = refusal.to_string();
        assert!(message.contains("block #2, slot 180"), "{message}");
        assert!(message.contains("history.mode = \"full\""), "{message}");

        // The same boundary from the full archive proceeds.
        assert_eq!(judge(&summary, &blocks, &blocks[3].0).unwrap().epoch, 2);
    }

    /// The chain's first block may be its second in number: the test is the
    /// one `doctor rebuild-state` applies, which tolerates a first block
    /// numbered 1.
    #[test]
    fn an_archive_whose_first_block_is_number_one_starts_at_origin() {
        let summary = summary(100);
        let blocks = chain(&[150, 160, 205]);

        assert_eq!(
            judge(&summary, &blocks[1..], &blocks[2].0).unwrap().epoch,
            2
        );
    }

    /// A pruned archive's first remaining block says nothing about what came
    /// before it.
    #[test]
    fn an_archive_starting_at_the_cursor_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205]);

        assert_eq!(
            reason(judge(&summary, &blocks[1..], &blocks[1].0)),
            Ineligible::MissingParent
        );
    }

    #[test]
    fn a_gap_before_the_cursor_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[150, 180, 205]);
        let gapped = [blocks[0].clone(), blocks[2].clone()];

        assert_eq!(
            reason(judge(&summary, &gapped, &blocks[2].0)),
            Ineligible::BrokenChain {
                predecessor: blocks[0].0.clone()
            }
        );
    }

    /// A Byron boundary block, then the first main block after it at the same
    /// slot: the chain a stop_epoch sync over a Byron epoch stores past.
    fn byron_boundary() -> [(ChainPoint, dolos_core::RawBlock); 3] {
        let last = make_conway_block_with_prev(byron_ebb_slot(1) - 2, None, 1);
        let ebb = make_byron_ebb(1, last.0.hash().unwrap());
        let main = make_conway_block_with_prev(byron_ebb_slot(1), ebb.0.hash(), 2);

        [last, ebb, main]
    }

    /// A Byron boundary block opens its epoch and shares its slot with the
    /// first main block after it. The boundary block is the epoch's first
    /// block; the main block is its second.
    #[test]
    fn a_byron_boundary_block_is_first_and_the_main_block_beside_it_is_not() {
        let summary = summary(BYRON_EPOCH_LENGTH);
        let [last, ebb, main] = byron_boundary();

        let boundary = judge(&summary, &[last.clone(), ebb.clone()], &ebb.0).unwrap();
        assert_eq!(boundary.epoch, 1);
        assert_eq!(boundary.parent, last.0);

        assert_eq!(
            reason(judge(&summary, &[last, ebb.clone(), main.clone()], &main.0)),
            Ineligible::LaterBlock { parent: ebb.0 }
        );
    }

    /// Stores rebuilt to a Byron boundary beside an archive that kept syncing
    /// hold the main block at the boundary block's slot, and its index and
    /// log records are indistinguishable from the boundary block's.
    #[test]
    fn a_block_after_the_cursor_at_its_slot_is_refused() {
        let summary = summary(BYRON_EPOCH_LENGTH);
        let blocks = byron_boundary();
        let [_, ebb, main] = &blocks;

        assert_eq!(
            reason(judge(&summary, &blocks, &ebb.0)),
            Ineligible::ArchivePastCursor {
                tip: main.0.clone()
            }
        );
    }

    /// An archive that ran on past a boundary state holds the rows later
    /// blocks wrote inside the stele's windows, and is refused up to its tip
    /// with the store to publish from instead.
    #[test]
    fn an_archive_past_the_cursor_is_refused() {
        let summary = summary(100);
        let blocks = chain(&[180, 205, 210, 320]);

        let refusal = reason(judge(&summary, &blocks, &blocks[1].0));

        assert_eq!(
            refusal,
            Ineligible::ArchivePastCursor {
                tip: blocks[3].0.clone()
            }
        );

        let message = refusal.to_string();

        assert!(
            message.contains(&format!("runs on past the cursor to {}", blocks[3].0)),
            "{message}"
        );
        assert!(
            message.contains(
                "publish from a node whose archive ends at the cursor, as dolos sync \
                 --stop-epoch leaves it"
            ),
            "{message}"
        );
    }
}
