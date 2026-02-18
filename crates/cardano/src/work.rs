use dolos_core::{Block as _, BlockSlot, ChainPoint};
use pallas::ledger::primitives::Epoch;

use crate::eras::ChainSummary;
use crate::owned::OwnedMultiEraBlock;
use crate::pallas_extras;
use crate::roll::{WorkBatch, WorkBlock};

/// Internal work unit marker used by the WorkBuffer state machine.
///
/// These markers tell `CardanoLogic::pop_work` what kind of work unit to construct.
/// The actual work unit instances are created in `pop_work` with the necessary context.
pub(crate) enum InternalWorkUnit {
    Genesis,
    Blocks(WorkBatch),
    EWrap(BlockSlot),
    EStart(BlockSlot),
    Rupd(BlockSlot),
    ForcedStop,
}

pub(crate) enum WorkBuffer {
    Empty,
    Restart(ChainPoint),
    Genesis(OwnedMultiEraBlock),
    OpenBatch(WorkBatch),
    PreRupdBoundary(WorkBatch, OwnedMultiEraBlock),
    RupdBoundary(OwnedMultiEraBlock),
    PreEwrapBoundary(WorkBatch, OwnedMultiEraBlock, Epoch),
    EwrapBoundary(OwnedMultiEraBlock, Epoch),
    EstartBoundary(OwnedMultiEraBlock, Epoch),
    PreForcedStop(OwnedMultiEraBlock),
    ForcedStop,
}

impl WorkBuffer {
    pub fn new_from_cursor(cursor: ChainPoint) -> Self {
        Self::Restart(cursor)
    }

    pub fn last_point_seen(&self) -> ChainPoint {
        match self {
            WorkBuffer::Empty => ChainPoint::Origin,
            WorkBuffer::Restart(x) => x.clone(),
            WorkBuffer::Genesis(block) => block.point(),
            WorkBuffer::OpenBatch(batch) => batch.last_point(),
            WorkBuffer::PreRupdBoundary(_, block) => block.point(),
            WorkBuffer::RupdBoundary(block) => block.point(),
            WorkBuffer::PreEwrapBoundary(_, block, _) => block.point(),
            WorkBuffer::EwrapBoundary(block, _) => block.point(),
            WorkBuffer::EstartBoundary(block, _) => block.point(),
            WorkBuffer::PreForcedStop(block) => block.point(),
            WorkBuffer::ForcedStop => unreachable!(),
        }
    }

    #[allow(clippy::match_like_matches_macro)]
    pub fn can_receive_block(&self) -> bool {
        match self {
            WorkBuffer::Empty => true,
            WorkBuffer::Restart(..) => true,
            WorkBuffer::OpenBatch(..) => true,
            _ => false,
        }
    }

    fn extend_batch(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Empty => {
                let batch = WorkBatch::for_single_block(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            WorkBuffer::Restart(_) => {
                let batch = WorkBatch::for_single_block(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            WorkBuffer::OpenBatch(mut batch) => {
                batch.add_work(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            _ => unreachable!(),
        }
    }

    fn on_genesis_boundary(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Empty => WorkBuffer::Genesis(next_block),
            _ => unreachable!(),
        }
    }

    fn on_rupd_boundary(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Restart(_) => WorkBuffer::RupdBoundary(next_block),
            WorkBuffer::OpenBatch(batch) => WorkBuffer::PreRupdBoundary(batch, next_block),
            _ => unreachable!(),
        }
    }

    fn on_ewrap_boundary(self, next_block: OwnedMultiEraBlock, epoch: Epoch) -> Self {
        match self {
            WorkBuffer::Restart(..) => WorkBuffer::EwrapBoundary(next_block, epoch),
            WorkBuffer::OpenBatch(batch) => WorkBuffer::PreEwrapBoundary(batch, next_block, epoch),
            _ => unreachable!(),
        }
    }

    pub fn receive_block(
        self,
        block: OwnedMultiEraBlock,
        eras: &ChainSummary,
        stability_window: u64,
    ) -> Self {
        assert!(
            self.can_receive_block(),
            "can't continue until previous work is completed"
        );

        if matches!(self, WorkBuffer::Empty) {
            return self.on_genesis_boundary(block);
        }

        let prev_slot = self.last_point_seen().slot();

        let next_slot = block.slot();

        let boundary = pallas_extras::epoch_boundary(eras, prev_slot, next_slot);

        if let Some((epoch, _, _)) = boundary {
            return self.on_ewrap_boundary(block, epoch);
        }

        let rupd_boundary =
            pallas_extras::rupd_boundary(stability_window, eras, prev_slot, next_slot);

        if rupd_boundary.is_some() {
            return self.on_rupd_boundary(block);
        }

        self.extend_batch(block)
    }

    pub fn pop_work(self, stop_epoch: Option<Epoch>) -> (Option<InternalWorkUnit>, Self) {
        if matches!(self, WorkBuffer::Restart(..)) || matches!(self, WorkBuffer::Empty) {
            return (None, self);
        }

        match self {
            WorkBuffer::Genesis(block) => (
                Some(InternalWorkUnit::Genesis),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::OpenBatch(batch) => {
                let last_point = batch.last_point();
                (
                    Some(InternalWorkUnit::Blocks(batch)),
                    Self::Restart(last_point),
                )
            }
            WorkBuffer::PreRupdBoundary(batch, block) => (
                Some(InternalWorkUnit::Blocks(batch)),
                Self::RupdBoundary(block),
            ),
            WorkBuffer::RupdBoundary(block) => (
                Some(InternalWorkUnit::Rupd(block.slot())),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::PreEwrapBoundary(batch, block, epoch) => (
                Some(InternalWorkUnit::Blocks(batch)),
                Self::EwrapBoundary(block, epoch),
            ),
            WorkBuffer::EwrapBoundary(block, epoch) => (
                Some(InternalWorkUnit::EWrap(block.slot())),
                Self::EstartBoundary(block, epoch + 1),
            ),
            WorkBuffer::EstartBoundary(block, epoch) => (
                Some(InternalWorkUnit::EStart(block.slot())),
                if stop_epoch.is_some_and(|x| x == epoch) {
                    Self::PreForcedStop(block)
                } else {
                    Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block)))
                },
            ),
            WorkBuffer::PreForcedStop(block) => (
                Some(InternalWorkUnit::Blocks(WorkBatch::for_single_block(
                    WorkBlock::new(block),
                ))),
                Self::ForcedStop,
            ),
            WorkBuffer::ForcedStop => (Some(InternalWorkUnit::ForcedStop), Self::ForcedStop),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EraBoundary, EraSummary};
    use dolos_testing::blocks::make_conway_block;

    /// Single Conway era, epoch_length=100, slot_length=1.
    /// Epoch boundaries at slots 0, 100, 200, ...
    fn test_chain_summary() -> ChainSummary {
        let mut summary = ChainSummary::default();
        summary.append_era(
            7, // Conway protocol
            EraSummary {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 100,
                slot_length: 1,
                protocol: 7,
            },
        );
        summary
    }

    fn make_block(slot: BlockSlot) -> OwnedMultiEraBlock {
        let (_, raw) = make_conway_block(slot);
        OwnedMultiEraBlock::decode(raw).unwrap()
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum WorkTag {
        Genesis,
        Blocks { first: BlockSlot, last: BlockSlot },
        Rupd(BlockSlot),
        EWrap(BlockSlot),
        EStart(BlockSlot),
        ForcedStop,
    }

    fn tag_from_internal(wu: &InternalWorkUnit) -> WorkTag {
        match wu {
            InternalWorkUnit::Genesis => WorkTag::Genesis,
            InternalWorkUnit::Blocks(batch) => WorkTag::Blocks {
                first: batch.first_slot(),
                last: batch.last_slot(),
            },
            InternalWorkUnit::Rupd(s) => WorkTag::Rupd(*s),
            InternalWorkUnit::EWrap(s) => WorkTag::EWrap(*s),
            InternalWorkUnit::EStart(s) => WorkTag::EStart(*s),
            InternalWorkUnit::ForcedStop => WorkTag::ForcedStop,
        }
    }

    /// Feed blocks one-by-one, drain all work units via pop_work.
    fn feed_and_drain(
        mut buf: WorkBuffer,
        slots: &[BlockSlot],
        eras: &ChainSummary,
        stability_window: u64,
        stop_epoch: Option<Epoch>,
    ) -> Vec<WorkTag> {
        let mut tags = Vec::new();

        for &slot in slots {
            // drain any pending work before feeding more
            loop {
                if buf.can_receive_block() {
                    break;
                }
                let (wu, next) = buf.pop_work(stop_epoch);
                buf = next;
                if let Some(wu) = wu {
                    tags.push(tag_from_internal(&wu));
                } else {
                    break;
                }
            }
            let block = make_block(slot);
            buf = buf.receive_block(block, eras, stability_window);
        }

        // drain remaining
        loop {
            let (wu, next) = buf.pop_work(stop_epoch);
            buf = next;
            match wu {
                Some(ref wu) if matches!(wu, InternalWorkUnit::ForcedStop) => {
                    tags.push(tag_from_internal(wu));
                    break;
                }
                Some(wu) => tags.push(tag_from_internal(&wu)),
                None => break,
            }
        }

        tags
    }

    /// Feed blocks and track committed cursor after each work unit.
    /// Cursor rules:
    /// - Blocks(batch) => cursor = batch.last_point() (Specific)
    /// - EStart(slot)  => cursor = ChainPoint::Slot(slot)
    /// - Genesis, Rupd, EWrap => cursor unchanged
    fn feed_drain_with_cursors(
        mut buf: WorkBuffer,
        slots: &[BlockSlot],
        eras: &ChainSummary,
        stability_window: u64,
        stop_epoch: Option<Epoch>,
    ) -> Vec<(WorkTag, ChainPoint)> {
        let mut results = Vec::new();
        let mut cursor = ChainPoint::Origin;

        for &slot in slots {
            loop {
                if buf.can_receive_block() {
                    break;
                }
                let (wu, next) = buf.pop_work(stop_epoch);
                buf = next;
                if let Some(wu) = wu {
                    update_cursor(&wu, &mut cursor);
                    results.push((tag_from_internal(&wu), cursor.clone()));
                } else {
                    break;
                }
            }
            let block = make_block(slot);
            buf = buf.receive_block(block, eras, stability_window);
        }

        // drain remaining
        loop {
            let (wu, next) = buf.pop_work(stop_epoch);
            buf = next;
            match wu {
                Some(ref wu) if matches!(wu, InternalWorkUnit::ForcedStop) => {
                    update_cursor(wu, &mut cursor);
                    results.push((tag_from_internal(wu), cursor.clone()));
                    break;
                }
                Some(wu) => {
                    update_cursor(&wu, &mut cursor);
                    results.push((tag_from_internal(&wu), cursor.clone()));
                }
                None => break,
            }
        }

        results
    }

    fn update_cursor(wu: &InternalWorkUnit, cursor: &mut ChainPoint) {
        match wu {
            InternalWorkUnit::Blocks(batch) => {
                *cursor = batch.last_point();
            }
            InternalWorkUnit::EStart(slot) => {
                *cursor = ChainPoint::Slot(*slot);
            }
            // Genesis, Rupd, EWrap do not advance the cursor
            _ => {}
        }
    }

    // ---------------------------------------------------------------
    // Test 1: normal flow produces genesis then rolls
    // ---------------------------------------------------------------
    #[test]
    fn normal_flow_produces_genesis_then_rolls() {
        let eras = test_chain_summary();
        let tags = feed_and_drain(WorkBuffer::Empty, &[0, 10, 20], &eras, 40, None);

        assert_eq!(tags[0], WorkTag::Genesis);
        // After genesis, should get at least one Blocks
        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::Blocks { .. })),
            "expected at least one Blocks work unit, got: {:?}",
            tags
        );
    }

    // ---------------------------------------------------------------
    // Test 2: rupd boundary produces roll then rupd
    // ---------------------------------------------------------------
    #[test]
    fn rupd_boundary_produces_roll_then_rupd() {
        let eras = test_chain_summary();
        // stability_window = 40, epoch starts at 0
        // RUPD boundary at slot 40. Feed blocks on either side.
        // Start from Restart so we skip genesis logic.
        let tags = feed_and_drain(
            WorkBuffer::Restart(ChainPoint::Slot(5)),
            &[10, 30, 50],
            &eras,
            40,
            None,
        );

        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::Blocks { .. })),
            "expected Blocks, got: {:?}",
            tags
        );
        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::Rupd(_))),
            "expected Rupd, got: {:?}",
            tags
        );
    }

    // ---------------------------------------------------------------
    // Test 3: epoch boundary produces full cascade
    // ---------------------------------------------------------------
    #[test]
    fn epoch_boundary_produces_full_cascade() {
        let eras = test_chain_summary();
        // Epoch boundary at slot 100. Feed blocks that cross it.
        let tags = feed_and_drain(
            WorkBuffer::Restart(ChainPoint::Slot(80)),
            &[90, 110],
            &eras,
            40,
            None,
        );

        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::Blocks { .. })),
            "expected Blocks, got: {:?}",
            tags
        );
        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::EWrap(_))),
            "expected EWrap, got: {:?}",
            tags
        );
        assert!(
            tags.iter().any(|t| matches!(t, WorkTag::EStart(_))),
            "expected EStart, got: {:?}",
            tags
        );
    }

    // ---------------------------------------------------------------
    // Test 4: restart safety — no skipped work
    // ---------------------------------------------------------------
    #[test]
    fn restart_safety_no_skipped_work() {
        let eras = test_chain_summary();
        let stability_window = 40;
        // Sequence that crosses RUPD boundary (slot 40) and epoch boundary (slot 100)
        let slots: Vec<BlockSlot> = vec![0, 10, 30, 50, 70, 90, 110, 130];

        let full = feed_drain_with_cursors(
            WorkBuffer::Empty,
            &slots,
            &eras,
            stability_window,
            None,
        );

        let full_tags: Vec<_> = full.iter().map(|(t, _)| t.clone()).collect();

        // For each position i, simulate restart from cursor_at_i
        for i in 0..full.len().saturating_sub(1) {
            let (_, ref cursor) = full[i];

            // Mirror real initialization: Origin → Empty, otherwise → Restart.
            // Slot cursors mean "epoch boundary processed, block not yet rolled",
            // so we replay from that slot inclusive. Specific cursors mean the block
            // at that slot was fully committed, so we replay strictly after.
            let (restart_buf, replay_slots) = match cursor {
                ChainPoint::Origin => {
                    (WorkBuffer::Empty, slots.to_vec())
                }
                ChainPoint::Slot(s) => {
                    let filtered: Vec<_> = slots
                        .iter()
                        .copied()
                        .filter(|&slot| slot >= *s)
                        .collect();
                    (WorkBuffer::Restart(cursor.clone()), filtered)
                }
                ChainPoint::Specific(s, _) => {
                    let filtered: Vec<_> = slots
                        .iter()
                        .copied()
                        .filter(|&slot| slot > *s)
                        .collect();
                    (WorkBuffer::Restart(cursor.clone()), filtered)
                }
            };

            let restart_tags =
                feed_and_drain(restart_buf, &replay_slots, &eras, stability_window, None);

            // Every tag in full_tags[i+1..] must appear in restart_tags
            let remaining = &full_tags[i + 1..];
            for expected in remaining {
                // ForcedStop not expected in normal flow
                if matches!(expected, WorkTag::ForcedStop) {
                    continue;
                }
                assert!(
                    restart_tags.contains(expected),
                    "After restart from position {i} (cursor {:?}), \
                     missing work unit {:?}.\n\
                     Full sequence: {:?}\n\
                     Restart sequence: {:?}",
                    cursor,
                    expected,
                    full_tags,
                    restart_tags,
                );
            }
        }
    }

    // ---------------------------------------------------------------
    // Test 5: restart after rupd replays rupd
    // ---------------------------------------------------------------
    #[test]
    fn restart_after_rupd_replays_rupd() {
        let eras = test_chain_summary();
        let stability_window = 40;
        // Feed blocks so RUPD fires. Slots: 10, 30, 50
        // RUPD boundary at 40. Block at 30 is before, 50 is after.
        let full = feed_drain_with_cursors(
            WorkBuffer::Restart(ChainPoint::Slot(5)),
            &[10, 30, 50],
            &eras,
            stability_window,
            None,
        );

        // Find the RUPD work unit
        let rupd_idx = full
            .iter()
            .position(|(t, _)| matches!(t, WorkTag::Rupd(_)));

        if let Some(idx) = rupd_idx {
            // The cursor at the Blocks before RUPD should allow RUPD to replay
            let prev_idx = idx.saturating_sub(1);
            let (_, ref cursor) = full[prev_idx];

            let restart_buf = WorkBuffer::Restart(cursor.clone());
            let replay_slots: Vec<_> = [10u64, 30, 50]
                .iter()
                .copied()
                .filter(|&s| s > cursor.slot())
                .collect();

            let restart_tags =
                feed_and_drain(restart_buf, &replay_slots, &eras, stability_window, None);

            assert!(
                restart_tags.iter().any(|t| matches!(t, WorkTag::Rupd(_))),
                "RUPD should replay after restart. Got: {:?}",
                restart_tags
            );
        }
    }

    // ---------------------------------------------------------------
    // Test 6: restart after ewrap replays ewrap
    // ---------------------------------------------------------------
    #[test]
    fn restart_after_ewrap_replays_ewrap() {
        let eras = test_chain_summary();
        let stability_window = 40;
        // Epoch boundary at 100. Blocks at 90 and 110 straddle it.
        let full = feed_drain_with_cursors(
            WorkBuffer::Restart(ChainPoint::Slot(80)),
            &[90, 110],
            &eras,
            stability_window,
            None,
        );

        // Find EWrap
        let ewrap_idx = full
            .iter()
            .position(|(t, _)| matches!(t, WorkTag::EWrap(_)));

        if let Some(idx) = ewrap_idx {
            // Cursor before EWrap (at the Blocks that preceded it)
            let prev_idx = idx.saturating_sub(1);
            let (_, ref cursor) = full[prev_idx];

            let restart_buf = WorkBuffer::Restart(cursor.clone());
            let replay_slots: Vec<_> = [90u64, 110]
                .iter()
                .copied()
                .filter(|&s| s > cursor.slot())
                .collect();

            let restart_tags =
                feed_and_drain(restart_buf, &replay_slots, &eras, stability_window, None);

            assert!(
                restart_tags.iter().any(|t| matches!(t, WorkTag::EWrap(_))),
                "EWrap should replay after restart. Got: {:?}",
                restart_tags
            );
            assert!(
                restart_tags
                    .iter()
                    .any(|t| matches!(t, WorkTag::EStart(_))),
                "EStart should replay after restart. Got: {:?}",
                restart_tags
            );
        }
    }

    // ---------------------------------------------------------------
    // Test 7: restart after estart replays the boundary block
    // ---------------------------------------------------------------
    #[test]
    fn restart_after_estart_replays_block() {
        let eras = test_chain_summary();
        let stability_window = 40;
        // Epoch boundary at 100. Blocks at 90, 110, 130.
        let full = feed_drain_with_cursors(
            WorkBuffer::Restart(ChainPoint::Slot(80)),
            &[90, 110, 130],
            &eras,
            stability_window,
            None,
        );

        // Find EStart
        let estart_idx = full
            .iter()
            .position(|(t, _)| matches!(t, WorkTag::EStart(_)));

        if let Some(idx) = estart_idx {
            let (_, ref cursor) = full[idx];
            // Cursor after EStart is Slot(slot), restart from there
            let restart_buf = WorkBuffer::Restart(cursor.clone());
            let replay_slots: Vec<_> = [90u64, 110, 130]
                .iter()
                .copied()
                .filter(|&s| s >= cursor.slot())
                .collect();

            let restart_tags =
                feed_and_drain(restart_buf, &replay_slots, &eras, stability_window, None);

            // The boundary block (110) should be the start of a Blocks work unit
            assert!(
                restart_tags.iter().any(|t| matches!(t, WorkTag::Blocks { first, .. } if *first == 110)),
                "Boundary block 110 should be the start of a Blocks unit after restart from EStart. Got: {:?}",
                restart_tags
            );
        }
    }
}
