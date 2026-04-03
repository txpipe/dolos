//! Chain consensus utilities for the pull stage.
//!
//! This module provides [`ChainFragment`], a lightweight tracker for the
//! upstream chain position that validates parent-hash continuity and slot
//! ordering on each new header, and accumulates pending points for block
//! fetching.

use dolos_core::{BlockHash, BlockSlot, ChainPoint};

/// Errors that can occur during consensus validation.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// A header's `previous_hash` doesn't match the expected chain tip.
    #[error("block at slot {slot} has parent hash {got} but expected {expected}")]
    BrokenContinuity {
        slot: BlockSlot,
        expected: BlockHash,
        got: BlockHash,
    },

    /// A header's slot is not strictly greater than the current tip slot.
    #[error("block slot {slot} does not advance from tip slot {tip_slot}")]
    SlotNotIncreasing {
        slot: BlockSlot,
        tip_slot: BlockSlot,
    },
}

/// Outcome of a [`ChainFragment::roll_back`] operation.
pub enum RollbackResult {
    /// The rollback point was found in the pending batch and handled
    /// internally. The batch is truncated and the tip updated.
    Handled,
    /// The rollback point is before the pending batch — the caller must
    /// propagate this as a rollback event to the downstream consumer.
    OutOfScope(ChainPoint),
}

// ============================================================================
// Consensus checks — each is a standalone function for readability
// ============================================================================

/// Verify that `prev_hash` matches the tip's hash.
/// Skipped when either side is unknown (Origin / genesis).
fn check_continuity(
    slot: BlockSlot,
    prev_hash: Option<BlockHash>,
    tip: &ChainPoint,
) -> Result<(), ConsensusError> {
    if let (Some(prev), Some(tip_hash)) = (prev_hash, tip.hash()) {
        if prev != tip_hash {
            return Err(ConsensusError::BrokenContinuity {
                slot,
                expected: tip_hash,
                got: prev,
            });
        }
    }

    Ok(())
}

/// Verify that the new slot is strictly greater than the tip's slot.
/// Skipped when the tip is Origin (no slot to compare against).
fn check_slot_increase(slot: BlockSlot, tip: &ChainPoint) -> Result<(), ConsensusError> {
    if matches!(tip, ChainPoint::Origin) {
        return Ok(());
    }

    if slot <= tip.slot() {
        return Err(ConsensusError::SlotNotIncreasing {
            slot,
            tip_slot: tip.slot(),
        });
    }

    Ok(())
}

// ============================================================================
// ChainFragment
// ============================================================================

/// Tracks the upstream chain position for the pull stage.
///
/// Maintains the current tip (for continuity and slot validation) separately
/// from the pending batch of points (which will be fetched via blockfetch).
pub struct ChainFragment {
    /// The latest known chain position.
    tip: ChainPoint,

    /// Points accumulated during the current header-gathering pass.
    /// Drained by [`take_pending`](Self::take_pending) after the pass
    /// completes.
    pending: Vec<ChainPoint>,
}

impl ChainFragment {
    /// Create a new fragment seeded from the chainsync intersection point.
    pub fn start(point: ChainPoint) -> Self {
        Self {
            tip: point,
            pending: Vec::new(),
        }
    }

    /// Validate consensus rules and accept a new point.
    ///
    /// `prev_hash` is the parent hash declared by the block header.
    /// Returns the accepted [`ChainPoint`] on success.
    /// Returns [`Err(ConsensusError)`] if any check fails.
    pub fn roll_forward(
        &mut self,
        point: ChainPoint,
        prev_hash: Option<BlockHash>,
    ) -> Result<ChainPoint, ConsensusError> {
        check_continuity(point.slot(), prev_hash, &self.tip)?;
        check_slot_increase(point.slot(), &self.tip)?;

        self.tip = point.clone();
        self.pending.push(point.clone());
        Ok(point)
    }

    /// Roll back to a given point.
    ///
    /// If the point is within the pending batch, truncates the batch and
    /// updates the tip. Otherwise signals that the rollback is out of scope
    /// and must be propagated downstream.
    pub fn roll_back(&mut self, point: &ChainPoint) -> RollbackResult {
        if let Some(pos) = self.pending.iter().position(|p| p == point) {
            self.pending.truncate(pos + 1);
            self.tip = point.clone();
            RollbackResult::Handled
        } else {
            self.pending.clear();
            self.tip = point.clone();
            RollbackResult::OutOfScope(point.clone())
        }
    }

    /// Drain the pending points accumulated since the last call.
    pub fn take_pending(&mut self) -> Vec<ChainPoint> {
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::crypto::hash::Hash;

    fn hash_of(n: u8) -> BlockHash {
        Hash::new([n; 32])
    }

    fn point_of(slot: u64, n: u8) -> ChainPoint {
        ChainPoint::Specific(slot, hash_of(n))
    }

    // -- construction --

    #[test]
    fn start_from_origin() {
        let chain = ChainFragment::start(ChainPoint::Origin);
        assert_eq!(chain.tip, ChainPoint::Origin);
        assert!(chain.pending.is_empty());
    }

    #[test]
    fn start_from_specific() {
        let chain = ChainFragment::start(point_of(10, 10));
        assert_eq!(chain.tip, point_of(10, 10));
        assert!(chain.pending.is_empty());
    }

    // -- continuity --

    #[test]
    fn from_origin_accepts_any_first_block() {
        let mut chain = ChainFragment::start(ChainPoint::Origin);

        let result = chain.roll_forward(point_of(1, 1), Some(hash_of(0)));
        assert!(result.is_ok());
        assert_eq!(chain.tip, point_of(1, 1));
        assert_eq!(chain.pending.len(), 1);
    }

    #[test]
    fn from_specific_validates_first_block() {
        let mut chain = ChainFragment::start(point_of(10, 10));

        let result = chain.roll_forward(point_of(11, 11), Some(hash_of(10)));
        assert!(result.is_ok());
        assert_eq!(chain.tip, point_of(11, 11));
    }

    #[test]
    fn rejects_mismatched_parent_hash() {
        let mut chain = ChainFragment::start(point_of(10, 10));

        let result = chain.roll_forward(point_of(11, 11), Some(hash_of(99)));
        assert!(matches!(
            result.unwrap_err(),
            ConsensusError::BrokenContinuity { .. }
        ));

        // State unchanged after rejection
        assert_eq!(chain.tip, point_of(10, 10));
        assert!(chain.pending.is_empty());
    }

    #[test]
    fn sequential_forwards_maintain_chain() {
        let mut chain = ChainFragment::start(point_of(0, 0));

        for i in 1..=5u8 {
            chain
                .roll_forward(point_of(i as u64, i), Some(hash_of(i - 1)))
                .unwrap();
        }

        assert_eq!(chain.tip, point_of(5, 5));
        assert_eq!(chain.pending.len(), 5);
    }

    // -- slot ordering --

    #[test]
    fn rejects_same_slot() {
        let mut chain = ChainFragment::start(point_of(10, 10));

        let result = chain.roll_forward(point_of(10, 11), Some(hash_of(10)));
        assert!(matches!(
            result.unwrap_err(),
            ConsensusError::SlotNotIncreasing {
                slot: 10,
                tip_slot: 10,
            }
        ));
    }

    #[test]
    fn rejects_decreasing_slot() {
        let mut chain = ChainFragment::start(point_of(10, 10));

        let result = chain.roll_forward(point_of(5, 11), Some(hash_of(10)));
        assert!(matches!(
            result.unwrap_err(),
            ConsensusError::SlotNotIncreasing {
                slot: 5,
                tip_slot: 10,
            }
        ));
    }

    #[test]
    fn slot_check_skipped_from_origin() {
        let mut chain = ChainFragment::start(ChainPoint::Origin);

        let result = chain.roll_forward(point_of(0, 1), None);
        assert!(result.is_ok());
    }

    // -- take_pending --

    #[test]
    fn take_pending_drains_and_preserves_tip() {
        let mut chain = ChainFragment::start(point_of(0, 0));

        chain
            .roll_forward(point_of(1, 1), Some(hash_of(0)))
            .unwrap();
        chain
            .roll_forward(point_of(2, 2), Some(hash_of(1)))
            .unwrap();

        let points = chain.take_pending();
        assert_eq!(points.len(), 2);
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, point_of(2, 2));

        // Can continue building from the same tip
        let result = chain.roll_forward(point_of(3, 3), Some(hash_of(2)));
        assert!(result.is_ok());
    }

    // -- rollback --

    #[test]
    fn rollback_within_pending_truncates() {
        let mut chain = ChainFragment::start(point_of(0, 0));

        chain
            .roll_forward(point_of(1, 1), Some(hash_of(0)))
            .unwrap();
        chain
            .roll_forward(point_of(2, 2), Some(hash_of(1)))
            .unwrap();
        chain
            .roll_forward(point_of(3, 3), Some(hash_of(2)))
            .unwrap();

        let result = chain.roll_back(&point_of(1, 1));
        assert!(matches!(result, RollbackResult::Handled));
        assert_eq!(chain.pending.len(), 1);
        assert_eq!(chain.tip, point_of(1, 1));

        // Can continue from the rollback point
        let result = chain.roll_forward(point_of(4, 4), Some(hash_of(1)));
        assert!(result.is_ok());
    }

    #[test]
    fn rollback_out_of_scope_clears_pending_and_updates_tip() {
        let mut chain = ChainFragment::start(point_of(0, 0));

        chain
            .roll_forward(point_of(1, 1), Some(hash_of(0)))
            .unwrap();
        chain
            .roll_forward(point_of(2, 2), Some(hash_of(1)))
            .unwrap();

        let result = chain.roll_back(&point_of(100, 100));
        assert!(matches!(result, RollbackResult::OutOfScope(_)));
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, point_of(100, 100));
    }

    #[test]
    fn rollback_to_origin_clears_tip() {
        let mut chain = ChainFragment::start(point_of(0, 0));

        chain
            .roll_forward(point_of(1, 1), Some(hash_of(0)))
            .unwrap();

        let result = chain.roll_back(&ChainPoint::Origin);
        assert!(matches!(result, RollbackResult::OutOfScope(_)));
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, ChainPoint::Origin);
    }
}
