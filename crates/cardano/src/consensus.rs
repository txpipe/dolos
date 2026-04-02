//! Chain consensus utilities for the pull stage.
//!
//! This module provides [`ChainFragment`], a lightweight tracker for the
//! upstream chain position that validates parent-hash continuity on each
//! new header and accumulates pending points for block fetching.

use dolos_core::{BlockHash, ChainPoint};
use pallas::ledger::traverse::MultiEraHeader;

/// Errors that can occur during consensus validation.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// A header's `previous_hash` doesn't match the expected chain tip.
    #[error("block at slot {slot} has parent hash {got} but expected {expected}")]
    BrokenContinuity {
        slot: u64,
        expected: BlockHash,
        got: BlockHash,
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

/// Tracks the upstream chain position for the pull stage.
///
/// Maintains the tip hash (for continuity validation) separately from
/// the pending batch of points (which will be fetched via blockfetch).
/// On each [`roll_forward`](Self::roll_forward), validates that the new
/// header's `previous_hash` matches the current tip.
pub struct ChainFragment {
    /// Hash of the latest known block. Used to validate that the next header
    /// is a valid continuation. `None` when starting from Origin.
    tip: Option<BlockHash>,

    /// Points accumulated during the current header-gathering pass.
    /// Drained by [`take_pending`](Self::take_pending) after the pass
    /// completes.
    pending: Vec<ChainPoint>,
}

impl ChainFragment {
    /// Create a new fragment seeded from the chainsync intersection point.
    pub fn from_intersection(point: &ChainPoint) -> Self {
        Self {
            tip: point.hash(),
            pending: Vec::new(),
        }
    }

    /// Validate chain continuity and accept a new header.
    ///
    /// Returns the header's [`ChainPoint`] on success.
    /// Returns [`Err(ConsensusError)`] if the header's `previous_hash`
    /// doesn't match our tip.
    pub fn roll_forward(&mut self, header: &MultiEraHeader) -> Result<ChainPoint, ConsensusError> {
        if let (Some(prev), Some(tip)) = (header.previous_hash(), self.tip) {
            if prev != tip {
                return Err(ConsensusError::BrokenContinuity {
                    slot: header.slot(),
                    expected: tip,
                    got: prev,
                });
            }
        }

        let point = ChainPoint::Specific(header.slot(), header.hash());
        self.tip = Some(header.hash());
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
            self.tip = point.hash();
            RollbackResult::Handled
        } else {
            self.pending.clear();
            self.tip = point.hash();
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

    /// Simulate roll_forward by providing prev_hash and own hash directly,
    /// bypassing header parsing.
    fn simulate_roll_forward(
        chain: &mut ChainFragment,
        slot: u64,
        prev_hash: Option<BlockHash>,
        own_hash: BlockHash,
    ) -> Result<ChainPoint, ConsensusError> {
        if let (Some(prev), Some(tip)) = (prev_hash, chain.tip) {
            if prev != tip {
                return Err(ConsensusError::BrokenContinuity {
                    slot,
                    expected: tip,
                    got: prev,
                });
            }
        }

        let point = ChainPoint::Specific(slot, own_hash);
        chain.tip = Some(own_hash);
        chain.pending.push(point.clone());
        Ok(point)
    }

    #[test]
    fn from_origin_accepts_any_first_block() {
        let mut chain = ChainFragment::from_intersection(&ChainPoint::Origin);

        let result = simulate_roll_forward(&mut chain, 1, Some(hash_of(0)), hash_of(1));
        assert!(result.is_ok());
        assert_eq!(chain.tip, Some(hash_of(1)));
        assert_eq!(chain.pending.len(), 1);
    }

    #[test]
    fn from_specific_intersection_validates_first_block() {
        let mut chain = ChainFragment::from_intersection(&point_of(10, 10));

        let result = simulate_roll_forward(&mut chain, 11, Some(hash_of(10)), hash_of(11));
        assert!(result.is_ok());
        assert_eq!(chain.tip, Some(hash_of(11)));
    }

    #[test]
    fn rejects_mismatched_parent_hash() {
        let mut chain = ChainFragment::from_intersection(&point_of(10, 10));

        let result = simulate_roll_forward(&mut chain, 11, Some(hash_of(99)), hash_of(11));
        assert!(result.is_err());

        let ConsensusError::BrokenContinuity {
            slot,
            expected,
            got,
        } = result.unwrap_err();
        assert_eq!(slot, 11);
        assert_eq!(expected, hash_of(10));
        assert_eq!(got, hash_of(99));

        // State unchanged after rejection
        assert_eq!(chain.tip, Some(hash_of(10)));
        assert!(chain.pending.is_empty());
    }

    #[test]
    fn sequential_forwards_maintain_chain() {
        let mut chain = ChainFragment::from_intersection(&point_of(0, 0));

        for i in 1..=5u8 {
            let result =
                simulate_roll_forward(&mut chain, i as u64, Some(hash_of(i - 1)), hash_of(i));
            assert!(result.is_ok());
        }

        assert_eq!(chain.tip, Some(hash_of(5)));
        assert_eq!(chain.pending.len(), 5);
    }

    #[test]
    fn take_pending_drains_and_preserves_tip() {
        let mut chain = ChainFragment::from_intersection(&point_of(0, 0));

        simulate_roll_forward(&mut chain, 1, Some(hash_of(0)), hash_of(1)).unwrap();
        simulate_roll_forward(&mut chain, 2, Some(hash_of(1)), hash_of(2)).unwrap();

        let points = chain.take_pending();
        assert_eq!(points.len(), 2);
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, Some(hash_of(2)));

        // Can continue building from the same tip
        let result = simulate_roll_forward(&mut chain, 3, Some(hash_of(2)), hash_of(3));
        assert!(result.is_ok());
    }

    #[test]
    fn rollback_within_pending_truncates() {
        let mut chain = ChainFragment::from_intersection(&point_of(0, 0));

        simulate_roll_forward(&mut chain, 1, Some(hash_of(0)), hash_of(1)).unwrap();
        simulate_roll_forward(&mut chain, 2, Some(hash_of(1)), hash_of(2)).unwrap();
        simulate_roll_forward(&mut chain, 3, Some(hash_of(2)), hash_of(3)).unwrap();

        let result = chain.roll_back(&point_of(1, 1));
        assert!(matches!(result, RollbackResult::Handled));
        assert_eq!(chain.pending.len(), 1);
        assert_eq!(chain.tip, Some(hash_of(1)));

        // Can continue from the rollback point
        let result = simulate_roll_forward(&mut chain, 4, Some(hash_of(1)), hash_of(4));
        assert!(result.is_ok());
    }

    #[test]
    fn rollback_out_of_scope_clears_pending_and_updates_tip() {
        let mut chain = ChainFragment::from_intersection(&point_of(0, 0));

        simulate_roll_forward(&mut chain, 1, Some(hash_of(0)), hash_of(1)).unwrap();
        simulate_roll_forward(&mut chain, 2, Some(hash_of(1)), hash_of(2)).unwrap();

        let result = chain.roll_back(&point_of(100, 100));
        assert!(matches!(result, RollbackResult::OutOfScope(_)));
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, Some(hash_of(100)));
    }

    #[test]
    fn rollback_to_origin_clears_tip() {
        let mut chain = ChainFragment::from_intersection(&point_of(0, 0));

        simulate_roll_forward(&mut chain, 1, Some(hash_of(0)), hash_of(1)).unwrap();

        let result = chain.roll_back(&ChainPoint::Origin);
        assert!(matches!(result, RollbackResult::OutOfScope(_)));
        assert!(chain.pending.is_empty());
        assert_eq!(chain.tip, None);
    }
}
