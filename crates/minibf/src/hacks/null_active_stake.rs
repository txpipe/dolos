//! This module lists the epochs where the reference indexer reports a `null`
//! active stake, but Dolos computes a genesis-derived value. The reference
//! indexer is db-sync, which the Blockfrost API uses.
//!
//! The minibf epoch mapper sums the per-pool `StakeLog`s to report the active
//! stake of a historical epoch. Dolos writes these logs from epoch
//! `first_shelley_epoch + 3` and later (see
//! `dolos_cardano::rupd::loading::RupdWork::relevant_epochs`). A look-ahead
//! makes Dolos report the sum one epoch earlier. Thus Dolos gives a value
//! continuously from that first epoch.
//!
//! The early history of preprod has a gap in the stake snapshot. The reference
//! shows this gap, but Dolos does not. The reference reports active stake for
//! epochs 6-12. It reports `null` for epochs 13-28. It reports real values from
//! epoch 29 and later.
//!
//! Epochs 0-5 have no `StakeLog` and already read `null`. Thus only epochs
//! 13-28 need an override.
//!
//! A test against the live Blockfrost API shows this pattern. Preprod epoch 12
//! has a value. Epochs 13-28 are `null`. Epoch 29 has a value again.
//!
//! Preview and mainnet have no such gap. Preview gives its first value at epoch
//! 2. Mainnet gives its first value at epoch 210. Thus their natural output
//! already matches the reference.

use pallas::ledger::primitives::Epoch;

/// This function returns `true` for an epoch that the reference reports as
/// `null`, but Dolos holds a value.
pub fn contains(magic: u32, epoch: Epoch) -> bool {
    match magic {
        1 => (13..=28).contains(&epoch), // preprod early stake-snapshot gap
        _ => false,
    }
}
