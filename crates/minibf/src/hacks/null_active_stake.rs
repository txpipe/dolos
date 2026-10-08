//! Epochs where Blockfrost (db-sync) reports a `null` active stake but Dolos
//! has a value.
//!
//! Dolos sums the per-pool `StakeLog`s from `first_shelley_epoch + 3` on (see
//! `dolos_cardano::rupd::loading::RupdWork::relevant_epochs`), one epoch early
//! thanks to a look-ahead, and with no gap. Preprod's early stake snapshot has
//! one: live Blockfrost has a value for epochs 6-12, `null` for 13-28 and
//! values again from 29. Epochs 0-5 have no `StakeLog` and read `null`
//! already. Preview and mainnet have no gap.

use pallas::ledger::primitives::Epoch;

/// Whether Blockfrost reports `null` for `epoch` where Dolos has a value.
pub fn contains(magic: u32, epoch: Epoch) -> bool {
    match magic {
        1 => (13..=28).contains(&epoch), // preprod early stake-snapshot gap
        _ => false,
    }
}
