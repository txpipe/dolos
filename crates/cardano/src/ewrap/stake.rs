//! The stake leg of the merged account-epoch row.
//!
//! An epoch's rows carry the distribution active in it, the one RUPD logs the
//! epoch's `PoolEpochLog` from. ESTART opening the epoch writes each row with
//! the stake leg alone, at the epoch's start slot, and the EWRAP closing the
//! epoch after it writes the full row under the same key. Both build the leg
//! here, from the same gate, pool filter and protocol, so the full row
//! replaces the stake-only one rather than disagreeing with it.

use std::collections::HashSet;

use pallas::ledger::primitives::Epoch;

use crate::{
    rupd::loading::stake_snapshot_epoch, AccountEpochLog, AccountState, ChainSummary, EraKey,
    PoolHash, PoolState,
};

/// `RupdWork::stake_epochs`, read from the closing boundary's side: the stake
/// snapshot RUPD logged the epoch's distribution from, and the protocol its
/// stake totals are read under. The rows it feeds describe `ending_epoch - 1`.
///
/// The gate is RUPD's stake gate ([`stake_snapshot_epoch`]), not its reward
/// gate: the first stake epoch, whose snapshot is the first Shelley epoch's,
/// has a distribution and no rewards, and its rows carry the stake leg alone.
/// A merged row written where RUPD logged no distribution would be a
/// population this log invented.
///
/// The protocol is the era of `snapshot_epoch + 1`, not of the snapshot epoch:
/// the Haskell ledger computes the mark snapshot under the *entering* epoch's
/// rules, which is what decides whether pointer-address stake counts.
pub(crate) fn stake_snapshot_of(
    ending_epoch: Epoch,
    chain: &ChainSummary,
) -> Option<(Epoch, EraKey)> {
    let snapshot_epoch = stake_snapshot_epoch(ending_epoch, chain.first_shelley_epoch())?;

    let protocol = EraKey::from(chain.era_for_epoch(snapshot_epoch + 1).protocol);

    Some((snapshot_epoch, protocol))
}

/// Whether `pool`'s delegators are in the distribution of `stake_epoch`: it
/// has a snapshot at that epoch, and the snapshot is not retired.
pub(crate) fn admits_pool(pool: &PoolState, stake_epoch: Epoch) -> bool {
    pool.snapshot
        .snapshot_at(stake_epoch)
        .is_some_and(|snapshot| !snapshot.is_retired)
}

/// `account`'s row carrying the stake leg alone, `None` when the account is
/// not in the distribution of `stake_epoch`: it delegated to no pool there, or
/// to one outside `pools`.
///
/// Not filtered on `> 0`: a zero-stake delegator's row must exist.
pub(crate) fn stake_leg(
    account: &AccountState,
    stake_epoch: Epoch,
    protocol: EraKey,
    pools: &HashSet<PoolHash>,
) -> Option<AccountEpochLog> {
    let pool = account.delegated_pool_at(stake_epoch)?;

    if !pools.contains(pool) {
        return None;
    }

    let stake = account
        .stake
        .snapshot_at(stake_epoch)
        .map(|stake| stake.total_for_era(protocol))
        .unwrap_or_default();

    Some(AccountEpochLog {
        active_stake: Some(stake),
        pool_id: Some(*pool),
        ..Default::default()
    })
}
