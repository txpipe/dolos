//! The stake distribution ESTART freezes.
//!
//! ESTART opening epoch E freezes the stake distribution of E-1. Each
//! account shard tallies, per pool, the stake, delegators and live pledge of
//! the accounts it rotates; `EStartProgress` carries the partial tally in
//! `EpochState.estart_tally`, in the same commit as the shard. Finalize
//! writes each pool's tally onto the snapshot that becomes `mark`, with the
//! filters of the reward calculation: a pool counts only if it exists in E-1
//! and is not retired there. The values rotate to `set` and `go` with the
//! snapshot, where minibf and RUPD read them.
//!
//! The same shards write the opening epoch's account-epoch rows with the stake
//! leg alone, from the distribution of E-2 (`mark` before rotation), so the
//! two latest epochs have rows before EWRAP writes them in full.

use std::collections::{BTreeMap, HashMap};

use dolos_core::{ChainError, Domain, StateStore};
use pallas::ledger::primitives::{Epoch, StakeCredential};

use crate::{
    estart::{ShardInputs, WorkContext},
    ewrap::stake::{admits_pool, stake_leg, stake_snapshot_of},
    pallas_extras, AccountEpochLog, AccountState, ChainSummary, EpochState, EraKey, PoolHash,
    PoolState, PoolTally,
};

/// Owner credential → the pools it owns.
pub type PoolOwners = HashMap<StakeCredential, Vec<PoolHash>>;

/// Index what every shard of the boundary closing `ended` reads, in one scan
/// of the pools: their declared owners, from the snapshot of `ended`, and the
/// distribution of the opening epoch's stake legs with the pools admitted to
/// it. The opening epoch's full rows are written by the EWRAP that closes the
/// epoch after it, so the gate is that boundary's.
pub fn shard_inputs<D: Domain>(
    state: &D::State,
    ended: Epoch,
    chain: &ChainSummary,
) -> Result<ShardInputs, ChainError> {
    let stake_snapshot = stake_snapshot_of(ended + 2, chain);
    let stake_epoch = stake_snapshot.map(|(epoch, _)| epoch);

    let mut inputs = ShardInputs {
        stake_snapshot,
        ..Default::default()
    };

    for record in state.iter_entities_typed::<PoolState>(None)? {
        let (_, pool) = record?;

        if stake_epoch.is_some_and(|epoch| admits_pool(&pool, epoch)) {
            inputs.stake_pools.insert(pool.operator);
        }

        let Some(snapshot) = pool.snapshot.snapshot_at(ended) else {
            continue;
        };

        for owner in &snapshot.params.pool_owners {
            let credential = pallas_extras::keyhash_to_stake_cred(*owner);
            inputs
                .owners
                .entry(credential)
                .or_default()
                .push(pool.operator);
        }
    }

    Ok(inputs)
}

/// The protocol the distribution of the closing epoch counts stake under:
/// the opening epoch's, as the ledger takes the snapshot under the rules of
/// the epoch it enters. The era summary only learns of an era change in
/// finalize, so a transition is read from the closing epoch's parameters.
pub fn tally_protocol(ended: &EpochState, active_protocol: EraKey) -> EraKey {
    ended
        .pparams
        .era_transition()
        .map(|x| x.new_version)
        .unwrap_or(active_protocol)
}

impl WorkContext {
    /// Add an account, as it stands before rotation, to its pool's tally.
    ///
    /// A delegator counts only with stake above zero: db-sync leaves the
    /// others out of `epoch_stake`, which is what Blockfrost's
    /// `delegators_count` counts. The pool keeps its entry either way.
    pub(crate) fn tally_account(
        &self,
        tally: &mut BTreeMap<PoolHash, PoolTally>,
        account: &AccountState,
    ) {
        let stake_epoch = self.ended_state().number;

        let Some(pool) = account.delegated_pool_at(stake_epoch) else {
            return;
        };

        let stake = account
            .stake
            .snapshot_at(stake_epoch)
            .map(|x| x.total_for_era(self.tally_protocol))
            .unwrap_or_default();

        let entry = tally.entry(*pool).or_default();
        entry.stake += stake;

        if stake > 0 {
            entry.delegators += 1;
        }

        let owns = self
            .inputs
            .owners
            .get(&account.credential)
            .is_some_and(|pools| pools.contains(pool));

        if owns {
            entry.live_pledge += stake;
        }
    }

    /// The opening epoch's stake-only row of an account, as it stands before
    /// rotation: the leg the EWRAP one boundary later writes into the full
    /// row, from the same snapshot, which `mark` holds until this boundary
    /// rotates it to `set`.
    pub(crate) fn account_epoch_row(&self, account: &AccountState) -> Option<AccountEpochLog> {
        let (stake_epoch, protocol) = self.inputs.stake_snapshot?;

        stake_leg(account, stake_epoch, protocol, &self.inputs.stake_pools)
    }

    /// The tally a pool's snapshot of the closing epoch freezes with, `None`
    /// when the pool has no such snapshot. A retired pool and a pool without
    /// delegators freeze zeros.
    pub(crate) fn frozen_tally(&self, pool: &PoolState) -> Option<PoolTally> {
        let ended = self.ended_state();
        let snapshot = pool.snapshot.snapshot_at(ended.number)?;

        if snapshot.is_retired {
            return Some(PoolTally::default());
        }

        let tally = ended
            .estart_tally
            .as_ref()
            .and_then(|x| x.get(&pool.operator))
            .copied()
            .unwrap_or_default();

        Some(tally)
    }
}
