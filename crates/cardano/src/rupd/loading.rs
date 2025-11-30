use dolos_core::{ChainError, Domain, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;
use tracing::trace;

use crate::{
    pots::Pots,
    rupd::{RupdWork, StakeSnapshot},
    AccountState, EraProtocol, FixedNamespace as _, PParamsSet, PoolHash, PoolParams, PoolState,
};

impl StakeSnapshot {
    fn track_stake(
        &mut self,
        account: &StakeCredential,
        pool_id: PoolHash,
        stake: u64,
    ) -> Result<(), ChainError> {
        self.accounts_by_pool
            .insert(pool_id, account.clone(), stake);

        self.pool_stake
            .entry(pool_id)
            .and_modify(|x| *x += stake)
            .or_insert(stake);

        self.active_stake_sum += stake;

        Ok(())
    }

    pub fn get_pool_stake(&self, pool: &PoolHash) -> u64 {
        *self.pool_stake.get(pool).unwrap_or(&0)
    }

    pub fn load<D: Domain>(
        state: &D::State,
        stake_epoch: u64,
        protocol: EraProtocol,
    ) -> Result<Self, ChainError> {
        let mut snapshot = Self::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            if pool.snapshot.snapshot_at(stake_epoch).is_some() {
                snapshot.pools.insert(pool.operator, pool.snapshot);
            }
        }

        let accounts = state.iter_entities_typed::<AccountState>(AccountState::NS, None)?;

        for record in accounts {
            let (_, account) = record?;

            // TODO: check if we really need to make ths check. It might be adding noise to the data.
            if account.is_registered() {
                snapshot
                    .registered_accounts
                    .insert(account.credential.clone());
            }

            let Some(pool) = account.delegated_pool_at(stake_epoch) else {
                continue;
            };

            let Some(pool_state) = snapshot.pools.get(pool) else {
                continue;
            };

            let Some(stake_snapshot) = pool_state.snapshot_at(stake_epoch) else {
                continue;
            };

            if stake_snapshot.is_retired {
                continue;
            }

            let stake = account
                .stake
                .snapshot_at(stake_epoch)
                .map(|x| x.total_for_era(protocol))
                .unwrap_or_default();

            snapshot.track_stake(&account.credential, *pool, stake)?;
        }

        for (pool_id, stake) in snapshot.pool_stake.iter() {
            trace!(%pool_id, %stake, "pool stake");
        }

        trace!(
            active_stake = %snapshot.active_stake_sum,
            "stake aggregation"
        );

        Ok(snapshot)
    }
}

impl std::fmt::Display for StakeSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (pool, stake) in self.pool_stake.iter() {
            writeln!(f, "| {pool} | {stake} |")?;
        }

        for (pool, pool_state) in self.pools.iter() {
            let params = pool_state.go().map(|x| &x.params);
            let pledge = params.map(|x| x.pledge).unwrap_or(0);
            let blocks = pool_state.mark().map(|x| x.blocks_minted).unwrap_or(0) as u64;
            writeln!(f, "| {pool} | {blocks} | {pledge} |")?;
        }

        Ok(())
    }
}

pub type SnapshotEpoch = u64;
pub type PerformanceEpoch = u64;

impl RupdWork {
    pub fn relevant_epochs(&self) -> Option<(SnapshotEpoch, PerformanceEpoch)> {
        if self.current_epoch < 4 {
            return None;
        }

        let snapshot_epoch = self.current_epoch - 3;
        let performance_epoch = self.current_epoch - 1;

        Some((snapshot_epoch, performance_epoch))
    }

    pub fn load<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<RupdWork, ChainError> {
        let epoch = crate::load_epoch::<D>(state)?;

        let current_epoch = epoch.number;

        let max_supply =
            genesis
                .shelley
                .max_lovelace_supply
                .ok_or(ChainError::GenesisFieldMissing(
                    "max_lovelace_supply".to_string(),
                ))?;

        let pots = epoch.initial_pots.clone();

        let chain = crate::load_era_summary::<D>(state)?;

        let mut work = RupdWork {
            chain,
            current_epoch,
            max_supply,
            pparams: epoch.pparams.mark().cloned(),
            pots,
            available_rewards: epoch.incentives.available_rewards,
            snapshot: StakeSnapshot::default(),
        };

        if let Some((snapshot_epoch, _)) = work.relevant_epochs() {
            let era = work.chain.era_for_epoch(snapshot_epoch);
            let protocol = EraProtocol::from(era.protocol);
            work.snapshot = StakeSnapshot::load::<D>(state, snapshot_epoch, protocol)?;
        }

        Ok(work)
    }
}

impl crate::rewards::RewardsContext for RupdWork {
    fn available_rewards(&self) -> u64 {
        self.available_rewards
    }

    fn pots(&self) -> &Pots {
        &self.pots
    }

    fn active_stake(&self) -> u64 {
        self.snapshot.active_stake_sum
    }

    fn epoch_blocks(&self) -> u64 {
        self.snapshot
            .pools
            .values()
            .map(|x| x.mark().map(|x| x.blocks_minted).unwrap_or(0) as u64)
            .sum()
    }

    fn pool_stake(&self, pool: PoolHash) -> u64 {
        *self.snapshot.pool_stake.get(&pool).unwrap_or(&0)
    }

    fn account_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64 {
        self.snapshot.accounts_by_pool.get_stake(pool, account)
    }

    fn is_account_registered(&self, account: &StakeCredential) -> bool {
        self.snapshot.registered_accounts.contains(account)
    }

    fn iter_all_pools(&self) -> impl Iterator<Item = PoolHash> {
        self.snapshot.pools.keys().cloned()
    }

    fn pool_params(&self, pool: PoolHash) -> &PoolParams {
        self.snapshot
            .pools
            .get(&pool)
            .unwrap()
            .go()
            .map(|x| &x.params)
            .unwrap()
    }

    fn pool_delegators(&self, pool: PoolHash) -> impl Iterator<Item = StakeCredential> {
        self.snapshot
            .accounts_by_pool
            .iter_delegators(&pool)
            .map(|(cred, _)| cred.clone())
    }

    fn pparams(&self) -> &PParamsSet {
        self.pparams
            .as_ref()
            .expect("pparams not available for rewards")
    }

    fn pool_blocks(&self, pool: PoolHash) -> u64 {
        *self
            .snapshot
            .pools
            .get(&pool)
            .unwrap()
            .mark()
            .map(|x| &x.blocks_minted)
            .unwrap() as u64
    }

    fn pre_allegra(&self) -> bool {
        self.pparams().protocol_major().unwrap_or(0) < 3
    }
}
