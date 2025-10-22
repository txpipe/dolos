use dolos_core::{ChainError, Domain, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;
use tracing::{debug, info};

use crate::{
    load_era_summary, pallas_ratio,
    pots::{self, EpochIncentives, Eta, Pots},
    ratio,
    rupd::{RupdWork, StakeSnapshot},
    AccountState, EpochState, FixedNamespace as _, PParamsSet, PoolHash, PoolParams, PoolState,
};

fn define_eta(
    genesis: &Genesis,
    pparams: &PParamsSet,
    epoch: &EpochState,
) -> Result<Eta, ChainError> {
    let blocks_minted = epoch.rolling.mark().map(|x| x.blocks_minted);

    let Some(blocks_minted) = blocks_minted else {
        // TODO: check if returning eta = 1 on epoch 0 is what the specs says.
        return Ok(ratio!(1));
    };

    let f_param = genesis
        .shelley
        .active_slots_coeff
        .ok_or(ChainError::GenesisFieldMissing(
            "active_slots_coeff".to_string(),
        ))?;

    let d_param = pparams.ensure_d()?;
    let epoch_length = pparams.ensure_epoch_length()?;

    let eta = pots::calculate_eta(
        blocks_minted as u64,
        pallas_ratio!(d_param),
        f_param,
        epoch_length,
    );

    Ok(eta)
}

fn neutral_incentives() -> EpochIncentives {
    EpochIncentives {
        total: 0,
        treasury_tax: 0,
        available_rewards: 0,
        used_fees: 0,
    }
}

fn define_epoch_incentives(
    genesis: &Genesis,
    epoch: &EpochState,
    reserves: u64,
) -> Result<EpochIncentives, ChainError> {
    let pparams = epoch.pparams.unwrap_live();

    if pparams.is_byron() {
        info!("no pot changes during Byron epoch");
        return Ok(neutral_incentives());
    }

    let rho_param = pparams.ensure_rho()?;
    let tau_param = pparams.ensure_tau()?;

    let fee_ss = epoch
        .rolling
        .mark()
        .map(|x| x.gathered_fees)
        .unwrap_or_default();

    let eta = define_eta(genesis, pparams, epoch)?;

    let incentives = pots::epoch_incentives(
        reserves,
        fee_ss,
        pallas_ratio!(rho_param),
        pallas_ratio!(tau_param),
        eta,
    );

    Ok(incentives)
}

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
        performance_epoch: u64,
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

            // registration status

            if account.is_registered() {
                snapshot
                    .registered_accounts
                    .insert(account.credential.clone());
            }

            let Some(pool) = account.delegated_pool_at(stake_epoch) else {
                continue;
            };

            let Some(pool_state) = snapshot.pools.get(&pool) else {
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
                .map(|x| x.total())
                .unwrap_or_default();

            snapshot.track_stake(&account.credential, *pool, stake)?;
        }

        for (pool_id, stake) in snapshot.pool_stake.iter() {
            debug!(%pool_id, %stake, "pool stake");
        }

        debug!(
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

        let incentives = define_epoch_incentives(genesis, &epoch, pots.reserves)?;

        debug!(
            %incentives.total,
            %incentives.treasury_tax,
            %incentives.available_rewards,
            "defined pot delta"
        );

        let chain = load_era_summary::<D>(state)?;

        let mut work = RupdWork {
            current_epoch,
            max_supply,
            chain,
            pparams: epoch.pparams.mark().cloned(),
            pots,
            incentives,
            snapshot: StakeSnapshot::default(),
        };

        if let Some((snapshot_epoch, performance_epoch)) = work.relevant_epochs() {
            work.snapshot = StakeSnapshot::load::<D>(state, snapshot_epoch, performance_epoch)?;
        }

        Ok(work)
    }
}

impl crate::rewards::RewardsContext for RupdWork {
    fn incentives(&self) -> &EpochIncentives {
        &self.incentives
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
