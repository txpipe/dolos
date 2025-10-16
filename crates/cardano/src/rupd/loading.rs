use dolos_core::{ChainError, Domain, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;
use tracing::{debug, info, warn};

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
    let blocks_minted = epoch
        .rolling
        .snapshot_at(epoch.number - 1)
        .map(|x| x.blocks_minted);

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
    if epoch.pparams.active().is_byron() {
        info!("no pot changes during Byron epoch");
        return Ok(neutral_incentives());
    }

    let rho_param = epoch.pparams.active().ensure_rho()?;
    let tau_param = epoch.pparams.active().ensure_tau()?;

    let fee_ss = epoch
        .rolling
        .snapshot_at(epoch.number - 1)
        .map(|x| x.gathered_fees)
        .unwrap_or_default();

    let eta = define_eta(genesis, epoch.pparams.active(), epoch)?;

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
        pool_id: Option<PoolHash>,
        stake: u64,
    ) -> Result<(), ChainError> {
        if let Some(pool_id) = pool_id {
            self.accounts_by_pool
                .insert(pool_id, account.clone(), stake);

            self.pool_stake
                .entry(pool_id)
                .and_modify(|x| *x += stake)
                .or_insert(stake);

            self.active_stake_sum += stake;
        }

        self.total_stake_sum += stake;

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

            let Some(pool_snapshot) = pool.snapshot.snapshot_at(stake_epoch) else {
                warn!(operator = %pool.operator, "skipping pool without stake epoch snapshot");
                continue;
            };

            if pool_snapshot.is_retired {
                warn!(operator = %pool.operator, "skipping retired or pending pool are stake epoch");
                continue;
            }

            // for tracking blocks we switch to the performance epoch (previous epoch, the
            // one we're computing rewards for)
            let Some(pool_snapshot) = pool.snapshot.snapshot_at(performance_epoch) else {
                continue;
            };

            snapshot
                .pool_params
                .insert(pool.operator, pool.params.clone());

            snapshot
                .pool_blocks
                .insert(pool.operator, pool_snapshot.blocks_minted as u64);
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

            let pool = account.pool.snapshot_at(stake_epoch).and_then(|x| *x);

            let stake = account
                .total_stake
                .snapshot_at(stake_epoch)
                .cloned()
                .unwrap_or_default();

            snapshot.track_stake(&account.credential, pool, stake)?;
        }

        for (pool_id, stake) in snapshot.pool_stake.iter() {
            debug!(%pool_id, %stake, "pool stake");
        }

        debug!(
            total_stake = %snapshot.total_stake_sum,
            active_stake = %snapshot.active_stake_sum,
            "stake aggregation"
        );

        Ok(snapshot)
    }
}

impl RupdWork {
    pub fn snapshot_epoch(&self) -> Option<u64> {
        if self.current_epoch < 3 {
            return None;
        }

        let snapshot_epoch = self.current_epoch - 3;

        Some(snapshot_epoch)
    }

    pub fn performance_epoch(&self) -> Option<u64> {
        if self.current_epoch < 3 {
            return None;
        }

        let performance_epoch = self.current_epoch - 1;

        Some(performance_epoch)
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
            pparams: epoch.pparams.for_rewards().cloned(),
            pots,
            incentives,
            snapshot: StakeSnapshot::default(),
        };

        if current_epoch >= 4 {
            work.snapshot = StakeSnapshot::load::<D>(state, current_epoch - 3, current_epoch - 1)?;
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

    fn total_stake(&self) -> u64 {
        self.snapshot.total_stake_sum
    }

    fn active_stake(&self) -> u64 {
        self.snapshot.active_stake_sum
    }

    fn epoch_blocks(&self) -> u64 {
        self.snapshot.pool_blocks.values().sum()
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

    fn iter_all_pools(&self) -> impl Iterator<Item = (PoolHash, &PoolParams)> {
        self.snapshot
            .pool_params
            .iter()
            .map(|(pool, params)| (*pool, params))
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
        *self.snapshot.pool_blocks.get(&pool).unwrap_or(&0)
    }

    fn pre_allegra(&self) -> bool {
        self.pparams().protocol_major().unwrap_or(0) < 3
    }
}
