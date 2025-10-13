use std::collections::HashSet;

use dolos_core::{ChainError, Domain, Genesis, StateStore};
use pallas::ledger::primitives::StakeCredential;
use tracing::{debug, info, trace};

use crate::{
    load_era_summary, pallas_ratio,
    pots::{self, Eta, PotDelta, Pots},
    ratio,
    rupd::{RupdWork, StakeSnapshot},
    AccountState, EpochState, FixedNamespace as _, PParamsSet, PoolHash, PoolParams, PoolState,
};

fn define_pparams(prev_epoch: Option<&EpochState>, current_epoch: &EpochState) -> PParamsSet {
    if let Some(prev_epoch) = prev_epoch {
        prev_epoch.pparams.clone()
    } else {
        current_epoch.pparams.clone()
    }
}

fn define_eta(
    genesis: &Genesis,
    pparams: &PParamsSet,
    epoch: Option<&EpochState>,
) -> Result<Eta, ChainError> {
    let Some(epoch) = epoch else {
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
        epoch.blocks_minted as u64,
        pallas_ratio!(d_param),
        f_param,
        epoch_length,
    );

    Ok(eta)
}

fn neutral_pot_delta() -> PotDelta {
    PotDelta {
        incentives: 0,
        treasury_tax: 0,
        available_rewards: 0,
        used_fees: 0,
        effective_rewards: None,
        unspendable_rewards: None,
    }
}

fn define_pot_delta(
    genesis: &Genesis,
    pparams: &PParamsSet,
    epoch: Option<&EpochState>,
    reserves: u64,
) -> Result<PotDelta, ChainError> {
    if pparams.is_byron() {
        info!("no pot changes during Byron epoch");
        return Ok(neutral_pot_delta());
    }

    let rho_param = pparams.ensure_rho()?;
    let tau_param = pparams.ensure_tau()?;

    let fee_ss = epoch.map(|e| e.gathered_fees).unwrap_or_default();

    let eta = define_eta(genesis, pparams, epoch)?;

    dbg!(&eta);

    let pot_delta = pots::delta(
        reserves,
        fee_ss,
        pallas_ratio!(rho_param),
        pallas_ratio!(tau_param),
        eta,
    );

    Ok(pot_delta)
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
                .insert(pool_id.clone(), account.clone(), stake);

            self.pool_stake
                .entry(pool_id.clone())
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
        snapshot_epoch: u64,
        performance_epoch: u64,
    ) -> Result<Self, ChainError> {
        let mut snapshot = Self::default();

        let pools = state.iter_entities_typed::<PoolState>(PoolState::NS, None)?;

        for record in pools {
            let (_, pool) = record?;

            let Some(pool_snapshot) = pool.snapshot.version_for(snapshot_epoch) else {
                continue;
            };

            if pool_snapshot.is_pending || pool_snapshot.is_retired {
                continue;
            }

            snapshot
                .pool_params
                .insert(pool.operator, pool.params.clone());

            // for tracking blocks we switch to the performance epoch (previous epoch, the
            // one we're computing rewards for)
            let Some(pool_snapshot) = pool.snapshot.version_for(performance_epoch) else {
                continue;
            };

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

            let pool = account.pool.version_for(snapshot_epoch).and_then(|x| *x);

            let stake = account
                .total_stake
                .version_for(snapshot_epoch)
                .cloned()
                .unwrap_or_default();

            snapshot.track_stake(&account.credential, pool, stake)?;
        }

        for (pool_id, stake) in snapshot.pool_stake.iter() {
            trace!(%pool_id, %stake, "pool stake");
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
    pub fn load<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<RupdWork, ChainError> {
        let set_epoch = crate::load_set_epoch::<D>(state)?;
        let mark_epoch = crate::load_mark_epoch::<D>(state)?;

        let current_epoch = mark_epoch.number;

        let max_supply =
            genesis
                .shelley
                .max_lovelace_supply
                .ok_or(ChainError::GenesisFieldMissing(
                    "max_lovelace_supply".to_string(),
                ))?;

        let pparams = define_pparams(set_epoch.as_ref(), &mark_epoch);

        let pots = mark_epoch.initial_pots.clone();

        let pot_delta = define_pot_delta(genesis, &pparams, set_epoch.as_ref(), pots.reserves)?;

        debug!(
            %pot_delta.incentives,
            %pot_delta.treasury_tax,
            %pot_delta.available_rewards,
            "defined pot delta"
        );

        let chain = load_era_summary::<D>(state)?;

        let mut work = RupdWork {
            for_epoch: set_epoch.as_ref().map(|e| e.number),
            max_supply,
            chain,
            pparams,
            pots,
            pot_delta,
            snapshot: StakeSnapshot::default(),
        };

        if current_epoch >= 3 {
            work.snapshot = StakeSnapshot::load::<D>(state, current_epoch - 3, current_epoch - 1)?;
        }

        Ok(work)
    }
}

impl crate::rewards::RewardsContext for RupdWork {
    fn pot_delta(&self) -> &PotDelta {
        &self.pot_delta
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
        &self.pparams
    }

    fn pool_blocks(&self, pool: PoolHash) -> u64 {
        *self.snapshot.pool_blocks.get(&pool).unwrap_or(&0)
    }

    fn pre_allegra(&self) -> bool {
        self.pparams.protocol_major().unwrap_or(0) < 3
    }
}
