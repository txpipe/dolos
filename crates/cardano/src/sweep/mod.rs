use std::collections::{HashMap, HashSet};

use dolos_core::{BlockSlot, ChainError, Domain, EntityKey};
use pallas::{crypto::hash::Hash, ledger::primitives::RationalNumber};
use tracing::{info, instrument};

use crate::{
    Config, DRepState, EpochState, EraProtocol, EraSummary, PParamsSet, RewardLog, StakeLog,
};

pub mod commit;
pub mod compute;
pub mod loading;

pub use compute::compute_genesis_pots;

// Epoch nomenclature
// - ending: the epoch that is currently ending in this boundary.
// - starting: the epoch that is currently starting in this boundary.
// - waiting: (ending - 1) the epoch that will become _active_ after the
//   boundary.
// - active: (ending - 2) the epoch considered active for stake distribution &
//   pool parameters

// Reward calculation sequencing
// - 1. Sweep account data.
//   - Take snapshot of account balances for _ending_ epoch
//   - Take snapshot of stake pool distribution for _ending_ epoch
// - 3. Compute theoretical pots
// - 4. Compute pool performance
// - 5. Distribute rewards to pools
// - 6. Rotate snapshot waiting -> active, ending -> waiting

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;

#[derive(Debug)]
pub struct PoolData {
    pub reward_account: Vec<u8>,
    pub fixed_cost: u64,
    pub margin_cost: RationalNumber,
    pub declared_pledge: u64,
    pub minted_blocks: u32,
    pub retiring_epoch: Option<u64>,
}

#[derive(Debug)]
pub struct Pots {
    pub reserves: u64,
    pub treasury: u64,
    pub utxos: u64,
}

#[derive(Debug, Default, Clone)]
pub struct DelegatorMap(HashMap<PoolId, HashMap<AccountId, u64>>);

impl DelegatorMap {
    pub fn insert(&mut self, pool_id: PoolId, account_id: AccountId, stake: u64) {
        self.0.entry(pool_id).or_default().insert(account_id, stake);
    }

    pub fn iter_delegators(&self, pool_id: &PoolId) -> impl Iterator<Item = (&AccountId, &u64)> {
        self.0.get(pool_id).into_iter().flatten()
    }
}

#[derive(Debug, Default)]
pub struct Snapshot {
    pub total_stake: u64,
    pub accounts_by_pool: DelegatorMap,
    pub accounts_by_drep: DelegatorMap,
    pub pool_stake: HashMap<PoolId, u64>,
    pub drep_stake: HashMap<DRepId, u64>,
}

impl Snapshot {
    // alias just for semantic clarity
    pub fn empty() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct PotDelta {
    pub incentives: u64,
    pub treasury_tax: u64,
    pub available_rewards: u64,
}

#[derive(Debug)]
pub struct EraTransition {
    pub prev_version: EraProtocol,
    pub new_version: EraProtocol,
    pub new_pparams: PParamsSet,
}

#[derive(Debug)]
pub struct BoundaryWork {
    // loaded
    pub active_protocol: EraProtocol,
    pub active_era: EraSummary,
    pub active_state: Option<EpochState>,
    pub active_snapshot: Snapshot,
    pub waiting_state: Option<EpochState>,
    pub ending_state: EpochState,
    pub ending_snapshot: Snapshot,
    pub pools: HashMap<PoolId, PoolData>,
    pub dreps: HashMap<DRepId, DRepState>,
    pub shelley_hash: Hash<32>,

    // computed
    pub pot_delta: Option<PotDelta>,
    pub effective_rewards: Option<u64>,
    pub pool_rewards: HashMap<PoolId, u64>,
    pub pool_stakes: HashMap<PoolId, StakeLog>,
    pub delegator_rewards: HashMap<AccountId, RewardLog>,
    pub starting_state: Option<EpochState>,
    pub era_transition: Option<EraTransition>,
    pub dropped_pool_delegators: HashSet<AccountId>,
    pub dropped_drep_delegators: HashSet<AccountId>,
    pub retired_dreps: HashSet<DRepId>,
}

#[instrument(skip_all, fields(slot = %slot))]
pub fn sweep<D: Domain>(domain: &D, slot: BlockSlot, config: &Config) -> Result<(), ChainError> {
    info!(slot, "executing sweep");

    let mut boundary = BoundaryWork::load(domain)?;

    boundary.compute(domain.genesis())?;

    boundary.commit(domain, config)?;

    if let Some(stop_epoch) = config.stop_epoch {
        if boundary.ending_state.number >= stop_epoch {
            return Err(ChainError::StopEpochReached);
        }
    }

    Ok(())
}
