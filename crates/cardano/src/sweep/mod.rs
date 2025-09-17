use std::collections::HashMap;

use dolos_core::{BlockSlot, ChainError, Domain, EntityKey};
use pallas::{crypto::hash::Hash, ledger::primitives::RationalNumber};

use crate::{Config, EpochState, EraSummary};

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

#[derive(Debug)]
pub struct PoolData {
    pub fixed_cost: u64,
    pub margin_cost: RationalNumber,
    pub declared_pledge: u64,
    pub minted_blocks: u32,
}

#[derive(Debug)]
pub struct Pots {
    pub reserves: u64,
    pub treasury: u64,
}

#[derive(Debug)]
#[derive(Default)]
pub struct Snapshot {
    pub total_stake: u64,
    pub pool_by_account: HashMap<AccountId, PoolId>,
    pub pool_stake: HashMap<PoolId, u64>,
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
    pub prev_version: u16,
    pub new_version: u16,
    pub epoch_length: u64,
    pub slot_length: u64,
}

#[derive(Debug)]
pub struct BoundaryWork {
    // loaded
    pub active_era: EraSummary,
    pub active_state: Option<EpochState>,
    pub active_snapshot: Snapshot,
    pub waiting_state: Option<EpochState>,
    pub ending_state: EpochState,
    pub ending_snapshot: Snapshot,
    pub pools: HashMap<PoolId, PoolData>,
    pub mutable_slots: u64,
    pub shelley_hash: Hash<32>,

    // computed
    pub pot_delta: Option<PotDelta>,
    pub effective_rewards: Option<u64>,
    pub pool_rewards: HashMap<PoolId, u64>,
    pub starting_state: Option<EpochState>,
    pub era_transition: Option<EraTransition>,
}

pub fn sweep<D: Domain>(domain: &D, _: BlockSlot, config: &Config) -> Result<(), ChainError> {
    // TODO: this should all be one big atomic operation, but for that we need to
    // refactor stores to include start / commit semantics

    let mut boundary = BoundaryWork::load(domain)?;

    boundary.compute()?;

    boundary.commit(domain)?;

    if let Some(stop_epoch) = config.stop_epoch {
        if boundary.ending_state.number >= stop_epoch {
            return Err(ChainError::StopEpochReached);
        }
    }

    Ok(())
}
