use std::collections::HashMap;

use dolos_core::{batch::WorkDeltas, BlockSlot, ChainError, Domain, EntityKey};
use pallas::crypto::hash::Hash;
use tracing::{info, instrument};

use crate::{
    AccountState, CardanoDelta, CardanoLogic, Config, DRepState, EpochState, EraProtocol,
    EraSummary, PParamsSet, PoolState,
};

pub mod commit;
pub mod compute;
pub mod loading;

// visitors
pub mod retires;
pub mod rewards;
pub mod transition;

pub use compute::compute_genesis_pots;

// Epoch nomenclature
// - ending: the epoch that is currently ending in this boundary.
// - starting: the epoch that is currently starting in this boundary.
// - waiting: (ending - 1) the epoch that will become _active_ after the
//   boundary.
// - active: (ending - 2) the epoch considered active for stake distribution &
//   pool parameters

pub trait BoundaryVisitor: Default {
    #[allow(unused_variables)]
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_account(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &AccountId,
        account: &AccountState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_drep(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        Ok(())
    }
}

pub type AccountId = EntityKey;
pub type PoolId = EntityKey;
pub type DRepId = EntityKey;

#[derive(Debug)]
pub struct Pots {
    pub reserves: u64,
    pub treasury: u64,
    pub utxos: u64,
}

#[derive(Debug, Default, Clone)]
pub struct DelegatorMap(HashMap<EntityKey, HashMap<AccountId, u64>>);

impl DelegatorMap {
    pub fn insert(&mut self, entity_id: EntityKey, account_id: AccountId, stake: u64) {
        self.0
            .entry(entity_id)
            .or_default()
            .insert(account_id, stake);
    }

    pub fn iter_delegators(
        &self,
        entity_id: &EntityKey,
    ) -> impl Iterator<Item = (&AccountId, &u64)> {
        self.0.get(entity_id).into_iter().flatten()
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

pub struct BoundaryWork {
    // loaded
    pub active_protocol: EraProtocol,
    pub active_era: EraSummary,
    pub active_state: Option<EpochState>,
    pub active_snapshot: Snapshot,
    pub waiting_state: Option<EpochState>,
    pub ending_state: EpochState,
    pub ending_snapshot: Snapshot,
    pub shelley_hash: Hash<32>,

    // deprecated in favor of deltas
    // pub dropped_pool_delegators: HashSet<AccountId>,
    pub deltas: WorkDeltas<CardanoLogic>,

    // computed
    pub pot_delta: Option<PotDelta>,
    pub starting_state: Option<EpochState>,
    pub era_transition: Option<EraTransition>,
}

impl BoundaryWork {
    pub fn starting_epoch_no(&self) -> u64 {
        self.ending_state.number as u64 + 1
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }
}

#[instrument(skip_all, fields(slot = %slot))]
pub fn sweep<D: Domain>(domain: &D, slot: BlockSlot, config: &Config) -> Result<(), ChainError> {
    info!(slot, "executing sweep");

    let mut boundary = BoundaryWork::load(domain)?;

    boundary.compute(domain)?;

    boundary.commit(domain)?;

    if let Some(stop_epoch) = config.stop_epoch {
        if boundary.ending_state.number >= stop_epoch {
            return Err(ChainError::StopEpochReached);
        }
    }

    Ok(())
}
