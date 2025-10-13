use std::collections::{HashMap, HashSet};

use dolos_core::{batch::WorkDeltas, BlockSlot, ChainError, Domain, EntityKey, Genesis};
use pallas::{crypto::hash::Hash, ledger::primitives::conway::DRep};
use tracing::{info, instrument};

use crate::{
    pots::{PotDelta, Pots},
    rewards::RewardMap,
    rupd::RupdWork,
    AccountState, CardanoDelta, CardanoEntity, CardanoLogic, Config, DRepState, EpochState,
    EraProtocol, EraSummary, Nonces, PParamsSet, PoolHash, PoolState, Proposal,
};

pub mod commit;
pub mod loading;
// visitors
pub mod retires;
pub mod rewards;
pub mod transition;

mod hacks;

// Epoch nomenclature
// - ending: the epoch that is currently ending in this boundary.
// - starting: the epoch that is currently starting in this boundary.
// - waiting: (ending - 1) the epoch that will become _active_ after the
//   boundary.
// - active: (ending - 2) the epoch considered active for stake distribution &
//   pool parameters

pub trait BoundaryVisitor {
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
    fn visit_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &Proposal,
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
pub type ProposalId = EntityKey;

#[derive(Debug, Default, Clone)]
pub struct DelegatorMap(HashMap<EntityKey, HashSet<AccountId>>);

impl DelegatorMap {
    pub fn insert(&mut self, entity_id: EntityKey, account_id: AccountId) {
        self.0.entry(entity_id).or_default().insert(account_id);
    }

    pub fn iter_delegators(&self, pool_id: &PoolId) -> impl Iterator<Item = &AccountId> {
        self.0.get(pool_id).into_iter().flatten()
    }

    pub fn count_delegators(&self, pool_id: &PoolId) -> u64 {
        self.0.get(pool_id).map(|x| x.len() as u64).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct Snapshot {
    pub total_stake_sum: u64,
    pub active_stake_sum: u64,
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
pub struct EraTransition {
    pub prev_version: EraProtocol,
    pub new_version: EraProtocol,
}

pub struct BoundaryWork {
    // loaded
    pub waiting_state: Option<EpochState>,
    pub ending_state: EpochState,
    pub network_magic: Option<u32>,
    pub shelley_hash: Hash<32>,
    pub active_protocol: EraProtocol,
    pub active_era: EraSummary,
    pub rewards: RewardMap<RupdWork>,

    // inferred
    pub retiring_pools: HashSet<PoolHash>,
    pub expiring_dreps: HashSet<DRep>,
    pub era_transition: Option<EraTransition>,
    pub next_pparams: Option<PParamsSet>,
    pub next_nonces: Option<Nonces>,
    pub next_pots: Option<Pots>,
    pub next_largest_stable_slot: Option<BlockSlot>,

    // computed via visitors
    pub deltas: WorkDeltas<CardanoLogic>,
    pub logs: Vec<(EntityKey, CardanoEntity)>,
}

impl BoundaryWork {
    pub fn starting_epoch_no(&self) -> u64 {
        self.ending_state.number + 1
    }

    pub fn add_delta(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.add_for_entity(delta);
    }
}

#[instrument("epoch", skip_all, fields(slot = %slot))]
pub fn execute<D: Domain>(
    state: &D::State,
    archive: &D::Archive,
    slot: BlockSlot,
    config: &Config,
    genesis: &Genesis,
    rewards: RewardMap<RupdWork>,
) -> Result<(), ChainError> {
    info!("executing epoch work unit");

    let mut boundary = BoundaryWork::load::<D>(state, genesis, rewards)?;

    // If we're going to stop, we need to do it before applying any changes.
    //
    // This is due to the fact that the WAL only tracks blocks, if we were to apply
    // the changes, WAL will think it's still before the epoch boundary and
    // re-apply everything in the next pass.
    if let Some(stop_epoch) = config.stop_epoch {
        if boundary.ending_state.number >= stop_epoch {
            return Err(ChainError::StopEpochReached);
        }
    }

    boundary.commit::<D>(state, archive)?;

    info!("epoch work unit committed");

    Ok(())
}
