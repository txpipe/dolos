use pallas::ledger::{
    primitives::Epoch,
    traverse::{MultiEraBlock, MultiEraOutput},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    batch::{WorkBatch, WorkBlock},
    Block as _, *,
};

use crate::{
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
    rewards::RewardMap,
    rupd::RupdWork,
};

// staging zone
pub mod math_macros;
pub mod pallas_extras;

// machinery
pub mod eras;
pub mod forks;
pub mod model;
pub mod owned;
pub mod pots;
pub mod rewards;
pub mod utils;
pub mod utxoset;

// work units
pub mod estart;
pub mod ewrap;
pub mod genesis;
pub mod roll;
pub mod rupd;

#[cfg(feature = "include-genesis")]
pub mod include;

pub use eras::*;
pub use model::*;
pub use utils::mutable_slots;

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

#[derive(Serialize, Deserialize, Clone)]
pub struct TrackConfig {
    pub account_state: bool,
    pub asset_state: bool,
    pub pool_state: bool,
    pub epoch_state: bool,
    pub drep_state: bool,
    pub proposal_logs: bool,
    pub tx_logs: bool,
    pub account_logs: bool,
    pub pool_logs: bool,
    pub epoch_logs: bool,
}

impl Default for TrackConfig {
    fn default() -> Self {
        Self {
            account_state: true,
            asset_state: true,
            pool_state: true,
            epoch_state: true,
            drep_state: true,
            tx_logs: true,
            account_logs: true,
            pool_logs: true,
            epoch_logs: true,
            proposal_logs: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub track: TrackConfig,
    pub stop_epoch: Option<Epoch>,
}

struct WorkBuffer {
    pending: VecDeque<WorkUnit<CardanoLogic>>,
    last_point_seen: ChainPoint,
}

impl WorkBuffer {
    fn new_from_cursor(cursor: ChainPoint) -> Self {
        Self {
            pending: Default::default(),
            last_point_seen: cursor,
        }
    }

    fn new_from_genesis() -> Self {
        Self {
            pending: VecDeque::from(vec![WorkUnit::Genesis]),
            last_point_seen: ChainPoint::Origin,
        }
    }

    fn enqueue_block(&mut self, block: WorkBlock<CardanoLogic>) {
        self.last_point_seen = ChainPoint::Slot(block.slot());
        self.pending.push_back(WorkUnit::Block(block));
    }

    fn enqueue_ewrap(&mut self, slot: BlockSlot) {
        self.last_point_seen = ChainPoint::Slot(slot);
        self.pending.push_back(WorkUnit::EWrap(slot));
    }

    fn enqueue_estart(&mut self, slot: BlockSlot) {
        self.last_point_seen = ChainPoint::Slot(slot);
        self.pending.push_back(WorkUnit::EStart(slot));
    }

    fn enqueue_rupd(&mut self, slot: BlockSlot) {
        self.last_point_seen = ChainPoint::Slot(slot);
        self.pending.push_back(WorkUnit::Rupd(slot));
    }
}

struct Cache {
    eras: ChainSummary,
    stability_window: u64,
    rewards: Option<RewardMap<RupdWork>>,
}

#[derive(Clone)]
pub struct CardanoLogic {
    config: Config,
    work: Arc<RwLock<WorkBuffer>>,
    cache: Arc<RwLock<Cache>>,
}

impl CardanoLogic {
    fn refresh_cache<D: Domain>(&self, state: &D::State) -> Result<(), ChainError> {
        let mut cache = self.cache.write().unwrap();

        cache.eras = eras::load_era_summary::<D>(state)?;

        Ok(())
    }
}

impl dolos_core::ChainLogic for CardanoLogic {
    type Config = Config;
    type Block = OwnedMultiEraBlock;
    type Utxo = OwnedMultiEraOutput;
    type Delta = CardanoDelta;
    type Entity = CardanoEntity;

    fn initialize<D: Domain>(
        config: Self::Config,
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<Self, ChainError> {
        let cursor = state.read_cursor()?;

        let work = match cursor {
            Some(cursor) => WorkBuffer::new_from_cursor(cursor),
            None => WorkBuffer::new_from_genesis(),
        };

        let eras = eras::load_era_summary::<D>(state)?;

        let stability_window = utils::stability_window(genesis);

        Ok(Self {
            config,
            cache: Arc::new(RwLock::new(Cache {
                eras,
                stability_window,
                rewards: None,
            })),
            work: Arc::new(RwLock::new(work)),
        })
    }

    fn receive_block(&self, raw: RawBlock) -> Result<(), ChainError> {
        let block = OwnedMultiEraBlock::decode(raw)?;

        let mut work = self.work.write().unwrap();

        let prev_slot = work.last_point_seen.slot();

        let next_slot = block.slot();

        let cache = self.cache.read().unwrap();

        let epoch_boundary = pallas_extras::epoch_boundary(&cache.eras, prev_slot, next_slot);

        if let Some(slot) = epoch_boundary {
            work.enqueue_ewrap(slot);
            work.enqueue_estart(slot);
        }

        let rupd_boundary =
            pallas_extras::rupd_boundary(cache.stability_window, &cache.eras, prev_slot, next_slot);

        if let Some(slot) = rupd_boundary {
            work.enqueue_rupd(slot);
        }

        let block = WorkBlock::new(block);

        work.enqueue_block(block);

        Ok(())
    }

    fn peek_work(&self) -> Option<WorkKind> {
        let work = self.work.read().unwrap();

        work.pending.front().map(|work| match work {
            WorkUnit::Block(_) => WorkKind::Block,
            WorkUnit::EWrap(_) => WorkKind::EWrap,
            WorkUnit::EStart(_) => WorkKind::EStart,
            WorkUnit::Rupd(_) => WorkKind::Rupd,
            WorkUnit::Genesis => WorkKind::Genesis,
        })
    }

    fn pop_work(&self) -> Option<WorkUnit<Self>> {
        let mut work = self.work.write().unwrap();
        work.pending.pop_front()
    }

    fn apply_genesis<D: Domain>(
        &self,
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<(), ChainError> {
        genesis::execute::<D>(state, &genesis)?;

        self.refresh_cache::<D>(state)?;

        Ok(())
    }

    fn apply_ewrap<D: Domain>(
        &self,
        state: &D::State,
        archive: &D::Archive,
        genesis: Arc<Genesis>,
        at: BlockSlot,
    ) -> Result<(), ChainError> {
        let mut cache = self.cache.write().unwrap();

        let rewards = cache.rewards.take().ok_or(ChainError::MissingRewards)?;

        ewrap::execute::<D>(state, archive, at, &self.config, genesis, rewards)?;

        drop(cache);

        self.refresh_cache::<D>(state)?;

        Ok(())
    }

    fn apply_estart<D: Domain>(
        &self,
        state: &D::State,
        archive: &D::Archive,
        genesis: Arc<Genesis>,
        at: BlockSlot,
    ) -> Result<(), ChainError> {
        estart::execute::<D>(state, archive, at, genesis)?;

        self.refresh_cache::<D>(state)?;

        Ok(())
    }

    fn apply_rupd<D: Domain>(
        &self,
        state: &D::State,
        archive: &D::Archive,
        genesis: Arc<Genesis>,
        at: BlockSlot,
    ) -> Result<(), ChainError> {
        let rewards = rupd::execute::<D>(state, archive, at, &genesis)?;

        let mut cache = self.cache.write().unwrap();

        cache.rewards = Some(rewards);

        Ok(())
    }

    fn decode_utxo(&self, utxo: Arc<EraCbor>) -> Result<Self::Utxo, ChainError> {
        let out = OwnedMultiEraOutput::decode(utxo)?;

        Ok(out)
    }

    fn mutable_slots(domain: &impl Domain) -> BlockSlot {
        utils::mutable_slots(&domain.genesis())
    }

    fn compute_delta<D: Domain>(
        &self,
        state: &D::State,
        batch: &mut WorkBatch<Self>,
    ) -> Result<(), ChainError> {
        let cache = self.cache.read().unwrap();

        roll::compute_delta::<D>(&self.config, &cache, state, batch)?;

        Ok(())
    }
}

pub fn load_effective_pparams<D: Domain>(state: &D::State) -> Result<PParamsSet, ChainError> {
    let epoch = load_epoch::<D>(state)?;
    let active = epoch.pparams.active();

    Ok(active.clone())
}

pub fn load_epoch<D: Domain>(state: &D::State) -> Result<EpochState, ChainError> {
    let epoch = state
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(CURRENT_EPOCH_KEY))?
        .ok_or(ChainError::NoActiveEpoch)?;

    Ok(epoch)
}

#[cfg(test)]
pub fn load_test_genesis(env: &str) -> Genesis {
    use std::path::PathBuf;

    let test_data = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("test_data")
        .join(env);

    Genesis::from_file_paths(
        test_data.join("genesis/byron.json"),
        test_data.join("genesis/shelley.json"),
        test_data.join("genesis/alonzo.json"),
        test_data.join("genesis/conway.json"),
        None,
    )
    .unwrap()
}
