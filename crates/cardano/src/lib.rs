use pallas::ledger::{
    primitives::Epoch,
    traverse::{MultiEraBlock, MultiEraOutput},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

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
pub mod hacks;
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

enum WorkBuffer {
    Empty,
    Restart(ChainPoint),
    Genesis(OwnedMultiEraBlock),
    OpenBatch(WorkBatch<CardanoLogic>),
    PreRupdBoundary(WorkBatch<CardanoLogic>, OwnedMultiEraBlock),
    RupdBoundary(OwnedMultiEraBlock),
    PreEwrapBoundary(WorkBatch<CardanoLogic>, OwnedMultiEraBlock),
    EwrapBoundary(OwnedMultiEraBlock),
    EstartBoundary(OwnedMultiEraBlock),
}

impl Default for WorkBuffer {
    fn default() -> Self {
        Self::Empty
    }
}

impl WorkBuffer {
    fn new_from_cursor(cursor: ChainPoint) -> Self {
        Self::Restart(cursor)
    }

    fn peek_work(&self) -> Option<WorkKind> {
        match self {
            WorkBuffer::Empty => None,
            WorkBuffer::Restart(..) => None,
            WorkBuffer::Genesis(..) => Some(WorkKind::Genesis),
            WorkBuffer::OpenBatch(..) => Some(WorkKind::Blocks),
            WorkBuffer::PreRupdBoundary(..) => Some(WorkKind::Blocks),
            WorkBuffer::RupdBoundary(..) => Some(WorkKind::Rupd),
            WorkBuffer::PreEwrapBoundary(..) => Some(WorkKind::Blocks),
            WorkBuffer::EwrapBoundary(..) => Some(WorkKind::EWrap),
            WorkBuffer::EstartBoundary(..) => Some(WorkKind::EStart),
        }
    }

    fn last_point_seen(&self) -> ChainPoint {
        match self {
            WorkBuffer::Empty => ChainPoint::Origin,
            WorkBuffer::Restart(x) => x.clone(),
            WorkBuffer::Genesis(block) => block.point(),
            WorkBuffer::OpenBatch(batch) => batch.last_point(),
            WorkBuffer::PreRupdBoundary(.., block) => block.point(),
            WorkBuffer::RupdBoundary(block) => block.point(),
            WorkBuffer::PreEwrapBoundary(.., block) => block.point(),
            WorkBuffer::EwrapBoundary(block) => block.point(),
            WorkBuffer::EstartBoundary(block) => block.point(),
        }
    }

    fn can_receive_block(&self) -> bool {
        match self {
            WorkBuffer::Empty => true,
            WorkBuffer::Restart(..) => true,
            WorkBuffer::OpenBatch(..) => true,
            _ => false,
        }
    }

    fn extend_batch(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Empty => {
                let batch = WorkBatch::for_single_block(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            WorkBuffer::Restart(_) => {
                let batch = WorkBatch::for_single_block(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            WorkBuffer::OpenBatch(mut batch) => {
                batch.add_work(WorkBlock::new(next_block));
                WorkBuffer::OpenBatch(batch)
            }
            _ => unreachable!(),
        }
    }

    fn on_genesis_boundary(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Empty => WorkBuffer::Genesis(next_block),
            _ => unreachable!(),
        }
    }

    fn on_rupd_boundary(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Restart(_) => WorkBuffer::RupdBoundary(next_block),
            WorkBuffer::OpenBatch(batch) => WorkBuffer::PreRupdBoundary(batch, next_block),
            _ => unreachable!(),
        }
    }

    fn on_ewrap_boundary(self, next_block: OwnedMultiEraBlock) -> Self {
        match self {
            WorkBuffer::Restart(..) => WorkBuffer::EwrapBoundary(next_block),
            WorkBuffer::OpenBatch(batch) => WorkBuffer::PreEwrapBoundary(batch, next_block),
            _ => unreachable!(),
        }
    }

    fn receive_block(
        self,
        block: OwnedMultiEraBlock,
        eras: &ChainSummary,
        stability_window: u64,
    ) -> Self {
        assert!(
            self.can_receive_block(),
            "can't continue until previous work is completed"
        );

        if matches!(self, WorkBuffer::Empty) {
            return self.on_genesis_boundary(block);
        }

        let prev_slot = self.last_point_seen().slot();

        let next_slot = block.slot();

        let epoch_boundary = pallas_extras::epoch_boundary(eras, prev_slot, next_slot);

        if epoch_boundary.is_some() {
            return self.on_ewrap_boundary(block);
        }

        let rupd_boundary =
            pallas_extras::rupd_boundary(stability_window, eras, prev_slot, next_slot);

        if rupd_boundary.is_some() {
            return self.on_rupd_boundary(block);
        }

        self.extend_batch(block)
    }

    fn pop_work(self) -> Option<(WorkUnit<CardanoLogic>, Self)> {
        if matches!(self, WorkBuffer::Restart(..)) || matches!(self, WorkBuffer::Empty) {
            return None;
        }

        let next = match self {
            WorkBuffer::Genesis(block) => (
                WorkUnit::Genesis,
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::OpenBatch(batch) => {
                let last_point = batch.last_point();
                (WorkUnit::Blocks(batch), Self::Restart(last_point))
            }
            WorkBuffer::PreRupdBoundary(batch, block) => {
                (WorkUnit::Blocks(batch), Self::RupdBoundary(block))
            }
            WorkBuffer::RupdBoundary(block) => (
                WorkUnit::Rupd(block.slot()),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::PreEwrapBoundary(batch, block) => {
                (WorkUnit::Blocks(batch), Self::EwrapBoundary(block))
            }
            WorkBuffer::EwrapBoundary(block) => {
                (WorkUnit::EWrap(block.slot()), Self::EstartBoundary(block))
            }
            WorkBuffer::EstartBoundary(block) => (
                WorkUnit::EStart(block.slot()),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            _ => unreachable!(),
        };

        Some(next)
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
    work: Arc<RwLock<Box<WorkBuffer>>>,
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
            None => WorkBuffer::Empty,
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
            work: Arc::new(RwLock::new(Box::new(work))),
        })
    }

    fn can_receive_block(&self) -> bool {
        let work = self.work.read().unwrap();
        work.can_receive_block()
    }

    fn receive_block(&self, raw: RawBlock) -> Result<BlockSlot, ChainError> {
        let mut work = self.work.write().unwrap();

        if !work.can_receive_block() {
            return Err(ChainError::CantReceiveBlock(raw));
        }

        let block = OwnedMultiEraBlock::decode(raw)?;

        let cache = self.cache.read().unwrap();

        // Extract the WorkBuffer from the Box, apply transformation, and put it back
        let new_work =
            std::mem::take(&mut **work).receive_block(block, &cache.eras, cache.stability_window);

        *work = Box::new(new_work);

        Ok(work.last_point_seen().slot())
    }

    fn peek_work(&self) -> Option<WorkKind> {
        let work = self.work.read().unwrap();
        work.peek_work()
    }

    fn pop_work(&self) -> Option<WorkUnit<Self>> {
        let mut work = self.work.write().unwrap();

        let Some((work_unit, new_buffer)) = std::mem::take(&mut **work).pop_work() else {
            return None;
        };

        *work = Box::new(new_buffer);
        Some(work_unit)
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

        let rewards = cache.rewards.take().unwrap_or_default();

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
        estart::execute::<D>(state, archive, at, &self.config, genesis)?;

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
    let active = epoch.pparams.unwrap_live();

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
