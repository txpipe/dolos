use pallas::ledger::{
    primitives::Epoch,
    traverse::{MultiEraBlock, MultiEraOutput},
};
use std::sync::Arc;
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    batch::{WorkBatch, WorkBlock},
    config::CardanoConfig,
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

pub mod validate;

#[cfg(feature = "include-genesis")]
pub mod include;

pub use eras::*;
pub use model::*;
pub use utils::mutable_slots;

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

enum WorkBuffer {
    Empty,
    Restart(ChainPoint),
    Genesis(OwnedMultiEraBlock),
    OpenBatch(WorkBatch<CardanoLogic>),
    PreRupdBoundary(WorkBatch<CardanoLogic>, OwnedMultiEraBlock),
    RupdBoundary(OwnedMultiEraBlock),
    PreEwrapBoundary(WorkBatch<CardanoLogic>, OwnedMultiEraBlock, Epoch),
    EwrapBoundary(OwnedMultiEraBlock, Epoch),
    EstartBoundary(OwnedMultiEraBlock, Epoch),
    PreForcedStop(OwnedMultiEraBlock),
    ForcedStop,
}

impl WorkBuffer {
    fn new_from_cursor(cursor: ChainPoint) -> Self {
        Self::Restart(cursor)
    }

    fn last_point_seen(&self) -> ChainPoint {
        match self {
            WorkBuffer::Empty => ChainPoint::Origin,
            WorkBuffer::Restart(x) => x.clone(),
            WorkBuffer::Genesis(block) => block.point(),
            WorkBuffer::OpenBatch(batch) => batch.last_point(),
            WorkBuffer::PreRupdBoundary(_, block) => block.point(),
            WorkBuffer::RupdBoundary(block) => block.point(),
            WorkBuffer::PreEwrapBoundary(_, block, _) => block.point(),
            WorkBuffer::EwrapBoundary(block, _) => block.point(),
            WorkBuffer::EstartBoundary(block, _) => block.point(),
            WorkBuffer::PreForcedStop(block) => block.point(),
            WorkBuffer::ForcedStop => unreachable!(),
        }
    }

    #[allow(clippy::match_like_matches_macro)]
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

    fn on_ewrap_boundary(self, next_block: OwnedMultiEraBlock, epoch: Epoch) -> Self {
        match self {
            WorkBuffer::Restart(..) => WorkBuffer::EwrapBoundary(next_block, epoch),
            WorkBuffer::OpenBatch(batch) => WorkBuffer::PreEwrapBoundary(batch, next_block, epoch),
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

        let boundary = pallas_extras::epoch_boundary(eras, prev_slot, next_slot);

        if let Some((epoch, _, _)) = boundary {
            return self.on_ewrap_boundary(block, epoch);
        }

        let rupd_boundary =
            pallas_extras::rupd_boundary(stability_window, eras, prev_slot, next_slot);

        if rupd_boundary.is_some() {
            return self.on_rupd_boundary(block);
        }

        self.extend_batch(block)
    }

    fn pop_work(self, stop_epoch: Option<Epoch>) -> (Option<WorkUnit<CardanoLogic>>, Self) {
        if matches!(self, WorkBuffer::Restart(..)) || matches!(self, WorkBuffer::Empty) {
            return (None, self);
        }

        match self {
            WorkBuffer::Genesis(block) => (
                Some(WorkUnit::Genesis),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::OpenBatch(batch) => {
                let last_point = batch.last_point();
                (Some(WorkUnit::Blocks(batch)), Self::Restart(last_point))
            }
            WorkBuffer::PreRupdBoundary(batch, block) => {
                (Some(WorkUnit::Blocks(batch)), Self::RupdBoundary(block))
            }
            WorkBuffer::RupdBoundary(block) => (
                Some(WorkUnit::Rupd(block.slot())),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::PreEwrapBoundary(batch, block, epoch) => (
                Some(WorkUnit::Blocks(batch)),
                Self::EwrapBoundary(block, epoch),
            ),
            WorkBuffer::EwrapBoundary(block, epoch) => (
                Some(WorkUnit::EWrap(block.slot())),
                Self::EstartBoundary(block, epoch + 1),
            ),
            WorkBuffer::EstartBoundary(block, epoch) => (
                Some(WorkUnit::EStart(block.slot())),
                if stop_epoch.is_some_and(|x| x == epoch) {
                    Self::PreForcedStop(block)
                } else {
                    Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block)))
                },
            ),
            WorkBuffer::PreForcedStop(block) => (
                Some(WorkUnit::Blocks(WorkBatch::for_single_block(
                    WorkBlock::new(block),
                ))),
                Self::ForcedStop,
            ),
            WorkBuffer::ForcedStop => (Some(WorkUnit::ForcedStop), Self::ForcedStop),
            _ => unreachable!(),
        }
    }
}

struct Cache {
    eras: ChainSummary,
    stability_window: u64,
    rewards: Option<RewardMap<RupdWork>>,
}

pub struct CardanoLogic {
    config: CardanoConfig,
    work: Option<WorkBuffer>,
    cache: Cache,
}

impl CardanoLogic {
    fn refresh_cache<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        self.cache.eras = eras::load_era_summary::<D>(state)?;

        Ok(())
    }
}

impl dolos_core::ChainLogic for CardanoLogic {
    type Config = CardanoConfig;
    type Block = OwnedMultiEraBlock;
    type Utxo = OwnedMultiEraOutput;
    type Delta = CardanoDelta;
    type Entity = CardanoEntity;

    fn initialize<D: Domain>(
        config: Self::Config,
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<Self, ChainError> {
        info!("initializing");

        let cursor = state.read_cursor()?;

        let work = match cursor {
            Some(cursor) => WorkBuffer::new_from_cursor(cursor),
            None => WorkBuffer::Empty,
        };

        let eras = eras::load_era_summary::<D>(state)?;

        let stability_window = utils::stability_window(genesis);

        Ok(Self {
            config,
            cache: Cache {
                eras,
                stability_window,
                rewards: None,
            },
            work: Some(work),
        })
    }

    fn can_receive_block(&self) -> bool {
        let work = self.work.as_ref().expect("work buffer is initialized");
        work.can_receive_block()
    }

    fn receive_block(&mut self, raw: RawBlock) -> Result<BlockSlot, ChainError> {
        if !self.can_receive_block() {
            return Err(ChainError::CantReceiveBlock(raw));
        }

        let block = OwnedMultiEraBlock::decode(raw)?;

        let work = self.work.take().expect("work buffer is initialized");

        let new_work = work.receive_block(block, &self.cache.eras, self.cache.stability_window);

        let last = new_work.last_point_seen().slot();

        self.work = Some(new_work);

        Ok(last)
    }

    fn pop_work(&mut self) -> Option<WorkUnit<Self>> {
        let work = self.work.take().expect("work buffer is initialized");

        let (work_unit, new_buffer) = work.pop_work(self.config.stop_epoch);

        self.work = Some(new_buffer);

        work_unit
    }

    fn apply_genesis<D: Domain>(
        &mut self,
        state: &D::State,
        genesis: Arc<Genesis>,
    ) -> Result<(), ChainError> {
        genesis::execute::<D>(state, &genesis, &self.config)?;

        self.refresh_cache::<D>(state)?;

        Ok(())
    }

    fn apply_ewrap<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        genesis: Arc<Genesis>,
        at: BlockSlot,
    ) -> Result<(), ChainError> {
        let rewards = self.cache.rewards.take().unwrap_or_default();

        ewrap::execute::<D>(state, archive, at, &self.config, genesis, rewards)?;

        self.refresh_cache::<D>(state)?;

        Ok(())
    }

    fn apply_estart<D: Domain>(
        &mut self,
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
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        genesis: Arc<Genesis>,
        at: BlockSlot,
    ) -> Result<(), ChainError> {
        let rewards = rupd::execute::<D>(state, archive, at, &genesis)?;

        self.cache.rewards = Some(rewards);

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
        genesis: Arc<Genesis>,
        batch: &mut WorkBatch<Self>,
    ) -> Result<(), ChainError> {
        roll::compute_delta::<D>(&self.config, genesis, &self.cache, state, batch)?;

        Ok(())
    }

    fn validate_tx<D: Domain>(
        &self,
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
        tip: Option<ChainPoint>,
        genesis: &Genesis,
    ) -> Result<MempoolTx, ChainError> {
        validate::validate_tx(cbor, utxos, tip, genesis)
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
