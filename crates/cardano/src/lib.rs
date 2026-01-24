use pallas::ledger::{
    primitives::Epoch,
    traverse::{MultiEraBlock, MultiEraOutput},
};
use std::sync::Arc;
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    config::CardanoConfig, Block as _, BlockSlot, ChainError, ChainPoint, Domain, DomainError,
    EntityKey, EraCbor, Genesis, MempoolAwareUtxoStore, MempoolTx, RawBlock, StateStore, WorkUnit,
};

use crate::{
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
    rewards::RewardMap,
    roll::{WorkBatch, WorkBlock},
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

/// Internal work unit variants used by the WorkBuffer state machine.
///
/// These are Cardano-specific work types that get converted to boxed
/// `WorkUnit` implementations when `pop_work` is called.
enum InternalWorkUnit {
    Genesis,
    Blocks(WorkBatch),
    EWrap(BlockSlot),
    EStart(BlockSlot),
    Rupd(BlockSlot),
    ForcedStop,
}

enum WorkBuffer {
    Empty,
    Restart(ChainPoint),
    Genesis(OwnedMultiEraBlock),
    OpenBatch(WorkBatch),
    PreRupdBoundary(WorkBatch, OwnedMultiEraBlock),
    RupdBoundary(OwnedMultiEraBlock),
    PreEwrapBoundary(WorkBatch, OwnedMultiEraBlock, Epoch),
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

    fn pop_work(self, stop_epoch: Option<Epoch>) -> (Option<InternalWorkUnit>, Self) {
        if matches!(self, WorkBuffer::Restart(..)) || matches!(self, WorkBuffer::Empty) {
            return (None, self);
        }

        match self {
            WorkBuffer::Genesis(block) => (
                Some(InternalWorkUnit::Genesis),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::OpenBatch(batch) => {
                let last_point = batch.last_point();
                (
                    Some(InternalWorkUnit::Blocks(batch)),
                    Self::Restart(last_point),
                )
            }
            WorkBuffer::PreRupdBoundary(batch, block) => (
                Some(InternalWorkUnit::Blocks(batch)),
                Self::RupdBoundary(block),
            ),
            WorkBuffer::RupdBoundary(block) => (
                Some(InternalWorkUnit::Rupd(block.slot())),
                Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block))),
            ),
            WorkBuffer::PreEwrapBoundary(batch, block, epoch) => (
                Some(InternalWorkUnit::Blocks(batch)),
                Self::EwrapBoundary(block, epoch),
            ),
            WorkBuffer::EwrapBoundary(block, epoch) => (
                Some(InternalWorkUnit::EWrap(block.slot())),
                Self::EstartBoundary(block, epoch + 1),
            ),
            WorkBuffer::EstartBoundary(block, epoch) => (
                Some(InternalWorkUnit::EStart(block.slot())),
                if stop_epoch.is_some_and(|x| x == epoch) {
                    Self::PreForcedStop(block)
                } else {
                    Self::OpenBatch(WorkBatch::for_single_block(WorkBlock::new(block)))
                },
            ),
            WorkBuffer::PreForcedStop(block) => (
                Some(InternalWorkUnit::Blocks(WorkBatch::for_single_block(
                    WorkBlock::new(block),
                ))),
                Self::ForcedStop,
            ),
            WorkBuffer::ForcedStop => (Some(InternalWorkUnit::ForcedStop), Self::ForcedStop),
            _ => unreachable!(),
        }
    }
}

pub(crate) struct Cache {
    pub eras: ChainSummary,
    pub stability_window: u64,
    pub rewards: Option<RewardMap<RupdWork>>,
}

pub struct CardanoLogic {
    config: CardanoConfig,
    work: Option<WorkBuffer>,
    pub(crate) cache: Cache,
}

impl CardanoLogic {
    /// Refresh the cached era summary from state.
    /// Called after work units that may change era information (like genesis).
    pub fn refresh_cache<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
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

    fn pop_work<D: Domain>(&mut self, domain: &D) -> Option<Box<dyn WorkUnit<D>>>
    where
        D: Domain<Chain = Self, Entity = CardanoEntity, EntityDelta = CardanoDelta>,
    {
        let work = self.work.take().expect("work buffer is initialized");

        let (work_unit, new_buffer) = work.pop_work(self.config.stop_epoch);

        self.work = Some(new_buffer);

        let work_unit = work_unit?;

        // Convert internal work unit to boxed WorkUnit trait object
        match work_unit {
            InternalWorkUnit::Genesis => Some(Box::new(genesis::GenesisWorkUnit::new(
                self.config.clone(),
                domain.genesis(),
            ))),
            InternalWorkUnit::Blocks(mut batch) => {
                // Load and decode UTxOs before computing deltas
                // This is done here because it needs access to domain and chain
                if let Err(e) = batch.load_utxos(domain) {
                    tracing::error!(error = %e, "failed to load UTxOs for roll batch");
                    return None;
                }

                if let Err(e) = batch.decode_utxos(self) {
                    tracing::error!(error = %e, "failed to decode UTxOs for roll batch");
                    return None;
                }

                // Compute deltas using the visitor pattern
                if let Err(e) = roll::compute_delta::<D>(
                    &self.config,
                    domain.genesis(),
                    &self.cache,
                    domain.state(),
                    &mut batch,
                ) {
                    tracing::error!(error = %e, "failed to compute roll deltas");
                    return None;
                }

                Some(Box::new(roll::RollWorkUnit::new(
                    batch,
                    domain.genesis(),
                    true, // live mode
                )))
            }
            InternalWorkUnit::Rupd(slot) => {
                Some(Box::new(rupd::RupdWorkUnit::new(slot, domain.genesis())))
            }
            InternalWorkUnit::EWrap(slot) => {
                // Take rewards from cache (set by rupd)
                let rewards = self.cache.rewards.take().unwrap_or_default();
                Some(Box::new(ewrap::EwrapWorkUnit::new(
                    slot,
                    self.config.clone(),
                    domain.genesis(),
                    rewards,
                )))
            }
            InternalWorkUnit::EStart(slot) => Some(Box::new(estart::EstartWorkUnit::new(
                slot,
                self.config.clone(),
                domain.genesis(),
            ))),
            InternalWorkUnit::ForcedStop => {
                // Return a special marker - the executor will handle this
                Some(Box::new(ForcedStopWorkUnit))
            }
        }
    }

    fn decode_utxo(&self, utxo: Arc<EraCbor>) -> Result<Self::Utxo, ChainError> {
        let out = OwnedMultiEraOutput::decode(utxo)?;

        Ok(out)
    }

    fn mutable_slots(domain: &impl Domain) -> BlockSlot {
        utils::mutable_slots(&domain.genesis())
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

/// A special work unit that signals a forced stop.
struct ForcedStopWorkUnit;

impl<D: Domain> WorkUnit<D> for ForcedStopWorkUnit {
    fn name(&self) -> &'static str {
        "forced_stop"
    }

    fn load(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        Ok(())
    }

    fn commit_state(&mut self, _domain: &D) -> Result<(), DomainError> {
        Err(DomainError::StopEpochReached)
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
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
