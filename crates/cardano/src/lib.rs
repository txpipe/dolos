use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::sync::Arc;
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    config::CardanoConfig, Block as _, BlockSlot, ChainError, ChainPoint, Domain, DomainError,
    EntityKey, EraCbor, Genesis, MempoolAwareUtxoStore, MempoolTx, MempoolUpdate, RawBlock,
    StateStore, TipEvent, WorkUnit,
};

use crate::{
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
    work::{InternalWorkUnit, WorkBuffer},
};

// staging zone
pub mod math_macros;
pub mod pallas_extras;

// machinery
pub mod cip25;
pub mod cip68;
pub mod eras;
pub mod forks;
pub mod hacks;
pub mod indexes;
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
mod work;

pub mod validate;

#[cfg(feature = "include-genesis")]
pub mod include;

pub use eras::*;
pub use model::*;
pub use utils::{mutable_slots, network_from_genesis};

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

/// Cardano-specific work unit variants.
///
/// This enum represents all possible work units that can be produced
/// by the Cardano chain logic. Each variant wraps a concrete work unit
/// implementation.
pub enum CardanoWorkUnit {
    /// Bootstrap chain from genesis configuration.
    Genesis(Box<genesis::GenesisWorkUnit>),
    /// Process a batch of blocks (roll forward).
    Roll(Box<roll::RollWorkUnit>),
    /// Compute rewards at stability window boundary.
    Rupd(Box<rupd::RupdWorkUnit>),
    /// Handle epoch boundary wrap-up processing.
    Ewrap(Box<ewrap::EwrapWorkUnit>),
    /// Handle epoch start processing.
    Estart(Box<estart::EstartWorkUnit>),
    /// Signal forced stop at configured epoch.
    ForcedStop,
}

impl<D> WorkUnit<D> for CardanoWorkUnit
where
    D: Domain<Chain = CardanoLogic, Entity = CardanoEntity, EntityDelta = CardanoDelta>,
{
    fn name(&self) -> &'static str {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::name(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::name(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::name(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::name(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::name(w),
            Self::ForcedStop => "forced_stop",
        }
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::compute(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::compute(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::compute(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::compute(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::compute(w),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::ForcedStop => Err(DomainError::StopEpochReached),
        }
    }

    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_archive(w, domain)
            }
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_archive(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_archive(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_archive(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_archive(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_indexes(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_indexes(w, domain)
            }
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_indexes(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_indexes(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_indexes(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_indexes(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn tip_events(&self) -> Vec<TipEvent> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::ForcedStop => Vec::new(),
        }
    }

    fn mempool_updates(&self) -> Vec<MempoolUpdate> {
        match self {
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::mempool_updates(w),
            _ => Vec::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Cache {
    pub eras: ChainSummary,
    pub stability_window: u64,
}

pub struct CardanoLogic {
    config: CardanoConfig,
    work: Option<WorkBuffer>,
    pub(crate) cache: Cache,
    /// Flag indicating the cache needs refresh after a work unit that modifies eras.
    /// Set after Genesis or EStart work units are popped, cleared at next pop_work call.
    needs_cache_refresh: bool,
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
    type WorkUnit<D: Domain<Chain = Self, Entity = Self::Entity, EntityDelta = Self::Delta>> =
        CardanoWorkUnit;

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

        // Use randomness_stability_window (4k/f) for the RUPD trigger boundary.
        // The Haskell ledger's startStep fires at randomnessStabilisationWindow
        // into the epoch, capturing addrsRew (registered accounts) for the pre-Babbage
        // prefilter. Using 4k/f instead of 3k/f ensures the state at RUPD time includes
        // all deregistrations up to the correct threshold.
        let stability_window = utils::randomness_stability_window(genesis);

        Ok(Self {
            config,
            cache: Cache {
                eras,
                stability_window,
            },
            work: Some(work),
            needs_cache_refresh: false,
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

    fn pop_work<D>(&mut self, domain: &D) -> Option<CardanoWorkUnit>
    where
        D: Domain<Chain = Self, Entity = CardanoEntity, EntityDelta = CardanoDelta>,
    {
        // Refresh cache if needed (after previous genesis or estart execution)
        if self.needs_cache_refresh {
            if let Err(e) = self.refresh_cache::<D>(domain.state()) {
                tracing::error!(error = %e, "failed to refresh cache after era-modifying work unit");
            }
            self.needs_cache_refresh = false;
        }

        let work = self.work.take().expect("work buffer is initialized");

        let (work_unit, new_buffer) = work.pop_work(self.config.stop_epoch);

        self.work = Some(new_buffer);

        let work_unit = work_unit?;

        // Convert internal work unit marker to concrete CardanoWorkUnit
        match work_unit {
            InternalWorkUnit::Genesis => {
                // Genesis modifies era summaries, schedule cache refresh
                self.needs_cache_refresh = true;
                Some(CardanoWorkUnit::Genesis(Box::new(
                    genesis::GenesisWorkUnit::new(self.config.clone(), domain.genesis()),
                )))
            }
            InternalWorkUnit::Blocks(batch) => {
                Some(CardanoWorkUnit::Roll(Box::new(roll::RollWorkUnit::new(
                    batch,
                    domain.genesis(),
                    true, // live mode
                    self.config.clone(),
                    self.cache.clone(),
                ))))
            }
            InternalWorkUnit::Rupd(slot) => Some(CardanoWorkUnit::Rupd(Box::new(
                rupd::RupdWorkUnit::new(slot, domain.genesis()),
            ))),
            InternalWorkUnit::EWrap(slot) => {
                // Rewards are loaded from state store during EWRAP load phase
                Some(CardanoWorkUnit::Ewrap(Box::new(ewrap::EwrapWorkUnit::new(
                    slot,
                    self.config.clone(),
                    domain.genesis(),
                ))))
            }
            InternalWorkUnit::EStart(slot) => {
                // EStart may trigger era transitions, schedule cache refresh
                self.needs_cache_refresh = true;
                Some(CardanoWorkUnit::Estart(Box::new(
                    estart::EstartWorkUnit::new(slot, self.config.clone(), domain.genesis()),
                )))
            }
            InternalWorkUnit::ForcedStop => Some(CardanoWorkUnit::ForcedStop),
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
