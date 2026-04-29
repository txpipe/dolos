use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::sync::Arc;
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    config::CardanoConfig, BlockSlot, ChainError, ChainPoint, Domain, DomainError, EntityKey,
    EraCbor, Genesis, MempoolAwareUtxoStore, MempoolTx, MempoolUpdate, RawBlock, StateStore,
    TipEvent, WorkUnit,
};

use crate::{
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
    work::{InternalWorkUnit, WorkBuffer},
};

// staging zone
pub mod math_macros;
pub mod pallas_extras;

// machinery
pub mod cip151;
pub mod cip25;
pub mod cip68;
pub mod consensus;
pub mod eras;
pub mod forks;
pub mod hacks;
pub mod indexes;
pub mod model;
pub mod owned;
pub mod pots;
pub mod rewards;
pub mod shard;
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
    /// Close the epoch boundary: per-account reward application across
    /// `crate::shard::ACCOUNT_SHARDS` shards (`WorkUnit::total_shards`),
    /// each covering a first-byte prefix range of the account key space
    /// and accumulating its contribution into `EpochState.end` via
    /// `EWrapProgress`. After the shard loop, `finalize()` runs the
    /// global Ewrap pass (pool/drep/proposal classification, MIRs,
    /// enactment, deposit refunds) and emits `EpochWrapUp` with the
    /// assembled final `EndStats`.
    Ewrap(Box<ewrap::EwrapWorkUnit>),
    /// Open the next epoch: per-account snapshot rotation across
    /// `crate::shard::ACCOUNT_SHARDS` shards (`WorkUnit::total_shards`),
    /// advancing `EpochState.estart_progress` via
    /// `EStartProgress`. After the shard loop, `finalize()` runs
    /// the global Estart pass (pool / drep / proposal transitions,
    /// `EpochTransition`, era transition, cursor advance). The cursor
    /// only moves in `finalize()`.
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

    fn total_shards(&self) -> u32 {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::total_shards(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::total_shards(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::total_shards(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::total_shards(w),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::total_shards(w)
            }
            Self::ForcedStop => 1,
        }
    }

    fn start_shard(&self) -> u32 {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::start_shard(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::start_shard(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::start_shard(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::start_shard(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::start_shard(w),
            Self::ForcedStop => 0,
        }
    }

    fn initialize(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::initialize(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::initialize(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::initialize(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::initialize(w, domain),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::initialize(w, domain)
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::load(w, domain, shard_index)
            }
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::load(w, domain, shard_index),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::load(w, domain, shard_index),
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::load(w, domain, shard_index)
            }
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::load(
                w,
                domain,
                shard_index,
            ),
            Self::ForcedStop => Ok(()),
        }
    }

    fn compute(&mut self, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::compute(w, shard_index)
            }
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::compute(w, shard_index)
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_wal(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_wal(w, domain, shard_index)
            }
            Self::Roll(w) => {
                <roll::RollWorkUnit as WorkUnit<D>>::commit_wal(w, domain, shard_index)
            }
            Self::Rupd(w) => {
                <rupd::RupdWorkUnit as WorkUnit<D>>::commit_wal(w, domain, shard_index)
            }
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_wal(w, domain, shard_index)
            }
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_wal(
                w,
                domain,
                shard_index,
            ),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_state(w, domain, shard_index)
            }
            Self::Roll(w) => {
                <roll::RollWorkUnit as WorkUnit<D>>::commit_state(w, domain, shard_index)
            }
            Self::Rupd(w) => {
                <rupd::RupdWorkUnit as WorkUnit<D>>::commit_state(w, domain, shard_index)
            }
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_state(w, domain, shard_index)
            }
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_state(
                    w,
                    domain,
                    shard_index,
                )
            }
            Self::ForcedStop => Err(DomainError::StopEpochReached),
        }
    }

    fn commit_archive(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_archive(w, domain, shard_index)
            }
            Self::Roll(w) => {
                <roll::RollWorkUnit as WorkUnit<D>>::commit_archive(w, domain, shard_index)
            }
            Self::Rupd(w) => {
                <rupd::RupdWorkUnit as WorkUnit<D>>::commit_archive(w, domain, shard_index)
            }
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_archive(w, domain, shard_index)
            }
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_archive(
                    w,
                    domain,
                    shard_index,
                )
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_indexes(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => {
                <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_indexes(w, domain, shard_index)
            }
            Self::Roll(w) => {
                <roll::RollWorkUnit as WorkUnit<D>>::commit_indexes(w, domain, shard_index)
            }
            Self::Rupd(w) => {
                <rupd::RupdWorkUnit as WorkUnit<D>>::commit_indexes(w, domain, shard_index)
            }
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_indexes(w, domain, shard_index)
            }
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_indexes(
                    w,
                    domain,
                    shard_index,
                )
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn finalize(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::finalize(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::finalize(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::finalize(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::finalize(w, domain),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::finalize(w, domain)
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn tip_events(&self) -> Vec<TipEvent> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::tip_events(w),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::tip_events(w)
            }
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

        // Crash-recovery check: if the previous process crashed mid-boundary,
        // `EpochState.ewrap_progress` will be `Some(p)` with `p.committed`
        // equal to the next Ewrap that should have run, and `p.total` the
        // boundary's shard count captured at the first commit. Detect and
        // warn — full resume requires re-fetching the boundary block from
        // upstream, which is tracked separately. The persisted `total` is
        // used for the in-flight boundary even if `crate::shard::ACCOUNT_SHARDS`
        // changed across versions, to avoid breaking the in-progress pipeline.
        if let Ok(epoch) = load_epoch::<D>(state) {
            if let Some(progress) = epoch.ewrap_progress.as_ref() {
                let configured = crate::shard::ACCOUNT_SHARDS;
                if progress.total != configured {
                    tracing::warn!(
                        epoch = epoch.number,
                        stored_total = progress.total,
                        configured_total = configured,
                        "in-flight boundary uses {} shards but ACCOUNT_SHARDS = {}; \
                         the in-flight boundary will continue with {} (the persisted total) \
                         and the new value takes effect on the next boundary",
                        progress.total,
                        configured,
                        progress.total,
                    );
                }
                if progress.committed < progress.total {
                    tracing::warn!(
                        epoch = epoch.number,
                        next_shard = progress.committed,
                        total_shards = progress.total,
                        "crash detected mid-boundary: ewrap_progress is set. \
                         On the next block that triggers the boundary, dolos will \
                         resume the Ewrap pipeline; correctness depends on shard \
                         idempotency (state deletes are no-ops if already applied; \
                         EWrapProgress guards on shard_index). Operators should \
                         monitor the subsequent boundary for inconsistency. \
                         TODO: implement true shard resume."
                    );
                } else {
                    tracing::warn!(
                        epoch = epoch.number,
                        committed = progress.committed,
                        total_shards = progress.total,
                        "found EpochState.ewrap_progress.committed == total at \
                         startup — Ewrap (closing phase) was not committed \
                         before crash. The next boundary attempt will re-run \
                         Ewraps and Ewrap; idempotency should keep the \
                         result correct."
                    );
                }
            }

            // Same crash-recovery check for the EStart-shard half of the
            // boundary. Only one of the two progress fields can be set at
            // any time — Ewrap clears `ewrap_progress` before any
            // EStart-shard runs, and `EpochTransition` clears
            // `estart_progress` when the new epoch opens.
            if let Some(progress) = epoch.estart_progress.as_ref() {
                let configured = crate::shard::ACCOUNT_SHARDS;
                if progress.total != configured {
                    tracing::warn!(
                        epoch = epoch.number,
                        stored_total = progress.total,
                        configured_total = configured,
                        "in-flight estart-shard boundary uses {} shards but \
                         ACCOUNT_SHARDS = {}; the in-flight boundary will \
                         continue with {} (the persisted total) and the new \
                         value takes effect on the next boundary",
                        progress.total,
                        configured,
                        progress.total,
                    );
                }
                if progress.committed < progress.total {
                    tracing::warn!(
                        epoch = epoch.number,
                        next_shard = progress.committed,
                        total_shards = progress.total,
                        "crash detected mid-boundary: estart_progress is set. \
                         On the next block that triggers the boundary, dolos will \
                         resume the EStart-shard pipeline; correctness depends on \
                         shard idempotency (EStartProgress guards on \
                         shard_index, but AccountTransition is not natively \
                         idempotent). Operators should monitor the subsequent \
                         boundary for inconsistency. TODO: implement true shard \
                         resume."
                    );
                } else {
                    tracing::warn!(
                        epoch = epoch.number,
                        committed = progress.committed,
                        total_shards = progress.total,
                        "found EpochState.estart_progress.committed == total \
                         at startup — Estart finalize was not committed before \
                         crash. The next boundary attempt will re-run \
                         EStart-shards and finalize; idempotency should keep the \
                         result correct."
                    );
                }
            }
        }

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

        // Convert internal work unit marker to concrete CardanoWorkUnit.
        // Sharding is a property of each work unit (`WorkUnit::total_shards`)
        // — the buffer no longer enumerates shards.
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
                    self.cache.clone(),
                ))))
            }
            InternalWorkUnit::Rupd(slot) => Some(CardanoWorkUnit::Rupd(Box::new(
                rupd::RupdWorkUnit::new(slot, domain.genesis()),
            ))),
            InternalWorkUnit::Ewrap(slot) => Some(CardanoWorkUnit::Ewrap(Box::new(
                ewrap::EwrapWorkUnit::new(slot, domain.genesis()),
            ))),
            InternalWorkUnit::Estart(slot) => {
                // Estart's `finalize()` runs the global Estart pass,
                // which may trigger era transitions — schedule cache
                // refresh so the next pop_work picks up the new eras.
                self.needs_cache_refresh = true;
                Some(CardanoWorkUnit::Estart(Box::new(
                    estart::EstartWorkUnit::new(slot, domain.genesis()),
                )))
            }
            InternalWorkUnit::ForcedStop => Some(CardanoWorkUnit::ForcedStop),
        }
    }

    fn compute_undo(
        block: &dolos_core::Cbor,
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<EraCbor>>,
        point: ChainPoint,
    ) -> Result<dolos_core::UndoBlockData, ChainError> {
        let block_arc = Arc::new(block.clone());
        let blockd = OwnedMultiEraBlock::decode(block_arc)?;
        let blockv = blockd.view();

        let decoded_inputs: std::collections::HashMap<_, _> = inputs
            .iter()
            .map(|(k, v)| {
                let out = (k.clone(), OwnedMultiEraOutput::decode(v.clone())?);
                Result::<_, ChainError>::Ok(out)
            })
            .collect::<Result<_, _>>()?;

        let utxo_delta = crate::utxoset::compute_undo_delta(blockv, &decoded_inputs)
            .map_err(ChainError::from)?;

        let index_delta = crate::indexes::index_delta_from_utxo_delta(point, &utxo_delta);

        let tx_hashes = blockv.txs().iter().map(|tx| tx.hash()).collect();

        Ok(dolos_core::UndoBlockData {
            utxo_delta,
            index_delta,
            tx_hashes,
        })
    }

    fn compute_catchup(
        block: &dolos_core::Cbor,
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<EraCbor>>,
        point: ChainPoint,
    ) -> Result<dolos_core::CatchUpBlockData, ChainError> {
        let block_arc = Arc::new(block.clone());
        let blockd = OwnedMultiEraBlock::decode(block_arc)?;
        let blockv = blockd.view();

        let decoded_inputs: std::collections::HashMap<_, _> = inputs
            .iter()
            .map(|(k, v)| {
                let out = (k.clone(), OwnedMultiEraOutput::decode(v.clone())?);
                Result::<_, ChainError>::Ok(out)
            })
            .collect::<Result<_, _>>()?;

        let utxo_delta = crate::utxoset::compute_apply_delta(blockv, &decoded_inputs)
            .map_err(ChainError::from)?;

        let mut builder = crate::indexes::CardanoIndexDeltaBuilder::new(point);

        // UTxO filter changes
        builder.add_produced_utxos_from_delta(&utxo_delta);
        builder.add_consumed_utxos_from_delta(&utxo_delta);

        // Archive indexes (shared logic)
        builder.index_block(blockv, &decoded_inputs);

        let tx_hashes = blockv.txs().iter().map(|tx| tx.hash()).collect();

        Ok(dolos_core::CatchUpBlockData {
            utxo_delta,
            index_delta: builder.build(),
            tx_hashes,
        })
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
