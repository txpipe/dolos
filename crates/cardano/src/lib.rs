use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::sync::Arc;
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    config::CustomUtxo, BlockSlot, ChainError, ChainPoint, Domain, DomainError, EntityMap, Genesis,
    MempoolAwareUtxoStore, MempoolTx, MempoolUpdate, NsKey, RawBlock, StateError, StateStore,
    TipEvent, UtxoValue, WorkUnit,
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
pub mod final_rows;
pub mod forks;
pub mod hacks;
pub mod indexes;
pub mod model;
pub mod owned;
pub mod periods;
pub mod pool_history;
pub mod pots;
pub mod rewards;
pub mod shard;
pub mod utils;
pub mod utxoset;

// work units
pub mod estart;
pub mod ewrap;
pub mod genesis;
mod migrate;
pub mod roll;
pub mod rupd;
mod work;

pub mod validate;

#[cfg(feature = "include-genesis")]
pub mod include;

#[cfg(feature = "include-genesis")]
pub mod networks;

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
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::total_shards(w),
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
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::initialize(w, domain),
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
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::load(w, domain, shard_index),
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::load(w, domain, shard_index)
            }
            Self::ForcedStop => Ok(()),
        }
    }

    fn compute(&mut self, shard_index: u32) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::compute(w, shard_index),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::compute(w, shard_index),
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
            Self::Estart(w) => {
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_wal(w, domain, shard_index)
            }
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
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_state(w, domain, shard_index)
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
                <estart::EstartWorkUnit as WorkUnit<D>>::commit_archive(w, domain, shard_index)
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
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::finalize(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn archive_index_deltas(&self) -> &[dolos_core::ArchiveIndexDelta] {
        match self {
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::archive_index_deltas(w),
            _ => &[],
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

/// What [`CardanoLogic`] runs with, resolved from the configuration and the
/// invocation.
#[derive(Debug, Clone, Default)]
pub struct CardanoLogicConfig {
    /// Stop once the chain reaches this epoch.
    pub stop_epoch: Option<pallas::ledger::primitives::Epoch>,
    /// Outputs added to the genesis UTxO set.
    pub custom_utxos: Vec<CustomUtxo>,
}

pub struct CardanoLogic {
    config: CardanoLogicConfig,
    work: Option<WorkBuffer>,
    pub(crate) cache: Cache,
    /// Flag indicating the cache needs refresh after a work unit that modifies
    /// eras. Set after Genesis or EStart work units are popped, cleared at
    /// next pop_work call.
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
    type Config = CardanoLogicConfig;
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

        // One-time self-heal migrations. Must run before anything reads
        // governance state and before WAL catch-up replays deltas that
        // expect the singleton to exist.
        migrate::ensure_gov_singleton::<D>(state)?;

        let cursor = state.read_cursor()?;

        let work = match cursor {
            Some(cursor) => WorkBuffer::new_from_cursor(cursor),
            None => WorkBuffer::Empty,
        };

        // Crash-recovery diagnostics for a boundary interrupted by a prior
        // crash. The two progress fields are NOT mutually exclusive: under
        // `EpochWrapUpV3`, `ewrap_progress` is left at `Some(total, total)`
        // once the close phase finishes and is only cleared by
        // `EpochTransition` at ESTART finalize — so mid-open both fields are
        // set and both checks can fire. These only warn; resume itself is not
        // yet fully idempotent.
        // TODO: implement true shard resume (#1018).
        if let Ok(epoch) = load_epoch::<D>(state) {
            let configured = crate::shard::ACCOUNT_SHARDS;
            let warn_resume = |phase: &'static str, p: &ShardProgress| {
                if p.total != configured {
                    tracing::warn!(
                        phase,
                        epoch = epoch.number,
                        stored_total = p.total,
                        configured_total = configured,
                        "CARDANO-003: in-flight boundary shard count differs from configured; \
                         continuing with the persisted total"
                    );
                }
                if p.committed < p.total {
                    tracing::warn!(
                        phase,
                        epoch = epoch.number,
                        next_shard = p.committed,
                        total_shards = p.total,
                        "CARDANO-004: crash detected mid-boundary; resuming on the next boundary trigger"
                    );
                } else {
                    tracing::warn!(
                        phase,
                        epoch = epoch.number,
                        committed = p.committed,
                        total_shards = p.total,
                        "CARDANO-005: boundary half-closed at startup; resuming the remaining phase"
                    );
                }
            };

            if let Some(p) = epoch.ewrap_progress.as_ref() {
                warn_resume("ewrap", p);
            }
            if let Some(p) = epoch.estart_progress.as_ref() {
                warn_resume("estart", p);
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
                    *batch,
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
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<UtxoValue>>,
        _point: ChainPoint,
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

        // The tags the block put in and took out, as it did them: the undo
        // delta names them inverted (undone = what the block produced,
        // recovered = what it consumed), and `undo_utxo_tags` inverts again.
        let mut builder = crate::indexes::CardanoIndexDeltaBuilder::new();
        builder.add_produced_utxos(&utxo_delta.undone_utxo);
        builder.add_consumed_utxos(&utxo_delta.recovered_stxi);

        // The archive entries the block wrote, rebuilt the way the apply path
        // built them, so `undo_index` removes exactly what `apply_index` added.
        builder.index_block(blockv, &decoded_inputs);

        let (utxo_index_delta, archive_index_deltas) = builder.into_parts();

        let tx_hashes = blockv.txs().iter().map(|tx| tx.hash()).collect();

        Ok(dolos_core::UndoBlockData {
            utxo_delta,
            utxo_index_delta,
            archive_index_deltas,
            tx_hashes,
        })
    }

    fn compute_catchup(
        block: &dolos_core::Cbor,
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<UtxoValue>>,
        point: ChainPoint,
    ) -> Result<dolos_core::CatchUpBlockData, ChainError> {
        let block_arc = Arc::new(block.clone());
        let blockd = OwnedMultiEraBlock::decode(block_arc)?;
        let blockv = blockd.view();
        let decoded_point = ChainPoint::Specific(blockv.slot(), blockv.hash());
        if point.slot() != blockv.slot() || point.hash() != Some(blockv.hash()) {
            return Err(ChainError::consensus(format!(
                "the WAL entry at {point} holds a different block, at {decoded_point}"
            )));
        }

        let decoded_inputs: std::collections::HashMap<_, _> = inputs
            .iter()
            .map(|(k, v)| {
                let out = (k.clone(), OwnedMultiEraOutput::decode(v.clone())?);
                Result::<_, ChainError>::Ok(out)
            })
            .collect::<Result<_, _>>()?;

        let utxo_delta = crate::utxoset::compute_apply_delta(blockv, &decoded_inputs)
            .map_err(ChainError::from)?;

        let mut builder = crate::indexes::CardanoIndexDeltaBuilder::new();

        // UTxO filter changes
        builder.add_utxo_tags_from_delta(&utxo_delta);

        // Archive indexes (shared logic)
        builder.index_block(blockv, &decoded_inputs);

        let (utxo_index_delta, archive_index_deltas) = builder.into_parts();

        let tx_hashes = blockv.txs().iter().map(|tx| tx.hash()).collect();

        Ok(dolos_core::CatchUpBlockData {
            utxo_delta,
            utxo_index_delta,
            archive_index_deltas,
            tx_hashes,
        })
    }

    fn prepare_replay<S: StateStore>(
        state: &S,
        genesis: Arc<Genesis>,
        block: &dolos_core::Cbor,
        deltas: &mut [Self::Delta],
    ) -> Result<(), ChainError> {
        let needs_mint_epoch = deltas
            .iter()
            .any(|delta| matches!(delta, CardanoDelta::MintedBlocksInc(_)));
        #[allow(deprecated)]
        let has_transition = deltas.iter().any(|delta| {
            matches!(
                delta,
                CardanoDelta::EpochTransition(_) | CardanoDelta::EpochTransitionV2(_)
            )
        });
        let epoch = if needs_mint_epoch || !has_transition {
            let block = MultiEraBlock::decode(block)?;
            let summary = eras::load_chain_summary_from_state(state)?;
            if summary.is_empty() {
                return Err(ChainError::EraNotFound);
            }
            let epoch = summary.slot_epoch(block.slot()).0;
            if !has_transition {
                let initialized = state
                    .read_entity_typed::<EpochState>(&())?
                    .ok_or(ChainError::NoActiveEpoch)?;
                if epoch > initialized.number {
                    return Err(ChainError::consensus(format!(
                        "cannot replay the WAL block at slot {} in epoch {epoch}. The initialized epoch is {}. The epoch boundary work must initialize epoch {epoch} before the replay",
                        block.slot(), initialized.number
                    )));
                }
            }
            Some(epoch)
        } else {
            None
        };

        #[allow(deprecated)]
        for delta in deltas {
            match delta {
                CardanoDelta::MintedBlocksInc(delta) => {
                    delta.epoch = epoch.expect("a minting delta has a block epoch");
                }
                CardanoDelta::EpochTransition(delta) => delta.genesis = Some(genesis.clone()),
                CardanoDelta::EpochTransitionV2(delta) => delta.genesis = Some(genesis.clone()),
                _ => {}
            }
        }

        Ok(())
    }

    fn rolling_back<D: Domain>(domain: &D, to: &ChainPoint) {
        estart::stake::warn_on_dropped_rows(domain, to);
    }

    fn restore_for_undo<S: StateStore>(
        state: &S,
        key: &NsKey,
    ) -> Result<Option<CardanoEntity>, StateError> {
        estart::prune::restore_for_undo(state, key)
    }

    fn settle_undo<S: StateStore>(
        state: &S,
        entities: &mut EntityMap<CardanoEntity>,
    ) -> Result<(), StateError> {
        estart::prune::settle_undo(state, entities)
    }

    fn decode_utxo(&self, utxo: Arc<UtxoValue>) -> Result<Self::Utxo, ChainError> {
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
    read_singleton::<D, EpochState>(state)?.ok_or(ChainError::NoActiveEpoch)
}

/// Load the governance singleton, whose existence is an invariant
/// (seeded at bootstrap or by the CARDANO-006 startup migration).
pub fn load_gov<D: Domain>(state: &D::State) -> Result<GovState, ChainError> {
    read_singleton::<D, GovState>(state)?.ok_or(ChainError::MissingGovState)
}

/// Read a singleton entity directly at its fixed key. `None` means the
/// row was never created — for the known singletons that's a broken or
/// pre-migration store, which the `load_*` wrappers turn into errors.
pub fn read_singleton<D: Domain, E: SingletonEntity>(
    state: &D::State,
) -> Result<Option<E>, ChainError> {
    Ok(state.read_entity_typed::<E>(&())?)
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

#[cfg(test)]
mod replay_tests {
    use super::*;
    use dolos_core::{builtin::MemoryStateStore, ChainLogic as _, StateWriter as _};
    use dolos_testing::{blocks::make_conway_block, toy_domain::ToyDomain};

    #[test]
    fn compute_catchup_accepts_matching_point() {
        let (point, block) = make_conway_block(42);
        CardanoLogic::compute_catchup(&block, &Default::default(), point).unwrap();
    }

    #[test]
    fn compute_catchup_rejects_wrong_slot() {
        let (point, block) = make_conway_block(42);
        let wrong = ChainPoint::Specific(43, point.hash().unwrap());
        let error = CardanoLogic::compute_catchup(&block, &Default::default(), wrong.clone())
            .err()
            .expect("wrong slot must fail");
        assert!(matches!(error, ChainError::Consensus(_)));
        assert!(error.to_string().contains(&wrong.to_string()));
        assert!(error.to_string().contains(&point.to_string()));
    }

    #[test]
    fn compute_catchup_rejects_wrong_hash() {
        let (point, block) = make_conway_block(42);
        let wrong = ChainPoint::Specific(point.slot(), [0; 32].into());
        let error = CardanoLogic::compute_catchup(&block, &Default::default(), wrong.clone())
            .err()
            .expect("wrong hash must fail");
        assert!(matches!(error, ChainError::Consensus(_)));
        assert!(error.to_string().contains(&wrong.to_string()));
        assert!(error.to_string().contains(&point.to_string()));
    }

    #[test]
    fn compute_catchup_rejects_undefined_points() {
        let (_, block) = make_conway_block(0);
        for point in [ChainPoint::Origin, ChainPoint::Slot(0)] {
            assert!(matches!(
                CardanoLogic::compute_catchup(&block, &Default::default(), point),
                Err(ChainError::Consensus(_))
            ));
        }
    }

    #[test]
    fn prepare_replay_accepts_initialized_epoch_without_minting() {
        let domain = ToyDomain::new(None, None);
        let initialized = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .unwrap();
        let summary = eras::load_chain_summary_from_state(domain.state()).unwrap();
        let (_, block) = make_conway_block(summary.epoch_start(initialized.number) + 1);
        CardanoLogic::prepare_replay(domain.state(), domain.genesis(), &block, &mut []).unwrap();
    }

    #[test]
    fn prepare_replay_accepts_prior_epoch_without_minting() {
        let domain = ToyDomain::new(None, None);
        let writer = domain.state().start_writer().unwrap();
        let mut initialized = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .unwrap();
        initialized.number = 2;
        writer.write_entity_typed(&(), &initialized).unwrap();
        writer.commit().unwrap();
        let summary = eras::load_chain_summary_from_state(domain.state()).unwrap();
        let (_, block) = make_conway_block(summary.epoch_start(1) + 1);

        CardanoLogic::prepare_replay(domain.state(), domain.genesis(), &block, &mut []).unwrap();
        assert_eq!(
            domain
                .state()
                .read_entity_typed::<EpochState>(&())
                .unwrap()
                .unwrap()
                .number,
            2
        );
    }

    #[test]
    fn prepare_replay_rejects_uninitialized_epoch_without_minting() {
        let domain = ToyDomain::new(None, None);
        let initialized = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .unwrap();
        let summary = eras::load_chain_summary_from_state(domain.state()).unwrap();
        let epoch = initialized.number + 1;
        let (_, block) = make_conway_block(summary.epoch_start(epoch));
        let error = CardanoLogic::prepare_replay(domain.state(), domain.genesis(), &block, &mut [])
            .unwrap_err();
        assert!(matches!(error, ChainError::Consensus(_)));
        let message = error.to_string();
        assert!(message.contains(&format!("in epoch {epoch}")));
        assert!(message.contains(&format!("initialized epoch is {}", initialized.number)));
        assert!(message.contains("epoch boundary work"));
    }

    #[test]
    fn prepare_replay_rejects_missing_epoch_state() {
        let domain = ToyDomain::new(None, None);
        let state = MemoryStateStore::new();
        let summary = eras::load_chain_summary_from_state(domain.state()).unwrap();
        let writer = state.start_writer().unwrap();
        writer
            .write_entity_typed(&EraKey::from(summary.edge().protocol), summary.edge())
            .unwrap();
        writer.commit().unwrap();
        let (_, block) = make_conway_block(summary.edge().start.slot + 1);

        assert!(matches!(
            CardanoLogic::prepare_replay(&state, domain.genesis(), &block, &mut []),
            Err(ChainError::NoActiveEpoch)
        ));
    }

    #[test]
    fn prepare_replay_uses_era_offsets_without_changing_encoding() {
        let domain = ToyDomain::new(None, None);
        let writer = domain.state().start_writer().unwrap();
        writer
            .write_entity_typed(
                &EraKey::from(9),
                &EraState {
                    start: EraBoundary {
                        epoch: 666,
                        slot: 100_000,
                        timestamp: 0,
                    },
                    end: None,
                    epoch_length: 100,
                    slot_length: 1,
                    protocol: 9,
                },
            )
            .unwrap();
        writer
            .write_entity_typed(
                &(),
                &EpochState {
                    number: 666,
                    ..Default::default()
                },
            )
            .unwrap();
        writer.commit().unwrap();
        let (_, block) = make_conway_block(100_007);
        let delta = CardanoDelta::from(MintedBlocksInc {
            operator: [1; 28].into(),
            count: 1,
            epoch: 666,
        });
        let encoded = bincode::serialize(&vec![delta]).unwrap();
        let mut deltas: Vec<CardanoDelta> = bincode::deserialize(&encoded).unwrap();
        let CardanoDelta::MintedBlocksInc(delta) = &deltas[0] else {
            panic!("the delta is not a minting delta");
        };
        assert_eq!(delta.epoch, 0);

        CardanoLogic::prepare_replay(domain.state(), domain.genesis(), &block, &mut deltas)
            .unwrap();

        let CardanoDelta::MintedBlocksInc(delta) = &deltas[0] else {
            panic!("the delta is not a minting delta");
        };
        assert_eq!(delta.epoch, 666);
        assert_eq!(bincode::serialize(&deltas).unwrap(), encoded);
    }

    #[test]
    fn prepare_replay_rejects_missing_era_summary() {
        let domain = ToyDomain::new(None, None);
        let state = MemoryStateStore::new();
        let (_, block) = make_conway_block(100_007);
        let mut deltas = vec![CardanoDelta::from(MintedBlocksInc {
            operator: [1; 28].into(),
            count: 1,
            epoch: 0,
        })];

        assert!(matches!(
            CardanoLogic::prepare_replay(&state, domain.genesis(), &block, &mut deltas),
            Err(ChainError::EraNotFound)
        ));
    }

    #[test]
    #[allow(deprecated)]
    fn prepare_replay_restores_minting_context_with_legacy_transitions() {
        let domain = ToyDomain::new(None, None);
        let initialized = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .unwrap();
        let epoch = initialized.number + 1;
        let summary = eras::load_chain_summary_from_state(domain.state()).unwrap();
        let (_, block) = make_conway_block(summary.epoch_start(epoch));
        let transitions = [
            CardanoDelta::from(EpochTransition::new(
                epoch,
                pots::Pots::default(),
                None,
                None,
            )),
            CardanoDelta::from(EpochTransitionV2::new(
                epoch,
                pots::Pots::default(),
                None,
                None,
                None,
            )),
        ];
        for transition in transitions {
            let deltas = vec![
                transition,
                CardanoDelta::from(MintedBlocksInc {
                    operator: [1; 28].into(),
                    count: 1,
                    epoch,
                }),
            ];
            let encoded = bincode::serialize(&deltas).unwrap();
            let mut deltas: Vec<CardanoDelta> = bincode::deserialize(&encoded).unwrap();

            CardanoLogic::prepare_replay(domain.state(), domain.genesis(), &block, &mut deltas)
                .unwrap();

            let CardanoDelta::MintedBlocksInc(delta) = &deltas[1] else {
                panic!("the delta is not a minting delta");
            };
            assert_eq!(delta.epoch, epoch);
            assert_eq!(bincode::serialize(&deltas).unwrap(), encoded);
            assert_eq!(
                domain
                    .state()
                    .read_entity_typed::<EpochState>(&())
                    .unwrap()
                    .unwrap()
                    .number,
                initialized.number
            );
        }
    }

    #[test]
    #[allow(deprecated)]
    fn prepare_replay_restores_transition_genesis_without_changing_encoding() {
        let domain = ToyDomain::new(None, None);
        let genesis = domain.genesis();
        let transition = Some(EraTransition {
            prev_version: EraKey::from(8),
            new_version: EraKey::from(9),
        });
        let deltas = vec![
            CardanoDelta::from(EpochTransition::new(
                1,
                pots::Pots::default(),
                transition.clone(),
                Some(genesis.clone()),
            )),
            CardanoDelta::from(EpochTransitionV2::new(
                1,
                pots::Pots::default(),
                transition,
                None,
                Some(genesis.clone()),
            )),
        ];
        let state = MemoryStateStore::new();
        for delta in deltas {
            let encoded = bincode::serialize(&vec![delta]).unwrap();
            let mut deltas: Vec<CardanoDelta> = bincode::deserialize(&encoded).unwrap();
            let restored = match &deltas[0] {
                CardanoDelta::EpochTransition(delta) => &delta.genesis,
                CardanoDelta::EpochTransitionV2(delta) => &delta.genesis,
                _ => unreachable!(),
            };
            assert!(restored.is_none());

            CardanoLogic::prepare_replay(&state, genesis.clone(), &vec![], &mut deltas).unwrap();

            let restored = match &deltas[0] {
                CardanoDelta::EpochTransition(delta) => &delta.genesis,
                CardanoDelta::EpochTransitionV2(delta) => &delta.genesis,
                _ => unreachable!(),
            };
            assert!(Arc::ptr_eq(restored.as_ref().unwrap(), &genesis));
            assert_eq!(bincode::serialize(&deltas).unwrap(), encoded);
        }
    }
}
