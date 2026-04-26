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
pub mod utils;
pub mod utxoset;

// work units
pub mod ashard;
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
    /// Handle the global portion of the epoch boundary (pool/drep/proposal
    /// classification, MIRs, enactment, deposit refunds) and close the
    /// boundary by emitting `EpochWrapUp` with the assembled final
    /// `EndStats` (prepare-time fields + accumulator fields populated by
    /// the preceding `AShard` runs). The `EpochState.end` slot itself
    /// is opened by ESTART's `EpochTransition` at the start of each epoch.
    Ewrap(Box<ewrap::EwrapWorkUnit>),
    /// Handle one shard of per-account reward application. Emitted
    /// `config.account_shards` times in sequence. Each shard covers a
    /// first-byte prefix range of the account key space and accumulates its
    /// contribution into `EpochState.end` via `EpochEndAccumulate`.
    AShard(Box<ashard::AShardWorkUnit>),
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
            Self::AShard(w) => <ashard::AShardWorkUnit as WorkUnit<D>>::name(w),
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
            Self::AShard(w) => <ashard::AShardWorkUnit as WorkUnit<D>>::load(w, domain),
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
            Self::AShard(w) => <ashard::AShardWorkUnit as WorkUnit<D>>::compute(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::compute(w),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_wal(w, domain)
            }
            Self::AShard(w) => {
                <ashard::AShardWorkUnit as WorkUnit<D>>::commit_wal(w, domain)
            }
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_state(w, domain)
            }
            Self::AShard(w) => {
                <ashard::AShardWorkUnit as WorkUnit<D>>::commit_state(w, domain)
            }
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
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_archive(w, domain)
            }
            Self::AShard(w) => {
                <ashard::AShardWorkUnit as WorkUnit<D>>::commit_archive(w, domain)
            }
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
            Self::Ewrap(w) => {
                <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_indexes(w, domain)
            }
            Self::AShard(w) => {
                <ashard::AShardWorkUnit as WorkUnit<D>>::commit_indexes(w, domain)
            }
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
            Self::AShard(w) => <ashard::AShardWorkUnit as WorkUnit<D>>::tip_events(w),
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
    /// Cached effective `account_shards` value: equal to
    /// `EpochState.ashard_progress.total` when a boundary is in flight, or
    /// `config.account_shards()` otherwise. Refreshed at every `pop_work`
    /// call (which has state access) so `receive_block` (which does not)
    /// can use the up-to-date value when constructing
    /// `WorkBuffer::AShardingBoundary`.
    effective_account_shards: u32,
}

impl CardanoLogic {
    /// Refresh the cached era summary from state.
    /// Called after work units that may change era information (like genesis).
    pub fn refresh_cache<D: Domain>(&mut self, state: &D::State) -> Result<(), ChainError> {
        self.cache.eras = eras::load_era_summary::<D>(state)?;

        Ok(())
    }

    /// Compute the effective `account_shards` value: stored
    /// `ashard_progress.total` if a boundary is in flight, otherwise the
    /// configured value.
    fn read_effective_account_shards<D: Domain>(&self, state: &D::State) -> u32 {
        load_epoch::<D>(state)
            .ok()
            .and_then(|e| e.ashard_progress.as_ref().map(|p| p.total))
            .unwrap_or_else(|| self.config.account_shards())
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

        // Reject misconfigured `account_shards` early. The shard-key-range
        // helper only `debug_assert!`s the divides-256 invariant, so an
        // invalid release-build config (0, 3, 7, 100, ...) would silently
        // corrupt key-range coverage.
        crate::ashard::shard::validate_total_shards(config.account_shards())
            .map_err(ChainError::InvalidConfig)?;

        let cursor = state.read_cursor()?;

        let work = match cursor {
            Some(cursor) => WorkBuffer::new_from_cursor(cursor),
            None => WorkBuffer::Empty,
        };

        // Crash-recovery check: if the previous process crashed mid-boundary,
        // `EpochState.ashard_progress` will be `Some(p)` with `p.committed`
        // equal to the next AShard that should have run, and `p.total` the
        // boundary's shard count captured at the first commit. Detect and
        // warn — full resume requires re-fetching the boundary block from
        // upstream, which is tracked separately. The persisted `total` is
        // used for the in-flight boundary even if `config.account_shards()`
        // changed, to avoid breaking the in-progress pipeline.
        if let Ok(epoch) = load_epoch::<D>(state) {
            if let Some(progress) = epoch.ashard_progress.as_ref() {
                let configured = config.account_shards();
                if progress.total != configured {
                    tracing::warn!(
                        epoch = epoch.number,
                        stored_total = progress.total,
                        configured_total = configured,
                        "in-flight boundary uses {} shards but config.account_shards = {}; \
                         the in-flight boundary will continue with {} (the persisted total) \
                         and the new config takes effect on the next boundary",
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
                        "crash detected mid-boundary: ashard_progress is set. \
                         On the next block that triggers the boundary, dolos will \
                         resume the AShard pipeline; correctness depends on shard \
                         idempotency (state deletes are no-ops if already applied; \
                         EpochEndAccumulate guards on shard_index). Operators should \
                         monitor the subsequent boundary for inconsistency. \
                         TODO: implement true shard resume."
                    );
                } else {
                    tracing::warn!(
                        epoch = epoch.number,
                        committed = progress.committed,
                        total_shards = progress.total,
                        "found EpochState.ashard_progress.committed == total at \
                         startup — Ewrap (closing phase) was not committed \
                         before crash. The next boundary attempt will re-run \
                         AShards and Ewrap; idempotency should keep the \
                         result correct."
                    );
                }
            }
        }

        // Capture the effective account_shards value: stored total if a
        // boundary is in flight, otherwise the configured value.
        let effective_account_shards = load_epoch::<D>(state)
            .ok()
            .and_then(|e| e.ashard_progress.as_ref().map(|p| p.total))
            .unwrap_or_else(|| config.account_shards());

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
            effective_account_shards,
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

        let new_work = work.receive_block(
            block,
            &self.cache.eras,
            self.cache.stability_window,
            self.effective_account_shards,
        );

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

        // Refresh effective `account_shards` from state. While a boundary is
        // in flight, `EpochState.ashard_progress.total` overrides config so
        // the in-progress pipeline isn't disrupted by a config change.
        self.effective_account_shards =
            self.read_effective_account_shards::<D>(domain.state());

        let work = self.work.take().expect("work buffer is initialized");

        let (work_unit, new_buffer) =
            work.pop_work(self.config.stop_epoch, self.effective_account_shards);

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
                    self.cache.clone(),
                ))))
            }
            InternalWorkUnit::Rupd(slot) => Some(CardanoWorkUnit::Rupd(Box::new(
                rupd::RupdWorkUnit::new(slot, domain.genesis()),
            ))),
            InternalWorkUnit::Ewrap(slot) => Some(CardanoWorkUnit::Ewrap(Box::new(
                ewrap::EwrapWorkUnit::new(slot, self.config.clone(), domain.genesis()),
            ))),
            InternalWorkUnit::AShard(slot, shard_index) => {
                Some(CardanoWorkUnit::AShard(Box::new(
                    ashard::AShardWorkUnit::new(
                        slot,
                        self.config.clone(),
                        domain.genesis(),
                        shard_index,
                    ),
                )))
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
