use pallas::{
    crypto::hash::Hasher,
    ledger::traverse::{MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use std::{path::Path, sync::Arc};
use tracing::info;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    config::CardanoConfig, BlockSlot, ChainError, ChainPoint, Domain, DomainError, EntityKey,
    MempoolAwareUtxoStore, MempoolTx, MempoolUpdate, RawBlock, StateStore, TaggedPayload,
    TipEvent,
    TxoRef, WorkUnit,
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

/// Trait alias for [`dolos_core::Domain`] implementations backed by Cardano chain logic.
///
/// Equivalent to `Domain<Chain = CardanoLogic, ChainSpecificError = CardanoError, Genesis = CardanoGenesis>`,
/// but avoids repeating all three associated-type constraints at every call site.
/// Use this in place of the verbose bound anywhere inside the `cardano` crate.
pub trait CardanoDomain:
    dolos_core::Domain<
    Chain = CardanoLogic,
    ChainSpecificError = CardanoError,
    Genesis = CardanoGenesis,
>
{
}

impl<T> CardanoDomain for T where
    T: dolos_core::Domain<
        Chain = CardanoLogic,
        ChainSpecificError = CardanoError,
        Genesis = CardanoGenesis,
    >
{
}

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

// ============================================================================
// Pallas ↔ dolos_core conversions (orphan rule prevents From/TryFrom impls)
// ============================================================================

pub fn pallas_hash_to_core<const N: usize>(
    h: pallas::crypto::hash::Hash<N>,
) -> dolos_core::hash::Hash<N> {
    dolos_core::hash::Hash::new(*h)
}

// Can the era integer be removed? Not sure. Santi said something about it.
pub(crate) fn multi_era_tx_from_era_cbor(
    era_body: &TaggedPayload,
) -> Result<MultiEraTx<'_>, CardanoError> {
    Ok(MultiEraTx::decode(era_body.bytes())?)
}

pub(crate) fn txo_ref_from_pallas(hash: pallas::crypto::hash::Hash<32>, idx: u32) -> TxoRef {
    TxoRef(pallas_hash_to_core(hash), idx)
}

pub(crate) fn era_cbor_from_output(output: &MultiEraOutput<'_>) -> TaggedPayload {
    TaggedPayload(output.era().into(), output.encode())
}

pub(crate) fn txo_ref_from_input(input: &MultiEraInput<'_>) -> TxoRef {
    TxoRef(pallas_hash_to_core(*input.hash()), input.index() as u32)
}

pub fn core_hash_to_pallas<const N: usize>(
    h: dolos_core::hash::Hash<N>,
) -> pallas::crypto::hash::Hash<N> {
    (*h.as_ref()).into()
}

fn multi_era_output_from_era_cbor(era_body: &TaggedPayload) -> Result<MultiEraOutput<'_>, CardanoError> {
    let era = pallas::ledger::traverse::Era::try_from(era_body.tag())?;
    Ok(MultiEraOutput::decode(era, era_body.bytes())?)
}

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
    D: Domain<
        Chain = CardanoLogic,
        Entity = CardanoEntity,
        EntityDelta = CardanoDelta,
        ChainSpecificError = CardanoError,
        Genesis = CardanoGenesis,
    >,
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

    fn load(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::load(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn compute(&mut self) -> Result<(), DomainError<D::ChainSpecificError>> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::compute(w),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::compute(w),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::compute(w),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::compute(w),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::compute(w),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_wal(w, domain),
            Self::ForcedStop => Ok(()),
        }
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        match self {
            Self::Genesis(w) => <genesis::GenesisWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Roll(w) => <roll::RollWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Rupd(w) => <rupd::RupdWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Ewrap(w) => <ewrap::EwrapWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::Estart(w) => <estart::EstartWorkUnit as WorkUnit<D>>::commit_state(w, domain),
            Self::ForcedStop => Err(DomainError::StopEpochReached),
        }
    }

    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
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

    fn commit_indexes(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
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
    pub fn refresh_cache<D: Domain>(
        &mut self,
        state: &D::State,
    ) -> Result<(), ChainError<D::ChainSpecificError>> {
        self.cache.eras = eras::load_era_summary::<D>(state)?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CardanoError {
    #[error("traverse error: {0}")]
    Traverse(#[from] pallas::ledger::traverse::Error),

    #[error("address decoding error: {0}")]
    Address(#[from] pallas::ledger::addresses::Error),

    #[error("cbor decoding error: {0}")]
    Cbor(#[from] pallas::codec::minicbor::decode::Error),

    #[error("validation error: {0}")]
    Validation(#[from] pallas::ledger::validate::utils::ValidationError),

    #[error("couldn't evaluate phase-2 script: {0}")]
    Phase2EvaluationError(String),

    #[error("phase-2 script rejected the transaction")]
    Phase2ValidationRejected(Vec<String>),

    #[error("invalid pool registration params")]
    InvalidPoolParams,

    #[error("invalid governance proposal params")]
    InvalidProposalParams,
}

#[derive(Clone)]
pub struct CardanoGenesis {
    pub byron: pallas::interop::hardano::configs::byron::GenesisFile,
    pub shelley: pallas::interop::hardano::configs::shelley::GenesisFile,
    pub alonzo: pallas::interop::hardano::configs::alonzo::GenesisFile,
    pub conway: pallas::interop::hardano::configs::conway::GenesisFile,
    pub shelley_hash: pallas::ledger::primitives::Hash<32>,
    pub force_protocol: Option<usize>,
}

impl dolos_core::Genesis for CardanoGenesis {}

impl CardanoGenesis {
    pub fn from_file_paths(
        byron: impl AsRef<Path>,
        shelley: impl AsRef<Path>,
        alonzo: impl AsRef<Path>,
        conway: impl AsRef<Path>,
        force_protocol: Option<usize>,
    ) -> Result<Self, std::io::Error> {
        let shelley_bytes = std::fs::read(shelley.as_ref())?;
        let mut hasher = Hasher::<256>::new();
        hasher.input(&shelley_bytes);
        let shelley_hash = hasher.finalize();

        let byron = pallas::ledger::configs::byron::from_file(byron.as_ref())?;
        let shelley = pallas::ledger::configs::shelley::from_file(shelley.as_ref())?;
        let alonzo = pallas::ledger::configs::alonzo::from_file(alonzo.as_ref())?;
        let conway = pallas::ledger::configs::conway::from_file(conway.as_ref())?;

        Ok(Self {
            byron,
            shelley,
            alonzo,
            conway,
            force_protocol,
            shelley_hash,
        })
    }
}

impl dolos_core::ChainLogic for CardanoLogic {
    type Config = CardanoConfig;
    type Block = OwnedMultiEraBlock;
    type Utxo = OwnedMultiEraOutput;
    type Delta = CardanoDelta;
    type Entity = CardanoEntity;
    type WorkUnit<
        D: Domain<
            Chain = Self,
            Entity = Self::Entity,
            EntityDelta = Self::Delta,
            ChainSpecificError = Self::ChainSpecificError,
            Genesis = Self::Genesis,
        >,
    > = CardanoWorkUnit;
    type ChainSpecificError = CardanoError;
    type Genesis = CardanoGenesis;

    fn initialize<D: Domain>(
        config: Self::Config,
        state: &D::State,
        genesis: Self::Genesis,
    ) -> Result<Self, ChainError<Self::ChainSpecificError>> {
        info!("initializing");

        let cursor = state.read_cursor()?;

        let work = match cursor {
            Some(cursor) => WorkBuffer::new_from_cursor(cursor),
            None => WorkBuffer::Empty,
        };

        let eras = eras::load_chain_summary_from_state(state)?;

        // Use randomness_stability_window (4k/f) for the RUPD trigger boundary.
        // The Haskell ledger's startStep fires at randomnessStabilisationWindow
        // into the epoch, capturing addrsRew (registered accounts) for the pre-Babbage
        // prefilter. Using 4k/f instead of 3k/f ensures the state at RUPD time includes
        // all deregistrations up to the correct threshold.
        let stability_window = utils::randomness_stability_window(&genesis)?;

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

    fn receive_block(
        &mut self,
        raw: RawBlock,
    ) -> Result<BlockSlot, ChainError<Self::ChainSpecificError>> {
        if !self.can_receive_block() {
            return Err(ChainError::CantReceiveBlock(raw));
        }

        let block = OwnedMultiEraBlock::decode(raw)
            .map_err(CardanoError::from)
            .map_err(ChainError::ChainSpecific)?;

        let work = self.work.take().expect("work buffer is initialized");

        let new_work = work.receive_block(block, &self.cache.eras, self.cache.stability_window);

        let last = new_work.last_point_seen().slot();

        self.work = Some(new_work);

        Ok(last)
    }

    fn pop_work<D>(&mut self, domain: &D) -> Option<CardanoWorkUnit>
    where
        D: Domain<
            Chain = Self,
            Entity = CardanoEntity,
            EntityDelta = CardanoDelta,
            ChainSpecificError = Self::ChainSpecificError,
            Genesis = Self::Genesis,
        >,
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

    fn compute_undo(
        block: &dolos_core::Cbor,
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<TaggedPayload>>,
        point: ChainPoint,
    ) -> Result<dolos_core::UndoBlockData, ChainError<Self::ChainSpecificError>> {
        let block_arc = Arc::new(block.clone());
        let blockd = OwnedMultiEraBlock::decode(block_arc)
            .map_err(CardanoError::from)
            .map_err(ChainError::ChainSpecific)?;
        let blockv = blockd.view();

        let decoded_inputs: std::collections::HashMap<_, _> = inputs
            .iter()
            .map(|(k, v)| {
                let decoded = OwnedMultiEraOutput::decode(v.clone())
                    .map_err(CardanoError::from)
                    .map_err(ChainError::ChainSpecific)?;
                let out = (k.clone(), decoded);
                Result::<_, ChainError<Self::ChainSpecificError>>::Ok(out)
            })
            .collect::<Result<_, _>>()?;

        let utxo_delta = crate::utxoset::compute_undo_delta(blockv, &decoded_inputs)
            .map_err(ChainError::from)?;

        let index_delta = crate::indexes::index_delta_from_utxo_delta(point, &utxo_delta);

        let tx_hashes = blockv
            .txs()
            .iter()
            .map(|tx| pallas_hash_to_core(tx.hash()))
            .collect();

        Ok(dolos_core::UndoBlockData {
            utxo_delta,
            index_delta,
            tx_hashes,
        })
    }

    fn compute_catchup(
        block: &dolos_core::Cbor,
        inputs: &std::collections::HashMap<dolos_core::TxoRef, Arc<TaggedPayload>>,
        point: ChainPoint,
    ) -> Result<dolos_core::CatchUpBlockData, ChainError<Self::ChainSpecificError>> {
        let block_arc = Arc::new(block.clone());
        let blockd = OwnedMultiEraBlock::decode(block_arc)
            .map_err(CardanoError::from)
            .map_err(ChainError::ChainSpecific)?;
        let blockv = blockd.view();

        let decoded_inputs: std::collections::HashMap<_, _> = inputs
            .iter()
            .map(|(k, v)| {
                let decoded = OwnedMultiEraOutput::decode(v.clone())
                    .map_err(CardanoError::from)
                    .map_err(ChainError::ChainSpecific)?;
                let out = (k.clone(), decoded);
                Result::<_, ChainError<Self::ChainSpecificError>>::Ok(out)
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

        let tx_hashes = blockv
            .txs()
            .iter()
            .map(|tx| pallas_hash_to_core(tx.hash()))
            .collect();

        Ok(dolos_core::CatchUpBlockData {
            utxo_delta,
            index_delta: builder.build(),
            tx_hashes,
        })
    }

    fn decode_utxo(
        &self,
        utxo: Arc<TaggedPayload>,
    ) -> Result<Self::Utxo, ChainError<Self::ChainSpecificError>> {
        let out = OwnedMultiEraOutput::decode(utxo)
            .map_err(CardanoError::from)
            .map_err(ChainError::ChainSpecific)?;

        Ok(out)
    }

    fn mutable_slots(
        domain: &impl Domain<Genesis = CardanoGenesis>,
    ) -> Result<BlockSlot, ChainError<CardanoError>> {
        utils::mutable_slots(&domain.genesis())
    }

    fn validate_tx<D: Domain<ChainSpecificError = CardanoError>>(
        &self,
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
        tip: Option<ChainPoint>,
        genesis: &CardanoGenesis,
    ) -> Result<MempoolTx, ChainError<Self::ChainSpecificError>> {
        validate::validate_tx(cbor, utxos, tip, genesis)
    }

    type EvalReport = pallas::ledger::validate::phase2::EvalReport;

    fn eval_tx<D: Domain<ChainSpecificError = CardanoError>>(
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
    ) -> Result<Self::EvalReport, ChainError<Self::ChainSpecificError>> {
        validate::evaluate_tx(cbor, utxos)
    }

    fn tx_produced_utxos(
        era_body: &TaggedPayload,
    ) -> Result<Vec<(dolos_core::TxoRef, TaggedPayload)>, CardanoError> {
        let tx = multi_era_tx_from_era_cbor(era_body)?;
        Ok(tx
            .produces()
            .iter()
            .map(|(idx, output)| {
                let txoref = txo_ref_from_pallas(tx.hash(), *idx as u32);
                let body = era_cbor_from_output(output);
                (txoref, body)
            })
            .collect())
    }

    fn tx_consumed_ref(era_body: &TaggedPayload) -> Result<Vec<dolos_core::TxoRef>, CardanoError> {
        let tx = multi_era_tx_from_era_cbor(era_body)?;
        Ok(tx.consumes().iter().map(txo_ref_from_input).collect())
    }
    fn find_tx_in_block(
        block: &[u8],
        tx_hash: &[u8],
    ) -> Result<Option<(TaggedPayload, dolos_core::TxOrder)>, Self::ChainSpecificError> {
        let block = MultiEraBlock::decode(block)?;
        let result = block
            .txs()
            .iter()
            .enumerate()
            .find(|(_, tx)| tx.hash().as_slice() == tx_hash)
            .map(|(idx, tx)| (TaggedPayload(block.era().into(), tx.encode()), idx));
        Ok(result)
    }
}

pub fn load_effective_pparams<D: Domain>(
    state: &D::State,
) -> Result<PParamsSet, ChainError<D::ChainSpecificError>> {
    let epoch = load_epoch::<D>(state)?;
    let active = epoch.pparams.unwrap_live();

    Ok(active.clone())
}

pub fn load_epoch<D: Domain>(
    state: &D::State,
) -> Result<EpochState, ChainError<D::ChainSpecificError>> {
    let epoch = state
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(CURRENT_EPOCH_KEY))?
        .ok_or(ChainError::NoActiveEpoch)?;

    Ok(epoch)
}

#[cfg(test)]
pub fn load_test_genesis(env: &str) -> CardanoGenesis {
    use std::path::PathBuf;

    let test_data = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("test_data")
        .join(env);

    CardanoGenesis::from_file_paths(
        test_data.join("genesis/byron.json"),
        test_data.join("genesis/shelley.json"),
        test_data.join("genesis/alonzo.json"),
        test_data.join("genesis/conway.json"),
        None,
    )
    .unwrap()
}
