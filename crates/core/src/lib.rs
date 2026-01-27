//! Traits and machinery that are common to all dolos crates.
//!
//! Glossary:
//!  - `chunk`: when the grouping is about cutting a continuous sequence into
//!    pieces for parallel processing (e.g. sequence of blocks to decode).
//!  - `batch`: when the grouping is about workload semantics for pipelining
//!    where the order of execution matters (e.g. batch of blocks that need to
//!    be processed together). A batch is usually split into chunks for parallel
//!    processing.

use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraInput, MultiEraOutput, MultiEraTx, MultiEraUpdate},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::Path,
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use tracing::info;

pub mod archive;
pub mod bootstrap;
pub mod builtin;
pub mod config;
pub mod crawl;
pub mod import;
pub mod indexes;
pub mod mempool;
pub mod point;
pub mod query;
pub mod state;
pub mod submit;
pub mod sync;
pub mod wal;
pub mod work_unit;

pub use bootstrap::BootstrapExt;
pub use import::ImportExt;
pub use submit::SubmitExt;
pub use sync::SyncExt;
pub use work_unit::WorkUnit;

pub type Era = u16;

/// The index of an output in a tx
pub type TxoIdx = u32;

/// The order of a tx in a block
pub type TxOrder = usize;

/// The slot of a block (a.k.a. block index)
pub type BlockSlot = u64;

/// The height of a block (a.k.a. block number)
pub type BlockHeight = u64;

pub type Cbor = Vec<u8>;
pub type BlockBody = Cbor;
pub type RawBlock = Arc<BlockBody>;
pub type RawBlockBatch = Vec<RawBlock>;
pub type RawUtxoMap = HashMap<TxoRef, Arc<EraCbor>>;
pub type BlockEra = pallas::ledger::traverse::Era;
pub type BlockHash = Hash<32>;
pub type BlockHeader = Cbor;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (u16, Cbor);
pub type ChainTip = pallas::network::miniprotocols::chainsync::Tip;
pub type LogSeq = u64;

pub use archive::*;
pub use indexes::*;
pub use mempool::*;
pub use point::*;
pub use query::*;
pub use state::*;
pub use wal::*;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EraCbor(pub Era, pub Cbor);

impl EraCbor {
    pub fn era(&self) -> Era {
        self.0
    }

    pub fn cbor(&self) -> &[u8] {
        &self.1
    }
}

impl AsRef<[u8]> for EraCbor {
    fn as_ref(&self) -> &[u8] {
        &self.1
    }
}

impl From<(Era, Cbor)> for EraCbor {
    fn from(value: (Era, Cbor)) -> Self {
        Self(value.0, value.1)
    }
}

impl From<EraCbor> for (Era, Cbor) {
    fn from(value: EraCbor) -> Self {
        (value.0, value.1)
    }
}

impl From<MultiEraOutput<'_>> for EraCbor {
    fn from(value: MultiEraOutput<'_>) -> Self {
        EraCbor(value.era().into(), value.encode())
    }
}

impl<'a> TryFrom<&'a EraCbor> for MultiEraOutput<'a> {
    type Error = pallas::codec::minicbor::decode::Error;

    fn try_from(value: &'a EraCbor) -> Result<Self, Self::Error> {
        let era = value.0.try_into().expect("era out of range");
        MultiEraOutput::decode(era, &value.1)
    }
}

impl<'a> TryFrom<&'a EraCbor> for MultiEraTx<'a> {
    type Error = pallas::codec::minicbor::decode::Error;

    fn try_from(value: &'a EraCbor) -> Result<Self, Self::Error> {
        let era = value.0.try_into().expect("era out of range");
        MultiEraTx::decode_for_era(era, &value.1)
    }
}

impl TryFrom<EraCbor> for MultiEraUpdate<'_> {
    type Error = pallas::codec::minicbor::decode::Error;

    fn try_from(value: EraCbor) -> Result<Self, Self::Error> {
        let era = value.0.try_into().expect("era out of range");
        MultiEraUpdate::decode_for_era(era, &value.1)
    }
}

impl From<&MultiEraInput<'_>> for TxoRef {
    fn from(value: &MultiEraInput<'_>) -> Self {
        TxoRef(*value.hash(), value.index() as u32)
    }
}

impl From<TxoRef> for Vec<u8> {
    fn from(value: TxoRef) -> Self {
        let mut bytes = value.0.to_vec();
        bytes.extend_from_slice(value.1.to_be_bytes().as_slice());
        bytes
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct TxoRef(pub TxHash, pub TxoIdx);

impl From<(TxHash, TxoIdx)> for TxoRef {
    fn from(value: (TxHash, TxoIdx)) -> Self {
        Self(value.0, value.1)
    }
}

impl From<TxoRef> for (TxHash, TxoIdx) {
    fn from(value: TxoRef) -> Self {
        (value.0, value.1)
    }
}

impl Display for TxoRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.0, self.1)
    }
}

impl FromStr for TxoRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('#').collect::<Vec<_>>();

        if parts.len() != 2 {
            return Err(format!("invalid txo ref: {}", s));
        }

        let tx_hash = TxHash::from_str(parts[0]).map_err(|_| format!("invalid txo ref: {}", s))?;

        let txo_idx = parts[1]
            .parse()
            .map_err(|_| format!("invalid txo ref: {}", s))?;

        Ok(Self(tx_hash, txo_idx))
    }
}

// TODO: remove legacy
// #[derive(Debug, Eq, PartialEq, Hash)]
// pub struct ChainPoint(pub BlockSlot, pub BlockHash);

#[derive(Debug, Error)]
pub enum BrokenInvariant {
    #[error("missing utxo {0:?}")]
    MissingUtxo(TxoRef),

    #[error("invalid genesis config")]
    InvalidGenesisConfig,

    #[error("bad bootstrap")]
    BadBootstrap,

    #[error("invalid epoch state")]
    InvalidEpochState,

    #[error("missing pool {}", hex::encode(.0))]
    MissingPool(Vec<u8>),

    #[error("epoch boundary incomplete")]
    EpochBoundaryIncomplete,
}

pub type UtxoMap = HashMap<TxoRef, Arc<EraCbor>>;

pub type UtxoSet = HashSet<TxoRef>;

#[derive(Default, Debug, Clone)]
pub struct UtxoSetDelta {
    pub produced_utxo: HashMap<TxoRef, Arc<EraCbor>>,
    pub consumed_utxo: HashMap<TxoRef, Arc<EraCbor>>,
    pub recovered_stxi: HashMap<TxoRef, Arc<EraCbor>>,
    pub undone_utxo: HashMap<TxoRef, Arc<EraCbor>>,
}

#[derive(Debug, Clone)]
pub enum PullEvent {
    RollForward(RawBlock),
    Rollback(ChainPoint),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogValue<D>
where
    D: EntityDelta,
{
    pub block: Cbor,
    pub delta: Vec<D>,
    pub inputs: HashMap<TxoRef, Arc<EraCbor>>,
}

impl<D> LogValue<D>
where
    D: EntityDelta,
{
    pub fn origin() -> Self {
        Self {
            block: vec![],
            delta: vec![],
            inputs: HashMap::new(),
        }
    }
}

pub type LogEntry<D> = (ChainPoint, LogValue<D>);

#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal is not empty")]
    NotEmpty,

    #[error("point not found in chain {0:?}")]
    PointNotFound(ChainPoint),

    #[error("slot not found in chain {0}")]
    SlotNotFound(BlockSlot),

    #[error("IO error: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl WalError {
    pub fn internal<T>(value: T) -> Self
    where
        T: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        WalError::Internal(value.into())
    }
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("failed to bind listener")]
    BindError(std::io::Error),

    #[error("failed to shutdown")]
    ShutdownError(std::io::Error),

    #[error(transparent)]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone)]
pub struct Genesis {
    pub byron: pallas::interop::hardano::configs::byron::GenesisFile,
    pub shelley: pallas::interop::hardano::configs::shelley::GenesisFile,
    pub alonzo: pallas::interop::hardano::configs::alonzo::GenesisFile,
    pub conway: pallas::interop::hardano::configs::conway::GenesisFile,
    pub shelley_hash: Hash<32>,
    pub force_protocol: Option<usize>,
}

impl Genesis {
    pub fn network_magic(&self) -> u32 {
        self.shelley.network_magic.unwrap_or_default()
    }

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

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("internal error: {0}")]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("traverse error: {0}")]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error("decode error: {0}")]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error(transparent)]
    IndexError(#[from] IndexError),

    #[error("plutus not supported")]
    PlutusNotSupported,

    #[error("invalid tx: {0}")]
    InvalidTx(String),

    #[error("pparams not available")]
    PParamsNotAvailable,
}

pub trait MempoolStore: Clone + Send + Sync + 'static {
    type Stream: futures_core::Stream<Item = Result<MempoolEvent, MempoolError>>
        + Unpin
        + Send
        + Sync;

    fn receive(&self, tx: mempool::MempoolTx) -> Result<(), MempoolError>;

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]);
    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage;
    fn subscribe(&self) -> Self::Stream;

    fn pending(&self) -> Vec<(TxHash, EraCbor)>;
}

pub trait Block: Sized + Send + Sync {
    fn depends_on(&self, loaded: &mut RawUtxoMap) -> Vec<TxoRef>;
    fn slot(&self) -> BlockSlot;
    fn hash(&self) -> BlockHash;
    fn raw(&self) -> RawBlock;

    fn point(&self) -> ChainPoint {
        let slot = self.slot();
        let hash = self.hash();
        ChainPoint::Specific(slot, hash)
    }
}

pub type Phase2Log = Vec<String>;

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("can't receive block until previous work is completed")]
    CantReceiveBlock(RawBlock),

    #[error(transparent)]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("decoding error")]
    DecodingError(#[from] pallas::ledger::traverse::Error),

    #[error("cbor error")]
    CborDecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("invalid namespace: {0}")]
    InvalidNamespace(Namespace),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error(transparent)]
    IndexError(#[from] IndexError),

    #[error(transparent)]
    ArchiveError(#[from] ArchiveError),

    #[error("genesis field missing: {0}")]
    GenesisFieldMissing(String),

    #[error("protocol params not found: {0}")]
    PParamsNotFound(String),

    #[error("no active epoch")]
    NoActiveEpoch,

    #[error("era not found")]
    EraNotFound,

    #[error("epoch value version not found for epoch {0}")]
    EpochValueVersionNotFound(Epoch),

    #[error("missing rewards")]
    MissingRewards,

    #[error("invalid pool params")]
    InvalidPoolParams,

    #[error("invalid proposal params")]
    InvalidProposalParams,

    #[error("phase-1 script rejected the transaction")]
    Phase1ValidationRejected(#[from] pallas::ledger::validate::utils::ValidationError),

    #[error("couldn't evaluate phase-2 script: {0}")]
    Phase2EvaluationError(String),

    #[error("phase-2 script rejected the transaction")]
    Phase2ValidationRejected(Phase2Log),
}

// Note: The WorkUnit trait is now defined in work_unit.rs
// Chain-specific work unit implementations live in their respective crates
// (e.g., dolos-cardano for Cardano work units)

/// Trait for blockchain-specific logic.
///
/// This trait defines the interface between the generic node infrastructure
/// and chain-specific implementations. It handles block reception, work unit
/// production, and transaction validation.
///
/// Work units are produced by the chain logic and executed by the generic
/// executor. This separation allows the core crate to remain chain-agnostic.
pub trait ChainLogic: Sized + Send + Sync {
    type Config: Clone;
    type Block: Block + Send + Sync;
    type Entity: Entity;
    type Utxo: Sized + Send + Sync;
    type Delta: EntityDelta<Entity = Self::Entity>;

    /// The concrete work unit type produced by this chain logic.
    type WorkUnit<D: Domain<Chain = Self, Entity = Self::Entity, EntityDelta = Self::Delta>>: WorkUnit<D>;

    /// Initialize the chain logic with configuration and state.
    fn initialize<D: Domain>(
        config: Self::Config,
        state: &D::State,
        genesis: &Genesis,
    ) -> Result<Self, ChainError>;

    /// Check if the chain logic can receive a new block.
    ///
    /// Returns false if there is pending work that must be processed
    /// before new blocks can be received.
    fn can_receive_block(&self) -> bool;

    /// Receive a raw block for processing.
    ///
    /// The block is queued for processing. Call `pop_work()` to get
    /// work units that should be executed.
    fn receive_block(&mut self, raw: RawBlock) -> Result<BlockSlot, ChainError>;

    /// Pop the next work unit to execute.
    ///
    /// Returns the next work unit to execute, or `None` if no work is
    /// currently ready.
    ///
    /// The returned work unit should be executed using `executor::execute_work_unit()`.
    fn pop_work<D: Domain>(&mut self, domain: &D) -> Option<Self::WorkUnit<D>>
    where
        D: Domain<Chain = Self, Entity = Self::Entity, EntityDelta = Self::Delta>;

    // TODO: remove from the interface - this is Cardano-specific
    fn decode_utxo(&self, utxo: Arc<EraCbor>) -> Result<Self::Utxo, ChainError>;

    // TODO: remove from the interface - this is Cardano-specific
    fn mutable_slots(domain: &impl Domain) -> BlockSlot;

    // TODO: remove from the interface - this is Cardano-specific
    fn last_immutable_slot(domain: &impl Domain, tip: BlockSlot) -> BlockSlot {
        tip.saturating_sub(Self::mutable_slots(domain))
    }

    /// Validate a transaction against the current ledger state.
    fn validate_tx<D: Domain>(
        &self,
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
        tip: Option<ChainPoint>,
        genesis: &Genesis,
    ) -> Result<mempool::MempoolTx, ChainError>;
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("wal error: {0}")]
    WalError(#[from] WalError),

    #[error("chain error: {0}")]
    ChainError(#[from] ChainError),

    #[error("state error: {0}")]
    StateError(#[from] StateError),

    #[error("archive error: {0}")]
    ArchiveError(#[from] ArchiveError),

    #[error("index error: {0}")]
    IndexError(#[from] IndexError),

    #[error("mempool error: {0}")]
    MempoolError(#[from] MempoolError),

    #[error("inconsistent state: {0}")]
    InconsistentState(String),

    #[error("wal is empty")]
    WalIsEmpty,

    #[error("wal is behind state: {0}")]
    WalIsBehindState(BlockSlot, BlockSlot),

    #[error("forced stop epoch reached")]
    StopEpochReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TipEvent {
    Mark(ChainPoint),
    Apply(ChainPoint, RawBlock),
    Undo(ChainPoint, RawBlock),
}

#[trait_variant::make(Send)]
pub trait TipSubscription: Send + Sync + 'static {
    async fn next_tip(&mut self) -> TipEvent;
}

#[trait_variant::make(Send)]
pub trait Domain: Send + Sync + Clone + 'static {
    type Entity: Entity;
    type EntityDelta: EntityDelta<Entity = Self::Entity> + std::fmt::Debug;

    type Chain: ChainLogic<
        Delta = Self::EntityDelta,
        Entity = Self::Entity,
        WorkUnit<Self> = Self::WorkUnit,
    >;

    /// The concrete work unit type for this domain.
    /// This should be an enum containing all possible work unit variants.
    type WorkUnit: WorkUnit<Self>;

    type Wal: WalStore<Delta = Self::EntityDelta>;
    type State: StateStore;
    type Archive: ArchiveStore;
    type Indexes: IndexStore;
    type Mempool: MempoolStore;
    type TipSubscription: TipSubscription;

    fn storage_config(&self) -> &config::StorageConfig;
    fn genesis(&self) -> Arc<Genesis>;

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain>;
    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain>;

    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn indexes(&self) -> &Self::Indexes;
    fn mempool(&self) -> &Self::Mempool;

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError>;
    fn notify_tip(&self, tip: TipEvent);

    const MAX_PRUNE_SLOTS_PER_HOUSEKEEPING: u64 = 10_000;

    fn housekeeping(&self) -> Result<bool, DomainError> {
        let max_ledger_slots = self
            .storage_config()
            .state
            .max_history()
            .unwrap_or(Self::Chain::mutable_slots(self));

        info!(max_ledger_slots, "pruning ledger for excess history");

        let mut archive_pruned = true;

        if let Some(max_slots) = self.storage_config().archive.max_history() {
            info!(max_slots, "pruning archive for excess history");

            archive_pruned = self
                .archive()
                .prune_history(max_slots, Some(Self::MAX_PRUNE_SLOTS_PER_HOUSEKEEPING))?;
        }

        let mut wal_pruned = true;

        if let Some(max_slots) = self.storage_config().wal.max_history() {
            info!(max_slots, "pruning wal for excess history");

            wal_pruned = self
                .wal()
                .prune_history(max_slots, Some(Self::MAX_PRUNE_SLOTS_PER_HOUSEKEEPING))?;
        }

        Ok(archive_pruned && wal_pruned)
    }
}

#[trait_variant::make(Send)]
pub trait CancelToken: Send + Sync + 'static + Clone {
    async fn cancelled(&self);
}

#[trait_variant::make(Send)]
pub trait Driver<D: Domain, C: CancelToken>: Send + Sync + 'static {
    type Config: Clone;

    async fn run(config: Self::Config, domain: D, cancel: C) -> Result<(), ServeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn slot_to_hash(slot: u64) -> BlockHash {
        let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
        hasher.input(&(slot as i32).to_le_bytes());
        hasher.finalize()
    }

    #[test]
    fn chainpoint_partial_eq() {
        assert_eq!(ChainPoint::Origin, ChainPoint::Origin);

        assert_eq!(
            ChainPoint::Specific(20, slot_to_hash(20)),
            ChainPoint::Specific(20, slot_to_hash(20))
        );

        assert_ne!(
            ChainPoint::Origin,
            ChainPoint::Specific(20, slot_to_hash(20))
        );

        assert_ne!(
            ChainPoint::Specific(20, slot_to_hash(20)),
            ChainPoint::Specific(50, slot_to_hash(50)),
        );

        assert_ne!(
            ChainPoint::Specific(50, slot_to_hash(20)),
            ChainPoint::Specific(50, slot_to_hash(50)),
        );
    }

    #[test]
    fn chainpoint_partial_ord() {
        assert!(ChainPoint::Origin <= ChainPoint::Origin);
        assert!(ChainPoint::Origin >= ChainPoint::Origin);
        assert!(ChainPoint::Origin < ChainPoint::Specific(20, slot_to_hash(20)));
        assert!(
            ChainPoint::Specific(19, slot_to_hash(19)) < ChainPoint::Specific(20, slot_to_hash(20))
        );
        assert!(
            ChainPoint::Specific(20, slot_to_hash(20))
                .cmp(&ChainPoint::Specific(20, slot_to_hash(200)))
                != std::cmp::Ordering::Equal
        );
    }
}
