//! Traits and machinery that are common to all dolos crates.
//!
//! Glossary:
//!  - `chunk`: when the grouping is about cutting a continuous sequence into
//!    pieces for parallel processing (e.g. sequence of blocks to decode).
//!  - `batch`: when the grouping is about workload semantics for pipelining
//!    where the order of execution matters (e.g. batch of blocks that need to
//!    be processed together). A batch is usually split into chunks for parallel
//!    processing.

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use tracing::info;

pub mod archive;
pub mod async_query;
pub mod bootstrap;
pub mod builtin;
pub mod config;
pub mod crawl;
pub mod hash;
pub mod import;
pub mod indexes;
pub mod mempool;
pub mod point;
pub mod state;
pub mod submit;
pub mod sync;
pub mod wal;
pub mod work_unit;

pub use bootstrap::BootstrapExt;
pub use import::ImportExt;
pub use submit::SubmitExt;
pub use sync::SyncExt;
pub use work_unit::{MempoolUpdate, WorkUnit};

pub type Era = u16;

pub type Epoch = u64;

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
pub type RawUtxoMap = HashMap<TxoRef, Arc<TaggedPayload>>;
pub type BlockHash = crate::hash::Hash<32>;
pub type BlockHeader = Cbor;
pub type TxHash = crate::hash::Hash<32>;

/// Data needed to undo a block during rollback.
///
/// Chain-specific implementations compute this from the raw block CBOR
/// and the resolved inputs stored in the WAL.
pub struct UndoBlockData {
    pub utxo_delta: UtxoSetDelta,
    pub index_delta: IndexDelta,
    pub tx_hashes: Vec<TxHash>,
}

/// Data needed to catch up stores from a WAL entry during recovery.
///
/// Chain-specific implementations compute this from the raw block CBOR
/// and the resolved inputs stored in the WAL. Used during bootstrap to
/// replay blocks that are in the WAL but not yet applied to indexes.
pub struct CatchUpBlockData {
    pub utxo_delta: UtxoSetDelta,
    pub index_delta: IndexDelta,
    pub tx_hashes: Vec<TxHash>,
}

pub type OutputIdx = u64;
pub type UtxoBody = (u16, Cbor);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTip {
    pub point: ChainPoint,
    pub block_number: u64,
}
pub type LogSeq = u64;

pub use archive::*;
pub use async_query::*;
pub use indexes::*;
pub use mempool::*;
pub use point::*;
pub use state::*;
pub use wal::*;

/// A chain-agnostic tagged payload: a `u16` discriminant (whose meaning is
/// chain-specific, e.g. era for Cardano) paired with raw bytes.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TaggedPayload(pub u16, pub Vec<u8>);

impl TaggedPayload {
    pub fn tag(&self) -> u16 {
        self.0
    }

    pub fn bytes(&self) -> &[u8] {
        &self.1
    }
}

impl AsRef<[u8]> for TaggedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.1
    }
}

impl From<(u16, Vec<u8>)> for TaggedPayload {
    fn from(value: (u16, Vec<u8>)) -> Self {
        Self(value.0, value.1)
    }
}

impl From<TaggedPayload> for (u16, Vec<u8>) {
    fn from(value: TaggedPayload) -> Self {
        (value.0, value.1)
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

impl TxoRef {
    /// Serialize to the 36-byte index key format: `[tx_hash (32 bytes) || output_index (4 bytes, big-endian)]`.
    pub fn to_index_bytes(&self) -> [u8; 36] {
        let mut bytes = [0u8; 36];
        bytes[0..32].copy_from_slice(self.0.as_slice());
        bytes[32..36].copy_from_slice(&self.1.to_be_bytes());
        bytes
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

pub type UtxoMap = HashMap<TxoRef, Arc<TaggedPayload>>;

pub type UtxoSet = HashSet<TxoRef>;

#[derive(Default, Debug, Clone)]
pub struct UtxoSetDelta {
    pub produced_utxo: HashMap<TxoRef, Arc<TaggedPayload>>,
    pub consumed_utxo: HashMap<TxoRef, Arc<TaggedPayload>>,
    pub recovered_stxi: HashMap<TxoRef, Arc<TaggedPayload>>,
    pub undone_utxo: HashMap<TxoRef, Arc<TaggedPayload>>,
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
    pub inputs: HashMap<TxoRef, Arc<TaggedPayload>>,
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

//#[derive(Clone)]
//pub struct GenesisCardanoCardano {
//    pub byron: pallas::interop::hardano::configs::byron::GenesisFile,
//    pub shelley: pallas::interop::hardano::configs::shelley::GenesisFile,
//    pub alonzo: pallas::interop::hardano::configs::alonzo::GenesisFile,
//    pub conway: pallas::interop::hardano::configs::conway::GenesisFile,
//    pub shelley_hash: Hash<32>,
//    pub force_protocol: Option<usize>,
//}

//impl GenesisCardanoCardano {
//    pub fn network_magic(&self) -> u32 {
//        self.shelley.network_magic.unwrap_or_default()
//    }
//
//    pub fn from_file_paths(
//        byron: impl AsRef<Path>,
//        shelley: impl AsRef<Path>,
//        alonzo: impl AsRef<Path>,
//        conway: impl AsRef<Path>,
//        force_protocol: Option<usize>,
//    ) -> Result<Self, std::io::Error> {
//        let shelley_bytes = std::fs::read(shelley.as_ref())?;
//        let mut hasher = Hasher::<256>::new();
//        hasher.input(&shelley_bytes);
//        let shelley_hash = hasher.finalize();
//
//        let byron = pallas::ledger::configs::byron::from_file(byron.as_ref())?;
//        let shelley = pallas::ledger::configs::shelley::from_file(shelley.as_ref())?;
//        let alonzo = pallas::ledger::configs::alonzo::from_file(alonzo.as_ref())?;
//        let conway = pallas::ledger::configs::conway::from_file(conway.as_ref())?;
//
//        Ok(Self {
//            byron,
//            shelley,
//            alonzo,
//            conway,
//            force_protocol,
//            shelley_hash,
//        })
//    }
//}

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
pub enum ChainError<E: std::error::Error + Send + Sync + 'static> {
    #[error("can't receive block until previous work is completed")]
    CantReceiveBlock(RawBlock),

    #[error(transparent)]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("invalid namespace: {0}")]
    InvalidNamespace(Namespace),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error(transparent)]
    IndexError(#[from] IndexError),

    #[error(transparent)]
    ArchiveError(#[from] ArchiveError<E>),

    #[error("genesis field missing: {0}")]
    GenesisFieldMissing(String),

    #[error(transparent)]
    ChainSpecific(E),
}

pub trait Genesis: Clone + Send + Sync + 'static {}

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
    type Genesis: Genesis;
    type ChainSpecificError: std::error::Error + Send + Sync;

    /// The concrete work unit type produced by this chain logic.
    type WorkUnit<D: Domain<Chain = Self, Entity = Self::Entity, EntityDelta = Self::Delta, ChainSpecificError = Self::ChainSpecificError, Genesis = Self::Genesis>>: WorkUnit<D>;

    /// Initialize the chain logic with configuration and state.
    fn initialize<D: Domain>(
        config: Self::Config,
        state: &D::State,
        genesis: Self::Genesis,
    ) -> Result<Self, ChainError<Self::ChainSpecificError>>;

    /// Check if the chain logic can receive a new block.
    ///
    /// Returns false if there is pending work that must be processed
    /// before new blocks can be received.
    fn can_receive_block(&self) -> bool;

    /// Receive a raw block for processing.
    ///
    /// The block is queued for processing. Call `pop_work()` to get
    /// work units that should be executed.
    fn receive_block(
        &mut self,
        raw: RawBlock,
    ) -> Result<BlockSlot, ChainError<Self::ChainSpecificError>>;

    /// Pop the next work unit to execute.
    ///
    /// Returns the next work unit to execute, or `None` if no work is
    /// currently ready.
    ///
    /// The returned work unit should be executed using `executor::execute_work_unit()`.
    fn pop_work<D>(&mut self, domain: &D) -> Option<Self::WorkUnit<D>>
    where
        D: Domain<
            Chain = Self,
            Entity = Self::Entity,
            EntityDelta = Self::Delta,
            ChainSpecificError = Self::ChainSpecificError,
            Genesis = Self::Genesis,
        >;

    /// Compute undo data for a block during rollback.
    ///
    /// Given the raw block CBOR and the resolved inputs from the WAL,
    /// returns the UTxO delta, index delta, and transaction hashes needed
    /// to reverse the block's effects.
    fn compute_undo(
        block: &Cbor,
        inputs: &HashMap<TxoRef, Arc<TaggedPayload>>,
        point: ChainPoint,
    ) -> Result<UndoBlockData, ChainError<Self::ChainSpecificError>>;

    /// Compute catch-up data from a WAL entry for recovery.
    ///
    /// Given the raw block CBOR and the resolved inputs stored in the WAL,
    /// computes the UTxO delta, index delta, and transaction hashes needed
    /// to replay the block's effects. Used during bootstrap to catch up
    /// stores that are behind the state store.
    fn compute_catchup(
        block: &Cbor,
        inputs: &HashMap<TxoRef, Arc<TaggedPayload>>,
        point: ChainPoint,
    ) -> Result<CatchUpBlockData, ChainError<Self::ChainSpecificError>>;

    // TODO: remove from the interface - this is Cardano-specific
    fn decode_utxo(
        &self,
        utxo: Arc<TaggedPayload>,
    ) -> Result<Self::Utxo, ChainError<Self::ChainSpecificError>>;

    // TODO: remove from the interface - this is Cardano-specific
    fn mutable_slots(
        domain: &impl Domain<Genesis = Self::Genesis>,
    ) -> Result<BlockSlot, ChainError<Self::ChainSpecificError>>;

    // TODO: remove from the interface - this is Cardano-specific
    fn last_immutable_slot(
        domain: &impl Domain<Genesis = Self::Genesis>,
        tip: BlockSlot,
    ) -> Result<BlockSlot, ChainError<Self::ChainSpecificError>> {
        Ok(tip.saturating_sub(Self::mutable_slots(domain)?))
    }

    fn tx_produced_utxos(
        era_body: &TaggedPayload,
    ) -> Result<Vec<(TxoRef, TaggedPayload)>, Self::ChainSpecificError>;
    fn tx_consumed_ref(era_body: &TaggedPayload) -> Result<Vec<TxoRef>, Self::ChainSpecificError>;

    fn find_tx_in_block(
        block: &[u8],
        tx_hash: &[u8],
    ) -> Result<Option<(TaggedPayload, TxOrder)>, Self::ChainSpecificError>;

    // Validate a transaction against the current ledger state.
    fn validate_tx<D: Domain<ChainSpecificError = Self::ChainSpecificError>>(
        &self,
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
        tip: Option<ChainPoint>,
        genesis: &Self::Genesis,
    ) -> Result<mempool::MempoolTx, ChainError<Self::ChainSpecificError>>;

    /// Evaluate a transaction's scripts and return execution unit reports.
    type EvalReport: Send + Sync;

    fn eval_tx<D: Domain<ChainSpecificError = Self::ChainSpecificError>>(
        cbor: &[u8],
        utxos: &MempoolAwareUtxoStore<D>,
    ) -> Result<Self::EvalReport, ChainError<Self::ChainSpecificError>>;
}

#[derive(Debug, Error)]
pub enum DomainError<E: std::error::Error + Send + Sync + 'static> {
    #[error("wal error: {0}")]
    WalError(#[from] WalError),

    #[error("chain error: {0}")]
    ChainError(#[from] ChainError<E>),

    #[error("state error: {0}")]
    StateError(#[from] StateError),

    #[error("archive error: {0}")]
    ArchiveError(#[from] ArchiveError<E>),

    #[error("index error: {0}")]
    IndexError(#[from] IndexError),

    #[error("mempool error: {0}")]
    MempoolError(#[from] MempoolError),

    #[error("inconsistent state")]
    InconsistentState {
        wal: Option<ChainPoint>,
        state: Option<ChainPoint>,
    },

    #[error("{0}")]
    Internal(String),

    #[error("wal is empty")]
    WalIsEmpty,

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
    type Genesis: Genesis;
    type ChainSpecificError: std::error::Error + Send + Sync;

    type Chain: ChainLogic<
        Delta = Self::EntityDelta,
        Entity = Self::Entity,
        WorkUnit<Self> = Self::WorkUnit,
        Genesis = Self::Genesis,
        ChainSpecificError = Self::ChainSpecificError,
    >;

    /// The concrete work unit type for this domain.
    /// This should be an enum containing all possible work unit variants.
    type WorkUnit: WorkUnit<Self>;

    type Wal: WalStore<Delta = Self::EntityDelta>;
    type State: StateStore;
    type Archive: ArchiveStore<ChainSpecificError = Self::ChainSpecificError>;
    type Indexes: IndexStore;
    type Mempool: MempoolStore;
    type TipSubscription: TipSubscription;

    fn storage_config(&self) -> &config::StorageConfig;
    fn sync_config(&self) -> &config::SyncConfig;
    fn genesis(&self) -> Arc<Self::Genesis>;

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain>;
    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain>;

    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn indexes(&self) -> &Self::Indexes;
    fn mempool(&self) -> &Self::Mempool;

    fn watch_tip(
        &self,
        from: Option<ChainPoint>,
    ) -> Result<Self::TipSubscription, DomainError<Self::ChainSpecificError>>;
    fn notify_tip(&self, tip: TipEvent);

    const MAX_PRUNE_SLOTS_PER_HOUSEKEEPING: u64 = 10_000;

    fn housekeeping(&self) -> Result<bool, DomainError<Self::ChainSpecificError>> {
        let max_ledger_slots = match self.storage_config().state.max_history() {
            Some(x) => x,
            None => Self::Chain::mutable_slots(self)?,
        };

        info!(max_ledger_slots, "pruning ledger for excess history");

        let mut archive_pruned = true;

        if let Some(max_slots) = self.sync_config().max_history {
            info!(max_slots, "pruning archive for excess history");

            archive_pruned = self
                .archive()
                .prune_history(max_slots, Some(Self::MAX_PRUNE_SLOTS_PER_HOUSEKEEPING))?;
        }

        let mut wal_pruned = true;

        if let Some(max_slots) = self.sync_config().max_rollback {
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
        let mut bytes = [0u8; 32];
        let slot_bytes = (slot as i32).to_le_bytes();
        bytes[..4].copy_from_slice(&slot_bytes);
        BlockHash::new(bytes)
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
