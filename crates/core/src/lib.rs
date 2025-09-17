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
    ledger::traverse::{MultiEraInput, MultiEraOutput, MultiEraTx, MultiEraUpdate},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use tracing::info;

pub mod archive;
pub mod batch;
pub mod crawl;
pub mod init;
pub mod mempool;
pub mod point;
pub mod state;
pub mod utxoset;
pub mod wal;

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
pub use mempool::*;
pub use point::*;
pub use state::*;
pub use wal::*;

use crate::batch::WorkBlock;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct EraCbor(pub Era, pub Cbor);

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
    pub new_position: Option<ChainPoint>,
    pub undone_position: Option<ChainPoint>,
    pub produced_utxo: HashMap<TxoRef, Arc<EraCbor>>,
    pub consumed_utxo: HashMap<TxoRef, Arc<EraCbor>>,
    pub recovered_stxi: HashMap<TxoRef, Arc<EraCbor>>,
    pub undone_utxo: HashMap<TxoRef, Arc<EraCbor>>,
    pub seen_txs: HashSet<TxHash>,
    pub unseen_txs: HashSet<TxHash>,
    pub new_pparams: Vec<Arc<EraCbor>>,
    pub new_block: BlockBody,
    pub undone_block: BlockBody,
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
}

impl<D> LogValue<D>
where
    D: EntityDelta,
{
    pub fn origin() -> Self {
        Self {
            block: vec![],
            delta: vec![],
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

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpstreamConfig {
    Peer(PeerConfig),
    Emulator(EmulatorConfig),
}

impl UpstreamConfig {
    pub fn network_magic(&self) -> Option<u64> {
        match self {
            Self::Peer(peer) => Some(peer.network_magic),
            Self::Emulator(_) => None,
        }
    }

    pub fn peer_address(&self) -> Option<&str> {
        match self {
            Self::Peer(peer) => Some(&peer.peer_address),
            Self::Emulator(_) => None,
        }
    }

    pub fn is_emulator(&self) -> bool {
        matches!(self, Self::Emulator(_))
    }

    pub fn as_peer_mut(&mut self) -> Option<&mut PeerConfig> {
        match self {
            Self::Peer(peer) => Some(peer),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct EmulatorConfig {
    pub block_production_interval: u64,
}

#[derive(Serialize, Deserialize)]
pub struct PeerConfig {
    pub peer_address: String,
    pub network_magic: u64,

    #[serde(default)]
    pub is_testnet: bool,
}

#[derive(Serialize, Deserialize, Default)]
pub struct SubmitConfig {
    pub prune_height: Option<u64>,
}

#[derive(Serialize, Default, PartialEq, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum StorageVersion {
    #[default]
    V0,
    V1,
}

impl<'de> Deserialize<'de> for StorageVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr: Option<String> = Deserialize::deserialize(deserializer)?;
        match repr {
            Some(version) => match version.as_str() {
                "v0" => Ok(StorageVersion::V0),
                "v1" => Ok(StorageVersion::V1),
                _ => Err(<D::Error as serde::de::Error>::custom("Invalid version")),
            },
            None => Ok(StorageVersion::V0),
        }
    }
}

impl Display for StorageVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::V0 => "v0",
                Self::V1 => "v1",
            }
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageConfig {
    pub version: StorageVersion,

    /// Directory where to find storage. If undefined, ephemeral storage will be
    /// used.
    pub path: Option<std::path::PathBuf>,

    /// Size (in Mb) of memory allocated for WAL caching
    pub wal_cache: Option<usize>,

    /// Size (in Mb) of memory allocated for ledger caching
    pub ledger_cache: Option<usize>,

    /// Size (in Mb) of memory allocated for chain caching
    pub chain_cache: Option<usize>,

    /// Maximum number of slots (not blocks) to keep in the WAL
    pub max_wal_history: Option<u64>,

    /// Maximum number of slots to keep in the ledger before pruning
    pub max_ledger_history: Option<u64>,

    /// Maximum number of slots (not blocks) to keep in Chain
    pub max_chain_history: Option<u64>,
}

impl StorageConfig {
    pub fn is_ephemeral(&self) -> bool {
        self.path.is_none()
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            version: Default::default(),
            path: Some(std::path::PathBuf::from("data")),
            wal_cache: None,
            ledger_cache: None,
            chain_cache: None,
            max_wal_history: None,
            max_ledger_history: None,
            max_chain_history: None,
        }
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

pub type Phase2Log = Vec<String>;

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("internal error: {0}")]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("traverse error: {0}")]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error("decode error: {0}")]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("tx validation failed during phase-1: {0}")]
    Phase1Error(#[from] pallas::ledger::validate::utils::ValidationError),

    #[error("tx evaluation failed during phase-2: {0}")]
    Phase2Error(#[from] pallas::ledger::validate::phase2::error::Error),

    #[error("phase-2 script yielded an error")]
    Phase2ExplicitError(Phase2Log),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error(transparent)]
    ChainError(#[from] ChainError),

    #[error("plutus not supported")]
    PlutusNotSupported,

    #[error("invalid tx: {0}")]
    InvalidTx(String),
}

pub trait MempoolStore: Clone + Send + Sync + 'static {
    type Stream: futures_core::Stream<Item = Result<MempoolEvent, MempoolError>>
        + Unpin
        + Send
        + Sync;

    fn receive_raw<D: Domain>(&self, domain: &D, cbor: &[u8]) -> Result<TxHash, MempoolError>;

    fn evaluate_raw<D: Domain>(&self, domain: &D, cbor: &[u8]) -> Result<EvalReport, MempoolError>;

    fn apply(&self, deltas: &[UtxoSetDelta]);
    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage;
    fn subscribe(&self) -> Self::Stream;
}

pub trait Block: Sized + Send + Sync {
    fn depends_on(&self, loaded: &mut RawUtxoMap) -> Vec<TxoRef>;
    fn slot(&self) -> BlockSlot;
    fn hash(&self) -> BlockHash;
}

#[derive(Debug, Error)]
pub enum ChainError {
    #[error(transparent)]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("decoding error")]
    DecodingError(#[from] pallas::ledger::traverse::Error),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error(transparent)]
    StateError(#[from] StateError),

    #[error("pparams not found")]
    PParamsNotFound,

    #[error("era not found")]
    EraNotFound,

    #[error("forced stop epoch reached")]
    StopEpochReached,
}

pub trait ChainLogic: Sized + Send + Sync {
    type Block: Block + Send + Sync;
    type Entity: Entity;
    type Utxo: Sized + Send + Sync;
    type Delta: EntityDelta<Entity = Self::Entity>;

    fn initialize<D: Domain>(&self, _domain: &D) -> Result<(), ChainError> {
        Ok(())
    }

    fn bootstrap<D: Domain>(&self, domain: &D) -> Result<(), ChainError>;

    fn decode_block(&self, block: Arc<BlockBody>) -> Result<Self::Block, ChainError>;

    fn decode_utxo(&self, utxo: Arc<EraCbor>) -> Result<Self::Utxo, ChainError>;

    fn mutable_slots(domain: &impl Domain) -> BlockSlot;

    fn execute_sweep<D: Domain>(&self, domain: &D, at: BlockSlot) -> Result<(), ChainError>;

    fn next_sweep<D: Domain>(&self, domain: &D, after: BlockSlot) -> Result<BlockSlot, ChainError>;

    /// Computes the last immutable slot
    ///
    /// Takes the latest known tip, reads the relevant genesis config values and
    /// uses the security window guarantee formula from consensus to calculate
    /// the latest slot that can be considered immutable. This is used
    /// mainly to define which slots can be finalized in the ledger store
    /// (aka: compaction).
    fn last_immutable_slot(domain: &impl Domain, tip: BlockSlot) -> BlockSlot {
        tip.saturating_sub(Self::mutable_slots(domain))
    }

    fn compute_block_utxo_delta(
        &self,
        block: &Self::Block,
        deps: &RawUtxoMap,
    ) -> Result<UtxoSetDelta, ChainError>;

    fn compute_delta(
        &self,
        block: &mut WorkBlock<Self>,
        deps: &HashMap<TxoRef, Self::Utxo>,
    ) -> Result<(), ChainError>;
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

    #[error("mempool error: {0}")]
    MempoolError(#[from] MempoolError),

    #[error("inconsistent state: {0}")]
    InconsistentState(String),

    #[error("wal is empty")]
    WalIsEmpty,

    #[error("wal is behind state: {0}")]
    WalIsBehindState(BlockSlot, BlockSlot),
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

pub trait Domain: Send + Sync + Clone + 'static {
    type Entity: Entity;
    type EntityDelta: EntityDelta<Entity = Self::Entity> + std::fmt::Debug;

    type Wal: WalStore<Delta = Self::EntityDelta>;
    type State: StateStore;
    type Archive: ArchiveStore;
    type Mempool: MempoolStore;
    type Chain: ChainLogic<Delta = Self::EntityDelta>;
    type TipSubscription: TipSubscription;

    fn storage_config(&self) -> &StorageConfig;
    fn genesis(&self) -> &Genesis;

    fn chain(&self) -> &Self::Chain;
    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn mempool(&self) -> &Self::Mempool;

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError>;
    fn notify_tip(&self, tip: TipEvent);

    const MAX_PRUNE_SLOTS_PER_HOUSEKEEPING: u64 = 10_000;

    fn housekeeping(&self) -> Result<bool, DomainError> {
        let max_ledger_slots = self
            .storage_config()
            .max_ledger_history
            .unwrap_or(Self::Chain::mutable_slots(self));

        info!(max_ledger_slots, "pruning ledger for excess history");

        let mut archive_pruned = true;

        if let Some(max_slots) = self.storage_config().max_chain_history {
            info!(max_slots, "pruning archive for excess history");

            archive_pruned = self
                .archive()
                .prune_history(max_slots, Some(Self::MAX_PRUNE_SLOTS_PER_HOUSEKEEPING))?;
        }

        let mut wal_pruned = true;

        if let Some(max_slots) = self.storage_config().max_wal_history {
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
