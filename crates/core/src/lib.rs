use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{MultiEraInput, MultiEraOutput, MultiEraTx, MultiEraUpdate},
    network::miniprotocols::Point,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Era = u16;
pub type TxoIdx = u32;
pub type TxOrder = usize;
pub type BlockSlot = u64;
pub type BlockHeight = u64;
pub type BlockBody = Vec<u8>;
pub type BlockEra = pallas::ledger::traverse::Era;
pub type BlockHash = Hash<32>;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (u16, Vec<u8>);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct EraCbor(pub Era, pub Vec<u8>);

impl From<(Era, Vec<u8>)> for EraCbor {
    fn from(value: (Era, Vec<u8>)) -> Self {
        Self(value.0, value.1)
    }
}

impl From<EraCbor> for (Era, Vec<u8>) {
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

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChainPoint(pub BlockSlot, pub BlockHash);

#[derive(Debug, Error)]
pub enum BrokenInvariant {
    #[error("missing utxo {0:?}")]
    MissingUtxo(TxoRef),
}

pub type UtxoMap = HashMap<TxoRef, EraCbor>;

pub type UtxoSet = HashSet<TxoRef>;

/// A slice of the ledger relevant for a specific task
///
/// A ledger slice represents a partial view of the ledger which is optimized
/// for a particular task, such tx validation. In essence, it is a subset of all
/// the UTxO which are being consumed or referenced by a block or tx.
#[derive(Clone)]
pub struct LedgerSlice {
    pub resolved_inputs: HashMap<TxoRef, EraCbor>,
}

#[derive(Default, Debug)]
pub struct LedgerDelta {
    pub new_position: Option<ChainPoint>,
    pub undone_position: Option<ChainPoint>,
    pub produced_utxo: HashMap<TxoRef, EraCbor>,
    pub consumed_utxo: HashMap<TxoRef, EraCbor>,
    pub recovered_stxi: HashMap<TxoRef, EraCbor>,
    pub undone_utxo: HashMap<TxoRef, EraCbor>,
    pub new_pparams: Vec<EraCbor>,
    pub new_block: BlockBody,
    pub undone_block: BlockBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBlock {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    pub era: BlockEra,
    pub body: BlockBody,
}

#[derive(Debug, Clone)]
pub enum PullEvent {
    RollForward(RawBlock),
    Rollback(Point),
}

#[derive(Debug, Clone)]
pub enum RollEvent {
    TipChanged,
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

#[derive(Serialize, Default, PartialEq)]
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

#[derive(Serialize, Deserialize)]
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
    #[error("Failed to bind listener")]
    BindError(std::io::Error),

    #[error("Failed to shutdown")]
    ShutdownError(std::io::Error),
}

pub struct Genesis {
    pub byron: pallas::interop::hardano::configs::byron::GenesisFile,
    pub shelley: pallas::interop::hardano::configs::shelley::GenesisFile,
    pub alonzo: pallas::interop::hardano::configs::alonzo::GenesisFile,
    pub conway: pallas::interop::hardano::configs::conway::GenesisFile,
    pub force_protocol: Option<usize>,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    InternalError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),
}

pub trait StateStore: Sized + pallas::interop::utxorpc::LedgerContext {
    fn cursor(&self) -> Result<Option<ChainPoint>, StateError>;

    fn is_empty(&self) -> Result<bool, StateError>;

    fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, StateError>;

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError>;

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError>;

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), StateError>;

    fn finalize(&self, until: BlockSlot) -> Result<(), StateError>;

    fn upgrade(self) -> Result<Self, StateError>;

    fn copy(&self, target: &Self) -> Result<(), StateError>;
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    InternalError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[from] pallas::ledger::traverse::Error),
}

pub trait ArchiveStore {
    type BlockIter<'a>: Iterator<Item = (BlockSlot, BlockBody)> + DoubleEndedIterator + 'a;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<Vec<u8>>, ArchiveError>;

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError>;

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError>;

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ArchiveError>;

    fn housekeeping(&mut self) -> Result<(), ArchiveError>;

    fn finalize(&self, until: BlockSlot) -> Result<(), ArchiveError>;
}

#[derive(Debug, Error)]
pub enum MempoolError {
    #[error("traverse error: {0}")]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error("decode error: {0}")]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("tx validation failed during phase-1: {0}")]
    Phase1Error(#[from] pallas::ledger::validate::utils::ValidationError),

    #[cfg(feature = "phase2")]
    #[error("tx evaluation failed during phase-2: {0}")]
    Phase2Error(#[from] pallas::ledger::validate::phase2::error::Error),

    #[error("state error: {0}")]
    StateError(#[from] StateError),

    #[error("plutus not supported")]
    PlutusNotSupported,

    #[error("invalid tx: {0}")]
    InvalidTx(String),
}

pub trait MempoolStore {
    fn receive_raw(&self, cbor: &[u8]) -> Result<TxHash, MempoolError>;
}

pub trait Domain: Send + Sync + Clone + 'static {
    type State: StateStore;
    type Archive: ArchiveStore;
    type Mempool: MempoolStore;

    fn genesis(&self) -> &Genesis;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn mempool(&self) -> &Self::Mempool;
}
