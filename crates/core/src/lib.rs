use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{MultiEraInput, MultiEraOutput, MultiEraTx, MultiEraUpdate},
    network::miniprotocols::Point as PallasPoint,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};
use thiserror::Error;
use tracing::info;

mod mempool;
mod state;
mod wal;

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
pub type BlockEra = pallas::ledger::traverse::Era;
pub type BlockHash = Hash<32>;
pub type BlockHeader = Cbor;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (u16, Cbor);
pub type ChainTip = pallas::network::miniprotocols::chainsync::Tip;
pub type LogSeq = u64;

pub use mempool::*;
pub use state::*;
pub use wal::*;

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

// TODO: remove legacy
// #[derive(Debug, Eq, PartialEq, Hash)]
// pub struct ChainPoint(pub BlockSlot, pub BlockHash);

#[derive(Debug, Error)]
pub enum BrokenInvariant {
    #[error("missing utxo {0:?}")]
    MissingUtxo(TxoRef),
}

pub type UtxoMap = HashMap<TxoRef, EraCbor>;

pub type UtxoSet = HashSet<TxoRef>;

pub struct LedgerQuery {
    pub required_inputs: Vec<TxoRef>,
    pub extra_inputs: HashMap<TxoRef, EraCbor>,
}

/// A slice of the ledger relevant for a specific task
///
/// A ledger slice represents a partial view of the ledger which is optimized
/// for a particular task, such tx validation. In essence, it is a subset of all
/// the UTxO which are being consumed or referenced by a block or tx.
#[derive(Clone)]
pub struct LedgerSlice {
    pub resolved_inputs: HashMap<TxoRef, EraCbor>,
}

#[derive(Default, Debug, Clone)]
pub struct LedgerDelta {
    pub new_position: Option<ChainPoint>,
    pub undone_position: Option<ChainPoint>,
    pub produced_utxo: HashMap<TxoRef, EraCbor>,
    pub consumed_utxo: HashMap<TxoRef, EraCbor>,
    pub recovered_stxi: HashMap<TxoRef, EraCbor>,
    pub undone_utxo: HashMap<TxoRef, EraCbor>,
    pub seen_txs: HashSet<TxHash>,
    pub unseen_txs: HashSet<TxHash>,
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

impl PartialEq for RawBlock {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && self.hash == other.hash
    }
}

impl PartialOrd for RawBlock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.slot.partial_cmp(&other.slot)
    }
}

#[derive(Debug, Clone)]
pub enum PullEvent {
    RollForward(RawBlock),
    Rollback(ChainPoint),
}

#[derive(Debug, Clone)]
pub enum RollEvent {
    TipChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub enum ChainPoint {
    Origin,
    Specific(BlockSlot, BlockHash),
}

impl ChainPoint {
    pub fn slot(&self) -> BlockSlot {
        match self {
            Self::Origin => 0,
            Self::Specific(slot, _) => *slot,
        }
    }
}

impl PartialEq for ChainPoint {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Specific(l0, l1), Self::Specific(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Origin, Self::Origin) => true,
            _ => false,
        }
    }
}

impl Ord for ChainPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Origin, Self::Origin) => std::cmp::Ordering::Equal,
            (Self::Origin, Self::Specific(_, _)) => std::cmp::Ordering::Less,
            (Self::Specific(_, _), Self::Origin) => std::cmp::Ordering::Greater,
            (Self::Specific(x, x_hash), Self::Specific(y, y_hash)) => match x.cmp(y) {
                std::cmp::Ordering::Equal => x_hash.cmp(y_hash),
                x => x,
            },
        }
    }
}

impl PartialOrd for ChainPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<PallasPoint> for ChainPoint {
    fn from(value: PallasPoint) -> Self {
        match value {
            PallasPoint::Origin => ChainPoint::Origin,
            PallasPoint::Specific(s, h) => ChainPoint::Specific(s, h.as_slice().into()),
        }
    }
}

impl From<ChainPoint> for PallasPoint {
    fn from(value: ChainPoint) -> Self {
        match value {
            ChainPoint::Origin => PallasPoint::Origin,
            ChainPoint::Specific(s, h) => PallasPoint::Specific(s, h.to_vec()),
        }
    }
}

impl From<&RawBlock> for ChainPoint {
    fn from(value: &RawBlock) -> Self {
        let RawBlock { slot, hash, .. } = value;
        ChainPoint::Specific(*slot, *hash)
    }
}

impl From<&LogValue> for ChainPoint {
    fn from(value: &LogValue) -> Self {
        match value {
            LogValue::Apply(x) => ChainPoint::from(x),
            LogValue::Undo(x) => ChainPoint::from(x),
            LogValue::Mark(x) => x.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogValue {
    Apply(RawBlock),
    Undo(RawBlock),
    Mark(ChainPoint),
}

impl LogValue {
    pub fn slot(&self) -> u64 {
        match self {
            LogValue::Apply(x) => x.slot,
            LogValue::Undo(x) => x.slot,
            LogValue::Mark(x) => x.slot(),
        }
    }
}

impl PartialEq for LogValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Apply(l0), Self::Apply(r0)) => l0 == r0,
            (Self::Undo(l0), Self::Undo(r0)) => l0 == r0,
            (Self::Mark(l0), Self::Mark(r0)) => l0 == r0,
            _ => false,
        }
    }
}

pub type LogEntry = (LogSeq, LogValue);

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

    // TODO: refactor this to avoid Pallas dependency
    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    // TODO: refactor this to avoid Pallas dependency
    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),
}

pub trait StateStore: Sized + Clone + Send + Sync + 'static {
    fn start(&self) -> Result<Option<ChainPoint>, StateError>;

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

    fn upgrade(self) -> Result<Self, StateError>;

    fn copy(&self, target: &Self) -> Result<(), StateError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, StateError>;
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

pub trait ArchiveStore: Clone + Send + Sync + 'static {
    type BlockIter<'a>: Iterator<Item = (BlockSlot, BlockBody)> + DoubleEndedIterator + 'a;
    type SparseBlockIter: Iterator<Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>>
        + DoubleEndedIterator;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError>;

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError>;

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_asset(&self, asset: &[u8]) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError>;

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError>;

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError>;

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ArchiveError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError>;
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

    #[error("state error: {0}")]
    StateError(#[from] StateError),

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

    fn receive_raw(&self, cbor: &[u8]) -> Result<TxHash, MempoolError>;

    fn evaluate_raw(&self, cbor: &[u8]) -> Result<EvalReport, MempoolError>;

    fn apply(&self, deltas: &[LedgerDelta]);
    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage;
    fn subscribe(&self) -> Self::Stream;
}

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("decoding error")]
    DecodingError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    State3Error(#[from] State3Error),
}

pub trait ChainLogic {
    type Block<'a>: Sized;

    fn decode_block<'a>(block: &'a [u8]) -> Result<Self::Block<'a>, ChainError>;

    fn mutable_slots(domain: &impl Domain) -> BlockSlot;

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

    fn ledger_query_for_block<'a>(
        block: &Self::Block<'a>,
        unapplied_deltas: &[LedgerDelta],
    ) -> Result<LedgerQuery, ChainError>;

    fn compute_origin_delta(&self, genesis: &Genesis) -> Result<LedgerDelta, ChainError>;

    fn compute_apply_delta<'a>(
        ledger: LedgerSlice,
        block: &Self::Block<'a>,
    ) -> Result<LedgerDelta, ChainError>;

    fn compute_undo_delta<'a>(
        ledger: LedgerSlice,
        block: &Self::Block<'a>,
    ) -> Result<LedgerDelta, ChainError>;

    fn load_slice_for_block<'a>(
        state: &impl StateStore,
        block: &Self::Block<'a>,
        unapplied_deltas: &[LedgerDelta],
    ) -> Result<LedgerSlice, DomainError> {
        let query = Self::ledger_query_for_block(block, unapplied_deltas)?;

        let required_utxos = StateStore::get_utxos(state, query.required_inputs)?;

        let out = LedgerSlice {
            resolved_inputs: [required_utxos, query.extra_inputs]
                .into_iter()
                .flatten()
                .collect(),
        };

        Ok(out)
    }

    fn compute_apply_delta3<'a>(
        &self,
        state: &impl State3Store,
        block: &Self::Block<'a>,
    ) -> Result<StateDelta, ChainError>;
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("wal error: {0}")]
    WalError(#[from] WalError),

    #[error("chain error: {0}")]
    ChainError(#[from] ChainError),

    #[error("state error: {0}")]
    StateError(#[from] StateError),

    #[error("state3 error: {0}")]
    State3Error(#[from] State3Error),

    #[error("archive error: {0}")]
    ArchiveError(#[from] ArchiveError),

    #[error("mempool error: {0}")]
    MempoolError(#[from] MempoolError),
}

pub trait Domain: Send + Sync + Clone + 'static {
    type Wal: WalStore;
    type State: StateStore;
    type Archive: ArchiveStore;
    type Mempool: MempoolStore;
    type Chain: ChainLogic;

    type State3: State3Store;

    fn storage_config(&self) -> &StorageConfig;
    fn genesis(&self) -> &Genesis;

    fn chain(&self) -> &Self::Chain;
    fn wal(&self) -> &Self::Wal;
    fn state(&self) -> &Self::State;
    fn archive(&self) -> &Self::Archive;
    fn mempool(&self) -> &Self::Mempool;

    fn state3(&self) -> &Self::State3;

    fn apply_origin(&self) -> Result<(), DomainError> {
        let deltas = vec![self.chain().compute_origin_delta(self.genesis())?];

        self.state().apply(&deltas)?;
        self.archive().apply(&deltas)?;

        Ok(())
    }

    fn compute_apply_deltas(&self, blocks: &[RawBlock]) -> Result<Vec<LedgerDelta>, DomainError> {
        let mut deltas = Vec::with_capacity(blocks.len());

        for block in blocks {
            let block = Self::Chain::decode_block(&block.body)?;
            let slice = Self::Chain::load_slice_for_block(self.state(), &block, &deltas)?;
            let delta = Self::Chain::compute_apply_delta(slice, &block)?;
            deltas.push(delta);
        }

        Ok(deltas)
    }

    fn compute_apply_deltas3(&self, blocks: &[RawBlock]) -> Result<Vec<StateDelta>, DomainError> {
        let mut deltas = Vec::with_capacity(blocks.len());

        for block in blocks {
            let block = Self::Chain::decode_block(&block.body)?;
            let delta = self.chain().compute_apply_delta3(self.state3(), &block)?;
            deltas.push(delta);
        }

        Ok(deltas)
    }

    fn apply_blocks(&self, blocks: &[RawBlock]) -> Result<(), DomainError> {
        let deltas = self.compute_apply_deltas(blocks)?;

        self.state().apply(&deltas)?;
        self.archive().apply(&deltas)?;
        self.mempool().apply(&deltas);

        for delta in self.compute_apply_deltas3(blocks)? {
            self.state3().apply_delta(delta)?;
        }

        Ok(())
    }

    fn compute_undo_deltas(&self, blocks: &[RawBlock]) -> Result<Vec<LedgerDelta>, DomainError> {
        let mut deltas = Vec::with_capacity(blocks.len());

        for block in blocks {
            let block = Self::Chain::decode_block(&block.body)?;
            let slice = Self::Chain::load_slice_for_block(self.state(), &block, &deltas)?;
            let delta = Self::Chain::compute_undo_delta(slice, &block)?;
            deltas.push(delta);
        }

        Ok(deltas)
    }

    fn undo_blocks(&self, blocks: &[RawBlock]) -> Result<(), DomainError> {
        let deltas = self.compute_undo_deltas(blocks)?;

        self.state().apply(&deltas)?;
        self.archive().apply(&deltas)?;
        self.mempool().apply(&deltas);

        Ok(())
    }

    const MAX_PRUNE_SLOTS_PER_HOUSEKEEPING: u64 = 1_000_000;

    fn housekeeping(&self) -> Result<bool, DomainError> {
        let max_ledger_slots = self
            .storage_config()
            .max_ledger_history
            .unwrap_or(Self::Chain::mutable_slots(self));
        info!(max_ledger_slots, "pruning ledger for excess history");
        let state_pruned = self.state().prune_history(
            max_ledger_slots,
            Some(Self::MAX_PRUNE_SLOTS_PER_HOUSEKEEPING),
        )?;

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

        Ok(state_pruned && archive_pruned && wal_pruned)
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
