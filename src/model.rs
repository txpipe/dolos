use pallas::{crypto::hash::Hash, network::miniprotocols::Point};
use serde::{Deserialize, Serialize};

pub type BlockSlot = u64;
pub type BlockHeight = u64;
pub type BlockBody = Vec<u8>;
pub type BlockEra = pallas::ledger::traverse::Era;
pub type BlockHash = Hash<32>;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (u16, Vec<u8>);

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
pub struct UpstreamConfig {
    pub peer_address: String,
    pub network_magic: u64,

    #[serde(default)]
    pub is_testnet: bool,
}

#[derive(Serialize, Deserialize, Default)]
pub struct SubmitConfig {
    pub prune_height: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct StorageConfig {
    pub path: std::path::PathBuf,

    /// Size (in Mb) of memory allocated for WAL caching
    pub wal_cache: Option<usize>,

    /// Size (in Mb) of memory allocated for ledger caching
    pub ledger_cache: Option<usize>,

    /// Maximum number of slots (not blocks) to keep in the WAL
    pub max_wal_history: Option<u64>,

    /// Maximum number of slots to keep in the ledger before pruning
    pub max_ledger_history: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: std::path::PathBuf::from("data"),
            wal_cache: None,
            ledger_cache: None,
            max_wal_history: None,
            max_ledger_history: None,
        }
    }
}
