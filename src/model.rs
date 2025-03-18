use std::fmt::Display;

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

    pub path: std::path::PathBuf,

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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            version: Default::default(),
            path: std::path::PathBuf::from("data"),
            wal_cache: None,
            ledger_cache: None,
            chain_cache: None,
            max_wal_history: None,
            max_ledger_history: None,
            max_chain_history: None,
        }
    }
}
