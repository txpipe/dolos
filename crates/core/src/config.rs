use std::{fmt::Display, net::SocketAddr, path::PathBuf};

use pallas::ledger::primitives::Epoch;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{Cbor, Era, TxoRef};

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

#[derive(Serialize, Deserialize, Clone, Default)]
pub enum SyncLimit {
    #[default]
    NoLimit,
    UntilTip,
    MaxBlocks(u64),
}

#[derive(Serialize, Deserialize)]
pub struct SyncConfig {
    pub pull_batch_size: Option<usize>,

    #[serde(default)]
    pub sync_limit: SyncLimit,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            pull_batch_size: Some(100),
            sync_limit: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct SubmitConfig {
    pub prune_height: Option<u64>,
}

#[derive(Serialize, Default, PartialEq, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum StorageVersion {
    V0,
    V1,
    V2,

    #[default]
    V3,
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
                "v2" => Ok(StorageVersion::V2),
                "v3" => Ok(StorageVersion::V3),
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
                Self::V2 => "v2",
                Self::V3 => "v3",
            }
        )
    }
}

/// Storage backend selection for stores that support multiple implementations.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    #[default]
    Redb,
    Fjall,
}

/// WAL store configuration.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct WalStoreConfig {
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
    /// Maximum number of slots to keep in the WAL.
    pub max_history: Option<u64>,
}

/// State store configuration.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct StateStoreConfig {
    /// Backend implementation (default: redb).
    #[serde(default)]
    pub backend: StorageBackend,
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
    /// Maximum number of slots to keep before pruning.
    pub max_history: Option<u64>,
}

/// Archive store configuration.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ArchiveStoreConfig {
    /// Backend implementation (default: redb).
    /// Note: Currently only redb is supported.
    #[serde(default)]
    pub backend: StorageBackend,
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
    /// Maximum number of slots to keep.
    pub max_history: Option<u64>,
}

/// Index store configuration.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct IndexStoreConfig {
    /// Backend implementation (default: redb).
    #[serde(default)]
    pub backend: StorageBackend,
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
}

/// Storage configuration with nested per-store settings.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageConfig {
    pub version: StorageVersion,

    /// Directory where to find storage. If undefined, ephemeral storage will be
    /// used.
    pub path: Option<std::path::PathBuf>,

    /// WAL store configuration.
    #[serde(default)]
    pub wal: WalStoreConfig,

    /// State store configuration.
    #[serde(default)]
    pub state: StateStoreConfig,

    /// Archive store configuration.
    #[serde(default)]
    pub archive: ArchiveStoreConfig,

    /// Index store configuration.
    #[serde(default)]
    pub index: IndexStoreConfig,
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
            wal: WalStoreConfig::default(),
            state: StateStoreConfig::default(),
            archive: ArchiveStoreConfig::default(),
            index: IndexStoreConfig::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct GenesisConfig {
    pub byron_path: PathBuf,
    pub shelley_path: PathBuf,
    pub alonzo_path: PathBuf,
    pub conway_path: PathBuf,
    pub force_protocol: Option<usize>,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        Self {
            byron_path: PathBuf::from("byron.json"),
            shelley_path: PathBuf::from("shelley.json"),
            alonzo_path: PathBuf::from("alonzo.json"),
            conway_path: PathBuf::from("conway.json"),
            force_protocol: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MithrilConfig {
    pub aggregator: String,
    pub genesis_key: String,
    pub ancillary_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SnapshotConfig {
    pub download_url: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OuroborosConfig {
    pub listen_path: PathBuf,
    pub magic: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GrpcConfig {
    pub listen_address: String,
    pub tls_client_ca_root: Option<PathBuf>,
    pub permissive_cors: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MinibfConfig {
    pub listen_address: SocketAddr,
    pub permissive_cors: Option<bool>,
    pub token_registry_url: Option<String>,
    pub url: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrpConfig {
    pub listen_address: SocketAddr,
    pub max_optimize_rounds: u8,
    pub permissive_cors: Option<bool>,
    pub extra_fees: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct ServeConfig {
    pub ouroboros: Option<OuroborosConfig>,
    pub grpc: Option<GrpcConfig>,
    pub minibf: Option<MinibfConfig>,
    pub trp: Option<TrpConfig>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct LoggingConfig {
    #[serde_as(as = "DisplayFromStr")]
    pub max_level: tracing::Level,

    #[serde(default)]
    pub include_tokio: bool,

    #[serde(default)]
    pub include_pallas: bool,

    #[serde(default)]
    pub include_grpc: bool,

    #[serde(default)]
    pub include_trp: bool,

    #[serde(default)]
    pub include_minibf: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_level: tracing::Level::INFO,
            include_tokio: Default::default(),
            include_pallas: Default::default(),
            include_grpc: Default::default(),
            include_trp: Default::default(),
            include_minibf: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TrackConfig {
    pub account_state: bool,
    pub asset_state: bool,
    pub pool_state: bool,
    pub epoch_state: bool,
    pub drep_state: bool,
    pub proposal_logs: bool,
    pub tx_logs: bool,
    pub account_logs: bool,
    pub pool_logs: bool,
    pub epoch_logs: bool,
}

impl Default for TrackConfig {
    fn default() -> Self {
        Self {
            account_state: true,
            asset_state: true,
            pool_state: true,
            epoch_state: true,
            drep_state: true,
            tx_logs: true,
            account_logs: true,
            pool_logs: true,
            epoch_logs: true,
            proposal_logs: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CustomUtxo {
    #[serde(rename = "ref")]
    pub ref_: TxoRef,
    pub era: Option<Era>,
    pub cbor: Cbor,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CardanoConfig {
    #[serde(default)]
    pub track: TrackConfig,

    pub stop_epoch: Option<Epoch>,

    #[serde(default)]
    pub custom_utxos: Vec<CustomUtxo>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChainConfig {
    Cardano(CardanoConfig),
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self::Cardano(CardanoConfig::default())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RelayConfig {
    pub listen_address: String,
    pub magic: u64,
}

#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub backoff_unit_sec: u64,
    pub backoff_factor: u32,
    pub max_backoff_sec: u64,
    pub dismissible: bool,
}

#[derive(Serialize, Deserialize)]
pub struct RootConfig {
    pub upstream: UpstreamConfig,
    pub storage: StorageConfig,
    pub genesis: GenesisConfig,
    pub sync: SyncConfig,
    pub submit: SubmitConfig,
    pub serve: ServeConfig,
    pub relay: Option<RelayConfig>,
    pub retries: Option<RetryConfig>,
    pub mithril: Option<MithrilConfig>,
    pub snapshot: Option<SnapshotConfig>,

    #[serde(default)]
    pub chain: ChainConfig,

    #[serde(default)]
    pub logging: LoggingConfig,
}
