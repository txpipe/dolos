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
}

#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub enum SyncLimit {
    #[default]
    NoLimit,
    UntilTip,
    MaxBlocks(u64),
}

impl SyncLimit {
    pub fn is_default(&self) -> bool {
        matches!(self, Self::NoLimit)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SyncConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pull_batch_size: Option<usize>,

    #[serde(default)]
    pub max_history: Option<u64>,

    #[serde(default)]
    pub max_rollback: Option<u64>,

    #[serde(default, skip_serializing_if = "SyncLimit::is_default")]
    pub sync_limit: SyncLimit,
}

impl SyncConfig {
    pub fn with_pull_batch_size(mut self, pull_batch_size: usize) -> Self {
        self.pull_batch_size = Some(pull_batch_size);
        self
    }

    pub fn pull_batch_size(&self) -> usize {
        self.pull_batch_size.unwrap_or(default_pull_batch_size())
    }

    pub fn is_default(&self) -> bool {
        self.pull_batch_size.is_none()
            && self.max_history.is_none()
            && self.max_rollback.is_none()
            && self.sync_limit.is_default()
    }
}

fn default_pull_batch_size() -> usize {
    100
}

#[derive(Serialize, Deserialize, Default)]
pub struct SubmitConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prune_height: Option<u64>,
}

impl SubmitConfig {
    pub fn is_default(&self) -> bool {
        self.prune_height.is_none()
    }
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

// ============================================================================
// WAL Store Configuration
// ============================================================================

/// Configuration for the Redb WAL backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RedbWalConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/wal`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
}

impl RedbWalConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none() && self.cache.is_none()
    }
}

/// WAL store configuration.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum WalStoreConfig {
    Redb(RedbWalConfig),
    /// In-memory backend (ephemeral, data lost on restart).
    #[serde(rename = "in_memory")]
    InMemory,
}

impl Default for WalStoreConfig {
    fn default() -> Self {
        Self::Redb(RedbWalConfig::default())
    }
}

impl WalStoreConfig {
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::Redb(cfg) => cfg.path.as_ref(),
            Self::InMemory => None,
        }
    }

    pub fn is_default(&self) -> bool {
        match self {
            Self::Redb(cfg) => cfg.is_default(),
            Self::InMemory => false,
        }
    }
}

// ============================================================================
// State Store Configuration
// ============================================================================

/// Configuration for the Redb state backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RedbStateConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/state`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
    /// Maximum number of slots to keep before pruning.
    #[serde(default)]
    pub max_history: Option<u64>,
}

impl RedbStateConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none() && self.cache.is_none() && self.max_history.is_none()
    }
}

/// Configuration for the Fjall state backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FjallStateConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/state`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
    /// Maximum number of slots to keep before pruning.
    #[serde(default)]
    pub max_history: Option<u64>,
    /// Maximum journal size in MB.
    #[serde(default)]
    pub max_journal_size: Option<usize>,
    /// Flush journal after each commit.
    #[serde(default)]
    pub flush_on_commit: Option<bool>,
    /// L0 compaction threshold (default: 4, lower = more aggressive).
    #[serde(default)]
    pub l0_threshold: Option<u8>,
    /// Number of background compaction worker threads.
    #[serde(default)]
    pub worker_threads: Option<usize>,
    /// Memtable size in MB before flush (default: 64).
    #[serde(default)]
    pub memtable_size_mb: Option<usize>,
}

impl FjallStateConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none()
            && self.cache.is_none()
            && self.max_history.is_none()
            && self.max_journal_size.is_none()
            && self.flush_on_commit.is_none()
            && self.l0_threshold.is_none()
            && self.worker_threads.is_none()
            && self.memtable_size_mb.is_none()
    }
}

/// State store configuration.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum StateStoreConfig {
    Redb(RedbStateConfig),
    /// In-memory backend (ephemeral, data lost on restart).
    #[serde(rename = "in_memory")]
    InMemory,
    Fjall(FjallStateConfig),
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Fjall(FjallStateConfig::default())
    }
}

impl StateStoreConfig {
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::Redb(cfg) => cfg.path.as_ref(),
            Self::Fjall(cfg) => cfg.path.as_ref(),
            Self::InMemory => None,
        }
    }

    pub fn max_history(&self) -> Option<u64> {
        match self {
            Self::Redb(cfg) => cfg.max_history,
            Self::Fjall(cfg) => cfg.max_history,
            Self::InMemory => None,
        }
    }

    pub fn is_default(&self) -> bool {
        match self {
            Self::Fjall(cfg) => cfg.is_default(),
            Self::Redb(cfg) => cfg.is_default(),
            Self::InMemory => false,
        }
    }
}

// ============================================================================
// Archive Store Configuration
// ============================================================================

/// Configuration for the Redb archive backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RedbArchiveConfig {
    /// Optional path override for the archive directory.
    /// If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/archive`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Optional path override for block segment files.
    /// If not specified, segment files are stored in the archive directory.
    #[serde(default)]
    pub blocks_path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
}

impl RedbArchiveConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none() && self.blocks_path.is_none() && self.cache.is_none()
    }
}

/// Archive store configuration.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum ArchiveStoreConfig {
    Redb(RedbArchiveConfig),
    /// In-memory backend (ephemeral, data lost on restart).
    #[serde(rename = "in_memory")]
    InMemory,
    /// No-op backend that discards all writes and returns empty results.
    NoOp,
}

impl Default for ArchiveStoreConfig {
    fn default() -> Self {
        Self::Redb(RedbArchiveConfig::default())
    }
}

impl ArchiveStoreConfig {
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::Redb(cfg) => cfg.path.as_ref(),
            Self::InMemory | Self::NoOp => None,
        }
    }

    pub fn is_default(&self) -> bool {
        match self {
            Self::Redb(cfg) => cfg.is_default(),
            Self::InMemory | Self::NoOp => false,
        }
    }
}

// ============================================================================
// Index Store Configuration
// ============================================================================

/// Configuration for the Redb index backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RedbIndexConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/index`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
}

impl RedbIndexConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none() && self.cache.is_none()
    }
}

/// Configuration for the Fjall index backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct FjallIndexConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/index`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
    /// Maximum journal size in MB (default: 1024).
    #[serde(default)]
    pub max_journal_size: Option<usize>,
    /// Flush journal after each commit (default: false).
    #[serde(default)]
    pub flush_on_commit: Option<bool>,
    /// L0 compaction threshold (default: 8, lower = more aggressive).
    #[serde(default)]
    pub l0_threshold: Option<u8>,
    /// Number of background compaction worker threads (default: 8).
    #[serde(default)]
    pub worker_threads: Option<usize>,
    /// Memtable size in MB before flush (default: 128).
    #[serde(default)]
    pub memtable_size_mb: Option<usize>,
}

impl FjallIndexConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none()
            && self.cache.is_none()
            && self.max_journal_size.is_none()
            && self.flush_on_commit.is_none()
            && self.l0_threshold.is_none()
            && self.worker_threads.is_none()
            && self.memtable_size_mb.is_none()
    }
}

/// Index store configuration.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum IndexStoreConfig {
    Redb(RedbIndexConfig),
    /// In-memory backend (ephemeral, data lost on restart).
    #[serde(rename = "in_memory")]
    InMemory,
    Fjall(FjallIndexConfig),
    /// No-op backend that discards all writes and returns empty results.
    NoOp,
}

impl Default for IndexStoreConfig {
    fn default() -> Self {
        Self::Fjall(FjallIndexConfig::default())
    }
}

impl IndexStoreConfig {
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::Redb(cfg) => cfg.path.as_ref(),
            Self::Fjall(cfg) => cfg.path.as_ref(),
            Self::InMemory | Self::NoOp => None,
        }
    }

    pub fn is_default(&self) -> bool {
        match self {
            Self::Fjall(cfg) => cfg.is_default(),
            Self::Redb(cfg) => cfg.is_default(),
            Self::InMemory | Self::NoOp => false,
        }
    }
}

// ============================================================================
// Mempool Store Configuration
// ============================================================================

/// Configuration for the Redb mempool backend.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RedbMempoolConfig {
    /// Optional path override. If relative, resolved from storage root.
    /// If not specified, defaults to `<storage.path>/mempool`.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    #[serde(default)]
    pub cache: Option<usize>,
}

impl RedbMempoolConfig {
    pub fn is_default(&self) -> bool {
        self.path.is_none() && self.cache.is_none()
    }
}

/// Mempool store configuration.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum MempoolStoreConfig {
    Redb(RedbMempoolConfig),
    /// In-memory backend (ephemeral, data lost on restart).
    #[serde(rename = "in_memory")]
    #[default]
    InMemory,
}

impl MempoolStoreConfig {
    pub fn is_default(&self) -> bool {
        match self {
            Self::InMemory => true,
            Self::Redb(_) => false,
        }
    }
}

// ============================================================================
// Storage Configuration
// ============================================================================

/// Storage configuration with nested per-store settings.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageConfig {
    pub version: StorageVersion,

    /// Root directory for storage files.
    pub path: std::path::PathBuf,

    /// WAL store configuration.
    #[serde(default, skip_serializing_if = "WalStoreConfig::is_default")]
    pub wal: WalStoreConfig,

    /// State store configuration.
    #[serde(default, skip_serializing_if = "StateStoreConfig::is_default")]
    pub state: StateStoreConfig,

    /// Archive store configuration.
    #[serde(default, skip_serializing_if = "ArchiveStoreConfig::is_default")]
    pub archive: ArchiveStoreConfig,

    /// Index store configuration.
    #[serde(default, skip_serializing_if = "IndexStoreConfig::is_default")]
    pub index: IndexStoreConfig,

    /// Mempool store configuration.
    #[serde(default, skip_serializing_if = "MempoolStoreConfig::is_default")]
    pub mempool: MempoolStoreConfig,
}

impl StorageConfig {
    /// Resolve path with a default subdir for backends that don't specify a custom path.
    fn resolve_store_path_with_default(
        &self,
        config_path: Option<&PathBuf>,
        default_subdir: &str,
    ) -> PathBuf {
        match config_path {
            Some(p) if p.is_absolute() => p.clone(),
            Some(p) => self.path.join(p),
            None => self.path.join(default_subdir),
        }
    }

    /// Get the resolved path for the WAL store.
    /// Returns `None` for in-memory backends.
    pub fn wal_path(&self) -> Option<PathBuf> {
        match &self.wal {
            WalStoreConfig::InMemory => None,
            WalStoreConfig::Redb(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "wal"))
            }
        }
    }

    /// Get the resolved path for the state store.
    /// Returns `None` for in-memory backends.
    pub fn state_path(&self) -> Option<PathBuf> {
        match &self.state {
            StateStoreConfig::InMemory => None,
            StateStoreConfig::Redb(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "state"))
            }
            StateStoreConfig::Fjall(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "state"))
            }
        }
    }

    /// Get the resolved path for the archive store.
    /// Returns `None` for in-memory or no-op backends.
    pub fn archive_path(&self) -> Option<PathBuf> {
        match &self.archive {
            ArchiveStoreConfig::InMemory | ArchiveStoreConfig::NoOp => None,
            ArchiveStoreConfig::Redb(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "archive"))
            }
        }
    }

    /// Get the resolved path for the index store.
    /// Returns `None` for in-memory or no-op backends.
    pub fn index_path(&self) -> Option<PathBuf> {
        match &self.index {
            IndexStoreConfig::InMemory | IndexStoreConfig::NoOp => None,
            IndexStoreConfig::Redb(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "index"))
            }
            IndexStoreConfig::Fjall(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "index"))
            }
        }
    }

    /// Get the resolved path for the mempool store.
    /// Returns `None` for in-memory backends.
    pub fn mempool_path(&self) -> Option<PathBuf> {
        match &self.mempool {
            MempoolStoreConfig::InMemory => None,
            MempoolStoreConfig::Redb(cfg) => {
                Some(self.resolve_store_path_with_default(cfg.path.as_ref(), "mempool"))
            }
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            version: Default::default(),
            path: std::path::PathBuf::from("data"),
            wal: WalStoreConfig::default(),
            state: StateStoreConfig::default(),
            archive: ArchiveStoreConfig::default(),
            index: IndexStoreConfig::default(),
            mempool: MempoolStoreConfig::default(),
        }
    }
}

// ============================================================================
// Other Configuration Types
// ============================================================================

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
}

#[derive(Serialize, Deserialize, Clone)]
pub struct GrpcConfig {
    pub listen_address: String,
    pub tls_client_ca_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,
}

impl GrpcConfig {
    pub fn new(listen_address: String, tls_client_ca_root: Option<PathBuf>) -> Self {
        Self {
            listen_address,
            tls_client_ca_root,
            permissive_cors: None,
        }
    }

    pub fn with_permissive_cors(mut self, permissive_cors: bool) -> Self {
        self.permissive_cors = Some(permissive_cors);
        self
    }

    pub fn permissive_cors(&self) -> bool {
        self.permissive_cors.unwrap_or(true)
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MinibfConfig {
    pub listen_address: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,
    pub token_registry_url: Option<String>,
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_scan_items: Option<u64>,
}

impl MinibfConfig {
    pub fn new(listen_address: SocketAddr) -> Self {
        Self {
            listen_address,
            permissive_cors: None,
            token_registry_url: None,
            url: None,
            max_scan_items: None,
        }
    }

    pub fn with_permissive_cors(mut self, permissive_cors: bool) -> Self {
        self.permissive_cors = Some(permissive_cors);
        self
    }

    pub fn permissive_cors(&self) -> bool {
        self.permissive_cors.unwrap_or(true)
    }

    pub fn with_max_scan_items(mut self, max_scan_items: u64) -> Self {
        self.max_scan_items = Some(max_scan_items);
        self
    }

    pub fn max_scan_items(&self) -> u64 {
        self.max_scan_items.unwrap_or(default_max_scan_items())
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MinikupoConfig {
    pub listen_address: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,
}

impl MinikupoConfig {
    pub fn new(listen_address: SocketAddr) -> Self {
        Self {
            listen_address,
            permissive_cors: None,
        }
    }

    pub fn with_permissive_cors(mut self, permissive_cors: bool) -> Self {
        self.permissive_cors = Some(permissive_cors);
        self
    }

    pub fn permissive_cors(&self) -> bool {
        self.permissive_cors.unwrap_or(true)
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TrpConfig {
    pub listen_address: SocketAddr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_optimize_rounds: Option<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,

    pub extra_fees: Option<u64>,
}

impl TrpConfig {
    pub fn new(listen_address: SocketAddr, extra_fees: Option<u64>) -> Self {
        Self {
            listen_address,
            max_optimize_rounds: None,
            permissive_cors: None,
            extra_fees,
        }
    }

    pub fn with_max_optimize_rounds(mut self, max_optimize_rounds: u8) -> Self {
        self.max_optimize_rounds = Some(max_optimize_rounds);
        self
    }

    pub fn max_optimize_rounds(&self) -> u8 {
        self.max_optimize_rounds
            .unwrap_or(default_max_optimize_rounds())
    }

    pub fn with_permissive_cors(mut self, permissive_cors: bool) -> Self {
        self.permissive_cors = Some(permissive_cors);
        self
    }

    pub fn permissive_cors(&self) -> bool {
        self.permissive_cors.unwrap_or(true)
    }
}

fn default_max_optimize_rounds() -> u8 {
    10
}

fn default_max_scan_items() -> u64 {
    3000
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct ServeConfig {
    pub ouroboros: Option<OuroborosConfig>,
    pub grpc: Option<GrpcConfig>,
    pub minibf: Option<MinibfConfig>,
    pub minikupo: Option<MinikupoConfig>,
    pub trp: Option<TrpConfig>,
}

impl ServeConfig {
    pub fn is_default(&self) -> bool {
        self.ouroboros.is_none()
            && self.grpc.is_none()
            && self.minibf.is_none()
            && self.minikupo.is_none()
            && self.trp.is_none()
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct LoggingConfig {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(
        default = "default_log_level",
        skip_serializing_if = "is_default_log_level"
    )]
    pub max_level: tracing::Level,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_tokio: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_pallas: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_grpc: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_trp: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_minibf: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_minikupo: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_otlp: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_fjall: bool,
}

impl LoggingConfig {
    pub fn is_default(&self) -> bool {
        is_default_log_level(&self.max_level)
            && !self.include_tokio
            && !self.include_pallas
            && !self.include_grpc
            && !self.include_trp
            && !self.include_minibf
            && !self.include_minikupo
            && !self.include_otlp
            && !self.include_fjall
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_level: default_log_level(),
            include_tokio: Default::default(),
            include_pallas: Default::default(),
            include_grpc: Default::default(),
            include_trp: Default::default(),
            include_minibf: Default::default(),
            include_minikupo: Default::default(),
            include_fjall: Default::default(),
            include_otlp: Default::default(),
        }
    }
}

fn default_log_level() -> tracing::Level {
    tracing::Level::INFO
}

fn is_default_log_level(level: &tracing::Level) -> bool {
    *level == default_log_level()
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_service_name() -> String {
    "dolos".to_string()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TelemetryConfig {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enabled: bool,
    #[serde(
        default = "default_otlp_endpoint",
        skip_serializing_if = "is_default_otlp_endpoint"
    )]
    pub otlp_endpoint: String,
    #[serde(
        default = "default_service_name",
        skip_serializing_if = "is_default_service_name"
    )]
    pub service_name: String,
}

impl TelemetryConfig {
    pub fn is_default(&self) -> bool {
        !self.enabled
            && is_default_otlp_endpoint(&self.otlp_endpoint)
            && is_default_service_name(&self.service_name)
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            service_name: default_service_name(),
        }
    }
}

fn is_default_otlp_endpoint(value: &str) -> bool {
    value == default_otlp_endpoint()
}

fn is_default_service_name(value: &str) -> bool {
    value == default_service_name()
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
    pub magic: u64,
    pub is_testnet: bool,
    pub stop_epoch: Option<Epoch>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_utxos: Vec<CustomUtxo>,

    /// Number of shards used to partition the per-account leg of the
    /// epoch-boundary pipeline (see `AccountShardWorkUnit`). Must divide 256
    /// (so shards are whole first-byte prefix buckets) and be >= 1. When
    /// `None`, defaults to `CardanoConfig::DEFAULT_ASHARD_TOTAL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ashard_total: Option<u32>,
}

impl CardanoConfig {
    pub const DEFAULT_ASHARD_TOTAL: u32 = 16;

    pub fn ashard_total(&self) -> u32 {
        self.ashard_total.unwrap_or(Self::DEFAULT_ASHARD_TOTAL)
    }
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

impl ChainConfig {
    pub fn magic(&self) -> u64 {
        match self {
            Self::Cardano(cfg) => cfg.magic,
        }
    }

    pub fn set_magic(&mut self, magic: u64) {
        match self {
            Self::Cardano(cfg) => cfg.magic = magic,
        }
    }

    pub fn is_testnet(&self) -> bool {
        match self {
            Self::Cardano(cfg) => cfg.is_testnet,
        }
    }

    pub fn set_is_testnet(&mut self, is_testnet: bool) {
        match self {
            Self::Cardano(cfg) => cfg.is_testnet = is_testnet,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RelayConfig {
    pub listen_address: String,
}

#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct RetryConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_retries: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backoff_unit_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backoff_factor: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_backoff_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dismissible: Option<bool>,
}

impl RetryConfig {
    pub fn max_retries(&self) -> usize {
        self.max_retries.unwrap_or(20)
    }

    pub fn backoff_unit_sec(&self) -> u64 {
        self.backoff_unit_sec.unwrap_or(2)
    }

    pub fn backoff_factor(&self) -> u32 {
        self.backoff_factor.unwrap_or(2)
    }

    pub fn max_backoff_sec(&self) -> u64 {
        self.max_backoff_sec.unwrap_or(60)
    }

    pub fn dismissible(&self) -> bool {
        self.dismissible.unwrap_or(false)
    }
}

#[derive(Serialize, Deserialize)]
pub struct RootConfig {
    pub upstream: UpstreamConfig,

    pub storage: StorageConfig,

    pub genesis: GenesisConfig,

    #[serde(default, skip_serializing_if = "SyncConfig::is_default")]
    pub sync: SyncConfig,

    #[serde(default, skip_serializing_if = "SubmitConfig::is_default")]
    pub submit: SubmitConfig,

    #[serde(default, skip_serializing_if = "ServeConfig::is_default")]
    pub serve: ServeConfig,

    pub relay: Option<RelayConfig>,

    pub retries: Option<RetryConfig>,

    pub mithril: Option<MithrilConfig>,

    pub snapshot: Option<SnapshotConfig>,

    pub chain: ChainConfig,

    #[serde(default, skip_serializing_if = "LoggingConfig::is_default")]
    pub logging: LoggingConfig,

    #[serde(default, skip_serializing_if = "TelemetryConfig::is_default")]
    pub telemetry: TelemetryConfig,
}
