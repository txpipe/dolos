//! The node's configuration: one TOML file, organised by responsibility.
//!
//! [`load`] reads it. The file declares the configuration contract it was
//! written for in the root `compatibility` field, which is checked before
//! anything else is read. Every table refuses keys it does not know, and
//! [`RootConfig::validate`] checks the rules that span fields before any store
//! opens. Network names and custom genesis files are resolved by the chain
//! crate, which knows the networks.

use std::{
    collections::BTreeMap,
    fmt::Display,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{compat::SUPPORTED_CONTRACTS, Cbor, Era, QueryLimiter, TxoRef};

/// The guide that maps an older configuration onto this one.
pub const MIGRATION_GUIDE_URL: &str = "https://docs.txpipe.io/dolos/configuration/migration";

/// The file read when no path is given.
pub const DEFAULT_CONFIG_FILE: &str = "dolos.toml";

/// The configuration contract a file declares, one of [`SUPPORTED_CONTRACTS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Compatibility(&'static str);

impl Compatibility {
    /// The contract this release writes.
    pub fn current() -> Self {
        Self(SUPPORTED_CONTRACTS[0])
    }

    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        SUPPORTED_CONTRACTS
            .iter()
            .find(|contract| **contract == value)
            .map(|contract| Self(contract))
            .ok_or_else(|| ConfigError::UnsupportedCompatibility(value.to_string()))
    }

    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl Display for Compatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl Serialize for Compatibility {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

impl<'de> Deserialize<'de> for Compatibility {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

fn supported_contracts() -> String {
    SUPPORTED_CONTRACTS
        .iter()
        .map(|contract| format!("\"{contract}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Cardano,
}

/// Which chain the node follows: a network the chain crate knows by name, or
/// `custom` with its identity under `cardano`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub protocol: Protocol,
    pub network: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardano: Option<CardanoConfig>,
}

impl ChainConfig {
    /// The network name that takes its identity from `cardano`.
    pub const CUSTOM: &'static str = "custom";

    /// A Cardano network known by name.
    pub fn known(network: &str) -> Self {
        Self {
            protocol: Protocol::Cardano,
            network: network.to_string(),
            cardano: None,
        }
    }

    /// A custom Cardano network.
    pub fn custom(cardano: CardanoConfig) -> Self {
        Self {
            protocol: Protocol::Cardano,
            network: Self::CUSTOM.to_string(),
            cardano: Some(cardano),
        }
    }

    pub fn is_custom(&self) -> bool {
        self.network == Self::CUSTOM
    }
}

/// The identity of a custom Cardano network.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CardanoConfig {
    /// Must equal the Shelley genesis `networkMagic`.
    pub magic: u32,
    pub is_testnet: bool,
    pub genesis: GenesisConfig,
    /// Outputs added to the genesis UTxO set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_utxos: Vec<CustomUtxo>,
}

/// Genesis files, relative to the configuration file's directory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisConfig {
    pub byron_path: PathBuf,
    pub shelley_path: PathBuf,
    pub alonzo_path: PathBuf,
    pub conway_path: PathBuf,
    /// The protocol major the chain starts at, when it cannot be derived from
    /// the Shelley genesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_protocol: Option<usize>,
}

impl GenesisConfig {
    /// The four files in `dir`, under their usual names.
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            byron_path: dir.join("byron.json"),
            shelley_path: dir.join("shelley.json"),
            alonzo_path: dir.join("alonzo.json"),
            conway_path: dir.join("conway.json"),
            force_protocol: None,
        }
    }

    fn resolve_against(&mut self, base: &Path) {
        for path in [
            &mut self.byron_path,
            &mut self.shelley_path,
            &mut self.alonzo_path,
            &mut self.conway_path,
        ] {
            if path.is_relative() {
                *path = base.join(&*path);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomUtxo {
    #[serde(rename = "ref")]
    pub ref_: TxoRef,
    pub era: Option<Era>,
    pub cbor: Cbor,
}

/// Where blocks come from. `type` is optional and defaults to `peer`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawUpstream", into = "RawUpstream")]
pub enum UpstreamConfig {
    Peer(PeerConfig),
    Emulator(EmulatorConfig),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PeerConfig {
    /// `host:port`; a known network defaults it from the catalog.
    pub peer_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmulatorConfig {
    /// Seconds between produced blocks.
    pub block_production_interval: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum UpstreamType {
    Peer,
    Emulator,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<UpstreamType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    peer_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_production_interval: Option<u64>,
}

impl TryFrom<RawUpstream> for UpstreamConfig {
    type Error = String;

    fn try_from(raw: RawUpstream) -> Result<Self, Self::Error> {
        match raw.kind.unwrap_or(UpstreamType::Peer) {
            UpstreamType::Peer => {
                if raw.block_production_interval.is_some() {
                    return Err(
                        "`block_production_interval` belongs to `type = \"emulator\"`".into(),
                    );
                }
                Ok(Self::Peer(PeerConfig {
                    peer_address: raw.peer_address,
                }))
            }
            UpstreamType::Emulator => {
                if raw.peer_address.is_some() {
                    return Err("an emulator upstream takes no `peer_address`".into());
                }
                let block_production_interval = raw
                    .block_production_interval
                    .ok_or("`type = \"emulator\"` requires `block_production_interval`")?;
                Ok(Self::Emulator(EmulatorConfig {
                    block_production_interval,
                }))
            }
        }
    }
}

impl From<UpstreamConfig> for RawUpstream {
    fn from(config: UpstreamConfig) -> Self {
        match config {
            UpstreamConfig::Peer(peer) => Self {
                kind: None,
                peer_address: peer.peer_address,
                block_production_interval: None,
            },
            UpstreamConfig::Emulator(emulator) => Self {
                kind: Some(UpstreamType::Emulator),
                peer_address: None,
                block_production_interval: Some(emulator.block_production_interval),
            },
        }
    }
}

impl UpstreamConfig {
    pub fn peer(peer_address: impl Into<String>) -> Self {
        Self::Peer(PeerConfig {
            peer_address: Some(peer_address.into()),
        })
    }

    pub fn is_emulator(&self) -> bool {
        matches!(self, Self::Emulator(_))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncConfig {
    /// The WAL directory, relative to `storage.path`; default `wal`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_path: Option<PathBuf>,
    /// Blocks fetched per request.
    pub pull_batch_size: usize,
    pub retry: RetryConfig,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            wal_path: None,
            pull_batch_size: 100,
            retry: RetryConfig::default(),
        }
    }
}

impl SyncConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// How pipeline stages retry a failed step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub backoff_unit_seconds: u64,
    pub backoff_factor: u32,
    pub max_backoff_seconds: u64,
    pub dismissible: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 20,
            backoff_unit_seconds: 2,
            backoff_factor: 2,
            max_backoff_seconds: 60,
            dismissible: false,
        }
    }
}

/// The tuning a Fjall store opens with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FjallPreset {
    #[default]
    Standard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    /// On disk.
    Fjall { preset: FjallPreset },
    /// In memory, lost on restart.
    Ephemeral,
}

impl Default for StoreType {
    fn default() -> Self {
        Self::Fjall {
            preset: FjallPreset::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StoreKind {
    Fjall,
    Ephemeral,
}

fn store_type(
    kind: Option<StoreKind>,
    preset: Option<FjallPreset>,
    paths: &[(&str, bool)],
) -> Result<StoreType, String> {
    match kind.unwrap_or(StoreKind::Fjall) {
        StoreKind::Fjall => Ok(StoreType::Fjall {
            preset: preset.unwrap_or_default(),
        }),
        StoreKind::Ephemeral => {
            let set = paths
                .iter()
                .chain([("preset", preset.is_some())].iter())
                .filter(|(_, set)| *set)
                .map(|(name, _)| format!("`{name}`"))
                .collect::<Vec<_>>();

            if !set.is_empty() {
                return Err(format!(
                    "an ephemeral store takes no other field; remove {}",
                    set.join(", ")
                ));
            }

            Ok(StoreType::Ephemeral)
        }
    }
}

fn store_fields(kind: StoreType) -> (Option<StoreKind>, Option<FjallPreset>) {
    match kind {
        StoreType::Fjall { preset } => (Some(StoreKind::Fjall), Some(preset)),
        StoreType::Ephemeral => (Some(StoreKind::Ephemeral), None),
    }
}

/// The ledger state store. `type` defaults to `fjall`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawStateStore", into = "RawStateStore")]
pub struct StateStoreConfig {
    pub kind: StoreType,
    /// Relative to `storage.path`; default `state`.
    pub path: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStateStore {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<StoreKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preset: Option<FjallPreset>,
}

impl TryFrom<RawStateStore> for StateStoreConfig {
    type Error = String;

    fn try_from(raw: RawStateStore) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: store_type(raw.kind, raw.preset, &[("path", raw.path.is_some())])?,
            path: raw.path,
        })
    }
}

impl From<StateStoreConfig> for RawStateStore {
    fn from(config: StateStoreConfig) -> Self {
        let (kind, preset) = store_fields(config.kind);
        Self {
            kind,
            path: config.path,
            preset,
        }
    }
}

impl StateStoreConfig {
    pub fn ephemeral() -> Self {
        Self {
            kind: StoreType::Ephemeral,
            path: None,
        }
    }
}

/// The block archive. `type` defaults to `fjall`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawArchiveStore", into = "RawArchiveStore")]
pub struct ArchiveStoreConfig {
    pub kind: StoreType,
    /// Relative to `storage.path`; default `archive`.
    pub path: Option<PathBuf>,
    /// Where block segment files go; default the archive directory.
    pub blocks_path: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArchiveStore {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<StoreKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocks_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preset: Option<FjallPreset>,
}

impl TryFrom<RawArchiveStore> for ArchiveStoreConfig {
    type Error = String;

    fn try_from(raw: RawArchiveStore) -> Result<Self, Self::Error> {
        let paths = [
            ("path", raw.path.is_some()),
            ("blocks_path", raw.blocks_path.is_some()),
        ];
        Ok(Self {
            kind: store_type(raw.kind, raw.preset, &paths)?,
            path: raw.path,
            blocks_path: raw.blocks_path,
        })
    }
}

impl From<ArchiveStoreConfig> for RawArchiveStore {
    fn from(config: ArchiveStoreConfig) -> Self {
        let (kind, preset) = store_fields(config.kind);
        Self {
            kind,
            path: config.path,
            blocks_path: config.blocks_path,
            preset,
        }
    }
}

impl ArchiveStoreConfig {
    pub fn ephemeral() -> Self {
        Self {
            kind: StoreType::Ephemeral,
            path: None,
            blocks_path: None,
        }
    }
}

/// The disposable cache for derived reads. No setting changes an answer, only
/// what is kept and for how long.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Off: every lookup misses and nothing is stored.
    pub enabled: bool,
    /// The disk tier's directory, relative to `storage.path`; default `cache`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// MiB the memory tier may hold, apart from the state and archive caches.
    pub memory_mb: u64,
    /// MiB the disk tier may hold.
    pub disk_mb: u64,
    /// The least cost an entry must have to be kept, by namespace, in place
    /// of the namespace's own default.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub admission: BTreeMap<String, u64>,
}

impl CacheConfig {
    pub const DEFAULT_MEMORY_MB: u64 = 512;
    pub const DEFAULT_DISK_MB: u64 = 4096;

    pub fn memory_bytes(&self) -> u64 {
        self.memory_mb.saturating_mul(1 << 20)
    }

    pub fn disk_bytes(&self) -> u64 {
        self.disk_mb.saturating_mul(1 << 20)
    }

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: None,
            memory_mb: Self::DEFAULT_MEMORY_MB,
            disk_mb: Self::DEFAULT_DISK_MB,
            admission: BTreeMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Root directory for the stores, relative to the working directory. Also
    /// holds `scratch/`, where a stele transfer over an OCI registry stages
    /// layers in flight.
    pub path: PathBuf,
    #[serde(skip_serializing_if = "is_default")]
    pub state: StateStoreConfig,
    #[serde(skip_serializing_if = "is_default")]
    pub archive: ArchiveStoreConfig,
    #[serde(skip_serializing_if = "CacheConfig::is_default")]
    pub cache: CacheConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            state: StateStoreConfig::default(),
            archive: ArchiveStoreConfig::default(),
            cache: CacheConfig::default(),
        }
    }
}

impl StorageConfig {
    /// `path` resolved against the storage root, or the root's `default`.
    pub fn resolve(&self, path: Option<&PathBuf>, default: &str) -> PathBuf {
        match path {
            Some(path) => self.path.join(path),
            None => self.path.join(default),
        }
    }

    /// `None` for an ephemeral store.
    pub fn state_path(&self) -> Option<PathBuf> {
        match self.state.kind {
            StoreType::Fjall { .. } => Some(self.resolve(self.state.path.as_ref(), "state")),
            StoreType::Ephemeral => None,
        }
    }

    /// `None` for an ephemeral store.
    pub fn archive_path(&self) -> Option<PathBuf> {
        match self.archive.kind {
            StoreType::Fjall { .. } => Some(self.resolve(self.archive.path.as_ref(), "archive")),
            StoreType::Ephemeral => None,
        }
    }

    /// `None` while the cache is off.
    pub fn cache_path(&self) -> Option<PathBuf> {
        self.cache
            .enabled
            .then(|| self.resolve(self.cache.path.as_ref(), "cache"))
    }
}

/// How much block history the archive keeps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistoryMode {
    /// No archive.
    None,
    /// The last `window`.
    Rolling,
    /// Everything.
    #[default]
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowUnit {
    Slot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryWindow {
    pub unit: WindowUnit,
    pub value: u64,
}

impl HistoryWindow {
    pub fn slots(value: u64) -> Self {
        Self {
            unit: WindowUnit::Slot,
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryConfig {
    pub mode: HistoryMode,
    /// Only with `rolling`, and required there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<HistoryWindow>,
}

impl HistoryConfig {
    pub fn none() -> Self {
        Self {
            mode: HistoryMode::None,
            window: None,
        }
    }

    pub fn rolling(window: HistoryWindow) -> Self {
        Self {
            mode: HistoryMode::Rolling,
            window: Some(window),
        }
    }

    /// The slots the archive keeps behind the tip; `None` keeps everything.
    pub fn max_slots(&self) -> Option<u64> {
        match (self.mode, self.window) {
            (HistoryMode::Rolling, Some(window)) => match window.unit {
                WindowUnit::Slot => Some(window.value),
            },
            _ => None,
        }
    }

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MempoolConfig {
    /// Keep pending transactions across a restart.
    pub persistent: bool,
    /// Relative to `storage.path`; default `mempool`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

impl MempoolConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OuroborosConfig {
    pub listen_path: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    pub listen_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_client_ca_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,

    /// Enable hyper's adaptive (BDP-based) flow-control windowing. Defaults to
    /// `true`. When enabled, the explicit `http2_initial_*_window_size` values
    /// are ignored by hyper (adaptive sizing takes precedence).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_adaptive_window: Option<bool>,
    /// Initial HTTP/2 stream-level flow-control window, in bytes. Ignored when
    /// `http2_adaptive_window` is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_initial_stream_window_size: Option<u32>,
    /// Initial HTTP/2 connection-level flow-control window, in bytes. Ignored
    /// when `http2_adaptive_window` is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_initial_connection_window_size: Option<u32>,
    /// Maximum HTTP/2 frame size, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_max_frame_size: Option<u32>,
    /// Maximum number of concurrent HTTP/2 streams per connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http2_max_concurrent_streams: Option<u32>,
}

impl GrpcConfig {
    pub fn new(listen_address: String, tls_client_ca_root: Option<PathBuf>) -> Self {
        Self {
            listen_address,
            tls_client_ca_root,
            permissive_cors: None,
            http2_adaptive_window: None,
            http2_initial_stream_window_size: None,
            http2_initial_connection_window_size: None,
            http2_max_frame_size: None,
            http2_max_concurrent_streams: None,
        }
    }

    pub fn with_permissive_cors(mut self, permissive_cors: bool) -> Self {
        self.permissive_cors = Some(permissive_cors);
        self
    }

    pub fn permissive_cors(&self) -> bool {
        self.permissive_cors.unwrap_or(true)
    }

    /// Whether adaptive HTTP/2 windowing should be used. Defaults to `true`.
    pub fn http2_adaptive_window(&self) -> bool {
        self.http2_adaptive_window.unwrap_or(true)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MinibfConfig {
    pub listen_address: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_registry_url: Option<String>,
    /// The URL clients reach this API at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_scan_items: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ipfs_gateways: Option<Vec<String>>,
}

impl MinibfConfig {
    pub fn new(listen_address: SocketAddr) -> Self {
        Self {
            listen_address,
            permissive_cors: None,
            token_registry_url: None,
            public_url: None,
            max_scan_items: None,
            ipfs_gateways: None,
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

    /// These are the base URLs of the HTTP gateways, in order. The gateways
    /// resolve `ipfs://` governance-anchor URLs. Dolos sends a request to each
    /// gateway in order. Dolos stops at the first gateway that serves the
    /// content. cardano-db-sync resolves `ipfs://` anchors the same way.
    ///
    /// An absent setting gives the default list. An empty list stops IPFS
    /// resolution, so `ipfs://` anchors do not resolve.
    pub fn ipfs_gateways(&self) -> Vec<String> {
        self.ipfs_gateways
            .clone()
            .unwrap_or_else(default_ipfs_gateways)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MinikupoConfig {
    pub listen_address: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissive_cors: Option<bool>,
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

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrpConfig {
    pub listen_address: SocketAddr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_optimize_rounds: Option<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    permissive_cors: Option<bool>,

    /// Lovelace added to every resolved transaction's fee.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_fees_lovelace: Option<u64>,
}

impl TrpConfig {
    pub fn new(listen_address: SocketAddr, extra_fees_lovelace: Option<u64>) -> Self {
        Self {
            listen_address,
            max_optimize_rounds: None,
            permissive_cors: None,
            extra_fees_lovelace,
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

fn default_ipfs_gateways() -> Vec<String> {
    // The main public gateway is first. A fallback gateway is second. If the
    // first gateway returns HTTP 429, the resolver uses the fallback.
    // cardano-db-sync uses the same two-entry shape. There the second entry is
    // a private gateway of Blockfrost that pins the content. Dolos has no
    // private infrastructure, so Dolos names a second public gateway.
    vec![
        "https://ipfs.io".to_string(),
        "https://gateway.pinata.cloud".to_string(),
    ]
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ServeConfig {
    /// Blocking query work the node runs at once, across every API.
    pub max_concurrent_queries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GrpcConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minibf: Option<MinibfConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minikupo: Option<MinikupoConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ouroboros: Option<OuroborosConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trp: Option<TrpConfig>,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: QueryLimiter::DEFAULT_CAP,
            grpc: None,
            minibf: None,
            minikupo: None,
            ouroboros: None,
            trp: None,
        }
    }
}

impl ServeConfig {
    pub const MAX_CONCURRENT_QUERIES: std::ops::RangeInclusive<usize> = 1..=64;

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelayConfig {
    pub listen_address: String,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// The level for Dolos's own events.
    #[serde_as(as = "DisplayFromStr")]
    pub level: tracing::Level,
    /// Further `target=level` directives, comma-separated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: tracing::Level::INFO,
            filter: None,
        }
    }
}

impl LoggingConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// OTLP trace export, on when the section is present.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    pub otlp_endpoint: String,
    pub service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "dolos".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RootConfig {
    compatibility: Compatibility,

    pub chain: ChainConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<UpstreamConfig>,

    #[serde(default, skip_serializing_if = "SyncConfig::is_default")]
    pub sync: SyncConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default, skip_serializing_if = "HistoryConfig::is_default")]
    pub history: HistoryConfig,

    #[serde(default, skip_serializing_if = "MempoolConfig::is_default")]
    pub mempool: MempoolConfig,

    #[serde(default, skip_serializing_if = "ServeConfig::is_default")]
    pub serve: ServeConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay: Option<RelayConfig>,

    #[serde(default, skip_serializing_if = "LoggingConfig::is_default")]
    pub logging: LoggingConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryConfig>,
}

impl RootConfig {
    /// A configuration for `chain` at the current contract, every other
    /// section at its default.
    pub fn new(chain: ChainConfig) -> Self {
        Self {
            compatibility: Compatibility::current(),
            chain,
            upstream: None,
            sync: SyncConfig::default(),
            storage: StorageConfig::default(),
            history: HistoryConfig::default(),
            mempool: MempoolConfig::default(),
            serve: ServeConfig::default(),
            relay: None,
            logging: LoggingConfig::default(),
            telemetry: None,
        }
    }

    pub fn compatibility(&self) -> Compatibility {
        self.compatibility
    }

    /// The WAL directory: on disk exactly when the state is.
    pub fn wal_path(&self) -> Option<PathBuf> {
        match self.storage.state.kind {
            StoreType::Fjall { .. } => {
                Some(self.storage.resolve(self.sync.wal_path.as_ref(), "wal"))
            }
            StoreType::Ephemeral => None,
        }
    }

    /// `None` for an in-memory mempool.
    pub fn mempool_path(&self) -> Option<PathBuf> {
        self.mempool
            .persistent
            .then(|| self.storage.resolve(self.mempool.path.as_ref(), "mempool"))
    }

    /// Whether the node keeps a block archive at all.
    pub fn keeps_archive(&self) -> bool {
        self.history.mode != HistoryMode::None
    }

    /// The rules that span fields. Network names and custom genesis files are
    /// checked where the chain is resolved.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));

        match (self.chain.is_custom(), &self.chain.cardano) {
            (true, None) => {
                return invalid(
                    "`network = \"custom\"` requires `[chain.cardano]` with the network's \
                     `magic`, `is_testnet` and `[chain.cardano.genesis]` files"
                        .into(),
                )
            }
            (false, Some(_)) => {
                return invalid(format!(
                    "`[chain.cardano]` is only for `network = \"custom\"`; the `{}` network's \
                     magic and genesis are built in",
                    self.chain.network
                ))
            }
            _ => (),
        }

        match (&self.upstream, self.chain.is_custom()) {
            (None, true) => {
                return invalid(
                    "a custom network has no default peer; add `[upstream]` with a \
                     `peer_address`, or `type = \"emulator\"`"
                        .into(),
                )
            }
            (Some(UpstreamConfig::Peer(PeerConfig { peer_address: None })), true) => {
                return invalid("`upstream.peer_address` is required on a custom network".into())
            }
            (Some(UpstreamConfig::Emulator(_)), false) => {
                return invalid(
                    "`upstream.type = \"emulator\"` is only valid with `network = \"custom\"`"
                        .into(),
                )
            }
            _ => (),
        }

        match (self.history.mode, self.history.window) {
            (HistoryMode::Rolling, None) => {
                return invalid(
                    "`history.mode = \"rolling\"` requires a `window`, such as \
                     `{ unit = \"slot\", value = 2592000 }`"
                        .into(),
                )
            }
            (HistoryMode::None | HistoryMode::Full, Some(_)) => {
                return invalid("`history.window` is only valid with `mode = \"rolling\"`".into())
            }
            _ => (),
        }

        let queries = self.serve.max_concurrent_queries;
        if !ServeConfig::MAX_CONCURRENT_QUERIES.contains(&queries) {
            return invalid(format!(
                "`serve.max_concurrent_queries` must be between {} and {}, found {queries}",
                ServeConfig::MAX_CONCURRENT_QUERIES.start(),
                ServeConfig::MAX_CONCURRENT_QUERIES.end(),
            ));
        }

        Ok(())
    }

    fn resolve_paths(&mut self, config_dir: &Path) {
        if let Some(cardano) = &mut self.chain.cardano {
            cardano.genesis.resolve_against(config_dir);
        }
    }
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("no configuration file at {}; pass `--config PATH` or create one with `dolos init`", .0.display())]
    NotFound(PathBuf),

    #[error("reading {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: ::config::ConfigError,
    },

    #[error(
        "the configuration does not declare `compatibility`; add `compatibility = \"{}\"` at the \
         top of the file (supported: {})",
        Compatibility::current(),
        supported_contracts()
    )]
    MissingCompatibility,

    #[error(
        "this is a Dolos v1 or v2.0.0-alpha.0 configuration, which this release does not read; \
         rewrite it as described in the migration guide at {MIGRATION_GUIDE_URL}"
    )]
    LegacyShape,

    #[error("`compatibility` must be a string such as \"{current}\", found {0}", current = Compatibility::current())]
    MalformedCompatibility(String),

    #[error("unsupported `compatibility = \"{0}\"`; this release supports {supported}", supported = supported_contracts())]
    UnsupportedCompatibility(String),

    #[error(
        "the configuration carries sections that are now command arguments; remove them: {}",
        .0.join("; ")
    )]
    MovedSections(Vec<String>),

    #[error("applying the environment overrides {}: {source}", .variables.join(", "))]
    Environment {
        variables: Vec<String>,
        #[source]
        source: ::config::ConfigError,
    },

    #[error("the environment variable {0} is not valid UTF-8")]
    NonUnicodeOverride(String),

    #[error("invalid configuration: {0}")]
    Invalid(String),
}

/// Sections an older configuration carried that are now inputs of the
/// commands that read them, and where each one went.
const MOVED_TO_ARGUMENTS: [(&str, &str); 3] = [
    (
        "mithril",
        "`dolos bootstrap mithril --aggregator --genesis-vkey`",
    ),
    (
        "snapshot",
        "`dolos bootstrap snapshot --download-url` and `dolos snapshot … --state-epochs`",
    ),
    (
        "stelae",
        "`--registry-user` / `DOLOS_STELAE_USER`, `DOLOS_STELAE_PASSWORD`, `DOLOS_STELAE_TOKEN` \
         and `--registry-credentials-file` on `dolos bootstrap stelae` and `dolos snapshot`",
    ),
];

/// Keys only a v1 or v2.0.0-alpha.0 file has.
const LEGACY_KEYS: [&str; 3] = ["genesis", "storage.version", "chain.type"];

const ENV_PREFIX: &str = "DOLOS";
const ENV_NESTING: &str = "__";

/// Read the configuration: `path`, or else `./dolos.toml`, and `DOLOS_`
/// environment variables over it.
///
/// A variable names its key with `__` between levels, as in
/// `DOLOS_UPSTREAM__PEER_ADDRESS`; one without `__` is not an override, which
/// leaves the command inputs such as `DOLOS_MITHRIL_AGGREGATOR` alone. An
/// error the overrides cause names them rather than the file.
/// `compatibility` is read from the file alone. Genesis paths resolve against
/// the file's directory.
pub fn load(path: Option<&Path>) -> Result<RootConfig, ConfigError> {
    let path = path.unwrap_or(Path::new(DEFAULT_CONFIG_FILE));

    if !path.is_file() {
        return Err(ConfigError::NotFound(path.to_path_buf()));
    }

    let read = |source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    };
    let file = || ::config::File::from(path).format(::config::FileFormat::Toml);

    let document = ::config::Config::builder()
        .add_source(file())
        .build()
        .map_err(read)?;

    check_compatibility(&document)?;
    reject_moved_sections(&document)?;

    let overrides = environment_overrides()?;
    let deserialize = |overrides: ::config::Map<String, String>| {
        ::config::Config::builder()
            .add_source(file())
            .add_source(environment(overrides))
            .build()
            .and_then(::config::Config::try_deserialize::<RootConfig>)
    };

    let mut config = match deserialize(overrides.clone()) {
        Ok(config) => config,
        Err(source) if !overrides.is_empty() && deserialize(Default::default()).is_ok() => {
            let mut variables: Vec<String> = overrides.into_keys().collect();
            variables.sort();
            return Err(ConfigError::Environment { variables, source });
        }
        Err(source) => return Err(read(source)),
    };

    config.resolve_paths(path.parent().unwrap_or(Path::new("")));
    config.validate()?;

    Ok(config)
}

fn check_compatibility(document: &::config::Config) -> Result<(), ConfigError> {
    let Ok(value) = document.get::<::config::Value>("compatibility") else {
        let legacy = LEGACY_KEYS
            .iter()
            .any(|key| document.get::<::config::Value>(key).is_ok());

        return Err(if legacy {
            ConfigError::LegacyShape
        } else {
            ConfigError::MissingCompatibility
        });
    };

    match value.kind {
        ::config::ValueKind::String(value) => Compatibility::parse(&value).map(|_| ()),
        _ => Err(ConfigError::MalformedCompatibility(value.to_string())),
    }
}

/// Refuses a file that still carries a moved section, naming where it went
/// rather than reporting an unknown field.
fn reject_moved_sections(document: &::config::Config) -> Result<(), ConfigError> {
    let moved: Vec<String> = MOVED_TO_ARGUMENTS
        .iter()
        .filter(|(section, _)| document.get::<::config::Value>(section).is_ok())
        .map(|(section, destination)| format!("[{section}] moved to {destination}"))
        .collect();

    if moved.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::MovedSections(moved))
    }
}

/// The `DOLOS_` variables with `__` in their name. One that is not valid
/// UTF-8 is refused rather than skipped, which would leave the file's value
/// in place unnoticed.
fn environment_overrides() -> Result<::config::Map<String, String>, ConfigError> {
    let prefix = format!("{ENV_PREFIX}_");
    let mut overrides = ::config::Map::new();

    for (key, value) in std::env::vars_os() {
        let name = key.to_string_lossy().into_owned();

        if !name
            .strip_prefix(&prefix)
            .is_some_and(|key| key.contains(ENV_NESTING))
        {
            continue;
        }

        match (key.into_string(), value.into_string()) {
            (Ok(key), Ok(value)) => {
                overrides.insert(key, value);
            }
            _ => return Err(ConfigError::NonUnicodeOverride(name)),
        }
    }

    Ok(overrides)
}

fn environment(overrides: ::config::Map<String, String>) -> ::config::Environment {
    ::config::Environment::with_prefix(ENV_PREFIX)
        .prefix_separator("_")
        .separator(ENV_NESTING)
        .source(Some(overrides))
}

/// Options for opening the Redb WAL backend.
#[derive(Clone, Debug, Default)]
pub struct RedbWalConfig {
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
}

/// Options for opening the legacy Redb state backend.
#[derive(Clone, Debug, Default)]
pub struct RedbStateConfig {
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
}

/// Options for opening the Redb mempool backend.
#[derive(Clone, Debug, Default)]
pub struct RedbMempoolConfig {
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
}

/// Options for opening the Fjall state backend. Not read from the file:
/// `preset = "standard"` opens with the defaults.
#[derive(Clone, Debug, Default)]
pub struct FjallStateConfig {
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
    /// Maximum journal size in MB.
    pub max_journal_size: Option<usize>,
    /// Flush journal after each commit.
    pub flush_on_commit: Option<bool>,
    /// L0 compaction threshold (default: 4, lower = more aggressive).
    pub l0_threshold: Option<u8>,
    /// Number of background compaction worker threads.
    pub worker_threads: Option<usize>,
    /// Memtable size in MB before flush (default: 64).
    pub memtable_size_mb: Option<usize>,
}

/// Options for opening the Fjall archive backend. Not read from the file:
/// `preset = "standard"` opens with the defaults.
///
/// Block segments are always compressed, one zstd frame per block with the
/// dictionary bundled in `dolos-flatfiles`; there is nothing to configure
/// about them.
#[derive(Clone, Debug, Default)]
pub struct FjallArchiveConfig {
    /// Where block segment files go; default the archive directory.
    pub blocks_path: Option<PathBuf>,
    /// Size (in MB) of memory allocated for caching.
    pub cache: Option<usize>,
    /// Maximum journal size in MB.
    pub max_journal_size: Option<usize>,
    /// Flush journal after each commit.
    pub flush_on_commit: Option<bool>,
    /// L0 compaction threshold (default: 4, lower = more aggressive).
    pub l0_threshold: Option<u8>,
    /// Number of background compaction worker threads.
    pub worker_threads: Option<usize>,
    /// Memtable size in MB before flush (default: 64).
    pub memtable_size_mb: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn preview() -> RootConfig {
        RootConfig::new(ChainConfig::known("preview"))
    }

    fn custom() -> CardanoConfig {
        CardanoConfig {
            magic: 42,
            is_testnet: true,
            genesis: GenesisConfig::in_dir(Path::new("genesis")),
            custom_utxos: vec![],
        }
    }

    fn invalid(config: &RootConfig) -> String {
        match config.validate() {
            Err(ConfigError::Invalid(message)) => message,
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    #[test]
    fn compatibility_accepts_only_the_supported_contracts() {
        assert_eq!(
            Compatibility::parse("v2-alpha.1").unwrap().as_str(),
            "v2-alpha.1"
        );
        assert_eq!(Compatibility::current().as_str(), SUPPORTED_CONTRACTS[0]);

        let message = Compatibility::parse("v2-alpha.0").unwrap_err().to_string();
        assert!(message.contains("v2-alpha.0"), "{message}");
        assert!(message.contains("\"v2-alpha.1\""), "{message}");
    }

    #[test]
    fn an_omitted_store_type_is_fjall_standard() {
        let state: StateStoreConfig = serde_json::from_value(json!({ "path": "s" })).unwrap();
        assert_eq!(
            state.kind,
            StoreType::Fjall {
                preset: FjallPreset::Standard
            }
        );

        let err = serde_json::from_value::<StateStoreConfig>(json!({ "preset": "fast" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("standard"), "{err}");
    }

    #[test]
    fn an_ephemeral_store_takes_no_other_field() {
        let archive: ArchiveStoreConfig =
            serde_json::from_value(json!({ "type": "ephemeral" })).unwrap();
        assert_eq!(archive, ArchiveStoreConfig::ephemeral());

        let err = serde_json::from_value::<ArchiveStoreConfig>(json!({
            "type": "ephemeral",
            "blocks_path": "blocks",
            "preset": "standard",
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("`blocks_path`, `preset`"), "{err}");
    }

    #[test]
    fn stores_refuse_keys_they_do_not_know() {
        let err = serde_json::from_value::<StateStoreConfig>(json!({ "blocks_path": "b" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("blocks_path"), "{err}");

        let err = serde_json::from_value::<ArchiveStoreConfig>(json!({ "cache": 16 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cache"), "{err}");
    }

    #[test]
    fn store_selections_round_trip() {
        for archive in [
            ArchiveStoreConfig::default(),
            ArchiveStoreConfig::ephemeral(),
            ArchiveStoreConfig {
                path: Some("a".into()),
                blocks_path: Some("/hdd/blocks".into()),
                ..Default::default()
            },
        ] {
            let json = serde_json::to_value(&archive).unwrap();
            assert_eq!(
                serde_json::from_value::<ArchiveStoreConfig>(json).unwrap(),
                archive
            );
        }

        let json = serde_json::to_value(StorageConfig::default()).unwrap();
        assert!(json.get("state").is_none());
        assert!(json.get("archive").is_none());
        assert!(json.get("cache").is_none());
    }

    #[test]
    fn upstream_type_defaults_to_peer() {
        let upstream: UpstreamConfig = serde_json::from_value(json!({})).unwrap();
        assert_eq!(upstream, UpstreamConfig::Peer(PeerConfig::default()));

        let upstream: UpstreamConfig = serde_json::from_value(json!({
            "type": "emulator",
            "block_production_interval": 5,
        }))
        .unwrap();
        assert_eq!(
            upstream,
            UpstreamConfig::Emulator(EmulatorConfig {
                block_production_interval: 5
            })
        );

        for (raw, field) in [
            (
                json!({ "block_production_interval": 5 }),
                "block_production_interval",
            ),
            (json!({ "type": "emulator" }), "block_production_interval"),
            (
                json!({ "type": "emulator", "block_production_interval": 5, "peer_address": "x:1" }),
                "peer_address",
            ),
        ] {
            let err = serde_json::from_value::<UpstreamConfig>(raw)
                .unwrap_err()
                .to_string();
            assert!(err.contains(field), "{err}");
        }
    }

    #[test]
    fn the_cache_takes_mebibytes() {
        let cache: CacheConfig =
            serde_json::from_value(json!({ "admission": { "totals": 5 } })).unwrap();
        assert!(cache.enabled);
        assert_eq!(cache.memory_bytes(), 512 * 1024 * 1024);
        assert_eq!(cache.disk_bytes(), 4 * 1024 * 1024 * 1024);
        assert_eq!(cache.admission["totals"], 5);

        let err = serde_json::from_value::<CacheConfig>(json!({ "memory_budget_bytes": 1 }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("memory_budget_bytes"), "{err}");
    }

    #[test]
    fn store_paths_resolve_against_the_storage_root() {
        let mut config = preview();
        config.storage.path = PathBuf::from("/data");

        assert_eq!(
            config.storage.state_path(),
            Some(PathBuf::from("/data/state"))
        );
        assert_eq!(
            config.storage.archive_path(),
            Some(PathBuf::from("/data/archive"))
        );
        assert_eq!(
            config.storage.cache_path(),
            Some(PathBuf::from("/data/cache"))
        );
        assert_eq!(config.wal_path(), Some(PathBuf::from("/data/wal")));
        assert_eq!(config.mempool_path(), None);

        config.storage.state.path = Some(PathBuf::from("fast/state"));
        config.storage.cache.path = Some(PathBuf::from("/ssd/cache"));
        config.sync.wal_path = Some(PathBuf::from("w"));
        config.mempool.persistent = true;

        assert_eq!(
            config.storage.state_path(),
            Some(PathBuf::from("/data/fast/state"))
        );
        assert_eq!(
            config.storage.cache_path(),
            Some(PathBuf::from("/ssd/cache"))
        );
        assert_eq!(config.wal_path(), Some(PathBuf::from("/data/w")));
        assert_eq!(config.mempool_path(), Some(PathBuf::from("/data/mempool")));

        config.storage.state = StateStoreConfig::ephemeral();
        config.storage.cache.enabled = false;

        assert_eq!(config.storage.state_path(), None);
        assert_eq!(config.wal_path(), None);
        assert_eq!(config.storage.cache_path(), None);
    }

    #[test]
    fn history_keeps_its_window_only_when_rolling() {
        assert_eq!(HistoryConfig::default().max_slots(), None);
        assert_eq!(HistoryConfig::none().max_slots(), None);
        assert_eq!(
            HistoryConfig::rolling(HistoryWindow::slots(100)).max_slots(),
            Some(100)
        );
    }

    #[test]
    fn a_default_config_is_valid() {
        preview().validate().unwrap();

        let mut config = RootConfig::new(ChainConfig::custom(custom()));
        config.upstream = Some(UpstreamConfig::peer("x:1"));
        config.validate().unwrap();
    }

    #[test]
    fn the_chain_section_follows_the_network() {
        let mut config = RootConfig::new(ChainConfig::custom(custom()));
        config.chain.cardano = None;
        assert!(invalid(&config).contains("requires `[chain.cardano]`"));

        let mut config = preview();
        config.chain.cardano = Some(custom());
        assert!(invalid(&config).contains("only for `network = \"custom\"`"));
    }

    #[test]
    fn a_custom_network_names_its_upstream() {
        let mut config = RootConfig::new(ChainConfig::custom(custom()));
        assert!(invalid(&config).contains("add `[upstream]`"));

        config.upstream = Some(UpstreamConfig::Peer(PeerConfig::default()));
        assert!(invalid(&config).contains("`upstream.peer_address` is required"));

        config.upstream = Some(UpstreamConfig::Emulator(EmulatorConfig {
            block_production_interval: 1,
        }));
        config.validate().unwrap();

        let mut config = preview();
        config.upstream = Some(UpstreamConfig::Emulator(EmulatorConfig {
            block_production_interval: 1,
        }));
        assert!(invalid(&config).contains("emulator"));
    }

    #[test]
    fn a_window_goes_with_rolling_history() {
        let mut config = preview();

        config.history.mode = HistoryMode::Rolling;
        assert!(invalid(&config).contains("requires a `window`"));

        config.history = HistoryConfig {
            mode: HistoryMode::Full,
            window: Some(HistoryWindow::slots(1)),
        };
        assert!(invalid(&config).contains("only valid with `mode = \"rolling\"`"));
    }

    #[test]
    fn concurrent_queries_stay_in_range() {
        let mut config = preview();

        for (queries, valid) in [(0, false), (1, true), (64, true), (65, false)] {
            config.serve.max_concurrent_queries = queries;
            assert_eq!(config.validate().is_ok(), valid, "{queries}");
        }
    }

    #[test]
    fn genesis_paths_resolve_against_the_config_directory() {
        let mut config = RootConfig::new(ChainConfig::custom(custom()));
        config.chain.cardano.as_mut().unwrap().genesis.conway_path = PathBuf::from("/abs/c.json");

        config.resolve_paths(Path::new("/etc/node"));

        let genesis = &config.chain.cardano.unwrap().genesis;
        assert_eq!(
            genesis.byron_path,
            PathBuf::from("/etc/node/genesis/byron.json")
        );
        assert_eq!(genesis.conway_path, PathBuf::from("/abs/c.json"));
    }
}
