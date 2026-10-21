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
pub const MIGRATION_GUIDE_URL: &str = "https://docs.txpipe.io/dolos/migration/dolos-v2";

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
    type Error = Mismatch;

    fn try_from(raw: RawUpstream) -> Result<Self, Self::Error> {
        match raw.kind.unwrap_or(UpstreamType::Peer) {
            UpstreamType::Peer => {
                if raw.block_production_interval.is_some() {
                    return Err(Mismatch::new(
                        ["type", "block_production_interval"],
                        "`block_production_interval` belongs to `type = \"emulator\"`",
                    ));
                }
                Ok(Self::Peer(PeerConfig {
                    peer_address: raw.peer_address,
                }))
            }
            UpstreamType::Emulator => {
                if raw.peer_address.is_some() {
                    return Err(Mismatch::new(
                        ["type", "peer_address"],
                        "an emulator upstream takes no `peer_address`",
                    ));
                }
                let block_production_interval = raw.block_production_interval.ok_or_else(|| {
                    Mismatch::new(
                        ["type", "block_production_interval"],
                        "`type = \"emulator\"` requires `block_production_interval`",
                    )
                })?;
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
    paths: &[(&'static str, bool)],
) -> Result<StoreType, Mismatch> {
    match kind.unwrap_or(StoreKind::Fjall) {
        StoreKind::Fjall => Ok(StoreType::Fjall {
            preset: preset.unwrap_or_default(),
        }),
        StoreKind::Ephemeral => {
            let set: Vec<&'static str> = paths
                .iter()
                .copied()
                .chain([("preset", preset.is_some())])
                .filter(|(_, set)| *set)
                .map(|(name, _)| name)
                .collect();

            if !set.is_empty() {
                let names: Vec<String> = set.iter().map(|name| format!("`{name}`")).collect();
                let message = format!(
                    "an ephemeral store takes no other field; remove {}",
                    names.join(", ")
                );
                return Err(Mismatch::new(std::iter::once("type").chain(set), message));
            }

            Ok(StoreType::Ephemeral)
        }
    }
}

/// Fields of one table that do not go together, named as the table names
/// them.
#[derive(Debug)]
struct Mismatch {
    fields: Vec<&'static str>,
    message: String,
}

impl Mismatch {
    fn new(fields: impl IntoIterator<Item = &'static str>, message: impl Into<String>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
            message: message.into(),
        }
    }

    /// The rule the mismatch breaks in the configuration, with its fields
    /// under `table`.
    fn at(self, table: &str) -> ConfigError {
        ConfigError::invalid(
            self.fields.iter().map(|field| format!("{table}.{field}")),
            format!("in `[{table}]`, {}", self.message),
        )
    }
}

impl Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
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
    type Error = Mismatch;

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
    type Error = Mismatch;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MempoolConfig {
    /// Keep pending transactions across a restart.
    pub persistent: bool,
    /// Relative to `storage.path`; default `mempool`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// The confirmations a transaction stays in the mempool for before it is
    /// finalized.
    pub retain_confirmed_blocks: u32,
}

impl MempoolConfig {
    pub const DEFAULT_RETAIN_CONFIRMED_BLOCKS: u32 = 6;

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            persistent: false,
            path: None,
            retain_confirmed_blocks: Self::DEFAULT_RETAIN_CONFIRMED_BLOCKS,
        }
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

/// A count, refusing the fraction the file reader would otherwise round away.
fn whole_number<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    use serde::de::{Error, Unexpected, Visitor};

    struct WholeNumber;

    impl Visitor<'_> for WholeNumber {
        type Value = usize;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a non-negative whole number")
        }

        fn visit_u64<E: Error>(self, value: u64) -> Result<usize, E> {
            usize::try_from(value).map_err(|_| E::invalid_value(Unexpected::Unsigned(value), &self))
        }

        fn visit_i64<E: Error>(self, value: i64) -> Result<usize, E> {
            usize::try_from(value).map_err(|_| E::invalid_value(Unexpected::Signed(value), &self))
        }

        // An environment override arrives as text.
        fn visit_str<E: Error>(self, value: &str) -> Result<usize, E> {
            value
                .parse()
                .map_err(|_| E::invalid_value(Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_any(WholeNumber)
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ServeConfig {
    /// Blocking query work the node runs at once, across every API.
    #[serde(deserialize_with = "whole_number")]
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
        match (self.chain.is_custom(), &self.chain.cardano) {
            (true, None) => {
                return Err(ConfigError::invalid(
                    ["chain.network", "chain.cardano"],
                    "`network = \"custom\"` requires `[chain.cardano]` with the network's \
                     `magic`, `is_testnet` and `[chain.cardano.genesis]` files",
                ))
            }
            (false, Some(_)) => {
                return Err(ConfigError::invalid(
                    ["chain.network", "chain.cardano"],
                    format!(
                        "`[chain.cardano]` is only for `network = \"custom\"`, found `network = \
                         \"{}\"`; a known network's magic and genesis are built in",
                        self.chain.network
                    ),
                ))
            }
            _ => (),
        }

        match (&self.upstream, self.chain.is_custom()) {
            (None, true) => {
                return Err(ConfigError::invalid(
                    ["chain.network", "upstream"],
                    "a custom network has no default peer; add `[upstream]` with a \
                     `peer_address`, or `type = \"emulator\"`",
                ))
            }
            (Some(UpstreamConfig::Peer(PeerConfig { peer_address: None })), true) => {
                return Err(ConfigError::invalid(
                    ["chain.network", "upstream"],
                    "`upstream.peer_address` is required on a custom network",
                ))
            }
            (Some(UpstreamConfig::Emulator(_)), false) => {
                return Err(ConfigError::invalid(
                    ["chain.network", "upstream.type"],
                    "`upstream.type = \"emulator\"` is only valid with `network = \"custom\"`",
                ))
            }
            _ => (),
        }

        match (self.history.mode, self.history.window) {
            (HistoryMode::Rolling, None) => {
                return Err(ConfigError::invalid(
                    ["history.mode", "history.window"],
                    "`history.mode = \"rolling\"` requires a `window`, such as \
                     `{ unit = \"slot\", value = 2592000 }`",
                ))
            }
            (HistoryMode::None | HistoryMode::Full, Some(_)) => {
                return Err(ConfigError::invalid(
                    ["history.mode", "history.window"],
                    "`history.window` is only valid with `mode = \"rolling\"`",
                ))
            }
            _ => (),
        }

        let queries = self.serve.max_concurrent_queries;
        if !ServeConfig::MAX_CONCURRENT_QUERIES.contains(&queries) {
            return Err(ConfigError::invalid(
                ["serve.max_concurrent_queries"],
                format!(
                    "`serve.max_concurrent_queries` must be between {} and {}, found {queries}",
                    ServeConfig::MAX_CONCURRENT_QUERIES.start(),
                    ServeConfig::MAX_CONCURRENT_QUERIES.end(),
                ),
            ));
        }

        let (state, archive) = (&self.storage.state, &self.storage.archive);
        let stray: Vec<(&str, &str)> = [
            (
                state.kind,
                "storage.state.type",
                "storage.state.path",
                state.path.is_some(),
            ),
            (
                archive.kind,
                "storage.archive.type",
                "storage.archive.path",
                archive.path.is_some(),
            ),
            (
                archive.kind,
                "storage.archive.type",
                "storage.archive.blocks_path",
                archive.blocks_path.is_some(),
            ),
        ]
        .into_iter()
        .filter(|(kind, _, _, set)| *kind == StoreType::Ephemeral && *set)
        .map(|(_, kind, key, _)| (kind, key))
        .collect();

        if !stray.is_empty() {
            let names: Vec<String> = stray.iter().map(|(_, key)| format!("`{key}`")).collect();
            return Err(ConfigError::invalid(
                stray.iter().flat_map(|(kind, key)| [*kind, *key]),
                format!(
                    "an ephemeral store takes no other field; remove {}",
                    names.join(", ")
                ),
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
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("the environment variable {0} is not valid UTF-8")]
    NonUnicodeOverride(String),

    #[error(
        "the environment variable {0} cannot name a key; after `DOLOS_`, use only letters, digits, \
         `_` and `-`, with `__` between levels"
    )]
    MalformedOverride(String),

    #[error(
        "the environment variables {table} and {key} overlap, since the first sets the table \
         holding the second's key; unset one of them"
    )]
    OverlappingOverrides { table: String, key: String },

    #[error("invalid configuration: {message}")]
    Invalid {
        /// The keys the broken rule is about, as the file names them.
        keys: Vec<String>,
        message: String,
    },
}

impl ConfigError {
    /// A rule the configuration breaks, about `keys`.
    pub fn invalid(
        keys: impl IntoIterator<Item = impl Into<String>>,
        message: impl Into<String>,
    ) -> Self {
        Self::Invalid {
            keys: keys.into_iter().map(Into::into).collect(),
            message: message.into(),
        }
    }
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

/// Tuning fields a store table of an earlier file set one by one.
const RETIRED_TUNING: [&str; 6] = [
    "cache",
    "max_journal_size",
    "flush_on_commit",
    "l0_threshold",
    "worker_threads",
    "memtable_size_mb",
];

/// Keys only a v1 or v2.0.0-alpha.0 file has.
const LEGACY_KEYS: [&str; 3] = ["genesis", "storage.version", "chain.type"];

const ENV_PREFIX: &str = "DOLOS";
const ENV_NESTING: &str = "__";

/// Read the configuration: `path`, or else `./dolos.toml`, and `DOLOS_`
/// environment variables over it.
///
/// A variable names its key with `__` between levels, as in
/// `DOLOS_UPSTREAM__PEER_ADDRESS`, in letters, digits, `_` and `-` only; one
/// without `__` is not an override, which leaves the command inputs such as
/// `DOLOS_MITHRIL_AGGREGATOR` alone. A variable that sets a table is refused
/// beside one that sets a key in it.
///
/// An error the variables cause names them rather than the file:
/// - each variable the configuration reads without;
/// - else the file is read with the variables its errors are about, added one
///   error at a time. A variable about to be added that is refused over a
///   configuration holding nothing else, such as an unknown key or a value of
///   the wrong type, is named with that error. An error that stands is the
///   variables' when every key it is about is theirs: one they set, or one
///   nobody sets inside a table only they hold. Otherwise it is the file's;
/// - when the file reads with those variables, every variable is named.
///
/// A variable that sets a key to the text the file has there changes nothing,
/// so it is neither named nor read. When both the file and a variable it needs
/// are refused, either error may be the one reported, each under its own
/// source.
///
/// `compatibility` is read from the file alone. Genesis paths resolve against
/// the file's directory.
pub fn load(path: Option<&Path>) -> Result<RootConfig, ConfigError> {
    load_checked(path, |_| Ok(()))
}

/// [`load`], with `check` among the rules the result must pass, so that an
/// override it refuses is named like one [`RootConfig::validate`] refuses.
/// `check` reports a broken rule as [`ConfigError::Invalid`] with the keys
/// it is about.
pub fn load_checked(
    path: Option<&Path>,
    check: impl Fn(&RootConfig) -> Result<(), ConfigError>,
) -> Result<RootConfig, ConfigError> {
    let path = path.unwrap_or(Path::new(DEFAULT_CONFIG_FILE));

    if !path.is_file() {
        return Err(ConfigError::NotFound(path.to_path_buf()));
    }

    let file = || ::config::File::from(path).format(::config::FileFormat::Toml);

    let document = ::config::Config::builder()
        .add_source(file())
        .build()
        .map_err(|source| Rejection::Parse(source).in_file(path))?;

    check_compatibility(&document)?;
    reject_moved_sections(&document)?;

    let overrides = environment_overrides()?;
    let read = |overrides: ::config::Map<String, String>| -> Result<RootConfig, Rejection> {
        let merged = ::config::Config::builder()
            .add_source(file())
            .add_source(environment(overrides))
            .build()
            .map_err(Rejection::Parse)?;

        check_tables(&merged).map_err(Rejection::Check)?;

        let mut config = merged
            .try_deserialize::<RootConfig>()
            .map_err(Rejection::Parse)?;

        config.resolve_paths(path.parent().unwrap_or(Path::new("")));
        config.validate().map_err(Rejection::Check)?;
        check(&config).map_err(Rejection::Check)?;

        Ok(config)
    };

    if overrides.is_empty() {
        return read(overrides).map_err(|rejection| rejection.in_file(path));
    }

    let rejection = match read(overrides.clone()) {
        Ok(config) => return Ok(config),
        Err(rejection) => rejection,
    };

    let mut variables: Vec<String> = overrides
        .iter()
        .filter(|(name, value)| !repeats_the_file(&document, name, value))
        .map(|(name, _)| name.clone())
        .collect();
    variables.sort();

    let only = |chosen: &[String]| -> ::config::Map<String, String> {
        overrides
            .iter()
            .filter(|(name, _)| chosen.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    };

    // To blame: each variable the configuration reads without.
    let culprits: Vec<String> = variables
        .iter()
        .filter(|variable| {
            let others: Vec<String> = variables
                .iter()
                .filter(|other| other != variable)
                .cloned()
                .collect();
            read(only(&others)).is_ok()
        })
        .cloned()
        .collect();

    if !culprits.is_empty() {
        return Err(rejection.in_environment(culprits));
    }

    // Otherwise the file is read with the variables its errors are about,
    // added one error at a time, until an error stands with all of them.
    let mut needed: Vec<String> = Vec::new();

    loop {
        let error = match read(only(&needed)) {
            Ok(_) => return Err(rejection.in_environment(variables)),
            Err(error) => error,
        };

        let added: Vec<String> = variables
            .iter()
            .filter(|name| !needed.contains(name) && error.is_about(&override_key(name)))
            .cloned()
            .collect();

        if added.is_empty() {
            return Err(match error.owners(&document, &needed) {
                Some(owners) => error.in_environment(owners),
                None => error.in_file(path),
            });
        }

        for name in &added {
            if let Some(own) = refused_alone(name, &overrides[name]) {
                return Err(Rejection::Parse(own).in_environment(vec![name.clone()]));
            }
        }

        needed.extend(added);
    }
}

/// A variable's own error: an unknown key or a value of the wrong type, which
/// it is refused for over a configuration holding nothing else. Keys missing
/// beside it and fields that do not go together depend on the file.
fn refused_alone(name: &str, value: &str) -> Option<::config::ConfigError> {
    let bare = format!(
        "compatibility = \"{}\"\n\n[chain]\nprotocol = \"cardano\"\nnetwork = \"mainnet\"\n",
        Compatibility::current().as_str()
    );
    let variable = std::iter::once((name.to_string(), value.to_string())).collect();

    let document = match ::config::Config::builder()
        .add_source(::config::File::from_str(&bare, ::config::FileFormat::Toml))
        .add_source(environment(variable))
        .build()
    {
        Ok(document) => document,
        Err(error) => return Some(error),
    };

    if check_tables(&document).is_err() {
        return None;
    }

    match document.try_deserialize::<RootConfig>() {
        Ok(_) | Err(::config::ConfigError::NotFound(_)) => None,
        Err(error) => Some(error),
    }
}

/// Why one reading of the configuration was refused, before it is put down to
/// the file or to the overrides.
enum Rejection {
    Parse(::config::ConfigError),
    Check(ConfigError),
}

impl Rejection {
    /// Whether the refusal is about `key`, which a variable sets: a rule
    /// spanning it, or a parse error about the value at it or the key it
    /// lacks.
    fn is_about(&self, key: &str) -> bool {
        match self {
            Self::Parse(error) => replaces(key, error),
            Self::Check(ConfigError::Invalid { keys, .. }) => keys
                .iter()
                .any(|rule| is_within(rule, key) || is_within(key, rule)),
            Self::Check(_) => false,
        }
    }

    /// The keys the refusal is about: a broken rule's, the key a value is
    /// refused at, or the one missing.
    fn keys(&self) -> Vec<&str> {
        use ::config::ConfigError as E;

        match self {
            Self::Parse(
                E::NotFound(key) | E::Type { key: Some(key), .. } | E::At { key: Some(key), .. },
            ) => vec![key.as_str()],
            Self::Check(ConfigError::Invalid { keys, .. }) => {
                keys.iter().map(String::as_str).collect()
            }
            _ => Vec::new(),
        }
    }

    /// The variables, of those `read`, the refusal is put down to, or `None`
    /// for the file. Each key it is about must be theirs: one a variable sets,
    /// at it or above it, or else, going up from the key, the first that the
    /// file or one of them has a value in must be one only they do.
    fn owners(&self, document: &::config::Config, read: &[String]) -> Option<Vec<String>> {
        let keys = self.keys();

        if keys.is_empty() {
            return None;
        }

        let setting = |within: &dyn Fn(&str) -> bool| -> Vec<String> {
            read.iter()
                .filter(|name| within(&override_key(name)))
                .cloned()
                .collect()
        };

        let mut owners = Vec::new();

        for key in keys {
            let mut found = setting(&|set| is_within(key, set));
            let mut table = Some(key);

            while found.is_empty() {
                let current = table?;

                if document.get::<::config::Value>(current).is_ok() {
                    return None;
                }

                found = setting(&|set| is_within(set, current));
                table = current.rsplit_once('.').map(|(parent, _)| parent);
            }

            owners.extend(found);
        }

        owners.sort();
        owners.dedup();
        Some(owners)
    }

    fn in_file(self, path: &Path) -> ConfigError {
        match self {
            Self::Parse(source) => ConfigError::Read {
                path: path.to_path_buf(),
                source,
            },
            Self::Check(error) => error,
        }
    }

    fn in_environment(self, variables: Vec<String>) -> ConfigError {
        let source: Box<dyn std::error::Error + Send + Sync> = match self {
            Self::Parse(source) => Box::new(source),
            Self::Check(error) => Box::new(error),
        };

        ConfigError::Environment { variables, source }
    }
}

/// Whether an override of `key` takes the place of what the parse `error` is
/// about: the value at the error's key, or the key it lacks. An unknown key in
/// a table stands beside an override of another key in it.
fn replaces(key: &str, error: &::config::ConfigError) -> bool {
    use ::config::ConfigError as E;

    match error {
        E::NotFound(missing) => is_within(missing, key) || is_within(key, missing),
        E::Type {
            key: Some(found), ..
        }
        | E::At {
            key: Some(found), ..
        } => is_within(found, key),
        _ => false,
    }
}

/// Whether `key` is `table` or a key inside it.
fn is_within(key: &str, table: &str) -> bool {
    key.strip_prefix(table)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(['.', '[']))
}

/// The key an override variable sets, as the configuration names it.
fn override_key(variable: &str) -> String {
    variable[ENV_PREFIX.len() + 1..]
        .to_lowercase()
        .replace(ENV_NESTING, ".")
}

/// Whether the variable `name` sets its key to the text the file has there.
/// Only a string in the file is compared, since config renders a float such
/// as `32.0` as `32`, the text of a whole number; a number or a flag in the
/// file is never taken as repeated.
fn repeats_the_file(document: &::config::Config, name: &str, value: &str) -> bool {
    document
        .get::<::config::Value>(&override_key(name))
        .is_ok_and(|found| matches!(found.kind, ::config::ValueKind::String(text) if text == value))
}

/// Refuses fields of a table that do not go together, naming their keys.
/// Deserializing refuses them too, but names only the table, which cannot
/// tell the file from a variable that sets a key in it. An absent or
/// malformed table is left to deserializing.
fn check_tables(document: &::config::Config) -> Result<(), ConfigError> {
    fn table<Raw, Table>(document: &::config::Config, key: &str) -> Result<(), ConfigError>
    where
        Raw: serde::de::DeserializeOwned,
        Table: TryFrom<Raw, Error = Mismatch>,
    {
        match document.get::<Raw>(key) {
            Ok(raw) => Table::try_from(raw)
                .map(|_| ())
                .map_err(|mismatch| mismatch.at(key)),
            Err(_) => Ok(()),
        }
    }

    table::<RawUpstream, UpstreamConfig>(document, "upstream")?;

    for (store, fields) in [
        ("storage.state", &["path", "preset"][..]),
        ("storage.archive", &["path", "blocks_path", "preset"]),
    ] {
        refuse_retired_tuning(document, store, fields)?;
    }

    table::<RawStateStore, StateStoreConfig>(document, "storage.state")?;
    table::<RawArchiveStore, ArchiveStoreConfig>(document, "storage.archive")
}

/// Refuses the per-store tuning fields of earlier files by name, where
/// deserializing would call them unknown: a Fjall store's preset sets all of
/// it, and an ephemeral store takes none of it, nor any of the table's
/// `fields`. A `type` that names no store is refused first, since
/// deserializing could otherwise report either error.
fn refuse_retired_tuning(
    document: &::config::Config,
    table: &str,
    fields: &[&'static str],
) -> Result<(), ConfigError> {
    let is_set = |field: &str| {
        document
            .get::<::config::Value>(&format!("{table}.{field}"))
            .is_ok()
    };

    let retired: Vec<&'static str> = RETIRED_TUNING
        .iter()
        .copied()
        .filter(|field| is_set(field))
        .collect();

    if retired.is_empty() {
        return Ok(());
    }

    let kind = format!("{table}.type");
    let ephemeral = is_set("type")
        && match document.get::<StoreKind>(&kind) {
            Ok(kind) => kind == StoreKind::Ephemeral,
            Err(error) => return Err(ConfigError::invalid([kind], error.to_string())),
        };

    let names = |set: &[&str]| -> String {
        let names: Vec<String> = set.iter().map(|name| format!("`{name}`")).collect();
        names.join(", ")
    };

    let mismatch = if ephemeral {
        let set: Vec<&'static str> = fields
            .iter()
            .copied()
            .filter(|field| is_set(field))
            .chain(retired)
            .collect();
        let message = format!(
            "an ephemeral store takes no other field; remove {}",
            names(&set)
        );
        Mismatch::new(std::iter::once("type").chain(set), message)
    } else {
        let message = format!("tuning is set by `preset`; remove {}", names(&retired));
        Mismatch::new(retired, message)
    };

    Err(mismatch.at(table))
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
/// in place unnoticed. So is one with a character config does not take as part
/// of a key name, such as `.` or `[`, which it would read as a path or a
/// subscript; letters, digits, `_` and `-` are kept, so that a key such as the
/// `offchain-failures` cache namespace can be set. So is a pair where one sets
/// a table and the other a key in it, which would apply in no fixed order.
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

        let (Ok(key), Ok(value)) = (key.into_string(), value.into_string()) else {
            return Err(ConfigError::NonUnicodeOverride(name));
        };

        if !key[prefix.len()..]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(ConfigError::MalformedOverride(key));
        }

        // Names are read in lowercase, so of two that differ only in case,
        // the later one in the environment wins.
        let lowercase = key.to_lowercase();
        overrides.retain(|name: &String, _| name.to_lowercase() != lowercase);
        overrides.insert(key, value);
    }

    let mut keys: Vec<(String, &String)> = overrides
        .keys()
        .map(|name| (override_key(name), name))
        .collect();
    keys.sort();

    for (table, table_variable) in &keys {
        let nested = keys
            .iter()
            .find(|(key, _)| key != table && is_within(key, table));

        if let Some((_, key_variable)) = nested {
            return Err(ConfigError::OverlappingOverrides {
                table: table_variable.to_string(),
                key: key_variable.to_string(),
            });
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
            Err(ConfigError::Invalid { message, .. }) => message,
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

        let err = serde_json::from_value::<ArchiveStoreConfig>(json!({ "compression": "zstd" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("compression"), "{err}");
    }

    fn table_check(toml: &str) -> Result<(), ConfigError> {
        let document = ::config::Config::builder()
            .add_source(::config::File::from_str(toml, ::config::FileFormat::Toml))
            .build()
            .unwrap();

        check_tables(&document)
    }

    #[test]
    fn stores_refuse_tuning_a_preset_sets() {
        let retired = [
            "cache",
            "max_journal_size",
            "flush_on_commit",
            "l0_threshold",
            "worker_threads",
            "memtable_size_mb",
        ];

        for table in ["storage.state", "storage.archive"] {
            for field in retired {
                for selection in ["preset = \"standard\"", "type = \"fjall\""] {
                    let toml = format!("[{table}]\n{selection}\n{field} = 1\n");

                    match table_check(&toml) {
                        Err(ConfigError::Invalid { keys, message }) => {
                            assert_eq!(keys, [format!("{table}.{field}")]);
                            assert_eq!(
                                message,
                                format!(
                                    "in `[{table}]`, tuning is set by `preset`; remove `{field}`"
                                )
                            );
                        }
                        other => panic!("{toml}: expected a refusal, got {other:?}"),
                    }
                }
            }
        }

        let err = table_check("[storage.state]\ncache = 1024\nl0_threshold = 8\n")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("tuning is set by `preset`; remove `cache`, `l0_threshold`"),
            "{err}"
        );
    }

    #[test]
    fn an_ephemeral_store_is_not_pointed_at_a_preset() {
        let cases = [
            (
                "[storage.archive]\ntype = \"ephemeral\"\ncache = 64\n",
                &["storage.archive.type", "storage.archive.cache"][..],
                "in `[storage.archive]`, an ephemeral store takes no other field; remove `cache`",
            ),
            (
                "[storage.state]\ntype = \"ephemeral\"\npath = \"x\"\ncache = 1\n",
                &[
                    "storage.state.type",
                    "storage.state.path",
                    "storage.state.cache",
                ],
                "in `[storage.state]`, an ephemeral store takes no other field; remove `path`, \
                 `cache`",
            ),
            (
                "[storage.archive]\ntype = \"ephemeral\"\npath = \"a\"\nblocks_path = \"b\"\n\
                 preset = \"standard\"\nl0_threshold = 8\n",
                &[
                    "storage.archive.type",
                    "storage.archive.path",
                    "storage.archive.blocks_path",
                    "storage.archive.preset",
                    "storage.archive.l0_threshold",
                ],
                "in `[storage.archive]`, an ephemeral store takes no other field; remove `path`, \
                 `blocks_path`, `preset`, `l0_threshold`",
            ),
        ];

        for (toml, expected_keys, expected) in cases {
            match table_check(toml) {
                Err(ConfigError::Invalid { keys, message }) => {
                    assert_eq!(keys, expected_keys, "{toml}");
                    assert_eq!(message, expected, "{toml}");
                }
                other => panic!("{toml}: expected a refusal, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_store_type_is_refused_before_its_tuning() {
        match table_check("[storage.archive]\ntype = \"Ephemeral\"\ncache = 64\n") {
            Err(ConfigError::Invalid { keys, message }) => {
                assert_eq!(keys, ["storage.archive.type"]);
                assert!(
                    message.contains("variant constructor Ephemeral"),
                    "{message}"
                );
                assert!(!message.contains("cache"), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn unknown_store_fields_list_only_current_ones() {
        let err = serde_json::from_value::<StateStoreConfig>(json!({ "backend": "fjall" }))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "unknown field `backend`, expected one of `type`, `path`, `preset`"
        );

        let err = serde_json::from_value::<ArchiveStoreConfig>(json!({ "backend": "fjall" }))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "unknown field `backend`, expected one of `type`, `path`, `blocks_path`, `preset`"
        );
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
    fn concurrent_queries_are_a_whole_number() {
        let serve = |value| {
            serde_json::from_value::<ServeConfig>(json!({ "max_concurrent_queries": value }))
        };

        assert_eq!(serve(json!(8)).unwrap().max_concurrent_queries, 8);
        assert_eq!(serve(json!("8")).unwrap().max_concurrent_queries, 8);

        for value in [json!(64.4), json!(0.6), json!(-1), json!("8.5")] {
            let err = serve(value.clone()).unwrap_err().to_string();
            assert!(
                err.contains("a non-negative whole number"),
                "{value}: {err}"
            );
        }
    }

    #[test]
    fn an_ephemeral_store_with_a_path_is_invalid() {
        let mut config = preview();
        config.storage.state = StateStoreConfig {
            path: Some("state".into()),
            ..StateStoreConfig::ephemeral()
        };
        assert!(invalid(&config).contains("remove `storage.state.path`"));

        config.storage.state = StateStoreConfig::ephemeral();
        config.storage.archive = ArchiveStoreConfig {
            blocks_path: Some("/hdd/blocks".into()),
            ..ArchiveStoreConfig::ephemeral()
        };
        assert!(invalid(&config).contains("remove `storage.archive.blocks_path`"));

        config.storage.archive.kind = StoreType::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn a_broken_rule_names_its_keys() {
        let mut config = preview();
        config.history.mode = HistoryMode::Rolling;

        match config.validate() {
            Err(ConfigError::Invalid { keys, .. }) => {
                assert_eq!(keys, ["history.mode", "history.window"])
            }
            other => panic!("expected a validation error, got {other:?}"),
        }

        let mismatch = UpstreamConfig::try_from(RawUpstream {
            kind: Some(UpstreamType::Emulator),
            peer_address: None,
            block_production_interval: None,
        })
        .unwrap_err();

        match mismatch.at("upstream") {
            ConfigError::Invalid { keys, message } => {
                assert_eq!(
                    keys,
                    ["upstream.type", "upstream.block_production_interval"]
                );
                assert!(message.starts_with("in `[upstream]`, "), "{message}");
            }
            other => panic!("expected a validation error, got {other:?}"),
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
