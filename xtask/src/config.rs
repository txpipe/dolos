//! Configuration types and loading for xtask.

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Network identifier used across xtask commands.
#[derive(Clone, Debug, ValueEnum)]
pub enum Network {
    Mainnet,
    Preview,
    Preprod,
}

impl Network {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Preview => "preview",
            Self::Preprod => "preprod",
        }
    }
}

/// Root configuration loaded from `xtask.toml`.
#[derive(Debug, Deserialize)]
pub struct XtaskConfig {
    pub instances_root: PathBuf,
    pub snapshots: SnapshotConfig,
    #[serde(default)]
    pub dbsync: DbSyncConfig,
    #[serde(default)]
    pub seeds: SeedConfig,
}

impl Default for XtaskConfig {
    fn default() -> Self {
        Self {
            instances_root: PathBuf::from("./xtask/instances"),
            snapshots: SnapshotConfig::default(),
            dbsync: DbSyncConfig::default(),
            seeds: SeedConfig::default(),
        }
    }
}

/// Snapshot directory configuration per network.
#[derive(Debug, Deserialize)]
pub struct SnapshotConfig {
    pub mainnet: PathBuf,
    pub preview: PathBuf,
    pub preprod: PathBuf,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mainnet: PathBuf::from("./xtask/snapshots/mainnet"),
            preview: PathBuf::from("./xtask/snapshots/preview"),
            preprod: PathBuf::from("./xtask/snapshots/preprod"),
        }
    }
}

impl SnapshotConfig {
    pub fn path_for_network(&self, network: &Network) -> &PathBuf {
        match network {
            Network::Mainnet => &self.mainnet,
            Network::Preview => &self.preview,
            Network::Preprod => &self.preprod,
        }
    }
}

/// Seed data directory configuration per network.
///
/// When set, the seed directory is copied into the instance's `data/` directory
/// before running the bootstrap command, allowing instances to start from
/// existing data instead of bootstrapping from scratch.
#[derive(Debug, Deserialize, Default)]
pub struct SeedConfig {
    pub mainnet: Option<PathBuf>,
    pub preview: Option<PathBuf>,
    pub preprod: Option<PathBuf>,
}

impl SeedConfig {
    pub fn path_for_network(&self, network: &Network) -> Option<&PathBuf> {
        match network {
            Network::Mainnet => self.mainnet.as_ref(),
            Network::Preview => self.preview.as_ref(),
            Network::Preprod => self.preprod.as_ref(),
        }
    }
}

/// DBSync connection URLs per network.
#[derive(Debug, Deserialize, Default)]
pub struct DbSyncConfig {
    pub mainnet_url: Option<String>,
    pub preview_url: Option<String>,
    pub preprod_url: Option<String>,
}

impl DbSyncConfig {
    pub fn url_for_network(&self, network: &Network) -> Option<&str> {
        match network {
            Network::Mainnet => self.mainnet_url.as_deref(),
            Network::Preview => self.preview_url.as_deref(), // Updated to match xtask.toml
            Network::Preprod => self.preprod_url.as_deref(),
        }
    }
}

/// Load xtask configuration from `xtask.toml` in the repository root.
///
/// Returns default configuration if the file doesn't exist.
pub fn load_xtask_config(repo_root: &Path) -> Result<XtaskConfig> {
    let config_path = repo_root.join("xtask.toml");
    if !config_path.exists() {
        return Ok(XtaskConfig::default());
    }

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading xtask config: {}", config_path.display()))?;
    let config = toml::from_str(&raw).context("parsing xtask.toml")?;
    Ok(config)
}
