//! Instance handle for opening pre-bootstrapped Dolos instances.
//!
//! This module provides `InstanceHandle` which opens the stores of an existing
//! Dolos instance (created via `cargo xtask create-test-instance`).

use dolos::storage;
use dolos_cardano::eras::ChainSummary;
use dolos_cardano::{EraProtocol, EraSummary, FixedNamespace};
use dolos_core::config::RootConfig;
use dolos_core::StateStore as _;
use thiserror::Error;

use super::config::TestPaths;

#[derive(Debug, Error)]
pub enum InstanceError {
    #[error("failed to read config: {0}")]
    ReadConfig(#[from] std::io::Error),

    #[error("failed to parse config: {0}")]
    ParseConfig(#[from] toml::de::Error),

    #[error("failed to open store: {0}")]
    OpenStore(String),

    #[error("state error: {0}")]
    State(#[from] dolos_core::StateError),

    #[error("archive error: {0}")]
    Archive(#[from] dolos_core::ArchiveError),

    #[error("chain error: {0}")]
    Chain(#[from] dolos_core::ChainError),
}

/// Handle to an existing Dolos instance's stores.
///
/// Opens the stores in read-only mode for comparison against ground-truth.
pub struct InstanceHandle {
    pub state: storage::StateStoreBackend,
    pub archive: storage::ArchiveStoreBackend,
}

impl InstanceHandle {
    /// Open an existing instance from the given paths.
    pub fn open(paths: &TestPaths) -> Result<Self, InstanceError> {
        // Load dolos.toml to get storage configuration
        let config_path = paths.dolos_config();
        let config_content = std::fs::read_to_string(&config_path)?;
        let mut config: RootConfig = toml::from_str(&config_content)?;

        if config.storage.path.is_relative() {
            config.storage.path = paths.instance_root.join(&config.storage.path);
        }

        let state = storage::open_state_store(&config)
            .map_err(|e| InstanceError::OpenStore(format!("state: {}", e)))?;

        let archive = storage::open_archive_store(&config)
            .map_err(|e| InstanceError::OpenStore(format!("archive: {}", e)))?;

        Ok(Self { state, archive })
    }

    /// Load the chain summary (era information) from state.
    ///
    /// This replicates `dolos_cardano::eras::load_era_summary` but works directly
    /// with our concrete store types instead of requiring a full Domain implementation.
    pub fn load_chain_summary(&self) -> Result<ChainSummary, InstanceError> {
        let eras = self.state.iter_entities_typed(EraSummary::NS, None)?;

        let mut chain = ChainSummary::default();

        for result in eras {
            let (key, era) = result?;
            let protocol = EraProtocol::from(key);
            chain.append_era(protocol.into(), era);
        }

        Ok(chain)
    }
}
