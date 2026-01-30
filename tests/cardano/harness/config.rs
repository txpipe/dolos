//! Configuration for integration tests using xtask layout.
//!
//! Tests use `xtask.toml` to resolve instance paths: `<instances_root>/test-<network>-<epoch>`
//! Ground-truth fixtures are expected inside the instance folder: `<instance>/ground-truth/`

use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("failed to parse TOML: {0}")]
    ParseToml(#[from] toml::de::Error),
}

/// Network identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Partial xtask.toml config (we only need the instances path).
#[derive(Debug, Deserialize)]
struct XtaskConfig {
    instances_root: PathBuf,
}

impl Default for XtaskConfig {
    fn default() -> Self {
        Self {
            instances_root: PathBuf::from("./xtask/instances"),
        }
    }
}

/// Resolved paths for a test case.
#[derive(Debug, Clone)]
pub struct TestPaths {
    /// Path to the Dolos instance root (contains `dolos.toml`, `data/`, and `ground-truth/`).
    pub instance_root: PathBuf,
}

/// Resolve the instances root directory from xtask.toml.
pub fn instances_root() -> Result<PathBuf, ConfigError> {
    let repo_root = find_repo_root()?;
    let xtask_config = load_xtask_config(&repo_root)?;
    Ok(resolve_path(&repo_root, &xtask_config.instances_root))
}

/// Result of trying to resolve test paths.
pub enum TestPathsResult {
    /// Paths resolved successfully, test can proceed.
    Ready(TestPaths),
    /// Instance not found, test should be skipped.
    InstanceNotFound { path: PathBuf },
}

impl TestPaths {
    /// Try to resolve paths for a given network and epoch.
    ///
    /// Returns `TestPathsResult` indicating whether the test can proceed or should be skipped.
    pub fn try_resolve(network: Network, epoch: u64) -> Result<TestPathsResult, ConfigError> {
        let repo_root = find_repo_root()?;
        let xtask_config = load_xtask_config(&repo_root)?;

        let name = format!("{}-{}", network.as_str(), epoch);
        let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);
        let instance_root = instances_root.join(&name);

        // Check if instance exists
        if !instance_root.exists() {
            return Ok(TestPathsResult::InstanceNotFound {
                path: instance_root,
            });
        }

        Ok(TestPathsResult::Ready(TestPaths { instance_root }))
    }

    /// Path to the instance's dolos.toml configuration.
    pub fn dolos_config(&self) -> PathBuf {
        self.instance_root.join("dolos.toml")
    }

    /// Path to the ground-truth directory inside the instance.
    pub fn ground_truth_dir(&self) -> PathBuf {
        self.instance_root.join("ground-truth")
    }

    /// Path to the eras ground-truth JSON file.
    pub fn eras_fixture(&self) -> PathBuf {
        self.ground_truth_dir().join("eras.json")
    }

    /// Path to the epochs ground-truth JSON file.
    pub fn epochs_fixture(&self) -> PathBuf {
        self.ground_truth_dir().join("epochs.json")
    }
}

/// Find the repository root by looking for Cargo.toml.
fn find_repo_root() -> Result<PathBuf, ConfigError> {
    let mut current = std::env::current_dir()?;

    loop {
        if current.join("Cargo.toml").exists() && current.join("xtask").exists() {
            return Ok(current);
        }

        if !current.pop() {
            // Fallback to current directory
            return Ok(std::env::current_dir()?);
        }
    }
}

/// Load xtask.toml from the repository root.
fn load_xtask_config(repo_root: &Path) -> Result<XtaskConfig, ConfigError> {
    let config_path = repo_root.join("xtask.toml");

    if !config_path.exists() {
        // Use defaults if xtask.toml doesn't exist
        return Ok(XtaskConfig::default());
    }

    let content = std::fs::read_to_string(&config_path)?;
    let config: XtaskConfig = toml::from_str(&content)?;

    Ok(config)
}

/// Resolve a path relative to a base directory.
fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
