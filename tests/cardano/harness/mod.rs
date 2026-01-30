//! Integration test harness for Cardano-specific tests.
//!
//! This module provides utilities for running integration tests against
//! pre-bootstrapped Dolos instances with ground-truth from DBSync.
//!
//! # Test Setup
//!
//! 1. Create a test instance:
//!    ```bash
//!    cargo xtask create-test-instance --network mainnet --epoch 20
//!    ```
//!
//! 2. Generate ground-truth (optional if you used create-test-instance):
//!    ```bash
//!    cargo xtask cardano-ground-truth --network mainnet --epoch 20
//!    ```
//!
//! 3. Run tests:
//!    ```bash
//!    cargo test --test cardano
//!    ```
//!
//! # File Layout (via xtask.toml)
//!
//! ```text
//! xtask/instances/test-<network>-<epoch>/
//! ├── dolos.toml
//! ├── data/
//! │   ├── state/
//! │   ├── archive/
//! │   └── ...
//! └── ground-truth/
//!     ├── eras.json      # Vec<EraSummary>
//!     └── epochs.json    # Vec<EpochState>
//! ```

pub mod assertions;
pub mod config;
pub mod instance;

pub use assertions::{load_epoch_from_archive, load_epochs_fixture, load_eras_fixture};
pub use config::{instances_root, Network, TestPaths, TestPathsResult};
pub use instance::InstanceHandle;
