//! Cardano integration tests for Dolos.
//!
//! These tests validate Dolos's internal state mutations against ground-truth
//! data from cardano-db-sync using pre-bootstrapped instances.
//!
//! # Setup
//!
//! 1. Create test instances for the target networks/epochs:
//!    ```bash
//!    cargo xtask create-test-instance --network mainnet --epoch 20
//!    cargo xtask create-test-instance --network mainnet --epoch 50
//!    cargo xtask create-test-instance --network mainnet --epoch 100
//!    # ... same for preview and preprod
//!    ```
//!
//! 2. Generate ground-truth from DBSync (optional if you used create-test-instance):
//!    ```bash
//!    cargo xtask cardano-ground-truth --network mainnet --epoch 20
//!    cargo xtask cardano-ground-truth --network mainnet --epoch 50
//!    cargo xtask cardano-ground-truth --network mainnet --epoch 100
//!    # ... same for preview and preprod
//!    ```
//!
//! 3. Run tests:
//!    ```bash
//!    # Run all tests
//!    cargo test --test cardano
//!
//!    # Show the report table
//!    cargo test --test cardano -- --nocapture
//!    ```
//!
//! # File Layout (via xtask.toml)
//!
//! ```text
//! xtask/instances/test-<network>-<epoch>/
//! ├── dolos.toml
//! └── data/
//!     ├── state/
//!     ├── chain/
//!     └── ...
//!
//! xtask/ground-truth/<network>-<epoch>/
//! ├── eras.json      # Vec<EraSummary>
//! └── epochs.json    # Vec<EpochState>
//! ```
//!
//! # Test Matrix
//!
//! Networks: mainnet, preview, preprod
//! Epochs: 20, 50, 100
//!
//! Tests run sequentially in a single test function to avoid parallel
//! access to the same instance.

mod harness;

use crate::harness::{
    instances_root, load_epoch_from_archive, load_epochs_fixture, load_eras_fixture,
    InstanceHandle, TestPaths,
};
use anyhow::Result;
use comfy_table::{presets::UTF8_FULL, Cell, Table};
use dolos_cardano::{EraSummary, FixedNamespace};
use dolos_core::StateStore;
use std::path::PathBuf;

struct InstanceReport {
    name: String,
    display_name: String,
    matches: usize,
    mismatches: usize,
    errors: Vec<String>,
    mismatch_samples: Vec<String>,
}

impl InstanceReport {
    fn new(name: String) -> Self {
        let display_name = name.strip_prefix("test-").unwrap_or(&name).to_string();
        Self {
            name,
            display_name,
            matches: 0,
            mismatches: 0,
            errors: Vec::new(),
            mismatch_samples: Vec::new(),
        }
    }
}

fn compare_eras(
    name: &str,
    instance: &InstanceHandle,
    paths: &TestPaths,
    report: &mut InstanceReport,
) -> Result<()> {
    let expected_eras = load_eras_fixture(&paths.eras_fixture())
        .map_err(|e| anyhow::anyhow!("Failed to load eras fixture for {name}: {e}"))?;

    let actual_eras: Vec<EraSummary> = instance
        .state
        .iter_entities_typed(EraSummary::NS, None)?
        .map(|r| r.map(|(_, era)| era))
        .collect::<std::result::Result<_, _>>()?;

    if actual_eras.is_empty() {
        report.errors.push("no eras found in state".to_string());
        return Ok(());
    }

    let mut actual_by_protocol = std::collections::HashMap::new();
    for era in &actual_eras {
        actual_by_protocol.insert(era.protocol, era);
    }

    for (index, expected) in expected_eras.iter().enumerate() {
        let context = format!("eras[{index}]");
        let actual = match actual_by_protocol.get(&expected.protocol) {
            Some(actual) => *actual,
            None => {
                report
                    .errors
                    .push(format!("missing era for protocol {}", expected.protocol));
                continue;
            }
        };

        compare_fields!(
            context,
            expected,
            actual,
            report,
            [
                ("protocol", expected.protocol, actual.protocol),
                ("epoch_length", expected.epoch_length, actual.epoch_length),
                ("slot_length", expected.slot_length, actual.slot_length),
            ]
        )?;
    }

    Ok(())
}

fn compare_epochs(
    name: &str,
    instance: &InstanceHandle,
    paths: &TestPaths,
    report: &mut InstanceReport,
) -> Result<()> {
    let era_count: usize = instance
        .state
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)?
        .count();

    if era_count == 0 {
        report
            .errors
            .push("no eras found in state; skipping epochs".to_string());
        return Ok(());
    }

    let eras = instance
        .load_chain_summary()
        .map_err(|e| anyhow::anyhow!("chain summary: {e}"))?;

    let expected_epochs = load_epochs_fixture(&paths.epochs_fixture())
        .map_err(|e| anyhow::anyhow!("Failed to load epochs fixture for {name}: {e}"))?;

    for expected in expected_epochs {
        let actual = load_epoch_from_archive(&instance.archive, &eras, expected.number)?;
        let context = format!("epoch {}", expected.number);

        compare_fields!(
            context,
            expected,
            actual,
            report,
            [("number", expected.number, actual.number)]
        )?;

        let expected_pots = &expected.initial_pots;
        let actual_pots = &actual.initial_pots;
        let pots_missing = expected_pots.reserves == 0
            && expected_pots.treasury == 0
            && expected_pots.rewards == 0
            && expected_pots.utxos == 0
            && expected_pots.fees == 0;

        if !pots_missing {
            let pot_fields = [
                (
                    "pots.reserves",
                    expected_pots.reserves,
                    actual_pots.reserves,
                ),
                (
                    "pots.treasury",
                    expected_pots.treasury,
                    actual_pots.treasury,
                ),
                ("pots.utxos", expected_pots.utxos, actual_pots.utxos),
                ("pots.rewards", expected_pots.rewards, actual_pots.rewards),
                ("pots.fees", expected_pots.fees, actual_pots.fees),
            ];

            for (field, expected_value, actual_value) in pot_fields {
                if expected_value != actual_value {
                    report.mismatches += 1;
                    if report.mismatch_samples.len() < 20 {
                        report.mismatch_samples.push(
                            crate::harness::assertions::format_pot_mismatch(
                                &context,
                                field,
                                expected_value,
                                actual_value,
                            ),
                        );
                    }
                } else {
                    report.matches += 1;
                }
            }
        }

        if let (Some(expected_nonces), Some(actual_nonces)) = (&expected.nonces, &actual.nonces) {
            compare_fields!(
                context,
                expected,
                actual,
                report,
                [(
                    "nonces.active",
                    expected_nonces.active,
                    actual_nonces.active
                )]
            )?;
        }
    }

    Ok(())
}

fn ground_truth_compare(name: &str, paths: &TestPaths) -> InstanceReport {
    let mut report = InstanceReport::new(name.to_string());
    let dolos_config = paths.dolos_config();
    let eras_fixture = paths.eras_fixture();
    let epochs_fixture = paths.epochs_fixture();

    if !dolos_config.exists() {
        report.errors.push("missing dolos.toml".to_string());
        return report;
    }

    if !eras_fixture.exists() || !epochs_fixture.exists() {
        report
            .errors
            .push("ground-truth fixtures not found".to_string());
        return report;
    }

    let instance = match InstanceHandle::open(paths) {
        Ok(instance) => instance,
        Err(e) => {
            report.errors.push(format!("failed to open instance: {e}"));
            return report;
        }
    };

    if let Err(e) = compare_eras(name, &instance, paths, &mut report) {
        report.errors.push(format!("era compare failed: {e}"));
    }

    if let Err(e) = compare_epochs(name, &instance, paths, &mut report) {
        report.errors.push(format!("epoch compare failed: {e}"));
    }

    report
}

fn find_test_instances() -> Result<Vec<PathBuf>> {
    let root = instances_root()?;
    let mut instances = Vec::new();

    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = match path.file_name().and_then(|v| v.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if name.starts_with("test-") {
            instances.push(path);
        }
    }

    instances.sort();
    Ok(instances)
}

#[test]
fn cardano_ground_truth_instances() {
    let instances = find_test_instances().unwrap();

    if instances.is_empty() {
        println!("[INFO] No test-* instances found");
        return;
    }

    println!("[INFO] Found {} test instance(s):", instances.len());
    for instance in &instances {
        if let Some(name) = instance.file_name().and_then(|v| v.to_str()) {
            println!("  - {}", name);
        } else {
            println!("  - {}", instance.display());
        }
    }

    let mut reports = Vec::new();
    for instance_root in instances {
        let name = instance_root
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("<unknown>")
            .to_string();
        let paths = TestPaths { instance_root };
        reports.push(ground_truth_compare(&name, &paths));
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Instance", "Matches", "Mismatches", "Errors"]);

    table.set_header(vec!["Instance", "M", "X", "E"]);

    for report in &reports {
        table.add_row(vec![
            Cell::new(&report.display_name),
            Cell::new(report.matches),
            Cell::new(report.mismatches),
            Cell::new(report.errors.len()),
        ]);
    }

    println!("{table}");

    for report in &reports {
        if report.mismatch_samples.is_empty() && report.errors.is_empty() {
            continue;
        }

        println!("[DETAIL] {}", report.display_name);
        for mismatch in &report.mismatch_samples {
            println!("  - {mismatch}");
        }
        for error in &report.errors {
            println!("  - ERROR: {error}");
        }
    }

    let total_mismatches: usize = reports.iter().map(|r| r.mismatches).sum();
    let total_errors: usize = reports.iter().map(|r| r.errors.len()).sum();

    if total_mismatches > 0 || total_errors > 0 {
        panic!(
            "Ground-truth validation failed: {} mismatches, {} errors",
            total_mismatches, total_errors
        );
    }
}
