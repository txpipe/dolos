# Cardano Integration Tests

This directory contains integration tests for validating Dolos's internal state mutations against ground-truth data from cardano-db-sync.

## Overview

These tests:
1. Open pre-bootstrapped Dolos instances (created via `cargo xtask create-test-instance`)
2. Compare the resulting Dolos state against ground-truth JSON fixtures
3. Validate era summaries and epoch logs

## Architecture

Tests use **pre-bootstrapped instances** rather than importing snapshots during test runs. This provides:
- Faster test execution (no import overhead)
- Reusable instances across test runs
- Separation of concerns (bootstrap vs. validation)

### Directory Layout

Each instance is a self-contained folder with Dolos data and ground-truth fixtures:

```
<repo_root>/
├── xtask.toml                    # Configuration (optional, uses defaults)
└── xtask/
    └── instances/                # Pre-bootstrapped Dolos instances
        ├── test-mainnet-20/      # Instance for mainnet epoch 20
        │   ├── dolos.toml        # Dolos configuration
        │   ├── data/             # Storage databases (state, archive, etc.)
        │   └── ground-truth/     # Ground-truth fixtures
        │       ├── eras.json     # Vec<EraSummary>
        │       └── epochs.json   # Vec<EpochState>
        ├── test-preview-50/
        └── ...
```

### xtask.toml Configuration

```toml
# Root directory for pre-bootstrapped instances
instances_root = "./xtask/instances"

# DBSync connection strings for generating ground-truth (optional)
[dbsync]
mainnet = "postgres://user:pass@host:port/mainnet"
preview = "postgres://user:pass@host:port/preview"
preprod = "postgres://user:pass@host:port/preprod"
```

## Running Tests

Tests scan the instances root and evaluate **every** subdirectory that starts with `test-`.
If a `test-` instance is missing required artifacts (dolos.toml or ground-truth), the test fails.

```bash
# Run all Cardano integration tests
cargo test --test cardano

# Show the per-instance report table
cargo test --test cardano -- --nocapture
```

## Preparing Test Data

### 1. Bootstrap a Dolos Instance

Use the xtask command to bootstrap an instance that stops at a specific epoch:

```bash
# Create test instances (bootstrap + ground-truth)
cargo xtask create-test-instance --network mainnet --epoch 20
cargo xtask create-test-instance --network preview --epoch 50
```

This creates an instance at `xtask/instances/test-{network}-{epoch}/` with:
- `dolos.toml` - Dolos configuration
- `byron.json`, `shelley.json`, `alonzo.json`, `conway.json` - Network genesis files
- `data/` - Populated storage databases

### 2. Generate Ground-Truth Fixtures

#### Option A: From DBSync (Recommended)

Use the xtask command to query cardano-db-sync and generate fixtures:

```bash
# Generate ground-truth for mainnet epoch 20
cargo xtask cardano-ground-truth --network mainnet --epoch 20
```

This requires:
1. A running cardano-db-sync instance with data up to the target epoch
2. Connection URL configured in `xtask.toml` under `[dbsync]`

The fixtures will be written to `xtask/instances/test-{network}-{epoch}/ground-truth/`.

#### Option B: From a Trusted Dolos Instance

If you have a verified Dolos instance, you can export fixtures:

```bash
# Export era summaries
dolos --config /path/to/dolos.toml data dump-state \
  --namespace eras --format json --count 100 > eras.json

# Export all epoch states
dolos --config /path/to/dolos.toml data dump-logs \
  --namespace epochs --format json --all > epochs.json
```

Place the files in `xtask/instances/test-{network}-{epoch}/ground-truth/`.

### 3. Verify Setup

Check that all paths are correctly set up:

```bash
# Instance should exist
ls xtask/instances/test-mainnet-20/dolos.toml

# Ground-truth should exist inside instance
ls xtask/instances/test-mainnet-20/ground-truth/eras.json
ls xtask/instances/test-mainnet-20/ground-truth/epochs.json
```

## Test Structure

```
tests/cardano/
├── main.rs           # Test entry point, scans test-* instances
└── harness/          # Test utilities
    ├── mod.rs        # Module exports
    ├── config.rs     # Path resolution from xtask.toml
    ├── instance.rs   # Opens pre-bootstrapped instances
    └── assertions.rs # Compare macro + fixture loaders
```

## Ground-Truth Format

### eras.json

Array of `EraSummary` objects:

```json
[
  {
    "protocol": 1,
    "start": { "epoch": 0, "slot": 0, "timestamp": 1506203091 },
    "end": { "epoch": 208, "slot": 4492800, "timestamp": 1596059091 },
    "epoch_length": 21600,
    "slot_length": 20000
  },
  ...
]
```

### epochs.json

Array of `EpochState` objects:

```json
[
  {
    "number": 0,
    "initial_pots": {
      "reserves": 13887515269916290,
      "treasury": 0,
      "utxos": 31112484730083710,
      "rewards": 0,
      "fees": 0,
      "pool_count": 0,
      "account_count": 0,
      "drep_deposits": 0,
      "proposal_deposits": 0
    },
    "nonces": {
      "candidate": "1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81"
    }
  },
  ...
]
```

## Assertions

The test compares a subset of fields from the ground-truth fixtures against the instance data.

### eras

- `protocol`
- `epoch_length`
- `slot_length`

Only eras present in `eras.json` are compared (earlier eras in state are ignored).

### epochs

- `number`
- `initial_pots.reserves`
- `initial_pots.treasury`
- `initial_pots.utxos`
- `initial_pots.rewards`
- `initial_pots.fees`
- `nonces.candidate`

If all pot values in a fixture are zero, pot comparisons are skipped for that epoch.

## DBSync Source of Truth

Ground-truth queries live in the xtask implementation:
- `xtask/src/ground_truth/mod.rs`

## Adding New Tests

Create a new `test-*` instance directory with `dolos.toml`, `data/`, and `ground-truth/`.
The test runner will automatically discover and evaluate it.
