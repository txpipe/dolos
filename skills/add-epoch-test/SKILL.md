---
name: add-epoch-test
description: Set up a new epoch_pots integration test with ground truth fixtures from DBSync
---

# Add an Epoch Pots Test

Epoch pots tests compare dolos ledger output against DBSync ground truth for a specific epoch. Each test needs: fixture CSVs from DBSync, a fixtures directory, and a test entry using the `epoch_test!` macro.

## Key Concepts

- **subject_epoch**: The epoch being tested. Dolos runs up to `subject_epoch + 1` so that the subject epoch completes fully.
- **performance_epoch**: `subject_epoch - 2`. This is the epoch whose rewards, delegation, and stake snapshots are compared. Cardano uses a 2-epoch lag for reward distribution.
- **snapshot_epoch**: Same as performance_epoch. Used in delegation/stake fixture filenames (e.g., `delegation-248.csv` for subject epoch 250).
- **seed**: A pre-built dolos state directory that the test copies and resumes from, to avoid replaying the entire chain. Seeds are configured in `xtask.toml` under `[seeds]`.

## Step 1: Verify a seed exists

Check `xtask.toml` for seeds and the constant arrays in `tests/epoch_pots/main.rs`:

```rust
const MAINNET_SEED_EPOCHS: &[u64] = &[200, 270, 300, 340];
const PREVIEW_SEED_EPOCHS: &[u64] = &[500, 700];
const PREPROD_SEED_EPOCHS: &[u64] = &[200];
```

The test picks the nearest lower seed. If no seed is available at or below the subject epoch, you need to create one first (see "Creating a new seed" below).

## Step 2: Generate ground truth fixtures

Run the xtask ground-truth generate command. The output goes directly into the fixtures directory:

```bash
# Create the fixtures directory
mkdir -p tests/epoch_pots/fixtures/{network}-{subject_epoch}

# Generate all ground truth CSVs
cargo run -p xtask -- ground-truth generate \
  --network {network} \
  --subject-epoch {subject_epoch} \
  --output-dir tests/epoch_pots/fixtures/{network}-{subject_epoch}
```

This queries DBSync (connection from `xtask.toml` `[dbsync]` section) and writes:

| File | Content |
|------|---------|
| `epochs.csv` | Treasury, reserves, rewards, utxo, fees, nonce, block_count for epochs 1..=subject |
| `pparams.csv` | Protocol parameters for epochs 1..=subject |
| `eras.csv` | Era boundaries (protocol version, start epoch, lengths) |
| `delegation-{perf}.csv` | Per-pool total stake at performance_epoch |
| `stake-{perf}.csv` | Per-account stake/pool pairs at performance_epoch |
| `rewards.csv` | Per-account rewards (leader/member) at performance_epoch |

Where `{perf}` = `subject_epoch - 2`.

You can regenerate individual datasets with `--only-*` flags:

```bash
cargo run -p xtask -- ground-truth generate \
  --network mainnet --subject-epoch 450 \
  --output-dir tests/epoch_pots/fixtures/mainnet-450 \
  --only-rewards
```

Available flags: `--only-eras`, `--only-epochs`, `--only-pparams`, `--only-delegation`, `--only-stake`, `--only-rewards`.

## Step 3: Add the test entry

Edit `tests/epoch_pots/main.rs`. Add an `epoch_test!` invocation in the test functions section at the bottom, maintaining order by epoch number:

```rust
epoch_test!(test_{network}_{subject}, fixtures_{network}_{subject}, "{network}", {subject}, {perf});
```

**Example** for mainnet epoch 450 (performance epoch 448):

```rust
epoch_test!(test_mainnet_450, fixtures_mainnet_450, "mainnet", 450, 448);
```

The macro arguments are:
1. Test function name: `test_{network}_{subject_epoch}`
2. Fixture module name: `fixtures_{network}_{subject_epoch}` (unused legacy, but must be unique)
3. Network string: `"mainnet"`, `"preprod"`, or `"preview"`
4. Subject epoch number
5. Snapshot/performance epoch number (`subject - 2`)

## Step 4: Verify it compiles

```bash
cargo test --test epoch_pots --no-run
```

## Step 5: Run the test

```bash
# Run the specific test
DOLOS_SEED_DIR=/path/to/seeds cargo test --test epoch_pots test_{network}_{subject} --release -- --nocapture

# The DOLOS_SEED_DIR should contain directories like mainnet-200/, mainnet-300/, etc.
# Each seed directory must have a state/ subdirectory with the dolos state.
```

The test:
1. Copies the nearest seed's `state/` to a temp directory
2. Replays from the Mithril snapshot immutable files up to `subject_epoch + 1`
3. Dumps dolos epochs, pparams, eras, delegation, stake, and rewards CSVs
4. Compares each against the ground truth fixtures
5. Reports diffs (up to 20 per dataset)

Set `EPOCH_POTS_KEEP_DIR=1` to preserve the temp directory for debugging.

## Step 6: Run all tests to check for regressions

```bash
DOLOS_SEED_DIR=/path/to/seeds cargo test --test epoch_pots --release -- --nocapture
```

## Creating a new seed

If no seed covers the target epoch range, create one. A seed is just the dolos `state/` directory at a specific epoch.

1. Create a test instance that stops at the seed epoch:

```bash
cargo xtask test-instance create --network mainnet --epoch {seed_epoch}
```

2. Wait for it to sync (exits with "forced stop epoch reached").

3. Copy the state to the seeds directory:

```bash
mkdir -p /path/to/seeds/mainnet-{seed_epoch}
cp -r {instances_root}/test-mainnet-{seed_epoch}/data/state /path/to/seeds/mainnet-{seed_epoch}/state
```

4. Update `xtask.toml` to register the seed:

```toml
[seeds]
mainnet = "/path/to/seeds/mainnet-{seed_epoch}"
```

5. Add the seed epoch to the constant array in `tests/epoch_pots/main.rs`:

```rust
const MAINNET_SEED_EPOCHS: &[u64] = &[200, 270, 300, 340, {seed_epoch}];
```

## Fixture directory structure

```
tests/epoch_pots/fixtures/{network}-{subject_epoch}/
├── epochs.csv              # Multi-row: all epochs 1..=subject
├── pparams.csv             # Multi-row: all epochs 1..=subject
├── eras.csv                # Era boundaries
├── delegation-{perf}.csv   # Per-pool totals
├── stake-{perf}.csv        # Per-account stake (can be very large)
└── rewards.csv             # Per-account rewards
```

## Common issues

**DBSync timeout on large queries**: Stake and rewards queries for mainnet can be very large. If they timeout, retry or use `--only-stake` / `--only-rewards` separately.

**Missing seed**: If the test panics with "no seed available for {network} <= {epoch}", you need to create a seed at or below the subject epoch.

**Large fixture files**: Stake CSVs for mainnet can be 100MB+. These are checked into the repo under `tests/epoch_pots/fixtures/`. Consider whether the epoch is worth testing (milestone epochs, era boundaries, known bug points are good candidates).
