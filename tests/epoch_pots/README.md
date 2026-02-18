# Epoch Pots Tests

End-to-end tests that replay Cardano chain data through the Dolos ledger and compare the resulting epoch state (pots, protocol params, eras, delegation, stake, rewards) against ground-truth data extracted from a reference node (e.g. cardano-db-sync).

## Data Sources

The test harness is driven by a single environment variable `DOLOS_FIXTURE_DIR` pointing to a base directory with three subdirectories:

```
$DOLOS_FIXTURE_DIR/
  seeds/
    mainnet-200/
      state/          # fjall state directory
    preview-500/
      state/
  ground-truth/
    mainnet-242/
      epochs.csv
      pparams.csv
      eras.csv
      delegation-240.csv
      stake-240.csv
      rewards.csv
    mainnet-243/
      ...
  upstream/
    mainnet-200-400/
      immutable/      # .chunk / .primary / .secondary files
    preview-500-700/
      immutable/
```

### `seeds/` — Ledger seeds

Pre-built ledger state snapshots used as starting points so the test doesn't have to replay the entire chain from genesis.

Directory names follow the pattern `{network}-{epoch}`. The test automatically picks the highest seed epoch that is `<= subject_epoch`.

### `upstream/` — Raw block data

Cardano immutable DB chunks scoped to epoch ranges. These provide the actual blocks the ledger replays.

Directory names follow the pattern `{network}-{start}-{end}`. The test finds the directory whose range covers the subject epoch.

### `ground-truth/` — Ground-truth data

CSV files with expected values, one directory per epoch being tested.

Directory names follow the pattern `{network}-{epoch}`. The test discovers all ground-truth directories and runs one comparison per entry.

## How a Test Run Works

1. **Discovery** — the test scans all three subdirectories to build lookup tables of available seeds, upstream ranges, and ground truths.
2. **For each ground truth** (`{network}-{epoch}`):
   a. Select the nearest seed with epoch `<= subject_epoch` and copy its state into a temp directory.
   b. Find the upstream directory whose `[start, end]` range covers the subject epoch.
   c. Configure a `LedgerHarness` with the seed state and upstream immutable files, set to stop after the subject epoch completes.
   d. Replay blocks through the ledger, capturing epoch state, delegation/stake snapshots (RUPD), and applied rewards (EWRAP) as CSV dumps.
   e. Compare each dump against the corresponding ground-truth CSV; collect mismatches.
3. **Report** — if any entry produced mismatches the test panics with a summary of all failures.

## Running Locally

```bash
export DOLOS_FIXTURE_DIR=/path/to/fixtures

cargo test --test epoch_pots -- --nocapture
```

Set `EPOCH_POTS_KEEP_DIR=1` to preserve the temporary working directories for debugging.
