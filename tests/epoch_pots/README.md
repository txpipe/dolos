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

## Running in CI

The [Epoch Tests](../../.github/workflows/epoch-tests.yml) workflow runs one job
per entry of [`matrix.json`](matrix.json). Each job downloads the entry's
seed, ground truth and upstream range from the bucket that the repository's
`S3_BUCKET` variable names, then runs this test on them. It only runs on
dispatch. Its inputs narrow a run to one network, or to the entries that carry
a tag (`conway` marks the subject epochs in the Conway era):

```bash
gh workflow run epoch-tests.yml --ref <branch> -f network=all -f tag=conway
```

An entry names three archives, each holding one directory of the layout above:

| Field | Archive | Holds |
|---|---|---|
| `seed_epoch` | `seeds/{network}-{seed_epoch}.tar` | `{network}-{seed_epoch}/state/` |
| `ground_truth_epoch` | `ground-truth/{network}-{ground_truth_epoch}.tar` | `{network}-{ground_truth_epoch}/*.csv` |
| `upstream_range` | `upstream/{network}-{upstream_range}.tar` | `{network}-{start}-{end}/immutable/` |

The matrix test in this suite checks `matrix.json` without fixtures. Each
seed must come before its subject epoch, each range must cover both, and each
entry must name the seed and the range that the harness would pick if every
fixture were present.

## Producing Fixtures

You need a Dolos checkout at the commit under test and a Mithril snapshot of
the network that reaches past the end of the range. In `xtask.toml` (see the
[xtask README](../../xtask/README.md)), point `snapshots.{network}` at the
snapshot and `dbsync.{network}_url` at a cardano-db-sync for the network.
Run the commands below from the checkout's root. They build xtask from the
checkout with `cargo run -p xtask`: an installed `cargo xtask` may predate
fixes to the fixtures it writes. The examples build the `mainnet-537` entry:
seed 535, range 535-538.

To list the archives a subset needs, here the `conway` entries:

```bash
jq -r '.[] | select((.tags // []) | any(. == "conway"))
  | "seeds/\(.network)-\(.seed_epoch).tar",
    "upstream/\(.network)-\(.upstream_range).tar",
    "ground-truth/\(.network)-\(.ground_truth_epoch).tar"' \
  tests/epoch_pots/matrix.json | sort -u
```

An archive already in the bucket serves only while it matches the code under
test: regenerate a seed after a breaking change to the state format, and
ground truth after a change to its generator. The `conway` subset includes
two older preview entries whose seed, `preview-500`, dates from before
several such state changes, so regenerate that seed too.

Create the archives with `COPYFILE_DISABLE=1 tar --no-xattrs`, as below. On
macOS a plain `tar` adds AppleDouble `._*` files, which the store and the
chunk reader on the runner would take for their own. The workflow also skips
them when it extracts.

### Seeds

A seed is the state store of an instance bootstrapped up to the seed epoch. A
bootstrap with `--stop-epoch S` stops on the first block of epoch S, after
the S-1 boundary. So the seed serves only subject epochs after S. Seeds
follow the state format: regenerate every seed after a breaking change to it.

```bash
cargo run -p xtask -- bootstrap-mithril-local --network mainnet --stop-epoch 535 \
  --name epoch-seeds-mainnet --skip-seed --verbose
mkdir -p seeds/mainnet-535
cp -R <instances_root>/epoch-seeds-mainnet/data/state seeds/mainnet-535/state
COPYFILE_DISABLE=1 tar --no-xattrs -C seeds -cf mainnet-535.tar mainnet-535
```

To take a later seed on the same network, continue that instance rather than
replaying from genesis again:

```bash
cargo run --release --bin dolos -- --config <instances_root>/epoch-seeds-mainnet/dolos.toml \
  bootstrap --verbose mithril --skip-download --skip-validation --retain-snapshot \
  --download-dir <snapshot> --stop-epoch 642 --continue
```

The instance keeps the full archive, so give it the disk of a synced node.

### Upstream Ranges

The range `{start}-{end}` holds the snapshot's immutable chunks from one
before the first chunk of epoch `start` up to the second chunk of epoch
`end + 1`. The replay stops on the first block after the subject epoch, and
the reader skips the highest chunk it finds. From the first Shelley epoch on,
every network has 20 chunks per epoch:

| Network | First chunk of epoch E |
|---|---|
| mainnet (E ≥ 208) | 20E − 3952 |
| preprod (E ≥ 4) | 20E − 76 |
| preview | 20E |

For `mainnet-535-538` these are chunks 6747 to 6829:

```bash
mkdir -p upstream/mainnet-535-538/immutable
for n in $(seq -f %05g 6747 6829); do
  cp <snapshot>/immutable/$n.{chunk,primary,secondary} upstream/mainnet-535-538/immutable/
done
COPYFILE_DISABLE=1 tar --no-xattrs -C upstream -cf mainnet-535-538.tar mainnet-535-538
```

### Ground Truth

```bash
cargo run -p xtask -- ground-truth generate --network mainnet --subject-epoch 537 \
  --output-dir ground-truth/mainnet-537
COPYFILE_DISABLE=1 tar --no-xattrs -C ground-truth -cf mainnet-537.tar mainnet-537
```

### Upload

Upload each archive under its prefix in the bucket:

```bash
aws s3 cp mainnet-535.tar "s3://$S3_BUCKET/seeds/"
aws s3 cp mainnet-535-538.tar "s3://$S3_BUCKET/upstream/"
aws s3 cp mainnet-537.tar "s3://$S3_BUCKET/ground-truth/"
```
