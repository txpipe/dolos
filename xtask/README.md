# xtask

Custom developer tasks for Dolos. These commands help bootstrap test instances from Mithril snapshots, generate ground-truth fixtures from cardano-db-sync, and benchmark the archive's block segments.

If `cargo xtask` isn't available yet, install the helper once:

```
cargo install --path xtask
```

An installed helper stays at the commit it was built from. Reinstall it with
`--force` after pulling, or run `cargo run -p xtask -- <command>` from the
checkout instead.

## Configuration

Most commands read settings from `xtask.toml` at the repo root:

```toml
instances_root = "./xtask/instances"

[snapshots]
mainnet = "./xtask/snapshots/mainnet"
preview = "./xtask/snapshots/preview"
preprod = "./xtask/snapshots/preprod"

[dbsync]
mainnet_url = "postgresql://user:pass@host:port/dbname"
preview_url = "postgresql://user:pass@host:port/dbname"
preprod_url = "postgresql://user:pass@host:port/dbname"
```

- `instances_root` — directory where test instances are stored.
- `snapshots.*` — directories containing pre-downloaded Mithril snapshots per network.
- `dbsync.*` — PostgreSQL connection URLs for cardano-db-sync per network (needed by `ground-truth generate` and `ground-truth query`).

Template Dolos configs live in `xtask/templates/default-{network}.toml` and are loaded during bootstrap.

## Commands

### `test-instance create`

Create a test instance by bootstrapping a Mithril snapshot into a Dolos instance.

```
cargo xtask test-instance create --network <NETWORK> --epoch <EPOCH>
```

| Flag | Description |
|---|---|
| `--network` | Target network: `mainnet`, `preview`, or `preprod` |
| `--epoch` | Stop syncing at the beginning of this epoch |

- Instances are created under `<instances_root>/test-{network}-{epoch}`.
- If the instance already exists the command fails. Use `test-instance delete` first.

### `test-instance delete`

Delete a test instance directory.

```
cargo xtask test-instance delete --network <NETWORK> --epoch <EPOCH> --yes
```

| Flag | Description |
|---|---|
| `--network` | Target network |
| `--epoch` | Target epoch |
| `--yes` | **Required.** Confirms deletion to prevent accidents |

Only directories whose name starts with `test-` can be deleted (safety check).

### `bootstrap-mithril-local`

Bootstrap a Dolos instance from a pre-downloaded Mithril snapshot. This is the lower-level command used internally by `test-instance create`.

```
cargo xtask bootstrap-mithril-local --network <NETWORK> --stop-epoch <EPOCH>
```

| Flag | Description |
|---|---|
| `--network` | Target network: `mainnet`, `preview`, or `preprod` |
| `--stop-epoch` | Epoch at which to stop syncing |
| `--name` | Optional instance name (defaults to `test-{network}-{epoch}`) |
| `--force` | Overwrite existing instance data |
| `--skip-seed` | Bootstrap from scratch, ignoring a configured `[seeds]` entry |
| `--verbose` | Enable verbose output |

What it does:

1. Writes genesis files (byron.json, shelley.json, alonzo.json, conway.json) into the instance directory.
2. Creates `dolos.toml` in the instance directory from the network template.
3. Runs `dolos bootstrap mithril` with `--skip-download`, `--skip-validation`, and `--retain-snapshot` using the local snapshot.
4. Writes RUPD snapshot CSVs to `<storage.path>/rupd-snapshot/{epoch}-pools.csv` and `{epoch}-accounts.csv`.

### `ground-truth generate`

Generate ground-truth CSV fixtures by querying cardano-db-sync. Requires a running DBSync instance and the corresponding URL in `xtask.toml`.

```
cargo xtask ground-truth generate --network <NETWORK> --subject-epoch <EPOCH> --output-dir <DIR>
```

| Flag | Description |
|---|---|
| `--network` | Target network |
| `--subject-epoch` | Generate ground-truth from origin up to this epoch (inclusive) |
| `--output-dir` | Directory the CSV files are written to |
| `--force` | Overwrite existing ground-truth files |

Output is written to `--output-dir`, the layout of one `ground-truth/{network}-{epoch}` directory of the [epoch tests](../tests/epoch_pots/README.md):

| File | Description |
|---|---|
| `eras.csv` | Protocol version boundaries and era parameters |
| `epochs.csv` | Epoch state (treasury, reserves, rewards, utxo, deposits, fees, nonce) |
| `delegation-{epoch}.csv` | Per-pool total delegation for epoch - 2 |
| `stake-{epoch}.csv` | Per-account stake amounts for epoch - 2 |
| `rewards.csv` | Earned rewards (member/leader) for epoch - 2 |

### `ground-truth query`

Query cardano-db-sync directly for a specific entity and epoch. Results are printed as CSV to stdout.

```
cargo xtask ground-truth query <ENTITY> --network <NETWORK> --epoch <EPOCH>
```

| Argument | Description |
|---|---|
| `<ENTITY>` | One of `pools`, `accounts`, or `rewards` |
| `--network` | Target network |
| `--epoch` | Epoch number to query |

Output fields per entity:

- **pools** — `pool_bech32,pool_hash,total_lovelace`
- **accounts** — `stake,pool,lovelace`
- **rewards** — `stake,pool,amount,type,earned_epoch`

### `perf`

Performance experiments for storage and minibf, with shared load generation,
version comparison and reporting. Start with the [perf overview](perf/README.md),
then choose [storage](perf/storage.md), [minibf](perf/minibf.md) or
[HTTP calibration](perf/http.md). Rust microbenchmarks remain under `cargo bench`.

### `debug`

Maintainer tooling over a node's stores, moved out of the `dolos` CLI. Each
command takes the node's `dolos.toml` with `--config PATH` and opens its stores
as they are on disk, creating and catching up nothing. **Stop the node first**:
a store another process holds is refused with its path.

```
cargo xtask debug find-seq --config dolos.toml --slot 100 --hash <hex>
cargo xtask debug copy-wal --config dolos.toml --output wal.redb [--since SLOT] [--until SLOT]
cargo xtask debug update-entity --config dolos.toml --namespace epochs --cbor-hex <hex> [--key <hex>] [--dry-run]
cargo xtask debug clear-state --config dolos.toml --namespace accounts
```

- `find-seq` prints the WAL entry that holds a block, or `can't find block`.
- `copy-wal` appends a slot range of the WAL to another WAL file. An output
  at an older WAL version is wiped first, with a warning; one at a newer WAL
  version is refused.
- `update-entity` replaces one entity of `epochs`, `accounts`, `pools` or
  `dreps`, printing the value it held; `--dry-run` prints it and writes nothing.
  It writes by default: the old `dolos doctor update-entity` wrote only with
  `--execute`.
- `clear-state` deletes every entity of `eras`, `epochs`, `accounts` or `pools`.

`update-entity` and `clear-state` open the state store alone, leaving the WAL
and the archive untouched.
Logs go to stderr at the config's `[logging]` level and filter, as the node's
do; results go to stdout.

### `store relabel`

Overwrite the compatibility identifier and internal format a state or archive
store recorded, so a development or soak instance survives a prerelease
identifier bump without a re-restore. **This is a development tool, never an
operator migration**: it does not ship in the `dolos` binary, and a store it
relabels is admitted on the developer's word, not because its data matches the
new release.

```
cargo xtask store relabel <PATH> --contract <ID> --format <N> [--yes]
```

- `<PATH>` is a state directory (`<storage.path>/state`) or an archive
  directory (`<storage.path>/archive`); the kind is detected from the store's
  keyspaces and printed. Any other directory is refused.
- Without `--yes` it prints the current and the new values and writes
  nothing. With `--yes` it writes both through the store's own
  `record_identity`, then reopens the store and prints what it reads back.
- `--contract` is taken as given, unchecked against the identifiers this
  release supports; `--format` must be 1 or more.
- The store is opened exclusively. **Stop the node first**: a store another
  process holds is refused and nothing is written.

To run it inside the dolos container image (Debian 12, glibc), build a
linux/amd64 binary in the Rust image of the same Debian release, from the
repository root, then copy it out of the build volume:

```
docker run --rm --platform linux/amd64 -v "$PWD":/src:ro -w /src \
  -v dolos-xtask-amd64:/target -e CARGO_TARGET_DIR=/target \
  rust:1.93-bookworm cargo build --locked --release -p xtask --bin cargo-xtask

docker run --rm -v dolos-xtask-amd64:/target:ro -v "$PWD":/out debian:12-slim \
  cp /target/release/cargo-xtask /out/cargo-xtask-linux-amd64
```

Copy `cargo-xtask-linux-amd64` into a one-off pod running the dolos image,
with the node's volume mounted and the node stopped, and run
`cargo-xtask-linux-amd64 store relabel …` there.

### `e2e-test`

Run the e2e smoke + sync test suites.

```
cargo xtask e2e-test
```

Executes:

- `cargo test --test smoke -- --ignored --nocapture`
- `cargo test --test sync -- --ignored --nocapture`

## Typical workflow

1. Download a Mithril snapshot for the target network into the snapshots directory.
2. Create a test instance to serve as the seed. It stops on the first block
   of `--epoch`, after the boundary before it, so stop it before the subject
   epoch:
   ```
   cargo xtask test-instance create --network preview --epoch 231
   ```
3. Generate ground-truth fixtures (requires DBSync):
   ```
   cargo xtask ground-truth generate --network preview --subject-epoch 233 --output-dir fixtures/ground-truth/preview-233
   ```
4. With a seed and an upstream range beside it (see the [epoch tests README](../tests/epoch_pots/README.md)), compare Dolos output against the ground truth:
   ```
   DOLOS_FIXTURE_DIR=fixtures cargo test --test epoch_pots -- --nocapture
   ```
5. Clean up when done:
   ```
   cargo xtask test-instance delete --network preview --epoch 231 --yes
   ```
