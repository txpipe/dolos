# xtask

Custom developer tasks for Dolos. These commands help bootstrap test instances from Mithril snapshots, generate ground-truth fixtures from cardano-db-sync, and benchmark the archive's block segments.

If `cargo xtask` isn't available yet, install the helper once:

```
cargo install --path xtask
```

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

What it does:

1. Writes genesis files (byron.json, shelley.json, alonzo.json, conway.json) into the instance directory.
2. Creates `dolos.toml` in the instance directory from the network template.
3. Runs `dolos bootstrap mithril` with `--skip-download`, `--skip-validation`, and `--retain-snapshot` using the local snapshot.
4. Writes RUPD snapshot CSVs to `<storage.path>/rupd-snapshot/{epoch}-pools.csv` and `{epoch}-accounts.csv`.

### `ground-truth generate`

Generate ground-truth CSV fixtures by querying cardano-db-sync. Requires a running DBSync instance and the corresponding URL in `xtask.toml`.

```
cargo xtask ground-truth generate --network <NETWORK> --epoch <EPOCH>
```

| Flag | Description |
|---|---|
| `--network` | Target network |
| `--epoch` | Generate ground-truth from origin up to this epoch (inclusive) |
| `--force` | Overwrite existing ground-truth files |

The instance directory must already exist. Output is written to `<instance>/ground-truth/`:

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

### `archive-bench`

Benchmarks for the archive's compressed block segments: store-level
workloads over the production `dolos-flatfiles` store beside a modelled
sink, node-level workloads that drive `dolos` binaries through their import
and API paths so two revisions can be paired, dictionary training and
evaluation, and a report renderer with gate verdicts.

```
cargo xtask archive-bench bench --preset all --corpus <DIR> --segments 448..451 --work <DIR> --out results.jsonl
cargo xtask archive-bench node --bin baseline=<PATH>:v3 --bin candidate=<PATH> --run <NAME> --immutable <DIR> --genesis <DIR> --work <DIR> --out node.jsonl
cargo xtask archive-bench report results.jsonl node.jsonl
cargo xtask archive-bench train --corpus <DIR> --sample 440..447=1500 --out cardano.dict
cargo xtask archive-bench evaluate --fixture label=<DIR>:448..455 --dictionary bundled
```

Every option, the gates, the corpus requirements and the committed results
are described in [`archive-bench/README.md`](archive-bench/README.md). The
`smoke` preset runs under `cargo test -p xtask`.

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
2. Create a test instance:
   ```
   cargo xtask test-instance create --network preview --epoch 233
   ```
3. Generate ground-truth fixtures (requires DBSync):
   ```
   cargo xtask ground-truth generate --network preview --epoch 233
   ```
4. Compare Dolos output against ground-truth by running the cardano integration tests:
   ```
   cargo test --test cardano
   ```
5. Clean up when done:
   ```
   cargo xtask test-instance delete --network preview --epoch 233 --yes
   ```
