# xtask

Custom developer tasks for Dolos. These commands help bootstrap test instances from Mithril snapshots and generate ground-truth fixtures from cardano-db-sync.

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

### `external-test`

Run the external smoke test suite.

```
cargo xtask external-test
```

Executes `cargo test --test smoke -- --ignored --nocapture`.

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
