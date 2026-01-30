# xtask

This folder contains custom developer tasks for Dolos.

If `cargo xtask` isn't available yet, install the helper once:

```
cargo install --path xtask
```

## Create test instances

Create a test instance and ground-truth fixtures in one step.

Example usage:

```
cargo xtask create-test-instance --network mainnet --epoch 512
cargo xtask create-test-instance --network preview --epoch 233
cargo xtask create-test-instance --network preprod --epoch 98
```

Notes:

- Instances are created under `<instances_root>/test-{network}-{epoch}`.
- If the instance directory already exists, the command fails. Use `delete-test-instance` first.
- Add `--skip-ground-truth` to run bootstrap only.
- Add `--skip-bootstrap` to regenerate ground truth only (instance must exist).

## Delete test instances

```
cargo xtask delete-test-instance --network preview --epoch 233 --yes
```

## Bootstrap local Mithril snapshot (advanced)

Bootstrap a local Mithril snapshot into a named Dolos instance using repo-local defaults.

Defaults are stored in `xtask.toml`:

```toml
instances_root = "./xtask/instances"

[snapshots]
mainnet = "./xtask/snapshots/mainnet"
preview = "./xtask/snapshots/preview"
preprod = "./xtask/snapshots/preprod"
```

Template configs live in `xtask/templates/default-{network}.toml` and are loaded into the config structs before overriding instance-specific values.

Example usage:

```
cargo xtask bootstrap-mithril-local --network mainnet --stop-epoch 512
cargo xtask bootstrap-mithril-local --network preview --stop-epoch 233
cargo xtask bootstrap-mithril-local --network preprod --stop-epoch 98
```

Notes:

- The instance name defaults to `test-{network}-{epoch}`.
- The instance config is written to `<instances_root>/test-{network}-{epoch}/dolos.toml` with storage rooted at `<instances_root>/test-{network}-{epoch}/data`.
- Genesis files are written into the instance root (byron.json, shelley.json, alonzo.json, conway.json).

## Generate ground-truth fixtures (advanced)

```
cargo xtask cardano-ground-truth --network mainnet --epoch 512
```

This command expects the instance directory to already exist.
