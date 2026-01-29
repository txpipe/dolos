# xtask

This folder contains custom developer tasks for Dolos.

If `cargo xtask` isn't available yet, install the helper once:

```
cargo install --path xtask
```

## Bootstrap local Mithril snapshot

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
cargo xtask bootstrap-mithril-local --network preview --stop-epoch 233 --name preview-233
cargo xtask bootstrap-mithril-local --network preprod --stop-epoch 98
```

Notes:

- If `--name` is omitted, the instance name defaults to `{network}-{epoch}`.
- The instance config is written to `<instances_root>/<name>/dolos.toml` with storage rooted at `<instances_root>/<name>/data`.
