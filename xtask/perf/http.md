# HTTP calibration

Use the release binary from the [overview](README.md#build-and-run).
Prepare populated baseline/candidate nodes with equivalent logical data and
pause sync. The driver does not restore, start, change or stop nodes.

## Describe the experiment

Create a manifest, replacing the placeholders with measured configuration and
independently verified expected responses—not just the candidate's output:

```json
{
  "schema": 1,
  "dataset": "<corpus identity>",
  "network": "mainnet",
  "tip_hash": "<64-hex-digit tip hash>",
  "server_environment": {"cpu": "<model>", "memory_bytes": 0, "disk": "<device/filesystem>"},
  "settings": {
    "storage_version": "v4", "dictionary": "<dictionary SHA-256>",
    "max_scan_items": 3000, "cache": "<state/archive budgets>",
    "durability": "production-defaults"
  },
  "cases": [{
    "name": "pool-blocks", "path": "/epochs/<epoch>/blocks/<pool>?count=2",
    "fields": [], "expected": ["<first block hash>", "<second block hash>"],
    "array": true, "live": false
  }]
}
```

`fields` lists JSON pointers to compare per item when `array` is true, or once
for an object response otherwise. Empty `fields` compares whole values; nonempty
projections verify only selected fields. Every measured response must also match
the validated warmup response's complete hash.

## Run both arms

```sh
"$XTASK" perf minibf http --url http://127.0.0.1:3000 --manifest mainnet.json \
  --server-binary /path/to/baseline/dolos --server-revision BASELINE_COMMIT \
  --run mainnet-01 --label baseline --out mainnet.jsonl --repeat 1 --repeat-start 0
```

Run the candidate with its URL, binary, revision and label. Keep the manifest,
run name and request settings identical. Repeat with increasing `--repeat-start`,
alternating arm order; use at least three pairs before applying
[`perf minibf check`](minibf.md#compare-revisions) to the combined JSONL.
For authentication, `--project-id-env VARIABLE_NAME` reads a secret without
recording its value in the command.

## Interpretation

The manifest tip is checked before and after each workload; changes invalidate
the result. Server binary/revision are operator assertions, not remote attestation.
CPU, I/O and RSS belong to the **client**, not the server; collect server metrics
separately. Stable-tip calibration does not establish live-sync performance.
