# Performance experiments

`cargo xtask perf` orchestrates experiments; `cargo bench --bench archive_backends`
measures isolated Rust operations. Neither a synthetic run nor a passing relative
comparison establishes mainnet capacity.

| Guide | Commands | Subject |
|---|---|---|
| [Storage](storage.md) | `perf storage run`, `node`, `train`, `evaluate` | Compression, I/O, import and storage-facing RPC |
| [Minibf](minibf.md) | `perf minibf run`, `compare`, `check` | In-process routes, live replay and version comparisons |
| [HTTP calibration](http.md) | `perf minibf http` | Minibf on prepared, populated nodes |

## Build and run

From the repository root:

```sh
cargo build --release -p xtask
XTASK="$PWD/target/release/cargo-xtask"
"$XTASK" perf --help
```

The guides use this release binary. Use each subcommand's `--help` for the full
option list. Put `--work` on the disk being measured; use a new output file for
each experiment because measurements append JSONL records.

```sh
"$XTASK" perf report results.jsonl
```

Reporting accepts storage, minibf or mixed records. It renders findings;
`perf minibf check` additionally exits nonzero when a comparison cannot pass.
Match corpus, host, build settings, cache, durability and load across arms.
Missing counters are unavailable, not zero; simulated cold-cache results are
not equivalent to physical cold reads.

## Layout and compatibility

`xtask::perf::{storage,minibf}` share measurement, provenance, load generation
and reporting. Reusable chain fixtures and store counters live in `dolos-testing`.
Harness checks are `perf_cli`, `storage_perf_smoke` and `minibf_perf`; CI runs
correctness checks, not noisy wall-clock thresholds.

`archive-bench` remains a legacy storage command; `bench` aliases `storage run`.
New scripts should use the commands above.

## Published evidence

Historical records and their original commands remain under `archive-bench/results`:
[compression study](../archive-bench/results/2026-09-08-m4-apfs-ssd/REPORT.md) and
[cutover acceptance](../archive-bench/results/2026-09-09-m4-apfs-ssd-acceptance/REPORT.md).
Each run separates findings (`REPORT.md`), reproduction (`RUNBOOK.md`), generated
tables and raw JSONL. They are evidence, not current CLI instructions.

Before publishing, replace local paths and session identifiers with consistent
labels. Preserve hashes, revisions and measurements, then regenerate tables from
the sanitized records. Output is not automatically sanitized; the
`published_artifacts` test guards committed evidence and dictionary provenance.
