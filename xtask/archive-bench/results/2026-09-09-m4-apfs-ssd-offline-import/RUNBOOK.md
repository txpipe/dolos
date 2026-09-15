# Reproduction

Use the same Apple M4 / 16 GiB / APFS SSD host and the immutable window
specified in the predecessor's runbook. Do not overlap another build, test
suite or load test. Each node arm gets a separate disposable store, recreated
for each repeat. Never point `WORK` at an existing node.

## Binaries and instrumentation

Export `9165dbd8` and `b21c8d55` into disposable source directories. Export
the implementation into a third directory. Apply the patch emitted by
`python3 xtask/archive-bench/instrument-import.py CHECKOUT` to each.

All three binaries use Rust 1.93, `cargo build --release --bin dolos`,
default features and `CARGO_INCREMENTAL=0`. The timing patch is identical
around each production commit; only the optimized arm can supply its new
buffer counters. `VERGEN_GIT_SHA` supplies each exported tree's revision.
The reported binary SHA-256 is the identity of the measured executable.

A shared release target was used to reuse third-party dependencies. Before
switching arms, `rg --files -0 | xargs -0 touch` forced recompilation of
every source in the exported tree. A first serial build without this step
failed with a stale v3 dependency (exit 101); it was discarded, rebuilt
successfully and never measured. Independent target directories also avoid
that problem.

The node harness is the workspace's `cargo-xtask` executable. Its parent
process scans the corpus before starting the measured subprocess. Only the
release Dolos child is timed and charged CPU/RSS. Individual commit samples
are retained in `instrumentation.commit_ns`; report percentiles are computed
from those samples.

## Inherited production gate

```sh
WINDOW=corpora/mainnet-immutable-window
GENESIS=genesis/mainnet
WORK=work/offline-import.noindex
X=target/debug/cargo-xtask

$X archive-bench node \
  --bin baseline=bin/raw:v3 --bin serial=bin/serial:v4 \
  --bin optimized=bin/optimized:v4 --run 2026-09-09-offline-import-500 \
  --immutable "$WINDOW" --genesis "$GENESIS" --work "$WORK" \
  --out node-500.jsonl --workloads import --chunk-size 500 --repeat 3
```

Repeat that command with `--chunk-size 5000`, its own run name and
`node-5000.jsonl` for the bootstrap-sized transaction shape.

For single-block Byron, use the original window with `--workloads live
--live-blocks 2000`, a distinct run name and `node-byron-1.jsonl`.
Here `live` is the inherited harness name for one-block archive imports;
it is not a simultaneous API workload or a claim about full live sync.

For single-block modern-era measurements, create a separate supplementary
immutable directory with the original origin block alone in `00000`, plus
chunks `08881`, `08882`, `08883` from the same source. Pallas excludes the
last chunk. The origin chunk is reproduced by retaining the first 56-byte
secondary entry, the chunk prefix ending at the next entry's big-endian
u64 offset, and primary bytes `01 00000000 00000038`. Then run the same
one-block workload on this directory. Its first block is the required Byron
origin anchor; subsequent measured blocks are Conway. This supplementary
corpus never substitutes for the inherited gate's full window.

## Supporting store measurements

```sh
$X archive-bench bench --preset write --immutable "$WINDOW" \
  --skip 43177 --limit-blocks 5000 --write-batches 1,500,5000 \
  --codecs store,store-import --repeat 3 --work "$WORK/store" \
  --out store-modern.jsonl

$X archive-bench bench --preset write --synthetic 20 \
  --synthetic-large-bytes 16777216 --write-batches 1,500,5000 \
  --codecs store-import --repeat 3 --work "$WORK/limits" \
  --out store-limits.jsonl
```

The large-body fixture is seeded SplitMix64 byte data, explicitly synthetic,
with a 16 MiB body in the middle of twenty mixed-size bodies spanning two
segments. It measures allocation bounds, not production block throughput.
The supporting store harness's RSS is the process lifetime high-water mark;
production arm RSS comes from separate child processes. CPU deltas and
transient encoded-buffer counters are scoped to the measured workload.

## Reports and validation

```sh
$X archive-bench report node-500.jsonl node-5000.jsonl \
  node-byron-1.jsonl node-modern-1.jsonl > tables-node.md
$X archive-bench report store-modern.jsonl store-limits.jsonl > tables-store.md
```

Sanitize local paths to the labels in this runbook before publishing JSONL.
Keep exploratory runs separate from acceptance runs. The original
per-block-mutex encoder's exploratory run overlapped a separate test suite;
it is retained as excluded evidence, not used in the acceptance verdict.
Never rewrite predecessor results.

`verification.json` records the required check commands and exit statuses.
The two Docker suites reached their first registry test but the daemon did
not respond; the complete process groups were terminated at 45 seconds and
recorded as exit 124. They are blocked checks, not passes.
