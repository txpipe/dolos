# Storage experiments

Use the release binary from the [overview](README.md#build-and-run).
These experiments do not invoke minibf handlers.

## Store workloads

```sh
"$XTASK" perf storage run --synthetic 400 --preset smoke \
  --work /path/to/scratch --out smoke.jsonl --verify
```

For real data, replace `--synthetic` with `--immutable DIR` or
`--corpus DIR --segments 448..451`. The latter expects pre-v4 raw segment files,
not current compressed flatfiles. Use `--preset all --repeat 3` for a fuller run.

| Preset | Work |
|---|---|
| `write` | Append batches of 1, 100 and 500 blocks |
| `read` | Point reads, 100-block pages and full scans |
| `mixed` | 90/10, 50/50 and 10/90 point/page mixes |
| `concurrent` | Append while readers run |
| `all`, `smoke` | All workloads; smoke bounds their size |

`--codecs` compares `raw`, `zstd1`, `zstd3`, their `-dict` variants and `store`.
Raw/zstd sinks isolate codec cost; `store` uses the production `FlatFileStore`.
Raw is **not** a supported production mode. The store measures whole operations,
not separate codec/fsync time; `--encode-threads` and `--no-fsync` do not affect it.
The production store now selects serial or bounded parallel encoding
automatically; see [encoding policy and evidence](encoding.md).

Cache choices are `warm`, `evict` and `nocache`. On macOS, eviction requires
`--evict-from DIR --evict-gib N` and is approximate. `nocache` also disables
readahead; the production store skips it. Unsupported regimes are reported and
skipped. Do not weaken durability to obtain a passing comparison.
Cache-setting failures and out-of-range scan latencies fail the experiment;
they are not reported as successful measurements with altered cache or timing.

Store reports pair codecs against raw with identical settings and repeats.
Provisional write gates require throughput ≥90% of raw and p95 commit latency
≤110%; read ratios are descriptive, not API latency gates.

## Compare Dolos binaries

```sh
"$XTASK" perf storage node \
  --bin baseline=/path/to/baseline/dolos --bin candidate=/path/to/candidate/dolos \
  --run storage-01 --immutable /path/to/immutable --genesis /path/to/genesis \
  --work /path/to/scratch --out node.jsonl --repeat 3 --verify
```

The runner creates fresh disposable instances. Binary specs are
`LABEL=PATH[:STORAGE_VERSION]`, defaulting to v4. Historical raw revision
`9165dbd8` needs `:v3`; that is a compression baseline, not a redb comparison.

`import` uses 500-block batches; `live` models one-block commits through offline
import, **not** daemon sync. `read` uses `dump-blocks` and UTxO RPC point/page
queries; `mixed` combines RPC point/page reads. This runner does not append while
serving: use the store's `concurrent` preset or [minibf live replay](minibf.md#live-replay).

The immutable corpus must include chunks `00000` and `00001`; its final chunk
is excluded. Before serving, the runner replays the first Byron epoch to seed
the era summary. Ambiguous boundary slots are excluded from RPC queries; scans
cover the contiguous tail. An arbitrary copied window is insufficient.

Reports pair against the `baseline` label: import throughput must be ≥90% and
point-read p95 ≤110% of baseline. Pages/scans have ratios but no verdict.
Uninstrumented nodes report mean batch time; explicitly instrumented records
also gate commit p95 at ≤110% of baseline and do not pair with uninstrumented runs.

## Dictionary experiments

```sh
"$XTASK" perf storage train --corpus /path/to/raw/segments \
  --sample 440..447=1500 --seed 0 --out candidate.dict
"$XTASK" perf storage evaluate \
  --fixture heldout=/path/to/raw/segments:448..455 \
  --dictionary bundled --dictionary candidate.dict --out dictionaries.jsonl
```

Training writes dictionary provenance beside the asset. Evaluation verifies
round trips and reports size, encoding throughput and decode cost on held-out
blocks. Changing the [bundled dictionary](../../crates/flatfiles/dictionary/README.md)
is a storage-format decision, not an operator tuning option.
