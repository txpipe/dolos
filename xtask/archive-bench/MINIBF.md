# Minibf performance benchmarks

The archive benchmark tooling has three layers:

| Layer | Entry point | Subject |
|---|---|---|
| Storage | `cargo bench --bench archive_backends` | Isolated log scans, local indexes, compressed body reads and scans |
| Routes and replay | `cargo xtask archive-bench minibf` | Actual minibf routers on fresh Fjall state/archive and a persistent redb WAL |
| Real-data calibration | `cargo xtask archive-bench minibf-http` | Verified HTTP requests against an independently prepared, stable-tip node |

There is no redb archive comparison or production compression switch. The new
measurement adapters live in `dolos-testing`; production store and route
implementations are unchanged.

## Build and smoke

Build the xtask from the revision being measured. A globally installed
`cargo-xtask` may be stale; the direct binary invocation is unambiguous:

```sh
cargo build --release -p xtask
target/release/cargo-xtask archive-bench minibf \
  --work /path/to/scratch --out smoke.jsonl --run smoke \
  --repeat 1 --requests 10
target/release/cargo-xtask archive-bench report smoke.jsonl
```

Smoke results deliberately lack the repetitions and samples to pass timing gates.
Ordinary CI runs correctness/work guards, not elapsed-time thresholds:

```sh
cargo test -p dolos-testing --test benchmark_fixtures
cargo test -p xtask --test minibf_bench
cargo test --bench archive_backends
```

These targets also run in the existing workspace CI commands; no dedicated
performance runner or new CI timing gate is required.

## Fixtures and workloads

The shared `dolos_testing::performance::ApiFixture` builds deterministic
synthetic Conway CBOR at the start of preview epoch zero, initializes genesis
with the required UTxOs, imports the initial history in bounded batches and
seeds typed account-epoch logs in 1,000-row batches. It retains an equally sized
tail for live replay. Both halves and the fixture shape contribute to the
recorded identity. This avoids an epoch jump that would invalidate strict-mode
fixtures. It is a synthetic query fixture, not a consensus-validation corpus.

| Knob | Default | Dimension |
|---|---:|---|
| `--blocks` | 16 | Initial blocks and, separately, tail blocks |
| `--transactions-per-block` | 3 | Distinct transaction hashes and account UTxO width |
| `--log-rows` | 256 | Typed rows in the queried epoch |
| `--pool-stride` | 8 | One matching pool row per this many rows |
| `--page`, `--page-size` | 2, 2 | Pagination depth and returned records |
| `--seed` | 0 | Credential and body metadata variation |
| `--cache-mib` | 64 | Cache budget for each Fjall store |
| `--max-scan-items` | 3000 | Actual minibf pagination guard |

The requested page must fit both the initial block history and pool matches.
Fixture construction is excluded from request timing. Synthetic fixtures are
bounded to 10,000 initial blocks and 100 transactions per block; generation
retains the block corpus and genesis inputs in memory. Increasing log rows is
batched but increasing the block corpus still costs memory.

Cases selected with `--cases name,name` (default `all`):

- `epoch-latest`: current state/stake path; verifies epoch and block count.
- `epoch-stakes-page`: typed log scan and pagination.
- `epoch-stakes-sparse-pool`: value filtering before pagination.
- `epoch-blocks-pool-page`: compressed bodies/header filtering to a matching page.
- `epoch-blocks-pool-no-match`: scans all bodies for a registered non-minting pool.
- `epoch-blocks-reverse`: reverse block traversal and ordering.
- `address-transactions-page`: archive tags, bodies and transaction filtering.
- `account-utxos-wide`: state tags, UTxOs and deduplicated archive metadata lookups.
- `transaction-utxos`: transaction lookup and input/output mapping.

Expected identifiers, ordering, counts and selected values come from the fixture,
not from recording the candidate's first response. Validation runs for every
request. The complete normalized JSON response is also hashed after that
independent validation; every measured response must match, and differing full
responses across revisions cannot pair. The first successful request primes the route before measurement;
the cache regime is named `route-primed`, never cold.

For example, sweep selectivity and pagination while holding the rest fixed:

```sh
target/release/cargo-xtask archive-bench minibf \
  --work /path/to/scratch --out medium.jsonl --run medium \
  --blocks 256 --log-rows 100000 --pool-stride 32 \
  --page 20 --page-size 5 --requests 1000 --repeat 3 \
  --cases epoch-stakes-sparse-pool,address-transactions-page
```

The storage bench additionally includes logs-only populations and a real Alonzo
block fixture. Its existing filler-byte cases remain isolated storage guards;
they are not representative compression-ratio measurements. The existing
archive-bench `bench` and `node` commands retain their real-corpus and cache
experiments.

## Request load and live replay

`--rates 0` uses bounded closed-loop concurrency. Positive rates schedule
arrivals independently of completions. `--concurrency` bounds active requests;
arrivals that cannot be admitted are counted as rejected rather than queued
without limit or silently omitted. Latency includes scheduler delay, route
execution, response collection, oracle validation and hashing; service latency excludes
scheduler delay. Both histograms include unsuccessful completed attempts.

`--timeout-ms` is the completion budget. In-process requests are drained even
when late, because cancelling an async caller does not cancel its blocking
storage work. Late completions do not count as successful throughput. HTTP
requests also use a transport timeout. Errors, late completions and rejected
arrivals retain their records and cause the command to fail.

Live mode uses the normal `SyncExt::roll_forward` path with persistent WAL,
state and archive writes while requests run. It does not run offline import
alongside a server. It records completed blocks and whole-roll-forward latency
(including all lifecycle work), not a fictional index-commit percentile.

```sh
target/release/cargo-xtask archive-bench minibf \
  --work /path/to/scratch --out live.jsonl --run live \
  --live --blocks 256 --requests 1000 --rates 25,100 \
  --timeout-ms 1000 --write-interval-ms 250 \
  --cases account-utxos-wide
```

The tail must span the offered-arrival duration plus the timeout budget. The
writer sleeps after each completed block, so the interval is a minimum delay,
not a promised chain ingestion rate. Latest-epoch and reverse-tip cases are
excluded in live mode because their expected results change with the tip.
Work counters in live mode include both API and writer reads; they are labelled
`api-and-writer`, not attributed to individual requests.

## Compare revisions

Both binaries must contain the same benchmark protocol/fixture implementation.
Build each from a separate worktree with equivalent release settings, preserve
the binaries, and use fresh stores for every case/repeat. A historical revision
without this harness needs the benchmark-only changes applied to its worktree;
the runner does not build or modify revisions for you.

```sh
target/release/cargo-xtask archive-bench minibf-compare \
  --bin baseline=/path/to/baseline/cargo-xtask \
  --bin candidate=/path/to/candidate/cargo-xtask -- \
  --work /path/to/scratch --out paired.jsonl --run comparison-01 \
  --requests 1000 --repeat 3
target/release/cargo-xtask archive-bench minibf-check paired.jsonl \
  --max-p95-ratio 1.10 --max-p99-ratio 1.20 --min-throughput-ratio 0.90 \
  --min-repeats 3 --min-samples 1000
```

The orchestrator alternates arm order on each repeat. These are default relative
budgets for controlled runners, not an adopted minibf SLO. Optional
`--p95-budget-ms` and `--p99-budget-ms` apply absolute limits to both arms.
Use more requests when assessing p99. Reported p95 ratios are medians of paired
ratios with their minimum/maximum; per-arm median p95/p99 are shown separately.

Missing/duplicate repeats, changing binary identities, incompatible fixtures or
settings, missing declared workloads, request failures and insufficient samples
cannot pass. A comparison of a binary against itself is useful to validate the
runner's noise; it is not evidence of a version improvement.

## Calibrate against restored data

Prepare separate populated instances with equivalent logical data and pause
sync. This command does not restore, start, alter or stop nodes. Author a JSON
manifest whose cases contain independently checked expected values:

```json
{
  "schema": 1,
  "dataset": "mainnet-window-identified-by-your-corpus-hash",
  "network": "mainnet",
  "tip_hash": "<64-hex-digit-tip-hash>",
  "server_environment": {
    "cpu": "<model>", "memory_bytes": 0, "disk": "<device/filesystem>"
  },
  "settings": {
    "storage_version": "v4", "dictionary": "<bundled-dictionary-sha256>",
    "max_scan_items": 3000, "cache": "<state/archive cache budgets>",
    "durability": "production-defaults"
  },
  "cases": [{
    "name": "sparse-pool", "path": "/epochs/<epoch>/blocks/<pool>?count=2",
    "fields": [], "expected": ["<first-block-hash>", "<second-block-hash>"],
    "array": true, "live": false
  }]
}
```

With `array: true`, `fields` contains JSON pointers projected from each returned
item; the exact projected array must equal `expected`. With `array: false`,
the projection applies once to the response object. Empty fields compare the
whole value. Use full responses when practical; a projection verifies only the
selected fields. Never derive the oracle solely from the candidate.

```sh
target/release/cargo-xtask archive-bench minibf-http \
  --url http://127.0.0.1:3000 --manifest mainnet.json \
  --server-binary /path/to/baseline/dolos --server-revision <commit> \
  --run mainnet-01 --label baseline --out mainnet.jsonl \
  --repeat 3 --requests 1000
```

Repeat against the candidate instance with its binary/revision and label, using
the same manifest, run name and workload settings. For alternating order, run
each arm with `--repeat 1 --repeat-start N`, swapping order each repeat. Use the
same `minibf-check` command on the resulting JSONL. Optional
`--project-id-env VARIABLE_NAME` supplies authentication without putting its
value in the recorded command.

The driver verifies the manifest's tip hash before and after each workload.
Changed tips invalidate the result. The server binary/revision are supplied by
the operator; the driver cannot attest which executable a remote server runs.
Resource counters in HTTP records belong only to the client; server CPU, I/O,
queueing and internal work counters need separately collected instrumentation.

## Evidence and limits

Records retain fixture shape/hash, expected response projection, binary SHA-256,
build revision/dirty state/profile for in-process runs, host facts, cache and
durability settings, request configuration, raw latency summaries, work/resource
counts and repeat identity. Work counters measure rows yielded by storage
iterators, successful decoded body reads/bytes, exact lookups and state/UTxO
reads. They do not measure internal LSM entries visited, physical frame bytes,
codec versus CBOR CPU separately, or semaphore queue depth. Atomic counting and
oracle validation add harness overhead on both arms.

Process CPU/I/O and lifetime peak RSS in in-process runs include the driver,
server and live writer. Storage footprints are file lengths sampled after each
case, not per-keyspace cardinalities. Work-count assertions are most useful in
read-only fixtures; use real-node calibration before converting synthetic
latencies into a production claim.

The synthetic replay stays in one epoch and does not establish epoch-transition,
pruning, full-mainnet LSM/compaction, or production traffic-mix behavior. It
generates valid CBOR, not a realistic distribution of block sizes/compressibility.
Those remain calibration workloads, not conclusions from a tiny-fixture pass.
Offline-import gates remain with the existing compression benchmark track.

Local results can contain workstation paths and query identifiers. Before
committing evidence, follow the sanitization and provenance rules in
[README.md](README.md#results); preserve hashes and measured values.
