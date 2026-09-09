# Minibf experiments

Use the release binary from the [overview](README.md#build-and-run).
`run` executes actual Axum routes and query facades against fresh Fjall state,
archive and persistent WAL stores, without a network socket.

```sh
"$XTASK" perf minibf run --work /path/to/scratch --out smoke.jsonl \
  --run smoke --repeat 1 --requests 10
```

This is a harness check, deliberately too small to pass a performance gate.

## Workloads and scale

Select workloads with `--cases name,name` (default: all):

| Cases | Access shape |
|---|---|
| `epoch-latest` | Current state/stake path |
| `epoch-stakes-page`, `epoch-stakes-sparse-pool` | Typed log scans, filtering and pagination |
| `epoch-blocks-pool-page`, `epoch-blocks-pool-no-match` | Compressed-body reads and issuer filtering |
| `epoch-blocks-reverse` | Reverse block traversal |
| `address-transactions-page` | Archive tags joined with blocks |
| `account-utxos-wide` | State tags, UTxOs and archive metadata |
| `transaction-utxos` | Transaction lookup and input/output mapping |

Scale with `--blocks`, `--transactions-per-block`, `--log-rows`, `--pool-stride`,
`--page` and `--page-size`; keep `--seed`, `--cache-mib` and `--max-scan-items`
fixed when comparing versions. Pages must fit the generated population.

Fixtures contain synthetic Conway blocks in preview epoch zero and an equally
sized replay tail. Setup is untimed; logs are populated in bounded batches, but
blocks and genesis inputs remain in memory. Limits are 10,000 initial blocks
and 100 transactions per block. This does not reproduce mainnet compression,
LSM depth, epoch transitions, pruning or the production traffic mix.

## Load and measurement

`--rates 0` is closed-loop load; positive rates schedule independent arrivals.
`--concurrency` bounds in-flight requests; excess arrivals count as rejected.
Late in-process requests are drained, since cancellation cannot stop their
blocking work. Errors, timeouts and rejections preserve records and fail the run.

Each request validates fixture-derived identities, ordering and values, then
checks the complete normalized response hash. Warmup primes the route: this is
`route-primed`, not cold. Latency includes scheduling, response collection,
validation and hashing; service latency excludes scheduling delay.

Counters track yielded rows, decoded bodies, lookups and state reads—not internal
LSM visits, physical frame bytes, separate codec CPU or semaphore queue depth.
Process resources include the harness; lifetime peak RSS is not per-request RSS.

## Live replay

```sh
"$XTASK" perf minibf run --work /path/to/scratch --out live.jsonl --run live \
  --live --blocks 256 --requests 1000 --rates 25 --timeout-ms 1000 \
  --write-interval-ms 250 --cases account-utxos-wide
```

The writer uses normal `roll_forward`, including WAL/state/archive commits.
The tail must span arrival duration plus timeout; the interval is a minimum
delay, not guaranteed ingestion throughput. Latest/reverse-tip cases are excluded.
Counters include API and writer work; writer latency covers the whole roll-forward.

## Compare revisions

Prepare release binaries with the same harness/fixture protocol and equivalent
build settings; the runner does not build or patch historical revisions.

```sh
"$XTASK" perf minibf compare \
  --bin baseline=/path/to/baseline/cargo-xtask \
  --bin candidate=/path/to/candidate/cargo-xtask -- \
  --work /path/to/scratch --out paired.jsonl --run comparison-01 \
  --requests 1000 --repeat 3
"$XTASK" perf minibf check paired.jsonl
```

Arm order alternates per repeat. Default gates require three paired repeats,
1,000 successful requests each, p95 ratio ≤1.10, p99 ratio ≤1.20 and throughput
ratio ≥0.90. These are provisional experiment budgets, not a production SLO;
use more samples for p99. Optional `--p95-budget-ms`/`--p99-budget-ms` apply to
both arms. Missing, duplicate, incompatible, failed or undersampled evidence cannot
pass. Confirm synthetic findings with [real-node calibration](http.md).
