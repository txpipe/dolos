# Automatic archive encoding: evaluation and acceptance

## Decision

The founder revised the ruling during PR #1317 review: optimize eligible
batches, not reserve resources for offline callers. The revised implementation
uses the original unified entry points and selects encoding strategy entirely
inside flatfiles. No offline method, writer intent, lifecycle variant or
backend forwarding hook remains.

**Automatic selection is justified, but not free.** The inherited production
throughput/p95 gates pass in these runs. Concurrent archive readers show a
material latency trade-off: batch-100 writer throughput roughly doubles,
while median per-run point-read p95 rises 12.9% and page-read p95 rises 28.0%.
This is not a correctness or durability reason to duplicate caller interfaces,
but it prevents any claim of resource isolation or unchanged query tails.
QA/founder should review that cost before merging.

## Provenance and limitations

Source: `5d13f2dae7037697b42cb8e394f74cfe371f6dd7`. Raw production:
`9165dbd862f55baa64d9c693a26519f6bfe3c00a`, v3. Serial compressed production:
`b21c8d554f8cdabcdfdb6b5f00421ba77edfa0dc`, v4. The automatic arm remains v4,
with the same independent zstd-3 frames and bundled dictionary.

Apple M4, 16 GiB, macOS 26.5.2, APFS SSD. The measurement driver waited for
other observed builds/tests to finish before starting; no other load test was
intentionally overlapped. However, macOS storage-management services were
observed consuming CPU during this window, and free disk space was only about
3 GiB. Timings have substantial variance. **Treat the reported ratios as this
window's measurements, not a clean or portable speedup estimate.** No samples
were dropped or replaced to improve the verdict.

The inherited 126,728-block corpus is unchanged: 43,177 Byron and 83,551 Conway
bodies, 674,364,546 decoded bytes, slots 0 through 193,535,999, largest admitted
corpus body 648,089 bytes. See the pinned
[corpus manifest](../2026-09-09-m4-apfs-ssd-offline-import/corpus-files.json).
The supplementary modern input is the original origin anchor plus 1,999
Conway bodies; it never replaces the inherited gate corpus.

[identities.json](identities.json) records binary hashes, exact source patch,
build settings and actual command exit statuses.
[RUNBOOK.md](RUNBOOK.md) reproduces the shapes and instrumentation.
The earlier offline candidate's evidence remains immutable and does not
describe this source revision.

## Production gate

Three paired release runs per shape; separate disposable stores and correct
version configs, unchanged production durability. Timing surrounds actual
`writer.commit()` calls: 254 samples at batch 500, 26 at batch 5000, and 2,001
in each 2,000-body single-block run, including the legacy final empty commit.
All 36 production imports exit 0.

The 500-block workload reaches **151.5% of raw throughput** and **134.4% of
serial-compressed throughput**, using medians over the three runs. Its commit
p95 is **91.3% of raw**: 40.567 ms versus 44.433 ms. The paired throughput
ratios are 1.064, 1.524 and 1.372, all above 90%. One individual pair's p95
ratio exceeds 110%; the declared median-percentile gate passes, not every
individual p95 comparison. Raw elapsed times span 6.653–9.729 seconds and
serial times 7.876–16.618 seconds, illustrating the noisy host window.

Elapsed time, throughput, process CPU and per-run percentiles below are
medians; RSS and buffers are maxima across repeats.

| Shape | Arm | Seconds | Blocks/s | CPU ms | RSS MiB | Commit p50 / p95 / p99 ms | Buffers MiB |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| Byron, 1 | raw | 9.454 | 212 | 611.0 | 15.81 | 4.375 / 5.947 / 10.830 | — |
| Byron, 1 | serial | 9.698 | 206 | 652.4 | 18.48 | 4.188 / 6.304 / 10.756 | — |
| Byron, 1 | automatic | 9.734 | 205 | 580.5 | 18.30 | 4.309 / 6.304 / 11.305 | 0.620 |
| Modern, 1 | raw | 10.729 | 186 | 1,077.1 | 16.77 | 4.882 / 6.906 / 8.380 | — |
| Modern, 1 | serial | 10.488 | 191 | 1,431.1 | 18.75 | 4.780 / 6.509 / 9.372 | — |
| Modern, 1 | automatic | 11.045 | 181 | 1,536.4 | 18.64 | 4.817 / 6.926 / 12.173 | 0.620 |
| Inherited, 500 | raw | 9.469 | 13,383 | 4,777.5 | 79.27 | 20.398 / 44.433 / 242.876 | — |
| Inherited, 500 | serial | 8.400 | 15,086 | 7,026.7 | 82.72 | 29.344 / 53.379 / 109.642 | — |
| Inherited, 500 | automatic | 6.252 | 20,269 | 8,257.2 | 101.94 | 19.448 / 40.567 / 57.442 | 4.617 |
| Inherited, 5000 | raw | 4.599 | 27,555 | 4,408.8 | 189.75 | 138.805 / 229.900 / 368.837 | — |
| Inherited, 5000 | serial | 6.160 | 20,573 | 6,593.7 | 192.59 | 215.876 / 391.905 / 657.457 | — |
| Inherited, 5000 | automatic | 4.055 | 31,252 | 7,455.2 | 220.66 | 122.618 / 218.890 / 329.777 | 5.566 |

Dashes mean unavailable historical buffer counters, not zero. CPU includes
all child threads. Batch-500 automatic CPU is 17.5% above serial and peak RSS
is 19.22 MiB higher. The automatic small-block path allocates no additional
encoders or owned frames: both one-block shapes record 2,000 serial appends
and zero parallel appends.

The other median thresholds also pass: batch 5000 has 113.4% raw throughput /
95.2% raw p95; Byron-1 has 97.1% / 106.0%; modern-1 has 97.1% / 100.3%.
Modern-1 is nevertheless about 5.0% slower than serial in this window.
No full-bootstrap speedup or universal non-regression is claimed.
See [tables-node.md](tables-node.md) and the complete `node-*.jsonl` samples.

## Selection and crossover

The internal cutoff is 256 KiB of decoded bodies in a multi-body window.
Single-body, tiny and one-worker cases stay serial. Callers already executing
on Rayon also stay serial, avoiding nested work stealing under the physical
writer lock; waiting workers help queued jobs rather than starving the pool.
There are no caller-mode branches above flatfiles.

Supporting measurements use identical development-profile harness settings,
not release production ingestion. They cover 1,024 real modern bodies and
three repeats per shape. The previous harness compares its serial path with
its always-windowed path; the new harness uses automatic selection:

| Batch | Previous serial blocks/s | Previous windowed blocks/s | Automatic blocks/s | Serial / windowed / automatic CPU ms |
| --- | ---: | ---: | ---: | --- |
| 8 | 1,110 | 1,239 | 1,137 | 359.82 / 831.18 / 372.50 |
| 32 | 3,068 | 4,656 | 3,076 | 185.14 / 350.25 / 195.33 |
| 64 | 4,463 | 8,289 | 7,514 | 150.43 / 217.41 / 244.41 |
| 128 | 5,956 | 12,674 | 11,634 | 130.36 / 208.35 / 235.29 |
| 500 | 6,972 | 18,353 | 18,571 | 128.83 / 198.35 / 196.57 |

Always-windowed batch 8 spends 2.31 times serial CPU for only an 11.6%
throughput gain in this window. Automatic selection keeps it serial.
Batch 32 sometimes crosses the byte threshold; the conservative policy
foregoes throughput opportunities there rather than paying the windowed CPU
cost for every batch. Larger shapes show clear elapsed-time gains.
This supports a conservative cutoff, not a universal optimal crossover:
compressibility, hardware, filesystem and scheduling all matter.

## Concurrent-reader trade-off

Four archive readers, 2,000 real modern bodies, three repeats. Readers query
the prefilled half while the writer appends the remainder. These are flatfile
measurements, not full API traffic or a live-node acceptance gate.

| Writer batch | Strategy | Writer blocks/s | Reader ops/s | Point p95 us | Page p95 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | previous serial | 233 | 12,008 | 77.887 | 3.840 |
| 1 | automatic | 234 | 11,877 | 78.655 | 3.846 |
| 100 | previous serial | 3,138 | 9,125 | 98.559 | 5.063 |
| 100 | automatic | 6,468 | 9,213 | 111.295 | 6.480 |
| 100 | automatic, four-worker experiment | 5,497 | 8,491 | 120.895 | 7.397 |

Single-block behavior is similar; batch-100 writer throughput improves 2.06
times while reader p95 degrades. Shorter writer runs also reduce reader
sample counts, and the host noise limits attribution. Nonetheless, the
observed tail cost is material and must not be hidden behind throughput.

An additional three-repeat experiment sets the existing
`RAYON_NUM_THREADS=4` environment variable only in the developer harness.
It does not improve reader tails in this window, so no unsupported fan-out
cap or operator setting was added to production. Its exact environment and
samples are retained separately in `concurrent-automatic-4.jsonl`.
Neither experiment establishes a query-latency guarantee.

## Bounds, verification and remaining review

Peak encoded buffers are 4,841,487 bytes at batch 500 and 5,836,179 at batch
5000, with at most ten additional contexts on this host. The seeded 16 MiB
incompressible body among mixed synthetic bodies stays serial in its own
window: peak scratch is 16,842,752 bytes, without an owned copy of that frame.
The general bound remains `max(W, B(M)) + (P + 2) * B(M)`; native contexts
are bounded by P + 1. See the [implementation notes](../../OFFLINE-IMPORT.md).

[verification.json](verification.json) records completed exit statuses:
clippy/build exit 0; default tests 1,457 passed / 48 ignored; all-features
selection 995 passed / 53 ignored. Seven pre-existing clippy warnings remain,
with no new warnings. Tests cover explicit byte-boundary selection, tiny and
one-worker fallbacks, deterministic out-of-order completion, worker starvation,
concurrent callers, actual live/import/Mithril/snapshot paths, duplicate
locations, same-slot Byron ordering, encode/partial-write/sync/index failures,
oversized refusal, rollback and restart/retry.

A post-measurement review tightened the public parallel-ordering integration
test: it now calls outside Rayon and asserts that selection actually used
parallel encoding when workers are available. All four workspace checks were
rerun successfully. This test-only fix is `ef72cf3e`; production sources remain
identical to the measured `5d13f2da` candidate.

Production snapshot restore/backfill and registry transport paths now match
the merged baseline; only their tests exercise the shared optimization.
The conditional Docker registry checks were not rerun. The prior unresponsive
daemon attempts remain recorded as timeouts in historical evidence, not passes.

The implementation and measured inherited median gates are complete. Review
the concurrent-reader tail cost and noisy-host limitations through the normal
code-QA/founder relay before merge or retirement. No deployment, live-instance
mutation, cloud spend or merge was performed.
