# Offline archive import: measurements

**The inherited throughput and commit-p95 gates pass.** On the full inherited
126,728-block corpus at batch 500, the optimized production binary achieves
**101.6% of pinned raw-production throughput** and **156.6% of serial-compressed
production throughput**, using medians over three paired repeats. Its commit
p95 is **81.5% of raw production's** (21.299 ms versus 26.132 ms). Every individual
paired throughput ratio also exceeds 90%: 1.027, 1.016 and 1.040.

## Scope and provenance

One declared Apple M4, 16 GiB, macOS 26.5.2, APFS SSD host. No deployments,
live-node mutations, cloud resources or API traffic were used. The clean
measurement window started after all observed build/test processes exited;
there were no competing build/test processes at its end. OS and filesystem
noise remain visible in the individual runs.

The raw arm is `9165dbd862f55baa64d9c693a26519f6bfe3c00a` with v3 config.
The serial arm is merged `b21c8d554f8cdabcdfdb6b5f00421ba77edfa0dc` with v4
config. The optimized arm uses that base plus the bounded offline implementation
and the measurement-only timing patch. The merged tree equals `3273dd54`;
refreshed origin/main still pointed at `b21c8d55` during this work.

[identities.json](identities.json) records executable hashes and the exact
patches used to build the three measured source trees. Rust 1.93 and identical
release/default-feature settings apply to all Dolos children. The developer
harness runs outside the timed process. Each repeat recreates separate
disposable stores; configs select each arm's correct storage version and
preserve its production durability. There is no raw adapter in current
production code.

The inherited corpus has 43,177 Byron and 83,551 Conway blocks, 674,364,546
decoded bytes, slots 0 through 193,535,999, and a largest body of 648,089 bytes.
It is the predecessor's immutable window, unchanged.
[corpus-files.json](corpus-files.json) pins every input file by SHA-256.
The [predecessor report](../2026-09-09-m4-apfs-ssd-acceptance/REPORT.md)
and its evidence remain immutable. No read-path KB measurements are used here.

## Production measurements

Three paired repeats per shape. Elapsed time, throughput, CPU and each
per-run commit percentile are summarized by their median. RSS and encoded
buffers are the maximum observed across repeats. CPU includes every child
thread. Percentiles come from actual individual `writer.commit()` durations
(254 commits at batch 500, 26 at batch 5000, 2,001 in each single-block run),
not elapsed time divided by batch count. All 36 child imports exited 0.
The one-block runs write 2,000 bodies and retain the legacy CLI's final empty
commit when it encounters the first block beyond `--to`; all arms include
that sample identically.

| Workload | Arm | Seconds | Blocks/s | Process CPU ms | Peak RSS MiB | Commit p50 / p95 / p99 ms | Encoded buffers MiB |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| Byron, batch 1 | baseline | 8.672 | 231 | 159.9 | 15.77 | 4.033 / 5.792 / 6.844 | — |
| Byron, batch 1 | serial | 8.862 | 226 | 182.1 | 18.38 | 4.037 / 5.935 / 8.122 | — |
| Byron, batch 1 | optimized | 9.865 | 203 | 184.8 | 18.36 | 4.030 / 6.218 / 29.245 | 0.642 |
| Conway, batch 1 | baseline | 8.596 | 233 | 220.1 | 16.64 | 4.049 / 5.550 / 6.316 | — |
| Conway, batch 1 | serial | 8.749 | 229 | 311.1 | 18.42 | 4.004 / 5.620 / 9.486 | — |
| Conway, batch 1 | optimized | 8.717 | 229 | 311.4 | 18.45 | 3.994 / 5.911 / 6.889 | 0.663 |
| Inherited, batch 500 | baseline | 3.703 | 34,225 | 3,693.3 | 79.02 | 8.180 / 26.132 / 58.032 | — |
| Inherited, batch 500 | serial | 5.710 | 22,195 | 5,904.8 | 82.91 | 17.285 / 44.532 / 90.309 | — |
| Inherited, batch 500 | optimized | 3.646 | 34,759 | 7,278.8 | 100.19 | 9.609 / 21.299 / 34.701 | 4.589 |
| Inherited, batch 5000 | baseline | 4.061 | 31,209 | 3,262.2 | 188.77 | 67.830 / 384.565 / 639.107 | — |
| Inherited, batch 5000 | serial | 4.610 | 27,492 | 5,696.4 | 192.95 | 154.141 / 304.611 / 517.997 | — |
| Inherited, batch 5000 | optimized | 2.666 | 47,537 | 6,689.7 | 221.52 | 55.149 / 239.469 / 274.465 | 5.566 |

Encoded-buffer counters are measured for the optimized arm; dashes mean
the historical arm has no such counter, not a measured zero. The counters
include retained serial scratch, extra encoder scratch and owned completed
frames. They exclude native zstd contexts and allocator/vector metadata,
which are included in process RSS and bounded as documented in the
[implementation notes](../../OFFLINE-IMPORT.md).

The one-block Byron shape is the first 2,000 inherited blocks. The modern
shape contains the required original Byron origin anchor followed by 1,999
Conway blocks from the original modern tail. It is separate supplementary
evidence and never replaces the full inherited gate corpus.

The optimized arm also passes the same median thresholds for batch 5000
(152.3% raw throughput, 62.3% raw p95) and single-block modern (98.6%, 106.5%).
Single-block Byron is a supplementary throughput shortfall: 87.9% of raw and
89.8% of serial throughput; its p95 remains within budget at 107.4% of raw,
but its p99 rises to 29.245 ms. The generic report therefore marks that shape
FAIL. The inherited throughput obligation is specifically batch 500; no
single-block improvement is claimed. Filesystem variance is substantial,
particularly for batch 5000 and the first modern repeat; all samples remain published.
Batch 5000 is an archive transaction shape,
**not a full-bootstrap speedup claim**. Parallel encoding spends more CPU and
RSS to recover elapsed-time throughput: the batch-500 CPU median rises from
3.693 CPU seconds raw and 5.905 serial to 7.279 optimized, with peak RSS
100.19 MiB versus 79.02 and 82.91 MiB.

The complete samples and generated comparisons are in
[tables-node.md](tables-node.md) and the four `node-*.jsonl` files.

## Bounds and supporting evidence

On production batch 500, peak encoded buffers are 4,811,572 bytes; on batch
5000 they are 5,836,179 bytes. Extra contexts peak at ten, bounded by the
existing Rayon pool. The larger transaction does not collect all of its
encoded frames at once. The full inherited corpus crosses segment boundaries.

[store-modern.jsonl](store-modern.jsonl) covers 5,000 mixed-size real Conway
bodies at batches 1, 500 and 5000 through both production flatfile paths.
[store-limits.jsonl](store-limits.jsonl) adds a seeded, incompressible 16 MiB
body among twenty mixed-size synthetic bodies spanning two segments. That
largest admitted body occupies its own window; peak encoded buffers are
33,640,132 bytes (32.08 MiB), with at most ten additional contexts. Bodies over
16 MiB are refused before writes in deterministic tests.

See [tables-store.md](tables-store.md) for timings, process CPU/RSS and commit
distributions. Store-level RSS is a lifetime process high-water mark; those
rows do not replace independent production-child RSS. For parallel encoding,
use process CPU, not the inherited caller-thread encode-CPU proxy.
`encode_threads` controls model sinks; actual offline context counts are in
`import_buffers.encoders_peak`.

The conservative encoded-buffer bound is
`max(8 MiB, B(16 MiB)) + (P + 2) * B(16 MiB)`, including the existing serial
encoder, where `B` includes zstd frame overhead and `P` is the executing pool
size. Vector metadata and native-context bounds are documented separately.
Growing-batch, one-worker, out-of-order completion, encode/partial-write/sync
failure, duplicate-location, index-commit failure, rollback and restart tests
pass. No per-batch threads, persistent job queue or background encoder is added.

The first per-block-mutex implementation was measured while another test suite
was active. [exploratory-overlap.jsonl](exploratory-overlap.jsonl) is retained
and explicitly excluded. It motivated investigation but establishes no
acceptance or isolated scheduler speedup claim.

The subsequent shared-reservation candidate was superseded because final
review restored the original live encoder allocation policy and restricted
exact reservation to offline contexts. Its six `superseded-*.jsonl` files and
`superseded-optimized.patch` are retained but excluded. Only the four unprefixed
`node-*.jsonl` files establish the final production verdict for source
`08f02d9bab9bd4d573e369a345a110e68c47567a`.

## Verification

[verification.json](verification.json) records actual commands and exit
statuses. The workspace build passes. Default tests pass (1,452 passed,
48 ignored); the required all-features selection passes (990 passed,
53 ignored). Clippy exits 0 with no new warnings; seven existing warnings
remain in untouched proposal/epoch test code and snapshot publish-test docs.

Mithril's actual import/resume driver exercises the offline writer. Logical
snapshot restore exercises it and resumes ordinary serial writes afterward.
The archive CLI preserves real same-slot Byron ordering and resumes without
duplicating history. Normal import/recovery and live sync remain explicitly
serial, including the shared work-unit lifecycle.

The Docker publish and restore suites remain blocked (exit 124 after 45 seconds
each). A non-disruptive `docker desktop start` reports already running, and
subsequent daemon checks still time out. No container or daemon was restarted.
