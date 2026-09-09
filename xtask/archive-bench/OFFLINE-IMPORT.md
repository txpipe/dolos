# Automatic archive encoding

The founder revised the offline-only ruling during PR #1317 review: optimize
eligible batches, rather than reserve parallel encoding for offline callers.
All callers now use the original APIs. There is no writer intent, batch flag,
node setting, global switch, second commit hook or offline import method.
The filename is retained so historical evidence links remain valid.

## Selection and trade-offs

Flatfiles alone selects encoding strategy. A byte-bounded window qualifies
when it contains at least two bodies totaling at least 256 KiB and the shared
Rayon pool has more than one worker. Empty/tiny batches, one large body and
small trailing windows use the original reusable serial encoder, without
allocating owned frames. Large caller batches consisting only of individually
oversized windows also stay serial. The cutoff is an internal scheduling
policy, not a storage format or operator setting.

Parallelism spends additional process CPU and memory to reduce elapsed
encoding time. Benefits depend on payload sizes, compressibility, CPU count,
fsync latency and competing work; a fixed byte cutoff is not a universal
crossover guarantee. One dominant body offers little parallel work, and
concurrent queries can lose CPU even though readers do not take the writer
lock. Selection does not claim CPU isolation or unchanged query tail latency.
These are resource/performance trade-offs, not reasons to distinguish live
and offline bodies semantically.

Rayon callers stay serial to avoid nested work stealing while holding the
physical writer lock. A Rayon caller waiting for that lock executes queued
pool work rather than blocking a worker, so external appenders can finish
encoding even when pool workers also want the same store. Serial encoding
never yields while holding the writer lock. No new pool, thread per batch,
persistent job queue or background encoder is added. Deterministic tests
exercise worker starvation and nested callers.

## Unified call paths

| Caller | Unchanged entry point |
| --- | --- |
| Archive CLI and logical snapshot block restore | `ArchiveStore::start_writer` |
| Mithril, offline backfill, doctor catch-up and rebuild | `ImportExt::import_blocks` |
| Live sync, including large roll batches | `WorkUnit::commit_archive` |
| Cardano roll work | `WorkBatch::commit_archive` |
| Fjall archive commit | `FlatFileStore::append_batch` |

There is one method at every layer. Memory, logs-only and no-op backends need
no optimization hooks. Import still skips WAL and notifications; sync still
performs its full lifecycle. Encoding selection changes neither lifecycle.

## Resource bounds and durability

Let `B(n) = zstd::compress_bound(n)`, `M = 16 MiB`, `W = 8 MiB`, and
`P = rayon::current_num_threads()`. Every batch is size-validated before
opening or writing segments; oversized refusal now leaves the entire batch
unwritten for every caller.

A window holds frame bounds totaling at most W, or one larger admitted body.
Owned completed frames occupy at most `max(W, B(M))`. Up to P additional
contexts reuse exact-reservation scratch across windows; the existing serial
encoder retains amortized scratch below `2 * B(M)` on the pinned toolchain.
The conservative encoded-buffer bound is
`max(W, B(M)) + (P + 2) * B(M)`, independent of transaction length.
`encoded_buffer_bytes_peak` includes owned frame capacities and all retained
encoder scratch, including successful serial batches. Native contexts are
bounded by P + 1; process RSS includes them and allocator overhead.
Frame-vector metadata is bounded by W / B(0) entries plus P task-vector
headers. Inputs, pending index records and returned locations already scale
with the transaction; they are not an extra encoded copy.

One physical writer retains the lock across the whole batch. Frames are
written in input order. Scheduling windows add no fsyncs or index commits.
Touched segments and new directory entries sync before locations reach the
index batch, preserving the existing Fjall durability policy. Error cleanup,
unindexed dead space, original duplicate locations, same-slot Byron ordering,
rollback and restart/retry remain covered. No new cross-store atomicity,
framing, dictionary, compression level or storage-version claim is made.

## Evidence

The initial offline candidate and its exact identities remain immutable in
`results/2026-09-09-m4-apfs-ssd-offline-import`; those measurements do not
describe the revised automatic candidate. New evidence is published separately.

Use `instrument-import.py CHECKOUT` on disposable source trees to add actual
commit timing, then compare pinned raw production `9165dbd8`, merged serial
production `b21c8d55`, and the revised candidate with equal release settings,
correct v3/v4 configs and production durability. Use three paired full-corpus
500-block repeats; the gates remain 90% raw throughput and at most 110% raw
commit p95. Also report tiny, modern, bootstrap-sized and concurrent workloads.

The supporting harness now has one production codec, `store`, which uses
automatic selection for write and concurrent presets. Historical
`store-import` results remain readable but that execution option is removed.
The legacy JSON key `import_buffers` is retained for result compatibility;
its counters now describe automatic encoding. Deterministic smoke coverage
runs in CI. Hardware results remain manual and must not overlap other tests
or load tests. Archive-only timings do not establish a bootstrap speedup.
