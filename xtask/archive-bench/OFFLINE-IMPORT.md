# Offline archive encoding

The opt-in is a writer intent, not a batch-size heuristic or node setting.
`ArchiveStore::start_writer` remains serial. The default implementation of
`start_import_writer` delegates to it; Fjall selects bounded parallel encoding.
The storage adapter forwards the intent, including memory, logs-only and no-op
backends. Frames, dictionary, zstd level 3 and storage v4 are unchanged.

## Call sites

| Caller | Intent |
| --- | --- |
| `data import-archive` | Explicit import writer after ordered parallel decode |
| Mithril `do_import` | `ImportExt::import_blocks_offline` |
| Snapshot blocks-layer restore | Explicit import writer per existing restore chunk |
| Offline snapshot backfill replay | `ImportExt::import_blocks_offline` |
| `ImportExt::import_blocks`, including doctor WAL catch-up and rebuild-state | Serial archive writes; existing no-WAL lifecycle |
| Normal sync, large roll batches, bootstrap WAL recovery, rollback | Serial archive writes |

The offline import lifecycle invokes `commit_archive_import` only for its
archive phase. Cardano dispatches this to the roll batch; other work units
retain their default phase. WAL, state, index, cursor and finalize ordering
are unchanged. Import intent does not depend on subscribers or input trust.

## Resource bounds

Let `B(n) = zstd::compress_bound(n)`, `M = 16 MiB` (the existing admitted-body
limit), `W = 8 MiB`, and `P = rayon::current_num_threads()` for the executing
pool. Before any writes, import rejects any body larger than `M`.

A scheduling window has sum of frame bounds at most `W`, or consists of
one larger admitted body. Thus owned completed frames occupy at most
`max(W, B(M))` bytes. Contexts are acquired from a call-local pool and reused
across windows: at most `P` additional contexts exist, alongside the store's
one existing serial encoder. Each additional context retains at most `B(M)`
bytes of encoding scratch; exact reservation avoids geometric Vec growth.
The existing serial encoder keeps its original amortized reservation policy,
with capacity below `2 * B(M)` on the pinned Rust toolchain. Total
encoded byte buffers, including any retained serial scratch, are bounded by
`max(W, B(M)) + (P + 2) * B(M)`, independent of
the caller's batch length. `import_buffer_bytes_peak` measures actual completed
frame capacities plus all retained encoder scratch capacities after each
window, when those buffers coexist. `import_window_bytes_peak` measures
completed frame lengths alone.

These byte counters exclude native zstd context allocations, the shared
prepared dictionary, allocator bookkeeping and Rayon stacks. Native contexts
are bounded in count by `P + 1` and operate at the fixed compression level with
inputs bounded by `M`; process peak RSS includes them. Frame-vector metadata
is also bounded: even an empty body's nonzero compression bound consumes
window budget, so at most `W / B(0)` entries (or one oversized-window entry)
exist, plus at most `P` task-vector headers. The input bodies, pending archive index work and returned locations
already scale with the caller's transaction; they are not an additional
encoded copy of the whole transaction.

The shared Rayon pool is reused. No threads or pools are created per batch,
no persistent work queue is added, and all jobs join before return, including
errors. Each window is split into at most `P` ordered runs; a task owns its
encoder across the run, so no mutex is acquired per block. Additional encoder
contexts and their scratch are dropped at the end of each call. A one-body
window reuses the store's serial encoder on the calling thread; that existing
encoder retains its scratch as ordinary writes already do. A one-worker pool works.
There is no payload-size crossover without measured evidence.

## Durability and errors

One physical writer holds the existing mutation lock for the whole batch.
Each window is collected in input order, then written in that order. Windows
do not commit indexes or sync files. All touched segment files and newly
created directory entries are synced at the original batch boundary before
locations are published to the index batch. Index durability remains the
existing Fjall policy; this change makes no cross-store atomicity claim.

Any append error drops touched handles so a retry opens at the true physical
end. Partial or complete unindexed frames remain dead space as before. Exact
duplicates retain the original indexed location, same-slot Byron bodies keep
their canonical ordering, and rollback continues to truncate by location.

Tests cover reversed completion using a condition variable, encoder failure,
partial writes, segment and directory sync failures, failure before index
commit, restart/retry, duplicate locations, rollback, one worker, empty and
maximal inputs, refusal of oversized bodies and growing caller batches.
Mithril tests run its actual import/resume driver; snapshot tests run its
actual logical restore. Domain tests compare offline, normal import and live
sync bodies, cursors, WAL behavior and writer counters.

## Measurement

Use the predecessor's immutable window and host, with no other load tests.
The production arms are raw `9165dbd8` with v3 config, serial-compressed
`b21c8d55` with v4 config, and this implementation with v4 config. The merged
`b21c8d55` tree equals the delivered `3273dd54` tree. Historical evidence
under `results/2026-09-09-m4-apfs-ssd-acceptance` is immutable.

`instrument-import.py CHECKOUT` emits an apply_patch patch for a disposable
checkout of each arm. Apply it before building. It adds identical monotonic
timing immediately around the production archive `writer.commit()` call,
stores all samples in memory, and writes them once after import if
`DOLOS_ARCHIVE_BENCH_METRICS` is set. The optimized arm also reports its buffer
counters. The instrumented sources are measurement artifacts, not production
configuration or a compression flag. Record the patch and binary hashes.

Build all arms with identical release settings. The node harness sets the
metrics destination inside each disposable instance. Missing instrumentation
is represented as null, never as a fabricated percentile. Instrumented and
uninstrumented records do not pair. The report requires three paired repeats,
at least 90% raw throughput and at most 110% raw commit p95 for instrumented
imports. It reports percentiles from individual commits, not elapsed time
divided by batch count.

```sh
cargo xtask archive-bench node \
  --bin baseline=bin/raw:v3 --bin serial=bin/serial:v4 \
  --bin optimized=bin/optimized:v4 --run offline-import-500 \
  --immutable "$WINDOW" --genesis "$GENESIS" --work "$WORK" \
  --out node-500.jsonl --workloads import --chunk-size 500 --repeat 3
cargo xtask archive-bench report node-500.jsonl
```

Also measure one-block Byron, representative modern-era bodies and bootstrap
batches. `bench --codecs store,store-import` exposes the production flatfile
paths for supporting mixed-size, segment-crossing and memory measurements.
It is not the production ingestion gate. The smoke preset exercises both
stores and validates bodies in CI; the offline codec is excluded from
concurrent reader/writer workloads. No archive-only result establishes a
full-bootstrap speedup.
