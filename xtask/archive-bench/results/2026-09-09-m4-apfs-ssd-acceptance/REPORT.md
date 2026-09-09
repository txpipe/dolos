# Direct-write acceptance, 2026-09-09

The production comparison the archive-compression acceptance batch
(`plans/dolos-archive-compression-acceptance.md` in the txpipe Brain) asks
for: the pre-cutover `dolos` binary and the cutover binary, on the same host,
corpus, durability and concurrency, through the node's own import and API
paths, three paired repeats. The gates are at least 90% of the baseline's
ingestion throughput and at most 10% higher API point p95 latency.

**Verdict.** Live-tip ingestion (one block per commit) is at parity. Every
API point workload — uniform and local, 1 and 8 threads, warm and evicted
cache — holds the 10% p95 budget. Bootstrap-shaped ingestion (batches of
500 blocks) does **not** hold the 90% throughput budget: the cutover binary
imports the corpus at 61% of the baseline's rate, because the serial
foreground encode adds about 2.5 s of CPU to a 17 s import. The measured
per-block optimization — the same store at zstd level 1 — recovers to 77%
for a compressed size 17% larger, and does not close the gap either. This
is the constraint the plan asks to be reported to `org/founder` rather than
waived: the budget can only be met by spending the encode CPU somewhere
other than the import thread (parallel encoding, not pre-approved) or by
accepting a bootstrap that is roughly 40% slower on this host.

The records are `node.jsonl` (the production comparison), `write.jsonl` and
`concurrent.jsonl` (the store-level supplement) beside this report;
`tables-node.md` and `tables-store.md` are `cargo xtask archive-bench
report` over them, every repeat on its own row. This report quotes medians
over the three paired repeats. `RUNBOOK.md` has the commands.

## Host, binaries, corpus, method

| | |
|---|---|
| host | Apple M4 (10 cores), 16 GiB, macOS 26.5.2, internal SSD, APFS, zstd 1.5.7 |
| harness | `cargo xtask archive-bench` from this pull request's tree (the records name the base commit `1c945421` as dirty: the harness had not been committed when it ran); the node harness measures binaries, so its own revision does not touch the measured path |
| `baseline` | dolos at `9165dbd8`, the last revision whose store appended raw bodies (the merge of #1311, parent of the cutover #1312); release profile, default features; reads a `v3` configuration; SHA-256 `6f22bd86…` |
| `candidate` | dolos at this pull request's head, before the results directory was added; release profile, default features; `v4`; SHA-256 `2d6205d6…` |
| `candidate-zstd1` | the candidate with `COMPRESSION_LEVEL` set to 1 in `crates/flatfiles/src/codec.rs` and nothing else changed — the optimization candidate, not a proposal to ship; SHA-256 `3105a350…` |
| corpus | a Mithril mainnet snapshot's immutable chunks 08881–08959 (mainnet slots 191,829,600–193,535,999, segments 444–448, 83,551 Conway blocks, 622 MiB) plus chunks 00000–00001 (43,177 Byron blocks, 21 MiB), which the node's reader needs for the origin check and the state seed; 126,728 blocks, 643.1 MiB in all. Segments 440–447 were among the bundled dictionary's training segments, so the compressed size below is a little better than the held-out 0.590 the dictionary README reports; throughput is what is measured here and does not depend on that |
| durability | the node's own: every frame written and `fdatasync`ed before its location is committed; one fjall batch commit per import chunk; never disabled |
| concurrency | imports single-process; reads with 1 and 8 client threads over one HTTP/2 connection each against a `dolos serve` with only the UTxO RPC service configured |
| cache regimes | `warm`: every segment streamed first. `evict`: 16 GiB of other files streamed through the page cache before every read workload (macOS has no unprivileged per-file drop), so evicted numbers are approximate |
| repeats | three, paired: each repeat runs baseline, candidate and candidate-zstd1 in turn, each on a fresh disposable instance |
| excluded | the readahead-off (`nocache`) regime has no meaning through a server; append under concurrent API load and batch commit percentiles are not observable at the node level (see the store-level supplement) |

## Ingestion

`dolos data import-archive` over the whole corpus, medians over three repeats.
`ms/batch` is the whole import divided by its batches (decode, append,
fsync, index commit); the child's CPU and peak RSS are the kernel's
(`wait4`).

| workload | binary | blocks/s | vs baseline | ms/batch | CPU s | peak RSS MiB | segments MiB | ratio | verdict |
|---|---|---|---|---|---|---|---|---|---|
| import-500 (bootstrap) | baseline | 36,642 | 1.000 | 13.6 | 3.8 | 80 | 643 | 1.000 | |
| | candidate | 22,313 | **0.609** | 22.4 | 6.3 | 82 | 342 | 0.531 | **FAIL** |
| | candidate-zstd1 | 28,099 | **0.767** | 17.8 | 5.0 | 82 | 402 | 0.624 | **FAIL** |
| import-1 (live tip, 2,000 blocks) | baseline | 237 | 1.000 | 4.2 | 0.5 | 16 | 2 | 1.000 | |
| | candidate | 236 | 0.996 | 4.2 | 0.6 | 18 | 1 | 0.429 | PASS |
| | candidate-zstd1 | 237 | 1.001 | 4.2 | 0.5 | 17 | 1 | 0.457 | PASS |

The live-tip shape is bound by the `fdatasync` and the index commit — 4.2 ms
a block on every binary — and the encode hides under it entirely. At the
bootstrap shape nothing hides it: the baseline imports 643 MiB in 17.5 s and
the candidate in 28.8 s, and the difference is the encode running serially
on the import thread (about 250 MiB per CPU-second at level 3, in line with
the store-level measurement below). Level 1 encodes at roughly twice that
rate and brings the import to 22.9 s, still 23% slower than raw, while
storing 17% more bytes. Peak resident memory is unchanged by compression
(80–82 MiB at batch 500): the encoder holds one context and one output
buffer.

## Reads through the UTxO RPC service

`FetchBlock` by slot (point) and `DumpHistory` of 100 blocks (page) against
`dolos serve`, medians over three repeats. Point p95 is the gated figure.

Warm cache:

| workload | binary | ops/s | vs baseline | point p50 / p95 / p99 µs | p95 vs baseline | page p95 ms | verdict |
|---|---|---|---|---|---|---|---|
| point-uniform t1 | baseline | 6,965 | 1.000 | — / 361 / 794 | 1.000 | | |
| | candidate | 6,782 | 0.974 | — / 368 / 827 | 1.019 | | PASS |
| point-uniform t8 | baseline | 26,230 | 1.000 | — / 699 / 1,419 | 1.000 | | |
| | candidate | 26,532 | 1.012 | — / 705 / 1,425 | 1.008 | | PASS |
| point-local t1 | baseline | 7,166 | 1.000 | — / 348 / 702 | 1.000 | | |
| | candidate | 7,090 | 0.989 | — / 358 / 724 | 1.029 | | PASS |
| point-local t8 | baseline | 26,956 | 1.000 | — / 685 / 1,391 | 1.000 | | |
| | candidate | 25,034 | 0.929 | — / 729 / 1,481 | 1.064 | | PASS |
| page-100 t1 | baseline | 136 | 1.000 | | | 17.9 | – |
| | candidate | 131 | 0.957 | | | 18.2 | – |
| page-100 t8 | baseline | 461 | 1.000 | | | 38.8 | – |
| | candidate | 454 | 0.984 | | | 38.9 | – |
| mix-90-10 t8 | baseline | 3,985 | 1.000 | — / 1,170 / 3,144 | 1.000 | 37.4 | |
| | candidate | 3,881 | 0.974 | — / 1,203 / 3,308 | 1.028 | 37.6 | PASS |
| mix-50-50 t8 | baseline | 913 | 1.000 | — / 3,617 / 7,078 | 1.000 | 38.8 | |
| | candidate | 884 | 0.968 | — / 3,674 / 7,180 | 1.016 | 39.8 | PASS |
| mix-10-90 t8 | baseline | 513 | 1.000 | — / 4,714 / 8,167 | 1.000 | 39.9 | |
| | candidate | 501 | 0.975 | — / 5,083 / 8,114 | 1.078 | 41.0 | PASS |

Evicted cache (16 GiB streamed before each workload):

| workload | binary | ops/s | vs baseline | point p95 / p99 µs | p95 vs baseline | page p95 ms | verdict |
|---|---|---|---|---|---|---|---|
| point-uniform t1 | baseline | 4,294 | 1.000 | 503 / 953 | 1.000 | | |
| | candidate | 4,411 | 1.027 | 496 / 969 | 0.986 | | PASS |
| point-uniform t8 | baseline | 23,147 | 1.000 | 752 / 1,481 | 1.000 | | |
| | candidate | 23,160 | 1.001 | 744 / 1,493 | 0.989 | | PASS |
| point-local t1 | baseline | 4,325 | 1.000 | 500 / 881 | 1.000 | | |
| | candidate | 4,522 | 1.046 | 489 / 899 | 0.977 | | PASS |
| point-local t8 | baseline | 23,937 | 1.000 | 734 / 1,406 | 1.000 | | |
| | candidate | 24,138 | 1.008 | 745 / 1,447 | 1.015 | | PASS |
| page-100 t1 | baseline | 133 | 1.000 | | | 18.0 | – |
| | candidate | 129 | 0.966 | | | 18.6 | – |
| page-100 t8 | baseline | 472 | 1.000 | | | 36.2 | – |
| | candidate | 470 | 0.995 | | | 37.9 | – |
| mix-90-10 t8 | baseline | 4,070 | 1.000 | 1,198 / 2,798 | 1.000 | 35.5 | |
| | candidate | 4,054 | 0.996 | 1,168 / 2,834 | 0.975 | 34.3 | PASS |
| mix-50-50 t8 | baseline | 927 | 1.000 | 3,555 / 6,840 | 1.000 | 37.6 | |
| | candidate | 897 | 0.967 | 3,656 / 7,082 | 1.028 | 38.9 | PASS |
| mix-10-90 t8 | baseline | 521 | 1.000 | 4,760 / 7,426 | 1.000 | 39.8 | |
| | candidate | 508 | 0.975 | 5,218 / 8,634 | **1.096** | 40.8 | PASS |

The candidate-zstd1 binary's read figures are in `tables-node.md`; they sit
where the candidate's do (its worst point p95 ratio is 1.064, its worst mix
1.072), which is expected: the level changes the encoder, not the decoder.

What these tables say: through the node the archive read is a fraction of
the request. A `FetchBlock` costs 350–500 µs at one thread, of which the
frame decode is about 2 µs (the store-level figure); the rest is the index
lookup, the block-to-protobuf mapping and the transport, and it is the same
on both binaries. The worst candidate point p95 is 6.4% over the baseline
(point-local, warm, 8 threads), the worst mix 9.6% (mix-10-90, evicted, 8
threads, where a 100-block page decodes 100 frames per request); every
figure is under the 10% budget, several are under the baseline, and the
spread between repeats is of the same size as the differences, so the
honest reading is parity within noise. Pages and scans are 3–10% slower —
100 decodes per page, 84,000 per scan — and have no gate.

The scan (`dolos data dump-blocks` over the contiguous 83,551-block tail, the
node's own iterator over every frame) runs at 35,600 blocks/s on the
baseline and 32,000 on the candidate (0.897), warm; the evicted numbers are
the same to within a percent, so on this SSD the scan is CPU-bound on both.

## Store level, as supplement

`cargo xtask archive-bench bench` on the write-benchmarks corpus (mainnet
segments 448–451, 84,481 Conway blocks, 423.5 MiB), the production
`FlatFileStore` (`store`) beside the raw byte sink and the level-1 and
level-3 dictionary codecs of the modelled sink, three paired repeats,
`fdatasync` per batch. This is what the node level cannot see: batch commit
percentiles at the store, and the writer under concurrent readers. The raw
sink is a codec reference, not a production baseline; the harness gates
every codec against it anyway, and `tables-store.md` carries the verdicts.
Medians over the three repeats:

| workload | codec | blocks/s | vs raw | batch p95 ms | p95 vs raw | CPU µs/block | peak heap MiB |
|---|---|---|---|---|---|---|---|
| write-100 | raw | 15,854 | 1.000 | 8.7 | 1.000 | 15.2 | 0.0 |
| | zstd1-dict | 12,443 | 0.785 | 10.5 | 1.208 | 36.3 | 0.2 |
| | zstd3-dict | 10,460 | 0.660 | 13.1 | 1.502 | 50.8 | 0.2 |
| | store | 10,262 | 0.647 | 13.6 | 1.565 | 51.3 | 0.2 |
| write-500 | raw | 39,105 | 1.000 | 18.9 | 1.000 | 8.9 | 0.0 |
| | zstd1-dict | 35,932 | 0.919 | 18.4 | 0.974 | 15.2 | 0.2 |
| | zstd3-dict | 25,762 | 0.659 | 28.0 | 1.486 | 25.6 | 0.2 |
| | store | 25,198 | 0.644 | 28.5 | 1.510 | 26.0 | 0.2 |

The production store at batch 500 sits where the node-level bootstrap
import does (0.644 here, 0.609 through the node), and the modelled
level-3 codec sits with it: the cost is the encode, not the store's
bookkeeping. The batch p95 the node cannot see is 50% over raw at both
batch sizes; every batch is one `fdatasync` on both sides, so the
difference is encode time inside the batch. Peak heap over a whole run is
0.2 MiB above the raw sink for every compressed codec: one encoder context
and one output buffer, however many segments the corpus spans.

Append under eight reader threads (`append-query`, the first 5,000 blocks
of each segment, readers on the already-written head):

| workload | codec | writer blocks/s | vs raw | writer batch p95 ms | p95 vs raw | reader ops/s | reader point p95 µs | verdict |
|---|---|---|---|---|---|---|---|---|
| append-query-1 | raw | 226 | 1.000 | 5.4 | 1.000 | 150,931 | 7 | |
| | zstd3-dict | 223 | 0.983 | 5.9 | 1.081 | 152,373 | 13 | PASS |
| | store | 219 | 0.970 | 5.7 | 1.053 | 12,910 | 91 | PASS |
| append-query-100 | raw | 16,856 | 1.000 | 7.7 | 1.000 | 144,554 | 8 | |
| | zstd3-dict | 10,809 | 0.641 | 13.3 | 1.726 | 147,215 | 13 | FAIL |
| | store | 10,840 | 0.643 | 12.5 | 1.677 | 12,996 | 91 | FAIL |

The live shape (batch 1) holds both budgets under read load, as it does
through the node; the batched shape misses them by the same margin as
without readers, so concurrent reads neither hide nor worsen the encode.
The readers' own figures are not comparable across the codec column: the
raw and modelled sinks answer a point read with a memcpy out of an
in-memory index, while the `store` column is the production
`FlatFileStore` opening the segment by path and decoding the frame — 91 µs
against 7, the price of the read path itself, which the node-level tables
above show to be a small fraction of an API request. Reader throughput on
the store is the same with a batch-1 and a batch-100 writer, so the append
lock does not stall readers.

## Removal inventory

The eight layers the direct-write cutover (#1312) was to have emptied,
checked at this pull request's head by reading the tree and by searching
code, help text, examples and documentation for the retired vocabulary
(`seal`, `thaw`, `train-dictionary`, `restore-raw`, `.zseg`,
`block_compression`, `DictionaryDir`, `chunk_target`, seek table, frame
cache, lease). The search finds nothing left except the intentional
refusals and their tests named below; historical mentions in
`results/2026-09-08-m4-apfs-ssd/` and the changelog are kept as history.

| layer | removed | retained, and why |
|---|---|---|
| reads | the seekable-container reader, span assembly, chunk decoding, `read_location`, the raw-representation branch | one `read(&BlockLocation)`: open the segment by path, read the frame at its physical offset, decode with the bundled dictionary. `BlockIter` skips a frame it cannot read and `get_block_by_slot` returns the error — the unreadable-frame behaviour a torn indexed frame needs, covered by `tests/archive_segments.rs` |
| caching | the decoded-frame cache, the seek-table cache, the dictionary cache and their config bounds | a pool of at most eight idle decoders, each holding one zstd context and at most a megabyte of buffers: bounded by concurrent reads, not by data. The page cache is the only block cache |
| rollback | thaw-before-mutate, the transition journal (`layout.rs`) | `undo` truncates the segment at the removed frame's start; `truncate_front` at the earliest removed frame. Both cut at frame boundaries the index already knows, so no scan of the file is needed |
| memory | seek-entry vectors, chunk scratch buffers, whole-segment conversion copies | one encoder context and output buffer behind the append lock; one append handle for the newest segment the last batch touched (this pull request; previously one per segment written since open); the decoder pool above. Peak RSS at bootstrap batch size is unchanged from the raw store (80 → 82 MiB) |
| config | `storage.archive.block_compression` and every field under it (profile, dictionary path, chunk target, cache bounds) | `FjallArchiveConfig` is `deny_unknown_fields`, so a leftover table fails the load naming the key (`crates/core/src/config.rs`, `src/bin/dolos/init.rs` tests). The `v4` storage version is the only enforcement, at the configuration level, by founder ruling |
| benchmarks | the standalone `crates/archive-bench` crate, its binary, its `Cargo.lock` package, the `dolos-archive-bench` command | `cargo xtask archive-bench` in the existing developer-tasks crate: the same workloads, presets, paired comparison, machine-readable records, fixture smoke test and privacy guard, plus the node-level paired runner. No second harness exists |
| maintenance | `dolos data archive-compression {train-dictionary, seal, inspect, restore-raw}`, the directory lease, the operations doc page | none. Dictionary training and evaluation are `cargo xtask archive-bench train` / `evaluate`, developer-only; the node has no dictionary command |
| writes | the raw append path, the seal step, the mixed-representation dispatch, the `merge_location` body-length shortcut | `append_batch`: encode each body, write its frame, `fdatasync` every touched segment, sync the directory when a segment is new, then hand the locations to the index batch. A failed batch drops its handles so the retry reopens at the true end |

## Findings

1. **Bootstrap-shaped ingestion misses the 90% budget** (0.609 with the
   shipped level-3 codec, 0.767 at level 1), on this host, this corpus,
   with the encode serial on the import thread. Live ingestion and every
   API point read are within budget. The constraint and the measured
   per-block option go to `org/founder` with this report; the plan does not
   let this batch pass by disabling compression, weakening durability or
   restoring the raw path, and parallel encoding is not pre-approved.
2. **The level-1 option is measured but not recommended as shipped**: it
   recovers a third of the gap for 17% more storage on mainnet Conway, and
   changing the level is a storage-format decision (every frame's dictionary
   and level are fixed by `v4`).
3. **Append under concurrent API load is measured at the store level only.**
   The node cannot import and serve from one store at once, and a syncing
   daemon needs an upstream. The store-level `concurrent` records are the
   evidence for that shape.
4. **The scan is 10% slower** through `dump-blocks`, CPU-bound on both
   binaries, and has no gate; a full-archive scan is not an operator path
   the node takes.
