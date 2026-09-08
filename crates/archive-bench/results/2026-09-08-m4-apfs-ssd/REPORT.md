# Direct-write evidence, 2026-09-08

The measured answer to the write-benchmarks plan
(`plans/dolos-archive-compression-write-benchmarks.md` in the txpipe Brain):
does one zstd-3 frame per block, encoded in the foreground on append with
the bundled dictionary, keep the provisional budgets against an isolated
raw byte sink? Writes at live-tip and import batch sizes do. Writes at the
bootstrap batch size and the import-sized writer under query load do not,
by encode CPU alone. The founder ruled that the cutover keeps the serial
foreground path regardless (see the ruling below); an optional
parallel-encode experiment is reported after it as evidence for a later
hypothesis, not as the design. Reads are at parity when the disk is the
cost and add about two microseconds of decode per block when it is not.

The records are the `*.jsonl` files beside this report; `tables.md` and
`tables-encode-threads-4.md` are `dolos-archive-bench report` over them,
every repeat on its own row. This report quotes medians over the three
paired repeats. `RUNBOOK.md` has the commands.

## Host, corpus, method

| | |
|---|---|
| host | Apple M4 (10 cores), 16 GiB, macOS 26.5.2, internal SSD, APFS, zstd 1.5.7 |
| harness | `dolos-archive-bench` at `7475c9c7`; the binary that measured every benchmark record was built at that commit. `evaluate.jsonl` predates it: the dictionary evaluation ran on the same harness source before it was committed, so its records name the parent `5e4030af` as dirty. `concurrent.jsonl` and `write-encode-threads-4.jsonl` say `dirty: true` because the report renderer, README and smoke test were being edited in the tree while they ran; nothing on the measuring path changed. The tables were re-rendered by the QA revision of `report`, which lists every run it finds (revision, host, corpus) and pairs repeats within one run only; measurement values are unchanged; workstation paths in records and provenance are replaced with stable relative labels, and the tables and their run IDs are regenerated from those sanitized records |
| corpus | mainnet segments 448–451: 84,481 Conway blocks, 423.5 MiB, mean 5,256 B, largest 89,720 B. Disjoint from every segment the bundled dictionary was trained on (8–447) |
| codecs | `raw` (bodies unframed, the reference), `zstd3` (one dictionary-free frame per block), `zstd3-dict` (the same with the bundled `cardano.dict`, the intended production design) |
| durability | one `fdatasync` per touched segment per batch, every run; never disabled. The `concurrent.jsonl` writer records predate the harness writing `fsync` and `encode_threads` into the writer block, so the gate table shows those two as `?` for them; the runbook's step 5 ran with both at their defaults, fsync on and one encoder thread |
| cache regimes | `warm`: every segment streamed first. `evict`: 16 GiB of other files streamed through the page cache (macOS has no unprivileged per-file drop), so cold numbers are approximate. One workload per codec and repeat was not cold at all: the harness at `7475c9c7` restored the regime before every workload except the one whose name matched the first, and with `--threads 1,8` that is `point-uniform` at `t8`, which therefore read pages the `t1` `page-100` workload had just warmed (its 1–2 µs p50 and 32–45 MiB of disk reads, against 180–240 MiB for its neighbours, say so). Those nine records (`point-uniform [evict t8]`, three codecs by three repeats) are excluded from every cold-cache conclusion in this report and were not rerun; `point-local t8`, `page-100 t8` and `scan` were restored and are cold. The harness now restores by position (`presets::read_plan`, with a regression test) and records the restore on each record. `nocache` was not run |
| counters | thread CPU, process CPU and disk bytes (`proc_pid_rusage`) and the peak-heap allocator were all available; no record has a null counter |
| repeats | three, paired: each repeat runs every codec on the same workload before the next |

## Writes

Medians over three repeats. `blocks/s` is ingestion throughput; the batch
latency is the commit (encode, write, fdatasync) as the writer sees it.

| workload | codec | blocks/s | MiB/s | ratio | batch p50 / p95 / p99 ms | fsync p50 ms | encode MiB per CPU-s | CPU µs/block | peak heap |
|---|---|---|---|---|---|---|---|---|---|
| write-1 (live tip) | raw | 230 | 1.2 | 1.000 | 4.07 / 5.72 / 8.01 | 3.90 | — | 229 | 0 |
| | zstd3 | 229 | 1.1 | 0.737 | 4.08 / 5.76 / 8.45 | 3.88 | 76 | 267 | 0.15 MiB |
| | zstd3-dict | 225 | 1.1 | 0.588 | 4.15 / 5.90 / 7.84 | 3.91 | 45 | 325 | 0.15 MiB |
| write-100 (import) | raw | 10,400 | 52.1 | 1.000 | 9.09 / 13.03 / 19.97 | 4.55 | — | 39 | 0 |
| | zstd3 | 10,336 | 51.8 | 0.737 | 9.04 / 13.71 / 22.92 | 4.29 | 246 | 41 | 0.15 MiB |
| | zstd3-dict | 9,867 | 49.5 | 0.588 | 9.76 / 13.98 / 20.53 | 4.27 | 169 | 49 | 0.15 MiB |
| write-500 (bootstrap) | raw | 27,654 | 138.6 | 1.000 | 16.08 / 26.35 / 76.22 | 6.87 | — | 15 | 0 |
| | zstd3 | 21,364 | 107.1 | 0.737 | 21.94 / 30.05 / 43.06 | 6.15 | 382 | 27 | 0.17 MiB |
| | zstd3-dict | 19,810 | 99.3 | 0.588 | 24.00 / 35.06 / 88.93 | 5.67 | 238 | 33 | 0.17 MiB |

The single-block encode costs for `write-1` (45 and 76 MiB per CPU-s) are
per-call overhead on 5 KiB inputs and are the honest cost of a live-tip
append: about 21 µs of CPU per block with the dictionary, under a 3.9 ms
fdatasync. At batch 500 the dictionary encoder runs at 238 MiB per CPU-s,
and that is the number that matters: the corpus takes the raw sink 2.9 s
(1.7 s of per-frame writes, 1.2 s of fdatasync) and the dictionary codec
adds 1.8 s of serial encode on top, for 4.3 s. Every segment crossing
inside a batch (three per pass) cost nothing visible.

Buffer needs are small and flat: one reusable encoder context and one
output buffer per writer thread, so the peak transient heap above the
baseline is 0.15–0.17 MiB for the whole corpus whatever the batch size.

### Gates

At least 90% of raw ingestion throughput and at most 10% more p95 commit
latency, medians over paired repeats:

| workload | candidate | throughput vs raw | p95 vs raw | verdict |
|---|---|---|---|---|
| write-1 | zstd3 | 0.997 | 1.008 | PASS |
| write-1 | zstd3-dict | 0.979 | 1.032 | PASS |
| write-100 | zstd3 | 0.994 | 1.053 | PASS |
| write-100 | zstd3-dict | 0.949 | 1.073 | PASS |
| write-500 | zstd3 | 0.773 | 1.141 | FAIL |
| write-500 | zstd3-dict | 0.716 | 1.331 | FAIL |

## Append under query load

Half the corpus written, then the other half appended while eight reader
threads run a 90/10 point/page mix with 0.5 locality over the written
half. Both sides measured over the same span, medians over three repeats.

| workload | codec | writer blocks/s | batch p50 / p95 / p99 ms | reader ops/s | point p50 / p95 µs | page p95 ms |
|---|---|---|---|---|---|---|
| append-query-1 | raw | 241 | 3.97 / 5.35 / 9.64 | 153,131 | 5 / 8 | 0.63 |
| | zstd3 | 251 | 3.95 / 5.02 / 6.73 | 129,593 | 4 / 20 | 1.00 |
| | zstd3-dict | 246 | 3.98 / 5.29 / 7.66 | 139,586 | 4 / 16 | 0.91 |
| append-query-100 | raw | 13,767 | 6.98 / 9.72 / 15.05 | 149,294 | 5 / 8 | 0.65 |
| | zstd3 | 12,932 | 7.39 / 10.40 / 15.27 | 129,162 | 4 / 20 | 1.00 |
| | zstd3-dict | 11,476 | 8.16 / 12.35 / 15.91 | 141,080 | 4 / 15 | 0.88 |

| workload | candidate | throughput vs raw | p95 vs raw | verdict |
|---|---|---|---|---|
| append-query-1 writer | zstd3 | 1.042 | 0.939 | PASS |
| append-query-1 writer | zstd3-dict | 1.022 | 0.990 | PASS |
| append-query-100 writer | zstd3 | 0.939 | 1.070 | PASS |
| append-query-100 writer | zstd3-dict | 0.834 | 1.271 | FAIL |

The live-tip writer is unaffected by readers. The import-sized writer
loses 17% with the dictionary once eight readers compete for the ten
cores: the same encode CPU that is hidden under fdatasync at batch 1 and
absorbed at batch 100 on an idle host becomes visible when the host is
busy. Readers keep 91–94% of their raw throughput against a writer.

## Reads

The plan's read gate is p95 API point latency, which only the production
path can measure; these are the warm microbenchmark costs it asks to have
reported separately, plus the cold cases. Medians over three repeats,
`t1`/`t8` reader threads.

Warm cache, where a raw read is a memcpy out of the page cache:

| workload | codec | ops/s | point p50 / p95 / p99 µs | page-100 p50 / p95 ms | decode µs/block |
|---|---|---|---|---|---|
| point-uniform t1 | raw | 924,264 | 1 / 2 / 3 | | 0 |
| | zstd3 | 265,383 | 2 / 15 / 25 | | 2.7 |
| | zstd3-dict | 308,013 | 2 / 11 / 20 | | 2.2 |
| point-uniform t8 | raw | 1,662,171 | 4 / 7 / 9 | | 0 |
| | zstd3 | 1,352,555 | 3 / 17 / 31 | | 2.9 |
| | zstd3-dict | 1,347,357 | 3 / 14 / 30 | | 2.7 |
| page-100 t1 | raw | 14,123 | | 0.07 / 0.10 | 0 |
| | zstd3 | 2,979 | | 0.30 / 0.61 | 2.7 |
| | zstd3-dict | 3,612 | | 0.25 / 0.49 | 2.2 |
| page-100 t8 | raw | 16,547 | | 0.48 / 0.62 | 0 |
| | zstd3 | 15,359 | | 0.41 / 0.88 | 2.9 |
| | zstd3-dict | 15,639 | | 0.41 / 0.84 | 2.6 |
| scan (all 84,481 blocks, t1) | raw | 60 ms | | | 0 |
| | zstd3 | 277 ms | | | 2.6 |
| | zstd3-dict | 224 ms | | | 2.0 |

Decoding with the dictionary costs 2.0–2.7 µs per 5 KiB block, less than
without it (the frames are smaller). A warm point read goes from 1 µs to
2 µs at the median and from 2 µs to 10–14 µs at p95; a warm 100-block
page from 70 µs to 250 µs single-threaded. A whole-segment-range scan
decodes at 1.9 GiB/s per core against a 7 GiB/s memcpy. `point-local`
matched `point-uniform` in every regime and is in the tables.

Evicted cache (approximate on macOS, see method), where the disk is the
cost:

| workload | codec | ops/s | point p50 / p95 / p99 µs | page-100 p50 / p95 ms | disk read MiB | read amplification |
|---|---|---|---|---|---|---|
| point-uniform t1 | raw | 5,085 | 89 / 844 / 1,133 | | 244 | 2.42 |
| | zstd3 | 4,813 | 86 / 880 / 1,166 | | 209 | 2.81 |
| | zstd3-dict | 4,966 | 79 / 880 / 1,163 | | 184 | 3.08 |
| point-uniform t8 (measured warm, see method; excluded) | raw | 365,083 | 1 / 136 / 233 | | 45 | 0.45 |
| | zstd3 | 264,083 | 2 / 132 / 541 | | 38 | 0.52 |
| | zstd3-dict | 294,548 | 2 / 123 / 531 | | 32 | 0.54 |
| point-local t8 | raw | 83,688 | 100 / 221 / 359 | | 240 | 2.37 |
| | zstd3 | 98,653 | 94 / 201 / 294 | | 205 | 2.75 |
| | zstd3-dict | 99,624 | 86 / 206 / 412 | | 181 | 3.03 |
| page-100 t1 | raw | 626 | | 1.42 / 4.08 | 342 | 0.68 |
| | zstd3 | 560 | | 1.47 / 4.24 | 254 | 0.68 |
| | zstd3-dict | 626 | | 1.30 / 3.99 | 203 | 0.69 |
| scan t1 | raw | 321 ms | | | 424 | 1.00 |
| | zstd3 | 475 ms | | | 312 | 1.00 |
| | zstd3-dict | 398 ms | | | 249 | 1.00 |

Cold point and page reads are at parity: single-threaded p95 within 4%,
and 7% better for `point-local` at eight threads, reading 25% fewer bytes
from disk. The `point-uniform t8` rows were measured warm (see method) and
say nothing about the disk; `point-local t8` stands in for the
eight-thread cold point case. The read amplification of a cold point read rises from 2.4× to 3.1× because
a 16 KiB page now holds more of the neighbours' bytes, while the bytes
themselves fall. A cold scan is slower compressed (398 ms against 321 ms)
because the single reader decodes serially after each read instead of
overlapping with the disk; the harness does not model a production scan's
readahead or its consumer.

Mixed point/page loads at eight threads:

| mix | cache | codec | ops/s | point p95 µs | page p95 ms |
|---|---|---|---|---|---|
| 90/10 | warm | raw | 151,643 | 7 | 0.66 |
| | | zstd3-dict | 142,540 | 14 | 0.80 |
| 50/50 | warm | raw | 33,426 | 7 | 0.61 |
| | | zstd3-dict | 33,563 | 14 | 0.81 |
| 10/90 | warm | raw | 18,477 | 7 | 0.62 |
| | | zstd3-dict | 19,410 | 14 | 0.79 |
| 90/10 | evict | raw | 59,381 | 279 | 3.01 |
| | | zstd3-dict | 51,907 | 272 | 2.76 |
| 50/50 | evict | raw | 25,695 | 133 | 1.99 |
| | | zstd3-dict | 24,261 | 117 | 1.97 |
| 10/90 | evict | raw | 16,261 | 10 | 1.21 |
| | | zstd3-dict | 15,442 | 22 | 1.30 |

The dictionary codec keeps 94–105% of raw throughput warm and 87–94%
cold; `zstd3` is a few points behind it everywhere.

## Dictionary

`crates/flatfiles/dictionary/README.md` has the training provenance and
the cross-era, cross-network evaluation (`evaluate.jsonl`). On this
held-out window the bundled dictionary reaches 0.588 against 0.737 for
dictionary-free zstd-3, matching the 0.590 / 0.730 the evaluation
measured on 448–455. The knowledge base's ratios were measured with a
different, Conway-only dictionary and are not claimed here.

## The failed gates, the ruling, and the parallel-encode experiment

Two gates fail, both for the same reason: a single writer thread encodes
the whole batch before writing it, so at 500 blocks the 1.8 s of encode
CPU per 424 MiB is added serially to the sink's 2.9 s, and at 100 blocks
the same encode is exposed once readers compete for cores. Neither is an
I/O effect: the compressed runs write 25–40% fewer bytes and their fdatasync
is faster.

**Ruling** (`org/founder`, 2026-09-08, on the escalation these two
failures raised): the direct-write cutover keeps serial foreground
encoding with the bundled dictionary. The two misses stand as documented
implementation risks; they are not waived, and this isolated sink does not
show that end-to-end bootstrap overhead is negligible. The initial cutover
adds no parallel batch encoding, payload threshold, extra buffering,
configuration or housekeeping. The production verdict is still open: the
acceptance batch (`plans/dolos-archive-compression-acceptance.md`) keeps
the API and commit gates on the production path and must escalate a
measured, narrowly scoped per-block optimization if they fail.

**The experiment**, kept as evidence and not as the design: the runbook's
step 7 measured the harness's `--encode-threads 4` on the import and
bootstrap batches, the batch's blocks encoded by four contexts in parallel
and the frames still written whole, in order, one per block, with the
same fdatasync. Medians over three repeats
(`write-encode-threads-4.jsonl`, `tables-encode-threads-4.md`):

| workload | codec | blocks/s | batch p50 / p95 / p99 ms | throughput vs raw | p95 vs raw | verdict |
|---|---|---|---|---|---|---|
| write-500 | raw | 31,333 | 14.88 / 28.13 / 41.03 | | | |
| | zstd3 | 29,412 | 15.93 / 24.67 / 58.33 | 0.939 | 0.877 | PASS |
| | zstd3-dict | 30,012 | 16.06 / 22.02 / 40.24 | 0.958 | 0.783 | PASS |
| write-100 | raw | 10,477 | 9.09 / 12.98 / 18.87 | | | |
| | zstd3 | 9,343 | 10.27 / 15.09 / 18.92 | 0.892 | 1.162 | FAIL |
| | zstd3-dict | 9,003 | 10.67 / 15.10 / 19.05 | 0.859 | 1.163 | FAIL |

Parallel encoding turns the bootstrap batch into a pass with room to
spare, and makes the import batch worse: the harness spawns its scoped
threads per batch and collects each frame into its own allocation
(peak heap 1.2–5.4 MiB against 0.15 MiB), and at 500 KiB of payload per
batch that overhead exceeds the 2.5 ms of encode it parallelizes.

What this supports is one hypothesis for a later bootstrap optimization:
parallel encode pays once a batch carries enough payload. It establishes
nothing more. The crossover lies somewhere between 100 and 500 blocks at
5 KiB each, which does not fix a threshold (the ~1 MiB figure proposed to
the founder is an estimate, not a measurement), and the experiment makes
the import-sized writer worse rather than addressing its miss under query
load. What was proposed with the escalation, and not adopted for the
initial cutover: parallel encode above a payload threshold on the existing
rayon pool with the single-context path below it, frames written in slot
order, and frame coalescing per segment as the next lever (the 1.7 s of
per-frame `write` calls is a floor the raw sink pays too). If the
acceptance batch fails its production gates, that proposal is where its
escalation starts, measured on the production path. Nothing in it
reintroduces a raw mode, a chunk mode, or a weaker fsync.

## Cross-checks for the successors

- Every `zstd3-dict` frame written by the harness decoded to its original
  bytes in the smoke test's `--verify` mode; the measured runs skipped
  verification for speed, and the length check on every read stayed on.
- `encode_threads: 1` is the foreground design and the default; every
  gate table above except the last used it.
- The `point-uniform [evict t8]` records in `read.jsonl` were measured
  warm (see method). Nothing here cites them as cold, and a successor must
  not either; the eight-thread cold point case is `point-local t8`.
- To reproduce: `RUNBOOK.md`. To regress: the same commands at a new
  revision, then `dolos-archive-bench report` over both sets of records.
  The report lists each run and pairs repeats within one run only, so the
  old and new records render side by side and never share a median; the
  serial foreground path (`encode_threads 1`, fsync on) is the setting to
  compare, and the gate rows say which settings each row was measured
  under.
