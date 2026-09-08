# dolos-archive-bench

Developer benchmarks for the archive's per-block compressed segments. Not
part of the `dolos` binary and not a production code path: the crate models
the direct-write design — one complete zstd frame per block, appended to a
segment file and addressed by its physical offset and compressed length, one
`fdatasync` per touched segment per batch — and measures it against an
isolated raw byte sink on the same workloads. The presets are the regression
guard; the results directory holds the measured evidence the cutover was
judged on.

## One command

```sh
cargo run --release -p dolos-archive-bench -- bench \
  --preset all \
  --corpus /path/to/raw/segments --segments 448..451 \
  --work /fast/disk/archive-bench.noindex \
  --out results.jsonl --repeat 3 --cache warm,evict --evict-from /some/big/dir
cargo run --release -p dolos-archive-bench -- report results.jsonl
```

`bench` appends one JSON record per measurement to `--out`; every record
carries the environment (revision, OS, CPU, memory, zstd version, the
filesystem under `--work`, which counters the host provides), the corpus
(source, block and byte counts, eras, segments), the codec, the repeat and
the cache regime beside the metrics. `report` renders markdown tables and the
gate verdicts from any set of result files.

Presets:

| preset | what runs |
|---|---|
| `write` | the corpus appended in slot order in batches of 1 (live tip), 100 (pull and backfill import) and 500 (mithril bootstrap) blocks, paired across codecs; encode CPU, ingestion throughput, batch commit latency, fsync latency, peak transient heap, process CPU and disk bytes |
| `read` | over a store written once per codec: uniform and local point reads, 100-block pages, a whole scan, at each `--threads` count and each `--cache` regime |
| `mixed` | 90/10, 50/50 and 10/90 point/page mixes at the highest `--threads` |
| `concurrent` | half the corpus written, then the other half appended in batches of 1 and 100 while readers run a 90/10 mix; both sides measured over the same span |
| `all` | all of the above |
| `smoke` | `all` at a size that finishes in seconds; `cargo test -p dolos-archive-bench` runs it on a synthetic corpus |

Codecs (`--codecs`): `raw` (the reference sink: bodies unframed), `zstd3`
(one dictionary-free zstd level 3 frame per block) and `zstd3-dict` (the
same with `--dictionary`, by default the one bundled in `dolos-flatfiles`).

Corpora: `--corpus DIR --segments 446..449` walks raw `NNNNNN.segment` files
as CBOR items and decodes each with pallas for its slot and era;
`--immutable DIR` reads a Cardano node immutable directory through pallas;
`--synthetic N` is the seeded stand-in the smoke test uses.

Cache regimes (`--cache`): `warm` primes every segment; `evict` drops the
segments' pages and leaves readahead on (`posix_fadvise` on Linux; on macOS
there is no unprivileged drop, so `--evict-from DIR --evict-gib N` streams
that much of other data through the cache instead); `nocache` evicts and
then reads with caching off at the descriptor (`F_NOCACHE`, which also
disables readahead — pessimistic for scans). A regime the host cannot
produce is skipped and said so on stderr. Cold numbers on macOS carry the
stream method in their `cache` field; treat them as approximate.

Counters: thread CPU is `CLOCK_THREAD_CPUTIME_ID`; process CPU and disk
bytes come from `proc_pid_rusage` on macOS and `getrusage` plus
`/proc/self/io` on Linux; peak transient heap is a counting global
allocator in the binary. A counter the host lacks is `null` in the record
and named under `environment.counters`.

## Gates

`report` judges every non-raw codec against raw on the median over paired
repeats: at least 90% of raw ingestion throughput and at most 10% higher
p95 commit latency on every write workload, and at most 10% higher p95
point latency on every read workload. These are the provisional gates the
cutover plan carries; a failure is a finding for the founder, not a reason
to weaken fsync or add a raw mode.

## Dictionary

```sh
cargo run --release -p dolos-archive-bench -- train \
  --corpus /path/to/mainnet/segments \
  --sample 8,25,40,60,120,250,330,380=1000 --sample 440..447=1500 \
  --seed 0 --max-size 112640 --out cardano.dict
cargo run --release -p dolos-archive-bench -- evaluate \
  --fixture mainnet-heldout=/path/to/mainnet/segments:448..455 \
  --fixture preprod=/path/to/preprod/segments:100,200,300 \
  --fixture preview=immutable:/path/to/preview/immutable \
  --dictionary bundled --dictionary cardano.dict --out evaluate.jsonl
```

`train` samples a seeded subset of each segment's blocks (`--segments`
with `--samples-per-segment`, or weighted `--sample SEGMENTS=COUNT` groups)
and runs zstd's trainer; `<out>.json` records every input file's SHA-256, the sample per
segment, the seed, the size cap and the zstd version, so the same command
over the same files yields the same bytes. `evaluate` encodes and decodes
every block of every fixture with the dictionary-free codec and each
dictionary given, checks the round trip, and reports ratio, encode
throughput and decode cost per block, per era.

The bundled asset lives at `crates/flatfiles/dictionary/cardano.dict` with
its provenance beside it; `dolos_flatfiles::compressed::bundled_dictionary`
exposes it and `crates/flatfiles/tests/bundled_dictionary.rs` pins its hash
and the 128 KiB budget.

## Results

`results/` holds the measured runs, one directory per host and date, each
with the raw `*.jsonl` records and a `REPORT.md` rendered from them with
the corpus, host and verdicts spelled out.
