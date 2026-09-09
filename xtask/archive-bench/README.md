# Storage benchmarks

Minibf route, live-replay and real-node HTTP benchmarks are documented in
[MINIBF.md](MINIBF.md), including paired version comparisons and CI work guards.

Developer benchmarks for the archive's compressed block segments, run as
`cargo xtask bench storage`. The former `cargo xtask archive-bench` entry point
remains compatible with existing storage scripts. Not part of the `dolos` binary and not a
production code path. Two layers of workloads:

- **store level** (`bench`): the production `dolos_flatfiles::FlatFileStore`
  beside a modelled sink — one zstd frame per block at a physical location,
  or the same bodies raw — on the same append, point, page, mixed and
  append-under-query workloads, so codec cost and I/O cost can be told apart.
  The raw sink is supplemental evidence about the codec; it is not a
  production baseline.
- **node level** (`node`): `dolos` binaries driven through their own import
  and API paths — `data import-archive` over a node immutable directory,
  `data dump-blocks` over the imported history, and the UTxO RPC sync service
  under a threaded query driver — so two revisions are measured on the same
  host, corpus, durability and concurrency. This is the production
  comparison the acceptance gates are judged on.

`report` renders markdown tables and gate verdicts from any set of result
files; `train` and `evaluate` are the developer-only dictionary tooling. The
`smoke` preset runs every store-level workload on a synthetic corpus in
seconds; `cargo test -p xtask` runs it, so the harness stays green in CI
without a corpus or a binary. `results/` holds the measured runs, one
directory per host and date.

## Store level

```sh
cargo xtask bench storage bench \
  --preset all \
  --corpus /path/to/raw/segments --segments 448..451 \
  --work /fast/disk/archive-bench.noindex \
  --out results.jsonl --repeat 3 --cache warm,evict --evict-from /some/big/dir
cargo xtask bench report results.jsonl
```

`bench` appends one JSON record per measurement to `--out`; every record
carries the environment (revision, OS, CPU, memory, zstd version, the
filesystem under `--work`, which counters the host provides), the corpus
(source, block and byte counts, eras, segments), the codec, the repeat and
the cache regime beside the metrics.

Presets:

| preset | what runs |
|---|---|
| `write` | the corpus appended in slot order in batches of 1 (live tip), 100 (pull and backfill import) and 500 (mithril bootstrap) blocks, paired across codecs; encode CPU, ingestion throughput, batch commit latency, fsync latency, peak transient heap, process CPU and disk bytes |
| `read` | over a store written once per codec: uniform and local point reads, 100-block pages, a whole scan, at each `--threads` count and each `--cache` regime |
| `mixed` | 90/10, 50/50 and 10/90 point/page mixes at the highest `--threads` |
| `concurrent` | half the corpus written, then the other half appended in batches of 1 and 100 while readers run a 90/10 mix; both sides measured over the same span |
| `all` | all of the above |
| `smoke` | `all` at a size that finishes in seconds; `cargo test -p xtask` runs it on a synthetic corpus |

Codecs (`--codecs`): `raw` (the reference sink: bodies unframed), `zstd1`
and `zstd3` (one dictionary-free frame per block at that level),
`zstd1-dict` and `zstd3-dict` (the same with `--dictionary`, by default the
one bundled in `dolos-flatfiles`; `zstd3-dict` is the production codec's
parameters) and `store` (the production `FlatFileStore` itself, appending
and reading exactly as the node does: one frame per block with the bundled
dictionary, serial encoding, one `fdatasync` per touched segment per batch).
The store codec is measured end to end — its records carry the whole append
in `write_ms` and the whole read in `decode_us_per_block`, with no encode or
fsync split, and `--encode-threads` and `--no-fsync` do not reach it — and
it skips the `nocache` regime, since it opens its own descriptors.

Corpora: `--corpus DIR --segments 446..449` walks raw `NNNNNN.segment` files
(the pre-v4 segment layout, bodies concatenated as CBOR items) and decodes
each with pallas for its slot and era; `--immutable DIR` reads a Cardano
node immutable directory through pallas; `--synthetic N` is the seeded
stand-in the smoke test uses.

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
allocator in the `cargo-xtask` binary. A counter the host lacks is `null`
in the record and named under `environment.counters`.

### Store-level gates

`report` judges every non-raw codec against raw on the median over paired
repeats: at least 90% of raw ingestion throughput and at most 10% higher
p95 commit latency on every write workload, including the writer under
concurrent query load. These are the provisional codec gates; a failure is
a finding, not a reason to weaken fsync or add a raw mode.

Only like is compared with like. Records are grouped by run (harness
revision and dirty state, host), by corpus, by workload, and by every
setting of the measurement other than its codec and repeat (batch size,
encoder threads, fsync, reader threads, mix, seed, eviction method); a
candidate keeps its dictionary identity, so two dictionaries are two
candidates. A verdict needs exactly one raw and one candidate sample per
repeat in that group: a candidate without a raw partner, repeats that do
not match, or a repeat recorded twice (a rerun appended to the same file) is
reported `UNPAIRED` with the reason, its ratios shown and its verdict
withheld. An older record that never wrote a setting shows it as `?` and
never matches one that did.

Read workloads get the same ratios against raw in a table of their own,
without a verdict: against the raw sink a warm point read is a memcpy out
of the page cache, so the ratio says how many microseconds decoding adds,
not whether an API budget holds. The API budget is the node level's.

## Node level

```sh
cargo xtask bench storage node \
  --bin baseline=/path/to/dolos-9165dbd8:v3 \
  --bin candidate=target/release/dolos \
  --run 2026-09-09-m4 \
  --immutable /fast/disk/immutable-window \
  --genesis /path/to/mainnet/genesis \
  --work /fast/disk/archive-bench.noindex/work \
  --out node.jsonl --repeat 3 --threads 1,8 --ops 20000 \
  --cache warm,evict --evict-from /some/big/dir --evict-gib 16
cargo xtask bench report node.jsonl
```

Every `--bin` is `LABEL=PATH[:STORAGE_VERSION]` (the version the binary's
`dolos.toml` declares; `v4` unless given). Every repeat runs each binary in
turn on a fresh, disposable instance under `--work`, so the two sides see
the same host in the same state; `--run` names the paired run and every
record carries it beside the label, the binary's SHA-256 and `--version`.

Workloads (`--workloads`, default all four):

| workload | what runs | measured |
|---|---|---|
| `import` | `dolos data import-archive` over the immutable directory in batches of `--chunk-size` (500, the mithril bootstrap shape) | wall, blocks/s, raw MiB/s, ms per batch, the child's CPU and peak RSS from `wait4`, segment bytes, compression ratio, index bytes |
| `live` | the same command with one block per commit over the first `--live-blocks` blocks: the live-tip shape, one `fdatasync` and one index commit per block | the same |
| `read` | `dolos data dump-blocks` over the corpus (the node's own scan of every frame), then `dolos serve` with the UTxO RPC service: `FetchBlock` by slot (uniform and 80%-local), `DumpHistory` pages of 100, at each `--threads` count and each `--cache` regime | ops/s, blocks/s, point and page latency histograms, the server's CPU, disk bytes and peak footprint over the workload |
| `mixed` | 90/10, 50/50 and 10/90 point/page mixes at the highest `--threads` | the same |

The corpus is what `--immutable` yields through pallas, minus its last
chunk (which the reader treats as not yet immutable). Two things follow
from how the node reads that directory:

- **It has to begin with chunk `00000`**: pallas refuses a directory whose
  first block is not the genesis block, and `import-archive` starts from
  the archive's tip, which is empty. A window copied out of a full immutable
  directory therefore carries chunks `00000` and `00001` beside the range
  under test; the first Byron epoch is small (a few thousand tiny blocks)
  and the records name the eras so it can be discounted.
- **The read side needs an era summary.** An archive-only import writes
  neither the block-hash index nor the era summary the block mappers read,
  so before `serve` the harness seeds the state with `doctor rebuild-state
  --stop-epoch 1` — a replay of exactly that first Byron epoch, which is the
  other reason chunk `00001` has to be there (the replay stops at the block
  that opens epoch 1). Reads then resolve blocks by slot; slot 0 and the
  two-block Byron boundary slots are left out of the query set because the
  service cannot name them unambiguously. The scan covers the contiguous
  tail of the corpus (`dump-blocks` asserts consecutive block numbers).

Every response is checked for success; `--verify` also checks the returned
header against the corpus. A workload with errors or mismatches fails the
run rather than recording a number.

Not measured at the node level, and why: **append under concurrent API
load**. `import-archive` and `serve` each take the store's exclusive lock,
and a `daemon` that syncs while serving needs an upstream, which makes the
run neither offline nor reproducible. The store-level `concurrent` preset —
the production `FlatFileStore` under readers — is the evidence for that
shape. **Batch commit latency percentiles** are not observable from
outside the process; the node records carry the mean (`ms_per_batch`) and
the store-level `write` preset carries the p50/p95/p99 of the production
store's append.

### Node gates

`report` pairs every label against the one named `baseline` within one run
name, corpus, workload and set of settings, on the median over paired
repeats, and needs one sample per label per repeat: ingestion (`import-500`
and `import-1`) passes at **at least 90% of baseline throughput**; point
reads pass at **at most 10% higher p95 latency**; pages and scans are
reported with their ratios and no verdict. These are the acceptance
batch's production budgets.

### Production baseline

The last revision whose store appended raw bodies is dolos `9165dbd8` (the
merge of #1311, the parent of the direct-write cutover #1312). It reads a
`v3` configuration, so its `--bin` spec ends in `:v3`. Build it from a
detached worktree (`git worktree add --detach ../dolos-baseline 9165dbd8 &&
cargo build --release --bin dolos`) and pass the binary's path.

## Dictionary

```sh
cargo xtask bench storage train \
  --corpus /path/to/mainnet/segments \
  --sample 8,25,40,60,120,250,330,380=1000 --sample 440..447=1500 \
  --seed 0 --max-size 112640 --out cardano.dict
cargo xtask bench storage evaluate \
  --fixture mainnet-heldout=/path/to/mainnet/segments:448..455 \
  --fixture preprod=/path/to/preprod/segments:100,200,300 \
  --fixture preview=immutable:/path/to/preview/immutable \
  --dictionary bundled --dictionary cardano.dict --out evaluate.jsonl
```

`train` samples a seeded subset of each segment's blocks (`--segments`
with `--samples-per-segment`, or weighted `--sample SEGMENTS=COUNT` groups)
and runs zstd's trainer; `<out>.json` records every input file's SHA-256,
the sample per segment, the seed, the size cap and the zstd version, so the
same command over the same files yields the same bytes. `evaluate` encodes
and decodes every block of every fixture with the dictionary-free codec and
each dictionary given, checks the round trip, and reports ratio, encode
throughput and decode cost per block, per era.

Dictionary preparation is developer tooling and nothing else: the node has
no training, sealing or dictionary command. The bundled asset lives at
`crates/flatfiles/dictionary/cardano.dict` with its provenance beside it;
`dolos_flatfiles::BUNDLED_DICTIONARY` exposes it and
`crates/flatfiles/tests/bundled_dictionary.rs` pins its hash and the
128 KiB budget. Changing those bytes is a storage-format decision.

## Results

`results/` holds the measured runs, one directory per host and date, each
with the raw `*.jsonl` records, the `report` output over them, the runbook
that produced them and a `REPORT.md` with the corpus, host, verdicts and
findings spelled out. Older directories keep the commands as they were run
at the time; where the command has since moved, the runbook says so at the
top rather than rewriting history.

Before publishing results, replace workstation paths, usernames and
temporary session directories in records, provenance and commands with
consistent relative corpus/work labels. Preserve corpus hashes, revision
metadata and measured values; regenerate reports from the sanitized
records because run IDs include environment metadata. Local harness output
is not automatically sanitized. `xtask/tests/published_artifacts.rs` guards
the committed results and dictionary provenance against common leaks.

The research this tooling grew out of — the reads-only measurements the
cutover design was first argued from — lives in the txpipe knowledge base
(`solution/dolos/kb/archive-flatfile-compression/`); the committed results
here are the evidence for the implementation as shipped.
