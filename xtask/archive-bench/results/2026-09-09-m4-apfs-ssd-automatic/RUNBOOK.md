# Automatic selection: reproduction

Use the predecessor's unchanged immutable corpus on the declared Apple M4,
16 GiB, macOS 26.5.2, APFS SSD host. Wait for all other builds/tests/load tests
to exit. Never use a live store or overlap a load test with these commands.
Each child imports into a separate disposable store, recreated for each run.

## Production binaries

Raw production is `9165dbd862f55baa64d9c693a26519f6bfe3c00a` with v3 config;
serial production is `b21c8d554f8cdabcdfdb6b5f00421ba77edfa0dc` with v4.
The automatic implementation is `5d13f2dae7037697b42cb8e394f74cfe371f6dd7`.
All three binaries use Rust 1.93, release/default features, and
`CARGO_INCREMENTAL=0 cargo build --release --bin dolos`.

The raw and serial binaries and their measurement-only timing patches are
the unchanged pinned arms from
[the preceding run](../2026-09-09-m4-apfs-ssd-offline-import/identities.json).
The automatic arm exports its source commit and applies the timing patch
emitted by `instrument-import.py CHECKOUT`. `measured-automatic.patch` is the
exact resulting source delta from the merged serial base.
`identities.json` records executable and patch hashes and every measurement
command's actual exit status.

Identical monotonic timing surrounds each production `writer.commit()`.
The harness collects individual samples, process CPU and per-child peak RSS.
Only the release child is timed; the development harness scans inputs before
launch. No raw adapter or weaker durability is added to production.
If reusing a release target across source exports, touch source mtimes before
building so Cargo cannot mistake an exported source for cached dependencies.

## Production runs

Use the commands recorded in `identities.json`, replacing portable
`bin/`, `corpora/`, `genesis/` and `work/` labels with local resources.
The inherited corpus and modern supplementary input have the exact hashes in
[corpus-files.json](../2026-09-09-m4-apfs-ssd-offline-import/corpus-files.json).

Run three paired repetitions of each:

- Full inherited 126,728-block corpus, batch 500 (the throughput/p95 gate).
- Full inherited corpus, batch 5000 (archive transaction shape, not bootstrap).
- First 2,000 inherited Byron bodies, batch 1.
- Original Byron origin anchor plus 1,999 Conway bodies, batch 1.

All arms use the same corpus, batch size and production sync frequency.
The partial one-block runs include the legacy final empty commit, identically
in each arm. Report medians of per-run elapsed time, throughput, CPU and
actual commit percentiles; report maximum RSS/buffers. Retain every sample.

## Supporting experiments

Supporting harness binaries use the same development profile with debug
information disabled and incremental compilation off. They are not production
ingestion evidence. The previous harness comes from the prior PR tree
`8b98a92a`; its `store` codec is serial and `store-import` is always-windowed.
The new harness has only `store`, with automatic selection. Both executable
hashes are recorded. Previous-harness records explicitly distinguish its
source revision from the checkout where the executable was invoked.

- Crossover: first 1,024 bodies after skipping 43,177 Byron bodies; batches
  8, 32, 64, 128 and 500; three repeats of serial, previous-windowed and
  automatic production flatfile paths.
- Concurrent readers: 2,000 modern bodies, four readers, writer batches
  1 and 100, three repeats per serial/automatic harness. These are archive
  readers, not full API traffic. Writer/read durations and latencies share
  the same interval; a shorter writer run also changes the read sample count.
- A separate automatic-harness experiment repeats the concurrent shapes with
  `RAYON_NUM_THREADS=4`; it is not the default production policy.
- Limits: twenty seeded synthetic mixed-size bodies, including one
  incompressible 16 MiB body, batches 1, 500 and 5000, three repeats.

Store-level RSS is a process lifetime high-water mark, unlike the separate
production-child RSS. Use process CPU rather than caller-thread encode CPU
for parallel work. The legacy `import_buffers` key now reports automatic
encoding counters; serial scratch is included in its buffer peak.

## Reports and checks

```sh
cargo xtask archive-bench report node-500.jsonl node-5000.jsonl \
  node-byron-1.jsonl node-modern-1.jsonl > tables-node.md
cargo xtask archive-bench report crossover-previous.jsonl \
  crossover-automatic.jsonl concurrent-previous.jsonl \
  concurrent-automatic.jsonl concurrent-automatic-4.jsonl \
  store-limits.jsonl > tables-support.md
```

Sanitize workstation paths and never rewrite historical results.
`verification.json` records all four completed workspace checks. The current
source no longer modifies production snapshot restore, backfill or registry
transport paths; they match the merged baseline. Registry Docker suites
were not rerun for this revision. The previous daemon timeouts remain
documented in their original evidence, not converted into passes.
