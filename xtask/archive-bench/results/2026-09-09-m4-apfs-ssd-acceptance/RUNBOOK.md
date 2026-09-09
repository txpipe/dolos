# Runbook

The commands that produced the records in this directory, in the order
they ran. Paths are portable placeholders, not workstation locations; point
the variables at local copies before running. Every record names the
binaries by SHA-256 and the corpus by its slot range and era counts.

```sh
X="cargo xtask archive-bench"
MAINNET_IMMUTABLE=corpora/mainnet-immutable          # a Mithril mainnet snapshot's immutable/ directory
WINDOW=corpora/mainnet-immutable-window              # internal SSD, APFS
GENESIS=genesis/mainnet                              # byron.json shelley.json alonzo.json conway.json
MAINNET_SEGMENTS=corpora/mainnet-segments            # pre-v4 raw segments 446..455 (the write-benchmarks corpus)
WORK=work/archive-bench.noindex                      # internal SSD, APFS
EVICT=corpora/cache-eviction                         # tens of GB of other files on the same SSD

# 0. binaries: the pre-cutover baseline at 9165dbd8 (reads a v3 config), the
#    candidate at this pull request's head, and the candidate with
#    COMPRESSION_LEVEL set to 1 in crates/flatfiles/src/codec.rs — the
#    measured per-block optimization candidate, built from the same tree
#    with that one constant changed and not otherwise committed
git worktree add --detach ../dolos-baseline 9165dbd8
(cd ../dolos-baseline && cargo build --release --bin dolos) && cp ../dolos-baseline/target/release/dolos bin/dolos-baseline
cargo build --release --bin dolos && cp target/release/dolos bin/dolos-candidate
sed -i '' 's/COMPRESSION_LEVEL: i32 = 3/COMPRESSION_LEVEL: i32 = 1/' crates/flatfiles/src/codec.rs
cargo build --release --bin dolos && cp target/release/dolos bin/dolos-candidate-zstd1
git checkout crates/flatfiles/src/codec.rs

# 1. the corpus: chunks 00000 and 00001 (the first Byron epoch, for the
#    origin check and the state seed) plus the snapshot's last 80 chunks,
#    08881..08960 — mainnet slots up to 193,535,999 (segments 444..448,
#    Conway); the reader drops the last chunk as not yet immutable
mkdir -p "$WINDOW"
for n in 00000 00001 $(seq 8881 8960); do
  cp "$MAINNET_IMMUTABLE"/0$n.chunk "$MAINNET_IMMUTABLE"/0$n.primary "$MAINNET_IMMUTABLE"/0$n.secondary "$WINDOW"/ 2>/dev/null || \
  cp "$MAINNET_IMMUTABLE"/$n.chunk "$MAINNET_IMMUTABLE"/$n.primary "$MAINNET_IMMUTABLE"/$n.secondary "$WINDOW"/
done

# 2. the production comparison: three paired repeats, every binary in turn
#    per repeat; imports in batches of 500 and of 1 (2,000 blocks), scans,
#    point and page reads at 1 and 8 threads, mixes at 8, warm and evicted
$X node --bin baseline=bin/dolos-baseline:v3 --bin candidate=bin/dolos-candidate:v4 \
   --bin candidate-zstd1=bin/dolos-candidate-zstd1:v4 \
   --run 2026-09-09-m4-node --immutable "$WINDOW" --genesis "$GENESIS" --work "$WORK/work" \
   --out node.jsonl --workloads import,live,read,mixed --live-blocks 2000 --repeat 3 \
   --threads 1,8 --ops 20000 --cache warm,evict --evict-from "$EVICT" --evict-gib 16

# 3. the store level on the write-benchmarks corpus: the production store
#    beside the raw sink and the level-1 and level-3 dictionary codecs,
#    three paired repeats, fsync per batch. The import and bootstrap batch
#    sizes only (the live shape is fsync-bound and measured at parity at
#    the node level above, and at 84,000 fsyncs per codec it is an hour a
#    repeat); the concurrent preset on the first 5,000 blocks of each
#    segment for the same reason (its writer runs at batch 1 as well)
$X bench --preset write --corpus "$MAINNET_SEGMENTS" --segments 448..451 --write-batches 100,500 \
   --codecs raw,zstd1-dict,zstd3-dict,store --work "$WORK" --out write.jsonl --repeat 3
$X bench --preset concurrent --corpus "$MAINNET_SEGMENTS" --segments 448..451 --limit-blocks 5000 \
   --codecs raw,zstd1-dict,zstd3-dict,store --work "$WORK" --out concurrent.jsonl --repeat 3

# 4. tables
$X report node.jsonl > tables-node.md
$X report write.jsonl concurrent.jsonl > tables-store.md
```
