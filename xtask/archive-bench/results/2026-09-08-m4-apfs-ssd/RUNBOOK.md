# Runbook

The commands that produced the records in this directory, in the order
they ran. They are kept as they were typed: the harness was then the
standalone `dolos-archive-bench` crate, dissolved since into
`cargo xtask archive-bench`, so `B` below is spelled today as
`B="cargo xtask archive-bench"` with the same subcommands and options.
The measurements are not re-run here; the revisions in the records are
the ones that produced them. Paths below are portable placeholders, not workstation locations.
Point these variables at local copies before running. The corpus is named by
the segment files' SHA-256 in `cardano.dict.json` and by segment id in every
record. Path redaction changes neither the measured values nor corpus hashes.

```sh
B="cargo run --release -p dolos-archive-bench --"
MAINNET_HDD="corpora/mainnet-hdd"   # raw segments 0..454
MAINNET_SSD=corpora/mainnet-ssd                # raw segments 446..455, same files
PREPROD_HDD="corpora/preprod"  # raw segments 0..306
PREVIEW_IMM="corpora/preview-immutable"
WORK=work/archive-bench                          # internal SSD, APFS
EVICT=corpora/cache-eviction                              # 75 GB of other files on the SSD

# 1. dictionary candidates
$B train --corpus "$MAINNET_HDD" --segments 8,25,40,60,120,250,330,380,420,440,445,446 \
   --samples-per-segment 1500 --seed 0 --max-size 112640 --out stratified.dict
$B train --corpus "$MAINNET_HDD" --segments 440..447 \
   --samples-per-segment 2250 --seed 0 --max-size 112640 --out recent.dict
$B train --corpus "$MAINNET_HDD" --sample 8,25,40,60,120,250,330,380=1000 --sample 440..447=1500 \
   --seed 0 --max-size 112640 --out balanced.dict                       # the bundled one

# 2. evaluation on held-out data
$B evaluate --dictionary stratified.dict --dictionary recent.dict --dictionary balanced.dict \
   --limit-blocks 20000 \
   --fixture mainnet-heldout-conway="$MAINNET_SSD":448..455 \
   --fixture mainnet-byron="$MAINNET_HDD":9 \
   --fixture mainnet-shelley="$MAINNET_HDD":30 \
   --fixture mainnet-mary="$MAINNET_HDD":70 \
   --fixture mainnet-alonzo="$MAINNET_HDD":130 \
   --fixture mainnet-babbage="$MAINNET_HDD":260 \
   --fixture mainnet-conway-early="$MAINNET_HDD":320 \
   --fixture preprod="$PREPROD_HDD":50,150,300 \
   --fixture preview=immutable:"$PREVIEW_IMM":200000 \
   --out evaluate.jsonl

# 3. write gates: three paired repeats, foreground encoding, fsync per batch
$B bench --preset write --corpus "$MAINNET_SSD" --segments 448..451 \
   --work "$WORK" --out write.jsonl --repeat 3

# 4. reads and mixes, warm and evicted cache, 1 and 8 threads, three paired repeats
$B bench --preset read --corpus "$MAINNET_SSD" --segments 448..451 \
   --work "$WORK" --out read.jsonl --repeat 3 --cache warm,evict --evict-from "$EVICT" --evict-gib 16
$B bench --preset mixed --corpus "$MAINNET_SSD" --segments 448..451 \
   --work "$WORK" --out mixed.jsonl --repeat 3 --cache warm,evict --evict-from "$EVICT" --evict-gib 16

# 5. append under query load
$B bench --preset concurrent --corpus "$MAINNET_SSD" --segments 448..451 \
   --work "$WORK" --out concurrent.jsonl --repeat 3

# 6. tables
$B report evaluate.jsonl write.jsonl read.jsonl mixed.jsonl concurrent.jsonl > tables.md

# 7. the optional experiment, not the production path: the import and
#    bootstrap batches again with four encoder threads per batch
$B bench --preset write --corpus "$MAINNET_SSD" --segments 448..451 \
   --work "$WORK" --out write-encode-threads-4.jsonl --repeat 3 \
   --write-batches 100,500 --encode-threads 4
$B report write-encode-threads-4.jsonl > tables-encode-threads-4.md
```
