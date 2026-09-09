# Bundled dictionary

`cardano.dict` is the zstd dictionary every build of `dolos-flatfiles`
carries (`dolos_flatfiles::BUNDLED_DICTIONARY`), so a fresh instance compresses
its first archive block with no corpus, download or setup. It is an asset of
the segment format, not an operator setting: changing these bytes is a
format decision, and `tests/bundled_dictionary.rs` pins the hash and the
128 KiB budget.

| | |
|---|---|
| bytes | 112,640 (zstd's default `--maxdict`) |
| SHA-256 | `c47b2eb1f69a997bf01a720f26bcc1b668e0fddb95431e91c90cebf4f9b87139` |
| zstd dictionary id | 1075630411 |
| trained with | `ZDICT_trainFromBuffer`, libzstd 1.5.7, seed 0 |
| trained on | 20,000 mainnet blocks (230 MiB) sampled from 16 segments across every era: 1,000 each from segments 8, 25, 40, 60, 120, 250, 330 and 380 (Byron through early Conway) and 1,500 each from 440–447 (Conway, the eight segments before the held-out window) |

`cardano.dict.json` is the full provenance: the SHA-256 and size of every
source segment file, the block population and sample of each, the seed,
the size cap, the zstd version and the command line. Re-running that
command over the same files yields the same bytes.

## How it was chosen

Three candidates were trained from the same corpus with the same seed and
evaluated with the harness's `evaluate` command (then
`dolos-archive-bench evaluate`, now `cargo xtask perf storage evaluate`)
on data disjoint from every
training segment: mainnet segments 448–455 (156,621 Conway blocks, 877 MiB,
the held-out window), one 20,000-block fixture per mainnet era, preprod
segments 50, 150 and 300, and 20,000 preview blocks from a node immutable
directory. Ratios are compressed over raw bytes at zstd level 3, one frame
per block; the dictionary-free column is the same codec without a
dictionary. Records and the other candidates' provenance are in
`xtask/archive-bench/results/2026-09-08-m4-apfs-ssd/`.

| fixture | no dictionary | stratified (16 segments, 1,500 each) | recent (440–447 only) | **balanced (bundled)** |
|---|---|---|---|---|
| mainnet held-out Conway 448–455 | 0.730 | 0.623 | 0.563 | **0.590** |
| mainnet Byron (segment 9) | 0.872 | 0.728 | 0.872 | **0.732** |
| mainnet Shelley (30) | 0.938 | 0.915 | 0.932 | **0.911** |
| mainnet Mary (70) | 0.848 | 0.822 | 0.840 | **0.826** |
| mainnet Alonzo (130) | 0.503 | 0.342 | 0.476 | **0.351** |
| mainnet Babbage (260) | 0.473 | 0.320 | 0.353 | **0.322** |
| mainnet early Conway (320) | 0.666 | 0.483 | 0.508 | **0.487** |
| preprod (50, 150, 300) | 0.682 | 0.658 | 0.655 | **0.657** |
| preview (immutable, 20,000 blocks) | 0.477 | 0.468 | 0.472 | **0.470** |

The recent-only dictionary is best on the newest Conway blocks and useless
on everything older; the stratified one is the reverse. The balanced sample
keeps within 3 points of the best candidate on every mainnet fixture, which
is what one dictionary for every era and network needs: the bulk of a
mainnet archive is Alonzo and Babbage, and every future append is Conway.

On held-out mainnet the bundled dictionary beats dictionary-free zstd-3 by
14 points (0.590 against 0.730). The testnets gain little (2–3 points):
their blocks are mostly empty and compress on their own. The knowledge
base's earlier ratios came from a Conway-only dictionary on a different
sample and do not transfer to this one.
