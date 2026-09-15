## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 52d46c0e | 08f02d9b | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 5000 blocks, 29.9 MiB | 2026-09-09T13:25:34.011349+00:00 |
| 1b474ee9 | 08f02d9b | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | synthetic seed 0: 20 blocks, 16.0 MiB | 2026-09-09T13:28:06.334016+00:00 |

## Writes

| run | workload | codec | rep | blocks/s | raw MB/s | ratio | encode MB/s (cpu) | batch p50 ms | p95 ms | p99 ms | fsync p50 ms | peak heap MiB | cpu µs/block |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 52d46c0e | write-1 | store | 0 | 205 | 1.2 | 0.564 | 19 | 4.26 | 7.79 | 12.53 | 0.00 | 0.1 | 325.0 |
| 52d46c0e | write-1 | store-import | 0 | 197 | 1.2 | 0.564 | 22 | 4.47 | 9.81 | 13.03 | 0.00 | 0.2 | 283.2 |
| 52d46c0e | write-500 | store | 0 | 6415 | 38.4 | 0.564 | 42 | 75.10 | 95.09 | 95.09 | 0.00 | 0.2 | 143.7 |
| 52d46c0e | write-500 | store-import | 0 | 20526 | 122.8 | 0.564 | 1351 | 22.97 | 30.23 | 30.23 | 0.00 | 2.4 | 224.0 |
| 52d46c0e | write-5000 | store | 0 | 6598 | 39.5 | 0.564 | 42 | 758.12 | 758.12 | 758.12 | 0.00 | 0.5 | 142.8 |
| 52d46c0e | write-5000 | store-import | 0 | 24965 | 149.3 | 0.564 | 1416 | 200.28 | 200.28 | 200.28 | 0.00 | 5.2 | 221.4 |
| 52d46c0e | write-1 | store | 1 | 177 | 1.1 | 0.564 | 22 | 4.75 | 11.76 | 14.96 | 0.00 | 0.1 | 278.6 |
| 52d46c0e | write-1 | store-import | 1 | 223 | 1.3 | 0.564 | 23 | 4.19 | 6.34 | 9.77 | 0.00 | 0.1 | 263.1 |
| 52d46c0e | write-500 | store | 1 | 5910 | 35.3 | 0.564 | 41 | 86.31 | 100.07 | 100.07 | 0.00 | 0.2 | 145.4 |
| 52d46c0e | write-500 | store-import | 1 | 13291 | 79.5 | 0.564 | 1285 | 32.18 | 53.87 | 53.87 | 0.00 | 2.4 | 238.1 |
| 52d46c0e | write-5000 | store | 1 | 6381 | 38.2 | 0.564 | 41 | 783.81 | 783.81 | 783.81 | 0.00 | 0.5 | 147.0 |
| 52d46c0e | write-5000 | store-import | 1 | 23200 | 138.8 | 0.564 | 1354 | 215.48 | 215.48 | 215.48 | 0.00 | 5.2 | 231.2 |
| 52d46c0e | write-1 | store | 2 | 216 | 1.3 | 0.564 | 22 | 4.32 | 6.11 | 11.02 | 0.00 | 0.1 | 274.0 |
| 52d46c0e | write-1 | store-import | 2 | 228 | 1.4 | 0.564 | 23 | 4.14 | 5.95 | 10.06 | 0.00 | 0.1 | 261.1 |
| 52d46c0e | write-500 | store | 2 | 6048 | 36.2 | 0.564 | 40 | 79.82 | 100.34 | 100.34 | 0.00 | 0.2 | 148.7 |
| 52d46c0e | write-500 | store-import | 2 | 20280 | 121.3 | 0.564 | 1180 | 23.12 | 32.00 | 32.00 | 0.00 | 2.4 | 224.7 |
| 52d46c0e | write-5000 | store | 2 | 6469 | 38.7 | 0.564 | 41 | 773.32 | 773.32 | 773.32 | 0.00 | 0.5 | 146.6 |
| 52d46c0e | write-5000 | store-import | 2 | 22619 | 135.3 | 0.564 | 1230 | 221.12 | 221.12 | 221.12 | 0.00 | 5.2 | 223.6 |
| 1b474ee9 | write-1 | store-import | 0 | 158 | 126.9 | 0.999 | 703 | 4.00 | 11.67 | 26.23 | 0.00 | 32.1 | 1222.5 |
| 1b474ee9 | write-500 | store-import | 0 | 639 | 512.2 | 0.999 | 854 | 31.29 | 31.29 | 31.29 | 0.00 | 32.1 | 1174.7 |
| 1b474ee9 | write-5000 | store-import | 0 | 271 | 217.5 | 0.999 | 791 | 73.73 | 73.73 | 73.73 | 0.00 | 32.2 | 1268.7 |
| 1b474ee9 | write-1 | store-import | 1 | 161 | 128.8 | 0.999 | 773 | 4.06 | 11.04 | 28.95 | 0.00 | 32.1 | 1042.0 |
| 1b474ee9 | write-500 | store-import | 1 | 649 | 519.8 | 0.999 | 843 | 30.83 | 30.83 | 30.83 | 0.00 | 32.1 | 1203.7 |
| 1b474ee9 | write-5000 | store-import | 1 | 590 | 472.9 | 0.999 | 862 | 33.91 | 33.91 | 33.91 | 0.00 | 32.2 | 1181.9 |
| 1b474ee9 | write-1 | store-import | 2 | 179 | 143.1 | 0.999 | 795 | 3.97 | 8.82 | 26.10 | 0.00 | 32.1 | 1013.2 |
| 1b474ee9 | write-500 | store-import | 2 | 621 | 497.6 | 0.999 | 858 | 32.21 | 32.21 | 32.21 | 0.00 | 32.1 | 1178.0 |
| 1b474ee9 | write-5000 | store-import | 2 | 428 | 343.2 | 0.999 | 852 | 46.73 | 46.73 | 46.73 | 0.00 | 32.2 | 1198.7 |

## Gates (writes: candidate against raw, medians over paired repeats within one run)

| run | workload | settings | candidate | dictionary | throughput vs raw | batch p95 vs raw | verdict |
|---|---|---|---|---|---|---|---|
| 1b474ee9 | write-1 | batch 1, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 1b474ee9 | write-500 | batch 500, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 1b474ee9 | write-5000 | batch 5000, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-1 | batch 1, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-1 | batch 1, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-500 | batch 500, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-500 | batch 500, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-5000 | batch 5000, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 52d46c0e | write-5000 | batch 5000, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |

