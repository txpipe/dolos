## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 2c1a1d87 | 8b98a92a | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 1024 blocks, 4.9 MiB | 2026-09-09T17:08:11.548350+00:00 |
| 3eb3efe8 | 5d13f2da | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 1024 blocks, 4.9 MiB | 2026-09-09T17:08:21.420275+00:00 |
| 2c1a1d87 | 8b98a92a | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 2000 blocks, 11.7 MiB | 2026-09-09T17:08:26.448815+00:00 |
| 3eb3efe8 | 5d13f2da | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 2000 blocks, 11.7 MiB | 2026-09-09T17:08:41.895467+00:00 |
| 3eb3efe8 | 5d13f2da | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | synthetic seed 0: 20 blocks, 16.0 MiB | 2026-09-09T17:08:56.253937+00:00 |

## Writes

| run | workload | codec | rep | blocks/s | raw MB/s | ratio | encode MB/s (cpu) | batch p50 ms | p95 ms | p99 ms | fsync p50 ms | peak heap MiB | cpu µs/block |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2c1a1d87 | write-8 | store | 0 | 1110 | 5.3 | 0.596 | 14 | 6.97 | 11.20 | 13.73 | 0.00 | 0.1 | 351.4 |
| 2c1a1d87 | write-8 | store-import | 0 | 1240 | 5.9 | 0.596 | 64 | 6.22 | 8.36 | 10.76 | 0.00 | 0.3 | 810.0 |
| 2c1a1d87 | write-32 | store | 0 | 3410 | 16.2 | 0.596 | 28 | 8.34 | 14.98 | 17.56 | 0.00 | 0.1 | 170.4 |
| 2c1a1d87 | write-32 | store-import | 0 | 4929 | 23.5 | 0.596 | 238 | 6.03 | 9.35 | 10.16 | 0.00 | 0.3 | 296.5 |
| 2c1a1d87 | write-64 | store | 0 | 4662 | 22.2 | 0.596 | 33 | 13.06 | 18.12 | 18.12 | 0.00 | 0.1 | 145.1 |
| 2c1a1d87 | write-64 | store-import | 0 | 8289 | 39.5 | 0.596 | 356 | 7.37 | 11.41 | 11.41 | 0.00 | 0.5 | 212.3 |
| 2c1a1d87 | write-128 | store | 0 | 5956 | 28.4 | 0.596 | 37 | 19.94 | 29.39 | 29.39 | 0.00 | 0.1 | 127.5 |
| 2c1a1d87 | write-128 | store-import | 0 | 12755 | 60.7 | 0.596 | 379 | 9.38 | 14.16 | 14.16 | 0.00 | 0.7 | 204.1 |
| 2c1a1d87 | write-500 | store | 0 | 6834 | 32.5 | 0.596 | 38 | 70.19 | 72.94 | 72.94 | 0.00 | 0.1 | 125.8 |
| 2c1a1d87 | write-500 | store-import | 0 | 18579 | 88.5 | 0.596 | 421 | 24.59 | 25.12 | 25.12 | 0.00 | 1.7 | 195.0 |
| 2c1a1d87 | write-8 | store | 1 | 1266 | 6.0 | 0.596 | 19 | 5.48 | 10.02 | 13.23 | 0.00 | 0.1 | 254.8 |
| 2c1a1d87 | write-8 | store-import | 1 | 1239 | 5.9 | 0.596 | 65 | 6.05 | 9.54 | 13.25 | 0.00 | 0.2 | 841.9 |
| 2c1a1d87 | write-32 | store | 1 | 3034 | 14.4 | 0.596 | 26 | 10.41 | 17.04 | 19.23 | 0.00 | 0.1 | 180.8 |
| 2c1a1d87 | write-32 | store-import | 1 | 4020 | 19.1 | 0.596 | 148 | 7.53 | 12.05 | 12.08 | 0.00 | 0.3 | 431.9 |
| 2c1a1d87 | write-64 | store | 1 | 4377 | 20.8 | 0.596 | 32 | 13.33 | 23.51 | 23.51 | 0.00 | 0.1 | 150.6 |
| 2c1a1d87 | write-64 | store-import | 1 | 8675 | 41.3 | 0.596 | 359 | 6.73 | 14.86 | 14.86 | 0.00 | 0.5 | 213.8 |
| 2c1a1d87 | write-128 | store | 1 | 6044 | 28.8 | 0.596 | 37 | 19.19 | 28.82 | 28.82 | 0.00 | 0.1 | 127.3 |
| 2c1a1d87 | write-128 | store-import | 1 | 12674 | 60.4 | 0.596 | 383 | 9.96 | 15.02 | 15.02 | 0.00 | 0.7 | 198.2 |
| 2c1a1d87 | write-500 | store | 1 | 6972 | 33.2 | 0.596 | 38 | 69.80 | 71.11 | 71.11 | 0.00 | 0.1 | 126.4 |
| 2c1a1d87 | write-500 | store-import | 1 | 17716 | 84.4 | 0.596 | 450 | 25.02 | 25.92 | 25.92 | 0.00 | 1.7 | 190.7 |
| 2c1a1d87 | write-8 | store | 2 | 1090 | 5.2 | 0.596 | 12 | 7.07 | 12.04 | 13.30 | 0.00 | 0.1 | 392.5 |
| 2c1a1d87 | write-8 | store-import | 2 | 1168 | 5.6 | 0.596 | 62 | 6.17 | 11.34 | 14.79 | 0.00 | 0.2 | 811.7 |
| 2c1a1d87 | write-32 | store | 2 | 3068 | 14.6 | 0.596 | 25 | 10.13 | 16.05 | 18.55 | 0.00 | 0.1 | 192.1 |
| 2c1a1d87 | write-32 | store-import | 2 | 4656 | 22.2 | 0.596 | 198 | 6.89 | 8.17 | 10.07 | 0.00 | 0.3 | 342.0 |
| 2c1a1d87 | write-64 | store | 2 | 4463 | 21.3 | 0.596 | 32 | 12.15 | 22.99 | 22.99 | 0.00 | 0.1 | 146.9 |
| 2c1a1d87 | write-64 | store-import | 2 | 7455 | 35.5 | 0.596 | 368 | 7.15 | 27.93 | 27.93 | 0.00 | 0.5 | 209.2 |
| 2c1a1d87 | write-128 | store | 2 | 5891 | 28.1 | 0.596 | 38 | 20.14 | 31.06 | 31.06 | 0.00 | 0.1 | 124.7 |
| 2c1a1d87 | write-128 | store-import | 2 | 11530 | 54.9 | 0.596 | 428 | 10.01 | 20.09 | 20.09 | 0.00 | 0.7 | 203.5 |
| 2c1a1d87 | write-500 | store | 2 | 7009 | 33.4 | 0.596 | 39 | 69.27 | 69.80 | 69.80 | 0.00 | 0.1 | 123.5 |
| 2c1a1d87 | write-500 | store-import | 2 | 18353 | 87.4 | 0.596 | 440 | 24.92 | 25.80 | 25.80 | 0.00 | 1.7 | 193.7 |
| 3eb3efe8 | write-8 | store | 0 | 1137 | 5.4 | 0.596 | 13 | 6.74 | 10.87 | 12.85 | 0.00 | 0.1 | 363.8 |
| 3eb3efe8 | write-32 | store | 0 | 3314 | 15.8 | 0.596 | 27 | 9.38 | 13.14 | 16.16 | 0.00 | 0.4 | 190.8 |
| 3eb3efe8 | write-64 | store | 0 | 7616 | 36.3 | 0.596 | 163 | 7.72 | 13.95 | 13.95 | 0.00 | 0.5 | 235.1 |
| 3eb3efe8 | write-128 | store | 0 | 11265 | 53.6 | 0.596 | 343 | 10.58 | 15.93 | 15.93 | 0.00 | 0.7 | 229.9 |
| 3eb3efe8 | write-500 | store | 0 | 17893 | 85.2 | 0.596 | 383 | 24.17 | 26.92 | 26.92 | 0.00 | 1.7 | 182.7 |
| 3eb3efe8 | write-8 | store | 1 | 1185 | 5.6 | 0.596 | 16 | 6.14 | 10.63 | 13.71 | 0.00 | 0.1 | 292.1 |
| 3eb3efe8 | write-32 | store | 1 | 3076 | 14.6 | 0.596 | 27 | 9.84 | 16.11 | 16.38 | 0.00 | 0.4 | 189.4 |
| 3eb3efe8 | write-64 | store | 1 | 7091 | 33.8 | 0.596 | 156 | 8.15 | 12.42 | 12.42 | 0.00 | 0.5 | 238.7 |
| 3eb3efe8 | write-128 | store | 1 | 11743 | 55.9 | 0.596 | 363 | 10.09 | 16.14 | 16.14 | 0.00 | 0.7 | 225.5 |
| 3eb3efe8 | write-500 | store | 1 | 18571 | 88.4 | 0.596 | 395 | 23.38 | 24.82 | 24.82 | 0.00 | 1.7 | 192.0 |
| 3eb3efe8 | write-8 | store | 2 | 1055 | 5.0 | 0.596 | 12 | 7.52 | 11.79 | 12.09 | 0.00 | 0.1 | 402.3 |
| 3eb3efe8 | write-32 | store | 2 | 3038 | 14.5 | 0.596 | 25 | 10.02 | 16.74 | 17.42 | 0.00 | 0.4 | 200.0 |
| 3eb3efe8 | write-64 | store | 2 | 7514 | 35.8 | 0.596 | 156 | 8.15 | 13.05 | 13.05 | 0.00 | 0.5 | 245.9 |
| 3eb3efe8 | write-128 | store | 2 | 11634 | 55.4 | 0.596 | 324 | 10.54 | 13.93 | 13.93 | 0.00 | 0.7 | 229.8 |
| 3eb3efe8 | write-500 | store | 2 | 18767 | 89.4 | 0.596 | 348 | 23.69 | 24.79 | 24.79 | 0.00 | 1.7 | 193.2 |
| 3eb3efe8 | write-1 | store | 0 | 62 | 49.7 | 0.999 | 405 | 5.01 | 12.54 | 221.38 | 0.00 | 16.1 | 1983.9 |
| 3eb3efe8 | write-500 | store | 0 | 556 | 445.4 | 0.999 | 928 | 35.98 | 35.98 | 35.98 | 0.00 | 16.1 | 867.4 |
| 3eb3efe8 | write-5000 | store | 0 | 539 | 432.2 | 0.999 | 982 | 37.09 | 37.09 | 37.09 | 0.00 | 16.2 | 817.3 |
| 3eb3efe8 | write-1 | store | 1 | 175 | 140.0 | 0.999 | 852 | 4.30 | 6.90 | 30.49 | 0.00 | 16.1 | 945.6 |
| 3eb3efe8 | write-500 | store | 1 | 603 | 483.0 | 0.999 | 915 | 33.19 | 33.19 | 33.19 | 0.00 | 16.1 | 876.8 |
| 3eb3efe8 | write-5000 | store | 1 | 592 | 474.7 | 0.999 | 966 | 33.78 | 33.78 | 33.78 | 0.00 | 16.2 | 830.8 |
| 3eb3efe8 | write-1 | store | 2 | 191 | 152.9 | 0.999 | 861 | 4.02 | 5.21 | 28.33 | 0.00 | 16.1 | 935.9 |
| 3eb3efe8 | write-500 | store | 2 | 627 | 502.3 | 0.999 | 983 | 31.92 | 31.92 | 31.92 | 0.00 | 16.1 | 816.0 |
| 3eb3efe8 | write-5000 | store | 2 | 632 | 506.3 | 0.999 | 987 | 31.67 | 31.67 | 31.67 | 0.00 | 16.2 | 813.3 |

## Append under query load

| run | workload | codec | rep | readers | writer blocks/s | batch p50 ms | p95 ms | p99 ms | reader ops/s | point p50 µs | p95 µs | page p95 ms | wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2c1a1d87 | append-query-1 | store | 0 | 4 | 234 | 4.06 | 5.98 | 6.73 | 12242 | 21 | 77 | 3.76 | 4.3 |
| 2c1a1d87 | append-query-100 | store | 0 | 4 | 3195 | 27.72 | 43.71 | 43.71 | 9125 | 26 | 99 | 5.03 | 0.3 |
| 2c1a1d87 | append-query-1 | store | 1 | 4 | 233 | 4.17 | 5.98 | 7.02 | 11865 | 22 | 80 | 3.98 | 4.3 |
| 2c1a1d87 | append-query-100 | store | 1 | 4 | 3119 | 28.92 | 45.35 | 45.35 | 9118 | 27 | 104 | 5.06 | 0.3 |
| 2c1a1d87 | append-query-1 | store | 2 | 4 | 228 | 4.11 | 6.04 | 6.95 | 12008 | 22 | 78 | 3.84 | 4.4 |
| 2c1a1d87 | append-query-100 | store | 2 | 4 | 3138 | 30.10 | 46.33 | 46.33 | 9662 | 25 | 95 | 5.12 | 0.3 |
| 3eb3efe8 | append-query-1 | store | 0 | 4 | 236 | 4.02 | 6.00 | 6.98 | 11877 | 22 | 79 | 3.85 | 4.2 |
| 3eb3efe8 | append-query-100 | store | 0 | 4 | 6468 | 14.07 | 22.20 | 22.20 | 9212 | 27 | 120 | 6.34 | 0.2 |
| 3eb3efe8 | append-query-1 | store | 1 | 4 | 228 | 4.04 | 6.09 | 10.94 | 12253 | 21 | 77 | 3.74 | 4.4 |
| 3eb3efe8 | append-query-100 | store | 1 | 4 | 6344 | 15.96 | 20.76 | 20.76 | 9244 | 26 | 109 | 6.48 | 0.2 |
| 3eb3efe8 | append-query-1 | store | 2 | 4 | 234 | 4.13 | 5.89 | 6.65 | 11740 | 22 | 79 | 3.94 | 4.3 |
| 3eb3efe8 | append-query-100 | store | 2 | 4 | 6942 | 13.46 | 17.79 | 17.79 | 9213 | 25 | 111 | 7.24 | 0.1 |
| 3eb3efe8 | append-query-1 | store | 0 | 4 | 230 | 4.21 | 5.62 | 7.29 | 11342 | 23 | 79 | 4.07 | 4.4 |
| 3eb3efe8 | append-query-100 | store | 0 | 4 | 5453 | 18.02 | 23.07 | 23.07 | 8695 | 28 | 121 | 7.70 | 0.2 |
| 3eb3efe8 | append-query-1 | store | 1 | 4 | 233 | 4.09 | 5.56 | 6.93 | 11452 | 23 | 81 | 4.11 | 4.3 |
| 3eb3efe8 | append-query-100 | store | 1 | 4 | 5497 | 16.07 | 25.12 | 25.12 | 8289 | 26 | 121 | 7.40 | 0.2 |
| 3eb3efe8 | append-query-1 | store | 2 | 4 | 229 | 4.13 | 5.92 | 7.11 | 11538 | 22 | 80 | 4.06 | 4.4 |
| 3eb3efe8 | append-query-100 | store | 2 | 4 | 5783 | 15.93 | 23.13 | 23.13 | 8491 | 26 | 105 | 7.04 | 0.2 |

## Gates (writes: candidate against raw, medians over paired repeats within one run)

| run | workload | settings | candidate | dictionary | throughput vs raw | batch p95 vs raw | verdict |
|---|---|---|---|---|---|---|---|
| 3eb3efe8 | append-query-1 writer | batch 1, encode_threads 1, fsync true, head_blocks 1000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 4, seed 0 | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | append-query-100 writer | batch 100, encode_threads 1, fsync true, head_blocks 1000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 4, seed 0 | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-1 | batch 1, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-128 | batch 128, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-32 | batch 32, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-500 | batch 500, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-500 | batch 500, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-5000 | batch 5000, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-64 | batch 64, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 3eb3efe8 | write-8 | batch 8, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | append-query-1 writer | batch 1, encode_threads 1, fsync true, head_blocks 1000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 4, seed 0 | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | append-query-100 writer | batch 100, encode_threads 1, fsync true, head_blocks 1000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 4, seed 0 | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-128 | batch 128, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-128 | batch 128, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-32 | batch 32, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-32 | batch 32, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-500 | batch 500, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-500 | batch 500, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-64 | batch 64, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-64 | batch 64, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-8 | batch 8, encode_threads 1, fsync true | store | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |
| 2c1a1d87 | write-8 | batch 8, encode_threads 1, fsync true | store-import | c47b2eb1 | 0.000 | 0.000 | UNPAIRED: no raw record in this run |

