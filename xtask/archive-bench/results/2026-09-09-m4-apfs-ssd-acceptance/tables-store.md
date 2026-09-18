## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 190e2213 | 1c945421 (dirty) | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | segments 448,449,450,451 of corpora/mainnet-segments: 84481 blocks, 423.5 MiB | 2026-09-09T03:08:09.562840+00:00 |
| 190e2213 | 1c945421 (dirty) | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | segments 448,449,450,451 of corpora/mainnet-segments: 20000 blocks, 101.0 MiB | 2026-09-09T03:10:07.994916+00:00 |

## Writes

| run | workload | codec | rep | blocks/s | raw MB/s | ratio | encode MB/s (cpu) | batch p50 ms | p95 ms | p99 ms | fsync p50 ms | peak heap MiB | cpu µs/block |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 190e2213 | write-100 | raw | 0 | 16939 | 84.9 | 1.000 | 31298 | 5.86 | 8.34 | 12.60 | 4.36 | 0.0 | 14.2 |
| 190e2213 | write-100 | zstd1-dict | 0 | 12443 | 62.4 | 0.673 | 198 | 7.91 | 10.52 | 15.09 | 4.26 | 0.2 | 36.7 |
| 190e2213 | write-100 | zstd3-dict | 0 | 11449 | 57.4 | 0.588 | 141 | 8.25 | 12.24 | 21.27 | 4.13 | 0.2 | 44.0 |
| 190e2213 | write-100 | store | 0 | 11619 | 58.2 | 0.588 | 112 | 8.22 | 11.92 | 17.29 | 0.00 | 0.2 | 44.7 |
| 190e2213 | write-500 | raw | 0 | 48421 | 242.8 | 1.000 | 34360 | 9.90 | 15.02 | 15.97 | 5.71 | 0.0 | 7.9 |
| 190e2213 | write-500 | zstd1-dict | 0 | 42115 | 211.1 | 0.673 | 464 | 11.85 | 15.17 | 20.14 | 4.37 | 0.2 | 14.7 |
| 190e2213 | write-500 | zstd3-dict | 0 | 27012 | 135.4 | 0.588 | 239 | 17.96 | 27.02 | 32.92 | 4.80 | 0.2 | 25.6 |
| 190e2213 | write-500 | store | 0 | 24755 | 124.1 | 0.588 | 193 | 18.50 | 32.34 | 84.02 | 0.00 | 0.2 | 26.0 |
| 190e2213 | write-100 | raw | 1 | 15854 | 79.5 | 1.000 | 25905 | 5.93 | 8.97 | 15.02 | 4.49 | 0.0 | 15.2 |
| 190e2213 | write-100 | zstd1-dict | 1 | 12565 | 63.0 | 0.673 | 201 | 7.84 | 10.31 | 15.91 | 4.18 | 0.2 | 36.0 |
| 190e2213 | write-100 | zstd3-dict | 1 | 10460 | 52.4 | 0.588 | 120 | 9.04 | 13.08 | 18.69 | 4.16 | 0.2 | 52.1 |
| 190e2213 | write-100 | store | 1 | 10262 | 51.4 | 0.588 | 95 | 9.33 | 13.63 | 19.82 | 0.00 | 0.2 | 52.8 |
| 190e2213 | write-500 | raw | 1 | 39105 | 196.1 | 1.000 | 36591 | 12.04 | 18.86 | 30.46 | 6.54 | 0.0 | 10.6 |
| 190e2213 | write-500 | zstd1-dict | 1 | 35462 | 177.8 | 0.673 | 494 | 13.53 | 19.25 | 30.02 | 5.76 | 0.2 | 15.2 |
| 190e2213 | write-500 | zstd3-dict | 1 | 25333 | 127.0 | 0.588 | 237 | 18.97 | 28.02 | 42.14 | 5.96 | 0.2 | 25.9 |
| 190e2213 | write-500 | store | 1 | 25198 | 126.3 | 0.588 | 191 | 19.37 | 28.48 | 37.58 | 0.00 | 0.2 | 26.3 |
| 190e2213 | write-100 | raw | 2 | 15248 | 76.4 | 1.000 | 24203 | 6.09 | 8.71 | 14.46 | 4.77 | 0.0 | 15.7 |
| 190e2213 | write-100 | zstd1-dict | 2 | 11651 | 58.4 | 0.673 | 198 | 8.13 | 11.64 | 19.81 | 4.64 | 0.2 | 36.3 |
| 190e2213 | write-100 | zstd3-dict | 2 | 10305 | 51.7 | 0.588 | 122 | 9.27 | 13.34 | 16.76 | 4.41 | 0.2 | 50.8 |
| 190e2213 | write-100 | store | 2 | 9834 | 49.3 | 0.588 | 98 | 9.81 | 14.07 | 21.04 | 0.00 | 0.2 | 51.3 |
| 190e2213 | write-500 | raw | 2 | 37424 | 187.6 | 1.000 | 32107 | 12.48 | 20.12 | 41.39 | 7.64 | 0.0 | 8.9 |
| 190e2213 | write-500 | zstd1-dict | 2 | 35932 | 180.1 | 0.673 | 458 | 13.42 | 18.37 | 22.79 | 5.64 | 0.2 | 15.8 |
| 190e2213 | write-500 | zstd3-dict | 2 | 25762 | 129.2 | 0.588 | 240 | 18.45 | 28.92 | 34.64 | 5.83 | 0.2 | 25.5 |
| 190e2213 | write-500 | store | 2 | 27144 | 136.1 | 0.588 | 206 | 17.68 | 26.97 | 32.33 | 0.00 | 0.2 | 24.4 |

## Append under query load

| run | workload | codec | rep | readers | writer blocks/s | batch p50 ms | p95 ms | p99 ms | reader ops/s | point p50 µs | p95 µs | page p95 ms | wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 190e2213 | append-query-1 | raw | 0 | 8 | 238 | 4.04 | 5.11 | 7.84 | 151046 | 5 | 7 | 0.63 | 42.0 |
| 190e2213 | append-query-1 | zstd1-dict | 0 | 8 | 237 | 4.05 | 5.23 | 7.96 | 153445 | 4 | 14 | 0.79 | 42.3 |
| 190e2213 | append-query-1 | zstd3-dict | 0 | 8 | 236 | 4.05 | 5.15 | 7.88 | 153207 | 4 | 13 | 0.80 | 42.4 |
| 190e2213 | append-query-1 | store | 0 | 8 | 237 | 4.06 | 5.10 | 7.91 | 12884 | 53 | 92 | 6.80 | 42.3 |
| 190e2213 | append-query-100 | raw | 0 | 8 | 16856 | 5.78 | 7.72 | 8.93 | 141808 | 5 | 8 | 0.93 | 0.6 |
| 190e2213 | append-query-100 | zstd1-dict | 0 | 8 | 14422 | 6.85 | 9.04 | 11.32 | 145090 | 4 | 15 | 0.83 | 0.7 |
| 190e2213 | append-query-100 | zstd3-dict | 0 | 8 | 10983 | 8.94 | 12.07 | 13.78 | 147905 | 4 | 13 | 0.82 | 0.9 |
| 190e2213 | append-query-100 | store | 0 | 8 | 10840 | 9.00 | 12.95 | 18.27 | 13341 | 51 | 87 | 6.50 | 0.9 |
| 190e2213 | append-query-1 | raw | 1 | 8 | 226 | 4.06 | 5.44 | 8.83 | 150931 | 5 | 7 | 0.60 | 44.2 |
| 190e2213 | append-query-1 | zstd1-dict | 1 | 8 | 213 | 4.10 | 6.49 | 11.11 | 154694 | 3 | 14 | 0.78 | 47.0 |
| 190e2213 | append-query-1 | zstd3-dict | 1 | 8 | 213 | 4.12 | 6.36 | 11.01 | 152144 | 4 | 13 | 0.81 | 47.1 |
| 190e2213 | append-query-1 | store | 1 | 8 | 211 | 4.19 | 6.23 | 9.52 | 13037 | 53 | 90 | 6.72 | 47.5 |
| 190e2213 | append-query-100 | raw | 1 | 8 | 15420 | 5.78 | 10.55 | 19.58 | 150546 | 5 | 7 | 0.73 | 0.6 |
| 190e2213 | append-query-100 | zstd1-dict | 1 | 8 | 13861 | 7.04 | 9.13 | 11.12 | 145711 | 4 | 15 | 0.84 | 0.7 |
| 190e2213 | append-query-100 | zstd3-dict | 1 | 8 | 10809 | 8.72 | 14.08 | 17.89 | 147215 | 4 | 14 | 0.81 | 0.9 |
| 190e2213 | append-query-100 | store | 1 | 8 | 10890 | 8.78 | 12.52 | 19.38 | 12996 | 51 | 91 | 6.95 | 0.9 |
| 190e2213 | append-query-1 | raw | 2 | 8 | 220 | 4.08 | 5.95 | 9.22 | 150754 | 5 | 7 | 0.62 | 45.5 |
| 190e2213 | append-query-1 | zstd1-dict | 2 | 8 | 211 | 4.11 | 6.16 | 9.76 | 153094 | 4 | 14 | 0.78 | 47.3 |
| 190e2213 | append-query-1 | zstd3-dict | 2 | 8 | 223 | 4.09 | 5.88 | 8.90 | 152373 | 4 | 13 | 0.79 | 44.9 |
| 190e2213 | append-query-1 | store | 2 | 8 | 219 | 4.10 | 5.72 | 9.42 | 12910 | 53 | 91 | 6.77 | 45.6 |
| 190e2213 | append-query-100 | raw | 2 | 8 | 17607 | 5.26 | 7.73 | 10.81 | 144554 | 5 | 8 | 0.68 | 0.6 |
| 190e2213 | append-query-100 | zstd1-dict | 2 | 8 | 14323 | 6.58 | 9.85 | 10.84 | 144558 | 4 | 15 | 0.84 | 0.7 |
| 190e2213 | append-query-100 | zstd3-dict | 2 | 8 | 10125 | 9.84 | 13.34 | 20.10 | 145902 | 4 | 13 | 0.88 | 1.0 |
| 190e2213 | append-query-100 | store | 2 | 8 | 10273 | 9.11 | 14.25 | 20.09 | 12819 | 51 | 92 | 7.38 | 1.0 |

## Gates (writes: candidate against raw, medians over paired repeats within one run)

| run | workload | settings | candidate | dictionary | throughput vs raw | batch p95 vs raw | verdict |
|---|---|---|---|---|---|---|---|
| 190e2213 | append-query-1 writer | batch 1, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | store | c47b2eb1 | 0.970 | 1.053 | PASS |
| 190e2213 | append-query-1 writer | batch 1, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd1-dict | c47b2eb1 | 0.940 | 1.134 | FAIL |
| 190e2213 | append-query-1 writer | batch 1, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3-dict | c47b2eb1 | 0.983 | 1.081 | PASS |
| 190e2213 | append-query-100 writer | batch 100, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | store | c47b2eb1 | 0.643 | 1.677 | FAIL |
| 190e2213 | append-query-100 writer | batch 100, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd1-dict | c47b2eb1 | 0.850 | 1.181 | FAIL |
| 190e2213 | append-query-100 writer | batch 100, encode_threads 1, fsync true, head_blocks 10000, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3-dict | c47b2eb1 | 0.641 | 1.726 | FAIL |
| 190e2213 | write-100 | batch 100, encode_threads 1, fsync true | store | c47b2eb1 | 0.647 | 1.565 | FAIL |
| 190e2213 | write-100 | batch 100, encode_threads 1, fsync true | zstd1-dict | c47b2eb1 | 0.785 | 1.208 | FAIL |
| 190e2213 | write-100 | batch 100, encode_threads 1, fsync true | zstd3-dict | c47b2eb1 | 0.660 | 1.502 | FAIL |
| 190e2213 | write-500 | batch 500, encode_threads 1, fsync true | store | c47b2eb1 | 0.644 | 1.510 | FAIL |
| 190e2213 | write-500 | batch 500, encode_threads 1, fsync true | zstd1-dict | c47b2eb1 | 0.919 | 0.974 | PASS |
| 190e2213 | write-500 | batch 500, encode_threads 1, fsync true | zstd3-dict | c47b2eb1 | 0.659 | 1.486 | FAIL |

