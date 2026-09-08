## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| f01c8f96 | 7475c9c7 (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | segments 448,449,450,451 of /Users/santiago/dolos-instances/flatfile-lab/mainnet-raw: 84481 blocks, 423.5 MiB | 2026-09-08T15:45:41.746758+00:00 |

## Writes

| run | workload | codec | rep | blocks/s | raw MB/s | ratio | encode MB/s (cpu) | batch p50 ms | p95 ms | p99 ms | fsync p50 ms | peak heap MiB | cpu µs/block |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f01c8f96 | write-100 | raw | 0 | 10533 | 52.8 | 1.000 | 4434 | 9.09 | 12.88 | 17.51 | 4.56 | 1.8 | 43.0 |
| f01c8f96 | write-100 | zstd3 | 0 | 9100 | 45.6 | 0.737 | 101 | 10.36 | 15.26 | 19.81 | 4.50 | 1.5 | 93.5 |
| f01c8f96 | write-100 | zstd3-dict | 0 | 8954 | 44.9 | 0.588 | 72 | 10.67 | 16.04 | 20.37 | 4.43 | 1.2 | 108.2 |
| f01c8f96 | write-500 | raw | 0 | 34171 | 171.3 | 1.000 | 12810 | 13.99 | 20.33 | 29.95 | 5.82 | 5.4 | 13.6 |
| f01c8f96 | write-500 | zstd3 | 0 | 29412 | 147.5 | 0.737 | 337 | 15.93 | 24.02 | 46.53 | 5.51 | 4.0 | 27.3 |
| f01c8f96 | write-500 | zstd3-dict | 0 | 30269 | 151.8 | 0.588 | 210 | 16.06 | 22.12 | 27.00 | 5.45 | 3.3 | 34.6 |
| f01c8f96 | write-100 | raw | 1 | 10477 | 52.5 | 1.000 | 5856 | 9.04 | 13.00 | 18.87 | 4.49 | 1.8 | 43.0 |
| f01c8f96 | write-100 | zstd3 | 1 | 9343 | 46.8 | 0.737 | 110 | 10.27 | 15.09 | 18.55 | 4.47 | 1.5 | 86.9 |
| f01c8f96 | write-100 | zstd3-dict | 1 | 9003 | 45.1 | 0.588 | 70 | 10.88 | 15.10 | 17.97 | 4.42 | 1.2 | 109.8 |
| f01c8f96 | write-500 | raw | 1 | 25986 | 130.3 | 1.000 | 8391 | 16.03 | 34.14 | 75.24 | 7.13 | 5.4 | 14.9 |
| f01c8f96 | write-500 | zstd3 | 1 | 30401 | 152.4 | 0.737 | 292 | 14.70 | 24.67 | 58.33 | 5.55 | 4.0 | 29.5 |
| f01c8f96 | write-500 | zstd3-dict | 1 | 29925 | 150.0 | 0.588 | 199 | 16.15 | 21.12 | 40.30 | 5.35 | 3.3 | 35.8 |
| f01c8f96 | write-100 | raw | 2 | 10226 | 51.3 | 1.000 | 3811 | 9.38 | 12.98 | 18.96 | 4.54 | 1.8 | 46.1 |
| f01c8f96 | write-100 | zstd3 | 2 | 9738 | 48.8 | 0.737 | 130 | 9.97 | 14.61 | 18.92 | 4.45 | 1.5 | 77.0 |
| f01c8f96 | write-100 | zstd3-dict | 2 | 9536 | 47.8 | 0.588 | 80 | 10.02 | 15.00 | 19.05 | 4.42 | 1.2 | 96.6 |
| f01c8f96 | write-500 | raw | 2 | 31333 | 157.1 | 1.000 | 6786 | 14.88 | 28.13 | 41.03 | 5.80 | 5.4 | 14.6 |
| f01c8f96 | write-500 | zstd3 | 2 | 27591 | 138.3 | 0.737 | 327 | 16.15 | 30.65 | 60.26 | 5.62 | 4.0 | 27.7 |
| f01c8f96 | write-500 | zstd3-dict | 2 | 30012 | 150.5 | 0.588 | 203 | 15.95 | 22.02 | 40.24 | 5.47 | 3.3 | 35.6 |

## Gates (writes: candidate against raw, medians over paired repeats within one run)

| run | workload | settings | candidate | dictionary | throughput vs raw | batch p95 vs raw | verdict |
|---|---|---|---|---|---|---|---|
| f01c8f96 | write-100 | batch 100, encode_threads 4, fsync true | zstd3 |  | 0.892 | 1.162 | FAIL |
| f01c8f96 | write-100 | batch 100, encode_threads 4, fsync true | zstd3-dict | c47b2eb1 | 0.859 | 1.163 | FAIL |
| f01c8f96 | write-500 | batch 500, encode_threads 4, fsync true | zstd3 |  | 0.939 | 0.877 | PASS |
| f01c8f96 | write-500 | batch 500, encode_threads 4, fsync true | zstd3-dict | c47b2eb1 | 0.958 | 0.783 | PASS |

