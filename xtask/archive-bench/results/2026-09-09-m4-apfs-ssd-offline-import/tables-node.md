## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 94356c17 | 08f02d9b | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 126728 blocks, 643.1 MiB | 2026-09-09T13:20:52.154466+00:00 |
| 94356c17 | 08f02d9b | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-modern-window: 2111 blocks, 13.1 MiB | 2026-09-09T13:24:09.345514+00:00 |

## Node imports

| run | workload | label | revision | storage | rep | blocks | blocks/s | raw MB/s | ms/batch | cpu µs/block | max RSS MiB | segments MiB | ratio | index MiB | commit p50 ms | commit p95 ms | commit p99 ms | encoded buffers MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-offline-final-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 126728 | 32375 | 164.3 | 15.41 | 29.1 | 79 | 643.1 | 1.000 | 64.0 | 9.30 | 27.79 | 52.56 | - |
| 2026-09-09-offline-final-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 21861 | 110.9 | 22.82 | 48.3 | 82 | 341.6 | 0.531 | 64.0 | 18.43 | 47.35 | 90.31 | - |
| 2026-09-09-offline-final-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 33252 | 168.7 | 15.00 | 57.4 | 100 | 341.6 | 0.531 | 64.0 | 9.87 | 21.30 | 47.15 | 4.59 |
| 2026-09-09-offline-final-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 126728 | 34225 | 173.7 | 14.58 | 29.1 | 79 | 643.1 | 1.000 | 64.0 | 8.18 | 26.13 | 58.03 | - |
| 2026-09-09-offline-final-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 22538 | 114.4 | 22.14 | 46.6 | 83 | 341.6 | 0.531 | 64.0 | 17.29 | 40.34 | 109.58 | - |
| 2026-09-09-offline-final-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 34759 | 176.4 | 14.35 | 55.9 | 98 | 341.6 | 0.531 | 64.0 | 9.61 | 24.26 | 33.00 | 4.59 |
| 2026-09-09-offline-final-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 126728 | 36370 | 184.6 | 13.72 | 28.5 | 78 | 643.1 | 1.000 | 64.0 | 7.64 | 23.15 | 61.44 | - |
| 2026-09-09-offline-final-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 22195 | 112.6 | 22.48 | 46.4 | 82 | 341.6 | 0.531 | 64.0 | 17.06 | 44.53 | 79.43 | - |
| 2026-09-09-offline-final-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 37814 | 191.9 | 13.19 | 57.5 | 99 | 341.6 | 0.531 | 64.0 | 9.15 | 17.12 | 34.70 | 4.59 |
| 2026-09-09-offline-final-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 126728 | 31209 | 158.4 | 156.18 | 26.0 | 188 | 643.1 | 1.000 | 64.0 | 67.83 | 217.58 | 639.11 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 26887 | 136.4 | 181.28 | 44.4 | 192 | 341.6 | 0.531 | 64.0 | 146.80 | 304.61 | 580.91 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 46772 | 237.4 | 104.21 | 52.7 | 222 | 341.6 | 0.531 | 64.0 | 60.13 | 239.47 | 277.87 | 5.57 |
| 2026-09-09-offline-final-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 126728 | 20246 | 102.7 | 240.75 | 25.5 | 188 | 643.1 | 1.000 | 64.0 | 72.42 | 725.61 | 1571.82 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 27492 | 139.5 | 177.29 | 44.9 | 193 | 341.6 | 0.531 | 64.0 | 154.14 | 353.37 | 508.30 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 51927 | 263.5 | 93.86 | 53.0 | 218 | 341.6 | 0.531 | 64.0 | 51.09 | 219.94 | 274.46 | 5.57 |
| 2026-09-09-offline-final-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 126728 | 41590 | 211.1 | 117.20 | 25.7 | 189 | 643.1 | 1.000 | 64.0 | 40.60 | 384.57 | 631.24 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 28557 | 144.9 | 170.68 | 45.1 | 193 | 341.6 | 0.531 | 64.0 | 154.27 | 279.97 | 518.00 | - |
| 2026-09-09-offline-final-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 47537 | 241.2 | 102.53 | 52.8 | 217 | 341.6 | 0.531 | 64.0 | 55.15 | 240.78 | 257.03 | 5.57 |
| 2026-09-09-offline-final-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 2000 | 227 | 0.2 | 4.41 | 79.9 | 16 | 1.9 | 1.000 | 64.0 | 4.00 | 5.92 | 10.83 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 235 | 0.2 | 4.25 | 89.6 | 18 | 0.8 | 0.429 | 64.0 | 4.01 | 5.23 | 6.11 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 205 | 0.2 | 4.87 | 90.8 | 18 | 0.8 | 0.429 | 64.0 | 4.03 | 6.22 | 29.05 | 0.64 |
| 2026-09-09-offline-final-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 2000 | 233 | 0.2 | 4.30 | 81.9 | 16 | 1.9 | 1.000 | 64.0 | 4.03 | 5.79 | 6.84 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 223 | 0.2 | 4.49 | 91.6 | 18 | 0.8 | 0.429 | 64.0 | 4.04 | 5.97 | 9.76 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 190 | 0.2 | 5.25 | 92.6 | 18 | 0.8 | 0.429 | 64.0 | 4.03 | 7.27 | 30.88 | 0.64 |
| 2026-09-09-offline-final-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 2000 | 231 | 0.2 | 4.34 | 79.3 | 16 | 1.9 | 1.000 | 64.0 | 4.05 | 5.70 | 6.81 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 226 | 0.2 | 4.43 | 91.1 | 18 | 0.8 | 0.429 | 64.0 | 4.04 | 5.94 | 8.12 | - |
| 2026-09-09-offline-final-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 203 | 0.2 | 4.93 | 92.4 | 18 | 0.8 | 0.429 | 64.0 | 4.01 | 6.21 | 29.25 | 0.64 |
| 2026-09-09-offline-final-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 2000 | 233 | 1.4 | 4.30 | 110.0 | 17 | 12.4 | 1.000 | 64.0 | 4.05 | 5.55 | 6.32 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 229 | 1.4 | 4.37 | 154.3 | 18 | 6.7 | 0.544 | 64.0 | 3.99 | 5.28 | 9.89 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 140 | 0.9 | 7.12 | 175.6 | 18 | 6.7 | 0.544 | 64.0 | 4.03 | 29.74 | 45.22 | 0.66 |
| 2026-09-09-offline-final-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 2000 | 221 | 1.4 | 4.52 | 114.9 | 17 | 12.4 | 1.000 | 64.0 | 4.07 | 6.01 | 7.41 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 229 | 1.4 | 4.37 | 159.1 | 18 | 6.7 | 0.544 | 64.0 | 4.00 | 5.76 | 7.10 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 229 | 1.4 | 4.36 | 155.7 | 18 | 6.7 | 0.544 | 64.0 | 3.99 | 5.91 | 6.89 | 0.66 |
| 2026-09-09-offline-final-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 2000 | 234 | 1.4 | 4.27 | 108.6 | 17 | 12.4 | 1.000 | 64.0 | 3.99 | 5.15 | 6.21 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 227 | 1.4 | 4.40 | 155.6 | 18 | 6.7 | 0.544 | 64.0 | 4.03 | 5.62 | 9.49 | - |
| 2026-09-09-offline-final-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 232 | 1.4 | 4.30 | 154.1 | 18 | 6.7 | 0.544 | 64.0 | 3.99 | 5.77 | 6.23 | 0.66 |

## Node gates (each label against `baseline`; medians over paired repeats within one run name)

| run | workload | settings | label | throughput vs baseline | p95 vs baseline | baseline p95 µs | label p95 µs | gate | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-offline-final-node-500 | import-500 | blocks 126728, chunk 500, commit_instrumented true | optimized | 1.016 | 0.815 | 26132 | 21299 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-offline-final-node-500 | import-500 | blocks 126728, chunk 500, commit_instrumented true | serial | 0.648 | 1.704 | 26132 | 44532 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | FAIL |
| 2026-09-09-offline-final-node-5000 | import-5000 | blocks 126728, chunk 5000, commit_instrumented true | optimized | 1.523 | 0.623 | 384565 | 239469 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-offline-final-node-5000 | import-5000 | blocks 126728, chunk 5000, commit_instrumented true | serial | 0.881 | 0.792 | 384565 | 304611 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | FAIL |
| 2026-09-09-offline-final-node-byron-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | optimized | 0.879 | 1.074 | 5792 | 6218 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | FAIL |
| 2026-09-09-offline-final-node-byron-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | serial | 0.979 | 1.025 | 5792 | 5935 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-offline-final-node-modern-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | optimized | 0.986 | 1.065 | 5550 | 5911 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-offline-final-node-modern-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | serial | 0.983 | 1.013 | 5550 | 5620 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |

