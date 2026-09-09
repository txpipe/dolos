## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 5fcf079a | 5d13f2da | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 126728 blocks, 643.1 MiB | 2026-09-09T17:01:54.130387+00:00 |
| 5fcf079a | 5d13f2da | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-modern-window: 2111 blocks, 13.1 MiB | 2026-09-09T17:06:27.400925+00:00 |

## Node imports

| run | workload | label | revision | storage | rep | blocks | blocks/s | raw MB/s | ms/batch | cpu µs/block | max RSS MiB | segments MiB | ratio | index MiB | commit p50 ms | commit p95 ms | commit p99 ms | encoded buffers MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-automatic-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 126728 | 19049 | 96.7 | 26.19 | 37.7 | 76 | 643.1 | 1.000 | 64.0 | 20.45 | 44.43 | 81.92 | - |
| 2026-09-09-automatic-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 15086 | 76.6 | 33.07 | 55.1 | 83 | 341.6 | 0.531 | 64.0 | 29.34 | 53.38 | 109.64 | - |
| 2026-09-09-automatic-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 20269 | 102.9 | 24.62 | 64.1 | 100 | 341.6 | 0.531 | 64.0 | 19.45 | 38.96 | 57.44 | 4.62 |
| 2026-09-09-automatic-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 126728 | 13383 | 67.9 | 37.28 | 36.7 | 79 | 643.1 | 1.000 | 64.0 | 20.40 | 102.83 | 242.88 | - |
| 2026-09-09-automatic-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 16091 | 81.7 | 31.01 | 55.4 | 83 | 341.6 | 0.531 | 64.0 | 26.35 | 53.28 | 85.98 | - |
| 2026-09-09-automatic-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 20396 | 103.5 | 24.46 | 65.2 | 102 | 341.6 | 0.531 | 64.0 | 18.42 | 40.57 | 124.58 | 4.62 |
| 2026-09-09-automatic-node-500 | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 126728 | 13025 | 66.1 | 38.30 | 37.8 | 79 | 643.1 | 1.000 | 64.0 | 19.42 | 37.39 | 301.47 | - |
| 2026-09-09-automatic-node-500 | import-500 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 7626 | 38.7 | 65.43 | 57.7 | 82 | 341.6 | 0.531 | 64.0 | 33.62 | 229.38 | 394.26 | - |
| 2026-09-09-automatic-node-500 | import-500 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 17870 | 90.7 | 27.92 | 65.3 | 99 | 341.6 | 0.531 | 64.0 | 22.13 | 42.76 | 53.44 | 4.62 |
| 2026-09-09-automatic-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 126728 | 27555 | 139.8 | 176.89 | 34.8 | 180 | 643.1 | 1.000 | 64.0 | 153.62 | 229.90 | 368.84 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 12516 | 63.5 | 389.43 | 52.5 | 192 | 341.6 | 0.531 | 64.0 | 215.88 | 597.69 | 4253.02 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 31252 | 158.6 | 155.96 | 59.3 | 219 | 341.6 | 0.531 | 64.0 | 117.70 | 218.89 | 329.78 | 5.57 |
| 2026-09-09-automatic-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 126728 | 30022 | 152.4 | 162.36 | 34.8 | 190 | 643.1 | 1.000 | 64.0 | 123.08 | 214.30 | 318.50 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 21598 | 109.6 | 225.67 | 51.9 | 192 | 341.6 | 0.531 | 64.0 | 212.47 | 384.30 | 598.74 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 33679 | 170.9 | 144.72 | 58.8 | 219 | 341.6 | 0.531 | 64.0 | 122.62 | 192.28 | 363.07 | 5.57 |
| 2026-09-09-automatic-node-5000 | import-5000 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 126728 | 12506 | 63.5 | 389.74 | 34.1 | 190 | 643.1 | 1.000 | 64.0 | 138.81 | 2667.58 | 3539.99 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 20573 | 104.4 | 236.92 | 52.0 | 193 | 341.6 | 0.531 | 64.0 | 228.33 | 391.91 | 657.46 | - |
| 2026-09-09-automatic-node-5000 | import-5000 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 30845 | 156.5 | 158.02 | 58.6 | 221 | 341.6 | 0.531 | 64.0 | 130.81 | 220.59 | 306.18 | 5.57 |
| 2026-09-09-automatic-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 2000 | 212 | 0.2 | 4.73 | 117.1 | 16 | 1.9 | 1.000 | 64.0 | 4.71 | 5.89 | 7.11 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 204 | 0.2 | 4.90 | 128.7 | 18 | 0.8 | 0.429 | 64.0 | 4.45 | 6.30 | 10.76 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 198 | 0.2 | 5.06 | 225.3 | 18 | 0.8 | 0.429 | 64.0 | 4.31 | 8.31 | 13.07 | 0.62 |
| 2026-09-09-automatic-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 2000 | 205 | 0.2 | 4.89 | 319.1 | 16 | 1.9 | 1.000 | 64.0 | 4.37 | 6.35 | 11.54 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 215 | 0.2 | 4.64 | 327.1 | 18 | 0.8 | 0.429 | 64.0 | 4.01 | 6.19 | 10.10 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 227 | 0.2 | 4.40 | 290.2 | 18 | 0.8 | 0.429 | 64.0 | 4.00 | 5.88 | 10.85 | 0.62 |
| 2026-09-09-automatic-node-byron-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 2000 | 220 | 0.2 | 4.55 | 305.5 | 16 | 1.9 | 1.000 | 64.0 | 4.10 | 5.95 | 10.83 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 206 | 0.2 | 4.85 | 326.2 | 18 | 0.8 | 0.429 | 64.0 | 4.19 | 7.53 | 13.01 | - |
| 2026-09-09-automatic-node-byron-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 205 | 0.2 | 4.87 | 391.0 | 18 | 0.8 | 0.429 | 64.0 | 4.31 | 6.30 | 11.30 | 0.62 |
| 2026-09-09-automatic-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 2000 | 181 | 1.1 | 5.53 | 482.6 | 17 | 12.4 | 1.000 | 64.0 | 5.01 | 7.22 | 11.98 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 190 | 1.2 | 5.26 | 713.5 | 18 | 6.7 | 0.544 | 64.0 | 4.79 | 6.62 | 9.37 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 181 | 1.1 | 5.52 | 700.0 | 18 | 6.7 | 0.544 | 64.0 | 4.82 | 6.93 | 12.17 | 0.62 |
| 2026-09-09-automatic-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 2000 | 186 | 1.2 | 5.36 | 578.9 | 17 | 12.4 | 1.000 | 64.0 | 4.88 | 6.70 | 8.27 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 191 | 1.2 | 5.24 | 725.1 | 18 | 6.7 | 0.544 | 64.0 | 4.78 | 6.51 | 10.60 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 118 | 0.7 | 8.50 | 771.5 | 19 | 6.7 | 0.544 | 64.0 | 5.01 | 32.00 | 48.37 | 0.62 |
| 2026-09-09-automatic-node-modern-1 | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 2000 | 187 | 1.2 | 5.36 | 538.5 | 16 | 12.4 | 1.000 | 64.0 | 4.88 | 6.91 | 8.38 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | serial | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 195 | 1.2 | 5.12 | 715.6 | 19 | 6.7 | 0.544 | 64.0 | 4.75 | 6.12 | 7.91 | - |
| 2026-09-09-automatic-node-modern-1 | import-1 | optimized | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 192 | 1.2 | 5.20 | 768.2 | 18 | 6.7 | 0.544 | 64.0 | 4.80 | 6.11 | 6.88 | 0.62 |

## Node gates (each label against `baseline`; medians over paired repeats within one run name)

| run | workload | settings | label | throughput vs baseline | p95 vs baseline | baseline p95 µs | label p95 µs | gate | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-automatic-node-500 | import-500 | blocks 126728, chunk 500, commit_instrumented true | optimized | 1.515 | 0.913 | 44433 | 40567 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-automatic-node-500 | import-500 | blocks 126728, chunk 500, commit_instrumented true | serial | 1.127 | 1.201 | 44433 | 53379 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | FAIL |
| 2026-09-09-automatic-node-5000 | import-5000 | blocks 126728, chunk 5000, commit_instrumented true | optimized | 1.134 | 0.952 | 229900 | 218890 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-automatic-node-5000 | import-5000 | blocks 126728, chunk 5000, commit_instrumented true | serial | 0.747 | 1.705 | 229900 | 391905 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | FAIL |
| 2026-09-09-automatic-node-byron-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | optimized | 0.971 | 1.060 | 5947 | 6304 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-automatic-node-byron-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | serial | 0.975 | 1.060 | 5947 | 6304 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-automatic-node-modern-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | optimized | 0.971 | 1.003 | 6906 | 6926 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |
| 2026-09-09-automatic-node-modern-1 | import-1 | blocks 2000, chunk 1, commit_instrumented true | serial | 1.023 | 0.942 | 6906 | 6509 | throughput >= 0.9, commit p95 <= 1.1 (3 pairs) | PASS |

