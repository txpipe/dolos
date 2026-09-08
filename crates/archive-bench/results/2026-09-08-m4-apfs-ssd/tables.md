## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 448,449,450,451,452,453,454,455 of /Users/santiago/dolos-instances/flatfile-lab/mainnet-raw: 156621 blocks, 876.7 MiB | 2026-09-08T12:50:16.396349+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 9 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 24.3 MiB | 2026-09-08T12:50:29.374662+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 30 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 33.2 MiB | 2026-09-08T12:50:30.079247+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 70 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 135.0 MiB | 2026-09-08T12:50:31.214304+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 130 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 1275.3 MiB | 2026-09-08T12:50:37.256095+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 260 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 943.2 MiB | 2026-09-08T12:50:55.110998+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 320 of /Volumes/Santi HDD/dolos-instances/mainnet-v17/blocks: 20000 blocks, 288.2 MiB | 2026-09-08T12:51:05.189875+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | segments 50,150,300 of /Volumes/Santi HDD/dolos-work/flatfile-compression/preprod-raw: 55101 blocks, 174.3 MiB | 2026-09-08T12:51:10.269545+00:00 |
| 706c3bd9 | 5e4030af (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | - | immutable /Volumes/Santi HDD/mithril-snapshots/preview-20260121/immutable: 20000 blocks, 202.7 MiB | 2026-09-08T12:51:13.631160+00:00 |
| 029a4449 | 7475c9c7 | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | segments 448,449,450,451 of /Users/santiago/dolos-instances/flatfile-lab/mainnet-raw: 84481 blocks, 423.5 MiB | 2026-09-08T14:03:02.645039+00:00 |
| f01c8f96 | 7475c9c7 (dirty) | 1.7.0-alpha.1 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | segments 448,449,450,451 of /Users/santiago/dolos-instances/flatfile-lab/mainnet-raw: 84481 blocks, 423.5 MiB | 2026-09-08T15:17:34.579282+00:00 |

## Writes

| run | workload | codec | rep | blocks/s | raw MB/s | ratio | encode MB/s (cpu) | batch p50 ms | p95 ms | p99 ms | fsync p50 ms | peak heap MiB | cpu µs/block |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 029a4449 | write-1 | raw | 0 | 235 | 1.2 | 1.000 | 16244 | 4.05 | 5.45 | 7.36 | 3.88 | 0.0 | 217.2 |
| 029a4449 | write-1 | zstd3 | 0 | 229 | 1.2 | 0.737 | 81 | 4.07 | 5.76 | 7.86 | 3.88 | 0.1 | 267.1 |
| 029a4449 | write-1 | zstd3-dict | 0 | 223 | 1.1 | 0.588 | 45 | 4.17 | 5.97 | 8.19 | 3.93 | 0.1 | 327.5 |
| 029a4449 | write-100 | raw | 0 | 9254 | 46.4 | 1.000 | 19345 | 10.10 | 14.95 | 24.72 | 4.67 | 0.0 | 48.3 |
| 029a4449 | write-100 | zstd3 | 0 | 9877 | 49.5 | 0.737 | 214 | 9.73 | 13.98 | 22.92 | 4.26 | 0.2 | 47.9 |
| 029a4449 | write-100 | zstd3-dict | 0 | 9686 | 48.6 | 0.588 | 159 | 10.04 | 14.00 | 16.65 | 4.27 | 0.2 | 51.4 |
| 029a4449 | write-500 | raw | 0 | 28968 | 145.2 | 1.000 | 56640 | 16.08 | 23.36 | 34.05 | 6.87 | 0.0 | 15.5 |
| 029a4449 | write-500 | zstd3 | 0 | 21364 | 107.1 | 0.737 | 383 | 21.94 | 31.06 | 77.27 | 6.15 | 0.2 | 26.8 |
| 029a4449 | write-500 | zstd3-dict | 0 | 19669 | 98.6 | 0.588 | 238 | 24.64 | 35.06 | 89.06 | 5.93 | 0.2 | 33.3 |
| 029a4449 | write-1 | raw | 1 | 221 | 1.1 | 1.000 | 14837 | 4.18 | 6.02 | 8.56 | 3.98 | 0.0 | 244.8 |
| 029a4449 | write-1 | zstd3 | 1 | 229 | 1.1 | 0.737 | 76 | 4.08 | 5.75 | 8.54 | 3.88 | 0.1 | 267.0 |
| 029a4449 | write-1 | zstd3-dict | 1 | 225 | 1.1 | 0.588 | 45 | 4.15 | 5.90 | 7.80 | 3.91 | 0.1 | 324.7 |
| 029a4449 | write-100 | raw | 1 | 10508 | 52.7 | 1.000 | 26072 | 9.04 | 12.91 | 19.97 | 4.53 | 0.0 | 39.2 |
| 029a4449 | write-100 | zstd3 | 1 | 10336 | 51.8 | 0.737 | 251 | 9.04 | 13.71 | 26.76 | 4.42 | 0.2 | 41.2 |
| 029a4449 | write-100 | zstd3-dict | 1 | 9867 | 49.5 | 0.588 | 169 | 9.76 | 13.45 | 21.68 | 4.28 | 0.2 | 48.8 |
| 029a4449 | write-500 | raw | 1 | 27654 | 138.6 | 1.000 | 56551 | 16.06 | 26.35 | 81.99 | 6.90 | 0.0 | 15.2 |
| 029a4449 | write-500 | zstd3 | 1 | 21261 | 106.6 | 0.737 | 382 | 21.95 | 30.05 | 39.71 | 6.38 | 0.2 | 26.6 |
| 029a4449 | write-500 | zstd3-dict | 1 | 19810 | 99.3 | 0.588 | 238 | 24.00 | 35.32 | 88.93 | 5.67 | 0.2 | 32.9 |
| 029a4449 | write-1 | raw | 2 | 230 | 1.2 | 1.000 | 15882 | 4.07 | 5.72 | 8.01 | 3.90 | 0.0 | 228.7 |
| 029a4449 | write-1 | zstd3 | 2 | 225 | 1.1 | 0.737 | 70 | 4.13 | 5.94 | 8.45 | 3.92 | 0.1 | 290.8 |
| 029a4449 | write-1 | zstd3-dict | 2 | 225 | 1.1 | 0.588 | 45 | 4.14 | 5.90 | 7.84 | 3.91 | 0.1 | 316.1 |
| 029a4449 | write-100 | raw | 2 | 10400 | 52.1 | 1.000 | 25735 | 9.09 | 13.03 | 19.42 | 4.55 | 0.0 | 39.0 |
| 029a4449 | write-100 | zstd3 | 2 | 10601 | 53.1 | 0.737 | 246 | 9.03 | 13.42 | 21.25 | 4.29 | 0.2 | 41.4 |
| 029a4449 | write-100 | zstd3-dict | 2 | 10499 | 52.6 | 0.588 | 182 | 8.95 | 13.98 | 20.53 | 4.16 | 0.2 | 44.8 |
| 029a4449 | write-500 | raw | 2 | 27258 | 136.7 | 1.000 | 53991 | 16.67 | 27.28 | 76.22 | 6.21 | 0.0 | 16.7 |
| 029a4449 | write-500 | zstd3 | 2 | 22773 | 114.2 | 0.737 | 377 | 20.89 | 28.93 | 43.06 | 5.76 | 0.2 | 26.7 |
| 029a4449 | write-500 | zstd3-dict | 2 | 20223 | 101.4 | 0.588 | 237 | 23.43 | 34.01 | 67.96 | 5.49 | 0.2 | 32.8 |

## Reads

| run | workload | cache | thr | codec | rep | ops/s | blocks/s | point p50 µs | p95 µs | p99 µs | page p50 ms | p95 ms | decode µs/blk | cpu µs/op | disk MiB | amplif. |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 029a4449 | point-uniform | warm | 1 | raw | 0 | 929046 | 929046 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.1 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | raw | 0 | 967366 | 967366 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.0 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | raw | 0 | 14123 | 1412295 | 0 | 0 | 0 | 0.07 | 0.10 | 0.0 | 70.7 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | raw | 0 | 1662171 | 1662171 | 4 | 7 | 10 | 0.00 | 0.00 | 0.0 | 4.4 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | raw | 0 | 1689594 | 1689594 | 4 | 7 | 9 | 0.00 | 0.00 | 0.0 | 4.4 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | raw | 0 | 16320 | 1632037 | 0 | 0 | 0 | 0.48 | 0.61 | 0.0 | 469.5 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | raw | 0 | 17 | 1411765 | 0 | 0 | 0 | 59.80 | 59.80 | 0.0 | 59763.7 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | raw | 0 | 4701 | 4701 | 89 | 878 | 1166 | 0.00 | 0.00 | 0.0 | 8.7 | 243.8 | 2.42 |
| 029a4449 | point-local | evict | 1 | raw | 0 | 4862 | 4862 | 86 | 866 | 1152 | 0.00 | 0.00 | 0.0 | 8.4 | 238.4 | 2.38 |
| 029a4449 | page-100 | evict | 1 | raw | 0 | 544 | 54408 | 0 | 0 | 0 | 1.50 | 4.83 | 0.0 | 237.3 | 345.1 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | raw | 0 | 339445 | 339445 | 1 | 135 | 243 | 0.00 | 0.00 | 0.0 | 3.4 | 44.7 | 0.45 |
| 029a4449 | point-local | evict | 8 | raw | 0 | 83688 | 83688 | 101 | 225 | 359 | 0.00 | 0.00 | 0.0 | 10.0 | 240.0 | 2.37 |
| 029a4449 | page-100 | evict | 8 | raw | 0 | 3696 | 369563 | 0 | 0 | 0 | 1.62 | 5.80 | 0.0 | 274.6 | 335.5 | 0.67 |
| 029a4449 | scan | evict | 1 | raw | 0 | 3 | 245246 | 0 | 0 | 0 | 344.46 | 344.46 | 0.0 | 181982.8 | 423.5 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3 | 0 | 70022 | 70022 | 2 | 15 | 25 | 0.00 | 0.00 | 8.2 | 3.8 | 0.3 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3 | 0 | 268446 | 268446 | 2 | 15 | 25 | 0.00 | 0.00 | 2.7 | 3.7 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3 | 0 | 2978 | 297814 | 0 | 0 | 0 | 0.30 | 0.62 | 2.7 | 335.6 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3 | 0 | 1352555 | 1352555 | 3 | 16 | 31 | 0.00 | 0.00 | 2.9 | 5.0 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3 | 0 | 1156003 | 1156003 | 3 | 16 | 31 | 0.00 | 0.00 | 2.8 | 4.8 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3 | 0 | 15359 | 1535899 | 0 | 0 | 0 | 0.40 | 0.88 | 2.9 | 444.9 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3 | 0 | 4 | 300958 | 0 | 0 | 0 | 280.76 | 280.76 | 2.7 | 280198.3 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3 | 0 | 4310 | 4310 | 106 | 938 | 1379 | 0.00 | 0.00 | 2.6 | 10.9 | 209.4 | 2.81 |
| 029a4449 | point-local | evict | 1 | zstd3 | 0 | 4634 | 4634 | 103 | 906 | 1202 | 0.00 | 0.00 | 2.6 | 10.4 | 207.4 | 2.81 |
| 029a4449 | page-100 | evict | 1 | zstd3 | 0 | 560 | 55951 | 0 | 0 | 0 | 1.47 | 4.39 | 2.4 | 389.5 | 253.9 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | zstd3 | 0 | 283710 | 283710 | 2 | 130 | 510 | 0.00 | 0.00 | 2.8 | 5.4 | 38.2 | 0.52 |
| 029a4449 | point-local | evict | 8 | zstd3 | 0 | 82686 | 82686 | 97 | 242 | 653 | 0.00 | 0.00 | 2.7 | 11.5 | 206.1 | 2.77 |
| 029a4449 | page-100 | evict | 8 | zstd3 | 0 | 3759 | 375899 | 0 | 0 | 0 | 1.62 | 4.95 | 2.8 | 487.5 | 238.9 | 0.64 |
| 029a4449 | scan | evict | 1 | zstd3 | 0 | 2 | 177859 | 0 | 0 | 0 | 475.00 | 475.00 | 2.3 | 350603.8 | 312.3 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3-dict | 0 | 323414 | 323414 | 2 | 10 | 19 | 0.00 | 0.00 | 2.0 | 3.1 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3-dict | 0 | 345432 | 345432 | 2 | 9 | 17 | 0.00 | 0.00 | 1.9 | 2.9 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3-dict | 0 | 4116 | 411608 | 0 | 0 | 0 | 0.22 | 0.42 | 1.9 | 241.9 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3-dict | 0 | 1284119 | 1284119 | 3 | 15 | 30 | 0.00 | 0.00 | 2.8 | 4.9 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3-dict | 0 | 1350796 | 1350796 | 3 | 14 | 30 | 0.00 | 0.00 | 2.7 | 4.8 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3-dict | 0 | 15426 | 1542628 | 0 | 0 | 0 | 0.43 | 0.92 | 2.8 | 437.4 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3-dict | 0 | 4 | 376753 | 0 | 0 | 0 | 224.26 | 224.26 | 2.0 | 223833.5 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3-dict | 0 | 4966 | 4966 | 79 | 880 | 1165 | 0.00 | 0.00 | 2.3 | 9.8 | 183.2 | 3.08 |
| 029a4449 | point-local | evict | 1 | zstd3-dict | 0 | 4818 | 4818 | 78 | 910 | 1327 | 0.00 | 0.00 | 2.3 | 9.8 | 180.7 | 3.07 |
| 029a4449 | page-100 | evict | 1 | zstd3-dict | 0 | 620 | 62032 | 0 | 0 | 0 | 1.30 | 4.13 | 2.0 | 331.6 | 203.9 | 0.69 |
| 029a4449 | point-uniform | evict | 8 | zstd3-dict | 0 | 297975 | 297975 | 2 | 123 | 507 | 0.00 | 0.00 | 2.4 | 4.7 | 32.0 | 0.54 |
| 029a4449 | point-local | evict | 8 | zstd3-dict | 0 | 99624 | 99624 | 87 | 207 | 310 | 0.00 | 0.00 | 2.5 | 9.5 | 181.1 | 3.04 |
| 029a4449 | page-100 | evict | 8 | zstd3-dict | 0 | 4550 | 455016 | 0 | 0 | 0 | 1.35 | 4.16 | 2.3 | 416.1 | 188.1 | 0.64 |
| 029a4449 | scan | evict | 1 | zstd3-dict | 0 | 3 | 214181 | 0 | 0 | 0 | 394.26 | 394.26 | 1.8 | 292112.9 | 249.0 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | raw | 1 | 903883 | 903883 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.1 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | raw | 1 | 963283 | 963283 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.0 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | raw | 1 | 13895 | 1389479 | 0 | 0 | 0 | 0.07 | 0.10 | 0.0 | 71.7 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | raw | 1 | 1855833 | 1855833 | 3 | 6 | 8 | 0.00 | 0.00 | 0.0 | 3.2 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | raw | 1 | 1665475 | 1665475 | 4 | 7 | 9 | 0.00 | 0.00 | 0.0 | 4.3 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | raw | 1 | 17465 | 1746549 | 0 | 0 | 0 | 0.44 | 0.67 | 0.0 | 400.4 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | raw | 1 | 17 | 1412929 | 0 | 0 | 0 | 59.74 | 59.74 | 0.0 | 59720.0 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | raw | 1 | 5085 | 5085 | 102 | 844 | 1133 | 0.00 | 0.00 | 0.0 | 8.8 | 243.2 | 2.41 |
| 029a4449 | point-local | evict | 1 | raw | 1 | 5118 | 5118 | 87 | 860 | 1271 | 0.00 | 0.00 | 0.0 | 8.3 | 239.0 | 2.39 |
| 029a4449 | page-100 | evict | 1 | raw | 1 | 626 | 62614 | 0 | 0 | 0 | 1.38 | 4.07 | 0.0 | 173.3 | 342.5 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | raw | 1 | 376054 | 376054 | 1 | 137 | 233 | 0.00 | 0.00 | 0.0 | 2.5 | 44.6 | 0.44 |
| 029a4449 | point-local | evict | 8 | raw | 1 | 80451 | 80451 | 100 | 221 | 389 | 0.00 | 0.00 | 0.0 | 8.8 | 239.8 | 2.37 |
| 029a4449 | page-100 | evict | 8 | raw | 1 | 3988 | 398844 | 0 | 0 | 0 | 1.46 | 5.44 | 0.0 | 220.3 | 322.5 | 0.64 |
| 029a4449 | scan | evict | 1 | raw | 1 | 3 | 263001 | 0 | 0 | 0 | 321.39 | 321.39 | 0.0 | 170718.1 | 423.5 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3 | 1 | 266315 | 266315 | 2 | 15 | 25 | 0.00 | 0.00 | 2.7 | 3.7 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3 | 1 | 267558 | 267558 | 2 | 15 | 25 | 0.00 | 0.00 | 2.7 | 3.7 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3 | 1 | 2979 | 297933 | 0 | 0 | 0 | 0.30 | 0.61 | 2.7 | 335.5 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3 | 1 | 1354501 | 1354501 | 3 | 17 | 30 | 0.00 | 0.00 | 2.9 | 4.9 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3 | 1 | 1059091 | 1059091 | 3 | 16 | 29 | 0.00 | 0.00 | 2.7 | 4.5 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3 | 1 | 15482 | 1548207 | 0 | 0 | 0 | 0.41 | 0.88 | 2.9 | 459.0 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3 | 1 | 4 | 310617 | 0 | 0 | 0 | 272.11 | 272.11 | 2.6 | 271606.0 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3 | 1 | 4813 | 4813 | 86 | 880 | 1166 | 0.00 | 0.00 | 2.5 | 10.4 | 208.8 | 2.80 |
| 029a4449 | point-local | evict | 1 | zstd3 | 1 | 4851 | 4851 | 84 | 877 | 1290 | 0.00 | 0.00 | 2.6 | 10.4 | 206.1 | 2.79 |
| 029a4449 | page-100 | evict | 1 | zstd3 | 1 | 554 | 55405 | 0 | 0 | 0 | 1.41 | 4.24 | 2.5 | 394.2 | 255.0 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | zstd3 | 1 | 255454 | 255454 | 2 | 133 | 603 | 0.00 | 0.00 | 2.9 | 5.7 | 38.1 | 0.51 |
| 029a4449 | point-local | evict | 8 | zstd3 | 1 | 98653 | 98653 | 94 | 201 | 294 | 0.00 | 0.00 | 2.7 | 9.9 | 205.2 | 2.75 |
| 029a4449 | page-100 | evict | 8 | zstd3 | 1 | 4103 | 410278 | 0 | 0 | 0 | 1.48 | 4.58 | 2.8 | 464.6 | 231.9 | 0.63 |
| 029a4449 | scan | evict | 1 | zstd3 | 1 | 2 | 175560 | 0 | 0 | 0 | 481.03 | 481.03 | 2.3 | 354808.4 | 312.2 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3-dict | 1 | 304924 | 304924 | 2 | 11 | 20 | 0.00 | 0.00 | 2.2 | 3.3 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3-dict | 1 | 313366 | 313366 | 2 | 11 | 19 | 0.00 | 0.00 | 2.2 | 3.2 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3-dict | 1 | 3596 | 359633 | 0 | 0 | 0 | 0.25 | 0.49 | 2.2 | 277.6 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3-dict | 1 | 1347357 | 1347357 | 3 | 14 | 33 | 0.00 | 0.00 | 2.6 | 4.8 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3-dict | 1 | 1322070 | 1322070 | 3 | 13 | 27 | 0.00 | 0.00 | 2.6 | 4.5 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3-dict | 1 | 15639 | 1563891 | 0 | 0 | 0 | 0.39 | 0.84 | 2.6 | 423.3 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3-dict | 1 | 4 | 376356 | 0 | 0 | 0 | 224.53 | 224.53 | 2.1 | 223689.5 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3-dict | 1 | 5070 | 5070 | 79 | 867 | 1125 | 0.00 | 0.00 | 2.3 | 9.4 | 184.3 | 3.10 |
| 029a4449 | point-local | evict | 1 | zstd3-dict | 1 | 5280 | 5280 | 76 | 848 | 1116 | 0.00 | 0.00 | 2.2 | 9.1 | 180.5 | 3.07 |
| 029a4449 | page-100 | evict | 1 | zstd3-dict | 1 | 628 | 62769 | 0 | 0 | 0 | 1.29 | 3.99 | 2.0 | 357.0 | 203.3 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | zstd3-dict | 1 | 269648 | 269648 | 2 | 123 | 672 | 0.00 | 0.00 | 2.4 | 5.0 | 32.0 | 0.54 |
| 029a4449 | point-local | evict | 8 | zstd3-dict | 1 | 96212 | 96212 | 86 | 206 | 431 | 0.00 | 0.00 | 2.6 | 9.9 | 180.6 | 3.03 |
| 029a4449 | page-100 | evict | 8 | zstd3-dict | 1 | 4780 | 478048 | 0 | 0 | 0 | 1.32 | 4.20 | 2.3 | 404.6 | 185.4 | 0.63 |
| 029a4449 | scan | evict | 1 | zstd3-dict | 1 | 2 | 170050 | 0 | 0 | 0 | 496.76 | 496.76 | 1.9 | 304489.0 | 249.0 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | raw | 2 | 924264 | 924264 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.1 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | raw | 2 | 946243 | 946243 | 1 | 2 | 3 | 0.00 | 0.00 | 0.0 | 1.1 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | raw | 2 | 14381 | 1438123 | 0 | 0 | 0 | 0.07 | 0.09 | 0.0 | 69.5 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | raw | 2 | 1655960 | 1655960 | 5 | 7 | 9 | 0.00 | 0.00 | 0.0 | 4.5 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | raw | 2 | 1674370 | 1674370 | 4 | 7 | 10 | 0.00 | 0.00 | 0.0 | 4.4 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | raw | 2 | 16547 | 1654695 | 0 | 0 | 0 | 0.48 | 0.62 | 0.0 | 455.4 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | raw | 2 | 17 | 1426340 | 0 | 0 | 0 | 59.18 | 59.18 | 0.0 | 59160.9 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | raw | 2 | 5591 | 5591 | 85 | 796 | 1054 | 0.00 | 0.00 | 0.0 | 8.4 | 243.9 | 2.42 |
| 029a4449 | point-local | evict | 1 | raw | 2 | 5718 | 5718 | 84 | 786 | 1056 | 0.00 | 0.00 | 0.0 | 8.1 | 238.3 | 2.38 |
| 029a4449 | page-100 | evict | 1 | raw | 2 | 633 | 63321 | 0 | 0 | 0 | 1.42 | 4.08 | 0.0 | 167.9 | 341.1 | 0.67 |
| 029a4449 | point-uniform | evict | 8 | raw | 2 | 365083 | 365083 | 1 | 136 | 233 | 0.00 | 0.00 | 0.0 | 2.9 | 44.6 | 0.44 |
| 029a4449 | point-local | evict | 8 | raw | 2 | 87795 | 87795 | 99 | 219 | 299 | 0.00 | 0.00 | 0.0 | 8.2 | 240.0 | 2.37 |
| 029a4449 | page-100 | evict | 8 | raw | 2 | 4137 | 413732 | 0 | 0 | 0 | 1.46 | 5.29 | 0.0 | 247.6 | 327.2 | 0.65 |
| 029a4449 | scan | evict | 1 | raw | 2 | 3 | 265925 | 0 | 0 | 0 | 317.72 | 317.72 | 0.0 | 170412.7 | 423.1 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3 | 2 | 265383 | 265383 | 2 | 15 | 25 | 0.00 | 0.00 | 2.7 | 3.8 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3 | 2 | 267421 | 267421 | 2 | 15 | 25 | 0.00 | 0.00 | 2.7 | 3.7 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3 | 2 | 3050 | 305023 | 0 | 0 | 0 | 0.29 | 0.60 | 2.6 | 327.6 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3 | 2 | 1227474 | 1227474 | 3 | 17 | 34 | 0.00 | 0.00 | 3.2 | 5.1 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3 | 2 | 1279151 | 1279151 | 3 | 17 | 32 | 0.00 | 0.00 | 3.0 | 5.0 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3 | 2 | 15156 | 1515572 | 0 | 0 | 0 | 0.43 | 0.92 | 3.0 | 465.5 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3 | 2 | 4 | 304957 | 0 | 0 | 0 | 277.09 | 277.09 | 2.6 | 276851.1 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3 | 2 | 4989 | 4989 | 85 | 866 | 1127 | 0.00 | 0.00 | 2.6 | 10.7 | 209.3 | 2.81 |
| 029a4449 | point-local | evict | 1 | zstd3 | 2 | 5030 | 5030 | 86 | 867 | 1154 | 0.00 | 0.00 | 2.5 | 10.3 | 205.6 | 2.79 |
| 029a4449 | page-100 | evict | 1 | zstd3 | 2 | 571 | 57055 | 0 | 0 | 0 | 1.50 | 4.19 | 2.4 | 385.9 | 252.5 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | zstd3 | 2 | 264083 | 264083 | 2 | 132 | 541 | 0.00 | 0.00 | 2.8 | 5.6 | 38.4 | 0.52 |
| 029a4449 | point-local | evict | 8 | zstd3 | 2 | 99954 | 99954 | 93 | 199 | 291 | 0.00 | 0.00 | 2.7 | 9.8 | 204.5 | 2.74 |
| 029a4449 | page-100 | evict | 8 | zstd3 | 2 | 4167 | 416698 | 0 | 0 | 0 | 1.48 | 4.46 | 2.8 | 463.1 | 233.5 | 0.63 |
| 029a4449 | scan | evict | 1 | zstd3 | 2 | 2 | 184007 | 0 | 0 | 0 | 459.01 | 459.01 | 2.3 | 342299.2 | 312.1 | 1.00 |
| 029a4449 | point-uniform | warm | 1 | zstd3-dict | 2 | 308013 | 308013 | 2 | 11 | 20 | 0.00 | 0.00 | 2.2 | 3.2 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 1 | zstd3-dict | 2 | 314091 | 314091 | 2 | 10 | 19 | 0.00 | 0.00 | 2.2 | 3.2 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 1 | zstd3-dict | 2 | 3612 | 361197 | 0 | 0 | 0 | 0.25 | 0.49 | 2.2 | 276.7 | 0.0 | 0.00 |
| 029a4449 | point-uniform | warm | 8 | zstd3-dict | 2 | 1431810 | 1431810 | 3 | 14 | 29 | 0.00 | 0.00 | 2.7 | 5.0 | 0.0 | 0.00 |
| 029a4449 | point-local | warm | 8 | zstd3-dict | 2 | 1406260 | 1406260 | 3 | 14 | 27 | 0.00 | 0.00 | 2.6 | 4.7 | 0.0 | 0.00 |
| 029a4449 | page-100 | warm | 8 | zstd3-dict | 2 | 16143 | 1614251 | 0 | 0 | 0 | 0.41 | 0.75 | 2.5 | 435.7 | 0.0 | 0.00 |
| 029a4449 | scan | warm | 1 | zstd3-dict | 2 | 4 | 379995 | 0 | 0 | 0 | 222.30 | 222.30 | 2.0 | 221732.8 | 0.0 | 0.00 |
| 029a4449 | point-uniform | evict | 1 | zstd3-dict | 2 | 4770 | 4770 | 85 | 893 | 1163 | 0.00 | 0.00 | 2.3 | 9.5 | 183.6 | 3.09 |
| 029a4449 | point-local | evict | 1 | zstd3-dict | 2 | 4969 | 4969 | 79 | 881 | 1162 | 0.00 | 0.00 | 2.3 | 9.4 | 180.5 | 3.07 |
| 029a4449 | page-100 | evict | 1 | zstd3-dict | 2 | 626 | 62605 | 0 | 0 | 0 | 1.39 | 3.98 | 1.9 | 321.3 | 201.3 | 0.68 |
| 029a4449 | point-uniform | evict | 8 | zstd3-dict | 2 | 294548 | 294548 | 2 | 120 | 531 | 0.00 | 0.00 | 2.4 | 4.8 | 32.4 | 0.55 |
| 029a4449 | point-local | evict | 8 | zstd3-dict | 2 | 102849 | 102849 | 84 | 194 | 412 | 0.00 | 0.00 | 2.4 | 9.2 | 179.9 | 3.02 |
| 029a4449 | page-100 | evict | 8 | zstd3-dict | 2 | 4866 | 486633 | 0 | 0 | 0 | 1.33 | 3.71 | 2.3 | 405.0 | 190.0 | 0.64 |
| 029a4449 | scan | evict | 1 | zstd3-dict | 2 | 3 | 212046 | 0 | 0 | 0 | 398.46 | 398.46 | 1.9 | 291235.5 | 249.0 | 1.00 |
| 029a4449 | mix-90-10 | warm | 8 | raw | 0 | 158744 | 1778245 | 4 | 7 | 12 | 0.42 | 0.66 | 0.0 | 43.0 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | raw | 0 | 33692 | 1713771 | 5 | 7 | 10 | 0.46 | 0.60 | 0.0 | 221.0 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | raw | 0 | 18477 | 1659385 | 5 | 7 | 10 | 0.48 | 0.60 | 0.0 | 413.4 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | raw | 0 | 59618 | 667838 | 2 | 273 | 481 | 0.23 | 2.99 | 0.0 | 21.6 | 410.6 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | raw | 0 | 26032 | 1324129 | 4 | 133 | 379 | 0.44 | 2.00 | 0.0 | 176.3 | 422.3 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | raw | 0 | 16412 | 1473939 | 5 | 9 | 305 | 0.45 | 1.14 | 0.0 | 355.2 | 423.5 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3 | 0 | 125055 | 1400857 | 3 | 19 | 34 | 0.45 | 0.94 | 3.1 | 53.8 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3 | 0 | 30536 | 1553278 | 3 | 19 | 36 | 0.43 | 0.94 | 3.1 | 235.7 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3 | 0 | 17688 | 1588545 | 3 | 18 | 35 | 0.44 | 0.92 | 3.0 | 424.6 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3 | 0 | 45126 | 505501 | 3 | 292 | 820 | 0.63 | 3.42 | 2.9 | 52.3 | 304.7 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3 | 0 | 21480 | 1092611 | 3 | 129 | 550 | 0.45 | 2.34 | 3.0 | 234.6 | 310.4 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3 | 0 | 13473 | 1209964 | 3 | 34 | 322 | 0.46 | 1.69 | 3.2 | 431.8 | 311.7 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3-dict | 0 | 142540 | 1596723 | 4 | 14 | 28 | 0.42 | 0.80 | 2.6 | 49.7 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3-dict | 0 | 33563 | 1707250 | 4 | 14 | 27 | 0.41 | 0.81 | 2.5 | 222.8 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3-dict | 0 | 19410 | 1743154 | 3 | 14 | 26 | 0.41 | 0.79 | 2.5 | 391.3 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3-dict | 0 | 51907 | 581457 | 3 | 272 | 886 | 0.50 | 2.81 | 2.2 | 42.5 | 245.4 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3-dict | 0 | 24052 | 1223424 | 3 | 120 | 535 | 0.42 | 1.94 | 2.4 | 213.5 | 247.9 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3-dict | 0 | 14918 | 1339756 | 4 | 26 | 280 | 0.43 | 1.45 | 2.7 | 390.1 | 248.6 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | raw | 1 | 151643 | 1698698 | 4 | 7 | 11 | 0.44 | 0.66 | 0.0 | 45.3 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | raw | 1 | 33426 | 1700257 | 5 | 7 | 11 | 0.46 | 0.61 | 0.0 | 225.5 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | raw | 1 | 19111 | 1716278 | 5 | 7 | 10 | 0.46 | 0.62 | 0.0 | 389.6 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | raw | 1 | 59381 | 665182 | 1 | 279 | 521 | 0.21 | 3.01 | 0.0 | 21.4 | 411.3 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | raw | 1 | 25562 | 1300224 | 4 | 131 | 363 | 0.44 | 1.98 | 0.0 | 184.9 | 419.1 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | raw | 1 | 16022 | 1438914 | 4 | 18 | 244 | 0.45 | 1.28 | 0.0 | 348.4 | 423.5 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3 | 1 | 135587 | 1518839 | 3 | 19 | 36 | 0.44 | 0.92 | 3.1 | 53.0 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3 | 1 | 30775 | 1565392 | 3 | 19 | 35 | 0.44 | 0.90 | 3.1 | 239.3 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3 | 1 | 17803 | 1598839 | 3 | 18 | 32 | 0.42 | 0.92 | 3.0 | 416.3 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3 | 1 | 46923 | 525628 | 3 | 268 | 780 | 0.61 | 3.13 | 2.7 | 49.0 | 305.1 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3 | 1 | 22002 | 1119143 | 3 | 118 | 429 | 0.46 | 2.20 | 3.0 | 233.4 | 308.5 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3 | 1 | 14295 | 1283797 | 3 | 28 | 296 | 0.45 | 1.48 | 3.0 | 421.4 | 312.0 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3-dict | 1 | 140043 | 1568753 | 3 | 14 | 30 | 0.41 | 0.83 | 2.6 | 49.1 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3-dict | 1 | 33267 | 1692189 | 4 | 14 | 28 | 0.42 | 0.83 | 2.6 | 222.8 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3-dict | 1 | 18683 | 1677908 | 4 | 14 | 25 | 0.41 | 0.83 | 2.5 | 391.0 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3-dict | 1 | 51788 | 580122 | 3 | 282 | 896 | 0.48 | 2.76 | 2.2 | 41.4 | 244.7 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3-dict | 1 | 24373 | 1239770 | 3 | 117 | 455 | 0.42 | 1.97 | 2.5 | 209.4 | 246.6 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3-dict | 1 | 15711 | 1410931 | 3 | 22 | 338 | 0.43 | 1.24 | 2.5 | 390.3 | 248.6 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | raw | 2 | 149048 | 1669632 | 5 | 8 | 13 | 0.47 | 0.60 | 0.0 | 50.0 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | raw | 2 | 33425 | 1700222 | 5 | 8 | 13 | 0.47 | 0.61 | 0.0 | 225.0 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | raw | 2 | 18338 | 1646940 | 5 | 8 | 10 | 0.48 | 0.64 | 0.0 | 409.7 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | raw | 2 | 56899 | 637382 | 2 | 284 | 535 | 0.26 | 3.13 | 0.0 | 22.3 | 412.4 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | raw | 2 | 25695 | 1307033 | 4 | 134 | 360 | 0.45 | 1.99 | 0.0 | 188.5 | 421.4 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | raw | 2 | 16261 | 1460408 | 5 | 10 | 268 | 0.46 | 1.21 | 0.0 | 361.8 | 422.9 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3 | 2 | 126205 | 1413737 | 3 | 19 | 36 | 0.46 | 0.89 | 3.2 | 54.7 | 0.1 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3 | 2 | 31088 | 1581314 | 3 | 19 | 33 | 0.44 | 0.91 | 3.0 | 243.4 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3 | 2 | 17664 | 1586325 | 3 | 18 | 35 | 0.45 | 0.90 | 3.1 | 430.4 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3 | 2 | 47807 | 535533 | 3 | 270 | 708 | 0.58 | 3.18 | 2.7 | 49.5 | 305.1 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3 | 2 | 21196 | 1078147 | 3 | 121 | 481 | 0.46 | 2.29 | 3.0 | 236.9 | 308.9 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3 | 2 | 14508 | 1302906 | 3 | 28 | 265 | 0.46 | 1.41 | 3.0 | 424.1 | 311.6 | 0.05 |
| 029a4449 | mix-90-10 | warm | 8 | zstd3-dict | 2 | 153480 | 1719274 | 4 | 14 | 25 | 0.40 | 0.77 | 2.4 | 49.1 | 0.0 | 0.00 |
| 029a4449 | mix-50-50 | warm | 8 | zstd3-dict | 2 | 34513 | 1755542 | 4 | 13 | 26 | 0.40 | 0.77 | 2.4 | 221.9 | 0.0 | 0.00 |
| 029a4449 | mix-10-90 | warm | 8 | zstd3-dict | 2 | 19560 | 1756631 | 3 | 12 | 24 | 0.39 | 0.78 | 2.4 | 385.7 | 0.0 | 0.00 |
| 029a4449 | mix-90-10 | evict | 8 | zstd3-dict | 2 | 52256 | 585364 | 3 | 246 | 750 | 0.51 | 2.74 | 2.4 | 45.1 | 243.3 | 0.37 |
| 029a4449 | mix-50-50 | evict | 8 | zstd3-dict | 2 | 24261 | 1234089 | 3 | 116 | 520 | 0.43 | 1.99 | 2.5 | 214.1 | 246.1 | 0.08 |
| 029a4449 | mix-10-90 | evict | 8 | zstd3-dict | 2 | 15442 | 1386847 | 4 | 22 | 331 | 0.43 | 1.30 | 2.5 | 392.3 | 248.6 | 0.05 |

## Append under query load

| run | workload | codec | rep | readers | writer blocks/s | batch p50 ms | p95 ms | p99 ms | reader ops/s | point p50 µs | p95 µs | page p95 ms | wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| f01c8f96 | append-query-1 | raw | 0 | 8 | 241 | 3.95 | 5.35 | 9.64 | 153131 | 5 | 7 | 0.61 | 175.5 |
| f01c8f96 | append-query-1 | zstd3 | 0 | 8 | 251 | 3.97 | 5.02 | 6.67 | 130997 | 4 | 20 | 0.99 | 168.5 |
| f01c8f96 | append-query-1 | zstd3-dict | 0 | 8 | 246 | 3.97 | 5.35 | 7.66 | 142259 | 4 | 15 | 0.88 | 171.8 |
| f01c8f96 | append-query-100 | raw | 0 | 8 | 13601 | 7.05 | 10.00 | 15.05 | 149294 | 5 | 8 | 0.63 | 3.1 |
| f01c8f96 | append-query-100 | zstd3 | 0 | 8 | 12964 | 7.25 | 10.40 | 12.96 | 129162 | 4 | 20 | 0.99 | 3.3 |
| f01c8f96 | append-query-100 | zstd3-dict | 0 | 8 | 4142 | 9.56 | 65.04 | 125.63 | 145870 | 4 | 15 | 0.85 | 10.2 |
| f01c8f96 | append-query-1 | raw | 1 | 8 | 249 | 3.97 | 5.21 | 7.19 | 152139 | 5 | 8 | 0.63 | 169.8 |
| f01c8f96 | append-query-1 | zstd3 | 1 | 8 | 251 | 3.95 | 5.12 | 7.32 | 129593 | 4 | 20 | 1.00 | 168.4 |
| f01c8f96 | append-query-1 | zstd3-dict | 1 | 8 | 247 | 3.99 | 5.14 | 7.16 | 139586 | 4 | 16 | 0.91 | 170.9 |
| f01c8f96 | append-query-100 | raw | 1 | 8 | 13767 | 6.85 | 9.58 | 19.42 | 148444 | 5 | 8 | 0.66 | 3.1 |
| f01c8f96 | append-query-100 | zstd3 | 1 | 8 | 12829 | 7.47 | 11.03 | 15.27 | 129508 | 4 | 20 | 1.00 | 3.3 |
| f01c8f96 | append-query-100 | zstd3-dict | 1 | 8 | 11476 | 8.15 | 12.35 | 15.91 | 139315 | 4 | 15 | 0.90 | 3.7 |
| f01c8f96 | append-query-1 | raw | 2 | 8 | 236 | 3.98 | 5.87 | 10.26 | 153508 | 5 | 8 | 0.64 | 179.3 |
| f01c8f96 | append-query-1 | zstd3 | 2 | 8 | 256 | 3.94 | 4.90 | 6.73 | 128964 | 4 | 20 | 1.02 | 165.3 |
| f01c8f96 | append-query-1 | zstd3-dict | 2 | 8 | 238 | 3.98 | 5.29 | 9.72 | 135646 | 4 | 16 | 0.94 | 177.4 |
| f01c8f96 | append-query-100 | raw | 2 | 8 | 13808 | 6.98 | 9.72 | 14.93 | 150169 | 5 | 8 | 0.65 | 3.1 |
| f01c8f96 | append-query-100 | zstd3 | 2 | 8 | 12932 | 7.39 | 10.31 | 16.06 | 126887 | 4 | 20 | 1.02 | 3.3 |
| f01c8f96 | append-query-100 | zstd3-dict | 2 | 8 | 11631 | 8.16 | 11.97 | 15.84 | 141080 | 4 | 15 | 0.88 | 3.6 |

## Dictionary evaluation

| run | fixture | codec | blocks | raw MiB | ratio | encode MB/s (cpu) | decode µs/blk | per era |
|---|---|---|---|---|---|---|---|---|
| 706c3bd9 | mainnet-heldout-conway | zstd3 | 156621 | 876.7 | 0.730 | 350 | 3.3 | conway 0.730 |
| 706c3bd9 | mainnet-heldout-conway | zstd3-dict:stratified | 156621 | 876.7 | 0.623 | 223 | 2.5 | conway 0.623 |
| 706c3bd9 | mainnet-heldout-conway | zstd3-dict:recent | 156621 | 876.7 | 0.563 | 242 | 2.3 | conway 0.563 |
| 706c3bd9 | mainnet-heldout-conway | zstd3-dict:balanced | 156621 | 876.7 | 0.590 | 233 | 2.4 | conway 0.590 |
| 706c3bd9 | mainnet-byron | zstd3 | 20000 | 24.3 | 0.872 | 184 | 0.3 | byron 0.872 |
| 706c3bd9 | mainnet-byron | zstd3-dict:stratified | 20000 | 24.3 | 0.728 | 188 | 0.4 | byron 0.728 |
| 706c3bd9 | mainnet-byron | zstd3-dict:recent | 20000 | 24.3 | 0.872 | 149 | 0.3 | byron 0.872 |
| 706c3bd9 | mainnet-byron | zstd3-dict:balanced | 20000 | 24.3 | 0.732 | 183 | 0.4 | byron 0.732 |
| 706c3bd9 | mainnet-shelley | zstd3 | 20000 | 33.2 | 0.938 | 252 | 0.3 | shelley 0.938 |
| 706c3bd9 | mainnet-shelley | zstd3-dict:stratified | 20000 | 33.2 | 0.915 | 171 | 0.4 | shelley 0.915 |
| 706c3bd9 | mainnet-shelley | zstd3-dict:recent | 20000 | 33.2 | 0.932 | 198 | 0.3 | shelley 0.932 |
| 706c3bd9 | mainnet-shelley | zstd3-dict:balanced | 20000 | 33.2 | 0.911 | 162 | 0.5 | shelley 0.911 |
| 706c3bd9 | mainnet-mary | zstd3 | 20000 | 135.0 | 0.848 | 361 | 2.2 | mary 0.848 |
| 706c3bd9 | mainnet-mary | zstd3-dict:stratified | 20000 | 135.0 | 0.822 | 166 | 2.0 | mary 0.822 |
| 706c3bd9 | mainnet-mary | zstd3-dict:recent | 20000 | 135.0 | 0.840 | 175 | 2.0 | mary 0.840 |
| 706c3bd9 | mainnet-mary | zstd3-dict:balanced | 20000 | 135.0 | 0.826 | 174 | 2.0 | mary 0.826 |
| 706c3bd9 | mainnet-alonzo | zstd3 | 20000 | 1275.3 | 0.503 | 531 | 42.4 | alonzo 0.503 |
| 706c3bd9 | mainnet-alonzo | zstd3-dict:stratified | 20000 | 1275.3 | 0.342 | 358 | 25.5 | alonzo 0.342 |
| 706c3bd9 | mainnet-alonzo | zstd3-dict:recent | 20000 | 1275.3 | 0.476 | 240 | 47.5 | alonzo 0.476 |
| 706c3bd9 | mainnet-alonzo | zstd3-dict:balanced | 20000 | 1275.3 | 0.351 | 345 | 25.4 | alonzo 0.351 |
| 706c3bd9 | mainnet-babbage | zstd3 | 20000 | 943.2 | 0.473 | 559 | 27.3 | babbage 0.473 |
| 706c3bd9 | mainnet-babbage | zstd3-dict:stratified | 20000 | 943.2 | 0.320 | 370 | 16.6 | babbage 0.320 |
| 706c3bd9 | mainnet-babbage | zstd3-dict:recent | 20000 | 943.2 | 0.353 | 342 | 19.3 | babbage 0.353 |
| 706c3bd9 | mainnet-babbage | zstd3-dict:balanced | 20000 | 943.2 | 0.322 | 370 | 16.7 | babbage 0.322 |
| 706c3bd9 | mainnet-conway-early | zstd3 | 20000 | 288.2 | 0.666 | 409 | 9.6 | conway 0.666 |
| 706c3bd9 | mainnet-conway-early | zstd3-dict:stratified | 20000 | 288.2 | 0.483 | 253 | 6.1 | conway 0.483 |
| 706c3bd9 | mainnet-conway-early | zstd3-dict:recent | 20000 | 288.2 | 0.508 | 239 | 6.9 | conway 0.508 |
| 706c3bd9 | mainnet-conway-early | zstd3-dict:balanced | 20000 | 288.2 | 0.487 | 250 | 6.4 | conway 0.487 |
| 706c3bd9 | preprod | zstd3 | 55101 | 174.3 | 0.682 | 308 | 1.9 | babbage 0.712, conway 0.648 |
| 706c3bd9 | preprod | zstd3-dict:stratified | 55101 | 174.3 | 0.658 | 223 | 1.5 | babbage 0.671, conway 0.642 |
| 706c3bd9 | preprod | zstd3-dict:recent | 55101 | 174.3 | 0.655 | 213 | 1.7 | babbage 0.686, conway 0.621 |
| 706c3bd9 | preprod | zstd3-dict:balanced | 55101 | 174.3 | 0.657 | 211 | 1.6 | babbage 0.681, conway 0.630 |
| 706c3bd9 | preview | zstd3 | 20000 | 202.7 | 0.477 | 511 | 6.3 | babbage 0.477 |
| 706c3bd9 | preview | zstd3-dict:stratified | 20000 | 202.7 | 0.468 | 285 | 5.8 | babbage 0.468 |
| 706c3bd9 | preview | zstd3-dict:recent | 20000 | 202.7 | 0.472 | 281 | 5.6 | babbage 0.472 |
| 706c3bd9 | preview | zstd3-dict:balanced | 20000 | 202.7 | 0.470 | 273 | 5.5 | babbage 0.470 |

## Gates (writes: candidate against raw, medians over paired repeats within one run)

| run | workload | settings | candidate | dictionary | throughput vs raw | batch p95 vs raw | verdict |
|---|---|---|---|---|---|---|---|
| 029a4449 | write-1 | batch 1, encode_threads 1, fsync true | zstd3 |  | 0.997 | 1.008 | PASS |
| 029a4449 | write-1 | batch 1, encode_threads 1, fsync true | zstd3-dict | c47b2eb1 | 0.979 | 1.032 | PASS |
| 029a4449 | write-100 | batch 100, encode_threads 1, fsync true | zstd3 |  | 0.994 | 1.053 | PASS |
| 029a4449 | write-100 | batch 100, encode_threads 1, fsync true | zstd3-dict | c47b2eb1 | 0.949 | 1.073 | PASS |
| 029a4449 | write-500 | batch 500, encode_threads 1, fsync true | zstd3 |  | 0.773 | 1.141 | FAIL |
| 029a4449 | write-500 | batch 500, encode_threads 1, fsync true | zstd3-dict | c47b2eb1 | 0.716 | 1.331 | FAIL |
| f01c8f96 | append-query-1 writer | batch 1, encode_threads ?, fsync ?, head_blocks 42240, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3 |  | 1.042 | 0.939 | PASS |
| f01c8f96 | append-query-1 writer | batch 1, encode_threads ?, fsync ?, head_blocks 42240, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3-dict | c47b2eb1 | 1.022 | 0.990 | PASS |
| f01c8f96 | append-query-100 writer | batch 100, encode_threads ?, fsync ?, head_blocks 42240, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3 |  | 0.939 | 1.070 | PASS |
| f01c8f96 | append-query-100 writer | batch 100, encode_threads ?, fsync ?, head_blocks 42240, mix (locality 0.5, page_len 100, point_share 0.9, window 512), readers 8, seed 0 | zstd3-dict | c47b2eb1 | 0.834 | 1.271 | FAIL |

## Reads against raw (medians over paired repeats within one run; microbenchmark, not the API gate)

| run | workload | settings | candidate | dictionary | ops/s vs raw | point p95 vs raw | raw p95 µs | candidate p95 µs | pairing |
|---|---|---|---|---|---|---|---|---|---|
| 029a4449 | mix-10-90 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.879 | 2.825 | 10 | 28 | paired |
| 029a4449 | mix-10-90 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.950 | 2.233 | 10 | 22 | paired |
| 029a4449 | mix-10-90 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.957 | 2.421 | 7 | 18 | paired |
| 029a4449 | mix-10-90 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 1.050 | 1.887 | 7 | 14 | paired |
| 029a4449 | mix-50-50 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.836 | 0.913 | 133 | 121 | paired |
| 029a4449 | mix-50-50 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.944 | 0.877 | 133 | 117 | paired |
| 029a4449 | mix-50-50 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.921 | 2.532 | 7 | 19 | paired |
| 029a4449 | mix-50-50 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 1.004 | 1.905 | 7 | 14 | paired |
| 029a4449 | mix-90-10 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.790 | 0.969 | 279 | 270 | paired |
| 029a4449 | mix-90-10 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.874 | 0.975 | 279 | 272 | paired |
| 029a4449 | mix-90-10 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.832 | 2.685 | 7 | 19 | paired |
| 029a4449 | mix-90-10 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.940 | 1.977 | 7 | 14 | paired |
| 029a4449 | point-local [evict t1] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.948 | 1.020 | 860 | 877 | paired |
| 029a4449 | point-local [evict t1] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.971 | 1.025 | 860 | 881 | paired |
| 029a4449 | point-local [evict t8] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 1.179 | 0.911 | 221 | 201 | paired |
| 029a4449 | point-local [evict t8] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 1.190 | 0.934 | 221 | 206 | paired |
| 029a4449 | point-local [warm t1] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.278 | 8.322 | 2 | 15 | paired |
| 029a4449 | point-local [warm t1] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.326 | 5.708 | 2 | 10 | paired |
| 029a4449 | point-local [warm t8] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.690 | 2.465 | 7 | 16 | paired |
| 029a4449 | point-local [warm t8] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.807 | 2.088 | 7 | 14 | paired |
| 029a4449 | point-uniform [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.946 | 1.043 | 844 | 880 | paired |
| 029a4449 | point-uniform [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.977 | 1.043 | 844 | 880 | paired |
| 029a4449 | point-uniform [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.723 | 0.971 | 136 | 132 | paired |
| 029a4449 | point-uniform [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.807 | 0.908 | 136 | 123 | paired |
| 029a4449 | point-uniform [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.287 | 8.200 | 2 | 15 | paired |
| 029a4449 | point-uniform [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.333 | 5.691 | 2 | 11 | paired |
| 029a4449 | point-uniform [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3 |  | 0.814 | 2.496 | 7 | 17 | paired |
| 029a4449 | point-uniform [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | zstd3-dict | c47b2eb1 | 0.811 | 2.182 | 7 | 14 | paired |

