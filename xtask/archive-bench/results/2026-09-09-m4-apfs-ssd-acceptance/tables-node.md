## Runs

| run | revision | harness | host | filesystem | corpus | first recorded |
|---|---|---|---|---|---|---|
| 6993c974 | 1c945421 (dirty) | 0.1.0 | Apple M4, macos macOS 26.5.2, 16 GiB, zstd 1.5.7 | apfs | immutable corpora/mainnet-immutable-window: 126728 blocks, 643.1 MiB | 2026-09-09T02:27:48.041606+00:00 |

## Node imports

| run | workload | label | revision | storage | rep | blocks | blocks/s | raw MB/s | ms/batch | cpu µs/block | max RSS MiB | segments MiB | ratio | index MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-m4-node | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 126728 | 38934 | 197.6 | 12.81 | 29.9 | 80 | 643.1 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 0 | 2000 | 232 | 0.2 | 4.30 | 228.5 | 16 | 1.9 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 22313 | 113.2 | 22.36 | 49.9 | 83 | 341.6 | 0.531 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 233 | 0.2 | 4.28 | 275.8 | 18 | 0.8 | 0.429 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 0 | 126728 | 28099 | 142.6 | 17.76 | 39.8 | 82 | 401.6 | 0.624 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 0 | 2000 | 239 | 0.2 | 4.19 | 249.7 | 17 | 0.9 | 0.457 | 64.0 |
| 2026-09-09-m4-node | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 126728 | 36001 | 182.7 | 13.86 | 30.2 | 80 | 643.1 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 1 | 2000 | 239 | 0.2 | 4.19 | 228.1 | 16 | 1.9 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 21927 | 111.3 | 22.75 | 49.9 | 82 | 341.6 | 0.531 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 236 | 0.2 | 4.24 | 271.8 | 18 | 0.8 | 0.429 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 1 | 126728 | 29245 | 148.4 | 17.06 | 39.4 | 82 | 401.6 | 0.624 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 1 | 2000 | 237 | 0.2 | 4.22 | 251.7 | 18 | 0.9 | 0.457 | 64.0 |
| 2026-09-09-m4-node | import-500 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 126728 | 36642 | 186.0 | 13.62 | 30.4 | 80 | 643.1 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-1 | baseline | Dolos 1.7.0-alpha.1 | v3 | 2 | 2000 | 237 | 0.2 | 4.22 | 241.8 | 16 | 1.9 | 1.000 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 22680 | 115.1 | 22.00 | 49.1 | 82 | 341.6 | 0.531 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 239 | 0.2 | 4.19 | 312.2 | 18 | 0.8 | 0.429 | 64.0 |
| 2026-09-09-m4-node | import-500 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 2 | 126728 | 27844 | 141.3 | 17.92 | 40.1 | 81 | 401.6 | 0.624 | 64.0 |
| 2026-09-09-m4-node | import-1 | candidate-zstd1 | Dolos 1.7.0-alpha.1 | v4 | 2 | 2000 | 237 | 0.2 | 4.23 | 259.0 | 17 | 0.9 | 0.457 | 64.0 |

## Node reads (through minibf)

| run | workload | cache | thr | label | rep | ops/s | blocks/s | point p50 µs | p95 µs | p99 µs | page p50 ms | p95 ms | server cpu ms | server disk MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-m4-node | scan | warm | 1 | baseline | 0 | 0 | 35939 | 0 | 0 | 0 | 0.00 | 0.00 | 2311 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | baseline | 0 | 0 | 34730 | 0 | 0 | 0 | 0.00 | 0.00 | 2365 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | baseline | 0 | 6984 | 6984 | 95 | 361 | 797 | 0.00 | 0.00 | 2021 | 2.6 |
| 2026-09-09-m4-node | point-local | warm | 1 | baseline | 0 | 7191 | 7191 | 92 | 346 | 702 | 0.00 | 0.00 | 1959 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | baseline | 0 | 137 | 13690 | 0 | 0 | 0 | 6.34 | 17.89 | 6221 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | baseline | 0 | 25906 | 25906 | 217 | 719 | 1487 | 0.00 | 0.00 | 3661 | 0.1 |
| 2026-09-09-m4-node | point-local | warm | 8 | baseline | 0 | 26588 | 26588 | 212 | 685 | 1391 | 0.00 | 0.00 | 3612 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | baseline | 0 | 447 | 44687 | 0 | 0 | 0 | 13.34 | 40.53 | 11685 | 0.1 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | baseline | 0 | 3985 | 42452 | 282 | 1190 | 3142 | 11.76 | 37.39 | 26918 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | baseline | 0 | 915 | 46133 | 434 | 3592 | 6939 | 12.82 | 38.37 | 122539 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | baseline | 0 | 513 | 46121 | 616 | 5022 | 8167 | 13.31 | 40.47 | 217499 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | baseline | 0 | 4294 | 4294 | 209 | 503 | 953 | 0.00 | 0.00 | 2006 | 248.8 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | baseline | 0 | 4373 | 4373 | 207 | 496 | 881 | 0.00 | 0.00 | 1965 | 245.1 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | baseline | 0 | 133 | 13257 | 0 | 0 | 0 | 6.56 | 18.28 | 6209 | 441.2 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | baseline | 0 | 23511 | 23511 | 277 | 735 | 1377 | 0.00 | 0.00 | 3209 | 249.8 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | baseline | 0 | 23581 | 23581 | 272 | 748 | 1406 | 0.00 | 0.00 | 3330 | 245.2 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | baseline | 0 | 472 | 47173 | 0 | 0 | 0 | 12.96 | 36.21 | 10800 | 382.5 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | baseline | 0 | 4070 | 43364 | 334 | 1229 | 2798 | 11.26 | 35.52 | 25231 | 564.5 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | baseline | 0 | 927 | 46756 | 463 | 3555 | 7016 | 12.69 | 37.62 | 120727 | 675.3 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | baseline | 0 | 521 | 46847 | 639 | 4760 | 7426 | 13.21 | 39.78 | 214947 | 690.3 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate | 0 | 0 | 32179 | 0 | 0 | 0 | 0.00 | 0.00 | 2585 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate | 0 | 0 | 30840 | 0 | 0 | 0 | 0.00 | 0.00 | 2686 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate | 0 | 6782 | 6782 | 99 | 368 | 824 | 0.00 | 0.00 | 2072 | 2.0 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate | 0 | 7089 | 7089 | 92 | 358 | 724 | 0.00 | 0.00 | 2013 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate | 0 | 132 | 13195 | 0 | 0 | 0 | 6.58 | 18.22 | 6494 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate | 0 | 26532 | 26532 | 219 | 700 | 1425 | 0.00 | 0.00 | 3721 | 0.0 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate | 0 | 25863 | 25863 | 218 | 729 | 1481 | 0.00 | 0.00 | 3719 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate | 0 | 454 | 45354 | 0 | 0 | 0 | 13.18 | 40.14 | 12029 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate | 0 | 3881 | 41352 | 284 | 1257 | 3312 | 12.08 | 37.62 | 28309 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate | 0 | 884 | 44563 | 442 | 3674 | 7180 | 13.39 | 39.35 | 127904 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate | 0 | 500 | 44971 | 612 | 4702 | 8872 | 13.78 | 40.93 | 224563 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate | 0 | 4347 | 4347 | 204 | 515 | 969 | 0.00 | 0.00 | 2046 | 183.2 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate | 0 | 4522 | 4522 | 194 | 489 | 920 | 0.00 | 0.00 | 1981 | 181.7 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate | 0 | 128 | 12783 | 0 | 0 | 0 | 6.72 | 18.86 | 6510 | 245.2 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate | 0 | 23160 | 23160 | 270 | 744 | 1493 | 0.00 | 0.00 | 3350 | 189.5 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate | 0 | 24555 | 24555 | 252 | 745 | 1447 | 0.00 | 0.00 | 3303 | 179.4 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate | 0 | 458 | 45754 | 0 | 0 | 0 | 12.98 | 37.88 | 11267 | 211.3 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate | 0 | 4108 | 43770 | 323 | 1168 | 2810 | 11.21 | 34.34 | 26059 | 310.6 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate | 0 | 892 | 44987 | 471 | 3738 | 7619 | 13.16 | 39.26 | 125090 | 365.3 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate | 0 | 513 | 46062 | 658 | 4747 | 8634 | 13.43 | 40.08 | 219176 | 365.7 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate-zstd1 | 0 | 0 | 31211 | 0 | 0 | 0 | 0.00 | 0.00 | 2664 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate-zstd1 | 0 | 0 | 30721 | 0 | 0 | 0 | 0.00 | 0.00 | 2699 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate-zstd1 | 0 | 6566 | 6566 | 104 | 375 | 825 | 0.00 | 0.00 | 2097 | 2.5 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate-zstd1 | 0 | 6971 | 6971 | 95 | 363 | 726 | 0.00 | 0.00 | 2034 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate-zstd1 | 0 | 130 | 13010 | 0 | 0 | 0 | 6.74 | 18.30 | 6588 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate-zstd1 | 0 | 25536 | 25536 | 223 | 755 | 1481 | 0.00 | 0.00 | 3810 | 0.1 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate-zstd1 | 0 | 26885 | 26885 | 212 | 693 | 1401 | 0.00 | 0.00 | 3638 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate-zstd1 | 0 | 459 | 45811 | 0 | 0 | 0 | 13.07 | 39.71 | 12050 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate-zstd1 | 0 | 3871 | 41237 | 290 | 1207 | 3047 | 12.18 | 38.44 | 27888 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate-zstd1 | 0 | 890 | 44877 | 441 | 3635 | 7131 | 13.41 | 39.49 | 126541 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate-zstd1 | 0 | 504 | 45307 | 591 | 5100 | 8733 | 13.75 | 41.12 | 224240 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate-zstd1 | 0 | 4245 | 4245 | 213 | 519 | 1037 | 0.00 | 0.00 | 2044 | 198.1 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate-zstd1 | 0 | 4250 | 4250 | 212 | 518 | 1020 | 0.00 | 0.00 | 2019 | 204.4 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate-zstd1 | 0 | 127 | 12661 | 0 | 0 | 0 | 6.74 | 18.63 | 6527 | 280.4 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate-zstd1 | 0 | 23578 | 23578 | 275 | 752 | 1440 | 0.00 | 0.00 | 3443 | 199.2 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate-zstd1 | 0 | 22176 | 22176 | 276 | 834 | 1625 | 0.00 | 0.00 | 3688 | 194.1 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate-zstd1 | 0 | 473 | 47230 | 0 | 0 | 0 | 13.11 | 36.90 | 11256 | 245.6 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate-zstd1 | 0 | 4069 | 43353 | 326 | 1174 | 2736 | 11.39 | 35.36 | 26042 | 362.1 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate-zstd1 | 0 | 902 | 45461 | 464 | 3523 | 7213 | 13.26 | 39.12 | 125542 | 426.4 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate-zstd1 | 0 | 510 | 45844 | 609 | 5120 | 8618 | 13.73 | 40.40 | 223101 | 426.4 |
| 2026-09-09-m4-node | scan | warm | 1 | baseline | 1 | 0 | 35497 | 0 | 0 | 0 | 0.00 | 0.00 | 2335 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | baseline | 1 | 0 | 34761 | 0 | 0 | 0 | 0.00 | 0.00 | 2382 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | baseline | 1 | 6940 | 6940 | 96 | 361 | 794 | 0.00 | 0.00 | 2020 | 2.7 |
| 2026-09-09-m4-node | point-local | warm | 1 | baseline | 1 | 7142 | 7142 | 92 | 353 | 722 | 0.00 | 0.00 | 1979 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | baseline | 1 | 136 | 13640 | 0 | 0 | 0 | 6.41 | 17.45 | 6248 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | baseline | 1 | 26663 | 26663 | 216 | 698 | 1372 | 0.00 | 0.00 | 3698 | 0.1 |
| 2026-09-09-m4-node | point-local | warm | 8 | baseline | 1 | 27488 | 27488 | 213 | 671 | 1340 | 0.00 | 0.00 | 3546 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | baseline | 1 | 467 | 46617 | 0 | 0 | 0 | 12.65 | 38.80 | 11646 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | baseline | 1 | 4037 | 43015 | 273 | 1160 | 3353 | 11.26 | 35.23 | 26752 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | baseline | 1 | 913 | 46035 | 439 | 3674 | 7078 | 12.77 | 38.76 | 122844 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | baseline | 1 | 520 | 46698 | 579 | 4714 | 8204 | 13.25 | 39.29 | 216106 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | baseline | 1 | 4204 | 4204 | 220 | 512 | 987 | 0.00 | 0.00 | 1991 | 248.8 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | baseline | 1 | 4325 | 4325 | 211 | 500 | 867 | 0.00 | 0.00 | 1944 | 245.2 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | baseline | 1 | 133 | 13317 | 0 | 0 | 0 | 6.50 | 17.91 | 6206 | 441.5 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | baseline | 1 | 23147 | 23147 | 275 | 752 | 1481 | 0.00 | 0.00 | 3317 | 249.8 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | baseline | 1 | 23937 | 23937 | 271 | 734 | 1439 | 0.00 | 0.00 | 3145 | 243.9 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | baseline | 1 | 467 | 46619 | 0 | 0 | 0 | 12.86 | 37.65 | 11077 | 382.9 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | baseline | 1 | 4134 | 44046 | 324 | 1189 | 2798 | 11.30 | 34.73 | 25260 | 564.6 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | baseline | 1 | 933 | 47042 | 450 | 3312 | 6672 | 12.60 | 37.52 | 120119 | 688.5 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | baseline | 1 | 526 | 47244 | 623 | 4461 | 7999 | 13.07 | 39.19 | 213875 | 681.3 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate | 1 | 0 | 31604 | 0 | 0 | 0 | 0.00 | 0.00 | 2626 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate | 1 | 0 | 32144 | 0 | 0 | 0 | 0.00 | 0.00 | 2586 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate | 1 | 6782 | 6782 | 98 | 370 | 827 | 0.00 | 0.00 | 2071 | 2.6 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate | 1 | 7150 | 7150 | 90 | 359 | 723 | 0.00 | 0.00 | 2010 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate | 1 | 130 | 13042 | 0 | 0 | 0 | 6.66 | 18.50 | 6557 | 0.2 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate | 1 | 26571 | 26571 | 217 | 705 | 1404 | 0.00 | 0.00 | 3740 | 0.1 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate | 1 | 25029 | 25029 | 226 | 697 | 1398 | 0.00 | 0.00 | 3411 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate | 1 | 455 | 45421 | 0 | 0 | 0 | 13.04 | 38.31 | 12124 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate | 1 | 3844 | 40957 | 279 | 1190 | 2994 | 12.48 | 38.63 | 28481 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate | 1 | 887 | 44723 | 443 | 3523 | 6828 | 13.26 | 39.78 | 128011 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate | 1 | 501 | 44974 | 613 | 5083 | 8114 | 13.88 | 40.96 | 227196 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate | 1 | 4411 | 4411 | 203 | 496 | 953 | 0.00 | 0.00 | 2029 | 183.2 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate | 1 | 4464 | 4464 | 197 | 498 | 899 | 0.00 | 0.00 | 2019 | 181.8 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate | 1 | 129 | 12948 | 0 | 0 | 0 | 6.61 | 18.53 | 6464 | 245.7 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate | 1 | 22185 | 22185 | 284 | 813 | 1572 | 0.00 | 0.00 | 3718 | 184.4 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate | 1 | 24138 | 24138 | 259 | 735 | 1403 | 0.00 | 0.00 | 3401 | 176.9 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate | 1 | 471 | 47056 | 0 | 0 | 0 | 12.64 | 38.57 | 11251 | 212.6 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate | 1 | 4014 | 42766 | 323 | 1247 | 3035 | 11.45 | 35.23 | 26061 | 310.3 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate | 1 | 897 | 45216 | 460 | 3631 | 6799 | 13.23 | 38.90 | 126056 | 365.7 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate | 1 | 508 | 45653 | 606 | 5501 | 7995 | 13.69 | 40.83 | 224415 | 360.4 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate-zstd1 | 1 | 0 | 31626 | 0 | 0 | 0 | 0.00 | 0.00 | 2625 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate-zstd1 | 1 | 0 | 30848 | 0 | 0 | 0 | 0.00 | 0.00 | 2674 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate-zstd1 | 1 | 6819 | 6819 | 96 | 371 | 831 | 0.00 | 0.00 | 2073 | 3.2 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate-zstd1 | 1 | 7039 | 7039 | 92 | 361 | 732 | 0.00 | 0.00 | 2028 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate-zstd1 | 1 | 131 | 13096 | 0 | 0 | 0 | 6.62 | 18.24 | 6545 | 0.1 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate-zstd1 | 1 | 26165 | 26165 | 218 | 725 | 1425 | 0.00 | 0.00 | 3828 | 0.1 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate-zstd1 | 1 | 27225 | 27225 | 211 | 685 | 1381 | 0.00 | 0.00 | 3654 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate-zstd1 | 1 | 442 | 44154 | 0 | 0 | 0 | 13.98 | 38.86 | 12119 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate-zstd1 | 1 | 3876 | 41291 | 289 | 1211 | 3338 | 12.30 | 37.03 | 27863 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate-zstd1 | 1 | 887 | 44703 | 442 | 3664 | 7107 | 13.43 | 39.39 | 127507 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate-zstd1 | 1 | 505 | 45390 | 619 | 4784 | 8012 | 13.66 | 40.70 | 225489 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate-zstd1 | 1 | 4283 | 4283 | 213 | 518 | 978 | 0.00 | 0.00 | 2055 | 198.2 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate-zstd1 | 1 | 4385 | 4385 | 204 | 504 | 903 | 0.00 | 0.00 | 2017 | 196.5 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate-zstd1 | 1 | 129 | 12851 | 0 | 0 | 0 | 6.65 | 18.40 | 6510 | 280.3 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate-zstd1 | 1 | 23906 | 23906 | 271 | 728 | 1361 | 0.00 | 0.00 | 3308 | 199.1 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate-zstd1 | 1 | 24582 | 24582 | 257 | 729 | 1369 | 0.00 | 0.00 | 3323 | 194.4 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate-zstd1 | 1 | 461 | 46060 | 0 | 0 | 0 | 13.24 | 37.52 | 11422 | 246.7 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate-zstd1 | 1 | 4015 | 42775 | 324 | 1195 | 2959 | 11.58 | 35.65 | 25925 | 362.6 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate-zstd1 | 1 | 902 | 45460 | 458 | 3514 | 6750 | 13.09 | 39.26 | 125390 | 429.7 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate-zstd1 | 1 | 506 | 45472 | 594 | 4932 | 8511 | 13.56 | 40.93 | 224465 | 430.6 |
| 2026-09-09-m4-node | scan | warm | 1 | baseline | 2 | 0 | 35636 | 0 | 0 | 0 | 0.00 | 0.00 | 2333 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | baseline | 2 | 0 | 34962 | 0 | 0 | 0 | 0.00 | 0.00 | 2372 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | baseline | 2 | 6965 | 6965 | 94 | 363 | 794 | 0.00 | 0.00 | 2024 | 1.9 |
| 2026-09-09-m4-node | point-local | warm | 1 | baseline | 2 | 7166 | 7166 | 92 | 348 | 701 | 0.00 | 0.00 | 1973 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 1 | baseline | 2 | 135 | 13464 | 0 | 0 | 0 | 6.46 | 18.12 | 6332 | 0.1 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | baseline | 2 | 26230 | 26230 | 217 | 699 | 1419 | 0.00 | 0.00 | 3687 | 0.0 |
| 2026-09-09-m4-node | point-local | warm | 8 | baseline | 2 | 26956 | 26956 | 211 | 689 | 1404 | 0.00 | 0.00 | 3584 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 8 | baseline | 2 | 461 | 46068 | 0 | 0 | 0 | 12.73 | 37.00 | 11660 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | baseline | 2 | 3901 | 41562 | 281 | 1170 | 3144 | 11.98 | 39.19 | 27669 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | baseline | 2 | 901 | 45428 | 441 | 3617 | 7320 | 13.00 | 39.09 | 125127 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | baseline | 2 | 510 | 45818 | 616 | 4588 | 8114 | 13.43 | 39.88 | 220505 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | baseline | 2 | 4312 | 4312 | 209 | 500 | 933 | 0.00 | 0.00 | 1987 | 248.8 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | baseline | 2 | 4255 | 4255 | 220 | 507 | 881 | 0.00 | 0.00 | 1956 | 244.9 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | baseline | 2 | 133 | 13347 | 0 | 0 | 0 | 6.53 | 18.01 | 6205 | 444.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | baseline | 2 | 20974 | 20974 | 302 | 850 | 1648 | 0.00 | 0.00 | 3733 | 249.8 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | baseline | 2 | 24378 | 24378 | 265 | 711 | 1318 | 0.00 | 0.00 | 3196 | 245.1 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | baseline | 2 | 478 | 47730 | 0 | 0 | 0 | 12.60 | 36.04 | 10864 | 383.6 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | baseline | 2 | 4032 | 42955 | 331 | 1198 | 2828 | 11.57 | 36.50 | 25526 | 562.9 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | baseline | 2 | 902 | 45447 | 470 | 3572 | 6840 | 12.78 | 39.45 | 122293 | 694.1 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | baseline | 2 | 519 | 46677 | 601 | 4866 | 7148 | 13.34 | 39.78 | 217410 | 684.7 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate | 2 | 0 | 31981 | 0 | 0 | 0 | 0.00 | 0.00 | 2596 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate | 2 | 0 | 31224 | 0 | 0 | 0 | 0.00 | 0.00 | 2622 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate | 2 | 6771 | 6771 | 98 | 368 | 840 | 0.00 | 0.00 | 2079 | 2.4 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate | 2 | 7090 | 7090 | 92 | 358 | 727 | 0.00 | 0.00 | 2011 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate | 2 | 131 | 13052 | 0 | 0 | 0 | 6.64 | 18.10 | 6566 | 0.2 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate | 2 | 26416 | 26416 | 220 | 712 | 1427 | 0.00 | 0.00 | 3764 | 0.0 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate | 2 | 25034 | 25034 | 225 | 766 | 1599 | 0.00 | 0.00 | 3850 | 0.1 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate | 2 | 447 | 44687 | 0 | 0 | 0 | 13.93 | 38.86 | 12234 | 0.0 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate | 2 | 3895 | 41497 | 279 | 1203 | 3308 | 12.37 | 37.22 | 28082 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate | 2 | 872 | 43982 | 474 | 3744 | 7504 | 13.48 | 39.94 | 127465 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate | 2 | 501 | 44993 | 616 | 5124 | 7975 | 13.91 | 41.25 | 227351 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate | 2 | 4455 | 4455 | 198 | 496 | 973 | 0.00 | 0.00 | 2045 | 183.2 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate | 2 | 4562 | 4562 | 193 | 485 | 895 | 0.00 | 0.00 | 2011 | 181.7 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate | 2 | 129 | 12867 | 0 | 0 | 0 | 6.78 | 18.61 | 6490 | 245.6 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate | 2 | 24251 | 24251 | 267 | 721 | 1362 | 0.00 | 0.00 | 3296 | 184.4 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate | 2 | 23252 | 23252 | 264 | 780 | 1531 | 0.00 | 0.00 | 3483 | 179.2 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate | 2 | 470 | 46942 | 0 | 0 | 0 | 12.77 | 36.50 | 11110 | 211.5 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate | 2 | 4054 | 43195 | 322 | 1164 | 2834 | 11.12 | 33.82 | 25917 | 310.7 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate | 2 | 904 | 45571 | 459 | 3656 | 7082 | 13.07 | 38.57 | 125381 | 364.1 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate | 2 | 499 | 44874 | 649 | 5218 | 9560 | 13.81 | 41.39 | 225218 | 358.4 |
| 2026-09-09-m4-node | scan | warm | 1 | candidate-zstd1 | 2 | 0 | 31403 | 0 | 0 | 0 | 0.00 | 0.00 | 2634 | 0.0 |
| 2026-09-09-m4-node | scan | evict stream | 1 | candidate-zstd1 | 2 | 0 | 30906 | 0 | 0 | 0 | 0.00 | 0.00 | 2679 | 0.0 |
| 2026-09-09-m4-node | point-uniform | warm | 1 | candidate-zstd1 | 2 | 6645 | 6645 | 102 | 376 | 826 | 0.00 | 0.00 | 2096 | 2.0 |
| 2026-09-09-m4-node | point-local | warm | 1 | candidate-zstd1 | 2 | 7175 | 7175 | 89 | 359 | 730 | 0.00 | 0.00 | 2013 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 1 | candidate-zstd1 | 2 | 131 | 13114 | 0 | 0 | 0 | 6.62 | 17.92 | 6532 | 0.1 |
| 2026-09-09-m4-node | point-uniform | warm | 8 | candidate-zstd1 | 2 | 24660 | 24660 | 221 | 718 | 1632 | 0.00 | 0.00 | 3477 | 0.0 |
| 2026-09-09-m4-node | point-local | warm | 8 | candidate-zstd1 | 2 | 25574 | 25574 | 218 | 707 | 1506 | 0.00 | 0.00 | 3523 | 0.0 |
| 2026-09-09-m4-node | page-100 | warm | 8 | candidate-zstd1 | 2 | 457 | 45651 | 0 | 0 | 0 | 13.39 | 37.65 | 11873 | 0.1 |
| 2026-09-09-m4-node | mix-90-10 | warm | 8 | candidate-zstd1 | 2 | 3897 | 41517 | 296 | 1224 | 3211 | 12.14 | 37.55 | 27664 | 0.0 |
| 2026-09-09-m4-node | mix-50-50 | warm | 8 | candidate-zstd1 | 2 | 886 | 44685 | 449 | 3555 | 6808 | 13.37 | 39.68 | 126672 | 0.0 |
| 2026-09-09-m4-node | mix-10-90 | warm | 8 | candidate-zstd1 | 2 | 501 | 45039 | 617 | 4944 | 7942 | 13.87 | 41.03 | 224695 | 0.0 |
| 2026-09-09-m4-node | point-uniform | evict stream | 1 | candidate-zstd1 | 2 | 4353 | 4353 | 203 | 508 | 968 | 0.00 | 0.00 | 2042 | 198.1 |
| 2026-09-09-m4-node | point-local | evict stream | 1 | candidate-zstd1 | 2 | 4400 | 4400 | 203 | 499 | 971 | 0.00 | 0.00 | 1995 | 196.4 |
| 2026-09-09-m4-node | page-100 | evict stream | 1 | candidate-zstd1 | 2 | 128 | 12834 | 0 | 0 | 0 | 6.80 | 18.53 | 6476 | 284.4 |
| 2026-09-09-m4-node | point-uniform | evict stream | 8 | candidate-zstd1 | 2 | 21519 | 21519 | 289 | 839 | 1669 | 0.00 | 0.00 | 3707 | 199.3 |
| 2026-09-09-m4-node | point-local | evict stream | 8 | candidate-zstd1 | 2 | 23329 | 23329 | 266 | 780 | 1583 | 0.00 | 0.00 | 3475 | 194.7 |
| 2026-09-09-m4-node | page-100 | evict stream | 8 | candidate-zstd1 | 2 | 472 | 47190 | 0 | 0 | 0 | 12.59 | 36.63 | 11073 | 248.3 |
| 2026-09-09-m4-node | mix-90-10 | evict stream | 8 | candidate-zstd1 | 2 | 3963 | 42224 | 339 | 1276 | 2953 | 11.68 | 35.75 | 26034 | 361.9 |
| 2026-09-09-m4-node | mix-50-50 | evict stream | 8 | candidate-zstd1 | 2 | 907 | 45722 | 455 | 3455 | 6787 | 12.96 | 38.70 | 124778 | 427.2 |
| 2026-09-09-m4-node | mix-10-90 | evict stream | 8 | candidate-zstd1 | 2 | 508 | 45641 | 624 | 5104 | 8225 | 13.66 | 40.04 | 222947 | 424.4 |

## Node gates (each label against `baseline`; medians over paired repeats within one run name)

| run | workload | settings | label | throughput vs baseline | point p95 vs baseline | baseline p95 µs | label p95 µs | gate | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-09-m4-node | import-1 | blocks 2000, chunk 1 | candidate | 0.996 | - | - | - | throughput >= 0.9 | PASS |
| 2026-09-09-m4-node | import-1 | blocks 2000, chunk 1 | candidate-zstd1 | 1.001 | - | - | - | throughput >= 0.9 | PASS |
| 2026-09-09-m4-node | import-500 | blocks 126728, chunk 500 | candidate | 0.609 | - | - | - | throughput >= 0.9 | FAIL |
| 2026-09-09-m4-node | import-500 | blocks 126728, chunk 500 | candidate-zstd1 | 0.767 | - | - | - | throughput >= 0.9 | FAIL |
| 2026-09-09-m4-node | mix-10-90 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | candidate | 0.975 | 1.096 | 4760 | 5218 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-10-90 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.974 | 1.072 | 4760 | 5104 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-10-90 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | candidate | 0.975 | 1.078 | 4714 | 5083 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-10-90 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.1, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.982 | 1.049 | 4714 | 4944 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-50-50 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | candidate | 0.967 | 1.028 | 3555 | 3656 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-50-50 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.972 | 0.988 | 3555 | 3514 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-50-50 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | candidate | 0.968 | 1.016 | 3617 | 3674 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-50-50 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.5, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.971 | 1.005 | 3617 | 3635 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-90-10 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | candidate | 0.996 | 0.975 | 1198 | 1168 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-90-10 [evict t8] | cache stream, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.986 | 0.997 | 1198 | 1195 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-90-10 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | candidate | 0.974 | 1.028 | 1170 | 1203 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | mix-90-10 [warm t8] | cache primed, mix (locality 0.5, page_len 100, point_share 0.9, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.973 | 1.035 | 1170 | 1211 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | page-100 [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate | 0.966 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate-zstd1 | 0.964 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate | 0.995 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate-zstd1 | 1.000 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate | 0.957 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate-zstd1 | 0.960 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate | 0.984 | - | - | - | - | - |
| 2026-09-09-m4-node | page-100 [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 0.0, window 512), ops 1000, scan false, seed 0 | candidate-zstd1 | 0.991 | - | - | - | - | - |
| 2026-09-09-m4-node | point-local [evict t1] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 1.046 | 0.977 | 500 | 489 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [evict t1] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 1.014 | 1.008 | 500 | 504 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [evict t8] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 1.008 | 1.015 | 734 | 745 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [evict t8] | cache stream, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.975 | 1.064 | 734 | 780 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [warm t1] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 0.989 | 1.029 | 348 | 358 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [warm t1] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.982 | 1.037 | 348 | 361 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [warm t8] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 0.929 | 1.064 | 685 | 729 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-local [warm t8] | cache primed, mix (locality 0.8, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.997 | 1.012 | 685 | 693 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 1.027 | 0.986 | 503 | 496 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.998 | 1.029 | 503 | 518 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 1.001 | 0.989 | 752 | 744 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [evict t8] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 1.019 | 1.000 | 752 | 752 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 0.974 | 1.019 | 361 | 368 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.954 | 1.038 | 361 | 375 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate | 1.012 | 1.008 | 699 | 705 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | point-uniform [warm t8] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 20000, scan false, seed 0 | candidate-zstd1 | 0.974 | 1.037 | 699 | 725 | point p95 <= 1.1 | PASS |
| 2026-09-09-m4-node | scan [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 1, scan true, seed 0 | candidate | 0.898 | - | - | - | - | - |
| 2026-09-09-m4-node | scan [evict t1] | cache stream, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 1, scan true, seed 0 | candidate-zstd1 | 0.887 | - | - | - | - | - |
| 2026-09-09-m4-node | scan [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 1, scan true, seed 0 | candidate | 0.897 | - | - | - | - | - |
| 2026-09-09-m4-node | scan [warm t1] | cache primed, mix (locality 0.0, page_len 100, point_share 1.0, window 512), ops 1, scan true, seed 0 | candidate-zstd1 | 0.881 | - | - | - | - | - |

