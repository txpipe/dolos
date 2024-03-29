# Long-running operation - October test

A "long-running" test refers to a type of test that is designed to evaluate the system's behavior under sustained use over a long period of time. These tests can be used to identify problems that might not be apparent in short-term testing scenarios.

Among other things, long-running tests are often used to identify memory leaks, resource leaks, or degradation in system performance over time. If a system runs perfectly for an hour but starts having issues after several hours or days, a long-running test would help identify such problems.

In particular, we want to measure CPU and memory usages of Dolos compared to the Haskell node. The main goal of Dolos is to provide a lightweight alternative (with substantially reduced features) for data access use-cases. To perform the comparison, both implementations were hosted under the equivalent conditions:

- same hardware
- both fully-synced to the Preview network
- similar client request profile (1 chain-sync consumer)

## CPU Usage

In this analysis we compare CPU usage. It is expressed as shares of a vCPU (~core). 1 share represents 1/1000 of a vCPU. Each bucket represent the average of shares utilized by each process in a 15-minute period. The information was gathered after continuous operations throughout a 24 hr period.

| Time                | Dolos | Haskell |
| ------------------- | ----- | ------- |
| 2023-10-19 14:45:00 | 26.9  | 302     |
| 2023-10-19 15:00:00 | 21.8  | 287     |
| 2023-10-19 15:15:00 | 27.8  | 309     |
| 2023-10-19 15:30:00 | 28.2  | 303     |
| 2023-10-19 15:45:00 | 29.0  | 301     |
| 2023-10-19 16:00:00 | 29.3  | 221     |
| 2023-10-19 16:15:00 | 30.5  | 327     |
| 2023-10-19 16:30:00 | 26.5  | 302     |
| 2023-10-19 16:45:00 | 26.9  | 300     |
| 2023-10-19 17:00:00 | 28.5  | 307     |
| 2023-10-19 17:15:00 | 26.1  | 300     |
| 2023-10-19 17:30:00 | 30.8  | 311     |
| 2023-10-19 17:45:00 | 22.9  | 285     |
| 2023-10-19 18:00:00 | 24.0  | 290     |
| 2023-10-19 18:15:00 | 26.5  | 303     |
| 2023-10-19 18:30:00 | 22.7  | 287     |
| 2023-10-19 18:45:00 | 22.7  | 293     |
| 2023-10-19 19:00:00 | 25.7  | 297     |
| 2023-10-19 19:15:00 | 21.0  | 291     |
| 2023-10-19 19:30:00 | 23.0  | 301     |
| 2023-10-19 19:45:00 | 22.5  | 297     |
| 2023-10-19 20:00:00 | 23.4  | 307     |
| 2023-10-19 20:15:00 | 25.7  | 302     |
| 2023-10-19 20:30:00 | 25.0  | 306     |
| 2023-10-19 20:45:00 | 25.6  | 302     |
| 2023-10-19 21:00:00 | 25.4  | 297     |
| 2023-10-19 21:15:00 | 23.5  | 300     |
| 2023-10-19 21:30:00 | 25.4  | 301     |
| 2023-10-19 21:45:00 | 22.2  | 303     |
| 2023-10-19 22:00:00 | 23.9  | 298     |
| 2023-10-19 22:15:00 | 26.7  | 296     |
| 2023-10-19 22:30:00 | 22.7  | 290     |
| 2023-10-19 22:45:00 | 25.5  | 300     |
| 2023-10-19 23:00:00 | 22.6  | 300     |
| 2023-10-19 23:15:00 | 22.8  | 290     |
| 2023-10-19 23:30:00 | 25.4  | 294     |
| 2023-10-19 23:45:00 | 23.6  | 301     |
| 2023-10-20 00:00:00 | 24.0  | 295     |
| 2023-10-20 00:15:00 | 21.9  | 301     |
| 2023-10-20 00:30:00 | 22.7  | 290     |
| 2023-10-20 00:45:00 | 23.9  | 302     |
| 2023-10-20 01:00:00 | 23.7  | 303     |
| 2023-10-20 01:15:00 | 23.4  | 288     |
| 2023-10-20 01:30:00 | 23.7  | 297     |
| 2023-10-20 01:45:00 | 23.5  | 296     |
| 2023-10-20 02:00:00 | 27.2  | 303     |
| 2023-10-20 02:15:00 | 22.2  | 290     |
| 2023-10-20 02:30:00 | 23.9  | 291     |
| 2023-10-20 02:45:00 | 23.2  | 289     |
| 2023-10-20 03:00:00 | 25.8  | 307     |
| 2023-10-20 03:15:00 | 26.3  | 290     |
| 2023-10-20 03:30:00 | 25.3  | 300     |
| 2023-10-20 03:45:00 | 22.0  | 290     |
| 2023-10-20 04:00:00 | 26.6  | 299     |
| 2023-10-20 04:15:00 | 26.4  | 301     |
| 2023-10-20 04:30:00 | 27.0  | 293     |
| 2023-10-20 04:45:00 | 26.8  | 302     |
| 2023-10-20 05:00:00 | 31.1  | 309     |
| 2023-10-20 05:15:00 | 24.7  | 290     |
| 2023-10-20 05:30:00 | 28.1  | 304     |
| 2023-10-20 05:45:00 | 24.1  | 282     |
| 2023-10-20 06:00:00 | 29.3  | 308     |
| 2023-10-20 06:15:00 | 25.3  | 298     |
| 2023-10-20 06:30:00 | 24.3  | 287     |
| 2023-10-20 06:45:00 | 32.2  | 317     |
| 2023-10-20 07:00:00 | 26.8  | 288     |
| 2023-10-20 07:15:00 | 27.0  | 295     |
| 2023-10-20 07:30:00 | 30.3  | 306     |
| 2023-10-20 07:45:00 | 28.6  | 300     |
| 2023-10-20 08:00:00 | 32.2  | 316     |
| 2023-10-20 08:15:00 | 32.4  | 301     |
| 2023-10-20 08:30:00 | 31.0  | 302     |
| 2023-10-20 08:45:00 | 41.5  | 310     |
| 2023-10-20 09:00:00 | 22.9  | 285     |
| 2023-10-20 09:15:00 | 31.8  | 302     |
| 2023-10-20 09:30:00 | 28.5  | 285     |
| 2023-10-20 09:45:00 | 31.0  | 299     |
| 2023-10-20 10:00:00 | 26.1  | 285     |
| 2023-10-20 10:15:00 | 32.2  | 309     |
| 2023-10-20 10:30:00 | 26.8  | 296     |
| 2023-10-20 10:45:00 | 25.3  | 285     |
| 2023-10-20 11:00:00 | 33.7  | 306     |
| 2023-10-20 11:15:00 | 31.0  | 303     |
| 2023-10-20 11:30:00 | 30.9  | 293     |
| 2023-10-20 11:45:00 | 30.1  | 308     |
| 2023-10-20 12:00:00 | 32.8  | 299     |
| 2023-10-20 12:15:00 | 23.9  | 293     |
| 2023-10-20 12:30:00 | 29.3  | 302     |
| 2023-10-20 12:45:00 | 32.7  | 299     |
| 2023-10-20 13:00:00 | 28.7  | 304     |
| 2023-10-20 13:15:00 | 32.9  | 307     |
| 2023-10-20 13:30:00 | 37.5  | 310     |
| 2023-10-20 13:45:00 | 32.0  | 309     |
| 2023-10-20 14:00:00 | 32.2  | 298     |
| 2023-10-20 14:15:00 | 33.1  | 309     |
| 2023-10-20 14:30:00 | 34.2  | 313     |
| 2023-10-20 14:45:00 | 35.7  | 308     |



## Memory Usage

In this analysis we compare memory usage. It is expressed as total amount of data (KB, MB, GB). Each bucket represent the average of memory utilized by each process in a 15-minute period. The information was gathered after continuous operations throughout a 24 hr period.

| Time                | Dolos   | Haskell |
| ------------------- | ------- | ------- |
| 2023-10-19 14:45:00 | 62.3 MB | 2.44 GB |
| 2023-10-19 15:00:00 | 63.2 MB | 2.44 GB |
| 2023-10-19 15:15:00 | 61.6 MB | 2.44 GB |
| 2023-10-19 15:30:00 | 62.5 MB | 2.44 GB |
| 2023-10-19 15:45:00 | 63.3 MB | 2.44 GB |
| 2023-10-19 16:00:00 | 63.0 MB | 2.44 GB |
| 2023-10-19 16:15:00 | 63.9 MB | 2.44 GB |
| 2023-10-19 16:30:00 | 64.4 MB | 2.44 GB |
| 2023-10-19 16:45:00 | 64.8 MB | 2.44 GB |
| 2023-10-19 17:00:00 | 64.9 MB | 2.44 GB |
| 2023-10-19 17:15:00 | 65.2 MB | 2.44 GB |
| 2023-10-19 17:30:00 | 65.8 MB | 2.44 GB |
| 2023-10-19 17:45:00 | 66.0 MB | 2.44 GB |
| 2023-10-19 18:00:00 | 66.1 MB | 2.44 GB |
| 2023-10-19 18:15:00 | 54.7 MB | 2.44 GB |
| 2023-10-19 18:30:00 | 55.4 MB | 2.44 GB |
| 2023-10-19 18:45:00 | 55.9 MB | 2.44 GB |
| 2023-10-19 19:00:00 | 57.0 MB | 2.44 GB |
| 2023-10-19 19:15:00 | 57.3 MB | 2.44 GB |
| 2023-10-19 19:30:00 | 57.6 MB | 2.44 GB |
| 2023-10-19 19:45:00 | 57.9 MB | 2.44 GB |
| 2023-10-19 20:00:00 | 58.4 MB | 2.44 GB |
| 2023-10-19 20:15:00 | 58.8 MB | 2.44 GB |
| 2023-10-19 20:30:00 | 59.0 MB | 2.44 GB |
| 2023-10-19 20:45:00 | 59.4 MB | 2.44 GB |
| 2023-10-19 21:00:00 | 59.6 MB | 2.44 GB |
| 2023-10-19 21:15:00 | 60.0 MB | 2.44 GB |
| 2023-10-19 21:30:00 | 60.2 MB | 2.44 GB |
| 2023-10-19 21:45:00 | 60.5 MB | 2.44 GB |
| 2023-10-19 22:00:00 | 61.0 MB | 2.44 GB |
| 2023-10-19 22:15:00 | 61.1 MB | 2.44 GB |
| 2023-10-19 22:30:00 | 61.5 MB | 2.44 GB |
| 2023-10-19 22:45:00 | 62.0 MB | 2.44 GB |
| 2023-10-19 23:00:00 | 62.3 MB | 2.44 GB |
| 2023-10-19 23:15:00 | 62.6 MB | 2.44 GB |
| 2023-10-19 23:30:00 | 62.8 MB | 2.44 GB |
| 2023-10-19 23:45:00 | 63.2 MB | 2.44 GB |
| 2023-10-20 00:00:00 | 63.4 MB | 2.44 GB |
| 2023-10-20 00:15:00 | 63.8 MB | 2.44 GB |
| 2023-10-20 00:30:00 | 64.3 MB | 2.44 GB |
| 2023-10-20 00:45:00 | 64.6 MB | 2.44 GB |
| 2023-10-20 01:00:00 | 64.9 MB | 2.44 GB |
| 2023-10-20 01:15:00 | 65.2 MB | 2.44 GB |
| 2023-10-20 01:30:00 | 65.4 MB | 2.44 GB |
| 2023-10-20 01:45:00 | 66.0 MB | 2.44 GB |
| 2023-10-20 02:00:00 | 66.4 MB | 2.44 GB |
| 2023-10-20 02:15:00 | 66.7 MB | 2.44 GB |
| 2023-10-20 02:30:00 | 66.9 MB | 2.44 GB |
| 2023-10-20 02:45:00 | 67.3 MB | 2.44 GB |
| 2023-10-20 03:00:00 | 68.0 MB | 2.44 GB |
| 2023-10-20 03:15:00 | 68.5 MB | 2.44 GB |
| 2023-10-20 03:30:00 | 69.0 MB | 2.44 GB |
| 2023-10-20 03:45:00 | 69.2 MB | 2.44 GB |
| 2023-10-20 04:00:00 | 69.4 MB | 2.44 GB |
| 2023-10-20 04:15:00 | 69.6 MB | 2.44 GB |
| 2023-10-20 04:30:00 | 70.1 MB | 2.44 GB |
| 2023-10-20 04:45:00 | 70.5 MB | 2.44 GB |
| 2023-10-20 05:00:00 | 70.8 MB | 2.44 GB |
| 2023-10-20 05:15:00 | 71.2 MB | 2.44 GB |
| 2023-10-20 05:30:00 | 71.5 MB | 2.44 GB |
| 2023-10-20 05:45:00 | 71.9 MB | 2.44 GB |
| 2023-10-20 06:00:00 | 72.2 MB | 2.44 GB |
| 2023-10-20 06:15:00 | 72.6 MB | 2.44 GB |
| 2023-10-20 06:30:00 | 72.7 MB | 2.44 GB |
| 2023-10-20 06:45:00 | 73.0 MB | 2.44 GB |
| 2023-10-20 07:00:00 | 73.3 MB | 2.44 GB |
| 2023-10-20 07:15:00 | 73.6 MB | 2.44 GB |
| 2023-10-20 07:30:00 | 74.0 MB | 2.44 GB |
| 2023-10-20 07:45:00 | 74.4 MB | 2.44 GB |
| 2023-10-20 08:00:00 | 74.9 MB | 2.44 GB |
| 2023-10-20 08:15:00 | 75.4 MB | 2.44 GB |
| 2023-10-20 08:30:00 | 75.9 MB | 2.44 GB |
| 2023-10-20 08:45:00 | 79.5 MB | 2.44 GB |
| 2023-10-20 09:00:00 | 79.6 MB | 2.44 GB |
| 2023-10-20 09:15:00 | 79.9 MB | 2.44 GB |
| 2023-10-20 09:30:00 | 80.3 MB | 2.44 GB |
| 2023-10-20 09:45:00 | 80.9 MB | 2.44 GB |
| 2023-10-20 10:00:00 | 81.3 MB | 2.44 GB |
| 2023-10-20 10:15:00 | 82.0 MB | 2.44 GB |
| 2023-10-20 10:30:00 | 82.4 MB | 2.44 GB |
| 2023-10-20 10:45:00 | 82.8 MB | 2.44 GB |
| 2023-10-20 11:00:00 | 83.3 MB | 2.44 GB |
| 2023-10-20 11:15:00 | 83.4 MB | 2.44 GB |
| 2023-10-20 11:30:00 | 83.9 MB | 2.44 GB |
| 2023-10-20 11:45:00 | 84.2 MB | 2.44 GB |
| 2023-10-20 12:00:00 | 84.7 MB | 2.44 GB |
| 2023-10-20 12:15:00 | 84.8 MB | 2.44 GB |
| 2023-10-20 12:30:00 | 85.5 MB | 2.44 GB |
| 2023-10-20 12:45:00 | 85.8 MB | 2.44 GB |
| 2023-10-20 13:00:00 | 86.1 MB | 2.44 GB |
| 2023-10-20 13:15:00 | 86.9 MB | 2.44 GB |
| 2023-10-20 13:30:00 | 88.1 MB | 2.44 GB |
| 2023-10-20 13:45:00 | 88.4 MB | 2.44 GB |
| 2023-10-20 14:00:00 | 88.6 MB | 2.44 GB |
| 2023-10-20 14:15:00 | 89.0 MB | 2.44 GB |
| 2023-10-20 14:30:00 | 89.2 MB | 2.44 GB |
| 2023-10-20 14:45:00 | 89.4 MB | 2.44 GB |

