# utxorpc-client

Examples of using the [UTxORPC](https://utxorpc.org) (U5C) client SDK against a
local [Dolos](https://github.com/txpipe/dolos) instance.

The first topic covered here is the **WatchTx** endpoint: a server-side–filtered
live stream of transactions as they reach the tip of the chain.

## Prerequisites

A Dolos instance serving its UTxORPC gRPC endpoint at `http://localhost:50051`
(override with the `URL` env var). Then:

```bash
cd examples/utxorpc-client
npm install
```

## WatchTx examples

Each script opens a stream, applies a filter **on the server**, and prints
matches as they arrive. Every match is rendered the same way: a full,
color-coded, multi-line breakdown of the transaction — hash, fee, inputs (with
their resolved outputs when the node provides them), outputs with ADA and native
assets, mint/burn, and certificates. Long hashes, addresses, and policy ids are
truncated for readability.

Between matches you'll see dim `💤 idle @ slot N` heartbeats — those are blocks
with no match, and they prove the stream is alive (Cardano blocks are ~20s apart
on average). Output is paced to one event per second so it stays readable. Press
Ctrl-C to stop.

Every script ships with a **default filter value that is known to match**, so
`npm run watch:*` produces output with no arguments. Override any of them with
env vars (`ADDRESS`, `POLICY_ID`, `ASSET_NAME`, `NOISE`).

| Script | Filters on | Run (defaults work as-is) |
| --- | --- | --- |
| `watch:inspect` | nothing (firehose) — full payload decode | `npm run watch:inspect` |
| `watch:address` | exact address (`hasAddress.exactAddress`) | `npm run watch:address` |
| `watch:payment` | payment credential (`hasAddress.paymentPart`) | `npm run watch:payment` |
| `watch:asset` | native-asset policy (`movesAsset`) | `npm run watch:asset` |
| `watch:composed` | boolean composition of the above | `npm run watch:composed` |

### Output coloring

Colors are applied via [`picocolors`](https://github.com/alexeyraspopov/picocolors)
and auto-disable when stdout isn't a TTY or `NO_COLOR` is set, so piping output
to a file or `cat` yields plain text with no stray escape codes.

| Concept | Color |
| --- | --- |
| tx hash | bold cyan |
| address | magenta |
| ADA amount | yellow |
| native asset | blue |
| fee | dim |
| `Tx Match:` badge | green |
| `UNDO` badge | yellow |
| `🛑 FAILED` badge | red |
| idle heartbeat | dim |

### Fixed replay point

These examples target **mainnet** and, rather than waiting at the live tip,
replay forward from a **fixed intersection point** (a real block defined as
`FIXED_INTERSECT` in `watch-tx/shared.ts`). Combined with the baked-in default
filter values — all observed at or after that block — every example produces
matches within seconds.

Control the start point with env vars:

- `SLOT=<n> HASH=<hex> npm run watch:asset` — replay from a different block.
- `NO_INTERSECT=1 npm run watch:asset` — stream live from the current tip instead.

The fixed block must still be within the history your Dolos instance retains; if
it has been pruned past it, set your own `SLOT`/`HASH` (or `NO_INTERSECT=1`).

### Start with `inspect`

`npm run watch:inspect` streams **every** transaction and prints a full
breakdown — hash, fee, inputs (with their resolved outputs when available),
outputs with ADA and native assets, and any mint/burn. It's also the easiest way
to discover a real address or policy id to feed the filtering examples: run it,
then copy a value out of the output.

### Filtering by pattern

- **`watch:address`** — matches a tx if the exact address appears in any input
  or output.
- **`watch:payment`** — matches on just the *payment credential*, so it catches
  every address that shares the same payment key (base, enterprise, and every
  stake-delegation variant). Use this to follow "a wallet" rather than one
  address string.
- **`watch:asset`** — matches any tx moving a given policy. Add `ASSET_NAME=<hex>`
  to pin a single asset.
- **`watch:composed`** — uses `watchTxByPredicate` to build
  `(touches ADDRESS OR moves POLICY_ID) AND NOT (touches NOISE)`, showing how the
  `anyOf` / `allOf` / `not` operators compose the single-pattern filters.

## Notes

- Filtering happens **server-side** — Dolos evaluates the predicate against each
  tx and only streams matches, so the client stays cheap even on a busy chain.
- All byte inputs (addresses, policy ids) go to the SDK as raw bytes; the helpers
  in `watch-tx/shared.ts` handle bech32 ↔ bytes conversion and the decoding of
  protobuf-wrapped integers (lovelace / asset quantities).
- Each watch method takes an optional `intersect` (a `{ slot, hash }` block ref)
  as its last argument; `watch-tx/shared.ts` centralises this as `startPoint()`
  (see the fixed replay point above). Pass `NO_INTERSECT=1` to start at the tip.
- The shared `printTx` function in `watch-tx/shared.ts` renders every matched tx
  the same way regardless of which filter produced it, so all examples show the
  same level of detail.
