# Removing Pallas from `dolos-core` — Decision Record

## Why this change

`dolos-core` is the foundation of the entire dolos stack. It defines the abstract traits (`Domain`, `ChainLogic`, `WalStore`, `StateStore`, etc.) that every crate in the workspace depends on. For it to work with a second blockchain (e.g. Midnight, which uses Substrate/SCALE), it must make zero assumptions about which chain it is running.

Today it depends on the full `pallas` umbrella crate, which pulls in Cardano protocol types, genesis file schemas, and validation error types into what should be a neutral layer. The goal is to remove `pallas` entirely from `dolos-core`'s dependency tree.

**Constraint**: no performance compromise. Every decision below was evaluated against this constraint.

---

## Decision 1 — `EraCbor` renamed to `EraBody`, `minicbor` stays as a direct dep

**What**: `EraCbor(Era, Cbor)` is renamed to `EraBody(Era, Vec<u8>)`. The name `EraCbor` implied the inner bytes are CBOR — they are not, they are opaque (Cardano stores CBOR there today; Midnight would store SCALE bytes). The type alias `Cbor = Vec<u8>` is also removed.

**Why minicbor stays**: `EraBody` retains its `#[derive(minicbor::Encode, minicbor::Decode)]`. This is required because `redb3/mempool.rs` embeds `EraBody` inside `InflightRecord` and `FinalizedEntry`, which are stored in redb using minicbor serialization. Removing the derives would require either reimplementing the encoding in redb3 (coupling) or restructuring the storage format (a breaking change to the on-disk format). The tradeoff is accepted: `minicbor` replaces `pallas` as the direct dep for this one purpose.

**What minicbor does NOT do**: it does not touch the inner bytes. The `#[cbor(with = "minicbor::bytes")]` annotation on the payload field means minicbor treats those bytes as a raw blob — the SCALE or CBOR content inside is completely untouched.

**Performance**: zero impact. The encoding of `(u16, Vec<u8>)` via minicbor is trivial and was already happening.

---

## Decision 2 — `Hash<N>` becomes a custom newtype in core

**What**: A new `crates/core/src/hash.rs` defines `Hash<const N: usize>([u8; N])` (~60 lines) with Display/hex, FromStr, serde, Copy, Eq, Hash, Deref impls. `BlockHash = Hash<32>` and `TxHash = Hash<32>` continue as type aliases. `pallas-crypto` is removed from core.

**Why not `pallas-crypto` directly**: depending on a pallas sub-crate still ties core to the pallas release cycle and keeps pallas in `cargo tree -p dolos-core`. A custom newtype is ~60 lines of boilerplate with zero ongoing maintenance burden.

**Why not raw `[u8; 32]`**: losing the newtype would lose hex Display/FromStr, making `ChainPoint` display and CLI parsing significantly more verbose. The type-level distinction between a hash and an arbitrary byte array is worth keeping.

**Boundary conversions**: `dolos-cardano` adds `From<pallas::crypto::hash::Hash<N>> for dolos_core::Hash<N>` and its inverse. These are zero-cost (same memory layout).

**Performance**: zero impact. `Hash<N>` is `repr(transparent)` over `[u8; N]`.

---

## Decision 3 — `Genesis` becomes a `GenesisConfig` trait

**What**: The `Genesis` struct (holding `byron`, `shelley`, `alonzo`, `conway` genesis files) moves to `dolos-cardano` as `CardanoGenesis`. Core defines a `GenesisConfig` trait:

```rust
pub trait GenesisConfig: Clone + Send + Sync + 'static {
    fn chain_id(&self) -> u32;
}
```

`ChainLogic` gets `type GenesisConfig: GenesisConfig` as an associated type. `Domain::genesis()` returns `Arc<<Self::Chain as ChainLogic>::GenesisConfig>`.

**Why `chain_id` and not `network_magic`**: `network_magic` is Cardano terminology. Every blockchain has some form of chain identifier — Midnight calls it something else. `chain_id()` returning `u32` is the minimal generic interface. Cardano's impl returns `shelley.network_magic`.

**Compromise**: everything else about genesis (epoch lengths, slot durations, protocol parameters) is Cardano-specific and stays in `CardanoGenesis`. Core knows nothing about genesis structure — only that a chain has an ID.

**Performance**: zero impact. The genesis config is read at startup and cached in an `Arc`.

---

## Decision 4 — `ChainTip` defined natively in core

**What**: `ChainTip = pallas::network::miniprotocols::chainsync::Tip` is replaced with:

```rust
pub struct ChainTip {
    pub point: ChainPoint,
    pub block_number: u64,
}
```

`dolos-cardano` adds `From<pallas::network::miniprotocols::chainsync::Tip> for ChainTip`.

**Why keep it in core**: every UTxO-based blockchain has a notion of "current tip" — a position in the chain plus a block height. This is not Cardano-specific. Removing it from core would force every chain integration to re-define it and lose the ability to express tip-awareness generically in sync machinery.

**Why not remove it**: sync progress tracking, tip subscriptions (`TipEvent`), and API responses all need a tip with both a point and a block number. Working with `ChainPoint` alone would lose the block number, requiring each chain layer to re-add it redundantly.

**Performance**: zero impact. It's a plain struct.

---

## Decision 5 — `BlockEra` dropped, `Era = u16` is the single representation

**What**: `pub type BlockEra = pallas::ledger::traverse::Era` is removed. `pub type Era = u16` (already present) is the sole era representation in core.

**Why**: both Cardano (eras: Byron=0, Shelley=1, ..., Conway=6) and Midnight (ledger versions: integer) serialize their version identifier as an unsigned integer. The `u16` wire type is the correct shared abstraction. Named variants (`Byron`, `Shelley`) are Cardano-specific and belong in `dolos-cardano`.

**Where named variants live**: `dolos-cardano` converts `pallas::ledger::traverse::Era ↔ u16` at its boundary. Pallas already implements this.

**Compromise**: code that currently writes `if era == BlockEra::Conway` must become `if era == CONWAY_ERA` (a constant in dolos-cardano). Slightly less ergonomic but correctly scoped.

**Performance**: zero impact. `Era = u16` is what was already stored.

---

## Decision 6 — `EvalReport` becomes `Option<Vec<u8>>` on `MempoolTx`

**What**: `pub report: Option<EvalReport>` (a pallas phase-2 evaluation result) becomes `pub report: Option<Vec<u8>>`. `dolos-cardano` serializes/deserializes the pallas `EvalReport` to bytes at its boundary.

**Why opaque bytes and not an associated type**: making `MempoolTx` generic over `D::EvalReport` would propagate a new generic parameter through every struct and trait that touches `MempoolTx` — a very wide blast radius. The eval report is only ever inspected by chain-specific code (the minibf/trp API layers), not by core machinery. Opaque bytes are the minimal interface.

**Compromise**: callers that want to inspect the eval report must deserialize from `Vec<u8>`. This is a one-line operation in dolos-cardano.

**Performance**: one extra serialize/deserialize round-trip per mempool tx admission. Mempool admission is rare compared to block processing — negligible.

---

## Decision 7 — Chain-specific error types via `ChainLogic::ChainSpecificError`

**What**: `ChainError` and `MempoolError` previously had variants typed directly to pallas errors (`pallas::ledger::traverse::Error`, `pallas::ledger::addresses::Error`, etc.). These are replaced with an associated type on `ChainLogic`:

```rust
pub trait ChainLogic {
    type ChainSpecificError: std::error::Error + Send + Sync + 'static;
}

pub enum ChainError<E: std::error::Error + Send + Sync + 'static> {
    // chain-agnostic variants unchanged ...
    ChainSpecific(E),  // typed, not hidden
}
```

`dolos-cardano` defines `CardanoError` wrapping all pallas error types and sets `type ChainSpecificError = CardanoError`.

**Why not string errors**: string errors destroy type information, make programmatic error handling impossible, and go against Rust idioms. A typed associated error preserves the full pallas error at the dolos-cardano boundary — callers that know they're in a Cardano context can match on `ChainError::ChainSpecific(CardanoError::Traverse(...))` and get the original pallas error.

**Why not `Box<dyn Error>`**: same reason — loses type information. The associated type approach is zero-cost (monomorphized) and keeps errors inspectable.

**Propagation**: `DomainError` resolves `E` as `<D::Chain as ChainLogic>::ChainSpecificError`. Since `D::Chain` is already an associated type on `Domain`, no extra generics appear at call sites.

**Performance**: zero impact. Associated types are resolved at compile time.

---

## Decision 8 — Block decoding moved behind `ChainLogic::find_tx_in_block`

**What**: `async_query.rs` had two methods (`block_by_tx_hash`, `tx_cbor`) that decoded raw block bytes inline using `MultiEraBlock`. These become a new `ChainLogic` static method:

```rust
fn find_tx_in_block(block: &[u8], tx_hash: &[u8]) -> Option<(EraBody, TxOrder)>;
```

Core calls this; `dolos-cardano` implements it with `MultiEraBlock`. `AsyncQueryFacade` stays in core.

**Why keep `AsyncQueryFacade` in core**: the async semaphore-limited dispatch pattern and the other query methods (`block_by_slot`, `block_by_number`, etc.) are entirely chain-agnostic. Moving the facade to dolos-cardano would force every API layer to re-implement the concurrency limiting.

**Performance**: zero impact. One additional virtual dispatch per query — negligible for a query path that's already doing database I/O.

---

## Decision 9 — Mempool UTxO scanning moved behind `ChainLogic` methods

**What**: `scan_mempool_utxos` and `exclude_inflight_stxis` in `mempool.rs` used `MultiEraTx`/`MultiEraOutput` to iterate mempool transaction inputs/outputs. These become two `ChainLogic` static methods:

```rust
fn tx_produced_utxos(era_body: &EraBody) -> Vec<(TxoRef, EraBody)>;
fn tx_consumed_refs(era_body: &EraBody) -> Vec<TxoRef>;
```

The predicate in `get_utxos_by_tag` changes from `Fn(&MultiEraOutput<'_>) -> bool` to `Fn(&EraBody) -> bool`.

**Why keep the scan logic in core**: mempool-aware UTxO queries are a generic concept for any UTxO-based chain. Midnight (or any other chain) would need the same "scan pending transactions and merge with confirmed state" logic. The only chain-specific part is how you decode a raw transaction into its inputs and outputs — which is exactly what the two `ChainLogic` methods encapsulate.

**Predicate change consequence**: callers in `dolos-minibf` and `dolos-trp` that currently receive `&MultiEraOutput` in their predicate will instead receive `&EraBody` and must decode it themselves to `MultiEraOutput`. This is a one-line change per callsite in those crates.

**Performance**: zero impact. The same decoding work happens — it just happens inside the predicate rather than before it.

---

## Summary of what stays in `dolos-core`

| Thing | Before | After |
|---|---|---|
| `Hash<N>` | from pallas-crypto | custom newtype (~60 lines) |
| `EraBody` (was `EraCbor`) | pallas re-export | stays in core, minicbor direct dep |
| `Era = u16` | unchanged | unchanged |
| `ChainTip` | pallas alias | native struct in core |
| `GenesisConfig` | pallas `Genesis` struct | generic trait |
| `ChainError` | pallas error variants | parameterized `ChainError<E>` |
| `minicbor` dep | via pallas | direct dep |
| `pallas` dep | present | **removed** |

## Summary of what moves to `dolos-cardano`

- `CardanoGenesis` (was `Genesis`) with all four genesis file fields
- `CardanoError` (wraps all pallas error types)
- `From<PallasPoint> for ChainPoint`, `TryFrom<ChainPoint> for PallasPoint`
- `From<PallasTip> for ChainTip`
- `From<pallas::crypto::hash::Hash<N>> for dolos_core::Hash<N>`
- `From<MultiEraOutput> for EraBody`, `TryFrom<&EraBody> for MultiEraOutput`, etc.
- `From<&MultiEraInput> for TxoRef`
- `ChainLogic::find_tx_in_block` implementation
- `ChainLogic::tx_produced_utxos` implementation
- `ChainLogic::tx_consumed_refs` implementation

---

## Verification

1. `cargo tree -p dolos-core | grep pallas` — must return nothing
2. `cargo check -p dolos-core` — must compile clean with zero pallas in scope
3. `cargo clippy --all-targets --all-features -- -D warnings` — zero warnings
4. `cargo test --workspace --all-targets` — all tests pass
