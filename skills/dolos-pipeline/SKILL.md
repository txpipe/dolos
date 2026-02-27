---
name: dolos-pipeline
description: Architecture of the Dolos processing pipeline — WorkUnit lifecycle, executor modes, CardanoWorkUnit variants, WorkBuffer state machine, and sequencing. Reference when debugging execution ordering, understanding phase boundaries, or adding new work unit types.
user-invocable: false
---

# Dolos Processing Pipeline

## WorkUnit Trait Lifecycle

Every piece of work in Dolos implements the `WorkUnit<D: Domain>` trait (`crates/core/src/work_unit.rs`) with 6 execution phases:

| Phase | Purpose | I/O |
|-------|---------|-----|
| `load()` | Query state/archive stores for required data | Read from stores |
| `compute()` | CPU-intensive work on loaded data | No storage access |
| `commit_wal()` | Write to write-ahead log for crash recovery | Write to WAL |
| `commit_state()` | Apply computed changes to state store | Write to state |
| `commit_archive()` | Write historical data/logs to archive | Write to archive |
| `commit_indexes()` | Update additional indexes | Write to indexes |

Additionally, `tip_events()` returns events for live subscribers.

Default no-op implementations exist for `commit_wal`, `commit_indexes`, and `tip_events`.

## Executor Modes

### Sync Mode (`crates/core/src/sync.rs`)

Full lifecycle for live block processing:
- Runs all 6 phases + tip event emission
- Includes WAL commits for crash recovery and rollback support
- Entry point: `SyncExt::roll_forward()` → drains pending work via `drain_pending_work()` → `execute_work_unit()` per unit

### Import Mode (`crates/core/src/import.rs`)

Optimized for bulk data import (Mithril snapshots):
- **Skips** `commit_wal()` (immutable source data, no crash recovery needed)
- **Skips** tip event emission (no live subscribers)
- Entry point: `ImportExt::import_blocks()` → same drain/execute pattern minus WAL/tips

## CardanoWorkUnit Enum

Defined in `crates/cardano/src/lib.rs`, dispatches to 5 concrete work units + a stop sentinel:

```
CardanoWorkUnit
├── Genesis(GenesisWorkUnit)     — Bootstrap initial state
├── Roll(RollWorkUnit)           — Process block batches
├── Rupd(RupdWorkUnit)           — Compute rewards at stability window
├── Ewrap(EwrapWorkUnit)         — Apply rewards, refund deposits at epoch end
├── Estart(EstartWorkUnit)       — Transition snapshots, compute new pots
└── ForcedStop                   — Signal test harness termination
```

`ForcedStop` returns `Err(DomainError::StopEpochReached)` in `commit_state()` to signal termination.

## WorkBuffer State Machine

The `WorkBuffer` enum (`crates/cardano/src/lib.rs`) sequences work unit creation through state transitions:

```
Empty → (genesis block) → Genesis
Empty → (regular block) → OpenBatch
OpenBatch → (more blocks) → OpenBatch
OpenBatch → (RUPD boundary) → PreRupdBoundary
OpenBatch → (epoch boundary) → PreEwrapBoundary
OpenBatch → (stop epoch) → PreForcedStop

PreRupdBoundary → pop_work() → [Roll for batch] → RupdBoundary
RupdBoundary → pop_work() → [Rupd] → Restart

PreEwrapBoundary → pop_work() → [Roll for batch] → EwrapBoundary
EwrapBoundary → pop_work() → [Ewrap] → EstartBoundary
EstartBoundary → pop_work() → [Estart] → Restart

PreForcedStop → pop_work() → [Roll for batch] → ForcedStop
ForcedStop → pop_work() → [ForcedStop sentinel]

Restart → (next block) → OpenBatch / boundary detection
```

**Key methods:**
- `receive_block()` — Accepts a new block, detects epoch/RUPD boundaries, transitions state
- `pop_work()` — Extracts next work unit, mutates buffer to next state
- `can_receive_block()` — True for `Empty`, `Restart`, `OpenBatch`; false for boundary states

**Boundary detection** (`crates/cardano/src/pallas_extras.rs`):
- `epoch_boundary(eras, prev_slot, next_slot)` — Returns `(prev_epoch, boundary_slot, next_epoch)` if slots span different epochs
- `rupd_boundary(stability_window, eras, prev_slot, next_slot)` — Returns boundary slot if the randomness stability window threshold is crossed within the same epoch

## Processing Sequence

```
Genesis → Roll batches → [4k/f slots] Rupd → Roll batches → [epoch end] Ewrap → Estart → Roll batches → ...
```

Within a single epoch:
1. **Roll** — Blocks are accumulated into batches and processed as `RollWorkUnit`s
2. **Rupd** — Fires once at `epoch_start + randomness_stability_window` (4k/f slots). Computes reward distribution using the mark snapshot
3. **Ewrap** — Fires at the epoch boundary. Applies spendable rewards, filters unspendable, refunds deposits, enacts proposals
4. **Estart** — Fires immediately after Ewrap. Rotates `EpochValue` snapshots (`go ← set ← mark ← live ← next`), computes initial pots for the new epoch

## Work Unit Details

### Genesis (`crates/cardano/src/genesis/work_unit.rs`)

**Trigger**: First block received on empty buffer.

| Phase | Action |
|-------|--------|
| load | No-op |
| compute | No-op |
| commit_wal | Reset WAL to Origin |
| commit_state | Bootstrap pots, pparams, epoch state from genesis config |
| commit_archive | No-op |

### Roll (`crates/cardano/src/roll/work_unit.rs`)

**Trigger**: Batch of blocks accumulated in `OpenBatch`, flushed at boundary or batch size limit.

**Key types:**
- `WorkBatch` — Contains `Vec<WorkBlock>`, fetched UTxOs, loaded entities
- `WorkBlock` — Single block + computed deltas + UTxO delta

**Visitor pattern**: `DeltaBuilder` (`crates/cardano/src/roll/mod.rs`) traverses each block visiting root metadata, then each TX's inputs/outputs/mints/certificates/withdrawals/updates/proposals. Entity visitors (AccountVisitor, PoolStateVisitor, etc.) produce deltas.

| Phase | Action |
|-------|--------|
| load | No-op (UTxO loading happens in `pop_work()`) |
| compute | No-op (delta computation happens in `pop_work()` via DeltaBuilder) |
| commit_wal | Sort batch by slot, append to WAL |
| commit_state | Load entities, apply deltas, commit state + cursor |
| commit_archive | Write blocks to archive |
| commit_indexes | Build and apply index deltas |
| tip_events | Emit Apply event per block (live mode only) |

### Rupd (`crates/cardano/src/rupd/`)

**Trigger**: Block crosses the `epoch_start + randomness_stability_window` threshold.

| Phase | Action |
|-------|--------|
| load | Load accounts, pools, protocol params from state |
| compute | Calculate rewards via pool/account visitors |
| commit_state | Store pending rewards in state |
| commit_archive | Write reward logs to archive |

Uses mark snapshot (E-1) for stake distribution and pool params. Pre-Babbage (protocol < 7): filters out unregistered accounts before calculation. Babbage+: calculates for all accounts.

### Ewrap (`crates/cardano/src/ewrap/`)

**Trigger**: Block crosses an epoch boundary (different epoch than previous block).

| Phase | Action |
|-------|--------|
| load | Load epoch state, pools, accounts, DReps, proposals |
| compute | Filter rewards, compute refunds, route unspendable to treasury |
| commit_state | Apply filtered rewards, refunds, treasury adjustments |
| commit_archive | Write boundary logs |

**Key visitors:**
- `rewards::RewardApplyVisitor` — Filter spendable rewards, track `applied_rewards`
- `refunds::DepositRefundVisitor` — Refund pool/proposal deposits
- `enactment::ProposalEnactmentVisitor` — Enact ratified proposals

### Estart (`crates/cardano/src/estart/`)

**Trigger**: Immediately after Ewrap completes.

| Phase | Action |
|-------|--------|
| load | Load epoch state, pools, accounts, proposals |
| compute | Transition snapshots, compute pot delta |
| commit_state | Apply snapshot transitions, update pots, increment epoch |
| commit_archive | Write new epoch state |

**Snapshot transition**: All `EpochValue<T>` fields advance: `go ← set ← mark ← live ← next`.

## Stability Windows

Computed from genesis parameters (`crates/cardano/src/utils.rs`):

| Window | Formula | Mainnet (k=2160, f≈0.05) |
|--------|---------|--------------------------|
| `stability_window` (3k/f) | `ceil(3 × k / f)` | ~129,600 slots |
| `randomness_stability_window` (4k/f) | `ceil(4 × k / f)` | ~172,800 slots |

**RUPD fires at 4k/f**, not 3k/f. This is critical for correct reward pre-filtering boundaries.

## Design Notes

- **Load/compute separation**: Heavy I/O in `load()`, pure computation in `compute()`, clean commit phases. This enables potential parallelization.
- **`needs_cache_refresh`**: Set after Genesis and Estart to reload era summary from state before the next work unit.
- **Roll pre-processing in `pop_work()`**: UTxO fetching (`batch.load_utxos()`) and delta computation (`roll::compute_delta()`) happen before the work unit is returned, not during `load()`/`compute()`. This keeps the `WorkUnit` trait implementation thin.
- **Boundary detection is slot-based**: Boundaries are detected by comparing consecutive block slots, not by counting blocks or epochs directly.
