# Cardano Work Units and Their Deltas

## Natural sequence within an epoch

```
Estart  →  Roll …  →  Rupd  →  Roll …  →  Ewrap
(open half:    (blocks)    (RUPD)    (blocks)    (close half:
 per-account                                       per-account
 snapshot                                          reward apply
 rotation                                          + global
 + global                                          Ewrap pass
 Estart pass                                       in finalize())
 in finalize())                                                     │
                                                                    ▼
                                                          next epoch's Estart
```

`Genesis` runs once at chain bootstrap; the first `Estart` fires at the first epoch boundary the chain crosses. `ForcedStop` is a sentinel that ends the loop at a configured epoch.

Every work unit reports its shard count via `WorkUnit::total_shards()` and the core executor (in `crates/core/src/sync.rs::run_lifecycle`) loops over shards calling `load → compute → commit_*` once per shard. Work units that don't need sharding default to `total_shards() = 1` and ignore the `shard_index` parameter. Two shard-agnostic hooks bracket the loop:

- **`initialize()`** runs once before any shard. Used to compute `total_shards`, hoist boundary-wide reads, and so on.
- **`finalize()`** runs once after the last shard's commits. Used for global teardown that depends on every shard having landed.

The boundary work splits into a *close* half (`Ewrap`, ending epoch N) and an *open* half (`Estart`, opening epoch N+1). Each is a single sharded work unit whose `finalize()` runs the corresponding global pass:

| Half | Sharded body | `finalize()` pass | Cursor advance? |
|------|--------------|-------------------|-----------------|
| Close (epoch N) | `Ewrap` — per-account reward apply | Ewrap globals + `EpochWrapUp` | no |
| Open (epoch N+1) | `Estart` — per-account snapshot rotation | Estart globals + `EpochTransition` + era transitions | yes (only here) |

Both sharded units share credential-key partitioning (`crate::shard::shard_key_ranges`) and the `account_shards` config knob; progress for each half is tracked separately on `EpochState.ashard_progress` and `EpochState.estart_shard_progress`. The shard count is captured at the first per-shard commit and persisted on `ShardProgress.total` so a config change between shards (e.g. across a crash and restart) can't break an in-flight pipeline.

The sections below walk the cycle starting at `Estart` (the first phase of every epoch).

---

## 1. `EstartWorkUnit` — open half (sharded body + Estart finalize)

- Variants: `CardanoWorkUnit::Estart` / `InternalWorkUnit::Estart(BlockSlot)`.
- Struct: `estart::EstartWorkUnit` (`crates/cardano/src/estart/work_unit.rs`). Reports `total_shards` (default 16) via `WorkUnit::total_shards`; the executor invokes `load(shard) → compute(shard) → commit_state(shard)` once per shard, then `finalize()`.

### `initialize()`

- Resolves the boundary's effective shard count (`EpochState.estart_shard_progress.total` if a boundary is in flight, else `config.account_shards()`).
- Computes the AVVM reclamation total once (used by every shard's `load` and by `finalize`). Returns 0 except at the Shelley→Allegra hardfork.

### Per-shard body — per-account snapshot rotation

- Each shard is scoped to a first-byte prefix bucket of the credential key space.
- Streams per-account snapshot transitions (rotate `EpochValue.go ← set ← mark ← live ← next`) for the accounts in this shard's range. Splits the account pass into bounded-memory chunks so `AccountTransition` deltas don't accumulate by the millions.
- Deltas emitted per shard:
  - `AccountTransition` — `estart/reset.rs:130` (visitor reused by the shard pass). One per account in range. Targets `accounts`.
  - `EStartShardAccumulate` — `estart/loading.rs:42`. Single per shard. Targets `epochs` — advances `estart_shard_progress = Some(ShardProgress { committed: shard_index + 1, total })`. Idempotent + ordered + total-mismatch guarded, mirroring `EpochEndAccumulate`.

### `finalize()` — Estart global pass

- Builds a fresh `WorkContext` via `WorkContext::load_finalize` (using the AVVM total computed in `initialize`).
- Iterates pools / dreps / proposals; emits transition deltas via the `nonces` and `reset` visitors.
- Emits the closing `EpochTransition` delta (rotates `number` / `initial_pots` / `rolling` / `pparams`, seeds `EpochState.end = Some(EndStats::default())`, resets *both* `ashard_progress = None` and `estart_shard_progress = None`).
- Direct writes: `EraSummary` writes during era transitions (Shelley→Allegra etc.).
- **Sets the cursor** to `ChainPoint::Slot(boundary_slot)` — the only phase across all work units that moves the cursor. A crash before `finalize()` restarts from the boundary block.
- Deltas emitted in finalize:
  - `NonceTransition` — `estart/nonces.rs:40`. Targets `epochs`.
  - `PoolTransition` — `estart/reset.rs:141`. One per pool. Targets `pools`.
  - `EpochTransition` — `estart/reset.rs:158` (emitted from `compute_global_deltas` in `estart/loading.rs`). Single. Targets `epochs`.

`AccountTransition` is not natively idempotent on re-apply (would double-roll), so true mid-shard resume remains a TODO — same posture as `EwrapWorkUnit`.

- Namespaces touched: `accounts` (shard range, per shard), `pools`, `epochs`, `eras` (in finalize).

## 2. `RollWorkUnit` — applies a batch of blocks

- Variants: `CardanoWorkUnit::Roll` / `InternalWorkUnit::Blocks(WorkBatch)`.
- Struct: `roll::RollWorkUnit` (`crates/cardano/src/roll/work_unit.rs`).
- Purpose: process per-transaction effects for a batch of blocks. Fires repeatedly across the epoch — once before RUPD, once after, with potentially many sub-batches each.
- Deltas emitted (22 distinct variants, 41 sites):
  - **Accounts** (`roll/accounts.rs`):
    - `ControlledAmountInc:102`
    - `ControlledAmountDec:78`
    - `StakeRegistration:125`
    - `StakeDelegation:129`
    - `StakeDeregistration:133`
    - `VoteDelegation:137`
    - `WithdrawalInc:191`
    - `EnqueueMir:163,167`
  - **Pools** (`roll/pools.rs`):
    - `MintedBlocksInc:35`
    - `PoolRegistration:46`
    - `PoolDeRegistration:68,77`
  - **DReps** (`roll/dreps.rs`):
    - `DRepRegistration:74`
    - `DRepUnRegistration:83`
    - `DRepActivity:53,93`
  - **Proposals** (`roll/proposals.rs`):
    - `NewProposal:286,329`
  - **Assets** (`roll/assets.rs`):
    - `MintStatsUpdate:35`
    - `MetadataTxUpdate:46,79`
  - **Datums** (`roll/datums.rs`):
    - `DatumRefIncrement:70`
    - `DatumRefDecrement:87`
  - **Epoch rolling stats** (`roll/epochs.rs`):
    - `EpochStatsUpdate:99`
    - `NoncesUpdate:115`
- Namespaces touched: `accounts`, `pools`, `dreps`, `proposals`, `assets`, `datums`, `epochs`, `pending_mirs`.

## 3. `RupdWorkUnit` — computes rewards at the stability window

- Variants: `CardanoWorkUnit::Rupd` / `InternalWorkUnit::Rupd(BlockSlot)`.
- Struct: `rupd::RupdWorkUnit` (`crates/cardano/src/rupd/work_unit.rs`).
- Purpose: at `randomness_stability_window` into the epoch, compute the reward distribution and persist pending rewards.
- Deltas: **none**. Writes `PendingRewardState` entities directly and updates `EpochState.incentives`.
- Namespaces touched: `pending_rewards`, `epochs`.

## 4. `EwrapWorkUnit` — close half (sharded body + Ewrap finalize)

- Variants: `CardanoWorkUnit::Ewrap` / `InternalWorkUnit::Ewrap(BlockSlot)`.
- Struct: `ewrap::EwrapWorkUnit` (`crates/cardano/src/ewrap/work_unit.rs`). Reports `total_shards` (default 16) via `WorkUnit::total_shards`; the executor invokes `load(shard) → compute(shard) → commit_state(shard)` once per shard, then `finalize()`.

### `initialize()`

- Resolves the boundary's effective shard count (`EpochState.ashard_progress.total` if a boundary is in flight, else `config.account_shards()`).

### Per-shard body — per-account reward application

- Each shard is scoped to a first-byte prefix bucket of the account key space.
- Applies rewards + drops for the accounts in this shard's range; accumulates the shard's reward contribution into `EpochState.end`.
- Deltas emitted per shard:
  - **Rewards** (`ewrap/rewards.rs`):
    - `AssignRewards:79` (one per rewarded account in range)
  - **Drops** (account-level, `ewrap/drops.rs`, also used in finalize for non-account targets):
    - `PoolDelegatorRetire:32`
    - `DRepDelegatorDrop:53`
  - **Accumulator** (`ewrap/loading.rs`):
    - `EpochEndAccumulate` (rolls up the shard's `effective` / `unspendable_to_treasury` / `unspendable_to_reserves` totals into `EpochState.end`, and writes `ashard_progress = Some(ShardProgress { committed: shard_index + 1, total })`)
- Direct deletes: `PendingRewardState` entries for credentials whose rewards landed.

### `finalize()` — Ewrap global pass

- Builds a fresh `BoundaryWork` via `BoundaryWork::load_ewrap`.
- Classifies retiring pools / expiring dreps / enacting+dropping proposals; processes pending MIRs; runs enactment + refund + wrapup-global visitors.
- Assembles the final `EndStats` (prepare-time fields combined with the accumulator fields populated by the preceding shard runs) and emits a single `EpochWrapUp` to close the boundary.
- Direct writes: writes the completed `EpochState` to the archive under the epoch-start temporal key.
- Direct deletes: `PendingMirState` entries for processed MIRs.
- Deltas emitted in finalize (10 distinct variants, 11 sites):
  - **Enactment** (`ewrap/enactment.rs`):
    - `PParamsUpdate:36,39`
    - `TreasuryWithdrawal:43`
  - **Refunds** (`ewrap/refunds.rs`):
    - `PoolDepositRefund:88`
    - `ProposalDepositRefund:39,62`
  - **Drops** (drep-level, `ewrap/drops.rs`):
    - `DRepExpiration:67`
    - `DRepDelegatorDrop:53` *(also fires in shard body for accounts)*
    - `PoolDelegatorRetire:32` *(also fires in shard body for accounts)*
  - **Wrap-up globals** (`ewrap/wrapup.rs`):
    - `PoolWrapUp:119`
    - `EpochWrapUp` (carries the final assembled `EndStats`; apply overwrites `entity.end`, rotates `rolling`/`pparams` snapshots forward, clears `ashard_progress`)
  - **MIR processing** (`ewrap/loading.rs`):
    - `AssignRewards:207` (one per registered MIR recipient)

- Namespaces touched: `accounts` (shard range, per shard; MIR recipients in finalize), `pools`, `dreps`, `proposals`, `epochs`. Deletes from `pending_rewards` (per shard) and `pending_mirs` (in finalize).

## 5. `GenesisWorkUnit` — one-time chain bootstrap

- Variants: `CardanoWorkUnit::Genesis` / `InternalWorkUnit::Genesis`.
- Struct: `genesis::GenesisWorkUnit` (`crates/cardano/src/genesis/mod.rs`).
- Purpose: bootstrap state from genesis configs (Byron/Shelley/Alonzo/Conway).
- Deltas: **none**. Writes `EpochState` and `EraSummary` entities directly via `bootstrap_epoch` / `bootstrap_eras`.
- Namespaces touched: `epochs`, `eras`.

## 6. `ForcedStop` — sentinel

- Variant: `CardanoWorkUnit::ForcedStop` / `InternalWorkUnit::ForcedStop`.
- No struct, no load/compute/commit. Causes the sync loop to stop at the configured `stop_epoch`.

---

## Defined deltas with no emission site

The following variants exist on `CardanoDelta` (`crates/cardano/src/model/mod.rs`) but no work unit emits them today:

- `DequeueMir`
- `EnqueueReward`
- `DequeueReward`
- `SetEpochIncentives`

Pending entities (`PendingMirState`, `PendingRewardState`) are removed via direct `delete_entity` calls instead. TODO: either wire these deltas in or remove the variants.
