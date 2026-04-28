# Cardano Work Units and Their Deltas

## Natural sequence within an epoch

```text
Estart  →  Roll …  →  Rupd  →  Roll …  →  Ewrap
(open half:    (blocks)    (RUPD:     (blocks)    (close half:
 per-account                per-account            per-account
 snapshot                   reward                 reward apply
 rotation                   compute                + global
 + global                   sharded                Ewrap pass
 Estart pass                + finalize             in finalize())
 in finalize())             incentives)                              │
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

`Rupd` is a third sharded unit, firing mid-epoch at the randomness stability window. It computes rewards from the E-3 stake snapshot and writes one `PendingRewardState` entity per rewarded account in this shard's range; `Ewrap` consumes those entries to apply the rewards.

All three sharded units share credential-key partitioning (`crate::shard::shard_key_ranges`) and the `crate::shard::ACCOUNT_SHARDS` constant; progress for each is tracked separately on `EpochState.ewrap_progress`, `EpochState.estart_progress`, and `EpochState.rupd_progress`. The shard count is captured at the first per-shard commit and persisted on `ShardProgress.total` so a value change between shards (e.g. across a crash and restart on a newer build) can't break an in-flight pipeline.

The sections below walk the cycle starting at `Estart` (the first phase of every epoch).

---

## 1. `EstartWorkUnit` — open half (sharded body + Estart finalize)

- Variants: `CardanoWorkUnit::Estart` / `InternalWorkUnit::Estart(BlockSlot)`.
- Struct: `estart::EstartWorkUnit` (`crates/cardano/src/estart/work_unit.rs`). Reports `total_shards` (`crate::shard::ACCOUNT_SHARDS`) via `WorkUnit::total_shards`; the executor invokes `load(shard) → compute(shard) → commit_state(shard)` once per shard, then `finalize()`.

### `initialize()`

- Resolves the boundary's effective shard count (`EpochState.estart_progress.total` if a boundary is in flight, else `crate::shard::ACCOUNT_SHARDS`).
- Computes the AVVM reclamation total once (used by every shard's `load` and by `finalize`). Returns 0 except at the Shelley→Allegra hardfork.

### Per-shard body — per-account snapshot rotation

- Each shard is scoped to a first-byte prefix bucket of the credential key space.
- Streams per-account snapshot transitions (rotate `EpochValue.go ← set ← mark ← live ← next`) for the accounts in this shard's range. Splits the account pass into bounded-memory chunks so `AccountTransition` deltas don't accumulate by the millions.
- Deltas emitted per shard:
  - `AccountTransition` — `estart/reset.rs:130` (visitor reused by the shard pass). One per account in range. Targets `accounts`.
  - `EStartProgress` — `estart/loading.rs:42`. Single per shard. Targets `epochs` — advances `estart_progress = Some(ShardProgress { committed: shard_index + 1, total })`. Idempotent + ordered + total-mismatch guarded, mirroring `EWrapProgress`.

### `finalize()` — Estart global pass

- Builds a fresh `WorkContext` via `WorkContext::load_finalize` (using the AVVM total computed in `initialize`).
- Iterates pools / dreps / proposals; emits transition deltas via the `nonces` and `reset` visitors.
- Emits the closing `EpochTransition` delta (rotates `number` / `initial_pots` / `rolling` / `pparams`, seeds `EpochState.end = Some(EndStats::default())`, resets *both* `ewrap_progress = None` and `estart_progress = None`).
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

## 3. `RupdWorkUnit` — computes rewards at the stability window (sharded body + Rupd finalize)

- Variants: `CardanoWorkUnit::Rupd` / `InternalWorkUnit::Rupd(BlockSlot)`.
- Struct: `rupd::RupdWorkUnit` (`crates/cardano/src/rupd/work_unit.rs`). Reports `total_shards` (`crate::shard::ACCOUNT_SHARDS`) via `WorkUnit::total_shards`; the executor invokes `load(shard) → compute(shard) → commit_state(shard)` once per shard, then `finalize()`.
- Purpose: at `randomness_stability_window` into the epoch, compute the reward distribution from the E-3 stake snapshot and persist `PendingRewardState` entities to be consumed by the close-half `Ewrap`.

### `initialize()`

- Resolves the boundary's effective shard count (`EpochState.rupd_progress.total` if a RUPD is in flight, else `crate::shard::ACCOUNT_SHARDS`).
- Builds boundary-wide globals via `RupdWork::load_globals`: pots, incentives (`epoch_incentives`), pparams (mark snapshot), `blocks_made_total`, chain summary, and the **pool-bounded** half of the stake snapshot — `pools` (`HashMap<PoolHash, EpochValue<PoolSnapshot>>`), `pool_stake` (per-pool totals summed across every account), `active_stake_sum`, `performance_epoch_pool_blocks`. Memory: O(pools) ≈ a few thousand entries; the per-account map is *not* built here.

### Per-shard body — per-credential reward emission

- Each shard is scoped to a first-byte prefix bucket of the credential key space.
- `load(shard)` extends the globals with shard-scoped `accounts_by_pool` (delegators in range) and `registered_accounts` (registered creds in range) by streaming `AccountState` only over the shard's two `EntityKey` ranges. Stashes the ranges on `RupdWork.shard_ranges` so `should_include` can gate emissions.
- `compute(shard)` runs `define_rewards` over **every pool** (pool rewards depend on global pool stake / blocks, both hoisted to `initialize`) but emits only credentials this shard owns:
  - delegator rewards are filtered naturally — `pool_delegators(pool)` returns only in-range creds
  - leader rewards are gated by `should_include(operator_account)`; the shard whose range contains the operator credential is the sole emitter for that pool's leader reward
- `commit_state(shard)` writes the in-range `PendingRewardState` entities (overwrite-by-key, idempotent) and emits a single `RupdProgress` delta to advance `EpochState.rupd_progress`. Per-pool reward + delegator-count contributions are accumulated on the work unit (O(pools)) for the finalize-phase StakeLog.
- Deltas emitted per shard:
  - `RupdProgress` — `rupd/work_unit.rs:commit_state`. Single per shard. Targets `epochs` — advances `rupd_progress = Some(ShardProgress { committed: shard_index + 1, total })`. Idempotency + ordering + total-mismatch guarded, mirroring `EWrapProgress` / `EStartProgress`.

### `finalize()` — Rupd global pass

- Direct write: `EpochState.incentives = Some(work.incentives)` and `EpochState.rupd_progress = None`. Single one-shot write so concurrent shard commits can't race.
- Direct writes: per-pool `StakeLog` archive entries from the shard-accumulator (`total_rewards`, `operator_share`, `delegators_count` rolled up across shards; `blocks_minted`, `total_stake`, `relative_size`, `declared_pledge`, `fixed_cost`, `margin_cost` from globals).
- No deltas emitted in finalize.

`PendingRewardState` writes are overwrite-by-key, so true mid-shard resume is safe (rerunning a shard rewrites the same credentials with identical data).

- Namespaces touched: `accounts` (read only, shard range, per shard), `pending_rewards` (write, shard range, per shard), `epochs` (`rupd_progress` per shard via delta; `incentives` direct-write in finalize).

## 4. `EwrapWorkUnit` — close half (sharded body + Ewrap finalize)

- Variants: `CardanoWorkUnit::Ewrap` / `InternalWorkUnit::Ewrap(BlockSlot)`.
- Struct: `ewrap::EwrapWorkUnit` (`crates/cardano/src/ewrap/work_unit.rs`). Reports `total_shards` (`crate::shard::ACCOUNT_SHARDS`) via `WorkUnit::total_shards`; the executor invokes `load(shard) → compute(shard) → commit_state(shard)` once per shard, then `finalize()`.

### `initialize()`

- Resolves the boundary's effective shard count (`EpochState.ewrap_progress.total` if a boundary is in flight, else `crate::shard::ACCOUNT_SHARDS`).

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
    - `EWrapProgress` (rolls up the shard's `effective` / `unspendable_to_treasury` / `unspendable_to_reserves` totals into `EpochState.end`, and writes `ewrap_progress = Some(ShardProgress { committed: shard_index + 1, total })`)
- Direct deletes: `PendingRewardState` entries for credentials whose rewards landed.

### `finalize()` — Ewrap global pass

- Builds a fresh `BoundaryWork` via `BoundaryWork::load_finalize`.
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
    - `EpochWrapUp` (carries the final assembled `EndStats`; apply overwrites `entity.end`, rotates `rolling`/`pparams` snapshots forward, clears `ewrap_progress`)
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

Pending entities (`PendingMirState`, `PendingRewardState`) are removed via direct `delete_entity` calls instead, and `EpochState.incentives` is direct-written by `RupdWorkUnit::finalize` (rather than via `SetEpochIncentives`). TODO: either wire these deltas in or remove the variants.
