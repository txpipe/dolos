# Cardano Work Units and Their Deltas

## Natural sequence within an epoch

```
Estart  →  Roll …  →  Rupd  →  Roll …  →  Ewrap  →  AccountShard ×N  →  EwrapFinalize
(open)     (blocks)   (RUPD)   (blocks)   (global)    (per-account)         (close)
                                                                                │
                                                                                ▼
                                                                        next epoch's Estart
```

`Genesis` runs once at chain bootstrap, before the first `Estart`. `ForcedStop` is a sentinel that ends the loop at a configured epoch.

The sections below walk the cycle starting at `Estart` (the opener of every epoch).

---

## 1. `EstartWorkUnit` — opens the epoch

- Variants: `CardanoWorkUnit::Estart` / `InternalWorkUnit::EStart(BlockSlot)`.
- Struct: `estart::EstartWorkUnit` (`crates/cardano/src/estart/work_unit.rs`).
- Purpose: roll the ledger into the new epoch — nonce transition, snapshot shifts, pot recalc, era transitions, and opening the new `EpochState.end` slot.
- Deltas emitted (4):
  - `NonceTransition` — `estart/nonces.rs:40`. Targets `epochs`.
  - `AccountTransition` — `estart/reset.rs:127`. One per account. Targets `accounts`.
  - `PoolTransition` — `estart/reset.rs:138`. One per pool. Targets `pools`.
  - `EpochTransition` — `estart/reset.rs:148`. Single. Targets `epochs`. In addition to rotating `number`/`initial_pots`/`rolling`/`pparams`, also seeds `EpochState.end = Some(EndStats::default())` and resets `ewrap_progress = None` for the new epoch — Ewrap later overwrites this default with the populated stats.
- Direct writes: `EraSummary` writes during era transitions (Shelley→Allegra etc.).
- Namespaces touched: `accounts`, `pools`, `epochs`, `eras`.

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

## 4. `EwrapWorkUnit` — global epoch-boundary work

- Variants: `CardanoWorkUnit::Ewrap` / `InternalWorkUnit::Ewrap(BlockSlot)`.
- Struct: `ewrap::EwrapWorkUnit` (`crates/cardano/src/ewrap/work_unit.rs`).
- Purpose: classify retiring pools / expiring dreps / enacting+dropping proposals, process pending MIRs, run enactment + refund + wrapup-global visitors, populate `EpochState.end` (slot already opened by ESTART's `EpochTransition`).
- Deltas emitted (10 distinct variants, 11 sites):
  - **Enactment** (`ewrap/enactment.rs`):
    - `PParamsUpdate:36,39`
    - `TreasuryWithdrawal:43`
  - **Refunds** (`ewrap/refunds.rs`):
    - `PoolDepositRefund:88`
    - `ProposalDepositRefund:39,62`
  - **Drops** (drep-level, `ewrap/drops.rs`):
    - `DRepExpiration:67`
    - `DRepDelegatorDrop:53` *(also fires in AccountShard for accounts)*
    - `PoolDelegatorRetire:32` *(also fires in AccountShard for accounts)*
  - **Wrap-up globals** (`ewrap/wrapup.rs`):
    - `PoolWrapUp:119`
    - `EpochEndInit:135` (overwrites the ESTART-seeded default with prepare-time globals + zeroed reward accumulators, sets `ewrap_progress = Some(0)`)
  - **MIR processing** (`ewrap/loading.rs`):
    - `AssignRewards:207` (one per registered MIR recipient)
- Direct deletes: `PendingMirState` entries for processed MIRs.
- Namespaces touched: `pools`, `dreps`, `proposals`, `accounts` (MIR recipients), `epochs`. Deletes from `pending_mirs`.

## 5. `AccountShardWorkUnit` — per-account reward application (×N shards)

- Variants: `CardanoWorkUnit::AccountShard` / `InternalWorkUnit::AccountShard(BlockSlot, u32)`.
- Struct: `ewrap::AccountShardWorkUnit` (`crates/cardano/src/ewrap/work_unit.rs`). Runs `total_shards` times in sequence (default 16), each scoped to a first-byte prefix bucket of the account key space.
- Purpose: apply rewards + drops for the accounts in this shard's range; accumulate the shard's contribution into `EpochState.end`.
- Deltas emitted (4 variants per shard run):
  - **Rewards** (`ewrap/rewards.rs`):
    - `AssignRewards:79` (one per rewarded account in range)
  - **Drops** (account-level, `ewrap/drops.rs`):
    - `PoolDelegatorRetire:32` *(also fires in EwrapPrepare)*
    - `DRepDelegatorDrop:53` *(also fires in EwrapPrepare)*
  - **Accumulator** (`ewrap/loading.rs`):
    - `EpochEndAccumulate:510` (single — rolls up the shard's `effective` / `unspendable_to_treasury` / `unspendable_to_reserves` totals into `EpochState.end`, advances `ewrap_progress`)
- Direct deletes: `PendingRewardState` entries for credentials whose rewards landed.
- Namespaces touched: `accounts` (shard range), `epochs`. Deletes from `pending_rewards` (shard range).

## 6. `EwrapFinalizeWorkUnit` — closes the epoch boundary

- Variants: `CardanoWorkUnit::EwrapFinalize` / `InternalWorkUnit::EwrapFinalize(BlockSlot)`.
- Struct: `ewrap::EwrapFinalizeWorkUnit` (`crates/cardano/src/ewrap/work_unit.rs`).
- Purpose: read the accumulated `EpochState.end`, emit the canonical wrap-up, write the completed epoch state to archive.
- Deltas emitted (1):
  - `EpochWrapUp` — `ewrap/loading.rs:527`. Transitions `rolling` and `pparams` snapshots forward, clears `ewrap_progress`. Targets `epochs`.
- Direct writes: writes the completed `EpochState` to the archive under the epoch-start temporal key.
- Namespaces touched: `epochs`.

## 7. `GenesisWorkUnit` — one-time chain bootstrap

- Variants: `CardanoWorkUnit::Genesis` / `InternalWorkUnit::Genesis`.
- Struct: `genesis::GenesisWorkUnit` (`crates/cardano/src/genesis/mod.rs`).
- Purpose: bootstrap state from genesis configs (Byron/Shelley/Alonzo/Conway).
- Deltas: **none**. Writes `EpochState` and `EraSummary` entities directly via `bootstrap_epoch` / `bootstrap_eras`.
- Namespaces touched: `epochs`, `eras`.

## 8. `ForcedStop` — sentinel

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
