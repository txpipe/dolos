---
name: ledger-gotchas
description: Reference of non-obvious Cardano ledger behaviors that have caused bugs in Dolos. Consult when debugging epoch pots mismatches or implementing new ledger logic.
user-invocable: false
---

# Cardano Ledger Gotchas

Non-obvious behaviors in the Cardano ledger that differ from naive expectations. Each of these has caused at least one bug in Dolos. When debugging a mismatch or implementing new ledger logic, check this list first.

---

## Certificate Processing Order

**Gotcha**: There is NO priority system for certificates. The Haskell ledger processes certificates in the exact order they appear in the block (tx order, then cert order within tx). A deregistration in tx 7 followed by a registration in tx 10 results in the account being registered.

**What went wrong**: Dolos had a `priority()` method that sorted deltas (registration=0, deregistration=5), causing certificates to apply out of order. A dereg+reg in the same slot resulted in the account appearing deregistered.

**Rule**: Always apply deltas in natural block traversal order. Never sort or reorder them.

---

## NEWEPOCH Sub-Rule Ordering

**Gotcha**: The three NEWEPOCH sub-rules execute in this exact order:
1. `applyRUpd` -- rewards applied to accounts
2. `SNAP` -- new mark snapshot captured; future pool params become current
3. `POOLREAP` -- pools retired, deposits refunded

**Consequence**: SNAP happens BEFORE POOLREAP. Pool deposit refunds from POOLREAP are NOT captured in the mark snapshot for that transition. They first appear in the NEXT epoch's mark.

**Consequence**: SNAP moves future pool params to current BEFORE POOLREAP runs. So when POOLREAP refunds a deposit, it uses the **new** pool params (potentially a different reward account than when the pool was originally registered).

---

## EpochValue Snapshot Write Targets

**Gotcha**: Different operations write to different snapshot slots, which propagate to `mark` at different times.

| Operation | Writes to | Appears in mark |
|-----------|-----------|-----------------|
| `unwrap_live_mut()` | `live` | Next transition (E+1) |
| `scheduled_or_default()` | `next` | Two transitions later (E+2) |

EWRAP reward visitors use `unwrap_live_mut()` (writes to `live`). EWRAP refund visitors (pool deposits, proposal deposits) use `scheduled_or_default()` (writes to `next`). This means refunds and rewards propagate to the stake snapshot at different times.

---

## DBSync epoch_stake Mapping

**Gotcha**: `epoch_stake.epoch_no = N` corresponds to the **set** snapshot at epoch N, which equals the **mark** snapshot captured at NEWEPOCH N-1, which reflects account state as of the end of epoch N-2.

This is the stake distribution used for leader election in epoch N. When comparing dolos stake against DBSync at `performance_epoch = subject_epoch - 2`, the fixture filename is `stake-{performance_epoch}.csv`.

---

## Pre-Alonzo MIR Overwrite (protocol < 5)

**Gotcha**: Before Alonzo, MIR certificates for the same address OVERWRITE previous values rather than accumulating. Haskell uses `Map.union` (pre-Alonzo) vs `Map.unionWith (<>)` (Alonzo+).

**Example**: Two MIR certs for the same address: 100M then 32M. Pre-Alonzo result = 32M (last wins). Alonzo+ result = 132M (sum).

**Rule**: When `protocol_version < 5`, set `overwrite = true` on `EnqueueMir`. Only the last MIR per address per epoch is applied.

---

## Pre-Allegra Reward Deduplication (protocol < 3)

**Gotcha**: Before Allegra, each stake address receives at most ONE reward per epoch, even if delegated to multiple pools. The Haskell ledger uses `Set.deleteFindMin` to keep the minimum element per the `Ord Reward` instance.

**Ord Reward** rules:
1. `LeaderReward < MemberReward` (leader wins over member)
2. Same type: smaller pool ID (by hash bytes) wins

**Rule**: When `protocol < 3`, if an account has multiple rewards, keep the one with the smallest `(reward_type, pool_id)` tuple. Leader < Member, then lexicographic pool hash comparison.

---

## Eta Calculation: Pool Blocks Only

**Gotcha**: The eta (η) monetary expansion calculation uses only pool-produced blocks, NOT total blocks. Federated/OBFT blocks are excluded.

```
η = min(1, pool_blocks / ((1-d) × f × L))
```

The Haskell ledger's `BlocksMade` map only tracks stake pool blocks. Using total `blocks_minted` (which includes federated blocks) inflates η and produces ~2.75% higher rewards during the decentralization transition.

**Rule**: Sum `blocks_minted` from individual pool snapshots only. Do not use `rolling.mark().blocks_minted`.

---

## Unspendable Rewards: Two Filter Points, Two Destinations

**Gotcha**: Rewards for unregistered accounts are filtered at TWO different points, with different routing:

### Filter 1: At RUPD time (stability window)
- **Protocol < 7 (pre-Babbage)**: Unregistered accounts are excluded from reward calculation entirely (`hardforkBabbageForgoRewardPrefilter`). Their would-be rewards stay in reserves (never leave the reward pot).
- **Protocol >= 7 (Babbage+)**: Rewards are calculated for ALL accounts regardless of registration.

### Filter 2: At EWRAP time (epoch boundary)
- Accounts that were registered at RUPD but deregistered by EWRAP have their rewards routed to **treasury**. This happens regardless of protocol version.
- This matches `frTotalUnregistered → casTreasury` in `applyRUpdFiltered`.

**Rule**: EWRAP-filtered rewards ALWAYS go to treasury. Never route them to reserves based on protocol version.

---

## RUPD Fires at randomnessStabilisationWindow, Not stabilityWindow

**Gotcha**: RUPD fires at `4k/f` slots into the epoch (randomness stability window = 172,800 on mainnet), NOT at `3k/f` (stability window = 129,600).

Accounts that deregister between `3k/f` and `4k/f` are still registered when RUPD runs. If you use the wrong boundary, these accounts are incorrectly excluded from reward calculation.

---

## Pre-Conway Proposal Timing

**Gotcha**: Pre-Conway (protocol 0-8) update proposals usually follow a 1-epoch lag: ratified in the submission epoch, enacted at EWRAP, effect at ESTART of next epoch. The `RatifiedCurrentEpoch` fallback handles this.

**Exceptions**:
- Proposals submitted one epoch before their target d-parameter epoch need explicit `Ratified(target_epoch)` entries to avoid enacting one epoch too early.
- Split-quorum proposals (submitted across two epochs) need `Ratified()` set to the epoch where quorum was actually reached.
- The preview v7→v8 transition anomalously shows a 2-epoch lag.

---

## Conway Governance Proposals Must Be Hardcoded

**Gotcha**: Dolos doesn't implement DRep governance voting. All Conway proposal outcomes (ratified/canceled/expired) are hardcoded in `crates/cardano/src/hacks.rs`. Missing entries cause:
- Wrong deposit refund timing (proposal expires instead of being enacted)
- Missed treasury withdrawals
- Committee/constitution not updated

**Rule**: `Ratified(enacted_epoch - 1)` from DBSync's `gov_action_proposal.enacted_epoch`. Dropped proposals that expired naturally (dropped_epoch = expired_epoch + 1) don't need entries; the `Unknown` outcome lets them expire via `max_epoch`.

---

## Pool Deposit Refund Account

**Gotcha**: When a pool retires, the deposit is refunded to the pool's reward account. But pools can be re-registered with a different reward account. The refund goes to whichever reward account is current at POOLREAP time (after SNAP has already moved future params to current).

Check the full `pool_update` history in DBSync -- `active_epoch_no` determines when new params take effect. The last update active before the retirement epoch determines the refund recipient.

---

## Pointer Addresses

**Gotcha**: Pointer addresses encode `(slot, tx_idx, cert_idx)` to reference a stake registration certificate. Many on-chain pointer addresses contain intentionally garbage values (e.g., `(12, 12, 12)`) that point to nonexistent registrations. These map to `None` (no stake rights).

Dolos resolves pointers via a hardcoded lookup table in `hacks.rs`. New valid pointers must be resolved by querying DBSync's `stake_registration` table.

---

## Pointer Address Overflow Values

**Gotcha**: Pointer address components are encoded as variable-length integers in the address bytes, but Cardano decodes them as unbounded integers. Values can overflow `u64` or contain astronomically large numbers like `18446744073709551615` (u64::MAX) or `16292793057`. These are NOT necessarily invalid -- some overflow pointers resolve to real stake credentials on-chain.

**Example from mainnet**:
- `(18446744073709551615, 1221092, 2)` → maps to a VALID `AddrKeyhash`
- `(16292793057, 1011302, 20)` → maps to `None`

**Resolution**: The standard DBSync SQL query (`WHERE block.slot_no = <SLOT>`) won't work for overflow pointers since the slot is invalid. Instead, use a block explorer to look up the full bech32 address (`addr1g...`) -- explorers typically show the resolved stake address. Multiple pointer addresses can share the same payment credential but have different pointer tuples; each tuple needs its own mapping entry.

---

## Deposit Constants by Network

| Constant | Mainnet | Preprod/Preview |
|----------|---------|-----------------|
| Pool deposit | 500,000,000 lovelace | 500,000,000 lovelace |
| Key deposit | 2,000,000 lovelace | 2,000,000 lovelace |

When a delegation/stake diff is off by exactly one of these constants, it's almost certainly a deposit refund or registration timing issue.

---

## Pots Invariant

The total lovelace in the system is always exactly 45,000,000,000,000,000 (45 billion ADA):

```
reserves + treasury + utxos + rewards + fees + obligations = max_supply
```

If any pot is wrong, the error must appear somewhere else with opposite sign. Use this invariant to cross-check: if treasury is too low by X, then X must be too high in some other pot (usually reserves or rewards).

---

## Single Stake Error Cascades Into Many Reward Diffs

**Gotcha**: A single account's stake being off by N lovelace changes that pool's total stake, which changes the reward calculation for ALL the pool's delegators -- producing hundreds or thousands of ±1 lovelace rounding differences.

**How to recognize**:
- `stake` diff shows 1 account off by an exact amount (e.g., 3,001,337)
- `delegation` diff shows 1 pool's total off by the same amount
- `rewards` diff shows many ±1 differences, all for delegators of that same pool
- `epochs` diff shows reserves/rewards off by a small amount (net rounding effect)

**Rule**: When you see this pattern, ignore the reward diffs entirely. Find and fix the single stake root cause, and all diffs resolve together.

---

## Mismatch Diagnosis by Work Unit

**Gotcha**: The shape of a mismatch tells you which work unit is wrong:

| Symptom | Likely work unit |
|---------|-----------------|
| Equal and opposite deltas between rewards and treasury | EWRAP -- unspendable reward routing |
| Rewards match but pots don't | EWRAP/ESTART -- pot aggregation or unspendable handling |
| Only a subset of pools/accounts affected | ROLL -- registration/retirement window or pool parameter update timing |
| Rounded or exact small delta | RUPD/EWRAP -- conditional logic or boundary condition |
| Thousands of ±1 reward diffs for one pool | ROLL/ESTART -- single stake error cascade (see above) |
