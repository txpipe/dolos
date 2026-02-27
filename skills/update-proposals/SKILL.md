---
name: update-proposals
description: Update hardcoded Conway governance proposal mappings in hacks.rs by querying DBSync
---

# Update Hardcoded Proposal Mappings

Dolos doesn't implement DRep governance voting, so Conway proposal outcomes are hardcoded in `crates/cardano/src/hacks.rs`. When testing advances to new epochs, missing proposals cause wrong deposit refund timing and missed treasury withdrawals, leading to pot mismatches.

This skill walks through querying DBSync and updating hacks.rs accordingly.

## Step 1: Get the DBSync connection string

Read `xtask.toml` at the repo root. The `[dbsync]` section has connection URLs per network:

```toml
[dbsync]
mainnet_url = "postgresql://..."
preprod_url = "postgresql://..."
preview_url = "postgresql://..."
```

Use the appropriate URL for the target network.

## Step 2: Query all governance proposals

Run this against DBSync to get the full proposal lifecycle:

```sql
SELECT
    encode(tx.hash, 'hex') || '#' || gap.index::text AS proposal_id,
    gap.type::text AS proposal_type,
    b.epoch_no AS submitted_epoch,
    gap.enacted_epoch,
    gap.dropped_epoch,
    gap.expired_epoch
FROM gov_action_proposal gap
JOIN tx ON tx.id = gap.tx_id
JOIN block b ON b.id = tx.block_id
ORDER BY b.epoch_no, gap.index;
```

**Key columns:**
- `proposal_id`: The `txhash#index` format used in hacks.rs match arms
- `type`: `TreasuryWithdrawals`, `NewConstitution`, `NewCommittee`, `ParameterChange`, `HardForkInitiation`, `InfoAction`
- `enacted_epoch`: Non-null if the proposal was enacted on-chain
- `dropped_epoch`: Non-null if the proposal was dropped (superseded by another)
- `expired_epoch`: Non-null if the proposal expired without ratification

## Step 3: Query enacted proposals with expected Ratified epoch

```sql
SELECT
    encode(tx.hash, 'hex') || '#' || gap.index::text AS proposal_id,
    gap.type::text AS proposal_type,
    b.epoch_no AS submitted_epoch,
    gap.enacted_epoch,
    (gap.enacted_epoch - 1)::text AS expected_ratified
FROM gov_action_proposal gap
JOIN tx ON tx.id = gap.tx_id
JOIN block b ON b.id = tx.block_id
WHERE gap.enacted_epoch IS NOT NULL
ORDER BY gap.enacted_epoch, gap.index;
```

The formula is: **`Ratified(enacted_epoch - 1)`**. Ratification happens one epoch before enactment in Conway governance.

## Step 4: Cross-reference against hacks.rs

Read `crates/cardano/src/hacks.rs` and find the network's `outcome()` function (e.g., `pub mod mainnet`). Compare every enacted proposal from Step 3 against existing match arms.

**Check for:**
1. **Missing entries**: Enacted proposals not in hacks.rs. These return `Unknown`, never get enacted, and eventually expire -- causing wrong deposit refund timing and missed treasury withdrawals.
2. **Wrong Ratified epoch**: Existing entries where the `Ratified(N)` doesn't match `enacted_epoch - 1`.
3. **Still-pending proposals**: Proposals with no `enacted_epoch`, `dropped_epoch`, or `expired_epoch` yet. These correctly return `Unknown` and will need entries added later when resolved.

## Step 5: Determine if dropped/expired proposals need entries

Check dropped/expired proposals:

```sql
SELECT
    encode(tx.hash, 'hex') || '#' || gap.index::text AS proposal_id,
    gap.type::text AS proposal_type,
    b.epoch_no AS submitted_epoch,
    gap.dropped_epoch,
    gap.expired_epoch
FROM gov_action_proposal gap
JOIN tx ON tx.id = gap.tx_id
JOIN block b ON b.id = tx.block_id
WHERE gap.enacted_epoch IS NULL
  AND (gap.dropped_epoch IS NOT NULL OR gap.expired_epoch IS NOT NULL)
ORDER BY b.epoch_no, gap.index;
```

**Rules:**
- If `dropped_epoch = expired_epoch + 1` (natural expiry), no entry needed. The `Unknown` outcome lets them expire via `max_epoch`, matching DBSync timing.
- If a proposal was dropped early (superseded by another proposal of the same type), it may need `Canceled(epoch)` if the timing differs from natural expiry.
- `InfoAction` proposals never need entries (they have no on-chain effect).

## Step 6: Add entries to hacks.rs

Insert new match arms in the appropriate network module, **before** the `_ => match protocol` fallback. Group entries logically (by epoch or proposal type) and add a brief comment describing each:

```rust
// Treasury Withdrawal for Catalyst Fund 14
"03f671791fd97011f30e4d6b76c9a91f4f6bcfb60ee37e5399b9545bb3f2757a#0" => {
    Ratified(597)
}
```

**ProposalOutcome variants:**
- `Ratified(epoch)` -- proposal was ratified at this epoch, enacted at epoch+1
- `Canceled(epoch)` -- proposal was canceled/superseded at this epoch
- `RatifiedCurrentEpoch` -- pre-Conway (protocol 0-8) proposals that ratify in the epoch they're submitted
- `Unknown` -- default for unresolved proposals; they expire naturally via `max_epoch`

## Step 7: Verify

```bash
cargo check -p dolos-cardano
```

Then run epoch tests that cover the affected epoch range:

```bash
cargo test --test epoch_pots --release -- --nocapture
```

## Reference: DBSync gov_action_proposal table

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigint | Primary key |
| `tx_id` | bigint | FK to `tx` -- the transaction containing the proposal |
| `index` | int | Index of the proposal within the transaction |
| `type` | text | Proposal type enum |
| `enacted_epoch` | int | Epoch when enacted (null if not enacted) |
| `dropped_epoch` | int | Epoch when dropped/superseded (null if not dropped) |
| `expired_epoch` | int | Epoch when expired (null if not expired) |

## Reference: Network magic values

Used in `proposals::outcome()` dispatch:
- Mainnet: `764824073`
- Preprod: `1`
- Preview: `2`
