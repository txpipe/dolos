---
name: debug-epoch-mismatch
description: Systematic workflow for debugging Cardano ledger epoch pots mismatches in Dolos. Use when an epoch test fails or treasury/reserves/rewards diverge from DBSync.
---

# Debug an Epoch Pots Mismatch

## Overall Approach

1. Reproduce the mismatch with a single-epoch test
2. Bisect to find the first failing epoch
3. Classify the mismatch by shape
4. Identify the root account/pool
5. Form a hypothesis and instrument to test it
6. Fix and verify with regressions

Always fix epochs in order. Start from the last known-good epoch. Do not jump ahead -- fixes to earlier epochs often resolve later ones.

## Epoch Nomenclature

These terms have precise meanings in the test harness and codebase:

| Term | Definition | Example (subject=250) |
|------|------------|-----------------------|
| `subject_epoch` | The epoch whose pots are compared | 250 |
| `stop_epoch` | `subject + 1`, where harness stops | 251 |
| `rupd_epoch` | `subject - 1`, when rewards are computed | 249 |
| `performance_epoch` | `subject - 2`, where pool performance is measured | 248 |
| `snapshot_epoch` | Same as performance_epoch, used in fixture filenames | 248 |

## Step 1: Reproduce

Run the specific failing test:

```bash
DOLOS_SEED_DIR=/path/to/seeds cargo test --test epoch_pots test_mainnet_250 --release -- --nocapture
```

Set `EPOCH_POTS_KEEP_DIR=1` to preserve the temp directory for inspection.

## Step 2: Bisect to First Failure

If you don't know where the mismatch starts:

1. Run a known-good epoch
2. Move forward in steps of 10 (e.g., 260, 270, 280, 290)
3. When a failure is found, bisect between last good and first bad
4. Create fixtures for each candidate with `/add-epoch-test`

This isolates when the mismatch class first appears. Different epoch ranges often reveal different bug classes.

## Step 3: Classify the Mismatch

The test compares 6 datasets. The failure pattern reveals the root cause:

| Pattern | Likely Cause | Work Unit |
|---------|-------------|-----------|
| Only `epochs` differs (treasury/reserves off) | Pot calculation -- unspendable routing, incentive formula, or delta field | EWRAP/ESTART |
| Equal and opposite delta between rewards and treasury | Unspendable reward routing issue | EWRAP |
| Rewards match but pots don't | Pot aggregation or unspendable handling | EWRAP/ESTART |
| `delegation` + `stake` + `rewards` + `epochs` all differ | Single stake discrepancy cascading | ROLL/ESTART |
| Thousands of small reward diffs (±1 lovelace) | One pool's total stake is wrong, rounding cascade | ROLL/ESTART |
| `rewards` has extra/missing rows | RUPD pre-filtering or EWRAP registration check | RUPD/EWRAP |
| `delegation` off by exactly 500,000,000 | Pool deposit refund timing | ROLL (POOLREAP) |
| `delegation` off by exactly 2,000,000 | Key deposit timing | ROLL |
| Only subset of pools/accounts affected | Registration/retirement window or pool param update | ROLL |

## Step 4: Identify the Root Account/Pool

When delegation or stake has differences:

1. Look at the `stake` diff first -- it shows the exact account and amount
2. Check if the amount matches a deposit constant (500M pool, 2M key)
3. Query DBSync for the account's stake history:

```sql
SELECT sa.view, es.epoch_no, es.amount::text
FROM epoch_stake es
JOIN stake_address sa ON sa.id = es.addr_id
WHERE sa.view = '<stake_address>'
AND es.epoch_no BETWEEN <N-3> AND <N+3>
ORDER BY es.epoch_no;
```

4. If amount jumps by exactly 500M, check pool retirements:

```sql
-- Is this account a pool reward account?
SELECT ph.view AS pool, pu.active_epoch_no
FROM pool_update pu
JOIN pool_hash ph ON ph.id = pu.hash_id
JOIN stake_address sa ON sa.id = pu.reward_addr_id
WHERE sa.view = '<stake_address>'
ORDER BY pu.active_epoch_no;

-- Did any of those pools retire?
SELECT ph.view, pr.retiring_epoch, b.epoch_no AS announced_epoch
FROM pool_retire pr
JOIN pool_hash ph ON ph.id = pr.hash_id
JOIN tx t ON t.id = pr.announced_tx_id
JOIN block b ON b.id = t.block_id
WHERE ph.view = '<pool_bech32>'
ORDER BY pr.retiring_epoch;
```

5. For reward extra/missing rows, check registration status around RUPD boundary:

```sql
-- Find deregistrations near RUPD boundary
SELECT sa.view, b.slot_no, b.epoch_no, t.block_index
FROM stake_deregistration sd
JOIN tx t ON t.id = sd.tx_id
JOIN block b ON b.id = t.block_id
JOIN stake_address sa ON sa.id = sd.addr_id
WHERE b.epoch_no = <subject_epoch - 1>
AND b.slot_no > <epoch_start + stability_window>
ORDER BY b.slot_no;
```

## Step 5: Map to Dolos Code

### Dolos Work Units → Haskell Concepts

| Dolos | Haskell | What it does |
|-------|---------|-------------|
| `ROLL` | Block processing | Applies transactions, certificates, updates pool/account state |
| `RUPD` | Reward calculation at stability window | Computes pending rewards using mark snapshot |
| `EWRAP` | `applyRUpd` + reward filtering | Applies rewards, filters unspendable, updates pots |
| `ESTART` | NEWEPOCH transition | Rotates snapshots, computes initial pots, creates new epoch |

### Key Source Files

| Area | Path |
|------|------|
| Pots & incentives | `crates/cardano/src/pots.rs` |
| Model types (EpochValue, etc.) | `crates/cardano/src/model.rs` |
| ESTART / epoch transition | `crates/cardano/src/estart/reset.rs` |
| EWRAP / reward application | `crates/cardano/src/ewrap/rewards.rs` |
| RUPD / reward calculation | `crates/cardano/src/rupd/loading.rs` |
| ROLL / certificate processing | `crates/cardano/src/roll/accounts.rs` |
| ROLL / batch delta application | `crates/cardano/src/roll/batch.rs` |
| Hardcoded hacks | `crates/cardano/src/hacks.rs` |
| Reward definition | `crates/cardano/src/rewards/mod.rs` |

### Haskell Ledger Reference

Source of truth for rules: `~/Code/IntersectMBO/cardano-ledger`

Focus areas:
- Reward calculation: `cardano-ledger-shelley/src/Cardano/Ledger/Shelley/Rewards.hs`
- Reward filtering: `cardano-ledger-shelley/src/Cardano/Ledger/Shelley/LedgerState/PulsingReward.hs`
- NEWEPOCH rule: `cardano-ledger-shelley/src/Cardano/Ledger/Shelley/Rules/NewEpoch.hs`
- Pot transitions: `cardano-ledger-shelley/src/Cardano/Ledger/Shelley/AdaPots.hs`
- Ord instance for Reward: `cardano-ledger-core/src/Cardano/Ledger/Rewards.hs`

When Dolos matches every epoch except a specific range, assume a pool or account-specific edge case rather than a systemic error.

## Step 6: Instrument to Test Hypothesis

Use targeted, temporary logs that point to a single hypothesis:

| Hypothesis | Where to instrument |
|------------|-------------------|
| Missing/wrong block attribution | Pool header data in ROLL visitor |
| Wrong pool block count | Pool block counts in RUPD loading |
| Wrong reward amounts | Reward map entries before EWRAP flush |
| Wrong pot delta | `apply_delta()` inputs in ESTART |
| Registration boundary issue | Account registration checks at RUPD/EWRAP boundary |

Guidelines:
- Use `eprintln!` for focused logs
- Always remove instrumentation after the hypothesis is tested
- Avoid noisy logs that make test output unusable
- Test one hypothesis at a time

## Step 7: Fix and Verify

After applying a fix:

1. Re-run ALL previously passing tests to check for regressions:
   ```bash
   DOLOS_SEED_DIR=/path/to/seeds cargo test --test epoch_pots --release -- --nocapture
   ```
2. Run the failing test that motivated the fix
3. Run the next few epochs to see if a new class of error appears

## DBSync Exploration Checklist

When investigating a failing epoch, check these in DBSync:

- [ ] Pool lifecycle: registrations, updates (new reward account?), retirements around the epoch
- [ ] Stake key registration/deregistration around RUPD and EWRAP boundaries
- [ ] Block counts per epoch, per pool (compare against dolos pool snapshots)
- [ ] Parameter changes around the mismatch epoch
- [ ] MIR certificates (pre-Alonzo: check for overwrites)
- [ ] Deposit refunds: pool retirements, proposal enactments

Connection strings are in `xtask.toml` under `[dbsync]`. Use `psql` directly or `cargo xtask ground-truth query`.
