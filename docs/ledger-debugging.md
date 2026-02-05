# Dolos Ledger Debugging Guide

This document summarizes our debugging workflow for Dolos ledger mismatches, with practical steps, tooling, and conceptual mapping to the Haskell Cardano ledger.

## How To Debug Dolos Ledger Issues Systematically

1. Reproduce the mismatch with a single-epoch integration test.
2. Identify the mismatch category (pots, rewards, fees, reserves/treasury, MIR, deposits/refunds, pool performance).
3. Derive the relevant epochs (subject, stop, rupd, performance, snapshot) and focus on those boundaries.
4. Compare Dolos outputs against ground truth CSV fixtures.
5. Use DBSync to validate chain data and lifecycle events (stake keys, pool registrations, retirements, parameter changes).
6. Use Haskell ledger as the source of truth for rules and edge cases.
7. Instrument targeted logs to test a single hypothesis at a time.
8. Remove instrumentation once the hypothesis is resolved or ruled out.

## How To Create And Run Tests

### Create Ground Truth (Fixtures)

Use `xtask` to generate fixtures for the target epoch (subject epoch) and network.

```bash
cargo run -p xtask -- ground-truth generate --network mainnet --subject-epoch <EPOCH> --output-dir tests/epoch_pots/fixtures/mainnet-<EPOCH>
```

Expected files per epoch (example):

- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/epochs.csv`
- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/pparams.csv`
- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/eras.csv`
- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/delegation-<SNAPSHOT_EPOCH>.csv`
- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/stake-<SNAPSHOT_EPOCH>.csv`
- `tests/epoch_pots/fixtures/mainnet-<EPOCH>/rewards.csv`

### Add Test Entry

Add the fixtures to `tests/epoch_pots/main.rs` and add a test function:

```rust
#[test]
fn test_mainnet_<EPOCH>() {
    init_tracing();
    let seed_dir = seed_dir_for("mainnet", <EPOCH>);
    run_epoch_pots_test(
        "mainnet",
        <EPOCH>,
        &seed_dir,
        fixtures::mainnet_<EPOCH>::EPOCHS,
        fixtures::mainnet_<EPOCH>::PPARAMS,
        fixtures::mainnet_<EPOCH>::ERAS,
        fixtures::mainnet_<EPOCH>::DELEGATION,
        fixtures::mainnet_<EPOCH>::STAKE,
        fixtures::mainnet_<EPOCH>::REWARDS,
    )
    .unwrap();
}
```

### Run a Single Test

Tests are expensive. Always run the specific epoch only, and always use `--release` for speed.

```bash
cargo test --release --test epoch_pots test_mainnet_<EPOCH>
```

You can keep the temporary test dir by setting the environment variable used by the test harness (this was previously added and should be preserved).

### Seed Selection (Per-Test)

Epoch tests select their seed folder per test. The base directory is defined by the `DOLOS_SEED_DIR` environment variable, and the test chooses the nearest lower seed epoch for the target network/subject epoch from a small per-network list in the test file.

Expected layout:

- `$DOLOS_SEED_DIR/mainnet-<SEED_EPOCH>/state`
- `$DOLOS_SEED_DIR/preview-<SEED_EPOCH>/state`
- `$DOLOS_SEED_DIR/preprod-<SEED_EPOCH>/state`

Example: a mainnet test for epoch 250 should use the mainnet 200 seed (`mainnet-200`), while a test for epoch 300 should use the mainnet 270 seed (`mainnet-270`).

## How To Use DBSync As An Exploration Tool

Use DBSync to answer ground-truth questions around the failing epoch:

- Pool lifecycle (registration, updates, retirements).
- Stake key registration/deregistration around RUPD and EWRAP.
- Block counts per epoch or per pool.
- Parameter changes around the mismatch epoch.
- MIR and deposit refunds.

DBSync is the best quick reference for chain data that Dolos should be consuming.

Tips:

- Query by epoch boundary using block slot or block height.
- Validate block counts and pool blocks to confirm reward performance inputs.
- Validate pool/credential status transitions relative to RUPD and EWRAP epochs.

## How To Use The Haskell Node As Reference

Use the Cardano ledger code as the source of truth for rules:

- Path: `~/Code/IntersectMBO/cardano-ledger`
- Focus on reward calculation rules, registration boundary conditions, MIR handling, and pot deltas.
- Align Dolos logic to the exact edge cases in the Haskell rules.

When Dolos matches every epoch except a specific range, assume a pool or account-specific edge case rather than a systemic error.

## How To Instrument Tests To Try Hypotheses

Use targeted, temporary logs that point to a single hypothesis:

- Log block discovery and decoding in the sync layer for missing block hypotheses.
- Log pool header data in the roll visitor if block issuer attribution is suspected.
- Log pool block counts in RUPD when performance is suspected.
- Log reward map entries before EWRAP flush when pot deltas are mismatched.

Guidelines:

- Use `eprintln!` for focused logs when needed.
- Always remove instrumentation after the hypothesis is tested.
- Avoid noisy logs that make the test output unusable.

## Epoch Nomenclature

We use the following terms:

- `subject_epoch`: the epoch whose pots are being compared.
- `stop_epoch`: `subject_epoch + 1`, where the harness stops just before ESTART.
- `rupd_epoch`: `subject_epoch - 1`, when rewards for the subject epoch are computed.
- `performance_epoch`: `rupd_epoch - 1`, where pool performance is measured.
- `snapshot_epoch` / `stake_epoch`: `performance_epoch - 2`, where stake snapshot is computed.

## Dolos Concepts vs Haskell Concepts

- Dolos `RUPD` corresponds to reward calculation at stability window.
- Dolos `EWRAP` corresponds to applying rewards and updating pots.
- Dolos `ESTART` corresponds to epoch transition and pot initialization.
- Dolos `ROLL` corresponds to block processing with ledger state updates.
- Dolos `EpochState` mirrors Haskell’s epoch-level derived data.

Key conceptual mapping:

- Pending rewards in Dolos correspond to ledger reward calculations before being applied.
- Pot deltas in Dolos mirror ledger pot transitions in Haskell.
- Account registration/retirement logic must match Haskell boundary conditions exactly.

## How The Dolos Work Unit System Works

Dolos processes data via `WorkUnit` lifecycle in `dolos-core`:

1. `load()`
2. `compute()`
3. `commit_wal()`
4. `commit_state()`
5. `commit_archive()`
6. `commit_indexes()`

Cardano work units in `dolos-cardano` include:

- `GenesisWorkUnit`
- `RollWorkUnit`
- `RupdWorkUnit`
- `EwrapWorkUnit`
- `EstartWorkUnit`

The executor in `dolos-core` (sync/import) drives these lifecycle steps and determines which stores are touched.

## How To Explore Epochs Sequentially And Bisect Gaps

Start with known-good epochs and move forward in steps.

Suggested process:

1. Run a known-good epoch.
2. Move forward in steps of 10 (e.g., 260, 270, 280, 290).
3. When a failure is found, bisect between the last good and first bad epoch.
4. Create fixtures for each candidate epoch and run only that test.

This keeps run time manageable and isolates when the mismatch first appears.

## How To Reason About Mismatches

Start by classifying the mismatch:

- Rewards pot mismatch
- Treasury/reserves mismatch
- Total incentives mismatch
- MIR or deposit refunds
- Stake/pool parameter mismatch

Then map it to the relevant work unit:

- RUPD issues affect pending rewards and incentives.
- EWRAP issues affect application of rewards and pot movements.
- ESTART issues affect initial pot deltas and epoch boundaries.
- ROLL issues affect chain data, pool stats, and block attribution.

Interpretation tips:

- Equal and opposite deltas between rewards and treasury often indicate unspendable routing issues.
- Rounded or exact small deltas often indicate ad-hoc conditional logic or boundary conditions.
- If spendable rewards match but pots don’t, the issue is likely in unspendable handling or pot aggregation.
- If only a subset of pools/accounts is affected, focus on registration/retirement windows and pool parameter updates.
