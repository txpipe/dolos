//! Deterministic reproduction harness for #1018 (epoch-boundary resume is not
//! idempotent — persisted pool-snapshot lag).
//!
//! The goal is a self-contained, deterministic test that crosses a *real*
//! epoch boundary with actual `PoolState`/`AccountState`/`EpochState`,
//! interrupts the boundary two ways, and asserts per-entity epochs against
//! `EpochState.number` — classifying **lead** vs **lag** so the exact
//! mechanism is captured (the issue's open question).
//!
//! Devnet genesis has `epochLength = 432000`, so we shrink the genesis to a
//! tiny but coherent world (`epoch_length`, byron `k`, `active_slots_coeff`)
//! where the randomness/stability windows land *inside* the epoch and the
//! boundary work units (RUPD/EWRAP/ESTART) fire after a few hundred blocks.

use std::collections::BTreeMap;
use std::sync::Arc;

use dolos_cardano::model::accounts::AccountState;
use dolos_cardano::model::pools::PoolState;
use dolos_cardano::model::FixedNamespace as _;
use dolos_cardano::{load_epoch, CardanoWorkUnit};
use dolos_core::sync::SyncExt as _;
use dolos_core::{ChainLogic, ChainPoint, Domain, StateStore as _, WorkUnit};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};

/// Epoch length (slots) for the shrunken test world. Synthetic blocks advance
/// one slot per block, so boundaries fall at slots 100, 200, 300, ...
const TINY_EPOCH_LENGTH: u32 = 100;

/// These tests must run in release. The synthetic block generator funds tx
/// fees and registration deposits from `custom_utxos` that are injected into
/// the UTxO set but not backed by the genesis pots — so crossing an epoch
/// boundary trips the `pots.is_consistent` **debug_assert** in
/// `estart/reset.rs`. That invariant lives in the monetary-accounting
/// subsystem, which is entirely orthogonal to the entity snapshot-rotation
/// logic these tests exercise (`EpochValue` epoch vs `EpochState.number`).
/// Running in release compiles out the unrelated debug_assert; the snapshot
/// measurements remain valid. (Heavy ledger tests here already run `--release`,
/// e.g. `epoch_pots`.) In debug we skip with a clear message instead of
/// failing on an unrelated invariant.
fn require_release() -> bool {
    if cfg!(debug_assertions) {
        eprintln!(
            "SKIP: boundary_resume tests require --release \
             (synthetic pots debug_assert is unrelated to the snapshot-lag bug under test)"
        );
        return false;
    }
    true
}

/// Build a tiny but internally consistent genesis. With `k = 1` and
/// `f = 0.05`, `randomness_stability_window = 4k/f = 80` and
/// `stability_window = 3k/f = 60`, both comfortably inside a 100-slot epoch,
/// so RUPD (fires at 4k/f into the epoch) and EWRAP/ESTART (epoch end) all run.
fn tiny_genesis() -> dolos_core::Genesis {
    let mut g = dolos_cardano::include::devnet::load();
    g.shelley.epoch_length = Some(TINY_EPOCH_LENGTH);
    g.shelley.security_param = Some(1);
    g.shelley.active_slots_coeff = Some(0.05);
    g.byron.protocol_consts.k = 1;
    g
}

/// Synthetic blocks crossing several epoch boundaries, paired with the config
/// that carries the seed UTxOs the txs consume. `block_count` blocks span
/// `block_count / TINY_EPOCH_LENGTH` epochs.
fn synthetic_world(block_count: usize) -> (Vec<dolos_core::RawBlock>, ToyDomain) {
    let cfg = SyntheticBlockConfig {
        block_count,
        slot: 1,
        start_block: 1,
        ..Default::default()
    };
    let (blocks, _vectors, mut cardano_config) = build_synthetic_blocks(cfg);
    // Baseline/scenarios drive all blocks; never force-stop mid-run.
    cardano_config.stop_epoch = None;

    let genesis = Arc::new(tiny_genesis());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, cardano_config, None, None);
    (blocks, domain)
}

/// Per-entity epoch fingerprint of the ledger, used to compare an interrupted
/// run against a clean one and to classify drift.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    epoch: u64,
    /// `EpochState.initial_pots` as (reserves, treasury, utxos, rewards, fees).
    /// Captures monetary-accounting corruption (e.g. a skipped EWRAP finalize
    /// that leaves `end.epoch_incentives` unapplied at ESTART reset).
    pots: (u64, u64, u64, u64, u64),
    /// hex(pool key) -> pool `snapshot.epoch()`
    pools: BTreeMap<String, Option<u64>>,
    /// hex(account key) -> account `stake.epoch()`
    accounts: BTreeMap<String, Option<u64>>,
}

fn fingerprint(domain: &ToyDomain) -> Fingerprint {
    let epoch_state = load_epoch::<ToyDomain>(domain.state()).unwrap();
    let epoch = epoch_state.number;
    let p = &epoch_state.initial_pots;
    let pots = (p.reserves, p.treasury, p.utxos, p.rewards, p.fees);

    let mut pools = BTreeMap::new();
    for item in domain
        .state()
        .iter_entities_typed::<PoolState>(PoolState::NS, None)
        .unwrap()
    {
        let (key, pool) = item.unwrap();
        pools.insert(hex::encode(key.as_ref()), pool.snapshot.epoch());
    }

    let mut accounts = BTreeMap::new();
    for item in domain
        .state()
        .iter_entities_typed::<AccountState>(AccountState::NS, None)
        .unwrap()
    {
        let (key, account) = item.unwrap();
        accounts.insert(hex::encode(key.as_ref()), account.stake.epoch());
    }

    Fingerprint {
        epoch,
        pots,
        pools,
        accounts,
    }
}

/// Human-readable classification of every entity whose epoch != the ledger
/// epoch, labelling each as `lag` (behind) or `lead` (ahead). Empty string
/// means fully aligned.
fn classify_drift(fp: &Fingerprint) -> String {
    let mut out = Vec::new();
    let mut check = |kind: &str, map: &BTreeMap<String, Option<u64>>| {
        for (key, ent_epoch) in map {
            match ent_epoch {
                Some(e) if *e == fp.epoch => {}
                Some(e) if *e < fp.epoch => {
                    out.push(format!("{kind} {key}: LAG (entity={e}, ledger={})", fp.epoch))
                }
                Some(e) => {
                    out.push(format!("{kind} {key}: LEAD (entity={e}, ledger={})", fp.epoch))
                }
                None => out.push(format!("{kind} {key}: GENESIS (ledger={})", fp.epoch)),
            }
        }
    };
    check("pool", &fp.pools);
    check("account", &fp.accounts);
    out.join("\n")
}

/// Run one shard's commit phases (no WAL — the import path).
fn run_shard(work: &mut CardanoWorkUnit, domain: &ToyDomain, shard: u32) {
    WorkUnit::<ToyDomain>::load(work, domain, shard).unwrap();
    WorkUnit::<ToyDomain>::compute(work, shard).unwrap();
    WorkUnit::<ToyDomain>::commit_state(work, domain, shard).unwrap();
    WorkUnit::<ToyDomain>::commit_archive(work, domain, shard).unwrap();
    WorkUnit::<ToyDomain>::commit_indexes(work, domain, shard).unwrap();
}

/// Run the full import lifecycle for a single work unit: initialize, every
/// shard (load/compute/commit_state/archive/indexes), then finalize. Mirrors
/// `core::sync::run_lifecycle` with `include_wal = false` (the import path).
fn run_work_unit_full(work: &mut CardanoWorkUnit, domain: &ToyDomain) {
    WorkUnit::<ToyDomain>::initialize(work, domain).unwrap();
    let total = WorkUnit::<ToyDomain>::total_shards(work);
    let start = WorkUnit::<ToyDomain>::start_shard(work);
    for shard in start..total {
        run_shard(work, domain, shard);
    }
    WorkUnit::<ToyDomain>::finalize(work, domain).unwrap();
}

/// Simulate a crash mid-ESTART, then resume — exercising the shard-resume
/// path under test. Commits shards `0..k` (each `commit_state` advances
/// `EpochState.estart_progress.committed`), then **stops without finalize**
/// (the "crash": the cursor only advances at finalize, so nothing past the
/// boundary is durable). Resume re-runs `initialize` on the same unit — which
/// re-reads `estart_progress` and sets `start_shard = committed` — then
/// completes the remaining shards and finalize.
///
/// This isolates ESTART (where pools/epoch rotate). A real process restart
/// would also re-derive and re-run EWRAP from the persisted cursor; EWRAP does
/// not rotate pool snapshots, so it is out of scope for the pool-snapshot-lag
/// question and intentionally not re-run here.
fn run_estart_crash_resume(work: &mut CardanoWorkUnit, domain: &ToyDomain, k: u32) {
    // --- crash leg: commit shards 0..k, no finalize ---
    WorkUnit::<ToyDomain>::initialize(work, domain).unwrap();
    let total = WorkUnit::<ToyDomain>::total_shards(work);
    let k = k.min(total);
    for shard in 0..k {
        run_shard(work, domain, shard);
    }

    // --- resume leg: re-init reads persisted progress, finish + finalize ---
    WorkUnit::<ToyDomain>::initialize(work, domain).unwrap();
    let total = WorkUnit::<ToyDomain>::total_shards(work);
    let start = WorkUnit::<ToyDomain>::start_shard(work);
    assert_eq!(
        start, k,
        "resume must skip the {k} already-committed shards (estart_progress.committed)"
    );
    for shard in start..total {
        run_shard(work, domain, shard);
    }
    WorkUnit::<ToyDomain>::finalize(work, domain).unwrap();
}

/// Drain all pending work through the full import lifecycle.
fn drain_import_full(chain: &mut dolos_cardano::CardanoLogic, domain: &ToyDomain) {
    while let Some(mut work) =
        <dolos_cardano::CardanoLogic as ChainLogic>::pop_work::<ToyDomain>(chain, domain)
    {
        run_work_unit_full(&mut work, domain);
    }
}

/// Feed every block through the import lifecycle (no WAL, no interruption).
fn feed_import_full(domain: &ToyDomain, blocks: &[dolos_core::RawBlock]) {
    let mut chain = domain.write_chain();
    for block in blocks {
        if !chain.can_receive_block() {
            drain_import_full(&mut chain, domain);
        }
        chain.receive_block(block.clone()).unwrap();
    }
    drain_import_full(&mut chain, domain);
}

// ---------------------------------------------------------------------------
// De-risking baseline: a clean run must cross real boundaries with aligned
// entities. This is the riskiest step (synthetic blocks have never crossed a
// real boundary before), so it gets its own test.
// ---------------------------------------------------------------------------

#[test]
fn baseline_clean_run_crosses_boundaries_aligned() {
    if !require_release() {
        return;
    }
    let (blocks, domain) = synthetic_world(260);

    feed_import_full(&domain, &blocks);

    let fp = fingerprint(&domain);

    assert!(
        fp.epoch >= 2,
        "clean run should cross >=2 epoch boundaries, got epoch {}",
        fp.epoch
    );
    assert!(
        !fp.pools.is_empty(),
        "synthetic blocks should register at least one pool"
    );

    let drift = classify_drift(&fp);
    assert!(
        drift.is_empty(),
        "clean run must leave every entity aligned with the ledger epoch:\n{drift}"
    );
}

// ---------------------------------------------------------------------------
// Scenario A — import crash mid-boundary then resume.
//
// Drives blocks through the import lifecycle, but the FIRST ESTART boundary is
// crashed mid-shard and resumed (`run_estart_crash_resume`). The final state
// must be byte-identical to an uninterrupted run.
//
// Expectation (per the #1018 evaluation): this PASSES — the shard-skip
// (`start_shard = estart_progress.committed`) plus the finalize-once guard mean
// no shard is applied twice and the boundary completes exactly once. A pass
// refutes the import path as the source of the reported *lag* (which is itself
// a recorded finding). A failure would surface a lead/lag with classification.
// ---------------------------------------------------------------------------

/// Drain pending work, crashing+resuming the first ESTART encountered.
fn drain_import_first_estart_crash(
    chain: &mut dolos_cardano::CardanoLogic,
    domain: &ToyDomain,
    crashed: &mut bool,
    k: u32,
) {
    while let Some(mut work) =
        <dolos_cardano::CardanoLogic as ChainLogic>::pop_work::<ToyDomain>(chain, domain)
    {
        if !*crashed && matches!(work, CardanoWorkUnit::Estart(_)) {
            *crashed = true;
            run_estart_crash_resume(&mut work, domain, k);
        } else {
            run_work_unit_full(&mut work, domain);
        }
    }
}

fn feed_import_first_estart_crash(domain: &ToyDomain, blocks: &[dolos_core::RawBlock], k: u32) -> bool {
    let mut crashed = false;
    let mut chain = domain.write_chain();
    for block in blocks {
        if !chain.can_receive_block() {
            drain_import_first_estart_crash(&mut chain, domain, &mut crashed, k);
        }
        chain.receive_block(block.clone()).unwrap();
    }
    drain_import_first_estart_crash(&mut chain, domain, &mut crashed, k);
    crashed
}

#[test]
fn import_crash_mid_estart_resumes_to_identical_state() {
    if !require_release() {
        return;
    }

    // Clean reference run.
    let (blocks, clean) = synthetic_world(260);
    feed_import_full(&clean, &blocks);
    let fp_clean = fingerprint(&clean);

    // Identical run, but crash+resume the first ESTART at shard 16/32.
    let (blocks2, crashed_domain) = synthetic_world(260);
    let did_crash = feed_import_first_estart_crash(&crashed_domain, &blocks2, 16);
    assert!(did_crash, "test should have injected a crash at the first ESTART");
    let fp_crash = fingerprint(&crashed_domain);

    assert_eq!(
        fp_crash, fp_clean,
        "import crash+resume must reproduce uninterrupted state.\n\
         clean   epoch={}\n\
         crashed epoch={}\n\
         drift in crashed run:\n{}",
        fp_clean.epoch,
        fp_crash.epoch,
        classify_drift(&fp_crash),
    );
}

// ---------------------------------------------------------------------------
// Scenario B — rollback across an epoch boundary, then re-apply.
//
// Uses the full SYNC lifecycle (`roll_forward`, WAL on) like the live driver
// (`src/sync/apply.rs`: rollback then roll_forward, no buffer reset). Crosses
// the first boundary (slot 100) into epoch 1, rolls back to a point in epoch 0,
// then re-applies to the same tip; the result must be byte-identical to an
// uninterrupted run.
//
// REPRODUCES #1018 — currently FAILS, hence `#[ignore]`. Confirmed mechanism:
// boundary transitions (`EpochTransition`/`PoolTransition`/`AccountTransition`)
// are NOT written to the WAL (only ROLL block deltas are — see
// `roll/batch.rs::commit_wal`; EWRAP/RUPD/ESTART use the default no-op
// `commit_wal`). So `rollback` (the only caller of `delta.undo()`, in
// `core/sync.rs`) iterates WAL logs and undoes block deltas but CANNOT undo the
// boundary transitions. Observed: after rolling back to a slot in epoch 0,
// `EpochState.number` stays at 1 and the pool snapshot stays at epoch 1 (the
// boundary was not reverted); re-applying then re-fires the boundary and
// double-advances every entity (here to epoch 4 vs the correct 2) — silent
// corruption, no guard fires. This is the same WAL/rollback asymmetry behind
// the reported pool-snapshot *lag*; the manifestation here is a *lead* (entity
// epochs over-advance in lockstep with the inflated `EpochState.number`).
//
// Un-ignore once boundary transitions are reversible on rollback (e.g.
// WAL-backed, or rollback re-derives boundary state) — see #1018 Step 3.
// ---------------------------------------------------------------------------

fn point_of(raw: &dolos_core::RawBlock) -> ChainPoint {
    let block = pallas::ledger::traverse::MultiEraBlock::decode(raw).unwrap();
    ChainPoint::Specific(block.slot(), block.hash())
}

#[test]
#[ignore = "reproduces #1018: rollback across an epoch boundary is not idempotent (un-ignore once fixed)"]
fn rollback_across_boundary_reapplies_to_identical_state() {
    if !require_release() {
        return;
    }

    // Clean reference run via the sync lifecycle.
    let (blocks, clean) = synthetic_world(260);
    for b in &blocks {
        clean.roll_forward(b.clone()).unwrap();
    }
    let fp_clean = fingerprint(&clean);

    // Rollback run: cross the first boundary (slot 100) into epoch 1...
    let (blocks2, dom) = synthetic_world(260);
    for b in &blocks2[..130] {
        dom.roll_forward(b.clone()).unwrap();
    }

    // ...roll back to a point in epoch 0 (block index 79 == slot 80)...
    let target = point_of(&blocks2[79]);
    let target_epoch = target.slot() / u64::from(TINY_EPOCH_LENGTH); // == 0
    dom.rollback(&target).unwrap();

    // Direct, crisp demonstration of the defect: rolling back into epoch 0 must
    // also revert the boundary transition, so the ledger epoch should be 0.
    // Today it stays at 1 because the boundary transition isn't in the WAL.
    let after_rb = fingerprint(&dom);
    assert_eq!(
        after_rb.epoch, target_epoch,
        "rollback to slot {} (epoch {target_epoch}) must revert the boundary transition, \
         but EpochState.number is {} — the boundary transition was not undone (not in WAL)",
        target.slot(),
        after_rb.epoch,
    );

    // ...then re-apply to the same tip.
    for b in &blocks2[80..] {
        dom.roll_forward(b.clone()).unwrap();
    }
    let fp_rb = fingerprint(&dom);

    assert_eq!(
        fp_rb, fp_clean,
        "rollback across a boundary + re-apply must reproduce uninterrupted state, \
         but diverged: clean epoch={}, rollback epoch={} ({}). \
         Boundary transitions are not reverted on rollback and re-fire on re-apply.",
        fp_clean.epoch,
        fp_rb.epoch,
        if fp_rb.epoch > fp_clean.epoch {
            "LEAD: epoch over-advanced"
        } else if fp_rb.epoch < fp_clean.epoch {
            "LAG: epoch under-advanced"
        } else {
            "epoch matches; entity-level divergence"
        },
    );
}

// ---------------------------------------------------------------------------
// Scenario C — import crash in the EWRAP *finalize window*.
//
// The last EWRAP shard sets ewrap_progress.committed == total
// (EWrapProgress::apply). EpochWrapUpV3 (the finalize delta that assembles the
// final EndStats and rotates rolling/pparams) runs in a SEPARATE commit and
// does not touch ewrap_progress. On restart, EwrapWorkUnit::initialize reads
// committed == total -> is_complete() -> skips both shards AND finalize.
//
// So a crash in [last EWRAP shard committed, before EpochWrapUpV3 commits]
// permanently skips EpochWrapUpV3 on resume. This test commits every EWRAP
// shard, stops before finalize, then resumes (re-init sees committed == total
// and short-circuits finalize), then lets ESTART run — and checks the result
// against a clean run. ESTART's reset consumes end.epoch_incentives, so if the
// finalize was wrongly skipped the pots diverge.
//
// Targets the 2nd boundary (epoch 1->2), where incentives are non-zero.
// ---------------------------------------------------------------------------

/// Crash in the EWRAP finalize window, then resume on the same work unit.
fn run_ewrap_finalize_window_crash_resume(work: &mut CardanoWorkUnit, domain: &ToyDomain) {
    // crash leg: commit ALL shards (committed -> total), no finalize.
    WorkUnit::<ToyDomain>::initialize(work, domain).unwrap();
    let total = WorkUnit::<ToyDomain>::total_shards(work);
    for shard in 0..total {
        run_shard(work, domain, shard);
    }
    // resume leg: re-init now reads committed == total.
    WorkUnit::<ToyDomain>::initialize(work, domain).unwrap();
    let total = WorkUnit::<ToyDomain>::total_shards(work);
    let start = WorkUnit::<ToyDomain>::start_shard(work);
    assert_eq!(
        start, total,
        "resumed EWRAP should see committed == total (finalize window)"
    );
    for shard in start..total {
        run_shard(work, domain, shard); // empty range
    }
    // is_complete() -> finalize short-circuits (EpochWrapUpV3 skipped).
    WorkUnit::<ToyDomain>::finalize(work, domain).unwrap();
}

fn feed_import_nth_ewrap_finalize_crash(
    domain: &ToyDomain,
    blocks: &[dolos_core::RawBlock],
    nth: u32,
) -> bool {
    fn drain(
        chain: &mut dolos_cardano::CardanoLogic,
        domain: &ToyDomain,
        seen: &mut u32,
        crashed: &mut bool,
        nth: u32,
    ) {
        while let Some(mut work) =
            <dolos_cardano::CardanoLogic as ChainLogic>::pop_work::<ToyDomain>(chain, domain)
        {
            if matches!(work, CardanoWorkUnit::Ewrap(_)) {
                *seen += 1;
                if !*crashed && *seen == nth {
                    *crashed = true;
                    run_ewrap_finalize_window_crash_resume(&mut work, domain);
                    continue;
                }
            }
            run_work_unit_full(&mut work, domain);
        }
    }

    let mut seen = 0;
    let mut crashed = false;
    let mut chain = domain.write_chain();
    for block in blocks {
        if !chain.can_receive_block() {
            drain(&mut chain, domain, &mut seen, &mut crashed, nth);
        }
        chain.receive_block(block.clone()).unwrap();
    }
    drain(&mut chain, domain, &mut seen, &mut crashed, nth);
    crashed
}

#[test]
#[ignore = "reproduces #1018: import crash in the EWRAP finalize window skips EpochWrapUpV3 on resume (un-ignore once fixed)"]
fn import_crash_ewrap_finalize_window_resumes_to_identical_state() {
    if !require_release() {
        return;
    }

    let (blocks, clean) = synthetic_world(260);
    feed_import_full(&clean, &blocks);
    let fp_clean = fingerprint(&clean);

    let (blocks2, dom) = synthetic_world(260);
    let crashed = feed_import_nth_ewrap_finalize_crash(&dom, &blocks2, 2);
    assert!(crashed, "test should have crashed the 2nd EWRAP");
    let fp = fingerprint(&dom);

    assert_eq!(
        fp, fp_clean,
        "import crash in the EWRAP finalize window must resume to identical state, but diverged.\n\
         clean   epoch={} pots(reserves,treasury,utxos,rewards,fees)={:?}\n\
         crashed epoch={} pots(reserves,treasury,utxos,rewards,fees)={:?}\n\
         The crashed run skipped EpochWrapUpV3 (committed==total was set by the last shard, \
         one commit before finalize), so epoch incentives + rolling rotation were never applied \
         at ESTART reset: reserves stay too high and treasury too low.",
        fp_clean.epoch, fp_clean.pots, fp.epoch, fp.pots,
    );
}
