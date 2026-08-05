//! A live node for the two drivers to run against.
//!
//! Shared by the export and restore suites so both are pointed at the same
//! ledger: a driver pair whose tests disagreed about what a node contains would
//! prove nothing about the pair.
//!
//! Distinct from `tests/common`, which is a hand-pinned stele of literals for
//! the goldens. This module is the opposite kind of fixture — everything in it
//! comes out of `dolos-testing`'s seeder and a real ledger computation.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::sync::Arc;

use dolos_core::{import::ImportExt as _, Domain as _, StateStore as _};
use dolos_snapshot::export::{self, Plan};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, seed_epoch_logs, seed_reward_logs, SyntheticBlockConfig},
    toy_domain::{ToyDomain, ToyStores},
};
use stelae::{dir::SteleDir, inscription::Inscription};

/// Preview genesis, synthetic blocks and seeded logs, all inside epoch zero.
///
/// **Epoch-coherent on purpose.** The blocks stay in the epoch genesis stamped,
/// so the ledger's `EpochValue`s and the chain agree throughout. The service
/// crates' fixtures jump from genesis straight to epoch two, which leaves the
/// two disagreeing and trips the `strict` coherence assertions in
/// `dolos-cardano`; CI excludes those crates from the all-features run for that
/// reason. An export has to run on a coherent ledger to mean anything, so this
/// fixture stays inside one epoch rather than joining the exclusion list.
///
/// The cost is that a harness stele covers a single epoch. That is not where
/// multi-epoch geometry is checked in any case: it is arithmetic over a
/// `ChainSummary`, and it is pinned by the three-epoch skeleton golden in
/// `tests/export.rs` and by `Plan`'s own unit tests. What the fixture is for is
/// *records* — a real UTxO set, real archive tags and exact records, real
/// logs — and one epoch holds all of those.
///
/// Deterministic: two calls build byte-identical ledgers, which is what lets
/// the restore suite cross-check against an independently built node.
pub fn harness<B: ToyStores>() -> ToyDomain<B> {
    let genesis = Arc::new(dolos_cardano::include::preview::load());

    let cfg = SyntheticBlockConfig {
        block_count: 5,
        txs_per_block: 3,
        slot: 100,
        ..Default::default()
    };

    let (blocks, vectors, chain_config) = build_synthetic_blocks(cfg);

    let domain: ToyDomain<B> = ToyDomain::with_backend(genesis, chain_config, None, None);
    domain.import_blocks(blocks).unwrap();

    let summary = dolos_cardano::eras::load_era_summary::<ToyDomain<B>>(domain.state()).unwrap();
    let tip = domain.state().read_cursor().unwrap().unwrap();
    let (epoch, _) = summary.slot_epoch(tip.slot());

    assert_eq!(epoch, 0, "the fixture left the epoch it was built for");

    // What makes the `logs` layer non-empty, which the export's done criterion 2
    // asks for by name: an all-empty layer set would prove nothing about
    // ordering.
    seed_epoch_logs(&domain, &[epoch]).unwrap();
    seed_reward_logs(&domain, &vectors.stake_address, &vectors.pool_id, &[epoch]).unwrap();

    domain
}

/// An archive, state and index store with nothing in them: where a restore
/// writes.
///
/// The same three backends [`harness`] binds, so a comparison between a
/// restored node and a replayed one is about the drivers and not about the
/// stores.
pub struct Blank<B: ToyStores> {
    pub archive: dolos_redb3::archive::ArchiveStore,
    pub stores: B,
}

impl<B: ToyStores> Blank<B> {
    pub fn open() -> Self {
        Self {
            archive: dolos_redb3::archive::ArchiveStore::in_memory(
                dolos_cardano::model::build_schema(),
            )
            .unwrap(),
            stores: B::open(),
        }
    }

    pub fn state(&self) -> &B::State {
        self.stores.state()
    }

    pub fn indexes(&self) -> &B::Indexes {
        self.stores.indexes()
    }
}

pub fn plan_for<B: ToyStores>(domain: &ToyDomain<B>) -> Plan {
    export::plan(domain.state(), domain.genesis().network_magic() as u64).unwrap()
}

pub fn export_to<B: ToyStores>(root: &std::path::Path, domain: &ToyDomain<B>) -> Inscription {
    let stele = SteleDir::create(root).unwrap();

    export::export(
        &stele,
        &plan_for(domain),
        domain.archive(),
        domain.state(),
        domain.indexes(),
        None,
    )
    .unwrap()
}
