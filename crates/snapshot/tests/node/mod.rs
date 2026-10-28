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

use dolos_cardano::pallas::ledger::traverse::MultiEraBlock;
use dolos_core::{
    import::ImportExt as _, ArchiveStore as _, ArchiveWriter as _, Domain as _, StateStore as _,
    StateWriter as _,
};
use dolos_snapshot::{
    export::{self, First, Plan},
    RetainedEpochs,
};
use dolos_testing::{
    blocks::make_conway_block_with_prev,
    synthetic::{build_synthetic_blocks, seed_epoch_logs, seed_reward_logs, SyntheticBlockConfig},
    toy_domain::{MemoryStores, ToyDomain, ToyStores},
};
use stelae::{dir::SteleDir, inscription::Inscription, progress::Observer};

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
/// The cost is that the ledger covers a single epoch. That is not where
/// multi-epoch geometry is checked in any case: it is arithmetic over a
/// `ChainSummary`, and it is pinned by the skeleton golden in `tests/export.rs`
/// and by `Plan`'s own unit tests. What the fixture is for is *records* — a
/// real UTxO set, real archive tags and exact records, real logs — and one
/// epoch holds all of those.
///
/// The stores then stand on the epoch-1 boundary — see [`stand_at_boundary`] —
/// because a stele is only ever cut there.
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

    stand_at_boundary(&domain, 1);

    domain
}

/// Put `domain`'s stores on the first block of `epoch`, as a `stop_epoch` sync
/// leaves them.
///
/// Appends two empty blocks chained onto the archive's tip — the last block of
/// the epoch before, on its last slot, and the epoch's first block, on its
/// first — and moves the state cursor onto the second. That is the evidence the
/// boundary check reads. The ledger is not run over them: the ledger state the
/// fixture computes stays the one a stele carries, and the blocks are what the
/// archive shows about where the state stands.
pub fn stand_at_boundary<B: ToyStores>(domain: &ToyDomain<B>, epoch: u64) {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();
    let start = summary.epoch_start(epoch);

    let (_, tip) = domain.archive().get_tip().unwrap().unwrap();
    let tip = MultiEraBlock::decode(&tip).unwrap();

    let (last, last_body) =
        make_conway_block_with_prev(start - 1, Some(tip.hash()), tip.number() + 1);
    let (first, first_body) = make_conway_block_with_prev(start, last.hash(), tip.number() + 2);

    let archive = domain.archive().start_writer().unwrap();
    archive.apply(&last, &last_body).unwrap();
    archive.apply(&first, &first_body).unwrap();
    archive.commit().unwrap();

    let state = domain.state().start_writer().unwrap();
    state.set_cursor(first).unwrap();
    state.commit().unwrap();
}

/// An archive and a state store with nothing in them: where a restore writes.
///
/// The same backends [`harness`] binds — the archive included, via the
/// `ToyStores` binding — so a comparison between a restored node and a
/// replayed one is about the drivers and not about the stores, and the
/// fjall-bound suites exercise a restore *into* a fjall archive.
pub struct Blank<B: ToyStores> {
    pub archive: B::Archive,
    pub stores: B,
}

impl<B: ToyStores> Blank<B> {
    pub fn open() -> Self {
        let stores = B::open();

        Self {
            archive: stores.archive().clone(),
            stores,
        }
    }

    pub fn state(&self) -> &B::State {
        self.stores.state()
    }
}

// The re-export mirrors the module's own rule stated above: every test binary
// compiles this file in full, so the suites that never open a registry see an
// import they do not use.
#[allow(unused_imports)]
pub use registry_node::Node;

/// The registry suites' node: the harness ledger and the two chain points it
/// publishes from.
///
/// Shared by `tests/publish.rs` and `tests/snapshot_verify.rs`, because a
/// stele the one suite publishes is what the other verifies, and a second
/// copy of the fixture would be a second answer to "where do the two plans
/// stand".
mod registry_node {
    use dolos_core::Domain as _;
    use dolos_snapshot::{
        export::Plan,
        registry::{self, Published, Publishing, Registry},
        Error, RetainedEpochs,
    };
    use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
    use stelae::progress::Observer;

    use super::{harness, plan_at_boundary};

    /// The harness ledger and the two plans it publishes: sequence 1 standing
    /// on the first block of epoch 1, and sequence 2 on the first block of
    /// epoch 2 (decision 0025: `sequence` is the epoch the cursor stands in).
    ///
    /// A stele is only cut from stores standing on its boundary, so each plan
    /// has its own: `earlier` stands at epoch 1, `domain` has gone on to
    /// epoch 2. Epoch 0's window is complete in both, so the second publish
    /// inherits it; epoch 1's is the first's boundary sliver and the second's
    /// whole epoch, so it is rebuilt.
    pub struct Node {
        pub earlier: ToyDomain<MemoryStores>,
        pub domain: ToyDomain<MemoryStores>,
        pub first: Plan,
        pub second: Plan,
    }

    impl Node {
        pub fn build() -> Self {
            let earlier = harness::<MemoryStores>();
            let domain = harness::<MemoryStores>();

            let retained = RetainedEpochs::default();

            let first = plan_at_boundary(&earlier, 1, retained.clone());
            let second = plan_at_boundary(&domain, 2, retained);

            assert_eq!(
                first.epochs[..1],
                second.epochs[..1],
                "epoch 0's window has to be the same in both, or there is nothing to inherit"
            );

            Self {
                earlier,
                domain,
                first,
                second,
            }
        }

        /// The stores `plan` was made from.
        pub fn stores(&self, plan: &Plan) -> &ToyDomain<MemoryStores> {
            if plan.sequence == self.first.sequence {
                &self.earlier
            } else {
                &self.domain
            }
        }

        pub fn publish(&self, repository: &Registry, plan: &Plan, rebuild: bool) -> Published {
            self.publish_as(Publishing::new(repository).rebuilding(rebuild), plan)
                .unwrap()
        }

        /// A publish with the whole of [`Publishing`] chosen by the caller —
        /// what the resume suite needs, since a resumption record is the one
        /// input a publish takes from the host rather than from the repository.
        pub fn publish_as(
            &self,
            publishing: Publishing<'_>,
            plan: &Plan,
        ) -> Result<Published, Error> {
            self.publish_watched(publishing, plan, &Observer::silent())
        }

        /// The same publish, with somebody listening.
        ///
        /// Separate from [`Node::publish_as`] so the suites that are not about
        /// progress stay unchanged and keep proving what they proved: an
        /// observer is meant to change nothing but what is said.
        pub fn publish_watched(
            &self,
            publishing: Publishing<'_>,
            plan: &Plan,
            observer: &Observer,
        ) -> Result<Published, Error> {
            let stores = self.stores(plan);

            registry::publish(
                publishing,
                plan,
                stores.archive(),
                stores.state(),
                None,
                observer,
            )
        }

        /// The same publish, through a writer of the caller's — the interrupted
        /// transport the resume suite kills at a layer it chose.
        pub fn publish_through<W: stelae::SteleWriter + Sync>(
            &self,
            stele: &W,
            publishing: Publishing<'_>,
            plan: &Plan,
        ) -> Result<Published, Error> {
            let stores = self.stores(plan);

            registry::publish_into(
                stele,
                publishing,
                plan,
                stores.archive(),
                stores.state(),
                None,
                &Observer::silent(),
            )
        }

        pub fn refuse(&self, repository: &Registry, plan: &Plan) -> Error {
            self.publish_as(Publishing::new(repository), plan)
                .unwrap_err()
        }
    }
}

/// Move `domain`'s stores one block past the boundary they stand on: an empty
/// block a few slots later in the same epoch, chained onto the cursor's, with
/// the cursor on it. Where a node that kept syncing past `stop_epoch` stands.
pub fn step_past_boundary<B: ToyStores>(domain: &ToyDomain<B>) -> dolos_core::ChainPoint {
    let next = extend_archive_past_cursor(domain);

    let state = domain.state().start_writer().unwrap();
    state.set_cursor(next.clone()).unwrap();
    state.commit().unwrap();

    next
}

/// Append one empty block to `domain`'s archive, a few slots after the state
/// cursor and chained onto its block, leaving the cursor where it is. Where a
/// `doctor rebuild-state --stop-epoch` leaves a node: a full archive beside a
/// boundary state.
pub fn extend_archive_past_cursor<B: ToyStores>(domain: &ToyDomain<B>) -> dolos_core::ChainPoint {
    let cursor = domain.state().read_cursor().unwrap().unwrap();
    let body = domain
        .archive()
        .get_block_by_slot(&cursor.slot())
        .unwrap()
        .unwrap();
    let number = MultiEraBlock::decode(&body).unwrap().number();

    let (next, next_body) =
        make_conway_block_with_prev(cursor.slot() + 5, cursor.hash(), number + 1);

    let archive = domain.archive().start_writer().unwrap();
    archive.apply(&next, &next_body).unwrap();
    archive.commit().unwrap();

    next
}

/// A plan for `domain` standing on the **first block of `epoch`**, retaining
/// the state dumps `retained` names.
///
/// Stores standing in an earlier epoch are moved onto that boundary first —
/// see [`stand_at_boundary`] — so the plan is the one a publish from them would
/// make. The harness ledger lives inside epoch zero, so this is how the dump
/// paths, which need a sequence a retained list can name, are reached at all.
pub fn plan_at_boundary<B: ToyStores>(
    domain: &ToyDomain<B>,
    epoch: u64,
    retained: RetainedEpochs,
) -> Plan {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();
    let cursor = domain.state().read_cursor().unwrap().unwrap();

    if summary.slot_epoch(cursor.slot()).0 < epoch {
        stand_at_boundary(domain, epoch);
    }

    let plan = plan_retaining(domain, retained);
    assert_eq!(plan.sequence, epoch);

    plan
}

/// [`export_to`], for a plan the caller built.
pub fn export_plan<B: ToyStores>(
    root: &std::path::Path,
    domain: &ToyDomain<B>,
    plan: &Plan,
) -> Inscription {
    let stele = SteleDir::create(root).unwrap();

    export::export(
        &stele,
        plan,
        domain.archive(),
        domain.state(),
        None,
        &First,
        &Observer::silent(),
    )
    .unwrap()
}

/// The publish plan for `domain`, retaining the state dumps `retained` names.
pub fn plan_retaining<B: ToyStores>(domain: &ToyDomain<B>, retained: RetainedEpochs) -> Plan {
    export::plan(
        domain.state(),
        domain.genesis().network_magic() as u64,
        retained,
    )
    .unwrap()
}

pub fn plan_for<B: ToyStores>(domain: &ToyDomain<B>) -> Plan {
    plan_retaining(domain, RetainedEpochs::default())
}

pub fn export_to<B: ToyStores>(root: &std::path::Path, domain: &ToyDomain<B>) -> Inscription {
    export_plan(root, domain, &plan_for(domain))
}

/// An account's row of `window`, written straight to `domain`'s archive.
pub fn write_account_row(
    domain: &ToyDomain<MemoryStores>,
    window: u64,
    row: dolos_cardano::AccountEpochLog,
) {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();
    let account = dolos_cardano::AccountKey::from(
        dolos_cardano::pallas::ledger::primitives::StakeCredential::AddrKeyhash([0x51; 28].into()),
    );

    let writer = domain.archive().start_writer().unwrap();
    writer
        .write_log_typed(summary.epoch_start(window), &account, &row)
        .unwrap();
    writer.commit().unwrap();
}

/// A row as ESTART writes it: the stake leg alone.
pub fn stake_only() -> dolos_cardano::AccountEpochLog {
    dolos_cardano::AccountEpochLog {
        active_stake: Some(2_000),
        pool_id: Some([0x52; 28].into()),
        ..Default::default()
    }
}

/// The same row as the EWRAP that settles it rewrites it.
pub fn settled() -> dolos_cardano::AccountEpochLog {
    dolos_cardano::AccountEpochLog {
        member_reward: Some(7),
        ..stake_only()
    }
}

pub fn account_layer(
    inscription: &stelae::inscription::Inscription,
    epoch: u64,
) -> stelae::inscription::LayerDescriptor {
    inscription
        .layers
        .iter()
        .find(|layer| {
            layer.kind == "log-account-epochs"
                && layer.scope.get("epoch").and_then(serde_json::Value::as_u64) == Some(epoch)
        })
        .cloned()
        .unwrap()
}
