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
use dolos_snapshot::{
    export::{self, First, Plan},
    Network, RetainedEpochs,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, seed_epoch_logs, seed_reward_logs, SyntheticBlockConfig},
    toy_domain::{ToyDomain, ToyStores},
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
/// The same three backends [`harness`] binds — the archive included, via the
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

    pub fn indexes(&self) -> &B::Indexes {
        self.stores.indexes()
    }
}

// The re-export mirrors the module's own rule stated above: every test binary
// compiles this file in full, so the suites that never open a registry see an
// import they do not use.
#[cfg(feature = "oci")]
#[allow(unused_imports)]
pub use registry_node::Node;

/// The registry suites' node: the harness ledger and the two chain points it
/// publishes from.
///
/// Shared by `tests/publish.rs` and `tests/snapshot_verify.rs`, because a
/// stele the one suite publishes is what the other verifies, and a second
/// copy of the fixture would be a second answer to "where do the two plans
/// stand".
#[cfg(feature = "oci")]
mod registry_node {
    use dolos_core::{BlockHash, ChainPoint, Domain as _};
    use dolos_snapshot::{
        export::Plan,
        registry::{self, Published, Publishing, Registry},
        Error, Network, RetainedEpochs,
    };
    use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
    use stelae::progress::Observer;

    use super::harness;

    /// The harness ledger and the two plans it publishes: sequence 0 standing
    /// on the last slot of epoch 0, and sequence 1 one slot past it — the
    /// first block of epoch 1 (decision 0025: `sequence` is the epoch the
    /// cursor stands in).
    ///
    /// That the first cursor sits on the boundary is not a convenience: a
    /// stele cut mid-epoch clamps its last window to the cursor, so the same
    /// epoch published later in full has a different scope and is correctly
    /// rebuilt rather than inherited. See `tests/publish.rs` for the longer
    /// argument.
    pub struct Node {
        pub domain: ToyDomain<MemoryStores>,
        pub first: Plan,
        pub second: Plan,
    }

    impl Node {
        pub fn build() -> Self {
            let domain = harness::<MemoryStores>();

            let summary =
                dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();

            let magic = u64::from(domain.genesis().network_magic());
            let network = Network::for_magic(magic);
            let boundary = summary.epoch_start(1);

            // Any hash will do: `position` needs one to exist, and nothing in
            // an export reads it back out of the store.
            let point = |slot| ChainPoint::Specific(slot, BlockHash::new([0xab; 32]));

            let retained = RetainedEpochs::default();

            let first = Plan::new(
                &summary,
                network.clone(),
                point(boundary - 1),
                retained.clone(),
            )
            .unwrap();
            let second = Plan::new(&summary, network, point(boundary), retained).unwrap();

            assert_eq!(first.sequence, 0);
            assert_eq!(second.sequence, 1);
            assert_eq!(
                first.epochs,
                second.epochs[..1],
                "epoch 0's window has to be the same in both, or there is nothing to inherit"
            );

            Self {
                domain,
                first,
                second,
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
            registry::publish(
                publishing,
                plan,
                self.domain.archive(),
                self.domain.state(),
                self.domain.indexes(),
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
            registry::publish_into(
                stele,
                publishing,
                plan,
                self.domain.archive(),
                self.domain.state(),
                self.domain.indexes(),
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

/// A plan for `domain` standing at the **first slot of `epoch`**, retaining the
/// state dumps `retained` names.
///
/// The harness ledger lives inside epoch zero and epoch zero is the one epoch a
/// retained list may not name, so nothing published at the live cursor can ever
/// cut a dump. Standing at a synthetic boundary point is how the dump paths are
/// reached at all — the same device `Node` already uses for its second publish,
/// and for the same reason: everything downstream is derived by `Plan::new`
/// from the chain summary, so the point being synthetic changes what the plan
/// covers and nothing about how it is built.
pub fn plan_at_boundary<B: ToyStores>(
    domain: &ToyDomain<B>,
    epoch: u64,
    retained: RetainedEpochs,
) -> Plan {
    plan_standing_at(
        domain,
        epoch,
        |summary| summary.epoch_start(epoch),
        retained,
    )
}

/// A plan for `domain` standing at the **last slot of `epoch`**, retaining the
/// state dumps `retained` names.
///
/// The sequence is the same as [`plan_at_boundary`]'s for that epoch, and the
/// epoch windows are not: a stele cut mid-epoch clamps its last window to the
/// cursor, so the same epoch published later in full wears a *different scope*
/// and is correctly rebuilt rather than inherited. A pair of publishes meant to
/// exercise inheritance has to start here, which is the same reason
/// `Node::first` sits where it does — see `tests/publish.rs` for the longer
/// argument.
pub fn plan_at_epoch_end<B: ToyStores>(
    domain: &ToyDomain<B>,
    epoch: u64,
    retained: RetainedEpochs,
) -> Plan {
    plan_standing_at(
        domain,
        epoch,
        |summary| summary.epoch_start(epoch + 1) - 1,
        retained,
    )
}

fn plan_standing_at<B: ToyStores>(
    domain: &ToyDomain<B>,
    epoch: u64,
    slot: impl Fn(&dolos_cardano::eras::ChainSummary) -> u64,
    retained: RetainedEpochs,
) -> Plan {
    let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();
    let magic = u64::from(domain.genesis().network_magic());

    // Any hash will do: `position` needs one to exist, and nothing in an export
    // reads it back out of the store.
    let point =
        dolos_core::ChainPoint::Specific(slot(&summary), dolos_core::BlockHash::new([0xab; 32]));

    let plan = Plan::new(&summary, Network::for_magic(magic), point, retained).unwrap();
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
        domain.indexes(),
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
