//! Boundary preparation through the real ledger writers: rewards, a refund, a
//! deregistration settling a log row after its boundary, seals and entity
//! layers.
//!
//! `crates/snapshot/tests/export.rs` holds the contract against a chain written
//! by hand. This holds a hand-written chain's assumptions against the writers
//! themselves. Not exercised: real Byron bodies through the ledger, the Byron
//! to Shelley fork, governance enactment.

mod node;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use dolos::core::{Genesis, RawBlock};
use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_cardano::model::{AccountEpochLog, PoolEpochLog};
use dolos_core::config::CustomUtxo;
use dolos_core::ArchiveStore as _;
use dolos_snapshot::export::{Document, First, Following, Predecessor, Producers};
use dolos_snapshot::inscription::Inscription;
use dolos_snapshot::prepare::{Bundle, Finalizing, IndexTail, Request, Store};
use dolos_snapshot::progress::Observer;
use dolos_snapshot::source::SnapshotSource as _;
use dolos_snapshot::staging::Budget;
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};
use pallas::ledger::primitives::conway::{Certificate, DRep, Value};
use pallas::ledger::primitives::{Bytes, RationalNumber, Set, StakeCredential};
use pallas::ledger::traverse::MultiEraBlock;

const WHALE: u64 = 100_000_000_000_000;
const KEY_DEPOSIT: u64 = 2_000_000;
const POOL_DEPOSIT: u64 = 500_000_000;
const DREP_DEPOSIT: u64 = 500_000_000;

/// The second block of epoch 5, after its reward update: the whale forfeits
/// what that update computed, so the boundary settles the pool's row.
const FORFEIT_BLOCK: usize = 11;

const BOUNDARIES: [u64; 4] = [3, 4, 5, 6];
const FRONTIER: u64 = 8;
const RETAINED: [u64; 1] = [3];

struct Fixture {
    blocks: Vec<RawBlock>,
    utxos: Vec<CustomUtxo>,
    genesis: Arc<Genesis>,
    magic: u64,
}

impl Fixture {
    fn node(&self) -> node::Node {
        let mut node = node::Node::new();
        node.config.chain = node.custom_chain(self.magic as u32, self.utxos.clone());
        node
    }

    /// Replay onward to the first block of `stop`, and where the next run
    /// starts.
    fn replay_to(&self, node: &node::Node, from: usize, stop: u64) -> (BulkReplaySession, usize) {
        let mut session =
            BulkReplaySession::open(&node.config, self.genesis.clone(), Some(stop)).unwrap();
        let progress = session.import_blocks(self.blocks[from..].to_vec()).unwrap();

        assert!(progress.is_boundary(), "stop {stop}: {progress:?}");

        let slot = progress.position().slot();
        let next = self
            .blocks
            .iter()
            .position(|body| MultiEraBlock::decode(body).unwrap().slot() > slot)
            .unwrap_or(self.blocks.len());

        (session, next)
    }

    fn retained() -> dolos_snapshot::RetainedEpochs {
        dolos_snapshot::planning::retained_epochs(&RETAINED).unwrap()
    }

    /// An independent replay into fresh stores, stopped at `stop`, and its
    /// ordinary export.
    fn frozen(&self, stop: u64, previous: Option<&Document>) -> Document {
        let node = self.node();
        let (session, _) = self.replay_to(&node, 0, stop);
        let snapshot = session.snapshot();
        let plan = snapshot.plan(self.magic, Self::retained()).unwrap();

        let document = match previous {
            None => snapshot.digest_document(&plan, &First).unwrap(),
            Some(previous) => snapshot
                .digest_document(&plan, &Following::read(&previous.canonical, &plan).unwrap())
                .unwrap(),
        };

        drop(snapshot);
        session.finish().unwrap();

        document
    }
}

/// By kind and scope: the digest and the scope's epoch.
fn layers(document: &Document) -> BTreeMap<(String, String), (String, Option<u64>)> {
    Inscription::parse(&document.canonical)
        .unwrap()
        .layers
        .iter()
        .map(|layer| {
            (
                (layer.kind.clone(), layer.scope.to_string()),
                (layer.diff_id.to_string(), layer.scope["epoch"].as_u64()),
            )
        })
        .collect()
}

/// A chain that earns real rewards: the block issuer's pool, registered in the
/// first block with a whale delegating to it, a second pool that retires, a
/// DRep, per-block first mints with re-mints, and fresh and re-seen stake
/// addresses in every block.
fn rich_fixture(frontier: u64) -> Fixture {
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);

    let epoch = genesis.shelley.epoch_length.unwrap() as u64;
    let count = (2 * frontier + 1) as usize;

    let defaults = SyntheticBlockConfig::default();
    let address = Address::from_bech32(&defaults.address).unwrap();
    let (cred, _) = dolos_cardano::pallas_extras::address_as_stake_cred(&address).unwrap();

    // The issuer key every synthetic header carries.
    let operator: Hash<28> = Hasher::<224>::hash(&[0x10, 0x11]);
    let retiring: Hash<28> = Hash::from([0xb1; 28]);
    let drep: Hash<28> = Hash::from([0xd1; 28]);
    // The pools' reward account and owner, so the whale earns as a member.
    let owner: Hash<28> = Hash::from([0xb3; 28]);

    let registration = |operator: Hash<28>| Certificate::PoolRegistration {
        operator,
        vrf_keyhash: [0xb2; 32].into(),
        pledge: 0,
        cost: 340_000_000,
        margin: RationalNumber {
            numerator: 1,
            denominator: 100,
        },
        reward_account: Bytes::from([[0xe0].as_slice(), owner.as_ref()].concat()),
        pool_owners: Set::from(vec![owner]),
        relays: vec![],
        pool_metadata: None,
    };

    let mut certs = vec![vec![vec![]]; count];
    certs[0] = vec![vec![
        Certificate::StakeRegistration(cred.clone()),
        Certificate::StakeRegistration(StakeCredential::AddrKeyhash(owner)),
        registration(operator),
        registration(retiring),
        Certificate::StakeDelegation(cred.clone(), operator),
        Certificate::RegDRepCert(StakeCredential::AddrKeyhash(drep), DREP_DEPOSIT, None),
        Certificate::VoteDeleg(cred.clone(), DRep::Key(drep)),
    ]];
    certs[1] = vec![vec![Certificate::PoolRetirement(retiring, 2)]];
    certs[FORFEIT_BLOCK] = vec![vec![Certificate::StakeDeregistration(cred.clone())]];

    let staked = |payment: u8, delegation: u8| {
        ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::key_hash(Hash::from([payment; 28])),
            ShelleyDelegationPart::key_hash(Hash::from([delegation; 28])),
        )
        .to_vec()
    };

    let outputs = (0..count)
        .map(|block| {
            let fresh = 0x40 + block as u8;
            let again = 0x40 + block.saturating_sub(3) as u8;

            vec![vec![
                staked(0x80 + block as u8, fresh),
                staked(0xb0 + block as u8, again),
            ]]
        })
        .collect();

    let names = (0..count)
        .map(|block| match block % 4 {
            3 => format!("A{}", block - 3),
            _ => format!("A{block}"),
        })
        .collect();

    let (blocks, _, mut chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: count,
        txs_per_block: 1,
        slot: 0,
        start_block: 0,
        slot_step: epoch / 2,
        plain_txs: true,
        lovelace: WHALE,
        seed_amount: WHALE + 7,
        extra_certs_by_block: certs,
        extra_outputs_by_block: outputs,
        asset_names_by_block: names,
        ..Default::default()
    });

    // The first tx pays the deposits of what it registers. The submit UTxO comes
    // first, then one seed per tx.
    chain.custom_utxos[1].cbor = dolos_testing::utxo_with_value(
        defaults.seed_address.clone(),
        Value::Coin(WHALE + 7 + 2 * KEY_DEPOSIT + 2 * POOL_DEPOSIT + DREP_DEPOSIT),
    )
    .1;
    chain.custom_utxos[1 + FORFEIT_BLOCK].cbor = dolos_testing::utxo_with_value(
        defaults.seed_address.clone(),
        Value::Coin(WHALE + 7 - KEY_DEPOSIT),
    )
    .1;

    Fixture {
        blocks,
        utxos: chain.custom_utxos,
        magic: u64::from(genesis.network_magic()),
        genesis: Arc::new(genesis),
    }
}

/// Prepare at each boundary from the session's real delta, replay on to the
/// frontier, stage once, and finish each on its own and chained.
struct Run {
    finished: BTreeMap<u64, Document>,
    chained: BTreeMap<u64, Document>,
    at_frontier: Document,
}

fn prepare_and_finish(fixture: &Fixture, node: &node::Node) -> Run {
    let scratch = tempfile::tempdir().unwrap();
    let store = Store::open(scratch.path().join("bundles"), "journal").unwrap();
    let silent = Observer::silent();
    let mut bundles = Vec::new();
    let mut from = 0;

    for boundary in BOUNDARIES {
        let (session, next) = fixture.replay_to(node, from, boundary);
        from = next;

        let delta = session.boundary_index_delta().unwrap().clone();
        let snapshot = session.snapshot();
        let plan = snapshot.plan(fixture.magic, Fixture::retained()).unwrap();
        let request = Request {
            store_contract: dolos::core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
            mutable_slots: dolos_cardano::mutable_slots(&fixture.genesis),
            tail: IndexTail::Delta(&delta),
        };

        let bundle = snapshot
            .prepare_boundary(&store, &plan, &request, &silent)
            .unwrap();

        // Real stores walked again produce what they captured, or a restart at
        // the boundary would refuse its own bundle.
        let again = snapshot
            .prepare_boundary(&store, &plan, &request, &silent)
            .unwrap();

        assert_eq!(
            bundle.layers().collect::<Vec<_>>(),
            again.layers().collect::<Vec<_>>(),
            "at {boundary}"
        );

        bundles.push(bundle);

        drop(snapshot);
        session.finish().unwrap();
    }

    let (session, _) = fixture.replay_to(node, from, FRONTIER);
    session.finish().unwrap();

    let workspace = ReplayWorkspace::open(&node.config, fixture.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();

    let plan = snapshot.plan(fixture.magic, Fixture::retained()).unwrap();
    let at_frontier = snapshot.digest_document(&plan, &First).unwrap();

    let staged = snapshot
        .stage_batch(
            &scratch.path().join("staged"),
            &bundles,
            &First,
            Budget::new(u64::MAX),
            &silent,
        )
        .unwrap();

    let finishing = |bundle: &Bundle, previous: &dyn Predecessor| {
        snapshot
            .finalize_document(
                &Finalizing {
                    store: &store,
                    bundle,
                    staged: &staged,
                    producers: Producers::DEFAULT,
                },
                previous,
            )
            .unwrap()
    };

    let mut finished = BTreeMap::new();
    let mut chained: BTreeMap<u64, Document> = BTreeMap::new();

    for bundle in &bundles {
        finished.insert(bundle.sequence(), finishing(bundle, &First));

        let following = bundle
            .sequence()
            .checked_sub(1)
            .and_then(|previous| chained.get(&previous));

        let document = match following {
            None => finishing(bundle, &First),
            Some(previous) => finishing(
                bundle,
                &Following::read(&previous.canonical, &bundle.plan()).unwrap(),
            ),
        };

        chained.insert(bundle.sequence(), document);
    }

    drop(snapshot);
    workspace.finish().unwrap();

    Run {
        finished,
        chained,
        at_frontier,
    }
}

/// Windows at or below `boundary` whose log or entity layer changed by the
/// frontier. The equality below means nothing for windows that never moved.
fn moved_after(
    frozen: &Document,
    at_frontier: &Document,
    boundary: u64,
) -> BTreeSet<(String, u64)> {
    let (frozen, at_frontier) = (layers(frozen), layers(at_frontier));
    let visible = |kind: &str, window: Option<u64>| -> Option<u64> {
        (kind.starts_with("log-") || kind.starts_with("entity-"))
            .then_some(window?)
            .filter(|window| *window <= boundary)
    };

    let mut moved = BTreeSet::new();

    for ((kind, scope), (digest, window)) in &frozen {
        if let Some(window) = visible(kind, *window) {
            let later = at_frontier.get(&(kind.clone(), scope.clone()));

            if later.is_none_or(|(later, _)| later != digest) {
                moved.insert((kind.clone(), window));
            }
        }
    }

    for ((kind, scope), (_, window)) in &at_frontier {
        if let Some(window) = visible(kind, *window) {
            if !frozen.contains_key(&(kind.clone(), scope.clone())) {
                moved.insert((kind.clone(), window));
            }
        }
    }

    moved
}

/// Every boundary of a chain with real rewards, settlements, seals and final
/// rows matches an independent replay stopped there, on its own and chained.
#[test]
fn real_writers_finish_as_the_store_frozen_there() {
    let fixture = rich_fixture(FRONTIER);
    let node = fixture.node();
    let run = prepare_and_finish(&fixture, &node);

    let mut chained_frozen: BTreeMap<u64, Document> = BTreeMap::new();
    let mut moved = BTreeSet::new();

    for boundary in BOUNDARIES {
        let frozen = fixture.frozen(boundary, None);

        assert_eq!(
            layers(&frozen),
            layers(&run.finished[&boundary]),
            "at {boundary}"
        );
        assert_eq!(
            frozen.canonical, run.finished[&boundary].canonical,
            "at {boundary}"
        );

        let chained = fixture.frozen(
            boundary,
            boundary.checked_sub(1).and_then(|p| chained_frozen.get(&p)),
        );

        assert_eq!(
            chained.canonical, run.chained[&boundary].canonical,
            "chained at {boundary}"
        );

        chained_frozen.insert(boundary, chained);

        for (kind, window) in moved_after(&frozen, &run.at_frontier, boundary) {
            assert!(
                window + 1 >= boundary,
                "{kind} window {window} moved after boundary {boundary}, below the capture"
            );

            moved.insert((boundary, kind, window));
        }
    }

    assert!(
        moved.contains(&(5, "log-pool-epochs".to_owned(), 4)),
        "the deregistration settles the window before boundary 5: {moved:?}"
    );

    // The rows that make this more than the hand-written fixture.
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let pool_rewards: u64 = stores
        .archive
        .iter_logs_typed::<PoolEpochLog>(None)
        .unwrap()
        .map(|row| row.unwrap().2.total_rewards)
        .sum();

    let (mut member, mut leader, mut refunds) = (0, 0, 0);

    for row in stores
        .archive
        .iter_logs_typed::<AccountEpochLog>(None)
        .unwrap()
    {
        let (_, _, row) = row.unwrap();

        member += row.member_reward.unwrap_or(0);
        leader += row.leader_rewards.iter().map(|x| x.1).sum::<u64>();
        refunds += row.deposit_refunds.iter().map(|x| x.1).sum::<u64>();
    }

    assert!(pool_rewards > 0 && member > 0 && leader > 0 && refunds > 0);
}
