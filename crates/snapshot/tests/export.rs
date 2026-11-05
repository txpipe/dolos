//! The export driver, against real store sets.
//!
//! Four properties, in increasing order of what they cost to check:
//!
//! 1. **The skeleton is pinned.** An export over the smallest store set a stele
//!    can be cut from — an era, a cursor, and the two empty blocks that show
//!    the cursor is its epoch's first — produces a document with no ledger
//!    content in it, so the golden below is a function of literals only: the
//!    network table, the `sequence`/`position`/`parameters` shapes, the order
//!    the layers are listed in, and every layer's header and scope encoding. A
//!    ledger change cannot move it, which is what makes it a golden rather than
//!    a value that gets re-pinned as a matter of routine.
//! 2. **A live node exports a complete stele.** Built by the harness domain
//!    from synthetic blocks, read back through `SteleDir`, verified clean.
//! 3. **Read-back equality.** Every layer, decoded through its own codec, is
//!    what walking the store yields — compared against an expectation that is
//!    *sorted independently* rather than produced by the same registry walk the
//!    exporter does, so an ordering bug shared by both would still show.
//! 4. **Determinism across backends.** The same blocks applied to the fjall
//!    stores and to the builtin memory stores publish the same inscription
//!    digest. This is the claim ADR-004 rests on and the first place it is
//!    measured rather than asserted.
//! 5. **A stele can be reproduced without being stored.** The discarding writer
//!    walks the same stores and produces the same canonical document, byte for
//!    byte, as the publish that wrote one to disk — which is what `dolos
//!    snapshot digest` is, and what makes an independent verifier possible at
//!    all.

mod common;
mod node;
mod watcher;

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use common::read_both_ways;
use dolos_cardano::{
    eras::ChainSummary,
    indexes::archive_dimensions,
    model::{AccountKey, AccountState, EraState, FixedNamespace as _, GovDistr},
    pallas::ledger::{primitives::StakeCredential, traverse::MultiEraBlock},
    EpochState, EraBoundary, EraKey, GovState, ShardProgress,
};
use dolos_core::{
    builtin::{MemoryArchiveStore, MemoryStateStore},
    ArchiveError, ArchiveStore, ArchiveWriter as _, BlockBody, BlockSlot, ChainPoint, Domain,
    EntityKey, EntityRecord, ExactRecord, IndexRecord, KeyRange, LogKey, Namespace, StateStore,
    StateWriter as _, TagRecord,
};
use dolos_snapshot::{
    boundary::Ineligible,
    export::{self, EpochWindow, Plan, Predecessor as _},
    facade::{Selection, SnapshotSource as _, StoreSnapshot},
    layers::{blocks, entities, indexes, logs, state},
    state_layer_count, state_ns_for, DolosProfile, Network, RetainedEpochs, BLOCKS, ENTITY_KINDS,
    INDEXES, LOG_KINDS, LOG_NAMESPACES, NAMESPACES, STATE_KINDS, UTXOS,
};
use dolos_testing::{
    blocks::make_conway_block_with_prev,
    toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores},
};
use node::{
    account_layer, export_plan, export_to, extend_archive_past_cursor, harness, plan_at_boundary,
    plan_for, settled, stake_only, step_past_boundary, write_account_row,
};
use stelae::{
    dir::SteleDir,
    progress::{Observer, Outcome},
    Discarding, SteleReader,
};

use watcher::Watcher;

/// The identity of an export over the skeleton store set.
const GOLDEN_SKELETON: &str =
    "sha256:ed7236c56ff338c81e1703e5f85afc1dc97270e0e6df2e0c97aca442352f06a4";

/// Where the skeleton fixture stands: on the first block of epoch 2 under
/// [`skeleton_summary`], after fifty empty slots, so the export covers three
/// epochs and the last window is the boundary sliver up to the cursor.
const SKELETON_SLOT: BlockSlot = 250;

/// The last block before the skeleton's boundary, in epoch 1.
const SKELETON_PARENT_SLOT: BlockSlot = 180;

// --------------------------------------------------------------------------
// 1. The pinned skeleton
// --------------------------------------------------------------------------

/// One era, hundred-slot epochs from slot zero. Literal, so nothing about the
/// golden depends on a genesis file.
fn skeleton_summary() -> ChainSummary {
    let mut chain = ChainSummary::default();

    chain.append_era(6, skeleton_era());

    chain
}

fn skeleton_era() -> EraState {
    EraState {
        start: EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: 0,
        },
        end: None,
        epoch_length: 100,
        slot_length: 1,
        protocol: 6,
    }
}

/// The two empty blocks that show where the skeleton stands: the last block of
/// epoch 1 and the first of epoch 2.
fn skeleton_blocks() -> [(ChainPoint, dolos_core::RawBlock); 2] {
    let parent = make_conway_block_with_prev(SKELETON_PARENT_SLOT, None, 0);
    let first = make_conway_block_with_prev(SKELETON_SLOT, parent.0.hash(), 1);

    [parent, first]
}

fn skeleton_point() -> ChainPoint {
    let [_, (first, _)] = skeleton_blocks();

    first
}

/// Put the skeleton's boundary blocks in `archive`.
fn with_skeleton_blocks<A: ArchiveStore>(archive: A) -> A {
    let writer = archive.start_writer().unwrap();

    for (point, body) in skeleton_blocks() {
        writer.apply(&point, &body).unwrap();
    }

    writer.commit().unwrap();

    archive
}

/// A state store holding the skeleton's era and a cursor on its boundary.
fn skeleton_state() -> MemoryStateStore {
    let state = MemoryStateStore::new();
    let writer = state.start_writer().unwrap();
    writer
        .write_entity_typed::<EraState>(&EraKey::from(6), &skeleton_era())
        .unwrap();
    writer.set_cursor(skeleton_point()).unwrap();
    writer.commit().unwrap();

    state
}

/// The smallest store set a stele can be cut from: an era, a cursor, and the
/// two blocks that show the cursor is the first of its epoch.
fn skeleton_stores() -> (MemoryArchiveStore, MemoryStateStore) {
    let archive = with_skeleton_blocks(MemoryArchiveStore::new(
        dolos_cardano::model::build_log_schema(),
    ));

    (archive, skeleton_state())
}

/// Done criterion 4.
///
/// Every value in the pinned document below is determined by this crate and
/// ADR-004 together. Beyond the two boundary blocks and the era, the layers
/// carry only their header records, so their `diffId`s freeze the profile name,
/// the kind strings and the scope CBOR — and nothing about what a ledger
/// happens to hold. A failure here means published identity moved.
#[test]
fn a_bare_boundary_exports_the_pinned_skeleton() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let (archive, state) = skeleton_stores();

    let plan = Plan::new(
        &skeleton_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        skeleton_point(),
        Default::default(),
    )
    .unwrap();

    let inscription = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    // Three epochs of blocks and indexes, then every state layer. No
    // `digests` layer: nothing supplies one — and **no log layers at all**,
    // which is the omit-if-empty rule seen from the other side: a store with no
    // logs in it publishes no layer claiming there are none.
    assert_eq!(inscription.layers.len(), 3 * 2 + state_layer_count());

    for (kind, _) in LOG_KINDS {
        assert_eq!(
            inscription.layers_of_kind(kind).count(),
            0,
            "{kind}: an empty namespace still produced a layer"
        );
    }

    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();
    assert_eq!(canonical, CANONICAL_SKELETON);

    assert_eq!(inscription.digest().unwrap().to_string(), GOLDEN_SKELETON);

    // And the directory it wrote is a stele by the protocol's own reckoning.
    let reopened = SteleDir::open(temp.path()).unwrap();
    let read = reopened.read_inscription().unwrap();

    assert_eq!(read, inscription);
    read.check_profile(&DolosProfile).unwrap();
}

/// Done criterion 2: a log under a namespace no `log-{ns}` kind carries stops
/// the publish and names the namespace.
///
/// The split traded an all-namespace walk for a closed list, and this is the
/// price of that trade paid up front. Written straight into the archive
/// rather than through a ledger phase, because no ledger phase writes one today
/// — the point is what happens on the day one does.
#[test]
fn a_log_under_an_uncovered_namespace_fails_the_export() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let state = skeleton_state();

    let stray = NAMESPACES
        .into_iter()
        .find(|ns| *ns != UTXOS && !LOG_NAMESPACES.contains(ns))
        .expect("a state namespace that carries no logs");

    let mut schema = dolos_cardano::model::build_log_schema();
    schema.insert(stray, dolos_core::NamespaceType::KeyValue);
    let archive = with_skeleton_blocks(MemoryArchiveStore::new(schema));

    let writer = archive.start_writer().unwrap();
    writer
        .write_log(
            stray,
            &LogKey::from_raw(&120u64.to_be_bytes()).unwrap(),
            &vec![0xa0],
        )
        .unwrap();
    writer.commit().unwrap();

    let plan = Plan::new(
        &skeleton_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        skeleton_point(),
        Default::default(),
    )
    .unwrap();

    let err = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap_err();

    match err {
        dolos_snapshot::Error::UncoveredLogNamespace { epoch, ns, .. } => {
            assert_eq!(epoch, 1, "slot 120 is in epoch 1 under this summary");
            assert_eq!(ns, stray);
        }
        other => panic!("{other:?}"),
    }
}

/// A magic with no entry in the table renders as `testnet-{magic}`, so a devnet
/// is publishable without anyone agreeing on a spelling.
#[test]
fn an_unnamed_network_renders_from_its_magic() {
    let plan = Plan::new(
        &skeleton_summary(),
        Network::for_magic(42),
        skeleton_point(),
        Default::default(),
    )
    .unwrap();

    let position = plan.position().unwrap();

    assert_eq!(position["network"]["magic"], 42);
    assert_eq!(position["network"]["name"], "testnet-42");
}

// --------------------------------------------------------------------------
// 2. A complete stele from a live store set
// --------------------------------------------------------------------------

/// Done criterion 1.
#[test]
fn a_harness_domain_exports_a_complete_stele() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    assert_eq!(inscription.sequence, plan.sequence);
    assert_eq!(
        inscription.position["network"]["name"], "preview",
        "the name comes from the magic, not from configuration"
    );

    inscription.validate().unwrap();
    inscription.check_profile(&DolosProfile).unwrap();

    // Every kind but the log kinds and `digests`, which have no source in this
    // slice. The state kinds are all here, seeded or not: a namespace the
    // harness never wrote still publishes its shards, empty.
    for kind in [BLOCKS, INDEXES]
        .into_iter()
        .chain(STATE_KINDS.into_iter().map(|(kind, _, _, _)| kind))
    {
        assert!(
            inscription.layers_of_kind(kind).next().is_some(),
            "no {kind} layer"
        );
    }

    // Done criterion 3, from the side a real ledger shows it: the harness seeds
    // `epochs` and `account-epochs` and nothing else, so exactly those two log
    // kinds travel and the third is absent — not present and empty.
    let carried: Vec<&str> = LOG_KINDS
        .into_iter()
        .filter(|(kind, _)| inscription.layers_of_kind(kind).next().is_some())
        .map(|(kind, _)| kind)
        .collect();

    assert_eq!(carried, ["log-account-epochs", "log-epochs"]);
    assert_eq!(
        inscription.layers_of_kind(dolos_snapshot::DIGESTS).count(),
        0
    );

    // The whole directory verifies: every blob hashes to its own name and
    // decompresses to a layer the inscription points at.
    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    for descriptor in &inscription.layers {
        assert!(
            index.blob_for(&descriptor.diff_id).is_some(),
            "layer {:?} has no blob",
            descriptor.kind
        );
    }
}

// --------------------------------------------------------------------------
// 3. Read-back equality
// --------------------------------------------------------------------------

/// Done criterion 2.
///
/// Each expectation is built by collecting the store's records and sorting them
/// by the layer's own ordering rule, rather than by repeating the exporter's
/// registry walk. An exporter that emitted the right records in the wrong order
/// would satisfy a mirror of its own loop and fail this.
#[test]
fn every_layer_reads_back_as_the_store_yields_it() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    let windows: Vec<EpochWindow> = plan.epochs.clone();

    // --- blocks -----------------------------------------------------------
    let written: Vec<_> = inscription.layers_of_kind(BLOCKS).collect();
    assert_eq!(written.len(), windows.len());

    let mut any_blocks = false;

    for (descriptor, window) in written.iter().zip(&windows) {
        let found: Vec<blocks::BlockRecord> = read_both_ways(&stele, &index, descriptor)
            .iter()
            .map(|raw| blocks::decode(raw).unwrap())
            .collect();

        any_blocks |= !found.is_empty();

        assert_eq!(
            found,
            expected_blocks(&domain, window),
            "epoch {}",
            window.epoch
        );
    }

    assert!(any_blocks, "the fixture archived no blocks");

    // --- logs -------------------------------------------------------------
    //
    // One layer per (epoch, namespace) pair that has records, and none for the
    // pairs that do not — so the expectation is built from the store first and
    // the layer list is checked against it, rather than the other way round.
    let mut any_logs = false;

    for (kind, ns) in LOG_KINDS {
        let written: Vec<_> = inscription.layers_of_kind(kind).collect();

        let populated: Vec<&EpochWindow> = windows
            .iter()
            .filter(|window| !expected_logs(&domain, window, ns).is_empty())
            .collect();

        assert_eq!(
            written.len(),
            populated.len(),
            "{kind}: a layer exists for a window with no records, or the reverse"
        );

        for (descriptor, window) in written.iter().zip(&populated) {
            assert_eq!(descriptor.scope["epoch"], window.epoch, "{kind}");

            let found: Vec<logs::LogRecord> = read_both_ways(&stele, &index, descriptor)
                .iter()
                .map(|raw| logs::decode(ns, raw).unwrap())
                .collect();

            any_logs |= !found.is_empty();

            assert_eq!(
                found,
                expected_logs(&domain, window, ns),
                "{kind}, epoch {}",
                window.epoch
            );
        }
    }

    assert!(
        any_logs,
        "the fixture produced no logs, so the log layers prove nothing"
    );

    // --- indexes ----------------------------------------------------------
    let written: Vec<_> = inscription.layers_of_kind(INDEXES).collect();
    assert_eq!(written.len(), windows.len());

    let mut tags = 0usize;
    let mut exact = 0usize;

    for (descriptor, window) in written.iter().zip(&windows) {
        let found: Vec<IndexRecord> = read_both_ways(&stele, &index, descriptor)
            .iter()
            .map(|raw| indexes::decode(raw).unwrap())
            .collect();

        for record in &found {
            match record {
                IndexRecord::Tag(_) => tags += 1,
                IndexRecord::Exact(_) => exact += 1,
            }
        }

        assert_eq!(
            found,
            expected_indexes(&domain, window),
            "epoch {}",
            window.epoch
        );
    }

    // Both shapes, not just one: they come from two separate iterators with two
    // separate ordering rules, and the layer carries them in one run.
    assert!(tags > 0, "the fixture produced no archive tags");
    assert!(exact > 0, "the fixture produced no exact records");

    // --- state ------------------------------------------------------------
    let expected = expected_state(&domain);

    let mut records = 0usize;

    for (kind, ns, shards, shard_byte) in STATE_KINDS {
        let written: Vec<_> = inscription.layers_of_kind(kind).collect();

        // Exactly the shards the kind's spec'd count promises, always: an empty
        // shard is still a shard, so a client planning a selective fetch never
        // has to discover the count from the data.
        assert_eq!(written.len(), shards as usize, "{kind}");

        let carried = expected.get(ns).cloned().unwrap_or_default();

        for (shard, descriptor) in written.iter().enumerate() {
            assert_eq!(descriptor.scope["shard"], shard, "{kind}");

            let found: Vec<state::StateRecord> = read_both_ways(&stele, &index, descriptor)
                .iter()
                .map(|raw| state::decode(ns, raw).unwrap())
                .collect();

            let expected: Vec<state::StateRecord> = carried
                .iter()
                .filter(|record| state::shard_of(&record.key, shards, shard_byte) as usize == shard)
                .cloned()
                .collect();

            assert_eq!(found, expected, "{kind} shard {shard}");

            records += found.len();
        }
    }

    assert!(
        records > 0,
        "the fixture produced no state, so the layers prove nothing"
    );
}

/// Accounts spread over the `state-accounts` shards by the first byte of
/// their credential hash, not into the one shard their shared CBOR header
/// would pick.
#[test]
fn accounts_export_into_the_shard_of_their_hash() {
    let domain: ToyDomain = harness();

    let seeded: Vec<(StakeCredential, u8)> = [
        (false, 0x0a),
        (true, 0x1b),
        (false, 0x5c),
        (true, 0xa0),
        (false, 0xf3),
    ]
    .into_iter()
    .map(|(script, first)| {
        let mut hash = [0x66; 28];
        hash[0] = first;
        let credential = match script {
            false => StakeCredential::AddrKeyhash(hash.into()),
            true => StakeCredential::ScriptHash(hash.into()),
        };
        (credential, first >> 4)
    })
    .collect();

    let writer = domain.state().start_writer().unwrap();
    for (credential, _) in &seeded {
        writer
            .write_entity_typed(
                &AccountKey::from(credential),
                &AccountState::new(0, credential.clone()),
            )
            .unwrap();
    }
    writer.commit().unwrap();

    let temp = tempfile::tempdir().unwrap();
    let inscription = export_to(temp.path(), &domain);

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    let mut found: BTreeMap<Vec<u8>, u8> = BTreeMap::new();

    for descriptor in inscription.layers_of_kind("state-accounts") {
        let shard = descriptor.scope["shard"].as_u64().unwrap() as u8;

        for raw in read_both_ways(&stele, &index, descriptor) {
            let record = state::decode(AccountState::NS, &raw).unwrap();
            found.insert(record.key, shard);
        }
    }

    for (credential, shard) in &seeded {
        let key = EntityKey::of(&AccountKey::from(credential));
        assert_eq!(found.get(key.as_ref()), Some(shard), "{credential:?}");
    }

    let occupied: std::collections::BTreeSet<u8> = found.values().copied().collect();
    assert!(occupied.len() > 1, "accounts collapsed into {occupied:?}");
}

fn expected_blocks<B: ToyStores>(
    domain: &ToyDomain<B>,
    window: &EpochWindow,
) -> Vec<blocks::BlockRecord> {
    let slots = window.slots();

    let mut found: Vec<blocks::BlockRecord> = domain
        .archive()
        .get_range(Some(slots.start), Some(slots.end))
        .unwrap()
        .map(|(slot, body)| {
            let hash = MultiEraBlock::decode(&body).unwrap().hash();
            blocks::BlockRecord::new(slot, hash, body)
        })
        .collect();

    found.sort_by_key(|record| record.slot);

    found
}

fn expected_logs<B: ToyStores>(
    domain: &ToyDomain<B>,
    window: &EpochWindow,
    ns: dolos_core::Namespace,
) -> Vec<logs::LogRecord> {
    let range = KeyRange::slots(window.slots());

    let mut found: Vec<logs::LogRecord> = domain
        .archive()
        .iter_logs(ns, range)
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            logs::LogRecord::new(key, value)
        })
        .collect();

    found.sort_by(|a, b| a.key.cmp(&b.key));

    found
}

fn expected_indexes<B: ToyStores>(domain: &ToyDomain<B>, window: &EpochWindow) -> Vec<IndexRecord> {
    let slots = window.slots();

    let mut tags: Vec<TagRecord> = domain
        .archive()
        .iter_archive_tags(&archive_dimensions::ALL, slots.clone())
        .unwrap()
        .map(Result::unwrap)
        .collect();

    tags.sort();

    let mut exact: Vec<ExactRecord> = domain
        .archive()
        .iter_exact_records(slots)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    exact.sort_by_key(|record| (record.kind, record.key().to_vec()));

    // Tags first, then exact records: the layer's discriminant sorts them that
    // way and the codec's order check enforces it.
    tags.into_iter()
        .map(IndexRecord::from)
        .chain(exact.into_iter().map(IndexRecord::from))
        .collect()
}

/// The state the store holds, by namespace — the grouping the layers are in
/// now, so the comparison above is per kind rather than across one mixed run.
fn expected_state<B: ToyStores>(
    domain: &ToyDomain<B>,
) -> BTreeMap<Namespace, Vec<state::StateRecord>> {
    let mut found: BTreeMap<Namespace, Vec<state::StateRecord>> = BTreeMap::new();

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        for entry in domain
            .state()
            .iter_entities(ns, dolos_core::KeyRange::all())
            .unwrap()
        {
            let (key, value) = entry.unwrap();
            found
                .entry(ns)
                .or_default()
                .push(state::entity(&key, &value));
        }
    }

    for entry in domain.state().iter_utxos().unwrap() {
        let (txo, value) = entry.unwrap();
        found
            .entry(UTXOS)
            .or_default()
            .push(state::utxo(&txo, &value).unwrap());
    }

    for records in found.values_mut() {
        records.sort_by(|a, b| a.key.cmp(&b.key));
    }

    found
}

// --------------------------------------------------------------------------
// 4. Determinism across backends
// --------------------------------------------------------------------------

/// Done criterion 3, and the claim ADR-004 rests on.
///
/// The two domains share no store implementation: the builtin in-memory state,
/// index and archive in one, the on-disk fjall trio in the other. Same genesis,
/// same blocks, same seeding — so the same inscription, byte for byte, or the
/// format does not mean what it claims.
#[test]
fn both_backends_publish_the_same_inscription() {
    // At the live cursor, which is what a publisher actually stands at, and
    // then over a plan that cuts a retained dump. The second is not a separate
    // property: a dump is written by teeing the tip's own sink, so a backend
    // whose walk order differed would produce two divergent descriptors from
    // one divergent walk, and the check that catches it has to be run against
    // a stele that carries them. The harness ledger sits inside epoch 0 and
    // epoch 0 may not be retained, so the dump-carrying plan stands at the
    // epoch-1 boundary — see `plan_at_boundary`.
    same_inscription_from_both_backends(None);
    same_inscription_from_both_backends(Some((1, RetainedEpochs::new(vec![1]).unwrap())));
}

/// The plan the comparison is run over: the live cursor, or a boundary point
/// with a retained list.
///
/// A free function rather than a closure because the two domains are two
/// types — the whole point of the comparison — and a closure cannot be generic
/// over them.
fn plan_of<B: ToyStores>(domain: &ToyDomain<B>, at: &Option<(u64, RetainedEpochs)>) -> Plan {
    match at {
        Some((epoch, retained)) => plan_at_boundary(domain, *epoch, retained.clone()),
        None => plan_for(domain),
    }
}

fn same_inscription_from_both_backends(at: Option<(u64, RetainedEpochs)>) {
    let memory: ToyDomain<MemoryStores> = harness();
    let fjall: ToyDomain<FjallStores> = harness();

    assert_eq!(
        memory.state().read_cursor().unwrap(),
        fjall.state().read_cursor().unwrap(),
        "the two domains did not end up at the same chain point"
    );

    let memory_root = tempfile::tempdir().unwrap();
    let fjall_root = tempfile::tempdir().unwrap();

    let from_memory = export_plan(memory_root.path(), &memory, &plan_of(&memory, &at));
    let from_fjall = export_plan(fjall_root.path(), &fjall, &plan_of(&fjall, &at));

    // Compared layer by layer first, so a divergence names the kind and the
    // scope it happened in rather than only moving the final digest.
    assert_eq!(
        from_memory.layers.len(),
        from_fjall.layers.len(),
        "different layer counts"
    );

    for (left, right) in from_memory.layers.iter().zip(&from_fjall.layers) {
        assert_eq!(
            (&left.kind, &left.scope, left.records, left.diff_id),
            (&right.kind, &right.scope, right.records, right.diff_id),
            "layer {:?} {} diverged between backends",
            left.kind,
            left.scope,
        );
    }

    assert_eq!(from_memory, from_fjall);
    assert_eq!(
        from_memory.digest().unwrap(),
        from_fjall.digest().unwrap(),
        "the two backends do not publish the same identity"
    );

    // Same document on disk, and the same blobs behind it.
    assert_eq!(
        std::fs::read(memory_root.path().join("inscription.json")).unwrap(),
        std::fs::read(fjall_root.path().join("inscription.json")).unwrap(),
    );

    let left = SteleDir::open(memory_root.path())
        .unwrap()
        .blob_index()
        .unwrap();
    let right = SteleDir::open(fjall_root.path())
        .unwrap()
        .blob_index()
        .unwrap();

    for descriptor in &from_memory.layers {
        assert_eq!(
            left.blob_for(&descriptor.diff_id),
            right.blob_for(&descriptor.diff_id),
            "layer {:?}",
            descriptor.kind
        );
    }
}

/// The pool changes nothing about the document — the property
/// [`export::Producers`] promises and every operator flag repeats.
///
/// One backend, one seeding, three walks: strictly serial (one producer, no
/// threads at all), the default pool, and a pool wider than the job list is
/// deep. Each runs over a plan that cuts a retained dump — so the tee through
/// `carry_again` is in the mix. Byte-identical inscriptions and blobs, or the
/// pool moved something it promised only to reschedule.
#[test]
fn a_pooled_production_is_byte_identical_to_the_serial_walk() {
    let domain: ToyDomain = harness();
    let retained = RetainedEpochs::new(vec![1]).unwrap();

    let pool_of = |producers: usize| {
        plan_at_boundary(&domain, 1, retained.clone()).with_producers(export::Producers::new(
            std::num::NonZeroUsize::new(producers).unwrap(),
        ))
    };

    let serial_root = tempfile::tempdir().unwrap();
    let pooled_root = tempfile::tempdir().unwrap();
    let wide_root = tempfile::tempdir().unwrap();

    let serial = export_plan(serial_root.path(), &domain, &pool_of(1));
    let pooled = export_plan(pooled_root.path(), &domain, &pool_of(4));
    let wide = export_plan(wide_root.path(), &domain, &pool_of(32));

    // Compared layer by layer first, so a divergence names the kind and the
    // scope it happened in rather than only moving the final digest.
    for (left, right) in serial.layers.iter().zip(&pooled.layers) {
        assert_eq!(
            (&left.kind, &left.scope, left.records, left.diff_id),
            (&right.kind, &right.scope, right.records, right.diff_id),
            "layer {:?} {} diverged between the serial and pooled walks",
            left.kind,
            left.scope,
        );
    }

    assert_eq!(serial, pooled);
    assert_eq!(serial, wide);
    assert_eq!(serial.digest().unwrap(), pooled.digest().unwrap());

    // Same document on disk, and the same blobs behind it.
    assert_eq!(
        std::fs::read(serial_root.path().join("inscription.json")).unwrap(),
        std::fs::read(pooled_root.path().join("inscription.json")).unwrap(),
    );

    let left = SteleDir::open(serial_root.path())
        .unwrap()
        .blob_index()
        .unwrap();
    let right = SteleDir::open(pooled_root.path())
        .unwrap()
        .blob_index()
        .unwrap();

    for descriptor in &serial.layers {
        assert_eq!(
            left.blob_for(&descriptor.diff_id),
            right.blob_for(&descriptor.diff_id),
            "layer {:?} {}",
            descriptor.kind,
            descriptor.scope,
        );
    }
}

/// A transport that refuses to open a sink for one kind, for the test below.
///
/// The directory twin of `tests/publish.rs`'s `Interrupted`: everything else
/// goes straight through, and `carry_again`'s default body is right here
/// because a directory's is the default.
struct DyingAt<'a> {
    inner: &'a SteleDir,
    kind: &'static str,
}

impl stelae::SteleWriter for DyingAt<'_> {
    type Sink = <SteleDir as stelae::SteleWriter>::Sink;

    fn layer_sink(
        &self,
        profile: &dyn stelae::Profile,
        spec: &stelae::LayerSpec,
        level: i32,
    ) -> Result<Self::Sink, stelae::Error> {
        if spec.kind == self.kind {
            return Err(stelae::Error::Io(std::io::Error::other(
                "the machine went away",
            )));
        }

        self.inner.layer_sink(profile, spec, level)
    }

    fn seal(
        &self,
        profile: &dyn stelae::Profile,
        inscription: &stelae::Inscription,
    ) -> Result<stelae::Digest, stelae::Error> {
        self.inner.seal(profile, inscription)
    }
}

/// A failing job fails the pooled export with its own error.
///
/// The pool's error path: the first failure stops the queues, the jobs already
/// running finish, and what the caller gets is the failure itself rather than
/// a document with a hole in it. The serial walk's version of this property is
/// exercised by every error test above; this one runs the pool.
#[test]
fn a_failing_producer_fails_the_pooled_export() {
    let domain: ToyDomain = harness();

    let root = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(root.path()).unwrap();
    let dying = DyingAt {
        inner: &stele,
        kind: BLOCKS,
    };

    let plan = plan_for(&domain).with_producers(export::Producers::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    ));

    let err = export::export(
        &dying,
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(err, dolos_snapshot::Error::Stelae(stelae::Error::Io(_))),
        "{err:?}"
    );
}

/// The dumps a stele carries, by the epoch their scope names.
fn dumps_in(
    inscription: &stelae::inscription::Inscription,
) -> BTreeMap<u64, Vec<&stelae::inscription::LayerDescriptor>> {
    let mut by_epoch: BTreeMap<u64, Vec<_>> = BTreeMap::new();

    for layer in &inscription.layers {
        if state_ns_for(&layer.kind).is_none() {
            continue;
        }

        if let Some(epoch) = layer.scope.get("epoch").and_then(serde_json::Value::as_u64) {
            by_epoch.entry(epoch).or_default().push(layer);
        }
    }

    by_epoch
}

/// Done criterion 1, the half that needs no registry: at the publish that cuts
/// it, a retained dump **is** the tip — one blob, two descriptors.
///
/// The property the whole family's riskiest code exists to hold. It is checked
/// three ways, because two of them can be true while the third is not: the
/// descriptors agree on identity and disagree only in scope; the directory
/// holds exactly as many blobs as there are distinct `diffId`s, so nothing was
/// written twice; and the pair appears in the document in the order decision
/// 0026 fixes — dumps ascending epoch, then the tip, shards ascending inside
/// each.
#[test]
fn a_dump_cut_at_the_sequence_is_the_tips_own_blob() {
    let domain: ToyDomain<MemoryStores> = harness();
    let temp = tempfile::tempdir().unwrap();

    let plan = plan_at_boundary(&domain, 1, RetainedEpochs::new(vec![1]).unwrap());
    let inscription = export_plan(temp.path(), &domain, &plan);

    let dumps = dumps_in(&inscription);
    assert_eq!(dumps.keys().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(dumps[&1].len(), state_layer_count());

    // One blob, two descriptors: same identity, same records, same size, and a
    // scope that differs in exactly the epoch.
    for dump in &dumps[&1] {
        let shard = dump.scope["shard"].as_u64().unwrap();

        let tip = inscription
            .layers
            .iter()
            .find(|layer| {
                layer.kind == dump.kind && layer.scope == serde_json::json!({"shard": shard})
            })
            .unwrap_or_else(|| panic!("no tip layer for {} shard {shard}", dump.kind));

        assert_eq!(dump.diff_id, tip.diff_id, "{}", dump.kind);
        assert_eq!(dump.records, tip.records, "{}", dump.kind);
        assert_eq!(
            dump.uncompressed_size, tip.uncompressed_size,
            "{}",
            dump.kind
        );
        assert_eq!(dump.media_type, tip.media_type, "{}", dump.kind);
        assert_eq!(dump.scope, serde_json::json!({"epoch": 1, "shard": shard}));
    }

    // And the bytes were written once. A directory names its blobs by content,
    // so two descriptors over one `diffId` are one file — the count of blobs is
    // the count of *distinct* identities and not of layers.
    let identities: std::collections::BTreeSet<_> = inscription
        .layers
        .iter()
        .map(|layer| layer.diff_id)
        .collect();

    let index = SteleDir::open(temp.path()).unwrap().blob_index().unwrap();

    assert_eq!(index.len(), identities.len());
    assert_eq!(
        inscription.layers.len(),
        identities.len() + state_layer_count(),
        "every dump shares its blob with the tip shard it was cut from",
    );

    // Per kind: the dump's shards ascending, then the tip's.
    let mut expected: Vec<(String, serde_json::Value)> = Vec::new();

    for (kind, _, shards, _) in STATE_KINDS {
        for shard in 0..shards {
            expected.push((
                kind.to_owned(),
                serde_json::json!({"epoch": 1, "shard": shard}),
            ));
        }
        for shard in 0..shards {
            expected.push((kind.to_owned(), serde_json::json!({"shard": shard})));
        }
    }

    let found: Vec<(String, serde_json::Value)> = inscription
        .layers
        .iter()
        .filter(|layer| state_ns_for(&layer.kind).is_some())
        .map(|layer| (layer.kind.clone(), layer.scope.clone()))
        .collect();

    assert_eq!(found, expected, "the state layers are out of order");
}

/// Done criterion 2: the retained list is **signed input**.
///
/// Two publishers over one ledger, standing at one point, differing in nothing
/// but their configuration, publish different documents — and the difference is
/// legible in `parameters` rather than buried in a layer. That is what makes a
/// publisher running the wrong list eject itself from co-signing instead of
/// quietly attesting a different history.
#[test]
fn two_publishers_with_different_retained_lists_do_not_agree() {
    let domain: ToyDomain<MemoryStores> = harness();

    let publish = |retained: RetainedEpochs| {
        let temp = tempfile::tempdir().unwrap();
        let plan = plan_at_boundary(&domain, 2, retained);

        (export_plan(temp.path(), &domain, &plan), temp)
    };

    let (none, _a) = publish(RetainedEpochs::default());
    let (one, _b) = publish(RetainedEpochs::new(vec![2]).unwrap());
    let (two, _c) = publish(RetainedEpochs::new(vec![1, 2]).unwrap());

    assert_eq!(none.parameters["stateEpochs"], serde_json::json!([]));
    assert_eq!(one.parameters["stateEpochs"], serde_json::json!([2]));
    assert_eq!(two.parameters["stateEpochs"], serde_json::json!([1, 2]));

    // Different parameters, therefore different identities — the divergence a
    // co-signer sees.
    let digests: std::collections::BTreeSet<_> = [&none, &one, &two]
        .into_iter()
        .map(|inscription| inscription.digest().unwrap())
        .collect();

    assert_eq!(digests.len(), 3, "two lists produced one identity");

    // The parameters are the *only* thing that moved between the first two: the
    // second carries a dump, the first does not, so the diff an operator reads
    // is one field and one layer set rather than a document they have to
    // compare by hand.
    for (left, right) in [(&none, &one), (&none, &two), (&one, &two)] {
        assert_eq!(left.position, right.position);
        assert_eq!(left.compression, right.compression);
        assert_ne!(left.parameters, right.parameters);
    }
}

/// A retained epoch below the sequence that no predecessor can hand over is a
/// warning and a shorter stele — never a failed publish.
///
/// Producing a past epoch's dump is a backfill run's job: this publish's stores
/// hold the tip, and the state as of an epoch it has moved past is not in them
/// to be written. The publish that discovers this is the ordinary case for
/// every publisher that adopts the list before it has backfilled, so it stands.
#[test]
fn a_past_dump_no_predecessor_carries_is_skipped_and_the_publish_stands() {
    let domain: ToyDomain<MemoryStores> = harness();
    let temp = tempfile::tempdir().unwrap();

    // `First` has nothing to inherit, which is exactly the adopt-miss.
    let plan = plan_at_boundary(&domain, 2, RetainedEpochs::new(vec![1, 2]).unwrap());
    let inscription = export_plan(temp.path(), &domain, &plan);

    let dumps = dumps_in(&inscription);

    assert_eq!(
        dumps.keys().copied().collect::<Vec<_>>(),
        vec![2],
        "epoch 1's dump has no source in these stores and had to be skipped",
    );
    assert_eq!(dumps[&2].len(), state_layer_count());

    // The list is still what it was configured to be: a skipped dump does not
    // rewrite the signed input, or two publishers with the same configuration
    // and different backfill states would stop agreeing on their parameters.
    assert_eq!(
        inscription.parameters["stateEpochs"],
        serde_json::json!([1, 2])
    );

    // And the stele is whole otherwise.
    assert_eq!(
        inscription
            .layers
            .iter()
            .filter(
                |layer| state_ns_for(&layer.kind).is_some() && layer.scope.get("epoch").is_none()
            )
            .count(),
        state_layer_count(),
    );
}

/// What a reproduction can and cannot check about a stele carrying dumps.
///
/// Two cases with opposite answers, and the boundary between them is the whole
/// of what `verify --reproduce` promises once dumps exist:
///
/// - the dump **at** the stele's own sequence is cut from the tip by the walk,
///   so a reproduction rebuilds it out of the stores like every other layer and
///   a divergence in it would be caught;
/// - a dump for an epoch **below** the sequence cannot be rebuilt by anything
///   standing at the tip — that epoch's state is not in these stores — so it is
///   carried forward from the document under verification and taken on trust,
///   in the same category `history` is already in.
///
/// A reproduction that insisted on building the second would report a
/// divergence on every stele carrying any state history at all, which is not
/// strictness but an unusable check.
#[test]
fn a_reproduction_rebuilds_the_dump_it_cuts_and_trusts_the_ones_it_inherits() {
    let domain: ToyDomain<MemoryStores> = harness();
    let retained = || RetainedEpochs::new(vec![1]).unwrap();

    // The publish that cuts epoch 1's dump, and its reproduction.
    let cutting = plan_at_boundary(&domain, 1, retained());
    let temp = tempfile::tempdir().unwrap();
    let cut = export_plan(temp.path(), &domain, &cutting);

    let reproduced =
        export::verify_reproduction(&cut, &cutting, domain.archive(), domain.state(), None)
            .unwrap();

    assert_eq!(reproduced, cut);
    assert_eq!(dumps_in(&reproduced)[&1].len(), state_layer_count());

    // And it really was rebuilt rather than copied: a reproduction chained the
    // same way but with nothing to inherit from produces the same document.
    let from_nothing = export::reproduce(
        &cutting,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
    )
    .unwrap();

    assert_eq!(from_nothing.layers, cut.layers);

    // The publish that inherits it. Built through `Following`, which is the
    // predecessor a verifier chaining from a canonical document holds.
    let following_plan = plan_at_boundary(&domain, 2, retained());
    let canonical = cut.canonicalize().unwrap();

    let followed = export::reproduce(
        &following_plan,
        domain.archive(),
        domain.state(),
        None,
        &export::Following::read(&canonical, &following_plan).unwrap(),
    )
    .unwrap();

    // Epoch 1's dump rode across, descriptor for descriptor.
    assert_eq!(dumps_in(&followed)[&1], dumps_in(&cut)[&1]);

    // And verifying that document reproduces it — which is the case that would
    // fail outright if an inherited dump had to be rebuilt from these stores.
    let verified = export::verify_reproduction(
        &followed,
        &following_plan,
        domain.archive(),
        domain.state(),
        None,
    )
    .unwrap();

    assert_eq!(verified, followed);
}

/// A predecessor that lends every inheritable layer of an earlier stele, as a
/// registry's chained publish does from a manifest or a resumption record, and
/// counts what it is asked for and what it lends.
struct Lending {
    history: Vec<stelae::inscription::HistoryEntry>,
    layers: Vec<stelae::inscription::LayerDescriptor>,
    asked: std::sync::Mutex<Vec<(String, u64)>>,
    lent: AtomicUsize,
    landed: std::sync::Mutex<Vec<stelae::inscription::LayerDescriptor>>,
}

impl Lending {
    /// A publish of `plan` chained onto `predecessor`, lending its layers.
    fn following(predecessor: &stelae::inscription::Inscription, plan: &Plan) -> Self {
        let canonical = predecessor.canonicalize().unwrap();
        let following = export::Following::read(&canonical, plan).unwrap();

        Self::with_history(following.history().to_vec(), predecessor)
    }

    /// A first publish resuming from a record of `interrupted`'s layers: no
    /// history, and the layers to lend.
    fn resuming(interrupted: &stelae::inscription::Inscription) -> Self {
        Self::with_history(vec![], interrupted)
    }

    fn with_history(
        history: Vec<stelae::inscription::HistoryEntry>,
        lender: &stelae::inscription::Inscription,
    ) -> Self {
        Self {
            history,
            layers: lender.layers.clone(),
            asked: Default::default(),
            lent: AtomicUsize::new(0),
            landed: Default::default(),
        }
    }

    fn find(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Option<stelae::inscription::LayerDescriptor> {
        self.layers
            .iter()
            .find(|layer| layer.kind == kind && &layer.scope == scope)
            .filter(|_| dolos_snapshot::is_inheritable(kind, scope))
            .cloned()
    }

    /// The windows a `log-account-epochs` layer was asked for.
    fn account_windows(&self) -> Vec<u64> {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(kind, _)| kind == "log-account-epochs")
            .map(|(_, epoch)| *epoch)
            .collect()
    }

    /// The forecast for `plan` against `domain`'s stores is what the export
    /// through this predecessor, `exported`, built and lent.
    fn forecast_matches(
        &self,
        plan: &Plan,
        domain: &ToyDomain<MemoryStores>,
        exported: &stelae::inscription::Inscription,
    ) {
        let forecast = export::forecast(plan, domain.archive(), self, None).unwrap();
        let lent = self.lent.load(Ordering::Relaxed);

        assert_eq!(
            forecast,
            export::Forecast {
                layers_built: exported.layers.len() - lent,
                layers_reused: lent,
            }
        );
    }
}

impl export::Predecessor for Lending {
    fn history(&self) -> &[stelae::inscription::HistoryEntry] {
        &self.history
    }

    fn adopt(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<stelae::inscription::LayerDescriptor>, stelae_driver::Error> {
        if let Some(epoch) = scope.get("epoch").and_then(serde_json::Value::as_u64) {
            self.asked.lock().unwrap().push((kind.to_owned(), epoch));
        }

        let found = self.find(kind, scope);
        self.lent
            .fetch_add(usize::from(found.is_some()), Ordering::Relaxed);

        Ok(found)
    }

    fn carried_forward(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<bool, stelae_driver::Error> {
        Ok(self.find(kind, scope).is_some())
    }

    fn landed(
        &self,
        descriptor: &stelae::inscription::LayerDescriptor,
    ) -> Result<(), stelae_driver::Error> {
        self.landed.lock().unwrap().push(descriptor.clone());

        Ok(())
    }
}

fn reproduce_fresh(
    plan: &Plan,
    domain: &ToyDomain<MemoryStores>,
) -> stelae::inscription::Inscription {
    export::reproduce(plan, domain.archive(), domain.state(), None, &export::First).unwrap()
}

/// Finding 1 of the QA review of dolos#1499: a stele cut at 2 carries window
/// 1's account rows as ESTART wrote them, stake leg alone; the EWRAP closing 2
/// rewrites them in full before 3 is cut. The publish at 3 builds that window's
/// layer from the stores rather than adopt the stale one, and still adopts
/// window 0's, so a chained publish equals one with nothing to inherit, and
/// its forecast counts what it then builds and lends.
#[test]
fn a_chained_publish_rebuilds_the_account_rows_ewrap_rewrote() {
    let domain: ToyDomain<MemoryStores> = harness();

    write_account_row(&domain, 1, stake_only());

    let cut_at_2 = plan_at_boundary(&domain, 2, RetainedEpochs::default());
    let predecessor = reproduce_fresh(&cut_at_2, &domain);

    write_account_row(&domain, 1, settled());

    let cut_at_3 = plan_at_boundary(&domain, 3, RetainedEpochs::default());
    let lending = Lending::following(&predecessor, &cut_at_3);

    let chained =
        export::reproduce(&cut_at_3, domain.archive(), domain.state(), None, &lending).unwrap();

    assert_eq!(chained.layers, reproduce_fresh(&cut_at_3, &domain).layers);

    // Window 1 really changed, and window 0 really was lent.
    assert_ne!(account_layer(&chained, 1), account_layer(&predecessor, 1));
    assert_eq!(account_layer(&chained, 0), account_layer(&predecessor, 0));

    assert_eq!(lending.account_windows(), vec![0]);
    let asked = lending.asked.lock().unwrap().clone();
    assert!(asked.contains(&("log-epochs".to_owned(), 1)), "{asked:?}");

    lending.forecast_matches(&cut_at_3, &domain, &chained);
}

/// Finding A of the second QA review of dolos#1499: a first publish
/// interrupted at 2 recorded window 1's stake-only account rows, and its retry
/// at 4, past the boundary that settled them, has only that record to lend
/// from. A record does not say when it built a layer, so the retry builds every
/// account window from its stores and still takes every other kind.
#[test]
fn a_resumed_first_publish_builds_every_account_window() {
    let domain: ToyDomain<MemoryStores> = harness();

    write_account_row(&domain, 1, stake_only());

    let cut_at_2 = plan_at_boundary(&domain, 2, RetainedEpochs::default());
    let interrupted = reproduce_fresh(&cut_at_2, &domain);

    write_account_row(&domain, 1, settled());

    let cut_at_4 = plan_at_boundary(&domain, 4, RetainedEpochs::default());
    let resuming = Lending::resuming(&interrupted);

    let resumed =
        export::reproduce(&cut_at_4, domain.archive(), domain.state(), None, &resuming).unwrap();

    assert_eq!(resumed.layers, reproduce_fresh(&cut_at_4, &domain).layers);
    assert_ne!(account_layer(&resumed, 1), account_layer(&interrupted, 1));

    assert!(resuming.account_windows().is_empty());
    let asked = resuming.asked.lock().unwrap().clone();
    assert!(asked.contains(&("blocks".to_owned(), 1)), "{asked:?}");
    assert!(resuming.lent.load(Ordering::Relaxed) > 0);

    resuming.forecast_matches(&cut_at_4, &domain, &resumed);
}

/// Finding E of the third QA review of dolos#1499: a host's publish at 2 was
/// interrupted after it recorded window 1's stake-only account rows. Another
/// host published 2, then 3 restricted to fewer epochs, so the manifest at 3
/// lacks window 1. The first host's publish at 4 finds window 1 settled and the
/// manifest silent, and the record would lend the stale layer. Held to the
/// manifest, it builds the window from its stores instead.
#[test]
fn a_record_never_supplies_an_account_window_the_manifest_lacks() {
    let domain: ToyDomain<MemoryStores> = harness();

    write_account_row(&domain, 1, stake_only());

    let cut_at_2 = plan_at_boundary(&domain, 2, RetainedEpochs::default());
    let interrupted = reproduce_fresh(&cut_at_2, &domain);

    write_account_row(&domain, 1, settled());

    let cut_at_3 = plan_at_boundary(&domain, 3, RetainedEpochs::default());
    let mut restricted = reproduce_fresh(&cut_at_3, &domain);
    let window_1 = account_layer(&restricted, 1);
    restricted.layers.retain(|layer| *layer != window_1);

    let cut_at_4 = plan_at_boundary(&domain, 4, RetainedEpochs::default());
    let lending_record = || {
        let mut lending = Lending::following(&restricted, &cut_at_4);
        lending.layers.push(account_layer(&interrupted, 1));
        lending
    };

    // Without the manifest's word, the record's stake-only rows ride along.
    let unheld = export::reproduce(
        &cut_at_4,
        domain.archive(),
        domain.state(),
        None,
        &lending_record(),
    )
    .unwrap();
    assert_eq!(account_layer(&unheld, 1), account_layer(&interrupted, 1));

    let settling = export::Settling::new(lending_record(), Some(&restricted)).unwrap();
    let held =
        export::reproduce(&cut_at_4, domain.archive(), domain.state(), None, &settling).unwrap();

    assert_eq!(held.layers, reproduce_fresh(&cut_at_4, &domain).layers);
    assert_eq!(account_layer(&held, 1), window_1);

    // Window 0 is in the manifest and still lent; window 1 never reached the
    // record's lender.
    assert_eq!(settling.inner().account_windows(), vec![0]);

    let forecast = export::forecast(&cut_at_4, domain.archive(), &settling, None).unwrap();
    let lent = settling.inner().lent.load(Ordering::Relaxed);

    assert_eq!(
        forecast,
        export::Forecast {
            layers_built: held.layers.len() - lent,
            layers_reused: lent,
        }
    );
}

/// Finding H of the fourth QA review of dolos#1499: the settling wrapper hands
/// every layer that lands to the predecessor it wraps, which is where a
/// registry publish writes its resumption record.
#[test]
fn a_settling_predecessor_passes_each_landed_layer_on() {
    let domain: ToyDomain<MemoryStores> = harness();

    let cut_at_2 = plan_at_boundary(&domain, 2, RetainedEpochs::default());
    let predecessor = reproduce_fresh(&cut_at_2, &domain);

    let cut_at_3 = plan_at_boundary(&domain, 3, RetainedEpochs::default());
    let landed = |predecessor: &dyn export::Predecessor| {
        export::reproduce(
            &cut_at_3,
            domain.archive(),
            domain.state(),
            None,
            predecessor,
        )
        .unwrap()
    };

    let bare = Lending::following(&predecessor, &cut_at_3);
    landed(&bare);

    let settling = export::Settling::new(
        Lending::following(&predecessor, &cut_at_3),
        Some(&predecessor),
    )
    .unwrap();
    landed(&settling);

    let sorted = |lending: &Lending| {
        let mut layers = lending.landed.lock().unwrap().clone();
        layers.sort_by_key(|layer| (layer.kind.clone(), layer.scope.to_string()));
        layers
    };

    assert!(!sorted(&bare).is_empty());
    assert_eq!(sorted(settling.inner()), sorted(&bare));
}

/// Finding C of the second QA review of dolos#1499: the forecast asks the
/// settling rule too. The predecessor at 2 holds window 1's stake-only account
/// rows, and these stores, cut at 3, hold none there, so the publish writes no
/// such layer and the forecast counts none.
#[test]
fn a_forecast_counts_no_unsettled_account_window_the_stores_lack() {
    let source: ToyDomain<MemoryStores> = harness();

    write_account_row(&source, 1, stake_only());

    let cut_at_2 = plan_at_boundary(&source, 2, RetainedEpochs::default());
    let predecessor = reproduce_fresh(&cut_at_2, &source);

    let domain: ToyDomain<MemoryStores> = harness();
    let cut_at_3 = plan_at_boundary(&domain, 3, RetainedEpochs::default());
    let lending = Lending::following(&predecessor, &cut_at_3);

    let chained =
        export::reproduce(&cut_at_3, domain.archive(), domain.state(), None, &lending).unwrap();

    assert!(!chained.layers.iter().any(|layer| {
        layer.kind == "log-account-epochs"
            && layer.scope.get("epoch").and_then(serde_json::Value::as_u64) == Some(1)
    }));

    lending.forecast_matches(&cut_at_3, &domain, &chained);
}

/// The whole skeleton document, so a change to a key spelling, a scope shape or
/// the order layers are listed in reads as text in the diff rather than only as
/// a moved hash.
///
/// JCS sorts object keys, so the layout is the protocol's; every string in it
/// is this profile's.
const CANONICAL_SKELETON: &str = concat!(
    r#"{"compression":{"algo":"zstd","level":9},"history":[],"layers":["#,
    r#"{"diffId":"sha256:922ee4d9b71402ecba461e7b11a76ff61354d2a0be45bb040e5e2ccb80ccb5a1","kind":"blocks","mediaType":"application/vnd.dolos.stelae.blocks.v1+zstd","records":1,"scope":{"endSlot":99,"epoch":0,"startSlot":0},"uncompressedSize":43},"#,
    r#"{"diffId":"sha256:c0dba8cfa17803797dc39470fa963f21e19e61a534fdea1c480f4160000f4b6d","kind":"blocks","mediaType":"application/vnd.dolos.stelae.blocks.v1+zstd","records":2,"scope":{"endSlot":199,"epoch":1,"startSlot":100},"uncompressedSize":144},"#,
    r#"{"diffId":"sha256:8013ed25e0a7280f0cb5e9fdcf70a8b3218356b27d25852566fed6515b85b894","kind":"blocks","mediaType":"application/vnd.dolos.stelae.blocks.v1+zstd","records":2,"scope":{"endSlot":250,"epoch":2,"startSlot":200},"uncompressedSize":177},"#,
    r#"{"diffId":"sha256:0517f93e76f9fe6059bcde6218af5c90f51020af09890c5e3a05cc8f3a32e447","kind":"indexes","mediaType":"application/vnd.dolos.stelae.indexes.v1+zstd","records":1,"scope":{"endSlot":99,"epoch":0,"startSlot":0},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:b64ae324280c4335089cdf9918ac0596fe7f429dbf5a54c2fd2d44f25280b935","kind":"indexes","mediaType":"application/vnd.dolos.stelae.indexes.v1+zstd","records":1,"scope":{"endSlot":199,"epoch":1,"startSlot":100},"uncompressedSize":45},"#,
    r#"{"diffId":"sha256:a76e2bb47934f8473a232a31da4930db8d50ce90a4cab2c232fc032644fff62e","kind":"indexes","mediaType":"application/vnd.dolos.stelae.indexes.v1+zstd","records":1,"scope":{"endSlot":250,"epoch":2,"startSlot":200},"uncompressedSize":45},"#,
    r#"{"diffId":"sha256:8da91bb0d7584079f11981e96d72bc9fd638b339a93ae5cd01fe42eaf6cb204f","kind":"state-account-addresses","mediaType":"application/vnd.dolos.stelae.state-account-addresses.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":58},"#,
    r#"{"diffId":"sha256:211a38851621f1de268f9f4cd0f9a07f7409fc387ed34525756e299f821cb790","kind":"state-account-history","mediaType":"application/vnd.dolos.stelae.state-account-history.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":56},"#,
    r#"{"diffId":"sha256:676465ffbf751fc1e1407fcad19d0c71c17b3e089903a20c79185e13c65f3311","kind":"state-account-stats","mediaType":"application/vnd.dolos.stelae.state-account-stats.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":54},"#,
    r#"{"diffId":"sha256:02522facba4552263d4dfaded9dbb82ec8a90937284a15539f6f1f7a7a9ef4fd","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:9c3e2a317428abc07cd8a29934c10d6ed406eef31b0ffebbaa95679ed11a4e89","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:bad9e2528c56aab15c5c593550f17bd850d52804dd17172fefb4b11940641d8d","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:9d296077954a6e1a79ec9139edc2b73c4f292999e697f99e688887045a1ab1e2","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:36a70b77ff0e7ff67d3e42a4dbf9aafc7c3e9e093d2fa0a746253f5d693b5362","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:48ddd612a9127f81231cd901135c791bc9b812bb54ee95d9e3544962e1b3a46c","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:4cb5ff0a33d2e88172898f0565fb91d0c3c0e3f636daff1ec0ab50c2da7a8dd1","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:0eb957c6d2d2b2ae787539fafacfccdd9a2aaad410d5af096fed5b60e5d30967","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:b3f376aa124ed9110f4d6ee6a30848bc8cbec2eaec65020f39439849d75e304e","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:96b86a20d7ba613efa3e1925148b2a9ccaa969c75ca4dc466434a46e0fe6a82f","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:462a4295f3c6ee4d43b2c201d166a225eb59d55e4880e66fe3b947d005a48132","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:87a29f9edd16dc7521df050f8645b99082df697fa42e772e262a61a5b756497a","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:b478aa2b0c1385dfeba5f26da0edc223260f0cac947d91da6d1dc629d48a7541","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:a2c9fa81aec9cac32b36fd325bdf3afe6ac72a95c7412cfaff6f05288354b085","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:5cf477db9c26c4f251c836844db2405f37fa789d03aea868540eb07cdf43bdfe","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:964771e3ed841725948305ad74029c7be92b0aad4e6c43a7569fb09b8e67253f","kind":"state-accounts","mediaType":"application/vnd.dolos.stelae.state-accounts.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:4400a639af46be80f2a3e76d30fc196fdf750f42513a5be55236c47949d9f3c8","kind":"state-address-stats","mediaType":"application/vnd.dolos.stelae.state-address-stats.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":54},"#,
    r#"{"diffId":"sha256:64f06aaf8b772ba285c46d00936a46ca9ca3782eb14267c363877870a3111804","kind":"state-asset-mints","mediaType":"application/vnd.dolos.stelae.state-asset-mints.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:5c79490ba5bd0febb78a297aa3273fff7119c9af37fa48a8fba01b3c4e93ba1a","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:128bba0aeae290cbe00284557a591198676a94dd8c948c5c5b8bf78deb892978","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:a1f52ea5164adaecaa2cdb1e306772afe5209fbe81d8ab1bcd66badfeb5238d5","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:468198f05873a9c1c748e6ed213fa0307b7bd6f1baf3784ec3ec765ca3b03383","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:f553bf184e843725c49e9645560876519f9e6994f2bf468efd90150a58ad45fb","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:0cd44b79cc2336ba42d9df42979c9145e391f98b028407e9895e24ceae188e72","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:99855d833d23137d927485ebe6a610b0640bafa7b89e8eceaf5b57754e75927b","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:3325d1e65ccc52abede1ba2bd35f4289af41afbd8f1ff3007c659fe12fb85e00","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:f8b305e7a74ddd9b49f36dc0af339fd1fd0bb600e7a1c86c99c3ad46e1b61fac","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:151817f8cc62d02d9bb9c7980df48240776c1b66f759ee6665057d3ee0056fbb","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:4775430b2bdf697c94bb53d77b304e6ddb7a16b4a51d533bf21915d1ac96c5df","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:df563c54b5d255acc3358a33271f0816673cb8e9ce2e3c29960f55e60d25baf5","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:b4945645a766617aaf3bcfbc09363b107c507455dcc4482e31c42b2a51fcb7a0","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:8140e32dd30b763b9e30d4ec9e3f4c4839674271068028c0eb280acf95fb6803","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:2ac9b61903e09ba2a01e2192cd9e6a1c0ecb30886d3752374cf0d45dfdc3fbda","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:d1f06100a97cbf233453ee235ae2af84fed2ebc9afba1ef44f1a560fa9092517","kind":"state-datums","mediaType":"application/vnd.dolos.stelae.state-datums.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:51446cf98e5fa84c241d0739fb69467a94a21066b46f9bd93eed46197c106b3e","kind":"state-drep-observations","mediaType":"application/vnd.dolos.stelae.state-drep-observations.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":58},"#,
    r#"{"diffId":"sha256:0c02e395163b8721766f4e70ade836dcbeabcb5b660e6538caee76757315305e","kind":"state-dreps","mediaType":"application/vnd.dolos.stelae.state-dreps.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:d2956a7d3251484d5ef8f45cd923e34ecbc890552eac579c500ba99b0a98cb9a","kind":"state-epochs","mediaType":"application/vnd.dolos.stelae.state-epochs.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:41fb8cc7b96fe6c203c328d01183f645ea55ebeec5aff6a63e50551a4b46bfa7","kind":"state-eras","mediaType":"application/vnd.dolos.stelae.state-eras.v1+zstd","records":2,"scope":{"shard":0},"uncompressedSize":60},"#,
    r#"{"diffId":"sha256:44040dbf942196735ba5eae62b2bb978c26618e62660156f45cf0ef6fef2db04","kind":"state-first-mints","mediaType":"application/vnd.dolos.stelae.state-first-mints.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:40710b7e955345354c8abd21ffdf9abcef049a7235cf40ae5b55bfb7670c9f85","kind":"state-first-scripts","mediaType":"application/vnd.dolos.stelae.state-first-scripts.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":54},"#,
    r#"{"diffId":"sha256:0a74ea019634f7c0135b1030e692d4e3680ff534c89462819fa04c15ada63114","kind":"state-gov","mediaType":"application/vnd.dolos.stelae.state-gov.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:488200771f6a1df2845680a65a9223c90e76e4c3fa0abfc0f32d4fcefc960516","kind":"state-metadata-labels","mediaType":"application/vnd.dolos.stelae.state-metadata-labels.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":56},"#,
    r#"{"diffId":"sha256:8c9c5bef47aa5ead3f5d7a913049385a75cfa6456dccff521c5bdd4d74cb6e44","kind":"state-payment-addresses","mediaType":"application/vnd.dolos.stelae.state-payment-addresses.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":58},"#,
    r#"{"diffId":"sha256:30da56c590c6877fce53f5bfcdb692c728930d88210ec399b5ca74ab3923ce04","kind":"state-payment-stats","mediaType":"application/vnd.dolos.stelae.state-payment-stats.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":54},"#,
    r#"{"diffId":"sha256:2e07a341e23cf37667da069d3e75ba7a7bf2665572f2183353ff0266369cf17f","kind":"state-pending-mirs","mediaType":"application/vnd.dolos.stelae.state-pending-mirs.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":53},"#,
    r#"{"diffId":"sha256:ad5994d901c69e757457b2ad6bac6a2679e45ae25769cc616375c10c8c6dc582","kind":"state-pending-rewards","mediaType":"application/vnd.dolos.stelae.state-pending-rewards.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":56},"#,
    r#"{"diffId":"sha256:da799488bcc20ea1d8c8e236169d7204e328b34f806075a951f266819bd35e34","kind":"state-pools","mediaType":"application/vnd.dolos.stelae.state-pools.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:640c0f9db84510212ae0d71860df7c518244db6eed6728e9fcc6540d99bdcbe7","kind":"state-proposals","mediaType":"application/vnd.dolos.stelae.state-proposals.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":50},"#,
    r#"{"diffId":"sha256:6978ebcdd1accb802d0711afbe6c25747eea28f533de050c184ea24bade637a5","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:aa056a5c4a03811c52492eec692ba0612f7f32f9c447ddd63f64a4c7b1c5ed4a","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:c0055cfef082a8ebe5ada0637de774cc2613d00714d05f74b19690ced71e89c2","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:5b298cc5852e2c25e9bccfcfcc7c379e68b55439b34ecdbbd26fb93d5306dbfd","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:09081604a9f2fc740fdeabb3a331ff3d35cdde0179a7a95271a2b43079d5962b","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:86e95890bde0bf9230b48d9ff30d0e37b3e4441f041046d9674d3aecc2381284","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:e6fe3b69728c8f4587689733e537d593ba4901e175d1b81f583ad6fe3f30b48a","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:9fc971b8fe01687d280947d27b4b495b8a13b11696b61379c81701db821e1d12","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:a3ddecb338e823bff9f3269571ae9f8858451ca8f98700501d57273ac141ec01","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:20c25aaf590428fd30f7ff2e17cdf8b12e2d4819dbdbd27cd36b2e3a59c79bf1","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:cd57e7f5986a2e811577422b7a174d02a786ea75c79b855a547292aaa3cb9801","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:ac1e3bdff176deca129ff6ea85f8e32d98c56eb55845fbbb535e145817c2f27c","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:1b257f003b61fd4389b580e368725779e84d0931916f4e5f21336e3b5775462f","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:4145245584090ba5ec600cc41743036cd02580999490cd38e94ccb47e713f529","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:fa3101937f3491ae614180384787cfeb1d2ffb7e522c4c731b60208472573917","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:e59d8b7ec7144216a9caab188b2de8d09d98c7c419e228a41131722796b81711","kind":"state-utxos","mediaType":"application/vnd.dolos.stelae.state-utxos.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":46}"#,
    r#"],"parameters":{"#,
    r#""entities":{"asset":32,"script":28,"stake":29},"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-epochs":1,"account-addresses":1,"account-epochs":1,"account-history":1,"account-stats":1,"accounts":1,"address-stats":1,"asset-mint-chunks":1,"asset-mints":1,"datums":1,"drep-epochs":1,"drep-observations":1,"dreps":1,"epochs":1,"eras":1,"first-mint-chunks":1,"first-mints":1,"first-script-chunks":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-address-epochs":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-first-mint-chunks":1,"pool-epochs":1,"pools":1,"proposals":1,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-history":1,"account-stats":1,"accounts":16,"address-stats":1,"asset-mints":1,"datums":16,"drep-observations":1,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"first-scripts":1,"gov":1,"metadata-labels":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"utxos":16},"#,
    r#""stateEpochs":[]"#,
    r#"},"position":{"epoch":2,"network":{"magic":764824073,"name":"mainnet"},"point":{"hash":"018f99053185b9f7bae6fad0b8aa50710ebc131352dbb47f32d5030622bf51df","slot":250}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":2}"#,
);

// --------------------------------------------------------------------------
// 5. A stele reproduced without being stored
// --------------------------------------------------------------------------

/// The check that the discarding writer is *faithful* rather than merely fast,
/// and no other check stands in for it.
///
/// Two exports over one store set: one into a directory through the retained
/// profile encoder, and one into nothing, as `snapshot digest` does. The
/// comparison is on the canonical bytes rather than on
/// the digest — the digest is a function of them, so an equal digest is
/// implied, while an unequal document says *where* in a two-kilobyte JSON the
/// two disagreed.
///
/// Run over the harness ledger rather than the skeleton on purpose: an empty
/// store set exercises the layer *skeleton*, and what a discarding writer could
/// plausibly get wrong is a layer with records in it — the compressor's state,
/// the record count, the uncompressed size.
#[test]
fn a_discarding_export_reproduces_what_a_publish_stores() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let temp = tempfile::tempdir().unwrap();
    let stored = export_to(temp.path(), &domain);

    let reproduced = export::export(
        &Discarding,
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    // Non-trivial: a stele of empty layers would compare equal for the wrong
    // reason.
    assert!(
        stored.uncompressed_size() > 1024,
        "the fixture has to carry records for this to prove anything"
    );

    assert_eq!(
        String::from_utf8(reproduced.canonicalize().unwrap()).unwrap(),
        String::from_utf8(stored.canonicalize().unwrap()).unwrap(),
    );

    assert_eq!(reproduced.digest().unwrap(), stored.digest().unwrap());

    // Nothing was written on the reproduction's behalf: the only stele on disk
    // is the one the directory export made, and it holds exactly its own
    // blobs.
    let blobs = std::fs::read_dir(temp.path().join("blobs").join("sha256"))
        .unwrap()
        .count();

    assert_eq!(blobs, distinct_layers(&stored));
}

/// A tip whose bookkeeping shows a boundary pass in flight is refused before a
/// layer: a sync cursor left on the epoch row, or a governance distribution
/// with a shard still to merge. The same stores export once it is finished.
#[test]
fn a_tip_with_unfinished_bookkeeping_is_refused_before_a_layer() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let epoch = domain
        .state()
        .read_entity_typed::<EpochState>(&())
        .unwrap()
        .expect("the harness has an epoch row");
    let gov = domain.state().read_entity_typed::<GovState>(&()).unwrap();

    let write = |epoch: &EpochState, gov: Option<&GovState>| {
        let writer = domain.state().start_writer().unwrap();
        writer.write_entity_typed(&(), epoch).unwrap();
        writer.save_entity_typed::<GovState>(&(), gov).unwrap();
        writer.commit().unwrap();
    };

    let refusal = || {
        let watcher = Arc::new(Watcher::default());
        let temp = tempfile::tempdir().unwrap();
        let stele = SteleDir::create(temp.path()).unwrap();

        let refused = export::export(
            &stele,
            &plan,
            domain.archive(),
            domain.state(),
            None,
            &export::First,
            &watcher.observer(),
        );

        assert_eq!(watcher.layers(), 0, "a refused export announced a layer");

        match refused {
            Err(dolos_snapshot::Error::NotABoundary { reason, .. }) => reason,
            other => panic!("{other:?}"),
        }
    };

    let mut mid_rupd = epoch.clone();
    mid_rupd.rupd_progress = Some(ShardProgress {
        committed: 3,
        total: 32,
    });
    write(&mid_rupd, gov.as_ref());

    assert_eq!(
        refusal(),
        Ineligible::SyncCursorsSet {
            cursors: vec!["rupd_progress"]
        }
    );

    let mut partial = GovDistr::new(epoch.number, 32);
    partial.committed_shards = 5;

    let mid_ewrap = GovState {
        distr: Some(partial),
        ..gov.clone().unwrap_or_default()
    };
    write(&epoch, Some(&mid_ewrap));

    assert_eq!(
        refusal(),
        Ineligible::IncompleteGovDistr {
            field: "distr",
            committed: 5,
            total: 32,
        }
    );

    write(&epoch, gov.as_ref());

    let temp = tempfile::tempdir().unwrap();
    export_to(temp.path(), &domain);
}

/// A store set one block past its boundary is refused the same way by every
/// path that builds layers — directory publication, digest, reproduction and
/// the headless facade over all three — and before any of them opens a layer.
#[test]
fn every_path_refuses_a_cursor_past_the_boundary_before_a_layer() {
    let domain: ToyDomain = harness();

    let eligible = tempfile::tempdir().unwrap();
    let published = export_to(eligible.path(), &domain);
    let first = domain.state().read_cursor().unwrap().unwrap();

    step_past_boundary(&domain);

    let plan = plan_for(&domain);
    assert_eq!(
        plan.sequence, 1,
        "one block past the boundary, still in epoch 1"
    );

    let expected = Ineligible::LaterBlock { parent: first };

    fn reason<T: std::fmt::Debug>(result: Result<T, dolos_snapshot::Error>) -> Ineligible {
        match result {
            Err(dolos_snapshot::Error::NotABoundary { reason, .. }) => reason,
            other => panic!("{other:?}"),
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let watcher = Arc::new(Watcher::default());
    let stele = SteleDir::create(temp.path().join("export")).unwrap();

    assert_eq!(
        reason(export::export(
            &stele,
            &plan,
            domain.archive(),
            domain.state(),
            None,
            &export::First,
            &watcher.observer(),
        )),
        expected
    );
    assert_eq!(watcher.layers(), 0, "a refused export announced a layer");

    let directory = temp.path().join("publish");

    assert_eq!(
        reason(export::publish(
            &directory,
            &plan,
            domain.archive(),
            domain.state(),
            None,
            &Observer::silent(),
        )),
        expected
    );

    for root in [temp.path().join("export"), directory.clone()] {
        let blobs = root.join("blobs").join("sha256");

        assert!(
            !blobs.exists() || std::fs::read_dir(&blobs).unwrap().next().is_none(),
            "a refused publish wrote a layer into {}",
            root.display()
        );
    }

    assert_eq!(
        reason(export::reproduce(
            &plan,
            domain.archive(),
            domain.state(),
            None,
            &export::First,
        )),
        expected
    );

    assert_eq!(
        reason(export::digest_document(
            &plan,
            domain.archive(),
            domain.state(),
            &export::First,
        )),
        expected
    );

    assert_eq!(
        reason(export::verify_reproduction(
            &published,
            &plan,
            domain.archive(),
            domain.state(),
            None,
        )),
        expected
    );

    let source = StoreSnapshot::new(domain.archive(), domain.state());

    assert_eq!(reason(source.boundary(&plan)), expected);
    assert_eq!(
        reason(source.publish_directory(&temp.path().join("facade"), &plan, &Observer::silent())),
        expected
    );
    assert_eq!(
        reason(source.digest_document(&plan, &export::First)),
        expected
    );
    assert_eq!(
        reason(source.verify_reproduction(&published, &plan)),
        expected
    );
}

/// A plan made while the stores stood on the boundary does not carry them past
/// it: once they move on, the export refuses the stale plan for the state it
/// no longer describes, before any layer. The plan alone would pass every
/// other test of the rule, so the state cursor is what is held against it.
#[test]
fn a_plan_the_stores_have_moved_past_is_refused_before_a_layer() {
    let domain: ToyDomain = harness();

    let plan = plan_for(&domain);
    let moved = step_past_boundary(&domain);

    let watcher = Arc::new(Watcher::default());
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let refused = export::export(
        &stele,
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &watcher.observer(),
    );

    match refused {
        Err(dolos_snapshot::Error::NotABoundary { reason, .. }) => {
            assert_eq!(
                reason,
                Ineligible::PlanMismatch {
                    stored: Some(moved)
                }
            )
        }
        other => panic!("{other:?}"),
    }

    assert_eq!(watcher.layers(), 0, "a refused export announced a layer");
}

/// An archive that runs past a boundary state, as `data rebuild
/// --stop-epoch` over a synced archive leaves one, is refused before a layer:
/// the blocks after the cursor wrote log and index rows inside the stele's
/// windows. A plan at the archive's tip is refused for the state it does not
/// describe.
#[test]
fn an_archive_past_a_boundary_state_is_refused_before_a_layer() {
    let domain: ToyDomain = harness();

    let boundary = domain.state().read_cursor().unwrap().unwrap();
    let tip = extend_archive_past_cursor(&domain);

    let plan = plan_for(&domain);
    assert_eq!(plan.cursor, boundary);

    let ahead = Plan {
        cursor: tip.clone(),
        ..plan.clone()
    };

    for (plan, expected) in [
        (plan, Ineligible::ArchivePastCursor { tip }),
        (
            ahead,
            Ineligible::PlanMismatch {
                stored: Some(boundary),
            },
        ),
    ] {
        let watcher = Arc::new(Watcher::default());
        let temp = tempfile::tempdir().unwrap();
        let stele = SteleDir::create(temp.path()).unwrap();

        let refused = export::export(
            &stele,
            &plan,
            domain.archive(),
            domain.state(),
            None,
            &export::First,
            &watcher.observer(),
        );

        match refused {
            Err(dolos_snapshot::Error::NotABoundary { reason, .. }) => {
                assert_eq!(reason, expected)
            }
            other => panic!("{other:?}"),
        }

        assert_eq!(watcher.layers(), 0, "a refused export announced a layer");
    }
}

/// Log rows keyed past the cursor stay out of the stele: the last window ends
/// at the cursor's slot whatever the log tables hold beyond it.
#[test]
fn log_rows_past_the_cursor_stay_out_of_the_stele() {
    let domain: ToyDomain = harness();

    let cursor = domain.state().read_cursor().unwrap().unwrap();
    let writer = domain.archive().start_writer().unwrap();

    for (i, ns) in LOG_NAMESPACES.into_iter().enumerate() {
        writer
            .write_log(
                ns,
                &common::log_key(ns, cursor.slot() + 5, 0x30 + i as u8),
                &vec![0x81, i as u8],
            )
            .unwrap();
    }

    writer.commit().unwrap();

    let temp = tempfile::tempdir().unwrap();
    let published = export_to(temp.path(), &domain);

    let bare: ToyDomain = harness();
    let temp = tempfile::tempdir().unwrap();
    assert_eq!(published.layers, export_to(temp.path(), &bare).layers);
}

/// A block after the cursor's at the cursor's own slot, as a Byron main block
/// sits beside the boundary block it follows, is refused before a layer. The
/// last window ends at that slot, and its index and log records carry no block
/// hash to leave the extra block's out by.
#[test]
fn a_block_beside_the_cursor_at_its_slot_is_refused_before_a_layer() {
    let domain: ToyDomain = harness();

    let cursor = domain.state().read_cursor().unwrap().unwrap();
    let body = domain
        .archive()
        .get_block_by_slot(&cursor.slot())
        .unwrap()
        .unwrap();
    let number = MultiEraBlock::decode(&body).unwrap().number();

    let (beside, beside_body) =
        make_conway_block_with_prev(cursor.slot(), cursor.hash(), number + 1);

    let archive = domain.archive().start_writer().unwrap();
    archive.apply(&beside, &beside_body).unwrap();
    archive.commit().unwrap();

    let plan = plan_for(&domain);
    assert_eq!(plan.cursor, cursor);

    let watcher = Arc::new(Watcher::default());
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let refused = export::export(
        &stele,
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &watcher.observer(),
    );

    match refused {
        Err(dolos_snapshot::Error::NotABoundary { reason, .. }) => {
            assert_eq!(reason, Ineligible::ArchivePastCursor { tip: beside })
        }
        other => panic!("{other:?}"),
    }

    assert_eq!(watcher.layers(), 0, "a refused export announced a layer");
}

/// A verifier checking a *different* stele than the one published is the
/// failure `--epochs` exists to prevent, so the two commands share one range
/// type and one restriction.
///
/// The restriction goes through `Plan::restrict_epochs` on both sides here for
/// the same reason: a reproduction over a narrower selection is a different
/// document, and it has to be the *same* different document.
#[test]
fn a_restricted_reproduction_matches_the_same_restricted_publish() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain).restrict_epochs(Some(2), None);

    assert!(
        plan.epochs.is_empty(),
        "the harness stands on the epoch-1 boundary; selecting above it selects nothing"
    );

    let temp = tempfile::tempdir().unwrap();

    let stored = export::publish(
        temp.path(),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &Observer::silent(),
    )
    .unwrap();

    let reproduced = export::export(
        &Discarding,
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    // The state tip alone, which is a legitimate publish and the narrowest one
    // there is.
    assert_eq!(stored.layers.len(), state_layer_count());

    assert_eq!(
        stored.canonicalize().unwrap(),
        reproduced.canonicalize().unwrap()
    );

    // And it is genuinely a different stele from the unrestricted one, so the
    // equality above is not the equality of two full exports.
    let whole = export::export(
        &Discarding,
        &plan_for(&domain),
        domain.archive(),
        domain.state(),
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    assert_ne!(whole.digest().unwrap(), reproduced.digest().unwrap());
}

/// How many blobs a stele's layers occupy: distinct `diffId`s, because two
/// layers with identical content are one content-addressed file.
fn distinct_layers(inscription: &stelae::Inscription) -> usize {
    inscription
        .layers
        .iter()
        .map(|layer| layer.diff_id)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// Every layer a publish writes is announced once and closed once, and the
/// records it reports are the records the document says it wrote.
///
/// Cross-checked against the inscription rather than against the stream: the
/// descriptor's `records` counts the header record the protocol writes and the
/// driver never sees, so the arithmetic below is a claim about two independent
/// counts agreeing and not a recording compared with itself.
///
/// A directory is the interesting transport for this half precisely because it
/// implements none of the byte reporting: `SteleDir` inherits the default no-op
/// attach, so what comes back here is exactly the profile driver's own stream,
/// with nothing from a transport mixed into it.
#[test]
fn a_directory_publish_reports_every_layer_and_record() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let temp = tempfile::tempdir().unwrap();
    let watcher = std::sync::Arc::new(Watcher::default());

    let inscription = export::publish(
        temp.path(),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &watcher.observer(),
    )
    .unwrap();

    watcher.assert_well_formed(inscription.layers.len());

    // A directory publish has no predecessor to inherit from and no registry to
    // find a blob already in, so every layer is built.
    assert_eq!(
        watcher.ended(Outcome::Transferred),
        inscription.layers.len(),
        "every layer of a directory publish is built"
    );
    assert_eq!(watcher.ended(Outcome::Inherited), 0);
    assert_eq!(watcher.ended(Outcome::Skipped), 0);

    // One header record per layer is the protocol's, written by `open_layer`
    // before the driver has a record of its own to write.
    let content: u64 = inscription
        .layers
        .iter()
        .map(|layer| layer.records - 1)
        .sum();

    assert!(
        content > 0,
        "the fixture has to carry records for this to prove anything"
    );

    assert_eq!(
        watcher.records(),
        content,
        "records reported, against what the document says was written"
    );

    // And nothing was invented on the transport's behalf: the default no-op
    // attach is what keeps the seam an offer rather than a tax, and this is the
    // assertion that it stayed one.
    assert_eq!(watcher.bytes(), 0);
    assert!(watcher.blobs(true).is_empty());
    assert!(watcher.blobs(false).is_empty());
}

/// A run nobody is watching is the run this crate has always made.
///
/// The seam's whole claim of being free when unused, checked where it can be:
/// the same plan exported twice, once watched and once silent, has to produce
/// the same document byte for byte.
#[test]
fn a_silent_publish_writes_exactly_what_a_watched_one_does() {
    let domain: ToyDomain = harness();
    let plan = plan_for(&domain);

    let watched = tempfile::tempdir().unwrap();
    let silent = tempfile::tempdir().unwrap();

    let watcher = std::sync::Arc::new(Watcher::default());

    let with = export::publish(
        watched.path(),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &watcher.observer(),
    )
    .unwrap();

    let without = export::publish(
        silent.path(),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &Observer::silent(),
    )
    .unwrap();

    assert!(watcher.layers() > 0, "the watched run reported nothing");

    assert_eq!(
        with.canonicalize().unwrap(),
        without.canonicalize().unwrap(),
    );
}

/// An index store that counts how many traversals were *constructed* over it.
///
/// The count is the property under test and inspection is not: one traversal
/// per family is a claim about how many times the store is walked, and a reader
/// satisfying themselves by looking at the loop is exactly what stops being
/// true the next time someone edits it. Every other method delegates, so an
/// export sees the store it would have seen.
#[derive(Clone)]
struct Counted<S> {
    inner: S,
    tags: Arc<AtomicUsize>,
    exacts: Arc<AtomicUsize>,
    entities: Arc<AtomicUsize>,
    /// The traversal that ends in a read error, after this many records.
    fault: Option<(Traversal, usize)>,
}

/// One of the store reads a [`Counted`] store can fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Traversal {
    Tags,
    Exacts,
    Entities,
    Logs,
}

/// An iterator that ends in a read error after `after` records, when set.
type Faulting<I> =
    std::iter::Chain<std::iter::Take<I>, std::option::IntoIter<<I as Iterator>::Item>>;

fn faulting<T, I: Iterator<Item = Result<T, ArchiveError>>>(
    iter: I,
    after: Option<usize>,
) -> Faulting<I> {
    iter.take(after.unwrap_or(usize::MAX))
        .chain(after.map(|_| Err(ArchiveError::InternalError("injected read fault".into()))))
}

impl<S: ArchiveStore> Counted<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            tags: Arc::default(),
            exacts: Arc::default(),
            entities: Arc::default(),
            fault: None,
        }
    }

    fn failing(inner: S, traversal: Traversal, after: usize) -> Self {
        Self {
            fault: Some((traversal, after)),
            ..Self::new(inner)
        }
    }

    fn fault_after(&self, traversal: Traversal) -> Option<usize> {
        self.fault
            .filter(|(faulted, _)| *faulted == traversal)
            .map(|(_, after)| after)
    }

    /// Entity index traversals built over the store.
    fn entity_traversals(&self) -> usize {
        self.entities.load(Ordering::Relaxed)
    }

    /// Traversals built over the store: tag runs and exact runs separately,
    /// because they are two separate scans of it.
    fn traversals(&self) -> (usize, usize) {
        (
            self.tags.load(Ordering::Relaxed),
            self.exacts.load(Ordering::Relaxed),
        )
    }
}

impl<S: ArchiveStore> ArchiveStore for Counted<S> {
    type BlockIter<'a> = S::BlockIter<'a>;
    type Writer = S::Writer;
    type LogIter = Faulting<S::LogIter>;
    type EntityValueIter = S::EntityValueIter;
    type SlotIter = S::SlotIter;
    type TagIter = Faulting<S::TagIter>;
    type ExactIter = Faulting<S::ExactIter>;
    type EntityRecordIter = Faulting<S::EntityRecordIter>;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        self.inner.start_writer()
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<dolos_core::EntityValue>>, ArchiveError> {
        self.inner.read_logs(ns, keys)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: dolos_core::KeyRange,
    ) -> Result<Self::LogIter, ArchiveError> {
        Ok(faulting(
            self.inner.iter_logs(ns, range)?,
            self.fault_after(Traversal::Logs),
        ))
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        self.inner.get_block_by_slot(slot)
    }

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        self.inner.get_blocks_by_slot(slot)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        self.inner.get_range(from, to)
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        self.inner.find_intersect(intersect)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        self.inner.get_tip()
    }

    fn prune_history(
        &self,
        max_slots: u64,
        max_prune_slots: Option<u64>,
        max_prune_index_rows: Option<u64>,
    ) -> Result<bool, ArchiveError> {
        self.inner
            .prune_history(max_slots, max_prune_slots, max_prune_index_rows)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        self.inner.truncate_front(after)
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_block_hash(hash)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_block_number(number)
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_tx_hash(hash)
    }

    fn entity_slots(
        &self,
        kind: dolos_core::EntityKind,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<BlockSlot>>, ArchiveError> {
        self.inner.entity_slots(kind, keys)
    }

    fn slots_by_tag(
        &self,
        dimension: dolos_core::TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.inner.slots_by_tag(dimension, key, start, end)
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[dolos_core::TagDimension],
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        self.tags.fetch_add(1, Ordering::Relaxed);

        Ok(faulting(
            self.inner.iter_archive_tags(dimensions, slots)?,
            self.fault_after(Traversal::Tags),
        ))
    }

    fn iter_exact_records(
        &self,
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::ExactIter, ArchiveError> {
        self.exacts.fetch_add(1, Ordering::Relaxed);

        Ok(faulting(
            self.inner.iter_exact_records(slots)?,
            self.fault_after(Traversal::Exacts),
        ))
    }

    fn iter_entity_records(
        &self,
        kinds: &[dolos_core::EntityKind],
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::EntityRecordIter, ArchiveError> {
        self.entities.fetch_add(1, Ordering::Relaxed);

        Ok(faulting(
            self.inner.iter_entity_records(kinds, slots)?,
            self.fault_after(Traversal::Entities),
        ))
    }
}

/// Index records spread across the skeleton's three epochs, so a staging that
/// misrouted one would produce a layer that is short and a layer that is long.
///
/// Two blocks per epoch, each carrying tags in every dimension and the three
/// exact kinds a block produces. Nothing here has to be a real ledger: what
/// staging does with a record is decided by its slot alone.
fn index_across_the_skeleton() -> MemoryArchiveStore {
    let store = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let writer = store.start_writer().unwrap();

    let mut archive = Vec::new();

    for epoch in 0..3u64 {
        for block in 0..2u64 {
            let slot = epoch * 100 + block * 40 + 10;

            archive.push(dolos_core::ArchiveIndexDelta {
                slot,
                block_hash: vec![slot as u8; 32],
                block_number: Some(slot),
                tx_hashes: vec![vec![0x80 | slot as u8; 32]],
                tags: archive_dimensions::ALL
                    .into_iter()
                    .map(|dimension| dolos_core::Tag::new(dimension, vec![slot as u8; 28]))
                    .collect(),
            });
        }
    }

    writer.apply_index(&archive).unwrap();
    writer.commit().unwrap();

    store
}

/// One export of N epochs stages them from one tag and one exact traversal,
/// counted rather than inspected, and a window the plan restricts away stays
/// out of the document.
#[test]
fn an_export_costs_one_index_traversal_however_many_epochs_it_covers() {
    for (first, windows) in [(None, 3usize), (Some(2), 1)] {
        let temp = tempfile::tempdir().unwrap();
        let stele = SteleDir::create(temp.path()).unwrap();

        let state = skeleton_state();
        let archive = Counted::new(with_skeleton_blocks(index_across_the_skeleton()));

        let plan = Plan::new(
            &skeleton_summary(),
            Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
            skeleton_point(),
            Default::default(),
        )
        .unwrap()
        .restrict_epochs(first, None);

        let watcher = Arc::new(Watcher::default());

        let inscription = export::export(
            &stele,
            &plan,
            &archive,
            &state,
            None,
            &export::First,
            &watcher.observer(),
        )
        .unwrap();

        watcher.assert_well_formed(inscription.layers.len());

        assert_eq!(archive.traversals(), (1, 1), "{windows} windows");
        assert_eq!(inscription.layers_of_kind(INDEXES).count(), windows);
    }
}

// --------------------------------------------------------------------------
// The entity index layers
// --------------------------------------------------------------------------

/// Entity index entries across the skeleton's windows, written as a seal
/// writes them: assets first seen in epochs 0 and 2, in an id order that is
/// not their slot order, a script in epoch 1, and one asset past the cursor
/// that no window covers.
fn entities_across_the_skeleton() -> MemoryArchiveStore {
    let store = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let writer = store.start_writer().unwrap();

    let (_, asset) = ENTITY_KINDS[0];
    let (_, script) = ENTITY_KINDS[1];

    for (kind, byte, slot) in [
        (asset, 0x03, 10),
        (asset, 0x01, 50),
        (asset, 0x02, 220),
        (script, 0x09, 150),
        (asset, 0x04, SKELETON_SLOT + 10),
    ] {
        writer
            .write_entity(kind, &vec![byte; kind.key_len()], slot)
            .unwrap();
    }

    writer.commit().unwrap();

    store
}

fn skeleton_plan() -> Plan {
    Plan::new(
        &skeleton_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        skeleton_point(),
        Default::default(),
    )
    .unwrap()
}

/// An entity layer exists for a window exactly when its kind has an entry
/// first seen in it, holds that window's entries in id order, and carries
/// nothing past the cursor.
#[test]
fn entity_layers_exist_only_where_an_entity_was_first_seen() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let archive = with_skeleton_blocks(entities_across_the_skeleton());
    let state = skeleton_state();
    let plan = skeleton_plan();

    let inscription = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    assert_eq!(
        inscription.layers.len(),
        3 * 2 + 3 + state_layer_count(),
        "three windows of blocks and indexes, three entity layers and the tip"
    );

    let index = stele.blob_index().unwrap();
    let (asset_layer, asset) = ENTITY_KINDS[0];
    let (script_layer, script) = ENTITY_KINDS[1];

    let read = |kind: &str, entity| -> Vec<(u64, Vec<EntityRecord>)> {
        inscription
            .layers_of_kind(kind)
            .map(|descriptor| {
                assert_eq!(
                    descriptor.media_type,
                    format!("application/vnd.dolos.stelae.{kind}.v1+zstd")
                );

                let epoch = descriptor.scope["epoch"].as_u64().unwrap();
                let records = read_both_ways(&stele, &index, descriptor)
                    .iter()
                    .map(|bytes| entities::decode(entity, bytes).unwrap())
                    .collect();

                (epoch, records)
            })
            .collect()
    };

    let entry = |kind: dolos_core::EntityKind, byte: u8, slot| {
        EntityRecord::new(kind, &vec![byte; kind.key_len()], slot).unwrap()
    };

    assert_eq!(
        read(asset_layer, asset),
        [
            (0, vec![entry(asset, 0x01, 50), entry(asset, 0x03, 10)]),
            (2, vec![entry(asset, 0x02, 220)]),
        ]
    );
    assert_eq!(
        read(script_layer, script),
        [(1, vec![entry(script, 0x09, 150)])]
    );

    // Kind-major after the logs, ascending epoch within a kind.
    let kinds: Vec<&str> = inscription
        .layers
        .iter()
        .map(|layer| layer.kind.as_str())
        .filter(|kind| kind.starts_with("entity-"))
        .collect();
    assert_eq!(kinds, [asset_layer, asset_layer, script_layer]);

    // The parameters declare every kind the publisher writes, layers or not.
    assert_eq!(
        inscription.parameters["entities"],
        serde_json::json!({"asset": 32, "script": 28, "stake": 29})
    );
}

/// Export the entity skeleton through `previous`, storing nothing, and report
/// the entity index traversals it took and the document.
fn export_entities(previous: &dyn export::Predecessor) -> (usize, Vec<u8>) {
    let archive = Counted::new(with_skeleton_blocks(entities_across_the_skeleton()));
    let state = skeleton_state();
    let plan = skeleton_plan();

    let watcher = Arc::new(Watcher::default());

    let inscription = export::export(
        &Discarding,
        &plan,
        &archive,
        &state,
        None,
        previous,
        &watcher.observer(),
    )
    .unwrap();

    watcher.assert_well_formed(inscription.layers.len());

    (
        archive.entity_traversals(),
        inscription.canonicalize().unwrap(),
    )
}

/// An entity layer is a scan of the index, so one traversal serves every window
/// and kind, and it is also what says which windows hold an entry.
#[test]
fn every_entity_kind_shares_one_traversal_with_no_presence_scan() {
    let (traversals, _) = export_entities(&export::First);

    assert_eq!(traversals, 1);
}

/// A closed window's entity layer is inherited per `(kind, scope)` like every
/// immutable epoch layer: a publish whose predecessor carries every entity
/// layer builds none. It still scans the index once, because no predecessor can
/// say a window without a layer holds nothing.
#[test]
fn an_entity_layer_a_predecessor_carries_is_inherited_and_not_rebuilt() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let archive = with_skeleton_blocks(entities_across_the_skeleton());
    let state = skeleton_state();
    let plan = skeleton_plan();

    let first = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    let lending = Lending::resuming(&first);
    let (traversals, document) = export_entities(&lending);

    assert_eq!(traversals, 1);
    assert_eq!(
        String::from_utf8(document).unwrap(),
        String::from_utf8(first.canonicalize().unwrap()).unwrap()
    );

    let lent: Vec<(String, u64)> = lending
        .asked
        .lock()
        .unwrap()
        .iter()
        .filter(|(kind, _)| kind.starts_with("entity-"))
        .cloned()
        .collect();

    // Asked for every window of every kind, the empty ones included: a
    // predecessor could carry a layer this store no longer has entries for.
    assert_eq!(lent.len(), 3 * ENTITY_KINDS.len());
}

/// Finding T1 of the first QA review of dolos#1511: a dry run counts the
/// entity layers a publish builds and the ones it inherits, so the forecast
/// and the publish that follows it agree.
#[test]
fn the_forecast_counts_the_entity_layers_a_publish_builds_and_inherits() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let archive = with_skeleton_blocks(entities_across_the_skeleton());
    let state = skeleton_state();
    let plan = skeleton_plan();

    let first = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        None,
        &export::First,
        &Observer::silent(),
    )
    .unwrap();

    assert_eq!(
        export::forecast(&plan, &archive, &export::First, None).unwrap(),
        export::Forecast {
            layers_built: first.layers.len(),
            layers_reused: 0,
        }
    );

    let lending = Lending::resuming(&first);

    let carried = export::export(
        &Discarding,
        &plan,
        &archive,
        &state,
        None,
        &lending,
        &Observer::silent(),
    )
    .unwrap();

    let lent = lending.lent.load(Ordering::Relaxed);

    assert_eq!(
        export::forecast(&plan, &archive, &lending, None).unwrap(),
        export::Forecast {
            layers_built: carried.layers.len() - lent,
            layers_reused: lent,
        }
    );

    // A layer the predecessor carries counts where this store holds no entry
    // for it, as the publish adopts it.
    let bare = with_skeleton_blocks(MemoryArchiveStore::new(
        dolos_cardano::model::build_log_schema(),
    ));
    let lender = Lending::resuming(&first);

    let adopted = export::export(
        &Discarding,
        &plan,
        &bare,
        &state,
        None,
        &lender,
        &Observer::silent(),
    )
    .unwrap();

    let lent = lender.lent.load(Ordering::Relaxed);

    assert_eq!(adopted.layers.len(), first.layers.len());
    assert_eq!(
        export::forecast(&plan, &bare, &lender, None).unwrap(),
        export::Forecast {
            layers_built: adopted.layers.len() - lent,
            layers_reused: lent,
        }
    );
}

/// The supported facade keeps planning, directory encoding, digest and
/// reproduction on one store view and one plan.
#[test]
fn the_headless_facade_plans_encodes_and_reproduces_one_document() {
    let domain: ToyDomain = harness();
    let source = StoreSnapshot::new(domain.archive(), domain.state());
    let plan = source
        .selected_plan(
            u64::from(domain.genesis().network_magic()),
            RetainedEpochs::default(),
            Selection::default(),
        )
        .unwrap();

    let destination = tempfile::tempdir().unwrap();
    let stele = destination.path().join("stele");
    let inscription = source
        .publish_directory(&stele, &plan, &Observer::silent())
        .unwrap();
    let reproduced = source.verify_reproduction(&inscription, &plan).unwrap();
    let document = source.digest_document(&plan, &export::First).unwrap();

    assert_eq!(reproduced.canonicalize().unwrap(), document.canonical);
    assert_eq!(inscription.digest().unwrap(), document.identity);
}

/// The index staging engine: one tag, one exact and one shared entity
/// traversal, partitioned into runs that finish as the layers a direct export
/// writes.
mod index_staging {
    use std::{collections::BTreeSet, fs, num::NonZeroUsize, path::Path};

    use dolos_cardano::EraBoundary;
    use dolos_core::{builtin::MemoryArchiveStore, ArchiveIndexDelta, Tag};
    use dolos_snapshot::{
        staging::{self, Budget, Family, Refusal, Request, Staged},
        Error, Scope as _, COMPRESSION_LEVEL, MAINNET_MAGIC,
    };
    use dolos_testing::blocks::{byron_ebb_slot, make_byron_ebb, BYRON_EPOCH_LENGTH};
    use stelae::{
        frame::CanonicalCbor, inscription::LayerDescriptor, DiscardingSink, RecordSink,
        SteleWriter as _, WrittenLayer,
    };

    use super::*;

    /// Closed hundred-slot epochs in the staging fixture before its frontier.
    const EPOCHS: u64 = 6;

    /// Seals an epoch at the second boundary after it, as Shelley epochs are.
    const MUTABLE: u64 = 50;

    fn families() -> Vec<Family> {
        std::iter::once(Family::Indexes)
            .chain(ENTITY_KINDS.map(|(_, kind)| Family::Entity(kind)))
            .collect()
    }

    /// Two blocks' index records per epoch of the skeleton geometry, entity
    /// entries in most windows with ids out of slot order, and a block opening
    /// epoch `epochs` as the frontier.
    fn staging_archive(epochs: u64) -> (MemoryArchiveStore, ChainPoint) {
        let store = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
        let writer = store.start_writer().unwrap();
        let mut deltas = Vec::new();

        for epoch in 0..epochs {
            for block in 0..2u64 {
                let slot = epoch * 100 + block * 40 + 10;
                let unique = |tag: u64| [(slot | tag << 32).to_be_bytes(); 4].concat();

                deltas.push(ArchiveIndexDelta {
                    slot,
                    block_hash: unique(1),
                    block_number: Some(slot),
                    tx_hashes: vec![unique(2)],
                    tags: archive_dimensions::ALL
                        .into_iter()
                        .map(|dimension| Tag::new(dimension, unique(3)[..28].to_vec()))
                        .collect(),
                });
            }

            for (i, (_, kind)) in ENTITY_KINDS.into_iter().enumerate() {
                if (epoch + i as u64).is_multiple_of(3) {
                    continue;
                }

                for (byte, offset) in [(0x30, 20), (0x10, 60)] {
                    let id = vec![byte + epoch as u8; kind.key_len()];
                    writer
                        .write_entity(kind, &id, epoch * 100 + offset)
                        .unwrap();
                }
            }
        }

        writer.apply_index(&deltas).unwrap();

        let (frontier, body) = make_conway_block_with_prev(epochs * 100 + 10, None, epochs);
        writer.apply(&frontier, &body).unwrap();
        writer.commit().unwrap();

        (store, frontier)
    }

    fn request(frontier: &ChainPoint, epochs: Vec<(Family, Vec<u64>)>) -> Request {
        Request {
            network_magic: MAINNET_MAGIC,
            frontier: frontier.clone(),
            store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
            epochs: epochs
                .into_iter()
                .map(|(family, epochs)| (family, epochs.into_iter().collect::<BTreeSet<_>>()))
                .collect(),
            live: false,
        }
    }

    /// Every family: the closed windows for `indexes`, the sealed ones for
    /// the entity kinds.
    fn every_family(frontier: &ChainPoint) -> Request {
        let epochs = families()
            .into_iter()
            .map(|family| match family {
                Family::Indexes => (family, (0..EPOCHS).collect()),
                Family::Entity(_) => (family, (0..EPOCHS - 1).collect()),
            })
            .collect();

        request(frontier, epochs)
    }

    fn stage<A: ArchiveStore>(
        root: &Path,
        archive: &A,
        request: &Request,
        budget: Budget,
    ) -> Result<Staged, Error> {
        staging::stage(root, archive, &skeleton_summary(), MUTABLE, request, budget)
    }

    fn unlimited() -> Budget {
        Budget::new(u64::MAX)
    }

    fn closed(summary: &ChainSummary, epoch: u64) -> EpochWindow {
        EpochWindow {
            epoch,
            start_slot: summary.epoch_start(epoch),
            end_slot: summary.epoch_start(epoch + 1) - 1,
        }
    }

    /// `family`'s layer for `window` from a traversal of that window alone:
    /// the layer staging has to reproduce.
    fn direct_layer<A: ArchiveStore>(
        archive: &A,
        family: Family,
        window: &EpochWindow,
    ) -> Option<LayerDescriptor> {
        let records: Vec<CanonicalCbor> = match family {
            Family::Indexes => archive
                .iter_archive_tags(&archive_dimensions::ALL, window.slots())
                .unwrap()
                .map(|tag| indexes::encode(&tag.unwrap().into()).unwrap())
                .chain(
                    archive
                        .iter_exact_records(window.slots())
                        .unwrap()
                        .map(|exact| indexes::encode(&exact.unwrap().into()).unwrap()),
                )
                .collect(),
            Family::Entity(kind) => archive
                .iter_entity_records(&[kind], window.slots())
                .unwrap()
                .map(|record| entities::encode(&record.unwrap()).unwrap())
                .collect(),
        };

        if records.is_empty() && family != Family::Indexes {
            return None;
        }

        let spec = window
            .scope(MAINNET_MAGIC)
            .layer_spec(family.kind().unwrap())
            .unwrap();

        Some(
            Discarding
                .write_layer(&DolosProfile, &spec, COMPRESSION_LEVEL, &records)
                .unwrap()
                .descriptor,
        )
    }

    fn refusal(result: Result<impl std::fmt::Debug, Error>) -> Refusal {
        match result.unwrap_err() {
            Error::Staging(refusal) => refusal,
            other => panic!("not a staging refusal: {other:?}"),
        }
    }

    #[test]
    fn one_traversal_per_family_stages_every_closed_window() {
        let (store, frontier) = staging_archive(EPOCHS);
        let archive = Counted::new(store);
        let root = tempfile::tempdir().unwrap();
        let request = every_family(&frontier);

        let staged = stage(root.path(), &archive, &request, unlimited()).unwrap();

        assert_eq!(archive.traversals(), (1, 1));
        assert_eq!(archive.entity_traversals(), 1);

        let report = staged.report();
        assert_eq!(
            (
                report.tag_traversals,
                report.exact_traversals,
                report.entity_traversals
            ),
            (1, 1, 1)
        );

        let mut empty_entity_windows = 0;

        for (family, epochs) in &request.epochs {
            for &epoch in epochs {
                let expected =
                    direct_layer(&archive.inner, *family, &closed(&skeleton_summary(), epoch));
                let staged = staged
                    .write_layer(&Discarding, *family, epoch, EPOCHS)
                    .unwrap();

                empty_entity_windows += usize::from(expected.is_none());
                assert_eq!(staged, expected, "{family:?} epoch {epoch}");
            }
        }

        assert!(
            empty_entity_windows > 0,
            "the fixture leaves entity windows empty"
        );
    }

    /// Staged closed windows are byte-identical to the exporter's own layers
    /// for the same publication.
    #[test]
    fn staged_layers_are_the_ones_the_exporter_writes() {
        let cases = [
            (index_across_the_skeleton(), vec![Family::Indexes]),
            (
                entities_across_the_skeleton(),
                families().into_iter().skip(1).collect(),
            ),
        ];

        for (store, families) in cases {
            let archive = with_skeleton_blocks(store);
            let inscription = export::export(
                &Discarding,
                &skeleton_plan(),
                &archive,
                &skeleton_state(),
                None,
                &export::First,
                &Observer::silent(),
            )
            .unwrap();

            let root = tempfile::tempdir().unwrap();
            let epochs = families
                .iter()
                .map(|family| (*family, vec![0, 1]))
                .collect();

            // Entries seal at the next boundary, as the fixture writes them.
            let staged = staging::stage(
                root.path(),
                &archive,
                &skeleton_summary(),
                0,
                &request(&skeleton_point(), epochs),
                unlimited(),
            )
            .unwrap();

            for family in families {
                for epoch in [0, 1] {
                    let exported = inscription
                        .layers
                        .iter()
                        .find(|layer| {
                            layer.kind == family.kind().unwrap() && layer.scope["epoch"] == epoch
                        })
                        .cloned();

                    let layer = staged.write_layer(&Discarding, family, epoch, 2).unwrap();

                    assert_eq!(layer, exported, "{family:?} epoch {epoch}");
                }
            }
        }
    }

    /// Inherited and unselected epochs get no run, and their records reach no
    /// neighbour.
    #[test]
    fn only_the_requested_epochs_are_staged() {
        let (store, frontier) = staging_archive(EPOCHS);
        let root = tempfile::tempdir().unwrap();
        let (_, asset) = ENTITY_KINDS[0];

        let request = request(
            &frontier,
            vec![
                (Family::Indexes, vec![1, 3]),
                (Family::Entity(asset), vec![1, 4]),
            ],
        );
        let staged = stage(root.path(), &store, &request, unlimited()).unwrap();

        for epoch in [0, 2, 4, 5] {
            assert_eq!(staged.records(Family::Indexes, epoch), None, "{epoch}");
            assert!(matches!(
                refusal(staged.write_layer(&Discarding, Family::Indexes, epoch, EPOCHS)),
                Refusal::NotStaged { epoch: e, .. } if e == epoch
            ));
        }

        for (family, epochs) in &request.epochs {
            for &epoch in epochs {
                assert_eq!(
                    staged
                        .write_layer(&Discarding, *family, epoch, EPOCHS)
                        .unwrap(),
                    direct_layer(&store, *family, &closed(&skeleton_summary(), epoch)),
                    "{family:?} epoch {epoch}"
                );
            }
        }
    }

    #[test]
    fn zero_and_one_epoch_stagings() {
        let (store, frontier) = staging_archive(EPOCHS);

        for epochs in [
            vec![],
            families().into_iter().map(|f| (f, vec![])).collect(),
        ] {
            let archive = Counted::new(store.clone());
            let root = tempfile::tempdir().unwrap();

            let staged = stage(
                root.path(),
                &archive,
                &request(&frontier, epochs),
                unlimited(),
            )
            .unwrap();

            assert_eq!(staged.report(), staging::Report::default());
            assert_eq!(archive.traversals(), (0, 0));
            assert_eq!(archive.entity_traversals(), 0);
        }

        let archive = Counted::new(store.clone());
        let root = tempfile::tempdir().unwrap();
        let one = request(&frontier, vec![(Family::Indexes, vec![EPOCHS - 1])]);

        let staged = stage(root.path(), &archive, &one, unlimited()).unwrap();

        assert_eq!(archive.traversals(), (1, 1));
        assert_eq!(
            staged
                .write_layer(&Discarding, Family::Indexes, EPOCHS - 1, EPOCHS)
                .unwrap(),
            direct_layer(
                &store,
                Family::Indexes,
                &closed(&skeleton_summary(), EPOCHS - 1)
            )
        );
    }

    /// An entity window is in the frontier's archive once sealed, but a
    /// publication before that seal must not carry it, whatever its
    /// first-seen slot says.
    #[test]
    fn an_entity_window_publishes_only_once_its_seal_has_run() {
        let (store, frontier) = staging_archive(EPOCHS);
        let summary = skeleton_summary();
        let (_, asset) = ENTITY_KINDS[0];
        let family = Family::Entity(asset);

        // One and a half epochs of mutable slots: an epoch seals three
        // boundaries on.
        let mutable = 150;
        assert_eq!(staging::sealed_by(&summary, mutable, 2, EPOCHS), Some(5));
        assert_eq!(staging::sealed_by(&summary, mutable, 4, EPOCHS), None);

        let root = tempfile::tempdir().unwrap();
        let staged = staging::stage(
            root.path(),
            &store,
            &summary,
            mutable,
            &request(&frontier, vec![(family, vec![2])]),
            unlimited(),
        )
        .unwrap();

        assert_eq!(staged.records(family, 2), Some(2));

        for sequence in 3..5 {
            assert!(!staged.publishes(family, 2, sequence).unwrap());
            assert_eq!(
                staged
                    .write_layer(&Discarding, family, 2, sequence)
                    .unwrap(),
                None
            );
        }

        for sequence in 5..=EPOCHS {
            assert!(staged.publishes(family, 2, sequence).unwrap());
            assert_eq!(
                staged
                    .write_layer(&Discarding, family, 2, sequence)
                    .unwrap(),
                direct_layer(&store, family, &closed(&summary, 2))
            );
        }

        let root = tempfile::tempdir().unwrap();
        let unsealed = staging::stage(
            root.path(),
            &store,
            &summary,
            mutable,
            &request(&frontier, vec![(family, vec![4])]),
            unlimited(),
        );

        assert!(matches!(
            refusal(unsealed),
            Refusal::Unsealed { epoch: 4, .. }
        ));
    }

    /// Twenty-slot epochs to epoch 3, then hundred-slot ones: the shape of
    /// mainnet's Byron to Shelley transition.
    fn two_era_summary() -> ChainSummary {
        let boundary = |epoch, slot| EraBoundary {
            epoch,
            slot,
            timestamp: slot,
        };

        let mut chain = ChainSummary::default();

        chain.append_era(
            1,
            EraState {
                start: boundary(0, 0),
                end: Some(boundary(3, 60)),
                epoch_length: 20,
                slot_length: 1,
                protocol: 1,
            },
        );
        chain.append_era(
            2,
            EraState {
                start: boundary(3, 60),
                end: None,
                epoch_length: 100,
                slot_length: 1,
                protocol: 2,
            },
        );

        chain
    }

    #[test]
    fn windows_and_seals_follow_the_chain_summary_across_eras() {
        let summary = two_era_summary();
        let store = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
        let writer = store.start_writer().unwrap();

        let deltas: Vec<ArchiveIndexDelta> = [5u64, 19, 25, 45, 59, 60, 159, 170]
            .into_iter()
            .map(|slot| ArchiveIndexDelta {
                slot,
                block_hash: vec![slot as u8; 32],
                block_number: Some(slot),
                tx_hashes: vec![],
                tags: vec![Tag::new(archive_dimensions::ALL[0], vec![slot as u8; 28])],
            })
            .collect();

        writer.apply_index(&deltas).unwrap();

        let (frontier, body) = make_conway_block_with_prev(270, None, 9);
        writer.apply(&frontier, &body).unwrap();
        writer.commit().unwrap();

        let root = tempfile::tempdir().unwrap();
        let staged = staging::stage(
            root.path(),
            &store,
            &summary,
            MUTABLE,
            &request(&frontier, vec![(Family::Indexes, (0..5).collect())]),
            unlimited(),
        )
        .unwrap();

        for epoch in 0..5 {
            assert_eq!(
                staged
                    .write_layer(&Discarding, Family::Indexes, epoch, 5)
                    .unwrap(),
                direct_layer(&store, Family::Indexes, &closed(&summary, epoch)),
                "epoch {epoch}"
            );
        }

        // Two blocks per window, each a tag and two exact records.
        assert_eq!(staged.records(Family::Indexes, 0), Some(6));
        assert_eq!(staged.records(Family::Indexes, 3), Some(6));

        // Short Byron epochs take more boundaries to seal than Shelley ones.
        let seals: Vec<_> = (0..4)
            .map(|epoch| staging::sealed_by(&summary, MUTABLE, epoch, 5))
            .collect();
        assert_eq!(seals, [Some(4), Some(4), Some(4), Some(5)]);
    }

    /// A Byron main block shares its boundary block's slot, so an archive
    /// holding it is past a frontier at that boundary block.
    #[test]
    fn a_main_block_sharing_the_frontier_slot_is_past_it() {
        let mut summary = ChainSummary::default();
        summary.append_era(
            0,
            EraState {
                start: EraBoundary {
                    epoch: 0,
                    slot: 0,
                    timestamp: 0,
                },
                end: None,
                epoch_length: BYRON_EPOCH_LENGTH,
                slot_length: 20,
                protocol: 0,
            },
        );

        let last = make_conway_block_with_prev(byron_ebb_slot(1) - 2, None, 1);
        let ebb = make_byron_ebb(1, last.0.hash().unwrap());
        let main = make_conway_block_with_prev(byron_ebb_slot(1), ebb.0.hash(), 2);

        let archive = |blocks: &[&(ChainPoint, dolos_core::RawBlock)]| {
            let store = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
            let writer = store.start_writer().unwrap();

            for (point, body) in blocks {
                writer.apply(point, body).unwrap();
            }

            writer.commit().unwrap();
            store
        };

        let staged_at = |store: &MemoryArchiveStore, frontier: &ChainPoint| {
            let root = tempfile::tempdir().unwrap();
            let request = request(frontier, vec![(Family::Indexes, vec![0])]);

            staging::stage(root.path(), store, &summary, MUTABLE, &request, unlimited())
        };

        let past = archive(&[&last, &ebb, &main]);
        assert!(matches!(
            refusal(staged_at(&past, &ebb.0)),
            Refusal::NotAtFrontier { .. }
        ));
        staged_at(&past, &main.0).unwrap();

        staged_at(&archive(&[&last, &ebb]), &ebb.0).unwrap();
    }

    #[test]
    fn open_windows_are_never_staged_or_published() {
        let (store, frontier) = staging_archive(EPOCHS);
        let root = tempfile::tempdir().unwrap();

        let open = stage(
            root.path(),
            &store,
            &request(&frontier, vec![(Family::Indexes, vec![EPOCHS])]),
            unlimited(),
        );
        assert!(matches!(
            refusal(open),
            Refusal::OpenWindow { epoch: EPOCHS, .. }
        ));

        let staged = stage(root.path(), &store, &every_family(&frontier), unlimited()).unwrap();

        // A publication's own window is its boundary tail, captured there.
        assert!(matches!(
            refusal(staged.write_layer(&Discarding, Family::Indexes, 3, 3)),
            Refusal::NotClosedAt {
                epoch: 3,
                sequence: 3
            }
        ));
        assert!(matches!(
            refusal(staged.write_layer(&Discarding, Family::Indexes, 3, EPOCHS + 1)),
            Refusal::PastFrontier { .. }
        ));
    }

    /// A traversal that fails leaves no completion marker, so nothing it
    /// wrote can be consumed, and a healthy retry stages afresh.
    #[test]
    fn a_failed_traversal_leaves_nothing_to_consume() {
        let (store, frontier) = staging_archive(EPOCHS);
        let root = tempfile::tempdir().unwrap();
        let entity_only = request(
            &frontier,
            families()
                .into_iter()
                .skip(1)
                .map(|family| (family, vec![1, 2]))
                .collect(),
        );

        for (request, traversal) in [
            (every_family(&frontier), Traversal::Tags),
            (every_family(&frontier), Traversal::Exacts),
            (entity_only, Traversal::Entities),
        ] {
            let failing = Counted::failing(store.clone(), traversal, 3);
            let failed = stage(root.path(), &failing, &request, unlimited());

            assert!(
                matches!(failed, Err(Error::Archive(_))),
                "{traversal:?}: {failed:?}"
            );

            for family in request.epochs.keys() {
                let marker = root.path().join(format!("{}.json", family.kind().unwrap()));
                assert!(!marker.exists(), "{family:?} was marked complete");
            }
        }

        let archive = Counted::new(store.clone());
        let staged = stage(root.path(), &archive, &every_family(&frontier), unlimited()).unwrap();

        assert_eq!(staged.report().reused, 0);
        assert_eq!(archive.traversals(), (1, 1));
        assert_eq!(
            staged
                .write_layer(&Discarding, Family::Indexes, 2, EPOCHS)
                .unwrap(),
            direct_layer(&store, Family::Indexes, &closed(&skeleton_summary(), 2))
        );
    }

    #[test]
    fn a_complete_staging_is_reused_and_another_source_is_refused() {
        let (store, frontier) = staging_archive(EPOCHS);
        let root = tempfile::tempdir().unwrap();
        let full = every_family(&frontier);

        let first = stage(root.path(), &store, &full, unlimited()).unwrap();

        let archive = Counted::new(store.clone());
        let again = stage(root.path(), &archive, &full, unlimited()).unwrap();

        assert_eq!(again.report().reused, families().len() as u64);
        assert_eq!(again.report().records, first.report().records);
        assert_eq!(again.report().bytes, 0);
        assert_eq!(archive.traversals(), (0, 0));
        assert_eq!(archive.entity_traversals(), 0);

        let contract = Request {
            store_contract: "v0-other".to_owned(),
            ..full.clone()
        };

        let coverage = request(
            &frontier,
            vec![(Family::Indexes, (0..EPOCHS - 1).collect())],
        );

        // The same coverage from the archive one block on: stageable, but not
        // the source the runs came from.
        let (other, _) = staging_archive(EPOCHS);
        let (next, body) =
            make_conway_block_with_prev(EPOCHS * 100 + 20, frontier.hash(), EPOCHS + 1);
        let writer = other.start_writer().unwrap();
        writer.apply(&next, &body).unwrap();
        writer.commit().unwrap();

        let moved = Request {
            frontier: next,
            ..full.clone()
        };
        stage(
            tempfile::tempdir().unwrap().path(),
            &other,
            &moved,
            unlimited(),
        )
        .unwrap();

        for (store, request) in [(&store, contract), (&store, coverage), (&other, moved)] {
            let refused = stage(root.path(), store, &request, unlimited());

            assert!(
                matches!(refusal(refused), Refusal::SourceMismatch { .. }),
                "{request:?}"
            );
        }
    }

    #[test]
    fn a_damaged_run_is_never_finished() {
        let (store, frontier) = staging_archive(EPOCHS);
        let root = tempfile::tempdir().unwrap();
        let request = request(&frontier, vec![(Family::Indexes, (0..EPOCHS).collect())]);
        let run = root.path().join("indexes").join("2.run");

        stage(root.path(), &store, &request, unlimited()).unwrap();
        let intact = fs::read(&run).unwrap();

        let mut flipped = intact.clone();
        *flipped.last_mut().unwrap() ^= 0x01;

        let mut oversized = intact.clone();
        oversized[..4].copy_from_slice(&u32::MAX.to_le_bytes());

        let truncated = intact[..intact.len() - 1].to_vec();

        // The same size, so a restage reuses the run and the replay catches it.
        for damaged in [flipped, oversized] {
            fs::write(&run, &damaged).unwrap();
            let staged = stage(root.path(), &store, &request, unlimited()).unwrap();

            assert_eq!(staged.report().reused, 1);
            assert!(matches!(
                refusal(staged.write_layer(&Discarding, Family::Indexes, 2, EPOCHS)),
                Refusal::Corrupt { .. }
            ));
        }

        // A staging made before the damage still refuses to finish the run.
        fs::write(&run, &intact).unwrap();
        let staged = stage(root.path(), &store, &request, unlimited()).unwrap();
        fs::write(&run, &truncated).unwrap();

        assert!(matches!(
            refusal(staged.write_layer(&Discarding, Family::Indexes, 2, EPOCHS)),
            Refusal::Corrupt { .. }
        ));

        // A run off its marked size, or a missing marker, is staged again.
        for incomplete in [false, true] {
            if incomplete {
                fs::remove_file(root.path().join("indexes.json")).unwrap();
            }

            let archive = Counted::new(store.clone());
            let staged = stage(root.path(), &archive, &request, unlimited()).unwrap();

            assert_eq!(staged.report().reused, 0);
            assert_eq!(archive.traversals(), (1, 1));
            assert_eq!(
                staged
                    .write_layer(&Discarding, Family::Indexes, 2, EPOCHS)
                    .unwrap(),
                direct_layer(&store, Family::Indexes, &closed(&skeleton_summary(), 2))
            );
        }

        // A marker naming fewer runs than its windows is refused, not trusted.
        let marker = root.path().join("indexes.json");
        let mut json: serde_json::Value =
            serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
        json["runs"].as_array_mut().unwrap().pop();
        fs::write(&marker, serde_json::to_vec(&json).unwrap()).unwrap();

        assert!(matches!(
            refusal(stage(root.path(), &store, &request, unlimited())),
            Refusal::Corrupt { .. }
        ));
    }

    #[test]
    fn the_scratch_budget_is_a_hard_limit() {
        let (store, frontier) = staging_archive(EPOCHS);
        let request = every_family(&frontier);

        let root = tempfile::tempdir().unwrap();
        let needed = stage(root.path(), &store, &request, unlimited())
            .unwrap()
            .report()
            .bytes;

        let root = tempfile::tempdir().unwrap();
        let refused = stage(root.path(), &store, &request, Budget::new(needed - 1));

        assert!(matches!(
            refusal(refused),
            Refusal::ScratchExceeded { limit } if limit == needed - 1
        ));

        // The index pass fit and stays complete; the entity pass that crossed
        // the budget is not marked.
        assert!(root.path().join("indexes.json").exists());

        for (kind, _) in ENTITY_KINDS {
            assert!(!root.path().join(format!("{kind}.json")).exists(), "{kind}");
        }

        let root = tempfile::tempdir().unwrap();
        let refused = stage(root.path(), &store, &request, Budget::new(1));

        assert!(matches!(
            refusal(refused),
            Refusal::ScratchExceeded { limit: 1 }
        ));
        assert!(!root.path().join("indexes.json").exists());

        let root = tempfile::tempdir().unwrap();
        stage(root.path(), &store, &request, Budget::new(needed)).unwrap();
    }

    /// The frames on disk in `family`'s runs under `root`.
    fn spooled(root: &Path, family: Family) -> usize {
        fs::read_dir(root.join(family.kind().unwrap()))
            .unwrap()
            .map(|entry| {
                let bytes = fs::read(entry.unwrap().path()).unwrap();
                let (mut at, mut frames) = (0, 0);

                while at < bytes.len() {
                    let header = bytes[at..at + 4].try_into().unwrap();
                    at += 4 + u32::from_le_bytes(header) as usize;
                    frames += 1;
                }

                assert_eq!(at, bytes.len());
                frames
            })
            .sum()
    }

    /// A run appends its frames to its file once they fill `run_buffer`, so a
    /// traversal's memory grows with its windows and the buffer, not with its
    /// records. With a one-byte buffer, a traversal that fails part-way has
    /// written out every frame it routed; with a buffer it never fills, none.
    #[test]
    fn runs_write_out_what_fills_their_buffer() {
        let (store, frontier) = staging_archive(EPOCHS);
        let entities: Vec<Family> = families().into_iter().skip(1).collect();
        let entity_only = request(
            &frontier,
            entities
                .iter()
                .map(|family| (*family, vec![1, 2]))
                .collect(),
        );

        for (request, traversal, faulted) in [
            (
                every_family(&frontier),
                Traversal::Tags,
                vec![Family::Indexes],
            ),
            (entity_only, Traversal::Entities, entities),
        ] {
            // Every record before the fault lands in a requested window.
            let routed = 3;
            let failing = Counted::failing(store.clone(), traversal, routed);
            let written = |run_buffer: usize| {
                let root = tempfile::tempdir().unwrap();
                let budget = Budget {
                    run_buffer,
                    ..unlimited()
                };

                assert!(matches!(
                    stage(root.path(), &failing, &request, budget),
                    Err(Error::Archive(_))
                ));

                faulted
                    .iter()
                    .map(|family| spooled(root.path(), *family))
                    .sum::<usize>()
            };

            assert_eq!(written(1), routed, "{traversal:?}");
            assert_eq!(written(usize::MAX), 0, "{traversal:?}");
        }
    }

    /// Every layer `request` stages, in its order.
    fn layers_of(request: &Request) -> Vec<(Family, u64)> {
        request
            .epochs
            .iter()
            .flat_map(|(family, epochs)| epochs.iter().map(|epoch| (*family, *epoch)))
            .collect()
    }

    /// `write_layers` returns what `write_layer` does for each layer, in
    /// input order, and fails when any of them fails.
    #[test]
    fn buffers_and_workers_move_no_bytes() {
        let (store, frontier) = staging_archive(EPOCHS);
        let request = every_family(&frontier);
        let layers = layers_of(&request);

        let mut with_open = layers.clone();
        with_open.insert(layers.len() / 2, (Family::Indexes, EPOCHS));

        let finish = |budget: Budget| {
            let root = tempfile::tempdir().unwrap();
            let staged = stage(root.path(), &store, &request, budget).unwrap();

            let one_by_one: Vec<_> = layers
                .iter()
                .map(|&(family, epoch)| {
                    staged
                        .write_layer(&Discarding, family, epoch, EPOCHS)
                        .unwrap()
                })
                .collect();

            assert_eq!(
                staged.write_layers(&Discarding, &layers, EPOCHS).unwrap(),
                one_by_one
            );
            assert!(matches!(
                refusal(staged.write_layers(&Discarding, &with_open, EPOCHS)),
                Refusal::NotClosedAt {
                    epoch: EPOCHS,
                    sequence: EPOCHS
                }
            ));

            one_by_one
        };

        let reference = finish(unlimited());

        assert_eq!(reference.len(), layers.len());
        assert!(reference.iter().any(Option::is_none));

        for (run_buffer, workers) in [(1, 1), (7, 3), (1 << 20, 16)] {
            let budget = Budget {
                run_buffer,
                workers: workers.try_into().unwrap(),
                ..unlimited()
            };

            assert_eq!(
                finish(budget),
                reference,
                "buffer {run_buffer}, {workers} workers"
            );
        }
    }

    /// Entity kinds asking for disjoint epochs still share one traversal,
    /// spanning all their windows, and each keeps only its own windows.
    #[test]
    fn entity_kinds_with_disjoint_windows_share_one_traversal() {
        let (store, frontier) = staging_archive(EPOCHS);
        let archive = Counted::new(store.clone());
        let root = tempfile::tempdir().unwrap();
        let [(_, asset), (_, script), (_, stake)] = ENTITY_KINDS;

        // Each window holds its kind's entries; script's epoch 1 does too,
        // inside its own span, and must be dropped.
        let request = request(
            &frontier,
            vec![
                (Family::Entity(asset), vec![4]),
                (Family::Entity(script), vec![0, 3]),
                (Family::Entity(stake), vec![2]),
            ],
        );

        let staged = stage(root.path(), &archive, &request, unlimited()).unwrap();

        assert_eq!(archive.entity_traversals(), 1);
        assert_eq!(staged.report().entity_traversals, 1);
        assert_eq!(staged.records(Family::Entity(script), 1), None);

        for (family, epochs) in &request.epochs {
            for &epoch in epochs {
                let expected = direct_layer(&store, *family, &closed(&skeleton_summary(), epoch));

                assert!(expected.is_some(), "{family:?} epoch {epoch} is empty");
                assert_eq!(
                    staged
                        .write_layer(&Discarding, *family, epoch, EPOCHS)
                        .unwrap(),
                    expected,
                    "{family:?} epoch {epoch}"
                );
            }
        }
    }

    /// An archive that holds the frontier block but has moved past it is not
    /// the frozen view the frontier names.
    #[test]
    fn an_archive_past_its_frontier_is_refused() {
        let (store, frontier) = staging_archive(EPOCHS);
        let (next, body) =
            make_conway_block_with_prev(EPOCHS * 100 + 20, frontier.hash(), EPOCHS + 1);
        let writer = store.start_writer().unwrap();
        writer.apply(&next, &body).unwrap();
        writer.commit().unwrap();

        let indexes = |frontier: &ChainPoint| request(frontier, vec![(Family::Indexes, vec![0])]);
        let root = tempfile::tempdir().unwrap();

        assert!(matches!(
            refusal(stage(root.path(), &store, &indexes(&frontier), unlimited())),
            Refusal::NotAtFrontier { .. }
        ));
        stage(root.path(), &store, &indexes(&next), unlimited()).unwrap();
    }

    /// A discarding transport that counts the sinks open at once. Opening one
    /// waits a little, so finishing more layers at once than allowed shows.
    #[derive(Default)]
    struct Sinks {
        live: Arc<AtomicUsize>,
        peak: AtomicUsize,
    }

    struct Live {
        inner: DiscardingSink,
        _open: Open,
    }

    struct Open(Arc<AtomicUsize>);

    impl Drop for Open {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl RecordSink for Live {
        fn write_record(&mut self, record: &CanonicalCbor) -> Result<(), stelae::Error> {
            self.inner.write_record(record)
        }

        fn records(&self) -> u64 {
            self.inner.records()
        }

        fn finish(self) -> Result<WrittenLayer, stelae::Error> {
            self.inner.finish()
        }
    }

    impl stelae::SteleWriter for Sinks {
        type Sink = Live;

        fn layer_sink(
            &self,
            profile: &dyn stelae::Profile,
            spec: &stelae::LayerSpec,
            level: i32,
        ) -> Result<Live, stelae::Error> {
            let inner = Discarding.layer_sink(profile, spec, level)?;
            let open = self.live.fetch_add(1, Ordering::SeqCst) + 1;
            let live = Live {
                inner,
                _open: Open(self.live.clone()),
            };

            self.peak.fetch_max(open, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(10));

            Ok(live)
        }

        fn seal(
            &self,
            profile: &dyn stelae::Profile,
            inscription: &stelae::Inscription,
        ) -> Result<stelae::Digest, stelae::Error> {
            Discarding.seal(profile, inscription)
        }
    }

    /// Finishing never holds more sinks, each a compression context, than the
    /// budget's workers.
    #[test]
    fn finishing_holds_at_most_workers_sinks_open() {
        let (store, frontier) = staging_archive(EPOCHS);
        let request = every_family(&frontier);
        let layers = layers_of(&request);
        let root = tempfile::tempdir().unwrap();

        for workers in [1, 3] {
            let budget = Budget {
                workers: NonZeroUsize::new(workers).unwrap(),
                ..unlimited()
            };
            let staged = stage(root.path(), &store, &request, budget).unwrap();
            let sinks = Sinks::default();

            staged.write_layers(&sinks, &layers, EPOCHS).unwrap();

            let peak = sinks.peak.load(Ordering::SeqCst);
            assert!(
                (1..=workers).contains(&peak),
                "{peak} sinks open with {workers} workers"
            );
            assert_eq!(sinks.live.load(Ordering::SeqCst), 0);
        }
    }
}

/// The durable capture of a boundary and its later finalization.
mod boundary_preparation {
    use std::{collections::BTreeSet, fs, path::Path};

    use dolos_cardano::{
        model::{AccountEpochLog, FirstScriptChunk, PoolEpochLog, PoolState},
        periods::newly_sealable,
    };
    use dolos_core::{ArchiveIndexDelta, Tag};
    use dolos_snapshot::{
        prepare::{self, Bundle, Finalizing, IndexTail, Refusal, Request, Store},
        staging::{self, Budget, Family, Staged},
        Error, MAINNET_MAGIC,
    };
    use dolos_testing::blocks::{byron_ebb_slot, make_byron_block, make_byron_ebb};
    use pallas::crypto::hash::Hash;
    use stelae::inscription::Inscription;

    use super::*;

    const CONTRACT: &str = dolos_core::compat::SUPPORTED_CONTRACTS[0];

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Shape {
        /// Hundred-slot epochs that seal at the second boundary after them.
        Shelley,
        /// Epochs opened by a boundary block whose slot the first main block
        /// shares, sealing at the fourth boundary after them.
        Byron,
    }

    /// A synthetic chain written the way a ledger writes its stores: a boundary
    /// closes and settles earlier windows, seals what it can, then commits the
    /// epoch's first block. Deterministic, so a second chain stopped at P is
    /// the store frozen at P.
    struct Chain {
        shape: Shape,
        era: EraState,
        summary: ChainSummary,
        mutable: u64,
        archive: Counted<MemoryArchiveStore>,
        state: MemoryStateStore,
        tip: Option<ChainPoint>,
        number: u64,
        epoch: u64,
        boundary_delta: Option<ArchiveIndexDelta>,
    }

    impl Chain {
        fn new(shape: Shape) -> Self {
            let epoch_length = match shape {
                Shape::Shelley => 100,
                Shape::Byron => dolos_testing::blocks::BYRON_EPOCH_LENGTH,
            };

            let era = EraState {
                epoch_length,
                ..skeleton_era()
            };

            let mut summary = ChainSummary::default();
            summary.append_era(6, era.clone());

            let mutable = match shape {
                Shape::Shelley => 50,
                Shape::Byron => 3 * epoch_length,
            };

            let mut chain = Self {
                shape,
                era,
                summary,
                mutable,
                archive: Counted::new(MemoryArchiveStore::new(
                    dolos_cardano::model::build_log_schema(),
                )),
                state: MemoryStateStore::new(),
                tip: None,
                number: 0,
                epoch: 0,
                boundary_delta: None,
            };

            chain.write_state(0);
            chain.open_epoch(0);

            chain
        }

        fn at(shape: Shape, epoch: u64) -> Self {
            let mut chain = Self::new(shape);
            chain.advance_to(epoch);
            chain
        }

        fn start(&self, epoch: u64) -> u64 {
            self.summary.epoch_start(epoch)
        }

        fn advance_to(&mut self, epoch: u64) {
            while self.epoch < epoch {
                self.finish_epoch();
                self.epoch += 1;
                self.open_epoch(self.epoch);
            }
        }

        /// The state at this point: rows that change every time it moves, so a
        /// later tip cannot pass for an earlier one.
        fn write_state(&self, marker: u8) {
            let writer = self.state.start_writer().unwrap();

            writer
                .write_entity_typed::<EraState>(&EraKey::from(6), &self.era)
                .unwrap();

            for (ns, byte) in [(PoolState::NS, 1), (AccountState::NS, 0x40)] {
                let key = common::state_key(ns, byte);

                writer
                    .write_entity(
                        ns,
                        &EntityKey::from_raw(&key),
                        &vec![0x82, marker, self.epoch as u8],
                    )
                    .unwrap();
            }

            if let Some(tip) = &self.tip {
                writer.set_cursor(tip.clone()).unwrap();
            }

            writer.commit().unwrap();
        }

        fn log(
            &self,
            writer: &impl dolos_core::ArchiveWriter,
            ns: Namespace,
            epoch: u64,
            byte: u8,
            value: u8,
        ) {
            writer
                .write_log(
                    ns,
                    &common::log_key(ns, self.start(epoch), byte),
                    &vec![0x82, value, epoch as u8],
                )
                .unwrap();
        }

        /// One block's body and its index delta, its tags out of order and
        /// repeated as a block's are.
        fn commit_block(
            &mut self,
            point: ChainPoint,
            body: dolos_core::RawBlock,
            tagged: bool,
            number: Option<u64>,
        ) -> ArchiveIndexDelta {
            let slot = point.slot();
            let hash = point.hash().unwrap();

            let tags = archive_dimensions::ALL
                .into_iter()
                .rev()
                .flat_map(|dimension| {
                    let key = |byte: u8| [[byte; 20].as_slice(), &slot.to_be_bytes()].concat();

                    [
                        Tag::new(dimension, key(2)),
                        Tag::new(dimension, key(1)),
                        Tag::new(dimension, key(2)),
                    ]
                })
                .filter(|_| tagged)
                .collect();

            let delta = ArchiveIndexDelta {
                slot,
                block_hash: hash.to_vec(),
                block_number: number,
                tx_hashes: match tagged {
                    true => [0xee, 0xdd]
                        .map(|byte| [[byte; 24].as_slice(), &slot.to_be_bytes()].concat())
                        .to_vec(),
                    false => vec![],
                },
                tags,
            };

            let writer = self.archive.start_writer().unwrap();
            writer.apply(&point, &body).unwrap();
            writer.apply_index(std::slice::from_ref(&delta)).unwrap();
            writer.commit().unwrap();

            self.tip = Some(point);
            self.number += 1;

            delta
        }

        fn previous_hash(&self) -> Hash<32> {
            self.tip
                .as_ref()
                .and_then(ChainPoint::hash)
                .unwrap_or_else(|| Hash::new([0; 32]))
        }

        /// The boundary work into `epoch` and the epoch's first block.
        fn open_epoch(&mut self, epoch: u64) {
            let writer = self.archive.start_writer().unwrap();

            if epoch > 0 {
                // Ewrap closes the epoch before and settles the account rows
                // estart wrote for the one before that.
                self.log(&writer, EpochState::NS, epoch - 1, 1, 10);

                if epoch >= 2 {
                    for byte in [1, 2] {
                        self.log(&writer, AccountEpochLog::NS, epoch - 2, byte, 22);
                    }
                }
            }

            // Estart's stake leg for the epoch it opens.
            for byte in [1, 2] {
                self.log(&writer, AccountEpochLog::NS, epoch, byte, 21);
            }

            for sealed in newly_sealable(&self.summary, self.mutable, epoch) {
                self.log(&writer, FirstScriptChunk::NS, sealed, 1, 11);

                if sealed.is_multiple_of(2) {
                    for (_, kind) in ENTITY_KINDS {
                        for (byte, offset) in [(0x30, 7), (0x10, 9)] {
                            let id = vec![byte + sealed as u8; kind.key_len()];

                            writer
                                .write_entity(kind, &id, self.start(sealed) + offset)
                                .unwrap();
                        }
                    }
                }
            }

            writer.commit().unwrap();

            let number = self.number;
            let previous = self.previous_hash();

            let (delta, marker) = match self.shape {
                Shape::Shelley => {
                    let (point, body) = make_conway_block_with_prev(
                        self.start(epoch) + 10,
                        self.tip.as_ref().and_then(ChainPoint::hash),
                        number,
                    );

                    (self.commit_block(point, body, true, Some(number)), 1)
                }
                Shape::Byron => {
                    let (point, body) = make_byron_ebb(epoch, previous);

                    assert_eq!(point.slot(), byron_ebb_slot(epoch));

                    (self.commit_block(point, body, false, None), 2)
                }
            };

            self.boundary_delta = Some(delta);
            self.write_state(marker);
        }

        /// What the rest of the epoch writes after its first block, all of it
        /// inside windows a boundary here has already published.
        fn finish_epoch(&mut self) {
            let epoch = self.epoch;
            let start = self.start(epoch);

            // The reward update writes the epoch before, after its boundary.
            let writer = self.archive.start_writer().unwrap();
            self.log(&writer, PoolEpochLog::NS, epoch, 1, 12);

            if epoch > 0 {
                self.log(&writer, PoolEpochLog::NS, epoch - 1, 2, 13);
            }

            writer.commit().unwrap();

            let number = self.number;
            let previous = self.previous_hash();

            match self.shape {
                Shape::Shelley => {
                    let (point, body) =
                        make_conway_block_with_prev(start + 50, Some(previous), number);
                    self.commit_block(point, body, true, Some(number));
                }
                Shape::Byron => {
                    let (point, body) = make_byron_block(epoch, 0, number, previous);
                    self.commit_block(point, body, true, Some(number));

                    let number = self.number;
                    let previous = self.previous_hash();
                    let (point, body) = make_byron_block(epoch, 100, number, previous);
                    self.commit_block(point, body, true, Some(number));
                }
            }

            self.write_state(3);
        }

        fn plan(&self, retained: &[u64]) -> Plan {
            Plan::new(
                &self.summary,
                Network::for_magic(MAINNET_MAGIC),
                self.tip.clone().unwrap(),
                RetainedEpochs::new(retained.to_vec()).unwrap(),
            )
            .unwrap()
        }

        fn prepare(&self, store: &Store, retained: &[u64]) -> Result<Bundle, Error> {
            prepare::prepare(
                store,
                &self.plan(retained),
                &self.archive,
                &self.state,
                &Request {
                    store_contract: CONTRACT.to_owned(),
                    mutable_slots: self.mutable,
                    tail: IndexTail::Delta(self.boundary_delta.as_ref().unwrap()),
                },
                &Observer::silent(),
            )
        }

        fn stage(
            &self,
            root: &Path,
            bundles: &[Bundle],
            previous: &dyn export::Predecessor,
        ) -> Staged {
            prepare::stage_batch(
                root,
                &self.archive,
                &self.state,
                bundles,
                previous,
                Budget::new(u64::MAX),
                &Observer::silent(),
            )
            .unwrap()
        }

        /// The ordinary export of the stores as they stand.
        fn export(
            &self,
            root: &Path,
            retained: &[u64],
            previous: &dyn export::Predecessor,
        ) -> Inscription {
            export::export(
                &SteleDir::create(root).unwrap(),
                &self.plan(retained),
                &self.archive,
                &self.state,
                None,
                previous,
                &Observer::silent(),
            )
            .unwrap()
        }

        fn finalize(
            &self,
            root: &Path,
            store: &Store,
            bundle: &Bundle,
            staged: &Staged,
            previous: &dyn export::Predecessor,
        ) -> Result<Inscription, Error> {
            prepare::finalize(
                &SteleDir::create(root).unwrap(),
                &Finalizing {
                    store,
                    bundle,
                    staged,
                    producers: export::Producers::DEFAULT,
                },
                &self.archive,
                None,
                previous,
                &Observer::silent(),
            )
        }
    }

    fn store(root: &Path) -> Store {
        Store::open(root, "journal").unwrap()
    }

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn blobs(root: &Path) -> BTreeSet<String> {
        fs::read_dir(root.join("blobs/sha256"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    /// The canonical document, layer by layer first so a failure names one,
    /// and every compressed blob.
    fn assert_same_stele(
        frozen: (&Path, &Inscription),
        finalized: (&Path, &Inscription),
        at: &str,
    ) {
        let key = |layer: &stelae::inscription::LayerDescriptor| {
            format!("{} {} {}", layer.kind, layer.scope, layer.diff_id)
        };

        assert_eq!(
            frozen.1.layers.iter().map(key).collect::<Vec<_>>(),
            finalized.1.layers.iter().map(key).collect::<Vec<_>>(),
            "{at}"
        );
        assert_eq!(
            frozen.1.canonicalize().unwrap(),
            finalized.1.canonicalize().unwrap(),
            "{at}"
        );
        assert_eq!(blobs(frozen.0), blobs(finalized.0), "{at}");
    }

    fn preparation_refusal<T: std::fmt::Debug>(result: Result<T, Error>) -> Refusal {
        match result.unwrap_err() {
            Error::Preparation(refusal) => refusal,
            other => panic!("{other:?}"),
        }
    }

    fn layer_of<'a>(
        inscription: &'a Inscription,
        kind: &'a str,
        epoch: u64,
    ) -> Option<&'a stelae::inscription::LayerDescriptor> {
        inscription
            .layers_of_kind(kind)
            .find(|layer| layer.scope["epoch"] == epoch)
    }

    /// Done-when 1: prepare, advance and stage once, then every publication is
    /// byte-equal to the ordinary export of the store frozen at its boundary.
    #[test]
    fn a_prepared_boundary_finishes_as_the_store_frozen_there() {
        for shape in [Shape::Shelley, Shape::Byron] {
            let retained = [3];
            let (root, stage_root) = (tempdir(), tempdir());
            let store = store(root.path());
            let mut chain = Chain::new(shape);
            let mut bundles = Vec::new();

            for boundary in [2, 3, 5] {
                chain.advance_to(boundary);
                bundles.push(chain.prepare(&store, &retained).unwrap());
            }

            assert_eq!(
                (
                    chain.archive.traversals(),
                    chain.archive.entity_traversals()
                ),
                ((0, 0), 0),
                "{shape:?}: preparing traversed the index",
            );

            chain.advance_to(7);

            let staged = chain.stage(stage_root.path(), &bundles, &export::First);

            assert_eq!(
                (
                    chain.archive.traversals(),
                    chain.archive.entity_traversals()
                ),
                ((1, 1), 1),
                "{shape:?}: one traversal per family for the whole batch",
            );

            for bundle in &bundles {
                let frozen_root = tempdir();
                let frozen_chain = Chain::at(shape, bundle.sequence());
                let frozen = frozen_chain.export(frozen_root.path(), &retained, &export::First);

                let finished_root = tempdir();
                let finished = chain
                    .finalize(
                        finished_root.path(),
                        &store,
                        bundle,
                        &staged,
                        &export::First,
                    )
                    .unwrap();

                assert_same_stele(
                    (frozen_root.path(), &frozen),
                    (finished_root.path(), &finished),
                    &format!("{shape:?} at {}", bundle.sequence()),
                );
            }

            assert_eq!(
                (
                    chain.archive.traversals(),
                    chain.archive.entity_traversals()
                ),
                ((1, 1), 1),
                "{shape:?}: finalizing traversed the index",
            );
        }
    }

    /// The same equality for a contiguous chain of publications, each following
    /// the real document before it: history, inheritance and settlement land
    /// only at finalization, and a window the first predecessor carries is
    /// never staged.
    #[test]
    fn a_chained_batch_matches_the_chained_exports() {
        for shape in [Shape::Shelley, Shape::Byron] {
            let retained = [3];
            let (root, stage_root, seed_root) = (tempdir(), tempdir(), tempdir());
            let store = store(root.path());

            let seed = Chain::at(shape, 1).export(seed_root.path(), &retained, &export::First);

            let mut chain = Chain::at(shape, 2);
            let mut bundles = Vec::new();

            for boundary in [2, 3, 4] {
                chain.advance_to(boundary);
                bundles.push(chain.prepare(&store, &retained).unwrap());
            }

            let lending = Lending::following(&seed, &bundles[0].plan());
            let staged = chain.stage(stage_root.path(), &bundles, &lending);

            assert_eq!(
                staged.records(Family::Indexes, 0),
                None,
                "{shape:?}: the first predecessor carries window 0",
            );
            assert!(staged.records(Family::Indexes, 1).is_some(), "{shape:?}");

            let mut frozen_previous = seed.clone();
            let mut finished_previous = seed.clone();

            for bundle in &bundles {
                let at = format!("{shape:?} at {}", bundle.sequence());
                let plan = bundle.plan();

                let frozen_root = tempdir();
                let frozen = Chain::at(shape, bundle.sequence()).export(
                    frozen_root.path(),
                    &retained,
                    &Lending::following(&frozen_previous, &plan),
                );

                let finished_root = tempdir();
                let finished = chain
                    .finalize(
                        finished_root.path(),
                        &store,
                        bundle,
                        &staged,
                        &Lending::following(&finished_previous, &plan),
                    )
                    .unwrap();

                assert_same_stele(
                    (frozen_root.path(), &frozen),
                    (finished_root.path(), &finished),
                    &at,
                );
                assert_eq!(finished.history.len(), frozen_previous.history.len() + 1);

                frozen_previous = frozen;
                finished_previous = finished;
            }
        }
    }

    /// The fixture has to show what a later store would get wrong, or the
    /// equalities above prove nothing: a Byron main block sharing the
    /// boundary's slot, settled account rows, and logs and entities a later
    /// seal writes.
    #[test]
    fn a_later_store_would_publish_something_else() {
        let (at_two, at_six) = (tempdir(), tempdir());
        let frozen = Chain::at(Shape::Byron, 2);
        let later = Chain::at(Shape::Byron, 6);

        assert_eq!(
            frozen
                .archive
                .get_blocks_by_slot(&byron_ebb_slot(2))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            later
                .archive
                .get_blocks_by_slot(&byron_ebb_slot(2))
                .unwrap()
                .len(),
            2
        );

        let before = frozen.export(at_two.path(), &[], &export::First);
        let after = later.export(at_six.path(), &[], &export::First);

        assert!(
            layer_of(&before, "log-first-script-chunks", 1).is_none()
                && layer_of(&after, "log-first-script-chunks", 1).is_some(),
            "the seal lands after the boundary"
        );
        assert!(
            layer_of(&before, "entity-asset", 0).is_none()
                && layer_of(&after, "entity-asset", 0).is_some(),
            "entries are visible from their seal, not their first-seen slot"
        );
        assert_ne!(
            layer_of(&before, "log-account-epochs", 1).unwrap().diff_id,
            layer_of(&after, "log-account-epochs", 1).unwrap().diff_id,
            "the rows settle one boundary after their window closes"
        );

        // The header and the boundary block's own hash: the main block at its
        // slot has not happened yet.
        let tail = before.layers_of_kind(INDEXES).last().unwrap();

        assert_eq!(tail.records, 2);
    }

    /// A retained dump at its boundary is the tip's blob under two descriptors,
    /// whether cut from the stores or from a bundle.
    #[test]
    fn a_due_dump_is_the_captured_tip() {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 3);
        let bundle = chain.prepare(&store, &[3]).unwrap();

        chain.advance_to(4);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let finished = chain
            .finalize(
                finished_root.path(),
                &store,
                &bundle,
                &staged,
                &export::First,
            )
            .unwrap();

        let dumps: Vec<_> = dumps_in(&finished, 3);

        assert_eq!(dumps.len(), state_layer_count());
        assert_eq!(
            bundle
                .layers()
                .filter(|layer| state_ns_for(&layer.kind).is_some())
                .count(),
            state_layer_count(),
            "the bundle holds one payload per shard, not two"
        );
    }

    /// Done-when 2: nothing at the boundary is read from the final store. Its
    /// tail comes from the delta, or from an explicit traversal, which is exact
    /// while the stores stand there and is the same layer.
    #[test]
    fn a_tail_comes_from_the_delta_unless_a_traversal_is_asked_for() {
        for shape in [Shape::Shelley, Shape::Byron] {
            let chain = Chain::at(shape, 2);
            let (delta_root, traversal_root) = (tempdir(), tempdir());

            let from_delta = chain.prepare(&store(delta_root.path()), &[]).unwrap();

            assert_eq!(chain.archive.traversals(), (0, 0), "{shape:?}");

            let from_traversal = prepare::prepare(
                &store(traversal_root.path()),
                &chain.plan(&[]),
                &chain.archive,
                &chain.state,
                &Request {
                    store_contract: CONTRACT.to_owned(),
                    mutable_slots: chain.mutable,
                    tail: IndexTail::Traverse,
                },
                &Observer::silent(),
            )
            .unwrap();

            assert_eq!(chain.archive.traversals(), (1, 1), "{shape:?}");

            assert_eq!(
                from_delta.layers().collect::<Vec<_>>(),
                from_traversal.layers().collect::<Vec<_>>(),
                "{shape:?}"
            );
        }
    }

    /// A delta from any other block would publish its rows as the boundary's.
    #[test]
    fn a_delta_for_another_block_is_refused() {
        let root = tempdir();
        let store = store(root.path());
        let earlier = Chain::at(Shape::Shelley, 2).boundary_delta.unwrap();
        let chain = Chain::at(Shape::Shelley, 3);

        let refused = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Delta(&earlier),
            },
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::ForeignTail { .. }
        ));
        assert!(store.sequences().unwrap().is_empty());
    }

    /// Stores that have moved past the boundary are not a boundary.
    #[test]
    fn a_store_past_its_boundary_is_not_prepared() {
        let root = tempdir();
        let mut chain = Chain::at(Shape::Shelley, 2);

        chain.finish_epoch();

        let refused = chain.prepare(&store(root.path()), &[]).unwrap_err();

        assert!(matches!(refused, Error::NotABoundary { .. }), "{refused:?}");
    }

    /// Done-when 3: a capture that dies leaves no completion marker, and a
    /// retry converges on the same bundle.
    #[test]
    fn an_interrupted_capture_marks_nothing_and_a_retry_completes() {
        let (root, clean_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let delta = chain.boundary_delta.clone().unwrap();

        let refused = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &Counted::failing(chain.archive.inner.clone(), Traversal::Logs, 1),
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Delta(&delta),
            },
            &Observer::silent(),
        );

        assert!(matches!(refused, Err(Error::Archive(_))), "{refused:?}");
        assert!(store.sequences().unwrap().is_empty());
        assert!(matches!(
            preparation_refusal(store.load(2)),
            Refusal::NotPrepared { sequence: 2 }
        ));

        // A marker that never landed is not a marker either.
        fs::write(root.path().join("bundles/2.json.partial"), b"{").unwrap();
        assert!(store.sequences().unwrap().is_empty());

        let retried = chain.prepare(&store, &[]).unwrap();
        let clean = chain.prepare(&self::store(clean_root.path()), &[]).unwrap();

        assert_eq!(
            retried.layers().collect::<Vec<_>>(),
            clean.layers().collect::<Vec<_>>()
        );
        assert_eq!(store.load(2).unwrap().bytes(), retried.bytes());
    }

    /// A payload gone or cut short stops the bundle loading; one damaged in
    /// place stops its publication before a descriptor exists.
    #[test]
    fn a_damaged_bundle_is_never_finalized() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let blob = root.path().join("blobs/sha256");

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        let digest = bundle.references().into_iter().next().unwrap();
        let name = digest.to_string();
        let path = blob.join(name.strip_prefix("sha256:").unwrap());
        let original = fs::read(&path).unwrap();

        fs::remove_file(&path).unwrap();
        assert!(matches!(
            preparation_refusal(store.load(2)),
            Refusal::MissingArtifact { sequence: 2, .. }
        ));

        fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(matches!(
            preparation_refusal(store.load(2)),
            Refusal::MissingArtifact { sequence: 2, .. }
        ));

        let mut flipped = original.clone();
        let middle = flipped.len() / 2;
        flipped[middle] ^= 0xff;
        fs::write(&path, &flipped).unwrap();

        // The size is right, so it loads; its digests are what refuse it.
        let loaded = store.load(2).unwrap();
        let finished_root = tempdir();
        let refused = chain.finalize(
            finished_root.path(),
            &store,
            &loaded,
            &staged,
            &export::First,
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::Corrupt { sequence: 2, .. }
        ),);
        assert!(!finished_root.path().join("inscription.json").exists());

        fs::write(&path, &original).unwrap();
        store.load(2).unwrap();
    }

    /// A manifest edited, cut short or from another build is never read as one.
    #[test]
    fn a_damaged_manifest_is_refused_and_released_explicitly() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);

        chain.prepare(&store, &[]).unwrap();

        let path = root.path().join("bundles/2.json");
        let original = fs::read_to_string(&path).unwrap();

        for damaged in [
            original[..original.len() / 2].to_owned(),
            original.replace("\"capture_from\":1", "\"capture_from\":2"),
        ] {
            assert_ne!(damaged, original);
            fs::write(&path, damaged).unwrap();

            assert!(matches!(
                preparation_refusal(store.load(2)),
                Refusal::Corrupt { sequence: 2, .. }
            ));
            assert!(
                chain.prepare(&store, &[]).is_err(),
                "a damaged bundle is not silently captured over"
            );
            assert!(
                store.sweep().is_err(),
                "nothing is swept past what it cannot read"
            );
        }

        store.release(2).unwrap();
        assert!(blobs(root.path()).is_empty());

        chain.prepare(&store, &[]).unwrap();
        store.load(2).unwrap();
    }

    /// Rewrite a manifest in place under a checksum that matches it, as a build
    /// of another format or a damaged claim would read.
    fn rewrite_manifest(path: &Path, edit: impl Fn(&str) -> String) {
        let raw = fs::read_to_string(path).unwrap();
        let start = raw.find("\"manifest\":").unwrap() + "\"manifest\":".len();
        let manifest = edit(&raw[start..raw.len() - 1]);

        assert_ne!(manifest, raw[start..raw.len() - 1]);

        let checksum = xxhash_rust::xxh3::xxh3_64(manifest.as_bytes());

        fs::write(
            path,
            format!("{{\"checksum\":{checksum},\"manifest\":{manifest}}}"),
        )
        .unwrap();
    }

    /// Another build's format is refused on its own account.
    #[test]
    fn a_manifest_of_another_format_is_refused() {
        let root = tempdir();
        let store = store(root.path());

        Chain::at(Shape::Shelley, 2).prepare(&store, &[]).unwrap();

        store.load(2).unwrap();
        rewrite_manifest(&root.path().join("bundles/2.json"), |manifest| {
            manifest.replace("\"format\":1", "\"format\":2")
        });

        let reason = match preparation_refusal(store.load(2)) {
            Refusal::Corrupt { reason, .. } => reason,
            other => panic!("{other:?}"),
        };

        assert!(reason.contains("format 2"), "{reason}");
    }

    /// A manifest's claims about a payload are held against the payload as it
    /// is read, so one that lies produces no layer.
    #[test]
    fn a_payload_is_held_to_what_its_manifest_claims() {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        let tail = bundle
            .layers()
            .find(|layer| layer.kind == INDEXES)
            .unwrap()
            .records;

        rewrite_manifest(&root.path().join("bundles/2.json"), |manifest| {
            manifest.replacen(
                &format!("\"records\":{tail}"),
                &format!("\"records\":{}", tail + 1),
                1,
            )
        });

        let lying = store.load(2).unwrap();
        let refused = chain.finalize(
            finished_root.path(),
            &store,
            &lying,
            &staged,
            &export::First,
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::Corrupt { sequence: 2, .. }
        ));
        assert!(!finished_root.path().join("inscription.json").exists());
    }

    /// The window before the boundary is captured even under a seal that would
    /// call it final: its rows still settle.
    #[test]
    fn the_window_before_the_boundary_is_always_captured() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let delta = chain.boundary_delta.clone().unwrap();

        let bundle = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: 0,
                tail: IndexTail::Delta(&delta),
            },
            &Observer::silent(),
        )
        .unwrap();

        assert!(bundle
            .layers()
            .any(|layer| layer.kind == "log-account-epochs" && layer.scope["epoch"] == 1));
    }

    /// A manifest renamed to another sequence names the one it was written for.
    #[test]
    fn a_manifest_under_another_sequence_is_refused() {
        let root = tempdir();
        let store = store(root.path());

        Chain::at(Shape::Shelley, 2).prepare(&store, &[]).unwrap();

        fs::rename(
            root.path().join("bundles/2.json"),
            root.path().join("bundles/3.json"),
        )
        .unwrap();

        assert!(matches!(
            preparation_refusal(store.load(3)),
            Refusal::Corrupt { sequence: 3, .. }
        ));
    }

    /// Done-when 3: changed source identity fails, and the marker stands.
    #[test]
    fn a_changed_source_is_refused() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let held = fs::read(root.path().join("bundles/2.json")).unwrap();

        // The same request over the same stores is the same bundle, and the
        // walk that establishes it writes nothing.
        let same = chain.prepare(&store, &[]).unwrap();
        assert_eq!(
            same.layers().collect::<Vec<_>>(),
            bundle.layers().collect::<Vec<_>>()
        );

        // Another retained list, another contract, another block at the boundary.
        let contract = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: "another".to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Traverse,
            },
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(chain.prepare(&store, &[1])),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));
        assert!(matches!(
            preparation_refusal(contract),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));

        assert_eq!(fs::read(root.path().join("bundles/2.json")).unwrap(), held);
    }

    /// The stores and runs a bundle is finished from have to be its own
    /// chain's.
    #[test]
    fn finalizing_from_another_source_is_refused() {
        let (root, stage_root, other_stage_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        // An archive holding another block at the boundary's slot.
        let other = Chain::at(Shape::Byron, 3);
        let finished_root = tempdir();
        let refused = prepare::finalize(
            &SteleDir::create(finished_root.path()).unwrap(),
            &Finalizing {
                store: &store,
                bundle: &bundle,
                staged: &staged,
                producers: export::Producers::DEFAULT,
            },
            &other.archive,
            None,
            &export::First,
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));

        // Runs of another network, and of another store contract.
        let frontier = chain.tip.clone().unwrap();

        for (network_magic, store_contract) in [(1, CONTRACT), (MAINNET_MAGIC, "another")] {
            let mismatched = staging::stage(
                other_stage_root.path().join(store_contract).as_path(),
                &chain.archive,
                &chain.summary,
                chain.mutable,
                &staging::Request {
                    network_magic,
                    frontier: frontier.clone(),
                    store_contract: store_contract.to_owned(),
                    epochs: Default::default(),
                    live: false,
                },
                Budget::new(u64::MAX),
            )
            .unwrap();

            let refused = chain.finalize(
                tempdir().path(),
                &store,
                &bundle,
                &mismatched,
                &export::First,
            );

            assert!(matches!(
                preparation_refusal(refused),
                Refusal::SourceMismatch { sequence: 2, .. }
            ));
        }
    }

    /// One traversal has to serve every bundle of a batch, so they share a
    /// source, and a batch of none has nothing to stage.
    #[test]
    fn a_batch_of_mixed_sources_is_refused() {
        let (root, other_root, stage_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let other = self::store(other_root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let delta = chain.boundary_delta.clone().unwrap();
        let first = chain.prepare(&store, &[]).unwrap();

        let foreign = prepare::prepare(
            &other,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: "another".to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Delta(&delta),
            },
            &Observer::silent(),
        )
        .unwrap();

        chain.advance_to(3);

        let stage = |bundles: &[Bundle]| {
            prepare::stage_batch(
                stage_root.path(),
                &chain.archive,
                &chain.state,
                bundles,
                &export::First,
                Budget::new(u64::MAX),
                &Observer::silent(),
            )
        };

        assert!(matches!(
            preparation_refusal(stage(&[first, foreign])),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));
        assert!(matches!(
            preparation_refusal(stage(&[])),
            Refusal::EmptyBatch
        ));
    }

    /// A window nothing staged and no predecessor carries fails loudly, rather
    /// than leaving its layer out of the document.
    #[test]
    fn a_window_neither_staged_nor_inherited_fails() {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let staged = staging::stage(
            stage_root.path(),
            &chain.archive,
            &chain.summary,
            chain.mutable,
            &staging::Request {
                network_magic: MAINNET_MAGIC,
                frontier: chain.tip.clone().unwrap(),
                store_contract: CONTRACT.to_owned(),
                epochs: Default::default(),
                live: false,
            },
            Budget::new(u64::MAX),
        )
        .unwrap();

        let refused = chain.finalize(
            finished_root.path(),
            &store,
            &bundle,
            &staged,
            &export::First,
        );

        assert!(
            matches!(
                refused,
                Err(Error::Staging(staging::Refusal::NotStaged { .. }))
            ),
            "{refused:?}"
        );
        assert!(!finished_root.path().join("inscription.json").exists());
    }

    /// A batch needs its stages' frontier past every boundary it finishes.
    #[test]
    fn runs_from_before_the_boundary_are_refused() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let early = chain.prepare(&store, &[]).unwrap();
        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&early),
            &export::First,
        );

        chain.advance_to(3);

        let later = chain.prepare(&store, &[]).unwrap();
        let refused = chain.finalize(tempdir().path(), &store, &later, &staged, &export::First);

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::SourceMismatch { sequence: 3, .. }
        ));
    }

    /// The archive still has to reach back as far as finalization reads it.
    #[test]
    fn an_archive_pruned_past_the_horizon_is_refused() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 3);
        let bundle = chain.prepare(&store, &[]).unwrap();

        assert_eq!(bundle.horizon(), Some(0));

        chain.advance_to(4);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        // The same blocks, less the first epochs.
        let pruned = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
        let writer = pruned.start_writer().unwrap();

        for (slot, body) in chain.archive.get_range(Some(chain.start(2)), None).unwrap() {
            let block = MultiEraBlock::decode(&body).unwrap();
            writer
                .apply(
                    &ChainPoint::Specific(slot, block.hash()),
                    &Arc::new(body.clone()),
                )
                .unwrap();
        }

        writer.commit().unwrap();

        let refused = prepare::finalize(
            &SteleDir::create(tempdir().path()).unwrap(),
            &Finalizing {
                store: &store,
                bundle: &bundle,
                staged: &staged,
                producers: export::Producers::DEFAULT,
            },
            &pruned,
            None,
            &export::First,
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::HorizonLost {
                sequence: 3,
                horizon: 0,
                ..
            }
        ));
    }

    /// A root holds one journal's bundles, and another's cannot use or drop
    /// them.
    #[test]
    fn a_bundle_answers_only_to_its_owner() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);

        chain.prepare(&store, &[]).unwrap();

        let foreign = Store::open(root.path(), "another").unwrap();

        assert!(matches!(
            preparation_refusal(foreign.load(2)),
            Refusal::ForeignOwner { sequence: 2, .. }
        ));
        assert!(matches!(
            preparation_refusal(foreign.release(2)),
            Refusal::ForeignOwner { sequence: 2, .. }
        ));
        assert!(matches!(
            preparation_refusal(chain.prepare(&foreign, &[])),
            Refusal::ForeignOwner { sequence: 2, .. }
        ));

        store.load(2).unwrap();
    }

    /// Identical payloads are one file: a release keeps what another bundle
    /// refers to, and the last one leaves nothing behind.
    #[test]
    fn bundles_share_payloads_until_the_last_is_released() {
        let root = tempdir();
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Byron, 5);
        let five = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(6);

        let six = chain.prepare(&store, &[]).unwrap();

        let shared: Vec<_> = five
            .references()
            .intersection(&six.references())
            .copied()
            .collect();

        assert!(
            !shared.is_empty(),
            "the unsealed windows repeat between them"
        );

        let union = five.references().union(&six.references()).count();

        assert_eq!(blobs(root.path()).len(), union);
        assert_eq!(store.sequences().unwrap(), vec![5, 6]);

        store.release(5).unwrap();

        assert_eq!(store.sequences().unwrap(), vec![6]);
        assert_eq!(blobs(root.path()).len(), six.references().len());
        store.load(6).unwrap();

        // Releasing it again, or one never prepared, is not a fault.
        store.release(5).unwrap();
        store.release(9).unwrap();

        store.release(6).unwrap();
        assert!(blobs(root.path()).is_empty());
    }

    /// A release that died after its marker left payloads nothing refers to,
    /// and a crash mid-write left a staging file. Neither survives a reopen and
    /// sweep; a live bundle's payloads do.
    #[test]
    fn a_sweep_finishes_what_a_crash_left() {
        let root = tempdir();
        let store = store(root.path());
        let bundle = Chain::at(Shape::Shelley, 2).prepare(&store, &[]).unwrap();

        fs::write(root.path().join("blobs/sha256/orphan"), b"x").unwrap();
        fs::write(root.path().join("blobs/.staging-1-1"), b"x").unwrap();

        let store = Store::open(root.path(), "journal").unwrap();

        assert!(!root.path().join("blobs/.staging-1-1").exists());

        store.sweep().unwrap();

        assert_eq!(blobs(root.path()).len(), bundle.references().len());
        store.load(2).unwrap();
    }

    /// A window restricted away gets no payload and no horizon; the state tip
    /// is the tip whatever history travels with it.
    #[test]
    fn a_restricted_plan_captures_only_its_windows() {
        let (root, stage_root, frozen_root, finished_root) =
            (tempdir(), tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 4);
        let delta = chain.boundary_delta.clone().unwrap();
        let plan = chain.plan(&[]).restrict_epochs(Some(4), None);

        let bundle = prepare::prepare(
            &store,
            &plan,
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Delta(&delta),
            },
            &Observer::silent(),
        )
        .unwrap();

        assert_eq!(bundle.horizon(), None);

        let frozen = export::export(
            &SteleDir::create(frozen_root.path()).unwrap(),
            &plan,
            &chain.archive,
            &chain.state,
            None,
            &export::First,
            &Observer::silent(),
        )
        .unwrap();

        chain.advance_to(5);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        assert_eq!(staged.records(Family::Indexes, 3), None);

        let finished = chain
            .finalize(
                finished_root.path(),
                &store,
                &bundle,
                &staged,
                &export::First,
            )
            .unwrap();

        assert_same_stele(
            (frozen_root.path(), &frozen),
            (finished_root.path(), &finished),
            "restricted",
        );
    }

    /// The host surface: a boundary captured, a batch staged and a document
    /// reproduced through the snapshot source alone, identical to the ordinary
    /// digest of the stores frozen there.
    #[test]
    fn the_headless_facade_prepares_stages_and_finalizes() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 3);
        let delta = chain.boundary_delta.clone().unwrap();
        let plan = chain.plan(&[]);

        let source = StoreSnapshot::new(&chain.archive, &chain.state);
        let bundle = source
            .prepare_boundary(
                &store,
                &plan,
                &Request {
                    store_contract: CONTRACT.to_owned(),
                    mutable_slots: chain.mutable,
                    tail: IndexTail::Delta(&delta),
                },
                &Observer::silent(),
            )
            .unwrap();
        let frozen = source.digest_document(&plan, &export::First).unwrap();

        chain.advance_to(4);

        let source = StoreSnapshot::new(&chain.archive, &chain.state);
        let staged = source
            .stage_batch(
                stage_root.path(),
                std::slice::from_ref(&bundle),
                &export::First,
                Budget::new(u64::MAX),
                &Observer::silent(),
            )
            .unwrap();

        let finalizing = Finalizing {
            store: &store,
            bundle: &bundle,
            staged: &staged,
            producers: export::Producers::DEFAULT,
        };

        let document = source
            .finalize_document(&finalizing, &export::First)
            .unwrap();

        assert_eq!(document.identity, frozen.identity);
        assert_eq!(document.canonical, frozen.canonical);
    }

    /// A capture reports what it writes, like every other layer path.
    #[test]
    fn a_capture_reports_every_layer() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let delta = chain.boundary_delta.clone().unwrap();
        let watcher = Arc::new(Watcher::default());

        // Retained epochs due at the boundary, one it would adopt and one it
        // cuts, are not captured: its tip is what a publication describes twice.
        let bundle = prepare::prepare(
            &store,
            &chain.plan(&[1, 2]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Delta(&delta),
            },
            &watcher.observer(),
        )
        .unwrap();

        watcher.assert_well_formed(bundle.layers().count());
    }

    /// Finalizing reports what an export reports, against a real count.
    #[test]
    fn finalizing_reports_every_layer_and_record() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 3);
        let bundle = chain.prepare(&store, &[3]).unwrap();

        chain.advance_to(4);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let watcher = Arc::new(Watcher::default());

        let finished = prepare::finalize(
            &Discarding,
            &Finalizing {
                store: &store,
                bundle: &bundle,
                staged: &staged,
                producers: export::Producers::DEFAULT,
            },
            &chain.archive,
            None,
            &export::First,
            &watcher.observer(),
        )
        .unwrap();

        watcher.assert_well_formed(finished.layers.len());

        // A dump described from its tip is the same layer's records once.
        let dumps = dumps_in(&finished, 3);
        let content: u64 = finished
            .layers
            .iter()
            .filter(|layer| !dumps.iter().any(|dump| std::ptr::eq(*dump, *layer)))
            .map(|layer| layer.records - 1)
            .sum();

        assert_eq!(watcher.records(), content);
    }

    fn dumps_in(
        inscription: &Inscription,
        epoch: u64,
    ) -> Vec<&stelae::inscription::LayerDescriptor> {
        inscription
            .layers
            .iter()
            .filter(|layer| state_ns_for(&layer.kind).is_some() && layer.scope["epoch"] == epoch)
            .collect()
    }

    fn plain_request<'a>(chain: &Chain, delta: &'a ArchiveIndexDelta) -> Request<'a> {
        Request {
            store_contract: CONTRACT.to_owned(),
            mutable_slots: chain.mutable,
            tail: IndexTail::Delta(delta),
        }
    }

    /// Prepare at 2, advance, read the manifest as another build's would, then
    /// load it.
    fn load_as_another_build(edit: impl Fn(&str) -> String) -> Refusal {
        let root = tempdir();
        let store = store(root.path());

        Chain::at(Shape::Shelley, 2).prepare(&store, &[]).unwrap();
        rewrite_manifest(&root.path().join("bundles/2.json"), edit);

        let refusal = preparation_refusal(store.load(2));

        // It is still this journal's to drop.
        store.release(2).unwrap();
        assert!(blobs(root.path()).is_empty());

        refusal
    }

    /// Old records would land in a sealed stele under this build's labels, and
    /// no layer header stops them.
    #[test]
    fn a_bundle_of_another_media_type_version_is_refused() {
        let refusal = load_as_another_build(|manifest| {
            manifest.replace("\"media_type_version\":1", "\"media_type_version\":2")
        });

        assert!(
            matches!(refusal, Refusal::ForeignBuild { sequence: 2, .. }),
            "{refusal:?}"
        );
    }

    #[test]
    fn a_bundle_of_another_profile_version_is_refused() {
        let refusal = load_as_another_build(|manifest| {
            manifest.replace("io.txpipe.dolos.cardano/1", "io.txpipe.dolos.cardano/2")
        });

        assert!(
            matches!(refusal, Refusal::ForeignBuild { sequence: 2, .. }),
            "{refusal:?}"
        );
    }

    /// A batch is staged from these stores, so a bundle captured under another
    /// contract is refused before anything is traversed.
    #[test]
    fn a_changed_contract_is_refused_through_the_batch() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let delta = chain.boundary_delta.clone().unwrap();

        let bundle = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: "another".to_owned(),
                ..plain_request(&chain, &delta)
            },
            &Observer::silent(),
        )
        .unwrap();

        chain.advance_to(3);

        let refused = prepare::stage_batch(
            stage_root.path(),
            &chain.archive,
            &chain.state,
            std::slice::from_ref(&bundle),
            &export::First,
            Budget::new(u64::MAX),
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));
        assert_eq!(chain.archive.traversals(), (0, 0));
    }

    /// Stores rebuilt to the same cursor with one row different: the held
    /// bundle is refused, naming the layer, and once released the rebuilt
    /// stores capture and finish as themselves.
    #[test]
    fn rebuilt_stores_do_not_reuse_a_bundle() {
        let (root, stage_root, frozen_root, finished_root) =
            (tempdir(), tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let first = Chain::at(Shape::Shelley, 2);

        first.prepare(&store, &[]).unwrap();

        let held_marker = fs::read(root.path().join("bundles/2.json")).unwrap();
        let held_blobs = blobs(root.path());
        let mut rebuilt = Chain::at(Shape::Shelley, 2);

        assert_eq!(rebuilt.tip, first.tip);

        let key = common::state_key(AccountState::NS, 0x40);
        let writer = rebuilt.state.start_writer().unwrap();

        writer
            .write_entity(
                AccountState::NS,
                &EntityKey::from_raw(&key),
                &vec![0x82, 0x09, 0x09],
            )
            .unwrap();
        writer.commit().unwrap();

        let what = match preparation_refusal(rebuilt.prepare(&store, &[])) {
            Refusal::SourceMismatch { sequence: 2, what } => what,
            other => panic!("{other:?}"),
        };

        assert!(what.contains("state-accounts"), "{what}");
        assert_eq!(
            fs::read(root.path().join("bundles/2.json")).unwrap(),
            held_marker,
            "the marker stands"
        );
        assert_eq!(blobs(root.path()), held_blobs, "the walk wrote nothing");

        store.release(2).unwrap();

        let frozen = rebuilt.export(frozen_root.path(), &[], &export::First);
        let bundle = rebuilt.prepare(&store, &[]).unwrap();

        rebuilt.advance_to(3);

        let staged = rebuilt.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let finished = rebuilt
            .finalize(
                finished_root.path(),
                &store,
                &bundle,
                &staged,
                &export::First,
            )
            .unwrap();

        assert_same_stele(
            (frozen_root.path(), &frozen),
            (finished_root.path(), &finished),
            "rebuilt",
        );
    }

    /// The check on a held bundle covers everything it captured: a log row and
    /// a delta for the tail, as well as a state row.
    #[test]
    fn a_held_bundle_is_held_to_its_logs_and_its_tail() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let mut delta = chain.boundary_delta.clone().unwrap();

        chain.prepare(&store, &[]).unwrap();

        delta.tx_hashes.pop();

        let what = match preparation_refusal(prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &plain_request(&chain, &delta),
            &Observer::silent(),
        )) {
            Refusal::SourceMismatch { what, .. } => what,
            other => panic!("{other:?}"),
        };

        assert!(what.contains(INDEXES), "{what}");

        let writer = chain.archive.start_writer().unwrap();
        chain.log(&writer, PoolEpochLog::NS, 1, 9, 99);
        writer.commit().unwrap();

        let what = match preparation_refusal(chain.prepare(&store, &[])) {
            Refusal::SourceMismatch { what, .. } => what,
            other => panic!("{other:?}"),
        };

        assert!(what.contains("log-pool-epochs"), "{what}");
    }

    /// A restart at the boundary with no delta left: the explicit traversal
    /// finds the same bundle, at the cost of one scan.
    #[test]
    fn a_restart_at_the_boundary_finds_its_bundle_by_traversal() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        let again = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Traverse,
            },
            &Observer::silent(),
        )
        .unwrap();

        assert_eq!(chain.archive.traversals(), (1, 1));
        assert_eq!(
            again.layers().collect::<Vec<_>>(),
            bundle.layers().collect::<Vec<_>>()
        );
    }

    #[test]
    fn another_block_at_the_boundary_is_refused() {
        let root = tempdir();
        let store = store(root.path());
        let held = Chain::at(Shape::Shelley, 2);

        held.prepare(&store, &[]).unwrap();

        let mut fork = Chain::new(Shape::Shelley);
        fork.number = 100;
        fork.advance_to(2);

        let (ours, theirs) = (held.tip.clone().unwrap(), fork.tip.clone().unwrap());

        assert_eq!(ours.slot(), theirs.slot());
        assert_ne!(ours.hash(), theirs.hash());

        let what = match preparation_refusal(fork.prepare(&store, &[])) {
            Refusal::SourceMismatch { sequence: 2, what } => what,
            other => panic!("{other:?}"),
        };

        assert!(what.contains("boundary block"), "{what}");
    }

    #[test]
    fn another_coverage_or_seal_is_refused() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 4);
        let delta = chain.boundary_delta.clone().unwrap();

        chain.prepare(&store, &[]).unwrap();

        let refused = |plan: Plan, request: Request<'_>| {
            let what = match preparation_refusal(prepare::prepare(
                &store,
                &plan,
                &chain.archive,
                &chain.state,
                &request,
                &Observer::silent(),
            )) {
                Refusal::SourceMismatch { sequence: 4, what } => what,
                other => panic!("{other:?}"),
            };

            what
        };

        let windows = refused(
            chain.plan(&[]).restrict_epochs(Some(4), None),
            plain_request(&chain, &delta),
        );
        assert!(windows.contains("windows"), "{windows}");

        let sealing = refused(
            chain.plan(&[]),
            Request {
                mutable_slots: chain.mutable + 1,
                ..plain_request(&chain, &delta)
            },
        );
        assert!(sealing.contains("mutable"), "{sealing}");
    }

    /// A changed retained list says so, rather than "other stores".
    #[test]
    fn the_refusal_names_what_differs() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);

        chain.prepare(&store, &[]).unwrap();

        let what = match preparation_refusal(chain.prepare(&store, &[1])) {
            Refusal::SourceMismatch { what, .. } => what,
            other => panic!("{other:?}"),
        };

        assert!(what.contains("retained"), "{what}");
    }

    fn blocks_from(chain: &Chain, from: u64) -> Vec<(ChainPoint, Vec<u8>)> {
        chain
            .archive
            .get_range(Some(from), None)
            .unwrap()
            .map(|(slot, body)| {
                let hash = MultiEraBlock::decode(&body).unwrap().hash();

                (ChainPoint::Specific(slot, hash), body.to_vec())
            })
            .collect()
    }

    fn archive_of(
        schema: dolos_core::StateSchema,
        blocks: Vec<(ChainPoint, Vec<u8>)>,
    ) -> MemoryArchiveStore {
        let archive = MemoryArchiveStore::new(schema);
        let writer = archive.start_writer().unwrap();

        for (point, body) in blocks {
            writer.apply(&point, &Arc::new(body)).unwrap();
        }

        writer.commit().unwrap();

        archive
    }

    fn pruned_archive(blocks: Vec<(ChainPoint, Vec<u8>)>) -> MemoryArchiveStore {
        archive_of(dolos_cardano::model::build_log_schema(), blocks)
    }

    fn finalize_on(
        root: &Path,
        store: &Store,
        bundle: &Bundle,
        staged: &Staged,
        archive: &MemoryArchiveStore,
    ) -> Result<Inscription, Error> {
        prepare::finalize(
            &SteleDir::create(root).unwrap(),
            &Finalizing {
                store,
                bundle,
                staged,
                producers: export::Producers::DEFAULT,
            },
            archive,
            None,
            &export::First,
            &Observer::silent(),
        )
    }

    /// Prepare `plan`, advance, prune past a closed window and finish: closed
    /// blocks are read from the archive, so that is `HorizonLost` and never a
    /// stele whose closed blocks read as empty.
    fn assert_a_prune_is_refused(mut chain: Chain, plan: Plan, prune_at_epoch: u64, horizon: u64) {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let delta = chain.boundary_delta.clone().unwrap();

        let bundle = prepare::prepare(
            &store,
            &plan,
            &chain.archive,
            &chain.state,
            &plain_request(&chain, &delta),
            &Observer::silent(),
        )
        .unwrap();

        assert_eq!(bundle.horizon(), Some(horizon));

        let advance = chain.epoch + 1;
        chain.advance_to(advance);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let pruned = pruned_archive(blocks_from(&chain, chain.start(prune_at_epoch)));

        let refused = finalize_on(finished_root.path(), &store, &bundle, &staged, &pruned);

        assert!(
            matches!(
                preparation_refusal(refused),
                Refusal::HorizonLost {
                    sequence,
                    horizon: lost,
                    ..
                } if sequence == bundle.sequence() && lost == horizon
            ),
            "at {}",
            bundle.sequence()
        );
    }

    #[test]
    fn a_restricted_plan_is_refused_past_its_closed_window() {
        let chain = Chain::at(Shape::Shelley, 4);
        let plan = chain.plan(&[]).restrict_epochs(Some(3), None);
        let horizon = chain.start(3);

        assert_a_prune_is_refused(chain, plan, 4, horizon);
    }

    #[test]
    fn the_first_boundary_is_refused_past_its_closed_window() {
        let chain = Chain::at(Shape::Shelley, 1);
        let plan = chain.plan(&[]);

        assert_a_prune_is_refused(chain, plan, 1, 0);
    }

    #[test]
    fn an_early_byron_boundary_is_refused_past_its_closed_windows() {
        let chain = Chain::at(Shape::Byron, 3);
        let plan = chain.plan(&[]);

        assert_a_prune_is_refused(chain, plan, 3, 0);
    }

    /// Staging reads closed windows too: a pruned archive would give it empty
    /// runs, so it is refused there, before a traversal.
    #[test]
    fn a_batch_is_not_staged_from_an_archive_past_its_horizon() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 4);
        let delta = chain.boundary_delta.clone().unwrap();
        let plan = chain.plan(&[]).restrict_epochs(Some(3), None);

        let bundle = prepare::prepare(
            &store,
            &plan,
            &chain.archive,
            &chain.state,
            &plain_request(&chain, &delta),
            &Observer::silent(),
        )
        .unwrap();

        chain.advance_to(5);

        let refused = prepare::stage_batch(
            stage_root.path(),
            &pruned_archive(blocks_from(&chain, chain.start(4))),
            &chain.state,
            std::slice::from_ref(&bundle),
            &export::First,
            Budget::new(u64::MAX),
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::HorizonLost { sequence: 4, .. }
        ));
    }

    /// With no closed window nothing needs retaining but the boundary block,
    /// and the publication finishes on the pruned archive. Without that block
    /// the chain cannot be checked, which is a refusal.
    #[test]
    fn no_horizon_needs_only_the_boundary_block() {
        let (root, stage_root, frozen_root, finished_root, gone_root) =
            (tempdir(), tempdir(), tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 4);
        let delta = chain.boundary_delta.clone().unwrap();
        let plan = chain.plan(&[]).restrict_epochs(Some(4), None);

        let bundle = prepare::prepare(
            &store,
            &plan,
            &chain.archive,
            &chain.state,
            &plain_request(&chain, &delta),
            &Observer::silent(),
        )
        .unwrap();

        assert_eq!(bundle.horizon(), None);

        let frozen = export::export(
            &SteleDir::create(frozen_root.path()).unwrap(),
            &plan,
            &chain.archive,
            &chain.state,
            None,
            &export::First,
            &Observer::silent(),
        )
        .unwrap();

        let boundary = bundle.cursor().slot();

        chain.advance_to(5);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        let finished = finalize_on(
            finished_root.path(),
            &store,
            &bundle,
            &staged,
            &pruned_archive(blocks_from(&chain, boundary)),
        )
        .unwrap();

        assert_same_stele(
            (frozen_root.path(), &frozen),
            (finished_root.path(), &finished),
            "no horizon",
        );

        let refused = finalize_on(
            gone_root.path(),
            &store,
            &bundle,
            &staged,
            &pruned_archive(blocks_from(&chain, boundary + 1)),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::SourceMismatch { sequence: 4, .. }
        ));
    }

    /// A delta for the main block sharing the boundary block's slot.
    #[test]
    fn a_delta_for_the_main_block_at_the_boundary_slot_is_refused() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Byron, 3);
        let (point, _) = make_byron_block(3, 0, chain.number, chain.previous_hash());
        let boundary = chain.tip.clone().unwrap();

        assert_eq!(point.slot(), boundary.slot());
        assert_ne!(point.hash(), boundary.hash());

        let foreign = ArchiveIndexDelta {
            slot: point.slot(),
            block_hash: point.hash().unwrap().to_vec(),
            block_number: Some(chain.number),
            tx_hashes: vec![[[0xeeu8; 24].as_slice(), &point.slot().to_be_bytes()].concat()],
            tags: vec![],
        };

        let refused = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &chain.archive,
            &chain.state,
            &plain_request(&chain, &foreign),
            &Observer::silent(),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::ForeignTail { .. }
        ));
    }

    /// An archive that really holds another block at the boundary's slot.
    #[test]
    fn a_fork_block_at_the_boundary_slot_is_refused() {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let slot = bundle.cursor().slot();

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );

        let (fork_point, fork_body) =
            make_conway_block_with_prev(slot, Some(Hash::new([7; 32])), 2);

        assert_eq!(fork_point.slot(), slot);
        assert_ne!(fork_point.hash(), bundle.cursor().hash());

        let blocks = blocks_from(&chain, 0)
            .into_iter()
            .map(|(point, body)| match point.slot() == slot {
                true => (fork_point.clone(), fork_body.to_vec()),
                false => (point, body),
            })
            .collect();

        let refused = finalize_on(
            finished_root.path(),
            &store,
            &bundle,
            &staged,
            &pruned_archive(blocks),
        );

        assert!(matches!(
            preparation_refusal(refused),
            Refusal::SourceMismatch { sequence: 2, .. }
        ));
    }

    /// A log namespace the format has no layer for is refused here as an export
    /// refuses it.
    #[test]
    fn prepare_refuses_an_uncovered_log_namespace() {
        let root = tempdir();
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);

        let stray = NAMESPACES
            .into_iter()
            .find(|ns| *ns != UTXOS && !LOG_NAMESPACES.contains(ns))
            .unwrap();

        let mut schema = dolos_cardano::model::build_log_schema();
        schema.insert(stray, dolos_core::NamespaceType::KeyValue);

        let archive = archive_of(schema, blocks_from(&chain, 0));
        let writer = archive.start_writer().unwrap();

        writer
            .write_log(
                stray,
                &LogKey::from_raw(&chain.start(1).to_be_bytes()).unwrap(),
                &vec![0xa0],
            )
            .unwrap();
        writer.commit().unwrap();

        let refused = prepare::prepare(
            &store,
            &chain.plan(&[]),
            &archive,
            &chain.state,
            &plain_request(&chain, chain.boundary_delta.as_ref().unwrap()),
            &Observer::silent(),
        );

        match refused {
            Err(Error::UncoveredLogNamespace { epoch, ns, .. }) => {
                assert_eq!(epoch, 1);
                assert_eq!(ns, stray);
            }
            other => panic!("{:?}", other.map(|bundle| bundle.sequence())),
        }
    }

    /// Records what the archive had been asked by the first event, and runs
    /// `then` at that moment: dies as an OOM kill does, or lets a peer run.
    struct AtFirstEvent {
        tags: Arc<AtomicUsize>,
        exacts: Arc<AtomicUsize>,
        entities: Arc<AtomicUsize>,
        seen: std::sync::Mutex<Option<(usize, usize, usize)>>,
        then: Box<dyn Fn() + Send + Sync>,
    }

    impl AtFirstEvent {
        fn over(chain: &Chain, then: impl Fn() + Send + Sync + 'static) -> Self {
            Self {
                tags: chain.archive.tags.clone(),
                exacts: chain.archive.exacts.clone(),
                entities: chain.archive.entities.clone(),
                seen: Default::default(),
                then: Box::new(then),
            }
        }
    }

    impl stelae::progress::Progress for AtFirstEvent {
        fn on(&self, _event: stelae::progress::Event<'_>) {
            let mut seen = self.seen.lock().unwrap();

            if seen.is_none() {
                *seen = Some((
                    self.tags.load(Ordering::Relaxed),
                    self.exacts.load(Ordering::Relaxed),
                    self.entities.load(Ordering::Relaxed),
                ));

                (self.then)();
            }
        }
    }

    fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();

        names.sort();
        names
    }

    fn size_of(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let meta = entry.metadata().unwrap();

                match meta.is_dir() {
                    true => size_of(&entry.path()),
                    false => meta.len(),
                }
            })
            .sum()
    }

    fn export_under(chain: &Chain, scratch: &Path, observer: &Observer) -> Inscription {
        export::export(
            &Discarding,
            &chain.plan(&[]).with_scratch(scratch),
            &chain.archive,
            &chain.state,
            None,
            &export::First,
            observer,
        )
        .unwrap()
    }

    /// Not a test by itself: dies without running destructors once its spool
    /// holds runs.
    #[cfg(unix)]
    #[test]
    fn child_killed_while_it_assembles() {
        let Ok(scratch) = std::env::var("BOUNDARY_TEST_KILLED_SCRATCH") else {
            return;
        };

        struct DieAtFirstLayer;

        impl stelae::progress::Progress for DieAtFirstLayer {
            fn on(&self, event: stelae::progress::Event<'_>) {
                if matches!(event, stelae::progress::Event::LayerStarted { .. }) {
                    std::process::abort();
                }
            }
        }

        export_under(
            &Chain::at(Shape::Shelley, 4),
            Path::new(&scratch),
            &Observer::new(Arc::new(DieAtFirstLayer)),
        );
    }

    /// A killed export leaves its whole spool, which the next one clears along
    /// with a directory something died before locking.
    #[cfg(unix)]
    #[test]
    fn a_retry_clears_what_a_killed_export_spooled() {
        let scratch = tempdir();

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "boundary_preparation::child_killed_while_it_assembles",
                "--test-threads=1",
            ])
            .env("BOUNDARY_TEST_KILLED_SCRATCH", scratch.path())
            .status()
            .unwrap();

        assert!(!status.success(), "the child was meant to die: {status:?}");
        assert_eq!(names_in(scratch.path()).len(), 1);
        assert!(size_of(scratch.path()) > 0, "the spool holds runs");

        fs::create_dir(scratch.path().join("dolos-staging-x")).unwrap();
        fs::write(scratch.path().join("unrelated"), b"x").unwrap();

        export_under(
            &Chain::at(Shape::Shelley, 4),
            scratch.path(),
            &Observer::silent(),
        );

        assert_eq!(names_in(scratch.path()), ["unrelated"]);
    }

    /// A digest and a publish can share a scratch directory, so an export's
    /// sweep leaves a running peer's spool alone.
    #[test]
    fn a_sweep_leaves_a_live_peers_spool() {
        let scratch = tempdir();
        let chain = Chain::at(Shape::Shelley, 4);
        let path = scratch.path().to_owned();
        let during = Arc::new(std::sync::Mutex::new(Vec::new()));
        let during_peer = during.clone();

        let progress = AtFirstEvent::over(&chain, move || {
            let before = names_in(&path);

            assert_eq!(before.len(), 1, "{before:?}");

            export_under(&Chain::at(Shape::Shelley, 3), &path, &Observer::silent());

            *during_peer.lock().unwrap() = names_in(&path);

            assert_eq!(names_in(&path), before, "the peer took a live spool");
        });

        let live = export_under(&chain, scratch.path(), &Observer::new(Arc::new(progress)));

        assert!(!live.layers.is_empty());
        assert_eq!(during.lock().unwrap().len(), 1);
        assert!(names_in(scratch.path()).is_empty());
    }

    /// Exports sharing a scratch directory, each sweeping while the others
    /// claim and work, all finish and leave nothing.
    #[test]
    fn peers_sharing_scratch_survive_each_others_sweeps() {
        let scratch = tempdir();
        let chain = Chain::at(Shape::Shelley, 2);
        let digest = export_under(&chain, scratch.path(), &Observer::silent())
            .digest()
            .unwrap();

        std::thread::scope(|threads| {
            for _ in 0..8 {
                threads.spawn(|| {
                    for _ in 0..16 {
                        let again = export_under(&chain, scratch.path(), &Observer::silent());

                        assert_eq!(again.digest().unwrap(), digest);
                    }
                });
            }
        });

        assert!(names_in(scratch.path()).is_empty());
    }

    /// The scratch a plan names holds the runs and is left empty.
    #[test]
    fn with_scratch_holds_the_runs_and_leaves_nothing() {
        let scratch = tempdir();
        let chain = Chain::at(Shape::Shelley, 4);
        let path = scratch.path().to_owned();
        let during = Arc::new(std::sync::Mutex::new(Vec::new()));
        let during_listing = during.clone();

        let progress = AtFirstEvent::over(&chain, move || {
            *during_listing.lock().unwrap() = names_in(&path);
        });

        export_under(&chain, scratch.path(), &Observer::new(Arc::new(progress)));

        let during = during.lock().unwrap();

        assert_eq!(during.len(), 1, "{during:?}");
        assert!(during[0].starts_with("dolos-staging-"), "{during:?}");
        assert!(names_in(scratch.path()).is_empty());
    }

    /// Not a test by itself: a digest under a `TMPDIR` nothing can be created
    /// in, with no scratch and then with one.
    #[cfg(unix)]
    #[test]
    fn child_digests_without_a_temporary_directory() {
        let Ok(scratch) = std::env::var("BOUNDARY_TEST_DIGEST_SCRATCH") else {
            return;
        };

        let chain = Chain::at(Shape::Shelley, 4);
        let digest = |plan: &Plan| {
            export::digest_document(plan, &chain.archive, &chain.state, &export::First)
                .map(|_| ())
                .map_err(|error| error.to_string())
        };

        println!("unset: {:?}", digest(&chain.plan(&[])));
        println!(
            "named: {:?}",
            digest(&chain.plan(&[]).with_scratch(&scratch))
        );
    }

    /// A digest spools like any export. With nowhere to do it the refusal says
    /// where and what to set, and the scratch a plan names is all it needs.
    #[cfg(unix)]
    #[test]
    fn a_digest_spools_where_its_plan_says() {
        let dir = tempdir();
        let not_a_directory = dir.path().join("file");

        fs::write(&not_a_directory, b"x").unwrap();

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "boundary_preparation::child_digests_without_a_temporary_directory",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("BOUNDARY_TEST_DIGEST_SCRATCH", dir.path().join("scratch"))
            .env("TMPDIR", &not_a_directory)
            .output()
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(
            stdout.contains("Plan::with_scratch"),
            "the refusal names the fix: {stdout}"
        );
        assert!(
            stdout.contains(not_a_directory.to_str().unwrap()),
            "the refusal names the place: {stdout}"
        );
        assert!(stdout.contains("named: Ok(())"), "{stdout}");
    }

    /// An observer hears before a traversal starts, not after it ends, so a
    /// hung one is not mistaken for a run with nothing to say.
    #[test]
    fn an_event_precedes_the_index_traversal() {
        let chain = Chain::at(Shape::Shelley, 4);
        let progress = Arc::new(AtFirstEvent::over(&chain, || {}));
        let scratch = tempdir();

        export_under(&chain, scratch.path(), &Observer::new(progress.clone()));

        assert_eq!(progress.seen.lock().unwrap().unwrap(), (0, 0, 0));
    }

    #[test]
    fn a_batch_tells_its_observer_before_it_traverses() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let progress = Arc::new(AtFirstEvent::over(&chain, || {}));

        prepare::stage_batch(
            stage_root.path(),
            &chain.archive,
            &chain.state,
            std::slice::from_ref(&bundle),
            &export::First,
            Budget::new(u64::MAX),
            &Observer::new(progress.clone()),
        )
        .unwrap();

        assert_eq!(progress.seen.lock().unwrap().unwrap(), (0, 0, 0));
    }

    /// A resumption record naming `recorded`: `carried_forward` says yes, and
    /// `adopt` says no unless the blob is still held, the one disagreement
    /// `Predecessor::carried_forward` documents.
    struct RecordedBlob {
        inner: Lending,
        recorded: stelae::inscription::LayerDescriptor,
        blob_held: bool,
    }

    impl export::Predecessor for RecordedBlob {
        fn history(&self) -> &[stelae::inscription::HistoryEntry] {
            export::Predecessor::history(&self.inner)
        }

        fn adopt(
            &self,
            kind: &str,
            scope: &serde_json::Value,
        ) -> Result<Option<stelae::inscription::LayerDescriptor>, stelae_driver::Error> {
            if self.blob_held && kind == self.recorded.kind && *scope == self.recorded.scope {
                return Ok(Some(self.recorded.clone()));
            }

            export::Predecessor::adopt(&self.inner, kind, scope)
        }

        fn carried_forward(
            &self,
            kind: &str,
            scope: &serde_json::Value,
        ) -> Result<bool, stelae_driver::Error> {
            Ok(
                (kind == self.recorded.kind && *scope == self.recorded.scope)
                    || export::Predecessor::carried_forward(&self.inner, kind, scope)?,
            )
        }

        fn landed(
            &self,
            descriptor: &stelae::inscription::LayerDescriptor,
        ) -> Result<(), stelae_driver::Error> {
            export::Predecessor::landed(&self.inner, descriptor)
        }
    }

    /// The seed at 1, and an interrupted publish at 2 whose layer of `kind` for
    /// `epoch` is what the record names.
    fn interrupted_at_two(
        kind: &str,
        epoch: u64,
    ) -> (
        Inscription,
        Inscription,
        stelae::inscription::LayerDescriptor,
    ) {
        let (seed_root, whole_root) = (tempdir(), tempdir());
        let seed = Chain::at(Shape::Shelley, 1).export(seed_root.path(), &[], &export::First);
        let at_two = Chain::at(Shape::Shelley, 2);
        let plan = at_two.plan(&[]);
        let whole = at_two.export(whole_root.path(), &[], &Lending::following(&seed, &plan));
        let recorded = layer_of(&whole, kind, epoch).unwrap().clone();

        (seed, whole, recorded)
    }

    /// The control: an export asks `adopt` first, so it rebuilds.
    #[test]
    fn an_export_rebuilds_a_dropped_recorded_blob() {
        let (seed, whole, recorded) = interrupted_at_two(INDEXES, 1);
        let chain = Chain::at(Shape::Shelley, 2);
        let plan = chain.plan(&[]);
        let root = tempdir();

        let rebuilt = chain.export(
            root.path(),
            &[],
            &RecordedBlob {
                inner: Lending::following(&seed, &plan),
                recorded,
                blob_held: false,
            },
        );

        assert!(rebuilt.canonicalize().unwrap() == whole.canonicalize().unwrap());
    }

    /// A batch leaves the window out on the forecast. Finalizing finds out, and
    /// stages it, on every attempt and leaving nothing behind.
    #[test]
    fn a_batch_rebuilds_a_dropped_recorded_blob() {
        let (seed, whole, recorded) = interrupted_at_two(INDEXES, 1);
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let plan = bundle.plan();

        chain.advance_to(3);

        let previous = RecordedBlob {
            inner: Lending::following(&seed, &plan),
            recorded,
            blob_held: false,
        };

        let staged = chain.stage(stage_root.path(), std::slice::from_ref(&bundle), &previous);

        assert_eq!(staged.records(Family::Indexes, 1), None);

        let staged_names = names_in(stage_root.path());

        for attempt in 1..=2 {
            let finished_root = tempdir();
            let finished = chain
                .finalize(finished_root.path(), &store, &bundle, &staged, &previous)
                .unwrap();

            assert!(
                finished.canonicalize().unwrap() == whole.canonicalize().unwrap(),
                "attempt {attempt}"
            );
            assert_eq!(
                names_in(stage_root.path()),
                staged_names,
                "attempt {attempt}"
            );
        }
    }

    /// The same for an entity window, which would otherwise go missing.
    #[test]
    fn a_batch_rebuilds_a_dropped_recorded_entity_blob() {
        let (seed_root, whole_root, root, stage_root, finished_root) =
            (tempdir(), tempdir(), tempdir(), tempdir(), tempdir());
        let seed = Chain::at(Shape::Shelley, 3).export(seed_root.path(), &[], &export::First);
        let at_four = Chain::at(Shape::Shelley, 4);
        let plan = at_four.plan(&[]);
        let whole = at_four.export(whole_root.path(), &[], &Lending::following(&seed, &plan));
        let recorded = layer_of(&whole, "entity-asset", 2).unwrap().clone();

        assert!(layer_of(&seed, "entity-asset", 2).is_none());

        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 4);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let plan = bundle.plan();

        chain.advance_to(5);

        let previous = RecordedBlob {
            inner: Lending::following(&seed, &plan),
            recorded,
            blob_held: false,
        };

        let staged = chain.stage(stage_root.path(), std::slice::from_ref(&bundle), &previous);
        let finished = chain
            .finalize(finished_root.path(), &store, &bundle, &staged, &previous)
            .unwrap();

        assert!(finished.canonicalize().unwrap() == whole.canonicalize().unwrap());
    }

    /// After the first commit and release, the rest of the batch reuses its
    /// runs, because what it wants has only shrunk.
    #[test]
    fn the_rest_of_a_batch_reuses_its_runs_after_a_commit() {
        let (root, stage_root, seed_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let seed = Chain::at(Shape::Shelley, 1).export(seed_root.path(), &[], &export::First);
        let mut chain = Chain::at(Shape::Shelley, 2);
        let mut bundles = Vec::new();

        for boundary in [2, 3, 4] {
            chain.advance_to(boundary);
            bundles.push(chain.prepare(&store, &[]).unwrap());
        }

        let lending = Lending::following(&seed, &bundles[0].plan());
        let staged = chain.stage(stage_root.path(), &bundles, &lending);
        let finished_root = tempdir();
        let first = chain
            .finalize(
                finished_root.path(),
                &store,
                &bundles[0],
                &staged,
                &Lending::following(&seed, &bundles[0].plan()),
            )
            .unwrap();

        store.release(bundles[0].sequence()).unwrap();

        let traversed = chain.archive.traversals();
        let following = Lending::following(&first, &bundles[1].plan());
        let restarted = chain.stage(stage_root.path(), &bundles[1..], &following);

        assert_eq!(chain.archive.traversals(), traversed);
        assert_eq!(restarted.report().bytes, 0);

        let (frozen_root, finished_root) = (tempdir(), tempdir());
        let plan = bundles[1].plan();
        let frozen = Chain::at(Shape::Shelley, bundles[1].sequence()).export(
            frozen_root.path(),
            &[],
            &Lending::following(&first, &plan),
        );
        let finished = chain
            .finalize(
                finished_root.path(),
                &store,
                &bundles[1],
                &restarted,
                &Lending::following(&first, &plan),
            )
            .unwrap();

        assert_same_stele(
            (frozen_root.path(), &frozen),
            (finished_root.path(), &finished),
            "after a restart",
        );
    }

    /// No commit needed: a recorded layer alone shrinks what a restart wants.
    #[test]
    fn a_restart_before_any_commit_reuses_its_runs() {
        let (seed, _, recorded) = interrupted_at_two(INDEXES, 1);
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let plan = bundle.plan();

        chain.advance_to(3);

        chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &Lending::following(&seed, &plan),
        );

        let restarted = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &RecordedBlob {
                inner: Lending::following(&seed, &plan),
                recorded,
                blob_held: true,
            },
        );

        assert_eq!(restarted.report().bytes, 0);
    }

    /// A restart wanting more than the root holds says so, in its own refusal,
    /// and the documented recovery gives the same stele.
    #[test]
    fn a_restart_wanting_more_is_refused_and_a_new_root_recovers() {
        let (seed, whole, recorded) = interrupted_at_two(INDEXES, 1);
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();
        let plan = bundle.plan();

        chain.advance_to(3);

        let previous = RecordedBlob {
            inner: Lending::following(&seed, &plan),
            recorded,
            blob_held: true,
        };

        chain.stage(stage_root.path(), std::slice::from_ref(&bundle), &previous);

        let refused = prepare::stage_batch(
            stage_root.path(),
            &chain.archive,
            &chain.state,
            std::slice::from_ref(&bundle),
            &Lending::following(&seed, &plan),
            Budget::new(u64::MAX),
            &Observer::silent(),
        );

        assert!(
            matches!(
                &refused,
                Err(Error::Staging(staging::Refusal::Uncovered { missing, .. })) if *missing == [1]
            ),
            "{refused:?}"
        );

        fs::remove_dir_all(stage_root.path()).unwrap();

        let staged = chain.stage(stage_root.path(), std::slice::from_ref(&bundle), &previous);
        let finished = chain
            .finalize(finished_root.path(), &store, &bundle, &staged, &previous)
            .unwrap();

        assert!(finished.canonicalize().unwrap() == whole.canonicalize().unwrap());
    }

    /// Runs of another frontier are another source however much they cover, and
    /// are not reported as wanting more.
    #[test]
    fn a_restart_at_another_frontier_is_another_source() {
        let (root, stage_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);
        chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        chain.advance_to(4);

        let refused = prepare::stage_batch(
            stage_root.path(),
            &chain.archive,
            &chain.state,
            std::slice::from_ref(&bundle),
            &export::First,
            Budget::new(u64::MAX),
            &Observer::silent(),
        );

        assert!(
            matches!(
                &refused,
                Err(Error::Staging(staging::Refusal::SourceMismatch { .. }))
            ),
            "{refused:?}"
        );
    }

    /// A payload unreadable after the bundle loaded is the filesystem's fault,
    /// and the variant must not send a host to `release`.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_payload_is_io() {
        use std::os::unix::fs::PermissionsExt;

        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let loaded = store.load(2).unwrap();

        for name in blobs(root.path()) {
            let path = root.path().join("blobs/sha256").join(name);

            fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        }

        let refused = chain.finalize(
            finished_root.path(),
            &store,
            &loaded,
            &staged,
            &export::First,
        );

        let refusal = preparation_refusal(refused);

        assert!(matches!(refusal, Refusal::Io(_)), "{refusal:?}");
    }

    #[test]
    fn a_vanished_payload_is_a_missing_artifact() {
        let (root, stage_root, finished_root) = (tempdir(), tempdir(), tempdir());
        let store = store(root.path());
        let mut chain = Chain::at(Shape::Shelley, 2);
        let bundle = chain.prepare(&store, &[]).unwrap();

        chain.advance_to(3);

        let staged = chain.stage(
            stage_root.path(),
            std::slice::from_ref(&bundle),
            &export::First,
        );
        let loaded = store.load(2).unwrap();

        for name in blobs(root.path()) {
            fs::remove_file(root.path().join("blobs/sha256").join(name)).unwrap();
        }

        let refused = chain.finalize(
            finished_root.path(),
            &store,
            &loaded,
            &staged,
            &export::First,
        );

        let refusal = preparation_refusal(refused);

        assert!(
            matches!(refusal, Refusal::MissingArtifact { sequence: 2, .. }),
            "{refusal:?}"
        );
    }

    /// A fault inside a capture job, after another job wrote its payload: no
    /// marker, and a retry converges. The tail's tag traversal runs only there.
    #[test]
    fn a_capture_dying_mid_write_marks_nothing_and_a_retry_completes() {
        let (root, clean_root) = (tempdir(), tempdir());
        let store = store(root.path());
        let chain = Chain::at(Shape::Shelley, 2);

        let refused = prepare::prepare(
            &store,
            &chain.plan(&[]).with_producers(export::Producers::new(
                std::num::NonZeroUsize::new(1).unwrap(),
            )),
            &Counted::failing(chain.archive.inner.clone(), Traversal::Tags, 1),
            &chain.state,
            &Request {
                store_contract: CONTRACT.to_owned(),
                mutable_slots: chain.mutable,
                tail: IndexTail::Traverse,
            },
            &Observer::silent(),
        );

        assert!(matches!(refused, Err(Error::Archive(_))), "{refused:?}");
        assert!(
            !blobs(root.path()).is_empty(),
            "the blocks job finished before the tail job died"
        );
        assert!(store.sequences().unwrap().is_empty());

        let retried = chain.prepare(&store, &[]).unwrap();
        let clean = chain.prepare(&self::store(clean_root.path()), &[]).unwrap();

        assert_eq!(
            retried.layers().collect::<Vec<_>>(),
            clean.layers().collect::<Vec<_>>()
        );
        assert_eq!(blobs(root.path()), blobs(clean_root.path()));
    }
}
