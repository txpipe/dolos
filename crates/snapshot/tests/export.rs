//! The export driver, against real store sets.
//!
//! Four properties, in increasing order of what they cost to check:
//!
//! 1. **The skeleton is pinned.** An export over an empty-but-cursored store
//!    set produces a document with no ledger content in it at all, so the
//!    golden below is a function of literals only: the network table, the
//!    `sequence`/`position`/`parameters` shapes, the order the layers are
//!    listed in, and every layer's header and scope encoding. A ledger change
//!    cannot move it, which is what makes it a golden rather than a value that
//!    gets re-pinned as a matter of routine.
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

use std::collections::BTreeMap;

use common::read_both_ways;
use dolos_cardano::{
    eras::ChainSummary, indexes::archive_dimensions, model::EraSummary,
    pallas::ledger::traverse::MultiEraBlock, EraBoundary,
};
use dolos_core::{
    builtin::{MemoryIndexStore, MemoryStateStore},
    ArchiveStore, ArchiveWriter as _, BlockSlot, ChainPoint, Domain, EntityKey, ExactRecord,
    IndexRecord, IndexStore, LogKey, Namespace, StateStore, StateWriter as _, TagRecord,
    TemporalKey,
};
use dolos_snapshot::{
    export::{self, EpochWindow, Plan},
    layers::{blocks, indexes, logs, state},
    state_layer_count, DolosProfile, Network, BLOCKS, INDEXES, LOG_KINDS, LOG_NAMESPACES,
    NAMESPACES, STATE_KINDS, UTXOS,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores};
use node::{export_to, harness, plan_for};
use stelae::{
    dir::SteleDir,
    progress::{Observer, Outcome},
    Discarding, SteleReader,
};

use watcher::Watcher;

/// The identity of an export over an empty store set at [`SKELETON_POINT`].
const GOLDEN_SKELETON: &str =
    "sha256:249fdf27139b4607e60668a3ab5002d7bfd1c6872da426e440b1bf42c9b43e2f";

/// The chain point the skeleton fixture stands at: mid-epoch-2 under
/// [`skeleton_summary`], so the export covers three epochs and the last window
/// is clamped to the cursor.
const SKELETON_SLOT: BlockSlot = 250;

// --------------------------------------------------------------------------
// 1. The pinned skeleton
// --------------------------------------------------------------------------

/// One era, hundred-slot epochs from slot zero. Literal, so nothing about the
/// golden depends on a genesis file.
fn skeleton_summary() -> ChainSummary {
    let mut chain = ChainSummary::default();

    chain.append_era(
        6,
        EraSummary {
            start: EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 0,
            },
            end: None,
            epoch_length: 100,
            slot_length: 1,
            protocol: 6,
        },
    );

    chain
}

fn skeleton_point() -> ChainPoint {
    ChainPoint::Specific(SKELETON_SLOT, dolos_core::BlockHash::new([0x0b; 32]))
}

/// An archive, state and index store holding nothing but a cursor.
fn empty_stores() -> (
    dolos_redb3::archive::ArchiveStore,
    MemoryStateStore,
    MemoryIndexStore,
) {
    let archive =
        dolos_redb3::archive::ArchiveStore::in_memory(dolos_cardano::model::build_schema())
            .unwrap();

    let state = MemoryStateStore::new();
    let writer = state.start_writer().unwrap();
    writer.set_cursor(skeleton_point()).unwrap();
    writer.commit().unwrap();

    (archive, state, MemoryIndexStore::new())
}

/// Done criterion 4.
///
/// Every value in the pinned document below is determined by this crate and
/// ADR-004 together. The layers carry only their header records, so their
/// `diffId`s freeze the profile name, the kind strings and the scope CBOR — and
/// nothing about what a ledger happens to hold. A failure here means published
/// identity moved.
#[test]
fn an_empty_store_set_exports_the_pinned_skeleton() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let (archive, state, index) = empty_stores();

    let plan = Plan::new(
        &skeleton_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        skeleton_point(),
    )
    .unwrap();

    let inscription = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        &index,
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
/// The split traded an all-namespace walk for a closed list of six, and this is
/// the price of that trade paid up front. Written straight into the archive
/// rather than through a ledger phase, because no ledger phase writes one today
/// — the point is what happens on the day one does.
#[test]
fn a_log_under_an_uncovered_namespace_fails_the_export() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let (archive, state, index) = empty_stores();

    let stray = NAMESPACES
        .into_iter()
        .find(|ns| *ns != UTXOS && !LOG_NAMESPACES.contains(ns))
        .expect("a state namespace that carries no logs");

    let writer = archive.start_writer().unwrap();
    writer
        .write_log(stray, &LogKey::from(TemporalKey::from(120u64)), &vec![0xa0])
        .unwrap();
    writer.commit().unwrap();

    let plan = Plan::new(
        &skeleton_summary(),
        Network::for_magic(dolos_snapshot::MAINNET_MAGIC),
        skeleton_point(),
    )
    .unwrap();

    let err = export::export(
        &stele,
        &plan,
        &archive,
        &state,
        &index,
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
        .chain(STATE_KINDS.into_iter().map(|(kind, _, _)| kind))
    {
        assert!(
            inscription.layers_of_kind(kind).next().is_some(),
            "no {kind} layer"
        );
    }

    // Done criterion 3, from the side a real ledger shows it: the harness seeds
    // `epochs` and `member-rewards` and nothing else, so exactly those two log
    // kinds travel and the other four are absent — not present and empty.
    let carried: Vec<&str> = LOG_KINDS
        .into_iter()
        .filter(|(kind, _)| inscription.layers_of_kind(kind).next().is_some())
        .map(|(kind, _)| kind)
        .collect();

    assert_eq!(carried, ["log-epochs", "log-member-rewards"]);
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
                .map(|raw| logs::decode(raw).unwrap())
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

    for (kind, ns, shards) in STATE_KINDS {
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
                .filter(|record| state::shard_of(&record.key, shards) as usize == shard)
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
    let slots = window.slots();
    let range =
        LogKey::from(TemporalKey::from(slots.start))..LogKey::from(TemporalKey::from(slots.end));

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
        .indexes()
        .iter_archive_tags(&archive_dimensions::ALL, slots.clone())
        .unwrap()
        .map(Result::unwrap)
        .collect();

    tags.sort();

    let mut exact: Vec<ExactRecord> = domain
        .indexes()
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
            .iter_entities(ns, EntityKey::full_range())
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
/// The two domains differ in exactly one thing: the state and index stores are
/// the builtin in-memory pair in one and the on-disk fjall pair in the other.
/// The archive is redb in both, because there is no second archive backend to
/// compare against. Same genesis, same blocks, same seeding — so the same
/// inscription, byte for byte, or the format does not mean what it claims.
#[test]
fn both_backends_publish_the_same_inscription() {
    let memory: ToyDomain<MemoryStores> = harness();
    let fjall: ToyDomain<FjallStores> = harness();

    assert_eq!(
        memory.state().read_cursor().unwrap(),
        fjall.state().read_cursor().unwrap(),
        "the two domains did not end up at the same chain point"
    );

    let memory_root = tempfile::tempdir().unwrap();
    let fjall_root = tempfile::tempdir().unwrap();

    let from_memory = export_to(memory_root.path(), &memory);
    let from_fjall = export_to(fjall_root.path(), &fjall);

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

/// The whole skeleton document, so a change to a key spelling, a scope shape or
/// the order layers are listed in reads as text in the diff rather than only as
/// a moved hash.
///
/// JCS sorts object keys, so the layout is the protocol's; every string in it
/// is this profile's.
const CANONICAL_SKELETON: &str = concat!(
    r#"{"compression":{"algo":"zstd","level":9},"history":[],"layers":["#,
    r#"{"diffId":"sha256:922ee4d9b71402ecba461e7b11a76ff61354d2a0be45bb040e5e2ccb80ccb5a1","kind":"blocks","mediaType":"application/vnd.dolos.stele.blocks.v1+zstd","records":1,"scope":{"endSlot":99,"epoch":0,"startSlot":0},"uncompressedSize":43},"#,
    r#"{"diffId":"sha256:67f548e9704915048e1c1f4316442c3b0d8b8fb7f12032137cba337143fd9cd2","kind":"blocks","mediaType":"application/vnd.dolos.stele.blocks.v1+zstd","records":1,"scope":{"endSlot":199,"epoch":1,"startSlot":100},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:635c9151dfbe340e7f5e8589f1651951ad9dd9b79583b6c90cdce99266899c5c","kind":"blocks","mediaType":"application/vnd.dolos.stele.blocks.v1+zstd","records":1,"scope":{"endSlot":250,"epoch":2,"startSlot":200},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:0517f93e76f9fe6059bcde6218af5c90f51020af09890c5e3a05cc8f3a32e447","kind":"indexes","mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":1,"scope":{"endSlot":99,"epoch":0,"startSlot":0},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:b64ae324280c4335089cdf9918ac0596fe7f429dbf5a54c2fd2d44f25280b935","kind":"indexes","mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":1,"scope":{"endSlot":199,"epoch":1,"startSlot":100},"uncompressedSize":45},"#,
    r#"{"diffId":"sha256:a76e2bb47934f8473a232a31da4930db8d50ce90a4cab2c232fc032644fff62e","kind":"indexes","mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":1,"scope":{"endSlot":250,"epoch":2,"startSlot":200},"uncompressedSize":45},"#,
    r#"{"diffId":"sha256:fabc518edf099744b9871e87076bbf4bc0e4d308f3212983d62187070dc84c80","kind":"state-account-epochs","mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":55},"#,
    r#"{"diffId":"sha256:318ad5ac29478218eaf3d04f8dfe93fc86834f2f7eca108817e811a603f09a8f","kind":"state-account-stakes","mediaType":"application/vnd.dolos.stele.state-account-stakes.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":55},"#,
    r#"{"diffId":"sha256:02522facba4552263d4dfaded9dbb82ec8a90937284a15539f6f1f7a7a9ef4fd","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:9c3e2a317428abc07cd8a29934c10d6ed406eef31b0ffebbaa95679ed11a4e89","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:bad9e2528c56aab15c5c593550f17bd850d52804dd17172fefb4b11940641d8d","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:9d296077954a6e1a79ec9139edc2b73c4f292999e697f99e688887045a1ab1e2","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:36a70b77ff0e7ff67d3e42a4dbf9aafc7c3e9e093d2fa0a746253f5d693b5362","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:48ddd612a9127f81231cd901135c791bc9b812bb54ee95d9e3544962e1b3a46c","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:4cb5ff0a33d2e88172898f0565fb91d0c3c0e3f636daff1ec0ab50c2da7a8dd1","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:0eb957c6d2d2b2ae787539fafacfccdd9a2aaad410d5af096fed5b60e5d30967","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:b3f376aa124ed9110f4d6ee6a30848bc8cbec2eaec65020f39439849d75e304e","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:96b86a20d7ba613efa3e1925148b2a9ccaa969c75ca4dc466434a46e0fe6a82f","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:462a4295f3c6ee4d43b2c201d166a225eb59d55e4880e66fe3b947d005a48132","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:87a29f9edd16dc7521df050f8645b99082df697fa42e772e262a61a5b756497a","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:b478aa2b0c1385dfeba5f26da0edc223260f0cac947d91da6d1dc629d48a7541","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:a2c9fa81aec9cac32b36fd325bdf3afe6ac72a95c7412cfaff6f05288354b085","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:5cf477db9c26c4f251c836844db2405f37fa789d03aea868540eb07cdf43bdfe","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:964771e3ed841725948305ad74029c7be92b0aad4e6c43a7569fb09b8e67253f","kind":"state-accounts","mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:6b217ee2a4173c3446c8ea31bc84d8c59f1c0b2eb583439f443322ada7dfb5a1","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:833ffb5dbd39bb5eaacf1c2439de47fdd31ad3a100c2ec50f6f1100eeee0801a","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:321c3b09a833a633a61fc7af4e6df23e3f3adfd3f08a11505efcf9069d9da8e2","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:f68da008f436dbac4425da5b7c582a35ea077559312e128755b0eef2899c1f12","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:9f6c54e7404ddd5d37db028eef00fe943dc5a29746f5af17fb9ed022f1c4489f","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:6e87f3f58d31f08051f7c7bdb404767203d56f699f6f05129921017328390946","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:adf44192f4f91e651b43a841fbc81ee74c0bf42bac15c059bcd4e9277ef9a08f","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:542b315989aa1e5f5c8a3fcf2bee5750b9de87ecf63e19aebb1544c5c303edbc","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:ec60a65290505b4351128d7e5676376f691656b9080fb4a9ade9dce8e3a760e7","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:68ef06144365168b8a36c352a3e1d26dcde9029a2857be937c01cda5906172ae","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:ddb6a5961aade95fb820a669d97dcf116021a1b54d8a0d59d4820849b922f286","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:9049a9631a589437e0dc2005552e44cbf414915ae25970d37dd78452162de6ac","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:403ac5c18e878ce6193b5697d754100267961e9a13d7dd5920a741c6a861de00","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:21660b8e2e4fadede65e89589ca10fd9aa467dd0845a7487ada048545d889cdc","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:7bf0d679da18a075420267ce16f38cfefb68efec28b5a74e8c1b7be9fed86654","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:8899bed13545893de75851c8537b383608ea92b29e9beeca19c6ff8f6e64109a","kind":"state-assets","mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:5c79490ba5bd0febb78a297aa3273fff7119c9af37fa48a8fba01b3c4e93ba1a","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:128bba0aeae290cbe00284557a591198676a94dd8c948c5c5b8bf78deb892978","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:a1f52ea5164adaecaa2cdb1e306772afe5209fbe81d8ab1bcd66badfeb5238d5","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:468198f05873a9c1c748e6ed213fa0307b7bd6f1baf3784ec3ec765ca3b03383","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:f553bf184e843725c49e9645560876519f9e6994f2bf468efd90150a58ad45fb","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:0cd44b79cc2336ba42d9df42979c9145e391f98b028407e9895e24ceae188e72","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:99855d833d23137d927485ebe6a610b0640bafa7b89e8eceaf5b57754e75927b","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:3325d1e65ccc52abede1ba2bd35f4289af41afbd8f1ff3007c659fe12fb85e00","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:f8b305e7a74ddd9b49f36dc0af339fd1fd0bb600e7a1c86c99c3ad46e1b61fac","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:151817f8cc62d02d9bb9c7980df48240776c1b66f759ee6665057d3ee0056fbb","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:4775430b2bdf697c94bb53d77b304e6ddb7a16b4a51d533bf21915d1ac96c5df","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:df563c54b5d255acc3358a33271f0816673cb8e9ce2e3c29960f55e60d25baf5","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:b4945645a766617aaf3bcfbc09363b107c507455dcc4482e31c42b2a51fcb7a0","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:8140e32dd30b763b9e30d4ec9e3f4c4839674271068028c0eb280acf95fb6803","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:2ac9b61903e09ba2a01e2192cd9e6a1c0ecb30886d3752374cf0d45dfdc3fbda","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:d1f06100a97cbf233453ee235ae2af84fed2ebc9afba1ef44f1a560fa9092517","kind":"state-datums","mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:0c02e395163b8721766f4e70ade836dcbeabcb5b660e6538caee76757315305e","kind":"state-dreps","mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:d2956a7d3251484d5ef8f45cd923e34ecbc890552eac579c500ba99b0a98cb9a","kind":"state-epochs","mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:a53fdfbe13f40703e1e8cd661ddc5ba5bb54541b22bf7cdcb3b7aa9697fd9cb6","kind":"state-eras","mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":45},"#,
    r#"{"diffId":"sha256:0a74ea019634f7c0135b1030e692d4e3680ff534c89462819fa04c15ada63114","kind":"state-gov","mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":44},"#,
    r#"{"diffId":"sha256:87ed310e3fefe592d1f45dde73677bf2de5642b4f9d64363b64debe6999d07a5","kind":"state-leader-rewards","mediaType":"application/vnd.dolos.stele.state-leader-rewards.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":55},"#,
    r#"{"diffId":"sha256:22a28d8b6fdb0add20994fda1401bfe694e50c79886d5c4517eac96ddb94bc0b","kind":"state-member-rewards","mediaType":"application/vnd.dolos.stele.state-member-rewards.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":55},"#,
    r#"{"diffId":"sha256:2e07a341e23cf37667da069d3e75ba7a7bf2665572f2183353ff0266369cf17f","kind":"state-pending-mirs","mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":53},"#,
    r#"{"diffId":"sha256:ad5994d901c69e757457b2ad6bac6a2679e45ae25769cc616375c10c8c6dc582","kind":"state-pending-rewards","mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":56},"#,
    r#"{"diffId":"sha256:1d1a0547655e532c3c52d29fab6fcd82e7a3ef3fed1d0c2a9e670f269384ba2b","kind":"state-pool-deposit-refunds","mediaType":"application/vnd.dolos.stele.state-pool-deposit-refunds.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":62},"#,
    r#"{"diffId":"sha256:da799488bcc20ea1d8c8e236169d7204e328b34f806075a951f266819bd35e34","kind":"state-pools","mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:640c0f9db84510212ae0d71860df7c518244db6eed6728e9fcc6540d99bdcbe7","kind":"state-proposals","mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":50},"#,
    r#"{"diffId":"sha256:c8517773130f10b7dfb51ca3415ccbe9005f1b84abada0418f46bee7dc7d095f","kind":"state-stakes","mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":47},"#,
    r#"{"diffId":"sha256:6978ebcdd1accb802d0711afbe6c25747eea28f533de050c184ea24bade637a5","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:aa056a5c4a03811c52492eec692ba0612f7f32f9c447ddd63f64a4c7b1c5ed4a","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:c0055cfef082a8ebe5ada0637de774cc2613d00714d05f74b19690ced71e89c2","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:5b298cc5852e2c25e9bccfcfcc7c379e68b55439b34ecdbbd26fb93d5306dbfd","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:09081604a9f2fc740fdeabb3a331ff3d35cdde0179a7a95271a2b43079d5962b","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:86e95890bde0bf9230b48d9ff30d0e37b3e4441f041046d9674d3aecc2381284","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:e6fe3b69728c8f4587689733e537d593ba4901e175d1b81f583ad6fe3f30b48a","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:9fc971b8fe01687d280947d27b4b495b8a13b11696b61379c81701db821e1d12","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:a3ddecb338e823bff9f3269571ae9f8858451ca8f98700501d57273ac141ec01","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:20c25aaf590428fd30f7ff2e17cdf8b12e2d4819dbdbd27cd36b2e3a59c79bf1","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:cd57e7f5986a2e811577422b7a174d02a786ea75c79b855a547292aaa3cb9801","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:ac1e3bdff176deca129ff6ea85f8e32d98c56eb55845fbbb535e145817c2f27c","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:1b257f003b61fd4389b580e368725779e84d0931916f4e5f21336e3b5775462f","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:4145245584090ba5ec600cc41743036cd02580999490cd38e94ccb47e713f529","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:fa3101937f3491ae614180384787cfeb1d2ffb7e522c4c731b60208472573917","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":46},"#,
    r#"{"diffId":"sha256:e59d8b7ec7144216a9caab188b2de8d09d98c7c419e228a41131722796b81711","kind":"state-utxos","mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":46}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-epochs":1,"account-stakes":1,"accounts":1,"assets":1,"datums":1,"dreps":1,"epochs":2,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":1},"#,
    r#""shards":{"account-epochs":1,"account-stakes":1,"accounts":16,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"gov":1,"leader-rewards":1,"member-rewards":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":1,"pools":1,"proposals":1,"stakes":1,"utxos":16}"#,
    r#"},"position":{"epoch":2,"network":{"magic":764824073,"name":"mainnet"},"point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":250}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":2}"#,
);

// --------------------------------------------------------------------------
// 5. A stele reproduced without being stored
// --------------------------------------------------------------------------

/// The check that the discarding writer is *faithful* rather than merely fast,
/// and no other check stands in for it.
///
/// Two exports over one store set: one into a directory, which is
/// `snapshot publish --output-dir`, and one into nothing, which is
/// `snapshot digest`. The comparison is on the canonical bytes rather than on
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
        domain.indexes(),
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
    let plan = plan_for(&domain).restrict_epochs(Some(1), None);

    assert!(
        plan.epochs.is_empty(),
        "the harness ledger lives in epoch zero; selecting above it selects nothing"
    );

    let temp = tempfile::tempdir().unwrap();

    let stored = export::publish(
        temp.path(),
        &plan,
        domain.archive(),
        domain.state(),
        domain.indexes(),
        None,
        &Observer::silent(),
    )
    .unwrap();

    let reproduced = export::export(
        &Discarding,
        &plan,
        domain.archive(),
        domain.state(),
        domain.indexes(),
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
        domain.indexes(),
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
        domain.indexes(),
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
        domain.indexes(),
        None,
        &watcher.observer(),
    )
    .unwrap();

    let without = export::publish(
        silent.path(),
        &plan,
        domain.archive(),
        domain.state(),
        domain.indexes(),
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
