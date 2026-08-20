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

use common::read_both_ways;
use dolos_cardano::{
    eras::ChainSummary, indexes::archive_dimensions, model::EraSummary,
    pallas::ledger::traverse::MultiEraBlock, EraBoundary,
};
use dolos_core::{
    builtin::{MemoryIndexStore, MemoryStateStore},
    ArchiveStore, ArchiveWriter as _, BlockSlot, ChainPoint, Domain, EntityKey, ExactRecord,
    IndexRecord, IndexStore, LogKey, StateStore, StateWriter as _, TagRecord, TemporalKey,
};
use dolos_snapshot::{
    export::{self, EpochWindow, Plan},
    layers::{blocks, indexes, logs, state},
    DolosProfile, Network, BLOCKS, INDEXES, LOG_KINDS, LOG_NAMESPACES, NAMESPACES, STATE,
    STATE_SHARDS, UTXOS,
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
    "sha256:6eecdf3b910983cc559f35df6246a2920b11fe442a6297d1e673224e25e24dba";

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

    // Three epochs of blocks and indexes, then sixteen state shards. No
    // `digests` layer: nothing supplies one — and **no log layers at all**,
    // which is the omit-if-empty rule seen from the other side: a store with no
    // logs in it publishes no layer claiming there are none.
    assert_eq!(inscription.layers.len(), 3 * 2 + STATE_SHARDS as usize);

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

    // Every kind but `digests`, which has no source in this slice.
    for kind in [BLOCKS, INDEXES, STATE] {
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
    let written: Vec<_> = inscription.layers_of_kind(STATE).collect();

    // Sixteen, always: an empty shard is still a shard, so a client planning a
    // selective fetch never has to discover the count from the data.
    assert_eq!(written.len(), STATE_SHARDS as usize);

    let expected = expected_state(&domain);

    for (shard, descriptor) in written.iter().enumerate() {
        assert_eq!(descriptor.scope["shard"], shard);

        let found: Vec<state::StateRecord> = read_both_ways(&stele, &index, descriptor)
            .iter()
            .map(|raw| state::decode(raw).unwrap())
            .collect();

        let expected: Vec<state::StateRecord> = expected
            .iter()
            .filter(|record| record.shard() as usize == shard)
            .cloned()
            .collect();

        assert_eq!(found, expected, "shard {shard}");
    }

    assert!(
        !expected.is_empty(),
        "the fixture produced no state, so the shards prove nothing"
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

fn expected_state<B: ToyStores>(domain: &ToyDomain<B>) -> Vec<state::StateRecord> {
    let mut found = Vec::new();

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
            found.push(state::entity(ns, &key, &value).unwrap());
        }
    }

    for entry in domain.state().iter_utxos().unwrap() {
        let (txo, value) = entry.unwrap();
        found.push(state::utxo(&txo, &value).unwrap());
    }

    found.sort_by(|a, b| (a.ns, &a.key).cmp(&(b.ns, &b.key)));

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
    r#"{"diffId":"sha256:c1d65578a9da3b2453c50a8958bcdf426a59b22873012e51409e8acb732822cc","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":0},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:21cb592e80427ce7a6c8a24c0f5d5cf3c51da28a61325f47ffcb900ca629ad8d","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":1},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:bf5d5484e7def2851a425f266836c1fc0bfdd67db35acad39a5b488742e32855","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":2},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:f6b34bef2565e75cef039500c892c07b0e73aa258afe8885c9abdd9537e5e83b","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":3},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:69a78d4b3460bbdd65910ad985086afc82fb22c4158ce62b19b65dbc74eb0e73","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":4},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:1dd622715f2275dd3a1bc20b34a825ed4fe1c790ea268e0812918e72c4b48856","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":5},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:367f3f93a2f78522a1d29bd8a70632c2983c9a47fd107b55948c88cf62bb89f4","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":6},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:0a54558b493112b15ee2e5061021cc8d0e2d5211a550e7bfcae66a597f2771ca","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":7},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:292f3cd8d6f7ea605d33c16f60ca16b0a3bf8322df4c79a561b2b2d96a1df066","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":8},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:0cf3c2340b708a503a5e4301c19f6568599a8b15238d18805b0e26df58293f66","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":9},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:e4c0d0e60343333a0e78fddd8cb1885cc05f386065c36f8a58e3ec2ff52ce6cf","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":10},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:242048cec84854de49abdb62dd42d1b186b243a7095f05ba95b1862a5cf5e907","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":11},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:3825bee5e3bcf04483294f9cb40732f6a66169e222bb2da9cd294a3befd4fbcb","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":12},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:e6efd8da5e50463a9380ec4600741407ea84ac2baac44917707bb137d1b119d2","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":13},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:1953096685ae64ae6f5d1bbc04d8d3678fbb567138809f581bdcdacfa1e1b90b","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":14},"uncompressedSize":40},"#,
    r#"{"diffId":"sha256:0eb6fed603f60e527eb9622e04d72a74c21dcd7ee88e98b95ae6f8895736d5fd","kind":"state","mediaType":"application/vnd.dolos.stele.state.v1+zstd","records":1,"scope":{"shard":15},"uncompressedSize":40}"#,
    r#"],"parameters":{"indexKeyHash":"xxh3-64","stateShards":16},"position":{"epoch":2,"network":{"magic":764824073,"name":"mainnet"},"point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":250}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":2}"#,
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
    assert_eq!(stored.layers.len(), STATE_SHARDS as usize);

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
