//! Restoring a node from a stele in a registry.
//!
//! Everything here needs a **registry**, and spawns one: `docker run` of an OCI
//! Distribution server, torn down on the way out. So everything here is
//! `#[ignore]`d, and an `#[ignore]`d test that was never executed proves
//! nothing — run it with:
//!
//! ```text
//! cargo test -p dolos-snapshot --features oci --test restore_registry -- --ignored --nocapture
//! ```
//!
//! `STELAE_TEST_REGISTRY_IMAGE` chooses the server (default `registry:2`), the
//! same knob `tests/publish.rs` and `crates/stelae/tests/oci.rs` use.
//!
//! ## The four properties
//!
//! 1. **A registry restore is a directory restore.** The same stele, published
//!    both ways and restored both ways, produces store sets that are equal —
//!    cursors, entities, the whole UTxO set, the archive, index records and the
//!    live-UTxO tag queries. The transport is where the bytes come from and
//!    nothing else, and this is the assertion that says so.
//! 2. **A killed restore resumes over the wire**, refetching only what it had
//!    not committed. Counted by the driver, not inferred from a duration.
//! 3. **A node pre-seeded with epochs fetches only what it lacks** when a newer
//!    stele arrives — ADR-004's second delta assertion for this phase. Over a
//!    registry, so what is *not* refetched is genuinely not downloaded.
//! 4. **A point names a stele.** `epoch-N` resolves to that sequence and
//!    `latest` to the newest, which is what makes a repository holding a
//!    history restorable at any of them.
//! 5. **Credentials are what opens the repository.** The fixture's registry
//!    demands Basic credentials, and every restore above reaches it through
//!    `registry::open` carrying them — so the four properties are all evidence
//!    for this fifth. The test named for it states the same thing directly,
//!    from both sides. Where a node's credentials *come from* is the `dolos`
//!    binary's decision and is tested there.
//!
//! ## Why the interruption is a layer boundary
//!
//! The same discipline `tests/restore.rs` states: the reader refuses a `diffId`
//! the test named, so the interruption lands where the test meant it to. A kill
//! after a wall-clock delay would sometimes interrupt nothing over a loopback
//! registry — the fixture stele is kilobytes — and pass for the wrong reason.

#![cfg(feature = "oci")]

mod node;
mod registry_fixture;
mod watcher;

use dolos_cardano::indexes::{archive_dimensions, index_delta_from_utxo_delta};
use dolos_core::{
    ArchiveStore, BlockHash, ChainPoint, Domain as _, EntityKey, EraCbor, ExactRecord, IndexStore,
    LogKey, StateStore, TagRecord, TxoRef, UtxoSet, UtxoSetDelta,
};
use dolos_snapshot::{
    export::Plan,
    registry::{self, Point},
    restore::{self, Budget, Checkpoint},
    Error, Network, NAMESPACES, STATE_SHARDS, UTXOS,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain, ToyStores};
use node::{harness, Blank};
use registry_fixture::Fixture;
use stelae::{
    frame::Limits,
    inscription::LayerDescriptor,
    oci::{Auth, Registry, Stele},
    plan::RestoreProgress,
    progress::{Observer, Outcome},
    transport::BlobIndex,
    Digest, LayerReader, Profile, SteleReader,
};

use watcher::Watcher;

/// Layers a publish of one epoch writes: three epoch kinds plus the state tip.
const PER_PUBLISH: usize = 3 + STATE_SHARDS as usize;

// ---------------------------------------------------------------------------
// The node, and the two chain points it publishes from
// ---------------------------------------------------------------------------

/// The harness ledger and the two plans it publishes.
///
/// The same shape `tests/publish.rs` uses and for the same reason: the first
/// cursor sits on the epoch-0 boundary so epoch 0's window is unclamped, and
/// the second one slot past it, so the two steles share epoch 0's layers
/// exactly and differ in their tips. That is the shape the resume rule is
/// about.
struct Node {
    domain: ToyDomain<MemoryStores>,
    magic: u64,
    first: Plan,
    second: Plan,
}

impl Node {
    fn build() -> Self {
        let domain = harness::<MemoryStores>();

        let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();

        let magic = u64::from(domain.genesis().network_magic());
        let network = Network::for_magic(magic);
        let boundary = summary.epoch_start(1);

        let point = |slot| ChainPoint::Specific(slot, BlockHash::new([0xab; 32]));

        let first = Plan::new(&summary, network.clone(), point(boundary - 1)).unwrap();
        let second = Plan::new(&summary, network, point(boundary)).unwrap();

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);

        Self {
            domain,
            magic,
            first,
            second,
        }
    }

    fn publish(&self, repository: &Registry, plan: &Plan) {
        registry::publish(
            registry::Publishing::new(repository),
            plan,
            self.domain.archive(),
            self.domain.state(),
            self.domain.indexes(),
            None,
            &Observer::silent(),
        )
        .unwrap();
    }
}

/// Restore from a repository into `blank`, checkpointing into `storage`.
fn restore_from(
    repository: &Registry,
    point: Point,
    storage: &std::path::Path,
    magic: u64,
    blank: &Blank<MemoryStores>,
    resume: bool,
) -> Result<restore::Summary, Error> {
    restore_watched(
        repository,
        point,
        storage,
        magic,
        blank,
        resume,
        &Observer::silent(),
    )
    .map(|(_, _, summary)| summary)
}

/// The same restore, with somebody listening, and the outlook the summary alone
/// does not carry.
///
/// Separate so every suite above stays exactly as it was: an observer is meant
/// to change nothing but what is said.
#[allow(clippy::too_many_arguments)]
fn restore_watched(
    repository: &Registry,
    point: Point,
    storage: &std::path::Path,
    magic: u64,
    blank: &Blank<MemoryStores>,
    resume: bool,
    observer: &Observer,
) -> Result<(restore::Plan, restore::Outlook, restore::Summary), Error> {
    registry::restore_registry(
        repository,
        point,
        restoring(storage, magic, resume),
        target(blank),
        observer,
    )
}

/// What the node reading a stele knows about itself.
fn restoring(storage: &std::path::Path, magic: u64, resume: bool) -> restore::Restoring<'_> {
    restore::Restoring {
        network_magic: magic,
        max_history: None,
        storage_path: storage,
        resume,
    }
}

/// Where a restore writes, for a blank store set.
fn target<B: ToyStores>(
    blank: &Blank<B>,
) -> restore::Target<'_, impl ArchiveStore, B::State, B::Indexes> {
    restore::Target::new(&blank.archive, blank.state(), blank.indexes())
}

// ---------------------------------------------------------------------------
// Done criterion 1
// ---------------------------------------------------------------------------

/// A stele restored from a registry is the stele restored from a directory.
///
/// The comparison the local slice established, now across the transport
/// boundary: the same node published both ways, restored both ways, and the two
/// store sets held against each other rather than each against the original.
/// That is the stronger of the two available checks — a transport bug that
/// happened to reproduce the export's own mistake would survive a comparison
/// against the export and not this one.
#[test]
#[ignore]
fn a_registry_restore_is_a_directory_restore() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let repository = fixture.repository("dolos/restore");
    node.publish(&repository, &node.first);

    // The same stele, written to a directory.
    let dir = tempfile::tempdir().unwrap();
    let from_dir = Blank::<MemoryStores>::open();

    {
        let stele = stelae::dir::SteleDir::create(dir.path()).unwrap();

        dolos_snapshot::export::export(
            &stele,
            &node.first,
            node.domain.archive(),
            node.domain.state(),
            node.domain.indexes(),
            None,
            &dolos_snapshot::export::First,
            &Observer::silent(),
        )
        .unwrap();
    }

    let dir_storage = tempfile::tempdir().unwrap();

    let (_, _, by_dir) = restore::restore_dir(
        dir.path(),
        restoring(dir_storage.path(), node.magic, false),
        target(&from_dir),
        &Observer::silent(),
    )
    .unwrap();

    // And out of the registry.
    let from_registry = Blank::<MemoryStores>::open();
    let registry_storage = tempfile::tempdir().unwrap();

    let by_registry = restore_from(
        &repository,
        Point::Latest,
        registry_storage.path(),
        node.magic,
        &from_registry,
        false,
    )
    .unwrap();

    assert_eq!(
        by_registry, by_dir,
        "the two transports restored different amounts of the same stele"
    );

    assert_eq!(by_registry.layers_fetched, PER_PUBLISH);
    assert_eq!(by_registry.layers_skipped, 0);

    assert_stores_match(&from_registry, &from_dir);

    // And the records are the published node's, not merely each other's — the
    // check that both transports did not reproduce one wrong thing.
    //
    // The *cursor* is deliberately not in this comparison. `Node::build` stands
    // at a synthetic epoch boundary so that epoch 0's window publishes
    // unclamped, which is a chain point the harness ledger never reached; the
    // restored node carries the stele's position, correctly, and that is not the
    // domain's. What a stele has to reproduce is the data, and that is what is
    // asserted.
    assert_eq!(
        utxos_of(from_registry.state()),
        utxos_of(node.domain.state()),
        "the utxo set the node published"
    );

    assert_eq!(
        blocks_of(&from_registry.archive),
        blocks_of(node.domain.archive()),
        "the blocks the node published"
    );

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        assert_eq!(
            entities_of(from_registry.state(), ns),
            entities_of(node.domain.state(), ns),
            "entities under {ns}"
        );
    }

    eprintln!(
        "registry restore: {} layers, {} blocks, {} utxos, {} entities — equal to the directory \
         restore",
        by_registry.layers_fetched, by_registry.blocks, by_registry.utxos, by_registry.entities,
    );
}

// ---------------------------------------------------------------------------
// Done criterion 2
// ---------------------------------------------------------------------------

/// Killed over the wire, resumed over the wire.
#[test]
#[ignore]
fn a_killed_registry_restore_resumes_where_it_stopped() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let repository = fixture.repository("dolos/resume");
    node.publish(&repository, &node.first);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    let stele = Point::Latest.pull(&repository).unwrap();
    let identity = stele.read_inscription().unwrap().digest().unwrap();
    let plan = restore::plan(&stele, node.magic, None).unwrap();
    let index = stele.blob_index().unwrap();

    let epoch_layers: Vec<Digest> = plan.immutable_layers().map(|l| l.diff_id).collect();
    assert!(epoch_layers.len() >= 2);

    // The interruption, at the second epoch layer the driver reaches.
    let mut checkpoint = Checkpoint::open(storage.path(), identity, false).unwrap();

    let err = restore::restore(
        &Interrupted {
            inner: &stele,
            stop_at: epoch_layers[1],
        },
        &index,
        &plan,
        target(&blank),
        Budget::default(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(err, Error::Stelae(stelae::Error::Io(_))),
        "{err:?}"
    );

    let progress = RestoreProgress::load(&Checkpoint::path_in(storage.path()))
        .unwrap()
        .expect("a killed restore left no progress file");

    assert_eq!(progress.completed, [epoch_layers[0]].into_iter().collect());

    assert!(
        blank.state().read_cursor().unwrap().is_none(),
        "an interrupted restore left a cursor behind"
    );

    // The resume: a fresh pull, a fresh transport, the progress file the only
    // thing carried between them — which is the situation after a reboot.
    let resumed = restore_from(
        &fixture.repository("dolos/resume"),
        Point::Latest,
        storage.path(),
        node.magic,
        &blank,
        true,
    )
    .unwrap();

    assert_eq!(resumed.layers_skipped, 1);
    assert_eq!(resumed.layers_fetched, PER_PUBLISH - 1);

    assert_eq!(
        RestoreProgress::load(&Checkpoint::path_in(storage.path())).unwrap(),
        None,
        "a finished restore left its progress file behind"
    );

    // The node the uninterrupted run would have produced.
    let uninterrupted = Blank::<MemoryStores>::open();
    let clean_storage = tempfile::tempdir().unwrap();

    restore_from(
        &fixture.repository("dolos/resume"),
        Point::Latest,
        clean_storage.path(),
        node.magic,
        &uninterrupted,
        false,
    )
    .unwrap();

    assert_stores_match(&blank, &uninterrupted);

    eprintln!(
        "resumed: {} layers fetched, {} skipped (of {PER_PUBLISH}) — and the same node",
        resumed.layers_fetched, resumed.layers_skipped,
    );
}

// ---------------------------------------------------------------------------
// Done criteria 3 and 4
// ---------------------------------------------------------------------------

/// A node that already holds epochs fetches only the layers it lacks, and a
/// point names which stele it is catching up to.
///
/// The repository holds two steles. A restore of the first is interrupted at
/// its tip, so the node holds epoch 0's layers and no cursor. Resuming against
/// the *second* — a newer inscription, a longer history, a different tip —
/// keeps epoch 0's layers and downloads epoch 1's plus the sixteen shards. That
/// is the resume rule and the delta assertion in one run, which is what they
/// are.
#[test]
#[ignore]
fn a_pre_seeded_node_fetches_only_what_it_lacks() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let repository = fixture.repository("dolos/delta");
    node.publish(&repository, &node.first);
    node.publish(&fixture.repository("dolos/delta"), &node.second);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // Sequence 1, interrupted at its first state shard: every epoch layer it
    // carries commits, the tip does not.
    let first = Point::Epoch(1).pull(&repository).unwrap();
    let identity = first.read_inscription().unwrap().digest().unwrap();
    let plan = restore::plan(&first, node.magic, None).unwrap();
    let index = first.blob_index().unwrap();

    let shards: Vec<Digest> = plan.tip_layers().map(|l| l.diff_id).collect();
    let epoch_layers = plan.immutable_layers().count();

    let mut checkpoint = Checkpoint::open(storage.path(), identity, false).unwrap();

    restore::restore(
        &Interrupted {
            inner: &first,
            stop_at: shards[0],
        },
        &index,
        &plan,
        target(&blank),
        Budget::default(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap_err();

    let seeded = RestoreProgress::load(&Checkpoint::path_in(storage.path()))
        .unwrap()
        .unwrap()
        .completed
        .len();

    assert_eq!(seeded, epoch_layers, "epoch 0's layers all committed");

    // Now catch up to sequence 2 — which describes epoch 0 with the same
    // identities, epoch 1 besides, and a tip of its own.
    let resumed = restore_from(
        &fixture.repository("dolos/delta"),
        Point::Latest,
        storage.path(),
        node.magic,
        &blank,
        true,
    )
    .unwrap();

    assert_eq!(
        resumed.layers_skipped, seeded,
        "epoch 0's layers were kept across the inscription change"
    );

    assert_eq!(
        resumed.layers_fetched, PER_PUBLISH,
        "epoch 1's three layers and the sixteen shards, and nothing else"
    );

    // The point resolved to what it claimed. `latest` is sequence 2 here, and
    // `epoch-1` is still the stele the first half of this test read.
    let latest = Point::Latest.pull(&repository).unwrap();
    assert_eq!(latest.read_inscription().unwrap().sequence, 2);
    assert_eq!(
        Point::Epoch(1)
            .pull(&repository)
            .unwrap()
            .read_inscription()
            .unwrap()
            .digest()
            .unwrap(),
        identity,
    );

    eprintln!(
        "delta restore: {} layers already held, {} fetched (of {} in the stele)",
        resumed.layers_skipped,
        resumed.layers_fetched,
        latest.read_inscription().unwrap().layers.len(),
    );
}

/// A point that names no stele fails as a refusal, not as a partial restore.
#[test]
#[ignore]
fn a_point_that_names_no_stele_is_refused() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let repository = fixture.repository("dolos/absent");
    node.publish(&repository, &node.first);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // The repository holds sequence 1 and nothing else.
    let err = restore_from(
        &repository,
        Point::Epoch(97),
        storage.path(),
        node.magic,
        &blank,
        false,
    )
    .unwrap_err();

    assert!(matches!(err, Error::Stelae(_)), "{err:?}");

    assert!(
        blank.state().read_cursor().unwrap().is_none(),
        "a refused point still wrote a cursor"
    );

    assert_eq!(
        RestoreProgress::load(&Checkpoint::path_in(storage.path())).unwrap(),
        None,
        "a restore that never started left a progress file"
    );
}

/// The credentials handed to `registry::open` are what a restore authenticates
/// with — and a node handing none does not get in.
///
/// This is the consumer half of the access policy the registry exists under:
/// pulling a stele costs nothing and identifies nobody, and is still refused
/// without a credential. Where a node's credentials come from is the `dolos`
/// binary's business and is tested there; what is tested here is that they
/// reach the wire and decide the outcome.
///
/// The negative half doubles as the fixture's own honesty check: a registry
/// this fixture did not manage to put behind htpasswd would run anonymous, and
/// every other test in this file would pass unchanged.
#[test]
#[ignore]
fn a_node_authenticates_with_the_credentials_it_was_given() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let repository = fixture.repository("dolos/credentialed");
    node.publish(&repository, &node.first);

    let reader = fixture.repository_as("dolos/credentialed", registry_fixture::credentials());

    let stele = Point::Latest.pull(&reader).unwrap();
    println!("with the right credentials: {stele:?}");

    // Without them, and with the wrong ones, the repository refuses — and the
    // refusal is an error rather than an empty repository. `latest` reading a
    // 401 as absence is what would let a publisher restart a history chain
    // against a registry that merely did not recognise it.
    let wrong = Auth::Basic {
        user: registry_fixture::USER.to_owned(),
        password: "not-the-password".to_owned(),
    };

    for (who, credentials) in [
        ("no credentials", Auth::Anonymous),
        ("the wrong pair", wrong),
    ] {
        let refused = fixture.repository_as("dolos/credentialed", credentials);

        let err = Point::Latest
            .pull(&refused)
            .expect_err("the registry answered an unauthenticated request");

        println!("{who}: {err}");

        assert!(
            refused.latest(&dolos_snapshot::DolosProfile).is_err(),
            "{who}: a refusal read as an empty repository",
        );
    }
}

// ---------------------------------------------------------------------------
// The interruption
// ---------------------------------------------------------------------------

/// A reader that stops at a layer the test chose.
///
/// The same device `tests/restore.rs` uses, over the registry transport rather
/// than a directory: refuses the moment `stream_layer` is asked for `stop_at`,
/// so the interrupted run committed exactly the layers ahead of that one and
/// nothing of it.
struct Interrupted<'a> {
    inner: &'a Stele,
    stop_at: Digest,
}

impl SteleReader for Interrupted<'_> {
    type Blob = std::fs::File;

    fn read_inscription(&self) -> Result<stelae::inscription::Inscription, stelae::Error> {
        self.inner.read_inscription()
    }

    fn blob_index(&self) -> Result<BlobIndex, stelae::Error> {
        self.inner.blob_index()
    }

    fn compressed_size(
        &self,
        index: &BlobIndex,
        descriptor: &LayerDescriptor,
    ) -> Result<Option<u64>, stelae::Error> {
        self.inner.compressed_size(index, descriptor)
    }

    fn stream_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<Self::Blob>, stelae::Error> {
        if descriptor.diff_id == self.stop_at {
            return Err(stelae::Error::Io(std::io::Error::other(
                "the machine went away",
            )));
        }

        self.inner.stream_layer(index, profile, descriptor, limits)
    }
}

// ---------------------------------------------------------------------------
// The comparison
// ---------------------------------------------------------------------------
//
// The same comparison `tests/restore.rs` makes, over two restored store sets
// rather than a restored one and a replayed one. Kept here rather than shared
// because the two suites compare different pairs, and a helper that took either
// would say less about what each one is checking.

fn assert_stores_match<B: ToyStores>(left: &Blank<B>, right: &Blank<B>) {
    assert_state_matches(left.state(), right.state());
    assert_archive_matches(&left.archive, &right.archive);
    assert_indexes_match(left.indexes(), right.indexes(), left.state());
}

fn assert_state_matches<S: StateStore>(left: &S, right: &S) {
    assert_eq!(
        left.read_cursor().unwrap(),
        right.read_cursor().unwrap(),
        "cursor"
    );

    assert!(
        matches!(left.read_cursor().unwrap(), Some(ChainPoint::Specific(..))),
        "a restored cursor has to be anchored, or the WAL cannot be reseeded from it"
    );

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let a = entities_of(left, ns);
        let b = entities_of(right, ns);

        any |= !b.is_empty();

        assert_eq!(a, b, "entities under {ns}");
    }

    assert!(any, "the fixture has no entities, so this proves nothing");

    let utxos = utxos_of(right);
    assert!(!utxos.is_empty(), "the fixture has no utxos");
    assert_eq!(utxos_of(left), utxos, "the utxo set");
}

fn assert_archive_matches<A: ArchiveStore>(left: &A, right: &A) {
    let blocks = blocks_of(right);
    assert!(!blocks.is_empty(), "the fixture archived no blocks");
    assert_eq!(blocks_of(left), blocks, "blocks");

    let mut any = false;

    for ns in NAMESPACES {
        if ns == UTXOS {
            continue;
        }

        let a = logs_of(left, ns);
        let b = logs_of(right, ns);

        any |= !b.is_empty();

        assert_eq!(a, b, "logs under {ns}");
    }

    assert!(any, "the fixture wrote no logs, so this proves nothing");
}

/// Both halves of the index store: the archive records the layers carry, and
/// the live-UTxO dimensions they deliberately do not.
fn assert_indexes_match<I: IndexStore, S: StateStore>(left: &I, right: &I, state: &S) {
    assert_eq!(left.cursor().unwrap(), right.cursor().unwrap(), "cursor");

    let tags = tags_of(right);
    assert!(!tags.is_empty(), "the fixture produced no archive tags");
    assert_eq!(tags_of(left), tags, "archive tags");

    let exact = exact_of(right);
    assert!(!exact.is_empty(), "the fixture produced no exact records");
    assert_eq!(exact_of(left), exact, "exact records");

    let delta = UtxoSetDelta {
        produced_utxo: utxos_of(state)
            .into_iter()
            .map(|(txo, value)| (txo, std::sync::Arc::new(value)))
            .collect(),
        ..Default::default()
    };

    let rebuilt = index_delta_from_utxo_delta(ChainPoint::Origin, &delta);
    let mut asked = 0usize;

    for (txo, tags) in &rebuilt.utxo.produced {
        for tag in tags {
            let a: UtxoSet = left.utxos_by_tag(tag.dimension, &tag.key).unwrap();
            let b: UtxoSet = right.utxos_by_tag(tag.dimension, &tag.key).unwrap();

            assert!(
                a.contains(txo),
                "the rebuilt index lost {txo:?} under {}",
                tag.dimension
            );
            assert_eq!(a, b, "utxos under {}", tag.dimension);

            asked += 1;
        }
    }

    assert!(
        asked > 0,
        "the fixture's utxos carry no tags, so the rebuild proves nothing"
    );
}

fn entities_of<S: StateStore>(store: &S, ns: &'static str) -> Vec<(EntityKey, Vec<u8>)> {
    store
        .iter_entities(ns, EntityKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn utxos_of<S: StateStore>(store: &S) -> std::collections::BTreeMap<TxoRef, EraCbor> {
    store.iter_utxos().unwrap().map(Result::unwrap).collect()
}

fn blocks_of<A: ArchiveStore>(store: &A) -> Vec<(u64, Vec<u8>)> {
    store.get_range(None, None).unwrap().collect()
}

fn logs_of<A: ArchiveStore>(store: &A, ns: &'static str) -> Vec<(LogKey, Vec<u8>)> {
    store
        .iter_logs(ns, LogKey::full_range())
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn tags_of<I: IndexStore>(store: &I) -> Vec<TagRecord> {
    let mut found: Vec<TagRecord> = store
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort();
    found
}

fn exact_of<I: IndexStore>(store: &I) -> Vec<ExactRecord> {
    let mut found: Vec<ExactRecord> = store
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    found.sort_by_key(|record| (record.kind, record.key().to_vec()));
    found
}

// ---------------------------------------------------------------------------
// The progress seam
// ---------------------------------------------------------------------------

/// The restore half of the observer's cross-check: what the stream said came
/// down, against what the manifest says the layers weigh.
///
/// This is the direction the seam exists for. A registry reader pulls a whole
/// blob into scratch before `stream_layer` yields its first record, so an
/// observer wired only to the profile driver would report nothing for the
/// entire download — which on a restore is the entire operation. What proves
/// the bytes are genuinely being reported *during* the pull, rather than
/// announced once at the end, is that the deltas sum to the same number the
/// manifest states independently.
#[test]
#[ignore]
fn a_registry_restore_reports_every_byte_it_pulls() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/progress-restore");

    node.publish(&repository, &node.first);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();
    let watcher = std::sync::Arc::new(Watcher::default());

    let (plan, outlook, summary) = restore_watched(
        &repository,
        Point::Latest,
        storage.path(),
        node.magic,
        &blank,
        false,
        &watcher.observer(),
    )
    .unwrap();

    watcher.assert_well_formed(plan.layers().count());

    assert_eq!(watcher.ended(Outcome::Transferred), summary.layers_fetched);
    assert_eq!(watcher.ended(Outcome::Skipped), summary.layers_skipped);
    assert_eq!(watcher.ended(Outcome::Skipped), 0, "nothing to resume here");

    // Every layer the plan names was pulled, and each was announced with the
    // size the manifest states for it.
    assert_eq!(watcher.blobs(true).len(), summary.layers_fetched);
    assert!(watcher.blobs(false).is_empty(), "a pull never skips a blob");

    assert_eq!(
        watcher.blob_bytes(true),
        outlook.remaining.compressed_bytes,
        "the blobs announced, against what the plan said was left to fetch"
    );

    // And the deltas — reported as the bytes were written, one `poll_write` at a
    // time — sum to the same total.
    assert_eq!(
        watcher.bytes(),
        outlook.remaining.compressed_bytes,
        "byte deltas summed, against the compressed total the manifest states"
    );

    assert!(
        outlook.remaining.compressed_bytes > 0,
        "a restore that moved nothing proves nothing about byte reporting"
    );
    assert_eq!(outlook.remaining.unsized_layers, 0);

    eprintln!(
        "restored {} layers, {} compressed bytes reported in {} blob(s)",
        summary.layers_fetched,
        watcher.bytes(),
        watcher.blobs(true).len(),
    );
}

/// A resumed restore over the wire reports the layers it did not have to pull.
///
/// The skip is the driver's, decided before the transport is asked for
/// anything, so what this pins is that the two emitters do not disagree: a
/// layer the resume skipped contributes no blob and no bytes, and the counts
/// still add up to the plan's.
#[test]
#[ignore]
fn a_resumed_registry_restore_reports_what_it_skipped() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/progress-resume");

    node.publish(&repository, &node.first);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // The interruption, at the second epoch layer the driver reaches — the same
    // deterministic placement `a_killed_registry_restore_resumes_where_it_stopped`
    // uses, and for the same reason: a wall-clock kill against a loopback
    // registry holding a kilobyte-scale stele interrupts nothing.
    {
        let stele = Point::Latest.pull(&repository).unwrap();
        let identity = stele.read_inscription().unwrap().digest().unwrap();
        let plan = restore::plan(&stele, node.magic, None).unwrap();
        let index = stele.blob_index().unwrap();

        let epoch_layers: Vec<Digest> = plan.immutable_layers().map(|l| l.diff_id).collect();
        assert!(epoch_layers.len() >= 2);

        let mut checkpoint = Checkpoint::open(storage.path(), identity, false).unwrap();

        restore::restore(
            &Interrupted {
                inner: &stele,
                stop_at: epoch_layers[1],
            },
            &index,
            &plan,
            target(&blank),
            Budget::default(),
            &mut checkpoint,
            &Observer::silent(),
        )
        .unwrap_err();
    }

    let watcher = std::sync::Arc::new(Watcher::default());

    let (plan, outlook, summary) = restore_watched(
        &repository,
        Point::Latest,
        storage.path(),
        node.magic,
        &blank,
        true,
        &watcher.observer(),
    )
    .unwrap();

    watcher.assert_well_formed(plan.layers().count());

    assert!(
        summary.layers_skipped > 0,
        "the scenario has to produce a skip, or the tallies below prove nothing"
    );

    assert_eq!(watcher.ended(Outcome::Skipped), summary.layers_skipped);
    assert_eq!(watcher.ended(Outcome::Transferred), summary.layers_fetched);

    // A skipped layer is never asked of the transport, so it contributes no
    // blob — which is why the blob count is the fetch count and not the plan's.
    assert_eq!(watcher.blobs(true).len(), summary.layers_fetched);

    assert_eq!(
        watcher.bytes(),
        outlook.remaining.compressed_bytes,
        "a resumed run reports the bytes it still had to move, not the original total"
    );
}
