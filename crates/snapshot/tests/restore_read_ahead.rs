//! A registry restore reads layers ahead of the one it writes, through the
//! transport's ordered layer stream, and writes them in plan order.
//!
//! Against the in-process fault registry, which can hold a layer's first byte
//! back, cut a layer off, and log each layer `GET`. No container, so these run
//! under plain `cargo test`. That the stream hands layers back in order, keeps
//! to its depth and opens a layer only when it is taken are stelae's own tests;
//! these hold the restore to what it does with the stream.

mod fault_registry;
mod node;
mod paired;
mod recorder;
mod watcher;

use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use dolos_core::{Domain as _, StateStore as _};
use dolos_snapshot::{
    registry::{self, Point},
    restore::{
        self, default_budget, progress_path_in, Checkpoint, Plan, ReadAhead, Repo, Summary, Target,
        DEFAULT_READ_AHEAD,
    },
    Error,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain};
use fault_registry::FaultRegistry;
use node::{harness, plan_at_boundary, Blank};
use recorder::{contents_digest, Journal, Op, RecordedArchive, RecordedState};
use stelae::{
    inscription::LayerDescriptor,
    oci::{Options, Registry, Snapshot, DEFAULT_READ_ATTEMPTS},
    plan::{RestoreProgress, Resume},
    progress::Observer,
    transport::{BlobIndex, SnapshotReader},
    Digest,
};
use watcher::Watcher;

const REPOSITORY: &str = "dolos/read-ahead";

/// Where a paired snapshot's blocks are, beside [`REPOSITORY`].
const BLOCKS_REPOSITORY: &str = "dolos/read-ahead/blocks";

/// Install `ring` as the process-default crypto provider, which `stelae::oci`
/// asks of whatever process opens a `Registry`.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("nothing else installed a provider first");
    });
}

/// The repository, staging into `scratch`, its reads retried after a
/// millisecond rather than the node's seconds so an exhausted layer fails fast.
fn opened(registry: &FaultRegistry, scratch: &Path) -> Registry {
    opened_at(registry, scratch, REPOSITORY)
}

/// [`opened`], for the repository `name`.
fn opened_at(registry: &FaultRegistry, scratch: &Path, name: &str) -> Registry {
    install_crypto_provider();

    Registry::open(
        &registry.repository(name),
        Options {
            insecure: true,
            scratch_dir: Some(scratch.to_path_buf()),
            read_delay: Duration::from_millis(1),
            ..Options::default()
        },
    )
    .unwrap()
}

/// The harness node published into a fresh registry, and the snapshot a restore
/// reads it back from.
struct Published {
    registry: FaultRegistry,
    /// Where the restore's registry stages its pulls.
    scratch: tempfile::TempDir,
    snapshot: Snapshot,
    index: BlobIndex,
    /// The blocks snapshot a paired snapshot names, and its blob index.
    blocks: Option<(Snapshot, BlobIndex)>,
    plan: Plan,
    identity: Digest,
    magic: u64,
}

impl Published {
    /// The harness published on boundaries 1 and 2, paired ([`paired`]) into
    /// [`REPOSITORY`] and [`BLOCKS_REPOSITORY`] of a fresh registry.
    fn paired() -> Self {
        Self::paired_to(2)
    }

    /// [`Published::paired`], on boundaries 1 to `head`.
    fn paired_to(head: u64) -> Self {
        let registry = FaultRegistry::spawn();
        let domain = harness::<MemoryStores>();
        let magic = u64::from(domain.genesis().network_magic());

        let source = tempfile::tempdir().unwrap();

        for epoch in 1..=head {
            dolos_snapshot::export::publish(
                source.path(),
                &plan_at_boundary(&domain, epoch),
                domain.archive(),
                domain.state(),
                &Observer::silent(),
            )
            .unwrap();
        }

        let publishing = tempfile::tempdir().unwrap();
        let pair = paired::pair(
            source.path(),
            &opened_at(&registry, publishing.path(), REPOSITORY),
            &opened_at(&registry, publishing.path(), BLOCKS_REPOSITORY),
        );

        let scratch = tempfile::tempdir().unwrap();
        let snapshot = Point::Latest
            .resolve(&opened(&registry, scratch.path()))
            .unwrap();
        let blocks = opened_at(&registry, scratch.path(), BLOCKS_REPOSITORY)
            .resolve_sequence(&stelae_blocks::BlocksProfile, pair.head().0)
            .unwrap();

        let plan = restore::plan(snapshot.chain().unwrap(), blocks.chain(), magic, None).unwrap();
        assert_eq!(plan.blocks, Some(pair.blocks[&pair.head().0]));

        let blocks_index = blocks.blob_index().unwrap();

        Self {
            identity: snapshot.chain().unwrap().identity(),
            index: snapshot.blob_index().unwrap(),
            blocks: Some((blocks, blocks_index)),
            registry,
            scratch,
            snapshot,
            plan,
            magic,
        }
    }

    fn new() -> Self {
        let registry = FaultRegistry::spawn();
        let domain = harness::<MemoryStores>();
        let magic = u64::from(domain.genesis().network_magic());

        let publishing = tempfile::tempdir().unwrap();
        registry::publish(
            registry::Publishing::new(&opened(&registry, publishing.path())),
            &plan_at_boundary(&domain, 1),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap();

        let scratch = tempfile::tempdir().unwrap();
        let snapshot = Point::Latest
            .resolve(&opened(&registry, scratch.path()))
            .unwrap();
        let index = snapshot.blob_index().unwrap();
        let plan = restore::plan(snapshot.chain().unwrap(), None, magic, None).unwrap();
        let identity = snapshot.chain().unwrap().identity();

        Self {
            registry,
            scratch,
            snapshot,
            index,
            blocks: None,
            plan,
            identity,
            magic,
        }
    }

    /// The blob of each layer in `layers`, as the registry names it, in
    /// whichever repository holds it.
    fn blobs<'a>(&self, layers: impl Iterator<Item = &'a LayerDescriptor>) -> Vec<String> {
        layers
            .map(|layer| {
                self.index
                    .blob_for(&layer.diff_id)
                    .or_else(|| self.blocks.as_ref()?.1.blob_for(&layer.diff_id))
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    fn restore<A, S>(
        &self,
        target: Target<'_, A, S>,
        read_ahead: usize,
        checkpoint: &mut Checkpoint,
        observer: &Observer,
    ) -> Result<Summary, Error>
    where
        A: dolos_core::ArchiveStore,
        S: dolos_core::StateStore,
    {
        restore::restore(
            &self.snapshot,
            &self.index,
            self.blocks
                .as_ref()
                .map(|(snapshot, index)| restore::Blocks { snapshot, index }),
            &self.plan,
            target,
            default_budget(),
            read_ahead,
            checkpoint,
            observer,
        )
    }

    /// A checkpoint in `storage`, honouring what is recorded there if `resume`.
    fn checkpoint(&self, storage: &Path, resume: bool) -> Checkpoint {
        Checkpoint::open(progress_path_in(storage), self.identity, resume).unwrap()
    }

    /// Whether the restore's scratch directory holds nothing.
    fn scratch_is_empty(&self) -> bool {
        std::fs::read_dir(self.scratch.path())
            .map(|entries| entries.count() == 0)
            .unwrap_or(true)
    }
}

/// Each commit and persist of a restore, with the layers the progress file
/// recorded when it returned.
type Journaled = Vec<(Op, Option<Vec<Digest>>)>;

/// Restore at `read_ahead` into the recorded fjall pair, checkpointing into a
/// fresh directory. Returns every commit and persist in order, each with the
/// layers the progress file recorded when it returned, and a digest of the
/// node it built.
fn journaled(published: &Published, read_ahead: usize) -> (Journaled, Digest) {
    let storage = tempfile::tempdir().unwrap();

    let blank = Blank::<FjallStores>::open();
    let journal = Journal::new(progress_path_in(storage.path()));

    let archive = RecordedArchive {
        inner: blank.archive.clone(),
        journal: journal.clone(),
    };
    let state = RecordedState {
        inner: blank.state().clone(),
        journal: journal.clone(),
    };

    published
        .restore(
            Target::new(&archive, &state),
            read_ahead,
            &mut published.checkpoint(storage.path(), false),
            &Observer::silent(),
        )
        .unwrap();

    let entries = journal
        .take()
        .into_iter()
        .map(|entry| (entry.op, entry.recorded))
        .collect();

    (entries, contents_digest(&blank.archive, blank.state()))
}

/// The layers the progress file in `storage` records.
fn recorded(storage: &Path) -> BTreeSet<Digest> {
    RestoreProgress::load(&progress_path_in(storage))
        .unwrap()
        .map(|progress| progress.completed)
        .unwrap_or_default()
}

/// Layers whose downloads finish in another order than the plan's are
/// committed, persisted and recorded exactly as a restore reading one at a
/// time does it, and build the same node.
#[test]
fn layers_that_arrive_out_of_order_are_written_and_recorded_in_plan_order() {
    let published = Published::new();
    let registry = &published.registry;
    let order = published.blobs(published.plan.layers());

    let (serial, serial_node) = journaled(&published, 1);

    // The earlier of the first four layers wait longer, so the ones after
    // them finish first.
    for (position, blob) in order.iter().take(4).enumerate() {
        registry.delay(
            std::slice::from_ref(blob),
            Duration::from_millis(50 * (4 - position as u64)),
        );
    }
    registry.forget();

    let (ahead, ahead_node) = journaled(&published, DEFAULT_READ_AHEAD);

    assert_ne!(
        registry.served(),
        order,
        "the layers arrived in plan order, so the order of the writes proves nothing"
    );
    assert!(registry.peak() > 1, "nothing was read ahead");
    assert!(
        registry.peak() <= DEFAULT_READ_AHEAD,
        "{} layers were fetched at once",
        registry.peak()
    );

    assert_eq!(ahead, serial, "the writes and records left plan order");
    assert_eq!(ahead_node, serial_node);
}

/// A layer that never arrives whole fails the restore at its turn, while
/// later layers are already being read: the layers before it are recorded,
/// it and the later ones are not, and nothing is left staged.
#[test]
fn a_failed_layer_fails_the_restore_at_its_turn() {
    let published = Published::new();
    let registry = &published.registry;
    let plan = &published.plan;

    let epoch: Vec<&LayerDescriptor> = plan.immutable_layers().collect();
    assert!(
        epoch.len() >= 3,
        "the fixture needs epoch layers after the fault"
    );

    let order = published.blobs(plan.layers());
    let failed = &order[1];
    let later = order.get(5).expect("the fixture needs a sixth layer");

    // The second layer fails only once a later one has been requested.
    registry.cut_always(failed);
    registry.hold(failed, later);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    let error = published
        .restore(
            Target::new(&blank.archive, blank.state()),
            DEFAULT_READ_AHEAD,
            &mut published.checkpoint(storage.path(), false),
            &Observer::silent(),
        )
        .unwrap_err();

    assert!(
        matches!(
            &error,
            Error::Stelae(stelae::Error::RetriesExhausted { .. })
        ),
        "{error:?}"
    );
    assert!(error.to_string().contains(failed.as_str()), "{error}");

    assert!(
        registry.arrived().contains(later),
        "the layers after the failed one were not read ahead of it"
    );

    assert_eq!(recorded(storage.path()), BTreeSet::from([epoch[0].diff_id]));
    assert!(
        published.scratch_is_empty(),
        "a staged layer was left behind"
    );
}

/// A restore that fails on its own side, a store refusing a commit, while
/// layers are still being read ahead of it ends those reads before it
/// returns: nothing it started moves another byte afterwards. Nor did it take
/// a layer before its turn, so nothing past the first layer's read-ahead was
/// requested.
#[test]
fn a_restore_that_fails_ends_the_reads_it_started() {
    let published = Published::new();
    let registry = &published.registry;

    // The first layer arrives once the sixth has been requested, and the
    // sixth is still arriving when the first layer's commit fails.
    let order = published.blobs(published.plan.layers());
    assert!(
        order.len() > DEFAULT_READ_AHEAD,
        "the fixture needs more layers than the depth"
    );
    let slow = order.get(5).expect("the fixture needs a sixth layer");
    registry.hold(&order[0], slow);
    registry.delay(std::slice::from_ref(slow), Duration::from_millis(2_000));

    // A restore that took the second layer before the first was written
    // would wait on it, and it arrives only once a layer past the first
    // layer's read-ahead has been requested: an early take cannot finish
    // before the check below sees that request.
    registry.hold(&order[1], &order[DEFAULT_READ_AHEAD]);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();
    let journal = Journal::crashing_at(progress_path_in(storage.path()), 1);
    let watcher = Arc::new(Watcher::default());

    published
        .restore(
            Target::new(
                &RecordedArchive {
                    inner: blank.archive.clone(),
                    journal: journal.clone(),
                },
                &RecordedState {
                    inner: blank.state().clone(),
                    journal,
                },
            ),
            DEFAULT_READ_AHEAD,
            &mut published.checkpoint(storage.path(), false),
            &watcher.observer(),
        )
        .unwrap_err();

    let bytes_at_return = watcher.bytes();

    assert!(
        registry.arrived().contains(slow),
        "the slow layer was not read ahead"
    );

    // The commit failed while the first layer was being written, so only its
    // read-ahead had been asked of the stream.
    let arrived = registry.arrived();
    assert!(
        arrived
            .iter()
            .all(|blob| order[..DEFAULT_READ_AHEAD].contains(blob)),
        "layers past the first {DEFAULT_READ_AHEAD} were read before their turn: {arrived:?}"
    );

    // Its answer comes after the restore returned; a pull still running
    // would report the bytes.
    std::thread::sleep(Duration::from_millis(2_500));

    assert_eq!(
        watcher.bytes(),
        bytes_at_return,
        "a pull outlived the restore"
    );
    assert!(
        published.scratch_is_empty(),
        "a staged layer was left behind"
    );
}

/// A resumed restore reading ahead never fetches a layer the interrupted one
/// recorded, and builds the node an uninterrupted restore builds.
#[test]
fn a_resumed_restore_fetches_no_recorded_layer_again() {
    let published = Published::new();
    let registry = &published.registry;
    let plan = &published.plan;

    let epoch: Vec<&LayerDescriptor> = plan.immutable_layers().collect();
    assert!(
        epoch.len() >= 3,
        "the fixture needs epoch layers after the fault"
    );

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();

    // The third layer fails every attempt of the first run, then arrives.
    let interrupted = published.blobs(epoch[2..3].iter().copied()).remove(0);
    registry.cut_next(&interrupted, DEFAULT_READ_ATTEMPTS as usize);

    published
        .restore(
            Target::new(&blank.archive, blank.state()),
            DEFAULT_READ_AHEAD,
            &mut published.checkpoint(storage.path(), false),
            &Observer::silent(),
        )
        .unwrap_err();

    let done = recorded(storage.path());
    assert_eq!(done, BTreeSet::from([epoch[0].diff_id, epoch[1].diff_id]));

    let recorded_blobs = published.blobs(epoch[..2].iter().copied());
    let gets_before: Vec<usize> = recorded_blobs.iter().map(|b| registry.gets(b)).collect();

    let summary = published
        .restore(
            Target::new(&blank.archive, blank.state()),
            DEFAULT_READ_AHEAD,
            &mut published.checkpoint(storage.path(), true),
            &Observer::silent(),
        )
        .unwrap();

    let gets_after: Vec<usize> = recorded_blobs.iter().map(|b| registry.gets(b)).collect();

    assert_eq!(
        gets_after, gets_before,
        "a recorded layer was fetched again"
    );
    assert_eq!(gets_before, vec![1, 1]);
    assert_eq!(summary.layers_skipped, done.len());

    let whole = Blank::<MemoryStores>::open();
    published
        .restore(
            Target::new(&whole.archive, whole.state()),
            1,
            &mut Checkpoint::none(),
            &Observer::silent(),
        )
        .unwrap();

    assert_eq!(
        contents_digest(&blank.archive, blank.state()),
        contents_digest(&whole.archive, whole.state())
    );
}

/// A depth of 0 or 1 is the serial restore: one layer `GET` at a time, in
/// plan order.
#[test]
fn a_depth_of_zero_or_one_fetches_one_layer_at_a_time() {
    let published = Published::new();
    let registry = &published.registry;
    let order = published.blobs(published.plan.layers());

    for depth in [0, 1] {
        registry.forget();

        let blank = Blank::<MemoryStores>::open();
        published
            .restore(
                Target::new(&blank.archive, blank.state()),
                depth,
                &mut Checkpoint::none(),
                &Observer::silent(),
            )
            .unwrap();

        assert_eq!(registry.arrived(), order, "depth {depth}");
        assert_eq!(registry.peak(), 1, "depth {depth}");
    }
}

/// The harness node, exported alone into a fresh image layout, naming no
/// blocks snapshot, and its magic: one stream to stage.
fn exported() -> (tempfile::TempDir, u64) {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = u64::from(domain.genesis().network_magic());

    let layout = tempfile::tempdir().unwrap();
    node::export_layout(layout.path(), &domain, &node::plan_for(&domain));

    (layout, magic)
}

/// The staging need is the compressed size of the layers a restore holds at
/// once: at the default depth, the nine largest still to pull.
#[test]
fn the_staging_preflight_charges_the_largest_layers_a_read_ahead_holds() {
    let (layout, magic) = exported();
    let dir = stelae::dir::SnapshotDir::open(layout.path())
        .unwrap()
        .resolve_latest(&dolos_snapshot::DolosProfile)
        .unwrap();
    let plan = restore::plan(dir.chain().unwrap(), None, magic, None).unwrap();
    let index = dir.blob_index().unwrap();

    let largest = |layers: &mut dyn Iterator<Item = &LayerDescriptor>, count: usize| {
        let mut sizes: Vec<u64> = layers
            .map(|layer| dir.compressed_size(&index, layer).unwrap().unwrap())
            .collect();

        sizes.sort_unstable_by(|a, b| b.cmp(a));

        assert!(
            sizes.len() > count,
            "the fixture needs more layers than are staged"
        );

        sizes[..count].iter().sum::<u64>()
    };

    let peak = |resume: &Resume, depth| {
        plan.staging_peak(&dir, &index, None, resume, depth)
            .unwrap()
    };

    assert_eq!(
        peak(&Resume::none(), DEFAULT_READ_AHEAD),
        Some(largest(&mut plan.layers(), 9))
    );
    assert_eq!(
        peak(&Resume::none(), 1),
        Some(largest(&mut plan.layers(), 1))
    );
    assert_eq!(peak(&Resume::none(), 0), peak(&Resume::none(), 1));

    // A resume that recorded every epoch layer stages only the tip.
    let mut progress = RestoreProgress::new(Digest::from_bytes([0; 32]));
    plan.immutable_layers()
        .for_each(|layer| progress.record(layer.diff_id));

    let resume = Resume::from_progress(Some(&progress));

    assert_eq!(
        peak(&resume, DEFAULT_READ_AHEAD),
        Some(largest(&mut plan.tip_layers(), 9))
    );
}

/// A paired snapshot reads both repositories ahead, its blocks stream as well
/// as its own, never more layers at once than the depth, and writes and records
/// them as a restore reading one layer at a time does, into the same node.
#[test]
fn both_repositories_are_read_ahead_under_one_depth() {
    let published = Published::paired();
    let registry = &published.registry;
    let plan = &published.plan;

    let blocks = published.blobs(plan.remaining_in(Repo::Blocks, &Resume::none()));
    let others = published.blobs(plan.remaining_in(Repo::Dolos, &Resume::none()));
    assert_eq!(blocks.len(), 2, "the fixture has two closed epochs");

    let (serial, serial_node) = journaled(&published, 1);

    // Epoch 0's blocks arrive only once epoch 1's have been asked for. A
    // blocks stream that reads ahead asks for both at its first take; one that
    // does not asks for epoch 1's after the rest of epoch 0 is written.
    registry.hold(&blocks[0], &blocks[1]);
    registry.forget();

    let (ahead, ahead_node) = journaled(&published, DEFAULT_READ_AHEAD);

    let arrived = registry.arrived();
    let at = |blob: &String| arrived.iter().position(|b| b == blob).unwrap();

    assert!(
        others.iter().all(|blob| at(&blocks[1]) < at(blob)),
        "the blocks stream did not read ahead: {arrived:?}"
    );
    assert!(registry.peak() > 1, "nothing was read ahead");
    assert!(
        registry.peak() <= DEFAULT_READ_AHEAD,
        "{} layers were fetched at once",
        registry.peak()
    );

    assert_eq!(ahead, serial, "the writes and records left plan order");
    assert_eq!(ahead_node, serial_node);
}

/// Each stream keeps to its share of the depth, whatever it has left to read:
/// at the default, the blocks stream fetches two layers at once and the
/// snapshot's own six, and the staging preflight charges each of them one layer
/// more, the one it drains.
#[test]
fn each_stream_reads_ahead_at_its_share_of_the_depth() {
    let published = Published::paired_to(3);
    let registry = &published.registry;
    let plan = &published.plan;
    let depths = ReadAhead::split(DEFAULT_READ_AHEAD, true);

    let blocks = published.blobs(plan.remaining_in(Repo::Blocks, &Resume::none()));
    let others = published.blobs(plan.remaining_in(Repo::Dolos, &Resume::none()));
    assert!(
        blocks.len() > depths.blocks,
        "{} blocks layers",
        blocks.len()
    );
    assert!(others.len() > DEFAULT_READ_AHEAD, "{} layers", others.len());

    // Every layer waits before its first byte, so the pulls a stream starts
    // together are in flight together.
    registry.delay(&[blocks, others].concat(), Duration::from_millis(200));
    registry.forget();

    let blank = Blank::<MemoryStores>::open();
    published
        .restore(
            Target::new(&blank.archive, blank.state()),
            DEFAULT_READ_AHEAD,
            &mut Checkpoint::none(),
            &Observer::silent(),
        )
        .unwrap();

    assert_eq!(registry.peak_in(BLOCKS_REPOSITORY), depths.blocks);

    // One over the share is a pull started as the registry was still closing
    // the answer the restore had just taken.
    let own = registry.peak_in(REPOSITORY);
    assert!(
        (depths.dolos..=depths.dolos + 1).contains(&own),
        "the snapshot's stream fetched {own} layers at once at a share of {}",
        depths.dolos
    );
}

/// The registry transport resolves the blocks repository at the snapshot's
/// sequence and restores the node the direct restore does; a blocks
/// repository holding nothing there is refused, naming the snapshot, and one
/// holding another profile's snapshot is refused as what it is.
#[test]
fn a_registry_restore_reads_the_blocks_repository_it_is_given() {
    let published = Published::paired();
    let reference = published.plan.blocks.unwrap();

    let direct = Blank::<MemoryStores>::open();
    published
        .restore(
            Target::new(&direct.archive, direct.state()),
            DEFAULT_READ_AHEAD,
            &mut Checkpoint::none(),
            &Observer::silent(),
        )
        .unwrap();

    let scratch = tempfile::tempdir().unwrap();
    let repository = |name: &str| {
        install_crypto_provider();

        registry::SnapshotRepository::open(
            &published.registry.repository(name),
            true,
            registry::Auth::default(),
            scratch.path().to_path_buf(),
            registry::Tuning::default(),
        )
        .unwrap()
    };

    let restored = |blocks: &registry::SnapshotRepository| {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<MemoryStores>::open();

        let outcome = restore::execute(
            restore::Input::Repository {
                repository: &repository(REPOSITORY),
                point: Point::Latest,
                blocks: Some(blocks),
            },
            restore::Restoring {
                network_magic: published.magic,
                max_history: None,
                storage_path: storage.path(),
                resume: false,
                skip_space_check: false,
                journals: 0,
                read_ahead: None,
                source: None,
            },
            Target::new(&blank.archive, blank.state()),
            None,
        );

        (outcome, blank)
    };

    let (outcome, blank) = restored(&repository(BLOCKS_REPOSITORY));
    assert_eq!(outcome.unwrap().plan.blocks, Some(reference));
    assert_eq!(
        contents_digest(&blank.archive, blank.state()),
        contents_digest(&direct.archive, direct.state())
    );

    let (outcome, blank) = restored(&repository("dolos/read-ahead/nothing"));
    let error = outcome.map(|o| o.plan.blocks).unwrap_err();

    let Error::BlocksMissing {
        reference: named, ..
    } = &error
    else {
        panic!("{error:?}");
    };
    assert_eq!(*named, reference);
    assert!(
        error.to_string().contains("holds no snapshot at epoch-"),
        "{error}"
    );
    assert_eq!(blank.state().read_cursor().unwrap(), None);

    let (outcome, blank) = restored(&repository(REPOSITORY));
    let error = outcome.map(|o| o.plan.blocks).unwrap_err();
    assert!(
        matches!(error, Error::Stelae(stelae::Error::UnknownProfile { .. })),
        "{error:?}"
    );
    assert_eq!(blank.state().read_cursor().unwrap(), None);
}

/// At a depth of 0 or 1 a paired restore asks for one layer at a time, from
/// either repository, in plan order.
#[test]
fn a_paired_restore_at_a_depth_of_zero_or_one_fetches_one_layer_at_a_time() {
    let published = Published::paired();
    let registry = &published.registry;
    let order = published.blobs(published.plan.layers());

    for depth in [0, 1] {
        registry.forget();

        let blank = Blank::<MemoryStores>::open();
        published
            .restore(
                Target::new(&blank.archive, blank.state()),
                depth,
                &mut Checkpoint::none(),
                &Observer::silent(),
            )
            .unwrap();

        assert_eq!(registry.arrived(), order, "depth {depth}");
        assert_eq!(registry.peak(), 1, "depth {depth}");
    }
}

/// The depth is split between the two streams, and the staging need is each
/// stream's largest layers at its share: a blocks stream reading one layer at
/// a time beside another that does too holds one layer in all.
#[test]
fn the_staging_preflight_charges_each_stream_its_share() {
    assert_eq!(
        ReadAhead::split(DEFAULT_READ_AHEAD, true),
        ReadAhead {
            dolos: DEFAULT_READ_AHEAD - 2,
            blocks: 2
        }
    );
    assert_eq!(
        ReadAhead::split(3, true),
        ReadAhead {
            dolos: 2,
            blocks: 1
        }
    );
    assert_eq!(ReadAhead::split(1, true).at_once(), 1);
    assert_eq!(
        ReadAhead::split(DEFAULT_READ_AHEAD, false),
        ReadAhead {
            dolos: DEFAULT_READ_AHEAD,
            blocks: 0
        }
    );

    let domain: ToyDomain<MemoryStores> = harness();
    let magic = u64::from(domain.genesis().network_magic());

    let source = tempfile::tempdir().unwrap();
    for epoch in [1, 2] {
        dolos_snapshot::export::publish(
            source.path(),
            &plan_at_boundary(&domain, epoch),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap();
    }

    let (snapshot_root, blocks_root) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let pair = paired::pair(
        source.path(),
        &stelae::dir::SnapshotDir::create(snapshot_root.path()).unwrap(),
        &stelae::dir::SnapshotDir::create(blocks_root.path()).unwrap(),
    );

    let snapshot = stelae::dir::SnapshotDir::open(snapshot_root.path())
        .unwrap()
        .resolve_latest(&dolos_snapshot::DolosProfile)
        .unwrap();
    let blocks = stelae::dir::SnapshotDir::open(blocks_root.path())
        .unwrap()
        .resolve_sequence(&stelae_blocks::BlocksProfile, pair.head().0)
        .unwrap();
    let (index, blocks_index) = (snapshot.blob_index().unwrap(), blocks.blob_index().unwrap());
    let plan = restore::plan(snapshot.chain().unwrap(), blocks.chain(), magic, None).unwrap();

    let pair = restore::Blocks {
        snapshot: &blocks,
        index: &blocks_index,
    };

    let sizes = |repo| -> Vec<u64> {
        let (snapshot, index) = match repo {
            Repo::Dolos => (&snapshot, &index),
            Repo::Blocks => (&blocks, &blocks_index),
        };

        let mut sizes: Vec<u64> = plan
            .remaining_in(repo, &Resume::none())
            .map(|layer| snapshot.compressed_size(index, layer).unwrap().unwrap())
            .collect();

        sizes.sort_unstable_by(|a, b| b.cmp(a));
        sizes
    };

    let largest = |sizes: &[u64], count: usize| sizes.iter().take(count).sum::<u64>();
    let (own, theirs) = (sizes(Repo::Dolos), sizes(Repo::Blocks));
    assert!(own.len() > DEFAULT_READ_AHEAD && theirs.len() == 2);

    let peak = |depth| {
        plan.staging_peak(&snapshot, &index, Some(pair), &Resume::none(), depth)
            .unwrap()
    };

    // Seven of the snapshot's own at depth 6 and both blocks layers at depth 2.
    assert_eq!(
        peak(DEFAULT_READ_AHEAD),
        Some(largest(&own, DEFAULT_READ_AHEAD - 1) + largest(&theirs, 3))
    );

    let mut both = [own.clone(), theirs.clone()].concat();
    both.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(peak(1), Some(both[0]));
    assert_eq!(peak(0), peak(1));

    // Without the blocks snapshot, the layers it serves have no transport.
    assert!(matches!(
        plan.staging_peak(&snapshot, &index, None, &Resume::none(), 1),
        Err(Error::BlocksMissing { .. })
    ));
}
