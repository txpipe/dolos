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
mod recorder;
mod watcher;

use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use dolos_core::Domain as _;
use dolos_snapshot::{
    registry::{self, Point},
    restore::{
        self, default_budget, progress_path_in, Checkpoint, Plan, Summary, Target,
        DEFAULT_READ_AHEAD,
    },
    Error,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain};
use fault_registry::FaultRegistry;
use node::{export_to, harness, plan_at_boundary, Blank};
use recorder::{contents_digest, Journal, Op, RecordedArchive, RecordedState};
use stelae::{
    dir::SteleDir,
    inscription::LayerDescriptor,
    oci::{Options, Registry, Stele, DEFAULT_READ_ATTEMPTS},
    plan::{RestoreProgress, Resume},
    progress::Observer,
    transport::{BlobIndex, SteleReader},
    Digest,
};
use watcher::Watcher;

const REPOSITORY: &str = "dolos/read-ahead";

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
    install_crypto_provider();

    Registry::open(
        &registry.repository(REPOSITORY),
        Options {
            insecure: true,
            scratch_dir: Some(scratch.to_path_buf()),
            read_delay: Duration::from_millis(1),
            ..Options::default()
        },
    )
    .unwrap()
}

/// The harness node published into a fresh registry, and the stele a restore
/// reads it back from.
struct Published {
    registry: FaultRegistry,
    /// Where the restore's registry stages its pulls.
    scratch: tempfile::TempDir,
    stele: Stele,
    index: BlobIndex,
    plan: Plan,
    identity: Digest,
}

impl Published {
    fn new() -> Self {
        let registry = FaultRegistry::spawn();
        let domain = harness::<MemoryStores>();
        let magic = u64::from(domain.genesis().network_magic());

        let publishing = tempfile::tempdir().unwrap();
        registry::publish(
            registry::Publishing::new(&opened(&registry, publishing.path())),
            &plan_at_boundary(&domain, 1, Default::default()),
            domain.archive(),
            domain.state(),
            None,
            &Observer::silent(),
        )
        .unwrap();

        let scratch = tempfile::tempdir().unwrap();
        let stele = Point::Latest
            .pull(&opened(&registry, scratch.path()))
            .unwrap();
        let index = stele.blob_index().unwrap();
        let plan = restore::plan(&stele, magic, None).unwrap();
        let identity = stele.read_inscription().unwrap().digest().unwrap();

        Self {
            registry,
            scratch,
            stele,
            index,
            plan,
            identity,
        }
    }

    /// The blob of each layer in `layers`, as the registry names it.
    fn blobs<'a>(&self, layers: impl Iterator<Item = &'a LayerDescriptor>) -> Vec<String> {
        layers
            .map(|layer| self.index.blob_for(&layer.diff_id).unwrap().to_string())
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
            &self.stele,
            &self.index,
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

/// The harness node, exported into a fresh directory, and its magic.
fn exported() -> (tempfile::TempDir, u64) {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = u64::from(domain.genesis().network_magic());

    let root = tempfile::tempdir().unwrap();
    export_to(root.path(), &domain);

    (root, magic)
}

/// The staging need is the compressed size of the layers a restore holds at
/// once: at the default depth, the nine largest still to pull.
#[test]
fn the_staging_preflight_charges_the_largest_layers_a_read_ahead_holds() {
    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();
    let plan = restore::plan(&dir, magic, None).unwrap();
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

    let peak = |resume: &Resume, depth| plan.staging_peak(&dir, &index, resume, depth).unwrap();

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
