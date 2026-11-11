//! A restore reads layers ahead of the one it writes, and writes them in plan
//! order.
//!
//! The reader here serves a stele directory the way a registry does, on a
//! script: how long each layer takes to arrive and which one fails. Its blobs
//! count themselves while they are open, so a test can see how many layers a
//! restore holds at once and that none is left behind.

mod node;
mod recorder;

use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use dolos_snapshot::{
    restore::{self, default_budget, progress_path_in, Checkpoint, Plan, Summary, Target},
    Error,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain};
use node::{export_to, harness, Blank};
use recorder::{contents_digest, Journal, Op, RecordedArchive, RecordedState};
use stelae::{
    dir::SteleDir,
    frame::Limits,
    inscription::{Inscription, LayerDescriptor},
    plan::{RestoreProgress, Resume},
    profile::Profile,
    progress::Observer,
    transport::{BlobIndex, SteleReader},
    Digest, LayerReader,
};

/// What the scripted registry answers with when a layer fails.
const FAILURE: &str = "the registry went away";

/// How many of something are live at once, and the most there ever were.
#[derive(Default)]
struct Gauge {
    now: AtomicUsize,
    peak: AtomicUsize,
}

impl Gauge {
    fn up(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
    }

    fn down(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }

    fn now(&self) -> usize {
        self.now.load(Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

/// A layer's blob, counted from when it is opened until it is dropped. Every
/// read after its first waits `slow`, which slows the writer and not the
/// worker that opened it.
struct Blob {
    file: File,
    open: Arc<Gauge>,
    reads: usize,
    slow: Duration,
}

impl Read for Blob {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.reads > 0 {
            std::thread::sleep(self.slow);
        }

        self.reads += 1;
        self.file.read(buf)
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        self.open.down();
    }
}

/// One answered `stream_layer`.
struct Answer {
    layer: Digest,
    thread: Option<String>,
    ok: bool,
}

/// A stele directory served on a script.
struct Scripted<'a> {
    inner: &'a SteleDir,
    /// Every layer of the plan, in plan order.
    order: Vec<Digest>,
    /// How long the layer at a position of `order` takes to arrive.
    delay: fn(usize) -> Duration,
    fail_at: Option<Digest>,
    slow_reads: Duration,
    open: Arc<Gauge>,
    in_flight: Gauge,
    answers: Mutex<Vec<Answer>>,
}

impl<'a> Scripted<'a> {
    fn new(inner: &'a SteleDir, magic: u64, delay: fn(usize) -> Duration) -> Self {
        let plan = restore::plan(inner, magic, None).unwrap();

        Self {
            inner,
            order: plan.layers().map(|layer| layer.diff_id).collect(),
            delay,
            fail_at: None,
            slow_reads: Duration::ZERO,
            open: Arc::default(),
            in_flight: Gauge::default(),
            answers: Mutex::default(),
        }
    }

    /// The answers in the order they were given.
    fn answers(&self) -> MutexGuard<'_, Vec<Answer>> {
        self.answers.lock().unwrap()
    }

    fn open_layer(
        &self,
        index: &BlobIndex,
        profile: &dyn Profile,
        descriptor: &LayerDescriptor,
        limits: Limits,
    ) -> Result<LayerReader<Blob>, stelae::Error> {
        if self.fail_at == Some(descriptor.diff_id) {
            return Err(stelae::Error::Io(std::io::Error::other(FAILURE)));
        }

        let blob = index.blob_for(&descriptor.diff_id).unwrap();
        let file = File::open(self.inner.blob_path(&blob)).map_err(stelae::Error::Io)?;

        self.open.up();

        let blob = Blob {
            file,
            open: self.open.clone(),
            reads: 0,
            slow: self.slow_reads,
        };

        LayerReader::new(blob, profile, descriptor, limits)
    }
}

impl SteleReader for Scripted<'_> {
    type Blob = Blob;

    fn read_inscription(&self) -> Result<Inscription, stelae::Error> {
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
        self.in_flight.up();

        let position = self
            .order
            .iter()
            .position(|layer| *layer == descriptor.diff_id)
            .expect("a layer the plan names");

        std::thread::sleep((self.delay)(position));

        let read = self.open_layer(index, profile, descriptor, limits);

        self.in_flight.down();

        self.answers().push(Answer {
            layer: descriptor.diff_id,
            thread: std::thread::current().name().map(str::to_owned),
            ok: read.is_ok(),
        });

        read
    }
}

fn no_delay(_: usize) -> Duration {
    Duration::ZERO
}

/// The harness node, exported into a fresh directory, and its magic.
fn exported() -> (tempfile::TempDir, u64) {
    let domain: ToyDomain<MemoryStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());

    let root = tempfile::tempdir().unwrap();
    export_to(root.path(), &domain);

    (root, magic)
}

fn restore_with<R, A, S>(
    stele: &R,
    plan: &Plan,
    target: Target<'_, A, S>,
    read_ahead: usize,
    checkpoint: &mut Checkpoint,
) -> Result<Summary, Error>
where
    R: SteleReader + Sync,
    R::Blob: Send,
    A: dolos_core::ArchiveStore,
    S: dolos_core::StateStore,
{
    let index = stele.blob_index().unwrap();

    restore::restore(
        stele,
        &index,
        plan,
        target,
        default_budget(),
        read_ahead,
        checkpoint,
        &Observer::silent(),
    )
}

/// Each commit and persist of a restore, with the layers the progress file
/// recorded when it returned.
type Journaled = Vec<(Op, Option<Vec<Digest>>)>;

/// Restore `stele` at `read_ahead` into the recorded fjall pair, checkpointing
/// into a fresh directory. Returns every commit and persist in order, each with
/// the layers the progress file recorded when it returned, and a digest of the
/// node it built.
fn journaled<R>(stele: &R, magic: u64, read_ahead: usize) -> (Journaled, Digest)
where
    R: SteleReader + Sync,
    R::Blob: Send,
{
    let storage = tempfile::tempdir().unwrap();
    let progress = progress_path_in(storage.path());

    let blank = Blank::<FjallStores>::open();
    let journal = Journal::new(progress.clone());

    let archive = RecordedArchive {
        inner: blank.archive.clone(),
        journal: journal.clone(),
    };
    let state = RecordedState {
        inner: blank.state().clone(),
        journal: journal.clone(),
    };

    let identity = stele.read_inscription().unwrap().digest().unwrap();
    let plan = restore::plan(stele, magic, None).unwrap();
    let mut checkpoint = Checkpoint::open(progress, identity, false).unwrap();

    restore_with(
        stele,
        &plan,
        Target::new(&archive, &state),
        read_ahead,
        &mut checkpoint,
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
fn recorded(storage: &std::path::Path) -> BTreeSet<Digest> {
    RestoreProgress::load(&progress_path_in(storage))
        .unwrap()
        .map(|progress| progress.completed)
        .unwrap_or_default()
}

/// Layers that arrive in reverse order within each window of the read-ahead
/// are committed, persisted and recorded exactly as a serial restore does it,
/// and build the same node.
#[test]
fn layers_that_arrive_out_of_order_are_written_and_recorded_in_plan_order() {
    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();

    let (serial, serial_node) = journaled(&dir, magic, 1);

    // The first layer of each window of four arrives last.
    let scripted = Scripted::new(&dir, magic, |position| {
        Duration::from_millis(15 * (4 - position as u64 % 4))
    });

    let (ahead, ahead_node) = journaled(&scripted, magic, 4);

    let arrived: Vec<Digest> = scripted.answers().iter().map(|a| a.layer).collect();

    assert_ne!(
        arrived, scripted.order,
        "the layers arrived in plan order, so the order of the writes proves nothing"
    );
    assert!(
        scripted
            .answers()
            .iter()
            .all(|a| a.thread.as_deref() == Some("restore-read-ahead")),
        "every layer is read ahead of the writer"
    );

    assert_eq!(ahead, serial, "the writes and records left plan order");
    assert_eq!(ahead_node, serial_node);
}

/// A layer that fails while later ones are already staged fails the restore
/// at its turn: the layers before it are recorded, it and the later ones are
/// not, and by the time the restore returns every worker has stopped and no
/// layer is held open.
#[test]
fn a_failed_layer_fails_the_restore_at_its_turn_and_leaves_nothing_staged() {
    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();
    let plan = restore::plan(&dir, magic, None).unwrap();

    let epoch: Vec<Digest> = plan.immutable_layers().map(|l| l.diff_id).collect();
    assert!(
        epoch.len() >= 3,
        "the fixture needs epoch layers after the fault"
    );

    // The second layer fails late, after the third and fourth are staged; the
    // fifth is still arriving when the restore fails.
    let mut scripted = Scripted::new(&dir, magic, |position| match position {
        1 => Duration::from_millis(100),
        4 => Duration::from_millis(400),
        _ => Duration::ZERO,
    });
    scripted.fail_at = Some(epoch[1]);

    let storage = tempfile::tempdir().unwrap();
    let identity = dir.read_inscription().unwrap().digest().unwrap();
    let mut checkpoint =
        Checkpoint::open(progress_path_in(storage.path()), identity, false).unwrap();

    let blank = Blank::<MemoryStores>::open();
    let error = restore_with(
        &scripted,
        &plan,
        Target::new(&blank.archive, blank.state()),
        4,
        &mut checkpoint,
    )
    .unwrap_err();

    assert!(error.to_string().contains(FAILURE), "{error}");

    assert_eq!(scripted.in_flight.now(), 0, "a worker outlived the restore");
    assert_eq!(
        scripted.open.now(),
        0,
        "a staged layer outlived the restore"
    );

    let staged_after: Vec<Digest> = scripted
        .answers()
        .iter()
        .filter(|a| a.ok && plan.layers().position(|l| l.diff_id == a.layer) > Some(1))
        .map(|a| a.layer)
        .collect();

    assert!(
        staged_after.len() >= 2,
        "the layers after the fault were not staged before it surfaced"
    );

    assert_eq!(recorded(storage.path()), BTreeSet::from([epoch[0]]));
}

/// However much faster the downloads are than the writer, a restore holds
/// at most its depth of layers beside the one it drains.
#[test]
fn a_restore_holds_no_more_layers_than_its_read_ahead_allows() {
    const DEPTH: usize = 3;

    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();
    let plan = restore::plan(&dir, magic, None).unwrap();

    let mut scripted = Scripted::new(&dir, magic, no_delay);
    scripted.slow_reads = Duration::from_millis(5);

    assert!(
        scripted.order.len() > 2 * (DEPTH + 1),
        "the fixture needs more layers than a read-ahead holds"
    );

    let blank = Blank::<MemoryStores>::open();
    restore_with(
        &scripted,
        &plan,
        Target::new(&blank.archive, blank.state()),
        DEPTH,
        &mut Checkpoint::none(),
    )
    .unwrap();

    assert!(scripted.open.peak() >= 2, "nothing was read ahead");
    assert!(
        scripted.open.peak() <= DEPTH + 1,
        "{} layers were open at once",
        scripted.open.peak()
    );
    assert!(scripted.in_flight.peak() <= DEPTH);
    assert_eq!(scripted.open.now(), 0);
}

/// A resumed restore reading ahead never reads a layer the interrupted one
/// recorded, and builds the node an uninterrupted restore builds.
#[test]
fn a_resumed_restore_reads_no_recorded_layer_again() {
    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();
    let plan = restore::plan(&dir, magic, None).unwrap();
    let identity = dir.read_inscription().unwrap().digest().unwrap();

    let epoch: Vec<Digest> = plan.immutable_layers().map(|l| l.diff_id).collect();
    assert!(
        epoch.len() >= 3,
        "the fixture needs epoch layers after the fault"
    );

    let storage = tempfile::tempdir().unwrap();
    let progress = progress_path_in(storage.path());
    let blank = Blank::<MemoryStores>::open();

    let mut interrupted = Scripted::new(&dir, magic, no_delay);
    interrupted.fail_at = Some(epoch[2]);

    restore_with(
        &interrupted,
        &plan,
        Target::new(&blank.archive, blank.state()),
        4,
        &mut Checkpoint::open(progress.clone(), identity, false).unwrap(),
    )
    .unwrap_err();

    let done = recorded(storage.path());
    assert_eq!(done, BTreeSet::from([epoch[0], epoch[1]]));

    let resumed = Scripted::new(&dir, magic, no_delay);

    let summary = restore_with(
        &resumed,
        &plan,
        Target::new(&blank.archive, blank.state()),
        4,
        &mut Checkpoint::open(progress, identity, true).unwrap(),
    )
    .unwrap();

    assert!(
        resumed.answers().iter().all(|a| !done.contains(&a.layer)),
        "a recorded layer was read again"
    );
    assert_eq!(summary.layers_skipped, done.len());

    let whole = Blank::<MemoryStores>::open();
    restore_with(
        &dir,
        &plan,
        Target::new(&whole.archive, whole.state()),
        1,
        &mut Checkpoint::none(),
    )
    .unwrap();

    assert_eq!(
        contents_digest(&blank.archive, blank.state()),
        contents_digest(&whole.archive, whole.state())
    );
}

/// A depth of 0 or 1 is the serial restore: every layer is read on the
/// writer's thread, at its turn, one at a time.
#[test]
fn a_depth_of_zero_or_one_reads_each_layer_at_its_turn() {
    let (root, magic) = exported();
    let dir = SteleDir::open(root.path()).unwrap();
    let plan = restore::plan(&dir, magic, None).unwrap();
    let writer = std::thread::current().name().map(str::to_owned);

    for depth in [0, 1] {
        let scripted = Scripted::new(&dir, magic, no_delay);
        let blank = Blank::<MemoryStores>::open();

        restore_with(
            &scripted,
            &plan,
            Target::new(&blank.archive, blank.state()),
            depth,
            &mut Checkpoint::none(),
        )
        .unwrap();

        let read: Vec<Digest> = scripted.answers().iter().map(|a| a.layer).collect();

        assert_eq!(read, scripted.order, "depth {depth}");
        assert!(
            scripted.answers().iter().all(|a| a.thread == writer),
            "depth {depth} read a layer off the writer's thread"
        );
        assert_eq!(scripted.open.peak(), 1, "depth {depth}");
    }
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
        peak(&Resume::none(), 8),
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

    assert_eq!(peak(&resume, 8), Some(largest(&mut plan.tip_layers(), 9)));
}
