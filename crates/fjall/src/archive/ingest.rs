//! The restore's writer for the archive's append-only layers.
//!
//! A `blocks` layer and a `log-*` layer each arrive in ascending key order,
//! so a restore writes them as fjall ingestions: sorted rows go straight into
//! tables, with no journal, memtable or flush. Each keyspace gets one run per
//! layer; a key that does not ascend (a Byron boundary block sharing its slot
//! with the next block) finishes the run and starts another, whose rows are
//! newer, so the later row wins as it would in a batch.
//!
//! Ingestion checks no write halt, so a run first waits while its keyspace
//! holds [`L0_GUARD`] or more L0 runs (lsm-tree records a level's run count
//! in one byte). Ingested tables are written uncompressed at level-1 policies,
//! so a restore ends with [`settle`], one major compaction per keyspace.

use std::time::{Duration, Instant};

use dolos_core::{
    ArchiveError, ArchiveIngest, BlockSlot, ChainPoint, EntityValue, LogKey, Namespace, RawBlock,
};
use dolos_flatfiles::{encode_locations, BlockLocation};
use fjall::{AbstractTree as _, Keyspace};

use super::{build_log_key, fjall_err, io_err, keyspace_names, merge_location, ArchiveStore};

/// The L0 run count at which a new run waits for compaction.
pub const L0_GUARD: usize = 64;

/// How often a waiting run asks for a compaction: fjall compacts only when a
/// flush or an ingestion asks, and a waiting writer asks for nothing.
const KICK_EVERY: Duration = Duration::from_millis(500);

const POLL: Duration = Duration::from_millis(20);

/// An open fjall ingestion. Its type has no public name, so it is kept
/// behind this trait.
trait Sink {
    fn write(&mut self, key: &[u8], value: &[u8]) -> fjall::Result<()>;
    fn finish(self: Box<Self>) -> fjall::Result<()>;
}

struct Ingestion<I> {
    inner: I,
    write: fn(&mut I, &[u8], &[u8]) -> fjall::Result<()>,
    finish: fn(I) -> fjall::Result<()>,
}

impl<I> Sink for Ingestion<I> {
    fn write(&mut self, key: &[u8], value: &[u8]) -> fjall::Result<()> {
        (self.write)(&mut self.inner, key, value)
    }

    fn finish(self: Box<Self>) -> fjall::Result<()> {
        (self.finish)(self.inner)
    }
}

fn start_ingestion(keyspace: &Keyspace) -> fjall::Result<Box<dyn Sink + '_>> {
    Ok(Box::new(Ingestion {
        inner: keyspace.start_ingestion()?,
        write: |ingestion, key, value| ingestion.write(key, value),
        finish: |ingestion| ingestion.finish(),
    }))
}

/// What the L0 guard reads and asks of a keyspace.
trait Compacts {
    fn l0_runs(&self) -> usize;

    /// Queue a compaction.
    fn kick(&self) -> fjall::Result<()>;
}

impl Compacts for Keyspace {
    fn l0_runs(&self) -> usize {
        self.tree.l0_run_count()
    }

    /// An empty ingestion writes no table and queues a compaction.
    fn kick(&self) -> fjall::Result<()> {
        self.start_ingestion()?.finish()
    }
}

/// Wait while `keyspace` holds `guard` or more L0 runs, queuing a compaction
/// every [`KICK_EVERY`]. Returns how long it waited.
fn wait_for_l0(keyspace: &impl Compacts, guard: usize) -> fjall::Result<Duration> {
    let started = Instant::now();
    let mut kicked: Option<Instant> = None;

    while keyspace.l0_runs() >= guard {
        if kicked.is_none_or(|at| at.elapsed() >= KICK_EVERY) {
            keyspace.kick()?;
            kicked = Some(Instant::now());
        }

        std::thread::sleep(POLL);
    }

    Ok(started.elapsed())
}

/// One keyspace's runs within a layer.
struct Run<'a> {
    keyspace: &'a Keyspace,
    name: &'static str,
    guard: usize,
    open: Option<Box<dyn Sink + 'a>>,
    last: Vec<u8>,
    runs: u64,
    rows: u64,
    waited: Duration,
}

impl<'a> Run<'a> {
    fn new(keyspace: &'a Keyspace, name: &'static str, guard: usize) -> Self {
        Self {
            keyspace,
            name,
            guard,
            open: None,
            last: Vec::new(),
            runs: 0,
            rows: 0,
            waited: Duration::ZERO,
        }
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> fjall::Result<()> {
        if self.open.is_none() || key <= self.last.as_slice() {
            self.close()?;
            self.waited += wait_for_l0(self.keyspace, self.guard)?;
            self.runs += 1;
            self.open = Some(start_ingestion(self.keyspace)?);
        }

        if let Some(sink) = &mut self.open {
            sink.write(key, value)?;
        }

        self.last.clear();
        self.last.extend_from_slice(key);
        self.rows += 1;

        Ok(())
    }

    fn close(&mut self) -> fjall::Result<()> {
        match self.open.take() {
            Some(sink) => sink.finish(),
            None => Ok(()),
        }
    }

    fn report(&self) {
        if self.runs > 0 {
            tracing::debug!(
                keyspace = self.name,
                runs = self.runs,
                rows = self.rows,
                waited_ms = self.waited.as_millis() as u64,
                "ingested a restore layer"
            );
        }
    }
}

/// [`ArchiveIngest`] for the fjall archive: block locations into
/// `archive-blocks` and log rows into `archive-logs`, as ingested runs.
pub struct Ingest<'a> {
    store: &'a ArchiveStore,
    blocks: Run<'a>,
    logs: Run<'a>,
    /// The last slot written and its locations: a slot's second block follows
    /// its first before that run is visible.
    last_block: Option<(BlockSlot, Vec<BlockLocation>)>,
}

impl<'a> Ingest<'a> {
    pub(crate) fn new(store: &'a ArchiveStore, guard: usize) -> Self {
        Self {
            store,
            blocks: Run::new(&store.blocks, keyspace_names::BLOCKS, guard),
            logs: Run::new(&store.logs, keyspace_names::LOGS, guard),
            last_block: None,
        }
    }
}

impl ArchiveIngest for Ingest<'_> {
    /// Bodies are appended (and synced) to the segment files first, as a
    /// batched commit does, then their locations are ingested.
    fn blocks(&mut self, blocks: Vec<(ChainPoint, RawBlock)>) -> Result<(), ArchiveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let items: Vec<(u32, &[u8])> = blocks
            .iter()
            .map(|(point, body)| {
                (
                    BlockLocation::segment_for_slot(point.slot()),
                    body.as_slice(),
                )
            })
            .collect();

        let appended = self.store.flatfiles.append_batch(&items).map_err(io_err)?;
        let snapshot = self.store.db.snapshot();

        for ((point, body), incoming) in blocks.iter().zip(appended) {
            let slot = point.slot();

            let existing = match self.last_block.take() {
                Some((last, locations)) if last == slot => locations,
                _ => self.store.stored_locations(&snapshot, slot)?,
            };

            let merged = merge_location(existing, incoming, body, &self.store.flatfiles)?;

            self.blocks
                .write(&slot.to_be_bytes(), &encode_locations(&merged))
                .map_err(fjall_err)?;

            self.last_block = Some((slot, merged));
        }

        Ok(())
    }

    fn logs(
        &mut self,
        ns: Namespace,
        rows: Vec<(LogKey, EntityValue)>,
    ) -> Result<(), ArchiveError> {
        self.store.check_namespace(ns)?;

        for (key, value) in &rows {
            self.logs
                .write(build_log_key(ns, key).as_slice(), value)
                .map_err(fjall_err)?;
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), ArchiveError> {
        self.blocks.close().map_err(fjall_err)?;
        self.logs.close().map_err(fjall_err)?;

        self.blocks.report();
        self.logs.report();

        Ok(())
    }
}

/// Major-compact `archive-logs` and `archive-blocks`, each on its own thread,
/// so ingested tables end compressed at the last level's table size.
pub(crate) fn settle(
    store: &ArchiveStore,
) -> Result<Vec<std::thread::JoinHandle<Result<(), ArchiveError>>>, ArchiveError> {
    [
        (keyspace_names::LOGS, store.logs.clone()),
        (keyspace_names::BLOCKS, store.blocks.clone()),
    ]
    .into_iter()
    .map(|(name, keyspace)| {
        std::thread::Builder::new()
            .name("archive-settle".into())
            .spawn(move || {
                let started = Instant::now();
                let before = keyspace.disk_space();

                keyspace.major_compact().map_err(fjall_err)?;

                tracing::info!(
                    keyspace = name,
                    secs = started.elapsed().as_secs(),
                    before,
                    after = keyspace.disk_space(),
                    "rewrote an ingested archive keyspace"
                );

                Ok(())
            })
            .map_err(io_err)
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use dolos_core::{
        ArchiveStore as _, ArchiveWriter as _, BlockBody, NamespaceType, StateSchema,
    };

    use super::*;
    use crate::preset::Preset;

    const NS: Namespace = "logs";

    fn open(path: &Path) -> ArchiveStore {
        let mut schema = StateSchema::default();
        schema.insert(NS, NamespaceType::KeyValue);

        ArchiveStore::open(schema, path, None, Preset::Standard).unwrap()
    }

    fn point(slot: BlockSlot) -> ChainPoint {
        ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new([slot as u8; 32]))
    }

    fn body(slot: BlockSlot, tag: u8) -> RawBlock {
        Arc::new(format!("block {slot} {tag} ").repeat(16).into_bytes())
    }

    fn key(slot: BlockSlot) -> LogKey {
        LogKey::of(slot, &7u32)
    }

    fn ingest(store: &ArchiveStore) -> Box<Ingest<'_>> {
        Box::new(Ingest::new(store, L0_GUARD))
    }

    fn table_files(keyspace: &Keyspace) -> usize {
        std::fs::read_dir(keyspace.path().join("tables")).map_or(0, |dir| dir.count())
    }

    /// The blocks and rows of the order-break case, as a store reads them.
    fn read_back(store: &ArchiveStore) -> (Vec<Vec<BlockBody>>, Vec<Option<EntityValue>>) {
        let blocks = [10, 20, 30]
            .iter()
            .map(|slot| store.get_blocks_by_slot(slot).unwrap())
            .collect();

        let keys = [key(5), key(6)];
        let logs = store
            .read_logs(NS, &keys.iter().collect::<Vec<_>>())
            .unwrap();

        (blocks, logs)
    }

    /// A slot written twice and a log key written twice each close a run and
    /// open the next; the later row wins, as in a batch, and still does once
    /// the store is reopened.
    #[test]
    fn an_order_break_starts_a_run_and_the_later_row_wins() {
        let blocks = vec![
            (point(10), body(10, 0)),
            (point(20), body(20, 0)),
            (point(20), body(20, 1)),
            (point(30), body(30, 0)),
        ];
        let rows = vec![(key(5), vec![1]), (key(5), vec![2]), (key(6), vec![3])];

        let batched = tempfile::tempdir().unwrap();
        let batched = open(batched.path());
        let writer = batched.start_writer().unwrap();
        for (point, body) in &blocks {
            writer.apply(point, body).unwrap();
        }
        for (key, value) in &rows {
            writer.write_log(NS, key, value).unwrap();
        }
        writer.commit().unwrap();
        let expected = read_back(&batched);
        assert_eq!(expected.0[1].len(), 2, "the slot holds both blocks");
        assert_eq!(expected.1[0], Some(vec![2]));

        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());

        let mut layer = ingest(&store);
        layer.blocks(blocks).unwrap();
        layer.logs(NS, rows).unwrap();
        assert_eq!((layer.blocks.runs, layer.logs.runs), (2, 2));
        layer.finish().unwrap();
        store.persist().unwrap();

        assert_eq!(read_back(&store), expected);

        store.shutdown().unwrap();
        drop(store);

        assert_eq!(read_back(&open(dir.path())), expected, "after a reopen");
    }

    /// A process that dies before `finish` leaves no row, and the reopen
    /// deletes the tables the run wrote.
    #[test]
    fn a_run_dropped_before_finish_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let before = table_files(&store.logs);

        let mut layer = ingest(&store);
        layer.logs(NS, vec![(key(5), vec![1])]).unwrap();
        assert!(table_files(&store.logs) > before, "the run wrote no table");
        drop(layer);

        assert_eq!(read_back(&store).1, vec![None, None]);

        store.shutdown().unwrap();
        drop(store);

        let store = open(dir.path());
        assert_eq!(read_back(&store).1, vec![None, None]);
        assert_eq!(table_files(&store.logs), before);
    }

    /// Past lsm-tree's 255 runs a level: the guard holds L0 under it, so the
    /// version file still reads back.
    #[test]
    fn three_hundred_runs_into_one_keyspace_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());

        for slot in 0..300 {
            let mut layer = ingest(&store);
            layer.logs(NS, vec![(key(slot), vec![slot as u8])]).unwrap();
            layer.finish().unwrap();
            assert!(store.logs.tree.l0_run_count() <= L0_GUARD);
        }

        store.persist().unwrap();
        store.shutdown().unwrap();
        drop(store);

        let store = open(dir.path());
        let keys: Vec<_> = (0..300).map(key).collect();
        let read = store
            .read_logs(NS, &keys.iter().collect::<Vec<_>>())
            .unwrap();

        assert!(read
            .iter()
            .enumerate()
            .all(|(slot, value)| *value == Some(vec![slot as u8])));
    }

    /// A backlog that compacts only when asked, as fjall does once its queue
    /// is empty.
    struct Backlog {
        runs: AtomicUsize,
        kicks: AtomicUsize,
    }

    impl Compacts for Backlog {
        fn l0_runs(&self) -> usize {
            self.runs.load(Ordering::SeqCst)
        }

        fn kick(&self) -> fjall::Result<()> {
            self.kicks.fetch_add(1, Ordering::SeqCst);
            self.runs.store(1, Ordering::SeqCst);

            Ok(())
        }
    }

    #[test]
    fn a_waiting_writer_asks_for_the_compaction_that_wakes_it() {
        let backlog = Arc::new(Backlog {
            runs: AtomicUsize::new(L0_GUARD + 6),
            kicks: AtomicUsize::new(0),
        });

        let (sender, receiver) = std::sync::mpsc::channel();
        let waiting = Arc::clone(&backlog);
        std::thread::spawn(move || {
            let _ = sender.send(wait_for_l0(waiting.as_ref(), L0_GUARD).map(|_| ()));
        });

        let woken = receiver.recv_timeout(Duration::from_secs(10));

        assert!(matches!(woken, Ok(Ok(()))), "the writer is still waiting");
        assert_eq!(backlog.kicks.load(Ordering::SeqCst), 1);
    }

    /// Ingested tables are uncompressed; the settle rewrites them compressed
    /// and keeps every row.
    #[test]
    fn the_settle_leaves_compressed_tables() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());

        let value = b"a compressible log value ".repeat(40);
        let rows: Vec<_> = (0..20_000).map(|slot| (key(slot), value.clone())).collect();

        let mut layer = ingest(&store);
        layer.logs(NS, rows).unwrap();
        layer.finish().unwrap();

        let ingested = store.logs.disk_space();

        store.settle_ingested().unwrap().join().unwrap();

        let settled = store.logs.disk_space();
        assert!(
            settled * 4 < ingested,
            "{settled} bytes settled from {ingested}"
        );

        let keys: Vec<_> = (0..20_000).map(key).collect();
        let read = store
            .read_logs(NS, &keys.iter().collect::<Vec<_>>())
            .unwrap();
        assert!(read
            .iter()
            .all(|row| row.as_deref() == Some(value.as_slice())));
    }
}
