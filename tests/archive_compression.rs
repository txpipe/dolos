//! Compressed block segments behave like raw ones through every archive
//! operation.
//!
//! Three stores receive the same history: the builtin memory archive as the
//! oracle, a fjall archive left raw as the byte-level control, and a fjall
//! archive whose segments are sealed into a mix of representations —
//! per-block with a dictionary, chunked, and raw. Every operation the node
//! performs on an archive — point and range reads from either end, rollback
//! within and across segment boundaries, a resumed import, a truncation and
//! a prune — is applied to all three and the results compared, then the
//! mixed store is reopened to prove what it holds is on disk.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use dolos_core::{
    builtin::MemoryArchiveStore, ArchiveStore as CoreArchiveStore, ArchiveWriter as _, BlockBody,
    BlockSlot, ChainPoint, StateSchema,
};
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::{
    compressed::{Dictionary, DictionaryDir, WriterOptions},
    Representation, DICTIONARIES_DIR, SLOTS_PER_SEGMENT,
};

fn open(dir: &Path) -> ArchiveStore {
    let config = dolos_core::config::FjallArchiveConfig {
        cache: Some(16),
        flush_on_commit: Some(false),
        worker_threads: Some(1),
        ..Default::default()
    };
    ArchiveStore::open(StateSchema::default(), dir, &config).expect("open the fjall archive")
}

fn point(slot: u64) -> ChainPoint {
    ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new([0u8; 32]))
}

fn slot(segment: u64, k: u64) -> BlockSlot {
    segment * SLOTS_PER_SEGMENT + k
}

/// A compressible body of a few hundred bytes, distinct per slot and tag.
fn body(slot: BlockSlot, tag: u8) -> Vec<u8> {
    format!("block at slot {slot} tag {tag} ")
        .repeat(12)
        .into_bytes()
}

fn dictionary() -> Dictionary {
    Dictionary::new(body(0, 0))
}

fn write<S: CoreArchiveStore>(store: &S, blocks: &[(BlockSlot, Vec<u8>)]) {
    let writer = store.start_writer().unwrap();
    for (slot, body) in blocks {
        writer
            .apply(&point(*slot), &Arc::new(body.clone()))
            .unwrap();
    }
    writer.commit().unwrap();
}

fn undo<S: CoreArchiveStore>(store: &S, slots: &[BlockSlot]) {
    let writer = store.start_writer().unwrap();
    for slot in slots {
        writer.undo(&point(*slot)).unwrap();
    }
    writer.commit().unwrap();
}

/// Four segments, with a slot in segments 0 and 1 holding two blocks (the
/// Byron boundary shape) and every segment written in more than one batch.
fn write_history<S: CoreArchiveStore>(store: &S) {
    write(
        store,
        &[
            (slot(0, 1), body(slot(0, 1), 0)),
            (slot(0, 5), body(slot(0, 5), 0)),
            (slot(0, 9), body(slot(0, 9), 0)),
            (slot(1, 0), body(slot(1, 0), 0)),
            (slot(1, 4), body(slot(1, 4), 0)),
        ],
    );
    write(
        store,
        &[
            (slot(0, 9), body(slot(0, 9), 1)),
            (slot(0, 13), body(slot(0, 13), 0)),
            (slot(1, 8), body(slot(1, 8), 0)),
            (slot(1, 8), body(slot(1, 8), 1)),
        ],
    );
    write(
        store,
        &[
            (slot(2, 2), body(slot(2, 2), 0)),
            (slot(2, 6), body(slot(2, 6), 0)),
            (slot(3, 1), body(slot(3, 1), 0)),
        ],
    );
}

fn seal_mixed(dir: &Path, store: &ArchiveStore) {
    let dictionary = dictionary();
    DictionaryDir::new(dir.join(DICTIONARIES_DIR))
        .install(&dictionary)
        .unwrap();
    store
        .seal_segment(0, &WriterOptions::per_block().with_dictionary(dictionary))
        .unwrap();
    store
        .seal_segment(1, &WriterOptions::chunked(1024))
        .unwrap();
}

fn representations(store: &ArchiveStore) -> BTreeMap<u32, Representation> {
    store
        .segments()
        .unwrap()
        .into_iter()
        .map(|info| (info.segment_id, info.representation))
        .collect()
}

/// Everything an archive answers about its blocks.
#[derive(Debug, PartialEq, Eq)]
struct View {
    forward: Vec<(BlockSlot, BlockBody)>,
    reverse: Vec<(BlockSlot, BlockBody)>,
    interleaved: Vec<(BlockSlot, BlockBody)>,
    bounded: Vec<(BlockSlot, BlockBody)>,
    by_slot: BTreeMap<BlockSlot, Vec<BlockBody>>,
    first_by_slot: BTreeMap<BlockSlot, Option<BlockBody>>,
    tip: Option<(BlockSlot, BlockBody)>,
}

fn view<S: CoreArchiveStore>(store: &S) -> View {
    let forward: Vec<_> = store.get_range(None, None).unwrap().collect();
    let reverse: Vec<_> = store.get_range(None, None).unwrap().rev().collect();

    let mut iter = store.get_range(None, None).unwrap();
    let mut front = Vec::new();
    let mut back = Vec::new();
    while let Some(item) = iter.next() {
        front.push(item);
        match iter.next_back() {
            Some(item) => back.push(item),
            None => break,
        }
    }
    back.reverse();
    front.extend(back);

    let bounded = store
        .get_range(Some(slot(0, 5)), Some(slot(2, 6)))
        .unwrap()
        .collect();

    let slots: Vec<BlockSlot> = (0..4)
        .flat_map(|s| (0..16).map(move |k| slot(s, k)))
        .collect();
    let by_slot = slots
        .iter()
        .map(|s| (*s, store.get_blocks_by_slot(s).unwrap()))
        .filter(|(_, blocks)| !blocks.is_empty())
        .collect();
    let first_by_slot = slots
        .iter()
        .map(|s| (*s, store.get_block_by_slot(s).unwrap()))
        .filter(|(_, block)| block.is_some())
        .collect();

    View {
        forward,
        reverse,
        interleaved: front,
        bounded,
        by_slot,
        first_by_slot,
        tip: store.get_tip().unwrap(),
    }
}

/// The oracle, the raw control and the mixed store, over the same history.
struct Trio {
    memory: MemoryArchiveStore,
    raw: ArchiveStore,
    /// Taken out only while the store is closed for a reopen: fjall locks
    /// its directory, so the drop has to finish before the open starts.
    mixed: Option<ArchiveStore>,
    _raw_dir: tempfile::TempDir,
    mixed_dir: tempfile::TempDir,
}

impl Trio {
    fn new() -> Self {
        let memory = MemoryArchiveStore::new(StateSchema::default());
        let raw_dir = tempfile::tempdir().unwrap();
        let mixed_dir = tempfile::tempdir().unwrap();
        let raw = open(raw_dir.path());
        let mixed = open(mixed_dir.path());
        for store in [&raw, &mixed] {
            write_history(store);
        }
        write_history(&memory);
        seal_mixed(mixed_dir.path(), &mixed);
        Self {
            memory,
            raw,
            mixed: Some(mixed),
            _raw_dir: raw_dir,
            mixed_dir,
        }
    }

    fn mixed(&self) -> &ArchiveStore {
        self.mixed.as_ref().expect("the mixed store is open")
    }

    /// The two on-disk stores, for applying one mutation to both.
    fn fjall(&self) -> [&ArchiveStore; 2] {
        [&self.raw, self.mixed()]
    }

    fn assert_agree(&self, what: &str) {
        let expected = view(&self.memory);
        assert_eq!(view(&self.raw), expected, "raw fjall diverged: {what}");
        assert_eq!(view(self.mixed()), expected, "mixed fjall diverged: {what}");
        assert!(!expected.forward.is_empty(), "{what}: the fixture is empty");
    }

    /// Close the mixed store and open it again from disk.
    fn reopen_mixed(&mut self) {
        drop(self.mixed.take());
        self.mixed = Some(open(self.mixed_dir.path()));
    }

    fn segment_files(&self, segment: u32) -> Vec<String> {
        let prefix = format!("{segment:06}.");
        let mut names: Vec<String> = fs::read_dir(self.mixed_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(&prefix))
            .collect();
        names.sort();
        names
    }
}

#[test]
fn a_mixed_store_reads_like_a_raw_one() {
    let mut trio = Trio::new();

    assert_eq!(
        representations(trio.mixed()),
        BTreeMap::from([
            (0, Representation::Compressed),
            (1, Representation::Compressed),
            (2, Representation::Raw),
            (3, Representation::Raw),
        ])
    );
    let sealed = trio.mixed().segments().unwrap();
    assert!(sealed[0].metadata.as_ref().unwrap().dictionary.is_some());
    assert!(sealed[1].metadata.as_ref().unwrap().dictionary.is_none());

    trio.assert_agree("after sealing");

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
    assert_eq!(trio.segment_files(0), vec!["000000.zseg"]);
    assert_eq!(trio.segment_files(1), vec!["000001.zseg"]);
}

#[test]
fn rollback_within_and_across_compressed_segments_preserves_the_rest() {
    let mut trio = Trio::new();

    // Within segment 1: the shared slot unwinds newest first, then the
    // older block, then a block earlier in the same compressed segment.
    let within = [slot(1, 8), slot(1, 8), slot(1, 4)];
    for store in trio.fjall() {
        undo(store, &within);
    }
    undo(&trio.memory, &within);
    trio.assert_agree("after undoing inside a compressed segment");
    assert_eq!(
        trio.mixed().segments().unwrap()[1].representation,
        Representation::Raw
    );
    assert_eq!(
        trio.mixed().segments().unwrap()[0].representation,
        Representation::Compressed,
        "a rollback in one segment must not convert another"
    );

    // Across: from the raw tip back through the compressed segment 0.
    let across = [slot(3, 1), slot(2, 6), slot(2, 2), slot(1, 0), slot(0, 13)];
    for store in trio.fjall() {
        undo(store, &across);
    }
    undo(&trio.memory, &across);
    trio.assert_agree("after undoing across segments");

    // Appending after the rollback continues where the cut left off.
    let more = [
        (slot(0, 13), body(slot(0, 13), 7)),
        (slot(1, 2), body(slot(1, 2), 7)),
        (slot(2, 3), body(slot(2, 3), 7)),
    ];
    for store in trio.fjall() {
        write(store, &more);
    }
    write(&trio.memory, &more);
    trio.assert_agree("after appending past the rollback");

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
    for segment in 0..3 {
        assert_eq!(
            trio.segment_files(segment),
            vec![format!("{segment:06}.segment")],
            "segment {segment} keeps only its raw file"
        );
    }
}

#[test]
fn truncate_front_at_a_segment_boundary_removes_the_compressed_segment_whole() {
    let mut trio = Trio::new();

    // The cut lands before segment 1's first byte: the segment goes as a
    // file, in its compressed form, and segment 0 is not touched.
    let after = point(slot(0, 13));
    for store in trio.fjall() {
        store.truncate_front(&after).unwrap();
    }
    trio.memory.truncate_front(&after).unwrap();
    trio.assert_agree("after truncate_front at the boundary");
    assert!(trio.segment_files(1).is_empty());
    assert_eq!(trio.segment_files(0), vec!["000000.zseg"]);

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
}

#[test]
fn truncate_front_inside_a_compressed_segment_thaws_it() {
    let mut trio = Trio::new();

    let after = point(slot(0, 5));
    for store in trio.fjall() {
        store.truncate_front(&after).unwrap();
    }
    trio.memory.truncate_front(&after).unwrap();
    trio.assert_agree("after truncate_front inside segment 0");
    assert_eq!(trio.segment_files(0), vec!["000000.segment"]);

    // Appending after the cut lands at the cut.
    let more = [(slot(0, 6), body(slot(0, 6), 9))];
    for store in trio.fjall() {
        write(store, &more);
    }
    write(&trio.memory, &more);
    trio.assert_agree("after appending past the cut");

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
}

#[test]
fn a_repeated_import_keeps_the_original_location() {
    let mut trio = Trio::new();
    let before: BTreeMap<u32, u64> = trio
        .mixed()
        .segments()
        .unwrap()
        .into_iter()
        .map(|info| (info.segment_id, info.logical_len))
        .collect();

    // The same block again, the way a resumed restore rewrites a layer.
    let again = [(slot(0, 5), body(slot(0, 5), 0))];
    for store in trio.fjall() {
        write(store, &again);
    }
    write(&trio.memory, &again);
    trio.assert_agree("after importing a block again");

    // The copy went to the end of the segment and nothing points at it; the
    // index still describes the original bytes, which the seal preserved.
    let after = trio.mixed().segments().unwrap();
    assert_eq!(after[0].representation, Representation::Raw);
    assert_eq!(
        after[0].logical_len,
        before[&0] + body(slot(0, 5), 0).len() as u64
    );

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
}

#[test]
fn pruning_drops_compressed_segments_and_survives_a_reopen() {
    let mut trio = Trio::new();

    // The history spans slots 1 .. 3·SEGMENT+1; keeping two segments' worth
    // moves the cutoff into segment 1, which prunes segment 0 whole.
    let max_slots = 2 * SLOTS_PER_SEGMENT;
    for store in trio.fjall() {
        assert!(store.prune_history(max_slots, None).unwrap());
    }
    assert!(trio.memory.prune_history(max_slots, None).unwrap());
    trio.assert_agree("after pruning");

    assert!(trio.segment_files(0).is_empty());
    assert_eq!(trio.segment_files(1), vec!["000001.zseg"]);
    assert!(!representations(trio.mixed()).contains_key(&0));

    trio.reopen_mixed();
    trio.assert_agree("after reopening");
}

#[test]
fn opening_refuses_a_sealed_segment_whose_dictionary_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    write_history(&store);
    let dictionary = dictionary();
    let installed = DictionaryDir::new(dir.path().join(DICTIONARIES_DIR))
        .install(&dictionary)
        .unwrap();
    store
        .seal_segment(
            0,
            &WriterOptions::per_block().with_dictionary(dictionary.clone()),
        )
        .unwrap();
    drop(store);

    fs::remove_file(&installed).unwrap();
    let config = dolos_core::config::FjallArchiveConfig::default();
    let error = match ArchiveStore::open(StateSchema::default(), dir.path(), &config) {
        Ok(_) => panic!("the archive opened without the dictionary its segment names"),
        Err(e) => e.to_string(),
    };
    assert!(error.contains("000000"), "{error}");
    assert!(error.contains(&dictionary.id().to_string()), "{error}");
}
