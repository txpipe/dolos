//! Compressed block segments behave like the memory oracle through every
//! archive operation.
//!
//! Two stores receive the same history: the builtin memory archive as the
//! oracle and a fjall archive writing frames to disk. Every operation the
//! node performs on an archive — point and range reads from either end,
//! rollback within and across segment boundaries, a resumed import, a
//! truncation and a prune — is applied to both and the results compared,
//! then the fjall store is reopened to prove what it holds is on disk and
//! its segment files are walked with libzstd alone.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use dolos_core::{
    builtin::MemoryArchiveStore, ArchiveStore as CoreArchiveStore, ArchiveWriter as _, BlockBody,
    BlockSlot, ChainPoint, StateSchema,
};
use dolos_fjall::archive::ArchiveStore;
use dolos_fjall::flatfiles::{BUNDLED_DICTIONARY, MAX_BODY_BYTES, SLOTS_PER_SEGMENT};

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

/// The oracle and the on-disk store, over the same history.
struct Pair {
    memory: MemoryArchiveStore,
    /// Taken out only while the store is closed for a reopen: fjall locks
    /// its directory, so the drop has to finish before the open starts.
    fjall: Option<ArchiveStore>,
    dir: tempfile::TempDir,
}

impl Pair {
    fn new() -> Self {
        let memory = MemoryArchiveStore::new(StateSchema::default());
        let dir = tempfile::tempdir().unwrap();
        let fjall = open(dir.path());
        write_history(&fjall);
        write_history(&memory);
        Self {
            memory,
            fjall: Some(fjall),
            dir,
        }
    }

    fn fjall(&self) -> &ArchiveStore {
        self.fjall.as_ref().expect("the fjall store is open")
    }

    fn assert_agree(&self, what: &str) {
        let expected = view(&self.memory);
        assert_eq!(view(self.fjall()), expected, "fjall diverged: {what}");
        assert!(!expected.forward.is_empty(), "{what}: the fixture is empty");
        self.assert_segments_are_frames(what);
    }

    /// Close the fjall store and open it again from disk.
    fn reopen(&mut self) {
        drop(self.fjall.take());
        self.fjall = Some(open(self.dir.path()));
    }

    fn segment_path(&self, segment: u32) -> std::path::PathBuf {
        self.dir.path().join(format!("{segment:06}.segment"))
    }

    fn segment_files(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(self.dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".segment"))
            .collect();
        names.sort();
        names
    }

    /// Every segment file is nothing but zstd frames for the bundled
    /// dictionary, and every body the oracle holds is one of them. Frames
    /// nothing points at — a removed block's, a repeated import's — may
    /// remain as dead space.
    fn assert_segments_are_frames(&self, what: &str) {
        let dictionary_id = zstd::zstd_safe::get_dict_id_from_dict(BUNDLED_DICTIONARY).unwrap();
        let mut decompressor =
            zstd::bulk::Decompressor::with_dictionary(BUNDLED_DICTIONARY).unwrap();
        let mut on_disk: Vec<BlockBody> = Vec::new();
        for name in self.segment_files() {
            let bytes = fs::read(self.dir.path().join(&name)).unwrap();
            assert!(!bytes.is_empty(), "{what}: {name} is empty");
            let mut cursor = 0;
            while cursor < bytes.len() {
                let size = zstd::zstd_safe::find_frame_compressed_size(&bytes[cursor..])
                    .unwrap_or_else(|_| panic!("{what}: {name} at {cursor} is not a frame"));
                let frame = &bytes[cursor..cursor + size];
                assert_eq!(
                    zstd::zstd_safe::get_dict_id_from_frame(frame),
                    Some(dictionary_id),
                    "{what}: {name} at {cursor} was not written with the bundled dictionary"
                );
                on_disk.push(decompressor.decompress(frame, MAX_BODY_BYTES).unwrap());
                cursor += size;
            }
        }
        on_disk.sort();
        for (slot, body) in view(&self.memory).forward {
            assert!(
                on_disk.binary_search(&body).is_ok(),
                "{what}: the block at slot {slot} is not a frame on disk"
            );
        }
    }
}

#[test]
fn a_fresh_store_compresses_from_the_first_block_and_reads_like_the_oracle() {
    let mut pair = Pair::new();
    pair.assert_agree("after writing");
    assert_eq!(
        pair.segment_files(),
        vec![
            "000000.segment",
            "000001.segment",
            "000002.segment",
            "000003.segment"
        ]
    );
    let raw: usize = view(&pair.memory)
        .forward
        .iter()
        .map(|(_, body)| body.len())
        .sum();
    let on_disk: u64 = (0..4)
        .map(|s| fs::metadata(pair.segment_path(s)).unwrap().len())
        .sum();
    assert!(
        on_disk < raw as u64,
        "{on_disk} bytes on disk for {raw} raw"
    );

    pair.reopen();
    pair.assert_agree("after reopening");
}

#[test]
fn rollback_within_and_across_segments_preserves_the_rest() {
    let mut pair = Pair::new();
    let segment0_before = fs::read(pair.segment_path(0)).unwrap();

    // Within segment 1: the shared slot unwinds newest first, then the
    // older block, then a block earlier in the same segment.
    let within = [slot(1, 8), slot(1, 8), slot(1, 4)];
    undo(pair.fjall(), &within);
    undo(&pair.memory, &within);
    pair.assert_agree("after undoing inside a segment");
    assert_eq!(
        fs::read(pair.segment_path(0)).unwrap(),
        segment0_before,
        "a rollback in one segment must not touch another"
    );

    // Across: from the tip back into segment 0.
    let across = [slot(3, 1), slot(2, 6), slot(2, 2), slot(1, 0), slot(0, 13)];
    undo(pair.fjall(), &across);
    undo(&pair.memory, &across);
    pair.assert_agree("after undoing across segments");
    let segment0_cut = fs::read(pair.segment_path(0)).unwrap();
    assert_eq!(
        segment0_cut[..],
        segment0_before[..segment0_cut.len()],
        "retained frames are not rewritten by the cut"
    );
    assert!(!pair.segment_path(3).exists(), "segment 3 is emptied");

    // Appending after the rollback continues where the cut left off.
    let more = [
        (slot(0, 13), body(slot(0, 13), 7)),
        (slot(1, 2), body(slot(1, 2), 7)),
        (slot(2, 3), body(slot(2, 3), 7)),
    ];
    write(pair.fjall(), &more);
    write(&pair.memory, &more);
    pair.assert_agree("after appending past the rollback");
    assert_eq!(
        fs::read(pair.segment_path(0)).unwrap()[..segment0_cut.len()],
        segment0_cut[..],
        "retained frames are not rewritten by the append"
    );

    pair.reopen();
    pair.assert_agree("after reopening");
}

#[test]
fn truncate_front_cuts_at_the_frame_in_and_between_segments() {
    let mut pair = Pair::new();

    // Inside segment 0: the cut lands before the first frame past the slot.
    // Later segments keep their files; their frames are dead space.
    let before = fs::metadata(pair.segment_path(0)).unwrap().len();
    let after = point(slot(0, 5));
    pair.fjall().truncate_front(&after).unwrap();
    pair.memory.truncate_front(&after).unwrap();
    pair.assert_agree("after truncate_front inside segment 0");
    assert!(fs::metadata(pair.segment_path(0)).unwrap().len() < before);
    assert_eq!(pair.segment_files().len(), 4);

    let more = [(slot(0, 6), body(slot(0, 6), 9))];
    write(pair.fjall(), &more);
    write(&pair.memory, &more);
    pair.assert_agree("after appending past the cut");

    pair.reopen();
    pair.assert_agree("after reopening");

    // At a boundary: the cut lands at segment 1's first frame, so its file
    // goes whole.
    let pair = Pair::new();
    let after = point(slot(0, 13));
    pair.fjall().truncate_front(&after).unwrap();
    pair.memory.truncate_front(&after).unwrap();
    pair.assert_agree("after truncate_front at the boundary");
    assert_eq!(
        pair.segment_files(),
        vec!["000000.segment", "000002.segment", "000003.segment"]
    );
}

#[test]
fn a_repeated_import_keeps_the_original_frame() {
    let mut pair = Pair::new();
    let before = fs::read(pair.segment_path(0)).unwrap();

    // The same block again, the way a resumed restore rewrites a layer: the
    // copy is appended and nothing points at it.
    let again = [(slot(0, 5), body(slot(0, 5), 0))];
    write(pair.fjall(), &again);
    write(&pair.memory, &again);
    pair.assert_agree("after importing a block again");
    let after = fs::read(pair.segment_path(0)).unwrap();
    assert_eq!(after[..before.len()], before[..]);
    assert!(after.len() > before.len());

    pair.reopen();
    pair.assert_agree("after reopening");
}

#[test]
fn pruning_drops_whole_segments_and_survives_a_reopen() {
    let mut pair = Pair::new();

    // The history spans slots 1 .. 3·SEGMENT+1; keeping two segments' worth
    // moves the cutoff into segment 1, which prunes segment 0 whole.
    let max_slots = 2 * SLOTS_PER_SEGMENT;
    assert!(pair.fjall().prune_history(max_slots, None).unwrap());
    assert!(pair.memory.prune_history(max_slots, None).unwrap());
    pair.assert_agree("after pruning");
    assert_eq!(
        pair.segment_files(),
        vec!["000001.segment", "000002.segment", "000003.segment"]
    );

    pair.reopen();
    pair.assert_agree("after reopening");
}
