//! The store's frames, locations and failure boundaries, read back through
//! the store and through libzstd alone.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::sync::Arc;

use dolos_flatfiles::{
    frame_bound, BlockLocation, FlatFileStore, BUNDLED_DICTIONARY, ENCODE_WINDOW_BYTES,
    MAX_BODY_BYTES,
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Compressible bodies of varied size, distinct per seed.
fn bodies(seed: u64, count: usize, min: usize, max: usize) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed | 1);
    let words: Vec<String> = (0..40).map(|i| format!("word{i}_")).collect();
    (0..count)
        .map(|_| {
            let len = min + rng.below((max - min + 1) as u64) as usize;
            let mut body = Vec::with_capacity(len + 8);
            while body.len() < len {
                body.extend_from_slice(words[rng.below(40) as usize].as_bytes());
            }
            body.truncate(len);
            body
        })
        .collect()
}

fn items(segment: u32, bodies: &[Vec<u8>]) -> Vec<(u32, &[u8])> {
    bodies.iter().map(|b| (segment, b.as_slice())).collect()
}

fn file_len(store: &FlatFileStore, segment: u32) -> u64 {
    fs::metadata(store.segment_path(segment)).unwrap().len()
}

/// Walk a segment file with libzstd alone: `(offset, length)` per frame.
fn walk_frames(bytes: &[u8]) -> Vec<(u64, u32)> {
    let mut frames = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let size = zstd::zstd_safe::find_frame_compressed_size(&bytes[cursor..])
            .unwrap_or_else(|_| panic!("frame at {cursor} is not a zstd frame"));
        frames.push((cursor as u64, size as u32));
        cursor += size;
    }
    frames
}

fn decode_independently(frame: &[u8]) -> Vec<u8> {
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(BUNDLED_DICTIONARY).unwrap();
    decompressor.decompress(frame, MAX_BODY_BYTES).unwrap()
}

#[test]
fn a_segment_is_a_sequence_of_frames_from_the_first_append() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let bodies = bodies(1, 40, 100, 3_000);
    let locations = store.append_batch(&items(0, &bodies)).unwrap();

    let bytes = fs::read(store.segment_path(0)).unwrap();
    let frames = walk_frames(&bytes);
    assert_eq!(frames.len(), bodies.len());
    let dictionary_id = zstd::zstd_safe::get_dict_id_from_dict(BUNDLED_DICTIONARY).unwrap();
    for ((offset, length), (loc, body)) in frames.iter().zip(locations.iter().zip(&bodies)) {
        assert_eq!((loc.offset, loc.length), (*offset, *length));
        let frame = &bytes[*offset as usize..(*offset + *length as u64) as usize];
        assert_eq!(
            zstd::zstd_safe::get_dict_id_from_frame(frame),
            Some(dictionary_id)
        );
        assert_eq!(&decode_independently(frame), body);
    }
    assert!(
        bytes.len() < bodies.iter().map(Vec::len).sum::<usize>(),
        "the segment is not smaller than its bodies"
    );
}

#[test]
fn reads_return_original_bytes_for_every_shape_of_body() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let mut rng = Rng(7);
    let random: Vec<u8> = (0..(1 << 20)).map(|_| rng.next() as u8).collect();
    let shapes: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0u8; 1],
        b"short".to_vec(),
        vec![0xAB; 200_000],
        random,
        bodies(3, 1, 90_000, 90_000).remove(0),
    ];
    let mut batch: Vec<(u32, &[u8])> = shapes
        .iter()
        .enumerate()
        .map(|(i, body)| (i as u32 % 2, body.as_slice()))
        .collect();
    batch.push((5, shapes[2].as_slice()));

    let locations = store.append_batch(&batch).unwrap();
    for (loc, (_, body)) in locations.iter().zip(&batch) {
        assert_eq!(&store.read(loc).unwrap(), body, "{loc:?}");
    }
    assert_eq!(store.resource_stats().writers, 1, "only the newest segment");
}

#[test]
fn a_body_over_the_limit_is_refused_and_nothing_is_written() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let first = bodies(4, 1, 500, 500).remove(0);
    let before = store.append_batch(&[(0, first.as_slice())]).unwrap();
    let len_before = file_len(&store, 0);

    let oversized = vec![0u8; MAX_BODY_BYTES + 1];
    let err = store
        .append_batch(&[(0, oversized.as_slice()), (1, first.as_slice())])
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(file_len(&store, 0), len_before);
    assert_eq!(store.read(&before[0]).unwrap(), first);

    let limit = vec![7u8; MAX_BODY_BYTES];
    let loc = store.append_batch(&[(0, limit.as_slice())]).unwrap()[0];
    assert_eq!(store.read(&loc).unwrap(), limit);
}

#[test]
fn a_frame_declaring_an_oversized_body_is_refused_before_decoding() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
    compressor.include_contentsize(true).unwrap();
    let frame = compressor.compress(&vec![0u8; MAX_BODY_BYTES + 1]).unwrap();
    assert!(
        frame.len() < 8192,
        "zeros should compress to almost nothing"
    );

    let mut file = File::create(store.segment_path(0)).unwrap();
    file.write_all(&frame).unwrap();
    let loc = BlockLocation {
        segment_id: 0,
        offset: 0,
        length: frame.len() as u32,
    };
    let err = store.read(&loc).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("over the"), "{err}");
}

#[test]
fn corrupted_frames_fail_and_their_neighbors_still_read() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let bodies = bodies(5, 6, 400, 900);
    let locations = store.append_batch(&items(0, &bodies)).unwrap();
    let path = store.segment_path(0);
    let clean = fs::read(&path).unwrap();

    let target = locations[3];
    for (name, at) in [
        (
            "payload",
            target.offset as usize + target.length as usize / 2,
        ),
        ("header", target.offset as usize),
    ] {
        let mut corrupted = clean.clone();
        corrupted[at] ^= 0x5A;
        fs::write(&path, &corrupted).unwrap();
        let err = store.read(&target).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{name}: {err}");
        assert!(err.to_string().contains("segment 000000"), "{name}: {err}");
        assert_eq!(store.read(&locations[2]).unwrap(), bodies[2], "{name}");
        assert_eq!(store.read(&locations[4]).unwrap(), bodies[4], "{name}");
    }

    let short = BlockLocation {
        length: target.length - 1,
        ..target
    };
    fs::write(&path, &clean).unwrap();
    assert!(store.read(&short).is_err());
    let past_end = BlockLocation {
        offset: clean.len() as u64,
        length: 10,
        segment_id: 0,
    };
    assert_eq!(
        store.read(&past_end).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        store
            .read(&BlockLocation {
                segment_id: 9,
                ..target
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
fn a_torn_tail_is_never_named_and_never_blocks_earlier_frames() {
    let dir = tempfile::tempdir().unwrap();
    let store = FlatFileStore::new(dir.path()).unwrap();
    let bodies = bodies(6, 3, 600, 900);
    let locations = store.append_batch(&items(0, &bodies)).unwrap();
    drop(store);

    // A crash mid-write leaves half of the last frame, whatever the index
    // committed before it.
    let path = dir.path().join("000000.segment");
    let torn_len = locations[2].offset + locations[2].length as u64 / 2;
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(torn_len)
        .unwrap();

    let store = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(store.read(&locations[0]).unwrap(), bodies[0]);
    assert_eq!(store.read(&locations[1]).unwrap(), bodies[1]);
    assert_eq!(
        store.read(&locations[2]).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );

    let more = self::bodies(7, 2, 300, 300);
    let after = store.append_batch(&items(0, &more)).unwrap();
    assert_eq!(
        after[0].offset, torn_len,
        "the retry lands past the torn tail"
    );
    for (loc, body) in after.iter().zip(&more) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
    assert_eq!(store.read(&locations[1]).unwrap(), bodies[1]);
    assert!(store.read(&locations[2]).is_err());

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(reopened.read(&after[1]).unwrap(), more[1]);
    assert_eq!(reopened.read(&locations[0]).unwrap(), bodies[0]);
}

#[test]
fn oversized_refusal_leaves_the_entire_batch_unwritten_and_retry_continues() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let first = bodies(8, 2, 500, 500);
    let before = store.append_batch(&items(0, &first)).unwrap();
    let len_before = file_len(&store, 0);

    let good = bodies(9, 1, 400, 400).remove(0);
    let oversized = vec![1u8; MAX_BODY_BYTES + 1];
    let err = store
        .append_batch(&[(0, good.as_slice()), (0, oversized.as_slice())])
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let dead = file_len(&store, 0) - len_before;
    assert_eq!(dead, 0, "size validation precedes all writes");
    assert_eq!(
        store.resource_stats().writers,
        1,
        "the untouched handle is retained"
    );

    let retry = store.append_batch(&[(0, good.as_slice())]).unwrap()[0];
    assert_eq!(retry.offset, len_before + dead);
    assert_eq!(store.read(&retry).unwrap(), good);
    assert_eq!(store.read(&before[1]).unwrap(), first[1]);
    assert_eq!(
        walk_frames(&fs::read(store.segment_path(0)).unwrap()).len(),
        3
    );
}

#[test]
fn an_unwritable_segment_fails_the_batch_before_anything_moves() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let first = bodies(10, 1, 500, 500);
    let before = store.append_batch(&items(0, &first)).unwrap();
    fs::create_dir(dir.path().join("000001.segment")).unwrap();

    let err = store
        .append_batch(&[(0, first[0].as_slice()), (1, first[0].as_slice())])
        .unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        file_len(&store, 0),
        before[0].offset + before[0].length as u64
    );
    assert_eq!(store.read(&before[0]).unwrap(), first[0]);

    fs::remove_dir(dir.path().join("000001.segment")).unwrap();
    let retry = store
        .append_batch(&[(0, first[0].as_slice()), (1, first[0].as_slice())])
        .unwrap();
    assert_eq!(retry[0].offset, before[0].offset + before[0].length as u64);
    assert_eq!(retry[1].offset, 0);
    assert_eq!(store.read(&retry[1]).unwrap(), first[0]);
}

#[test]
fn truncation_cuts_at_a_frame_and_the_next_append_starts_there() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let bodies = bodies(11, 5, 300, 800);
    let locations = store.append_batch(&items(0, &bodies)).unwrap();
    let bytes_before = fs::read(store.segment_path(0)).unwrap();

    store.truncate(0, file_len(&store, 0) + 10).unwrap();
    assert_eq!(fs::read(store.segment_path(0)).unwrap(), bytes_before);
    store.truncate(3, 5).unwrap();
    assert!(!store.segment_path(3).exists());

    let cut = locations[3].offset;
    store.truncate(0, cut).unwrap();
    assert_eq!(file_len(&store, 0), cut);
    assert_eq!(
        &fs::read(store.segment_path(0)).unwrap()[..],
        &bytes_before[..cut as usize],
        "retained frames are not rewritten"
    );
    for (loc, body) in locations[..3].iter().zip(&bodies) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
    for loc in &locations[3..] {
        assert!(store.read(loc).is_err(), "{loc:?} survived the cut");
    }

    let again = self::bodies(12, 2, 300, 300);
    let after = store.append_batch(&items(0, &again)).unwrap();
    assert_eq!(after[0].offset, cut);
    assert_eq!(store.read(&after[1]).unwrap(), again[1]);

    store.truncate(0, 0).unwrap();
    assert!(!store.segment_path(0).exists());
    assert_eq!(
        store.read(&locations[0]).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    let fresh = store.append_batch(&items(0, &again)).unwrap();
    assert_eq!(fresh[0].offset, 0);
    assert_eq!(store.read(&fresh[0]).unwrap(), again[0]);
}

#[test]
fn pruning_removes_the_segments_below_and_releases_their_handles() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let bodies = bodies(13, 4, 300, 300);
    let batch: Vec<(u32, &[u8])> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| (i as u32, b.as_slice()))
        .collect();
    let locations = store.append_batch(&batch).unwrap();
    fs::write(dir.path().join("notes.txt"), b"not a segment").unwrap();
    assert_eq!(store.resource_stats().writers, 1);

    store.delete_segments_before(2).unwrap();

    assert!(!store.segment_path(0).exists());
    assert!(!store.segment_path(1).exists());
    assert!(store.segment_path(2).exists());
    assert!(dir.path().join("notes.txt").exists());
    assert_eq!(store.resource_stats().writers, 1);
    assert_eq!(
        store.read(&locations[0]).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(store.read(&locations[3]).unwrap(), bodies[3]);

    store.delete_segments_before(4).unwrap();
    assert_eq!(
        store.resource_stats().writers,
        0,
        "the pruned handle is released"
    );
}

#[test]
fn concurrent_reads_beside_appends_are_consistent_and_keep_bounded_decoders() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let store = Arc::new(store);
    let bodies = Arc::new(bodies(14, 200, 100, 2_000));
    let locations = Arc::new(store.append_batch(&items(0, &bodies)).unwrap());

    let readers: Vec<_> = (0..16)
        .map(|t| {
            let store = store.clone();
            let bodies = bodies.clone();
            let locations = locations.clone();
            std::thread::spawn(move || {
                let mut rng = Rng(100 + t);
                for _ in 0..500 {
                    let i = rng.below(locations.len() as u64) as usize;
                    assert_eq!(store.read(&locations[i]).unwrap(), bodies[i]);
                }
            })
        })
        .collect();

    let more = self::bodies(15, 50, 100, 2_000);
    let mut appended = Vec::new();
    for chunk in more.chunks(5) {
        appended.extend(store.append_batch(&items(1, chunk)).unwrap());
    }
    for reader in readers {
        reader.join().unwrap();
    }
    for (loc, body) in appended.iter().zip(&more) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }

    let stats = store.resource_stats();
    assert!(stats.idle_decoders <= 8, "{stats:?}");
    assert!(stats.idle_decoders >= 1, "{stats:?}");
    assert_eq!(stats.writers, 1);
}

#[test]
fn what_the_store_holds_is_bounded_by_the_work_in_flight_not_the_history() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let mut all = Vec::new();
    // Sixty segments, one batch each, then a batch that spans four of them
    // the way a bootstrap import does when its chunk crosses epochs.
    for segment in 0..60u32 {
        let bodies = bodies(200 + segment as u64, 20, 200, 2_000);
        all.push((
            bodies.clone(),
            store.append_batch(&items(segment, &bodies)).unwrap(),
        ));
        let stats = store.resource_stats();
        assert_eq!(stats.writers, 1, "segment {segment}: {stats:?}");
    }
    let spanning: Vec<Vec<u8>> = bodies(999, 4, 300, 300);
    let batch: Vec<(u32, &[u8])> = spanning
        .iter()
        .enumerate()
        .map(|(i, b)| (60 + i as u32, b.as_slice()))
        .collect();
    let locs = store.append_batch(&batch).unwrap();
    assert_eq!(store.resource_stats().writers, 1);
    for (loc, body) in locs.iter().zip(&spanning) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }

    // Re-appending to an old segment reopens it at its true end.
    let again = bodies(1000, 1, 300, 300);
    let before = fs::metadata(store.segment_path(3)).unwrap().len();
    let loc = store.append_batch(&items(3, &again)).unwrap()[0];
    assert_eq!(loc.offset, before);
    assert_eq!(store.read(&loc).unwrap(), again[0]);
    assert_eq!(store.resource_stats().writers, 1);

    // Reading everything ever written pools no more than eight decoders.
    for (bodies, locations) in &all {
        for (loc, body) in locations.iter().zip(bodies) {
            assert_eq!(&store.read(loc).unwrap(), body);
        }
    }
    let stats = store.resource_stats();
    assert!(stats.idle_decoders <= 8, "{stats:?}");
    assert_eq!(stats.writers, 1);
}

/// The two stores hold the same bytes in every one of `segments`.
fn assert_same_segments(a: &FlatFileStore, b: &FlatFileStore, segments: &[u32]) {
    for &segment in segments {
        assert_eq!(
            fs::read(a.segment_path(segment)).unwrap(),
            fs::read(b.segment_path(segment)).unwrap(),
            "segment {segment} differs"
        );
    }
}

#[test]
fn an_import_batch_leaves_the_segments_and_locations_a_serial_batch_would() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let (_a, serial) = FlatFileStore::for_tempdir().unwrap();
    let (_b, import) = FlatFileStore::for_tempdir().unwrap();

    // Three batches of varied bodies over three segments, one of them
    // crossing a segment inside the batch.
    let batches: Vec<Vec<(u32, Vec<u8>)>> = vec![
        bodies(20, 120, 100, 6_000)
            .into_iter()
            .map(|b| (0, b))
            .collect(),
        bodies(21, 90, 100, 20_000)
            .into_iter()
            .enumerate()
            .map(|(i, b)| (if i < 40 { 0 } else { 1 }, b))
            .collect(),
        bodies(22, 60, 1, 3_000)
            .into_iter()
            .map(|b| (2, b))
            .collect(),
    ];
    let mut all = Vec::new();
    for batch in &batches {
        let items: Vec<(u32, &[u8])> = batch.iter().map(|(s, b)| (*s, b.as_slice())).collect();
        let from_serial = pool.install(|| serial.append_batch(&items)).unwrap();
        let from_import = import.append_batch(&items).unwrap();
        assert_eq!(from_serial, from_import);
        assert_eq!(import.resource_stats().writers, 1);
        all.extend(
            from_import
                .into_iter()
                .zip(batch.iter().map(|(_, b)| b.clone())),
        );
    }
    assert_same_segments(&serial, &import, &[0, 1, 2]);
    for (loc, body) in &all {
        assert_eq!(&import.read(loc).unwrap(), body);
    }

    let stats = import.append_stats();
    assert_eq!(stats.serial_batches + stats.parallel_batches, 3);
    assert!(stats.serial_batches > 0, "{stats:?}");
    assert_eq!(stats.parallel_batches > 0, rayon::current_num_threads() > 1);
    assert!(
        stats.parallel_encoders_peak <= rayon::current_num_threads(),
        "{stats:?}"
    );
    let stats = serial.append_stats();
    assert_eq!((stats.serial_batches, stats.parallel_batches), (3, 0));
    assert_eq!(stats.parallel_encoders_peak, 0);
}

#[test]
fn automatic_parallel_batches_keep_physical_frame_order() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();

    // A first body that takes longest to encode, then two hundred tiny ones
    // whose frames are ready long before it: they must still land after it.
    let mut bodies = bodies(23, 1, 2 << 20, 2 << 20);
    bodies.extend(self::bodies(24, 200, 50, 400));
    let locations = store.append_batch(&items(0, &bodies)).unwrap();

    for pair in locations.windows(2) {
        assert_eq!(pair[1].offset, pair[0].offset + pair[0].length as u64);
    }
    let bytes = fs::read(store.segment_path(0)).unwrap();
    let frames = walk_frames(&bytes);
    assert_eq!(frames.len(), bodies.len());
    for ((offset, length), (loc, body)) in frames.iter().zip(locations.iter().zip(&bodies)) {
        assert_eq!((loc.offset, loc.length), (*offset, *length));
        let frame = &bytes[*offset as usize..(*offset + *length as u64) as usize];
        assert_eq!(&decode_independently(frame), body);
    }
    let stats = store.append_stats();
    assert!(
        stats.parallel_encoders_peak <= rayon::current_num_threads(),
        "{stats:?}"
    );
    assert_eq!(
        stats.parallel_batches > 0,
        rayon::current_num_threads() > 1,
        "{stats:?}"
    );
}

#[test]
fn parallel_windows_bound_what_is_held_encoded_without_moving_a_frame() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let (_a, serial) = FlatFileStore::for_tempdir().unwrap();
    let (_b, import) = FlatFileStore::for_tempdir().unwrap();

    // Bodies of a quarter window each: the batch spans several windows, and
    // a segment boundary falls inside one of them.
    let bodies = bodies(25, 12, ENCODE_WINDOW_BYTES / 4, ENCODE_WINDOW_BYTES / 4);
    let items: Vec<(u32, &[u8])> = bodies
        .iter()
        .enumerate()
        .map(|(i, b)| (if i < 7 { 0 } else { 1 }, b.as_slice()))
        .collect();
    let from_serial = pool.install(|| serial.append_batch(&items)).unwrap();
    let from_import = import.append_batch(&items).unwrap();
    assert_eq!(from_serial, from_import);
    assert_same_segments(&serial, &import, &[0, 1]);

    let stats = import.append_stats();
    assert_eq!(
        stats.parallel_batches,
        u64::from(rayon::current_num_threads() > 1)
    );
    if rayon::current_num_threads() > 1 {
        assert!(stats.parallel_windows >= 3, "{stats:?}");
    }
    assert!(
        stats.parallel_window_bytes_peak <= ENCODE_WINDOW_BYTES,
        "{stats:?}"
    );
}

#[test]
fn retained_encoding_buffers_are_bounded_as_the_callers_batch_grows() {
    let body = vec![42u8; 128 << 10];
    for count in [100, 10_000] {
        let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
        let items = vec![(0, body.as_slice()); count];
        store.append_batch(&items).unwrap();
        let stats = store.append_stats();
        assert!(stats.parallel_encoders_peak <= rayon::current_num_threads());
        assert!(stats.parallel_window_bytes_peak <= ENCODE_WINDOW_BYTES);
        assert!(
            stats.encoded_buffer_bytes_peak
                <= ENCODE_WINDOW_BYTES
                    + (rayon::current_num_threads() + 2) * frame_bound(body.len())
        );
    }
}

#[test]
fn a_maximal_body_is_a_window_of_its_own_and_a_larger_one_is_refused_unwritten() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let small = bodies(26, 2, 300, 300);
    let maximal = vec![7u8; MAX_BODY_BYTES];
    let locations = store
        .append_batch(&[
            (0, small[0].as_slice()),
            (0, maximal.as_slice()),
            (0, small[1].as_slice()),
        ])
        .unwrap();
    assert_eq!(store.read(&locations[1]).unwrap(), maximal);
    assert_eq!(store.read(&locations[2]).unwrap(), small[1]);
    let stats = store.append_stats();
    assert_eq!(stats.parallel_windows, 0, "{stats:?}");
    assert_eq!(stats.serial_batches, 1, "{stats:?}");
    assert!(
        stats.parallel_window_bytes_peak <= ENCODE_WINDOW_BYTES.max(frame_bound(MAX_BODY_BYTES)),
        "{stats:?}"
    );

    // One byte more is refused before the batch touches a file: the first
    // body is not written and the new segment is not created.
    let len_before = file_len(&store, 0);
    let oversized = vec![1u8; MAX_BODY_BYTES + 1];
    let err = store
        .append_batch(&[(0, small[0].as_slice()), (1, oversized.as_slice())])
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(file_len(&store, 0), len_before);
    assert!(!store.segment_path(1).exists());
    assert_eq!(store.append_stats().parallel_batches, 0);
}

#[test]
fn an_import_batch_works_on_one_worker_with_one_body_and_with_none() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();

    assert!(store.append_batch(&[]).unwrap().is_empty());
    assert_eq!(store.resource_stats().writers, 0);

    let one = bodies(27, 1, 500, 500);
    let loc = pool
        .install(|| store.append_batch(&items(0, &one)))
        .unwrap();
    assert_eq!(store.read(&loc[0]).unwrap(), one[0]);

    let many = bodies(28, 300, 100, 5_000);
    let locations = pool
        .install(|| store.append_batch(&items(0, &many)))
        .unwrap();
    for (loc, body) in locations.iter().zip(&many) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }

    let stats = store.append_stats();
    assert_eq!(stats.serial_batches, 3);
    assert_eq!(stats.parallel_batches, 0);
    assert_eq!(stats.parallel_encoders_peak, 0, "{stats:?}");
}

#[test]
fn an_import_batch_fails_like_a_serial_one_and_the_retry_continues_at_the_true_end() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let first = bodies(29, 3, 500, 500);
    let before = store.append_batch(&items(0, &first)).unwrap();
    fs::create_dir(dir.path().join("000001.segment")).unwrap();

    let err = store
        .append_batch(&[(0, first[0].as_slice()), (1, first[0].as_slice())])
        .unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        file_len(&store, 0),
        before[2].offset + before[2].length as u64
    );
    assert_eq!(store.resource_stats().writers, 0, "the handles are dropped");

    fs::remove_dir(dir.path().join("000001.segment")).unwrap();
    let retry = store
        .append_batch(&[(0, first[0].as_slice()), (1, first[0].as_slice())])
        .unwrap();
    assert_eq!(retry[0].offset, before[2].offset + before[2].length as u64);
    assert_eq!(retry[1].offset, 0);
    assert_eq!(store.read(&retry[1]).unwrap(), first[0]);
    assert_eq!(store.resource_stats().writers, 1, "only the newest handle");
    for (loc, body) in before.iter().zip(&first) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
}
