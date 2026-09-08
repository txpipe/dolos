//! The store over mixed raw and compressed segments: reads dispatched by
//! representation, appends and truncation that thaw first, pruning across
//! representations, the recovery table at every interruption point, and the
//! validation an open performs.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use dolos_flatfiles::compressed::{
    CacheLimits, Dictionary, DictionaryDir, WriterOptions, METADATA_FRAME_SIZE,
};
use dolos_flatfiles::{
    BlockLocation, FlatFileOptions, FlatFileStore, Representation, DICTIONARIES_DIR,
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

fn vocabulary() -> Vec<Vec<u8>> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..48)
        .map(|_| {
            let len = 6 + rng.below(30) as usize;
            (0..len).map(|_| rng.below(256) as u8).collect()
        })
        .collect()
}

/// Compressible pseudo-blocks between `min` and `max` bytes.
fn blocks(seed: u64, count: usize, min: usize, max: usize) -> Vec<Vec<u8>> {
    let words = vocabulary();
    let mut rng = Rng(seed | 1);
    (0..count)
        .map(|_| {
            let target = min + rng.below((max - min + 1) as u64) as usize;
            let mut block = Vec::with_capacity(target + 40);
            while block.len() < target {
                block.extend_from_slice(&words[rng.below(words.len() as u64) as usize]);
            }
            block.truncate(target);
            block
        })
        .collect()
}

fn dictionary() -> Dictionary {
    Dictionary::new(vocabulary().concat())
}

type Entry = (Vec<u8>, BlockLocation);

/// Six batches over segments 0, 1 and 2 — two batches per segment, so every
/// segment has blocks appended in more than one call.
fn populate(store: &FlatFileStore, seed: u64) -> Vec<Entry> {
    let bodies = blocks(seed, 90, 200, 3_000);
    let mut out = Vec::new();
    for (i, chunk) in bodies.chunks(15).enumerate() {
        let segment = (i / 2) as u32;
        let items: Vec<(u32, &[u8])> = chunk.iter().map(|b| (segment, b.as_slice())).collect();
        let locations = store.append_batch(&items).unwrap();
        out.extend(chunk.iter().cloned().zip(locations));
    }
    out
}

fn in_segment(entries: &[Entry], segment: u32) -> Vec<BlockLocation> {
    entries
        .iter()
        .filter(|(_, loc)| loc.segment_id == segment)
        .map(|(_, loc)| *loc)
        .collect()
}

fn assert_all_readable(store: &FlatFileStore, entries: &[Entry]) {
    for (body, loc) in entries {
        assert_eq!(
            &store.read(loc).unwrap(),
            body,
            "location {loc:?} does not read back"
        );
    }
}

fn files_of(dir: &Path, segment: u32) -> Vec<String> {
    let prefix = format!("{segment:06}.");
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&prefix))
        .collect();
    names.sort();
    names
}

fn raw_path(dir: &Path, segment: u32) -> PathBuf {
    dir.join(format!("{segment:06}.segment"))
}

fn zseg_path(dir: &Path, segment: u32) -> PathBuf {
    dir.join(format!("{segment:06}.zseg"))
}

fn seal_mixed(dir: &Path, store: &FlatFileStore, entries: &[Entry]) {
    let dictionary = dictionary();
    DictionaryDir::new(dir.join(DICTIONARIES_DIR))
        .install(&dictionary)
        .unwrap();
    store
        .seal(
            0,
            &in_segment(entries, 0),
            &WriterOptions::per_block().with_dictionary(dictionary),
        )
        .unwrap();
    store
        .seal(1, &in_segment(entries, 1), &WriterOptions::chunked(4096))
        .unwrap();
}

#[test]
fn mixed_segments_read_like_raw() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 1);
    let raw_lens: Vec<u64> = (0..3)
        .map(|s| fs::metadata(raw_path(dir.path(), s)).unwrap().len())
        .collect();

    seal_mixed(dir.path(), &store, &entries);

    assert_eq!(store.representation(0), Some(Representation::Compressed));
    assert_eq!(store.representation(1), Some(Representation::Compressed));
    assert_eq!(store.representation(2), Some(Representation::Raw));
    assert_eq!(store.representation(3), None);
    assert_all_readable(&store, &entries);

    let infos = store.segments().unwrap();
    assert_eq!(infos.len(), 3);
    for (info, raw_len) in infos.iter().zip(&raw_lens) {
        assert_eq!(info.logical_len, *raw_len, "segment {}", info.segment_id);
    }
    assert!(infos[0].metadata.as_ref().unwrap().dictionary.is_some());
    assert!(infos[1].metadata.as_ref().unwrap().dictionary.is_none());
    assert!(infos[2].metadata.is_none());
    assert!(infos[0].physical_len < infos[0].logical_len);

    assert_eq!(files_of(dir.path(), 0), vec!["000000.zseg"]);
    assert_eq!(files_of(dir.path(), 1), vec!["000001.zseg"]);
    assert_eq!(files_of(dir.path(), 2), vec!["000002.segment"]);

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(reopened.representation(0), Some(Representation::Compressed));
    assert_eq!(reopened.representation(2), Some(Representation::Raw));
    assert_all_readable(&reopened, &entries);
}

#[test]
fn bytes_no_location_covers_survive_sealing() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 2);
    // A body appended again, the way a resumed import does, that the index
    // keeps pointing past: nothing describes it, yet it stays in the stream.
    let dead = blocks(7, 1, 500, 500).remove(0);
    let dead_loc = store.append_batch(&[(0, dead.as_slice())]).unwrap()[0];

    store
        .seal(0, &in_segment(&entries, 0), &WriterOptions::per_block())
        .unwrap();

    assert_all_readable(&store, &entries);
    assert_eq!(store.read(&dead_loc).unwrap(), dead);
    assert_eq!(
        store.segments().unwrap()[0].logical_len,
        dead_loc.offset + dead.len() as u64
    );

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(reopened.read(&dead_loc).unwrap(), dead);
}

#[test]
fn appending_to_a_compressed_segment_thaws_it_at_the_same_offsets() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 3);
    seal_mixed(dir.path(), &store, &entries);
    let logical_len = store.segments().unwrap()[0].logical_len;

    let more = blocks(11, 3, 300, 900);
    let items: Vec<(u32, &[u8])> = more.iter().map(|b| (0, b.as_slice())).collect();
    let locations = store.append_batch(&items).unwrap();

    assert_eq!(locations[0].offset, logical_len);
    assert_eq!(store.representation(0), Some(Representation::Raw));
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
    assert_all_readable(&store, &entries);
    for (body, loc) in more.iter().zip(&locations) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
    // The other compressed segment is untouched by the append.
    assert_eq!(store.representation(1), Some(Representation::Compressed));

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_all_readable(&reopened, &entries);
    assert_eq!(reopened.read(&locations[2]).unwrap(), more[2]);
}

#[test]
fn truncating_inside_a_compressed_segment_thaws_it_first() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 4);
    seal_mixed(dir.path(), &store, &entries);
    let logical_len = store.segments().unwrap()[0].logical_len;

    // At the logical end nothing is cut, so nothing is converted; past the
    // end of a raw segment nothing is cut and nothing grows either.
    store.truncate(0, logical_len).unwrap();
    assert_eq!(store.representation(0), Some(Representation::Compressed));
    let raw_len = store.segments().unwrap()[2].logical_len;
    store.truncate(2, raw_len + 1).unwrap();
    assert_eq!(store.segments().unwrap()[2].logical_len, raw_len);

    let seg0: Vec<&Entry> = entries.iter().filter(|(_, l)| l.segment_id == 0).collect();
    let cut = seg0[20].1;
    store.truncate(0, cut.offset).unwrap();

    assert_eq!(store.representation(0), Some(Representation::Raw));
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
    for (body, loc) in &seg0[..20] {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
    for (_, loc) in &seg0[20..] {
        assert!(store.read(loc).is_err(), "{loc:?} survived the cut");
    }

    let again = blocks(12, 1, 400, 400).remove(0);
    let loc = store.append_batch(&[(0, again.as_slice())]).unwrap()[0];
    assert_eq!(loc.offset, cut.offset);
    assert_eq!(store.read(&loc).unwrap(), again);
}

#[test]
fn truncating_to_zero_removes_a_compressed_segment_without_thawing() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 5);
    seal_mixed(dir.path(), &store, &entries);

    store.truncate(0, 0).unwrap();

    assert!(files_of(dir.path(), 0).is_empty());
    assert_eq!(store.representation(0), None);
    let first = &entries[0].1;
    assert_eq!(
        store.read(first).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );

    let body = blocks(13, 1, 400, 400).remove(0);
    let loc = store.append_batch(&[(0, body.as_slice())]).unwrap()[0];
    assert_eq!(loc.offset, 0);
    assert_eq!(store.representation(0), Some(Representation::Raw));

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(reopened.read(&loc).unwrap(), body);
}

#[test]
fn pruning_removes_every_representation_and_releases_handles() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 6);
    seal_mixed(dir.path(), &store, &entries);
    assert_all_readable(&store, &entries);
    assert!(store.cache_stats().open_handles > 0);

    // Remnants a crash could leave beside an authoritative file.
    fs::write(dir.path().join("000000.zseg.tmp"), b"half").unwrap();
    fs::write(dir.path().join("000001.transition.tmp"), b"seal").unwrap();

    store.delete_segments_before(2).unwrap();

    assert!(files_of(dir.path(), 0).is_empty());
    assert!(files_of(dir.path(), 1).is_empty());
    assert_eq!(files_of(dir.path(), 2), vec!["000002.segment"]);
    assert_eq!(store.cache_stats().open_handles, 0);
    assert_eq!(store.representation(0), None);
    assert_eq!(store.representation(1), None);
    for (_, loc) in entries.iter().filter(|(_, l)| l.segment_id < 2) {
        assert_eq!(store.read(loc).unwrap_err().kind(), io::ErrorKind::NotFound);
    }
    for (body, loc) in entries.iter().filter(|(_, l)| l.segment_id == 2) {
        assert_eq!(&store.read(loc).unwrap(), body);
    }

    drop(store);
    let reopened = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(reopened.segments().unwrap().len(), 1);
}

/// One segment's stream in both representations, for composing interrupted
/// directories by hand.
struct Pair {
    entries: Vec<Entry>,
    raw: Vec<u8>,
    compressed: Vec<u8>,
}

fn pair() -> Pair {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let bodies = blocks(21, 30, 200, 2_000);
    let items: Vec<(u32, &[u8])> = bodies.iter().map(|b| (0, b.as_slice())).collect();
    let locations = store.append_batch(&items).unwrap();
    let entries: Vec<Entry> = bodies.into_iter().zip(locations).collect();
    let raw = fs::read(raw_path(dir.path(), 0)).unwrap();
    store
        .seal(0, &in_segment(&entries, 0), &WriterOptions::per_block())
        .unwrap();
    let compressed = fs::read(zseg_path(dir.path(), 0)).unwrap();
    Pair {
        entries,
        raw,
        compressed,
    }
}

/// A directory state: file name suffix (after `000000.`) and content.
type State = Vec<(&'static str, Vec<u8>)>;

fn compose(state: &State) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (suffix, content) in state {
        fs::write(dir.path().join(format!("000000.{suffix}")), content).unwrap();
    }
    dir
}

fn error_of(dir: &Path) -> String {
    match FlatFileStore::new(dir) {
        Ok(_) => panic!("{} opened", dir.display()),
        Err(e) => e.to_string(),
    }
}

/// Every state a seal or a thaw can be interrupted in, and what a restart
/// makes of it. `LIFECYCLE.md` is the prose form of this table.
#[test]
fn recovery_resolves_every_interruption_point() {
    let pair = pair();
    let raw = || pair.raw.clone();
    let zseg = || pair.compressed.clone();
    let partial = |bytes: &[u8]| bytes[..bytes.len() / 2].to_vec();
    let seal = || b"seal\n".to_vec();
    let thaw = || b"thaw\n".to_vec();

    let cases: Vec<(&str, State, Representation, Vec<&str>)> = vec![
        // The seal sequence: R → R+I → R+I+T → R+I+Z → I+Z → Z.
        (
            "seal: at rest",
            vec![("segment", raw())],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "seal: record being staged",
            vec![("segment", raw()), ("transition.tmp", seal())],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "seal: record written",
            vec![("segment", raw()), ("transition", seal())],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "seal: output partly staged",
            vec![
                ("segment", raw()),
                ("transition", seal()),
                ("zseg.tmp", partial(&pair.compressed)),
            ],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "seal: output fully staged, not published",
            vec![
                ("segment", raw()),
                ("transition", seal()),
                ("zseg.tmp", zseg()),
            ],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "seal: published, raw not retired",
            vec![("segment", raw()), ("transition", seal()), ("zseg", zseg())],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        (
            "seal: raw retired, record not",
            vec![("transition", seal()), ("zseg", zseg())],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        (
            "seal: done",
            vec![("zseg", zseg())],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        // The thaw sequence: Z → Z+I → Z+I+T → Z+I+R → I+R → R.
        (
            "thaw: record written",
            vec![("zseg", zseg()), ("transition", thaw())],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        (
            "thaw: output partly staged",
            vec![
                ("zseg", zseg()),
                ("transition", thaw()),
                ("segment.tmp", partial(&pair.raw)),
            ],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        (
            "thaw: output fully staged, not published",
            vec![
                ("zseg", zseg()),
                ("transition", thaw()),
                ("segment.tmp", raw()),
            ],
            Representation::Compressed,
            vec!["000000.zseg"],
        ),
        (
            "thaw: published, compressed not retired",
            vec![("zseg", zseg()), ("transition", thaw()), ("segment", raw())],
            Representation::Raw,
            vec!["000000.segment"],
        ),
        (
            "thaw: compressed retired, record not",
            vec![("transition", thaw()), ("segment", raw())],
            Representation::Raw,
            vec!["000000.segment"],
        ),
    ];

    for (name, state, expected, remaining) in cases {
        let dir = compose(&state);
        for pass in 0..2 {
            let store = FlatFileStore::new(dir.path())
                .unwrap_or_else(|e| panic!("{name}, pass {pass}: {e}"));
            assert_eq!(
                store.representation(0),
                Some(expected),
                "{name}, pass {pass}"
            );
            assert_all_readable(&store, &pair.entries);
            assert_eq!(files_of(dir.path(), 0), remaining, "{name}, pass {pass}");
        }
    }
}

/// Once a thaw has published its raw file the raw file is the segment, and
/// what happens to it afterwards — an append, a truncation — is never undone
/// by a restart that still finds the compressed file waiting to be retired.
#[test]
fn recovery_never_revives_compressed_bytes_over_a_mutated_raw_file() {
    let pair = pair();
    let extra = blocks(31, 1, 700, 700).remove(0);

    let mut appended = pair.raw.clone();
    appended.extend_from_slice(&extra);
    let dir = compose(&vec![
        ("zseg", pair.compressed.clone()),
        ("transition", b"thaw\n".to_vec()),
        ("segment", appended),
    ]);
    let store = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(store.representation(0), Some(Representation::Raw));
    assert_all_readable(&store, &pair.entries);
    let extra_loc = BlockLocation {
        segment_id: 0,
        offset: pair.raw.len() as u64,
        length: extra.len() as u32,
    };
    assert_eq!(store.read(&extra_loc).unwrap(), extra);
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
    drop(store);

    let cut = pair.entries[10].1.offset as usize;
    let dir = compose(&vec![
        ("zseg", pair.compressed.clone()),
        ("transition", b"thaw\n".to_vec()),
        ("segment", pair.raw[..cut].to_vec()),
    ]);
    let store = FlatFileStore::new(dir.path()).unwrap();
    assert_eq!(store.representation(0), Some(Representation::Raw));
    for (body, loc) in &pair.entries[..10] {
        assert_eq!(&store.read(loc).unwrap(), body);
    }
    assert!(store.read(&pair.entries[10].1).is_err());
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
}

#[test]
fn recovery_refuses_states_no_transition_produces() {
    let pair = pair();

    let dir = compose(&vec![
        ("segment", pair.raw.clone()),
        ("zseg", pair.compressed.clone()),
    ]);
    let message = error_of(dir.path());
    assert!(message.contains("000000"), "{message}");
    assert!(message.contains("no transition record"), "{message}");
    // Refusing changed nothing, so the operator can still decide.
    assert_eq!(
        files_of(dir.path(), 0),
        vec!["000000.segment", "000000.zseg"]
    );

    let dir = compose(&vec![("transition", b"seal\n".to_vec())]);
    let message = error_of(dir.path());
    assert!(message.contains("neither representation"), "{message}");

    let dir = compose(&vec![
        ("segment", pair.raw.clone()),
        ("transition", b"melt\n".to_vec()),
    ]);
    let message = error_of(dir.path());
    assert!(message.contains("unreadable"), "{message}");
}

#[test]
fn open_refuses_a_missing_or_wrong_dictionary() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 8);
    let dictionary = dictionary();
    let dictionaries = DictionaryDir::new(dir.path().join(DICTIONARIES_DIR));
    let installed = dictionaries.install(&dictionary).unwrap();
    store
        .seal(
            1,
            &in_segment(&entries, 1),
            &WriterOptions::per_block().with_dictionary(dictionary.clone()),
        )
        .unwrap();
    drop(store);

    fs::remove_file(&installed).unwrap();
    let message = error_of(dir.path());
    assert!(message.contains("000001"), "{message}");
    assert!(message.contains(&dictionary.id().to_string()), "{message}");
    assert!(message.contains("not available"), "{message}");

    fs::write(&installed, b"not the dictionary the segment names").unwrap();
    let message = error_of(dir.path());
    assert!(message.contains("000001"), "{message}");
    assert!(message.contains("hashes to"), "{message}");

    fs::remove_file(&installed).unwrap();
    dictionaries.install(&dictionary).unwrap();
    let store = FlatFileStore::new(dir.path()).unwrap();
    assert_all_readable(&store, &entries);
}

#[test]
fn open_refuses_metadata_this_build_does_not_read() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 9);
    store
        .seal(2, &in_segment(&entries, 2), &WriterOptions::per_block())
        .unwrap();
    drop(store);

    let path = zseg_path(dir.path(), 2);
    let mut bytes = fs::read(&path).unwrap();
    // The payload version sits four bytes into the metadata payload.
    bytes[8 + 4..8 + 6].copy_from_slice(&7u16.to_le_bytes());
    fs::write(&path, bytes).unwrap();

    let message = error_of(dir.path());
    assert!(message.contains("000002"), "{message}");
    assert!(message.contains("not supported"), "{message}");
}

#[test]
fn frame_corruption_is_a_read_error_not_an_open_error() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 10);
    store
        .seal(0, &in_segment(&entries, 0), &WriterOptions::per_block())
        .unwrap();
    drop(store);

    let path = zseg_path(dir.path(), 0);
    let mut bytes = fs::read(&path).unwrap();
    let inside_first_frame = METADATA_FRAME_SIZE + 24;
    bytes[inside_first_frame] ^= 0xFF;
    fs::write(&path, bytes).unwrap();

    let store = FlatFileStore::new(dir.path()).unwrap();
    let first = &entries[0].1;
    assert_eq!(
        store.read(first).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let last_in_segment = in_segment(&entries, 0).last().copied().unwrap();
    assert!(store.read(&last_in_segment).is_ok());
}

#[test]
fn a_failed_seal_leaves_the_raw_segment_and_no_remnants() {
    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 14);
    let seg0 = in_segment(&entries, 0);

    // Overlapping locations are refused before anything is written.
    let mut overlapping = seg0.clone();
    overlapping.push(BlockLocation {
        segment_id: 0,
        offset: seg0[0].offset + 1,
        length: 10,
    });
    let error = store
        .seal(0, &overlapping, &WriterOptions::per_block())
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);

    // A dictionary the store cannot resolve fails the verification of the
    // staged output, after the record and the staging file exist.
    let uninstalled = dictionary();
    let error = store
        .seal(
            0,
            &seg0,
            &WriterOptions::per_block().with_dictionary(uninstalled.clone()),
        )
        .unwrap_err();
    assert!(error.to_string().contains("verification"), "{error}");
    assert_eq!(store.representation(0), Some(Representation::Raw));
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
    assert_all_readable(&store, &entries);

    DictionaryDir::new(dir.path().join(DICTIONARIES_DIR))
        .install(&uninstalled)
        .unwrap();
    store
        .seal(
            0,
            &seg0,
            &WriterOptions::per_block().with_dictionary(uninstalled),
        )
        .unwrap();
    assert_all_readable(&store, &entries);
}

#[cfg(unix)]
#[test]
fn an_unwritable_directory_fails_the_seal_before_anything_moves() {
    use std::os::unix::fs::PermissionsExt;

    let (dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 15);

    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o555)).unwrap();
    let result = store.seal(0, &in_segment(&entries, 0), &WriterOptions::per_block());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();

    if result.is_ok() {
        // Running with privileges that ignore directory modes.
        return;
    }
    assert_eq!(store.representation(0), Some(Representation::Raw));
    assert_eq!(files_of(dir.path(), 0), vec!["000000.segment"]);
    assert_all_readable(&store, &entries);
}

#[test]
fn transitions_refuse_the_wrong_source() {
    let (_dir, store) = FlatFileStore::for_tempdir().unwrap();
    let entries = populate(&store, 16);
    let seg0 = in_segment(&entries, 0);

    assert_eq!(
        store.thaw(0).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    store.seal(0, &seg0, &WriterOptions::per_block()).unwrap();
    assert_eq!(
        store
            .seal(0, &seg0, &WriterOptions::per_block())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        store
            .seal(9, &[], &WriterOptions::per_block())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    store.thaw(0).unwrap();
    assert_all_readable(&store, &entries);
}

#[test]
fn readers_never_observe_a_transition_in_progress() {
    let dir = tempfile::tempdir().unwrap();
    let store = FlatFileStore::with_options(
        dir.path(),
        FlatFileOptions {
            dictionaries: None,
            cache: Some(CacheLimits {
                handles: 2,
                inflight_reads: 4,
                ..CacheLimits::default()
            }),
        },
    )
    .unwrap();
    let store = Arc::new(store);
    let entries = Arc::new(populate(&store, 17));
    let seg0 = in_segment(&entries, 0);
    store.seal(0, &seg0, &WriterOptions::per_block()).unwrap();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let store = store.clone();
            let entries = entries.clone();
            thread::spawn(move || {
                for _ in 0..40 {
                    assert_all_readable(&store, &entries);
                }
            })
        })
        .collect();

    for round in 0..6 {
        store.thaw(0).unwrap();
        let options = if round % 2 == 0 {
            WriterOptions::chunked(2048)
        } else {
            WriterOptions::per_block()
        };
        store.seal(0, &seg0, &options).unwrap();
    }

    for reader in readers {
        reader
            .join()
            .expect("a reader saw a bad read mid-transition");
    }

    let stats = store.cache_stats();
    assert!(stats.open_handles <= 2, "{stats:?}");
    store.delete_segments_before(3).unwrap();
    assert_eq!(store.cache_stats().open_handles, 0);
}
