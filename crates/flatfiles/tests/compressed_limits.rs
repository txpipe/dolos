use std::fs;
use std::io;
use std::sync::{mpsc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use dolos_flatfiles::compressed::{
    CacheLimits, Dictionary, DictionaryId, DictionarySet, DictionarySource, NoDictionaries,
    ReadCache, SegmentIndex, SegmentReader, SegmentRef, SegmentWriter, WriterOptions,
};

fn segment(segment_id: u32) -> SegmentRef {
    SegmentRef {
        segment_id,
        generation: 1,
    }
}

fn encode(dictionary: Option<Dictionary>) -> Vec<u8> {
    let options = WriterOptions {
        dictionary,
        ..WriterOptions::per_block()
    };
    let mut writer = SegmentWriter::new(Vec::new(), options).unwrap();
    writer.push(b"test block").unwrap();
    writer.finish().unwrap().1
}

#[test]
fn malformed_dictionary_returns_an_error_from_both_readers() {
    let dictionary = Dictionary::new(vec![0x37, 0xa4, 0x30, 0xec, 1, 0, 0, 0]);
    let error = dictionary.prepare().err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&dictionary.id().to_string()));

    let mut bytes = encode(None);
    let mut metadata = SegmentIndex::parse(&bytes).unwrap().metadata().clone();
    metadata.dictionary = Some(dictionary.id());
    metadata.zstd_dictionary_id = dictionary.zstd_id();
    bytes[..metadata.encode().len()].copy_from_slice(&metadata.encode());
    let dictionaries = DictionarySet::new().with(dictionary);
    let error = SegmentReader::new(&bytes, &dictionaries).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("segment.zseg");
    fs::write(&path, bytes).unwrap();
    let cache = ReadCache::new(CacheLimits::DISABLED);
    let error = cache
        .read(segment(0), &path, 0, 10, &dictionaries)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(cache.stats(), Default::default());
    fs::write(&path, encode(None)).unwrap();
    assert_eq!(
        cache
            .read(segment(1), &path, 0, 10, &NoDictionaries)
            .unwrap(),
        b"test block"
    );
}

#[test]
fn variable_sized_caches_obey_byte_budgets_and_evict() {
    let directory = tempfile::tempdir().unwrap();
    let dictionaries = [
        Dictionary::new(vec![1; 4096]),
        Dictionary::new(vec![2; 4096]),
    ];
    let dictionary_weight = dictionaries[0].prepare().unwrap().memory_size();
    assert!(dictionary_weight > 2 * dictionaries[0].bytes().len());
    assert_eq!(
        dictionary_weight,
        dictionaries[1].prepare().unwrap().memory_size()
    );
    let bytes = encode(Some(dictionaries[0].clone()));
    let index_weight = SegmentIndex::parse(&bytes).unwrap().memory_size();
    let paths = [
        directory.path().join("first.zseg"),
        directory.path().join("second.zseg"),
    ];
    for (path, dictionary) in paths.iter().zip(&dictionaries) {
        fs::write(path, encode(Some(dictionary.clone()))).unwrap();
    }
    let source = DictionarySet::new()
        .with(dictionaries[0].clone())
        .with(dictionaries[1].clone());

    for (index_bytes, dictionary_bytes, entries) in [
        (0, 0, 0),
        (index_weight - 1, dictionary_weight - 1, 0),
        (index_weight, dictionary_weight, 1),
    ] {
        let cache = ReadCache::new(CacheLimits {
            index_bytes,
            dictionary_bytes,
            frame_bytes: 0,
            ..CacheLimits::default()
        });
        for turn in 0..8 {
            let selected = turn % 2;
            assert_eq!(
                cache
                    .read(segment(selected as u32), &paths[selected], 0, 10, &source)
                    .unwrap(),
                b"test block",
            );
            let stats = cache.stats();
            assert_eq!(stats.index_entries, entries);
            assert_eq!(stats.dictionary_entries, entries);
            assert_eq!(stats.index_bytes, entries * index_weight);
            assert_eq!(stats.dictionary_bytes, entries * dictionary_weight);
        }
        cache.invalidate(1);
        assert_eq!(cache.stats().index_bytes, 0);
        cache.clear();
        assert_eq!(cache.stats(), Default::default());
    }
}

struct BlockingDictionary {
    dictionary: Dictionary,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl DictionarySource for BlockingDictionary {
    fn dictionary(&self, _: &DictionaryId) -> io::Result<Option<Dictionary>> {
        self.entered.send(()).map_err(io::Error::other)?;
        self.release
            .lock()
            .unwrap()
            .recv()
            .map_err(io::Error::other)?;
        Ok(Some(self.dictionary.clone()))
    }
}

#[test]
fn concurrent_reads_bound_preparation_and_open_handles() {
    for (handles, inflight_reads) in [(8, 1), (1, 8), (0, 0)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("segment.zseg");
        let dictionary = Dictionary::new(b"dictionary vocabulary".to_vec());
        fs::write(&path, encode(Some(dictionary.clone()))).unwrap();
        let cache = ReadCache::new(CacheLimits {
            handles,
            inflight_reads,
            dictionary_entries: 0,
            ..CacheLimits::default()
        });
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let source = BlockingDictionary {
            dictionary,
            entered: entered_tx,
            release: Mutex::new(release_rx),
        };
        let start = Barrier::new(9);
        let (first, extra, stats, cleared) = thread::scope(|scope| {
            for segment_id in 0..8 {
                let cache = &cache;
                let source = &source;
                let path = &path;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    assert_eq!(
                        cache
                            .read(segment(segment_id), path, 0, 10, source)
                            .unwrap(),
                        b"test block",
                    );
                });
            }
            start.wait();
            let first = entered_rx.recv_timeout(Duration::from_secs(5));
            let extra = entered_rx.recv_timeout(Duration::from_millis(100));
            let stats = cache.stats();
            cache.clear();
            let cleared = cache.stats();
            for _ in 0..8 {
                let _ = release_tx.send(());
            }
            (first, extra, stats, cleared)
        });
        first.unwrap();
        assert_eq!(extra, Err(mpsc::RecvTimeoutError::Timeout));
        assert_eq!(stats.open_handles, 1);
        assert!(stats.handles <= handles);
        assert!(stats.inflight_reads <= inflight_reads.max(1));
        assert_eq!(stats.index_entries, 1);
        assert_eq!(cleared.handles, 0);
        assert_eq!(cleared.open_handles, 1);
        cache.clear();
        assert_eq!(cache.stats(), Default::default());
    }
}

#[test]
fn failed_opens_release_handle_and_operation_permits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("segment.zseg");
    let cache = ReadCache::new(CacheLimits::DISABLED);
    assert!(cache.index(segment(0), &path).is_err());
    assert_eq!(cache.stats(), Default::default());
    fs::write(&path, b"invalid").unwrap();
    assert!(cache.index(segment(0), &path).is_err());
    assert_eq!(cache.stats(), Default::default());
    fs::write(&path, encode(None)).unwrap();
    let index = cache.index(segment(0), &path).unwrap();
    assert_eq!(index.logical_len(), 10);
    assert_eq!(cache.stats(), Default::default());
}
