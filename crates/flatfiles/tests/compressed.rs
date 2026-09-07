//! Round trips, malformed input, independent decoding and cache bounds for
//! the compressed segment codec.

use std::fs;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use dolos_flatfiles::compressed::{
    CacheLimits, Dictionary, DictionaryId, DictionarySet, DictionarySource, FrameMode, Metadata,
    NoDictionaries, ReadCache, SegmentIndex, SegmentReader, SegmentRef, SegmentWriter,
    WriterOptions, METADATA_FRAME_MAGIC, METADATA_FRAME_SIZE, METADATA_MAGIC,
    METADATA_PAYLOAD_SIZE, METADATA_VERSION, SEEKABLE_MAGIC, SEEK_ENTRY_SIZE,
    SEEK_TABLE_FOOTER_SIZE, SEEK_TABLE_FRAME_MAGIC, SKIPPABLE_HEADER_SIZE, ZSTD_FRAME_MAGIC,
};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

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

/// A fixed vocabulary shared by every generated block, which is what gives a
/// dictionary something to learn.
fn vocabulary() -> Vec<Vec<u8>> {
    let mut rng = Rng::new(7);
    (0..64)
        .map(|_| {
            let len = 4 + rng.below(9) as usize;
            (0..len).map(|_| b'a' + rng.below(26) as u8).collect()
        })
        .collect()
}

/// Deterministic block bodies: vocabulary words plus a 32-byte random tail.
fn blocks(seed: u64, count: usize, min: usize, max: usize) -> Vec<Vec<u8>> {
    let words = vocabulary();
    let mut rng = Rng::new(seed);
    (0..count)
        .map(|_| {
            let target = min + rng.below((max - min) as u64) as usize;
            let mut block = Vec::with_capacity(target + 48);
            while block.len() < target {
                block.extend_from_slice(&words[rng.below(64) as usize]);
                block.push(b' ');
            }
            for _ in 0..32 {
                block.push(rng.next() as u8);
            }
            block
        })
        .collect()
}

fn logical(blocks: &[Vec<u8>]) -> Vec<u8> {
    blocks.concat()
}

fn encode(blocks: &[Vec<u8>], options: WriterOptions) -> (Vec<u8>, Vec<(u64, u32)>) {
    let mut writer = SegmentWriter::new(Vec::new(), options).unwrap();
    let mut locations = Vec::new();
    for block in blocks {
        let offset = writer.push(block).unwrap();
        locations.push((offset, block.len() as u32));
    }
    let (summary, bytes) = writer.finish().unwrap();
    assert_eq!(summary.physical_len, bytes.len() as u64);
    assert_eq!(summary.logical_len, logical(blocks).len() as u64);
    (bytes, locations)
}

fn raw_dictionary() -> Dictionary {
    let dictionary = Dictionary::new(vocabulary().concat());
    assert_eq!(dictionary.zstd_id(), 0, "raw content has no zstd id");
    dictionary
}

fn trained_dictionary() -> Dictionary {
    let samples = blocks(99, 3000, 200, 1200);
    let dictionary = Dictionary::new(zstd::dict::from_samples(&samples, 4096).unwrap());
    assert_ne!(
        dictionary.zstd_id(),
        0,
        "a trained dictionary carries an id"
    );
    dictionary
}

fn per_block() -> WriterOptions {
    WriterOptions::per_block()
}

fn chunked(target: u32) -> WriterOptions {
    WriterOptions::chunked(target)
}

fn decode_independently(bytes: &[u8], dictionary: Option<&Dictionary>) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    match dictionary {
        Some(dictionary) => {
            zstd::stream::read::Decoder::with_dictionary(Cursor::new(bytes), dictionary.bytes())?
                .read_to_end(&mut out)?
        }
        None => zstd::stream::read::Decoder::new(Cursor::new(bytes))?.read_to_end(&mut out)?,
    };
    Ok(out)
}

fn round_trip(options: WriterOptions) {
    let dictionary = options.dictionary.clone();
    let blocks = blocks(1, 120, 100, 3000);
    let stream = logical(&blocks);
    let (bytes, locations) = encode(&blocks, options.clone());

    let set = dictionary
        .clone()
        .map(|d| DictionarySet::new().with(d))
        .unwrap_or_default();
    let reader = SegmentReader::new(bytes.as_slice(), &set).unwrap();
    assert_eq!(reader.index().logical_len(), stream.len() as u64);
    assert_eq!(
        reader.index().metadata(),
        &Metadata {
            mode: options.mode,
            level: options.level,
            dictionary: dictionary.as_ref().map(Dictionary::id),
            zstd_dictionary_id: dictionary.as_ref().map_or(0, Dictionary::zstd_id),
            zstd_version: zstd::zstd_safe::version_number(),
        }
    );
    if options.mode == FrameMode::PerBlock {
        assert_eq!(reader.index().frames().len(), blocks.len());
    }

    for (block, (offset, length)) in blocks.iter().zip(&locations) {
        assert_eq!(&reader.read(*offset, *length).unwrap(), block);
    }

    let mut rng = Rng::new(2);
    for _ in 0..300 {
        let start = rng.below(stream.len() as u64) as usize;
        let length = rng.below((stream.len() - start) as u64 + 1) as usize;
        assert_eq!(
            reader.read(start as u64, length as u32).unwrap(),
            &stream[start..start + length]
        );
    }
    assert_eq!(reader.read(0, stream.len() as u32).unwrap(), stream);

    let independent = decode_independently(&bytes, dictionary.as_ref()).unwrap();
    assert_eq!(independent, stream);
    assert!(bytes.len() < stream.len(), "the segment should shrink");
}

#[test]
fn per_block_round_trip() {
    round_trip(per_block());
}

#[test]
fn per_block_round_trip_with_raw_dictionary() {
    round_trip(per_block().with_dictionary(raw_dictionary()));
}

#[test]
fn per_block_round_trip_with_trained_dictionary() {
    round_trip(per_block().with_dictionary(trained_dictionary()));
}

#[test]
fn chunked_round_trip() {
    round_trip(chunked(8 * 1024));
}

#[test]
fn chunked_round_trip_with_trained_dictionary() {
    round_trip(chunked(8 * 1024).with_dictionary(trained_dictionary()));
}

#[test]
fn chunked_round_trip_at_higher_level() {
    round_trip(chunked(4 * 1024).with_level(9));
}

#[test]
fn chunked_groups_blocks_and_isolates_oversized_ones() {
    let sizes = [1000, 1000, 1000, 1500, 10_000, 500, 600];
    let blocks: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(i, size)| vec![i as u8; *size])
        .collect();
    let (bytes, locations) = encode(&blocks, chunked(4096));
    let index = SegmentIndex::parse(&bytes).unwrap();

    let frames: Vec<u32> = index.frames().iter().map(|f| f.decompressed_size).collect();
    assert_eq!(frames, [3000, 1500, 10_000, 1100]);
    assert_eq!(index.frames()[2].logical_offset, locations[4].0);

    let reader = SegmentReader::new(bytes.as_slice(), &NoDictionaries).unwrap();
    for (block, (offset, length)) in blocks.iter().zip(&locations) {
        assert_eq!(&reader.read(*offset, *length).unwrap(), block);
    }
}

#[test]
fn empty_stream_is_a_valid_segment() {
    let (bytes, _) = encode(&[], chunked(1024));
    assert_eq!(
        bytes.len(),
        METADATA_FRAME_SIZE + SKIPPABLE_HEADER_SIZE + SEEK_ENTRY_SIZE + SEEK_TABLE_FOOTER_SIZE
    );
    let reader = SegmentReader::new(bytes.as_slice(), &NoDictionaries).unwrap();
    assert_eq!(reader.index().logical_len(), 0);
    assert!(reader.index().frames().is_empty());
    assert_eq!(reader.read(0, 0).unwrap(), Vec::<u8>::new());
    assert_eq!(
        reader.read(0, 1).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        reader.read(1, 0).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        decode_independently(&bytes, None).unwrap(),
        Vec::<u8>::new()
    );
}

#[test]
fn empty_blocks_occupy_no_bytes() {
    let blocks = vec![b"one".to_vec(), Vec::new(), b"two".to_vec()];
    let (bytes, locations) = encode(&blocks, per_block());
    assert_eq!(locations, [(0, 3), (3, 0), (3, 3)]);
    let reader = SegmentReader::new(bytes.as_slice(), &NoDictionaries).unwrap();
    assert_eq!(reader.index().frames().len(), 2);
    assert_eq!(reader.read(3, 0).unwrap(), Vec::<u8>::new());
    assert_eq!(reader.read(3, 3).unwrap(), b"two");
}

#[test]
fn reads_cross_frame_boundaries() {
    let blocks = blocks(3, 40, 500, 900);
    let stream = logical(&blocks);
    let (bytes, _) = encode(&blocks, chunked(2000));
    let reader = SegmentReader::new(bytes.as_slice(), &NoDictionaries).unwrap();
    let frames = reader.index().frames().to_vec();
    assert!(
        frames.len() >= 5,
        "need several frames, got {}",
        frames.len()
    );

    let first = frames[1];
    let exact = reader
        .read(first.logical_offset, first.decompressed_size)
        .unwrap();
    assert_eq!(
        exact,
        &stream[first.logical_offset as usize..first.logical_end() as usize]
    );

    let straddle = reader.read(first.logical_end() - 1, 2).unwrap();
    assert_eq!(
        straddle,
        &stream[first.logical_end() as usize - 1..first.logical_end() as usize + 1]
    );

    let start = frames[1].logical_offset + 7;
    let end = frames[3].logical_end() - 5;
    let three = reader.read(start, (end - start) as u32).unwrap();
    assert_eq!(three, &stream[start as usize..end as usize]);

    let last = *frames.last().unwrap();
    assert_eq!(last.logical_end(), stream.len() as u64);
    assert!(
        last.decompressed_size < 2000,
        "the last frame should be the partial one"
    );
    let tail = reader.read(last.logical_end() - 1, 1).unwrap();
    assert_eq!(tail, &stream[stream.len() - 1..]);
}

struct Mismatch(Dictionary);

impl DictionarySource for Mismatch {
    fn dictionary(&self, _id: &DictionaryId) -> io::Result<Option<Dictionary>> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn wrong_dictionary_is_refused_before_decoding() {
    let right = trained_dictionary();
    let other = raw_dictionary();
    let blocks = blocks(4, 10, 200, 400);
    let (bytes, locations) = encode(&blocks, per_block().with_dictionary(right.clone()));

    let err = SegmentReader::new(bytes.as_slice(), &Mismatch(other.clone())).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains(&right.id().to_string()));

    let index = SegmentIndex::parse(&bytes).unwrap();
    let prepared = other.prepare();
    let err = index.decode_frame(&bytes, 0, Some(&prepared)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    let err = index.decode_frame(&bytes, 0, None).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    let prepared = right.prepare();
    let (offset, length) = locations[0];
    assert_eq!(
        index
            .assemble(offset, length, |f| index
                .decode_frame(&bytes, f, Some(&prepared))
                .map(Arc::from))
            .unwrap(),
        blocks[0]
    );
}

#[test]
fn missing_dictionary_is_a_not_found_error() {
    let dictionary = raw_dictionary();
    let (bytes, _) = encode(
        &blocks(5, 5, 100, 200),
        per_block().with_dictionary(dictionary.clone()),
    );
    let err = SegmentReader::new(bytes.as_slice(), &NoDictionaries).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    assert!(err.to_string().contains(&dictionary.id().to_string()));

    let cache = ReadCache::new(CacheLimits::default());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dict.zseg");
    fs::write(&path, &bytes).unwrap();
    let err = cache
        .read(segment(1, 1), &path, 0, 1, &NoDictionaries)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn dictionary_free_segment_refuses_a_dictionary() {
    let (bytes, _) = encode(&blocks(6, 5, 100, 200), per_block());
    let index = SegmentIndex::parse(&bytes).unwrap();
    let prepared = raw_dictionary().prepare();
    let err = index.decode_frame(&bytes, 0, Some(&prepared)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

/// A file holding only the given metadata frame and a seek table for it.
fn only_metadata(frame: &[u8]) -> Vec<u8> {
    let mut bytes = frame.to_vec();
    bytes.extend_from_slice(&SEEK_TABLE_FRAME_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&((SEEK_ENTRY_SIZE + SEEK_TABLE_FOOTER_SIZE) as u32).to_le_bytes());
    bytes.extend_from_slice(&(frame.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&SEEKABLE_MAGIC.to_le_bytes());
    bytes
}

fn metadata_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&METADATA_FRAME_MAGIC.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

#[test]
fn malformed_metadata_is_an_error() {
    let (good, _) = encode(&[], per_block());
    let payload = good[SKIPPABLE_HEADER_SIZE..METADATA_FRAME_SIZE].to_vec();
    assert_eq!(payload.len(), METADATA_PAYLOAD_SIZE);
    assert_eq!(payload[0..4], METADATA_MAGIC);
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&payload))).is_ok());

    let truncated = &payload[..20];
    let err = SegmentIndex::parse(&only_metadata(&metadata_frame(truncated))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let err = SegmentIndex::parse(&only_metadata(&metadata_frame(&payload[..3]))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let mut newer = payload.clone();
    newer[4..6].copy_from_slice(&(METADATA_VERSION + 1).to_le_bytes());
    let err = SegmentIndex::parse(&only_metadata(&metadata_frame(&newer))).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    assert!(err.to_string().contains("version 2"));

    let mut foreign = payload.clone();
    foreign[0..4].copy_from_slice(b"NOPE");
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&foreign))).is_err());

    let mut reserved = payload.clone();
    reserved[60] = 1;
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&reserved))).is_err());

    let mut bad_mode = payload.clone();
    bad_mode[6] = 9;
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&bad_mode))).is_err());

    let mut stray_zstd_id = payload.clone();
    stray_zstd_id[16] = 1;
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&stray_zstd_id))).is_err());

    let mut longer = payload.clone();
    longer.push(0);
    assert!(SegmentIndex::parse(&only_metadata(&metadata_frame(&longer))).is_err());

    let mut zstd_first = Vec::new();
    zstd_first.extend_from_slice(&ZSTD_FRAME_MAGIC.to_le_bytes());
    zstd_first.extend_from_slice(&[0; 4]);
    assert!(SegmentIndex::parse(&only_metadata(&zstd_first)).is_err());

    let not_first = SegmentIndex::parse(&only_metadata(&good)).unwrap_err();
    assert_eq!(not_first.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn invalid_sizes_are_errors_not_panics() {
    let blocks = blocks(8, 30, 200, 900);
    let (bytes, _) = encode(&blocks, chunked(2048));
    assert!(SegmentIndex::parse(&bytes).is_ok());

    for cut in [
        1,
        2,
        8,
        9,
        20,
        bytes.len() / 2,
        bytes.len() - METADATA_FRAME_SIZE,
    ] {
        let truncated = &bytes[..bytes.len() - cut];
        assert!(SegmentIndex::parse(&truncated).is_err(), "cut {cut}");
    }
    assert!(SegmentIndex::parse(&&bytes[..0]).is_err());
    assert!(SegmentIndex::parse(&&bytes[..5]).is_err());

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(SegmentIndex::parse(&trailing).is_err());

    let footer = bytes.len() - SEEK_TABLE_FOOTER_SIZE;
    let mut inflated = bytes.clone();
    inflated[footer..footer + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(SegmentIndex::parse(&inflated).is_err());

    let mut fewer = bytes.clone();
    let count = u32::from_le_bytes(bytes[footer..footer + 4].try_into().unwrap());
    fewer[footer..footer + 4].copy_from_slice(&(count - 1).to_le_bytes());
    assert!(SegmentIndex::parse(&fewer).is_err());

    let mut reserved = bytes.clone();
    reserved[footer + 4] = 0b0000_0100;
    assert_eq!(
        SegmentIndex::parse(&reserved).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );

    let mut bad_magic = bytes.clone();
    bad_magic[footer + 5] ^= 0xFF;
    assert!(SegmentIndex::parse(&bad_magic).is_err());

    let table = footer - count as usize * SEEK_ENTRY_SIZE - SKIPPABLE_HEADER_SIZE;
    let first_data_entry = table + SKIPPABLE_HEADER_SIZE + SEEK_ENTRY_SIZE;

    let mut grown = bytes.clone();
    let compressed = u32::from_le_bytes(
        bytes[first_data_entry..first_data_entry + 4]
            .try_into()
            .unwrap(),
    );
    grown[first_data_entry..first_data_entry + 4].copy_from_slice(&(compressed + 1).to_le_bytes());
    assert!(SegmentIndex::parse(&grown).is_err());

    let mut inflated_frame = bytes.clone();
    let decompressed = u32::from_le_bytes(
        bytes[first_data_entry + 4..first_data_entry + 8]
            .try_into()
            .unwrap(),
    );
    inflated_frame[first_data_entry + 4..first_data_entry + 8]
        .copy_from_slice(&(decompressed + 1).to_le_bytes());
    let index = SegmentIndex::parse(&inflated_frame).unwrap();
    assert!(index.decode_frame(&inflated_frame, 0, None).is_err());

    let mut shrunk_frame = bytes.clone();
    shrunk_frame[first_data_entry + 4..first_data_entry + 8]
        .copy_from_slice(&(decompressed - 1).to_le_bytes());
    let index = SegmentIndex::parse(&shrunk_frame).unwrap();
    assert!(index.decode_frame(&shrunk_frame, 0, None).is_err());

    let mut oversized = bytes.clone();
    oversized[first_data_entry + 4..first_data_entry + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    let index = SegmentIndex::parse(&oversized).unwrap();
    let err = index.decode_frame(&oversized, 0, None).unwrap_err();
    assert!(
        err.to_string().contains("seek table says 4294967295"),
        "the header check must reject the entry before allocating: {err}"
    );

    let index = SegmentIndex::parse(&bytes).unwrap();
    let len = index.logical_len();
    assert!(index.span(len, 1).is_err());
    assert!(index.span(len - 1, 2).is_err());
    assert!(index.span(u64::MAX, 1).is_err());
    assert!(index.span(u64::MAX, 0).is_err());
    assert_eq!(index.span(len, 0).unwrap(), 0..0);
    assert!(index
        .decode_frame(&bytes, index.frames().len(), None)
        .is_err());
}

#[test]
fn corrupted_frames_fail_their_checksum() {
    let blocks = blocks(9, 20, 400, 900);
    let (bytes, locations) = encode(&blocks, per_block());
    let index = SegmentIndex::parse(&bytes).unwrap();
    let frame = index.frames()[3];

    let mut corrupted = bytes.clone();
    let middle = frame.physical_offset as usize + frame.compressed_size as usize / 2;
    corrupted[middle] ^= 0x5A;
    let reader = SegmentReader::new(corrupted.as_slice(), &NoDictionaries).unwrap();
    let (offset, length) = locations[3];
    assert!(reader.read(offset, length).is_err());
    assert_eq!(
        reader.read(locations[2].0, locations[2].1).unwrap(),
        blocks[2]
    );

    let mut header = bytes.clone();
    header[frame.physical_offset as usize] ^= 0xFF;
    let reader = SegmentReader::new(header.as_slice(), &NoDictionaries).unwrap();
    assert!(reader.read(offset, length).is_err());
}

/// Frames as libzstd sees them: compressed size and declared content size.
type SpecFrames = Vec<(usize, Option<u64>)>;
/// Seek table entries as the spec lays them out.
type SpecEntries = Vec<(u32, u32)>;

/// Walk the file frame by frame with libzstd alone and parse the seek table
/// from the spec's constants, without the crate's reader.
fn spec_walk(bytes: &[u8]) -> (SpecFrames, SpecEntries) {
    let mut frames = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let size = zstd::zstd_safe::find_frame_compressed_size(&bytes[cursor..]).unwrap();
        let magic = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
        let content = if magic == ZSTD_FRAME_MAGIC {
            zstd::zstd_safe::get_frame_content_size(&bytes[cursor..cursor + size]).unwrap()
        } else {
            assert_eq!(magic & 0xFFFF_FFF0, 0x184D_2A50, "skippable magic");
            None
        };
        frames.push((size, content));
        cursor += size;
    }
    assert_eq!(cursor, bytes.len());

    let footer = &bytes[bytes.len() - SEEK_TABLE_FOOTER_SIZE..];
    assert_eq!(
        u32::from_le_bytes(footer[5..9].try_into().unwrap()),
        SEEKABLE_MAGIC
    );
    assert_eq!(footer[4], 0, "no checksum flag, no reserved bits");
    let count = u32::from_le_bytes(footer[0..4].try_into().unwrap()) as usize;
    let table = bytes.len() - SEEK_TABLE_FOOTER_SIZE - count * SEEK_ENTRY_SIZE;
    let header = &bytes[table - SKIPPABLE_HEADER_SIZE..table];
    assert_eq!(
        u32::from_le_bytes(header[0..4].try_into().unwrap()),
        SEEK_TABLE_FRAME_MAGIC
    );
    assert_eq!(
        u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize,
        count * SEEK_ENTRY_SIZE + SEEK_TABLE_FOOTER_SIZE
    );
    let entries = bytes[table..table + count * SEEK_ENTRY_SIZE]
        .chunks_exact(SEEK_ENTRY_SIZE)
        .map(|e| {
            (
                u32::from_le_bytes(e[0..4].try_into().unwrap()),
                u32::from_le_bytes(e[4..8].try_into().unwrap()),
            )
        })
        .collect();
    (frames, entries)
}

fn assert_standard_layout(bytes: &[u8], stream: &[u8], dictionary: Option<&Dictionary>) {
    let (frames, entries) = spec_walk(bytes);
    assert_eq!(
        frames.len(),
        entries.len() + 1,
        "every frame but the seek table is listed"
    );
    assert_eq!(frames[0].0, METADATA_FRAME_SIZE);
    assert_eq!(entries[0], (METADATA_FRAME_SIZE as u32, 0));
    for (i, ((size, content), (compressed, decompressed))) in
        frames.iter().zip(&entries).enumerate().skip(1)
    {
        assert_eq!(*size as u32, *compressed, "frame {i} compressed size");
        assert_eq!(
            *content,
            Some(*decompressed as u64),
            "frame {i} content size"
        );
    }
    let listed: u64 = entries.iter().map(|(_, d)| *d as u64).sum();
    assert_eq!(listed, stream.len() as u64);
    assert_eq!(decode_independently(bytes, dictionary).unwrap(), stream);

    let index = SegmentIndex::parse(&bytes).unwrap();
    let ours: Vec<(u32, u32)> = index
        .frames()
        .iter()
        .map(|f| (f.compressed_size, f.decompressed_size))
        .collect();
    assert_eq!(ours, entries[1..]);
    let mut physical = METADATA_FRAME_SIZE as u64;
    for frame in index.frames() {
        assert_eq!(frame.physical_offset, physical);
        physical += frame.compressed_size as u64;
    }
}

#[test]
fn layout_is_a_standard_seekable_stream() {
    let blocks = blocks(10, 50, 300, 1500);
    let stream = logical(&blocks);

    let (plain, _) = encode(&blocks, chunked(4096));
    assert_standard_layout(&plain, &stream, None);

    let dictionary = trained_dictionary();
    let (with_dictionary, _) = encode(&blocks, per_block().with_dictionary(dictionary.clone()));
    assert_standard_layout(&with_dictionary, &stream, Some(&dictionary));

    let index = SegmentIndex::parse(&with_dictionary).unwrap();
    let frame = index.frames()[0];
    let start = frame.physical_offset as usize;
    let header = &with_dictionary[start..start + frame.compressed_size as usize];
    assert_eq!(
        zstd::zstd_safe::get_dict_id_from_frame(header).map(|id| id.get()),
        Some(dictionary.zstd_id()),
        "frames carry the dictionary id zstd derives"
    );
    assert!(decode_independently(&with_dictionary, None).is_err());
}

fn segment(segment_id: u32, generation: u64) -> SegmentRef {
    SegmentRef {
        segment_id,
        generation,
    }
}

struct Store {
    _dir: tempfile::TempDir,
    paths: Vec<PathBuf>,
    blocks: Vec<Vec<Vec<u8>>>,
    locations: Vec<Vec<(u64, u32)>>,
    dictionaries: DictionarySet,
}

fn store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let first = trained_dictionary();
    let second = raw_dictionary();
    let dictionaries = DictionarySet::new()
        .with(first.clone())
        .with(second.clone());
    let plans = [
        per_block().with_dictionary(first),
        chunked(3000).with_dictionary(second),
        chunked(2000),
    ];
    let mut paths = Vec::new();
    let mut all_blocks = Vec::new();
    let mut all_locations = Vec::new();
    for (i, options) in plans.into_iter().enumerate() {
        let blocks = blocks(20 + i as u64, 60, 200, 1200);
        let (bytes, locations) = encode(&blocks, options);
        let path = dir.path().join(format!("{i:06}.zseg"));
        fs::write(&path, bytes).unwrap();
        paths.push(path);
        all_blocks.push(blocks);
        all_locations.push(locations);
    }
    Store {
        _dir: dir,
        paths,
        blocks: all_blocks,
        locations: all_locations,
        dictionaries,
    }
}

fn assert_within(cache: &ReadCache) {
    let stats = cache.stats();
    let limits = cache.limits();
    assert!(stats.frame_bytes <= limits.frame_bytes, "{stats:?}");
    assert!(stats.frame_entries <= limits.frame_entries, "{stats:?}");
    assert!(stats.index_entries <= limits.index_entries, "{stats:?}");
    assert!(
        stats.dictionary_entries <= limits.dictionary_entries,
        "{stats:?}"
    );
    assert!(stats.handles <= limits.handles, "{stats:?}");
    if limits.inflight_decodes > 0 {
        assert!(
            stats.inflight_decodes <= limits.inflight_decodes,
            "{stats:?}"
        );
    }
}

fn hammer(cache: &ReadCache, store: &Store, threads: usize, reads: usize) {
    thread::scope(|scope| {
        for t in 0..threads {
            scope.spawn(move || {
                let mut rng = Rng::new(100 + t as u64);
                for i in 0..reads {
                    let s = rng.below(store.paths.len() as u64) as usize;
                    let stream = logical(&store.blocks[s]);
                    let (offset, length) = if rng.below(2) == 0 {
                        store.locations[s][rng.below(store.locations[s].len() as u64) as usize]
                    } else {
                        let start = rng.below(stream.len() as u64);
                        let length = rng.below((stream.len() as u64 - start).min(5000) + 1);
                        (start, length as u32)
                    };
                    let got = cache
                        .read(
                            segment(s as u32, 1),
                            &store.paths[s],
                            offset,
                            length,
                            &store.dictionaries,
                        )
                        .unwrap();
                    assert_eq!(
                        got,
                        &stream[offset as usize..offset as usize + length as usize]
                    );
                    if i % 17 == 0 {
                        assert_within(cache);
                    }
                }
            });
        }
    });
}

#[test]
fn cache_serves_concurrent_reads_within_its_limits() {
    let store = store();
    let cache = ReadCache::new(CacheLimits {
        frame_bytes: 12_000,
        frame_entries: 6,
        index_entries: 2,
        dictionary_entries: 1,
        handles: 2,
        inflight_decodes: 3,
    });
    hammer(&cache, &store, 8, 400);
    assert_within(&cache);
    let stats = cache.stats();
    assert!(stats.frame_entries > 0 && stats.index_entries > 0 && stats.handles > 0);
    assert_eq!(stats.inflight_decodes, 0);
    assert_eq!(stats.dictionary_entries, 1);
}

#[test]
fn cache_holds_every_dictionary_when_allowed() {
    let store = store();
    let cache = ReadCache::new(CacheLimits::default());
    hammer(&cache, &store, 4, 100);
    assert_eq!(cache.stats().dictionary_entries, 2);
    assert_eq!(cache.stats().index_entries, 3);
    assert_eq!(cache.stats().handles, 3);
    assert_within(&cache);
}

#[test]
fn disabled_cache_still_reads_correctly() {
    let store = store();
    let cache = ReadCache::new(CacheLimits::DISABLED);
    hammer(&cache, &store, 4, 60);
    assert_eq!(cache.stats(), Default::default());
}

#[test]
fn eviction_and_clear_release_everything() {
    let store = store();
    let cache = ReadCache::new(CacheLimits {
        frame_bytes: 4_000,
        ..CacheLimits::default()
    });
    hammer(&cache, &store, 2, 200);
    let stats = cache.stats();
    assert!(stats.frame_bytes <= 4_000 && stats.frame_entries > 0);
    cache.invalidate(0);
    cache.invalidate(1);
    cache.invalidate(2);
    let stats = cache.stats();
    assert_eq!(
        (stats.frame_entries, stats.index_entries, stats.handles),
        (0, 0, 0)
    );
    assert_eq!(stats.dictionary_entries, 2);
    cache.clear();
    assert_eq!(cache.stats(), Default::default());
    hammer(&cache, &store, 2, 20);
}

#[test]
fn a_new_generation_never_sees_the_old_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000007.zseg");
    let old = blocks(30, 10, 300, 400);
    let new: Vec<Vec<u8>> = old.iter().map(|b| b.iter().map(|x| !x).collect()).collect();
    let (old_bytes, locations) = encode(&old, per_block());
    let (new_bytes, new_locations) = encode(&new, per_block());
    assert_eq!(locations, new_locations);
    let (offset, length) = locations[4];

    let cache = ReadCache::new(CacheLimits::default());
    fs::write(&path, &old_bytes).unwrap();
    assert_eq!(
        cache
            .read(segment(7, 1), &path, offset, length, &NoDictionaries)
            .unwrap(),
        old[4]
    );

    fs::write(&path, &new_bytes).unwrap();
    assert_eq!(
        cache
            .read(segment(7, 2), &path, offset, length, &NoDictionaries)
            .unwrap(),
        new[4]
    );
    assert_eq!(
        cache
            .read(segment(7, 1), &path, offset, length, &NoDictionaries)
            .unwrap(),
        old[4],
        "the old generation keeps serving what it cached"
    );

    cache.invalidate(7);
    assert_eq!(
        cache
            .read(segment(7, 1), &path, offset, length, &NoDictionaries)
            .unwrap(),
        new[4],
        "after invalidation only the file on disk remains"
    );
}

#[test]
fn dictionary_ids_are_content_hashes() {
    let dictionary = raw_dictionary();
    assert_eq!(DictionaryId::of(dictionary.bytes()), dictionary.id());
    let hex = dictionary.id().to_string();
    assert_eq!(hex.len(), 64);
    assert_eq!(DictionaryId::from_hex(&hex), Some(dictionary.id()));
    assert_eq!(DictionaryId::from_hex("zz"), None);
    assert!(Dictionary::verified(dictionary.bytes().to_vec(), dictionary.id()).is_ok());
    assert!(Dictionary::verified(b"other".to_vec(), dictionary.id()).is_err());
}

#[test]
fn writer_rejects_a_zero_chunk_target() {
    assert!(SegmentWriter::new(Vec::new(), chunked(0)).is_err());
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

/// The blocks every committed fixture was written from.
fn fixture_blocks() -> Vec<Vec<u8>> {
    blocks(0x5EED, 96, 300, 2500)
}

fn fixture_dictionary() -> Dictionary {
    Dictionary::new(fs::read(fixtures().join("sample.dict")).unwrap())
}

fn check_fixture(name: &str, dictionary: Option<Dictionary>, mode: FrameMode) {
    let bytes = fs::read(fixtures().join(name)).unwrap();
    let blocks = fixture_blocks();
    let stream = logical(&blocks);
    assert_standard_layout(&bytes, &stream, dictionary.as_ref());

    let set = dictionary
        .clone()
        .map(|d| DictionarySet::new().with(d))
        .unwrap_or_default();
    let reader = SegmentReader::new(bytes.as_slice(), &set).unwrap();
    assert_eq!(reader.index().metadata().mode, mode);
    assert_eq!(
        reader.index().metadata().dictionary,
        dictionary.as_ref().map(Dictionary::id)
    );
    let mut offset = 0;
    for block in &blocks {
        assert_eq!(&reader.read(offset, block.len() as u32).unwrap(), block);
        offset += block.len() as u64;
    }
}

#[test]
fn committed_chunked_fixture_reads() {
    check_fixture("chunked.zseg", None, FrameMode::Chunked { target: 4096 });
}

#[test]
fn committed_dictionary_fixture_reads() {
    check_fixture(
        "per-block-dict.zseg",
        Some(fixture_dictionary()),
        FrameMode::PerBlock,
    );
}

/// Rewrites the committed fixtures. Run only when the format changes:
/// `cargo test -p dolos-flatfiles --test compressed regenerate -- --ignored`.
#[test]
#[ignore]
fn regenerate_fixtures() {
    let samples = blocks(0xD1C7, 3000, 200, 1200);
    let blocks = fixture_blocks();
    let dictionary = Dictionary::new(zstd::dict::from_samples(&samples, 4096).unwrap());
    fs::write(fixtures().join("sample.dict"), dictionary.bytes()).unwrap();
    let (chunked, _) = encode(&blocks, chunked(4096));
    fs::write(fixtures().join("chunked.zseg"), chunked).unwrap();
    let (per_block, _) = encode(&blocks, per_block().with_dictionary(dictionary));
    fs::write(fixtures().join("per-block-dict.zseg"), per_block).unwrap();
}
