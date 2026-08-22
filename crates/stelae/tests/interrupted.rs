//! A read that was interrupted is not a read that failed.
//!
//! Why the crate retries `ErrorKind::Interrupted` at all is stated once, on
//! `digest::read_uninterrupted`; what these tests add is that the three loops
//! routed through it actually hold the property under interruption.
//!
//! Every test below reads through [`Interrupting`], which raises `Interrupted`
//! before *every* successful read and then hands over a few bytes.
//! Interruptions therefore land between records and inside them, and the
//! results — records, digests, sizes, content — must be indistinguishable from
//! a read of the same bytes that was never interrupted.

use std::io::{self, Read, Write as _};

use stelae::{
    digest::{digest_reader, read_blob, scan_blob},
    frame::{encode, Limits, RecordReader, SeqWriter},
    LayerWriter,
};

/// A source that returns `ErrorKind::Interrupted` before every successful read,
/// and yields at most `chunk` bytes when it does succeed.
///
/// `chunk` is deliberately not a multiple of the record size: a source that
/// only ever stopped on a record boundary would leave the harder half of the
/// property — an interruption in the middle of a record the reader is still
/// assembling — untested.
struct Interrupting<'a> {
    remaining: &'a [u8],
    chunk: usize,
    interrupt_next: bool,
    interruptions: usize,
}

impl<'a> Interrupting<'a> {
    fn new(bytes: &'a [u8], chunk: usize) -> Self {
        Self {
            remaining: bytes,
            chunk,
            interrupt_next: true,
            interruptions: 0,
        }
    }
}

impl Read for Interrupting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.interrupt_next {
            self.interrupt_next = false;
            self.interruptions += 1;
            return Err(io::Error::new(io::ErrorKind::Interrupted, "signal"));
        }

        self.interrupt_next = true;

        let take = self.chunk.min(buf.len()).min(self.remaining.len());
        buf[..take].copy_from_slice(&self.remaining[..take]);
        self.remaining = &self.remaining[take..];

        Ok(take)
    }
}

const RECORDS: u64 = 256;

/// Not a divisor of any record's encoded length, so the cut points walk across
/// record boundaries rather than lining up with them.
const CHUNK: usize = 7;

fn record(i: u64) -> stelae::CanonicalCbor {
    encode(|e| {
        e.array(2)?
            .u64(i)?
            .str("a repeated payload that compresses")?;
        Ok(())
    })
    .unwrap()
}

fn sequence() -> Vec<u8> {
    let mut writer = SeqWriter::new(Vec::new());

    for i in 0..RECORDS {
        writer.write_record(&record(i)).unwrap();
    }

    writer.into_inner()
}

fn layer_blob(content: &[u8]) -> Vec<u8> {
    let mut writer = LayerWriter::new(Vec::new(), 9).unwrap();
    writer.write_all(content).unwrap();
    writer.finish().unwrap().0
}

/// `RecordReader::fill` refills a bounded window from the stream. An
/// interruption there used to end the walk with an `Io` error partway through a
/// layer; the whole sequence has to come out instead, in order, once.
#[test]
fn a_record_reader_walks_an_interrupted_stream_to_the_end() {
    let bytes = sequence();

    let mut source = Interrupting::new(&bytes, CHUNK);
    let mut reader = RecordReader::with_limits(
        &mut source,
        Limits {
            // Small enough that the window refills many times over the
            // sequence, so the retry is exercised on every kind of boundary.
            window: 128,
            ..Default::default()
        },
    );

    let mut seen = Vec::new();
    while let Some(next) = reader.next_record() {
        seen.push(next.unwrap().to_vec());
    }

    assert!(!reader.failed());
    assert_eq!(reader.count(), RECORDS);
    assert_eq!(
        seen,
        (0..RECORDS)
            .map(|i| record(i).as_ref().to_vec())
            .collect::<Vec<_>>()
    );
    assert!(
        source.interruptions > RECORDS as usize,
        "the source should have interrupted far more often than once per record, \
         got {}",
        source.interruptions
    );
}

/// `digest_reader` is what checks a stored blob against the digest it is named
/// by. An interruption must not turn a good blob into a failed verification.
#[test]
fn digest_reader_hashes_every_byte_of_an_interrupted_stream() {
    let blob = layer_blob(&sequence());
    let expected = digest_reader(blob.as_slice()).unwrap();

    let interrupted = digest_reader(Interrupting::new(&blob, CHUNK)).unwrap();

    assert_eq!(interrupted, expected);
    assert_eq!(interrupted.1, blob.len() as u64);
}

/// Both blob readers run the decompressor over the stream, so the interruption
/// surfaces underneath zstd rather than at the top of the loop. Digests, sizes
/// and the uncompressed content all have to match the uninterrupted read.
#[test]
fn a_blob_reads_back_identically_through_interruptions() {
    let content = sequence();
    let blob = layer_blob(&content);
    let expected = scan_blob(blob.as_slice()).unwrap();

    let scanned = scan_blob(Interrupting::new(&blob, CHUNK)).unwrap();
    assert_eq!(scanned, expected);

    let (read, digests) =
        read_blob(Interrupting::new(&blob, CHUNK), expected.uncompressed_size).unwrap();
    assert_eq!(read, content);
    assert_eq!(digests, expected);
}

/// The retry is for `Interrupted` and nothing else: a source that fails for a
/// real reason still fails, and it fails at the loop that read it.
#[test]
fn a_real_io_error_still_ends_the_read() {
    struct Broken;

    impl Read for Broken {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::ConnectionReset, "peer went"))
        }
    }

    assert!(digest_reader(Broken).is_err());
    assert!(scan_blob(Broken).is_err());

    let mut reader = RecordReader::new(Broken);
    assert!(reader.next_record().unwrap().is_err());
}
