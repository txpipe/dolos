//! Peak memory during a layer read is a property, not an aspiration.
//!
//! The protocol's read path used to hold a whole uncompressed layer: fine for a
//! fixture, fatal at the sizes a profile publishes (ADR-004's worked example
//! gives a state shard of 402,653,184 bytes). These tests are what stops that
//! from coming back. They instrument the global allocator with `stats_alloc` —
//! the idiom the root package's `tests/memory.rs` uses for store iteration —
//! and read a layer far larger than the reader's window while watching what the
//! process asks for.
//!
//! `bytes_allocated` is cumulative over the region, which is a *stronger*
//! statement than peak: a run that never allocates more than N bytes in total
//! certainly never holds more than N at once. It also means a reader that
//! allocated and freed one record per iteration would be caught here, not
//! excused by its tidiness.
//!
//! ## The write path is measured differently, and has to be
//!
//! Cumulative is unavailable on the write side. A producer handing 32,768
//! records to a layer has to *encode* 32,768 records, and those allocations are
//! its own — incurred identically whether it buffers them or streams them, so a
//! cumulative figure reports the same ~33 MB for both paths and distinguishes
//! nothing. What separates them is what is *held*: bytes allocated inside the
//! region and not yet given back, sampled at every record and maximized. That
//! is peak rather than a bound on peak, which is the weaker of the two claims —
//! stated here rather than quietly substituted.
//!
//! ## Why these tests take a lock
//!
//! A `Region` reads a *process-wide* counter, and the test harness runs tests
//! in parallel by default — so a second test's setup, which is tens of
//! megabytes here, lands inside the first test's measurement and swamps a
//! 64 KiB budget. It is a race, so it passes on the machine you tried it on and
//! fails on the one you did not (this one surfaced on Windows CI). Every test
//! in this file therefore holds [`SERIAL`] for its whole body, setup included.

use std::{
    alloc::System,
    sync::{Mutex, MutexGuard},
};

use serde_json::json;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};

use stelae::{
    dir::{LayerSpec, SteleDir},
    frame::{encode, CanonicalCbor, Limits, RecordReader, SeqWriter},
    Compression, Error, Inscription, Profile, RecordSink, SteleReader, SteleWriter,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Held by every test in this file, so that only one is allocating at a time.
/// A new test that forgets to take it will not fail here — it will make some
/// *other* test flaky, which is the failure worth spending a comment on.
static SERIAL: Mutex<()> = Mutex::new(());

fn exclusive() -> MutexGuard<'static, ()> {
    // A poisoned lock means another test in this file panicked. That is its
    // failure to report; this one still wants an uncontended allocator.
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const PROFILE_NAME: &str = "dev.example.bulk";
const COMPRESSION_LEVEL: i32 = 3;

/// Records of roughly a kilobyte, which is the order of a Dolos `indexes` or
/// `state` record. Blocks are larger, but the point of the bound is that the
/// layer's size does not enter it.
const RECORD_BODY: usize = 1000;

/// ~33 MB of layer against a 64 KiB window: 500 windows, so a reader that
/// buffered even a percent of the layer would show up.
const RECORDS: u64 = 32 * 1024;

/// How many windows the layer must span for these tests to be worth running.
const MINIMUM_WINDOWS: usize = 400;

/// What the streaming path is allowed to allocate, cumulatively, to read all of
/// it. Two orders of magnitude below the layer, and it does not move when the
/// layer does — which is the property under test.
///
/// Above the 64 KiB window because the zstd bindings buffer on either side of
/// the decoder. What this does *not* cover is libzstd's own context: that is
/// `malloc`ed inside the C library and never reaches a Rust global allocator,
/// so `stats_alloc` cannot see it. It is bounded by the frame's window size —
/// a property of the compression parameters, not of the layer — so it does not
/// weaken the claim, but the number below is the Rust side only and saying so
/// is cheaper than someone rediscovering it.
const STREAMING_BUDGET: usize = 1024 * 1024;

/// What the streaming *write* path is allowed to hold at any one moment: one
/// record, the compressor's buffers, and the sink's own handful of fields.
///
/// The same figure as the read budget, for the same reasons — the zstd bindings
/// buffer on either side of the encoder, and libzstd's own context is
/// `malloc`ed inside the C library where `stats_alloc` cannot see it. The
/// observed peak is around 34 KB against a 33 MB layer, so this is a ceiling
/// with room under it rather than a number tuned until the test passed; what
/// matters is that it does not move when the layer does.
const SINK_BUDGET: usize = 1024 * 1024;

/// Bytes allocated inside `region` and not yet returned.
///
/// Saturating because a region can also *free* memory that was allocated before
/// it began, which is a negative change and not a measurement of anything.
fn in_flight(region: &Region<'_, System>) -> usize {
    let change = region.change();
    change
        .bytes_allocated
        .saturating_sub(change.bytes_deallocated)
}

struct BulkProfile;

impl Profile for BulkProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["bulk"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        Ok(format!("application/vnd.example.stele.{kind}.v1+zstd"))
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        Ok(format!("bulk-{sequence}"))
    }
}

fn bulk_record(i: u64) -> CanonicalCbor {
    // Not a constant body: identical records would compress to nothing and let
    // a decoder cheat its way through the test.
    let body: Vec<u8> = (0..RECORD_BODY).map(|b| (b as u64 ^ i) as u8).collect();

    encode(|e| {
        e.array(2)?.u64(i)?.bytes(&body)?;
        Ok(())
    })
    .unwrap()
}

fn scope() -> CanonicalCbor {
    encode(|e| {
        e.u64(0)?;
        Ok(())
    })
    .unwrap()
}

/// The raw uncompressed sequence, as it appears inside a layer blob.
fn sequence() -> Vec<u8> {
    let mut writer = SeqWriter::new(Vec::new());

    for i in 0..RECORDS {
        writer.write_record(&bulk_record(i)).unwrap();
    }

    writer.into_inner()
}

/// The framing reader on its own: no compression, no digests, nothing but the
/// refill loop. The bound here is tight enough to name — one window, plus the
/// small change of the walk.
#[test]
fn framing_a_large_sequence_costs_one_window() {
    let _serial = exclusive();

    let sequence = sequence();

    let window = 64 * 1024;
    let budget = 2 * window;

    assert!(
        sequence.len() > MINIMUM_WINDOWS * window,
        "the layer has to dwarf the window for this to prove anything"
    );

    let region = Region::new(GLOBAL);

    let mut reader = RecordReader::with_limits(
        std::io::Cursor::new(sequence.as_slice()),
        Limits {
            window,
            ..Limits::default()
        },
    );

    let mut count = 0u64;
    while let Some(record) = reader.next_record() {
        // Touch the record so nothing about this loop is optimized away, but
        // never keep it: holding records is the caller's choice to make, and
        // this caller declines.
        assert!(!record.unwrap().is_empty());
        count += 1;
    }

    let allocated = region.change().bytes_allocated;

    assert_eq!(count, RECORDS);
    assert!(
        allocated < budget,
        "framing {} bytes should cost one {window}-byte window, not {allocated} bytes",
        sequence.len(),
    );
}

/// The whole path, as a restore would use it: a compressed blob on disk,
/// verified against its descriptor, records walked without ever holding the
/// layer.
#[test]
fn streaming_a_layer_does_not_scale_with_its_size() {
    let _serial = exclusive();

    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let records: Vec<CanonicalCbor> = (0..RECORDS).map(bulk_record).collect();

    let written = stele
        .write_layer(
            &BulkProfile,
            &LayerSpec::new("bulk", scope(), json!({})),
            COMPRESSION_LEVEL,
            &records,
        )
        .unwrap();

    let mut inscription = Inscription::new(
        &BulkProfile,
        0,
        json!({}),
        json!({}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.layers = vec![written.descriptor.clone()];
    stele.seal(&BulkProfile, &inscription).unwrap();

    let descriptor = &inscription.layers[0];
    assert!(
        descriptor.uncompressed_size > (MINIMUM_WINDOWS * stelae::frame::DEFAULT_WINDOW) as u64,
        "the layer has to dwarf the window for this to prove anything"
    );

    // Everything before the region is setup: building the index is a scan of
    // the stele, and what it costs is not what is under test.
    drop(records);
    let index = stele.blob_index().unwrap();

    let region = Region::new(GLOBAL);

    let mut reader = stele
        .stream_layer(&index, &BulkProfile, descriptor, Limits::default())
        .unwrap();

    let mut count = 1u64; // the header record, already consumed
    while let Some(record) = reader.next_record() {
        assert!(!record.unwrap().is_empty());
        count += 1;
    }

    let digests = reader.finish().unwrap();

    let allocated = region.change().bytes_allocated;

    assert_eq!(count, descriptor.records);
    assert_eq!(digests.diff_id, descriptor.diff_id);
    assert!(
        allocated < STREAMING_BUDGET,
        "streaming a {}-byte layer allocated {allocated} bytes; \
         the budget is {STREAMING_BUDGET}",
        descriptor.uncompressed_size,
    );

    // The control. Without it this test proves only that some number is small:
    // the buffered path reads the same blob and, by design, pays the layer's
    // size for it. If that ever stops being true the budget above has stopped
    // measuring the difference between the two paths.
    let region = Region::new(GLOBAL);
    let layer = stele.read_layer(&index, &BulkProfile, descriptor).unwrap();
    let buffered = region.change().bytes_allocated;

    assert_eq!(layer.as_bytes().len() as u64, descriptor.uncompressed_size);
    assert!(
        buffered as u64 > descriptor.uncompressed_size,
        "the buffered path allocated {buffered} bytes for a {}-byte layer; \
         it is supposed to hold the whole thing",
        descriptor.uncompressed_size,
    );
}

/// The same property on the way in: a producer streams a layer it could not
/// hold.
///
/// This is the bound the Dolos export needs. `write_layer` takes an iterator of
/// *references*, so every record has to be materialized somewhere that outlives
/// the call — fine for a chapter of notes, impossible for a mainnet epoch of
/// blocks at 0.5–1.5 GB. A sink takes each record by reference for the length
/// of one call and keeps nothing.
///
/// Both paths write the same layer here, so the comparison is between two ways
/// of producing one artifact and not between two artifacts. They go into
/// separate steles: the blob is named by its own digest, so writing it twice
/// into one directory would be a rename onto a file that is already there.
#[test]
fn writing_a_layer_through_a_sink_does_not_scale_with_its_size() {
    let _serial = exclusive();

    let streamed_dir = tempfile::tempdir().unwrap();
    let buffered_dir = tempfile::tempdir().unwrap();

    let streamed_stele = SteleDir::create(streamed_dir.path()).unwrap();
    let buffered_stele = SteleDir::create(buffered_dir.path()).unwrap();

    let spec = LayerSpec::new("bulk", scope(), json!({}));

    // The sink. Each record is encoded, written and dropped; the peak is
    // sampled with the record still in hand, so what it reports is one record
    // plus whatever the protocol is holding on its behalf.
    let region = Region::new(GLOBAL);

    let mut sink = streamed_stele
        .layer_sink(&BulkProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    let mut held = 0usize;

    for i in 0..RECORDS {
        let record = bulk_record(i);
        sink.write_record(&record).unwrap();
        held = held.max(in_flight(&region));
    }

    let streamed = sink.finish().unwrap();
    let streamed_held = held.max(in_flight(&region));

    // The control. Without it this test proves only that some number is small:
    // the buffered path writes the same records and, by construction, has to
    // hold every one of them until the call returns.
    let region = Region::new(GLOBAL);

    let records: Vec<CanonicalCbor> = (0..RECORDS).map(bulk_record).collect();
    let buffered = buffered_stele
        .write_layer(&BulkProfile, &spec, COMPRESSION_LEVEL, &records)
        .unwrap();

    let buffered_held = in_flight(&region);
    drop(records);

    let size = streamed.descriptor.uncompressed_size;

    assert!(
        size > (16 * SINK_BUDGET) as u64,
        "the layer has to dwarf the budget for this to prove anything: \
         {size} bytes against {SINK_BUDGET}"
    );

    // One artifact, two ways of writing it.
    assert_eq!(streamed.descriptor, buffered.descriptor);
    assert_eq!(streamed.digests, buffered.digests);

    assert!(
        streamed_held < SINK_BUDGET,
        "streaming a {size}-byte layer held {streamed_held} bytes at peak; \
         the budget is {SINK_BUDGET}",
    );

    assert!(
        buffered_held as u64 > size,
        "the buffered path held {buffered_held} bytes for a {size}-byte layer; \
         it is supposed to hold the whole thing",
    );
}
