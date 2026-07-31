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

use std::alloc::System;

use serde_json::json;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};

use stelae::{
    dir::{LayerSpec, SteleDir},
    frame::{encode, CanonicalCbor, Limits, RecordReader, SeqWriter},
    Compression, Error, Inscription, Profile,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

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
    stele.write_inscription(&inscription).unwrap();

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
