//! The boundary proof: a stele of a profile the protocol knows nothing about.
//!
//! `dev.example.toy` publishes chapters of notes. It has no chain, no epochs,
//! no blocks, no ledger — nothing Cardano-shaped and nothing Dolos-shaped. If
//! the protocol had absorbed an assumption from its first real profile, this
//! file would not compile or would not pass, which is why it is the only
//! profile in the tree while the core is being written.
//!
//! What it demonstrates, end to end:
//!
//! 1. A stele is written to a directory and read back, records intact.
//! 2. Two independent write runs produce the same inscription digest.
//! 3. Every vendor-owned string in the artifact came from the profile; the core
//!    composed none of them.
//! 4. `position`, `parameters` and `scope` survive as arbitrary shapes — the
//!    core canonicalizes and hashes them without ever typing them.
//! 5. A stele of a *different* profile, or a profile major version above the
//!    one implemented, is refused cleanly.
//! 6. Both write paths — a layer handed over whole, and a layer streamed into a
//!    sink — produce the same artifact, and this profile publishes one of each.
//! 7. A stele carrying a layer kind this build does not define is *readable* —
//!    the layer is reported and skipped — and not publishable on top of.

use std::{collections::BTreeSet, io::Write as _};

use serde_json::json;

use stelae::{
    digest::read_blob,
    dir::{BlobIndex, LayerSpec, SteleDir, WrittenLayer},
    frame::{encode, CanonicalCbor, Limits},
    Compression, Discarding, Error, Inscription, LayerDescriptor, LayerWriter, Profile, RecordSink,
    SteleReader, SteleWriter,
};

const PROFILE_NAME: &str = "dev.example.toy";
const NOTES_MEDIA_TYPE: &str = "application/vnd.example.stele.notes.v1+zstd";
const INDEX_MEDIA_TYPE: &str = "application/vnd.example.stele.index.v1+zstd";
const COVERS_MEDIA_TYPE: &str = "application/vnd.example.stele.covers.v1+zstd";
const COMPRESSION_LEVEL: i32 = 9;

/// A vendor's profile. Everything here is the vendor's business; none of it is
/// known to `stelae`.
struct ToyProfile;

impl Profile for ToyProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        match kind {
            "notes" => Ok(NOTES_MEDIA_TYPE.to_owned()),
            "index" => Ok(INDEX_MEDIA_TYPE.to_owned()),
            other => Err(Error::UnknownLayerKind {
                profile: PROFILE_NAME.to_owned(),
                kind: other.to_owned(),
            }),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        Ok(format!("chapter-{sequence}"))
    }
}

/// The profile's own record shapes. The protocol never sees inside these.
struct Note {
    id: u64,
    title: &'static str,
    body: &'static [u8],
}

const NOTES: &[Note] = &[
    Note {
        id: 1,
        title: "on stelae",
        body: b"a standing inscribed slab",
    },
    Note {
        id: 2,
        title: "on determinism",
        body: b"two publishers, one digest",
    },
    Note {
        id: 3,
        title: "on profiles",
        body: b"the vendor owns the payload",
    },
];

fn note_record(note: &Note) -> CanonicalCbor {
    encode(|e| {
        e.array(3)?
            .u64(note.id)?
            .str(note.title)?
            .bytes(note.body)?;
        Ok(())
    })
    .unwrap()
}

fn index_record(note: &Note) -> CanonicalCbor {
    encode(|e| {
        e.array(2)?.str(note.title)?.u64(note.id)?;
        Ok(())
    })
    .unwrap()
}

/// The profile's scope for the notes layer: a CBOR array in the header record,
/// the same idea as a JSON object in the inscription. Two encodings of one
/// vendor-owned concept; the protocol carries both without reading either.
fn notes_scopes() -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.array(3)?.u64(3)?.u64(1)?.u64(3)?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": 3, "firstId": 1, "lastId": 3}))
}

fn index_scopes() -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.map(1)?.str("chapter")?.u64(3)?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": 3}))
}

/// Read a layer both ways and insist the two paths agree.
///
/// Everything a consumer can observe is compared: the header, the records, the
/// digests, and — where they fail — the failure. The two readers exist because
/// one of them can afford to hold a layer and the other cannot; nothing else
/// about them is allowed to differ, because a layer that restores through one
/// and not the other is a determinism bug in the format itself.
fn read_both_ways(
    stele: &SteleDir,
    index: &BlobIndex,
    descriptor: &LayerDescriptor,
) -> Result<Vec<Vec<u8>>, Error> {
    let buffered = stele.read_layer(index, &ToyProfile, descriptor);

    // A window far below one record, so the streaming path is exercised at its
    // refill boundaries rather than swallowing the layer in one read.
    let limits = Limits {
        window: 8,
        ..Limits::default()
    };

    let streamed = (|| {
        let mut reader = stele.stream_layer(index, &ToyProfile, descriptor, limits)?;

        let mut records = Vec::new();
        while let Some(record) = reader.next_record() {
            records.push(record?.to_vec());
        }

        let digests = reader.finish()?;

        Ok::<_, Error>((records, digests))
    })();

    match (buffered, streamed) {
        (Ok(layer), Ok((records, digests))) => {
            assert_eq!(layer.digests(), &digests, "digests");

            let buffered_records: Vec<Vec<u8>> = layer
                .records()
                .map(|r| r.unwrap().to_vec())
                .collect::<Vec<_>>();

            assert_eq!(buffered_records, records, "records");

            Ok(records)
        }
        (Err(buffered), Err(streamed)) => {
            assert_eq!(
                std::mem::discriminant(&buffered),
                std::mem::discriminant(&streamed),
                "both paths must refuse for the same reason: \
                 buffered {buffered:?}, streaming {streamed:?}"
            );

            Err(buffered)
        }
        (Ok(_), Err(streamed)) => panic!("the streaming path alone refused it: {streamed:?}"),
        (Err(buffered), Ok(_)) => panic!("the buffered path alone refused it: {buffered:?}"),
    }
}

/// Write a complete stele of the toy profile into `root`.
///
/// This is the publisher side of the protocol in miniature: frame the profile's
/// records into layers, collect the descriptors the layers yield, and put them
/// in an inscription whose digest is the stele's identity.
fn write_stele(root: &std::path::Path) -> (Inscription, stelae::Digest) {
    let stele = SteleDir::create(root).unwrap();

    let (notes_header_scope, notes_scope) = notes_scopes();
    let (index_header_scope, index_scope) = index_scopes();

    let notes: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let written_notes = stele
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header_scope, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    // The index layer is streamed into a sink instead, so this profile — which
    // the protocol knows nothing about — exercises both write paths in the same
    // stele. The records are never collected: a profile publishing at Dolos
    // sizes cannot hold a layer, and the shape it needs has to work for a
    // three-note chapter too. That the goldens below do not move is the proof
    // that the two paths produce one artifact.
    let mut sorted: Vec<&Note> = NOTES.iter().collect();
    sorted.sort_by_key(|n| n.title);

    let mut index_sink = stele
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("index", index_header_scope, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    for note in sorted {
        index_sink.write_record(&index_record(note)).unwrap();
    }

    let written_index = index_sink.finish().unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east", "curator": {"name": "example", "since": 1998}}),
        json!({"noteWidth": 40, "titleOrder": "byte"}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.history = vec![
        stelae::HistoryEntry {
            sequence: 1,
            inscription_digest: stelae::Digest::from_bytes([0x11; 32]),
        },
        stelae::HistoryEntry {
            sequence: 2,
            inscription_digest: stelae::Digest::from_bytes([0x22; 32]),
        },
    ];

    inscription.layers = vec![written_notes.descriptor, written_index.descriptor];

    let digest = stele.seal(&ToyProfile, &inscription).unwrap();

    (inscription, digest)
}

#[test]
fn writes_a_stele_and_reads_it_back() {
    let temp = tempfile::tempdir().unwrap();
    let (written, digest) = write_stele(temp.path());

    // On-disk shape: one canonical document plus content-addressed blobs.
    assert!(temp.path().join("inscription.json").is_file());
    let blobs = temp.path().join("blobs").join("sha256");
    assert_eq!(std::fs::read_dir(&blobs).unwrap().count(), 2);

    let stele = SteleDir::open(temp.path()).unwrap();
    let read = stele.read_inscription().unwrap();

    assert_eq!(read, written);
    assert_eq!(read.digest().unwrap(), digest);
    read.check_profile(&ToyProfile).unwrap();

    // The layers come back through the same identity the inscription pins.
    let index = stele.blob_index().unwrap();
    assert_eq!(index.len(), 2);

    let notes_descriptor = read.layers_of_kind("notes").next().unwrap();
    let layer = stele
        .read_layer(&index, &ToyProfile, notes_descriptor)
        .unwrap();

    assert_eq!(layer.header().profile, PROFILE_NAME);
    assert_eq!(layer.header().kind, "notes");
    assert_eq!(layer.header().scope, notes_scopes().0);
    assert_eq!(layer.digests().diff_id, notes_descriptor.diff_id);

    let records: Vec<&[u8]> = layer.records().collect::<Result<_, _>>().unwrap();
    assert_eq!(records.len(), NOTES.len());

    for (record, note) in records.iter().zip(NOTES) {
        assert_eq!(*record, note_record(note).as_bytes());
    }

    // And the profile can decode its own records, which the protocol never does.
    let mut decoder = minicbor::Decoder::new(records[1]);
    assert_eq!(decoder.array().unwrap(), Some(3));
    assert_eq!(decoder.u64().unwrap(), 2);
    assert_eq!(decoder.str().unwrap(), "on determinism");
    assert_eq!(decoder.bytes().unwrap(), b"two publishers, one digest");

    // The second layer reads back the same way.
    let index_descriptor = read.layers_of_kind("index").next().unwrap();
    let index_layer = stele
        .read_layer(&index, &ToyProfile, index_descriptor)
        .unwrap();
    assert_eq!(index_layer.header().kind, "index");
    assert_eq!(index_layer.records().count(), NOTES.len());

    // And both layers read back identically without ever being held.
    for descriptor in &read.layers {
        read_both_ways(&stele, &index, descriptor).unwrap();
    }
}

/// The streaming reader is the one a restore uses, so the profile's records
/// have to survive it unchanged — through a window smaller than any of them,
/// which is the case that has to work for a 400 MB layer to be readable at all.
#[test]
fn a_layer_streams_back_record_for_record() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    let notes_descriptor = inscription.layers_of_kind("notes").next().unwrap();

    let mut reader = stele
        .stream_layer(
            &index,
            &ToyProfile,
            notes_descriptor,
            Limits {
                window: 4,
                ..Limits::default()
            },
        )
        .unwrap();

    assert_eq!(reader.header().profile, PROFILE_NAME);
    assert_eq!(reader.header().kind, "notes");
    assert_eq!(reader.header().scope, notes_scopes().0);

    let mut notes = NOTES.iter();
    while let Some(record) = reader.next_record() {
        let expected = notes.next().expect("no more records were written");
        assert_eq!(record.unwrap(), note_record(expected).as_bytes());
    }
    assert!(notes.next().is_none(), "every record came back");

    // Only now is the layer proven. Everything above was read on the strength
    // of the descriptor, which is the contract this reader makes explicit.
    let digests = reader.finish().unwrap();
    assert_eq!(digests.diff_id, notes_descriptor.diff_id);
    assert_eq!(
        digests.uncompressed_size,
        notes_descriptor.uncompressed_size
    );
}

/// `finish` is the confirmation, and it does not depend on the caller having
/// been diligent: a reader dropped after one record proves nothing, and a
/// reader finished without consuming anything still reads the whole layer,
/// because the identity digest covers every byte either way.
#[test]
fn finish_confirms_the_layer_whether_or_not_the_records_were_read() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();
    let descriptor = &inscription.layers[0];

    // Not a single content record consumed.
    let untouched = stele
        .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
        .unwrap()
        .finish()
        .unwrap();

    assert_eq!(untouched.diff_id, descriptor.diff_id);

    // One record consumed, then finished. Same verdict, same digests.
    let mut reader = stele
        .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
        .unwrap();
    reader.next_record().unwrap().unwrap();

    assert_eq!(reader.finish().unwrap(), untouched);
}

/// Done criterion 2, and the property the whole protocol rests on: the same
/// source data written twice, independently, yields the same identity.
#[test]
fn two_independent_writes_produce_the_same_inscription_digest() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let (left, left_digest) = write_stele(first.path());
    let (right, right_digest) = write_stele(second.path());

    assert_eq!(left, right);
    assert_eq!(left_digest, right_digest);
    assert_eq!(left.canonicalize().unwrap(), right.canonicalize().unwrap());

    // Byte-for-byte on disk, both the document and every blob.
    assert_eq!(
        std::fs::read(first.path().join("inscription.json")).unwrap(),
        std::fs::read(second.path().join("inscription.json")).unwrap(),
    );

    for descriptor in &left.layers {
        let left_blob = SteleDir::open(first.path())
            .unwrap()
            .blob_index()
            .unwrap()
            .blob_for(&descriptor.diff_id)
            .unwrap();
        let right_blob = SteleDir::open(second.path())
            .unwrap()
            .blob_index()
            .unwrap()
            .blob_for(&descriptor.diff_id)
            .unwrap();

        assert_eq!(left_blob, right_blob, "layer {:?}", descriptor.kind);
    }
}

/// Layer bytes survive a write → read → write round trip unchanged, so a
/// republished layer keeps its identity.
#[test]
fn layers_round_trip_byte_identically() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    for descriptor in &inscription.layers {
        let layer = stele.read_layer(&index, &ToyProfile, descriptor).unwrap();

        // Re-frame the records that came back and compare the whole sequence.
        // The header is re-encoded from its parsed form, so a field that did not
        // survive parsing would show up as a byte difference here.
        let mut rewritten = Vec::new();
        let mut writer = stelae::SeqWriter::new(&mut rewritten);

        let reencoded_header = layer.header().encode().unwrap();
        assert_eq!(reencoded_header.as_bytes(), layer.header_bytes());
        writer.write_record(&reencoded_header).unwrap();

        for record in layer.records() {
            writer
                .write_record(&CanonicalCbor::new(record.unwrap().to_vec()).unwrap())
                .unwrap();
        }

        assert_eq!(rewritten, layer.as_bytes(), "layer {:?}", descriptor.kind);
        assert_eq!(
            stelae::Digest::compute(&rewritten),
            descriptor.diff_id,
            "layer {:?} identity",
            descriptor.kind
        );
    }
}

/// The two write paths are one write path.
///
/// The same records, handed over whole and streamed a record at a time, yield
/// the same identity digest, the same blob digest, the same descriptor and the
/// same bytes on disk. `write_layer` is a wrapper over `layer_sink`, so what
/// this pins is that the wrapper stayed thin: staging, digest-naming, the
/// rename and the descriptor have one implementation, and a change that made
/// the buffered path special would have to move a digest here to land.
#[test]
fn both_write_paths_produce_the_same_layer() {
    let buffered_dir = tempfile::tempdir().unwrap();
    let streamed_dir = tempfile::tempdir().unwrap();

    let buffered_stele = SteleDir::create(buffered_dir.path()).unwrap();
    let streamed_stele = SteleDir::create(streamed_dir.path()).unwrap();

    let (header_scope, scope) = notes_scopes();
    let spec = LayerSpec::new("notes", header_scope, scope);
    let records: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let buffered = buffered_stele
        .write_layer(&ToyProfile, &spec, COMPRESSION_LEVEL, &records)
        .unwrap();

    let mut sink = streamed_stele
        .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    // A sink is a layer already in progress: the protocol's header record is
    // written before the handle is returned, so a producer only ever adds its
    // own records and the count is never off by one.
    assert_eq!(sink.records(), 1);

    for record in &records {
        sink.write_record(record).unwrap();
    }

    assert_eq!(sink.records(), 1 + NOTES.len() as u64);

    let streamed = sink.finish().unwrap();

    assert_eq!(buffered.descriptor, streamed.descriptor);
    assert_eq!(buffered.digests, streamed.digests);

    // Same blob digest means the same file name; the bytes under it are the
    // same too, which is what an OCI registry would be asked to deduplicate.
    let blob = |stele: &SteleDir, written: &WrittenLayer| {
        std::fs::read(stele.blob_path(&written.digests.blob_digest)).unwrap()
    };

    assert_eq!(
        blob(&buffered_stele, &buffered),
        blob(&streamed_stele, &streamed)
    );

    // And one layer in each stele, with no staging file beside it.
    for root in [buffered_dir.path(), streamed_dir.path()] {
        let blobs = root.join("blobs");
        assert_eq!(
            std::fs::read_dir(&blobs).unwrap().count(),
            1,
            "only sha256/"
        );
        assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 1);
    }
}

/// A layer whose blob is already on disk is deduplicated, not rewritten.
///
/// The same records under the same header scope hash to the same blob digest,
/// which is the same file name — so the second write finds its destination
/// occupied by the bytes it was about to write. Publishing it anyway would
/// rewrite hundreds of megabytes with their own contents, and on Windows would
/// collide with any reader holding the blob open. The second write is therefore
/// expected to keep the file that is there and drop its own staging copy, while
/// handing back the very same [`WrittenLayer`] the first write produced.
///
/// The proof that no bytes moved is a doctored modification time. A rename
/// replaces the file behind the name, and the timestamp belongs to the file, so
/// a mark set on the first write's blob survives only if the second write left
/// it alone — evidence that neither an equality assertion on the descriptor nor
/// a count of the directory could give on its own.
#[test]
fn a_blob_that_already_exists_is_deduplicated() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let (header_scope, scope) = notes_scopes();
    let spec = LayerSpec::new("notes", header_scope, scope);
    let records: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    // Byte-identical writes: same records, same header scope, same stele. The
    // header scope matters — it is inside the layer, so two shards of one kind
    // that differ only there are different blobs and never meet here.
    let write = || {
        let mut sink = stele
            .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
            .unwrap();

        for record in &records {
            sink.write_record(record).unwrap();
        }

        sink.finish().unwrap()
    };

    let first = write();
    let blob = stele.blob_path(&first.digests.blob_digest);
    assert!(blob.is_file());

    let mark = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000_000);
    std::fs::File::options()
        .write(true)
        .open(&blob)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(mark))
        .unwrap();

    let second = write();

    // Whatever the writer observes is what it would have observed first: the
    // descriptor and the digests come from the record stream, not from the
    // rename that did not happen.
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(first.digests, second.digests);

    // One blob, and it is the first write's file: the mark is still on it, so
    // nothing was written over it.
    assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 1);
    assert_eq!(
        std::fs::metadata(&blob).unwrap().modified().unwrap(),
        mark,
        "the blob was rewritten"
    );

    // And the duplicate staging file went with the sink that made it.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );

    // The layer the second writer describes reads back, through both readers,
    // out of the blob the first writer published.
    let index = stele.blob_index().unwrap();
    assert_eq!(index.len(), 1);

    let expected: Vec<Vec<u8>> = records.iter().map(|r| r.as_bytes().to_vec()).collect();
    assert_eq!(
        read_both_ways(&stele, &index, &second.descriptor).unwrap(),
        expected
    );
}

/// Sixteen sinks open at once, which is the case the sink exists for.
///
/// The Dolos profile shards its state into sixteen layers and cannot walk the
/// store sixteen times, so it walks once and routes each record to the shard it
/// belongs in. That works only if a sink is an ordinary independent value: no
/// borrow of the stele it will land in, no shared staging name, no ordering
/// between them. Here the records interleave across all sixteen and every layer
/// still reads back — through both readers — exactly what was routed to it.
#[test]
fn sixteen_sinks_are_written_in_one_pass() {
    const SHARDS: u64 = 16;
    const RECORDS: u64 = 8 * SHARDS;

    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let mut sinks: Vec<_> = (0..SHARDS)
        .map(|shard| {
            let header_scope = encode(|e| {
                e.array(1)?.u64(shard)?;
                Ok(())
            })
            .unwrap();

            stele
                .layer_sink(
                    &ToyProfile,
                    &LayerSpec::new("notes", header_scope, json!({"shard": shard})),
                    COMPRESSION_LEVEL,
                )
                .unwrap()
        })
        .collect();

    let mut routed: Vec<Vec<Vec<u8>>> = vec![Vec::new(); SHARDS as usize];

    for id in 0..RECORDS {
        let shard = (id % SHARDS) as usize;
        let record = encode(|e| {
            e.array(2)?.u64(id)?.str("routed")?;
            Ok(())
        })
        .unwrap();

        sinks[shard].write_record(&record).unwrap();
        routed[shard].push(record.as_bytes().to_vec());
    }

    // Sixteen staging files beside `sha256/`, and not one layer yet: nothing is
    // published until its digest is known, which is not until `finish`.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count() as u64,
        SHARDS + 1
    );
    assert!(stele.blob_index().unwrap().is_empty());

    let written: Vec<WrittenLayer> = sinks
        .into_iter()
        .map(|sink| sink.finish().unwrap())
        .collect();

    // Sixteen distinct layers, sixteen distinct blobs, nothing staged left.
    let diff_ids: BTreeSet<_> = written.iter().map(|w| w.descriptor.diff_id).collect();
    let blob_digests: BTreeSet<_> = written.iter().map(|w| w.digests.blob_digest).collect();
    assert_eq!(diff_ids.len() as u64, SHARDS);
    assert_eq!(blob_digests.len() as u64, SHARDS);
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );

    let index = stele.blob_index().unwrap();
    assert_eq!(index.len() as u64, SHARDS);

    for (shard, written) in written.iter().enumerate() {
        let records = read_both_ways(&stele, &index, &written.descriptor).unwrap();
        assert_eq!(records, routed[shard], "shard {shard}");
        assert_eq!(
            written.descriptor.records,
            routed[shard].len() as u64 + 1,
            "shard {shard} record count, header included"
        );
    }
}

/// A sink that is never finished leaves nothing behind.
///
/// The case is a mainnet export that fails partway: sixteen state shards open,
/// hundreds of megabytes written, and then a store iterator returns an error.
/// Nothing would ever *read* what is left — staging files sit beside `sha256/`
/// and `blob_index` only considers digest-named entries inside it — but leaving
/// them on the disk is its own incident, so the sink removes its file on the
/// way out.
#[test]
fn a_sink_dropped_without_finishing_leaves_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let (header_scope, scope) = notes_scopes();

    {
        let mut sink = stele
            .layer_sink(
                &ToyProfile,
                &LayerSpec::new("notes", header_scope, scope),
                COMPRESSION_LEVEL,
            )
            .unwrap();

        for note in NOTES {
            sink.write_record(&note_record(note)).unwrap();
        }

        // Open: one staging file, which is not a blob and never was.
        let staged: Vec<_> = std::fs::read_dir(&blobs)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .collect();

        assert_eq!(staged.len(), 1, "{staged:?}");
        assert!(stele.blob_index().unwrap().is_empty());
    }

    // Dropped: nothing staged, nothing published.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );
    assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 0);
    assert!(stele.blob_index().unwrap().is_empty());
}

/// Unknown #4: nothing vendor-owned in the artifact was composed by the core.
///
/// Every media type and tag in the stele is character-for-character what
/// `ToyProfile` returned. There is no fallback, no default and no template in
/// `stelae` that could have produced them — remove the profile and the strings
/// have no other source.
#[test]
fn the_core_composes_no_vendor_owned_string() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    assert_eq!(inscription.profile.name, ToyProfile.name());

    for descriptor in &inscription.layers {
        let from_profile = ToyProfile.layer_media_type(&descriptor.kind).unwrap();
        assert_eq!(descriptor.media_type, from_profile);
    }

    assert_eq!(
        stelae::profile::checked_tag_for_sequence(&ToyProfile, inscription.sequence).unwrap(),
        "chapter-3"
    );
    assert_eq!(ToyProfile.moving_tag(), "latest");

    // The canonical document mentions the vendor's names and never the
    // protocol's reserved one as a payload type.
    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();
    assert!(canonical.contains(NOTES_MEDIA_TYPE));
    assert!(canonical.contains(INDEX_MEDIA_TYPE));
    assert!(!canonical.contains("vnd.stelae.stele"));
    assert!(!canonical.contains("dolos"));
    assert!(!canonical.contains("cardano"));
}

/// The three opaque fields keep whatever the profile put in them, however alien
/// — the core canonicalizes and hashes, and never types them.
#[test]
fn opaque_fields_are_carried_not_interpreted() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let alien = json!({
        "shelf": ["east", "west"],
        "curator": {"name": "example", "since": 1998, "active": true},
        "tags": [],
        "retired": null,
    });

    let header_scope = encode(|e| {
        e.array(2)?.str("anything")?.bool(true)?;
        Ok(())
    })
    .unwrap();

    let written = stele
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", header_scope.clone(), alien.clone()),
            COMPRESSION_LEVEL,
            &[note_record(&NOTES[0])],
        )
        .unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        0,
        alien.clone(),
        alien.clone(),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.layers = vec![written.descriptor];
    stele.seal(&ToyProfile, &inscription).unwrap();

    let read = SteleDir::open(temp.path())
        .unwrap()
        .read_inscription()
        .unwrap();

    assert_eq!(read.position, alien);
    assert_eq!(read.parameters, alien);
    assert_eq!(read.layers[0].scope, alien);

    let index = SteleDir::open(temp.path()).unwrap().blob_index().unwrap();
    let layer = SteleDir::open(temp.path())
        .unwrap()
        .read_layer(&index, &ToyProfile, &read.layers[0])
        .unwrap();
    assert_eq!(layer.header().scope, header_scope);
}

/// A client fails closed on a stele it cannot read, rather than restoring part
/// of it.
#[test]
fn a_foreign_profile_is_refused() {
    struct Other;

    impl Profile for Other {
        fn name(&self) -> &str {
            "com.acme.receipts"
        }
        fn version(&self) -> u64 {
            1
        }
        fn kinds(&self) -> &[&str] {
            &["receipts"]
        }
        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.acme.stele.{kind}.v1+zstd"))
        }
        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("r-{sequence}"))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let err = inscription.check_profile(&Other).unwrap_err();
    assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");

    // A profile major version above the implemented one is refused too.
    let mut future = inscription.clone();
    future.profile.version = 2;
    let err = future.check_profile(&ToyProfile).unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedProfileVersion { .. }),
        "{err:?}"
    );

    // A layer kind the profile does not define is *not* one of these: it is
    // skippable at read and refused only on the publish side — see
    // `an_unknown_layer_kind_is_skippable_at_read_and_refused_at_publish`.
    let mut unknown_kind = inscription.clone();
    unknown_kind.layers[0].kind = "receipts".to_owned();
    unknown_kind.check_profile(&ToyProfile).unwrap();
    let err = unknown_kind.check_profile_strict(&ToyProfile).unwrap_err();
    assert!(matches!(err, Error::UnknownLayerKind { .. }), "{err:?}");
}

/// Tampering is caught on both halves of a stele: the document, whose bytes are
/// its digest, and the blobs, whose names are their digests.
#[test]
fn tampering_is_caught_on_read() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    // Re-indenting the inscription leaves the content intact but changes the
    // bytes a verifier would hash.
    let pretty = serde_json::to_vec_pretty(&inscription).unwrap();
    std::fs::write(temp.path().join("inscription.json"), &pretty).unwrap();

    let err = SteleDir::open(temp.path())
        .unwrap()
        .read_inscription()
        .unwrap_err();
    assert!(matches!(err, Error::NonCanonicalInscription), "{err:?}");

    // A blob whose content no longer matches its name is corruption.
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());
    let stele = SteleDir::open(temp.path()).unwrap();
    let blob = stele
        .blob_index()
        .unwrap()
        .blob_for(&inscription.layers[0].diff_id)
        .unwrap();

    let path = stele.blob_path(&blob);
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    assert!(
        stele.blob_index().is_err(),
        "a blob that disagrees with its name must not index"
    );
}

/// A descriptor that lies about its layer is refused even when the blob itself
/// is intact — and refused identically whether the layer is held or streamed.
#[test]
fn a_descriptor_that_disagrees_with_its_layer_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    let mut wrong_size = inscription.layers[0].clone();
    wrong_size.uncompressed_size += 1;
    let err = read_both_ways(&stele, &index, &wrong_size).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_count = inscription.layers[0].clone();
    wrong_count.records += 1;
    let err = read_both_ways(&stele, &index, &wrong_count).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_kind = inscription.layers[0].clone();
    wrong_kind.kind = "index".to_owned();
    let err = read_both_ways(&stele, &index, &wrong_kind).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut absent = inscription.layers[0].clone();
    absent.diff_id = stelae::Digest::from_bytes([0xab; 32]);
    let err = read_both_ways(&stele, &index, &absent).unwrap_err();
    assert!(matches!(err, Error::LayerNotFound { .. }), "{err:?}");

    // A descriptor claiming *less* than the layer holds is refused during
    // decompression rather than after it, on both paths: the claim is the
    // ceiling, and a blob that expands past its own descriptor is not read to
    // the end just to be told so.
    let mut too_small = inscription.layers[0].clone();
    too_small.uncompressed_size -= 1;
    let err = read_both_ways(&stele, &index, &too_small).unwrap_err();
    assert!(matches!(err, Error::DecompressedTooLarge { .. }), "{err:?}");
}

/// A malformed record is not a record.
///
/// Both readers report the first bad one and then end, so *counting* their
/// items tallies the failure itself as a record and discards the error with it.
/// A publisher who sets `records` to match that inflated number would hand back
/// a corrupt layer as `Ok`, in the one place whose documented job is to check
/// everything the descriptor claims. This is the guarantee a refill loop is
/// most likely to lose, which is why it is checked on both paths.
#[test]
fn a_malformed_record_is_reported_not_counted() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let descriptor = inscription.layers[0].clone();

    // Take the layer's bytes back out and append the head of a CBOR text string
    // that never arrives — a byte the framing must refuse.
    let blob_digest = stele
        .blob_index()
        .unwrap()
        .blob_for(&descriptor.diff_id)
        .unwrap();

    let (mut content, _) = read_blob(
        std::fs::File::open(stele.blob_path(&blob_digest)).unwrap(),
        descriptor.uncompressed_size,
    )
    .unwrap();

    content.push(0x62);

    // Store it as a blob in its own right, under a descriptor claiming exactly
    // what `count()` would have said: the real records, plus the failure.
    let mut writer = LayerWriter::new(Vec::new(), COMPRESSION_LEVEL).unwrap();
    writer.write_all(&content).unwrap();
    let (blob, digests) = writer.finish().unwrap();
    std::fs::write(stele.blob_path(&digests.blob_digest), &blob).unwrap();

    let corrupt = LayerDescriptor {
        diff_id: digests.diff_id,
        uncompressed_size: digests.uncompressed_size,
        records: descriptor.records + 1,
        ..descriptor
    };

    let index = stele.blob_index().unwrap();
    let err = read_both_ways(&stele, &index, &corrupt).unwrap_err();

    assert!(matches!(err, Error::TruncatedCbor { .. }), "{err:?}");

    // And a caller that ignores the bad record does not get a confirmation out
    // of `finish` instead. This layer is the awkward case: the malformed byte
    // is the *last* one, so every byte still reached the hasher and the digest
    // and size the descriptor claims both hold. Only the record count and the
    // reader's own memory of having failed stand between a corrupt layer and an
    // `Ok`.
    let mut reader = stele
        .stream_layer(&index, &ToyProfile, &corrupt, Limits::default())
        .unwrap();

    while let Some(record) = reader.next_record() {
        if record.is_err() {
            break;
        }
    }

    let err = reader.finish().unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");
}

/// A descriptor's media type has to be the one *this* profile defines for that
/// kind. `validate_structure` can only establish that the name is well formed
/// and does not squat the reserved vendor; whether it belongs to the profile
/// the inscription claims needs the profile in hand, which is `check_profile`.
#[test]
fn a_layer_media_type_that_is_not_the_profiles_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let with_media_type = |media_type: &str| {
        let mut tampered = inscription.clone();
        tampered.layers[0].media_type = media_type.to_owned();
        tampered
    };

    // Another vendor's name, and this vendor's name for a different kind. Both
    // are well formed, so structural validation passes them.
    for media_type in [
        "application/vnd.other.stele.notes.v1+zstd",
        INDEX_MEDIA_TYPE,
    ] {
        let tampered = with_media_type(media_type);
        tampered.validate().unwrap();

        let err = tampered.check_profile(&ToyProfile).unwrap_err();
        assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
    }

    // Version and codec are transport detail the profile may move within one
    // major, so they are not frozen here — only the vendor and the kind are.
    for media_type in [
        "application/vnd.example.stele.notes.v2+zstd",
        "application/vnd.example.stele.notes.v1+cbor",
    ] {
        with_media_type(media_type)
            .check_profile(&ToyProfile)
            .unwrap();
    }
}

/// A profile that hands back a name it does not own is stopped at the boundary,
/// before anything is written.
#[test]
fn a_profile_cannot_claim_the_protocols_namespace() {
    struct Squatter;

    impl Profile for Squatter {
        fn name(&self) -> &str {
            "dev.example.squatter"
        }
        fn version(&self) -> u64 {
            1
        }
        fn kinds(&self) -> &[&str] {
            &["notes"]
        }
        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.stelae.stele.{kind}.v1+zstd"))
        }
        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("c-{sequence}"))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let (header_scope, scope) = notes_scopes();
    let err = stele
        .write_layer(
            &Squatter,
            &LayerSpec::new("notes", header_scope, scope),
            COMPRESSION_LEVEL,
            &[note_record(&NOTES[0])],
        )
        .unwrap_err();

    assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");

    // Nothing was written.
    assert_eq!(
        std::fs::read_dir(temp.path().join("blobs").join("sha256"))
            .unwrap()
            .count(),
        0
    );
}

/// Golden digests.
///
/// Every value below is a sha256 over bytes the spec fully determines — the
/// canonical JSON of the inscription, and the deterministic CBOR sequence of
/// each layer. Nothing platform-, timing- or compression-dependent enters them,
/// so they are stable across machines and across zstd versions.
///
/// That makes this the drift alarm for the whole encoding stack. If a change to
/// the CBOR framing, the JCS canonicalization, the schema's field names or the
/// header record's shape alters a single byte, these values move — and because
/// they *are* published identity, moving one silently is the failure the
/// protocol exists to prevent. A deliberate format change updates them in the
/// same commit that changes the spec; an accidental one shows up here first.
#[test]
fn golden_digests_pin_the_encoding() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, digest) = write_stele(temp.path());

    assert_eq!(
        digest.to_string(),
        "sha256:127aa748abafed971fc7ef690a60f1c7d5d1ee49d2e25d043920545c2be2f274",
        "inscription digest drifted"
    );

    let expected_layers = [
        (
            "notes",
            "sha256:e4f2187aa877f927788b5b4d59241fa2c92de3077eae30022731b4cfba0614f8",
            4u64,
            155u64,
        ),
        (
            "index",
            "sha256:c00d73e03ccfa604f1c7ed5294f2986a4987d3400286ebda8c233503b350e502",
            4,
            77,
        ),
    ];

    for (descriptor, (kind, diff_id, records, size)) in
        inscription.layers.iter().zip(expected_layers)
    {
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.diff_id.to_string(), diff_id, "{kind} diffId");
        assert_eq!(descriptor.records, records, "{kind} record count");
        assert_eq!(descriptor.uncompressed_size, size, "{kind} size");
    }

    // The canonical document itself, so a change to key naming or ordering is
    // visible in the diff rather than only as a moved hash.
    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();
    assert_eq!(
        canonical,
        concat!(
            r#"{"compression":{"algo":"zstd","level":9},"#,
            r#""history":[{"inscriptionDigest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","sequence":1},"#,
            r#"{"inscriptionDigest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","sequence":2}],"#,
            r#""layers":[{"diffId":"sha256:e4f2187aa877f927788b5b4d59241fa2c92de3077eae30022731b4cfba0614f8","kind":"notes","#,
            r#""mediaType":"application/vnd.example.stele.notes.v1+zstd","records":4,"#,
            r#""scope":{"chapter":3,"firstId":1,"lastId":3},"uncompressedSize":155},"#,
            r#"{"diffId":"sha256:c00d73e03ccfa604f1c7ed5294f2986a4987d3400286ebda8c233503b350e502","kind":"index","#,
            r#""mediaType":"application/vnd.example.stele.index.v1+zstd","records":4,"#,
            r#""scope":{"chapter":3},"uncompressedSize":77}],"#,
            r#""parameters":{"noteWidth":40,"titleOrder":"byte"},"#,
            r#""position":{"chapter":3,"curator":{"name":"example","since":1998},"shelf":"east"},"#,
            r#""profile":{"name":"dev.example.toy","version":1},"schema":1,"sequence":3}"#,
        )
    );
}

/// The discarding writer is faithful, not merely fast.
///
/// The same records through both write halves: one into a directory, one into
/// nothing. Every field of the descriptor and every one of the four digests and
/// sizes has to agree — including the *blob* digest and the compressed size,
/// which only exist if zstd actually ran. That is the assertion this test is
/// for: a discarding writer that skipped compression would still reproduce
/// `diffId`, `records` and `uncompressedSize`, and would be exactly as wrong as
/// one that never ran at all.
///
/// The seal is compared too, since a reproduction reports an identity: a
/// directory's comes from the bytes it wrote to `inscription.json`, and this
/// one from the document in hand.
#[test]
fn a_discarding_writer_reproduces_what_a_directory_stores() {
    let temp = tempfile::tempdir().unwrap();
    let (stored, stored_digest) = write_stele(temp.path());

    let (notes_header_scope, notes_scope) = notes_scopes();
    let (index_header_scope, index_scope) = index_scopes();

    let notes: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let reproduced_notes = Discarding
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header_scope, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    let mut sorted: Vec<&Note> = NOTES.iter().collect();
    sorted.sort_by_key(|n| n.title);

    let mut index_sink = Discarding
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("index", index_header_scope, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    for note in sorted {
        index_sink.write_record(&index_record(note)).unwrap();
    }

    let reproduced_index = index_sink.finish().unwrap();

    // The stored stele's own blob digests, recovered the way a directory has
    // to: by hashing the files it holds. Nothing in an inscription carries
    // them, which is the point — they are transport, and a reproduction that
    // agreed on identity while disagreeing on the compressed bytes would still
    // publish a different blob.
    let stored_blobs: BTreeSet<String> =
        std::fs::read_dir(temp.path().join("blobs").join(stelae::Digest::ALGORITHM))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

    for (stored, reproduced) in stored
        .layers
        .iter()
        .zip([&reproduced_notes, &reproduced_index])
    {
        assert_eq!(
            *stored, reproduced.descriptor,
            "{}: the descriptor a publish would have written",
            stored.kind,
        );

        assert!(
            stored_blobs.contains(&reproduced.digests.blob_digest.to_hex()),
            "{}: the reproduction named a blob the directory does not hold ({})",
            stored.kind,
            reproduced.digests.blob_digest,
        );

        let on_disk = std::fs::metadata(
            temp.path()
                .join("blobs")
                .join(stelae::Digest::ALGORITHM)
                .join(reproduced.digests.blob_digest.to_hex()),
        )
        .unwrap()
        .len();

        assert_eq!(
            reproduced.digests.compressed_size, on_disk,
            "{}: the compressed size only exists if zstd ran",
            stored.kind,
        );
    }

    // And the identity, over a document assembled exactly as `write_stele`
    // assembles it.
    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east", "curator": {"name": "example", "since": 1998}}),
        json!({"noteWidth": 40, "titleOrder": "byte"}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.history = stored.history.clone();
    inscription.layers = vec![
        reproduced_notes.descriptor.clone(),
        reproduced_index.descriptor.clone(),
    ];

    assert_eq!(
        Discarding.seal(&ToyProfile, &inscription).unwrap(),
        stored_digest,
    );

    // Nothing was written anywhere on the way: the only stele on disk is the
    // one the directory wrote, and it has exactly its own two blobs.
    assert_eq!(stored_blobs.len(), 2);
}

/// The same vendor, one kind ahead: `dev.example.toy` after it started
/// publishing cover art. Same profile name and same major version, because an
/// *additive* kind is exactly the change that does not break a reader — which
/// is the claim the tests below check rather than assume.
struct FutureToyProfile;

impl Profile for FutureToyProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index", "covers"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        match kind {
            "covers" => Ok(COVERS_MEDIA_TYPE.to_owned()),
            other => ToyProfile.layer_media_type(other),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        ToyProfile.tag_for_sequence(sequence)
    }
}

/// Write the toy stele, then have the newer publisher add its `covers` layer
/// and re-seal.
///
/// Through the ordinary writer and the ordinary seal, so what the assertions
/// read back is a stele somebody could have published — not an inscription with
/// a descriptor pasted into it, which would prove nothing about the layer being
/// real.
fn published_ahead(root: &std::path::Path, scope: serde_json::Value) -> Inscription {
    let (mut inscription, _) = write_stele(root);
    let stele = SteleDir::open(root).unwrap();

    let header = encode(|e| {
        e.map(1)?.str("chapter")?.u64(3)?;
        Ok(())
    })
    .unwrap();

    let cover = encode(|e| {
        e.str("a woodcut of the east shelf")?;
        Ok(())
    })
    .unwrap();

    let written = stele
        .write_layer(
            &FutureToyProfile,
            &LayerSpec::new("covers", header, scope),
            COMPRESSION_LEVEL,
            &[cover],
        )
        .unwrap();

    inscription.layers.push(written.descriptor);
    stele.seal(&FutureToyProfile, &inscription).unwrap();

    inscription
}

/// The blast radius of an additive kind, in one test.
///
/// A profile that gains a kind publishes it as a new media type on a new layer.
/// If an older reader refused the whole stele over it, every additive change
/// would brick every deployed reader; so the reader takes the document, keeps
/// the layers it models, and *reports* the one it does not. The publish side
/// keeps the old rule, because a publisher attests every layer it lists.
#[test]
fn an_unknown_layer_kind_is_skippable_at_read_and_refused_at_publish() {
    let temp = tempfile::tempdir().unwrap();
    let ahead = published_ahead(temp.path(), json!({"chapter": 3}));

    // The older reader takes the document.
    ahead.check_profile(&ToyProfile).unwrap();

    // And the layer it cannot model comes back whole — kind and scope — which
    // is what leaves the skip-or-refuse decision with the profile rather than
    // with the protocol.
    let unknown = ahead.unknown_layers(&ToyProfile);
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].kind, "covers");
    assert_eq!(unknown[0].scope, json!({"chapter": 3}));

    // The publisher that wrote it skips nothing, and may chain onto it.
    assert!(ahead.unknown_layers(&FutureToyProfile).is_empty());
    ahead.check_profile_strict(&FutureToyProfile).unwrap();

    // The older binary may not: it would either drop `covers` from the stele it
    // publishes next or attest bytes it never read.
    let err = ahead.check_profile_strict(&ToyProfile).unwrap_err();
    assert!(
        matches!(&err, Error::UnknownLayerKind { kind, .. } if kind == "covers"),
        "{err:?}"
    );

    // Skipping is about consumption and nothing else: the layers the old reader
    // does model are still reachable through the identities the inscription
    // pins, and the skipped one is still a layer of the stele.
    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();
    assert_eq!(index.len(), 3);

    for kind in ["notes", "index"] {
        let descriptor = ahead.layers_of_kind(kind).next().unwrap();
        stele.read_layer(&index, &ToyProfile, descriptor).unwrap();
    }
}

/// `required: true` in a layer's scope is a publisher telling older readers
/// that this layer is not optional: refuse the stele rather than restore a
/// partial one.
///
/// The protocol never reads the flag — a scope is profile-owned and opaque — so
/// the planner below is the whole of the profile side, written out to show how
/// little `unknown_layers` leaves it to do.
#[test]
fn a_required_unknown_layer_is_the_profiles_own_refusal() {
    fn plan(inscription: &Inscription) -> Result<Vec<String>, String> {
        let unknown = inscription.unknown_layers(&ToyProfile);

        match unknown
            .iter()
            .find(|layer| layer.scope.get("required") == Some(&json!(true)))
        {
            Some(layer) => Err(format!("{} is required: {}", layer.kind, layer.scope)),
            None => Ok(unknown.iter().map(|layer| layer.kind.clone()).collect()),
        }
    }

    let optional = tempfile::tempdir().unwrap();
    let skipped = plan(&published_ahead(optional.path(), json!({"chapter": 3}))).unwrap();
    assert_eq!(skipped, vec!["covers".to_owned()]);

    // The same stele, the same reader, one flag apart.
    let required = tempfile::tempdir().unwrap();
    let refusal = plan(&published_ahead(
        required.path(),
        json!({"chapter": 3, "required": true}),
    ))
    .unwrap_err();

    assert!(refusal.contains("covers"), "{refusal}");
    assert!(refusal.contains("chapter"), "{refusal}");

    // `required` is a scope field like any other to the protocol: it neither
    // makes the layer known nor stops the document being read.
    let stele = SteleDir::open(required.path()).unwrap();
    stele
        .read_inscription()
        .unwrap()
        .check_profile(&ToyProfile)
        .unwrap();
}
