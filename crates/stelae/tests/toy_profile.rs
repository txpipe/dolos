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

use std::io::Write as _;

use serde_json::json;

use stelae::{
    digest::read_blob,
    dir::{LayerSpec, SteleDir},
    frame::{encode, CanonicalCbor},
    Compression, Error, Inscription, LayerDescriptor, LayerWriter, Profile,
};

const PROFILE_NAME: &str = "dev.example.toy";
const NOTES_MEDIA_TYPE: &str = "application/vnd.example.stele.notes.v1+zstd";
const INDEX_MEDIA_TYPE: &str = "application/vnd.example.stele.index.v1+zstd";
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
    let mut sorted: Vec<&Note> = NOTES.iter().collect();
    sorted.sort_by_key(|n| n.title);
    let index: Vec<CanonicalCbor> = sorted.into_iter().map(index_record).collect();

    let written_notes = stele
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header_scope, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    let written_index = stele
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("index", index_header_scope, index_scope),
            COMPRESSION_LEVEL,
            &index,
        )
        .unwrap();

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

    let digest = stele.write_inscription(&inscription).unwrap();

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
    stele.write_inscription(&inscription).unwrap();

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

    // As is a layer kind the profile does not define.
    let mut unknown_kind = inscription.clone();
    unknown_kind.layers[0].kind = "receipts".to_owned();
    let err = unknown_kind.check_profile(&ToyProfile).unwrap_err();
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
/// is intact.
#[test]
fn a_descriptor_that_disagrees_with_its_layer_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let stele = SteleDir::open(temp.path()).unwrap();
    let index = stele.blob_index().unwrap();

    let mut wrong_size = inscription.layers[0].clone();
    wrong_size.uncompressed_size += 1;
    let err = stele
        .read_layer(&index, &ToyProfile, &wrong_size)
        .unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_count = inscription.layers[0].clone();
    wrong_count.records += 1;
    let err = stele
        .read_layer(&index, &ToyProfile, &wrong_count)
        .unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_kind = inscription.layers[0].clone();
    wrong_kind.kind = "index".to_owned();
    let err = stele
        .read_layer(&index, &ToyProfile, &wrong_kind)
        .unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut absent = inscription.layers[0].clone();
    absent.diff_id = stelae::Digest::from_bytes([0xab; 32]);
    let err = stele.read_layer(&index, &ToyProfile, &absent).unwrap_err();
    assert!(matches!(err, Error::LayerNotFound { .. }), "{err:?}");
}

/// A malformed record is not a record.
///
/// `SeqReader` reports the first bad one and then ends, so *counting* the
/// iterator's items tallies the failure itself as a record and discards the
/// error with it. A publisher who sets `records` to match that inflated number
/// would hand back a corrupt layer as `Ok`, in the one place whose documented
/// job is to check everything the descriptor claims.
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
    let err = stele.read_layer(&index, &ToyProfile, &corrupt).unwrap_err();

    assert!(matches!(err, Error::TruncatedCbor { .. }), "{err:?}");
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
