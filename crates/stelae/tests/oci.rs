//! A stele in a registry, and the delta transfer that is the reason for it.
//!
//! Two kinds of test live here and they have opposite requirements, which is
//! why they share a file rather than a run:
//!
//! - [`the_manifest_shape_is_frozen`] and the refusals beside it need **no
//!   network and no compressor**. They build a manifest from fixed digests and
//!   compare the exact bytes, so a change to the manifest's shape is a
//!   deliberate re-pin in the same commit and never a silent drift. They run in
//!   CI, under `--all-features`, like any other test.
//! - everything marked `#[ignore]` needs a **registry**, and spawns one:
//!   `docker run` of an OCI Distribution server, torn down on the way out. They
//!   are the ones that prove a stele survives the round trip, that the second
//!   push moves only what the registry lacks, and that neither direction holds
//!   a layer.
//!
//! Run the second kind with:
//!
//! ```text
//! cargo test -p stelae --all-features --test oci -- --ignored --nocapture
//! ```
//!
//! `STELAE_TEST_REGISTRY_IMAGE` chooses the server (default `registry:2`), so
//! the same suite can be pointed at another implementation — which is the only
//! way to find out whether a given registry accepts an OCI 1.1 `artifactType`.

#![cfg(feature = "oci")]

use std::{
    alloc::System,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex, MutexGuard,
    },
};

use serde_json::json;
use stats_alloc::{StatsAlloc, INSTRUMENTED_SYSTEM};

use stelae::{
    dir::SteleDir,
    frame::{encode, CanonicalCbor, Limits},
    inscription::LayerDescriptor,
    oci::{
        build_manifest, manifest_bytes, read_manifest, Options, Registry, Transfer,
        DIFF_ID_ANNOTATION, KIND_ANNOTATION, SCOPE_ANNOTATION,
    },
    Compression, Digest, Error, HistoryEntry, Inscription, LayerDigests, LayerSpec, Profile,
    RecordSink, SteleReader, SteleWriter, WrittenLayer,
};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Held by every test that spawns a registry.
///
/// Two reasons, and the second is the one that would otherwise be found the
/// hard way: containers are expensive enough that starting five at once is
/// slower than starting them in turn, and the peak-allocation test reads a
/// *process-wide* counter, which cannot tell one test's allocations from
/// another's.
static SERIAL: Mutex<()> = Mutex::new(());

fn exclusive() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const PROFILE_NAME: &str = "dev.example.toy";
const NOTES_MEDIA_TYPE: &str = "application/vnd.example.stele.notes.v1+zstd";
const INDEX_MEDIA_TYPE: &str = "application/vnd.example.stele.index.v1+zstd";
const COMPRESSION_LEVEL: i32 = 9;

/// The same profile the directory tests use, and for the same reason: if the
/// transport had absorbed an assumption from Dolos, a vendor publishing
/// chapters of notes would not be able to use it.
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

// ---------------------------------------------------------------------------
// The manifest, frozen without a network
// ---------------------------------------------------------------------------

fn digest_of(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

fn note_record(id: u64) -> CanonicalCbor {
    encode(|e| {
        e.array(2)?.u64(id)?.str("a note")?;
        Ok(())
    })
    .unwrap()
}

fn notes_scope(chapter: u64) -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.array(2)?.u64(chapter)?.str("notes")?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": chapter}))
}

fn index_scope(chapter: u64) -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.array(2)?.u64(chapter)?.str("index")?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": chapter, "sortedBy": "title"}))
}

/// An inscription and the layers a publisher would have written for it, both
/// entirely synthetic.
///
/// Nothing here is compressed. That is deliberate: a manifest carries the
/// *compressed* digest and size of every layer, and zstd's output moves between
/// library versions — which is the whole reason identity is anchored on
/// uncompressed bytes. A golden over real blobs would pin the compressor, fail
/// on an unrelated upgrade, and teach whoever hit it that the golden is noise.
fn fixture() -> (Inscription, Vec<WrittenLayer>) {
    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east"}),
        json!({"noteWidth": 40}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.history = vec![
        HistoryEntry {
            sequence: 1,
            inscription_digest: digest_of(0x11),
        },
        HistoryEntry {
            sequence: 2,
            inscription_digest: digest_of(0x22),
        },
    ];

    inscription.layers = vec![
        LayerDescriptor {
            kind: "notes".to_owned(),
            media_type: NOTES_MEDIA_TYPE.to_owned(),
            diff_id: digest_of(0xaa),
            records: 4,
            uncompressed_size: 155,
            scope: notes_scope(3).1,
        },
        LayerDescriptor {
            kind: "index".to_owned(),
            media_type: INDEX_MEDIA_TYPE.to_owned(),
            diff_id: digest_of(0xbb),
            records: 4,
            uncompressed_size: 77,
            scope: index_scope(3).1,
        },
    ];

    let layers = vec![
        written(&inscription.layers[0], digest_of(0xa1), 91, 155),
        written(&inscription.layers[1], digest_of(0xb1), 60, 77),
    ];

    (inscription, layers)
}

fn written(
    descriptor: &LayerDescriptor,
    blob_digest: Digest,
    compressed_size: u64,
    uncompressed_size: u64,
) -> WrittenLayer {
    WrittenLayer {
        descriptor: descriptor.clone(),
        digests: LayerDigests {
            diff_id: descriptor.diff_id,
            blob_digest,
            uncompressed_size,
            compressed_size,
        },
    }
}

/// Done criterion 3: the manifest's shape is a re-pin, never a drift.
///
/// Every byte below is determined by the specification — the artifact type, the
/// config media type, the profile's own layer media types, the annotation keys
/// and the canonical JSON encoding. If any of them moves, this string moves
/// with it, in the same commit.
#[test]
fn the_manifest_shape_is_frozen() {
    let (inscription, layers) = fixture();

    let (manifest, config) = build_manifest(&inscription, &layers).unwrap();

    // The config blob is the canonical inscription, and the descriptor names it
    // by its own digest — the one place identity and transport meet.
    assert_eq!(config, inscription.canonicalize().unwrap());
    assert_eq!(
        manifest.config.digest,
        inscription.digest().unwrap().to_string()
    );
    assert_eq!(manifest.config.size as usize, config.len());

    let body = manifest_bytes(&manifest).unwrap();

    assert_eq!(
        String::from_utf8(body).unwrap(),
        concat!(
            r#"{"artifactType":"application/vnd.stelae.stele.v1","#,
            r#""config":{"digest":"sha256:3eff2efc90e091c19097c2b7c33e6d5270bdcbc11306c7a0ce9265d0d5601cc4","#,
            r#""mediaType":"application/vnd.stelae.inscription.v1+json","size":873},"#,
            r#""layers":[{"annotations":{"#,
            r#""dev.stelae.layer.diffId":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","#,
            r#""dev.stelae.layer.kind":"notes","#,
            r#""dev.stelae.layer.scope":"{\"chapter\":3}"},"#,
            r#""digest":"sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","#,
            r#""mediaType":"application/vnd.example.stele.notes.v1+zstd","size":91},"#,
            r#"{"annotations":{"#,
            r#""dev.stelae.layer.diffId":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","#,
            r#""dev.stelae.layer.kind":"index","#,
            r#""dev.stelae.layer.scope":"{\"chapter\":3,\"sortedBy\":\"title\"}"},"#,
            r#""digest":"sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1","#,
            r#""mediaType":"application/vnd.example.stele.index.v1+zstd","size":60}],"#,
            r#""mediaType":"application/vnd.oci.image.manifest.v1+json","schemaVersion":2}"#,
        ),
        "the manifest shape drifted"
    );
}

/// The map a pull reads off the manifest is the map a directory rebuilds by
/// decompressing everything, and this is where the two are held to be the same
/// thing.
#[test]
fn a_manifest_yields_the_identity_to_blob_map() {
    let (inscription, layers) = fixture();
    let (manifest, _) = build_manifest(&inscription, &layers).unwrap();

    let blobs = read_manifest(&manifest, &inscription).unwrap();

    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs.blob_for(&digest_of(0xaa)), Some(digest_of(0xa1)));
    assert_eq!(blobs.blob_for(&digest_of(0xbb)), Some(digest_of(0xb1)));
    assert_eq!(blobs.blob_for(&digest_of(0xcc)), None);
}

/// The annotations ADR-004 asks for, checked as content rather than as bytes,
/// so a reader of this file can see what a generic OCI tool would find.
#[test]
fn every_layer_is_annotated_with_its_kind_diff_id_and_scope() {
    let (inscription, layers) = fixture();
    let (manifest, _) = build_manifest(&inscription, &layers).unwrap();

    for (oci, described) in manifest.layers.iter().zip(&inscription.layers) {
        let annotations = oci.annotations.as_ref().unwrap();

        assert_eq!(annotations[KIND_ANNOTATION], described.kind);
        assert_eq!(
            annotations[DIFF_ID_ANNOTATION],
            described.diff_id.to_string()
        );
        assert_eq!(
            annotations[SCOPE_ANNOTATION],
            serde_json::to_string(&described.scope).unwrap()
        );
    }
}

/// A disagreement between the two documents is a refusal, never a preference.
#[test]
fn a_manifest_that_disagrees_with_the_inscription_is_refused() {
    let (inscription, layers) = fixture();

    // A layer the inscription describes and nobody wrote.
    let err = build_manifest(&inscription, &layers[..1]).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("never written")),
        "{err:?}"
    );

    // A layer that was written and the inscription does not describe: a blob
    // nothing attests.
    let mut orphaned = layers.clone();
    orphaned.push(written(&layers[0].descriptor, digest_of(0xc1), 10, 20));
    let err = build_manifest(&inscription, &orphaned).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("does not describe")),
        "{err:?}"
    );

    let (manifest, _) = build_manifest(&inscription, &layers).unwrap();

    // Fewer layers than the document describes.
    let mut short = manifest.clone();
    short.layers.pop();
    let err = read_manifest(&short, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("layer(s)")),
        "{err:?}"
    );

    // The right blobs in the wrong order. The `diffId` annotations still map
    // every layer to a real blob, so only the positional check catches this.
    let mut reordered = manifest.clone();
    reordered.layers.reverse();
    let err = read_manifest(&reordered, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("annotated")),
        "{err:?}"
    );

    // A layer with no annotation says nothing about which layer it holds.
    let mut unannotated = manifest.clone();
    unannotated.layers[0].annotations = None;
    let err = read_manifest(&unannotated, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains(DIFF_ID_ANNOTATION)),
        "{err:?}"
    );

    // A media type the inscription does not claim.
    let mut mistyped = manifest.clone();
    mistyped.layers[0].media_type = INDEX_MEDIA_TYPE.to_owned();
    let err = read_manifest(&mistyped, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("in the manifest")),
        "{err:?}"
    );
}

/// The envelope is checked before anything inside it is trusted, and a missing
/// `artifactType` fails closed.
#[test]
fn a_manifest_that_is_not_a_steles_is_refused() {
    let (inscription, layers) = fixture();
    let (manifest, _) = build_manifest(&inscription, &layers).unwrap();

    let mut stripped = manifest.clone();
    stripped.artifact_type = None;
    let err = read_manifest(&stripped, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("no artifactType")),
        "{err:?}"
    );

    let mut foreign = manifest.clone();
    foreign.artifact_type = Some("application/vnd.acme.thing.v1".to_owned());
    let err = read_manifest(&foreign, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("artifactType is")),
        "{err:?}"
    );

    let mut wrong_config = manifest;
    wrong_config.config.media_type = "application/vnd.oci.image.config.v1+json".to_owned();
    let err = read_manifest(&wrong_config, &inscription).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("config blob is")),
        "{err:?}"
    );
}

/// What the 4 MiB ceiling refuses, stated as a test rather than as a comment.
///
/// A descriptor and its three annotations run to roughly 350 bytes, so a
/// manifest reaches the ceiling somewhere around twelve thousand layers —
/// nearly seven times a mainnet stele's ~1,816 (ADR-004's ~600 epochs × three
/// per-epoch kinds, plus sixteen state shards). Nothing is expected to meet it.
/// The point is that when something does, the refusal names the document and
/// the layer count instead of arriving as a registry's `413`.
#[test]
fn a_manifest_past_the_size_ceiling_is_refused() {
    /// Layers of a mainnet stele, by ADR-004's own sizing.
    const MAINNET: usize = 600 * 3 + 16;
    /// Comfortably past the ceiling: nothing here depends on where exactly it
    /// falls, only that it is far above anything a profile would publish.
    const TOO_MANY: usize = 16_000;

    let (template, written_template) = {
        let (inscription, layers) = fixture();
        (inscription.layers[0].clone(), layers[0].clone())
    };

    let build = |count: usize| {
        let (mut inscription, _) = fixture();
        inscription.layers.clear();

        let mut layers = Vec::with_capacity(count);

        for index in 0..count as u32 {
            let mut described = template.clone();

            // Distinct identities, so nothing collapses into one descriptor.
            let mut bytes = [0u8; 32];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            described.diff_id = Digest::from_bytes(bytes);
            described.scope = json!({"chapter": index});

            let mut layer = written_template.clone();
            layer.descriptor = described.clone();
            layer.digests.diff_id = described.diff_id;

            inscription.layers.push(described);
            layers.push(layer);
        }

        build_manifest(&inscription, &layers).unwrap().0
    };

    let err = manifest_bytes(&build(TOO_MANY)).unwrap_err();

    assert!(
        matches!(err, Error::ManifestTooLarge { layers, .. } if layers == TOO_MANY),
        "{err:?}"
    );

    // A mainnet-sized stele passes it with room to spare, which is the claim
    // ADR-004 sized the format against.
    let body = manifest_bytes(&build(MAINNET)).unwrap();

    println!(
        "manifest: {} bytes for {MAINNET} layers ({} bytes per layer), \
         ceiling {} bytes",
        body.len(),
        body.len() / MAINNET,
        stelae::MANIFEST_SIZE_LIMIT,
    );

    assert!(
        body.len() < stelae::MANIFEST_SIZE_LIMIT / 2,
        "a mainnet-sized manifest is {} bytes",
        body.len(),
    );
}

// ---------------------------------------------------------------------------
// A registry the test spawns
// ---------------------------------------------------------------------------

/// A container running an OCI Distribution server, removed when this is
/// dropped.
///
/// `docker` rather than a library: the point of these tests is that the client
/// talks to a *real* registry, and a fake one written here would only ever
/// agree with this implementation's reading of the specification.
struct Fixture {
    container: String,
    port: u16,
}

impl Fixture {
    fn spawn() -> Self {
        let image =
            std::env::var("STELAE_TEST_REGISTRY_IMAGE").unwrap_or_else(|_| "registry:2".to_owned());

        let run = std::process::Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--publish",
                "127.0.0.1::5000",
                &image,
            ])
            .output()
            .expect("docker is required to run the registry tests");

        assert!(
            run.status.success(),
            "docker run {image}: {}",
            String::from_utf8_lossy(&run.stderr)
        );

        let container = String::from_utf8(run.stdout).unwrap().trim().to_owned();

        let ports = std::process::Command::new("docker")
            .args(["port", &container, "5000/tcp"])
            .output()
            .expect("docker port");

        let mapped = String::from_utf8(ports.stdout).unwrap();
        let port = mapped
            .lines()
            .find_map(|line| line.rsplit(':').next())
            .and_then(|port| port.trim().parse::<u16>().ok())
            .unwrap_or_else(|| panic!("no published port in {mapped:?}"));

        let fixture = Self { container, port };
        fixture.wait_until_ready();

        eprintln!("registry: {image} on 127.0.0.1:{port}");

        fixture
    }

    /// Wait until the server answers `GET /v2/`.
    ///
    /// A connect is *not* the readiness signal, however much it looks like one:
    /// Docker's port forwarder accepts on the published port from the moment
    /// the container exists and only then tries to reach the process inside, so
    /// a connect succeeds and the request that follows it dies as an incomplete
    /// message. That failure looks exactly like a registry rejecting the
    /// request, which is the wrong thing to conclude about a registry.
    ///
    /// So the probe is a real request, spoken by hand over the socket rather
    /// than with an HTTP client this crate does not otherwise need.
    fn wait_until_ready(&self) {
        use std::io::{Read, Write};

        let address = format!("127.0.0.1:{}", self.port);

        for _ in 0..300 {
            let answered = std::net::TcpStream::connect(&address)
                .ok()
                .and_then(|mut socket| {
                    socket
                        .write_all(
                            format!("GET /v2/ HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes(),
                        )
                        .ok()?;

                    let mut answer = [0u8; 16];
                    let read = socket.read(&mut answer).ok()?;

                    answer[..read].starts_with(b"HTTP/1.").then_some(())
                });

            if answered.is_some() {
                return;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        panic!("the registry never answered GET /v2/ on {address}");
    }

    fn registry(&self, repository: &str) -> Registry {
        Registry::open(
            &format!("oci://127.0.0.1:{}/{repository}", self.port)
                .parse()
                .expect("the fixture named a usable repository"),
            Options {
                insecure: true,
                scratch_dir: None,
            },
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

/// Write a small stele of the toy profile through any transport.
///
/// One layer handed over whole and one streamed into a sink, so both write
/// paths are exercised against whatever this is pointed at. `chapter` decides
/// what goes in the index layer, which is how the delta test makes two steles
/// that share a blob and differ in one.
fn write_stele<W: SteleWriter>(stele: &W, chapter: u64) -> Inscription {
    let (notes_header, notes_scope) = notes_scope(3);
    let (index_header, index_scope) = index_scope(chapter);

    let notes: Vec<CanonicalCbor> = (1..=3).map(note_record).collect();

    let written_notes = stele
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    let mut sink = stele
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("index", index_header, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    for id in 1..=chapter {
        sink.write_record(&note_record(id)).unwrap();
    }

    let written_index = sink.finish().unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        chapter,
        json!({"chapter": chapter, "shelf": "east"}),
        json!({"noteWidth": 40}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.layers = vec![written_notes.descriptor, written_index.descriptor];

    stele.seal(&ToyProfile, &inscription).unwrap();

    inscription
}

/// Every record of every layer, read back through the streaming reader.
fn records_of<R: SteleReader>(stele: &R, inscription: &Inscription) -> Vec<Vec<Vec<u8>>> {
    let index = stele.blob_index().unwrap();

    inscription
        .layers
        .iter()
        .map(|descriptor| {
            let mut reader = stele
                .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
                .unwrap();

            let mut records = vec![reader.header().encode().unwrap().as_bytes().to_vec()];

            while let Some(record) = reader.next_record() {
                records.push(record.unwrap().to_vec());
            }

            // Only now is the layer proven: the identity digest covers every
            // byte, so nothing above was trustworthy until this returned.
            let digests = reader.finish().unwrap();
            assert_eq!(digests.diff_id, descriptor.diff_id);

            records
        })
        .collect()
}

/// Done criterion 1: a stele pushed to a registry and pulled back is the same
/// stele.
///
/// "The same" is checked against a directory rather than against itself: the
/// same records go into a `SteleDir` and into the registry, and the two are
/// compared on the inscription digest, on the identity of every layer, on the
/// compressed blob digest — the byte string the registry stores against the
/// byte string the directory names its file by — and on the records that come
/// back out.
#[test]
#[ignore = "spawns a registry"]
fn a_stele_survives_the_round_trip() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/roundtrip");

    let temp = tempfile::tempdir().unwrap();
    let directory = SteleDir::create(temp.path()).unwrap();

    let published = write_stele(&registry, 3);
    let on_disk = write_stele(&directory, 3);

    assert_eq!(published, on_disk, "one stele, two transports");
    println!(
        "identity: {} ({} layers)",
        published.digest().unwrap(),
        published.layers.len()
    );

    // Both tags resolve, and to the same stele.
    let latest = registry.pull_latest(&ToyProfile).unwrap();
    let by_sequence = registry
        .pull_sequence(&ToyProfile, published.sequence)
        .unwrap();

    assert_eq!(latest.read_inscription().unwrap(), published);
    assert_eq!(by_sequence.read_inscription().unwrap(), published);
    assert_eq!(
        latest.read_inscription().unwrap().digest().unwrap(),
        on_disk.digest().unwrap(),
    );

    // The identity→blob map came off the manifest; the directory's came from
    // decompressing everything. They agree blob for blob, which is what makes
    // the manifest a shortcut rather than a second source of truth.
    let pulled_blobs = latest.blob_index().unwrap();
    let disk_blobs = directory.blob_index().unwrap();

    assert_eq!(pulled_blobs.len(), disk_blobs.len());

    for descriptor in &published.layers {
        assert_eq!(
            pulled_blobs.blob_for(&descriptor.diff_id),
            disk_blobs.blob_for(&descriptor.diff_id),
            "layer {:?} is a different blob in the registry",
            descriptor.kind,
        );
    }

    // And the records themselves.
    assert_eq!(
        records_of(&latest, &published),
        records_of(&directory, &on_disk),
        "layers differ record for record",
    );

    println!(
        "pulled {} layers, {} compressed bytes",
        published.layers.len(),
        latest.total_compressed_size(),
    );

    // The whole-stele figure is the per-layer one summed, which is what makes
    // the per-layer answer usable for a restore's remaining-download estimate:
    // a subset of the layers weighs a subset of the bytes, on the same scale.
    let index = latest.blob_index().unwrap();

    let summed: u64 = published
        .layers
        .iter()
        .map(|layer| {
            latest
                .compressed_size(&index, layer)
                .unwrap()
                .expect("a pulled stele states every layer's compressed size")
        })
        .sum();

    assert_eq!(summed, latest.total_compressed_size());

    // A stele of another profile is refused before a layer is fetched.
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

    let err = registry.pull(&Other, "latest").unwrap_err();
    assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");
}

/// Done criterion 2: the second push moves only what the registry lacks, and
/// the transport says so in a number.
///
/// The two steles share their `notes` layer byte for byte — same records, same
/// scope, so the same `diffId` and, at a pinned compression level, the same
/// blob — and differ in their `index` layer. What the registry has to receive
/// is therefore exactly one blob, and what it must be spared is exactly one.
#[test]
#[ignore = "spawns a registry"]
fn a_second_push_uploads_only_what_is_missing() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/delta");

    let first = write_stele(&registry, 3);
    let first_transfer = registry.take_transfer();

    println!("first push:  {first_transfer:?}");

    assert_eq!(first_transfer.layers_uploaded, 2, "an empty repository");
    assert_eq!(first_transfer.layers_skipped, 0);
    assert!(first_transfer.bytes_uploaded > 0);
    assert_eq!(first_transfer.bytes_skipped, 0);

    let second = write_stele(&registry, 4);
    let second_transfer = registry.take_transfer();

    println!("second push: {second_transfer:?}");

    assert_eq!(
        second_transfer.layers_skipped, 1,
        "the shared notes layer should not have moved",
    );
    assert_eq!(
        second_transfer.layers_uploaded, 1,
        "only the new index layer should have moved",
    );
    assert!(second_transfer.bytes_skipped > 0);

    // The skip is not a lie: both steles pull back whole, and the layer that
    // was skipped is the one the first push put there.
    let notes = &first.layers[0];
    assert_eq!(notes.diff_id, second.layers[0].diff_id);

    for sequence in [first.sequence, second.sequence] {
        let stele = registry.pull_sequence(&ToyProfile, sequence).unwrap();
        let inscription = stele.read_inscription().unwrap();

        assert_eq!(inscription.sequence, sequence);
        records_of(&stele, &inscription);
    }

    // And `latest` followed the second one.
    let latest = registry.pull_latest(&ToyProfile).unwrap();
    assert_eq!(latest.read_inscription().unwrap(), second);
}

// ---------------------------------------------------------------------------
// Peak allocation, in both directions
// ---------------------------------------------------------------------------

/// Records of roughly a kilobyte, the order of a Dolos `indexes` or `state`
/// record.
const BULK_RECORD_BODY: usize = 1000;

/// ~50 MB of layer. Ten times the budget below, so a transport that buffered
/// even a fifth of a layer would show up.
const BULK_RECORDS: u64 = 48 * 1024;

/// What either direction may hold at any one moment.
///
/// Above the 1 MiB upload chunk because the HTTP client copies a body on its
/// way to the socket and zstd buffers on either side of the codec. What matters
/// is not the number but that it does not move when the layer does — the layer
/// is fifty times it.
const TRANSPORT_BUDGET: usize = 5 * 1024 * 1024;

/// A record whose body zstd cannot shrink.
///
/// This matters more here than it does in `tests/memory.rs`. That file measures
/// the framing and the codec, where a compressible body only makes the layer
/// cheaper to hold. Here the layer has to cross a socket, and a body that
/// compresses to nothing would leave the *transfer* — the part this test exists
/// to bound — moving a few kilobytes while the assertions talked about fifty
/// megabytes. So the body is a splitmix64 stream: cheap to generate,
/// deterministic, and incompressible.
fn bulk_record(i: u64) -> CanonicalCbor {
    /// The splitmix64 finalizer.
    fn mix(x: u64) -> u64 {
        let z = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    // Chained on the *state* rather than on a shared additive counter. The
    // obvious version — seed by `i`, then step by a constant — makes record
    // `i + 1` a one-word shift of record `i`, which zstd finds across its
    // window and compresses seventy-fold. Every record here is its own orbit.
    let mut state = mix(i);
    let mut body = Vec::with_capacity(BULK_RECORD_BODY + 8);

    while body.len() < BULK_RECORD_BODY {
        state = mix(state ^ 0x9e37_79b9_7f4a_7c15);
        body.extend_from_slice(&state.to_le_bytes());
    }

    body.truncate(BULK_RECORD_BODY);

    encode(|e| {
        e.array(2)?.u64(i)?.bytes(&body)?;
        Ok(())
    })
    .unwrap()
}

/// Bytes allocated and not yet returned, sampled from another thread for as
/// long as it is alive.
///
/// A `Region` can only be read from the thread that owns it, and the peak that
/// matters here is *inside* a single call — the upload in `finish`, the
/// download in `stream_layer` — where there is nowhere to put a sample. So the
/// process-wide counters are polled instead, against a baseline taken when the
/// sampler starts.
struct Peak {
    stop: std::sync::Arc<AtomicBool>,
    peak: std::sync::Arc<AtomicUsize>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Peak {
    fn start() -> Self {
        let base = GLOBAL.stats();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let peak = std::sync::Arc::new(AtomicUsize::new(0));

        let handle = {
            let stop = std::sync::Arc::clone(&stop);
            let peak = std::sync::Arc::clone(&peak);

            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let now = GLOBAL.stats();
                    let allocated = now.bytes_allocated.saturating_sub(base.bytes_allocated);
                    let freed = now.bytes_deallocated.saturating_sub(base.bytes_deallocated);

                    peak.fetch_max(allocated.saturating_sub(freed), Ordering::Relaxed);

                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
            })
        };

        Self {
            stop,
            peak,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> usize {
        self.stop.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        self.peak.load(Ordering::Relaxed)
    }
}

/// Done criterion 4: neither direction scales with the layer.
///
/// The layer is fifty times the budget and never fits in it, so a transport
/// that held one — buffering a blob before uploading it, or decompressing a
/// pulled one into memory — cannot pass. This is the `tests/memory.rs`
/// discipline extended to the transport; it lives here rather than there
/// because it needs a registry, and a bound measured against a mock would only
/// be a bound on the mock.
#[test]
#[ignore = "spawns a registry"]
fn neither_direction_holds_a_layer() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/memory");

    let (header_scope, scope) = notes_scope(1);
    let spec = LayerSpec::new("notes", header_scope, scope);

    // --- up ---------------------------------------------------------------
    let sampler = Peak::start();

    let mut sink = registry
        .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    for i in 0..BULK_RECORDS {
        sink.write_record(&bulk_record(i)).unwrap();
    }

    let layer = sink.finish().unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        1,
        json!({"chapter": 1}),
        json!({}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.layers = vec![layer.descriptor.clone()];

    registry.seal(&ToyProfile, &inscription).unwrap();

    let pushed = sampler.finish();

    let size = layer.descriptor.uncompressed_size;
    let compressed = layer.digests.compressed_size;

    println!("push: {size} uncompressed / {compressed} compressed bytes, peak {pushed} bytes held",);

    assert!(
        size > 8 * TRANSPORT_BUDGET as u64,
        "the layer has to dwarf the budget for this to prove anything: \
         {size} against {TRANSPORT_BUDGET}",
    );

    // And so does what actually crossed the socket. A compressible fixture
    // would leave every assertion here true and none of them about the
    // transfer.
    assert!(
        compressed > 8 * TRANSPORT_BUDGET as u64,
        "the blob has to dwarf the budget too, or the upload proved nothing: \
         {compressed} against {TRANSPORT_BUDGET}",
    );

    assert!(
        pushed < TRANSPORT_BUDGET,
        "pushing a {size}-byte layer held {pushed} bytes at peak; \
         the budget is {TRANSPORT_BUDGET}",
    );

    // --- down -------------------------------------------------------------
    let sampler = Peak::start();

    let stele = registry.pull_latest(&ToyProfile).unwrap();
    let read = stele.read_inscription().unwrap();
    let blobs = stele.blob_index().unwrap();

    let mut reader = stele
        .stream_layer(&blobs, &ToyProfile, &read.layers[0], Limits::default())
        .unwrap();

    let mut count = 1u64; // the header record, already consumed
    while let Some(record) = reader.next_record() {
        assert!(!record.unwrap().is_empty());
        count += 1;
    }

    let digests = reader.finish().unwrap();
    let pulled = sampler.finish();

    println!("pull: {count} records, peak {pulled} bytes held");

    assert_eq!(count, layer.descriptor.records);
    assert_eq!(digests.diff_id, layer.descriptor.diff_id);

    assert!(
        pulled < TRANSPORT_BUDGET,
        "pulling a {size}-byte layer held {pulled} bytes at peak; \
         the budget is {TRANSPORT_BUDGET}",
    );
}

/// A blob that is not a layer never reaches the reader as one.
///
/// The registry is content-addressed, so tampering with a stored blob is not
/// possible without changing its name — which is exactly what makes the
/// interesting failure a *manifest* that points at the wrong blob.
///
/// Both refusals here land in `LayerReader::new`, before a single record past
/// the header is read: one because the header names another kind, one because
/// the index has no blob under that identity at all. The check at the *other*
/// end of the layer — the identity digest over every byte — is out of reach
/// from here for the reason the first case shows, and is
/// [`a_same_kind_blob_is_refused_when_the_layer_ends`].
#[test]
#[ignore = "spawns a registry"]
fn a_layer_that_is_not_the_one_described_is_refused() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/tamper");

    let inscription = write_stele(&registry, 3);

    let stele = registry.pull_latest(&ToyProfile).unwrap();
    let blobs = stele.blob_index().unwrap();

    // The notes layer's blob, under the index layer's descriptor: a real blob,
    // correctly named, holding the wrong layer.
    let mut swapped = inscription.layers[1].clone();
    swapped.diff_id = inscription.layers[0].diff_id;

    let err = stele
        .stream_layer(&blobs, &ToyProfile, &swapped, Limits::default())
        .unwrap_err();

    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    // And a descriptor naming a layer the stele does not carry.
    let mut absent = inscription.layers[0].clone();
    absent.diff_id = digest_of(0xee);

    let err = stele
        .stream_layer(&blobs, &ToyProfile, &absent, Limits::default())
        .unwrap_err();

    assert!(matches!(err, Error::LayerNotFound { .. }), "{err:?}");
}

/// The identity check at the end of a layer, reached.
///
/// A `diffId` annotation lives in the manifest, outside the inscription, so
/// nothing about a stele's *identity* covers it — which makes a manifest that
/// points a descriptor at the wrong blob the tamper this format has to survive
/// on its own. Point it at a blob of another kind and the header record settles
/// it immediately, which is what
/// [`a_layer_that_is_not_the_one_described_is_refused`] shows. Point it at a
/// blob of its own kind and the header has nothing to say: only the hash of
/// every byte, once the layer ends, can tell the two apart.
#[test]
#[ignore = "spawns a registry"]
fn a_same_kind_blob_is_refused_when_the_layer_ends() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/tamper-same-kind");

    // Two layers of one kind under one scope, differing only in how many
    // records they hold, so their headers are byte-identical and their
    // identities are not.
    let write = |count: u64| {
        let (header, scope) = notes_scope(3);

        let mut sink = registry
            .layer_sink(
                &ToyProfile,
                &LayerSpec::new("notes", header, scope),
                COMPRESSION_LEVEL,
            )
            .unwrap();

        for id in 1..=count {
            sink.write_record(&note_record(id)).unwrap();
        }

        sink.finish().unwrap()
    };

    let short = write(3);
    let long = write(6);

    let mut inscription = Inscription::new(
        &ToyProfile,
        1,
        json!({"chapter": 1, "shelf": "east"}),
        json!({"noteWidth": 40}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.layers = vec![short.descriptor.clone(), long.descriptor.clone()];
    registry.seal(&ToyProfile, &inscription).unwrap();

    let stele = registry.pull_latest(&ToyProfile).unwrap();

    // The long layer's identity, pointed at the short layer's blob. The short
    // one is the target on purpose: reading it stays inside the size the long
    // descriptor claims, so the meter cannot refuse this before the digest
    // does, and it is the digest that is under test.
    let mut tampered = stele.blob_index().unwrap();
    tampered.insert(long.descriptor.diff_id, short.digests.blob_digest);

    let mut reader = stele
        .stream_layer(&tampered, &ToyProfile, &long.descriptor, Limits::default())
        .unwrap();

    // Every record reads cleanly. Nothing up to here is wrong; the layer is
    // simply not the one that was asked for.
    while let Some(record) = reader.next_record() {
        record.unwrap();
    }

    let err = reader.finish().unwrap_err();

    // Named exactly, because a `DigestMismatch` is also what a blob that
    // arrived corrupt would produce: this one has to be the identity check,
    // reporting the layer that was asked for against the one that was read.
    assert!(
        matches!(
            &err,
            Error::DigestMismatch { subject, expected, actual }
                if subject.contains("notes")
                    && *expected == long.descriptor.diff_id.to_string()
                    && *actual == short.descriptor.diff_id.to_string()
        ),
        "{err:?}"
    );
}

/// The scratch directory is honoured, and nothing survives a push.
///
/// A mainnet state shard is hundreds of megabytes compressed, and sixteen of
/// them staged in the platform temporary directory is how a publish fills a
/// volume nobody was watching. Staging files are unlinked at creation, so this
/// checks the directory is used and left empty rather than that files are
/// cleaned up afterwards.
#[test]
#[ignore = "spawns a registry"]
fn staging_stays_in_the_scratch_directory_and_leaves_nothing() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let scratch = tempfile::tempdir().unwrap();
    let registry = Registry::open(
        &format!("oci://127.0.0.1:{}/stelae/scratch", fixture.port)
            .parse()
            .expect("the fixture named a usable repository"),
        Options {
            insecure: true,
            scratch_dir: Some(scratch.path().to_owned()),
        },
    )
    .unwrap();

    let inscription = write_stele(&registry, 3);
    let stele = registry.pull_latest(&ToyProfile).unwrap();

    records_of(&stele, &inscription);

    let left: Vec<_> = std::fs::read_dir(scratch.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    assert!(left.is_empty(), "{left:?}");

    // A sink that is abandoned mid-layer takes its staging with it.
    let (header_scope, scope) = notes_scope(9);
    let mut sink = registry
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("notes", header_scope, scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    sink.write_record(&note_record(1)).unwrap();
    drop(sink);

    let left: Vec<_> = std::fs::read_dir(scratch.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    assert!(left.is_empty(), "{left:?}");
}

/// `latest` tells "this repository holds nothing" apart from "this repository
/// could not be read", which is the distinction a publisher's history chain
/// rests on.
///
/// Both halves against a real server, because the shape a registry uses to say
/// "no such manifest" is exactly the thing that cannot be established by
/// reading a client's source.
#[test]
#[ignore = "spawns a registry"]
fn latest_is_absent_until_something_is_published() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.registry("stelae/eventually");

    assert!(
        registry.latest(&ToyProfile).unwrap().is_none(),
        "an empty repository holds no stele, and that is not an error"
    );

    let published = write_stele(&registry, 3);

    let found = registry
        .latest(&ToyProfile)
        .unwrap()
        .expect("the repository holds a stele now");

    assert_eq!(found.read_inscription().unwrap(), published);

    // A repository that never existed is absent too, and by a different
    // registry error code than a missing tag in one that does.
    let empty = fixture.registry("stelae/never-written-to");
    assert!(empty.latest(&ToyProfile).unwrap().is_none());
}

/// A layer cannot be carried forward into a repository that does not hold its
/// blob.
///
/// The guard exists because the failure it prevents is invisible: a manifest
/// pointing at a reclaimed blob is a perfectly well-formed stele that nobody
/// can restore.
///
/// **Provoked with a second registry, not a second repository**, and the
/// difference is a real one this test found. `zot` answers `HEAD` *and* `GET`
/// for a blob under a repository it was never pushed to — its storage is
/// content-addressed across the whole registry — while `distribution` 2.8 and
/// 3.0 answer 404. So a sibling repository is not reliably a place a blob is
/// absent from, and on `zot` it would not even be the wrong answer: a registry
/// that will serve the blob is a registry where the manifest works. Only a
/// separate server is absent everywhere.
#[test]
#[ignore = "spawns a registry"]
fn a_layer_whose_blob_is_not_there_cannot_be_carried_forward() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let source = fixture.registry("stelae/source");
    let published = write_stele(&source, 3);
    let stele = source.latest(&ToyProfile).unwrap().unwrap();

    let somewhere_else = Fixture::spawn();
    let elsewhere = somewhere_else.registry("stelae/source");

    let err = elsewhere
        .adopt_layer(&stele, published.layers[0].clone())
        .unwrap_err();

    assert!(matches!(err, Error::BlobMissing { .. }), "{err:?}");
    assert_eq!(elsewhere.transfer(), Transfer::default(), "nothing counted");

    // The same call against the repository that does hold it is the whole
    // operation: no bytes move, and the layer is counted as carried forward.
    // Reset first, so what is read back is this call's cost and not the two
    // layers `write_stele` pushed through the same transport.
    source.take_transfer();

    source
        .adopt_layer(&stele, published.layers[0].clone())
        .unwrap();

    let transfer = source.take_transfer();

    assert_eq!(transfer.layers_reused, 1);
    assert_eq!(transfer.layers_uploaded, 0);
    assert!(transfer.bytes_reused > 0);
}

/// The annotation map is a `BTreeMap`, so the canonical JSON above is not
/// hostage to insertion order. Cheap to state, and the kind of thing that only
/// breaks in the diff of an unrelated change.
#[test]
fn annotation_keys_are_ordered() {
    let (inscription, layers) = fixture();
    let (manifest, _) = build_manifest(&inscription, &layers).unwrap();

    let annotations: &BTreeMap<String, String> = manifest.layers[0].annotations.as_ref().unwrap();
    let keys: Vec<&str> = annotations.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        vec![DIFF_ID_ANNOTATION, KIND_ANNOTATION, SCOPE_ANNOTATION]
    );
}
