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
//!
//! ## The fixture demands credentials
//!
//! Every registry this suite spawns is behind htpasswd, and every transport it
//! opens carries the pair. That is not incidental hardening: the registry this
//! transport is aimed at authenticates every request — access to a stele
//! repository is free and identity-less, and still credentialed — so a suite
//! that only ever spoke to an anonymous server would prove the round trip
//! against a server unlike the one it runs against.
//!
//! [`credentials_are_required`] is what keeps that honest. A server the fixture
//! does not know how to configure would run anonymous and every other test here
//! would pass regardless; that one fails instead, and says so.
//!
//! ## Running them over TLS
//!
//! The fixture speaks plaintext by default, which is enough for everything
//! about the *protocol* and evidence for nothing about the transport's crypto.
//! Set both of
//!
//! ```text
//! STELAE_TEST_REGISTRY_TLS_CERT=/abs/path/server.pem
//! STELAE_TEST_REGISTRY_TLS_KEY=/abs/path/server.key
//! ```
//!
//! and the same suite runs against the same server terminating TLS, with the
//! client verifying the certificate for real. The certificate has to cover
//! `127.0.0.1` — that is where the fixture publishes — and its issuer has to be
//! trusted by the process, which on Linux means `SSL_CERT_FILE` pointing at the
//! CA. Nothing here weakens verification to make a self-signed certificate
//! work: a suite that accepted any certificate would pass just as happily with
//! the handshake broken, which is the one thing this mode exists to detect.
//!
//! ## Pointing them at a deployment
//!
//! Set
//!
//! ```text
//! STELAE_TEST_REGISTRY_URL=oci.example.com
//! STELAE_TEST_REGISTRY_USER=publisher
//! STELAE_TEST_REGISTRY_PASSWORD=…
//! ```
//!
//! and the same suite runs against that registry instead of spawning one —
//! over TLS, no exceptions: a deployment is the one place plaintext has no
//! business. The docker knobs above are the container's and are ignored.
//!
//! A deployment persists between runs where a container never does, so every
//! fixture scopes its repositories under a fresh `staging/…` namespace — the
//! assertions written against an empty repository stay true, and a finished
//! run leaves its namespace to the deployment's own garbage collection.
//!
//! `STELAE_TEST_REGISTRY_PULL_USER` / `_PULL_PASSWORD` optionally name a
//! second pair, narrower by contract: expected to read a stele and be refused
//! a write, which is what [`the_read_only_pair_pulls_and_cannot_push`] proves.
//! The htpasswd fixture knows one pair, so that proof asks for a deployment.

#![cfg(feature = "oci")]

use std::{
    alloc::System,
    collections::BTreeMap,
    path::PathBuf,
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
        build_manifest, manifest_bytes, read_manifest, Auth, Options, Registry, DIFF_ID_ANNOTATION,
        KIND_ANNOTATION, SCOPE_ANNOTATION,
    },
    progress::{Event, Observer, Progress},
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
            r#""store.stelae.layer.diffId":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","#,
            r#""store.stelae.layer.kind":"notes","#,
            r#""store.stelae.layer.scope":"{\"chapter\":3}"},"#,
            r#""digest":"sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","#,
            r#""mediaType":"application/vnd.example.stele.notes.v1+zstd","size":91},"#,
            r#"{"annotations":{"#,
            r#""store.stelae.layer.diffId":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","#,
            r#""store.stelae.layer.kind":"index","#,
            r#""store.stelae.layer.scope":"{\"chapter\":3,\"sortedBy\":\"title\"}"},"#,
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

/// The certificate the fixture hands the server, when the environment supplies
/// one.
///
/// Read from the environment rather than generated here, because the half that
/// matters is the one this process cannot do to itself: the issuer has to be
/// trusted *before* the first client is built, and on Linux that is
/// `SSL_CERT_FILE`, which is read once. A test that minted a certificate would
/// have nowhere to put its CA.
struct Tls {
    certificate: String,
    key: String,
}

impl Tls {
    fn from_env() -> Option<Self> {
        match (
            std::env::var("STELAE_TEST_REGISTRY_TLS_CERT"),
            std::env::var("STELAE_TEST_REGISTRY_TLS_KEY"),
        ) {
            (Ok(certificate), Ok(key)) if !certificate.is_empty() && !key.is_empty() => {
                Some(Self { certificate, key })
            }
            (Ok(_), _) | (_, Ok(_)) => {
                panic!("STELAE_TEST_REGISTRY_TLS_CERT and _KEY are set together or not at all")
            }
            _ => None,
        }
    }

    /// The `docker run` arguments that make the server terminate TLS.
    ///
    /// Single-file bind mounts, so the two may live in different directories
    /// and neither directory is exposed whole.
    fn docker_args(&self) -> Vec<String> {
        vec![
            "--volume".to_owned(),
            format!("{}:/tls/certificate.pem:ro", self.certificate),
            "--volume".to_owned(),
            format!("{}:/tls/key.pem:ro", self.key),
            "--env".to_owned(),
            "REGISTRY_HTTP_TLS_CERTIFICATE=/tls/certificate.pem".to_owned(),
            "--env".to_owned(),
            "REGISTRY_HTTP_TLS_KEY=/tls/key.pem".to_owned(),
        ]
    }
}

/// Install `ring` as the process-default crypto provider.
///
/// `oci.rs` documents this as the caller's job — the transport is built on
/// rustls with no provider wired in — so the suite does it explicitly. Doing
/// it here rather than relying on whatever a dependency might have installed
/// is the point: if the precondition were ever dropped from the transport's
/// documentation, this line is what would still be true.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("nothing else installed a provider first");
    });
}

/// The credentials the fixture's registry demands.
///
/// A test credential, not a secret: it lives as long as one container.
/// [`HTPASSWD`] is the bcrypt encoding of this pair — `distribution` accepts no
/// other hash algorithm in an htpasswd file — so the two move together or not
/// at all.
const USER: &str = "stelae";
const PASSWORD: &str = "stelae-fixture";

const HTPASSWD: &str = "stelae:$2y$05$1Hb22zONvzLAj4WaYl34/uDWF5rDgQkS9MoewgRvsTlsNrusMYTW6\n";

/// zot's whole configuration, which is a file or nothing: it has no environment
/// equivalent, and the image's own default carries no auth.
const ZOT_CONFIG: &str = r#"{
  "distSpecVersion": "1.1.1",
  "storage": { "rootDirectory": "/var/lib/registry" },
  "http": {
    "address": "0.0.0.0",
    "port": "5000",
    "auth": { "htpasswd": { "path": "/auth/htpasswd" } }
  },
  "log": { "level": "warn" }
}
"#;

/// The registry under test: a container this suite spawned, or a deployment
/// the environment pointed it at.
///
/// `docker` rather than a library when it spawns one: the point of these tests
/// is that the client talks to a *real* registry, and a fake one written here
/// would only ever agree with this implementation's reading of the
/// specification. The remote arm is the same conviction carried further — the
/// registry the transport is actually aimed at, reached the way an operator
/// reaches it.
struct Fixture {
    server: Server,
    tls: bool,
}

/// Where [`Fixture`]'s registry lives.
enum Server {
    /// A container running an OCI Distribution server, removed when the
    /// fixture is dropped.
    Container {
        container: String,
        port: u16,
        /// The htpasswd file and the configuration naming it, held so they
        /// outlive the container that has them mounted.
        _auth: tempfile::TempDir,
    },
    /// A deployed registry, reached over TLS and never torn down.
    Remote {
        host: String,
        push_user: String,
        push_password: String,
        pull: Option<(String, String)>,
        /// This fixture's private corner of a registry that outlives it: a
        /// repository prefix no other run writes to.
        namespace: String,
    },
}

impl Fixture {
    fn spawn() -> Self {
        install_crypto_provider();

        if let Some(fixture) = Self::remote() {
            fixture.wait_until_ready();

            eprintln!(
                "registry: deployment on {}, TLS, basic auth as {:?}",
                fixture.address(),
                fixture.push_user(),
            );

            return fixture;
        }

        let image =
            std::env::var("STELAE_TEST_REGISTRY_IMAGE").unwrap_or_else(|_| "registry:2".to_owned());

        let tls = Tls::from_env();
        let auth = auth_dir();

        let mut args: Vec<String> = ["run", "--detach", "--rm", "--publish", "127.0.0.1::5000"]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();

        if let Some(tls) = &tls {
            args.extend(tls.docker_args());
        }

        args.extend(auth_args(auth.path()));
        args.push(image.clone());

        let run = std::process::Command::new("docker")
            .args(&args)
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

        let fixture = Self {
            server: Server::Container {
                container,
                port,
                _auth: auth,
            },
            tls: tls.is_some(),
        };

        fixture.wait_until_ready();

        eprintln!(
            "registry: {image} on {}, {}, basic auth as {USER:?}",
            fixture.address(),
            if fixture.tls { "TLS" } else { "plaintext" }
        );

        fixture
    }

    /// The deployment the environment names, if it names one.
    ///
    /// A fresh namespace per fixture, because a deployment persists where a
    /// container never does: two fixtures in one test are two namespaces —
    /// which is what lets
    /// [`a_layer_whose_blob_is_not_there_cannot_be_carried_forward`] keep
    /// meaning "a place the blob is absent from" — and two runs never share
    /// one.
    fn remote() -> Option<Self> {
        let host = std::env::var("STELAE_TEST_REGISTRY_URL").ok()?;

        let push_user = std::env::var("STELAE_TEST_REGISTRY_USER")
            .expect("STELAE_TEST_REGISTRY_URL is set, so _USER is too");
        let push_password = std::env::var("STELAE_TEST_REGISTRY_PASSWORD")
            .expect("STELAE_TEST_REGISTRY_URL is set, so _PASSWORD is too");

        let pull = match (
            std::env::var("STELAE_TEST_REGISTRY_PULL_USER"),
            std::env::var("STELAE_TEST_REGISTRY_PULL_PASSWORD"),
        ) {
            (Ok(user), Ok(password)) => Some((user, password)),
            (Ok(_), _) | (_, Ok(_)) => panic!(
                "STELAE_TEST_REGISTRY_PULL_USER and _PULL_PASSWORD are set \
                 together or not at all"
            ),
            _ => None,
        };

        static FIXTURES: AtomicUsize = AtomicUsize::new(0);

        let run = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let namespace = format!(
            "staging/{run:x}-{}",
            FIXTURES.fetch_add(1, Ordering::Relaxed)
        );

        Some(Self {
            server: Server::Remote {
                host,
                push_user,
                push_password,
                pull,
                namespace,
            },
            tls: true,
        })
    }

    fn address(&self) -> String {
        match &self.server {
            Server::Container { port, .. } => format!("127.0.0.1:{port}"),
            Server::Remote { host, .. } => host.clone(),
        }
    }

    /// Wait until the server answers a real request.
    ///
    /// A connect is *not* the readiness signal, however much it looks like one:
    /// Docker's port forwarder accepts on the published port from the moment
    /// the container exists and only then tries to reach the process inside, so
    /// a connect succeeds and the request that follows it dies as an incomplete
    /// message. That failure looks exactly like a registry rejecting the
    /// request, which is the wrong thing to conclude about a registry.
    ///
    /// So the probe is the client under test asking a repository nothing has
    /// ever been written to for its latest stele. `Ok(None)` is the answer only
    /// a registry that read the request can give — and under TLS it is
    /// reachable only through a handshake that verified, which makes the same
    /// call the readiness probe and the first assertion.
    fn wait_until_ready(&self) {
        for _ in 0..300 {
            if self
                .registry("stelae/readiness")
                .latest(&ToyProfile)
                .is_ok()
            {
                return;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let refusal = self
            .registry("stelae/readiness")
            .latest(&ToyProfile)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();

        panic!(
            "the registry never answered on {}: {refusal}",
            self.address()
        );
    }

    fn registry(&self, repository: &str) -> Registry {
        self.registry_staging_in(repository, None)
    }

    /// The same transport, with the publish path's concurrency named.
    ///
    /// Only the tests whose subject is the concurrency itself set it; every
    /// other test in this file runs at the default, which is the arrangement
    /// the deployment uses.
    fn registry_at(&self, repository: &str, concurrency: usize) -> Registry {
        self.options(repository, |options| options.concurrency = concurrency)
    }

    /// The same transport, re-proving every layer it carries forward.
    fn verifying(&self, repository: &str) -> Registry {
        self.options(repository, |options| options.verify_adopted = true)
    }

    /// Every transport in this file is built here, so that whether the fixture
    /// is speaking TLS — and which credentials it presents — is decided in
    /// exactly one place. A test that assembled its own [`Options`] to set a
    /// scratch directory would keep working against a plaintext fixture and
    /// quietly send `http://` at a TLS one.
    fn registry_staging_in(&self, repository: &str, scratch_dir: Option<PathBuf>) -> Registry {
        self.registry_as(repository, scratch_dir, self.credentials())
    }

    /// The pair the fixture's registry accepts writes under.
    fn credentials(&self) -> Auth {
        match &self.server {
            Server::Container { .. } => Auth::Basic {
                user: USER.to_owned(),
                password: PASSWORD.to_owned(),
            },
            Server::Remote {
                push_user,
                push_password,
                ..
            } => Auth::Basic {
                user: push_user.clone(),
                password: push_password.clone(),
            },
        }
    }

    /// The user [`credentials`](Self::credentials) authenticates as — what a
    /// test that presents the right name with the wrong password asks for.
    fn push_user(&self) -> &str {
        match &self.server {
            Server::Container { .. } => USER,
            Server::Remote { push_user, .. } => push_user,
        }
    }

    /// A second pair with narrower rights, where the registry has one.
    ///
    /// `None` against a container: htpasswd grants every authenticated pair
    /// the same thing, so there is no narrower pair to hand out.
    fn pull_credentials(&self) -> Option<Auth> {
        match &self.server {
            Server::Container { .. } => None,
            Server::Remote { pull, .. } => pull.as_ref().map(|(user, password)| Auth::Basic {
                user: user.clone(),
                password: password.clone(),
            }),
        }
    }

    fn registry_as(&self, repository: &str, scratch_dir: Option<PathBuf>, auth: Auth) -> Registry {
        self.open(repository, scratch_dir, auth, |_| {})
    }

    /// The fixture's own transport, with one thing about it changed.
    ///
    /// The knob the tests below reach for. Spelled as an edit rather than as
    /// another argument so that a test naming the concurrency does not also
    /// have to restate the credentials and the scheme this fixture decided.
    fn options(&self, repository: &str, set: impl FnOnce(&mut Options)) -> Registry {
        self.open(repository, None, self.credentials(), set)
    }

    fn open(
        &self,
        repository: &str,
        scratch_dir: Option<PathBuf>,
        auth: Auth,
        set: impl FnOnce(&mut Options),
    ) -> Registry {
        let repository = match &self.server {
            Server::Container { .. } => repository.to_owned(),
            Server::Remote { namespace, .. } => format!("{namespace}/{repository}"),
        };

        let mut options = Options {
            insecure: !self.tls,
            scratch_dir,
            auth,
            ..Default::default()
        };

        set(&mut options);

        Registry::open(
            &format!("oci://{}/{repository}", self.address())
                .parse()
                .expect("the fixture named a usable repository"),
            options,
        )
        .unwrap()
    }
}

/// An htpasswd file, plus the configuration a registry that wants one in a file
/// rather than in the environment reads.
///
/// Returned as a directory the caller holds: the container has both mounted,
/// and a `TempDir` dropped early would unlink them out from under it.
fn auth_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a temporary directory for the htpasswd file");

    std::fs::write(dir.path().join("htpasswd"), HTPASSWD).expect("writing the htpasswd file");
    std::fs::write(dir.path().join("zot.json"), ZOT_CONFIG).expect("writing the zot config");

    dir
}

/// The `docker run` arguments that make a registry demand
/// [`USER`]/[`PASSWORD`].
///
/// **Both configurations, unconditionally, and no per-image branch.** The two
/// server families this suite is pointed at read their auth from different
/// places and each ignores the other's: `distribution` reads `REGISTRY_AUTH_*`
/// out of the environment and never opens `/etc/zot/config.json`, while `zot`
/// reads that file and knows nothing about `REGISTRY_*`. Applying both is
/// therefore not a guess about which image is running — it is the union of two
/// settings that cannot collide.
///
/// A registry that reads neither would run anonymous, which every other test
/// here would be perfectly happy with. [`credentials_are_required`] is what
/// notices.
fn auth_args(dir: &std::path::Path) -> Vec<String> {
    let path = |name: &str| dir.join(name).display().to_string();

    vec![
        "--volume".to_owned(),
        format!("{}:/auth/htpasswd:ro", path("htpasswd")),
        "--volume".to_owned(),
        format!("{}:/etc/zot/config.json:ro", path("zot.json")),
        "--env".to_owned(),
        "REGISTRY_AUTH=htpasswd".to_owned(),
        "--env".to_owned(),
        "REGISTRY_AUTH_HTPASSWD_REALM=stelae".to_owned(),
        "--env".to_owned(),
        "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd".to_owned(),
    ]
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Server::Container { container, .. } = &self.server {
            let _ = std::process::Command::new("docker")
                .args(["rm", "--force", container])
                .output();
        }
    }
}

/// Write a small stele of the toy profile through any transport.
///
/// One layer handed over whole and one streamed into a sink, so both write
/// paths are exercised against whatever this is pointed at. `chapter` decides
/// what goes in the index layer, which is how the delta test makes two steles
/// that share a blob and differ in one.
fn write_stele<W: SteleWriter>(stele: &W, chapter: u64) -> Inscription {
    write_stele_fallibly(stele, chapter).unwrap()
}

/// The same publish, with every refusal handed back rather than unwrapped.
///
/// For the tests whose subject *is* a refusal. Split out rather than made the
/// only spelling because the twenty callers that expect a stele would each
/// grow an `.unwrap()` that says nothing, and the one that does not would stop
/// standing out.
fn write_stele_fallibly<W: SteleWriter>(stele: &W, chapter: u64) -> Result<Inscription, Error> {
    let (notes_header, notes_scope) = notes_scope(3);
    let (index_header, index_scope) = index_scope(chapter);

    let notes: Vec<CanonicalCbor> = (1..=3).map(note_record).collect();

    let written_notes = stele.write_layer(
        &ToyProfile,
        &LayerSpec::new("notes", notes_header, notes_scope),
        COMPRESSION_LEVEL,
        &notes,
    )?;

    let mut sink = stele.layer_sink(
        &ToyProfile,
        &LayerSpec::new("index", index_header, index_scope),
        COMPRESSION_LEVEL,
    )?;

    for id in 1..=chapter {
        sink.write_record(&note_record(id))?;
    }

    let written_index = sink.finish()?;

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

    stele.seal(&ToyProfile, &inscription)?;

    Ok(inscription)
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

/// [`BULK_RECORDS`], unless the run says otherwise.
///
/// `STELAE_TEST_BULK_RECORDS` exists for the registry deployment's gates,
/// which ask the same question at a mainnet shard's scale — a gibibyte and up
/// — where a default that size would make every local run unbearable. Nothing
/// else moves: the budget stays fixed precisely because the layer does not.
fn bulk_records() -> u64 {
    match std::env::var("STELAE_TEST_BULK_RECORDS") {
        Ok(count) => count
            .parse()
            .expect("STELAE_TEST_BULK_RECORDS is a record count"),
        Err(_) => BULK_RECORDS,
    }
}

/// What either direction may hold at any one moment, on the *streamed* path.
///
/// The push peak is one upload chunk and a little change — 4 MiB and some
/// tens of kilobytes, measured, and stable to a fraction of a percent across
/// runs, because the client splits the chunk it is handed rather than copying
/// it. The pull side sits far below that. 5 MiB is the larger of the two with
/// room over it.
///
/// What matters is not the number but that it does not move when the layer
/// does: the peak is bound by the chunk, and the layer here is ten times the
/// budget. `STELAE_TEST_BULK_RECORDS` below asks the same question at a
/// mainnet shard's scale and this constant does not follow it up.
///
/// It is not the bound on the *single-request* path, which holds a whole layer
/// by construction and is bounded by [`Options::upload_memory`] instead —
/// [`the_single_request_path_is_bounded_by_its_byte_budget`]. Every push test
/// here therefore has to say which path it is measuring, and this one says so
/// by naming a threshold below the layer it sends.
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

/// Done criterion 4: neither direction scales with the layer, on the streamed
/// path.
///
/// The layer is fifty times the budget and never fits in it, so a transport
/// that held one — buffering a blob before uploading it, or decompressing a
/// pulled one into memory — cannot pass. This is the `tests/memory.rs`
/// discipline extended to the transport; it lives here rather than there
/// because it needs a registry, and a bound measured against a mock would only
/// be a bound on the mock.
///
/// The threshold is named rather than left at its default, and that is the
/// whole reason this test still means what it meant: at the default a fifty
/// megabyte layer is *under* [`Options::monolithic_max`] and goes up in one
/// request, holding itself while it does. Sending it as a chain is now a
/// decision a caller makes, so a test about the chain has to make it.
#[test]
#[ignore = "spawns a registry"]
fn neither_direction_holds_a_layer() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();
    let registry = fixture.options("stelae/memory", |options| options.monolithic_max = 0);

    let (header_scope, scope) = notes_scope(1);
    let spec = LayerSpec::new("notes", header_scope, scope);

    // --- up ---------------------------------------------------------------
    let sampler = Peak::start();
    let started = std::time::Instant::now();

    let mut sink = registry
        .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    for i in 0..bulk_records() {
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

    println!(
        "push: {size} uncompressed / {compressed} compressed bytes, \
         peak {pushed} bytes held, {:.1?} elapsed",
        started.elapsed(),
    );

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
    let started = std::time::Instant::now();

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

    println!(
        "pull: {count} records, peak {pulled} bytes held, {:.1?} elapsed",
        started.elapsed(),
    );

    assert_eq!(count, layer.descriptor.records);
    assert_eq!(digests.diff_id, layer.descriptor.diff_id);

    assert!(
        pulled < TRANSPORT_BUDGET,
        "pulling a {size}-byte layer held {pulled} bytes at peak; \
         the budget is {TRANSPORT_BUDGET}",
    );
}

// ---------------------------------------------------------------------------
// One request, or a chain of them
// ---------------------------------------------------------------------------

/// Every [`Event::Bytes`] the transport emitted, in order.
///
/// The only in-process view of *how many requests* an upload took. The
/// streamed path emits one of these per chunk handed to the client and a chunk
/// is one `PATCH` — its own documentation says so — while the single-request
/// path has nothing finer to report and emits once. So a sequence of deltas is
/// the shape of the upload, and comparing two of them compares two paths.
///
/// Not a request count read off the wire, and it should not be mistaken for
/// one: a registry's own log would say more, and none of the three servers this
/// fixture runs against says it the same way.
#[derive(Default)]
struct Chunks(Mutex<Vec<u64>>);

impl Progress for Chunks {
    fn on(&self, event: Event<'_>) {
        if let Event::Bytes(n) = event {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(n);
        }
    }
}

impl Chunks {
    fn seen(&self) -> Vec<u64> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// A layer that fits goes up in one request; one that does not still streams;
/// and neither is a different layer for it.
///
/// The second half is the one decision 0026 rests on. A stele carries its
/// predecessor's layers forward by identity, so a layer republished through a
/// different transport path has to *be* the same layer — same `diffId` over the
/// same uncompressed bytes, same blob under the same digest — or every stele
/// after this change would carry nothing forward and every publish would be a
/// full upload.
///
/// Both layers go into one publish, past one threshold, so neither the
/// registry's blob-skip nor a second server can be what the difference is: the
/// small layer is under [`Options::monolithic_max`] and the bulk layer is over
/// it, and they are new blobs in an empty repository either way.
///
/// Serial on purpose. The deltas carry no layer identity, so attributing them
/// needs the uploads not to overlap — which is what a concurrency of one buys,
/// and the only thing this test wants from it.
#[test]
#[ignore = "spawns a registry"]
fn a_layer_under_the_threshold_goes_up_in_one_request() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    // Between the two layers below, and under the bulk layer by enough that it
    // still takes more than one chunk: the claim is a chain against a single
    // request, and a "chain" of one would prove nothing.
    const THRESHOLD: u64 = 1024 * 1024;

    // Comfortably over one upload chunk once compressed — the bodies are
    // incompressible, so this is close to what crosses the socket.
    const BULK: u64 = 6 * 1024;

    let chunks = std::sync::Arc::new(Chunks::default());

    let registry = fixture.options("stelae/one-request", |options| {
        options.monolithic_max = THRESHOLD;
        options.concurrency = 1;
    });

    registry.observe(Observer::new(chunks.clone()));

    let (notes_header, notes_scope) = notes_scope(1);
    let notes: Vec<CanonicalCbor> = (1..=3).map(note_record).collect();

    let small = registry
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    let (index_header, index_scope) = index_scope(1);

    let mut sink = registry
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("index", index_header, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    // Its own range of the record space. Two of the three registries this runs
    // against address blobs across the whole registry rather than per
    // repository, so a layer that happened to be another test's layer would be
    // skipped rather than uploaded and there would be nothing to count.
    for i in 0..BULK {
        sink.write_record(&bulk_record(2_000_000 + i)).unwrap();
    }

    let bulk = sink.finish().unwrap();

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

    inscription.layers = vec![small.descriptor.clone(), bulk.descriptor.clone()];

    registry.seal(&ToyProfile, &inscription).unwrap();

    let small_size = small.digests.compressed_size;
    let bulk_size = bulk.digests.compressed_size;

    assert!(
        small_size <= THRESHOLD,
        "the small layer has to be under the threshold: {small_size} against {THRESHOLD}",
    );
    assert!(
        bulk_size > 2 * THRESHOLD,
        "the bulk layer has to be over the threshold by more than one chunk: \
         {bulk_size} against {THRESHOLD}",
    );

    let seen = chunks.seen();

    assert_eq!(
        seen.first().copied(),
        Some(small_size),
        "the layer under the threshold reported {seen:?}; \
         one request is one delta, and it is the whole layer",
    );
    assert!(
        seen.len() > 2,
        "the layer over the threshold reported {seen:?}; \
         a chain is more than one chunk",
    );
    assert_eq!(
        seen.iter().sum::<u64>(),
        small_size + bulk_size,
        "the deltas do not add up to what was published: {seen:?}",
    );

    // --- and the same layers, whichever way they went ----------------------
    let stele = registry.pull_latest(&ToyProfile).unwrap();
    let read = stele.read_inscription().unwrap();

    assert_eq!(read, inscription, "the inscription came back different");

    let blobs = stele.blob_index().unwrap();

    for (described, written) in read.layers.iter().zip([&small, &bulk]) {
        let mut reader = stele
            .stream_layer(&blobs, &ToyProfile, described, Limits::default())
            .unwrap();

        while let Some(record) = reader.next_record() {
            record.unwrap();
        }

        // Recomputed over every byte the registry gave back, so this is the
        // identity the carry-forward will look for and not a number copied out
        // of the descriptor that claimed it.
        assert_eq!(
            reader.finish().unwrap().diff_id,
            written.descriptor.diff_id,
            "the {} layer is not the layer that was pushed",
            described.kind,
        );
    }
}

/// The single-request path holds its budget, not its concurrency.
///
/// The one thing this change can break in production. A monolithic push is
/// resident in full, so a bound counted in *layers* would let
/// [`Options::concurrency`] multiply a hundred megabytes by thirty-two and take
/// the publisher pod down mid-publish — which costs an epoch and looks like a
/// registry failure. The bound is counted in bytes instead, and this is the
/// assertion that it is.
///
/// Eight layers, all under the threshold so all resident, against a budget of
/// one and a half of them. A transport bounded by the layer count would hold
/// eight; one bounded by the budget holds two and makes the rest wait.
#[test]
#[ignore = "spawns a registry"]
fn the_single_request_path_is_bounded_by_its_byte_budget() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    /// Layers, all in flight at once as far as the permits are concerned.
    const LAYERS: u64 = 8;

    /// Records each, ~4 MB compressed — incompressible bodies, so the layer and
    /// the blob are the same order of magnitude.
    const BULK: u64 = 4 * 1024;

    /// What the transport may hold. Under two layers, so seven eighths of the
    /// publish cannot be resident whatever the concurrency says.
    const BUDGET: u64 = 6 * 1024 * 1024;

    let registry = fixture.options("stelae/budget", |options| {
        options.concurrency = LAYERS as usize;
        options.upload_memory = BUDGET;
    });

    let sampler = Peak::start();

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

    for layer in 0..LAYERS {
        let (header, scope) = notes_scope(layer);

        let mut sink = registry
            .layer_sink(
                &ToyProfile,
                &LayerSpec::new("notes", header, scope),
                COMPRESSION_LEVEL,
            )
            .unwrap();

        // Offset per layer so no two layers are the same blob, and offset again
        // past every other test in this file so no *other* test's blob is one
        // of these: two of the three registries this runs against address blobs
        // across the whole registry, and a skipped layer would leave the budget
        // untested.
        for i in 0..BULK {
            sink.write_record(&bulk_record(3_000_000 + layer * BULK + i))
                .unwrap();
        }

        inscription.layers.push(sink.finish().unwrap().descriptor);
    }

    registry.seal(&ToyProfile, &inscription).unwrap();

    let held = sampler.finish();

    let published: u64 = inscription
        .layers
        .iter()
        .map(|layer| layer.uncompressed_size)
        .sum();

    println!("budget: {published} bytes published, peak {held} bytes held");

    assert_eq!(
        registry.transfer().layers_uploaded,
        LAYERS,
        "every layer has to have been uploaded, or the budget was never asked for",
    );

    // The budget plus the streaming allowance: the staging, the compressor and
    // the client's own buffers are not what this bounds, and `TRANSPORT_BUDGET`
    // is already the measured size of them.
    let ceiling = BUDGET as usize + TRANSPORT_BUDGET;

    assert!(
        held < ceiling,
        "publishing {LAYERS} layers held {held} bytes at peak against a \
         {BUDGET}-byte budget; a bound counted in layers would hold about \
         {}",
        LAYERS as usize * (published as usize / LAYERS as usize),
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
    let registry = fixture.registry_staging_in("stelae/scratch", Some(scratch.path().to_owned()));

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

/// A staging directory that cannot be used says which one, in both directions.
///
/// Both directions, because one [`Options::scratch_dir`] serves both — a sink
/// on the way up and a pulled blob on the way down — and fixing the direction
/// somebody happened to test first is how this defect would come back.
///
/// `an_unusable_staging_directory_names_itself` in `src/oci.rs` makes the same
/// claim without a container, and says why the unusable directory is an
/// existing regular file; this one makes it through a real publish and a real
/// pull.
#[test]
#[ignore = "spawns a registry"]
fn a_staging_directory_that_cannot_be_used_names_itself() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let root = tempfile::tempdir().unwrap();
    let occupied = root.path().join("not-a-directory");
    std::fs::write(&occupied, b"").unwrap();

    let names_it = |err: &Error| {
        assert!(
            matches!(err, Error::Scratch { dir, .. } if dir == &occupied),
            "fell through to the catch-all: {err:?}",
        );

        let message = err.to_string();
        assert!(
            message.contains(&occupied.display().to_string()),
            "{message}",
        );
        assert!(message.contains("staging directory"), "{message}");
    };

    // Up: the sink stages the layer it is building.
    let (header_scope, scope) = notes_scope(3);
    let Err(err) = fixture
        .registry_staging_in("stelae/occupied", Some(occupied.clone()))
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("notes", header_scope, scope),
            COMPRESSION_LEVEL,
        )
    else {
        panic!("staged a layer in a regular file")
    };

    names_it(&err);

    // Down: the same directory, against a stele that is really there. Published
    // through a staging directory that works, so what fails below is the pull.
    let staged = tempfile::tempdir().unwrap();
    let published = write_stele(
        &fixture.registry_staging_in("stelae/occupied", Some(staged.path().to_owned())),
        3,
    );

    let reader = fixture.registry_staging_in("stelae/occupied", Some(occupied.clone()));
    let stele = reader.pull_latest(&ToyProfile).unwrap();
    let index = stele.blob_index().unwrap();

    let err = stele
        .stream_layer(&index, &ToyProfile, &published.layers[0], Limits::default())
        .expect_err("staged a pulled blob in a regular file");

    names_it(&err);
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

/// The registry the fixture spawns actually demands credentials, and a refusal
/// is never read as absence.
///
/// Two claims, and the second is the one with teeth. `Registry::latest` turns
/// "no such manifest" into `None`, and a publisher reads `None` as "nothing to
/// chain to" and starts a fresh history — so a 401 widening into absence would
/// silently restart the attestation chain against a registry that simply did
/// not recognise the caller. `is_absent` is written not to, and this is that
/// claim against a server that really answers 401 rather than against a
/// hand-built error value.
///
/// The first claim is what keeps the rest of this file honest: the fixture
/// configures htpasswd for the two server families it knows, and a registry
/// that read neither would run anonymous with every other test here passing
/// exactly as before. This one fails instead — which, for an operator pointing
/// `STELAE_TEST_REGISTRY_IMAGE` at a fourth implementation, is the fixture
/// saying it does not know how to make that one ask for credentials.
#[test]
#[ignore = "spawns a registry"]
fn credentials_are_required() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    // The pair the fixture configured reads the repository, which is the
    // baseline every other test in this file rests on.
    let allowed = fixture.registry("stelae/credentials");
    assert!(allowed.latest(&ToyProfile).unwrap().is_none());

    for (who, auth) in [
        ("anonymous", Auth::Anonymous),
        (
            "the wrong password",
            Auth::Basic {
                user: fixture.push_user().to_owned(),
                password: "not-the-password".to_owned(),
            },
        ),
    ] {
        let refused = fixture.registry_as("stelae/credentials", None, auth);

        let err = refused
            .latest(&ToyProfile)
            .expect_err("the registry answered an unauthenticated request");

        println!("{who}: {err}");

        // And a publish through this transport is refused rather than starting
        // a chain, which is the consequence that matters.
        assert!(refused.pull_latest(&ToyProfile).is_err(), "{who}");
    }
}

/// The published read-only pair reads a stele whole and cannot write one.
///
/// The deployment this suite points at hands consumers a pull-only pair —
/// free, identity-less, and still credentialed — and its access policy rests
/// on the registry enforcing that narrowness: a pull-only pair that could
/// push would make the published credential a write credential. So both
/// halves run against the real enforcement: a stele published under the full
/// pair pulls back whole under the narrow one, and the same narrow pair is
/// refused an upload.
///
/// Only a deployment names a second pair; htpasswd grants every pair the same
/// thing. Against a container this says so and proves nothing.
#[test]
#[ignore = "spawns a registry"]
fn the_read_only_pair_pulls_and_cannot_push() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let Some(pull) = fixture.pull_credentials() else {
        eprintln!(
            "no pull-only pair here: set STELAE_TEST_REGISTRY_PULL_USER and \
             _PULL_PASSWORD to run this against a deployment"
        );
        return;
    };

    let published = write_stele(&fixture.registry("stelae/read-only"), 3);

    let reading = fixture.registry_as("stelae/read-only", None, pull);
    let stele = reading.pull_latest(&ToyProfile).unwrap();
    let inscription = stele.read_inscription().unwrap();

    assert_eq!(inscription, published);
    records_of(&stele, &inscription);

    // Through the seal, because that is where an upload's refusal is now
    // reported: `finish` closes the layer and hands the round trips to the
    // pool, so the credential is not asked about them until they are joined.
    // The claim is unchanged — this pair cannot put a stele in this
    // repository — and the seal is the honest place to make it, since a
    // publish that seals is a publish that happened.
    let refused = write_stele_fallibly(&reading, 4)
        .expect_err("a pull-only pair was allowed to publish a stele");

    println!("write through the pull-only pair: {refused}");
}

/// Carrying a layer forward costs nothing and asks nothing.
///
/// The default, and the reason the publish path stopped getting slower with
/// every epoch behind it: `source` is a stele this transport pulled, its
/// manifest is live under a tag, and a registry may not reclaim a blob in that
/// position. So the layer is carried on the manifest's word — no round trip,
/// no bytes — and only the counters move.
#[test]
#[ignore = "spawns a registry"]
fn a_layer_is_carried_forward_on_the_manifest_that_names_it() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let source = fixture.registry("stelae/source");
    let published = write_stele(&source, 3);
    let stele = source.latest(&ToyProfile).unwrap().unwrap();

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

/// An operator who does not trust the repository's retention gets the check
/// back, and it still lands before the manifest does.
///
/// This is the guarantee `verify_adopted` exists for, and the failure it
/// prevents is invisible without it: a manifest pointing at a reclaimed blob is
/// a perfectly well-formed stele that nobody can restore. What has moved is
/// *when* the refusal is reported — the `HEAD` runs concurrently with the rest
/// of the publish and is joined at the seal — and what has not moved is that
/// the refusal comes before anything is tagged.
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
fn a_verified_carry_refuses_a_blob_the_repository_does_not_hold() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let source = fixture.registry("stelae/source");
    let published = write_stele(&source, 3);
    let stele = source.latest(&ToyProfile).unwrap().unwrap();

    let somewhere_else = Fixture::spawn();
    let elsewhere = somewhere_else.verifying("stelae/source");

    // The descriptor is handed back: nothing has been asked yet, and what the
    // caller holds is a fact about bytes rather than a promise about a
    // registry.
    elsewhere
        .adopt_layer(&stele, published.layers[0].clone())
        .unwrap();

    // The seal is where the promise is collected, and it is refused.
    let mut inscription = published.clone();
    inscription.layers = vec![published.layers[0].clone()];

    let err = elsewhere.seal(&ToyProfile, &inscription).unwrap_err();

    assert!(matches!(err, Error::BlobMissing { .. }), "{err:?}");

    // And nothing was published: the moving tag in a repository that never had
    // a stele still resolves to nothing.
    assert!(
        elsewhere.latest(&ToyProfile).unwrap().is_none(),
        "a refused seal tagged a manifest anyway",
    );
}

/// Concurrency changes what a publish costs and nothing about what it
/// produces.
///
/// The claim the whole change rests on. The same records, published through the
/// serial path and through eight-way concurrency, must give the same
/// inscription — the same identity — the same manifest bytes, and the same
/// transfer counters, because none of those is a function of the order the
/// blobs happened to land in.
///
/// Two repositories in one registry rather than two registries, so the
/// comparison is not also comparing two servers — with the one consequence
/// that the *counters* cannot be compared to each other. `zot` addresses
/// blobs across the whole registry rather than per repository, as
/// [`a_verified_carry_refuses_a_blob_the_repository_does_not_hold`] documents
/// at more length, so on that registry the second publish skips what the first
/// one uploaded. What is asserted instead is the property that holds on either
/// kind and is the one worth having: every layer and every byte the stele
/// describes is accounted for, whichever way the registry answered.
#[test]
#[ignore = "spawns a registry"]
fn concurrency_changes_the_cost_of_a_publish_and_not_the_stele() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let serial = fixture.registry_at("stelae/serial", 1);
    let concurrent = fixture.registry_at("stelae/concurrent", 8);

    let one = write_stele(&serial, 5);
    let other = write_stele(&concurrent, 5);

    assert_eq!(one, other, "the inscriptions differ");
    assert_eq!(
        one.digest().unwrap(),
        other.digest().unwrap(),
        "the identities differ",
    );

    let from_serial = serial.pull_latest(&ToyProfile).unwrap();
    let from_concurrent = concurrent.pull_latest(&ToyProfile).unwrap();

    assert_eq!(
        manifest_bytes(from_serial.manifest()).unwrap(),
        manifest_bytes(from_concurrent.manifest()).unwrap(),
        "the manifests differ",
    );

    // The serial publish is the first into this registry, so nothing can have
    // been there before it: two layers, both uploaded.
    let counted = serial.transfer();

    assert_eq!(counted.layers_uploaded, one.layers.len() as u64);
    assert_eq!(counted.layers_skipped, 0);

    // And the concurrent one accounts for exactly the same layers and the same
    // bytes, however the registry split them between "uploaded" and "the far
    // side already had it".
    let against = concurrent.transfer();

    assert_eq!(
        against.layers_uploaded + against.layers_skipped,
        counted.layers_uploaded + counted.layers_skipped,
        "a layer went unaccounted for",
    );
    assert_eq!(
        against.bytes_uploaded + against.bytes_skipped,
        counted.bytes_uploaded + counted.bytes_skipped,
        "the bytes do not add up",
    );
    assert_eq!(against.layers_reused, 0, "nothing was carried forward");

    // And it reads back, which is the property the manifest exists to serve.
    let inscription = from_concurrent.read_inscription().unwrap();

    assert_eq!(inscription, one);
    records_of(&from_concurrent, &inscription);
}

/// A publish abandoned while its layers are still in flight leaves the stele
/// before it standing.
///
/// The concurrent path's version of the ordering argument, and the reason the
/// join is at the seal rather than anywhere later: layers go up in parallel,
/// but nothing is tagged until all of them are up, so a publisher that dies in
/// the middle — here, a transport dropped without a seal — leaves untagged
/// blobs the registry reclaims and a moving tag still pointing at a stele that
/// restores.
#[test]
#[ignore = "spawns a registry"]
fn a_publish_dropped_mid_flight_leaves_the_previous_stele_standing() {
    let _serial = exclusive();

    let fixture = Fixture::spawn();

    let standing = write_stele(&fixture.registry("stelae/abandoned"), 3);

    {
        let abandoning = fixture.registry_at("stelae/abandoned", 8);

        let (header, scope) = notes_scope(4);
        let mut sink = abandoning
            .layer_sink(
                &ToyProfile,
                &LayerSpec::new("notes", header, scope),
                COMPRESSION_LEVEL,
            )
            .unwrap();

        for id in 1..=64 {
            sink.write_record(&note_record(id)).unwrap();
        }

        sink.finish().unwrap();

        // And dropped here, with the upload deferred and no seal to join it.
    }

    let reopened = fixture.registry("stelae/abandoned");
    let stele = reopened.pull_latest(&ToyProfile).unwrap();

    assert_eq!(stele.read_inscription().unwrap(), standing);
}

/// A deferred upload's failure is the seal's failure, and it stays the seal's
/// failure.
///
/// No registry: the transport is pointed at a port nothing is listening on, so
/// every round trip it defers is refused. That is enough to hold the two
/// properties that matter about the deferral, and it holds them under plain
/// `cargo test` rather than only where a container can be spawned.
///
/// 1. **`finish` succeeds.** Closing a layer is a fact about bytes the sink
///    already has; a transport that could not reach the registry still hands
///    back the descriptor, because the caller's next act is to read more of its
///    store and not to wait on a socket.
/// 2. **`seal` fails, and every seal after it fails too.** The join empties the
///    handles it awaited, so a transport that forgot would find nothing
///    outstanding the second time, agree that every layer was up, and publish a
///    manifest naming a blob that never landed. That is the one document this
///    transport must never write, and it is exactly the document a concurrent
///    publish makes reachable — so the refusal is remembered rather than
///    recomputed.
/// 3. **The failure was retried first, and said so.** A connection nobody
///    answers is the transient class, so the round trip is made again before it
///    is anybody's failure — and the retry is announced, because a transport
///    that absorbed a registry's bad minute in silence would have hidden the
///    measurement that motivated absorbing it. Two attempts here rather than
///    the default four, so the test proves the loop runs without waiting out
///    the whole of its patience.
#[test]
fn a_deferred_upload_that_fails_fails_every_seal() {
    let _serial = exclusive();

    install_crypto_provider();

    // Bound and dropped: the kernel just told us a port nobody has, and
    // refusing a connection is faster and more portable than any other way of
    // failing one.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);

    let registry = Registry::open(
        &format!("oci://{address}/stelae/nowhere").parse().unwrap(),
        Options {
            insecure: true,
            concurrency: 4,
            attempts: 2,
            ..Default::default()
        },
    )
    .unwrap();

    let retries = std::sync::Arc::new(Retries::default());
    registry.observe(Observer::new(retries.clone()));

    let (header, scope) = notes_scope(1);
    let notes: Vec<CanonicalCbor> = (1..=3).map(note_record).collect();

    let written = registry
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", header, scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .expect("closing a layer waited on the registry");

    let mut inscription = Inscription::new(
        &ToyProfile,
        1,
        json!({"chapter": 1}),
        json!({"noteWidth": 40}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.layers = vec![written.descriptor];

    let refused = registry
        .seal(&ToyProfile, &inscription)
        .expect_err("sealed against a port nothing is listening on");

    // The *cause*, not the sticky refusal — and asserting which way round that
    // is, is the point. `LayerNotWritten` is what every seal *after* this one
    // answers; a first seal that reported it would have thrown away what the
    // network actually said, leaving an operator to debug a connection failure
    // from a message about a layer.
    assert!(
        matches!(refused, Error::Registry(_)),
        "the first seal reported the refusal instead of its cause: {refused:?}",
    );

    println!("the seal collected the deferred failure: {refused}");

    // And it is remembered: nothing is outstanding any more, so a transport
    // that only asked what was in flight would seal this stele over a blob that
    // never landed.
    let again = registry
        .seal(&ToyProfile, &inscription)
        .expect_err("the second seal published a manifest over a blob that never landed");

    assert!(
        matches!(again, Error::LayerNotWritten(_)),
        "the second seal did not remember the first: {again:?}",
    );

    // Carrying the cause, so the operator reading the second refusal is not
    // told less than the one who read the first.
    let Error::LayerNotWritten(why) = &again else {
        unreachable!()
    };

    assert!(!why.is_empty(), "the refusal names no cause");

    println!("and every seal after it: {again}");

    // One round trip was deferred — the existence check for the one layer — and
    // it was made twice. The second seal had nothing outstanding to retry, so
    // this also says the sticky refusal is answered without touching the
    // network again.
    assert_eq!(
        retries.seen(),
        vec![(1, 1)],
        "a refused connection was not retried before the layer was declared lost",
    );
}

/// What a transport said about the round trips it made again.
#[derive(Default)]
struct Retries(Mutex<Vec<(u32, u32)>>);

impl Progress for Retries {
    fn on(&self, event: Event<'_>) {
        if let Event::Retry {
            attempt, remaining, ..
        } = event
        {
            self.0.lock().unwrap().push((attempt, remaining));
        }
    }
}

impl Retries {
    fn seen(&self) -> Vec<(u32, u32)> {
        self.0.lock().unwrap().clone()
    }
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
