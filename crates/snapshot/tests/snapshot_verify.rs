//! Verifying and inspecting a published stele, end to end.
//!
//! Nearly everything here needs a **registry**, and spawns one: `docker run`
//! of an OCI Distribution server, torn down on the way out. So all of it but
//! what runs against a listener that answers nothing is `#[ignore]`d, and an
//! `#[ignore]`d test that was never executed proves nothing — run it with:
//!
//! ```text
//! cargo test -p dolos-snapshot --test snapshot_verify -- --ignored --nocapture
//! ```
//!
//! ## What this suite proves
//!
//! 1. **A freshly published stele verifies clean**, inherited layers included:
//!    the manifest and the inscription agree, and every blob streams back to
//!    both of its digests.
//! 2. **Each of the three tampers is refused with the offence named**: a blob
//!    that is not the layer it is addressed as, a manifest `diffId` annotation
//!    disagreeing with the inscription, and a history that skips a sequence.
//!    The tampered artifacts are planted through the raw distribution API,
//!    because the transport refuses to write any of them — which is the point.
//! 3. **A reproduction from the publisher's own stores matches**, and a store
//!    standing at a different epoch is refused before a single layer is
//!    rebuilt.
//! 4. **An inspection reports what the manifest carries**, and its canonical
//!    JSON is exactly what `digest --chain-from` takes.
//! 5. **A resumed publish reproduces**: a resumption record never supplies an
//!    account window its predecessor's manifest lacks.
//!
//! ## The registry is content-addressed, so tampering means manifests
//!
//! A stored blob cannot be altered without changing its name; what *can* lie
//! is the mutable tag, and everything it points at. Each tamper here is
//! therefore a manifest (or config blob) rewritten under `latest` — the shape
//! of attack a verifier actually faces.

mod node;
mod registry_fixture;

use dolos_core::{Domain as _, StateStore as _};
use dolos_snapshot::{
    boundary::Ineligible,
    export::{self, Following, Predecessor as _},
    registry::{self, Point, PublishRecord, Publishing},
    DolosProfile, Error, RetainedEpochs,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use serde_json::json;
use stelae::{Digest, SteleReader as _};

use node::{account_layer, plan_at_boundary, settled, stake_only, write_account_row, Node};
use registry_fixture::{Fixture, Unanswered};

/// Layers of epoch 0 that sequence 2 inherits: `blocks`, `indexes` and
/// `log-epochs`. The harness also seeds `log-account-epochs`, which a publish
/// builds again until its window has settled (`dolos_snapshot::is_settled`).
/// The other log kinds have no records in the window, so they have no layer.
const EPOCH_0: usize = 3;

// ---------------------------------------------------------------------------
// Done criterion 3: verify --repo
// ---------------------------------------------------------------------------

/// The pass, before the refusals mean anything.
///
/// The stele verified at `latest` is the interesting one: it inherited epoch
/// 0's layers from its predecessor, so a clean verification here is the
/// transport half of the trust-gap closure — every attested blob streamed and
/// checked, whether or not this publish built it.
#[test]
#[ignore = "spawns a registry"]
fn a_freshly_published_stele_verifies_clean() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository_name = "dolos/verify-clean";
    let repository = fixture.repository(repository_name);

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(
        second.layers_reused, EPOCH_0,
        "the interesting stele is one with inherited layers"
    );

    let reader = fixture.snapshot_repository(repository_name);
    let verified = reader.verify(Point::Latest).unwrap();

    assert_eq!(verified.identity, second.identity);
    assert_eq!(
        verified.inscription.layers.len(),
        second.inscription.layers.len()
    );
    assert!(verified.compressed_bytes > 0);

    // The immutable tag reads the predecessor back just as clean.
    let predecessor = reader.verify(Point::Epoch(1)).unwrap();

    assert_eq!(predecessor.identity, first.identity);

    eprintln!(
        "verified latest = {} ({} layers, {} compressed bytes) and epoch-1 = {}",
        verified.identity,
        verified.inscription.layers.len(),
        verified.compressed_bytes,
        predecessor.identity,
    );
}

/// A blob that is not the layer it is addressed as, refused with the layer
/// named.
///
/// The closest a content-addressed registry comes to a corrupted blob: the
/// bytes are correctly named — so the in-flight blob-digest check passes and
/// the two documents still agree — and they are simply not the layer. Only
/// the streaming check can catch it, and the refusal has to say which layer.
#[test]
#[ignore = "spawns a registry"]
fn a_blob_that_is_not_the_layer_is_refused_with_the_layer_named() {
    let fixture = Fixture::spawn();
    let distribution = Distribution::new(&fixture);
    let node = Node::build();

    let name = "dolos/verify-corrupt";
    let repository = fixture.repository(name);

    node.publish(&repository, &node.first, false);

    // A well-formed zstd frame over bytes nobody attested, through the same
    // pipeline a real layer takes so nothing about its shape gives it away
    // before the content does.
    let blob = {
        use std::io::Write as _;

        let mut writer = stelae::LayerWriter::new(Vec::new(), 3).unwrap();
        writer.write_all(b"not the layer anybody attested").unwrap();
        writer.finish().unwrap().0
    };

    let planted = distribution.put_blob(name, &blob);

    // Point the manifest's first layer at it, leaving the annotations and the
    // inscription untouched: the pull's document cross-check still passes,
    // which is exactly why verify streams.
    let mut manifest = distribution.manifest(name, "latest");
    manifest["layers"][0]["digest"] = json!(planted);
    manifest["layers"][0]["size"] = json!(blob.len());
    distribution.put_manifest(name, "latest", &manifest);

    let err = registry::verify(&repository, Point::Latest).unwrap_err();
    let message = err.to_string();

    assert!(matches!(err, Error::LayerVerification { .. }), "{err:?}");

    // Layer 0 is epoch 0's blocks layer, and the message names it with its
    // scope — the two things an operator needs to know what to re-publish.
    assert!(message.contains("blocks"), "{message}");
    assert!(message.contains("epoch"), "{message}");

    eprintln!("corrupted blob: {message}");
}

/// A manifest `diffId` annotation that disagrees with the inscription,
/// refused at the pull with the layer position and both identities named.
#[test]
#[ignore = "spawns a registry"]
fn a_diff_id_annotation_that_disagrees_is_refused() {
    let fixture = Fixture::spawn();
    let distribution = Distribution::new(&fixture);
    let node = Node::build();

    let name = "dolos/verify-annotation";
    let repository = fixture.repository(name);

    node.publish(&repository, &node.first, false);

    let wrong = Digest::compute([0xee]).to_string();

    let mut manifest = distribution.manifest(name, "latest");
    manifest["layers"][0]["annotations"]["store.stelae.layer.diffId"] = json!(wrong);
    distribution.put_manifest(name, "latest", &manifest);

    let err = registry::verify(&repository, Point::Latest).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("layer 0"), "{message}");
    assert!(message.contains(&wrong), "{message}");

    eprintln!("diffId disagreement: {message}");
}

/// A history that skips a sequence, refused before a single blob is fetched.
///
/// The gapped chain is planted as a rewritten config blob under `latest` —
/// the protocol refuses to *produce* one, so the only way it exists is a
/// registry serving a document this code never wrote. The refusal happens at
/// parse, naming the gap.
#[test]
#[ignore = "spawns a registry"]
fn a_history_that_skips_a_sequence_is_refused() {
    let fixture = Fixture::spawn();
    let distribution = Distribution::new(&fixture);
    let node = Node::build();

    let name = "dolos/verify-gap";
    let repository = fixture.repository(name);

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    // Sequence 4 claiming a history of 1 and 3: the entries themselves ascend,
    // and the chain still skips sequence 2.
    let mut gapped = serde_json::to_value(&second.inscription).unwrap();
    gapped["sequence"] = json!(4);
    gapped["history"] = json!([
        {"sequence": 1, "inscriptionDigest": first.identity.to_string()},
        {"sequence": 3, "inscriptionDigest": second.identity.to_string()},
    ]);

    let config = stelae::inscription::canonical_json(&gapped).unwrap();
    let planted = distribution.put_blob(name, &config);

    let mut manifest = distribution.manifest(name, "latest");
    manifest["config"]["digest"] = json!(planted);
    manifest["config"]["size"] = json!(config.len());
    distribution.put_manifest(name, "latest", &manifest);

    let err = registry::verify(&repository, Point::Latest).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("gap"), "{message}");
    assert!(message.contains('1') && message.contains('3'), "{message}");

    eprintln!("skipped sequence: {message}");
}

// ---------------------------------------------------------------------------
// Done criterion 4: verify --repo --reproduce
// ---------------------------------------------------------------------------

/// The reproduction passes against the store the stele was published from,
/// and a store standing at a different epoch is refused before the walk.
///
/// The published stele inherited epoch 0's layers without rebuilding them, so
/// the pass here is the whole trust-gap closure in one assertion: every
/// attested layer came back out of the stores byte-identical, history and all.
/// The refusal is the same ledger one epoch earlier — which is what "a store at
/// a different epoch" is — and it costs a comparison of two sequences, not
/// hours of compression.
#[test]
#[ignore = "spawns a registry"]
fn a_reproduction_passes_at_the_published_epoch_and_fails_at_another() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/verify-reproduce");

    node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(
        second.layers_reused, EPOCH_0,
        "there is no trust gap to close unless the publish inherited something"
    );

    let verified = registry::verify(&repository, Point::Latest).unwrap();

    let reproduced = export::verify_reproduction(
        &verified.inscription,
        &node.second,
        node.domain.archive(),
        node.domain.state(),
        None,
    )
    .unwrap();

    assert_eq!(reproduced.digest().unwrap(), verified.identity);

    let err = export::verify_reproduction(
        &verified.inscription,
        &node.first,
        node.earlier.archive(),
        node.earlier.state(),
        None,
    )
    .unwrap_err();

    let message = err.to_string();

    assert!(matches!(err, Error::ReproductionMismatch { .. }), "{err:?}");
    assert!(
        message.contains("sequence 2") && message.contains("sequence 1"),
        "{message}"
    );

    eprintln!(
        "reproduced {} == published {}; a store at another epoch: {message}",
        reproduced.digest().unwrap(),
        verified.identity,
    );
}

/// A registry publish and its dry run refuse stores past their boundary, and
/// the refusal comes before the repository is touched: nothing is published,
/// and a stele at the boundary still publishes into it afterwards.
#[test]
#[ignore = "spawns a registry"]
fn a_registry_publish_and_its_dry_run_refuse_a_cursor_past_the_boundary() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/boundary");

    let past = node::harness::<dolos_testing::toy_domain::MemoryStores>();
    let first = past.state().read_cursor().unwrap().unwrap();
    node::step_past_boundary(&past);
    let plan = node::plan_for(&past);

    let refused = |err: Error| match err {
        Error::NotABoundary { reason, .. } => reason,
        other => panic!("{other:?}"),
    };

    let expected = Ineligible::LaterBlock { parent: first };

    let preview = registry::preview(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
        None,
    );
    assert_eq!(refused(preview.unwrap_err()), expected);

    let publish = registry::publish(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
        None,
        &stelae::progress::Observer::silent(),
    );
    assert_eq!(refused(publish.unwrap_err()), expected);

    assert!(
        repository.latest(&DolosProfile).unwrap().is_none(),
        "a refused publish left a stele behind"
    );

    let published = node.publish(&repository, &node.first, false);
    assert_eq!(published.inscription.sequence, 1);
}

/// The same refusals ask the registry nothing: neither opens a connection to
/// it. The repository answers no request, so a refusal that came after a
/// read would surface as a transport error instead.
#[test]
fn a_registry_refusal_comes_before_any_request() {
    let unanswered = Unanswered::start();
    let repository = unanswered.repository("dolos/boundary");

    let past = node::harness::<dolos_testing::toy_domain::MemoryStores>();
    let first = past.state().read_cursor().unwrap().unwrap();
    node::step_past_boundary(&past);
    let plan = node::plan_for(&past);

    let expected = Ineligible::LaterBlock { parent: first };

    let refused = |err: Error| match err {
        Error::NotABoundary { reason, .. } => reason,
        other => panic!("{other:?}"),
    };

    let opened = unanswered.connections();

    let preview = registry::preview(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
        None,
    );
    assert_eq!(refused(preview.unwrap_err()), expected);

    let publish = registry::publish(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
        None,
        &stelae::progress::Observer::silent(),
    );
    assert_eq!(refused(publish.unwrap_err()), expected);

    assert_eq!(
        unanswered.connections(),
        opened,
        "a refused publish reached the registry"
    );
}

// ---------------------------------------------------------------------------
// A resumed publish
// ---------------------------------------------------------------------------

/// Finding G of the fourth QA review of dolos#1499, through the registry: a
/// resumption record holds a settled account window the predecessor's manifest
/// lacks, and the publish that resumes from it rebuilds that window.
///
/// Host A's first publish at 2 dies at the seal, after window 1's stake-only
/// account rows were uploaded and recorded. Host B publishes 2 and 3 from
/// stores with no row in window 1, so the manifest at 3 has no such layer. The
/// EWRAP closing 2 settles A's row, and A resumes at 4: the window is settled
/// and the manifest silent, so only the record could lend it. The stele
/// reproduces from A's stores, and the dry run counted what the publish built
/// and reused.
#[test]
#[ignore = "spawns a registry"]
fn a_resumed_registry_publish_rebuilds_an_account_window_the_manifest_lacks() {
    let fixture = Fixture::spawn();
    // One handle per process, as the hosts would hold them.
    let process = || fixture.repository("dolos/resume-account-window");
    let record = tempfile::tempdir().unwrap();
    let record_path = registry::record_path_in(record.path());

    let host_a: ToyDomain<MemoryStores> = node::harness();
    write_account_row(&host_a, 1, stake_only());

    let cut_at_2 = plan_at_boundary(&host_a, 2, RetainedEpochs::default());
    let dying = process();
    let interrupted = registry::publish_into(
        &SealFails(&dying),
        Publishing::new(&dying).recording_in(&record_path),
        &cut_at_2,
        host_a.archive(),
        host_a.state(),
        None,
        &stelae::progress::Observer::silent(),
    );
    assert!(interrupted.is_err(), "the seal was meant to fail");

    let stale = export::reproduce(
        &cut_at_2,
        host_a.archive(),
        host_a.state(),
        None,
        &export::First,
    )
    .unwrap();
    let stale = account_layer(&stale, 1);

    // The record was written through the wrapper the registry publishes with.
    let recorded = PublishRecord::load(&record_path).unwrap().unwrap();
    assert!(
        recorded
            .layers
            .iter()
            .any(|layer| layer.descriptor == stale),
        "the interrupted publish recorded no stake-only window 1"
    );

    // The failed seal never joined the uploads, and a resume adopts a recorded
    // layer only once its blob is up, so wait for it.
    let recorded = recorded
        .layers
        .into_iter()
        .find(|layer| layer.descriptor == stale)
        .unwrap();
    let probe = process();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !probe.adopt_carried(recorded.clone()).unwrap() {
        assert!(
            std::time::Instant::now() < deadline,
            "the stake-only window 1 never finished uploading"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let host_b: ToyDomain<MemoryStores> = node::harness();
    for sequence in [2, 3] {
        let plan = plan_at_boundary(&host_b, sequence, RetainedEpochs::default());
        registry::publish(
            Publishing::new(&process()),
            &plan,
            host_b.archive(),
            host_b.state(),
            None,
            &stelae::progress::Observer::silent(),
        )
        .unwrap();
    }

    let manifest = process()
        .latest(&DolosProfile)
        .unwrap()
        .unwrap()
        .read_inscription()
        .unwrap();
    assert!(
        !manifest.layers.iter().any(|layer| {
            layer.kind == "log-account-epochs"
                && layer.scope.get("epoch").and_then(serde_json::Value::as_u64) == Some(1)
        }),
        "the manifest at 3 was meant to lack window 1"
    );

    // Through 3 as B went, so the two hosts hold the same chain.
    node::stand_at_boundary(&host_a, 3);
    write_account_row(&host_a, 1, settled());
    let cut_at_4 = plan_at_boundary(&host_a, 4, RetainedEpochs::default());

    let resumed = process();
    let resuming = || Publishing::new(&resumed).recording_in(&record_path);
    let preview = registry::preview(
        resuming(),
        &cut_at_4,
        host_a.archive(),
        host_a.state(),
        None,
    )
    .unwrap();
    let published = registry::publish(
        resuming(),
        &cut_at_4,
        host_a.archive(),
        host_a.state(),
        None,
        &stelae::progress::Observer::silent(),
    )
    .unwrap();

    assert_ne!(account_layer(&published.inscription, 1), stale);
    assert!(published.layers_reused > 0, "nothing was inherited");
    assert_eq!(
        (preview.layers_built, preview.layers_reused),
        (published.layers_built, published.layers_reused)
    );

    let reproduced = export::verify_reproduction(
        &published.inscription,
        &cut_at_4,
        host_a.archive(),
        host_a.state(),
        None,
    )
    .unwrap();
    assert_eq!(reproduced.digest().unwrap(), published.identity);
}

/// The registry, except that the seal fails: every layer is up and recorded,
/// and the stele never becomes readable.
struct SealFails<'a>(&'a stelae::oci::Registry);

impl stelae::SteleWriter for SealFails<'_> {
    type Sink = <stelae::oci::Registry as stelae::SteleWriter>::Sink;

    fn layer_sink(
        &self,
        profile: &dyn stelae::Profile,
        spec: &stelae::LayerSpec,
        level: i32,
    ) -> Result<Self::Sink, stelae::Error> {
        self.0.layer_sink(profile, spec, level)
    }

    fn seal(
        &self,
        _profile: &dyn stelae::Profile,
        _inscription: &stelae::Inscription,
    ) -> Result<Digest, stelae::Error> {
        Err(stelae::Error::Io(std::io::Error::other(
            "the machine went away",
        )))
    }

    fn carry_again(
        &self,
        written: &stelae::transport::WrittenLayer,
        scope: serde_json::Value,
    ) -> Result<stelae::transport::WrittenLayer, stelae::Error> {
        self.0.carry_again(written, scope)
    }
}

// ---------------------------------------------------------------------------
// Done criterion 5: inspect --repo
// ---------------------------------------------------------------------------

/// An inspection lists every layer with the compressed size the manifest
/// carries, and its canonical JSON chains a digest.
///
/// The round trip is the criterion's own: inspect the predecessor, hand the
/// bytes to the same `Following::read` that `digest --chain-from` calls, and
/// the reproduction arrives at the digest of the stele published against it.
#[test]
#[ignore = "spawns a registry"]
fn an_inspection_reports_the_manifest_and_its_json_chains_a_digest() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository_name = "dolos/inspect";
    let repository = fixture.repository(repository_name);

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    let reader = fixture.snapshot_repository(repository_name);
    let inspected = reader.inspect(Point::Latest).unwrap();

    assert_eq!(inspected.identity, second.identity);
    assert_eq!(
        inspected.compressed.len(),
        inspected.inscription.layers.len()
    );

    // Every layer carries a real compressed size — the inherited ones
    // included, whose bytes this publish never moved — and they sum to the
    // manifest's own total.
    let mut total = 0;

    for (index, size) in inspected.compressed.iter().enumerate() {
        let size = size.unwrap_or_else(|| panic!("layer {index} carries no compressed size"));

        assert!(size > 0, "layer {index}");
        total += size;
    }

    assert_eq!(total, inspected.total_compressed);

    // The `--json` output is the canonical document, verbatim.
    let predecessor = reader.inspect(Point::Epoch(1)).unwrap();

    assert_eq!(predecessor.identity, first.identity);

    let canonical = predecessor.inscription.canonicalize().unwrap();
    let following = Following::read(&canonical, &node.second).unwrap();

    assert_eq!(following.history().len(), 1);

    let reproduced = export::reproduce(
        &node.second,
        node.domain.archive(),
        node.domain.state(),
        None,
        &following,
    )
    .unwrap();

    assert_eq!(
        reproduced.digest().unwrap(),
        second.identity,
        "inspect's own output chained the digest of the stele published against it"
    );

    eprintln!(
        "inspected {} layers, {} compressed bytes; epoch-1's document chained to {}",
        inspected.inscription.layers.len(),
        inspected.total_compressed,
        reproduced.digest().unwrap(),
    );
}

// ---------------------------------------------------------------------------
// The raw distribution client the tampers need
// ---------------------------------------------------------------------------

/// The distribution API, spoken directly.
///
/// Exists because the tampered artifacts these tests plant are exactly the
/// documents the transport refuses to write. HTTP/1.0 with `Connection:
/// close` over a loopback socket, which keeps the parsing to a status line,
/// a header block and a read-to-EOF body — and assumes the `distribution`
/// server the fixture runs by default.
struct Distribution {
    address: String,
}

impl Distribution {
    fn new(fixture: &Fixture) -> Self {
        Self {
            address: fixture.address(),
        }
    }

    /// Every socket operation is bounded, for the reason the fixture's
    /// `wait_until_ready` bounds its own: a registry that accepts and then says
    /// nothing turns an `#[ignore]`d end-to-end test into a CI job that hangs
    /// rather than one that fails with a message. A loopback exchange of a few
    /// test-sized blobs has no legitimate use for more than this.
    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(30);

    fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> (u16, Vec<(String, String)>, Vec<u8>) {
        use std::io::{Read as _, Write as _};

        // `Fixture::address` is `127.0.0.1:{port}`, so this parses without
        // resolution — which is what lets the connect itself be bounded.
        let socket_address: std::net::SocketAddr =
            self.address.parse().expect("a loopback address and a port");

        let mut socket =
            std::net::TcpStream::connect_timeout(&socket_address, Self::PATIENCE).unwrap();

        socket.set_write_timeout(Some(Self::PATIENCE)).unwrap();
        socket.set_read_timeout(Some(Self::PATIENCE)).unwrap();

        let credentials =
            base64(format!("{}:{}", registry_fixture::USER, registry_fixture::PASSWORD).as_bytes());

        let mut request = format!(
            "{method} {path} HTTP/1.0\r\nHost: {}\r\nAuthorization: Basic {credentials}\r\n\
             Connection: close\r\nContent-Length: {}\r\n",
            self.address,
            body.len(),
        );

        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }

        request.push_str("\r\n");

        socket.write_all(request.as_bytes()).unwrap();
        socket.write_all(body).unwrap();

        let mut raw = Vec::new();
        socket.read_to_end(&mut raw).unwrap();

        let boundary = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("an HTTP response has a header block");

        let head = String::from_utf8_lossy(&raw[..boundary]).into_owned();
        let body = raw[boundary + 4..].to_vec();

        let mut lines = head.lines();

        let status: u16 = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("no status line in {head:?}"));

        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect();

        (status, headers, body)
    }

    fn manifest(&self, repository: &str, tag: &str) -> serde_json::Value {
        let (status, _, body) = self.request(
            "GET",
            &format!("/v2/{repository}/manifests/{tag}"),
            &[(
                "Accept",
                "application/vnd.oci.image.manifest.v1+json".to_owned(),
            )],
            &[],
        );

        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));

        serde_json::from_slice(&body).unwrap()
    }

    fn put_manifest(&self, repository: &str, tag: &str, manifest: &serde_json::Value) {
        let body = serde_json::to_vec(manifest).unwrap();

        let (status, _, response) = self.request(
            "PUT",
            &format!("/v2/{repository}/manifests/{tag}"),
            &[(
                "Content-Type",
                "application/vnd.oci.image.manifest.v1+json".to_owned(),
            )],
            &body,
        );

        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&response));
    }

    /// Upload a blob and return the digest it is addressed by.
    ///
    /// The registry checks the digest against the bytes, so this can plant
    /// wrong *content* but never a wrong name — which is the property the
    /// corrupted-blob test leans on.
    fn put_blob(&self, repository: &str, bytes: &[u8]) -> String {
        let digest = Digest::compute(bytes).to_string();

        let (status, headers, response) = self.request(
            "POST",
            &format!("/v2/{repository}/blobs/uploads/"),
            &[],
            &[],
        );

        assert_eq!(status, 202, "{}", String::from_utf8_lossy(&response));

        let location = headers
            .iter()
            .find(|(name, _)| name == "location")
            .map(|(_, value)| value.clone())
            .expect("an upload start answers with a Location");

        // Absolute or path-relative, per the server's taste.
        let location = location
            .strip_prefix(&format!("http://{}", self.address))
            .unwrap_or(&location)
            .to_owned();

        let separator = if location.contains('?') { '&' } else { '?' };

        let (status, _, response) = self.request(
            "PUT",
            &format!("{location}{separator}digest={digest}"),
            &[("Content-Type", "application/octet-stream".to_owned())],
            bytes,
        );

        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&response));

        digest
    }
}

/// Standard base64, for the Basic credential pair.
///
/// Hand-rolled rather than imported: it is eleven lines, it runs in a test,
/// and a dev-dependency for one header is a dependency review nobody needs.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::new();

    for chunk in bytes.chunks(3) {
        let mut word = 0u32;

        for (index, byte) in chunk.iter().enumerate() {
            word |= u32::from(*byte) << (16 - 8 * index);
        }

        for index in 0..4 {
            if index <= chunk.len() {
                encoded.push(ALPHABET[((word >> (18 - 6 * index)) & 0x3f) as usize] as char);
            } else {
                encoded.push('=');
            }
        }
    }

    encoded
}
