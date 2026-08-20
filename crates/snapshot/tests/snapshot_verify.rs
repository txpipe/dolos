//! Verifying and inspecting a published stele, end to end.
//!
//! Everything here needs a **registry**, and spawns one: `docker run` of an
//! OCI Distribution server, torn down on the way out. So everything here is
//! `#[ignore]`d, and an `#[ignore]`d test that was never executed proves
//! nothing — run it with:
//!
//! ```text
//! cargo test -p dolos-snapshot --features oci --test snapshot_verify -- --ignored --nocapture
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
//!
//! ## The registry is content-addressed, so tampering means manifests
//!
//! A stored blob cannot be altered without changing its name; what *can* lie
//! is the mutable tag, and everything it points at. Each tamper here is
//! therefore a manifest (or config blob) rewritten under `latest` — the shape
//! of attack a verifier actually faces.

#![cfg(feature = "oci")]

mod node;
mod registry_fixture;

use dolos_core::Domain as _;
use dolos_snapshot::{
    export::{self, Following, Predecessor as _},
    registry::{self, Point},
    Error,
};
use serde_json::json;
use stelae::Digest;

use node::Node;
use registry_fixture::Fixture;

// ---------------------------------------------------------------------------
// Done criterion 3: verify --repo
// ---------------------------------------------------------------------------

/// The pass, before the refusals mean anything.
///
/// The stele verified at `latest` is the interesting one: it inherited epoch
/// 0's three layers from its predecessor, so a clean verification here is the
/// transport half of the trust-gap closure — every attested blob streamed and
/// checked, whether or not this publish built it.
#[test]
#[ignore = "spawns a registry"]
fn a_freshly_published_stele_verifies_clean() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/verify-clean");

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(
        second.layers_reused, 3,
        "the interesting stele is one with inherited layers"
    );

    let verified = registry::verify(&repository, Point::Latest).unwrap();

    assert_eq!(verified.identity, second.identity);
    assert_eq!(
        verified.inscription.layers.len(),
        second.inscription.layers.len()
    );
    assert!(verified.compressed_bytes > 0);

    // The immutable tag reads the predecessor back just as clean.
    let predecessor = registry::verify(&repository, Point::Epoch(0)).unwrap();

    assert_eq!(predecessor.identity, first.identity);

    eprintln!(
        "verified latest = {} ({} layers, {} compressed bytes) and epoch-0 = {}",
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

    // Sequence 3 claiming a history of 0 and 2: the entries themselves ascend,
    // and the chain still skips sequence 1.
    let mut gapped = serde_json::to_value(&second.inscription).unwrap();
    gapped["sequence"] = json!(3);
    gapped["history"] = json!([
        {"sequence": 0, "inscriptionDigest": first.identity.to_string()},
        {"sequence": 2, "inscriptionDigest": second.identity.to_string()},
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
    assert!(message.contains('0') && message.contains('2'), "{message}");

    eprintln!("skipped sequence: {message}");
}

// ---------------------------------------------------------------------------
// Done criterion 4: verify --repo --reproduce
// ---------------------------------------------------------------------------

/// The reproduction passes against the store the stele was published from,
/// and a store standing at a different epoch is refused before the walk.
///
/// The published stele inherited three layers it never rebuilt, so the pass
/// here is the whole trust-gap closure in one assertion: every attested layer
/// came back out of the stores byte-identical, history and all. The refusal
/// is the same ledger one epoch earlier — which is what "a store at a
/// different epoch" is — and it costs a comparison of two sequences, not
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
        second.layers_reused, 3,
        "there is no trust gap to close unless the publish inherited something"
    );

    let verified = registry::verify(&repository, Point::Latest).unwrap();

    let reproduced = export::verify_reproduction(
        &verified.inscription,
        &node.second,
        node.domain.archive(),
        node.domain.state(),
        node.domain.indexes(),
        None,
    )
    .unwrap();

    assert_eq!(reproduced.digest().unwrap(), verified.identity);

    let err = export::verify_reproduction(
        &verified.inscription,
        &node.first,
        node.domain.archive(),
        node.domain.state(),
        node.domain.indexes(),
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
    let repository = fixture.repository("dolos/inspect");

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    let inspected = registry::inspect(&repository, Point::Latest).unwrap();

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
    let predecessor = registry::inspect(&repository, Point::Epoch(0)).unwrap();

    assert_eq!(predecessor.identity, first.identity);

    let canonical = predecessor.inscription.canonicalize().unwrap();
    let following = Following::read(&canonical, &node.second).unwrap();

    assert_eq!(following.history().len(), 1);

    let reproduced = export::reproduce(
        &node.second,
        node.domain.archive(),
        node.domain.state(),
        node.domain.indexes(),
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
        "inspected {} layers, {} compressed bytes; epoch-0's document chained to {}",
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
