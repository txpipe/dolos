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
//! 1. **A freshly published stele verifies clean**, through its chain: the
//!    manifests and the inscriptions agree, and every blob of the layer set
//!    streams back to both of its digests.
//! 2. **Each of the three tampers is refused with the offence named**: a blob
//!    that is not the layer it is addressed as, a manifest `diffId` annotation
//!    disagreeing with the inscription, and a link naming a document that is
//!    not the one at its sequence. The tampered artifacts are planted through
//!    the raw distribution API, because the transport refuses to write any of
//!    them — which is the point.
//! 3. **A reproduction from the publisher's own stores matches**, and a store
//!    standing at a different epoch is refused before a single layer is
//!    rebuilt.
//! 4. **An inspection reports what the manifest carries**, and its canonical
//!    JSON is exactly what `digest --chain-from` takes.
//! 5. **A resumed publish reproduces**: a resumption record carries the layers
//!    its sequence lists in `diff`, and the tip is built again.
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
    DolosProfile, Error,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use serde_json::json;
use stelae::{inscription::Link, Digest, SnapshotReader as _};

use node::{plan_at_boundary, Node};
use registry_fixture::{Distribution, Fixture, Unanswered};

/// The link a publish at `sequence + 1` carries to `published`.
fn link_to(published: &registry::Published) -> Option<Link> {
    Some(Link {
        sequence: published.inscription.sequence,
        inscription_digest: published.identity,
    })
}

// ---------------------------------------------------------------------------
// Done criterion 3: verify --repo
// ---------------------------------------------------------------------------

/// The pass, before the refusals mean anything.
///
/// The stele verified at `latest` is the interesting one: it lists only its
/// own layers and links to its predecessor, so a clean verification streams
/// and checks the layers of both inscriptions.
#[test]
#[ignore = "spawns a registry"]
fn a_freshly_published_stele_verifies_clean() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository_name = "dolos/verify-clean";
    let repository = fixture.repository(repository_name);

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(second.inscription.previous, link_to(&first));
    assert_eq!(second.layers_reused, 0);

    let reader = fixture.snapshot_repository(repository_name);
    let verified = reader.verify(Point::Latest).unwrap();

    assert_eq!(verified.identity, second.identity);
    assert_eq!(verified.inscription, second.inscription);
    assert_eq!(
        verified.chain.layers().len(),
        first.inscription.diff.len() + second.inscription.layers().count()
    );
    assert!(verified.compressed_bytes > 0);

    // The immutable tag reads the predecessor back just as clean.
    let predecessor = reader.verify(Point::Epoch(1)).unwrap();

    assert_eq!(predecessor.identity, first.identity);

    eprintln!(
        "verified latest = {} ({} layers, {} compressed bytes) and epoch-1 = {}",
        verified.identity,
        verified.chain.layers().len(),
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

/// A link naming a document other than the one at its sequence, refused
/// before a single blob is fetched.
///
/// The tampered head is planted as a rewritten config blob under both of
/// `latest`'s manifests — the protocol refuses to *produce* one, so the only
/// way it exists is a registry serving a document this code never wrote. The
/// walk refuses it at the link, naming both sequences.
#[test]
#[ignore = "spawns a registry"]
fn a_link_to_another_document_is_refused() {
    let fixture = Fixture::spawn();
    let distribution = Distribution::new(&fixture);
    let node = Node::build();

    let name = "dolos/verify-link";
    let repository = fixture.repository(name);

    node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    let mut relinked = serde_json::to_value(&second.inscription).unwrap();
    relinked["previous"]["inscriptionDigest"] = json!(Digest::compute([0xee]).to_string());

    let config = stelae::inscription::canonical_json(&relinked).unwrap();
    let planted = distribution.put_blob(name, &config);

    for tag in ["latest", "epoch-2.tip"] {
        let mut manifest = distribution.manifest(name, tag);
        manifest["config"]["digest"] = json!(planted);
        manifest["config"]["size"] = json!(config.len());
        distribution.put_manifest(name, tag, &manifest);
    }

    let err = registry::verify(&repository, Point::Latest).unwrap_err();
    let message = err.to_string();

    assert!(
        matches!(
            &err,
            Error::Stelae(stelae::Error::DigestMismatch { subject, .. })
                if subject.contains("sequence 1, linked from sequence 2")
        ),
        "{err:?}"
    );

    eprintln!("relinked head: {message}");
}

// ---------------------------------------------------------------------------
// Done criterion 4: verify --repo --reproduce
// ---------------------------------------------------------------------------

/// The reproduction passes against the store the stele was published from,
/// and a store standing at a different epoch is refused before the walk.
///
/// The published stele lists its own layers and its link, and the pass here
/// is every one of them coming back out of the stores byte-identical, under
/// the link it carries. The refusal is the same ledger one epoch earlier —
/// which is what "a store at a different epoch" is — and it costs a
/// comparison of two sequences, not hours of compression.
#[test]
#[ignore = "spawns a registry"]
fn a_reproduction_passes_at_the_published_epoch_and_fails_at_another() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/verify-reproduce");

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(second.inscription.previous, link_to(&first));

    let verified = registry::verify(&repository, Point::Latest).unwrap();

    let reproduced = export::verify_reproduction(
        &verified.inscription,
        &node.second,
        node.domain.archive(),
        node.domain.state(),
    )
    .unwrap();

    assert_eq!(reproduced.digest().unwrap(), verified.identity);

    let err = export::verify_reproduction(
        &verified.inscription,
        &node.first,
        node.earlier.archive(),
        node.earlier.state(),
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
    );
    assert_eq!(refused(preview.unwrap_err()), expected);

    let publish = registry::publish(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
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
    );
    assert_eq!(refused(preview.unwrap_err()), expected);

    let publish = registry::publish(
        Publishing::new(&repository),
        &plan,
        past.archive(),
        past.state(),
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

/// A publish that dies at the seal leaves a record of the layers its sequence
/// lists in `diff`, and none of its tip. The publish that resumes from it
/// carries every recorded layer, builds the tip again, and seals the stele the
/// stores reproduce; the dry run counts every layer the stele lists.
#[test]
#[ignore = "spawns a registry"]
fn a_resumed_registry_publish_carries_its_diff_and_builds_its_tip() {
    let fixture = Fixture::spawn();
    // One handle per process, as the hosts would hold them.
    let process = || fixture.repository("dolos/resume-diff");
    let record = tempfile::tempdir().unwrap();
    let record_path = registry::record_path_in(record.path());
    let silent = stelae::progress::Observer::silent();

    let host: ToyDomain<MemoryStores> = node::harness();

    let cut_at_1 = plan_at_boundary(&host, 1);
    let first = registry::publish(
        Publishing::new(&process()),
        &cut_at_1,
        host.archive(),
        host.state(),
        &silent,
    )
    .unwrap();

    let cut_at_2 = plan_at_boundary(&host, 2);
    let dying = process();
    let interrupted = registry::publish_into(
        &SealFails(&dying),
        Publishing::new(&dying).recording_in(&record_path),
        &cut_at_2,
        host.archive(),
        host.state(),
        &silent,
    );
    assert!(interrupted.is_err(), "the seal was meant to fail");

    let expected = export::reproduce(
        &cut_at_2,
        host.archive(),
        host.state(),
        &Following::new(&first.inscription, &cut_at_2).unwrap(),
    )
    .unwrap();
    assert!(!expected.diff.is_empty() && !expected.tip.is_empty());

    // The record was written through the wrapper the registry publishes with.
    let recorded = PublishRecord::load(&record_path).unwrap().unwrap();
    let recorded_layers: Vec<_> = recorded
        .layers
        .iter()
        .map(|layer| layer.descriptor.clone())
        .collect();

    assert_eq!(
        recorded_layers.len(),
        expected.diff.len(),
        "the record holds this sequence's diff"
    );
    assert!(recorded_layers
        .iter()
        .all(|layer| expected.diff.contains(layer)));

    // The failed seal never joined the uploads, and a resume carries a
    // recorded layer only once its blob is up, so wait for them.
    let probe = process();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    for layer in &recorded.layers {
        while !probe.adopt_carried(layer.clone()).unwrap() {
            assert!(
                std::time::Instant::now() < deadline,
                "a recorded layer never finished uploading"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let resumed = process();
    let resuming = || Publishing::new(&resumed).recording_in(&record_path);
    let preview = registry::preview(resuming(), &cut_at_2, host.archive(), host.state()).unwrap();
    let published =
        registry::publish(resuming(), &cut_at_2, host.archive(), host.state(), &silent).unwrap();

    assert_eq!(published.identity, expected.digest().unwrap());
    assert_eq!(published.layers_reused, expected.diff.len());
    assert_eq!(published.layers_built, expected.tip.len());
    assert_eq!(preview.layers_built, expected.layers().count());
    assert!(PublishRecord::load(&record_path).unwrap().is_none());

    let reproduced = export::verify_reproduction(
        &published.inscription,
        &cut_at_2,
        host.archive(),
        host.state(),
    )
    .unwrap();
    assert_eq!(reproduced.digest().unwrap(), published.identity);
}

/// A batch prepared and staged once, then published in sequence, is the chain
/// the direct publishes write: same identities, and each chained onto the
/// stele its predecessor left.
#[test]
#[ignore = "spawns a registry"]
fn a_prepared_batch_publishes_what_direct_publishes_do() {
    use dolos_snapshot::{
        prepare::{self, Finalizing, IndexTail, Request},
        staging::Budget,
    };

    let fixture = Fixture::spawn();
    let direct = fixture.repository("dolos/direct");
    let batched = fixture.repository("dolos/batched");
    let node = Node::build();

    let scratch = tempfile::tempdir().unwrap();
    let store = prepare::Store::open(scratch.path().join("bundles"), "test").unwrap();
    let silent = stelae::progress::Observer::silent();

    let bundles: Vec<_> = [&node.first, &node.second]
        .into_iter()
        .map(|plan| {
            let stores = node.stores(plan);

            prepare::prepare(
                &store,
                plan,
                stores.archive(),
                stores.state(),
                &Request {
                    store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
                    mutable_slots: dolos_cardano::mutable_slots(stores.genesis().as_ref()),
                    tail: IndexTail::Traverse,
                },
                &silent,
            )
            .unwrap()
        })
        .collect();

    let staged = prepare::stage_batch(
        &scratch.path().join("runs"),
        node.domain.archive(),
        node.domain.state(),
        &bundles,
        &export::First,
        Budget::new(u64::MAX),
        &silent,
    )
    .unwrap();

    for (plan, bundle) in [&node.first, &node.second].into_iter().zip(&bundles) {
        let expected = node.publish(&direct, plan, false);

        let published = registry::publish_prepared(
            Publishing::new(&batched),
            &Finalizing {
                store: &store,
                bundle,
                staged: &staged,
                producers: export::Producers::DEFAULT,
            },
            node.domain.archive(),
            &silent,
        )
        .unwrap();

        assert_eq!(published.identity, expected.identity);
        assert_eq!(published.layers_reused, expected.layers_reused);
    }

    assert_eq!(
        batched
            .latest(&DolosProfile)
            .unwrap()
            .unwrap()
            .read_inscription()
            .unwrap()
            .sequence,
        node.second.sequence,
    );
}

/// The registry, except that the seal fails: every layer is up and recorded,
/// and the stele never becomes readable.
struct SealFails<'a>(&'a stelae::oci::Registry);

impl stelae::SnapshotWriter for SealFails<'_> {
    type Sink = <stelae::oci::Registry as stelae::SnapshotWriter>::Sink;

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
}

// ---------------------------------------------------------------------------
// Done criterion 5: inspect --repo
// ---------------------------------------------------------------------------

/// An inspection lists every layer of the chain with the compressed size its
/// manifest carries, and its canonical JSON chains a digest.
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
    assert_eq!(inspected.inscription, second.inscription);
    assert_eq!(inspected.compressed.len(), inspected.chain.layers().len());
    assert!(inspected.chain.layers().len() > second.inscription.layers().count());

    // Every layer carries a real compressed size — the predecessor's
    // included, whose bytes this publish never moved — and they sum to the
    // manifests' own total.
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

    assert_eq!(following.previous(), link_to(&first));

    let reproduced = export::reproduce(
        &node.second,
        node.domain.archive(),
        node.domain.state(),
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
        inspected.chain.layers().len(),
        inspected.total_compressed,
        reproduced.digest().unwrap(),
    );
}
