//! Publishing a Dolos stele into a repository, twice.
//!
//! Everything here needs a **registry**, and spawns one: `docker run` of an OCI
//! Distribution server, torn down on the way out. So everything here is
//! `#[ignore]`d, and an `#[ignore]`d test that was never executed proves
//! nothing — run it with:
//!
//! ```text
//! cargo test -p dolos-snapshot --features oci --test publish -- --ignored --nocapture
//! ```
//!
//! `STELAE_TEST_REGISTRY_IMAGE` chooses the server (default `registry:2`), the
//! same knob `crates/stelae/tests/oci.rs` uses, so this suite can be pointed at
//! another implementation.
//!
//! ## The four properties
//!
//! 1. **A second publish builds and uploads only what is new.** Both numbers
//!    come from counters the code keeps, not from a duration and not from
//!    asking the registry afterwards — a publish that was merely *fast* would
//!    satisfy neither.
//! 2. **The history chains, and a publish that would break it is refused**
//!    before a single layer is built.
//! 3. **Reuse is faithful.** The same pair of publishes, one inheriting and one
//!    rebuilding from the stores, produce the same inscription digest. This is
//!    the check that inheritance is correct rather than merely cheap, and no
//!    other check here can stand in for it.
//! 4. **A different predecessor is a different digest**, deliberately —
//!    `history` is inside the canonical document, so a repository's identity is
//!    path-dependent.
//!
//! ## Why the two cursors sit where they do
//!
//! The harness ledger lives inside epoch zero, so the two publishes are made by
//! standing at two synthetic chain points and letting `Plan::new` derive
//! everything: one at the last slot of epoch 0, which publishes sequence 1 with
//! epoch 0's window **unclamped**, and one at the first slot of epoch 1, which
//! publishes sequence 2 with an identical epoch-0 window plus an empty epoch 1.
//!
//! That the first cursor has to sit on the boundary is not a convenience. A
//! stele cut mid-epoch clamps its last window to the cursor, so the same epoch
//! published later in full has a *different scope* and is correctly rebuilt
//! rather than inherited. Reuse across a sequence is a property of a publisher
//! that stops on epoch boundaries, which is what ADR-004's pipeline does.

#![cfg(feature = "oci")]

mod node;

use dolos_core::{BlockHash, ChainPoint, Domain as _};
use dolos_snapshot::{
    export::Plan,
    registry::{self, Published},
    DolosProfile, Error, Network, BLOCKS, INDEXES, LOGS, STATE_SHARDS,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use stelae::{oci::Registry, SteleReader as _};

use node::harness;

/// Layers a publish of one epoch writes: three epoch kinds plus the state tip.
const PER_PUBLISH: usize = 3 + STATE_SHARDS as usize;

// ---------------------------------------------------------------------------
// A registry the test spawns
// ---------------------------------------------------------------------------

/// A container running an OCI Distribution server, removed when this is
/// dropped.
///
/// Deliberately a second copy of `Fixture` in `crates/stelae/tests/oci.rs`
/// rather than a shared one. `stelae` must never depend on a `dolos-*` package
/// — that is the boundary ADR-004 sets and `cargo tree` checks — so a fixture
/// both suites could import would have to live somewhere neither of them owns.
/// Two copies of sixty lines is the cheaper of the two prices. Keep them in
/// step; the readiness probe below in particular was arrived at the hard way.
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
    /// the container exists and only then tries to reach the process inside.
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

    fn repository(&self, name: &str) -> Registry {
        registry::open(format!("127.0.0.1:{}", self.port), name.to_owned(), true).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

// ---------------------------------------------------------------------------
// The node, and the two chain points it publishes from
// ---------------------------------------------------------------------------

/// The harness ledger and the two plans it publishes: sequence 1 standing on
/// the epoch-0 boundary, and sequence 2 one slot past it.
struct Node {
    domain: ToyDomain<MemoryStores>,
    first: Plan,
    second: Plan,
}

impl Node {
    fn build() -> Self {
        let domain = harness::<MemoryStores>();

        let summary = dolos_cardano::eras::load_chain_summary_from_state(domain.state()).unwrap();

        let magic = u64::from(domain.genesis().network_magic());
        let network = Network::for_magic(magic);
        let boundary = summary.epoch_start(1);

        // Any hash will do: `position` needs one to exist, and nothing in an
        // export reads it back out of the store.
        let point = |slot| ChainPoint::Specific(slot, BlockHash::new([0xab; 32]));

        let first = Plan::new(&summary, network.clone(), point(boundary - 1)).unwrap();
        let second = Plan::new(&summary, network, point(boundary)).unwrap();

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(
            first.epochs,
            second.epochs[..1],
            "epoch 0's window has to be the same in both, or there is nothing to inherit"
        );

        Self {
            domain,
            first,
            second,
        }
    }

    fn publish(&self, repository: &Registry, plan: &Plan, rebuild: bool) -> Published {
        registry::publish(
            repository,
            plan,
            self.domain.archive(),
            self.domain.state(),
            self.domain.indexes(),
            None,
            rebuild,
        )
        .unwrap()
    }

    fn refuse(&self, repository: &Registry, plan: &Plan) -> Error {
        registry::publish(
            repository,
            plan,
            self.domain.archive(),
            self.domain.state(),
            self.domain.indexes(),
            None,
            false,
        )
        .unwrap_err()
    }
}

// ---------------------------------------------------------------------------
// Done criteria 1 and 2
// ---------------------------------------------------------------------------

/// The whole point of the slice: the second publish pays for one epoch.
#[test]
#[ignore]
fn a_second_publish_builds_and_uploads_only_what_is_new() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/incremental");

    let first = node.publish(&repository, &node.first, false);

    assert!(
        first.inscription.history.is_empty(),
        "the first stele of a repository carries no history"
    );
    assert_eq!(first.layers_built, PER_PUBLISH);
    assert_eq!(first.layers_reused, 0);
    assert_eq!(first.transfer.layers_uploaded, PER_PUBLISH as u64);

    let second = node.publish(&repository, &node.second, false);

    // Epoch 0's three layers are inherited; epoch 1's three and the sixteen
    // state shards are built. The state tip is never inherited — it is the tip,
    // and its scope names no epoch that could distinguish two publishes.
    assert_eq!(
        second.layers_reused, 3,
        "epoch 0's blocks, indexes and logs"
    );
    assert_eq!(
        second.layers_built, PER_PUBLISH,
        "epoch 1's three layers and the sixteen state shards"
    );
    assert_eq!(second.inscription.layers.len(), PER_PUBLISH + 3);

    // Uploaded, not merely "not rebuilt": an inherited layer moves no bytes at
    // all, and every built one here is genuinely new to the registry because a
    // state shard's header record names its epoch.
    assert_eq!(second.transfer.layers_uploaded, PER_PUBLISH as u64);
    assert_eq!(second.transfer.layers_reused, 3);
    assert_eq!(second.transfer.layers_skipped, 0);

    eprintln!(
        "publish 1: built {}, reused {}, uploaded {} ({} bytes)\n\
         publish 2: built {}, reused {} ({} bytes not moved), uploaded {} ({} bytes)",
        first.layers_built,
        first.layers_reused,
        first.transfer.layers_uploaded,
        first.transfer.bytes_uploaded,
        second.layers_built,
        second.layers_reused,
        second.transfer.bytes_reused,
        second.transfer.layers_uploaded,
        second.transfer.bytes_uploaded,
    );

    // Done criterion 2: the chain, and that it validates.
    assert_eq!(second.inscription.history.len(), 1);
    assert_eq!(second.inscription.history[0].sequence, 1);
    assert_eq!(
        second.inscription.history[0].inscription_digest,
        first.identity
    );
    second.inscription.validate().unwrap();

    // And the inherited descriptors are the predecessor's, byte for byte.
    for kind in [BLOCKS, INDEXES, LOGS] {
        let before = layer(&first.inscription, kind, 0);
        let after = layer(&second.inscription, kind, 0);

        assert_eq!(before, after, "{kind}: epoch 0's layer was inherited whole");
    }

    // The moving tag resolves, and what comes back is what was published.
    let latest = repository.pull_latest(&DolosProfile).unwrap();

    assert_eq!(
        latest.read_inscription().unwrap().digest().unwrap(),
        second.identity
    );
}

/// A dry run says what the publish then does.
///
/// The number a publisher checks before committing hours is worth nothing if it
/// is computed by a second reading of the same rules, so this holds the two
/// against each other rather than against a literal.
#[test]
#[ignore]
fn a_dry_run_agrees_with_the_publish_that_follows_it() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/dry-run");

    // Against an empty repository: nothing to follow, nothing to inherit.
    let empty = registry::preview(&repository, &node.first, false).unwrap();

    assert_eq!(empty.predecessor, None);
    assert_eq!(empty.history, 0);
    assert_eq!(empty.layers_reused, 0);
    assert_eq!(empty.layers_built, PER_PUBLISH);

    let first = node.publish(&repository, &node.first, false);

    assert_eq!(first.layers_built, empty.layers_built);
    assert_eq!(first.layers_reused, empty.layers_reused);

    // And against the stele that is now there.
    let next = registry::preview(&repository, &node.second, false).unwrap();

    assert_eq!(next.predecessor, Some((1, first.identity)));
    assert_eq!(next.history, 1);

    // `--rebuild` is visible in the preview, not only in the publish. Asked
    // here, while sequence 2 is still unpublished: a preview reads the chain
    // through the same rule a publish does, so asking after the fact would be
    // refused as a republish.
    let rebuilding = registry::preview(&repository, &node.second, true).unwrap();

    assert_eq!(rebuilding.layers_reused, 0);
    assert_eq!(rebuilding.layers_built, PER_PUBLISH + 3);
    assert_eq!(rebuilding.history, 1, "a rebuild still chains");

    let second = node.publish(&repository, &node.second, false);

    assert_eq!(
        (next.layers_built, next.layers_reused),
        (second.layers_built, second.layers_reused),
        "the dry run and the publish read the same rules once, not twice"
    );

    assert_eq!(
        next.history,
        second.inscription.history.len(),
        "and agree about the chain they extend"
    );

    // A preview reaches the chain refusal too, which is what makes `--dry-run`
    // the cheap way to find out that a publisher has skipped an epoch.
    let refused = registry::preview(&repository, &node.second, false).unwrap_err();

    assert!(matches!(refused, Error::HistoryBreak { .. }), "{refused:?}");
}

/// Done criterion 3. The check that inheritance is *faithful* rather than fast.
#[test]
#[ignore]
fn reuse_and_a_forced_rebuild_agree_on_the_digest() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let inherited = fixture.repository("dolos/inherited");
    node.publish(&inherited, &node.first, false);
    let inherited = node.publish(&inherited, &node.second, false);

    let rebuilt = fixture.repository("dolos/rebuilt");
    node.publish(&rebuilt, &node.first, false);
    let rebuilt = node.publish(&rebuilt, &node.second, true);

    assert_eq!(inherited.layers_reused, 3);
    assert_eq!(rebuilt.layers_reused, 0, "--rebuild inherits nothing");
    assert_eq!(
        rebuilt.layers_built,
        PER_PUBLISH + 3,
        "and builds everything"
    );

    // A rebuild still chains: reproducing what you published is not the same
    // act as forgetting that you published it.
    assert_eq!(rebuilt.inscription.history, inherited.inscription.history);

    assert_eq!(
        inherited.identity, rebuilt.identity,
        "a layer carried forward is the layer this node would have built"
    );

    eprintln!(
        "inherited {} == rebuilt {}",
        inherited.identity, rebuilt.identity
    );
}

/// Done criterion 3's other half, and the determinism consequence the history
/// field brings with it: two repositories that diverged once never agree again.
#[test]
#[ignore]
fn a_different_predecessor_is_a_different_digest() {
    let fixture = Fixture::spawn();
    let node = Node::build();

    let chained = fixture.repository("dolos/chained");
    node.publish(&chained, &node.first, false);
    let chained = node.publish(&chained, &node.second, false);

    // Sequence 2 into an empty repository is legal — the protocol permits an
    // empty history at any sequence — and it is the same stores, the same plan
    // and the same layers.
    let fresh = fixture.repository("dolos/fresh");
    let fresh = node.publish(&fresh, &node.second, false);

    assert!(fresh.inscription.history.is_empty());
    assert_eq!(fresh.layers_reused, 0);

    assert_eq!(
        fresh.inscription.layers, chained.inscription.layers,
        "the layers are identical; `history` is the only field that differs"
    );

    assert_ne!(
        fresh.identity, chained.identity,
        "a stele's identity depends on the chain it extends, deliberately"
    );
}

/// Done criterion 2's refusal, in all three shapes.
#[test]
#[ignore]
fn a_publish_that_does_not_follow_latest_is_refused() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/broken-chain");

    node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    // A republish of the sequence already there.
    expect_break(node.refuse(&repository, &node.second), 2, 2);

    // And one behind it.
    expect_break(node.refuse(&repository, &node.first), 2, 1);

    // A gap: the repository is at 2 and this stele is sequence 4.
    let mut skipped = node.second.clone();
    skipped.sequence = 4;
    expect_break(node.refuse(&repository, &skipped), 2, 4);

    // Every refusal happened before anything was written, so the repository is
    // exactly where it was.
    let latest = repository.pull_latest(&DolosProfile).unwrap();

    assert_eq!(
        latest.read_inscription().unwrap().digest().unwrap(),
        second.identity,
        "a refused publish moved nothing"
    );
}

fn expect_break(error: Error, latest: u64, publishing: u64) {
    let message = error.to_string();

    assert!(
        matches!(error, Error::HistoryBreak { .. }),
        "publishing {publishing}: {error:?}"
    );

    assert!(
        message.contains(&latest.to_string()) && message.contains(&publishing.to_string()),
        "both sequences belong in the message: {message}"
    );
}

/// One epoch layer of a given kind, by the epoch its scope names.
fn layer<'a>(
    inscription: &'a stelae::inscription::Inscription,
    kind: &str,
    epoch: u64,
) -> &'a stelae::inscription::LayerDescriptor {
    inscription
        .layers
        .iter()
        .find(|layer| layer.kind == kind && layer.scope["epoch"] == epoch)
        .unwrap_or_else(|| panic!("no {kind} layer for epoch {epoch}"))
}
