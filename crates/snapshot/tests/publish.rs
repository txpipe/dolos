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
//! ## The properties
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
//! 5. **A stele published with reuse on can be reproduced from stores alone.**
//!    The trust gap the incremental publish opened: layers inherited rather
//!    than rebuilt are attested without being reproduced, and until something
//!    reproduces them nothing but the publisher has ever checked the
//!    attestation. `export::reproduce` is that something.
//! 6. **A repository already at this node's sequence is not a fault.** The
//!    detection a publisher on a timer needs, read off the same moving tag a
//!    publish reads.
//! 7. **A restarted publish never rebuilds an epoch layer it already uploaded**
//!    — and produces the manifest an uninterrupted publish would have, byte for
//!    byte. The interruption is real: a transport that dies at a layer the test
//!    named, leaving blobs up that no manifest references.
//!
//! ## Why the two cursors sit where they do
//!
//! The harness ledger lives inside epoch zero, so the two publishes are made by
//! standing at two synthetic chain points and letting `Plan::new` derive
//! everything: one at the last slot of epoch 0, which publishes sequence 0 with
//! epoch 0's window **unclamped**, and one at the first slot of epoch 1, which
//! publishes sequence 1 with an identical epoch-0 window plus an empty epoch 1.
//!
//! That the first cursor has to sit on the boundary is not a convenience. A
//! stele cut mid-epoch clamps its last window to the cursor, so the same epoch
//! published later in full has a *different scope* and is correctly rebuilt
//! rather than inherited. Reuse across a sequence is a property of a publisher
//! that stops on epoch boundaries, which is what ADR-004's pipeline does.

#![cfg(feature = "oci")]

mod node;
mod registry_fixture;
mod watcher;

use dolos_core::Domain as _;
use dolos_snapshot::{
    export::{self, Following, Predecessor as _, Standing},
    is_state_kind,
    registry::{self, Publishing},
    state_layer_count, DolosProfile, Error, RetainedEpochs, BLOCKS, INDEXES, STATE_KINDS,
};
use stelae::{progress::Outcome, SteleReader as _};

use watcher::Watcher;

use node::{plan_at_boundary, plan_at_epoch_end, Node};
use registry_fixture::Fixture;

/// The log kinds epoch 0 carries.
///
/// The harness seeds `epochs` and `account-epochs` and nothing else, so those
/// are the two log layers epoch 0 publishes — the third log kind has no
/// records in the window and therefore no layer at all. Epoch 1 is the boundary
/// sliver, whose logs key at `epoch_start(0)`, so it carries none.
const EPOCH_0_LOGS: [&str; 2] = ["log-account-epochs", "log-epochs"];

/// Layers epoch 0 contributes: `blocks`, `indexes` and its two log layers.
const EPOCH_0: usize = 2 + EPOCH_0_LOGS.len();

/// Layers epoch 1 contributes: `blocks` and `indexes`, and no logs.
const EPOCH_1: usize = 2;

/// Layers a publish of sequence 0 writes: epoch 0 plus the state tip.
const PER_PUBLISH: usize = EPOCH_0 + state_layer_count();

/// Layers a publish of sequence 1 carries: both epochs plus the state tip.
const WHOLE_SECOND: usize = EPOCH_0 + EPOCH_1 + state_layer_count();

/// Layers epoch 2 contributes, on the same reading as epoch 1: it is a
/// boundary sliver of one slot with no blocks and no logs in it, so `blocks`
/// and `indexes` and nothing else.
const EPOCH_2: usize = 2;

/// The retained list the dump suites publish under.
///
/// Epoch 1 rather than epoch 0, because epoch 0 is the one epoch a list may
/// not name — and rather than epoch 2, because the point of the pair below is
/// a dump that is *cut* by one publish and *inherited* by the next.
fn retaining_epoch_1() -> RetainedEpochs {
    RetainedEpochs::new(vec![1]).unwrap()
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

    // Epoch 0's four layers are inherited; epoch 1's two and the sixteen state
    // shards are built. The state tip is never inherited — it is the tip, and
    // its scope names no epoch that could distinguish two publishes.
    assert_eq!(
        second.layers_reused, EPOCH_0,
        "epoch 0's blocks, indexes and its two log layers"
    );
    assert_eq!(
        second.layers_built,
        EPOCH_1 + state_layer_count(),
        "epoch 1's blocks and indexes, and the sixteen state shards"
    );
    assert_eq!(second.inscription.layers.len(), WHOLE_SECOND);

    // Uploaded, not merely "not rebuilt": an inherited layer moves no bytes at
    // all, and every built one here is genuinely new to the registry because a
    // state shard's header record names its epoch.
    assert_eq!(
        second.transfer.layers_uploaded,
        (EPOCH_1 + state_layer_count()) as u64
    );
    assert_eq!(second.transfer.layers_reused, EPOCH_0 as u64);
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
    assert_eq!(second.inscription.history[0].sequence, 0);
    assert_eq!(
        second.inscription.history[0].inscription_digest,
        first.identity
    );
    second.inscription.validate().unwrap();

    // And the inherited descriptors are the predecessor's, byte for byte.
    for kind in [BLOCKS, INDEXES].into_iter().chain(EPOCH_0_LOGS) {
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

/// Done criterion 1: a retained dump is cut once and inherited thereafter, and
/// the publish that cuts it moves its bytes once.
///
/// Two publishes, sequence 1 then sequence 2, both configured to retain epoch
/// 1. The first stands *in* epoch 1, so its dump is its own tip under a second
/// scope; the second stands past it, so the dump can only come from the
/// manifest — and if scope equality did not identify it, this is where it
/// would be silently rebuilt out of a store that no longer holds epoch 1's
/// state.
#[test]
#[ignore]
fn a_retained_dump_is_cut_once_and_inherited_after() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/dumps");

    let cutting = plan_at_epoch_end(&node.domain, 1, retaining_epoch_1());
    let following = plan_at_boundary(&node.domain, 2, retaining_epoch_1());

    let cut = node.publish(&repository, &cutting, false);

    assert_eq!(
        cut.inscription.layers.len(),
        EPOCH_0 + EPOCH_1 + 2 * state_layer_count(),
        "both epochs, the tip, and the dump the tip was teed into",
    );

    // The dump is not "reused": nothing was carried forward into this publish.
    // It was produced here, by the walk that produced the tip.
    assert_eq!(cut.layers_reused, 0);
    assert_eq!(
        cut.layers_built,
        EPOCH_0 + EPOCH_1 + 2 * state_layer_count()
    );

    // **The property.** Every dump descriptor names the same bytes as the tip
    // shard it was cut from, and those bytes crossed the wire once: the tip's
    // upload, plus a skip for the second descriptor.
    let dumps = dumps_of(&cut.inscription, 1);
    assert_eq!(dumps.len(), state_layer_count());

    for dump in &dumps {
        let shard = dump.scope["shard"].as_u64().unwrap();
        let tip = tip_of(&cut.inscription, &dump.kind, shard);

        assert_eq!(dump.diff_id, tip.diff_id, "{}", dump.kind);
        assert_eq!(dump.records, tip.records, "{}", dump.kind);
        assert_eq!(
            dump.uncompressed_size, tip.uncompressed_size,
            "{}",
            dump.kind
        );
    }

    assert_eq!(
        cut.transfer.layers_uploaded,
        (EPOCH_0 + EPOCH_1 + state_layer_count()) as u64,
        "a dump cut from the tip uploaded a blob of its own",
    );
    assert_eq!(
        cut.transfer.layers_skipped,
        state_layer_count() as u64,
        "and every one of them is the tip's blob, already up",
    );
    assert_eq!(cut.transfer.layers_reused, 0);

    let followed = node.publish(&repository, &following, false);

    assert_eq!(
        followed.layers_reused,
        EPOCH_0 + EPOCH_1 + state_layer_count(),
        "both closed epochs off the manifest, and epoch 1's whole dump with them",
    );
    assert_eq!(
        followed.layers_built,
        EPOCH_2 + state_layer_count(),
        "epoch 2's two layers and a fresh tip, and nothing else",
    );
    assert_eq!(
        followed.inscription.layers.len(),
        EPOCH_0 + EPOCH_1 + EPOCH_2 + 2 * state_layer_count(),
        "three epochs, the inherited dump, and this sequence's tip",
    );

    // Only new blobs moved. The dump's did not: it is epoch 1's state, and
    // epoch 1 closed before this publish began.
    assert_eq!(
        followed.transfer.layers_uploaded,
        (EPOCH_2 + state_layer_count()) as u64
    );
    assert_eq!(
        followed.transfer.layers_reused,
        (EPOCH_0 + EPOCH_1 + state_layer_count()) as u64
    );
    assert_eq!(followed.transfer.layers_skipped, 0);

    // Inherited whole: the descriptors are the predecessor's, byte for byte.
    assert_eq!(dumps_of(&followed.inscription, 1), dumps);

    // And the dump and the tip have parted company, which is the whole of what
    // a dump is for. What separates them *here* is the header: a state layer's
    // header record carries its scope's epoch, and a tip's epoch is the stele's
    // sequence — 1 where the dump was cut, 2 now — so the header bytes differ
    // and the `diffId` over them differs too. The records behind it are
    // identical, because this harness's state store does not move between the
    // two publishes. On a real node they would differ as well; the assertion
    // does not depend on that, and should not be read as proving it.
    for dump in &dumps {
        let shard = dump.scope["shard"].as_u64().unwrap();
        let tip = tip_of(&followed.inscription, &dump.kind, shard);

        assert_ne!(
            dump.diff_id, tip.diff_id,
            "{} shard {shard}: the dump is still the tip one sequence on",
            dump.kind,
        );
    }

    followed.inscription.validate().unwrap();

    eprintln!(
        "cut:       built {}, uploaded {} ({} bytes), skipped {}\n\
         inherited: built {}, reused {} ({} bytes not moved), uploaded {}",
        cut.layers_built,
        cut.transfer.layers_uploaded,
        cut.transfer.bytes_uploaded,
        cut.transfer.layers_skipped,
        followed.layers_built,
        followed.layers_reused,
        followed.transfer.bytes_reused,
        followed.transfer.layers_uploaded,
    );
}

/// The retained dumps a stele carries for `epoch`, in inscription order.
fn dumps_of(
    inscription: &stelae::inscription::Inscription,
    epoch: u64,
) -> Vec<stelae::inscription::LayerDescriptor> {
    inscription
        .layers
        .iter()
        .filter(|layer| {
            is_state_kind(&layer.kind)
                && layer.scope.get("epoch").and_then(serde_json::Value::as_u64) == Some(epoch)
        })
        .cloned()
        .collect()
}

/// One tip shard, by the kind and shard a dump names.
fn tip_of<'a>(
    inscription: &'a stelae::inscription::Inscription,
    kind: &str,
    shard: u64,
) -> &'a stelae::inscription::LayerDescriptor {
    inscription
        .layers
        .iter()
        .find(|layer| layer.kind == kind && layer.scope == serde_json::json!({"shard": shard}))
        .unwrap_or_else(|| panic!("no tip layer for {kind} shard {shard}"))
}

/// The risk decision 0026 named: a dump is the first **immutable state layer**,
/// and a resume path that reads "state" as "always rebuild" would drop it.
///
/// The publish is killed inside its state pass — at the first tip shard, which
/// is the first thing opened after that kind's dump has been adopted and
/// recorded. So the record it leaves contains a state layer, which no record
/// could contain before, and the resume has to honour it on exactly an epoch
/// layer's terms.
#[test]
#[ignore]
fn a_restarted_publish_carries_forward_the_retained_dump_it_adopted() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let storage = tempfile::tempdir().unwrap();

    let cutting = plan_at_epoch_end(&node.domain, 1, retaining_epoch_1());
    let following = plan_at_boundary(&node.domain, 2, retaining_epoch_1());

    let first = fixture.repository("dolos/dump-resume");
    node.publish_as(publishing(&first, &storage), &cutting)
        .unwrap();

    // The first state kind in `STATE_KINDS` order: its dump is adopted, then
    // its tip is opened, and that open is where this dies.
    let (first_kind, _, _) = STATE_KINDS[0];

    let dying = fixture.repository("dolos/dump-resume");

    let killed = node
        .publish_through(
            &Interrupted {
                inner: &dying,
                kind: first_kind,
                epoch: None,
            },
            publishing(&dying, &storage),
            &following,
        )
        .unwrap_err();

    assert!(
        matches!(killed, Error::Stelae(stelae::Error::Io(_))),
        "{killed:?}"
    );

    let record = registry::PublishRecord::load(&record_path(&storage))
        .unwrap()
        .expect("an interrupted publish left no record");

    // Every state layer in the record is a dump, and the dump that got in is
    // the one whose kind the interruption stopped at.
    let recorded_state: Vec<_> = record
        .layers
        .iter()
        .filter(|layer| is_state_kind(&layer.descriptor.kind))
        .collect();

    assert!(
        !recorded_state.is_empty(),
        "no state layer reached the record; the interruption did not land in the state pass",
    );

    for layer in &recorded_state {
        assert_eq!(layer.descriptor.kind, first_kind);
        assert_eq!(
            layer
                .descriptor
                .scope
                .get("epoch")
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "a tip shard was recorded: {}",
            layer.descriptor.scope,
        );
    }

    // The restart, against the same repository and the same storage.
    let resumed_into = fixture.repository("dolos/dump-resume");

    let resumed = node
        .publish_as(publishing(&resumed_into, &storage), &following)
        .unwrap();

    // The stele is the one an uninterrupted publish would have produced, down
    // to the manifest bytes — the only statement strong enough to say the
    // half-adopted dump was neither dropped nor duplicated. Counters are held
    // against that same run rather than against literals, so this test says
    // "the interruption changed nothing" rather than re-deriving the
    // arithmetic the test above already pins.
    let clean = fixture.repository("dolos/dump-uninterrupted");

    node.publish(&clean, &cutting, false);
    let uninterrupted = node.publish(&clean, &following, false);

    assert_eq!(resumed.identity, uninterrupted.identity);
    assert_eq!(resumed.inscription, uninterrupted.inscription);
    assert_eq!(manifest_of(&resumed_into), manifest_of(&clean));

    // The sharpest statement about the dump: the resume rebuilt the tip and
    // *nothing else*. A resume that had dropped the half-adopted dump would
    // have had to build one, and one that had lost track of it would have
    // built seventy-seven.
    assert_eq!(resumed.layers_built, state_layer_count());

    // The two runs agree on the whole and differ where they must: a resumed
    // publish carries forward from the record as well as from the manifest, so
    // it reuses strictly more than an uninterrupted one — the layers the
    // killed attempt had already uploaded.
    assert_eq!(
        resumed.layers_built + resumed.layers_reused,
        uninterrupted.layers_built + uninterrupted.layers_reused,
    );
    assert!(
        resumed.layers_reused > uninterrupted.layers_reused,
        "the record bought nothing: {} reused against {}",
        resumed.layers_reused,
        uninterrupted.layers_reused,
    );

    assert_eq!(
        registry::PublishRecord::load(&record_path(&storage)).unwrap(),
        None,
        "a resumed publish that sealed left its record behind",
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
    let empty = registry::preview(
        Publishing::new(&repository),
        &node.first,
        node.domain.archive(),
        None,
    )
    .unwrap();

    assert_eq!(empty.predecessor, None);
    assert_eq!(empty.history, 0);
    assert_eq!(empty.layers_reused, 0);
    assert_eq!(empty.layers_built, PER_PUBLISH);

    let first = node.publish(&repository, &node.first, false);

    assert_eq!(first.layers_built, empty.layers_built);
    assert_eq!(first.layers_reused, empty.layers_reused);

    // And against the stele that is now there.
    let next = registry::preview(
        Publishing::new(&repository),
        &node.second,
        node.domain.archive(),
        None,
    )
    .unwrap();

    assert_eq!(next.predecessor, Some((0, first.identity)));
    assert_eq!(next.history, 1);

    // `--rebuild` is visible in the preview, not only in the publish. Asked
    // here, while sequence 1 is still unpublished: a preview reads the chain
    // through the same rule a publish does, so asking after the fact would be
    // refused as a republish.
    let rebuilding = registry::preview(
        Publishing::new(&repository).rebuilding(true),
        &node.second,
        node.domain.archive(),
        None,
    )
    .unwrap();

    assert_eq!(rebuilding.layers_reused, 0);
    assert_eq!(rebuilding.layers_built, WHOLE_SECOND);
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
    let refused = registry::preview(
        Publishing::new(&repository),
        &node.second,
        node.domain.archive(),
        None,
    )
    .unwrap_err();

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

    assert_eq!(inherited.layers_reused, EPOCH_0);
    assert_eq!(rebuilt.layers_reused, 0, "--rebuild inherits nothing");
    assert_eq!(rebuilt.layers_built, WHOLE_SECOND, "and builds everything");

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
    expect_break(node.refuse(&repository, &node.second), 1, 1);

    // And one behind it.
    expect_break(node.refuse(&repository, &node.first), 1, 0);

    // A gap: the repository is at 1 and this stele is sequence 4.
    let mut skipped = node.second.clone();
    skipped.sequence = 4;
    expect_break(node.refuse(&repository, &skipped), 1, 4);

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

// ---------------------------------------------------------------------------
// The trust gap, closed
// ---------------------------------------------------------------------------

/// A stele published **with reuse on** is reproduced from the stores alone.
///
/// This is the check the incremental publish deferred. `a_second_publish` shows
/// that the second stele inherits epoch 0's four layers and never opens the
/// store for them; `reuse_and_a_forced_rebuild_agree_on_the_digest` shows the
/// same publisher rebuilding them and agreeing. Neither is an *independent*
/// reproduction: both go through the registry publisher, and one of them is
/// the very code whose inheritance is in question.
///
/// Here the reproduction touches no registry at all. The discarding writer
/// walks the stores, builds every layer including the ones the publish
/// inherited, chains onto the predecessor's inscription through the same
/// `history_for` the publish used — and has to arrive at the digest that is in
/// the repository.
///
/// The predecessor is an input rather than something the reproduction works
/// out, and it has to be: `history` is inside the canonical document, so a
/// verifier that guessed the chain would compute a digest that is correct for a
/// stele nobody published. That is the residual independence gap, and it is
/// what a signature closes rather than this.
#[test]
#[ignore]
fn a_stele_published_with_reuse_is_reproduced_from_the_stores() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/reproduced");

    let first = node.publish(&repository, &node.first, false);
    let second = node.publish(&repository, &node.second, false);

    assert_eq!(
        second.layers_reused, EPOCH_0,
        "there is nothing to reproduce unless the publish inherited something"
    );

    // What a verifier is handed: the predecessor's canonical bytes, exactly as
    // they sit in the repository's config blob.
    let canonical = first.inscription.canonicalize().unwrap();
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
        reproduced.canonicalize().unwrap(),
        second.inscription.canonicalize().unwrap(),
        "the reproduction and the published stele are not the same document"
    );

    assert_eq!(reproduced.digest().unwrap(), second.identity);

    eprintln!(
        "published ({EPOCH_0} layers inherited) {} == reproduced from stores {}",
        second.identity,
        reproduced.digest().unwrap(),
    );

    // And the reproduction is not trivially right: chained onto nothing it is a
    // different document, which is the path-dependence the history field is
    // for.
    let unchained = export::reproduce(
        &node.second,
        node.domain.archive(),
        node.domain.state(),
        node.domain.indexes(),
        None,
        &export::First,
    )
    .unwrap();

    assert_ne!(unchained.digest().unwrap(), second.identity);
}

// ---------------------------------------------------------------------------
// Incremental detection
// ---------------------------------------------------------------------------

/// The four readings of a repository, against a real one.
///
/// The arithmetic is unit-tested in `export`; what this adds is that
/// `registry::standing` reads the *same* moving tag a publish reads and gets
/// the same sequence out of it — the half of the comparison a unit test cannot
/// supply.
#[test]
#[ignore]
fn a_publisher_can_ask_where_it_stands() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/standing");

    assert_eq!(
        registry::standing(&repository, &node.first).unwrap(),
        Standing::Empty,
        "nothing published yet"
    );

    node.publish(&repository, &node.first, false);

    // The ordinary case for a job on a timer: the node has not entered a new
    // epoch since the last run. It used to arrive as the same refusal a skipped
    // epoch raises.
    assert_eq!(
        registry::standing(&repository, &node.first).unwrap(),
        Standing::UpToDate { latest: 0 },
    );

    assert_eq!(
        registry::standing(&repository, &node.second).unwrap(),
        Standing::Next { latest: 0 },
    );

    // A node three epochs ahead of the repository.
    let mut skipped = node.second.clone();
    skipped.sequence = 3;

    assert_eq!(
        registry::standing(&repository, &skipped).unwrap(),
        Standing::Ahead {
            latest: 0,
            distance: 3
        },
    );

    // And the refusal that still stands behind it names both sequences and the
    // distance.
    let message = node.refuse(&repository, &skipped).to_string();

    assert!(message.contains('0'), "{message}");
    assert!(message.contains('3'), "{message}");
    assert!(message.contains("3 sequences ahead"), "{message}");
}

// ---------------------------------------------------------------------------
// The staging preflight
// ---------------------------------------------------------------------------

/// What a publish stages at once, sized off the stele before it.
///
/// The half of the publish-side preflight that needs a registry: a manifest
/// stating a compressed size per layer is the only thing this number can come
/// from, and it is why the check costs no `HEAD` and no extra round trip
/// beyond the one `standing` already makes. What the number then *means* for a
/// volume — refuse a measured shortfall, warn about anything else — is decided
/// in `registry`'s own unit tests, which need no registry to decide it.
///
/// The peak is deliberately not the stele's size. A publish holds all sixteen
/// shard sinks open across one walk of the store plus as many finished layers
/// as the transport is uploading at once beside them, and never the whole
/// document, so an operator sizing a scratch volume off the repository's total
/// would size it for a run that never happens.
#[test]
#[ignore]
fn a_publish_sizes_its_staging_off_the_stele_before_it() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/staging");

    // The volume the preflight sizes is the one the transport will write to,
    // because the transport is what it asks. Both the publish check and the
    // restore driver stand on this, and neither could catch it going wrong: a
    // preflight against the wrong directory passes for the same reason the
    // right one does.
    assert_eq!(
        stelae::oci::Registry::scratch_dir(&repository),
        Some(fixture.scratch()),
    );

    // The no-predecessor path: an empty repository states no sizes, so nothing
    // can be measured, so nothing is refused. A first publish runs.
    assert_eq!(registry::staging_peak(&repository).unwrap(), None);
    registry::preflight(&repository).unwrap();

    node.publish(&repository, &node.first, false);

    let peak = registry::staging_peak(&repository).unwrap().unwrap();

    assert_eq!(
        peak.unsized_layers, 0,
        "a manifest states a size for every layer it names"
    );

    // The same arithmetic, from the same manifest, read back through the
    // command an operator would read it with — so the peak is held against the
    // registry's own numbers rather than against a literal that would have to
    // be maintained beside the fixture.
    let inspected = registry::inspect(&repository, registry::Point::Latest).unwrap();

    let mut state_bytes = 0;
    let mut others = Vec::new();

    for (descriptor, size) in inspected
        .inscription
        .layers
        .iter()
        .zip(&inspected.compressed)
    {
        let size = size.expect("the manifest sizes every layer");

        if is_state_kind(&descriptor.kind) {
            state_bytes += size;
        } else {
            others.push(size);
        }
    }

    others.sort_unstable_by(|a, b| b.cmp(a));

    assert_eq!(peak.state_bytes, state_bytes, "every state layer summed");

    // As many non-state layers as the transport uploads at once, because each
    // holds its staging file until its own round trip lands — capped by how
    // many the stele has. Read off the transport, not off a literal that would
    // drift from the default.
    let concurrency = stelae::oci::Registry::concurrency(&repository);
    let staged = others.len().min(concurrency);

    assert_eq!(
        peak.concurrent_other_bytes,
        others[..staged],
        "the largest {staged} of the {} layers beside the state pass",
        others.len(),
    );

    assert_eq!(
        peak.largest_other_bytes(),
        others[0],
        "and the first of them is the largest",
    );

    assert_eq!(
        peak.bytes(),
        state_bytes + others[..staged].iter().sum::<u64>(),
    );

    // Never more than the repository holds: the publish stages a subset of the
    // stele, which is the whole difference between what a repository holds and
    // what a publish holds at once. Equality here is this fixture's stele
    // having fewer epoch layers than the transport has permits — a mainnet
    // stele has hundreds — so the claim is stated as the bound it is.
    assert!(
        peak.bytes() <= inspected.total_compressed,
        "peak {} is above the stele's {} compressed bytes",
        peak.bytes(),
        inspected.total_compressed,
    );

    // And a transport that uploads one at a time stages exactly one of them
    // beside the pass, which is the arrangement this check was first written
    // for and the floor the concurrent one is measured against.
    let serial = fixture.repository_tuned(
        "dolos/staging",
        registry_fixture::credentials(),
        registry::Tuning {
            concurrency: std::num::NonZeroUsize::new(1),
            verify_adopted: false,
        },
    );

    let alone = registry::staging_peak(&serial).unwrap().unwrap();

    assert_eq!(alone.bytes(), state_bytes + others[0]);
    assert!(alone.bytes() < inspected.total_compressed);

    // And the volume the fixture stages on holds it, so the publish that
    // follows is not refused.
    registry::preflight(&repository).unwrap();

    eprintln!(
        "staging peak: {} bytes ({state_bytes} across sixteen shards, {:?} for the {staged} \
         largest layers beside them), against {} compressed bytes in the repository",
        peak.bytes(),
        peak.concurrent_other_bytes,
        inspected.total_compressed,
    );
}

/// Done criterion 1: a restarted publish never rebuilds an epoch layer it
/// already uploaded.
///
/// The interruption lands at a layer this test names rather than after a
/// wall-clock delay, for the reason `tests/restore_registry.rs` states about
/// its own: a kill on a timer over a loopback registry sometimes interrupts
/// nothing. Here it is `indexes` for epoch 1 — the last layer sequence 1
/// *builds* before the state tip, since epoch 1 carries no logs and epoch 0's
/// log layers are inherited rather than opened. Three epoch layers landed ahead
/// of it.
///
/// One of those three is what makes the record load-bearing rather than
/// decorative. Epoch 0's layers would be inherited from sequence 0's manifest
/// with or without a record; epoch 1's `blocks` exists only as a blob nothing
/// references, and only the record knows it is there.
#[test]
#[ignore]
fn a_restarted_publish_carries_forward_the_layers_it_finished() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let storage = tempfile::tempdir().unwrap();

    let first = fixture.repository("dolos/resume");

    node.publish_as(publishing(&first, &storage), &node.first)
        .unwrap();

    assert_eq!(
        registry::PublishRecord::load(&record_path(&storage)).unwrap(),
        None,
        "a publish that sealed left its record behind",
    );

    // The interruption. A fresh transport each time, because the process this
    // stands in for is a fresh process.
    let dying = fixture.repository("dolos/resume");

    let killed = node
        .publish_through(
            &Interrupted {
                inner: &dying,
                kind: INDEXES,
                epoch: Some(1),
            },
            publishing(&dying, &storage),
            &node.second,
        )
        .unwrap_err();

    assert!(
        matches!(killed, Error::Stelae(stelae::Error::Io(_))),
        "{killed:?}"
    );

    let record = registry::PublishRecord::load(&record_path(&storage))
        .unwrap()
        .expect("an interrupted publish left no record");

    assert_eq!(
        record.layers.len(),
        3,
        "every epoch layer ahead of the one that died, and only those",
    );

    assert!(
        record
            .layers
            .iter()
            .all(|layer| !is_state_kind(&layer.descriptor.kind)),
        "a state layer is the tip, and is never recorded",
    );

    // The restart, against the same repository and the same storage.
    let resumed_into = fixture.repository("dolos/resume");

    let resumed = node
        .publish_as(publishing(&resumed_into, &storage), &node.second)
        .unwrap();

    assert_eq!(
        resumed.layers_reused,
        EPOCH_0 + 1,
        "epoch 0's four off the manifest, epoch 1's blocks off the record",
    );

    assert_eq!(
        resumed.layers_built,
        1 + state_layer_count(),
        "epoch 1's indexes and every state layer, and nothing else",
    );

    // The claim in the counters the transport keeps: the layers the record
    // carried moved no bytes at all, and the state layers it never carried did.
    assert_eq!(resumed.transfer.layers_reused, (EPOCH_0 + 1) as u64);
    assert_eq!(
        resumed.transfer.layers_uploaded,
        1 + state_layer_count() as u64
    );
    assert_eq!(
        resumed.transfer.layers_skipped, 0,
        "a layer skipped by the blob check is one that was built first, which is \
         the cost this record exists to remove",
    );

    assert_eq!(
        registry::PublishRecord::load(&record_path(&storage)).unwrap(),
        None,
        "a resumed publish that sealed left its record behind",
    );

    // Done criterion 1's other half: the stele is the one an uninterrupted
    // publish would have produced, down to the manifest bytes. A second
    // repository published into identically is the only thing that can say so —
    // a repository's identity is path-dependent, so the comparison has to be
    // against a chain built the same way rather than against a literal.
    let clean = fixture.repository("dolos/uninterrupted");

    node.publish(&clean, &node.first, false);
    let uninterrupted = node.publish(&clean, &node.second, false);

    assert_eq!(resumed.identity, uninterrupted.identity);
    assert_eq!(resumed.inscription, uninterrupted.inscription);
    assert_eq!(manifest_of(&resumed_into), manifest_of(&clean));

    eprintln!(
        "resumed: {} layers carried forward, {} built, {} bytes not moved — and the same manifest",
        resumed.layers_reused, resumed.layers_built, resumed.transfer.bytes_reused,
    );
}

/// Done criterion 2, both halves: the two ways a record is not honoured.
///
/// `--rebuild` is the publisher choosing to reproduce, and it ignores the
/// record exactly as it already ignores inheritance. A record naming another
/// repository is ignored because a blob digest is an address in one repository
/// and means nothing in another — and this is the case a record could get
/// silently wrong, since the layers it names would be adopted into a manifest
/// the registry cannot serve.
#[test]
#[ignore]
fn a_rebuild_and_another_repository_both_ignore_the_record() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let storage = tempfile::tempdir().unwrap();

    let first = fixture.repository("dolos/ignored-a");
    node.publish_as(publishing(&first, &storage), &node.first)
        .unwrap();

    let other = fixture.repository("dolos/ignored-b");
    node.publish_as(publishing(&other, &storage), &node.first)
        .unwrap();

    let interrupt = |repository: &registry::Registry| {
        node.publish_through(
            &Interrupted {
                inner: repository,
                kind: INDEXES,
                epoch: Some(1),
            },
            publishing(repository, &storage),
            &node.second,
        )
        .unwrap_err();

        registry::PublishRecord::load(&record_path(&storage))
            .unwrap()
            .expect("an interrupted publish left no record")
            .layers
            .len()
    };

    let dying = fixture.repository("dolos/ignored-a");
    assert_eq!(interrupt(&dying), 3);

    // The other repository's publish reaches the same record and takes nothing
    // from it: epoch 0's four layers come off *its own* manifest, and epoch 1's
    // come out of the stores.
    let elsewhere = fixture.repository("dolos/ignored-b");

    let published = node
        .publish_as(publishing(&elsewhere, &storage), &node.second)
        .unwrap();

    assert_eq!(
        published.layers_reused, EPOCH_0,
        "epoch 0's four, off this repository's own manifest and nothing else",
    );
    assert_eq!(
        published.layers_built,
        EPOCH_1 + state_layer_count(),
        "epoch 1's two layers and every state layer, exactly as if no \
         record existed",
    );

    // Back to the first repository, now with `--rebuild`. The record is there
    // and describes layers this publish could carry; it is ignored wholesale,
    // and overwritten rather than read.
    let dying = fixture.repository("dolos/ignored-a");
    assert_eq!(interrupt(&dying), 3);

    let rebuilt_into = fixture.repository("dolos/ignored-a");

    let rebuilt = node
        .publish_as(
            publishing(&rebuilt_into, &storage).rebuilding(true),
            &node.second,
        )
        .unwrap();

    assert_eq!(
        rebuilt.layers_reused, 0,
        "a rebuild carries nothing forward"
    );
    assert_eq!(rebuilt.layers_built, WHOLE_SECOND);

    // And it is the same stele either way, which is the point of being allowed
    // to ignore the record: reproducing what you published is not the same act
    // as forgetting that you published it.
    assert_eq!(
        rebuilt.inscription.history.len(),
        1,
        "a rebuild still chains",
    );
}

/// A publish into `repository`, recording beside the stores at `storage`.
fn publishing<'a>(
    repository: &'a registry::Registry,
    storage: &'a tempfile::TempDir,
) -> registry::Publishing<'a> {
    registry::Publishing::new(repository).recording_in(storage.path())
}

fn record_path(storage: &tempfile::TempDir) -> std::path::PathBuf {
    registry::record_path_in(storage.path())
}

/// The manifest a repository's moving tag resolves to, as bytes.
fn manifest_of(repository: &registry::Registry) -> Vec<u8> {
    let stele = repository.pull_latest(&DolosProfile).unwrap();

    stelae::oci::manifest_bytes(stele.manifest()).unwrap()
}

/// A transport that stops at a layer the test chose.
///
/// The publish-side twin of `tests/restore_registry.rs`'s interrupted reader:
/// it refuses the moment a sink is asked for the named layer, so the
/// interrupted run uploaded exactly the layers ahead of that one and nothing of
/// it. Everything else goes straight through to the registry, which is what
/// `registry::publish_into` requires of a writer it is handed.
struct Interrupted<'a> {
    inner: &'a registry::Registry,
    kind: &'static str,
    /// The epoch the named layer's **descriptor scope** carries, and `None`
    /// for a layer whose scope has none — which is how a state tip shard is
    /// named, and the only way to stop a publish inside its state pass.
    epoch: Option<u64>,
}

impl stelae::SteleWriter for Interrupted<'_> {
    type Sink = stelae::oci::RegistrySink;

    fn layer_sink(
        &self,
        profile: &dyn stelae::Profile,
        spec: &stelae::transport::LayerSpec,
        level: i32,
    ) -> Result<Self::Sink, stelae::Error> {
        let epoch = spec.scope.get("epoch").and_then(serde_json::Value::as_u64);

        if spec.kind == self.kind && epoch == self.epoch {
            return Err(stelae::Error::Io(std::io::Error::other(
                "the machine went away",
            )));
        }

        self.inner.layer_sink(profile, spec, level)
    }

    /// Forwarded, like `seal`: a transport double that swallowed the second
    /// descriptor would fail the seal of any publish that cuts a retained
    /// dump, for a reason that has nothing to do with the interruption under
    /// test.
    fn carry_again(
        &self,
        written: &stelae::transport::WrittenLayer,
        scope: serde_json::Value,
    ) -> Result<stelae::transport::WrittenLayer, stelae::Error> {
        self.inner.carry_again(written, scope)
    }

    fn seal(
        &self,
        profile: &dyn stelae::Profile,
        inscription: &stelae::inscription::Inscription,
    ) -> Result<stelae::Digest, stelae::Error> {
        self.inner.seal(profile, inscription)
    }
}

/// The publish half of the observer's cross-check: what the stream said moved,
/// against what the transport counted.
///
/// Three publishes rather than one, because a single publish into an empty
/// repository produces only one of the three things a layer can end as, and the
/// third takes a second repository to reach:
///
/// 1. **built and uploaded** — every layer of a first stele;
/// 2. **inherited** — epoch 0's three layers when the second stele chains onto
///    the first;
/// 3. **built and then skipped** — the same three layers under `--rebuild`,
///    which suppresses inheritance and so builds them, at which point the
///    registry turns out to hold their blobs already. It needs its own
///    repository: `--rebuild` does not let a publisher republish a sequence the
///    chain has already reached, so the skip has to come from a repository
///    standing one sequence back.
///
/// Every tally is held against `Transfer`, which the transport keeps for the
/// publisher's report and not for this test, so nothing here is the recording
/// agreeing with itself.
#[test]
#[ignore]
fn a_publish_reports_what_the_transfer_counted() {
    let fixture = Fixture::spawn();
    let node = Node::build();
    let repository = fixture.repository("dolos/progress");

    let watcher = std::sync::Arc::new(Watcher::default());
    let first = node
        .publish_watched(
            Publishing::new(&repository),
            &node.first,
            &watcher.observer(),
        )
        .unwrap();

    watcher.assert_well_formed(first.inscription.layers.len());

    assert_eq!(
        watcher.ended(Outcome::Transferred),
        PER_PUBLISH,
        "every layer of a first publish is built"
    );
    assert_eq!(watcher.ended(Outcome::Inherited), 0);

    assert_eq!(
        watcher.blobs(true).len() as u64,
        first.transfer.layers_uploaded,
        "blobs announced as moving, against the uploads the transport counted"
    );
    assert_eq!(
        watcher.blobs(false).len() as u64,
        first.transfer.layers_skipped,
        "blobs announced as already present, against the skips the transport counted"
    );
    assert_eq!(
        watcher.bytes(),
        first.transfer.bytes_uploaded,
        "byte deltas summed, against the bytes the transport counted"
    );
    assert!(
        first.transfer.bytes_uploaded > 0,
        "a publish that moved nothing proves nothing about byte reporting"
    );

    let watcher = std::sync::Arc::new(Watcher::default());
    let second = node
        .publish_watched(
            Publishing::new(&repository),
            &node.second,
            &watcher.observer(),
        )
        .unwrap();

    watcher.assert_well_formed(second.inscription.layers.len());

    assert_eq!(
        watcher.ended(Outcome::Inherited) as u64,
        second.transfer.layers_reused,
        "layers announced as carried forward, against the transport's count"
    );
    assert_eq!(
        watcher.blobs(true).len() as u64,
        second.transfer.layers_uploaded,
    );
    assert_eq!(watcher.bytes(), second.transfer.bytes_uploaded);

    // An inherited layer moves no blob at all — it is not built, so nothing is
    // staged and nothing is offered to the registry. That is what makes it a
    // different outcome from a skip rather than a spelling of one.
    assert_eq!(
        watcher.blobs(true).len() + watcher.blobs(false).len() + watcher.ended(Outcome::Inherited),
        second.inscription.layers.len(),
    );

    let rebuilt_into = fixture.repository("dolos/progress-rebuild");
    node.publish(&rebuilt_into, &node.first, false);

    let watcher = std::sync::Arc::new(Watcher::default());
    let again = node
        .publish_watched(
            Publishing::new(&rebuilt_into).rebuilding(true),
            &node.second,
            &watcher.observer(),
        )
        .unwrap();

    watcher.assert_well_formed(again.inscription.layers.len());

    assert_eq!(
        watcher.ended(Outcome::Inherited),
        0,
        "a rebuild carries nothing forward"
    );
    assert_eq!(
        watcher.blobs(false).len() as u64,
        again.transfer.layers_skipped,
        "blobs announced as already present, against the skips the transport counted"
    );
    assert!(
        again.transfer.layers_skipped > 0,
        "a rebuild over a repository that already holds those blobs has to skip some"
    );
    assert_eq!(
        watcher.blob_bytes(false),
        again.transfer.bytes_skipped,
        "the sizes of the skipped blobs, against what the transport counted them as"
    );
    assert_eq!(
        watcher.blobs(true).len() as u64,
        again.transfer.layers_uploaded,
    );
    assert_eq!(watcher.bytes(), again.transfer.bytes_uploaded);
}
