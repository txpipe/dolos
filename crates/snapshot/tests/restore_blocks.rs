//! A snapshot whose closed epochs' blocks are in a blocks repository restores
//! them from there.
//!
//! The fixture is an export into a network root, which writes the blocks
//! lineage beside the snapshot ([`export::publish`]); its control is the
//! archive the export was cut from. [`paired`] copies the pair into other
//! layouts to build the wrong, missing and tampered ones. Every refusal leaves
//! the stores as they were.

mod node;
mod paired;
mod recorder;

use std::path::{Path, PathBuf};

use dolos_core::{ArchiveStore as _, BlockSlot, Domain as _, StateStore as _};
use dolos_snapshot::{
    export,
    restore::{self, Input, Repo, RestoreOutcome, Restoring, Target, DEFAULT_READ_AHEAD},
    Error, BLOCKS,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use node::{harness, plan_at_boundary, Blank};
use paired::{Paired, Tamper};
use recorder::contents_digest;
use stelae::{
    dir::SnapshotDir, inscription::LayerDescriptor, progress::Observer, Resume, SnapshotReader,
};

/// The boundary the fixture's head is cut at; epochs 0 to 2 are closed.
const HEAD: u64 = 3;

/// The harness published on boundaries 1 to [`HEAD`] into a network root.
struct Fixture {
    magic: u64,
    /// The stores the export was cut from.
    domain: ToyDomain<MemoryStores>,
    /// The network root: the snapshot lineage and the blocks lineage it names.
    source: tempfile::TempDir,
    paired: Paired,
}

impl Fixture {
    fn new() -> Self {
        let (domain, source) = published(HEAD, |_| None);
        let magic = u64::from(domain.genesis().network_magic());
        let paired = paired::digests(source.path());

        Self {
            magic,
            domain,
            source,
            paired,
        }
    }

    /// The root's snapshot layout.
    fn dolos(&self) -> PathBuf {
        node::dolos(self.source.path())
    }

    /// The root's blocks layout.
    fn blocks(&self) -> PathBuf {
        node::blocks(self.source.path()).unwrap()
    }

    /// The blocks snapshot the head names.
    fn reference(&self) -> stelae::Digest {
        self.paired.blocks[&HEAD]
    }

    /// The archive's blocks from `slot` on, as a restore keeping them writes
    /// them.
    fn archived_from(&self, slot: BlockSlot) -> Vec<(BlockSlot, Vec<u8>)> {
        self.domain
            .archive()
            .get_range(Some(slot), None)
            .unwrap()
            .map(|(slot, body)| (slot, body.to_vec()))
            .collect()
    }

    /// Restore the layout at `snapshot`, with `blocks` beside it, into fresh
    /// stores, keeping `max_history` slots of history.
    fn restore(
        &self,
        snapshot: &Path,
        blocks: Option<&Path>,
        max_history: Option<u64>,
    ) -> (Result<RestoreOutcome, Error>, Blank<MemoryStores>) {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<MemoryStores>::open();

        let outcome = restore::execute(
            Input::Directory {
                path: snapshot,
                blocks,
            },
            Restoring {
                network_magic: self.magic,
                max_history,
                storage_path: storage.path(),
                resume: false,
                skip_space_check: false,
                journals: 0,
                read_ahead: DEFAULT_READ_AHEAD,
                source: None,
            },
            Target::new(&blank.archive, blank.state()),
            None,
        );

        (outcome, blank)
    }
}

/// The harness published into a fresh network root on boundaries 1 to
/// `head`, each plan's `position.blocks` set to what `blocks` gives for its
/// sequence, which writes no blocks snapshot.
fn published(
    head: u64,
    blocks: impl Fn(u64) -> Option<stelae::Digest>,
) -> (ToyDomain<MemoryStores>, tempfile::TempDir) {
    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();

    for epoch in 1..=head {
        let mut plan = plan_at_boundary(&domain, epoch);
        plan.blocks = blocks(epoch);

        export::publish(
            root.path(),
            &plan,
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap();
    }

    (domain, root)
}

/// Every block the archive holds, by slot, in order.
fn blocks_of(blank: &Blank<MemoryStores>) -> Vec<(BlockSlot, Vec<u8>)> {
    blank
        .archive
        .get_range(None, None)
        .unwrap()
        .map(|(slot, body)| (slot, body.to_vec()))
        .collect()
}

/// Stores no restore has written to.
fn untouched(blank: &Blank<MemoryStores>) -> bool {
    blank.state().read_cursor().unwrap().is_none() && blocks_of(blank).is_empty()
}

/// The media types of the plan's `blocks` layers, in plan order, by
/// repository.
fn blocks_layers(outcome: &RestoreOutcome) -> Vec<(u64, Repo, String)> {
    let plan = &outcome.plan;

    plan.epochs
        .iter()
        .filter_map(|epoch| {
            let layer = epoch.blocks.as_ref()?;
            Some((
                epoch.epoch,
                plan.blocks_repo(epoch),
                layer.media_type.clone(),
            ))
        })
        .collect()
}

const DOLOS_BLOCKS: &str = "application/vnd.dolos.stelae.blocks.v1+zstd";

/// An export into a network root restores every block its archive holds,
/// reading the closed epochs' from the blocks repository and the boundary
/// block, last, from the snapshot's own tip; a copy of the pair through the
/// blocks profile's plain writer restores the same node.
#[test]
fn an_exported_root_restores_every_block_of_its_archive() {
    let fixture = Fixture::new();

    let (outcome, restored) = fixture.restore(&fixture.dolos(), Some(&fixture.blocks()), None);
    let outcome = outcome.unwrap();

    assert_eq!(outcome.plan.blocks, Some(fixture.reference()));

    let read = blocks_layers(&outcome);
    let from_repository: Vec<u64> = read
        .iter()
        .filter(|(_, repo, media_type)| {
            *repo == Repo::Blocks && media_type == stelae_blocks::BLOCKS_MEDIA_TYPE
        })
        .map(|(epoch, ..)| *epoch)
        .collect();

    assert_eq!(from_repository, (0..HEAD).collect::<Vec<_>>(), "{read:?}");
    assert_eq!(
        read.last().unwrap(),
        &(HEAD, Repo::Dolos, DOLOS_BLOCKS.to_owned()),
        "the boundary block is not the snapshot's own, after the closed epochs"
    );

    let blocks = blocks_of(&restored);
    assert!(blocks.len() > HEAD as usize, "{} blocks", blocks.len());
    assert_eq!(blocks, fixture.archived_from(0));
    assert_eq!(outcome.summary.blocks, blocks.len() as u64);

    let (dolos, copied) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let copy = paired::pair(
        fixture.source.path(),
        &SnapshotDir::create(dolos.path()).unwrap(),
        &SnapshotDir::create(copied.path()).unwrap(),
    );
    assert_eq!(copy.blocks, fixture.paired.blocks);
    assert_eq!(copy.dolos, fixture.paired.dolos);

    let (again, from_copy) = fixture.restore(dolos.path(), Some(copied.path()), None);
    assert_eq!(again.unwrap().summary, outcome.summary);
    assert_eq!(
        contents_digest(&from_copy.archive, from_copy.state()),
        contents_digest(&restored.archive, restored.state())
    );
}

/// The download and space figures cover both repositories: every planned
/// layer is counted and sized by the transport its repository is read from.
#[test]
fn the_remaining_work_and_the_estimate_cover_both_repositories() {
    let fixture = Fixture::new();

    let (outcome, _) = fixture.restore(&fixture.dolos(), Some(&fixture.blocks()), None);
    let outcome = outcome.unwrap();

    assert_eq!(
        outcome.outlook.remaining.layers,
        outcome.plan.layers().count()
    );
    assert_eq!(outcome.outlook.remaining.unsized_layers, 0);

    let snapshot = node::resolved(fixture.source.path());
    let blocks = SnapshotDir::open(fixture.blocks())
        .unwrap()
        .resolve_sequence(&stelae_blocks::BlocksProfile, HEAD)
        .unwrap();
    let (index, blocks_index) = (snapshot.blob_index().unwrap(), blocks.blob_index().unwrap());
    let pair = restore::Blocks {
        snapshot: &blocks,
        index: &blocks_index,
    };

    // Each repository's layers, sized by its own transport.
    let sizes = |repo| -> Vec<u64> {
        let sized = |snapshot: &dyn SnapshotReader<Blob = std::fs::File>,
                     index,
                     layer: &LayerDescriptor| {
            snapshot.compressed_size(index, layer).unwrap().unwrap()
        };

        outcome
            .plan
            .remaining_in(repo, &Resume::none())
            .map(|layer| match repo {
                Repo::Dolos => sized(&snapshot, &index, layer),
                Repo::Blocks => sized(&blocks, &blocks_index, layer),
            })
            .collect()
    };
    let (own, theirs) = (sizes(Repo::Dolos), sizes(Repo::Blocks));
    assert_eq!(theirs.len(), HEAD as usize);

    let remaining = &outcome.outlook.remaining;
    assert_eq!(
        remaining.compressed_bytes,
        own.iter().chain(&theirs).sum::<u64>()
    );
    assert_eq!(
        remaining.largest_compressed,
        own.iter().chain(&theirs).copied().max()
    );

    let estimate = outcome
        .plan
        .estimate(&snapshot, &index, Some(pair), &Resume::none(), 0)
        .unwrap();

    assert_eq!(estimate.fallback_layers, 0);
    assert!(estimate.blocks > 0);

    // Without the blocks snapshot, the layers it serves have no transport.
    let err = outcome
        .plan
        .estimate(&snapshot, &index, None, &Resume::none(), 0)
        .unwrap_err();
    assert!(matches!(err, Error::BlocksMissing { .. }), "{err:?}");
}

/// A history window reads the blocks of the closed epochs it keeps and no
/// others, and one that keeps no closed epoch reads no blocks repository.
#[test]
fn a_history_window_reads_the_blocks_of_the_epochs_it_keeps() {
    let fixture = Fixture::new();
    let chain = node::resolved(fixture.source.path());
    let chain = chain.chain().unwrap();
    let start = |outcome: &RestoreOutcome, epoch: u64| {
        outcome
            .plan
            .epochs
            .iter()
            .find(|layers| layers.epoch == epoch)
            .unwrap()
            .start_slot
    };

    // One slot back from the cut reaches the last slot of epoch 2.
    let (outcome, paired) = fixture.restore(&fixture.dolos(), Some(&fixture.blocks()), Some(1));
    let outcome = outcome.unwrap();

    assert_eq!(
        blocks_layers(&outcome)
            .iter()
            .map(|(epoch, repo, _)| (*epoch, *repo))
            .collect::<Vec<_>>(),
        vec![(HEAD - 1, Repo::Blocks), (HEAD, Repo::Dolos)]
    );
    assert_eq!(outcome.plan.skipped_epochs, HEAD as usize - 1);
    assert_eq!(
        blocks_of(&paired),
        fixture.archived_from(start(&outcome, HEAD - 1))
    );
    assert_eq!(
        restore::blocks_wanted(chain, Some(1)).unwrap(),
        Some(fixture.reference())
    );

    // A window of nothing keeps the cut's epoch alone, whose block is the
    // snapshot's: no blocks repository is needed.
    assert_eq!(restore::blocks_wanted(chain, Some(0)).unwrap(), None);

    let (outcome, paired) = fixture.restore(&fixture.dolos(), None, Some(0));
    let outcome = outcome.unwrap();

    assert_eq!(outcome.plan.blocks, None);
    assert_eq!(
        blocks_of(&paired),
        fixture.archived_from(start(&outcome, HEAD))
    );
    assert_eq!(outcome.summary.blocks, blocks_of(&paired).len() as u64);
}

/// A blocks repository whose snapshot at the Dolos snapshot's sequence is not
/// the one the Dolos snapshot names is refused before anything is written, even
/// when it holds the same blocks: the reference is an identity.
#[test]
fn a_blocks_repository_holding_another_snapshot_is_refused() {
    let fixture = Fixture::new();

    // The same epochs, sealed as a lineage that starts at the head.
    let other = tempfile::tempdir().unwrap();
    let others = paired::blocks_lineage(
        fixture.source.path(),
        &SnapshotDir::create(other.path()).unwrap(),
        HEAD,
    );
    assert_ne!(others[&HEAD], fixture.reference());

    let (outcome, blank) = fixture.restore(&fixture.dolos(), Some(other.path()), None);

    let Err(Error::BlocksMismatch { expected, found }) = outcome else {
        panic!("{:?}", outcome.map(|o| o.plan.blocks));
    };

    assert_eq!(expected, fixture.reference());
    assert_eq!(found, others[&HEAD]);
    assert!(untouched(&blank));
}

/// A snapshot whose blocks are in a blocks repository is refused, naming the
/// snapshot, when the restore has no repository, when the one it has is not
/// a repository, and when it holds nothing at the snapshot's sequence.
#[test]
fn a_missing_blocks_repository_is_refused() {
    let fixture = Fixture::new();

    let empty = tempfile::tempdir().unwrap();

    // The same lineage, one sequence short of the snapshot.
    let (_, shorter) = published(HEAD - 1, |_| None);
    let short = tempfile::tempdir().unwrap();
    paired::blocks_lineage(
        shorter.path(),
        &SnapshotDir::create(short.path()).unwrap(),
        1,
    );

    for (blocks, says) in [
        (None, "no blocks repository".to_owned()),
        (Some(empty.path()), "is not a blocks repository".to_owned()),
        (
            Some(short.path()),
            format!("no blocks snapshot at epoch-{HEAD}"),
        ),
    ] {
        let (outcome, blank) = fixture.restore(&fixture.dolos(), blocks, None);

        let Err(error @ Error::BlocksMissing { reference, .. }) = &outcome else {
            panic!("{blocks:?}: {:?}", outcome.map(|o| o.plan.blocks));
        };

        assert_eq!(*reference, fixture.reference(), "{blocks:?}");
        assert!(error.to_string().contains(&says), "{blocks:?}: {error}");
        assert!(
            error.to_string().contains(&fixture.reference().to_string()),
            "{error}"
        );
        assert!(untouched(&blank), "{blocks:?}");
    }

    // A layout holding another profile's snapshot at that sequence is not a
    // missing blocks repository, and is refused as what it is.
    let (outcome, blank) = fixture.restore(&fixture.dolos(), Some(&fixture.dolos()), None);
    let error = outcome.map(|o| o.plan.blocks).unwrap_err();
    assert!(
        matches!(error, Error::Stelae(stelae::Error::UnknownProfile { .. })),
        "{error:?}"
    );
    assert!(untouched(&blank));
}

/// A snapshot chain that lists a closed epoch's `blocks` layer, as snapshots
/// did before decision 0080, is refused before anything is written, whether a
/// blocks repository is given or not: only the boundary window's blocks are
/// the snapshot's.
#[test]
fn a_snapshot_listing_a_closed_epochs_blocks_is_refused() {
    let fixture = Fixture::new();

    for closed in 0..HEAD {
        refuses_a_listing_of(&fixture, closed);
    }
}

fn refuses_a_listing_of(fixture: &Fixture, closed: u64) {
    let listing = tempfile::tempdir().unwrap();
    paired::listing_closed_blocks(
        fixture.source.path(),
        &SnapshotDir::create(listing.path()).unwrap(),
        closed,
    );

    let resolved = SnapshotDir::open(listing.path())
        .unwrap()
        .resolve_latest(&dolos_snapshot::DolosProfile)
        .unwrap();
    let listed: Vec<_> = resolved
        .chain()
        .unwrap()
        .descriptors()
        .filter(|layer| layer.kind == BLOCKS)
        .map(|layer| layer.scope["epoch"].as_u64().unwrap())
        .collect();
    assert_eq!(listed, [closed, HEAD]);

    let refused = |error: &Error| {
        matches!(error, Error::MalformedInscription { reason, .. }
            if reason.contains(&format!("closed epoch {closed};")))
    };

    let error = restore::blocks_wanted(resolved.chain().unwrap(), None).unwrap_err();
    assert!(refused(&error), "{error:?}");

    for blocks in [None, Some(fixture.blocks())] {
        let (outcome, blank) = fixture.restore(listing.path(), blocks.as_deref(), None);
        let error = outcome.map(|o| o.plan.blocks).unwrap_err();

        assert!(refused(&error), "{closed}, {blocks:?}: {error:?}");
        assert!(untouched(&blank), "{closed}, {blocks:?}");
    }
}

/// A blocks-repository layer that breaks the blocks profile's reader rules is
/// refused, and the restore never writes its cursor: a header scope that is
/// not the descriptor's, in its end slot or its epoch, a block past either
/// end of its epoch, and a slot going backwards. Each layer was sealed past
/// the plain writer, so its `diffId` and its descriptor's bounds are what the
/// snapshot expects.
#[test]
fn a_blocks_layer_breaking_the_profiles_reader_rules_is_refused() {
    let fixture = Fixture::new();

    for tamper in [
        Tamper::HeaderScope,
        Tamper::HeaderEpoch,
        Tamper::OutsideEpoch,
        Tamper::BeforeEpoch,
        Tamper::Backwards,
    ] {
        let (dolos, blocks) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        paired::pair_tampered(
            fixture.source.path(),
            &SnapshotDir::create(dolos.path()).unwrap(),
            &SnapshotDir::create(blocks.path()).unwrap(),
            Some((1, tamper)),
        );

        let (outcome, blank) = fixture.restore(dolos.path(), Some(blocks.path()), None);
        let error = outcome.map(|o| o.summary).unwrap_err();

        let refused = match tamper {
            Tamper::HeaderScope | Tamper::HeaderEpoch => matches!(
                &error,
                Error::Stelae(stelae::Error::LayerMismatch { kind, .. }) if kind == BLOCKS
            ),
            Tamper::OutsideEpoch | Tamper::BeforeEpoch => matches!(
                error,
                Error::Blocks(stelae_blocks::Error::OutsideEpoch { epoch: 1, .. })
            ),
            Tamper::Backwards => {
                matches!(error, Error::Blocks(stelae_blocks::Error::OutOfOrder(_)))
            }
        };

        assert!(refused, "{tamper:?}: {error:?}");
        assert_eq!(blank.state().read_cursor().unwrap(), None, "{tamper:?}");
    }
}

/// Done criterion 3: a single-boundary export into an empty root starts both
/// lineages at its cut, the blocks snapshot listing every epoch before it,
/// and the root restores every block the archive holds.
#[test]
fn a_single_boundary_export_to_a_root_restores_its_full_block_history() {
    let domain = harness::<MemoryStores>();
    let magic = u64::from(domain.genesis().network_magic());
    let root = tempfile::tempdir().unwrap();

    let snapshot = export::publish(
        root.path(),
        &plan_at_boundary(&domain, HEAD),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();

    let blocks = node::resolved_blocks(root.path()).unwrap();
    let head = blocks.read_inscription().unwrap();

    assert_eq!((snapshot.sequence, snapshot.previous), (HEAD, None));
    assert_eq!((head.sequence, head.previous), (HEAD, None));
    assert_eq!(head.diff.len() as u64, HEAD, "one layer per closed epoch");
    assert!(head.tip.is_empty());
    assert_eq!(
        dolos_snapshot::read_position(&snapshot.position)
            .unwrap()
            .blocks,
        Some(head.digest().unwrap())
    );
    assert_eq!(snapshot.layers_of_kind(BLOCKS).count(), 1);

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<MemoryStores>::open();
    let outcome = restore::execute(
        Input::Directory {
            path: &node::dolos(root.path()),
            blocks: node::blocks(root.path()).as_deref(),
        },
        Restoring {
            network_magic: magic,
            max_history: None,
            storage_path: storage.path(),
            resume: false,
            skip_space_check: false,
            journals: 0,
            read_ahead: DEFAULT_READ_AHEAD,
            source: None,
        },
        Target::new(&blank.archive, blank.state()),
        None,
    )
    .unwrap();

    let archived: Vec<_> = domain
        .archive()
        .get_range(None, None)
        .unwrap()
        .map(|(slot, body)| (slot, body.to_vec()))
        .collect();

    assert_eq!(outcome.plan.blocks, Some(head.digest().unwrap()));
    assert_eq!(blocks_of(&blank), archived);
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        domain.state().read_cursor().unwrap()
    );
}

/// A root export interrupted after its blocks snapshot sealed is written
/// again on the same link: the retry seals the same blocks snapshot and the
/// snapshot an uninterrupted export writes.
#[test]
fn an_interrupted_root_export_writes_the_same_pair_again() {
    let domain = harness::<MemoryStores>();
    let (whole, interrupted) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let publish = |root: &Path, epoch| {
        export::publish(
            root,
            &plan_at_boundary(&domain, epoch),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap()
    };

    publish(whole.path(), 1);
    publish(interrupted.path(), 1);
    let expected = publish(whole.path(), 2);

    // What the interrupted export left: the blocks snapshot at 2, no snapshot.
    let blocks = interrupted.path().join(export::BLOCKS_REPOSITORY);
    std::fs::remove_dir_all(&blocks).unwrap();
    copy_dir(&whole.path().join(export::BLOCKS_REPOSITORY), &blocks);

    assert_eq!(publish(interrupted.path(), 2), expected);
    assert_eq!(
        paired::digests(interrupted.path()).blocks,
        paired::digests(whole.path()).blocks
    );
}

/// A root whose blocks lineage stands two sequences behind the export is
/// refused by the link rule before a snapshot layer is written.
#[test]
fn a_root_export_whose_blocks_lineage_does_not_follow_is_refused() {
    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();

    export::publish(
        root.path(),
        &plan_at_boundary(&domain, 1),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();
    std::fs::remove_dir_all(node::dolos(root.path())).unwrap();

    let error = export::publish(
        root.path(),
        &plan_at_boundary(&domain, HEAD),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(
            error,
            Error::LinkBreak {
                latest: 1,
                publishing: HEAD,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(!node::dolos(root.path()).exists());
}

/// A root whose blocks lineage is another network's is refused before a
/// snapshot layer is written.
#[test]
fn a_root_export_onto_another_networks_blocks_is_refused() {
    use stelae::SnapshotWriter as _;

    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();
    let other = 42;

    // The blocks snapshot at 1 of a network with magic 42.
    let layout = SnapshotDir::create(root.path().join(export::BLOCKS_REPOSITORY)).unwrap();
    let scope = stelae_blocks::EpochScope::new(other, 0, 0, 86_399).unwrap();
    let layer =
        stelae_blocks::write_layer(&layout, scope, stelae_blocks::COMPRESSION_LEVEL, []).unwrap();
    let mut inscription = stelae::Inscription::new(
        &stelae_blocks::BlocksProfile,
        1,
        stelae_blocks::position(&stelae_blocks::Position {
            network_magic: other,
            epoch: 1,
        }),
        stelae_blocks::parameters(),
        stelae_blocks::compression(),
    );
    inscription.diff.push(layer.descriptor);
    layout
        .seal(&stelae_blocks::BlocksProfile, &inscription)
        .unwrap();

    let error = export::publish(
        root.path(),
        &plan_at_boundary(&domain, 2),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(error, Error::NetworkMismatch { found: 42, .. }),
        "{error:?}"
    );
    assert!(!node::dolos(root.path()).exists());
}

/// A root whose blocks lineage stands ahead of its snapshot lineage is refused
/// by the Dolos snapshot's link rule before a blocks snapshot is sealed, so the
/// root can still be put right by exporting the snapshot it lacks.
#[test]
fn a_root_export_whose_snapshot_lineage_does_not_follow_seals_no_blocks() {
    let domain = harness::<MemoryStores>();
    let (ahead, root) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let publish = |root: &Path, epoch| {
        export::publish(
            root,
            &plan_at_boundary(&domain, epoch),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
    };

    publish(ahead.path(), 1).unwrap();
    publish(root.path(), 1).unwrap();
    publish(ahead.path(), 2).unwrap();

    // The snapshot lineage at 1, the blocks lineage at 2.
    let blocks = root.path().join(export::BLOCKS_REPOSITORY);
    std::fs::remove_dir_all(&blocks).unwrap();
    copy_dir(&ahead.path().join(export::BLOCKS_REPOSITORY), &blocks);
    let blobs = blocks_blobs(root.path());
    assert!(!blobs.is_empty());

    let error = publish(root.path(), HEAD).unwrap_err();

    assert!(
        matches!(
            error,
            Error::LinkBreak {
                latest: 1,
                publishing: HEAD,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(latest_blocks(root.path()), Some(2));
    assert_eq!(blocks_blobs(root.path()), blobs);
}

/// A root whose snapshot lineage is another network's is refused before a
/// blocks lineage is started for this one.
#[test]
fn a_root_export_onto_another_networks_snapshot_seals_no_blocks() {
    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();

    let mut foreign = plan_at_boundary(&domain, 1);
    foreign.network = dolos_snapshot::Network::for_magic(42);
    export::publish(
        root.path(),
        &foreign,
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();
    std::fs::remove_dir_all(root.path().join(export::BLOCKS_REPOSITORY)).unwrap();

    let error = export::publish(
        root.path(),
        &plan_at_boundary(&domain, 2),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(error, Error::NetworkMismatch { found: 42, .. }),
        "{error:?}"
    );
    assert_eq!(latest_blocks(root.path()), None);
    assert!(!root.path().join(export::BLOCKS_REPOSITORY).exists());
}

/// A first root export interrupted after its blocks snapshot sealed and its
/// snapshot layout was created, before the snapshot sealed, leaves `dolos/` a
/// layout with no snapshot: the retry starts the snapshot lineage and writes
/// what an uninterrupted export does.
#[test]
fn a_first_root_export_interrupted_before_its_snapshot_seals_starts_the_lineage() {
    let domain = harness::<MemoryStores>();
    let (whole, interrupted) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let publish = |root: &Path| {
        export::publish(
            root,
            &plan_at_boundary(&domain, 1),
            domain.archive(),
            domain.state(),
            &Observer::silent(),
        )
        .unwrap()
    };

    let expected = publish(whole.path());

    copy_dir(
        &whole.path().join(export::BLOCKS_REPOSITORY),
        &interrupted.path().join(export::BLOCKS_REPOSITORY),
    );
    SnapshotDir::create(node::dolos(interrupted.path())).unwrap();
    assert!(node::dolos(interrupted.path())
        .join(stelae::dir::LAYOUT_FILE)
        .exists());

    assert_eq!(publish(interrupted.path()), expected);
    assert_eq!(
        paired::digests(interrupted.path()).dolos,
        paired::digests(whole.path()).dolos
    );
}

/// A plan that names its blocks snapshot, as the publisher host's does, is
/// held to the snapshot lineage's link rule: a root whose snapshot lineage
/// stands two sequences behind is refused.
#[test]
fn a_referenced_root_export_whose_snapshot_lineage_does_not_follow_is_refused() {
    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();

    export::publish(
        root.path(),
        &plan_at_boundary(&domain, 1),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();
    let snapshot = paired::digests(root.path()).dolos;

    let error = export::publish(
        root.path(),
        &plan_at_boundary(&domain, HEAD).with_blocks(some_blocks()),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(
            error,
            Error::LinkBreak {
                latest: 1,
                publishing: HEAD,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(paired::digests(root.path()).dolos, snapshot);
}

/// A plan that names its blocks snapshot is held to the Dolos snapshot
/// lineage's network: a root whose snapshot lineage is another network's is
/// refused.
#[test]
fn a_referenced_root_export_onto_another_networks_snapshot_is_refused() {
    let domain = harness::<MemoryStores>();
    let root = tempfile::tempdir().unwrap();

    let mut foreign = plan_at_boundary(&domain, 1);
    foreign.network = dolos_snapshot::Network::for_magic(42);
    export::publish(
        root.path(),
        &foreign,
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap();
    let snapshot = paired::digests(root.path()).dolos;

    let error = export::publish(
        root.path(),
        &plan_at_boundary(&domain, 2).with_blocks(some_blocks()),
        domain.archive(),
        domain.state(),
        &Observer::silent(),
    )
    .unwrap_err();

    assert!(
        matches!(error, Error::NetworkMismatch { found: 42, .. }),
        "{error:?}"
    );
    assert_eq!(paired::digests(root.path()).dolos, snapshot);
}

/// A blocks reference for the tests that name one.
fn some_blocks() -> stelae::Digest {
    format!("sha256:{}", "ab".repeat(32)).parse().unwrap()
}

/// The blobs in the blocks layout of the root at `root`, if it has any.
fn blocks_blobs(root: &Path) -> Vec<std::ffi::OsString> {
    let blobs = root
        .join(export::BLOCKS_REPOSITORY)
        .join("blobs")
        .join("sha256");

    if !blobs.exists() {
        return Vec::new();
    }

    let mut names: Vec<_> = std::fs::read_dir(blobs)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

/// The sequence of the latest blocks snapshot in the root at `root`, if any.
fn latest_blocks(root: &Path) -> Option<u64> {
    let layout = root.join(export::BLOCKS_REPOSITORY);

    if !layout.join(stelae::dir::LAYOUT_FILE).exists() {
        return None;
    }

    match SnapshotDir::open(layout)
        .unwrap()
        .pull_latest(&stelae_blocks::BlocksProfile)
    {
        Ok(latest) => Some(latest.read_inscription().unwrap().sequence),
        Err(error) if error.is_absent() => None,
        Err(error) => panic!("{error:?}"),
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();

    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());

        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}
