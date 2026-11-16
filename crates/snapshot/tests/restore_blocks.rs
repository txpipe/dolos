//! A stele whose closed epochs' blocks are in a blocks repository restores
//! them from there.
//!
//! The pair is copied from an ordinary export ([`paired`]), so the export
//! restored alone, from its own per-epoch `blocks` layers, is the control: a
//! paired restore writes the same archive blocks, block by block, and the
//! same node. Every refusal leaves the stores as they were.

mod node;
mod paired;
mod recorder;

use std::path::Path;

use dolos_core::{ArchiveStore as _, BlockSlot, Domain as _, StateStore as _};
use dolos_snapshot::{
    export,
    restore::{self, Input, Repo, RestoreOutcome, Restoring, Target, DEFAULT_READ_AHEAD},
    Error, BLOCKS,
};
use dolos_testing::toy_domain::{MemoryStores, ToyDomain};
use node::{harness, plan_at_boundary, Blank};
use paired::Paired;
use recorder::contents_digest;
use stelae::{dir::SnapshotDir, progress::Observer, Resume, SnapshotReader as _};

/// The boundary the fixture's head is cut at; epochs 0 to 2 are closed.
const HEAD: u64 = 3;

/// The harness published on boundaries 1 to [`HEAD`], and its pair.
struct Fixture {
    magic: u64,
    /// The export, which lists its closed epochs' blocks itself.
    source: tempfile::TempDir,
    /// The export's chain without them.
    dolos: tempfile::TempDir,
    /// The blocks lineage the stele names.
    blocks: tempfile::TempDir,
    paired: Paired,
}

impl Fixture {
    fn new() -> Self {
        let (domain, source) = published(HEAD, |_| None);
        let magic = u64::from(domain.genesis().network_magic());

        let dolos = tempfile::tempdir().unwrap();
        let blocks = tempfile::tempdir().unwrap();
        let paired = paired::pair(
            source.path(),
            &SnapshotDir::create(dolos.path()).unwrap(),
            &SnapshotDir::create(blocks.path()).unwrap(),
        );

        Self {
            magic,
            source,
            dolos,
            blocks,
            paired,
        }
    }

    /// The blocks snapshot the paired head names.
    fn reference(&self) -> stelae::Digest {
        self.paired.blocks[&HEAD]
    }

    /// Restore the layout at `stele`, with `blocks` beside it, into fresh
    /// stores, keeping `max_history` slots of history.
    fn restore(
        &self,
        stele: &Path,
        blocks: Option<&Path>,
        max_history: Option<u64>,
    ) -> (Result<RestoreOutcome, Error>, Blank<MemoryStores>) {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<MemoryStores>::open();

        let outcome = restore::execute(
            Input::Directory {
                path: stele,
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

/// The harness published into a fresh layout on boundaries 1 to `head`, each
/// plan's `position.blocks` set to what `blocks` gives for its sequence.
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

/// The paired stele writes the blocks the export's own layers did, block by
/// block, reading the closed epochs' from the blocks repository and the
/// boundary block, last, from its own tip.
#[test]
fn a_paired_stele_restores_the_blocks_its_own_layers_did() {
    let fixture = Fixture::new();

    let (control, alone) = fixture.restore(fixture.source.path(), None, None);
    let control = control.unwrap();

    let (outcome, paired) =
        fixture.restore(fixture.dolos.path(), Some(fixture.blocks.path()), None);
    let outcome = outcome.unwrap();

    assert_eq!(control.plan.blocks, None);
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
        "the boundary block is not the stele's own, after the closed epochs"
    );

    let blocks = blocks_of(&paired);
    assert!(blocks.len() > HEAD as usize, "{} blocks", blocks.len());
    assert_eq!(blocks, blocks_of(&alone));
    assert_eq!(outcome.summary.blocks, control.summary.blocks);
    assert_eq!(
        outcome.summary.layers_fetched,
        control.summary.layers_fetched
    );
    assert_eq!(
        contents_digest(&paired.archive, paired.state()),
        contents_digest(&alone.archive, alone.state())
    );
}

/// The download and space figures cover both repositories: every planned
/// layer is counted and sized by the transport its repository is read from.
#[test]
fn the_remaining_work_and_the_estimate_cover_both_repositories() {
    let fixture = Fixture::new();

    let (outcome, _) = fixture.restore(fixture.dolos.path(), Some(fixture.blocks.path()), None);
    let outcome = outcome.unwrap();

    assert_eq!(
        outcome.outlook.remaining.layers,
        outcome.plan.layers().count()
    );
    assert_eq!(outcome.outlook.remaining.unsized_layers, 0);

    let stele = node::resolved(fixture.dolos.path());
    let blocks = SnapshotDir::open(fixture.blocks.path())
        .unwrap()
        .resolve_sequence(&stelae_blocks::BlocksProfile, HEAD)
        .unwrap();
    let (index, blocks_index) = (stele.blob_index().unwrap(), blocks.blob_index().unwrap());
    let pair = restore::Blocks {
        snapshot: &blocks,
        index: &blocks_index,
    };

    let estimate = outcome
        .plan
        .estimate(&stele, &index, Some(pair), &Resume::none(), 0)
        .unwrap();

    assert_eq!(estimate.fallback_layers, 0);
    assert!(estimate.blocks > 0);

    // Without the blocks snapshot, the layers it serves have no transport.
    let err = outcome
        .plan
        .estimate(&stele, &index, None, &Resume::none(), 0)
        .unwrap_err();
    assert!(matches!(err, Error::BlocksMissing { .. }), "{err:?}");
}

/// A history window reads the blocks of the closed epochs it keeps and no
/// others, and one that keeps no closed epoch reads no blocks repository.
#[test]
fn a_history_window_reads_the_blocks_of_the_epochs_it_keeps() {
    let fixture = Fixture::new();
    let chain = node::resolved(fixture.dolos.path());
    let chain = chain.chain().unwrap();

    // One slot back from the cut reaches the last slot of epoch 2.
    let (control, alone) = fixture.restore(fixture.source.path(), None, Some(1));
    let (outcome, paired) =
        fixture.restore(fixture.dolos.path(), Some(fixture.blocks.path()), Some(1));
    let (control, outcome) = (control.unwrap(), outcome.unwrap());

    assert_eq!(
        blocks_layers(&outcome)
            .iter()
            .map(|(epoch, repo, _)| (*epoch, *repo))
            .collect::<Vec<_>>(),
        vec![(HEAD - 1, Repo::Blocks), (HEAD, Repo::Dolos)]
    );
    assert_eq!(outcome.plan.skipped_epochs, control.plan.skipped_epochs);
    assert_eq!(blocks_of(&paired), blocks_of(&alone));
    assert_eq!(
        restore::blocks_wanted(chain, Some(1)).unwrap(),
        Some(fixture.reference())
    );

    // A window of nothing keeps the cut's epoch alone, whose block is the
    // stele's: no blocks repository is needed.
    assert_eq!(restore::blocks_wanted(chain, Some(0)).unwrap(), None);

    let (control, alone) = fixture.restore(fixture.source.path(), None, Some(0));
    let (outcome, paired) = fixture.restore(fixture.dolos.path(), None, Some(0));
    let (control, outcome) = (control.unwrap(), outcome.unwrap());

    assert_eq!(outcome.plan.blocks, None);
    assert_eq!(outcome.summary.blocks, control.summary.blocks);
    assert_eq!(blocks_of(&paired), blocks_of(&alone));
}

/// A blocks repository whose snapshot at the stele's sequence is not the one
/// the stele names is refused before anything is written, even when it holds
/// the same blocks: the reference is an identity.
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

    let (outcome, blank) = fixture.restore(fixture.dolos.path(), Some(other.path()), None);

    let Err(Error::BlocksMismatch { expected, found }) = outcome else {
        panic!("{:?}", outcome.map(|o| o.plan.blocks));
    };

    assert_eq!(expected, fixture.reference());
    assert_eq!(found, others[&HEAD]);
    assert!(untouched(&blank));
}

/// A stele whose blocks are in a blocks repository is refused, naming the
/// snapshot, when the restore has no repository, when the one it has is not
/// a repository, and when it holds nothing at the stele's sequence.
#[test]
fn a_missing_blocks_repository_is_refused() {
    let fixture = Fixture::new();

    let empty = tempfile::tempdir().unwrap();

    // The same lineage, one sequence short of the stele.
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
        let (outcome, blank) = fixture.restore(fixture.dolos.path(), blocks, None);

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
}

/// A stele that still lists its closed epochs' `blocks` layers restores from
/// them and reads no blocks repository, even when it names a blocks snapshot
/// and the repository given holds another.
#[test]
fn a_stele_listing_its_own_blocks_reads_no_blocks_repository() {
    let fixture = Fixture::new();
    let (_, named) = published(HEAD, |sequence| Some(fixture.paired.blocks[&sequence]));

    let head = node::resolved(named.path()).read_inscription().unwrap();
    assert_eq!(
        head.position["blocks"],
        serde_json::json!(fixture.reference().to_string())
    );
    assert!(head.diff.iter().any(|layer| layer.kind == BLOCKS));

    let elsewhere = tempfile::tempdir().unwrap();
    paired::blocks_lineage(
        fixture.source.path(),
        &SnapshotDir::create(elsewhere.path()).unwrap(),
        HEAD,
    );

    let (control, alone) = fixture.restore(fixture.source.path(), None, None);
    let control = control.unwrap();

    for blocks in [None, Some(elsewhere.path())] {
        let (outcome, restored) = fixture.restore(named.path(), blocks, None);
        let outcome = outcome.unwrap();

        assert_eq!(outcome.plan.blocks, None, "{blocks:?}");
        assert!(
            blocks_layers(&outcome)
                .iter()
                .all(|(_, repo, media_type)| *repo == Repo::Dolos && media_type == DOLOS_BLOCKS),
            "{blocks:?}"
        );
        assert_eq!(outcome.summary.blocks, control.summary.blocks);
        assert_eq!(blocks_of(&restored), blocks_of(&alone), "{blocks:?}");
    }
}
