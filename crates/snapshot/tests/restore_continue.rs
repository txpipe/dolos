//! A restore that records its source resumes through that record.
//!
//! What `dolos bootstrap stelae --continue` drives: a restore given its
//! source writes the source record before the first layer, and a resume
//! given the same source reads the stele the record names, skips the layers
//! the progress file names, and refuses records it can't trust before
//! anything is written.
//!
//! The stores are the real fjall pair. An interruption is a stop at a chosen
//! commit or persist, from the first layer's to the ones after the cursor,
//! and every resumed node is held against an uninterrupted one by one digest
//! of everything the stores hold.

mod node;
mod recorder;

use std::path::{Path, PathBuf};

use dolos_core::{Domain as _, StateStore as _};
use dolos_snapshot::{
    restore::{
        self, progress_path_in, source_path_in, Input, Restoring, ResumeRefusal, Source,
        SourceRecord, Target,
    },
    Error,
};
use dolos_testing::toy_domain::{FjallStores, ToyDomain};
use node::{export_to, harness, Blank};
use recorder::{contents_digest, Journal, Op, RecordedArchive, RecordedState};
use stelae::{plan::RestoreProgress, Digest};

fn magic_of(domain: &ToyDomain<FjallStores>) -> u64 {
    u64::from(dolos_core::Domain::genesis(domain).network_magic())
}

fn restoring<'a>(
    storage: &'a Path,
    magic: u64,
    resume: bool,
    source: Option<&'a Source>,
) -> Restoring<'a> {
    Restoring {
        network_magic: magic,
        max_history: None,
        storage_path: storage,
        resume,
        skip_space_check: false,
        journals: 0,
        read_ahead: restore::DEFAULT_READ_AHEAD,
        source,
    }
}

/// A stele directory exported from the harness, with the source naming it.
struct Stele {
    domain: ToyDomain<FjallStores>,
    dir: tempfile::TempDir,
    source: Source,
    magic: u64,
}

impl Stele {
    fn export() -> Self {
        let domain = harness::<FjallStores>();
        let magic = magic_of(&domain);
        let dir = tempfile::tempdir().unwrap();
        export_to(dir.path(), &domain);

        Self {
            source: Source::Dir(dir.path().to_path_buf()),
            domain,
            dir,
            magic,
        }
    }

    /// Steles published on boundaries 1 and 2, the second linked to the
    /// first, so the head is not where its chain starts.
    fn chained() -> Self {
        let domain = harness::<FjallStores>();
        let magic = magic_of(&domain);
        let dir = tempfile::tempdir().unwrap();

        for epoch in [1, 2] {
            dolos_snapshot::export::publish(
                dir.path(),
                &node::plan_at_boundary(&domain, epoch),
                domain.archive(),
                domain.state(),
                &stelae::progress::Observer::silent(),
            )
            .unwrap();
        }

        Self {
            source: Source::Dir(dir.path().to_path_buf()),
            domain,
            dir,
            magic,
        }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn digest(&self) -> Digest {
        use stelae::SnapshotReader as _;

        node::resolved(self.path())
            .read_inscription()
            .unwrap()
            .digest()
            .unwrap()
    }

    /// Restore into `blank`, recording into `storage`.
    fn restore(
        &self,
        storage: &Path,
        blank: &Blank<FjallStores>,
        resume: bool,
    ) -> Result<restore::RestoreOutcome, Error> {
        restore::execute(
            Input::Directory(self.path()),
            restoring(storage, self.magic, resume, Some(&self.source)),
            Target::new(&blank.archive, blank.state()),
            None,
        )
    }

    /// The same, through stores that log to `journal`.
    fn restore_recorded(
        &self,
        storage: &Path,
        blank: &Blank<FjallStores>,
        resume: bool,
        journal: &Journal,
    ) -> Result<restore::RestoreOutcome, Error> {
        let archive = RecordedArchive {
            inner: blank.archive.clone(),
            journal: journal.clone(),
        };
        let state = RecordedState {
            inner: blank.state().clone(),
            journal: journal.clone(),
        };

        restore::execute(
            Input::Directory(self.path()),
            restoring(storage, self.magic, resume, Some(&self.source)),
            Target::new(&archive, &state),
            None,
        )
    }

    /// The same, through stores that stop at the `call`-th commit or persist.
    fn stopped_at(&self, storage: &Path, blank: &Blank<FjallStores>, call: usize) -> Error {
        let journal = Journal::crashing_at(progress_path_in(storage), call);

        self.restore_recorded(storage, blank, false, &journal)
            .unwrap_err()
    }

    /// The commits and persists of an uninterrupted recorded restore, in
    /// order, with the digest of the node it leaves.
    fn uninterrupted(&self) -> (Vec<Op>, Digest) {
        self.uninterrupted_under(|journal| journal)
    }

    /// The same, under the journal `adjust` returns.
    fn uninterrupted_under(&self, adjust: impl FnOnce(Journal) -> Journal) -> (Vec<Op>, Digest) {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<FjallStores>::open();
        let journal = adjust(Journal::new(progress_path_in(storage.path())));

        self.restore_recorded(storage.path(), &blank, false, &journal)
            .unwrap();

        let ops = journal.take().into_iter().map(|entry| entry.op).collect();

        (ops, contents_digest(&blank.archive, blank.state()))
    }
}

fn recorded(storage: &Path) -> Option<SourceRecord> {
    SourceRecord::load(&source_path_in(storage)).unwrap()
}

fn progress(storage: &Path) -> Option<RestoreProgress> {
    RestoreProgress::load(&progress_path_in(storage)).unwrap()
}

/// Stopped at a commit anywhere from the first layer's to the persists after
/// the cursor, resumed with the same source, and the node an uninterrupted
/// restore builds, with every layer the progress file named skipped.
#[test]
fn a_restore_stopped_anywhere_resumes_from_its_records_into_the_same_node() {
    let stele = Stele::export();
    let (ops, control) = stele.uninterrupted();

    let calls = |wanted: &[Op]| -> Vec<usize> {
        ops.iter()
            .enumerate()
            .filter(|(_, at)| wanted.contains(at))
            .map(|(index, _)| index + 1)
            .collect()
    };

    // A layer's writes end in a commit, or in an ingest's finish.
    let archive_commits = calls(&[Op::ArchiveCommit, Op::ArchiveIngest]);
    let state_commits = calls(&[Op::StateCommit]);
    let cursor_commit = *state_commits.last().unwrap();

    assert!(archive_commits.len() >= 3, "{ops:?}");
    assert!(state_commits.len() >= 3, "{ops:?}");

    let stops = [
        // The first layer, before anything is recorded.
        archive_commits[0],
        // An epoch layer with one recorded ahead of it.
        archive_commits[1],
        archive_commits[archive_commits.len() / 2],
        // The tip: its first commit, and one in the middle of it.
        state_commits[0],
        state_commits[state_commits.len() / 2],
        // The cursor commit, and the persists after it.
        cursor_commit,
        cursor_commit + 1,
        cursor_commit + 2,
    ];

    for stop in stops {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<FjallStores>::open();

        let error = stele.stopped_at(storage.path(), &blank, stop);
        assert!(error.to_string().contains("stopped at call"), "{error}");

        let record = recorded(storage.path()).expect("a stopped restore left no source record");
        assert_eq!(record.source, stele.source.to_string());
        assert_eq!(record.inscription_digest, stele.digest());

        let skipped = progress(storage.path()).map_or(0, |progress| progress.completed.len());

        let resumed = stele.restore(storage.path(), &blank, true).unwrap();

        assert_eq!(resumed.summary.layers_skipped, skipped, "stopped at {stop}");
        assert_eq!(recorded(storage.path()), None, "stopped at {stop}");
        assert_eq!(progress(storage.path()), None, "stopped at {stop}");
        assert_eq!(
            contents_digest(&blank.archive, blank.state()),
            control,
            "stopped at call {stop} of {ops:?}"
        );
    }
}

/// The archive ingests the blocks and log layers, and leaves the node a
/// restore that writes every layer in batches leaves.
#[test]
fn an_ingesting_restore_leaves_the_node_a_batched_one_does() {
    let stele = Stele::export();

    let (ops, ingested) = stele.uninterrupted();
    let (batched_ops, batched) = stele.uninterrupted_under(Journal::batched);

    assert!(ops.contains(&Op::ArchiveIngest), "{ops:?}");
    assert!(!batched_ops.contains(&Op::ArchiveIngest), "{batched_ops:?}");
    assert_eq!(ingested, batched);
}

/// Stopped at an ingested layer's finish, its runs never finished, and at the
/// persist after it, the layer finished but not recorded: either way the
/// resume fetches the layer again and builds the uninterrupted node.
#[test]
fn a_restore_stopped_around_an_ingest_resumes_into_the_same_node() {
    let stele = Stele::export();
    let (ops, control) = stele.uninterrupted();

    let finishes: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| **op == Op::ArchiveIngest)
        .map(|(index, _)| index + 1)
        .collect();

    assert!(finishes.len() >= 2, "{ops:?}");

    for finish in [finishes[0], finishes[finishes.len() - 1]] {
        assert_eq!(ops[finish], Op::ArchivePersist, "{ops:?}");

        for stop in [finish, finish + 1] {
            let storage = tempfile::tempdir().unwrap();
            let blank = Blank::<FjallStores>::open();

            let error = stele.stopped_at(storage.path(), &blank, stop);
            assert!(error.to_string().contains("stopped at call"), "{error}");

            let skipped = progress(storage.path()).map_or(0, |progress| progress.completed.len());
            let resumed = stele.restore(storage.path(), &blank, true).unwrap();

            assert_eq!(resumed.summary.layers_skipped, skipped, "stopped at {stop}");
            assert_eq!(
                contents_digest(&blank.archive, blank.state()),
                control,
                "stopped at call {stop} of {ops:?}"
            );
        }
    }
}

/// The archive settles once, after the last ingested layer and before the
/// cursor commit.
#[test]
fn an_ingesting_restore_settles_before_the_cursor_commit() {
    let stele = Stele::export();
    let (ops, _) = stele.uninterrupted();

    assert_settled_once(&ops);
}

/// Stopped at the settle, and at the cursor commit after it: the resume has
/// no layer left to ingest, settles again, and builds the uninterrupted node.
#[test]
fn a_restore_stopped_at_or_after_the_settle_settles_again_on_resume() {
    let stele = Stele::export();
    let (ops, control) = stele.uninterrupted();

    let settle = ops.iter().position(|op| *op == Op::ArchiveSettle).unwrap() + 1;
    let cursor_commit = ops.iter().rposition(|op| *op == Op::StateCommit).unwrap() + 1;

    for stop in [settle, cursor_commit] {
        let storage = tempfile::tempdir().unwrap();
        let blank = Blank::<FjallStores>::open();

        let error = stele.stopped_at(storage.path(), &blank, stop);
        assert!(error.to_string().contains("stopped at call"), "{error}");

        let journal = Journal::new(progress_path_in(storage.path()));
        stele
            .restore_recorded(storage.path(), &blank, true, &journal)
            .unwrap();

        let resumed: Vec<Op> = journal.take().into_iter().map(|entry| entry.op).collect();

        assert!(
            !resumed.contains(&Op::ArchiveIngest),
            "stopped at {stop}: {resumed:?}"
        );
        assert_settled_once(&resumed);
        assert_eq!(
            contents_digest(&blank.archive, blank.state()),
            control,
            "stopped at call {stop} of {ops:?}"
        );
    }
}

/// A settle that fails, as a rewrite that ran out of space, fails the restore
/// before its cursor: the node stays unfinished and a resume completes it.
#[test]
fn a_settle_that_fails_fails_the_restore_before_the_cursor() {
    let stele = Stele::export();
    let (_, control) = stele.uninterrupted();

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();
    let journal = Journal::failing_settle(progress_path_in(storage.path()));

    let error = stele
        .restore_recorded(storage.path(), &blank, false, &journal)
        .unwrap_err();

    assert!(error.to_string().contains("the settle failed"), "{error}");
    assert_eq!(blank.state().read_cursor().unwrap(), None);
    assert!(progress(storage.path()).is_some());

    stele.restore(storage.path(), &blank, true).unwrap();
    assert_eq!(contents_digest(&blank.archive, blank.state()), control);
}

fn assert_settled_once(ops: &[Op]) {
    let settles: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| **op == Op::ArchiveSettle)
        .map(|(index, _)| index)
        .collect();
    let cursor_commit = ops.iter().rposition(|op| *op == Op::StateCommit).unwrap();

    assert_eq!(settles.len(), 1, "{ops:?}");
    assert!(settles[0] < cursor_commit, "{ops:?}");

    if let Some(last_ingest) = ops.iter().rposition(|op| *op == Op::ArchiveIngest) {
        assert!(last_ingest < settles[0], "{ops:?}");
    }
}

/// A head that links to an earlier stele resumes the same way: the record
/// names the head, not the inscription its chain starts at.
#[test]
fn a_stopped_restore_of_a_chained_head_resumes_from_its_records() {
    let stele = Stele::chained();
    let (ops, control) = stele.uninterrupted();

    let head = {
        use stelae::SnapshotReader as _;
        node::resolved(stele.path()).read_inscription().unwrap()
    };
    assert_eq!(head.sequence, 2);
    assert!(head.previous.is_some());

    // The tip's first commit, with the epoch layers before it recorded.
    let stop = ops.iter().position(|op| *op == Op::StateCommit).unwrap() + 1;

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();
    stele.stopped_at(storage.path(), &blank, stop);

    let record = recorded(storage.path()).expect("a stopped restore left no source record");
    assert_eq!(record.sequence, head.sequence);
    assert_eq!(record.inscription_digest, head.digest().unwrap());

    let skipped = progress(storage.path()).map_or(0, |progress| progress.completed.len());
    assert!(skipped > 0, "the stop recorded no layer to skip");

    let resumed = stele.restore(storage.path(), &blank, true).unwrap();

    assert_eq!(resumed.summary.layers_skipped, skipped);
    assert_eq!(recorded(storage.path()), None);
    assert_eq!(contents_digest(&blank.archive, blank.state()), control);
}

/// The record's fields are the published contract: the source URL, the
/// stele's sequence and its inscription digest, and nothing else.
#[test]
fn the_source_record_names_the_source_the_sequence_and_the_digest() {
    let stele = Stele::export();
    let (ops, _) = stele.uninterrupted();
    let first_state_commit = ops.iter().position(|op| *op == Op::StateCommit).unwrap() + 1;

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();
    stele.stopped_at(storage.path(), &blank, first_state_commit);

    let raw: serde_json::Value =
        serde_json::from_slice(&std::fs::read(source_path_in(storage.path())).unwrap()).unwrap();

    let inscription = {
        use stelae::SnapshotReader as _;
        node::resolved(stele.path()).read_inscription().unwrap()
    };

    assert_eq!(
        raw,
        serde_json::json!({
            "source": format!("file://{}", stele.path().display()),
            "sequence": inscription.sequence,
            "inscriptionDigest": stele.digest().to_string(),
        })
    );
}

/// With nothing recorded and empty stores, a resume is a restore: every layer
/// fetched and the same node.
#[test]
fn a_resume_with_nothing_recorded_restores_from_the_top() {
    let stele = Stele::export();
    let (_, control) = stele.uninterrupted();

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();

    let outcome = stele.restore(storage.path(), &blank, true).unwrap();

    assert_eq!(outcome.summary.layers_skipped, 0);
    assert_eq!(contents_digest(&blank.archive, blank.state()), control);
    assert_eq!(recorded(storage.path()), None);
}

/// Stop a recorded restore at its first tip commit, leaving the epoch layers
/// recorded.
fn interrupted(stele: &Stele) -> (tempfile::TempDir, Blank<FjallStores>) {
    let (ops, _) = stele.uninterrupted();
    let first_state_commit = ops.iter().position(|op| *op == Op::StateCommit).unwrap() + 1;

    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();
    stele.stopped_at(storage.path(), &blank, first_state_commit);

    assert!(recorded(storage.path()).is_some());
    assert!(progress(storage.path()).is_some_and(|progress| !progress.completed.is_empty()));

    (storage, blank)
}

/// A refused resume wrote nothing: the records and the stores are as the
/// interruption left them.
fn assert_refused_untouched(
    error: Error,
    storage: &Path,
    blank: &Blank<FjallStores>,
    before: (Option<SourceRecord>, Option<RestoreProgress>, Digest),
) -> ResumeRefusal {
    let Error::Resume(refusal) = error else {
        panic!("not a resume refusal: {error:?}");
    };

    assert_eq!(recorded(storage), before.0, "{refusal}");
    assert_eq!(progress(storage), before.1, "{refusal}");
    assert_eq!(contents_digest(&blank.archive, blank.state()), before.2);
    assert_eq!(blank.state().read_cursor().unwrap(), None);

    refusal
}

fn snapshot(
    storage: &Path,
    blank: &Blank<FjallStores>,
) -> (Option<SourceRecord>, Option<RestoreProgress>, Digest) {
    (
        recorded(storage),
        progress(storage),
        contents_digest(&blank.archive, blank.state()),
    )
}

/// A progress file with no record beside it, as a release that kept none
/// leaves, is refused.
#[test]
fn a_progress_file_without_a_record_is_refused() {
    let stele = Stele::export();
    let (storage, blank) = interrupted(&stele);

    SourceRecord::remove(&source_path_in(storage.path())).unwrap();
    let before = snapshot(storage.path(), &blank);

    let error = stele.restore(storage.path(), &blank, true).unwrap_err();
    let refusal = assert_refused_untouched(error, storage.path(), &blank, before);

    assert!(
        matches!(refusal, ResumeRefusal::Unrecorded { .. }),
        "{refusal:?}"
    );
}

/// The recorded directory gone, or holding another stele, is the stele the
/// restore started from no longer being there.
#[test]
fn a_directory_that_no_longer_holds_the_recorded_stele_is_refused() {
    let stele = Stele::export();
    let (storage, blank) = interrupted(&stele);
    let before = snapshot(storage.path(), &blank);

    // Another stele under the same path: a later epoch of the same ledger.
    let moved = tempfile::tempdir().unwrap();
    std::fs::rename(stele.path(), moved.path().join("original")).unwrap();

    let later = harness::<FjallStores>();
    node::stand_at_boundary(&later, 2);
    export_to(stele.path(), &later);

    let error = stele.restore(storage.path(), &blank, true).unwrap_err();
    let refusal = assert_refused_untouched(error, storage.path(), &blank, before.clone());

    let ResumeRefusal::Gone(record) = &refusal else {
        panic!("{refusal:?}");
    };
    assert_eq!(
        record.inscription_digest,
        before.0.clone().unwrap().inscription_digest
    );

    let message = refusal.to_string();
    assert!(message.contains("is no longer in"), "{message}");
    assert!(
        message.contains(&record.inscription_digest.to_string()),
        "{message}"
    );
    assert!(
        message.contains(&format!("epoch-{}", record.sequence)),
        "{message}"
    );

    // And the directory gone altogether.
    std::fs::remove_dir_all(stele.path()).unwrap();

    let error = stele.restore(storage.path(), &blank, true).unwrap_err();
    let refusal = assert_refused_untouched(error, storage.path(), &blank, before);

    assert!(matches!(refusal, ResumeRefusal::Gone(_)), "{refusal:?}");
}

/// A resume given another source than the record's is refused, though it
/// holds the same stele.
#[test]
fn a_resume_from_another_source_is_refused() {
    let stele = Stele::export();
    let (storage, blank) = interrupted(&stele);
    let before = snapshot(storage.path(), &blank);

    let copy = tempfile::tempdir().unwrap();
    copy_dir(stele.path(), copy.path());
    let elsewhere = Source::Dir(copy.path().to_path_buf());

    let error = restore::execute(
        Input::Directory(copy.path()),
        restoring(storage.path(), stele.magic, true, Some(&elsewhere)),
        Target::new(&blank.archive, blank.state()),
        None,
    )
    .unwrap_err();

    let refusal = assert_refused_untouched(error, storage.path(), &blank, before);

    assert!(
        matches!(&refusal, ResumeRefusal::OtherSource { recorded, .. } if *recorded == stele.source.to_string()),
        "{refusal:?}"
    );
}

/// A progress file for another stele than its record names is refused.
#[test]
fn a_progress_file_the_record_disagrees_with_is_refused() {
    let stele = Stele::export();
    let (storage, blank) = interrupted(&stele);

    let mut progress = progress(storage.path()).unwrap();
    progress.inscription_digest = Digest::from_bytes([7; 32]);
    progress.save(&progress_path_in(storage.path())).unwrap();

    let before = snapshot(storage.path(), &blank);

    let error = stele.restore(storage.path(), &blank, true).unwrap_err();
    let refusal = assert_refused_untouched(error, storage.path(), &blank, before);

    assert!(
        matches!(refusal, ResumeRefusal::Disagrees { .. }),
        "{refusal:?}"
    );
}

/// A record that does not parse is refused rather than read as absent.
#[test]
fn a_record_that_does_not_parse_is_refused() {
    let stele = Stele::export();
    let (storage, blank) = interrupted(&stele);

    std::fs::write(source_path_in(storage.path()), b"{\"source\":").unwrap();

    let error = stele.restore(storage.path(), &blank, true).unwrap_err();

    assert!(
        matches!(error, Error::Resume(ResumeRefusal::Unreadable { .. })),
        "{error:?}"
    );
    assert_eq!(blank.state().read_cursor().unwrap(), None);
}

/// A restore that doesn't resume removes the progress an earlier run left
/// before it writes, so stopping it before its first layer is recorded leaves
/// a resume nothing to skip. Its stores are new, as when `--force` runs over
/// storage holding no data.
#[test]
fn a_restore_that_does_not_resume_starts_its_progress_over() {
    let stele = Stele::export();
    let (ops, control) = stele.uninterrupted();
    let first_archive_commit = ops
        .iter()
        .position(|op| matches!(op, Op::ArchiveCommit | Op::ArchiveIngest))
        .unwrap()
        + 1;

    let (storage, _) = interrupted(&stele);

    let blank = Blank::<FjallStores>::open();
    stele.stopped_at(storage.path(), &blank, first_archive_commit);

    assert_eq!(progress(storage.path()), None);
    assert!(recorded(storage.path()).is_some());

    let resumed = stele.restore(storage.path(), &blank, true).unwrap();

    assert_eq!(resumed.summary.layers_skipped, 0);
    assert_eq!(contents_digest(&blank.archive, blank.state()), control);
}

/// A state cursor other than the stele's, as a daemon that synced over the
/// interrupted restore commits, is refused before anything is written,
/// whatever its shape: genesis alone leaves `Origin`.
#[test]
fn a_cursor_another_writer_committed_is_refused() {
    use dolos_core::{ChainPoint, StateWriter as _};

    let stele = Stele::export();

    for theirs in [
        ChainPoint::Origin,
        ChainPoint::Slot(1),
        ChainPoint::Specific(1, [7; 32].into()),
    ] {
        let (storage, blank) = interrupted(&stele);

        let writer = blank.state().start_writer().unwrap();
        writer.set_cursor(theirs.clone()).unwrap();
        writer.commit().unwrap();

        let before = snapshot(storage.path(), &blank);

        let error = stele.restore(storage.path(), &blank, true).unwrap_err();

        let Error::Resume(ResumeRefusal::Written { cursor, point }) = &error else {
            panic!("{theirs:?} not refused as another writer's cursor: {error:?}");
        };
        assert_eq!(*cursor, theirs);
        assert_ne!(*point, theirs);

        assert_eq!(snapshot(storage.path(), &blank), before);
        assert_eq!(blank.state().read_cursor().unwrap(), Some(theirs));
    }
}

/// A restore that records nothing still resumes as it did before records:
/// the progress file alone, whatever stele it was written for.
#[test]
fn a_restore_given_no_source_records_nothing() {
    let stele = Stele::export();
    let storage = tempfile::tempdir().unwrap();
    let blank = Blank::<FjallStores>::open();

    restore::execute(
        Input::Directory(stele.path()),
        restoring(storage.path(), stele.magic, false, None),
        Target::new(&blank.archive, blank.state()),
        None,
    )
    .unwrap();

    assert_eq!(recorded(storage.path()), None);
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        dolos_core::Domain::state(&stele.domain)
            .read_cursor()
            .unwrap()
    );
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target: PathBuf = to.join(entry.file_name());

        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}
