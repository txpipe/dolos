//! A restore records a layer only once what it wrote is durable.
//!
//! The stores commit to buffered journals, so a commit that returned survives
//! a killed process but can be lost to a power loss or an OS crash. The
//! progress file must therefore never name a layer whose writes were not yet
//! synced, or a resume after a power loss would skip a layer the stores no
//! longer hold.
//!
//! The stores here are the real fjall pair, each wrapped by a recorder that
//! logs every commit and persist in order, with the progress file as it stood
//! when the call returned. The assertions read the order off that log. A
//! journal that fails one chosen persist checks the other half: a failed sync
//! stops the restore and leaves the layer it covered unrecorded.

mod node;
mod recorder;

use std::path::Path;

use dolos_core::{ArchiveStore as _, ArchiveWriter as _};
use dolos_snapshot::{
    restore::{self, default_budget, progress_path_in, Checkpoint, Target},
    Error, ENTITY_KINDS,
};
use dolos_testing::toy_domain::{FjallStores, ToyDomain};
use node::{export_plan, export_to, harness, plan_for, Blank};
use recorder::{Entry, Journal, Op, RecordedArchive, RecordedState};
use stelae::{plan::RestoreProgress, progress::Observer, Digest, SnapshotReader as _};

/// Restore `root` into the recorded fjall pair, checkpointing into `storage`,
/// and return the journal of the run with the epoch layers it checkpoints.
fn recorded_restore(root: &Path, storage: &Path, magic: u64) -> (Vec<Entry>, Vec<Digest>) {
    let journal = Journal::new(progress_path_in(storage));
    let (result, epoch_layers) = journaled_restore(root, storage, magic, &journal);
    result.unwrap();

    (journal.take(), epoch_layers)
}

/// The same restore under `journal`, returning its result and the epoch layers
/// it checkpoints, in the order it checkpoints them.
fn journaled_restore(
    root: &Path,
    storage: &Path,
    magic: u64,
    journal: &Journal,
) -> (Result<restore::Summary, Error>, Vec<Digest>) {
    let blank = Blank::<FjallStores>::open();

    let archive = RecordedArchive {
        inner: blank.archive.clone(),
        journal: journal.clone(),
    };
    let state = RecordedState {
        inner: blank.state().clone(),
        journal: journal.clone(),
    };

    let stele = node::resolved(root);
    let chain = stele.chain().unwrap();
    let identity = chain.identity();
    let blocks = node::resolved_blocks(root);
    let blocks_index = blocks.as_ref().map(|blocks| blocks.blob_index().unwrap());
    let plan = restore::plan(
        chain,
        blocks.as_ref().and_then(|blocks| blocks.chain()),
        magic,
        None,
    )
    .unwrap();
    let index = stele.blob_index().unwrap();
    let mut checkpoint = Checkpoint::open(progress_path_in(storage), identity, false).unwrap();

    let epoch_layers: Vec<Digest> = plan
        .layers()
        .map(|layer| layer.diff_id)
        .filter(|diff_id| !plan.tip_layers().any(|tip| tip.diff_id == *diff_id))
        .collect();

    let result = restore::restore(
        &stele,
        &index,
        blocks
            .as_ref()
            .zip(blocks_index.as_ref())
            .map(|(snapshot, index)| restore::Blocks { snapshot, index }),
        &plan,
        Target::new(&archive, &state),
        default_budget(),
        restore::DEFAULT_READ_AHEAD,
        &mut checkpoint,
        &Observer::silent(),
    );

    (result, epoch_layers)
}

/// Entries as a seal writes them, straight into `domain`'s archive, so the
/// stele carries entity layers beside the blocks, logs and indexes.
fn seed_entities(domain: &ToyDomain<FjallStores>) {
    let writer = dolos_core::Domain::archive(domain).start_writer().unwrap();

    for (byte, (slot, (_, kind))) in [100, 200].into_iter().zip(&ENTITY_KINDS).enumerate() {
        let key = vec![byte as u8 + 1; kind.key_len()];
        writer.write_entity(*kind, &key, slot).unwrap();
    }

    writer.commit().unwrap();
}

/// Ruling 12: each epoch layer's `diffId` first appears in the progress file
/// only after an archive persist that returned after that layer's last archive
/// commit or finished ingest; and the state and archive are persisted after the
/// cursor commit and before the progress file is removed.
#[test]
fn a_layer_is_recorded_only_after_its_writes_are_persisted() {
    let domain: ToyDomain<FjallStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());
    seed_entities(&domain);

    // With no stability window the boundary seals the window before it, so
    // its entity layers are listed.
    let mut plan = plan_for(&domain);
    plan.mutable_slots = 0;

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_plan(stele.path(), &domain, &plan);
    let storage = tempfile::tempdir().unwrap();

    assert!(
        inscription
            .layers()
            .any(|layer| layer.kind.starts_with("entity-")),
        "the fixture needs an entity layer, or one layer family goes unchecked"
    );

    let (entries, epoch_layers) = recorded_restore(stele.path(), storage.path(), magic);

    assert!(
        epoch_layers.len() >= 2,
        "the fixture needs several epoch layers for the order to mean anything"
    );

    for layer in &epoch_layers {
        let recorded_at = entries
            .iter()
            .position(|entry| {
                entry
                    .recorded
                    .as_ref()
                    .is_some_and(|set| set.contains(layer))
            })
            .unwrap_or_else(|| panic!("layer {layer:?} was never recorded"));

        let last_commit = entries[..recorded_at]
            .iter()
            .rposition(|entry| matches!(entry.op, Op::ArchiveCommit | Op::ArchiveIngest))
            .unwrap_or_else(|| panic!("layer {layer:?} was recorded before any archive write"));

        assert!(
            entries[last_commit + 1..recorded_at]
                .iter()
                .any(|entry| entry.op == Op::ArchivePersist),
            "layer {layer:?} was recorded without an archive persist after its last commit: {:?}",
            &entries[last_commit..=recorded_at],
        );
    }

    let cursor_commit = entries
        .iter()
        .rposition(|entry| entry.op == Op::StateCommit)
        .expect("the restore committed no state");

    for op in [Op::StatePersist, Op::ArchivePersist] {
        assert!(
            entries[cursor_commit + 1..]
                .iter()
                .any(|entry| entry.op == op && entry.recorded.is_some()),
            "no {op:?} after the cursor commit while the progress file was still there: {:?}",
            &entries[cursor_commit..],
        );
    }

    assert_eq!(
        RestoreProgress::load(&progress_path_in(storage.path())).unwrap(),
        None,
        "a finished restore left its progress file behind"
    );
}

/// Restore with the `nth` persist of `op` failing, and return the error, the
/// layers the progress file was left recording (`None`: no file) and the epoch
/// layers in checkpoint order.
fn restore_failing_persist(op: Op, nth: usize) -> (Error, Option<Vec<Digest>>, Vec<Digest>) {
    let domain: ToyDomain<FjallStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());

    let stele = tempfile::tempdir().unwrap();
    export_to(stele.path(), &domain);
    let storage = tempfile::tempdir().unwrap();

    let journal = Journal::failing_persist(progress_path_in(storage.path()), op, nth);
    let (result, epoch_layers) = journaled_restore(stele.path(), storage.path(), magic, &journal);
    let err = result.expect_err("a restore whose persist failed reported success");

    let recorded = RestoreProgress::load(&progress_path_in(storage.path()))
        .unwrap()
        .map(|progress| progress.completed.into_iter().collect());

    (err, recorded, epoch_layers)
}

/// The persists a clean restore of the fixture makes, by op.
fn persists_of_a_clean_restore(op: Op) -> usize {
    let domain: ToyDomain<FjallStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());

    let stele = tempfile::tempdir().unwrap();
    export_to(stele.path(), &domain);
    let storage = tempfile::tempdir().unwrap();

    let (entries, _) = recorded_restore(stele.path(), storage.path(), magic);
    entries.iter().filter(|entry| entry.op == op).count()
}

/// A failed archive persist after an epoch layer's writes stops the restore
/// with that error, and the progress file never names the layer.
#[test]
fn a_failed_layer_persist_fails_the_restore_and_leaves_the_layer_unrecorded() {
    let (err, recorded, epoch_layers) = restore_failing_persist(Op::ArchivePersist, 1);

    assert!(
        matches!(&err, Error::Archive(dolos_core::ArchiveError::InternalError(reason)) if reason.contains("ArchivePersist 1 failed")),
        "{err:?}"
    );
    assert!(
        !recorded.unwrap_or_default().contains(&epoch_layers[0]),
        "the layer whose persist failed was recorded anyway"
    );
}

/// The final persists, after the cursor commit: if either fails, the restore
/// returns its error and keeps the progress file, every epoch layer still on
/// it.
#[test]
fn a_failed_final_persist_fails_the_restore_and_keeps_the_progress_file() {
    let last_archive = persists_of_a_clean_restore(Op::ArchivePersist);
    assert_eq!(
        persists_of_a_clean_restore(Op::StatePersist),
        1,
        "the fixture's restore is expected to persist the state once, at the end"
    );

    for (op, nth) in [(Op::StatePersist, 1), (Op::ArchivePersist, last_archive)] {
        let (err, recorded, epoch_layers) = restore_failing_persist(op, nth);

        let reason = match &err {
            Error::Archive(dolos_core::ArchiveError::InternalError(reason)) => reason,
            Error::State(dolos_core::StateError::InternalStoreError(reason)) => reason,
            other => panic!("{op:?} {nth}: unexpected error {other:?}"),
        };
        assert!(reason.contains(&format!("{op:?} {nth} failed")), "{err:?}");

        let recorded = recorded
            .unwrap_or_else(|| panic!("{op:?} {nth} failed, yet the progress file was removed"));
        for layer in &epoch_layers {
            assert!(
                recorded.contains(layer),
                "{op:?} {nth}: the kept progress file lost layer {layer:?}"
            );
        }
    }
}
