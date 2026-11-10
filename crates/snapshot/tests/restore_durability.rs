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
//! when the call returned. The assertions read the order off that log.

mod node;
mod recorder;

use std::path::Path;

use dolos_core::{ArchiveStore as _, ArchiveWriter as _};
use dolos_snapshot::{
    restore::{self, default_budget, progress_path_in, Checkpoint, Target},
    ENTITY_KINDS,
};
use dolos_testing::toy_domain::{FjallStores, ToyDomain};
use node::{export_to, harness, Blank};
use recorder::{Entry, Journal, Op, RecordedArchive, RecordedState};
use stelae::{dir::SteleDir, plan::RestoreProgress, progress::Observer, Digest, SteleReader as _};

/// Restore `root` into the recorded fjall pair, checkpointing into `storage`,
/// and return the journal of the run with the epoch layers it checkpoints.
fn recorded_restore(root: &Path, storage: &Path, magic: u64) -> (Vec<Entry>, Vec<Digest>) {
    let blank = Blank::<FjallStores>::open();
    let journal = Journal::new(progress_path_in(storage));

    let archive = RecordedArchive {
        inner: blank.archive.clone(),
        journal: journal.clone(),
    };
    let state = RecordedState {
        inner: blank.state().clone(),
        journal: journal.clone(),
    };

    let stele = SteleDir::open(root).unwrap();
    let identity = stele.read_inscription().unwrap().digest().unwrap();
    let plan = restore::plan(&stele, magic, None).unwrap();
    let index = stele.blob_index().unwrap();
    let mut checkpoint = Checkpoint::open(progress_path_in(storage), identity, false).unwrap();

    let epoch_layers: Vec<Digest> = plan
        .layers()
        .map(|layer| layer.diff_id)
        .filter(|diff_id| !plan.tip_layers().any(|tip| tip.diff_id == *diff_id))
        .collect();

    restore::restore(
        &stele,
        &index,
        &plan,
        Target::new(&archive, &state),
        default_budget(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap();

    (journal.take(), epoch_layers)
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
/// commit; and the state and archive are persisted after the cursor commit and
/// before the progress file is removed.
#[test]
fn a_layer_is_recorded_only_after_its_writes_are_persisted() {
    let domain: ToyDomain<FjallStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());
    seed_entities(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);
    let storage = tempfile::tempdir().unwrap();

    assert!(
        inscription
            .layers
            .iter()
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
            .rposition(|entry| entry.op == Op::ArchiveCommit)
            .unwrap_or_else(|| panic!("layer {layer:?} was recorded before any archive commit"));

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
