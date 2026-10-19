mod common;
mod node;

use dolos_core::{
    ArchiveStore, ArchiveWriter as _, ChainPoint, Domain as _, RelationContext, RelationRecord,
    RelationSource, StateStore, StateWriter as _,
};
use dolos_snapshot::{
    export::{self, First},
    layers::committee::{self, CommitteeScope, Record},
    restore::{self, Checkpoint, Target},
    DolosProfile, Error, RetainedEpochs, Scope,
};
use dolos_testing::toy_domain::{FjallStores, MemoryStores, ToyDomain, ToyStores};
use stelae::{dir::SteleDir, frame, progress::Observer, SteleReader, SteleWriter};

fn witness(cold: u8, hot: u8, slot: u64) -> RelationRecord {
    RelationRecord {
        source: RelationSource {
            dimension: committee::DIMENSION.to_owned(),
            subject: [vec![cold], vec![0xab; 28]].concat(),
            object: [vec![hot], vec![0xcd; 28]].concat(),
            source_slot: slot,
        },
        observed_at: Some(10),
        active: true,
    }
}

fn context() -> RelationContext {
    RelationContext {
        version: RelationContext::VERSION,
        complete: true,
        initialized_at: 5,
    }
}

fn records<A: ArchiveStore>(archive: &A) -> Vec<RelationRecord> {
    archive
        .iter_relations(committee::DIMENSION, None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn restore_into<B: ToyStores>(
    stele: &SteleDir,
    blank: &node::Blank<B>,
    magic: u64,
) -> Result<restore::Summary, Error> {
    let plan = restore::plan(stele, magic, Some(0))?;
    restore::restore(
        stele,
        &stele.blob_index()?,
        &plan,
        Target::new(&blank.archive, blank.state()),
        restore::Budget {
            commit_records: 1,
            commit_bytes: 1,
            ..restore::default_budget()
        },
        &mut Checkpoint::none(),
        &Observer::silent(),
    )
}

fn restricted_roundtrip<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let magic = u64::from(domain.genesis().network_magic());
    let plan = node::plan_at_boundary(&domain, 1, RetainedEpochs::default())
        .restrict_epochs(Some(1), None);
    let tip = plan.cursor.slot();
    let mut protected = witness(0x12, 0x02, 1);
    protected.observed_at = Some(tip + 100);
    let mut inactive = witness(0x13, 0x03, 2);
    inactive.active = false;
    inactive.observed_at = Some(tip + 200);
    let raw = RelationRecord::new(witness(0x13, 0x02, 3).source);
    let reused = RelationRecord::new(witness(0x12, 0x02, 4).source);
    let writer = domain.archive().start_writer().unwrap();
    for record in [&protected, &inactive, &raw, &reused] {
        writer.put_relation(record).unwrap();
    }
    let mut exported_context = context();
    exported_context.initialized_at = tip + 100;
    writer
        .set_relation_context(committee::DIMENSION, &exported_context)
        .unwrap();
    writer.commit().unwrap();
    let root = tempfile::tempdir().unwrap();
    let inscription = node::export_plan(root.path(), &domain, &plan);
    let stele = SteleDir::open(root.path()).unwrap();
    let descriptor = inscription.layers_of_kind(committee::KIND).next().unwrap();
    assert_eq!(descriptor.scope["required"], true);
    assert_eq!(descriptor.scope["point"], inscription.position["point"]);
    assert!(!dolos_snapshot::is_inheritable(
        committee::KIND,
        &descriptor.scope
    ));
    assert_eq!(descriptor.records, 8);
    let encoded = common::read_both_ways(&stele, &stele.blob_index().unwrap(), descriptor);
    let decoded: Vec<_> = encoded
        .iter()
        .map(|record| committee::decode(record).unwrap())
        .collect();
    assert_eq!(decoded.len(), 7);
    assert_eq!(records(domain.archive()).len(), 4);
    assert_eq!(
        domain
            .archive()
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(exported_context.clone())
    );
    assert!(records(domain.archive())
        .iter()
        .any(|record| record.observed_at.is_some_and(|slot| slot > tip)));
    let blank = node::Blank::<B>::open();
    let stale = witness(0x12, 0x03, 4);
    let writer = blank.archive.start_writer().unwrap();
    writer.put_relation(&stale).unwrap();
    writer.commit().unwrap();
    let summary = restore_into(&stele, &blank, magic).unwrap();
    assert_eq!(summary.blocks, 1);
    assert_eq!(summary.relations, 4);
    exported_context.initialized_at = tip;
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(exported_context)
    );
    protected.observed_at = Some(tip);
    inactive.observed_at = Some(tip);
    let mut expected = vec![protected, inactive, raw, reused];
    expected.sort_by(|a, b| a.source.cmp(&b.source));
    assert_eq!(records(&blank.archive), expected);
    let writer = blank.archive.start_writer().unwrap();
    writer.put_relation(&stale).unwrap();
    let mut changed = expected[0].clone();
    changed.observed_at = Some(tip + 999);
    writer.put_relation(&changed).unwrap();
    writer.commit().unwrap();
    assert!(restore_into(&stele, &blank, magic).is_err());
    let blank = node::Blank {
        archive: blank.archive.clone(),
        stores: B::open(),
    };
    restore_into(&stele, &blank, magic).unwrap();
    assert_eq!(records(&blank.archive), expected);
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        Some(plan.cursor.clone())
    );
    let reproduced = export::reproduce(&plan, &blank.archive, blank.state(), None, &First).unwrap();
    assert_eq!(reproduced.digest().unwrap(), inscription.digest().unwrap());
    let context_layer = inscription.layers_of_kind(committee::KIND).next().unwrap();
    let mut checkpointed = stelae::plan::RestoreProgress::new(inscription.digest().unwrap());
    checkpointed.record(context_layer.diff_id);
    let storage = tempfile::tempdir().unwrap();
    checkpointed
        .save(&restore::progress_path_in(storage.path()))
        .unwrap();
    let mut checkpoint = Checkpoint::open(
        restore::progress_path_in(storage.path()),
        inscription.digest().unwrap(),
        true,
    )
    .unwrap();
    let restore_plan = restore::plan(&stele, magic, Some(0)).unwrap();
    let writer = blank.archive.start_writer().unwrap();
    writer.put_relation(&stale).unwrap();
    writer.commit().unwrap();
    let blank = node::Blank {
        archive: blank.archive.clone(),
        stores: B::open(),
    };
    let resumed = restore::restore(
        &stele,
        &stele.blob_index().unwrap(),
        &restore_plan,
        Target::new(&blank.archive, blank.state()),
        restore::default_budget(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap();
    assert_eq!(resumed.relations, 4);
    assert_eq!(records(&blank.archive), expected);
}

#[test]
fn restricted_history_preserves_context_on_memory() {
    restricted_roundtrip::<MemoryStores>();
}

#[test]
fn restricted_history_preserves_context_on_fjall() {
    restricted_roundtrip::<FjallStores>();
}

fn legacy_restore<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let root = tempfile::tempdir().unwrap();
    let mut inscription = node::export_to(root.path(), &domain);
    inscription
        .layers
        .retain(|layer| layer.kind != committee::KIND);
    let stele = SteleDir::open(root.path()).unwrap();
    stele.seal(&DolosProfile, &inscription).unwrap();
    let blank = node::Blank::<B>::open();
    let writer = blank.archive.start_writer().unwrap();
    writer.put_relation(&witness(0x12, 0x02, 1)).unwrap();
    writer
        .set_relation_context(committee::DIMENSION, &context())
        .unwrap();
    writer.commit().unwrap();
    restore_into(&stele, &blank, u64::from(domain.genesis().network_magic())).unwrap();
    assert!(records(&blank.archive).is_empty());
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(RelationContext {
            version: RelationContext::VERSION,
            complete: false,
            initialized_at: inscription.position["point"]["slot"].as_u64().unwrap(),
        })
    );
}

#[test]
fn legacy_restore_clears_witnesses_and_marks_context_incomplete_on_memory() {
    legacy_restore::<MemoryStores>();
}

#[test]
fn legacy_restore_clears_witnesses_and_marks_context_incomplete_on_fjall() {
    legacy_restore::<FjallStores>();
}

fn initialized_incomplete_roundtrip<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let mut incomplete = context();
    incomplete.complete = false;
    let writer = domain.archive().start_writer().unwrap();
    writer
        .set_relation_context(committee::DIMENSION, &incomplete)
        .unwrap();
    writer.put_relation(&witness(0x12, 0x02, 1)).unwrap();
    writer.commit().unwrap();
    let root = tempfile::tempdir().unwrap();
    let inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let blank = node::Blank::<B>::open();
    restore_into(&stele, &blank, u64::from(domain.genesis().network_magic())).unwrap();
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(incomplete)
    );
    assert_eq!(records(&blank.archive), records(domain.archive()));
    let reproduced = export::reproduce(
        &node::plan_for(&domain),
        &blank.archive,
        blank.state(),
        None,
        &First,
    )
    .unwrap();
    assert_eq!(reproduced.digest().unwrap(), inscription.digest().unwrap());
}

#[test]
fn initialized_upgrade_gap_survives_restore_on_memory() {
    initialized_incomplete_roundtrip::<MemoryStores>();
}

#[test]
fn initialized_upgrade_gap_survives_restore_on_fjall() {
    initialized_incomplete_roundtrip::<FjallStores>();
}

#[test]
fn uninitialized_export_is_explicitly_incomplete() {
    let domain: ToyDomain = node::harness();
    let archive =
        dolos_core::builtin::MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let root = tempfile::tempdir().unwrap();
    let inscription = export::publish(
        root.path(),
        &node::plan_for(&domain),
        &archive,
        domain.state(),
        None,
        &Observer::silent(),
    )
    .unwrap();
    let stele = SteleDir::open(root.path()).unwrap();
    let descriptor = inscription.layers_of_kind(committee::KIND).next().unwrap();
    let layer = stele
        .read_layer(&stele.blob_index().unwrap(), &DolosProfile, descriptor)
        .unwrap();
    let decoded = committee::decode(layer.records().next().unwrap().unwrap()).unwrap();
    assert_eq!(
        decoded,
        Record::Context(RelationContext {
            version: RelationContext::VERSION,
            complete: false,
            initialized_at: node::plan_for(&domain).cursor.slot(),
        })
    );
}

fn replace_layer(
    stele: &SteleDir,
    inscription: &mut stelae::inscription::Inscription,
    scope: &CommitteeScope,
    records: &[frame::CanonicalCbor],
) {
    let written = stele
        .write_layer(
            &DolosProfile,
            &scope.layer_spec(committee::KIND).unwrap(),
            dolos_snapshot::COMPRESSION_LEVEL,
            records,
        )
        .unwrap();
    inscription
        .layers
        .retain(|layer| layer.kind != committee::KIND);
    inscription.layers.push(written.descriptor);
    stele.seal(&DolosProfile, inscription).unwrap();
}

fn malformed_context<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let tip = node::plan_for(&domain).cursor;
    let good = committee::encode(&Record::Context(context())).unwrap();
    let backfill = committee::encode(&Record::Backfill(committee::reset_backfill())).unwrap();
    let rollback = committee::encode(&Record::Rollback(committee::reset_rollback())).unwrap();
    let valid = committee::encode(&Record::Witness(witness(0x12, 0x02, 1))).unwrap();
    let future = committee::encode(&Record::Witness(witness(0x12, 0x02, tip.slot() + 1))).unwrap();
    let unknown_version = frame::encode(|e| {
        e.array(4)?.u64(0)?.u64(2)?.bool(true)?.u64(5)?;
        Ok(())
    })
    .unwrap();
    let unknown_tag = frame::encode(|e| {
        e.array(4)?.u64(4)?.u64(1)?.bool(true)?.u64(5)?;
        Ok(())
    })
    .unwrap();
    let bad_credential = frame::encode(|e| {
        e.array(6)?
            .u64(3)?
            .bytes(&[0x12; 28])?
            .bytes(&[0x02; 29])?
            .u64(1)?
            .null()?
            .bool(false)?;
        Ok(())
    })
    .unwrap();
    let pending = frame::encode(|e| {
        e.array(4)?.u64(2)?.u64(1)?.bool(false)?.u64(5)?;
        Ok(())
    })
    .unwrap();
    let unfinished = frame::encode(|e| {
        e.array(4)?.u64(1)?.u64(1)?.bool(false)?.u64(5)?;
        Ok(())
    })
    .unwrap();
    for forged in [
        vec![],
        vec![good.clone(), backfill.clone(), pending],
        vec![good.clone(), unfinished, rollback.clone()],
        vec![
            good.clone(),
            backfill.clone(),
            rollback.clone(),
            valid.clone(),
            rollback.clone(),
        ],
        vec![valid.clone()],
        vec![good.clone(), good.clone()],
        vec![
            good.clone(),
            backfill.clone(),
            rollback.clone(),
            valid.clone(),
            valid.clone(),
        ],
        vec![good.clone(), backfill.clone(), rollback.clone(), future],
        vec![
            good.clone(),
            backfill.clone(),
            rollback.clone(),
            bad_credential,
        ],
        vec![good.clone(), rollback.clone(), backfill.clone()],
        vec![
            good.clone(),
            backfill.clone(),
            backfill.clone(),
            rollback.clone(),
        ],
        vec![good.clone(), backfill.clone()],
        vec![unknown_version],
        vec![unknown_tag],
        vec![
            good.clone(),
            backfill.clone(),
            rollback.clone(),
            valid.clone(),
            committee::encode(&Record::Witness(witness(0x12, 0x02, 0))).unwrap(),
        ],
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut inscription = node::export_to(root.path(), &domain);
        let stele = SteleDir::open(root.path()).unwrap();
        replace_layer(
            &stele,
            &mut inscription,
            &CommitteeScope {
                network_magic: 2,
                point: tip.clone(),
            },
            &forged,
        );
        let blank = node::Blank::<B>::open();
        assert!(restore_into(&stele, &blank, 2).is_err());
        assert!(blank.state().read_cursor().unwrap().is_none());
        assert!(
            !blank
                .archive
                .relation_context(committee::DIMENSION)
                .unwrap()
                .unwrap()
                .complete
        );
        replace_layer(
            &stele,
            &mut inscription,
            &CommitteeScope {
                network_magic: 2,
                point: tip.clone(),
            },
            &[
                good.clone(),
                backfill.clone(),
                rollback.clone(),
                valid.clone(),
            ],
        );
        restore_into(&stele, &blank, 2).unwrap();
        assert_eq!(records(&blank.archive), vec![witness(0x12, 0x02, 1)]);
        assert_eq!(
            blank
                .archive
                .relation_context(committee::DIMENSION)
                .unwrap(),
            Some(context())
        );
        assert_eq!(blank.state().read_cursor().unwrap(), Some(tip.clone()));
    }
}

#[test]
fn malformed_context_never_completes_restore_on_memory() {
    malformed_context::<MemoryStores>();
}

#[test]
fn malformed_context_never_completes_restore_on_fjall() {
    malformed_context::<FjallStores>();
}

#[test]
fn mismatched_header_position_is_refused() {
    let domain: ToyDomain = node::harness();
    let root = tempfile::tempdir().unwrap();
    let mut inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let tip = node::plan_for(&domain).cursor;
    replace_layer(
        &stele,
        &mut inscription,
        &CommitteeScope {
            network_magic: 2,
            point: ChainPoint::Specific(tip.slot() + 1, [0xaa; 32].into()),
        },
        &[committee::encode(&Record::Context(context())).unwrap()],
    );
    let descriptor = inscription
        .layers
        .iter_mut()
        .find(|layer| layer.kind == committee::KIND)
        .unwrap();
    descriptor.scope["point"] = inscription.position["point"].clone();
    stele.seal(&DolosProfile, &inscription).unwrap();
    let blank = node::Blank::<MemoryStores>::open();
    assert!(restore_into(&stele, &blank, 2).is_err());
    assert!(blank.state().read_cursor().unwrap().is_none());
}

#[test]
fn restore_accepts_conservative_observations_above_the_tip() {
    let domain: ToyDomain = node::harness();
    let plan = node::plan_for(&domain);
    let tip = plan.cursor.slot();
    let root = tempfile::tempdir().unwrap();
    let mut inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let mut observed = witness(0x12, 0x02, 1);
    observed.observed_at = Some(tip + 100);
    replace_layer(
        &stele,
        &mut inscription,
        &CommitteeScope {
            network_magic: u64::from(domain.genesis().network_magic()),
            point: plan.cursor,
        },
        &[
            committee::encode(&Record::Context(context())).unwrap(),
            committee::encode(&Record::Backfill(committee::reset_backfill())).unwrap(),
            committee::encode(&Record::Rollback(committee::reset_rollback())).unwrap(),
            committee::encode(&Record::Witness(observed.clone())).unwrap(),
        ],
    );
    let blank = node::Blank::<MemoryStores>::open();
    restore_into(&stele, &blank, u64::from(domain.genesis().network_magic())).unwrap();
    assert_eq!(records(&blank.archive), vec![observed]);
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(context())
    );
}

#[test]
fn export_refuses_sources_above_the_snapshot_position() {
    let domain: ToyDomain = node::harness();
    let plan = node::plan_for(&domain);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .put_relation(&witness(0x12, 0x02, plan.cursor.slot() + 1))
        .unwrap();
    writer.commit().unwrap();
    assert!(export::reproduce(&plan, domain.archive(), domain.state(), None, &First).is_err());
}

#[test]
fn export_refuses_a_stale_plan() {
    let domain: ToyDomain = node::harness();
    let plan = node::plan_for(&domain);
    let writer = domain.state().start_writer().unwrap();
    writer
        .set_cursor(ChainPoint::Specific(
            plan.cursor.slot() + 1,
            [0xaa; 32].into(),
        ))
        .unwrap();
    writer.commit().unwrap();
    assert!(export::reproduce(&plan, domain.archive(), domain.state(), None, &First).is_err());
}

#[test]
fn context_scope_is_required_position_bound_and_never_immutable() {
    let domain: ToyDomain = node::harness();
    let root = tempfile::tempdir().unwrap();
    let original = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let descriptor = original
        .layers_of_kind(committee::KIND)
        .next()
        .unwrap()
        .clone();
    let plan = restore::plan(&stele, 2, Some(0)).unwrap();
    assert!(plan.tip_layers().any(|layer| layer.kind == committee::KIND));
    assert!(!plan
        .immutable_layers()
        .any(|layer| layer.kind == committee::KIND));
    let mut progress = stelae::plan::RestoreProgress::new(original.digest().unwrap());
    progress.record(descriptor.diff_id);
    assert!(plan
        .remaining_layers(&progress.resume())
        .any(|layer| layer.kind == committee::KIND));
    for mutation in 0..3 {
        let mut inscription = original.clone();
        let at = inscription
            .layers
            .iter()
            .position(|layer| layer.kind == committee::KIND)
            .unwrap();
        match mutation {
            0 => inscription.layers[at].scope["required"] = serde_json::json!(false),
            1 => inscription.layers[at].scope["point"]["slot"] = serde_json::json!(0),
            _ => inscription.layers.push(descriptor.clone()),
        }
        stele.seal(&DolosProfile, &inscription).unwrap();
        assert!(restore::plan(&stele, 2, Some(0)).is_err());
    }
}

#[test]
fn unsupported_committee_media_types_are_refused_before_restore() {
    let domain: ToyDomain = node::harness();
    let root = tempfile::tempdir().unwrap();
    let original = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    for media_type in [
        "application/vnd.dolos.stele.archive-context-committee.v2+zstd",
        "application/vnd.dolos.stele.archive-context-committee.v1+gzip",
        "application/vnd.other.stele.archive-context-committee.v1+zstd",
        "application/vnd.dolos.stele.blocks.v1+zstd",
    ] {
        let mut inscription = original.clone();
        inscription
            .layers
            .iter_mut()
            .find(|layer| layer.kind == committee::KIND)
            .unwrap()
            .media_type = media_type.to_owned();
        stele.seal(&DolosProfile, &inscription).unwrap();
        let blank = node::Blank::<MemoryStores>::open();
        assert!(restore_into(&stele, &blank, 2).is_err(), "{media_type}");
        assert!(blank.state().read_cursor().unwrap().is_none());
        assert!(blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap()
            .is_none());
    }
}

fn completed_target_refusal<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let root = tempfile::tempdir().unwrap();
    let mut inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let blank = node::Blank::<B>::open();
    restore_into(&stele, &blank, 2).unwrap();
    let cursor = blank.state().read_cursor().unwrap();
    let metadata = blank
        .archive
        .relation_context(committee::DIMENSION)
        .unwrap();
    let writer = blank.archive.start_writer().unwrap();
    let protected = witness(0x12, 0x02, 1);
    writer.put_relation(&protected).unwrap();
    writer.commit().unwrap();
    replace_layer(
        &stele,
        &mut inscription,
        &CommitteeScope {
            network_magic: 2,
            point: cursor.clone().unwrap(),
        },
        &[],
    );
    assert!(restore_into(&stele, &blank, 2).is_err());
    assert_eq!(blank.state().read_cursor().unwrap(), cursor);
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        metadata
    );
    assert_eq!(records(&blank.archive), vec![protected]);
}

#[test]
fn failed_repeated_restore_preserves_completed_target_on_memory() {
    completed_target_refusal::<MemoryStores>();
}

#[test]
fn failed_repeated_restore_preserves_completed_target_on_fjall() {
    completed_target_refusal::<FjallStores>();
}

#[test]
fn verified_completed_checkpoint_only_finishes_cleanup() {
    let domain: ToyDomain = node::harness();
    let root = tempfile::tempdir().unwrap();
    let inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let blank = node::Blank::<MemoryStores>::open();
    restore_into(&stele, &blank, 2).unwrap();
    let plan = restore::plan(&stele, 2, None).unwrap();
    let storage = tempfile::tempdir().unwrap();
    let path = restore::progress_path_in(storage.path());
    let mut progress = stelae::plan::RestoreProgress::new(inscription.digest().unwrap());
    for descriptor in plan.immutable_layers() {
        progress.record(descriptor.diff_id);
    }
    progress.save(&path).unwrap();
    let mut checkpoint =
        Checkpoint::open(path.clone(), inscription.digest().unwrap(), true).unwrap();
    let summary = restore::restore(
        &stele,
        &stele.blob_index().unwrap(),
        &plan,
        Target::new(&blank.archive, blank.state()),
        restore::default_budget(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap();
    assert_eq!(summary.layers_fetched, 0);
    assert_eq!(summary.layers_skipped, plan.layers().count());
    assert!(!path.exists());
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        Some(plan.position.point.clone())
    );
    let writer = blank.archive.start_writer().unwrap();
    let stale = witness(0x12, 0x02, 1);
    writer.put_relation(&stale).unwrap();
    writer.commit().unwrap();
    progress.save(&path).unwrap();
    let mut checkpoint =
        Checkpoint::open(path.clone(), inscription.digest().unwrap(), true).unwrap();
    assert!(restore::restore(
        &stele,
        &stele.blob_index().unwrap(),
        &plan,
        Target::new(&blank.archive, blank.state()),
        restore::default_budget(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .is_err());
    assert!(path.exists());
    assert_eq!(records(&blank.archive), vec![stale]);
    assert_eq!(
        blank.state().read_cursor().unwrap(),
        Some(plan.position.point)
    );
}

#[test]
fn export_refuses_an_archive_behind_the_state_or_on_another_hash() {
    let domain: ToyDomain = node::harness();
    let plan = node::plan_for(&domain);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .set_relation_context(committee::DIMENSION, &context())
        .unwrap();
    writer.commit().unwrap();
    let writer = domain.state().start_writer().unwrap();
    writer
        .set_cursor(ChainPoint::Specific(plan.cursor.slot(), [0xaa; 32].into()))
        .unwrap();
    writer.commit().unwrap();
    let forked = node::plan_for(&domain);
    assert!(export::reproduce(&forked, domain.archive(), domain.state(), None, &First).is_err());
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(plan.cursor.clone()).unwrap();
    writer.commit().unwrap();
    domain
        .archive()
        .truncate_front(&ChainPoint::Slot(plan.cursor.slot() - 1))
        .unwrap();
    assert!(export::reproduce(&plan, domain.archive(), domain.state(), None, &First).is_err());
    domain
        .archive()
        .truncate_front(&ChainPoint::Origin)
        .unwrap();
    assert!(export::reproduce(&plan, domain.archive(), domain.state(), None, &First).is_err());
}

fn auxiliary_roundtrip<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let plan = node::plan_for(&domain);
    let tip = plan.cursor.slot();
    let mut incomplete = context();
    incomplete.complete = false;
    let backfill = RelationContext {
        initialized_at: tip - 1,
        ..context()
    };
    let rollback = RelationContext {
        initialized_at: tip + 100,
        ..context()
    };
    let writer = domain.archive().start_writer().unwrap();
    writer
        .set_relation_context(committee::DIMENSION, &incomplete)
        .unwrap();
    writer
        .set_relation_context(committee::BACKFILL, &backfill)
        .unwrap();
    writer
        .set_relation_context(committee::ROLLBACK, &rollback)
        .unwrap();
    writer.commit().unwrap();
    let root = tempfile::tempdir().unwrap();
    let inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    let blank = node::Blank::<B>::open();
    restore_into(&stele, &blank, 2).unwrap();
    assert_eq!(
        blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap(),
        Some(incomplete)
    );
    assert_eq!(
        blank.archive.relation_context(committee::BACKFILL).unwrap(),
        Some(backfill)
    );
    assert_eq!(
        blank.archive.relation_context(committee::ROLLBACK).unwrap(),
        Some(RelationContext {
            initialized_at: tip,
            ..rollback.clone()
        })
    );
    assert_eq!(
        domain
            .archive()
            .relation_context(committee::ROLLBACK)
            .unwrap(),
        Some(rollback)
    );
    assert_eq!(
        export::reproduce(&plan, &blank.archive, blank.state(), None, &First)
            .unwrap()
            .digest()
            .unwrap(),
        inscription.digest().unwrap()
    );
    assert!(dolos_cardano::committee::prepare_stores(blank.state(), &blank.archive).unwrap());
    assert!(
        !blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn auxiliary_metadata_roundtrips_on_memory() {
    auxiliary_roundtrip::<MemoryStores>();
}

#[test]
fn auxiliary_metadata_roundtrips_on_fjall() {
    auxiliary_roundtrip::<FjallStores>();
}

fn pending_rollback<B: ToyStores>() {
    let domain: ToyDomain<B> = node::harness();
    let plan = node::plan_for(&domain);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .set_relation_context(
            committee::ROLLBACK,
            &RelationContext {
                complete: false,
                initialized_at: plan.cursor.slot() + 10,
                ..context()
            },
        )
        .unwrap();
    writer.commit().unwrap();
    let root = tempfile::tempdir().unwrap();
    let error = export::publish(
        root.path(),
        &plan,
        domain.archive(),
        domain.state(),
        None,
        &Observer::silent(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("The committee rollback is not finished."),
        "{error}"
    );
    assert!(!root.path().join("inscription.json").exists());
}

#[test]
fn pending_rollback_prevents_export_on_memory() {
    pending_rollback::<MemoryStores>();
}

#[test]
fn pending_rollback_prevents_export_on_fjall() {
    pending_rollback::<FjallStores>();
}

fn backfill_restarts<B: ToyStores>(legacy: bool) {
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use pallas::ledger::{
        primitives::{conway::Certificate, StakeCredential},
        traverse::MultiEraBlock,
    };

    let domain: ToyDomain<B> = node::harness();
    let plan = node::plan_for(&domain);
    let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 1,
        txs_per_block: 1,
        slot: 100,
        extra_certs_by_block: vec![vec![vec![Certificate::AuthCommitteeHot(
            StakeCredential::AddrKeyhash([0xab; 28].into()),
            StakeCredential::AddrKeyhash([0xcd; 28].into()),
        )]]],
        ..Default::default()
    });
    let block = MultiEraBlock::decode(&blocks[0]).unwrap();
    let expected = dolos_cardano::indexes::committee_authorization_sources(&block);
    assert_eq!(expected.len(), 1);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(
            &ChainPoint::Specific(block.slot(), block.hash()),
            &blocks[0],
        )
        .unwrap();
    writer
        .set_relation_context(
            committee::DIMENSION,
            &RelationContext {
                complete: false,
                ..context()
            },
        )
        .unwrap();
    let progress = RelationContext {
        complete: false,
        initialized_at: plan.cursor.slot(),
        ..context()
    };
    writer
        .set_relation_context(committee::BACKFILL, &progress)
        .unwrap();
    writer.commit().unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut inscription = node::export_to(root.path(), &domain);
    let stele = SteleDir::open(root.path()).unwrap();
    if legacy {
        inscription
            .layers
            .retain(|layer| layer.kind != committee::KIND);
        stele.seal(&DolosProfile, &inscription).unwrap();
    }
    let blank = node::Blank::<B>::open();
    let writer = blank.archive.start_writer().unwrap();
    writer
        .set_relation_context(committee::BACKFILL, &context())
        .unwrap();
    writer
        .set_relation_context(
            committee::ROLLBACK,
            &RelationContext {
                complete: false,
                ..context()
            },
        )
        .unwrap();
    writer.commit().unwrap();
    restore_into(&stele, &blank, 2).unwrap();
    assert!(records(&blank.archive).is_empty());
    assert_eq!(
        blank.archive.relation_context(committee::BACKFILL).unwrap(),
        Some(committee::reset_backfill())
    );
    assert_eq!(
        blank.archive.relation_context(committee::ROLLBACK).unwrap(),
        Some(committee::reset_rollback())
    );
    assert_eq!(
        domain
            .archive()
            .relation_context(committee::BACKFILL)
            .unwrap(),
        Some(progress)
    );
    assert!(dolos_cardano::committee::prepare_stores(blank.state(), &blank.archive).unwrap());
    assert_eq!(
        records(&blank.archive),
        expected
            .into_iter()
            .map(RelationRecord::new)
            .collect::<Vec<_>>()
    );
    assert!(
        blank
            .archive
            .relation_context(committee::BACKFILL)
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(
        !blank
            .archive
            .relation_context(committee::DIMENSION)
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn unfinished_and_legacy_backfill_replay_retained_blocks_on_memory() {
    backfill_restarts::<MemoryStores>(false);
    backfill_restarts::<MemoryStores>(true);
}

#[test]
fn unfinished_and_legacy_backfill_replay_retained_blocks_on_fjall() {
    backfill_restarts::<FjallStores>(false);
    backfill_restarts::<FjallStores>(true);
}

#[test]
fn tagged_records_have_pinned_canonical_bytes() {
    let metadata = committee::encode(&Record::Context(context())).unwrap();
    assert_eq!(hex::encode(metadata.as_bytes()), "840001f505");
    for (record, expected) in [
        (Record::Backfill(committee::reset_backfill()), "840101f400"),
        (Record::Rollback(committee::reset_rollback()), "840201f500"),
    ] {
        let encoded = committee::encode(&record).unwrap();
        assert_eq!(hex::encode(encoded.as_bytes()), expected);
        assert_eq!(committee::decode(encoded.as_bytes()).unwrap(), record);
    }
    let scope = CommitteeScope {
        network_magic: dolos_snapshot::MAINNET_MAGIC,
        point: ChainPoint::Specific(250, [0x0b; 32].into()),
    };
    let header = stelae::LayerHeader::new(
        dolos_snapshot::PROFILE_NAME,
        committee::KIND,
        scope.header().unwrap(),
    )
    .encode()
    .unwrap();
    let mut bytes = header.into_bytes();
    bytes.extend_from_slice(
        committee::encode(&Record::Context(RelationContext {
            version: RelationContext::VERSION,
            complete: false,
            initialized_at: 250,
        }))
        .unwrap()
        .as_bytes(),
    );
    for record in [
        Record::Backfill(committee::reset_backfill()),
        Record::Rollback(committee::reset_rollback()),
    ] {
        bytes.extend_from_slice(committee::encode(&record).unwrap().as_bytes());
    }
    assert_eq!(bytes.len(), 111);
    assert_eq!(
        stelae::Digest::compute(&bytes).to_string(),
        "sha256:4437e8096c1885af2a29000a2dae8a694fe1b3a58d04558b7012fd4932c0bc89"
    );
    let record = Record::Witness(witness(0x12, 0x02, 1));
    let encoded = committee::encode(&record).unwrap();
    assert_eq!(
        hex::encode(encoded.as_bytes()),
        format!(
            "8603581d12{}581d02{}010af5",
            "ab".repeat(28),
            "cd".repeat(28)
        )
    );
    assert_eq!(committee::decode(encoded.as_bytes()).unwrap(), record);
    assert_eq!(
        committee::decode(metadata.as_bytes()).unwrap(),
        Record::Context(context())
    );
    for cold in [0x12, 0x13] {
        for hot in [0x02, 0x03] {
            let record = Record::Witness(witness(cold, hot, 1));
            assert_eq!(
                committee::decode(committee::encode(&record).unwrap().as_bytes()).unwrap(),
                record
            );
        }
    }
    for (cold, hot) in [(0x11, 0x02), (0x12, 0x12), (0x02, 0x02), (0x12, 0x04)] {
        assert!(committee::encode(&Record::Witness(witness(cold, hot, 1))).is_err());
    }
    let mut trailing = encoded.as_bytes().to_vec();
    trailing.push(0);
    assert!(committee::decode(&trailing).is_err());
    let raw = Record::Witness(RelationRecord::new(witness(0x13, 0x03, 1).source));
    assert_eq!(
        committee::decode(committee::encode(&raw).unwrap().as_bytes()).unwrap(),
        raw
    );
    let mut future_metadata = context();
    future_metadata.initialized_at = 1000;
    committee::validate(&Record::Context(future_metadata), 1).unwrap();
    let mut future_observation = witness(0x12, 0x02, 1);
    future_observation.observed_at = Some(1000);
    committee::validate(&Record::Witness(future_observation), 1).unwrap();
}
