use dolos_cardano::{
    committee::{self, Maintenance},
    indexes::{archive_dimensions, COMMITTEE_AUTHORIZATIONS},
    pallas_extras::{committee_cold_id_bytes, committee_hot_id_bytes},
    CardanoDelta, CommitteeAuthorization, CommitteeResign, GovState,
};
use dolos_core::{
    ArchiveIndexDelta, ArchiveStore, ArchiveWriter, ChainPoint, Domain, RelationRecord,
    RelationSource, StateStore, StateWriter, Tag, WalStore,
};
use dolos_testing::{blocks::make_conway_block, toy_domain::ToyDomain};
use pallas::ledger::primitives::StakeCredential;

fn credential(byte: u8) -> StakeCredential {
    StakeCredential::AddrKeyhash([byte; 28].into())
}

fn relation(slot: u64) -> RelationRecord {
    RelationRecord::new(RelationSource {
        dimension: COMMITTEE_AUTHORIZATIONS.into(),
        subject: committee_cold_id_bytes(&credential(1)),
        object: committee_hot_id_bytes(&credential(2)),
        source_slot: slot,
    })
}

fn position(domain: &ToyDomain, tip: u64, first: u64, rollback: Option<u64>) {
    let (point, block) = make_conway_block(tip);
    let state = domain.state().start_writer().unwrap();
    state.set_cursor(point.clone()).unwrap();
    state.commit().unwrap();
    let archive = domain.archive().start_writer().unwrap();
    if first != tip {
        let (point, body) = make_conway_block(first);
        archive.apply(&point, &body).unwrap();
    }
    archive.apply(&point, &block).unwrap();
    archive.commit().unwrap();
    domain
        .archive()
        .prune_history(tip - first, None, None)
        .unwrap();
    if let Some(slot) = rollback {
        domain
            .wal()
            .reset_to(&ChainPoint::Specific(slot, [3; 32].into()))
            .unwrap();
        domain.wal().remove_before(slot).unwrap();
        domain
            .wal()
            .append_entries(&[(point, dolos_core::LogValue::origin())])
            .unwrap();
    }
}

fn authorize(domain: &ToyDomain, slot: u64) {
    let mut gov: GovState = domain.state().read_entity_typed(&()).unwrap().unwrap();
    gov.committee_auths.insert(
        credential(1),
        vec![(slot, CommitteeAuthorization::HotCredential(credential(2)))],
    );
    let writer = domain.state().start_writer().unwrap();
    writer.write_entity_typed(&(), &gov).unwrap();
    writer.commit().unwrap();
}

fn depart(domain: &ToyDomain) {
    let mut gov: GovState = domain.state().read_entity_typed(&()).unwrap().unwrap();
    gov.committee_auths.clear();
    let writer = domain.state().start_writer().unwrap();
    writer.write_entity_typed(&(), &gov).unwrap();
    writer.commit().unwrap();
}

fn put(domain: &ToyDomain, records: &[RelationRecord]) {
    let writer = domain.archive().start_writer().unwrap();
    for record in records {
        writer.put_relation(record).unwrap();
    }
    writer.commit().unwrap();
}

fn records(domain: &ToyDomain) -> Vec<RelationRecord> {
    domain
        .archive()
        .iter_relations(COMMITTEE_AUTHORIZATIONS, None)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn maintain(domain: &ToyDomain) {
    let mut maintenance = Maintenance::default();
    for _ in 0..100 {
        if maintenance.run(domain, 2).unwrap() {
            return;
        }
    }
    panic!("Maintenance did not finish.");
}

#[test]
fn housekeeping_observes_after_configured_pruning() {
    let mut config = dolos_core::config::SyncConfig::default();
    config.max_history = Some(10);
    config.max_rollback = Some(20);
    let domain = ToyDomain::new(None, None).with_sync_config(config);
    position(&domain, 100, 10, Some(10));
    domain
        .wal()
        .append_entries(&[
            (make_conway_block(80).0, dolos_core::LogValue::origin()),
            (make_conway_block(100).0, dolos_core::LogValue::origin()),
        ])
        .unwrap();
    put(&domain, &[relation(5)]);
    assert!(domain.housekeeping().unwrap());
    assert_eq!(
        domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .next()
            .unwrap()
            .0,
        100
    );
    assert_eq!(domain.wal().find_start().unwrap().unwrap().0.slot(), 80);
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    assert!(domain.housekeeping().unwrap());
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    position(&domain, 130, 120, Some(110));
    assert!(domain.housekeeping().unwrap());
    assert!(records(&domain).is_empty());
}

#[test]
fn housekeeping_pins_active_state_and_observes_departure() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    authorize(&domain, 10);
    domain.drain_housekeeping(Some(10)).unwrap();
    assert!(records(&domain)[0].active);
    depart(&domain);
    position(&domain, 110, 100, Some(90));
    assert!(domain.housekeeping().unwrap());
    assert!(!records(&domain)[0].active);
    assert_eq!(records(&domain)[0].observed_at, Some(110));
}

#[test]
fn lifecycle_preparation_preserves_genesis_context() {
    use dolos_core::{import::ImportExt, sync::SyncExt, BootstrapExt};
    let domain = ToyDomain::new(None, None);
    let initial = domain
        .archive()
        .relation_context(COMMITTEE_AUTHORIZATIONS)
        .unwrap()
        .unwrap();
    domain.roll_forward(make_conway_block(1).1).unwrap();
    domain.housekeeping().unwrap();
    domain.bootstrap().unwrap();
    assert_eq!(
        domain
            .archive()
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap(),
        initial
    );
    let imported = ToyDomain::new(None, None);
    imported
        .import_blocks(vec![make_conway_block(1).1])
        .unwrap();
    imported.housekeeping().unwrap();
    assert_eq!(
        imported
            .archive()
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap(),
        initial
    );
}

#[test]
fn active_authorization_survives_before_first_vote() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    authorize(&domain, 10);
    maintain(&domain);
    assert_eq!(records(&domain).len(), 1);
    assert!(records(&domain)[0].active);
    position(&domain, 200, 190, Some(180));
    maintain(&domain);
    assert!(records(&domain)[0].active);
}

#[test]
fn first_inactive_observation_protects_an_old_source_without_renewal() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    put(&domain, &[relation(10)]);
    maintain(&domain);
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    position(&domain, 120, 110, Some(95));
    maintain(&domain);
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    position(&domain, 130, 120, Some(101));
    maintain(&domain);
    assert!(records(&domain).is_empty());
}

#[test]
fn departure_restart_and_reactivation_preserve_protection() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    authorize(&domain, 10);
    maintain(&domain);
    depart(&domain);
    position(&domain, 110, 100, Some(90));
    maintain(&domain);
    assert_eq!(records(&domain)[0].observed_at, Some(110));
    position(&domain, 105, 100, Some(90));
    domain
        .archive()
        .truncate_front(&domain.state().read_cursor().unwrap().unwrap())
        .unwrap();
    authorize(&domain, 10);
    maintain(&domain);
    assert!(records(&domain)[0].active);
    assert_eq!(records(&domain)[0].observed_at, Some(110));
}

#[test]
fn resignation_undo_after_maintenance_restart_restores_old_pin() {
    use dolos_core::{EntityDelta, LogValue, SyncExt};
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(90));
    authorize(&domain, 10);
    maintain(&domain);
    let target = domain.state().read_cursor().unwrap().unwrap();
    let (point, block) = make_conway_block(110);
    let mut gov = domain.state().read_entity_typed::<GovState>(&()).unwrap();
    let mut resignation = CommitteeResign::new(credential(1), None, 110);
    resignation.apply(&mut gov);
    let writer = domain.state().start_writer().unwrap();
    writer
        .write_entity_typed(&(), gov.as_ref().unwrap())
        .unwrap();
    writer.set_cursor(point.clone()).unwrap();
    writer.commit().unwrap();
    let writer = domain.archive().start_writer().unwrap();
    writer.apply(&point, &block).unwrap();
    writer.commit().unwrap();
    domain
        .wal()
        .append_entries(&[
            (target.clone(), LogValue::origin()),
            (
                point,
                LogValue {
                    block: block.as_ref().clone(),
                    delta: vec![CardanoDelta::from(resignation)],
                    inputs: Default::default(),
                },
            ),
        ])
        .unwrap();
    maintain(&domain);
    assert_eq!(records(&domain)[0].observed_at, Some(110));
    domain.rollback(&target).unwrap();
    maintain(&domain);
    assert!(records(&domain)[0].active);
    assert_eq!(records(&domain)[0].observed_at, Some(110));
}

#[test]
fn rollback_rearms_pin_before_replacement_resignation_without_housekeeping() {
    use dolos_core::{sync::SyncExt, EntityDelta, LogValue};
    let domain = ToyDomain::new(None, None);
    position(&domain, 80, 75, Some(75));
    authorize(&domain, 10);
    let target = domain.state().read_cursor().unwrap().unwrap();
    let mut gov = domain.state().read_entity_typed::<GovState>(&()).unwrap();
    let mut resignation = CommitteeResign::new(credential(1), None, 90);
    resignation.apply(&mut gov);
    let writer = domain.state().start_writer().unwrap();
    writer
        .write_entity_typed(&(), gov.as_ref().unwrap())
        .unwrap();
    writer.commit().unwrap();
    position(&domain, 100, 80, Some(75));
    domain
        .wal()
        .append_entries(&[
            (target.clone(), LogValue::origin()),
            (
                make_conway_block(100).0,
                LogValue {
                    block: make_conway_block(100).1.as_ref().clone(),
                    delta: Vec::new(),
                    inputs: Default::default(),
                },
            ),
            (
                make_conway_block(90).0,
                LogValue {
                    block: make_conway_block(90).1.as_ref().clone(),
                    delta: vec![CardanoDelta::from(resignation)],
                    inputs: Default::default(),
                },
            ),
        ])
        .unwrap();
    put(&domain, &[relation(10)]);
    assert!(domain.housekeeping().unwrap());
    assert!(!records(&domain)[0].active);
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    domain.rollback(&target).unwrap();
    assert!(records(&domain)[0].active);
    depart(&domain);
    position(&domain, 160, 155, Some(110));
    assert!(domain.housekeeping().unwrap());
    assert_eq!(records(&domain)[0].observed_at, Some(160));
    assert!(!records(&domain)[0].active);
}

#[test]
fn housekeeping_preserves_repair_wal_when_archive_commit_is_missing() {
    let mut config = dolos_core::config::SyncConfig::default();
    config.max_history = Some(5);
    config.max_rollback = Some(50);
    let domain = ToyDomain::new(None, None).with_sync_config(config);
    position(&domain, 100, 80, Some(80));
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(make_conway_block(160).0).unwrap();
    writer.commit().unwrap();
    domain
        .wal()
        .append_entries(&[(make_conway_block(160).0, dolos_core::LogValue::origin())])
        .unwrap();
    assert!(domain.housekeeping().unwrap());
    assert_eq!(domain.wal().find_start().unwrap().unwrap().0.slot(), 80);
    assert_eq!(
        domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .next()
            .unwrap()
            .0,
        80
    );
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(&make_conway_block(160).0, &make_conway_block(160).1)
        .unwrap();
    writer.commit().unwrap();
    domain
        .wal()
        .append_entries(&[(make_conway_block(170).0, dolos_core::LogValue::origin())])
        .unwrap();
    assert!(domain.housekeeping().unwrap());
    assert_eq!(domain.wal().find_start().unwrap().unwrap().0.slot(), 80);
    assert_eq!(
        domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .next()
            .unwrap()
            .0,
        80
    );
}

#[test]
fn archive_alignment_distinguishes_origin_from_missing_and_specific_cursors() {
    use dolos_core::builtin::{MemoryArchiveStore, MemoryStateStore};

    let cases = [
        (None, None, false),
        (Some(ChainPoint::Origin), None, true),
        (Some(ChainPoint::Slot(0)), None, false),
        (Some(make_conway_block(0).0), None, false),
        (Some(make_conway_block(1).0), None, false),
        (None, Some(1), false),
        (Some(ChainPoint::Origin), Some(0), false),
        (Some(ChainPoint::Origin), Some(1), false),
        (Some(ChainPoint::Slot(1)), Some(1), false),
        (Some(make_conway_block(0).0), Some(0), true),
        (Some(make_conway_block(1).0), Some(1), true),
        (Some(make_conway_block(2).0), Some(1), false),
        (
            Some(ChainPoint::Specific(1, [9; 32].into())),
            Some(1),
            false,
        ),
    ];

    for (cursor, archive_slot, expected) in cases {
        let state = MemoryStateStore::new();
        let archive = MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
        if let Some(cursor) = &cursor {
            let writer = state.start_writer().unwrap();
            writer.set_cursor(cursor.clone()).unwrap();
            writer.commit().unwrap();
        }
        if let Some(slot) = archive_slot {
            let (point, body) = make_conway_block(slot);
            let writer = archive.start_writer().unwrap();
            writer.apply(&point, &body).unwrap();
            writer.commit().unwrap();
        }
        assert_eq!(
            committee::archive_aligned(&state, &archive).unwrap(),
            expected,
            "cursor: {cursor:?}, archive slot: {archive_slot:?}"
        );
    }
}

#[test]
fn bootstrap_finishes_origin_rollback_after_both_truncations() {
    use dolos_core::{BootstrapExt, SyncExt};
    use dolos_testing::faults::{FaultyToyDomain, TestFault};

    let domain = ToyDomain::new(None, None);
    domain.roll_forward(make_conway_block(1).1).unwrap();
    let interrupted = FaultyToyDomain::new(domain.clone(), TestFault::CrashAfterCommits(3));
    assert!(interrupted.rollback(&ChainPoint::Origin).is_err());
    drop(interrupted);

    assert_eq!(
        domain.state().read_cursor().unwrap(),
        Some(ChainPoint::Origin)
    );
    assert_eq!(
        domain.wal().find_tip().unwrap().unwrap().0,
        ChainPoint::Origin
    );
    assert!(domain.archive().get_tip().unwrap().is_none());
    let mut marker = domain
        .archive()
        .relation_context("committee_authorizations_rollback")
        .unwrap()
        .unwrap();
    assert!(!marker.complete);
    assert_eq!(marker.initialized_at, 1);
    assert!(!committee::maintenance_ready(&domain).unwrap());

    marker.complete = true;
    for _ in 0..2 {
        domain.bootstrap().unwrap();
        assert_eq!(
            domain
                .archive()
                .relation_context("committee_authorizations_rollback")
                .unwrap(),
            Some(marker.clone())
        );
        assert_eq!(
            domain.state().read_cursor().unwrap(),
            Some(ChainPoint::Origin)
        );
        assert_eq!(
            domain.wal().find_tip().unwrap().unwrap().0,
            ChainPoint::Origin
        );
        assert!(domain.archive().get_tip().unwrap().is_none());
        assert!(committee::maintenance_ready(&domain).unwrap());
    }
}

#[test]
fn bootstrap_finishes_protection_after_completed_rollback_truncation() {
    use dolos_core::BootstrapExt;
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    authorize(&domain, 10);
    let mut record = relation(10);
    record.observed_at = Some(100);
    put(&domain, &[record]);
    committee::before_rollback(&domain).unwrap();
    let (target, body) = make_conway_block(80);
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(target.clone()).unwrap();
    writer.commit().unwrap();
    let writer = domain.archive().start_writer().unwrap();
    writer.apply(&target, &body).unwrap();
    writer.commit().unwrap();
    domain
        .wal()
        .append_entries(&[(target.clone(), dolos_core::LogValue::origin())])
        .unwrap();
    domain.archive().truncate_front(&target).unwrap();
    domain.wal().truncate_front(&target).unwrap();
    domain.bootstrap().unwrap();
    assert!(records(&domain)[0].active);
    assert_eq!(records(&domain)[0].observed_at, Some(100));
    assert!(
        domain
            .archive()
            .relation_context("committee_authorizations_rollback")
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(committee::maintenance_ready(&domain).unwrap());
}

#[test]
fn bootstrap_rejects_unproven_interrupted_rollback_before_catchup() {
    use dolos_core::BootstrapExt;
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    committee::before_rollback(&domain).unwrap();
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(make_conway_block(80).0).unwrap();
    writer.commit().unwrap();
    assert!(domain.bootstrap().is_err());
    assert_eq!(
        domain.state().read_cursor().unwrap().unwrap(),
        make_conway_block(80).0
    );
    assert!(
        !domain
            .archive()
            .relation_context("committee_authorizations_rollback")
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn housekeeping_rejects_same_slot_different_archive_hash() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    let wrong = ChainPoint::Specific(100, [9; 32].into());
    let writer = domain.state().start_writer().unwrap();
    writer.set_cursor(wrong.clone()).unwrap();
    writer.commit().unwrap();
    domain.wal().reset_to(&wrong).unwrap();
    domain.wal().remove_before(80).unwrap();
    assert!(!committee::maintenance_ready(&domain).unwrap());
    assert!(domain.housekeeping().unwrap());
    assert_eq!(
        domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .next()
            .unwrap()
            .0,
        90
    );
}

#[test]
fn housekeeping_boundary_progress_preserves_archive_and_wal() {
    let mut config = dolos_core::config::SyncConfig::default();
    config.max_history = Some(5);
    config.max_rollback = Some(5);
    let domain = ToyDomain::new(None, None).with_sync_config(config);
    position(&domain, 100, 80, Some(80));
    let mut epoch = domain
        .state()
        .read_entity_typed::<dolos_cardano::EpochState>(&())
        .unwrap()
        .unwrap();
    epoch.estart_progress = Some(dolos_cardano::ShardProgress {
        committed: 1,
        total: 2,
    });
    let writer = domain.state().start_writer().unwrap();
    writer.write_entity_typed(&(), &epoch).unwrap();
    writer.commit().unwrap();
    assert!(domain.housekeeping().unwrap());
    assert_eq!(domain.wal().find_start().unwrap().unwrap().0.slot(), 80);
    assert_eq!(
        domain
            .archive()
            .get_range(None, None)
            .unwrap()
            .next()
            .unwrap()
            .0,
        80
    );
}

#[test]
fn restored_incomplete_coverage_backfills_in_bounded_restartable_steps() {
    let state = dolos_core::builtin::MemoryStateStore::new();
    let archive =
        dolos_core::builtin::MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let writer = archive.start_writer().unwrap();
    for slot in 1..=2050 {
        let (point, body) = make_conway_block(slot);
        writer.apply(&point, &body).unwrap();
    }
    writer
        .set_relation_context(
            COMMITTEE_AUTHORIZATIONS,
            &dolos_core::RelationContext {
                version: dolos_core::RelationContext::VERSION,
                complete: false,
                initialized_at: 2050,
            },
        )
        .unwrap();
    writer.commit().unwrap();
    let writer = state.start_writer().unwrap();
    writer.set_cursor(make_conway_block(2050).0).unwrap();
    writer.commit().unwrap();
    assert!(!committee::prepare_stores(&state, &archive).unwrap());
    let progress = archive
        .relation_context("committee_authorizations_backfill")
        .unwrap()
        .unwrap();
    assert_eq!(progress.initialized_at, 1024);
    assert!(!progress.complete);
    assert!(!committee::prepare_stores(&state.clone(), &archive.clone()).unwrap());
    assert_eq!(
        archive
            .relation_context("committee_authorizations_backfill")
            .unwrap()
            .unwrap()
            .initialized_at,
        2047
    );
    assert!(committee::prepare_stores(&state, &archive).unwrap());
    assert!(
        archive
            .relation_context("committee_authorizations_backfill")
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(
        !archive
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn stable_duplicates_merge_across_bounded_passes() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    let mut sources: Vec<_> = (10..20).map(relation).collect();
    sources[3].observed_at = Some(200);
    sources[7].active = true;
    sources[7].observed_at = Some(50);
    put(&domain, &sources);
    maintain(&domain);
    let remaining = records(&domain);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].source.source_slot, 10);
    assert_eq!(remaining[0].observed_at, Some(200));
    assert!(!remaining[0].active);
}

#[test]
fn retained_votes_use_the_actual_archive_floor() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    let mut record = relation(10);
    record.observed_at = Some(20);
    put(&domain, &[record]);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot: 90,
            tags: vec![Tag::new(
                archive_dimensions::VOTER_VOTES,
                committee_hot_id_bytes(&credential(2)),
            )],
            ..Default::default()
        }])
        .unwrap();
    writer.commit().unwrap();
    maintain(&domain);
    assert_eq!(records(&domain).len(), 1);
    position(&domain, 200, 190, Some(180));
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot: 90,
            tags: vec![Tag::new(
                archive_dimensions::VOTER_VOTES,
                committee_hot_id_bytes(&credential(2)),
            )],
            ..Default::default()
        }])
        .unwrap();
    writer.commit().unwrap();
    maintain(&domain);
    assert!(records(&domain).is_empty());
}

#[test]
fn cleanup_skips_mismatched_stores_and_boundary_progress() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    let mut record = relation(10);
    record.observed_at = Some(20);
    put(&domain, &[record.clone()]);
    let writer = domain.state().start_writer().unwrap();
    writer
        .set_cursor(ChainPoint::Specific(101, [4; 32].into()))
        .unwrap();
    writer.commit().unwrap();
    maintain(&domain);
    assert_eq!(records(&domain), [record.clone()]);
    let mut epoch = domain
        .state()
        .read_entity_typed::<dolos_cardano::EpochState>(&())
        .unwrap()
        .unwrap();
    epoch.ewrap_progress = Some(dolos_cardano::ShardProgress {
        committed: 1,
        total: 2,
    });
    let writer = domain.state().start_writer().unwrap();
    writer.write_entity_typed(&(), &epoch).unwrap();
    writer.set_cursor(make_conway_block(100).0).unwrap();
    writer.commit().unwrap();
    maintain(&domain);
    assert_eq!(records(&domain), [record]);
}

#[test]
fn existing_legacy_context_stays_incomplete_and_preserves_observation() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    let mut record = relation(10);
    record.observed_at = Some(200);
    put(&domain, &[record.clone()]);
    let writer = domain.archive().start_writer().unwrap();
    writer
        .set_relation_context(
            COMMITTEE_AUTHORIZATIONS,
            &dolos_core::RelationContext {
                version: dolos_core::RelationContext::VERSION,
                complete: false,
                initialized_at: 100,
            },
        )
        .unwrap();
    writer.commit().unwrap();
    assert!(committee::prepare(&domain).unwrap());
    assert_eq!(records(&domain), [record]);
    assert!(
        !domain
            .archive()
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn legacy_migration_seeds_only_effective_events() {
    let state = dolos_core::builtin::MemoryStateStore::new();
    let archive =
        dolos_core::builtin::MemoryArchiveStore::new(dolos_cardano::model::build_log_schema());
    let (point, body) = make_conway_block(100);
    let writer = archive.start_writer().unwrap();
    writer.apply(&point, &body).unwrap();
    let mut protected = relation(5);
    protected.observed_at = Some(150);
    writer.put_relation(&protected).unwrap();
    writer.commit().unwrap();
    let mut gov = GovState::default();
    gov.committee_auths.insert(
        credential(1),
        vec![
            (10, CommitteeAuthorization::HotCredential(credential(3))),
            (20, CommitteeAuthorization::HotCredential(credential(2))),
        ],
    );
    gov.committee_auths.insert(
        credential(4),
        vec![
            (10, CommitteeAuthorization::HotCredential(credential(5))),
            (30, CommitteeAuthorization::Resigned(None)),
        ],
    );
    let writer = state.start_writer().unwrap();
    writer.write_entity_typed(&(), &gov).unwrap();
    writer.set_cursor(point).unwrap();
    writer.commit().unwrap();
    assert!(committee::prepare_stores(&state, &archive).unwrap());
    let records = archive
        .iter_relations(COMMITTEE_AUTHORIZATIONS, None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records, [protected, relation(20)]);
    assert!(
        !archive
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(committee::prepare_stores(&state, &archive).unwrap());
    assert!(
        !archive
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap()
            .complete
    );
}

#[test]
fn initialized_context_does_not_rescan_archive_bodies() {
    let domain = ToyDomain::new(None, None);
    let context = domain
        .archive()
        .relation_context(COMMITTEE_AUTHORIZATIONS)
        .unwrap()
        .unwrap();
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply(
            &ChainPoint::Specific(100, [5; 32].into()),
            &std::sync::Arc::new(vec![0xff]),
        )
        .unwrap();
    writer.commit().unwrap();
    assert!(committee::prepare(&domain).unwrap());
    assert_eq!(
        domain
            .archive()
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap(),
        context
    );
}

#[test]
fn housekeeping_with_empty_wal_never_deletes_old_witnesses() {
    let domain = ToyDomain::new(None, None);
    domain.wal().truncate_front(&ChainPoint::Origin).unwrap();
    domain.wal().remove_before(1).unwrap();
    position(&domain, 100, 90, None);
    let mut record = relation(10);
    record.observed_at = Some(20);
    put(&domain, &[record.clone()]);
    assert!(domain.housekeeping().unwrap());
    assert_eq!(records(&domain), [record]);
}

#[test]
fn retained_and_rollbackable_duplicates_are_not_compacted() {
    let domain = ToyDomain::new(None, None);
    position(&domain, 100, 90, Some(80));
    put(
        &domain,
        &[relation(10), relation(20), relation(80), relation(90)],
    );
    maintain(&domain);
    assert_eq!(
        records(&domain)
            .iter()
            .map(|record| record.source.source_slot)
            .collect::<Vec<_>>(),
        [10, 80, 90]
    );
}

#[test]
fn fresh_genesis_establishes_complete_context() {
    let domain = ToyDomain::new(None, None);
    assert!(
        domain
            .archive()
            .relation_context(COMMITTEE_AUTHORIZATIONS)
            .unwrap()
            .unwrap()
            .complete
    );
}
