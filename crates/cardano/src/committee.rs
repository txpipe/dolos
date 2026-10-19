use std::collections::BTreeMap;

use dolos_core::{
    ArchiveError, ArchiveStore, ArchiveWriter, ChainPoint, Domain, DomainError, RelationContext,
    RelationRecord, RelationSource, StateStore, WalStore,
};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    indexes::{archive_dimensions, committee_authorization_sources, COMMITTEE_AUTHORIZATIONS},
    pallas_extras::{committee_cold_id_bytes, committee_hot_id_bytes},
    CommitteeAuthorization, EpochState, GovState,
};

type Pair = (Vec<u8>, Vec<u8>);

const WRITE_CHUNK: usize = 1024;
const BACKFILL: &str = "committee_authorizations_backfill";
const ROLLBACK: &str = "committee_authorizations_rollback";

#[derive(Default)]
pub struct Maintenance {
    after: Option<RelationSource>,
    stable: Option<RelationRecord>,
    checkpoint: Option<ChainPoint>,
    queued: bool,
}

fn effective(state: &impl StateStore) -> Result<BTreeMap<Pair, u64>, DomainError> {
    let gov = state.read_entity_typed::<GovState>(&())?;
    Ok(gov
        .into_iter()
        .flat_map(|gov| gov.committee_auths)
        .filter_map(|(cold, history)| {
            let (slot, auth) = history.last()?;
            let CommitteeAuthorization::HotCredential(hot) = auth else {
                return None;
            };
            Some((
                (committee_cold_id_bytes(&cold), committee_hot_id_bytes(hot)),
                *slot,
            ))
        })
        .collect())
}

fn source(pair: &Pair, slot: u64) -> RelationSource {
    RelationSource {
        dimension: COMMITTEE_AUTHORIZATIONS.into(),
        subject: pair.0.clone(),
        object: pair.1.clone(),
        source_slot: slot,
    }
}

fn predecessor(source: &RelationSource) -> Result<RelationSource, ArchiveError> {
    let mut before = source.clone();
    if before.source_slot > 0 {
        before.source_slot -= 1;
    } else {
        let Some(index) = before.object.iter().rposition(|byte| *byte != 0) else {
            return Err(ArchiveError::InternalError(
                "The committee relation object is not valid.".into(),
            ));
        };
        before.object[index] -= 1;
        before.object[index + 1..].fill(u8::MAX);
        before.source_slot = u64::MAX;
    }
    Ok(before)
}

fn existing<A: ArchiveStore>(
    archive: &A,
    source: &RelationSource,
) -> Result<Option<RelationRecord>, ArchiveError> {
    archive
        .iter_relations_from(COMMITTEE_AUTHORIZATIONS, Some(&predecessor(source)?))?
        .next()
        .transpose()
        .map(|record| record.filter(|record| record.source == *source))
}

fn observe(record: &mut RelationRecord, tip: u64, active: bool) {
    let previous = record.observed_at;
    record.observe(tip, active);
    record.observed_at = record.observed_at.max(previous);
}

pub fn prepare<D: Domain>(domain: &D) -> Result<bool, DomainError> {
    prepare_stores(domain.state(), domain.archive())
}

pub fn prepare_stores(
    state: &impl StateStore,
    archive: &impl ArchiveStore,
) -> Result<bool, DomainError> {
    let context = match archive.relation_context(COMMITTEE_AUTHORIZATIONS) {
        Ok(Some(context)) if context.version != RelationContext::VERSION => {
            return Err(ArchiveError::InvalidStoreVersion.into());
        }
        Ok(Some(context)) if context.complete => return Ok(true),
        Ok(context) => context,
        Err(ArchiveError::Unsupported(_)) => return Ok(true),
        Err(error) => return Err(error.into()),
    };
    let mut progress = archive.relation_context(BACKFILL)?;
    if progress
        .as_ref()
        .is_some_and(|progress| progress.version != RelationContext::VERSION)
    {
        return Err(ArchiveError::InvalidStoreVersion.into());
    }
    if progress.as_ref().is_some_and(|progress| progress.complete) {
        return Ok(true);
    }
    let cursor = state.read_cursor()?;
    let tip = archive.get_tip()?.map(|(slot, _)| slot);
    let fresh = context.is_none()
        && matches!(cursor, None | Some(ChainPoint::Origin))
        && effective(state)?.is_empty()
        && tip.is_none()
        && archive
            .iter_relations_from(COMMITTEE_AUTHORIZATIONS, None)?
            .next()
            .is_none();
    if !fresh && !archive_aligned(state, archive)? {
        return Ok(false);
    }
    if context.is_none() {
        let writer = archive.start_writer()?;
        writer.set_relation_context(
            COMMITTEE_AUTHORIZATIONS,
            &RelationContext {
                version: RelationContext::VERSION,
                complete: fresh,
                initialized_at: cursor.as_ref().map(ChainPoint::slot).unwrap_or_default(),
            },
        )?;
        writer.commit()?;
        if fresh {
            return Ok(true);
        }
        tracing::warn!("The committee authorization history does not contain all older records. The system will add records from the certificates that remain in the archive.");
    }
    let start = progress.as_ref().map(|progress| progress.initialized_at);
    let mut blocks = archive.get_range(start, None)?;
    let mut writer = archive.start_writer()?;
    let mut pending = 0;
    let mut scanned = 0;
    let mut exhausted = false;
    while scanned < WRITE_CHUNK {
        let Some((slot, body)) = blocks.next() else {
            exhausted = true;
            break;
        };
        scanned += 1;
        let block = MultiEraBlock::decode(&body).map_err(ArchiveError::from)?;
        for source in committee_authorization_sources(&block) {
            if existing(archive, &source)?.is_none() {
                writer.put_relation(&RelationRecord::new(source))?;
                pending += 1;
            }
            if pending == WRITE_CHUNK {
                writer.commit()?;
                writer = archive.start_writer()?;
                pending = 0;
            }
        }
        progress = Some(RelationContext {
            version: RelationContext::VERSION,
            complete: false,
            initialized_at: slot,
        });
    }
    if exhausted {
        for (pair, slot) in effective(state)? {
            let source = source(&pair, slot);
            if existing(archive, &source)?.is_none() {
                writer.put_relation(&RelationRecord::new(source))?;
                pending += 1;
            }
            if pending == WRITE_CHUNK {
                writer.commit()?;
                writer = archive.start_writer()?;
                pending = 0;
            }
        }
    }
    let mut progress = progress.unwrap_or(RelationContext {
        version: RelationContext::VERSION,
        complete: false,
        initialized_at: 0,
    });
    progress.complete = exhausted;
    writer.set_relation_context(BACKFILL, &progress)?;
    writer.commit()?;
    Ok(exhausted)
}

pub fn archive_aligned(
    state: &impl StateStore,
    archive: &impl ArchiveStore,
) -> Result<bool, DomainError> {
    let cursor = state.read_cursor()?;
    let point = archive
        .get_tip()?
        .map(|(slot, body)| {
            let block = MultiEraBlock::decode(&body).map_err(ArchiveError::from)?;
            if block.slot() != slot {
                return Err(ArchiveError::InternalError(
                    "The archive tip and its block body have different slots.".into(),
                ));
            }
            Ok(ChainPoint::Specific(slot, block.hash()))
        })
        .transpose()?;
    Ok(cursor.is_some() && cursor == point)
}

pub fn reconcile_rollback<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let context = match domain.archive().relation_context(ROLLBACK) {
        Ok(Some(context)) if !context.complete => context,
        Ok(_) | Err(ArchiveError::Unsupported(_)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if context.version != RelationContext::VERSION {
        return Err(ArchiveError::InvalidStoreVersion.into());
    }
    let state = domain.state().read_cursor()?;
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);
    if state.is_none() || wal.is_none() || wal != state || ready(domain)?.is_none() {
        return Err(DomainError::InconsistentState { wal, state });
    }
    after_rollback(domain)
}

fn ready<D: Domain>(domain: &D) -> Result<Option<u64>, DomainError> {
    let Some(cursor) = domain.state().read_cursor()? else {
        return Ok(None);
    };
    let tip = cursor.slot();
    if !archive_aligned(domain.state(), domain.archive())? {
        return Ok(None);
    }
    if domain
        .wal()
        .find_tip()?
        .is_some_and(|(point, _)| point != cursor)
    {
        return Ok(None);
    }
    if let Some(epoch) = domain.state().read_entity_typed::<EpochState>(&())? {
        if epoch.ewrap_progress.is_some()
            || epoch.estart_progress.is_some()
            || epoch.rupd_progress.is_some()
        {
            return Ok(None);
        }
    }
    Ok(Some(tip))
}

pub fn maintenance_ready<D: Domain>(domain: &D) -> Result<bool, DomainError> {
    match domain.archive().relation_context(COMMITTEE_AUTHORIZATIONS) {
        Err(ArchiveError::Unsupported(_)) => return Ok(true),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    if domain.state().read_cursor()?.is_none()
        || domain
            .archive()
            .relation_context(ROLLBACK)?
            .is_some_and(|context| !context.complete)
    {
        return Ok(false);
    }
    Ok(ready(domain)?.is_some())
}

pub fn before_rollback<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let archive = domain.archive();
    match archive.relation_context(COMMITTEE_AUTHORIZATIONS) {
        Err(ArchiveError::Unsupported(_)) => return Ok(()),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let high = domain
        .state()
        .read_cursor()?
        .map(|point| point.slot())
        .unwrap_or_default();
    let high = archive
        .relation_context(ROLLBACK)?
        .map(|context| context.initialized_at.max(high))
        .unwrap_or(high);
    let writer = archive.start_writer()?;
    writer.set_relation_context(
        ROLLBACK,
        &RelationContext {
            version: RelationContext::VERSION,
            complete: false,
            initialized_at: high,
        },
    )?;
    writer.commit()?;
    Ok(())
}

pub fn after_rollback<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let archive = domain.archive();
    let mut context = match archive.relation_context(ROLLBACK) {
        Ok(Some(context)) => context,
        Ok(None) | Err(ArchiveError::Unsupported(_)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let writer = archive.start_writer()?;
    for (pair, slot) in effective(domain.state())? {
        let source = source(&pair, slot);
        let mut before_pair = source.clone();
        before_pair.source_slot = 0;
        let mut record = archive
            .iter_relations_from(COMMITTEE_AUTHORIZATIONS, Some(&predecessor(&before_pair)?))?
            .next()
            .transpose()?
            .filter(|record| same_pair(&record.source, &source))
            .unwrap_or_else(|| RelationRecord::new(source));
        observe(&mut record, context.initialized_at, true);
        writer.put_relation(&record)?;
    }
    context.complete = true;
    writer.set_relation_context(ROLLBACK, &context)?;
    writer.commit()?;
    Ok(())
}

fn same_pair(left: &RelationSource, right: &RelationSource) -> bool {
    left.subject == right.subject && left.object == right.object
}

fn retained<A: ArchiveStore>(
    archive: &A,
    record: &RelationRecord,
    first: u64,
    rollback: Option<u64>,
    tip: u64,
) -> Result<bool, ArchiveError> {
    if record.active || record.source.source_slot >= first {
        return Ok(true);
    }
    let Some(rollback) = rollback else {
        return Ok(true);
    };
    if record.source.source_slot >= rollback
        || record
            .observed_at
            .is_none_or(|observed| observed >= rollback)
    {
        return Ok(true);
    }
    archive
        .slots_by_tag(
            archive_dimensions::VOTER_VOTES,
            &record.source.object,
            first,
            tip,
        )?
        .next()
        .transpose()
        .map(|slot| slot.is_some())
}

impl Maintenance {
    pub fn run<D: Domain>(&mut self, domain: &D, budget: usize) -> Result<bool, DomainError> {
        let Some(tip) = ready(domain)? else {
            *self = Self::default();
            return Ok(true);
        };
        let checkpoint = domain.state().read_cursor()?;
        if self.checkpoint.as_ref().is_some_and(|previous| {
            checkpoint.as_ref().is_none_or(|current| {
                current.slot() < previous.slot()
                    || (current.slot() == previous.slot() && current != previous)
            })
        }) {
            *self = Self::default();
        }
        match domain.archive().relation_context(COMMITTEE_AUTHORIZATIONS) {
            Ok(Some(context)) if context.version == RelationContext::VERSION => {}
            Ok(Some(_)) => return Err(ArchiveError::InvalidStoreVersion.into()),
            Ok(None) | Err(ArchiveError::Unsupported(_)) => return Ok(true),
            Err(error) => return Err(error.into()),
        }
        if budget == 0 {
            return Ok(false);
        }
        let current = effective(domain.state())?;
        let archive = domain.archive();
        let Some((first, _)) = archive.get_range(None, None)?.next() else {
            return Ok(true);
        };
        let rollback = domain.wal().find_start()?.map(|(point, _)| point.slot());
        let writer = archive.start_writer()?;
        let mut after = self.after.clone();
        let mut stable = self.stable.clone();
        let mut queued = self.queued;
        for (pair, slot) in &current {
            let source = source(pair, *slot);
            if existing(archive, &source)?.is_none() {
                let mut before_pair = source.clone();
                before_pair.source_slot = 0;
                let before_pair = predecessor(&before_pair)?;
                let witness = archive
                    .iter_relations_from(COMMITTEE_AUTHORIZATIONS, Some(&before_pair))?
                    .next()
                    .transpose()?;
                if !witness.is_some_and(|record| same_pair(&record.source, &source)) {
                    let mut record = RelationRecord::new(source);
                    observe(&mut record, tip, true);
                    writer.put_relation(&record)?;
                    queued = true;
                }
            }
        }
        let mut rows = archive.iter_relations_from(COMMITTEE_AUTHORIZATIONS, after.as_ref())?;
        let mut exhausted = false;
        for _ in 0..budget {
            let Some(record) = rows.next() else {
                exhausted = true;
                break;
            };
            let mut record = record?;
            let pair = (record.source.subject.clone(), record.source.object.clone());
            if let Some(mut previous) = stable.take() {
                if same_pair(&previous.source, &record.source) {
                    stable = Some(previous);
                } else {
                    let active = current.contains_key(&(
                        previous.source.subject.clone(),
                        previous.source.object.clone(),
                    ));
                    observe(&mut previous, tip, active);
                    if !retained(archive, &previous, first, rollback, tip)? {
                        writer.delete_relation(&previous.source)?;
                    } else {
                        writer.put_relation(&previous)?;
                    }
                }
            }
            after = Some(record.source.clone());
            if record.source.source_slot > tip {
                continue;
            }
            observe(&mut record, tip, current.contains_key(&pair));
            let compactable = rollback.is_some_and(|floor| {
                record.source.source_slot < first && record.source.source_slot < floor
            });
            if compactable {
                if let Some(previous) = stable.as_mut() {
                    previous.merge_protection(&record);
                    writer.delete_relation(&record.source)?;
                    writer.put_relation(previous)?;
                } else {
                    writer.put_relation(&record)?;
                    stable = Some(record);
                }
            } else if retained(archive, &record, first, rollback, tip)? {
                writer.put_relation(&record)?;
            } else {
                writer.delete_relation(&record.source)?;
            }
        }
        if exhausted {
            if let Some(mut previous) = stable.take() {
                let active = current.contains_key(&(
                    previous.source.subject.clone(),
                    previous.source.object.clone(),
                ));
                observe(&mut previous, tip, active);
                if retained(archive, &previous, first, rollback, tip)? {
                    writer.put_relation(&previous)?;
                } else {
                    writer.delete_relation(&previous.source)?;
                }
            }
        }
        drop(rows);
        writer.commit()?;
        self.after = if exhausted { None } else { after };
        self.stable = stable;
        self.checkpoint = checkpoint;
        self.queued = !exhausted && queued;
        Ok(exhausted && !queued)
    }
}
