//! Commit logic for the open half of the epoch boundary (per-shard runs
//! plus the finalize pass).
//!
//! Both code paths use the same streaming pattern: each entity namespace
//! is read one record at a time, deltas for that record are applied, and
//! the result is written immediately. Per-shard commits flush
//! `EpochState`'s `EStartProgress` and the shard's account-range
//! slice; the finalize commit flushes pool / drep / proposal transitions,
//! the closing `EpochTransition`, optional era-summary writes, archive
//! logs, and advances the cursor.

use dolos_core::{
    ArchiveStore, ArchiveWriter, BlockSlot, BrokenInvariant, ChainError, ChainPoint, Domain,
    Entity, EntityDelta as _, EntityKey, LogKey, NsKey, StateStore, StateWriter, TemporalKey,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    forks, AccountState, CardanoEntity, DRepState, EpochState, EraSummary, FixedNamespace,
    GovState, PoolState, ProposalState, GOV_STATE_KEY,
};

/// Era transition data collected from state.
struct EraTransitionData {
    prev_key: EntityKey,
    prev_summary: EraSummary,
    new_key: EntityKey,
    new_summary: EraSummary,
}

impl super::WorkContext {
    /// Collect era transition data from state (reads only).
    ///
    /// Returns None if no era transition is needed.
    /// This reads only 1-2 entities, so no streaming needed here.
    fn collect_era_transition(
        &self,
        state: &impl StateStore,
    ) -> Result<Option<EraTransitionData>, ChainError> {
        let Some(transition) = self.ended_state().pparams.era_transition() else {
            return Ok(None);
        };

        tracing::info!(from=%transition.prev_version, to=%transition.new_version, "era transition detected");

        let previous = state.read_entity_typed::<EraSummary>(
            EraSummary::NS,
            &EntityKey::from(transition.prev_version),
        )?;

        let Some(mut previous) = previous else {
            return Err(BrokenInvariant::BadBootstrap.into());
        };

        previous.define_end(self.starting_epoch_no());

        let consts = forks::protocol_constants(transition.new_version.into(), &self.genesis);

        let new = EraSummary {
            start: previous.end.clone().unwrap(),
            end: None,
            epoch_length: consts.epoch_length,
            slot_length: consts.slot_length,
            protocol: transition.new_version.into(),
        };

        Ok(Some(EraTransitionData {
            prev_key: EntityKey::from(transition.prev_version),
            prev_summary: previous,
            new_key: EntityKey::from(transition.new_version),
            new_summary: new,
        }))
    }

    /// Stream entities from a namespace, apply deltas, and write immediately.
    ///
    /// Processes entities one at a time without accumulating them in memory,
    /// reducing peak memory usage during epoch boundary commits.
    pub(crate) fn stream_and_apply_namespace<D, E>(
        &mut self,
        state: &D::State,
        writer: &<D::State as StateStore>::Writer,
        range: Option<std::ops::Range<dolos_core::EntityKey>>,
    ) -> Result<(), ChainError>
    where
        D: Domain,
        E: Entity + FixedNamespace + Into<CardanoEntity>,
    {
        let records = state.iter_entities_typed::<E>(E::NS, range)?;

        for record in records {
            let (entity_id, entity) = record?;

            // Check if this entity has deltas to apply
            let to_apply = self
                .deltas
                .entities
                .remove(&NsKey::from((E::NS, entity_id.clone())));

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                // Write immediately - don't collect!
                writer.save_entity_typed(E::NS, &entity_id, entity.as_ref())?;
            } else {
                trace!(ns = E::NS, key = %entity_id, "no deltas for entity");
            }
        }

        Ok(())
    }

    /// Apply any queued deltas for the governance singleton.
    ///
    /// Unlike `stream_and_apply_namespace`, this handles the entity being
    /// absent: the `GovGenesisInit` delta emitted at the Conway era boundary
    /// *creates* the singleton, so streaming existing records would silently
    /// drop it.
    fn apply_gov_state_deltas<D: Domain>(
        &mut self,
        state: &D::State,
        writer: &<D::State as StateStore>::Writer,
    ) -> Result<(), ChainError> {
        let entity_key = EntityKey::from(GOV_STATE_KEY);

        let to_apply = self
            .deltas
            .entities
            .remove(&NsKey::from((GovState::NS, entity_key.clone())));

        let Some(to_apply) = to_apply else {
            return Ok(());
        };

        let mut entity: Option<CardanoEntity> = state
            .read_entity_typed::<GovState>(GovState::NS, &entity_key)?
            .map(Into::into);

        for mut delta in to_apply {
            delta.apply(&mut entity);
        }

        writer.save_entity_typed(GovState::NS, &entity_key, entity.as_ref())?;

        Ok(())
    }

    /// Commit a single per-shard run: stream-and-apply per-account snapshot
    /// transitions for the shard's key ranges, then commit the
    /// `EStartProgress` delta against `EpochState`. Archive logs
    /// (if any) are flushed too — the start-of-epoch temporal key is
    /// shared across shards.
    ///
    /// **Does not advance the cursor.** Cursor moves only in
    /// `commit_finalize`.
    #[instrument(skip(self, state, archive))]
    pub fn commit_shard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        ranges: Vec<std::ops::Range<EntityKey>>,
    ) -> Result<(), ChainError> {
        debug!("committing estart changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Stream accounts in this shard's ranges only (one per
        // StakeCredential variant). Each call drains the matching deltas
        // from `self.deltas`, so a delta keyed inside range N stays in
        // the map until range N is streamed.
        for range in ranges {
            self.stream_and_apply_namespace::<D, AccountState>(state, &writer, Some(range))?;
        }

        // EpochState gets the EStartProgress delta (single entity).
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer, None)?;

        // Archive logs — share the start-of-epoch temporal key across shards.
        let start_of_epoch = self.chain_summary.epoch_start(self.starting_epoch_no());
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        debug!(log_count = self.logs.len(), "writing shard archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted shard deltas");
        }

        writer.commit()?;
        archive_writer.commit()?;

        debug!("estart commit complete");
        Ok(())
    }

    /// Commit the finalize half: pool / drep / proposal transitions + the
    /// closing `EpochTransition` + (optional) era-summary writes + archive
    /// logs + cursor advance.
    ///
    /// `AccountState` is intentionally **not** streamed here — per-account
    /// snapshot transitions were committed by the preceding per-shard
    /// runs. The cursor is set only here, so a crash mid-shard restarts
    /// from the boundary block and the pre-finalize state stays at the
    /// previous-epoch cursor.
    #[instrument(skip_all)]
    pub fn commit_finalize<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        slot: BlockSlot,
    ) -> Result<(), ChainError> {
        debug!("committing estart finalize changes");

        // Finalize advances the epoch and rotates every pool in one commit, so
        // it must run exactly once and only after all shards committed. Require
        // `committed == total` and that the epoch has not advanced, turning a
        // would-be double-rotation into a loud error. (Guards finalize only,
        // not per-shard replay — see the "true shard resume" TODO.)
        let ended = self.ended_state();
        let progress = ended.estart_progress.as_ref();
        let all_shards_committed = progress.is_some_and(|p| p.committed == p.total);
        if !all_shards_committed {
            return Err(dolos_core::BrokenInvariant::EpochBoundaryIncomplete {
                epoch: ended.number,
                committed: progress.map(|p| p.committed),
                total: progress.map(|p| p.total),
            }
            .into());
        }

        // Collect era transition data first (only 1-2 entities, not a memory concern)
        let era_transition = self.collect_era_transition(state)?;

        // Prepare archive logs
        let start_of_epoch = self.chain_summary.epoch_start(self.starting_epoch_no());
        let temporal_key = TemporalKey::from(&ChainPoint::Slot(start_of_epoch));

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        // Skip AccountState — committed earlier by per-shard runs.

        debug!("streaming pool entities");
        self.stream_and_apply_namespace::<D, PoolState>(state, &writer, None)?;

        debug!("streaming drep entities");
        self.stream_and_apply_namespace::<D, DRepState>(state, &writer, None)?;

        debug!("streaming proposal entities");
        self.stream_and_apply_namespace::<D, ProposalState>(state, &writer, None)?;

        debug!("streaming epoch entities");
        self.stream_and_apply_namespace::<D, EpochState>(state, &writer, None)?;

        // Governance singleton — targeted apply (the entity may not exist
        // yet; the Conway-boundary `GovGenesisInit` creates it).
        self.apply_gov_state_deltas::<D>(state, &writer)?;

        // Write era transition if needed (only 2 entities)
        if let Some(transition) = era_transition {
            writer
                .write_entity_typed::<EraSummary>(&transition.prev_key, &transition.prev_summary)?;
            writer
                .write_entity_typed::<EraSummary>(&transition.new_key, &transition.new_summary)?;
        }

        // Write archive logs (accumulated during compute_global_deltas, much smaller
        // than entities)
        debug!(log_count = self.logs.len(), "writing archive logs");
        for (entity_key, log) in self.logs.drain(..) {
            let log_key = LogKey::from((temporal_key.clone(), entity_key));
            archive_writer.write_log_typed(&log_key, &log)?;
        }

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // Set cursor — only in finalize, never in shards.
        writer.set_cursor(ChainPoint::Slot(slot))?;

        // Commit both writers atomically
        writer.commit()?;
        archive_writer.commit()?;

        debug!("estart finalize commit complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::{Domain as _, StateStore as _};
    use dolos_testing::toy_domain::ToyDomain;

    use crate::{gov_from_conway_genesis, ChainSummary, EraProtocol, GovGenesisInit};

    use super::*;

    fn empty_context(domain: &ToyDomain) -> super::super::WorkContext {
        super::super::WorkContext {
            ended_state: Default::default(),
            active_protocol: EraProtocol::from(9),
            chain_summary: ChainSummary::default(),
            genesis: domain.genesis(),
            avvm_reclamation: 0,
            deltas: Default::default(),
            logs: Default::default(),
        }
    }

    fn read_gov(domain: &ToyDomain) -> Option<GovState> {
        domain
            .state()
            .read_entity_typed::<GovState>(GovState::NS, &EntityKey::from(GOV_STATE_KEY))
            .unwrap()
    }

    /// The targeted gov commit path must create the singleton when it does
    /// not exist yet — `stream_and_apply_namespace` would silently drop the
    /// `GovGenesisInit` emitted at the Conway boundary.
    #[test]
    fn gov_deltas_create_absent_singleton() {
        let domain = ToyDomain::new(None, None);
        let state = domain.state();

        // the devnet bootstrap seeds the entity; delete it to simulate a
        // store reaching the Chang boundary without one
        let writer = state.start_writer().unwrap();
        writer
            .delete_entity(GovState::NS, &EntityKey::from(GOV_STATE_KEY))
            .unwrap();
        writer.commit().unwrap();
        assert_eq!(read_gov(&domain), None);

        let (constitution, committee) = gov_from_conway_genesis(&domain.genesis().conway).unwrap();

        let mut ctx = empty_context(&domain);
        ctx.add_delta(GovGenesisInit::new(constitution.clone(), committee.clone()));

        let writer = state.start_writer().unwrap();
        ctx.apply_gov_state_deltas::<ToyDomain>(state, &writer)
            .unwrap();
        writer.commit().unwrap();

        let gov = read_gov(&domain).expect("singleton created by delta");
        assert_eq!(gov.constitution, Some(constitution));
        assert_eq!(gov.committee, Some(committee));

        // the queue entry was drained — no double apply possible
        assert!(ctx.deltas.entities.is_empty());
    }

    /// With no queued gov deltas the pass is a no-op that leaves the
    /// existing entity untouched.
    #[test]
    fn gov_apply_without_deltas_is_noop() {
        let domain = ToyDomain::new(None, None);
        let state = domain.state();

        let before = read_gov(&domain).expect("devnet bootstrap seeds the entity");

        let mut ctx = empty_context(&domain);

        let writer = state.start_writer().unwrap();
        ctx.apply_gov_state_deltas::<ToyDomain>(state, &writer)
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(read_gov(&domain), Some(before));
    }
}
