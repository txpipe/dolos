//! Commit logic for the open half of the epoch boundary (per-shard runs
//! plus the finalize pass).
//!
//! Both code paths stream: each entity namespace is read one record at a
//! time and the result is written immediately. Per-shard commits rotate
//! the shard's account-range slice in that same pass and flush
//! `EpochState`'s `EStartProgress`; the finalize commit applies the
//! queued pool / drep / proposal transitions, the closing
//! `EpochTransition` and optional era-summary writes, and advances the
//! cursor.

use std::collections::BTreeMap;

use dolos_core::{
    save_entity_at, ArchiveStore, ArchiveWriter, BlockSlot, BrokenInvariant, ChainError,
    ChainPoint, Domain, EntityDelta as _, Genesis, NsKey, RangeOf, StateEntity, StateStore,
    StateWriter,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    forks, AccountKey, AccountState, CardanoEntity, DRepState, EStartProgress, EpochState, EraKey,
    EraState, GovState, PoolState, ProposalState,
};

/// Close `previous` at `starting_epoch` and open the era that `new_version`
/// starts there, sized with that protocol's epoch and slot lengths.
pub(super) fn split_era(
    mut previous: EraState,
    starting_epoch: u64,
    new_version: EraKey,
    genesis: &Genesis,
) -> (EraState, EraState) {
    previous.define_end(starting_epoch);

    let consts = forks::protocol_constants(new_version.into(), genesis);

    let new = EraState {
        start: previous.end.clone().unwrap(),
        end: None,
        epoch_length: consts.epoch_length,
        slot_length: consts.slot_length,
        protocol: new_version.into(),
    };

    (previous, new)
}

/// Era transition data collected from state.
struct EraTransitionData {
    prev_key: EraKey,
    prev_summary: EraState,
    new_key: EraKey,
    new_summary: EraState,
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

        let previous = state.read_entity_typed::<EraState>(&transition.prev_version)?;

        let Some(previous) = previous else {
            return Err(BrokenInvariant::BadBootstrap.into());
        };

        let (previous, new) = split_era(
            previous,
            self.starting_epoch_no(),
            transition.new_version,
            &self.genesis,
        );

        Ok(Some(EraTransitionData {
            prev_key: transition.prev_version,
            prev_summary: previous,
            new_key: transition.new_version,
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
        range: Option<RangeOf<E::Key>>,
    ) -> Result<(), ChainError>
    where
        D: Domain,
        E: StateEntity + Into<CardanoEntity>,
    {
        let records = state.iter_entities_typed::<E>(range)?;

        for record in records {
            let (entity_id, entity) = record?;
            let key = NsKey::of::<E>(&entity_id);

            // Check if this entity has deltas to apply
            let to_apply = self.deltas.entities.remove(&key);

            if let Some(to_apply) = to_apply {
                let mut entity: Option<CardanoEntity> = Some(entity.into());

                for mut delta in to_apply {
                    delta.apply(&mut entity);
                }

                // Write immediately - don't collect!
                save_entity_at(writer, &key, entity.as_ref())?;
            } else {
                trace!(%key, "no deltas for entity");
            }
        }

        Ok(())
    }

    /// Commit a single per-shard run in one state transaction: stream the
    /// accounts of the shard's key ranges once, adding each to the shard's
    /// tally and writing its stake-only account-epoch row to the archive as
    /// it stands, then rotating it and writing it back. The shard's
    /// `EStartProgress` (carrying the tally) applies to `EpochState` in the
    /// same transaction, after the rows reach the archive.
    ///
    /// No per-account deltas are built: ESTART writes no WAL and is never
    /// undone. A crash before the state commit leaves the shard's accounts
    /// untouched, and the resumed boundary reads them again.
    ///
    /// **Does not advance the cursor.** Cursor moves only in
    /// `commit_finalize`.
    #[instrument(skip(self, state, archive))]
    pub fn commit_shard<D: Domain>(
        &mut self,
        state: &D::State,
        archive: &D::Archive,
        shard_index: u32,
        total_shards: u32,
        ranges: Vec<RangeOf<AccountKey>>,
    ) -> Result<(), ChainError> {
        debug!("committing estart changes");

        let writer = state.start_writer()?;
        let archive_writer = archive.start_writer()?;

        let next_epoch = self.starting_epoch_no();
        let slot = self.account_epoch_slot();
        let mut tally = BTreeMap::new();

        for range in ranges {
            for record in state.iter_entities_typed::<AccountState>(Some(range))? {
                let (account_id, mut account) = record?;

                self.tally_account(&mut tally, &account);

                if let Some(row) = self.account_epoch_row(&account) {
                    archive_writer.write_log_typed(slot, &account_id, &row)?;
                }

                account.rotate(next_epoch);

                writer.write_entity_typed::<AccountState>(&account_id, &account)?;
            }
        }

        self.add_delta(EStartProgress::new(shard_index, total_shards, tally));

        self.deltas
            .apply_singleton::<EpochState, _>(state, &writer)?;

        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted shard deltas");
        }

        // Archive first, as EWRAP's shards do: the state commit marks the
        // shard done in `EStartProgress`, and a restart never re-runs a done
        // shard. A crash in between re-runs the shard from the same
        // pre-rotation accounts, which writes the same rows under the same
        // keys again.
        archive_writer.commit()?;
        writer.commit()?;

        debug!("estart commit complete");
        Ok(())
    }

    /// Commit the finalize half: pool / drep / proposal transitions + the
    /// closing `EpochTransition` + (optional) era-summary writes + cursor
    /// advance.
    ///
    /// `AccountState` is intentionally **not** streamed here — per-account
    /// snapshot transitions were committed by the preceding per-shard
    /// runs. The cursor is set only here, so a crash mid-shard restarts
    /// from the boundary block and the pre-finalize state stays at the
    /// previous-epoch cursor.
    ///
    /// It is also where the Shelley→Allegra AVVM reclamation deletes its
    /// UTxOs — see [`crate::estart::avvm`] for why that belongs in the same
    /// transaction as the pot delta.
    #[instrument(skip_all)]
    pub fn commit_finalize<D: Domain>(
        &mut self,
        state: &D::State,
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

        let writer = state.start_writer()?;

        // Skip AccountState — committed earlier by per-shard runs.

        debug!("streaming pool entities");
        self.stream_and_apply_namespace::<D, PoolState>(state, &writer, None)?;

        debug!("streaming drep entities");
        self.stream_and_apply_namespace::<D, DRepState>(state, &writer, None)?;

        debug!("streaming proposal entities");
        self.stream_and_apply_namespace::<D, ProposalState>(state, &writer, None)?;

        debug!("applying singleton deltas");
        self.deltas
            .apply_singleton::<EpochState, _>(state, &writer)?;

        // Gov isn't streamed by namespace; its boundary deltas (e.g. the
        // Conway-boundary `GovGenesisInit`) go through the singleton path.
        self.deltas.apply_singleton::<GovState, _>(state, &writer)?;

        let avvm_deletion =
            (!self.avvm_reclamation.is_empty()).then(|| self.avvm_reclamation.deletion_delta());

        if let Some(delta) = avvm_deletion.as_ref() {
            debug!(
                count = self.avvm_reclamation.utxos.len(),
                total = self.avvm_reclamation.total,
                "deleting unredeemed AVVM utxos"
            );

            // The by-address (and every other UTxO filter) tag has to lose
            // the reclaimed refs in the same commit, or the serving APIs keep
            // answering with outputs the state store no longer holds.
            writer.apply_utxoset(delta)?;
            writer.apply_utxo_tags(&crate::indexes::utxo_index_delta_from_utxo_delta(delta))?;
        }

        // Write era transition if needed (only 2 entities)
        if let Some(transition) = era_transition {
            writer
                .write_entity_typed::<EraState>(&transition.prev_key, &transition.prev_summary)?;
            writer.write_entity_typed::<EraState>(&transition.new_key, &transition.new_summary)?;
        }

        // Verify all deltas were processed
        if !self.deltas.entities.is_empty() {
            warn!(quantity = %self.deltas.entities.len(), "uncommitted deltas");
        }

        // Set cursor — only in finalize, never in shards.
        writer.set_cursor(ChainPoint::Slot(slot))?;

        writer.commit()?;

        debug!("estart finalize commit complete");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::{Domain as _, StateStore as _};
    use dolos_testing::toy_domain::ToyDomain;

    use crate::{gov_from_conway_genesis, ChainSummary, EraKey, GovGenesisInit};

    use super::*;

    fn empty_context(domain: &ToyDomain) -> super::super::WorkContext {
        super::super::WorkContext {
            ended_state: Default::default(),
            active_protocol: EraKey::from(9),
            chain_summary: ChainSummary::default(),
            genesis: domain.genesis(),
            avvm_reclamation: Default::default(),
            inputs: Default::default(),
            tally_protocol: EraKey::from(9),
            deltas: Default::default(),
            active_stake: None,
        }
    }

    fn read_gov(domain: &ToyDomain) -> Option<GovState> {
        domain.state().read_entity_typed::<GovState>(&()).unwrap()
    }

    /// `GovGenesisInit` applied through the singleton path activates the
    /// existing (inactive) row with the genesis enact-state.
    #[test]
    fn gov_genesis_init_activates_existing_singleton() {
        let domain = ToyDomain::new(None, None);
        let state = domain.state();

        // the devnet bootstrap activates the row; reset it to the
        // inactive state a chain crossing Chang would carry
        let writer = state.start_writer().unwrap();
        writer
            .write_entity_typed(&(), &GovState::default())
            .unwrap();
        writer.commit().unwrap();
        assert_eq!(read_gov(&domain), Some(GovState::default()));

        let (constitution, committee) = gov_from_conway_genesis(&domain.genesis().conway).unwrap();

        let mut ctx = empty_context(&domain);
        ctx.add_delta(GovGenesisInit::new(
            constitution.clone(),
            committee.clone(),
            507,
        ));

        let writer = state.start_writer().unwrap();
        ctx.deltas
            .apply_singleton::<GovState, _>(state, &writer)
            .unwrap();
        writer.commit().unwrap();

        let gov = read_gov(&domain).expect("singleton exists");
        assert_eq!(gov.constitution, Some(constitution));
        assert_eq!(gov.committee, Some(committee));
        assert_eq!(gov.active_since, Some(507));

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
        ctx.deltas
            .apply_singleton::<GovState, _>(state, &writer)
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(read_gov(&domain), Some(before));
    }
}
