//! Estart work unit — the open half of the epoch boundary.
//!
//! Sharded: `total_shards()` reports the boundary's shard count and the
//! executor invokes `load` / `commit_state` once per shard. Each shard
//! covers a first-byte prefix range of the credential key space, iterates
//! accounts in range, runs the snapshot-rotation visitor
//! (`AccountTransition`), and emits `EStartProgress` to advance
//! `EpochState.estart_progress`.
//!
//! After the per-shard loop, `finalize()` runs the global "Estart" pass:
//! pool / drep / proposal snapshot transitions, nonce transition, pot
//! recalculation (`EpochTransition`), era transitions (if protocol version
//! changes), and the cursor advance. The cursor moves only here, so a
//! crash before finalize restarts from the boundary block.
//!
//! `AccountTransition` is not natively idempotent (re-applying double-rolls
//! the EpochValue snapshot), so true mid-shard resume requires additional
//! work — same TODO posture as `EwrapWorkUnit`.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{
    estart::WorkContext, load_epoch, shard::shard_key_ranges, CardanoLogic, EpochState,
};

pub struct EstartWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    /// Number of shards this boundary's pipeline runs.
    ///
    /// Populated in `initialize()` from
    /// `EpochState.estart_progress.total` when a boundary is in
    /// flight (so a config change can't disrupt the in-progress pipeline)
    /// or from `config.account_shards()` for a fresh boundary.
    total_shards: u32,

    /// AVVM reclamation total for this boundary, computed once in
    /// `initialize()`. Reused across all shards so the per-shard `load`
    /// doesn't re-query the UTxO set.
    avvm_reclamation: u64,

    /// During the per-shard loop, holds the in-flight shard's
    /// `WorkContext` (build-and-discard between shards). After
    /// `finalize()`, holds the global Estart pass's `WorkContext` so
    /// post-finalize introspection (e.g. tests reading `ended_state()`)
    /// sees the global pass's data, not the last shard's leftover slice.
    context: Option<WorkContext>,
}

impl EstartWorkUnit {
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            config,
            genesis,
            total_shards: 0,
            avvm_reclamation: 0,
            context: None,
        }
    }

    pub fn context(&self) -> Option<&WorkContext> {
        self.context.as_ref()
    }

    /// The completed (ended) epoch state, available after `finalize()`
    /// has run. Returns `None` during the per-shard loop or before
    /// initialize.
    pub fn ended_state(&self) -> Option<&EpochState> {
        self.context.as_ref().map(|ctx| ctx.ended_state())
    }
}

impl<D> WorkUnit<D> for EstartWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "estart"
    }

    fn total_shards(&self) -> u32 {
        self.total_shards
    }

    fn initialize(&mut self, domain: &D) -> Result<(), DomainError> {
        // Resolve the effective shard count for this boundary. While a
        // boundary is in flight, the persisted
        // `estart_progress.total` is authoritative — guards against
        // a config change between shards (e.g. across a crash and
        // restart). Falls back to current config for a fresh boundary.
        self.total_shards = match load_epoch::<D>(domain.state()) {
            Ok(epoch) => epoch
                .estart_progress
                .as_ref()
                .map(|p| p.total)
                .unwrap_or_else(|| self.config.account_shards()),
            Err(_) => self.config.account_shards(),
        };

        // Compute AVVM reclamation once per boundary instead of once per
        // shard. Returns 0 unless we're crossing the Shelley→Allegra
        // hardfork — a one-time chain event, but the per-shard cost adds
        // up at that boundary.
        self.avvm_reclamation =
            WorkContext::compute_boundary_avvm::<D>(domain.state(), &self.genesis)?;

        debug!(
            slot = self.slot,
            total = self.total_shards,
            avvm = self.avvm_reclamation,
            "estart initialize"
        );
        Ok(())
    }

    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let ranges = shard_key_ranges(shard_index, self.total_shards);

        debug!(
            slot = self.slot,
            shard = shard_index,
            total = self.total_shards,
            "loading estart context"
        );

        let context = WorkContext::load_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.avvm_reclamation,
            shard_index,
            self.total_shards,
            ranges,
        )?;

        info!(
            epoch = context.starting_epoch_no(),
            shard = shard_index,
            "estart"
        );

        self.context = Some(context);

        debug!("estart context loaded");
        Ok(())
    }

    fn compute(&mut self, _shard_index: u32) -> Result<(), DomainError> {
        debug!("estart compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let ranges = shard_key_ranges(shard_index, self.total_shards);

        let context = self
            .context
            .as_mut()
            .ok_or_else(|| DomainError::Internal("estart context not loaded".into()))?;

        context.commit_shard::<D>(domain.state(), domain.archive(), ranges)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        Ok(())
    }

    fn finalize(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading estart finalize work context");

        let mut context = WorkContext::load_finalize::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = context.starting_epoch_no(), "starting epoch");

        context.commit_finalize::<D>(domain.state(), domain.archive(), self.slot)?;

        // Replace the per-shard context with the finalize-phase
        // WorkContext so post-finalize introspection (e.g. `ended_state`
        // for tests) sees the global pass's data.
        self.context = Some(context);

        debug!("estart finalize state committed");
        Ok(())
    }
}
