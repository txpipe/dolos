//! Estart work unit — the open half of the epoch boundary.
//!
//! Sharded: `total_shards()` reports the boundary's shard count and the
//! executor invokes `load` / `commit_state` once per shard. Each shard
//! covers a first-byte prefix range of the credential key space. Its
//! commit reads the accounts in range once, tallying, rotating and writing
//! each in one state transaction with the `EStartProgress` that advances
//! `EpochState.estart_progress`.
//!
//! After the per-shard loop, `finalize()` seals the period logs of every
//! immutable epoch (`crate::periods`), then runs the global "Estart" pass:
//! pool / drep / proposal snapshot transitions, nonce transition, pot
//! recalculation (`EpochTransition`), era transitions (if protocol version
//! changes), and the cursor advance. The cursor moves only here, so a
//! crash before finalize restarts from the boundary block.
//!
//! The rotation is not idempotent (rotating twice double-rolls the
//! `EpochValue` snapshots), so a resumed boundary skips the committed
//! shards; a shard is all or nothing, as its rows and its progress commit
//! together. `finalize` is separately guarded to run exactly once (see
//! `commit_finalize`).

use std::sync::Arc;

use dolos_core::{BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{
    estart::{stake, AvvmReclamation, PoolOwners, WorkContext},
    load_epoch,
    shard::ACCOUNT_SHARDS,
    CardanoLogic, EpochState,
};

pub struct EstartWorkUnit {
    slot: BlockSlot,
    genesis: Arc<Genesis>,

    /// Number of shards this boundary's pipeline runs.
    ///
    /// Populated in `initialize()` from
    /// `EpochState.estart_progress.total` when a boundary is in
    /// flight (so a value change across versions can't disrupt the
    /// in-progress pipeline) or from `crate::shard::ACCOUNT_SHARDS`
    /// for a fresh boundary.
    total_shards: u32,

    /// First shard to run on this invocation. Populated in `initialize()`
    /// from `EpochState.estart_progress.committed` so a restart after a
    /// mid-boundary crash skips already-committed shards (the rotation is
    /// non-idempotent on replay — it would double-rotate every account in
    /// the replayed shard).
    start_shard: u32,

    /// AVVM reclamation for this boundary, computed once in `initialize()`.
    /// Reused across all shards so the per-shard `load` doesn't re-query the
    /// UTxO set.
    avvm_reclamation: AvvmReclamation,

    /// The owners of every pool in the closing epoch, indexed once in
    /// `initialize()` and shared by every shard's live-pledge tally. Pools
    /// rotate only in `finalize()`, so a resumed boundary rebuilds the same
    /// index.
    owners: Arc<PoolOwners>,

    /// During the per-shard loop, holds the in-flight shard's
    /// `WorkContext` (build-and-discard between shards). After
    /// `finalize()`, holds the global Estart pass's `WorkContext` so
    /// post-finalize introspection (e.g. tests reading `ended_state()`)
    /// sees the global pass's data, not the last shard's leftover slice.
    context: Option<WorkContext>,
}

impl EstartWorkUnit {
    pub fn new(slot: BlockSlot, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            genesis,
            total_shards: 0,
            start_shard: 0,
            avvm_reclamation: AvvmReclamation::default(),
            owners: Default::default(),
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

    fn start_shard(&self) -> u32 {
        self.start_shard
    }

    fn initialize(&mut self, domain: &D) -> Result<(), DomainError> {
        // Resolve the effective shard count + resume cursor for this
        // boundary. While a boundary is in flight, the persisted
        // `estart_progress` is authoritative — `total` guards against a
        // config change mid-boundary, and `committed` lets a restart
        // skip shards whose state already landed (the rotation is
        // non-idempotent on replay — it would double-rotate every
        // account in the replayed shard).
        //
        // Errors propagate: state-read failure must not silently fall
        // back to a fresh boundary's defaults.
        let epoch = load_epoch::<D>(domain.state())?;
        let progress = epoch.estart_progress.as_ref();
        self.total_shards = progress.map(|p| p.total).unwrap_or(ACCOUNT_SHARDS);
        self.start_shard = progress.map(|p| p.committed).unwrap_or(0);

        // Empty unless we're crossing the Shelley→Allegra hardfork — a
        // one-time chain event, but the per-shard cost adds up at that
        // boundary.
        self.avvm_reclamation = AvvmReclamation::at_boundary::<D>(domain.state(), &self.genesis)?;

        self.owners = Arc::new(stake::pool_owners::<D>(domain.state(), epoch.number)?);

        debug!(
            slot = self.slot,
            total = self.total_shards,
            start = self.start_shard,
            avvm = self.avvm_reclamation.total,
            avvm_utxos = self.avvm_reclamation.utxos.len(),
            pool_owners = self.owners.len(),
            "estart initialize"
        );
        Ok(())
    }

    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        debug!(
            slot = self.slot,
            shard = shard_index,
            total = self.total_shards,
            "loading estart context"
        );

        let context = WorkContext::load_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.avvm_reclamation.clone(),
            self.owners.clone(),
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
        debug!("estart compute (accounts rotate during commit)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let ranges = crate::AccountKey::shard_ranges(shard_index, self.total_shards);

        let context = self
            .context
            .as_mut()
            .ok_or_else(|| DomainError::Internal("estart context not loaded".into()))?;

        context.commit_shard::<D>(domain.state(), shard_index, self.total_shards, ranges)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        Ok(())
    }

    fn finalize(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading estart finalize work context");

        let mut context = WorkContext::load_finalize::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = context.starting_epoch_no(), "starting epoch");

        crate::periods::seal_staged::<D>(
            domain.state(),
            domain.archive(),
            &context.chain_summary,
            &self.genesis,
            context.starting_epoch_no(),
        )?;

        context.commit_finalize::<D>(domain.state(), self.slot)?;

        // Replace the per-shard context with the finalize-phase
        // WorkContext so post-finalize introspection (e.g. `ended_state`
        // for tests) sees the global pass's data.
        self.context = Some(context);

        debug!("estart finalize state committed");
        Ok(())
    }
}
