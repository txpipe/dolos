//! Ewrap work unit — the close half of the epoch boundary.
//!
//! Sharded: `total_shards()` reports the boundary's shard count and the
//! executor invokes `load` / `commit_state` once per shard. Each shard
//! covers a first-byte prefix range of the account key space, range-loads
//! pending rewards, iterates accounts in range, applies rewards + drops
//! visitors, and emits `EWrapProgress` to populate the reward
//! accumulators on `EpochState.end` (the slot is opened by
//! `EpochTransition` during the previous epoch's Estart finalize).
//!
//! After the per-shard loop, `finalize()` runs the global "Ewrap" pass:
//! pool / drep / proposal classification, MIR processing, deposit refunds,
//! enactment, then assembles the final `EndStats` and emits a single
//! `EpochWrapUp` delta that closes the boundary.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{ewrap::BoundaryWork, load_epoch, CardanoLogic};

use crate::shard::shard_key_ranges;

pub struct EwrapWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    /// Number of shards this boundary's pipeline runs.
    ///
    /// Populated in `initialize()` from `EpochState.ewrap_progress.total`
    /// when a boundary is in flight (so a config change can't disrupt the
    /// in-progress pipeline) or from `config.account_shards()` for a fresh
    /// boundary.
    total_shards: u32,

    /// During the per-shard loop, holds the in-flight shard's
    /// `BoundaryWork` (build-and-discard between shards). After
    /// `finalize()`, holds the global Ewrap pass's `BoundaryWork` so
    /// post-finalize introspection (e.g. tests) can read it.
    boundary: Option<BoundaryWork>,
}

impl EwrapWorkUnit {
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            config,
            genesis,
            total_shards: 0,
            boundary: None,
        }
    }

    pub fn boundary(&self) -> Option<&BoundaryWork> {
        self.boundary.as_ref()
    }
}

impl<D> WorkUnit<D> for EwrapWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ewrap"
    }

    fn total_shards(&self) -> u32 {
        self.total_shards
    }

    fn initialize(&mut self, domain: &D) -> Result<(), DomainError> {
        // Resolve the effective shard count for this boundary. While a
        // boundary is in flight, the persisted `ewrap_progress.total` is
        // authoritative — guards against a config change between shards
        // (e.g. across a crash and restart) breaking the in-flight
        // pipeline. Falls back to current config for a fresh boundary.
        self.total_shards = match load_epoch::<D>(domain.state()) {
            Ok(epoch) => epoch
                .ewrap_progress
                .as_ref()
                .map(|p| p.total)
                .unwrap_or_else(|| self.config.account_shards()),
            Err(_) => self.config.account_shards(),
        };

        debug!(
            slot = self.slot,
            total = self.total_shards,
            "ewrap initialize"
        );
        Ok(())
    }

    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let ranges = shard_key_ranges(shard_index, self.total_shards);

        debug!(
            slot = self.slot,
            shard = shard_index,
            total = self.total_shards,
            "loading ewrap context"
        );

        let boundary = BoundaryWork::load_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            shard_index,
            self.total_shards,
            ranges,
        )?;

        info!(
            epoch = boundary.ending_state().number,
            shard = shard_index,
            "ewrap"
        );

        self.boundary = Some(boundary);

        debug!("ewrap shard context loaded");
        Ok(())
    }

    fn compute(&mut self, _shard_index: u32) -> Result<(), DomainError> {
        debug!("ewrap compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError> {
        let ranges = shard_key_ranges(shard_index, self.total_shards);

        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ewrap boundary not loaded".into()))?;

        boundary.commit_shard::<D>(domain.state(), domain.archive(), ranges)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        Ok(())
    }

    fn finalize(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading ewrap context");

        let mut boundary = BoundaryWork::load_finalize::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = boundary.ending_state().number, "ewrap");

        boundary.commit_finalize::<D>(domain.state(), domain.archive())?;

        // Replace the per-shard boundary state with the finalize-phase
        // BoundaryWork so post-finalize introspection (e.g. tests reading
        // `ending_state()`) sees the global pass's data, not the last
        // shard's leftover slice.
        self.boundary = Some(boundary);

        debug!("ewrap finalize complete");
        Ok(())
    }
}
