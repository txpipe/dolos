//! Ewrap (Epoch Wrap) work units — two-phase boundary pipeline.
//!
//! The epoch boundary is partitioned into two distinct work units scheduled
//! sequentially by the chain logic:
//!
//! 1. `AccountShardWorkUnit { shard_index }` — emitted `total_shards` times.
//!    Each shard covers a first-byte prefix range of the account key space,
//!    range-loads pending rewards, iterates accounts in range, applies
//!    rewards + drops visitors, and emits `EpochEndAccumulate` to populate
//!    the reward accumulators on `EpochState.end` (the slot is opened by
//!    `EpochTransition` during ESTART).
//! 2. `EwrapWorkUnit` — global (non-account) work and the boundary close:
//!    pool/drep/proposal classification, enactment, MIR processing, deposit
//!    refunds, then assembles the final `EndStats` (combining prepare-time
//!    fields with the shard-populated accumulators) and emits a single
//!    `EpochWrapUp` delta. `EpochWrapUp::apply` overwrites `entity.end` with
//!    the final stats, rotates rolling/pparams snapshots forward, and
//!    clears `ewrap_progress`. The completed `EpochState` is also written
//!    to archive at commit time.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::CardanoLogic;

use super::{shard::shard_key_range, BoundaryWork};

// ---------------------------------------------------------------------------
// EwrapWorkUnit
// ---------------------------------------------------------------------------

pub struct EwrapWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    boundary: Option<BoundaryWork>,
}

impl EwrapWorkUnit {
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            config,
            genesis,
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

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading ewrap context");

        let boundary = BoundaryWork::load_ewrap::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = boundary.ending_state().number, "ewrap");

        self.boundary = Some(boundary);

        debug!("ewrap context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("ewrap compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ewrap boundary not loaded".into()))?;

        boundary.commit_ewrap::<D>(domain.state(), domain.archive())?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AccountShardWorkUnit
// ---------------------------------------------------------------------------

pub struct AccountShardWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,
    shard_index: u32,

    boundary: Option<BoundaryWork>,
}

impl AccountShardWorkUnit {
    pub fn new(
        slot: BlockSlot,
        config: CardanoConfig,
        genesis: Arc<Genesis>,
        shard_index: u32,
    ) -> Self {
        Self {
            slot,
            config,
            genesis,
            shard_index,
            boundary: None,
        }
    }

    pub fn shard_index(&self) -> u32 {
        self.shard_index
    }

    pub fn boundary(&self) -> Option<&BoundaryWork> {
        self.boundary.as_ref()
    }
}

impl<D> WorkUnit<D> for AccountShardWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "account_shard"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.ewrap_total_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        debug!(
            slot = self.slot,
            shard = self.shard_index,
            total = total_shards,
            "loading account shard context"
        );

        let boundary = BoundaryWork::load_account_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.shard_index,
            range,
        )?;

        info!(
            epoch = boundary.ending_state().number,
            shard = self.shard_index,
            "account shard"
        );

        self.boundary = Some(boundary);

        debug!("account shard context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("account shard compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.ewrap_total_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("account shard boundary not loaded".into()))?;

        boundary.commit_account_shard::<D>(domain.state(), domain.archive(), range)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}

