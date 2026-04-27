//! EStartShard work unit — per-account leg of the epoch-start pipeline.
//!
//! Emitted `total_shards` times in sequence after Ewrap. Each shard covers
//! a first-byte prefix range of the credential key space, iterates accounts
//! in range, runs the snapshot-rotation visitor (`AccountTransition`), and
//! emits `EStartShardAccumulate` to advance
//! `EpochState.estart_shard_progress`.
//!
//! Followed by the `EstartWorkUnit` finalize phase which closes the
//! boundary by emitting pool / drep / proposal transitions and the single
//! `EpochTransition`.
//!
//! The cursor is **not** advanced per shard — only the finalize unit moves
//! it. A crash mid-shard restarts from the boundary block and idempotency
//! rests on `EStartShardAccumulate.committed` guard. Note that
//! `AccountTransition` is not natively idempotent (re-applying double-rolls
//! the EpochValue snapshot), so true mid-shard resume requires additional
//! work. Same TODO posture as `AShardWorkUnit`.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{estart::WorkContext, load_epoch, shard::shard_key_ranges, CardanoLogic};

pub struct EStartShardWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,
    shard_index: u32,

    context: Option<WorkContext>,
}

impl EStartShardWorkUnit {
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
            context: None,
        }
    }

    pub fn shard_index(&self) -> u32 {
        self.shard_index
    }

    pub fn context(&self) -> Option<&WorkContext> {
        self.context.as_ref()
    }
}

impl<D> WorkUnit<D> for EStartShardWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "estart_shard"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        // If a boundary is in flight, the persisted
        // `estart_shard_progress.total` is authoritative — guards against
        // a config change between shards (e.g. across a crash and
        // restart). Falls back to current config for a fresh boundary.
        let total_shards = match load_epoch::<D>(domain.state()) {
            Ok(epoch) => epoch
                .estart_shard_progress
                .as_ref()
                .map(|p| p.total)
                .unwrap_or_else(|| self.config.account_shards()),
            Err(_) => self.config.account_shards(),
        };
        let ranges = shard_key_ranges(self.shard_index, total_shards);

        debug!(
            slot = self.slot,
            shard = self.shard_index,
            total = total_shards,
            "loading estart_shard context"
        );

        let context = WorkContext::load_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.shard_index,
            total_shards,
            ranges,
        )?;

        info!(
            epoch = context.starting_epoch_no(),
            shard = self.shard_index,
            "estart_shard"
        );

        self.context = Some(context);

        debug!("estart_shard context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("estart_shard compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        // Mirror the same effective-total logic as `load` so the commit's
        // key range matches what `load` used.
        let total_shards = match load_epoch::<D>(domain.state()) {
            Ok(epoch) => epoch
                .estart_shard_progress
                .as_ref()
                .map(|p| p.total)
                .unwrap_or_else(|| self.config.account_shards()),
            Err(_) => self.config.account_shards(),
        };
        let ranges = shard_key_ranges(self.shard_index, total_shards);

        let context = self
            .context
            .as_mut()
            .ok_or_else(|| DomainError::Internal("estart_shard context not loaded".into()))?;

        context.commit_shard::<D>(domain.state(), domain.archive(), ranges)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}
