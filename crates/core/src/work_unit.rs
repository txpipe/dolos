//! Work Unit trait and related types.
//!
//! A work unit represents a unit of work that is defined by the chain-specific
//! logic but executed by the generic node infrastructure. This abstraction
//! allows the core crate to remain chain-agnostic while supporting different
//! blockchain implementations.

use crate::{ChainPoint, Domain, DomainError, TipEvent, TxHash};

/// An update for the mempool based on a confirmed block.
pub struct MempoolUpdate {
    pub point: ChainPoint,
    pub seen_txs: Vec<TxHash>,
}

/// A unit of work defined by the chain but executed by the node infrastructure.
///
/// Every work unit is conceptually sharded: `total_shards()` reports how
/// many shards the work splits into, and the executor invokes the load /
/// compute / commit phases once per shard. Work units that don't need
/// sharding take the default `total_shards() = 1` and ignore the
/// `shard_index` parameter passed to each phase.
///
/// The lifecycle is:
///
/// 1. **Definition** - Lightweight construction with required parameters.
///    Happens when the work unit is created by the chain logic.
///
/// 2. **Initialize** - Shard-agnostic setup that runs once before any
///    shard. The implementation can use this to compute and cache its
///    `total_shards()` value, hoist boundary-wide reads out of the
///    per-shard loop, etc.
///
/// 3. **Per-shard loop**, repeated `total_shards()` times with `shard_index`
///    advancing from `0` to `total_shards() - 1`:
///
///    a. **Load** - Query state/archive stores for this shard.
///    b. **Compute** - CPU work over loaded data; no storage access.
///    c. **Commit WAL** - Persist to write-ahead log.
///    d. **Commit State** - Apply changes to the state store.
///    e. **Commit Archive** - Apply changes to the archive store.
///    f. **Commit Indexes** - Apply changes to index stores.
///
/// 4. **Finalize** - Shard-agnostic teardown that runs once after the
///    last shard's commits succeed.
///
/// 5. **Tip notifications + mempool updates** - shard-agnostic, fired once
///    after `finalize()`.
///
/// # Type Parameters
///
/// * `D` - The domain type that provides access to storage and configuration.
pub trait WorkUnit<D: Domain>: Send {
    /// Human-readable name for logging and debugging.
    ///
    /// This should return a static string identifying the type of work unit,
    /// such as "genesis", "roll", "rupd", "ewrap", or "estart".
    fn name(&self) -> &'static str;

    /// Number of shards this work unit splits into.
    ///
    /// The executor calls each per-shard phase `total_shards()` times.
    /// Defaults to `1` for non-sharded work units. The returned value
    /// must be valid after `initialize()` has run — implementations that
    /// derive the count from persisted state should compute it inside
    /// `initialize()` and cache it on `self`.
    fn total_shards(&self) -> u32 {
        1
    }

    /// First shard the executor will run.
    ///
    /// Defaults to `0` (run every shard from the start). Restart-aware
    /// work units override this to resume mid-pipeline after a crash:
    /// they read a persisted commit cursor (e.g. `*_progress.committed`)
    /// in `initialize()` and return it here. The executor then runs
    /// `start_shard()..total_shards()` so already-committed shards are
    /// skipped, which matters when the per-shard deltas are
    /// non-idempotent (replaying a committed shard would double-apply).
    ///
    /// Must satisfy `start_shard() <= total_shards()`. The returned
    /// value must be valid after `initialize()` has run.
    fn start_shard(&self) -> u32 {
        0
    }

    /// Shard-agnostic setup, run once before any shard executes.
    ///
    /// Use this to compute `total_shards()`, load boundary-wide data
    /// that doesn't depend on the shard, or perform any other one-shot
    /// preparation. The default implementation does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if setup fails.
    fn initialize(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }

    /// Load data from state/archive stores needed for computation.
    ///
    /// This phase is called before `compute()` and is the appropriate place
    /// to query the state store, archive, or other data sources to gather
    /// all information needed for the computation phase.
    ///
    /// # Errors
    ///
    /// Returns an error if data loading fails (e.g., storage errors,
    /// missing required data).
    fn load(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError>;

    /// Execute CPU-intensive computation over loaded data.
    ///
    /// This phase should NOT access storage. All required data should have
    /// been loaded in the `load()` phase. This separation allows for
    /// potential parallelization and clearer resource management.
    ///
    /// # Errors
    ///
    /// Returns an error if computation fails (e.g., invalid data,
    /// computation errors).
    fn compute(&mut self, shard_index: u32) -> Result<(), DomainError>;

    /// Persist to write-ahead log for crash recovery.
    ///
    /// Called after `compute()`, before state commits. This ensures that
    /// the work can be recovered in case of a crash during the commit phase.
    ///
    /// The default implementation does nothing, which is appropriate for
    /// work units that don't require WAL persistence (e.g., boundary work
    /// units like ewrap, estart).
    ///
    /// # Errors
    ///
    /// Returns an error if WAL persistence fails.
    fn commit_wal(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        Ok(())
    }

    /// Apply computed changes to the state store.
    ///
    /// This phase persists the results of computation to the ledger state.
    /// It should be called after `commit_wal()` to ensure crash recovery
    /// is possible.
    ///
    /// # Errors
    ///
    /// Returns an error if state persistence fails.
    fn commit_state(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError>;

    /// Apply computed changes to the archive store.
    ///
    /// This phase persists historical data and logs to the archive.
    /// It is called after `commit_state()`.
    ///
    /// # Errors
    ///
    /// Returns an error if archive persistence fails.
    fn commit_archive(&mut self, domain: &D, shard_index: u32) -> Result<(), DomainError>;

    /// Apply computed changes to index stores.
    ///
    /// This phase updates any additional indexes maintained by the node.
    /// The default implementation does nothing, which is appropriate for
    /// work units that don't require index updates.
    ///
    /// # Errors
    ///
    /// Returns an error if index persistence fails.
    fn commit_indexes(&mut self, _domain: &D, _shard_index: u32) -> Result<(), DomainError> {
        Ok(())
    }

    /// Shard-agnostic teardown, run once after the last shard's commits.
    ///
    /// The default implementation does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if teardown fails.
    fn finalize(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }

    /// Return tip events for notification after completion.
    ///
    /// Used for live sync notifications. Work units that affect the chain
    /// tip should return appropriate events here so that subscribers can
    /// be notified of the changes.
    ///
    /// The default implementation returns an empty vector, which is
    /// appropriate for work units that don't produce tip events
    /// (e.g., boundary work units).
    fn tip_events(&self) -> Vec<TipEvent> {
        Vec::new()
    }

    /// Return mempool updates for blocks processed by this work unit.
    ///
    /// Used to transition mempool transactions to confirmed/finalized/dropped
    /// as blocks arrive. The default implementation returns an empty vector,
    /// which is appropriate for work units that don't process blocks.
    fn mempool_updates(&self) -> Vec<MempoolUpdate> {
        Vec::new()
    }
}
