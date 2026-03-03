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
/// The lifecycle of a work unit consists of several phases:
///
/// 1. **Definition** - Lightweight construction with required parameters.
///    This happens when the work unit is created by the chain logic.
///
/// 2. **Loading** - Query state/archive stores to gather data needed for
///    execution. This phase may involve I/O operations.
///
/// 3. **Compute** - Execute CPU-intensive work over the loaded data.
///    This phase should NOT access storage.
///
/// 4. **Commit WAL** - Persist to write-ahead log for crash recovery.
///    Called after compute, before state commits.
///
/// 5. **Commit State** - Apply computed changes to the state store.
///
/// 6. **Commit Archive** - Apply computed changes to the archive store.
///
/// 7. **Commit Indexes** - Apply computed changes to index stores (optional).
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
    fn load(&mut self, domain: &D) -> Result<(), DomainError>;

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
    fn compute(&mut self) -> Result<(), DomainError>;

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
    fn commit_wal(&mut self, _domain: &D) -> Result<(), DomainError> {
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
    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError>;

    /// Apply computed changes to the archive store.
    ///
    /// This phase persists historical data and logs to the archive.
    /// It is called after `commit_state()`.
    ///
    /// # Errors
    ///
    /// Returns an error if archive persistence fails.
    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError>;

    /// Apply computed changes to index stores.
    ///
    /// This phase updates any additional indexes maintained by the node.
    /// The default implementation does nothing, which is appropriate for
    /// work units that don't require index updates.
    ///
    /// # Errors
    ///
    /// Returns an error if index persistence fails.
    fn commit_indexes(&mut self, _domain: &D) -> Result<(), DomainError> {
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
