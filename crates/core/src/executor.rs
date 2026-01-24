//! Generic work unit executor.
//!
//! This module provides the infrastructure for executing work units through
//! their complete lifecycle, independent of the specific chain implementation.

use tracing::{debug, info, instrument};

use crate::{Domain, DomainError, WorkUnit};

/// Execute a work unit through its complete lifecycle.
///
/// This function orchestrates the execution of a work unit by calling each
/// lifecycle method in sequence:
///
/// 1. `load()` - Load required data from storage
/// 2. `compute()` - Execute computation over loaded data
/// 3. `commit_wal()` - Persist to write-ahead log
/// 4. `commit_state()` - Apply changes to state store
/// 5. `commit_archive()` - Apply changes to archive store
/// 6. `commit_indexes()` - Apply changes to index stores
/// 7. Notify tip events
///
/// # Type Parameters
///
/// * `D` - The domain type providing storage access
/// * `W` - The work unit type to execute
///
/// # Errors
///
/// Returns an error if any phase of execution fails. The error will indicate
/// which phase failed.
///
/// # Example
///
/// ```ignore
/// use dolos_core::{executor::execute_work_unit, Domain, WorkUnit};
///
/// fn process_work<D: Domain>(domain: &D, work: &mut impl WorkUnit<D>) {
///     execute_work_unit(domain, work).expect("work unit execution failed");
/// }
/// ```
#[instrument(skip_all, fields(work_unit = %work.name()))]
pub fn execute_work_unit<D: Domain, W: WorkUnit<D> + ?Sized>(
    domain: &D,
    work: &mut W,
) -> Result<(), DomainError> {
    info!("executing work unit");

    work.load(domain)?;
    debug!("load phase complete");

    work.compute()?;
    debug!("compute phase complete");

    work.commit_wal(domain)?;
    debug!("wal commit complete");

    work.commit_state(domain)?;
    debug!("state commit complete");

    work.commit_archive(domain)?;
    debug!("archive commit complete");

    work.commit_indexes(domain)?;
    debug!("index commit complete");

    // Notify tip events to subscribers
    for event in work.tip_events() {
        domain.notify_tip(event);
    }

    info!("work unit completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
