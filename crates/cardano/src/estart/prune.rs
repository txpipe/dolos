//! The accounts ESTART deletes from state.
//!
//! A shard deletes each account that is dead once rotated
//! ([`AccountState::is_dead`]) instead of writing it back. The history the
//! API serves of it ([`AccountState::remainder`]) adds to the credential's
//! `pruned-accounts` row in the same commit.

use dolos_core::{ChainError, StateStore, StateWriter as _};

use crate::{AccountKey, AccountState, PrunedAccountState};

/// Delete the dead, rotated `account` from state, and add its history to the
/// credential's `pruned-accounts` row. Returns whether it wrote that row.
///
/// The row is read from `state`, which the shard's own commit has not
/// written yet. That is exact: a credential appears once in a shard, and a
/// committed shard never runs again, so each pruned life adds once.
pub fn prune_account<S: StateStore>(
    state: &S,
    writer: &S::Writer,
    key: &AccountKey,
    account: &AccountState,
) -> Result<bool, ChainError> {
    writer.save_entity_typed::<AccountState>(key, None)?;

    let Some(remainder) = account.remainder() else {
        return Ok(false);
    };

    let row = match state.read_entity_typed::<PrunedAccountState>(key)? {
        Some(mut row) => {
            row.absorb(&remainder);
            row
        }
        None => remainder,
    };

    writer.write_entity_typed(key, &row)?;

    Ok(true)
}
