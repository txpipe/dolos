//! The accounts ESTART deletes from state, and how a rollback across the
//! boundary takes them back.
//!
//! A shard deletes each account that is dead once rotated
//! ([`AccountState::is_dead`]) and did not deregister in the closing epoch,
//! instead of writing it back ([`prunes`]). The history the API serves of it
//! ([`AccountState::remainder`]) adds to the credential's `pruned-accounts`
//! row in the same commit.
//!
//! ESTART is never undone, but a rollback across the boundary undoes the
//! closing epoch's last blocks, and their account deltas can reach a row the
//! boundary pruned:
//! - A certificate or a withdrawal needs the account registered, so a dead row
//!   one reached deregistered after it, in the closing epoch. Such a row waits
//!   for the next boundary ([`prunes`]), and the undo finds it.
//! - A value paid to an address the credential controls and spent again leaves
//!   no trace in the row. The rollback restores the row for the undo
//!   ([`restore_for_undo`]), and strips the restored life's history from it,
//!   which the remainder keeps ([`settle_undo`]).
//!
//! A rollback never changes the remainder, so only a prune adds to it.
//!
//! Both hold while the rollback window is shorter than an epoch, as on every
//! shipped network: a longer one could undo a deregistration that the next
//! boundary pruned, and the remainder would keep it.

use dolos_core::{
    BlockSlot, ChainError, EntityMap, FixedNamespace as _, InvariantViolation, NsKey, StateError,
    StateStore, StateWriter as _,
};
use tracing::debug;

use crate::{sub, AccountKey, AccountState, CardanoEntity, EpochState, PrunedAccountState};

/// Whether the shard deletes `account`, rotated: it is dead, and it did not
/// deregister in the closing epoch, which starts at `closing_start`.
///
/// The remainder keeps only the newest deregistration, so it could not give
/// back an earlier one were a rollback to undo this one. The row waits for
/// the next boundary instead.
pub fn prunes(account: &AccountState, closing_start: BlockSlot) -> bool {
    account.is_dead()
        && account
            .deregistered_at
            .is_none_or(|slot| slot < closing_start)
}

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

/// The row a rollback undoes an account delta on when ESTART pruned it after
/// the delta applied: a fresh row at the current epoch. The pruned row held
/// no registration, balance or delegation, and the history it carried stays
/// in the remainder, so the fresh row serves what it did. Any other key
/// restores nothing.
pub fn restore_for_undo<S: StateStore>(
    state: &S,
    key: &NsKey,
) -> Result<Option<CardanoEntity>, StateError> {
    if key.ns() != AccountState::NS {
        return Ok(None);
    }

    let account = key.key().decode::<AccountKey>()?;

    let epoch = state
        .read_entity_typed::<EpochState>(&())?
        .ok_or_else(|| InvariantViolation::EntityNotFound(NsKey::of::<EpochState>(&())))?
        .number;

    debug!(?account, "restored a pruned account for the rollback");

    Ok(Some(AccountState::new(epoch, account.0).into()))
}

/// Once a rollback has undone every delta, strip from each account row the
/// lifetime rewards the undos gave it beyond the row the rollback started
/// from, which `state` still holds. The remainder already counts them.
///
/// Rewards are paid only at a boundary, so a row the undos take back to an
/// earlier value never holds more of them, unless that value is from before
/// ESTART pruned the row and put its history in the remainder:
/// - the undo restored the pruned row, and set its value from before the
///   boundary;
/// - an earlier rollback restored and stripped the row, committed the state and
///   died before the WAL was truncated. Catch-up replayed the logged deltas
///   onto the stripped row, and their undos set its value from before the
///   boundary again.
///
/// Both sums lose the same amount, so the balance the ledger reads stays.
/// A row the undos leave with less, because the boundary paid it, keeps what
/// they left, as on `main`.
pub fn settle_undo<S: StateStore>(
    state: &S,
    entities: &mut EntityMap<CardanoEntity>,
) -> Result<(), StateError> {
    for (key, entity) in entities.iter_mut() {
        let Some(CardanoEntity::AccountState(row)) = entity else {
            continue;
        };

        let rewards = lifetime_rewards(Some(row));

        if rewards == 0 {
            continue;
        }

        let account = key.key().decode::<AccountKey>()?;
        let found = state.read_entity_typed::<AccountState>(&account)?;
        let gained = rewards.saturating_sub(lifetime_rewards(found.as_ref()));

        if gained == 0 {
            continue;
        }

        debug!(
            ?account,
            gained, "stripped a pruned life the rollback restored"
        );

        let live = row.stake.unwrap_live_mut();
        live.rewards_sum -= gained;
        live.withdrawals_sum = sub!(live.withdrawals_sum, gained);
    }

    Ok(())
}

fn lifetime_rewards(row: Option<&AccountState>) -> u64 {
    row.and_then(|x| x.stake.live())
        .map(|x| x.rewards_sum)
        .unwrap_or_default()
}
