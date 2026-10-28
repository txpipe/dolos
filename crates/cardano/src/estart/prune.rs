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
//!   ([`restore_for_undo`]) and takes the restored life's history back out of
//!   the remainder ([`settle_undo`]).
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

    Ok(Some(AccountState::new(epoch, account.0).into()))
}

/// Once a rollback has undone every delta, settle the remainder of each
/// credential whose row it restored. Rewards are paid only at a boundary,
/// which no rollback undoes, and the closing boundary paid the unregistered
/// pruned row nothing. So the rollback leaves the credential's lifetime
/// rewards, row and remainder together, as it found them: the remainder
/// gives up what the restored row holds beyond the row the rollback started
/// from, which the store still holds.
///
/// That row is absent, or fresh from the opening epoch, with no rewards, and
/// the remainder gives up the restored life's: the undo of a value delta put
/// them back in the row. Or it is the restored row itself: an earlier attempt
/// at this rollback committed the state and died before the WAL was
/// truncated, catch-up replayed the logged deltas onto the restored row, and
/// a `was_new` logged before the rollback deletes it again. The remainder,
/// settled once, gives up nothing.
///
/// The restored life's deregistration, older than the closing epoch, stays in
/// the remainder, which serves it while the row has neither date. A restored
/// row the undos removed again began inside the rollback. It was never
/// registered, so its prune moved no history.
pub fn settle_undo<S: StateStore>(
    state: &S,
    entities: &mut EntityMap<CardanoEntity>,
    restored: &[NsKey],
) -> Result<(), StateError> {
    for key in restored {
        let Some(Some(CardanoEntity::AccountState(row))) = entities.get(key) else {
            continue;
        };

        let account = key.key().decode::<AccountKey>()?;
        let found = state.read_entity_typed::<AccountState>(&account)?;
        let rewards = sub!(
            lifetime_rewards(Some(row)),
            lifetime_rewards(found.as_ref())
        );

        debug!(
            ?account,
            rewards, "restored a pruned account for the rollback"
        );

        if rewards == 0 {
            continue;
        }

        let Some(remainder) = state.read_entity_typed::<PrunedAccountState>(&account)? else {
            continue;
        };

        entities.insert(
            NsKey::of::<PrunedAccountState>(&account),
            remainder.without(rewards).map(Into::into),
        );
    }

    Ok(())
}

fn lifetime_rewards(row: Option<&AccountState>) -> u64 {
    row.and_then(|x| x.stake.live())
        .map(|x| x.rewards_sum)
        .unwrap_or_default()
}
