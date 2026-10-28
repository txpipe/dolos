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

/// Once a rollback has undone every delta, strip the lifetime rewards from
/// each account row no boundary rotated. The remainder already counts them.
///
/// Rewards are paid only at EWRAP, and ESTART then rotates every row it keeps,
/// which gives the row a `mark` snapshot. A row without one was created in the
/// opening epoch, by a block or by [`restore_for_undo`], and earned nothing.
/// Any rewards the undos give it are from the life ESTART pruned:
/// - the undo restored the pruned row, and set its value from before the
///   boundary;
/// - an earlier rollback restored and stripped the row, committed the state and
///   died before the WAL was truncated. Catch-up replayed the logged deltas
///   onto the stripped row, and their undos set its value from before the
///   boundary again.
///
/// A rotated row was in state through the boundary, so ESTART did not prune
/// it, and it keeps what the undos leave, as on `main`. That holds even when
/// a crashed rollback's catch-up replayed it without the boundary's payment
/// and the undos give that payment back. A block's undo sets only the live
/// stake value, so each row keeps the snapshots the store held.
///
/// Both sums lose the same amount, so the balance the ledger reads stays.
pub fn settle_undo(entities: &mut EntityMap<CardanoEntity>) {
    for entity in entities.values_mut() {
        let Some(CardanoEntity::AccountState(row)) = entity else {
            continue;
        };

        if row.stake.mark().is_some() {
            continue;
        }

        let rewards = row.stake.live().map(|x| x.rewards_sum).unwrap_or_default();

        if rewards == 0 {
            continue;
        }

        debug!(
            account = %AccountKey::from(&row.credential),
            rewards, "stripped a pruned life the rollback restored"
        );

        let live = row.stake.unwrap_live_mut();
        live.rewards_sum = 0;
        live.withdrawals_sum = sub!(live.withdrawals_sum, rewards);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dolos_core::{Entity as _, EntityValue};
    use pallas::ledger::primitives::StakeCredential;

    use super::*;

    /// A row at epoch 2 holding 7 lovelace and the lifetime sums
    /// `(rewards, withdrawals)`, rotated in from epoch 1 when `rotated`.
    fn row(byte: u8, (rewards, withdrawals): (u64, u64), rotated: bool) -> AccountState {
        let credential = StakeCredential::AddrKeyhash([byte; 28].into());
        let mut row = AccountState::new(if rotated { 1 } else { 2 }, credential);

        let live = row.stake.unwrap_live_mut();
        live.utxo_sum = 7;
        live.rewards_sum = rewards;
        live.withdrawals_sum = withdrawals;

        if rotated {
            row.rotate(2);
        }

        row
    }

    fn key(row: &AccountState) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&row.credential))
    }

    fn encoded(entities: &EntityMap<CardanoEntity>) -> BTreeMap<NsKey, Option<EntityValue>> {
        entities
            .iter()
            .map(|(key, entity)| {
                let value = entity.as_ref().map(|x| CardanoEntity::encode_entity(x).1);
                (key.clone(), value)
            })
            .collect()
    }

    /// Restored lives beside every row the settle leaves: rotated rows with
    /// rewards, unrotated rows with none, deleted rows and other entities.
    /// Each round hashes a fresh map in another order, so a settle that stops
    /// early leaves a restored life in place.
    #[test]
    fn a_settle_strips_every_unrotated_row_and_nothing_else() {
        const PRUNED: (u64, u64) = (450_000, 450_000);
        const PAID: (u64, u64) = (150_000, 100_000);

        for _ in 0..8 {
            let mut entities = EntityMap::default();
            let mut expected = EntityMap::default();

            for byte in 0..8 {
                let restored = row(byte, PRUNED, false);
                let stripped = row(byte, (0, 0), false);
                expected.insert(key(&restored), Some(stripped.into()));
                entities.insert(key(&restored), Some(restored.into()));

                for kept in [
                    row(0x40 + byte, PAID, true),
                    row(0x80 + byte, (0, 0), false),
                ] {
                    expected.insert(key(&kept), Some(kept.clone().into()));
                    entities.insert(key(&kept), Some(kept.into()));
                }

                let deleted = key(&row(0xc0 + byte, (0, 0), false));
                expected.insert(deleted.clone(), None);
                entities.insert(deleted, None);

                let credential = row(0xe0 + byte, PRUNED, false).credential;
                let other = NsKey::of::<PrunedAccountState>(&AccountKey::from(&credential));
                let remainder = PrunedAccountState {
                    lifetime_rewards: PRUNED.0,
                    deregistered_at: Some(300),
                };
                expected.insert(other.clone(), Some(remainder.clone().into()));
                entities.insert(other, Some(remainder.into()));
            }

            settle_undo(&mut entities);

            assert_eq!(encoded(&entities), encoded(&expected));
        }
    }
}
