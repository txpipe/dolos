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
//!   ([`restore_for_undo`]), and strips the restored life's lifetime rewards
//!   and withdrawals from it, which the remainder keeps ([`settle_undo`]).
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
use pallas::ledger::primitives::Epoch;
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

    debug!(?account, "restored a pruned account for the rollback");

    Ok(Some(
        AccountState::new(current_epoch(state)?, account.0).into(),
    ))
}

/// The epoch in `state`, which no rollback changes.
fn current_epoch<S: StateStore>(state: &S) -> Result<Epoch, StateError> {
    Ok(state
        .read_entity_typed::<EpochState>(&())?
        .ok_or_else(|| InvariantViolation::EntityNotFound(NsKey::of::<EpochState>(&())))?
        .number)
}

/// Once a rollback has undone every delta, strip the lifetime rewards from
/// each account row at the current epoch with no `mark` snapshot. The
/// remainder already counts them.
///
/// Rewards are paid only at EWRAP, and ESTART then rotates every row it keeps
/// to the opening epoch, which gives the row a `mark`. A row at the current
/// epoch without one was created since the boundary, by a block or by
/// [`restore_for_undo`], and earned nothing. Any rewards the undos give it are
/// from the life ESTART pruned:
/// - the undo restored the pruned row, and set its value from before the
///   boundary;
/// - an earlier rollback restored and stripped the row, committed the state and
///   died before the WAL was truncated. Catch-up replayed the logged deltas
///   onto the stripped row, and their undos set its value from before the
///   boundary again.
///
/// Every other row keeps what the undos leave, as on `main`. A block's undo
/// sets only the live stake value, so each row keeps the stake value's
/// snapshots and epoch the store held:
/// - A rotated row was in state through the boundary, so ESTART did not prune
///   it. That holds even when a crashed rollback's catch-up replayed it without
///   the boundary's payment and the undos give that payment back.
/// - A row that a closing-epoch block created, that a crashed rollback deleted
///   and that catch-up created again is at that block's epoch. The credential
///   had no row before the block, and the boundary paid the row only while it
///   was registered, when it is never dead, so the undos give it back at most
///   that payment.
///
/// Both sums lose the same amount, so the balance the ledger reads stays.
///
/// The epoch is read only once a row with no `mark` and with rewards is found.
/// A fresh node's first rollback, to the origin, comes before genesis has
/// written any state, and has nothing to strip.
pub fn settle_undo<S: StateStore>(
    state: &S,
    entities: &mut EntityMap<CardanoEntity>,
) -> Result<(), StateError> {
    strip_restored_lives(|| current_epoch(state), entities)
}

fn strip_restored_lives(
    epoch: impl FnOnce() -> Result<Epoch, StateError>,
    entities: &mut EntityMap<CardanoEntity>,
) -> Result<(), StateError> {
    let mut unrotated = entities
        .values_mut()
        .filter_map(|entity| match entity {
            Some(CardanoEntity::AccountState(row)) => Some(row),
            _ => None,
        })
        .filter(|row| row.stake.mark().is_none() && rewards(row) > 0)
        .peekable();

    if unrotated.peek().is_none() {
        return Ok(());
    }

    let epoch = epoch()?;

    for row in unrotated.filter(|row| row.stake.is_at_epoch(epoch)) {
        let rewards = rewards(row);

        debug!(
            account = %AccountKey::from(&row.credential),
            rewards, "stripped a pruned life the undos gave back"
        );

        let live = row.stake.unwrap_live_mut();
        live.rewards_sum = 0;
        live.withdrawals_sum = sub!(live.withdrawals_sum, rewards);
    }

    Ok(())
}

fn rewards(row: &AccountState) -> u64 {
    row.stake.live().map(|x| x.rewards_sum).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dolos_core::{Entity as _, EntityValue};
    use pallas::ledger::primitives::StakeCredential;

    use super::*;

    /// The epoch the settle runs at.
    const EPOCH: Epoch = 2;

    enum Kind {
        /// Created at [`EPOCH`], as a restore does.
        Restored,
        /// Created at the epoch before and rotated in.
        Rotated,
        /// Created at the epoch before and not rotated, as catch-up recreates
        /// a closing-epoch block's row.
        Recreated,
    }

    /// A row holding `utxo` lovelace and the lifetime sums
    /// `(rewards, withdrawals)`.
    fn row(byte: u8, utxo: u64, (rewards, withdrawals): (u64, u64), kind: Kind) -> AccountState {
        let credential = StakeCredential::AddrKeyhash([byte; 28].into());
        let created = match kind {
            Kind::Restored => EPOCH,
            Kind::Rotated | Kind::Recreated => EPOCH - 1,
        };
        let mut row = AccountState::new(created, credential);

        let live = row.stake.unwrap_live_mut();
        live.utxo_sum = utxo;
        live.rewards_sum = rewards;
        live.withdrawals_sum = withdrawals;

        if let Kind::Rotated = kind {
            row.rotate(EPOCH);
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
    /// rewards, one with every reward withdrawn and no value (an empty
    /// `mark`), recreated rows with rewards, restored rows with none, deleted
    /// rows and other entities. Each round hashes a fresh map in another
    /// order, so a settle that stops early leaves a restored life in place.
    #[test]
    fn a_settle_strips_every_restored_row_and_nothing_else() {
        const PRUNED: (u64, u64) = (450_000, 450_000);
        const PAID: (u64, u64) = (150_000, 100_000);

        for _ in 0..8 {
            let mut entities = EntityMap::default();
            let mut expected = EntityMap::default();

            for byte in 0..8 {
                let restored = row(byte, 7, PRUNED, Kind::Restored);
                let stripped = row(byte, 7, (0, 0), Kind::Restored);
                expected.insert(key(&restored), Some(stripped.into()));
                entities.insert(key(&restored), Some(restored.into()));

                for kept in [
                    row(0x40 + byte, 7, PAID, Kind::Rotated),
                    row(0x50 + byte, 0, (PAID.0, PAID.0), Kind::Rotated),
                    row(0x60 + byte, 7, PAID, Kind::Recreated),
                    row(0x80 + byte, 7, (0, 0), Kind::Restored),
                ] {
                    expected.insert(key(&kept), Some(kept.clone().into()));
                    entities.insert(key(&kept), Some(kept.into()));
                }

                let deleted = key(&row(0xc0 + byte, 7, (0, 0), Kind::Restored));
                expected.insert(deleted.clone(), None);
                entities.insert(deleted, None);

                let credential = row(0xe0 + byte, 7, PRUNED, Kind::Restored).credential;
                let other = NsKey::of::<PrunedAccountState>(&AccountKey::from(&credential));
                let remainder = PrunedAccountState {
                    lifetime_rewards: PRUNED.0,
                    deregistered_at: Some(300),
                };
                expected.insert(other.clone(), Some(remainder.clone().into()));
                entities.insert(other, Some(remainder.into()));
            }

            strip_restored_lives(|| Ok(EPOCH), &mut entities).unwrap();

            assert_eq!(encoded(&entities), encoded(&expected));
        }
    }

    /// The settle reads the epoch only for a row with lifetime rewards and no
    /// `mark`, so it needs none when the rollback undid nothing, as a fresh
    /// node's first does before genesis, or left only rows it keeps.
    #[test]
    fn a_settle_with_nothing_to_strip_reads_no_epoch() {
        let state = dolos_testing::toy_domain::empty_state_store();

        let mut entities = EntityMap::default();
        settle_undo(&state, &mut entities).unwrap();
        assert!(entities.is_empty());

        for kept in [
            row(0x40, 7, (150_000, 100_000), Kind::Rotated),
            row(0x50, 0, (150_000, 150_000), Kind::Rotated),
            row(0x80, 7, (0, 0), Kind::Restored),
            row(0xa0, 7, (0, 0), Kind::Recreated),
        ] {
            entities.insert(key(&kept), Some(kept.into()));
        }

        entities.insert(key(&row(0xc0, 7, (0, 0), Kind::Restored)), None);

        let expected = encoded(&entities);

        settle_undo(&state, &mut entities).unwrap();

        assert_eq!(encoded(&entities), expected);
    }
}
