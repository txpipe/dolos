//! Stake address log operations for the `archive-stake-log` keyspace of the
//! archive store.
//!
//! The log answers one query: the addresses seen under a stake credential,
//! ordered by first on-chain appearance. Two entry shapes share the
//! keyspace, discriminated by a tag byte:
//!
//! - Pair entry: `[0x00][stake_len:1][stake][address]` -> `[slot:8][order:4]`.
//!   One per known `(stake, address)` pair. This is the membership probe on the
//!   write path and the undo key on rollback.
//! - Ordered entry: `[0x01][stake_len:1][stake][slot:8][order:4][address]` ->
//!   empty. Lexicographic key order is chronological order, so a page read is a
//!   prefix scan windowed from either end.
//!
//! Only the first appearance of a pair is stored. The write batch cannot read
//! its own writes, so the writer threads a [`PendingPairs`] map through
//! [`apply`] and [`undo`]: what one writer inserted or removed is what its
//! later calls see, and pairs a batch repeats are deduped there. When a replay
//! reaches an appearance earlier than the stored one, [`apply`] moves the pair
//! to it.
//!
//! The keyspace is not swept by `prune_history`: its entries are first
//! appearances, so removing one below the cutoff would drop an address the
//! account may still use. A rollback removes entries through [`undo`].

use std::collections::HashMap;

use dolos_core::{BlockSlot, StakeAddressAppearance};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::Error;

/// Tag byte for pair (membership) entries.
const PAIR_TAG: u8 = 0x00;

/// Tag byte for ordered (page-read) entries.
const ORDERED_TAG: u8 = 0x01;

/// Width of the `[slot:8][order:4]` sort key.
const SORT_KEY_SIZE: usize = 12;

fn build_pair_key(stake: &[u8], address: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + stake.len() + address.len());
    key.push(PAIR_TAG);
    key.push(stake.len() as u8);
    key.extend_from_slice(stake);
    key.extend_from_slice(address);
    key
}

fn build_ordered_key(stake: &[u8], slot: BlockSlot, order: u32, address: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + stake.len() + SORT_KEY_SIZE + address.len());
    key.push(ORDERED_TAG);
    key.push(stake.len() as u8);
    key.extend_from_slice(stake);
    key.extend_from_slice(&slot.to_be_bytes());
    key.extend_from_slice(&order.to_be_bytes());
    key.extend_from_slice(address);
    key
}

/// Prefix covering every ordered entry of one stake credential.
fn build_ordered_prefix(stake: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + stake.len());
    prefix.push(ORDERED_TAG);
    prefix.push(stake.len() as u8);
    prefix.extend_from_slice(stake);
    prefix
}

fn encode_sort_key(slot: BlockSlot, order: u32) -> [u8; SORT_KEY_SIZE] {
    let mut value = [0u8; SORT_KEY_SIZE];
    value[..8].copy_from_slice(&slot.to_be_bytes());
    value[8..].copy_from_slice(&order.to_be_bytes());
    value
}

fn decode_sort_key(value: &[u8]) -> Option<(BlockSlot, u32)> {
    if value.len() != SORT_KEY_SIZE {
        return None;
    }

    let slot = BlockSlot::from_be_bytes(value[..8].try_into().ok()?);
    let order = u32::from_be_bytes(value[8..].try_into().ok()?);
    Some((slot, order))
}

/// The pairs one writer has touched, with the position each holds once the
/// pending batch commits: `Some` after an insert, `None` after a removal.
///
/// The batch cannot read its own writes, and one writer spans many blocks,
/// so [`apply`] and [`undo`] consult this before the committed store. It also
/// dedups the pairs a batch repeats.
pub type PendingPairs = HashMap<Vec<u8>, Option<(BlockSlot, u32)>>;

/// The position a pair holds as this writer sees it: the pending batch first,
/// the committed store second.
fn position_of<R: Readable>(
    keyspace: &Keyspace,
    readable: &R,
    pending: &PendingPairs,
    pair_key: &[u8],
) -> Result<Option<(BlockSlot, u32)>, Error> {
    if let Some(position) = pending.get(pair_key) {
        return Ok(*position);
    }

    Ok(readable
        .get(keyspace, pair_key)?
        .and_then(|value| decode_sort_key(&value)))
}

/// Insert the first appearance of each pair in one block.
///
/// A stored pair keeps its position unless this appearance is earlier. A log
/// that started mid-chain holds the first appearance it saw, not the first on
/// chain; a replay from origin (`doctor rebuild-state --rewrite-logs`) reaches
/// the earlier one and moves the pair to it.
pub fn apply<R: Readable>(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    readable: &R,
    pending: &mut PendingPairs,
    slot: BlockSlot,
    appearances: &[StakeAddressAppearance],
) -> Result<(), Error> {
    for appearance in appearances {
        let pair_key = build_pair_key(&appearance.stake, &appearance.address);
        let position = (slot, appearance.order);

        match position_of(keyspace, readable, pending, &pair_key)? {
            Some(stored) if position < stored => {
                batch.remove(
                    keyspace,
                    build_ordered_key(&appearance.stake, stored.0, stored.1, &appearance.address),
                );
            }
            Some(stored) => {
                pending.insert(pair_key, Some(stored));
                continue;
            }
            None => {}
        }

        batch.insert(
            keyspace,
            pair_key.clone(),
            encode_sort_key(slot, appearance.order),
        );
        batch.insert(
            keyspace,
            build_ordered_key(
                &appearance.stake,
                slot,
                appearance.order,
                &appearance.address,
            ),
            [],
        );

        pending.insert(pair_key, Some(position));
    }

    Ok(())
}

/// Remove the pairs whose stored first appearance is the undone block.
///
/// A pair first seen in an earlier block stays untouched: the undone block
/// merely repeated an address the account already had.
pub fn undo<R: Readable>(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    readable: &R,
    pending: &mut PendingPairs,
    slot: BlockSlot,
    appearances: &[StakeAddressAppearance],
) -> Result<(), Error> {
    for appearance in appearances {
        let pair_key = build_pair_key(&appearance.stake, &appearance.address);

        let Some((first_slot, order)) = position_of(keyspace, readable, pending, &pair_key)? else {
            continue;
        };

        if first_slot != slot {
            continue;
        }

        batch.remove(keyspace, pair_key.clone());
        batch.remove(
            keyspace,
            build_ordered_key(&appearance.stake, first_slot, order, &appearance.address),
        );

        pending.insert(pair_key, None);
    }

    Ok(())
}

/// Read one page of addresses for a stake credential, ordered by first
/// appearance (or its exact reverse).
pub fn page<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    stake: &[u8],
    offset: usize,
    limit: usize,
    reverse: bool,
) -> Result<Vec<Vec<u8>>, Error> {
    let prefix = build_ordered_prefix(stake);
    let header = prefix.len() + SORT_KEY_SIZE;

    let iter = readable.prefix(keyspace, prefix);

    let mut page = Vec::new();

    let mut push = |guard: fjall::Guard| -> Result<(), Error> {
        let key = guard.key()?;

        if key.len() > header {
            page.push(key[header..].to_vec());
        }

        Ok(())
    };

    if reverse {
        for guard in iter.rev().skip(offset).take(limit) {
            push(guard)?;
        }
    } else {
        for guard in iter.skip(offset).take(limit) {
            push(guard)?;
        }
    }

    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_keys_sort_by_slot_then_order() {
        let stake = [0xaa; 29];
        let a = build_ordered_key(&stake, 1, 7, &[1; 57]);
        let b = build_ordered_key(&stake, 2, 0, &[0; 57]);
        let c = build_ordered_key(&stake, 2, 1, &[0; 57]);

        assert!(a < b, "slot dominates order");
        assert!(b < c, "order breaks ties within a slot");
    }

    #[test]
    fn ordered_keys_stay_under_their_stake_prefix() {
        let stake = [0xaa; 29];
        let other = [0xab; 29];
        let key = build_ordered_key(&stake, 5, 0, &[1; 57]);

        assert!(key.starts_with(&build_ordered_prefix(&stake)));
        assert!(!key.starts_with(&build_ordered_prefix(&other)));
    }

    #[test]
    fn pair_and_ordered_entries_never_share_a_prefix() {
        let stake = [0xaa; 29];
        let pair = build_pair_key(&stake, &[1; 57]);

        assert!(!pair.starts_with(&build_ordered_prefix(&stake)));
    }

    #[test]
    fn sort_key_round_trips() {
        let encoded = encode_sort_key(141_868_807, 0x0003_0002);
        assert_eq!(decode_sort_key(&encoded), Some((141_868_807, 0x0003_0002)));
        assert_eq!(decode_sort_key(&encoded[..11]), None);
    }
}
