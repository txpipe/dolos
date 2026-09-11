//! Stake address log operations for the `archive-stake-log` keyspace of the
//! archive store.
//!
//! The log answers one query: the addresses seen under a stake credential,
//! ordered by first on-chain appearance. Three entry shapes share the
//! keyspace, discriminated by a tag byte:
//!
//! - Pair entry: `[0x00][stake_len:1][stake][address]` -> `[slot:8][order:4]`.
//!   One per known `(stake, address)` pair. This is the membership probe on the
//!   write path and the undo key on rollback.
//! - Ordered entry: `[0x01][stake_len:1][stake][slot:8][order:4][address]` ->
//!   empty. Lexicographic key order is chronological order, so a page read is a
//!   prefix scan windowed from either end.
//! - Ready marker: `[0xff]` -> `[1]`. Written once by genesis bootstrap. Reads
//!   answer `None` until it exists, because a store that was not synced from
//!   genesis with the log in place holds an incomplete log.
//!
//! Only the first appearance of a pair is stored. The write batch cannot read
//! its own pending inserts, so the writer threads a `seen` set through
//! [`apply`] to dedup pairs inside one batch; the pair entry dedups across
//! batches.
//!
//! The keyspace is not swept by `prune_history`: its entries are first
//! appearances, so removing one below the cutoff would drop an address the
//! account may still use. A rollback removes entries through [`undo`].

use std::collections::HashSet;

use dolos_core::{BlockSlot, StakeAddressAppearance};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::Error;

/// Tag byte for pair (membership) entries.
const PAIR_TAG: u8 = 0x00;

/// Tag byte for ordered (page-read) entries.
const ORDERED_TAG: u8 = 0x01;

/// Key of the ready marker: a tag byte no pair or ordered entry can start
/// with, so it never lands inside a prefix scan.
const READY_KEY: &[u8] = &[0xff];

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

/// Insert the first appearance of each pair in one block.
///
/// `seen` dedups pairs inside the current write batch: the batch cannot
/// read its own pending inserts, and one batch spans many blocks.
pub fn apply<R: Readable>(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    readable: &R,
    seen: &mut HashSet<Vec<u8>>,
    slot: BlockSlot,
    appearances: &[StakeAddressAppearance],
) -> Result<(), Error> {
    for appearance in appearances {
        let pair_key = build_pair_key(&appearance.stake, &appearance.address);

        if seen.contains(&pair_key) {
            continue;
        }

        if readable.get(keyspace, &pair_key)?.is_some() {
            seen.insert(pair_key);
            continue;
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

        seen.insert(pair_key);
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
    slot: BlockSlot,
    appearances: &[StakeAddressAppearance],
) -> Result<(), Error> {
    for appearance in appearances {
        let pair_key = build_pair_key(&appearance.stake, &appearance.address);

        let Some(value) = readable.get(keyspace, &pair_key)? else {
            continue;
        };

        let Some((first_slot, order)) = decode_sort_key(&value) else {
            continue;
        };

        if first_slot != slot {
            continue;
        }

        batch.remove(keyspace, pair_key);
        batch.remove(
            keyspace,
            build_ordered_key(&appearance.stake, slot, order, &appearance.address),
        );
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

/// Whether genesis has declared the log complete.
pub fn is_ready<R: Readable>(readable: &R, keyspace: &Keyspace) -> Result<bool, Error> {
    Ok(readable.get(keyspace, READY_KEY)?.is_some())
}

/// Write the ready marker.
pub fn mark_ready(batch: &mut OwnedWriteBatch, keyspace: &Keyspace) {
    batch.insert(keyspace, READY_KEY, [1u8]);
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
        assert_ne!(pair.as_slice(), READY_KEY);
    }

    #[test]
    fn sort_key_round_trips() {
        let encoded = encode_sort_key(141_868_807, 0x0003_0002);
        assert_eq!(decode_sort_key(&encoded), Some((141_868_807, 0x0003_0002)));
        assert_eq!(decode_sort_key(&encoded[..11]), None);
    }
}
