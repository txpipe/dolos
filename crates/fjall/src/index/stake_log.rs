//! Stake address log operations for the `stake-log` keyspace.
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
//! Only the first appearance of a pair is stored. The write batch cannot
//! read its own pending inserts, so the caller threads a `seen` set through
//! `apply` to dedup pairs inside one batch; the pair entry dedups across
//! batches.

use std::collections::HashSet;

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

/// Insert the first appearance of each pair the delta carries.
///
/// `seen` dedups pairs inside the current write batch: the batch cannot
/// read its own pending inserts, and one batch can span many blocks (WAL
/// catch-up applies a whole range through a single writer).
pub fn apply<R: Readable>(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    readable: &R,
    seen: &mut HashSet<Vec<u8>>,
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
            encode_sort_key(appearance.slot, appearance.order),
        );

        batch.insert(
            keyspace,
            build_ordered_key(
                &appearance.stake,
                appearance.slot,
                appearance.order,
                &appearance.address,
            ),
            [],
        );

        seen.insert(pair_key);
    }

    Ok(())
}

/// Remove pairs whose stored first appearance is the undone block.
///
/// A pair first seen in an earlier block stays untouched: the undone block
/// merely repeated an address the account already had.
pub fn undo<R: Readable>(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    readable: &R,
    appearances: &[StakeAddressAppearance],
) -> Result<(), Error> {
    for appearance in appearances {
        let pair_key = build_pair_key(&appearance.stake, &appearance.address);

        let Some(value) = readable.get(keyspace, &pair_key)? else {
            continue;
        };

        let Some((slot, order)) = decode_sort_key(&value) else {
            continue;
        };

        if slot != appearance.slot {
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

    if reverse {
        for guard in iter.rev().skip(offset).take(limit) {
            let key = guard.key()?;

            if key.len() > header {
                page.push(key[header..].to_vec());
            }
        }
    } else {
        for guard in iter.skip(offset).take(limit) {
            let key = guard.key()?;

            if key.len() > header {
                page.push(key[header..].to_vec());
            }
        }
    }

    Ok(page)
}
