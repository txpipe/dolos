//! Cross-cutting helpers for delta round-trip property tests.
//!
//! Only the `assert_delta_roundtrip` helper and primitive/pallas-typed strategies live here.
//! Strategies for entity states and for types owned by a specific module live next to
//! those types — see `pub(crate) mod testing` in each `model/*.rs` (and in `pots.rs`,
//! `pallas_extras.rs`).

#![allow(dead_code)]

use dolos_core::EntityDelta;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::{
    conway::{Anchor, DRep, RationalNumber},
    Epoch, StakeCredential,
};
use proptest::prelude::*;
use serde::{de::DeserializeOwned, Serialize};

use super::pools::PoolHash;

/// Asserts that applying then undoing a delta restores the original entity state.
///
/// Requires `PartialEq` on the entity type — we added derives for this on every
/// entity/component specifically to get good shrink output from proptest.
pub fn assert_delta_roundtrip<T, D>(entity: Option<T>, mut delta: D)
where
    T: Clone + PartialEq + core::fmt::Debug,
    D: EntityDelta<Entity = T>,
{
    let original = entity.clone();
    let mut state = entity;
    delta.apply(&mut state);
    delta.undo(&mut state);
    assert_eq!(
        state, original,
        "delta apply/undo is not reversible: undo did not restore original state"
    );
}

/// Asserts that the WAL serialize/deserialize round-trip preserves apply→undo
/// reversibility.
///
/// The lifecycle in `core/sync.rs::run_lifecycle` writes `commit_wal` *before*
/// `commit_state`, then `apply_entities` mutates each delta in-place to capture
/// `prev_*` undo state — but those mutations only land on the in-memory copy.
/// The WAL row itself is encoded **after** apply runs (post-fix), so the
/// deserialized form must still admit a valid undo.
///
/// This helper exercises that contract: serialize the *post-apply* delta,
/// deserialize it, and assert that calling `undo` on the deserialized instance
/// restores the original entity. Catches regressions where a delta forgets to
/// mark a `prev_*` field as `serde`-serialized, or relies on transient state
/// that doesn't survive the wire format.
pub fn assert_delta_serde_roundtrip<T, D>(entity: Option<T>, mut delta: D)
where
    T: Clone + PartialEq + core::fmt::Debug,
    D: EntityDelta<Entity = T> + Serialize + DeserializeOwned,
{
    let original = entity.clone();
    let mut state = entity;

    delta.apply(&mut state);

    // Bincode is the WAL's encoding (see crates/redb3/src/wal/mod.rs).
    let bytes = bincode::serialize(&delta).expect("serialize delta");
    let restored: D = bincode::deserialize(&bytes).expect("deserialize delta");

    restored.undo(&mut state);

    assert_eq!(
        state, original,
        "delta serde-then-undo is not reversible: deserialized undo did not restore original state",
    );
}

// --- Primitive strategies ---

prop_compose! {
    pub fn any_hash_28()(bytes in any::<[u8; 28]>()) -> Hash<28> {
        Hash::new(bytes)
    }
}

prop_compose! {
    pub fn any_hash_32()(bytes in any::<[u8; 32]>()) -> Hash<32> {
        Hash::new(bytes)
    }
}

pub fn any_pool_hash() -> impl Strategy<Value = PoolHash> {
    any_hash_28()
}

/// Bounded epoch range so transitions and arithmetic stay well-defined. Kept ≥ 3 so
/// `go`/`set`/`mark` positions are always reachable.
pub fn any_epoch() -> impl Strategy<Value = Epoch> {
    3u64..1_000_000u64
}

pub fn any_slot() -> impl Strategy<Value = u64> {
    0u64..1_000_000_000u64
}

/// Bounded lovelace amount. Avoids near-u64::MAX so saturating arithmetic in delta
/// apply implementations doesn't mask asymmetry with undo.
pub fn any_lovelace() -> impl Strategy<Value = u64> {
    0u64..1_000_000_000_000u64
}

pub fn any_tx_order() -> impl Strategy<Value = usize> {
    0usize..10_000usize
}

pub fn any_stake_credential() -> impl Strategy<Value = StakeCredential> {
    prop_oneof![
        any_hash_28().prop_map(StakeCredential::AddrKeyhash),
        any_hash_28().prop_map(StakeCredential::ScriptHash),
    ]
}

pub fn any_drep() -> impl Strategy<Value = DRep> {
    prop_oneof![
        any_hash_28().prop_map(DRep::Key),
        any_hash_28().prop_map(DRep::Script),
        Just(DRep::Abstain),
        Just(DRep::NoConfidence),
    ]
}

prop_compose! {
    pub fn any_anchor()(
        url in "[a-z]{1,16}",
        hash in any_hash_32(),
    ) -> Anchor {
        Anchor { url, content_hash: hash }
    }
}

prop_compose! {
    pub fn any_rational()(num in 1u64..100u64, den in 1u64..100u64) -> RationalNumber {
        RationalNumber { numerator: num, denominator: den }
    }
}
