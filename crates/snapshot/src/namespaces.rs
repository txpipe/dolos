//! The closed set of state namespaces a Dolos stele carries.
//!
//! ADR-004 defines state as one uniform key-value space: the fourteen entity
//! namespaces of `dolos_cardano::model::build_schema()` plus [`UTXOS`], which
//! is the UTxO set wearing the same shape as everything else. That uniformity
//! is deliberate — it means the format has one state record, and the planned
//! refactor folding UTxOs into the entity system (#1042) is invisible to it.
//!
//! The names are not spelled here. Fourteen of them are read off the entity
//! types' own `FixedNamespace::NS`, and `utxos` is defined here because nothing
//! else in the tree defines it. A namespace that exists in `build_schema` and
//! not in [`NAMESPACES`] would be silently dropped from every published stele,
//! so `namespace_coverage_matches_build_schema` in `tests/coverage.rs` makes
//! that a build failure instead.

use dolos_cardano::model::{
    AccountState, AssetState, DRepState, DatumState, EpochState, EraSummary, FixedNamespace,
    LeaderRewardLog, MemberRewardLog, PendingMirState, PendingRewardState, PoolDepositRefundLog,
    PoolState, ProposalState, StakeLog,
};
use dolos_core::Namespace;

use crate::Error;

/// The UTxO set as a state namespace.
///
/// The one namespace with no entity type behind it, so the one string this
/// crate owns outright. Its key is `tx_hash(32) ‖ output_index(4, BE)` and its
/// value is CBOR `[era, body]` — see [`crate::layers::state`].
pub const UTXOS: Namespace = "utxos";

/// Every state namespace, in ascending byte order.
///
/// Sorted because a state layer's records are ordered by `(ns, key)` and the
/// coverage test compares this list against `build_schema()` directly;
/// `namespaces_are_sorted` keeps it that way.
pub const NAMESPACES: [Namespace; 15] = [
    AccountState::NS,
    AssetState::NS,
    DatumState::NS,
    DRepState::NS,
    EpochState::NS,
    EraSummary::NS,
    LeaderRewardLog::NS,
    MemberRewardLog::NS,
    PendingMirState::NS,
    PendingRewardState::NS,
    PoolDepositRefundLog::NS,
    PoolState::NS,
    ProposalState::NS,
    StakeLog::NS,
    UTXOS,
];

/// Resolve a namespace read off the wire to the `&'static str` the stores use.
///
/// Fails closed. A namespace this profile does not define cannot be restored —
/// no store has a table for it — so decoding it into an owned string and hoping
/// a later stage notices would only move the failure somewhere less
/// informative.
pub fn resolve(name: &str) -> Result<Namespace, Error> {
    NAMESPACES
        .into_iter()
        .find(|known| *known == name)
        .ok_or_else(|| Error::UnknownNamespace(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_sorted_and_distinct() {
        let mut sorted = NAMESPACES;
        sorted.sort_unstable();

        assert_eq!(NAMESPACES, sorted, "NAMESPACES must be in byte order");

        let mut deduped = sorted.to_vec();
        deduped.dedup();
        assert_eq!(deduped.len(), NAMESPACES.len(), "duplicate namespace");
    }

    #[test]
    fn resolve_round_trips_every_namespace_and_refuses_the_rest() {
        for ns in NAMESPACES {
            assert_eq!(resolve(ns).unwrap(), ns);
        }

        for unknown in ["", "utxo", "Accounts", "blocks", "metadata"] {
            let err = resolve(unknown).unwrap_err();
            assert!(matches!(err, Error::UnknownNamespace(_)), "{unknown:?}");
        }
    }
}
