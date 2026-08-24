//! The closed set of state namespaces a Dolos stele carries.
//!
//! ADR-004 defines state as one uniform key-value space: the thirteen entity
//! namespaces of `dolos_cardano::model::build_schema()` plus [`UTXOS`], which
//! is the UTxO set wearing the same shape as everything else. That uniformity
//! is deliberate — it means the format has one state record, and the planned
//! refactor folding UTxOs into the entity system (#1042) is invisible to it.
//!
//! The names are not spelled here. Thirteen of them are read off the entity
//! types' own `FixedNamespace::NS`, and `utxos` is defined here because nothing
//! else in the tree defines it. A namespace that exists in `build_schema` and
//! not in [`NAMESPACES`] would be silently dropped from every published stele,
//! so `namespace_registry_matches_build_schema` in `tests/coverage.rs` makes
//! that a build failure instead.

use dolos_cardano::model::{
    AccountEpochLog, AccountState, AssetState, DRepState, DatumState, EpochState, EraSummary,
    FixedNamespace, GovState, PendingMirState, PendingRewardState, PoolState, ProposalState,
    StakeLog,
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
pub const NAMESPACES: [Namespace; 14] = [
    AccountEpochLog::NS,
    AccountState::NS,
    AssetState::NS,
    DatumState::NS,
    DRepState::NS,
    EpochState::NS,
    EraSummary::NS,
    GovState::NS,
    PendingMirState::NS,
    PendingRewardState::NS,
    PoolState::NS,
    ProposalState::NS,
    StakeLog::NS,
    UTXOS,
];

/// The schema revision of each namespace's record content, as
/// `parameters.schemas` reports it.
///
/// The revision moves when a namespace's stored record shape changes in a way
/// its `state-{ns}` / `log-{ns}` layers carry — the signal the compatibility
/// machinery of decision 0026 keys adoption on, and the thing that turns a
/// cross-publisher digest divergence into a diagnosable "you are behind".
/// Every value here is pinned by a canary in
/// `crates/snapshot/tests/field_registry.rs`, which fails the build when a
/// record's field table moves without its revision, or the other way round.
///
/// `epochs` is at 2: `RollingStats::registered_pools` was a `HashSet`, whose
/// per-instance iteration order made the namespace's bytes irreproducible
/// across publishers of identical state. Kept beside [`NAMESPACES`], in the
/// same order, and held to it by `every_namespace_has_a_schema_rev` below.
pub const SCHEMA_REVS: [(Namespace, u64); 14] = [
    (AccountEpochLog::NS, 1),
    (AccountState::NS, 1),
    (AssetState::NS, 1),
    (DatumState::NS, 1),
    (DRepState::NS, 2),
    (EpochState::NS, 2),
    (EraSummary::NS, 1),
    (GovState::NS, 1),
    (PendingMirState::NS, 1),
    (PendingRewardState::NS, 1),
    (PoolState::NS, 1),
    (ProposalState::NS, 1),
    (StakeLog::NS, 1),
    (UTXOS, 1),
];

/// The revision a retired namespace's `parameters.schemas` entry carries.
///
/// Zero is not a schema revision — no record has ever been at revision 0 — so
/// it reads as "this profile version defines no records for this namespace",
/// which is a statement a stele can make and an absent entry cannot.
pub const RETIRED_SCHEMA_REV: u64 = 0;

/// The namespaces this profile version once defined and no longer does.
///
/// They stay in `parameters.schemas` at [`RETIRED_SCHEMA_REV`] rather than
/// being dropped from it, because **removing a kind is not made safe by
/// `required: true`** (ADR-0027). The compatibility rule decision 0026 sets
/// covers a kind a reader does not *know*: the layer is there, the reader can
/// see it, and declining it is a plan-time choice it reports. A retired kind
/// is the opposite shape — its layers are simply not there, and absence
/// already means something else entirely: `write_logs` omits an empty log
/// layer by rule, and [`crate::restore`]'s epoch grouping passes over a kind it
/// does not recognise. A reader that models `member-rewards`, finds no such
/// layer and restores happily has no mechanism to notice that it has just
/// built a node with no reward history in it.
///
/// So the retirement is declared instead of inferred, and
/// [`crate::restore::plan`] fails closed on it. The declaration only binds
/// readers from the version that reads it forward; what protects the ones
/// already deployed is that all four of the namespaces retired here are also
/// *state* namespaces, and the state tip's completeness check refuses a stele
/// missing any kind it expects. A log-only namespace would have had no such
/// backstop, which is the case this rule exists for.
///
/// One-way, like `required`: a namespace named here constrains every reader
/// older than the retirement, forever. Naming one is an ADR-level act.
pub const RETIRED_NAMESPACES: [Namespace; 4] = [
    "account-stakes",
    "leader-rewards",
    "member-rewards",
    "pool-deposit-refunds",
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
    fn every_namespace_has_a_schema_rev() {
        assert_eq!(
            SCHEMA_REVS.map(|(ns, _)| ns),
            NAMESPACES,
            "SCHEMA_REVS and NAMESPACES disagree about the namespaces"
        );
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
