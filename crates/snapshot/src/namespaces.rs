//! The closed set of namespaces a Dolos stele carries.
//!
//! ADR-004 defines state as one uniform key-value space: the entity
//! namespaces of `dolos_cardano::model::build_schema()` plus [`UTXOS`], which
//! is the UTxO set wearing the same shape as everything else. That uniformity
//! is deliberate — it means the format has one state record, and the planned
//! refactor folding UTxOs into the entity system (#1042) is invisible to it.
//! The archive-only logs of `build_log_schema()` are namespaces too, carried
//! by log layers alone.
//!
//! The names are not spelled here. The entity namespaces are read off the
//! entity types' own `FixedNamespace::NS`, and `utxos` is defined here because
//! nothing else in the tree defines it. A namespace that exists in either
//! schema and not in [`NAMESPACES`] would be silently dropped from every
//! published stele, so `namespace_registry_matches_the_schemas` in
//! `tests/coverage.rs` makes that a build failure instead.

use dolos_cardano::model::{
    AccountAddressChunk, AccountAddressSet, AccountEpochLog, AccountState, AccountStats,
    AddressStats, AssetMintChunk, AssetMintSet, AssetState, DRepEpochLog, DRepSightingState,
    DRepState, DatumState, EpochState, EraState, FirstMintChunk, FirstMintSet, FirstScriptChunk,
    FirstScriptSet, FixedNamespace, GovState, MetadataLabelState, PaymentAddressChunk,
    PaymentAddressSet, PaymentStats, PendingMirState, PendingRewardState, PolicyMintChunk,
    PoolEpochLog, PoolState, ProposalState,
};
use dolos_core::{KeyCodec, KeyError, LogEntity, Namespace, StateEntity};

use crate::Error;

/// The UTxO set as a state namespace.
///
/// The one namespace with no entity type behind it, so the one string this
/// crate owns outright. Its key is `tx_hash(32) ‖ output_index(4, BE)` and its
/// value is CBOR `[era, body, slot, tx_index]`, or `[era, body]` for a UTxO no
/// block produced — see [`crate::layers::state`].
pub const UTXOS: Namespace = "utxos";

/// Every namespace, state or log, in ascending byte order.
///
/// Sorted because a state layer's records are ordered by `(ns, key)` and the
/// coverage test compares this list against the schemas directly;
/// `namespaces_are_sorted` keeps it that way.
pub const NAMESPACES: [Namespace; 31] = [
    AccountAddressChunk::NS,
    AccountAddressSet::NS,
    AccountEpochLog::NS,
    AccountStats::NS,
    AccountState::NS,
    AddressStats::NS,
    AssetMintChunk::NS,
    AssetMintSet::NS,
    AssetState::NS,
    DatumState::NS,
    DRepEpochLog::NS,
    DRepSightingState::NS,
    DRepState::NS,
    EpochState::NS,
    EraState::NS,
    FirstMintChunk::NS,
    FirstMintSet::NS,
    FirstScriptChunk::NS,
    FirstScriptSet::NS,
    GovState::NS,
    MetadataLabelState::NS,
    PaymentAddressChunk::NS,
    PaymentAddressSet::NS,
    PaymentStats::NS,
    PendingMirState::NS,
    PendingRewardState::NS,
    PolicyMintChunk::NS,
    PoolEpochLog::NS,
    PoolState::NS,
    ProposalState::NS,
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
/// `epochs` reached 2 when `RollingStats::registered_pools` stopped being a
/// `HashSet`, whose per-instance iteration order made the namespace's bytes
/// irreproducible across publishers of identical state. The keys of `dreps`,
/// `epochs`, `eras`, `first-mint-chunks`, `first-mints`, `gov`,
/// `metadata-labels`, `pool-epochs`, `pools` and `proposals` stopped being
/// padded or truncated to a fixed width (decision 0051), and each of those
/// moved one revision for it. `epochs` (4) and `pools` (3) then gained the
/// active-stake distribution ESTART freezes: `EpochState.active_stake` and
/// `estart_tally`, and the frozen tally on each `PoolSnapshot`. Kept beside
/// [`NAMESPACES`], in the same order, and held to it by
/// `every_namespace_has_a_schema_rev` below.
pub const SCHEMA_REVS: [(Namespace, u64); 31] = [
    (AccountAddressChunk::NS, 1),
    (AccountAddressSet::NS, 1),
    (AccountEpochLog::NS, 1),
    (AccountStats::NS, 1),
    (AccountState::NS, 1),
    (AddressStats::NS, 1),
    (AssetMintChunk::NS, 1),
    (AssetMintSet::NS, 1),
    (AssetState::NS, 1),
    (DatumState::NS, 1),
    (DRepEpochLog::NS, 1),
    (DRepSightingState::NS, 1),
    (DRepState::NS, 2),
    (EpochState::NS, 4),
    (EraState::NS, 2),
    (FirstMintChunk::NS, 2),
    (FirstMintSet::NS, 2),
    (FirstScriptChunk::NS, 1),
    (FirstScriptSet::NS, 1),
    (GovState::NS, 4),
    (MetadataLabelState::NS, 2),
    (PaymentAddressChunk::NS, 1),
    (PaymentAddressSet::NS, 1),
    (PaymentStats::NS, 1),
    (PendingMirState::NS, 1),
    (PendingRewardState::NS, 1),
    (PolicyMintChunk::NS, 1),
    (PoolEpochLog::NS, 2),
    (PoolState::NS, 3),
    (ProposalState::NS, 4),
    (UTXOS, 1),
];

/// Width of a UTxO key: `tx_hash(32) ‖ output_index(4, BE)`.
pub const UTXO_KEY_LEN: usize = 36;

fn exact<K: KeyCodec>(key: &[u8]) -> Result<(), KeyError> {
    K::decode_key(key).map(|_| ())
}

/// Refuse a state key that is not one `ns` stores: the exact encoding of the
/// key kind its entity declares, or a UTxO ref under [`UTXOS`].
///
/// A log namespace whose entity declares no state key holds no state rows, so
/// every key under it is refused.
pub fn check_state_key(ns: Namespace, key: &[u8]) -> Result<(), Error> {
    let checked = match ns {
        AccountAddressSet::NS => exact::<<AccountAddressSet as StateEntity>::Key>(key),
        AccountState::NS => exact::<<AccountState as StateEntity>::Key>(key),
        AccountStats::NS => exact::<<AccountStats as StateEntity>::Key>(key),
        AddressStats::NS => exact::<<AddressStats as StateEntity>::Key>(key),
        AssetMintSet::NS => exact::<<AssetMintSet as StateEntity>::Key>(key),
        AssetState::NS => exact::<<AssetState as StateEntity>::Key>(key),
        DatumState::NS => exact::<<DatumState as StateEntity>::Key>(key),
        DRepSightingState::NS => exact::<<DRepSightingState as StateEntity>::Key>(key),
        DRepState::NS => exact::<<DRepState as StateEntity>::Key>(key),
        EpochState::NS => exact::<<EpochState as StateEntity>::Key>(key),
        EraState::NS => exact::<<EraState as StateEntity>::Key>(key),
        GovState::NS => exact::<<GovState as StateEntity>::Key>(key),
        MetadataLabelState::NS => exact::<<MetadataLabelState as StateEntity>::Key>(key),
        PendingMirState::NS => exact::<<PendingMirState as StateEntity>::Key>(key),
        PendingRewardState::NS => exact::<<PendingRewardState as StateEntity>::Key>(key),
        PoolState::NS => exact::<<PoolState as StateEntity>::Key>(key),
        ProposalState::NS => exact::<<ProposalState as StateEntity>::Key>(key),
        FirstMintSet::NS => exact::<<FirstMintSet as StateEntity>::Key>(key),
        FirstScriptSet::NS => exact::<<FirstScriptSet as StateEntity>::Key>(key),
        PaymentAddressSet::NS => exact::<<PaymentAddressSet as StateEntity>::Key>(key),
        PaymentStats::NS => exact::<<PaymentStats as StateEntity>::Key>(key),
        UTXOS => exact::<[u8; UTXO_KEY_LEN]>(key),
        AccountAddressChunk::NS
        | AccountEpochLog::NS
        | AssetMintChunk::NS
        | DRepEpochLog::NS
        | FirstMintChunk::NS
        | FirstScriptChunk::NS
        | PaymentAddressChunk::NS
        | PolicyMintChunk::NS
        | PoolEpochLog::NS => {
            return Err(Error::malformed("state", format!("{ns} holds no state")));
        }
        _ => return Err(Error::UnknownNamespace(ns.to_owned())),
    };

    checked.map_err(|err| Error::malformed("state", format!("{ns} key: {err}")))
}

/// Refuse a log key whose part after the slot is not the exact encoding of
/// the key kind `ns`'s log declares.
pub fn check_log_key(ns: Namespace, key: &[u8]) -> Result<(), Error> {
    let checked = match ns {
        AccountAddressChunk::NS => exact::<<AccountAddressChunk as LogEntity>::Key>(key),
        AccountEpochLog::NS => exact::<<AccountEpochLog as LogEntity>::Key>(key),
        AccountStats::NS => exact::<<AccountStats as LogEntity>::Key>(key),
        AddressStats::NS => exact::<<AddressStats as LogEntity>::Key>(key),
        AssetMintChunk::NS => exact::<<AssetMintChunk as LogEntity>::Key>(key),
        DRepEpochLog::NS => exact::<<DRepEpochLog as LogEntity>::Key>(key),
        EpochState::NS => exact::<<EpochState as LogEntity>::Key>(key),
        FirstMintChunk::NS => exact::<<FirstMintChunk as LogEntity>::Key>(key),
        FirstScriptChunk::NS => exact::<<FirstScriptChunk as LogEntity>::Key>(key),
        PaymentAddressChunk::NS => exact::<<PaymentAddressChunk as LogEntity>::Key>(key),
        PolicyMintChunk::NS => exact::<<PolicyMintChunk as LogEntity>::Key>(key),
        PoolEpochLog::NS => exact::<<PoolEpochLog as LogEntity>::Key>(key),
        PoolState::NS => exact::<<PoolState as LogEntity>::Key>(key),
        ProposalState::NS => exact::<<ProposalState as LogEntity>::Key>(key),
        _ => return Err(Error::UnknownNamespace(ns.to_owned())),
    };

    checked.map_err(|err| Error::malformed("log", format!("{ns} key: {err}")))
}

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
/// already deployed is that every namespace retired here is also a *state*
/// namespace, and the state tip's completeness check refuses a stele
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
