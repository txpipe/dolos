//! Done criterion 4: the vocabulary this profile ships is the vocabulary the
//! rest of the workspace has.
//!
//! Three closed sets feed the format — the archive dimensions, the exact-record
//! kinds and the state namespaces — and all three are defined elsewhere. Adding
//! to any of them is a routine change in `dolos-core` or `dolos-cardano` that
//! has nothing to do with snapshots, and the failure mode is silent: a new
//! dimension is simply never exported, or a new namespace is exported but never
//! frozen by a golden, and the first anyone hears of it is a restored node
//! missing data.
//!
//! So the sets are checked in both directions, and the *fixture* is checked
//! against them too: a golden digest only freezes what its input contains, and
//! an addition upstream has to break a test here rather than quietly widen what
//! a stele may hold.

mod common;

use std::collections::BTreeSet;

use common::*;
use dolos_cardano::{indexes::archive_dimensions, model::build_schema};
use dolos_core::{ExactKind, IndexRecord, Namespace};
use dolos_snapshot::{layers::indexes, namespaces, NAMESPACES, UTXOS};

/// The state namespaces this profile carries are exactly `build_schema`'s, plus
/// the UTxO set.
#[test]
fn namespace_registry_matches_build_schema() {
    let schema: BTreeSet<Namespace> = build_schema().keys().copied().collect();
    let registry: BTreeSet<Namespace> = NAMESPACES.into_iter().collect();

    assert!(
        registry.contains(UTXOS),
        "the utxo set is a state namespace"
    );

    let entities: BTreeSet<Namespace> =
        registry.iter().copied().filter(|ns| *ns != UTXOS).collect();

    assert_eq!(
        entities, schema,
        "the profile's state namespaces have drifted from build_schema()"
    );

    // ADR-004 says sixteen entity namespaces plus `utxos`. If that count moves,
    // the sentence in the ADR moves with it.
    assert_eq!(entities.len(), 16);
    assert_eq!(NAMESPACES.len(), 17);
}

#[test]
fn every_namespace_resolves_and_nothing_else_does() {
    for ns in NAMESPACES {
        assert_eq!(namespaces::resolve(ns).unwrap(), ns);
    }

    for absent in ["utxo", "accounts ", "UTXOS", "", "blocks", "wal"] {
        assert!(namespaces::resolve(absent).is_err(), "{absent:?}");
    }
}

#[test]
fn every_dimension_resolves_and_nothing_else_does() {
    for dimension in archive_dimensions::ALL {
        assert_eq!(indexes::resolve_dimension(dimension).unwrap(), dimension);
    }

    // The UTxO filter dimensions are live-state indexes, rebuilt at restore from
    // the UTxO set rather than shipped — so they are deliberately not archive
    // dimensions, and `spent_txo` is the archive one whose name is nearest.
    for absent in ["utxo::address", "spent-txo", "", "Address"] {
        assert!(indexes::resolve_dimension(absent).is_err(), "{absent:?}");
    }
}

#[test]
fn every_exact_kind_resolves_and_nothing_else_does() {
    for kind in ExactKind::ALL {
        assert_eq!(indexes::resolve_exact_kind(kind.as_str()).unwrap(), kind);
    }

    for absent in ["block_number", "tx", "", "BlockHash"] {
        assert!(indexes::resolve_exact_kind(absent).is_err(), "{absent:?}");
    }
}

/// Every archive dimension appears in the golden `indexes` layer, so every
/// dimension name is a string some published digest depends on.
#[test]
fn the_golden_indexes_layer_covers_every_dimension_and_kind() {
    let records = common::indexes();

    let dimensions: BTreeSet<String> = records
        .iter()
        .filter_map(|record| match record {
            IndexRecord::Tag(tag) => Some(tag.dimension().to_owned()),
            IndexRecord::Exact(_) => None,
        })
        .collect();

    let expected: BTreeSet<String> = archive_dimensions::ALL
        .into_iter()
        .map(str::to_owned)
        .collect();

    assert_eq!(
        dimensions, expected,
        "the golden indexes layer no longer freezes every dimension name"
    );

    // Twelve, as the module documentation of `tests/goldens.rs` states. Pinned
    // here so that prose cannot go stale while every other test still passes.
    assert_eq!(expected.len(), 12);

    let kinds: BTreeSet<ExactKind> = records
        .iter()
        .filter_map(|record| match record {
            IndexRecord::Exact(exact) => Some(exact.kind),
            IndexRecord::Tag(_) => None,
        })
        .collect();

    assert_eq!(
        kinds,
        ExactKind::ALL.into_iter().collect::<BTreeSet<_>>(),
        "the golden indexes layer no longer freezes every exact-record kind"
    );
}

/// Every state namespace appears in the golden `state` layers.
#[test]
fn the_golden_state_layers_cover_every_namespace() {
    let expected: BTreeSet<Namespace> = NAMESPACES.into_iter().collect();

    for shard in SHARDS {
        let present: BTreeSet<Namespace> = common::state(shard)
            .iter()
            .map(|record| record.ns)
            .collect();

        assert_eq!(
            present, expected,
            "shard {shard}: the golden state layer no longer freezes every namespace"
        );
    }
}

/// The `logs` layer draws from the same namespace registry, so a log written
/// under a namespace no state layer knows would be refused rather than shipped.
#[test]
fn the_golden_logs_layer_draws_from_the_namespace_registry() {
    let registry: BTreeSet<Namespace> = NAMESPACES.into_iter().collect();

    let present: BTreeSet<Namespace> = common::logs().iter().map(|record| record.ns).collect();

    assert!(present.is_subset(&registry));
    assert!(!present.is_empty());
    assert!(!present.contains(UTXOS), "the utxo set is not a log");
}
