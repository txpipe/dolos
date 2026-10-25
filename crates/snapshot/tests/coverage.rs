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

use dolos_cardano::{
    indexes::{archive_dimensions, entities},
    model::{build_log_schema, build_schema},
};
use dolos_core::{EntityKind, ExactKind, IndexRecord, Namespace};
use dolos_snapshot::{
    entity_kind_for, entity_layer_for, layers::indexes, log_kind_for, log_ns_for, namespaces,
    shards_for, state_kind_for, state_ns_for, ENTITY_INDEX_KINDS, ENTITY_KINDS, EPOCH_KINDS,
    LOG_KINDS, LOG_NAMESPACES, NAMESPACES, STATE_KINDS, UTXOS,
};

/// The namespaces this profile carries are exactly the two schemas', plus the
/// UTxO set: the state kinds carry `build_schema`'s and the log kinds carry
/// `build_log_schema`'s.
#[test]
fn namespace_registry_matches_the_schemas() {
    let states: BTreeSet<Namespace> = build_schema().keys().copied().collect();
    let logs: BTreeSet<Namespace> = build_log_schema().keys().copied().collect();
    let registry: BTreeSet<Namespace> = NAMESPACES.into_iter().collect();

    assert!(
        registry.contains(UTXOS),
        "the utxo set is a state namespace"
    );

    let entities: BTreeSet<Namespace> =
        registry.iter().copied().filter(|ns| *ns != UTXOS).collect();

    assert_eq!(
        entities,
        states.union(&logs).copied().collect(),
        "the profile's namespaces have drifted from the schemas"
    );

    let state_kinds: BTreeSet<Namespace> = STATE_KINDS
        .into_iter()
        .map(|(_, ns, _)| ns)
        .filter(|ns| *ns != UTXOS)
        .collect();

    assert_eq!(
        state_kinds, states,
        "the state kinds have drifted from build_schema()"
    );

    assert_eq!(
        LOG_NAMESPACES.into_iter().collect::<BTreeSet<_>>(),
        logs,
        "the log kinds have drifted from build_log_schema()"
    );
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

/// Dimensions stay fail-closed, and that is a carve-out rather than an
/// oversight (decision 0026).
///
/// An unknown layer *kind* is a plan-time choice — the restore sees the layer
/// in the inscription, decides to skip it, and says so. An unknown *dimension*
/// is not: `indexes` is one layer, so the dimension only appears mid-stream,
/// record by record, and skipping there is silent data loss inside a layer the
/// plan already committed to restoring. Changing the dimension set therefore
/// remains a media-type-version event.
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

/// Every state namespace appears in the golden state layers.
///
/// The record no longer names its namespace, so this counts *layers* rather
/// than records: a namespace is covered when the fixture writes the kind that
/// carries it. That is the only thing keeping the split honest here — a
/// namespace whose kind the fixture skipped would have no golden digest
/// freezing its media type.
#[test]
fn the_golden_state_layers_cover_every_namespace() {
    let expected: BTreeSet<Namespace> = STATE_KINDS.into_iter().map(|(_, ns, _)| ns).collect();

    let present: BTreeSet<Namespace> = common::state_layers()
        .into_iter()
        .map(|(_, ns, _)| ns)
        .collect();

    assert_eq!(
        present, expected,
        "the golden state layers no longer freeze every namespace"
    );

    // And each of those layers actually carries a record: an empty layer would
    // freeze the kind string and nothing about the codec under it.
    for (kind, ns, shard) in common::state_layers() {
        assert!(
            !common::state(ns, shard).is_empty(),
            "{kind}/{shard}: no golden record"
        );
    }
}

/// Every state kind appears in the golden stele, so every state kind string is
/// frozen by a published digest — the state half of
/// `the_golden_stele_covers_every_log_kind`.
#[test]
fn the_golden_stele_covers_every_state_kind() {
    let present: BTreeSet<&str> = common::all_layers()
        .iter()
        .filter_map(|(kind, _, _)| state_ns_for(kind).map(|_| *kind))
        .collect();

    let expected: BTreeSet<&str> = STATE_KINDS.into_iter().map(|(kind, _, _)| kind).collect();

    assert_eq!(
        present, expected,
        "the golden stele no longer freezes every state kind"
    );
}

/// The log namespaces draw from the same registry, so a log kind can never name
/// a namespace the profile does not know.
#[test]
fn the_log_namespaces_are_registered() {
    let registry: BTreeSet<Namespace> = NAMESPACES.into_iter().collect();
    let logs: BTreeSet<Namespace> = LOG_NAMESPACES.into_iter().collect();

    assert_eq!(logs.len(), LOG_NAMESPACES.len(), "duplicate log namespace");
    assert!(logs.is_subset(&registry));
    assert!(!logs.contains(UTXOS), "the utxo set is not a log");
}

/// Namespaces are kebab-case (decision 0056): lowercase ASCII words joined by
/// hyphens, so a kind is the namespace verbatim and needs no rewrite.
#[test]
fn namespaces_are_kebab_case() {
    for ns in NAMESPACES {
        assert!(
            !ns.is_empty()
                && !ns.starts_with('-')
                && !ns.ends_with('-')
                && !ns.contains("--")
                && ns.bytes().all(|b| b.is_ascii_lowercase() || b == b'-'),
            "{ns:?} is not kebab-case"
        );
    }
}

/// Done criterion 2: the KIND↔NS table, held to its own rule.
///
/// The table is spelled out rather than composed — house style, and what makes
/// the wire vocabulary greppable — so the rule that produced the spelling has
/// to be a test or it is nothing. The kind is the namespace verbatim, because
/// namespaces are kebab-case (decision 0056) and a media type's kind token
/// admits hyphens and not underscores; `namespaces_are_kebab_case` pins the
/// half of that rule this test leans on.
#[test]
fn log_kinds_derive_from_their_namespaces() {
    let mut kinds = Vec::new();

    for (kind, ns) in LOG_KINDS {
        assert_eq!(kind, format!("log-{ns}"), "{ns}");

        // Both directions of the lookup, against the spelling and not against
        // the derivation: a table entry nothing can find is a layer nothing can
        // restore.
        assert_eq!(log_kind_for(ns), Some(kind), "{ns}");
        assert_eq!(log_ns_for(kind), Some(ns), "{kind}");

        kinds.push(kind);
    }

    // Injective in both columns. Two namespaces sharing a kind would publish one
    // namespace's logs under another's name; two kinds sharing a namespace would
    // restore the same records twice.
    let distinct_kinds: BTreeSet<&str> = kinds.iter().copied().collect();
    let distinct_namespaces: BTreeSet<Namespace> =
        LOG_KINDS.into_iter().map(|(_, ns)| ns).collect();

    assert_eq!(distinct_kinds.len(), LOG_KINDS.len());
    assert_eq!(distinct_namespaces.len(), LOG_KINDS.len());

    // Ascending, in both columns: the inscription lists layers in `KINDS` order,
    // so the table's order is published order.
    let mut sorted = kinds.clone();
    sorted.sort_unstable();
    assert_eq!(kinds, sorted, "LOG_KINDS must be in byte order");

    let mut sorted = LOG_NAMESPACES;
    sorted.sort_unstable();
    assert_eq!(
        LOG_NAMESPACES, sorted,
        "LOG_NAMESPACES must be in byte order"
    );

    // Nothing else is a log kind — `logs`, above all, which is what these
    // replaced.
    for absent in ["logs", "log-", "log-utxos", "blocks", "", "log-stakes"] {
        assert!(log_ns_for(absent).is_none(), "{absent:?}");
    }
}

/// Every log kind appears in the golden stele, so every log kind string is
/// frozen by a published digest.
#[test]
fn the_golden_stele_covers_every_log_kind() {
    let present: BTreeSet<&str> = common::all_layers()
        .iter()
        .filter_map(|(kind, _, _)| log_ns_for(kind).map(|_| *kind))
        .collect();

    let expected: BTreeSet<&str> = LOG_KINDS.into_iter().map(|(kind, _)| kind).collect();

    assert_eq!(
        present, expected,
        "the golden stele no longer freezes every log kind"
    );

    for (_, ns) in LOG_KINDS {
        assert!(!common::logs(ns).is_empty(), "{ns}: no golden record");
    }
}

/// The ENTITY_KINDS table, held to the rule that spelled it: `entity-` and the
/// kind's name, one layer kind per kind the chain declares, in byte order.
#[test]
fn entity_kinds_derive_from_their_names() {
    let mut layers = Vec::new();

    for (layer, kind) in ENTITY_KINDS {
        assert_eq!(layer, format!("entity-{}", kind.name()), "{kind}");

        // A media type's kind token admits hyphens and not underscores.
        assert!(
            kind.name()
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "{kind}: not a kind token"
        );

        assert_eq!(entity_layer_for(kind), Some(layer), "{kind}");
        assert_eq!(entity_kind_for(layer), Some(kind), "{layer}");

        layers.push(layer);
    }

    // Every kind the chain declares, and nothing else.
    assert_eq!(ENTITY_INDEX_KINDS, entities::ALL);

    let mut sorted = layers.clone();
    sorted.sort_unstable();
    assert_eq!(layers, sorted, "ENTITY_KINDS must be in byte order");

    // Epoch-scoped, so a window's entity layer inherits per `(kind, scope)`.
    for layer in &layers {
        assert!(EPOCH_KINDS.contains(layer), "{layer}");
    }

    for absent in ["entity-", "entity-datum", "entities", "indexes", ""] {
        assert!(entity_kind_for(absent).is_none(), "{absent:?}");
    }

    assert!(entity_layer_for(EntityKind::new("datum", 32)).is_none());
}

/// Every entity kind appears in the golden stele, so every entity kind string
/// and id width is frozen by a published digest.
#[test]
fn the_golden_stele_covers_every_entity_kind() {
    let present: BTreeSet<&str> = common::all_layers()
        .iter()
        .filter_map(|(kind, _, _)| entity_kind_for(kind).map(|_| *kind))
        .collect();

    let expected: BTreeSet<&str> = ENTITY_KINDS.into_iter().map(|(kind, _)| kind).collect();

    assert_eq!(
        present, expected,
        "the golden stele no longer freezes every entity kind"
    );

    for (_, kind) in ENTITY_KINDS {
        assert!(
            !common::entities(kind).is_empty(),
            "{kind}: no golden record"
        );
    }
}

/// Done criterion 3's table half: the STATE_KINDS table, held to its own rule.
///
/// The same discipline `log_kinds_derive_from_their_namespaces` applies, plus
/// the column the log table does not have — the shard count, which is
/// specification rather than tuning and so is pinned here namespace by
/// namespace.
#[test]
fn state_kinds_derive_from_their_namespaces() {
    let mut kinds = Vec::new();

    for (kind, ns, shards) in STATE_KINDS {
        assert_eq!(kind, format!("state-{ns}"), "{ns}");

        // Both directions of the lookup, against the spelling and not against
        // the derivation: a table entry nothing can find is a layer nothing can
        // restore.
        assert_eq!(state_kind_for(ns), Some(kind), "{ns}");
        assert_eq!(state_ns_for(kind), Some(ns), "{kind}");
        assert_eq!(shards_for(ns), Some(shards), "{ns}");

        kinds.push(kind);
    }

    // The pre-0056 spellings are not aliases: v2 steles are a fresh lineage.
    for retired in ["pending_rewards", "pending_mirs", "stakes"] {
        assert!(state_kind_for(retired).is_none(), "{retired}");
    }

    // Injective in both columns. Two namespaces sharing a kind would publish one
    // namespace's tip under another's name; two kinds sharing a namespace would
    // restore the same records twice.
    let distinct_kinds: BTreeSet<&str> = kinds.iter().copied().collect();
    let distinct_namespaces: BTreeSet<Namespace> =
        STATE_KINDS.into_iter().map(|(_, ns, _)| ns).collect();

    assert_eq!(distinct_kinds.len(), STATE_KINDS.len());
    assert_eq!(distinct_namespaces.len(), STATE_KINDS.len());

    // Ascending, in both columns: the inscription lists layers in `KINDS` order,
    // so the table's order is published order.
    let mut sorted = kinds.clone();
    sorted.sort_unstable();
    assert_eq!(kinds, sorted, "STATE_KINDS must be in byte order");

    let states: BTreeSet<Namespace> = build_schema().keys().copied().collect();
    let registered: Vec<Namespace> = NAMESPACES
        .into_iter()
        .filter(|ns| *ns == UTXOS || states.contains(ns))
        .collect();

    assert_eq!(
        STATE_KINDS.map(|(_, ns, _)| ns).to_vec(),
        registered,
        "STATE_KINDS and the state namespaces disagree"
    );

    // An archive-only log has no state kind: a stele carries no state layer
    // for it.
    for ns in NAMESPACES {
        if ns != UTXOS && !states.contains(ns) {
            assert_eq!(state_kind_for(ns), None, "{ns}");
        }
    }

    // The shard map is the spec's, not the data's: the chain-scale namespaces
    // split sixteen ways, and every other namespace is a single blob. Changing
    // one of these is a media-type-version event for that kind, so it is spelled
    // out here rather than derived from the table it is checking.
    for (_, ns, shards) in STATE_KINDS {
        let expected = match ns {
            "utxos" | "accounts" | "assets" | "datums" | "scripts" => 16,
            _ => 1,
        };

        assert_eq!(shards, expected, "{ns}");
    }

    // Nothing else is a state kind — `state`, above all, which is what these
    // replaced.
    for absent in ["state", "state-", "state-logs", "blocks", "", "state_utxos"] {
        assert!(state_ns_for(absent).is_none(), "{absent:?}");
    }

    // And a namespace this profile does not define has neither kind nor count.
    assert_eq!(state_kind_for("receipts"), None);
    assert_eq!(shards_for("receipts"), None);
}
