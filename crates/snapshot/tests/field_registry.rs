//! Done criterion: the compatibility contract of decision 0026, enforced.
//!
//! `goldens.rs` pins what a *layer* looks like — digests over framed record
//! streams. This suite pins what a *record* looks like: the numbered field
//! table of every type a stele can carry, one namespace at a time. The two
//! are complementary. A renumbered field inside an entity value moves a
//! golden digest too, but it moves it without saying why; here it fails with
//! the namespace, the revision and the byte position of the drift.
//!
//! What a failure in this file means, in order of likelihood:
//!
//! - **A field was renumbered, removed, or retyped.** Breaking. The namespace's
//!   `state-{ns}` / `log-{ns}` kind needs a media-type version bump; editing
//!   the golden hides a break that every published stele already carries.
//! - **A field was appended and `SCHEMA_REVS` was not bumped** (or the reverse).
//!   Additive, and the fix is the bump plus a new retained canary beside the
//!   old one — never an edit to the old one.
//! - **A variant was added to a reachable enum.** Breaking within `v{x}`
//!   regardless of the field policy: decoders refuse indexes they do not know.
//! - **The same value encoded to different bytes twice.** Not a re-pin under
//!   any circumstances — it is an encoding-determinism defect, and it breaks
//!   the cross-party digest identity ADR-004 rests on.
//!
//! Adding a namespace, a revision or an enum is `tests/registry/`; this file
//! only states the rules.

mod registry;

use dolos_core::Namespace;
use dolos_snapshot::{NAMESPACES, SCHEMA_REVS};
use registry::{decode_hex, ground_rules, normalize, Entry};

/// How many times a canary is rebuilt and re-encoded before its bytes are
/// believed.
///
/// One pass would only catch a value that varies with the machine. The failure
/// this guards against is a container whose iteration order varies per
/// *instance* — a `HashSet` built twice in one process is the canonical case —
/// which needs repetition inside a single run to surface. Thirty-two is far
/// past the point where a two-way order divergence would go unseen.
const DETERMINISM_ROUNDS: usize = 32;

fn schema_rev(ns: Namespace) -> u64 {
    SCHEMA_REVS
        .into_iter()
        .find(|(known, _)| *known == ns)
        .map(|(_, rev)| rev)
        .unwrap_or_else(|| panic!("{ns} has no entry in SCHEMA_REVS"))
}

/// The assertions every registry entry answers, applied to one of them.
///
/// Shared by the seventeen real namespaces and by the synthetic entry of
/// `ground_rules`, so the append-only path is exercised by the same code that
/// enforces it rather than by a parallel imitation of it.
fn check_entry(entry: &Entry, expected_rev: u64) {
    let ns = entry.ns;

    assert!(
        !entry.history.is_empty(),
        "{ns}: the registry carries no canary",
    );

    // Revisions are append-only: ascending, distinct, starting at 1.
    let mut previous = 0;
    for pinned in entry.history {
        assert!(
            pinned.rev > previous,
            "{ns}: revisions must ascend and be distinct, found {} after {previous}",
            pinned.rev,
        );
        previous = pinned.rev;
    }

    assert_eq!(
        entry.history[0].rev, 1,
        "{ns}: the retained history must reach back to revision 1",
    );

    let current = entry.history.last().expect("checked non-empty");

    assert_eq!(
        current.rev, expected_rev,
        "{ns}: the registry's current revision and SCHEMA_REVS disagree. \
         A field appended to this namespace needs both a SCHEMA_REVS bump and \
         a new canary retained beside the old one; a bump with no new canary \
         is a revision nothing pins.",
    );

    // The current canary still encodes to its pinned bytes.
    let (reported_ns, encoded) = (entry.encode)();

    assert_eq!(
        reported_ns, ns,
        "{ns}: the canary's own encode path reports a different namespace",
    );

    assert_eq!(
        hex::encode(&encoded),
        normalize(current.hex),
        "{ns}: the canary no longer encodes to the bytes pinned at revision {}. \
         If a field was renumbered, removed or retyped, this is a breaking \
         change and needs a media-type version bump on this namespace's kind — \
         not a new golden.",
        current.rev,
    );

    // Every retained revision, current included, still decodes under today's
    // decoder. This is the tolerance the readers rely on, asserted rather than
    // assumed.
    for pinned in entry.history {
        let bytes = decode_hex(pinned.hex);

        if let Err(err) = (entry.decode)(&bytes) {
            panic!(
                "{ns}: today's decoder refuses the canary retained at revision {}: {err}. \
                 A reader that cannot read what was published is a compatibility break, \
                 not a stale golden.",
                pinned.rev,
            );
        }
    }
}

#[test]
fn registry_covers_every_namespace() {
    let entries = registry::registry();

    let mut covered: Vec<Namespace> = entries.iter().map(|entry| entry.ns).collect();
    covered.sort_unstable();

    let deduped = {
        let mut copy = covered.clone();
        copy.dedup();
        copy
    };

    assert_eq!(deduped, covered, "a namespace has two registry entries");

    assert_eq!(
        covered,
        NAMESPACES.to_vec(),
        "the field registry and NAMESPACES disagree. A namespace with no canary \
         is a record shape nothing pins — which is the state the registry exists \
         to make impossible.",
    );
}

#[test]
fn every_canary_matches_the_registry() {
    for entry in registry::registry() {
        check_entry(&entry, schema_rev(entry.ns));
    }
}

/// The append-only path, exercised on the one entry whose history has actually
/// moved.
///
/// Every real namespace sits at revision 1 today, so the retained-canary
/// assertions in [`check_entry`] would otherwise be running over a single
/// element and proving nothing about the case they exist for. The synthetic
/// entry appends a field for real: revision 1's bytes stay pinned and must
/// still decode under revision 2's type.
#[test]
fn the_append_only_path_holds_on_a_history_that_moved() {
    check_entry(
        &ground_rules::synthetic_entry(),
        ground_rules::SYNTHETIC_REV,
    );
}

/// Encoding a canary must be a function of the value alone.
///
/// The profile copies stored entity values verbatim into layers, so a record
/// whose bytes depend on anything but its content — a hash container's
/// iteration order above all — makes two honest publishers disagree about the
/// same ledger. ADR-004's independent-builds check would catch it eventually,
/// on a machine, hours into a publish; this catches it here.
#[test]
fn canary_encodings_are_deterministic() {
    for entry in registry::registry() {
        let (_, first) = (entry.encode)();

        for round in 1..DETERMINISM_ROUNDS {
            let (_, again) = (entry.encode)();

            assert_eq!(
                hex::encode(&again),
                hex::encode(&first),
                "{}: the same value encoded to different bytes on round {round}. \
                 This is an encoding-determinism defect, not a golden to re-pin: \
                 a record that does not encode deterministically cannot be \
                 published, because independent publishers of the same ledger \
                 would produce different digests for identical state.",
                entry.ns,
            );
        }
    }
}

#[test]
fn enum_variant_tables_are_pinned() {
    for table in registry::enums::tables() {
        let encoded = (table.encode)();

        let names: Vec<&str> = encoded.iter().map(|(name, _)| *name).collect();
        let pinned_names: Vec<&str> = table.pinned.iter().map(|(name, _)| *name).collect();

        assert_eq!(
            names, pinned_names,
            "{}: the variant table changed shape. {}",
            table.name, table.policy,
        );

        for ((name, bytes), (_, pinned)) in encoded.iter().zip(table.pinned) {
            assert_eq!(
                hex::encode(bytes),
                normalize(pinned),
                "{}::{name} no longer encodes to its pinned index. {}",
                table.name,
                table.policy,
            );
        }
    }
}

/// Rule 1: a decoder skips a trailing field it does not know.
#[test]
fn an_unknown_trailing_field_is_skipped() {
    let bytes = decode_hex(ground_rules::AFTER_APPEND_HEX);

    let old: ground_rules::BeforeAppend =
        minicbor::decode(&bytes).expect("an older reader reads a newer record");

    assert_eq!(old, ground_rules::before_append());
}

/// Rule 3: a missing trailing field is tolerated, as `None` for an `Option`
/// and as the type's default under `#[cbor(default)]`.
#[test]
fn a_missing_trailing_field_is_defaulted() {
    let bytes = decode_hex(ground_rules::BEFORE_APPEND_HEX);

    let optional: ground_rules::AfterAppend =
        minicbor::decode(&bytes).expect("a newer reader reads an older record");
    assert_eq!(optional.c, None);

    let defaulted: ground_rules::AfterDefaultedAppend =
        minicbor::decode(&bytes).expect("a newer reader reads an older record");
    assert_eq!(defaulted.c, 0);
}

/// Rule 2: a gap in the index sequence is null-padded rather than closed, so
/// the fields before it keep their positions.
#[test]
fn an_index_gap_is_null_padded() {
    let encoded = minicbor::to_vec(ground_rules::gapped()).expect("the gapped canary encodes");

    assert_eq!(
        hex::encode(&encoded),
        ground_rules::GAPPED_HEX,
        "minicbor no longer pads index gaps the way this profile's records assume",
    );
}

/// The failure the registry exists to catch, demonstrated on itself: two types
/// with the same fields and swapped indexes encode to different bytes.
#[test]
fn renumbering_moves_bytes() {
    let straight = minicbor::to_vec(ground_rules::before_append()).expect("the canary encodes");
    let swapped = minicbor::to_vec(ground_rules::renumbered()).expect("the canary encodes");

    assert_eq!(hex::encode(&straight), ground_rules::BEFORE_APPEND_HEX);
    assert_eq!(hex::encode(&swapped), ground_rules::RENUMBERED_HEX);

    assert_ne!(
        straight, swapped,
        "renumbering two fields left the encoding unchanged, which would make \
         the whole field registry unable to see a renumbering at all",
    );
}

/// Print the current encodings, for the one legitimate reason to need them:
/// pinning a **new** revision after a deliberate field append.
///
/// Never a way to refresh an existing pin. A golden that disagrees with a
/// re-run on unchanged code is a determinism defect; a golden that disagrees
/// after a code change is the change being breaking. Both are findings, and
/// neither is fixed here.
///
/// `cargo test -p dolos-snapshot --test field_registry -- --ignored --nocapture`
#[test]
#[ignore = "prints goldens for a new revision; never re-pins an existing one"]
fn print_current_canary_encodings() {
    for entry in registry::registry() {
        let (_, encoded) = (entry.encode)();
        println!("{} rev {}", entry.ns, schema_rev(entry.ns));
        println!("{}", hex::encode(&encoded));
    }

    for table in registry::enums::tables() {
        for (name, bytes) in (table.encode)() {
            println!("{}::{name} {}", table.name, hex::encode(&bytes));
        }
    }
}
