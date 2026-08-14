//! Cursor coherence — the cheapest check, and the one that explains most of
//! the others when it fails.
//!
//! `dolos data summary` already prints these four tips; this check asserts
//! the relationships between them. The apply pipeline commits a block to the
//! stores in a fixed order (`sync::run_lifecycle`): WAL, then state, then
//! archive, then indexes. Every rule below follows from that order, so a
//! violation names both the invariant and the commit that did not run.

use dolos_core::{ArchiveStore, BlockSlot, ChainPoint, IndexStore, StateStore, WalStore};
use miette::{Context as _, IntoDiagnostic as _};

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::Cursors;

/// The four stores' positions, read once so the check itself is a pure
/// function of them.
///
/// The WAL and the two derived cursors are kept as [`ChainPoint`]s rather
/// than slots because [`ChainPoint::Origin`] is not slot 0 — it is *before*
/// any block. Genesis leaves every one of them at Origin with an empty
/// archive, and a check that read Origin as a position would call the one
/// state every node starts in corrupt.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tips {
    pub wal_start: Option<ChainPoint>,
    pub wal_tip: Option<ChainPoint>,
    pub archive_start: Option<BlockSlot>,
    pub archive_tip: Option<BlockSlot>,
    pub state: Option<ChainPoint>,
    pub indexes: Option<ChainPoint>,
}

/// The slot of a cursor that has actually applied a block.
///
/// `None` for both "no cursor" and Origin: neither has applied anything, and
/// every rule below is about stores that have.
fn applied(cursor: &Option<ChainPoint>) -> Option<BlockSlot> {
    match cursor {
        None | Some(ChainPoint::Origin) => None,
        Some(point) => Some(point.slot()),
    }
}

/// Assert that the four tips tell one story.
///
/// An empty archive is not itself an issue — a data directory that has never
/// synced is consistent — but a state or index cursor over an empty archive
/// is, because those cursors are only written after a block has been applied.
pub fn check_cursors(tips: &Tips) -> Vec<Issue> {
    let mut issues = Vec::new();

    let Some(archive_tip) = tips.archive_tip else {
        for (name, cursor) in [("state", &tips.state), ("index", &tips.indexes)] {
            if let Some(slot) = applied(cursor) {
                issues.push(Issue::new(
                    CHECK,
                    format!(
                        "{name} cursor sits at slot {slot} but the archive holds no blocks at all"
                    ),
                ));
            }
        }

        if let Some(wal_tip) = applied(&tips.wal_tip) {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the WAL reaches slot {wal_tip} but the archive holds no blocks; the archive \
                     commit never ran — `dolos doctor catchup-stores` replays the WAL into the \
                     other stores"
                ),
            ));
        }

        return issues;
    };

    // The archive is the store queries read, so both derived cursors have to
    // sit inside the range it can actually serve.
    let archive_start = tips.archive_start.unwrap_or(archive_tip);

    for (name, cursor) in [("state", &tips.state), ("index", &tips.indexes)] {
        let Some(slot) = applied(cursor) else {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the archive reaches slot {archive_tip} but there is no {name} cursor; the \
                     {name} store was never advanced"
                ),
            ));
            continue;
        };

        if slot > archive_tip {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "{name} cursor at slot {slot} is ahead of the archive tip at slot \
                     {archive_tip}; the archive commit did not run for at least one applied block"
                ),
            ));
        } else if slot < archive_start {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "{name} cursor at slot {slot} is before the archive's first block at slot \
                     {archive_start}; the archive cannot serve the range the {name} store claims"
                ),
            ));
        }
    }

    // The WAL is written first and pruned from behind, so it must both reach
    // the archive tip and start at or before it — otherwise there is a hole
    // between what the archive holds and what the WAL can replay.
    match (applied(&tips.wal_start), applied(&tips.wal_tip)) {
        (_, None) => issues.push(Issue::new(
            CHECK,
            format!(
                "the archive reaches slot {archive_tip} but the WAL is empty; blocks are written \
                 to the WAL before any other store"
            ),
        )),
        (_, Some(wal_tip)) if wal_tip < archive_tip => issues.push(Issue::new(
            CHECK,
            format!(
                "WAL tip at slot {wal_tip} is behind the archive tip at slot {archive_tip}; \
                 blocks are written to the WAL before the archive, so the WAL cannot be the \
                 shorter of the two"
            ),
        )),
        (Some(wal_start), Some(_)) if wal_start > archive_tip => issues.push(Issue::new(
            CHECK,
            format!(
                "the WAL starts at slot {wal_start}, after the archive tip at slot {archive_tip}; \
                 nothing links the two stores across that gap"
            ),
        )),
        _ => (),
    }

    issues
}

pub fn run(stores: &crate::common::Stores) -> miette::Result<Vec<Issue>> {
    let wal_start = stores
        .wal
        .find_start()
        .into_diagnostic()
        .context("reading WAL start")?
        .map(|(point, _)| point);

    let wal_tip = stores
        .wal
        .find_tip()
        .into_diagnostic()
        .context("reading WAL tip")?
        .map(|(point, _)| point);

    let archive_tip = stores
        .archive
        .get_tip()
        .into_diagnostic()
        .context("reading archive tip")?
        .map(|(slot, _)| slot);

    // The first entry of the full range: one read, not a scan.
    let archive_start = stores
        .archive
        .get_range(None, None)
        .into_diagnostic()
        .context("reading archive start")?
        .next()
        .map(|(slot, _)| slot);

    let state = stores
        .state
        .read_cursor()
        .into_diagnostic()
        .context("reading state cursor")?;

    let indexes = stores
        .indexes
        .cursor()
        .into_diagnostic()
        .context("reading index cursor")?;

    let tips = Tips {
        wal_start,
        wal_tip,
        archive_start,
        archive_tip,
        state,
        indexes,
    };

    Ok(check_cursors(&tips))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(slot: BlockSlot) -> Option<ChainPoint> {
        Some(ChainPoint::Slot(slot))
    }

    fn healthy() -> Tips {
        Tips {
            wal_start: point(500),
            wal_tip: point(1000),
            archive_start: Some(0),
            archive_tip: Some(1000),
            state: point(1000),
            indexes: point(1000),
        }
    }

    #[test]
    fn healthy_tips_pass() {
        assert!(check_cursors(&healthy()).is_empty());
    }

    /// A data directory that has never synced is consistent, not broken.
    #[test]
    fn empty_stores_pass() {
        assert!(check_cursors(&Tips::default()).is_empty());
    }

    /// What genesis leaves behind, and what every node looks like before its
    /// first block: cursors at Origin over an empty archive. Origin is not
    /// slot 0 — it is before any block — so nothing here has applied
    /// anything, and there is nothing to disagree about.
    #[test]
    fn a_freshly_bootstrapped_store_passes() {
        let tips = Tips {
            wal_start: Some(ChainPoint::Origin),
            wal_tip: Some(ChainPoint::Origin),
            archive_start: None,
            archive_tip: None,
            state: Some(ChainPoint::Origin),
            indexes: Some(ChainPoint::Origin),
        };

        let issues = check_cursors(&tips);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The corruption this check exists for: a crash between `commit_state`
    /// and `commit_archive` leaves the state cursor one block past the
    /// archive.
    #[test]
    fn state_cursor_ahead_of_archive_is_reported() {
        let tips = Tips {
            state: point(1001),
            ..healthy()
        };

        let issues = check_cursors(&tips);

        assert_eq!(issues.len(), 1);
        assert!(issues[0].detail.contains("state cursor at slot 1001"));
        assert!(issues[0].detail.contains("archive tip at slot 1000"));
    }

    /// Every issue is reported, not the first: both derived cursors are
    /// stale here, and the WAL gap is a third, independent finding.
    #[test]
    fn every_issue_is_collected() {
        let tips = Tips {
            archive_start: Some(900),
            state: point(800),
            indexes: point(800),
            wal_start: point(1200),
            ..healthy()
        };

        let issues = check_cursors(&tips);

        assert_eq!(issues.len(), 3);
        assert!(issues.iter().all(|x| x.check == CheckKind::Cursors));
    }

    #[test]
    fn wal_behind_archive_is_reported() {
        let tips = Tips {
            wal_tip: point(900),
            ..healthy()
        };

        let issues = check_cursors(&tips);

        assert_eq!(issues.len(), 1);
        assert!(issues[0].detail.contains("WAL tip at slot 900"));
    }

    /// A cursor over an empty archive means the archive lost data the other
    /// stores still claim.
    #[test]
    fn cursors_over_an_empty_archive_are_reported() {
        let tips = Tips {
            archive_start: None,
            archive_tip: None,
            ..healthy()
        };

        let issues = check_cursors(&tips);

        assert_eq!(issues.len(), 3);
        assert!(issues.iter().any(|x| x.detail.contains("state cursor")));
        assert!(issues.iter().any(|x| x.detail.contains("index cursor")));
        assert!(issues.iter().any(|x| x.detail.contains("the WAL reaches")));
    }
}
