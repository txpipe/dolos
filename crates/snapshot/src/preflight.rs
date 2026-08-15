//! One free-space policy, in both directions.
//!
//! A publish and a restore have opposite shapes and the same problem: each
//! needs room on a volume before it starts, and the operator finds out hours in
//! when it does not have it. The peaks differ — a publish holds all sixteen
//! shard sinks open across one walk of the store, a restore stages one layer at
//! a time and drops it — but the *policy* over those numbers is one thing, and
//! it lives here rather than in each driver so the two cannot come to disagree.
//!
//! ## The policy
//!
//! - **A measured shortfall refuses**, naming the volume and how far short it
//!   is. Only what was actually measured can refuse.
//! - **What cannot be measured warns and proceeds** — free space that will not
//!   read, or a need nothing could size (a first publish has no predecessor to
//!   size from).
//!
//! There is no override flag. A `--scratch-dir` pointed at a bigger volume is
//! the escape hatch, and a better one than a flag that turns the check off.
//!
//! ## Needs that share a volume are summed
//!
//! A restore's destination and its staging directory are the same pool of free
//! bytes whenever the scratch directory sits on the storage filesystem — which
//! the default, `<storage.path>/scratch`, guarantees. Checking each against the
//! whole of that pool would pass two needs that together do not fit, so
//! [`check`] groups by volume first and compares the sum.

use std::path::{Path, PathBuf};

use crate::Error;

/// One demand a run makes on a filesystem, before it makes it.
#[derive(Debug, Clone)]
pub struct Need {
    /// What the bytes are for, phrased to read as the subject of a refusal:
    /// "*restoring it* needs at least N bytes at …".
    what: String,
    /// Where they land. Need not exist yet — [`check`] measures the nearest
    /// ancestor that does, which is the shape a fresh node's storage path and
    /// an uncreated scratch directory both have.
    path: PathBuf,
    size: Sizing,
}

/// How many bytes a need is, or why nothing could say.
#[derive(Debug, Clone)]
enum Sizing {
    /// A floor, and always a floor: every number here is compressed or
    /// uncompressed bytes as some document states them, and a store keeps
    /// indexes and slack of its own. Under-stating is the safe direction for a
    /// check whose job is to catch the obviously-doomed run.
    Bytes(u64),
    /// Nothing could size it, and why. A warning, never a refusal.
    Unknown(String),
}

impl Need {
    /// A need of `bytes` at `path`.
    pub fn of(what: impl Into<String>, path: impl Into<PathBuf>, bytes: u64) -> Self {
        Self {
            what: what.into(),
            path: path.into(),
            size: Sizing::Bytes(bytes),
        }
    }

    /// A need nothing could size, and the reason to tell the operator.
    pub fn unsized_because(
        what: impl Into<String>,
        path: impl Into<PathBuf>,
        why: impl Into<String>,
    ) -> Self {
        Self {
            what: what.into(),
            path: path.into(),
            size: Sizing::Unknown(why.into()),
        }
    }

    /// `bytes` when it is `Some`, and a warning carrying `why` when it is not.
    ///
    /// The shape almost every caller has: a size read out of a document that
    /// may not carry one.
    pub fn or_unsized(
        what: impl Into<String>,
        path: impl Into<PathBuf>,
        bytes: Option<u64>,
        why: impl Into<String>,
    ) -> Self {
        match bytes {
            Some(bytes) => Self::of(what, path, bytes),
            None => Self::unsized_because(what, path, why),
        }
    }
}

/// Refuse a run whose measured needs cannot fit; warn about the rest.
///
/// The single entry point for both directions. Needs landing on one volume are
/// summed before the comparison — see the module documentation.
pub fn check(needs: &[Need]) -> Result<(), Error> {
    let mut sized: Vec<(&Need, u64, PathBuf)> = Vec::new();

    for need in needs {
        match &need.size {
            Sizing::Unknown(why) => tracing::warn!(
                path = %need.path.display(),
                "could not size {}; skipping the free-space check: {why}",
                need.what,
            ),
            Sizing::Bytes(bytes) => match probe_of(&need.path) {
                Some(probe) => sized.push((need, *bytes, probe)),
                // A run that cannot measure the disk is not a run that should
                // refuse to start, but it is one whose operator should hear
                // about it.
                None => tracing::warn!(
                    path = %need.path.display(),
                    "could not determine free space; skipping the free-space check for {}",
                    need.what,
                ),
            },
        }
    }

    for group in group_by_volume(&sized) {
        let probe = &sized[group[0]].2;

        let available = match fs4::available_space(probe) {
            Ok(available) => available,
            Err(e) => {
                tracing::warn!(
                    path = %probe.display(),
                    "could not determine free space; skipping the free-space check: {e}"
                );
                continue;
            }
        };

        // Saturating because these are two documents' numbers added together
        // and nothing bounds their sum; a total that pins at `u64::MAX` refuses
        // for the same reason the true total would.
        let required = group
            .iter()
            .fold(0u64, |total, i| total.saturating_add(sized[*i].1));

        if required > available {
            let parts: Vec<&(&Need, u64, PathBuf)> = group.iter().map(|i| &sized[*i]).collect();

            return Err(shortfall(&parts, required, available));
        }
    }

    Ok(())
}

/// The refusal, naming the volume and how far short it is.
///
/// Two phrasings, because two needs sharing a volume is not the same incident
/// as one need overflowing it: an operator reading "restoring it needs" when
/// the staging is half the number would go looking in the wrong place, so the
/// shared case breaks the total down by need and by path before it states it.
fn shortfall(parts: &[&(&Need, u64, PathBuf)], required: u64, available: u64) -> Error {
    let short = required - available;

    match parts {
        [(only, _, _)] => Error::NotEnoughSpace(format!(
            "{} needs at least {required} bytes at {}, which has {available} free — {short} bytes \
             short",
            only.what,
            only.path.display(),
        )),
        many => {
            let breakdown: Vec<String> = many
                .iter()
                .map(|(need, bytes, _)| {
                    format!("{} ({bytes} bytes at {})", need.what, need.path.display())
                })
                .collect();

            Error::NotEnoughSpace(format!(
                "{} share one volume and together need at least {required} bytes, which has \
                 {available} free — {short} bytes short",
                breakdown.join(" and "),
            ))
        }
    }
}

/// The nearest existing ancestor of `path`, which is what the filesystem can be
/// asked about.
///
/// A fresh node's `storage.path` and an uncreated scratch directory are both
/// about to exist, and neither can be `stat`ed yet; the volume they will land
/// on is the one holding the deepest ancestor that does exist. `None` means
/// nothing in the chain answered, which is the case the caller warns about.
fn probe_of(path: &Path) -> Option<PathBuf> {
    let mut probe = path;

    loop {
        if probe.metadata().is_ok() {
            return Some(probe.to_path_buf());
        }

        probe = probe.parent()?;
    }
}

/// Partition needs into the volumes they draw on, as indices into `sized`.
fn group_by_volume(sized: &[(&Need, u64, PathBuf)]) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();

    for (i, (_, _, probe)) in sized.iter().enumerate() {
        match groups
            .iter_mut()
            .find(|group| same_volume(&sized[group[0]].2, probe))
        {
            Some(group) => group.push(i),
            None => groups.push(vec![i]),
        }
    }

    groups
}

/// Whether two existing paths draw on the same pool of free bytes.
///
/// On Unix this is the filesystem's device id, which is exact in both
/// directions: a dedicated mount *under* `storage.path` reads as a different
/// volume, and two paths on one filesystem read as the same however differently
/// they are spelled — so `--scratch-dir` pointed elsewhere on the same disk is
/// still summed.
///
/// Elsewhere stable Rust exposes no device id, so the test is the canonical
/// path's **prefix** — the drive letter or UNC share, which is the coarsest
/// thing Windows calls a volume. Two directories on `C:` are one pool however
/// they are spelled and whether or not either contains the other, which is what
/// containment alone would have got wrong for two siblings. What it still
/// cannot see is a volume *mounted into a folder* on another drive: those read
/// as one pool and are two, so the need is over-stated. For a refusal with no
/// override that is the direction that gets reported, rather than the direction
/// that gets discovered at hour eight.
#[cfg(unix)]
fn same_volume(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    match (a.metadata(), b.metadata()) {
        (Ok(a), Ok(b)) => a.dev() == b.dev(),
        // Both were probed a moment ago, so this is a directory that went away
        // mid-check. Treating it as its own volume drops it out of every sum,
        // which under-states rather than over-states.
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_volume(a: &Path, b: &Path) -> bool {
    let (a, b) = match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return false,
    };

    match (a.components().next(), b.components().next()) {
        (Some(std::path::Component::Prefix(a)), Some(std::path::Component::Prefix(b))) => a == b,
        // No prefix to compare — not a shape a canonical Windows path has.
        // Fall back to the narrowest honest answer.
        _ => a == b,
    }
}

/// Every need here is a proportion of the free space the test measured, and
/// deliberately a coarse one: `check()` takes its own measurement of the same
/// volume, so an assertion pinned within a few bytes of what the test read is a
/// race against whatever else the machine is doing — under `cargo test`'s
/// threads, that includes the other tests in this module creating and dropping
/// temporary directories. A fixed byte cushion would not do: runner free space
/// differs by orders of magnitude across hosts, and only a proportion is
/// generous on all of them.
#[cfg(test)]
mod tests {
    use super::*;

    /// The whole of the free-space policy, over needs that share one volume.
    ///
    /// Both halves in one test because they are one rule: what was measured
    /// decides, and what was not is a warning that changes nothing. The sizes
    /// are taken from the volume the test is running on, so the assertions hold
    /// on any host.
    #[test]
    fn a_measured_shortfall_refuses_and_an_unmeasurable_need_does_not() {
        let temp = tempfile::tempdir().unwrap();
        let available = fs4::available_space(temp.path()).unwrap();

        check(&[Need::of("restoring it", temp.path(), available / 4)]).unwrap();

        let err = check(&[Need::of("restoring it", temp.path(), available / 2 * 3)]).unwrap_err();
        assert!(matches!(err, Error::NotEnoughSpace(_)), "{err:?}");

        // Nothing sized it, so nothing refuses — however impossible the run.
        check(&[Need::unsized_because(
            "staging this publish",
            temp.path(),
            "this repository holds no stele to size from",
        )])
        .unwrap();
    }

    /// Two needs on one volume are one need against one pool.
    ///
    /// Each of these fits on its own and the pair does not, so a check that
    /// passed them separately would pass this and a check that sums them
    /// refuses it.
    ///
    /// Both shapes the pair can take, because they are not the same test on
    /// every platform: `<destination>/scratch` is what the default makes of
    /// every restore that names no directory, and a *sibling* is what
    /// `--scratch-dir` next to the storage path makes of one that does.
    /// Containment answers the first and not the second.
    #[test]
    fn needs_sharing_a_volume_are_summed() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("data");
        std::fs::create_dir(&storage).unwrap();

        // A sibling only counts as one if it exists: an absent directory is
        // measured through its parent, which here is an *ancestor* of the
        // storage path and so would prove nothing about siblings.
        let beside = root.path().join("staging");
        std::fs::create_dir(&beside).unwrap();

        let available = fs4::available_space(&storage).unwrap();
        // One of these fits with a quarter of the pool to spare; the pair asks
        // half again as much as the whole of it.
        let each = available / 4 * 3;

        for scratch in [storage.join("scratch"), beside] {
            check(&[Need::of("restoring it", &storage, each)]).unwrap();
            check(&[Need::of("staging the layers it pulls", &scratch, each)]).unwrap();

            let err = check(&[
                Need::of("restoring it", &storage, each),
                Need::of("staging the layers it pulls", &scratch, each),
            ])
            .unwrap_err();

            let Error::NotEnoughSpace(message) = &err else {
                panic!("{err:?}");
            };

            assert!(message.contains("share one volume"), "{message}");

            for (what, path) in [
                ("restoring it", storage.clone()),
                ("staging the layers it pulls", scratch.clone()),
            ] {
                let part = format!("{what} ({each} bytes at {})", path.display());
                assert!(message.contains(&part), "{part:?} missing from {message:?}");
            }
        }
    }

    /// A directory that does not exist yet is measured through its parent —
    /// the shape a fresh node's storage path and an uncreated scratch
    /// directory both have.
    #[test]
    fn a_path_that_does_not_exist_yet_is_measured_through_its_parent() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("not").join("created").join("yet");

        assert_eq!(probe_of(&missing).as_deref(), Some(temp.path()));
        assert!(same_volume(temp.path(), &probe_of(&missing).unwrap()));

        check(&[Need::of("restoring it", &missing, 1)]).unwrap();
    }
}
