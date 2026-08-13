//! What a restore has already done, and what it has left to fetch.
//!
//! A restore of a stele at profile sizes is hours of download and ingestion,
//! and the interruptions it has to survive are the ordinary ones: a reboot, a
//! dropped connection, a ctrl-C. Without something on disk saying what landed,
//! the only answer is "start again". This module is that something, plus the
//! arithmetic a caller needs to say how much is left.
//!
//! ## The resume rule is content addressing, not a policy
//!
//! A layer is done when its **`diffId`** is in the progress file. That is the
//! whole rule, and it is a consequence of what a `diffId` *is*: sha256 over the
//! layer's uncompressed bytes. A match means the same bytes, so a layer
//! completed under an *older* inscription is still the layer this restore would
//! fetch — the stele it was published in has nothing to do with it.
//!
//! [`Resume`] therefore exposes exactly one question, [`Resume::is_done`], and
//! it takes a `Digest` and nothing else. There is deliberately no way to ask it
//! about a kind, a scope, a sequence or an inscription: comparing any of those
//! to decide a layer is done would be the wrong rule, and the cheapest place to
//! rule it out is the signature.
//!
//! ## What is protocol here, and what is not
//!
//! The file's **shape** and its invariants are protocol: an inscription digest
//! and a set of `diffId`s. What counts as a layer being **complete** is not —
//! that is the profile's commit boundary, and only a profile knows where its
//! writes become durable. So nothing in this module decides when to call
//! [`RestoreProgress::record`]; it only guarantees that what was recorded
//! survives.
//!
//! The file's **name and location** are the profile's too. ADR-004 spells them
//! `<storage.path>/.snapshot-restore.json` for the Dolos profile, and
//! "snapshot" is that profile's word for a stele rather than the protocol's —
//! so every entry point here takes a path a caller chose.
//!
//! ## What this module is not
//!
//! Not layer selection, and not the preflight. ADR-004's code-layout sketch
//! puts both here, and both belong to the profile for the reason the Dolos
//! restore driver states: a layer's `scope` is opaque to the protocol, so
//! nothing but a profile can read an epoch out of one, and nothing but a
//! profile knows which epochs a node wants.
//!
//! Not a renderer either. [`Remaining`] is a pair of numbers. Turning them into
//! a progress bar is a caller's business, and an observer here would be a
//! second one beside [`crate::progress`], which is the seam the export and
//! restore commands share and the only one either of them reports through.

use std::{
    collections::BTreeSet,
    io::Write as _,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{inscription::LayerDescriptor, transport::BlobIndex, Digest, Error, SteleReader};

/// A restore's progress, as it survives the process making it.
///
/// Exactly what ADR-004 asks the file to record: the inscription digest, and
/// the `diffId`s of the layers that are done. The digest is not what decides a
/// resume — see the module documentation — but it is what lets an operator, or
/// a later diagnostic, tell which stele a half-finished restore was aimed at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreProgress {
    /// The stele this restore is reading, by identity.
    pub inscription_digest: Digest,

    /// The layers whose records are committed, by identity.
    ///
    /// A set rather than a list: the order layers complete in carries no
    /// meaning, and an ordered set gives the file the same bytes for the same
    /// progress, which is one less thing to wonder about when reading it by
    /// hand.
    pub completed: BTreeSet<Digest>,
}

impl RestoreProgress {
    /// A restore of `inscription_digest` that has completed nothing.
    pub fn new(inscription_digest: Digest) -> Self {
        Self {
            inscription_digest,
            completed: BTreeSet::new(),
        }
    }

    /// Read the progress file at `path`, or `None` if there is none.
    ///
    /// **Only absence is `None`.** A file that exists and does not parse is an
    /// error, not an empty resume: treating it as absence would silently
    /// restart a restore from zero, which is the multi-hour outcome this
    /// file exists to prevent, arrived at without anything looking wrong.
    /// An operator who wants that outcome has `--force`, which says so.
    pub fn load(path: &Path) -> Result<Option<Self>, Error> {
        let raw = match std::fs::read(path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some(serde_json::from_slice(&raw)?))
    }

    /// Record that the layer identified by `diff_id` is committed.
    ///
    /// In memory only. [`RestoreProgress::save`] is what makes it survive, and
    /// the two are separate calls because a caller may want to record several
    /// layers against one write — though the Dolos driver does not, since one
    /// layer is one commit.
    pub fn record(&mut self, diff_id: Digest) {
        self.completed.insert(diff_id);
    }

    /// Write the progress file at `path`, atomically.
    ///
    /// Through a temporary sibling and a rename, because the failure this file
    /// exists to survive is a process that stops mid-write. A progress file
    /// truncated half way through its own `completed` array would be refused by
    /// [`RestoreProgress::load`] — correctly, and uselessly: the restore it
    /// described would have to start over, which is the thing being avoided. A
    /// rename is atomic on every filesystem this runs on, so a reader sees the
    /// previous complete file or the next one.
    pub fn save(&self, path: &Path) -> Result<(), Error> {
        let staging = staging_path(path);

        // Scoped so the handle is closed before the rename: Windows refuses to
        // rename a file that is still open.
        {
            let mut file = std::fs::File::create(&staging)?;

            file.write_all(&serde_json::to_vec(self)?)?;

            // Before the rename, not after: a rename that lands pointing at
            // bytes the page cache has not written yet is the same truncated
            // file by another route.
            file.sync_all()?;
        }

        std::fs::rename(&staging, path)?;

        Ok(())
    }

    /// Delete the progress file at `path`.
    ///
    /// A file that is not there is not an error: this is called after a restore
    /// finishes, and a restore that never had to checkpoint anything is a
    /// restore that finished too.
    pub fn remove(path: &Path) -> Result<(), Error> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// The resume this progress permits.
    pub fn resume(&self) -> Resume {
        Resume {
            completed: self.completed.clone(),
        }
    }
}

fn staging_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    path.with_file_name(format!(".{name}.{}.tmp", std::process::id()))
}

/// Which layers a restore may skip because they are already done.
///
/// One question, taking one kind of argument. See the module documentation for
/// why the shape is the point: a `diffId` match means the same bytes, and
/// nothing else about a layer is evidence that it landed.
///
/// **A caller must only ask about layers that are immutable.** The protocol
/// cannot tell which those are — a layer's `scope` is the profile's — so this
/// type answers whatever it is asked, and it is the profile's job never to ask
/// about a layer that describes a moving tip. In the Dolos profile that means
/// the epoch layers may be skipped and the state shards never are.
#[derive(Debug, Clone, Default)]
pub struct Resume {
    completed: BTreeSet<Digest>,
}

impl Resume {
    /// A restore that starts from nothing — the ordinary case, and what a
    /// caller without `--continue` passes.
    pub fn none() -> Self {
        Self::default()
    }

    /// The resume a progress file permits, or a fresh start if there is none.
    ///
    /// Takes the completed set whatever inscription it was recorded under. That
    /// is the resume rule, not an oversight: an epoch layer is named by its
    /// content, so a newer stele describing the same layer describes the same
    /// bytes.
    pub fn from_progress(progress: Option<&RestoreProgress>) -> Self {
        match progress {
            Some(progress) => progress.resume(),
            None => Self::none(),
        }
    }

    /// Whether the layer identified by `diff_id` is already committed.
    pub fn is_done(&self, diff_id: &Digest) -> bool {
        self.completed.contains(diff_id)
    }

    /// How many layers this resume carries. For a caller reporting what it
    /// inherited from an earlier attempt.
    pub fn len(&self) -> usize {
        self.completed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.completed.is_empty()
    }
}

/// What a restore still has to move over the wire.
///
/// Compressed bytes, because that is what a download costs and what a time
/// estimate divides by a rate. The inscription carries only *uncompressed*
/// sizes — identity does not depend on a compressor — so these come from the
/// transport, through [`SteleReader::compressed_size`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Remaining {
    /// Layers still to fetch.
    pub layers: usize,

    /// Compressed bytes across those layers, as the transport states them.
    pub compressed_bytes: u64,

    /// Layers the transport could not state a compressed size for, and which
    /// `compressed_bytes` therefore does not include.
    ///
    /// Carried rather than folded in, so an estimate that is missing something
    /// says so. A total that silently under-reports is worse than one a caller
    /// can see the shape of.
    pub unsized_layers: usize,

    /// The largest of those layers, compressed.
    ///
    /// A restore stages one layer at a time — pulled, staged, drained and
    /// dropped in sequence — so what its scratch volume has to hold at once is
    /// this and never [`Remaining::compressed_bytes`]. Summed here rather than
    /// in a second pass because the walk that sums the total is already
    /// visiting every size there is.
    ///
    /// `None` when nothing is left to fetch, and when no remaining layer could
    /// be sized at all.
    pub largest_compressed: Option<u64>,
}

impl Remaining {
    /// Sum what `layers` will cost to fetch from `stele`.
    ///
    /// The caller has already decided *which* layers those are — dropped what a
    /// resume says is done, and what its own selection never wanted. That split
    /// is the same one the rest of this module keeps: which layers a restore
    /// needs is the profile's question, and how big they are is the
    /// transport's.
    ///
    /// One pass answers both questions a caller has about these bytes: what
    /// they cost to move, and — through [`Remaining::largest_compressed`] —
    /// what has to fit on disk while one of them is being moved.
    pub fn of<'a, R, I>(stele: &R, index: &BlobIndex, layers: I) -> Result<Self, Error>
    where
        R: SteleReader,
        I: IntoIterator<Item = &'a LayerDescriptor>,
    {
        let mut remaining = Self::default();

        for descriptor in layers {
            remaining.layers += 1;

            match stele.compressed_size(index, descriptor)? {
                Some(bytes) => {
                    remaining.compressed_bytes += bytes;
                    remaining.largest_compressed = remaining.largest_compressed.max(Some(bytes));
                }
                None => remaining.unsized_layers += 1,
            }
        }

        Ok(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Digest {
        Digest::from_bytes([byte; 32])
    }

    #[test]
    fn a_progress_file_round_trips_through_the_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".snapshot-restore.json");

        assert_eq!(RestoreProgress::load(&path).unwrap(), None);

        let mut progress = RestoreProgress::new(digest(0xaa));
        progress.record(digest(1));
        progress.record(digest(2));
        progress.save(&path).unwrap();

        assert_eq!(RestoreProgress::load(&path).unwrap(), Some(progress));

        RestoreProgress::remove(&path).unwrap();
        assert_eq!(RestoreProgress::load(&path).unwrap(), None);

        // Removing what is not there is what the end of a restore that never
        // checkpointed does.
        RestoreProgress::remove(&path).unwrap();
    }

    /// A save leaves the file and nothing beside it: the staging sibling is
    /// renamed, not left for the next reader to trip over.
    #[test]
    fn a_save_leaves_one_file_behind() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".snapshot-restore.json");

        let progress = RestoreProgress::new(digest(0xaa));

        progress.save(&path).unwrap();
        progress.save(&path).unwrap();

        let found: Vec<String> = std::fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(found, vec![".snapshot-restore.json".to_owned()]);
    }

    /// Absence is a fresh start; a file that does not parse is not.
    ///
    /// The two are a sentence apart in the code and hours apart for an
    /// operator: reading a corrupt file as "nothing has been done" restarts
    /// a restore from zero without anything looking wrong.
    #[test]
    fn a_corrupt_progress_file_is_not_an_empty_one() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".snapshot-restore.json");

        std::fs::write(&path, b"{\"inscriptionDigest\": \"not a digest\"").unwrap();

        assert!(RestoreProgress::load(&path).is_err());
    }

    /// The resume rule, stated as the test that would fail if anything but a
    /// `diffId` were being compared: the progress was recorded under one
    /// inscription and is read while restoring another.
    #[test]
    fn a_layer_stays_done_across_an_inscription_change() {
        let mut progress = RestoreProgress::new(digest(0xaa));
        progress.record(digest(1));

        let resume = Resume::from_progress(Some(&progress));

        assert!(resume.is_done(&digest(1)));
        assert!(!resume.is_done(&digest(2)));
        assert_eq!(resume.len(), 1);

        // The same set, now consulted while restoring a different stele. The
        // digest the file records is not an input to the question, which is the
        // whole of the rule.
        let newer = RestoreProgress {
            inscription_digest: digest(0xbb),
            completed: progress.completed.clone(),
        };

        assert!(Resume::from_progress(Some(&newer)).is_done(&digest(1)));
    }

    #[test]
    fn no_progress_is_a_resume_that_skips_nothing() {
        let resume = Resume::from_progress(None);

        assert!(resume.is_empty());
        assert!(!resume.is_done(&digest(1)));
    }
}
